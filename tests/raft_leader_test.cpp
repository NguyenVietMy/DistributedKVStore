#include "dkv/raft_leader.hpp"

#include "dkv/persistent_log.hpp"
#include "dkv/persistent_metadata.hpp"
#include "dkv/raft_apply.hpp"
#include "dkv/raft_election.hpp"

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace {
    int failures = 0;

    void expect(bool condition, std::string_view message) {
        if (!condition) {
            std::cerr << "FAIL: " << message << '\n';
            ++failures;
        }
    }

    class TempDirectory {
    public:
        TempDirectory() {
            const auto name = (std::filesystem::temp_directory_path() / "dkv-leader-XXXXXX").string();
            std::vector<char> path(name.begin(), name.end());
            path.push_back('\0');
            if (char* created = ::mkdtemp(path.data())) path_ = created;
        }
        ~TempDirectory() {
            if (!path_.empty()) std::filesystem::remove_all(path_);
        }
        const std::filesystem::path& path() const { return path_; }
    private:
        std::filesystem::path path_;
    };

    struct NodeStorage {
        TempDirectory dir;
        std::unique_ptr<dkv::PersistentMetadata> metadata;
        std::unique_ptr<dkv::PersistentLog> log;
        std::uint64_t commit_index{0};

        NodeStorage() {
            if (dir.path().empty()) {
                expect(false, "could not create temporary directory");
                return;
            }
            auto metadata_result = dkv::PersistentMetadata::open(dir.path() / "raft.meta");
            auto* opened_metadata =
                std::get_if<std::unique_ptr<dkv::PersistentMetadata>>(&metadata_result);
            if (opened_metadata) metadata = std::move(*opened_metadata);

            auto log_result = dkv::PersistentLog::open(dir.path() / "raft.log");
            auto* opened_log = std::get_if<std::unique_ptr<dkv::PersistentLog>>(&log_result);
            if (opened_log) log = std::move(*opened_log);
            expect(ready(), "could not open test node storage");
        }

        bool ready() const { return metadata && log; }
    };

    dkv::LogEntry no_op(std::uint64_t index, std::uint64_t term) {
        return {index, term, {dkv::CommandType::NoOp, "", ""}};
    }

    void append(NodeStorage& node, const dkv::LogEntry& entry) {
        expect(node.log->append(entry) == dkv::PersistentLogAppendResult::Appended,
               "could not prepare test log");
    }

    std::unique_ptr<dkv::RaftElection> elect_leader(NodeStorage& node) {
        auto election = std::make_unique<dkv::RaftElection>(
            1, std::array<std::uint64_t, 2>{2, 3}, *node.metadata, *node.log);
        const auto start = election->start_election();
        const auto* request = std::get_if<dkv::RequestVote>(&start);
        expect(request != nullptr, "leader election did not start");
        if (!request) return nullptr;
        expect(election->handle_vote_reply(2, {request->term, true}) ==
                   dkv::VoteReplyOutcome::BecameLeader,
               "leader election did not reach a majority");
        return election;
    }

    dkv::AppendEntries request_from(dkv::RaftLeaderReplication& leader,
                                    std::uint64_t peer_id, std::size_t max_entries = 64) {
        auto result = leader.make_request(peer_id, max_entries);
        const auto* request = std::get_if<dkv::AppendEntries>(&result);
        expect(request != nullptr, "leader could not make AppendEntries request");
        return request ? *request : dkv::AppendEntries{};
    }

    dkv::AppendEntriesReply reply_from(NodeStorage& follower,
                                       const dkv::AppendEntries& request) {
        const auto result = dkv::handle_append_entries(
            *follower.metadata, *follower.log, follower.commit_index, request);
        const auto* reply = std::get_if<dkv::AppendEntriesReply>(&result);
        expect(reply != nullptr, "follower did not return an AppendEntries reply");
        return reply ? *reply : dkv::AppendEntriesReply{};
    }

    void test_backtracking_and_current_term_commit() {
        NodeStorage leader_node;
        NodeStorage follower;
        if (!leader_node.ready() || !follower.ready()) return;
        append(leader_node, no_op(1, 1));
        append(leader_node, no_op(2, 1));
        append(leader_node, no_op(3, 2));
        append(follower, no_op(1, 1));
        append(follower, no_op(2, 2));
        expect(leader_node.metadata->advance_term(2) ==
                   dkv::PersistentMetadataUpdateResult::Persisted,
               "could not prepare term two");
        auto election = elect_leader(leader_node);
        if (!election) return;
        dkv::RaftLeaderReplication leader{
            *election, *leader_node.metadata, *leader_node.log, leader_node.commit_index};

        const auto probe = request_from(leader, 2);
        expect(probe.term == 3 && probe.prev_log_index == 3 && probe.entries.empty(),
               "initial request did not probe the leader's last index");
        const auto rejected = reply_from(follower, probe);
        expect(rejected == dkv::AppendEntriesReply{3, false},
               "follower accepted a missing preceding entry");
        expect(leader.handle_reply(2, probe, rejected) == dkv::AppendReplyOutcome::Retry &&
               leader.progress(2) == dkv::PeerProgress{3, 0},
               "leader did not backtrack after rejection");

        const auto conflicting = request_from(leader, 2);
        expect(conflicting.prev_log_index == 2 && conflicting.prev_log_term == 1 &&
               conflicting.entries == std::vector<dkv::LogEntry>{no_op(3, 2)},
               "first retry did not test the preceding term");
        expect(leader.handle_reply(2, probe, {3, true}) == dkv::AppendReplyOutcome::Ignored,
               "late reply changed an outstanding request");
        expect(leader.handle_reply(2, conflicting, reply_from(follower, conflicting)) ==
                   dkv::AppendReplyOutcome::Retry &&
               leader.progress(2) == dkv::PeerProgress{2, 0},
               "leader did not backtrack past a conflicting entry");

        const auto catchup = request_from(leader, 2);
        expect(catchup.prev_log_index == 1 && catchup.prev_log_term == 1 &&
               catchup.entries ==
                   std::vector<dkv::LogEntry>{no_op(2, 1), no_op(3, 2)},
               "retry did not send the entries after the matching prefix");
        expect(leader.handle_reply(2, catchup, reply_from(follower, catchup)) ==
                   dkv::AppendReplyOutcome::Recorded,
               "old-term entry was counted as committed");
        expect(leader_node.commit_index == 0 &&
               leader.progress(2) == dkv::PeerProgress{4, 3} &&
               follower.log->entry_at(2) == no_op(2, 1),
               "old-term majority advanced commit index or lost follower progress");

        const auto appended = leader.append_command({dkv::CommandType::NoOp, "", ""});
        expect(std::get_if<std::uint64_t>(&appended) &&
               *std::get_if<std::uint64_t>(&appended) == 4,
               "leader did not append a current-term no-op");
        const auto current_term = request_from(leader, 2);
        expect(current_term.prev_log_index == 3 &&
               current_term.entries == std::vector<dkv::LogEntry>{no_op(4, 3)},
               "leader did not send the new current-term entry");
        expect(leader.handle_reply(2, current_term, reply_from(follower, current_term)) ==
                   dkv::AppendReplyOutcome::Committed,
               "current-term majority did not commit the log");
        expect(leader_node.commit_index == 4 && follower.commit_index == 0,
               "leader commit or follower notification happened at the wrong time");

        const auto heartbeat = request_from(leader, 2);
        expect(heartbeat.entries.empty() && heartbeat.prev_log_index == 4 &&
               heartbeat.leader_commit == 4,
               "heartbeat did not carry the new commit index");
        expect(leader.handle_reply(2, heartbeat, reply_from(follower, heartbeat)) ==
                   dkv::AppendReplyOutcome::Recorded && follower.commit_index == 4,
               "follower did not learn the committed prefix");
    }

    void test_majority_commits_only_the_acknowledged_prefix() {
        NodeStorage leader_node;
        NodeStorage follower;
        if (!leader_node.ready() || !follower.ready()) return;
        auto election = elect_leader(leader_node);
        if (!election) return;
        dkv::RaftLeaderReplication leader{
            *election, *leader_node.metadata, *leader_node.log, leader_node.commit_index};
        for (const auto value : {"one", "two", "three"}) {
            const auto result = leader.append_command({dkv::CommandType::Put, "key", value});
            expect(std::get_if<std::uint64_t>(&result) != nullptr,
                   "leader could not durably append a client command");
        }
        expect(leader_node.log->last_index() == 3 && leader_node.commit_index == 0,
               "unreplicated commands were committed");

        const auto first = request_from(leader, 3, 1);
        expect(first.prev_log_index == 0 && first.entries.size() == 1 &&
               first.entries[0].index == 1,
               "bounded request did not send only the first entry");
        expect(leader.make_request(3, 1) == dkv::MakeAppendEntriesResult{first},
               "lost request was not retried unchanged");
        expect(leader.handle_reply(3, first, reply_from(follower, first)) ==
                   dkv::AppendReplyOutcome::Committed,
               "first durable follower acknowledgment was not committed");
        expect(leader_node.commit_index == 1 && leader_node.log->last_index() == 3 &&
               leader.progress(3) == dkv::PeerProgress{2, 1} &&
               leader.progress(2) == dkv::PeerProgress{1, 0},
               "one acknowledged entry committed later unsent entries");
        dkv::KvStateMachine state;
        const auto first_application =
            dkv::apply_committed(*leader_node.log, state, leader_node.commit_index);
        expect(std::get_if<std::vector<dkv::AppliedLogEntry>>(&first_application) &&
               state.get("key") == "one" && state.last_applied() == 1,
               "leader applied an uncommitted suffix or missed the committed entry");

        const auto remaining = request_from(leader, 3);
        expect(remaining.prev_log_index == 1 && remaining.entries.size() == 2 &&
               remaining.entries[0].index == 2 && remaining.entries[1].index == 3 &&
               remaining.leader_commit == 1,
               "second request did not send the remaining entries and commit index");
        expect(leader.handle_reply(3, remaining, reply_from(follower, remaining)) ==
                   dkv::AppendReplyOutcome::Committed,
               "remaining entries did not commit after follower sync");
        expect(leader_node.commit_index == 3 &&
               leader.progress(3) == dkv::PeerProgress{4, 3},
               "leader did not advance to the acknowledged prefix");
        const auto final_application =
            dkv::apply_committed(*leader_node.log, state, leader_node.commit_index);
        expect(std::get_if<std::vector<dkv::AppliedLogEntry>>(&final_application) &&
               state.get("key") == "three" && state.last_applied() == 3,
               "leader did not apply the newly committed entries in order");
    }

    void test_invalid_commands_and_higher_term_reply() {
        NodeStorage node;
        if (!node.ready()) return;
        auto election = elect_leader(node);
        if (!election) return;
        dkv::RaftLeaderReplication leader{*election, *node.metadata, *node.log, node.commit_index};

        const auto invalid = leader.append_command({dkv::CommandType::Delete, "key", "value"});
        expect(invalid == dkv::LeaderAppendResult{dkv::LeaderReplicationError::InvalidCommand} &&
               node.log->last_index() == 0,
               "invalid local command changed the log");
        const auto request = request_from(leader, 2);
        expect(leader.handle_reply(99, request, {99, false}) == dkv::AppendReplyOutcome::Ignored &&
               node.metadata->state().current_term == 1,
               "unknown peer changed the term");
        expect(leader.handle_reply(2, request, {2, false}) ==
                   dkv::AppendReplyOutcome::SteppedDown,
               "higher-term reply did not make leader step down");
        expect(!leader.active() && election->role() == dkv::RaftRole::Follower &&
               node.metadata->state().current_term == 2,
               "higher term was not persisted or election role stayed leader");
        expect(leader.make_request(2) ==
                   dkv::MakeAppendEntriesResult{dkv::LeaderReplicationError::NotLeader} &&
               leader.append_command({dkv::CommandType::NoOp, "", ""}) ==
                   dkv::LeaderAppendResult{dkv::LeaderReplicationError::NotLeader},
               "stepped-down leader still accepted work");

        auto reopened = dkv::PersistentMetadata::open(node.dir.path() / "raft.meta");
        const auto* metadata = std::get_if<std::unique_ptr<dkv::PersistentMetadata>>(&reopened);
        expect(metadata && *metadata && (*metadata)->state().current_term == 2,
               "higher term did not survive restart");
    }
}

int main() {
    test_backtracking_and_current_term_commit();
    test_majority_commits_only_the_acknowledged_prefix();
    test_invalid_commands_and_higher_term_reply();
    if (failures != 0) {
        std::cerr << failures << " test check(s) failed\n";
        return 1;
    }
    std::cout << "SUCCESS\n";
}

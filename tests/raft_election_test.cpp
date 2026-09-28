#include "dkv/raft_election.hpp"

#include "dkv/persistent_log.hpp"
#include "dkv/persistent_metadata.hpp"
#include "dkv/raft_vote.hpp"

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
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
            const auto name = (std::filesystem::temp_directory_path() / "dkv-election-XXXXXX").string();
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

    std::unique_ptr<dkv::PersistentMetadata> open_metadata(const std::filesystem::path& path) {
        auto result = dkv::PersistentMetadata::open(path);
        auto* metadata = std::get_if<std::unique_ptr<dkv::PersistentMetadata>>(&result);
        expect(metadata != nullptr && *metadata != nullptr, "could not open test metadata");
        return metadata ? std::move(*metadata) : nullptr;
    }

    std::unique_ptr<dkv::PersistentLog> open_log(const std::filesystem::path& path) {
        auto result = dkv::PersistentLog::open(path);
        auto* log = std::get_if<std::unique_ptr<dkv::PersistentLog>>(&result);
        expect(log != nullptr && *log != nullptr, "could not open test log");
        return log ? std::move(*log) : nullptr;
    }

    void expect_request(const dkv::ElectionStartResult& result,
                        const dkv::RequestVote& wanted,
                        std::string_view message) {
        const auto* request = std::get_if<dkv::RequestVote>(&result);
        expect(request != nullptr && *request == wanted, message);
    }

    void expect_start_error(const dkv::ElectionStartResult& result,
                            dkv::ElectionStartError wanted,
                            std::string_view message) {
        const auto* error = std::get_if<dkv::ElectionStartError>(&result);
        expect(error != nullptr && *error == wanted, message);
    }

    void test_self_vote_request_and_majority() {
        TempDirectory dir;
        if (dir.path().empty()) {
            expect(false, "could not create temporary directory");
            return;
        }
        const auto metadata_path = dir.path() / "raft.meta";
        auto metadata = open_metadata(metadata_path);
        auto log = open_log(dir.path() / "raft.log");
        if (!metadata || !log) return;
        using R = dkv::PersistentMetadataUpdateResult;
        expect(metadata->advance_term(4) == R::Persisted, "could not set up current term");
        expect(log->append({1, 2, {dkv::CommandType::NoOp, "", ""}}) ==
                   dkv::PersistentLogAppendResult::Appended, "could not append first test entry");
        expect(log->append({2, 3, {dkv::CommandType::NoOp, "", ""}}) ==
                   dkv::PersistentLogAppendResult::Appended, "could not append second test entry");

        dkv::RaftElection election{0, {1, 2}, *metadata, *log};
        expect(election.role() == dkv::RaftRole::Follower && election.granted_votes() == 0,
               "new election object did not start as follower");
        expect_request(election.start_election(), {5, 0, 2, 3},
                       "election request did not include new term and last log position");
        expect(election.role() == dkv::RaftRole::Candidate && election.granted_votes() == 1,
               "candidate did not count its own vote");
        auto recovered = open_metadata(metadata_path);
        if (recovered) expect(recovered->state() == dkv::RaftMetadata{5, 0},
                              "self-vote was not durable before request was produced");

        expect(election.handle_vote_reply(99, {100, true}) == dkv::VoteReplyOutcome::Ignored,
               "unknown peer changed the election");
        expect(election.handle_vote_reply(1, {5, true}) == dkv::VoteReplyOutcome::BecameLeader,
               "self-vote plus one peer vote did not win");
        expect(election.role() == dkv::RaftRole::Leader && election.granted_votes() == 2,
               "elected leader has wrong role or vote count");
        expect(election.handle_vote_reply(1, {5, true}) == dkv::VoteReplyOutcome::Ignored,
               "duplicate vote was counted");
        expect(election.handle_vote_reply(2, {5, true}) == dkv::VoteReplyOutcome::Ignored,
               "late vote changed an already completed election");
        expect(election.granted_votes() == 2, "late or duplicate vote changed count");
        expect_start_error(election.start_election(), dkv::ElectionStartError::AlreadyLeader,
                           "leader started another election");
        expect(election.handle_vote_reply(2, {6, false}) == dkv::VoteReplyOutcome::SteppedDown,
               "leader did not step down on a higher term");
        expect(election.role() == dkv::RaftRole::Follower && election.granted_votes() == 0,
               "higher term did not clear leader election state");
        expect(metadata->state() == dkv::RaftMetadata{6, std::nullopt},
               "higher reply term was not persisted");
    }

    void test_split_vote_retry_ignores_old_and_duplicate_replies() {
        TempDirectory dir;
        if (dir.path().empty()) return;
        auto metadata = open_metadata(dir.path() / "raft.meta");
        auto log = open_log(dir.path() / "raft.log");
        if (!metadata || !log) return;
        dkv::RaftElection election{0, {1, 2}, *metadata, *log};

        expect_request(election.start_election(), {1, 0, 0, 0},
                       "first election did not start in term one");
        expect(election.handle_vote_reply(1, {1, false}) == dkv::VoteReplyOutcome::Recorded,
               "denied vote was not recorded");
        expect(election.handle_vote_reply(1, {1, true}) == dkv::VoteReplyOutcome::Ignored,
               "second response from same peer was counted");
        expect(election.handle_vote_reply(2, {1, false}) == dkv::VoteReplyOutcome::Recorded,
               "second denied vote was not recorded");
        expect(election.role() == dkv::RaftRole::Candidate && election.granted_votes() == 1,
               "split vote incorrectly elected a leader");

        expect_request(election.start_election(), {2, 0, 0, 0},
                       "retry did not advance to term two");
        expect(election.handle_vote_reply(2, {1, true}) == dkv::VoteReplyOutcome::Ignored,
               "late term-one vote affected term two");
        expect(election.handle_vote_reply(1, {2, true}) == dkv::VoteReplyOutcome::BecameLeader,
               "peer could not vote in a new term");
        expect(election.granted_votes() == 2, "retry counted stale votes");
    }

    void test_higher_term_and_receiver_update_stop_candidate() {
        TempDirectory dir;
        if (dir.path().empty()) return;
        const auto metadata_path = dir.path() / "raft.meta";
        auto metadata = open_metadata(metadata_path);
        auto log = open_log(dir.path() / "raft.log");
        if (!metadata || !log) return;
        dkv::RaftElection election{0, {1, 2}, *metadata, *log};

        expect_request(election.start_election(), {1, 0, 0, 0}, "could not start election");
        expect(election.handle_vote_reply(1, {3, false}) == dkv::VoteReplyOutcome::SteppedDown,
               "candidate did not step down on a higher-term reply");
        expect(metadata->state() == dkv::RaftMetadata{3, std::nullopt},
               "higher term was not saved or self-vote was not cleared");
        auto recovered = open_metadata(metadata_path);
        if (recovered) expect(recovered->state() == dkv::RaftMetadata{3, std::nullopt},
                              "higher reply term did not survive reopening");
        expect(election.handle_vote_reply(2, {1, true}) == dkv::VoteReplyOutcome::Ignored,
               "old reply revived a stopped election");
        expect_request(election.start_election(), {4, 0, 0, 0},
                       "new election did not start after observing higher term");

        const auto inbound = dkv::handle_request_vote(*metadata, *log, {5, 2, 0, 0});
        const auto* reply = std::get_if<dkv::RequestVoteReply>(&inbound);
        expect(reply != nullptr && *reply == dkv::RequestVoteReply{5, true},
               "could not grant higher-term inbound vote");
        expect(election.role() == dkv::RaftRole::Follower,
               "external metadata term change left candidate role visible");
        expect(election.handle_vote_reply(1, {4, true}) == dkv::VoteReplyOutcome::SteppedDown,
               "candidate counted a vote after granting a higher-term request");
    }

    void test_persistence_failures_produce_no_request_or_leader() {
        TempDirectory dir;
        if (dir.path().empty()) return;
        const auto metadata_dir = dir.path() / "metadata";
        std::filesystem::create_directory(metadata_dir);
        auto metadata = open_metadata(metadata_dir / "raft.meta");
        auto log = open_log(dir.path() / "raft.log");
        if (!metadata || !log) return;
        dkv::RaftElection election{0, {1, 2}, *metadata, *log};

        std::filesystem::remove(metadata_dir);
        expect_start_error(election.start_election(), dkv::ElectionStartError::PersistenceError,
                           "failed term update produced a vote request");
        expect(election.role() == dkv::RaftRole::Follower && election.granted_votes() == 0,
               "failed election remained candidate");
        std::filesystem::create_directory(metadata_dir);
        expect_start_error(election.start_election(), dkv::ElectionStartError::PersistenceError,
                           "failed election resumed without recovery");

        auto metadata2 = open_metadata(metadata_dir / "raft.meta");
        if (!metadata2) return;
        dkv::RaftElection second{0, {1, 2}, *metadata2, *log};
        expect_request(second.start_election(), {1, 0, 0, 0},
                       "could not start second election");
        std::filesystem::remove_all(metadata_dir);
        expect(second.handle_vote_reply(1, {2, false}) == dkv::VoteReplyOutcome::PersistenceError,
               "failed higher-term update did not report an error");
        expect(second.role() == dkv::RaftRole::Follower,
               "failed higher-term update left node as candidate");
    }

    void test_term_overflow_is_rejected() {
        TempDirectory dir;
        if (dir.path().empty()) return;
        auto metadata = open_metadata(dir.path() / "raft.meta");
        auto log = open_log(dir.path() / "raft.log");
        if (!metadata || !log) return;
        expect(metadata->advance_term(std::numeric_limits<std::uint64_t>::max()) ==
                   dkv::PersistentMetadataUpdateResult::Persisted,
               "could not set up maximum term");
        dkv::RaftElection election{0, {1, 2}, *metadata, *log};
        expect_start_error(election.start_election(), dkv::ElectionStartError::TermExhausted,
                           "maximum term wrapped around");
        expect(election.role() == dkv::RaftRole::Follower,
               "term overflow changed role");
    }
}

int main() {
    test_self_vote_request_and_majority();
    test_split_vote_retry_ignores_old_and_duplicate_replies();
    test_higher_term_and_receiver_update_stop_candidate();
    test_persistence_failures_produce_no_request_or_leader();
    test_term_overflow_is_rejected();
    if (failures != 0) {
        std::cerr << failures << " test check(s) failed\n";
        return 1;
    }
    std::cout << "SUCCESS\n";
}

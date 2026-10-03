#include "dkv/raft_node.hpp"

#include <array>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
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
            const auto name =
                (std::filesystem::temp_directory_path() / "dkv-node-XXXXXX").string();
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

    struct ClusterNode {
        TempDirectory dir;
        std::unique_ptr<dkv::RaftNode> node;

        ClusterNode(std::uint64_t id, std::array<std::uint64_t, 2> peers) {
            if (dir.path().empty()) return;
            auto opened = dkv::RaftNode::open(id, peers, dir.path());
            if (auto* value = std::get_if<std::unique_ptr<dkv::RaftNode>>(&opened)) {
                node = std::move(*value);
            }
            expect(node != nullptr, "could not open cluster node");
        }
    };

    const dkv::RaftNodeActions* actions(const dkv::RaftNodeResult& result,
                                        std::string_view context) {
        const auto* value = std::get_if<dkv::RaftNodeActions>(&result);
        expect(value != nullptr, context);
        return value;
    }

    template <class T>
    std::optional<T> message_to(const dkv::RaftNodeActions& actions,
                                std::uint64_t peer_id) {
        for (const auto& outbound : actions.messages) {
            if (outbound.peer_id != peer_id) continue;
            if (const auto* value = std::get_if<T>(&outbound.message)) return *value;
        }
        expect(false, "missing outbound message");
        return std::nullopt;
    }

    struct ElectedCluster {
        ClusterNode a{1, {2, 3}};
        ClusterNode b{2, {1, 3}};
        ClusterNode c{3, {1, 2}};

        bool ready() const { return a.node && b.node && c.node; }

        std::optional<dkv::RaftNodeActions> elect_a() {
            if (!ready()) return std::nullopt;
            auto started = a.node->on_election_timeout();
            const auto* request_actions = actions(started, "A did not start election");
            if (!request_actions) return std::nullopt;
            expect(request_actions->reset_election_timer &&
                       request_actions->messages.size() == 2,
                   "candidate did not request both votes and reset its timer");
            const auto request = message_to<dkv::RequestVote>(*request_actions, 2);
            if (!request) return std::nullopt;
            expect(a.node->role() == dkv::RaftRole::Candidate &&
                       request->term == 1 && request->candidate_id == 1,
                   "A did not become a term-one candidate");

            auto voted = b.node->on_request_vote(1, *request);
            const auto* vote_actions = actions(voted, "B did not process vote request");
            if (!vote_actions) return std::nullopt;
            const auto vote = message_to<dkv::RequestVoteReply>(*vote_actions, 1);
            if (!vote) return std::nullopt;
            expect(vote_actions->reset_election_timer && vote->vote_granted &&
                       vote->term == 1,
                   "B did not grant and durably record its vote");

            auto elected = a.node->on_vote_reply(2, *vote);
            const auto* leader_actions = actions(elected, "A did not process granted vote");
            if (!leader_actions) return std::nullopt;
            expect(a.node->role() == dkv::RaftRole::Leader &&
                       a.node->last_index() == 1 && a.node->commit_index() == 0 &&
                       leader_actions->schedule_heartbeat,
                   "A did not append an uncommitted no-op on leadership");
            return *leader_actions;
        }
    };

    void test_election_noop_commit_and_follower_notification() {
        ElectedCluster cluster;
        const auto elected = cluster.elect_a();
        if (!elected) return;
        const auto first_b = message_to<dkv::AppendEntries>(*elected, 2);
        const auto first_c = message_to<dkv::AppendEntries>(*elected, 3);
        if (!first_b || !first_c) return;
        expect(first_b->entries.size() == 1 &&
                   first_b->entries[0].command.type == dkv::CommandType::NoOp &&
                   first_b->entries[0].term == 1 && first_b->leader_commit == 0,
               "first AppendEntries did not contain the current-term no-op");

        auto appended_b = cluster.b.node->on_append_entries(1, *first_b);
        const auto* b_actions = actions(appended_b, "B did not process AppendEntries");
        if (!b_actions) return;
        const auto reply_b = message_to<dkv::AppendEntriesReply>(*b_actions, 1);
        if (!reply_b) return;
        expect(reply_b->success && b_actions->reset_election_timer &&
                   cluster.b.node->known_leader() == 1 &&
                   cluster.b.node->last_index() == 1 &&
                   cluster.b.node->commit_index() == 0,
               "B did not durably append the no-op before acknowledging");

        auto committed = cluster.a.node->on_append_reply(2, *first_b, *reply_b);
        const auto* commit_actions = actions(committed, "A did not process majority ack");
        if (!commit_actions) return;
        expect(cluster.a.node->commit_index() == 1 &&
                   cluster.a.node->last_applied() == 1 &&
                   commit_actions->applied ==
                       std::vector<dkv::AppliedLogEntry>{{1, dkv::ApplyResult::Applied}},
               "A did not commit and apply no-op after majority acknowledgment");
        const auto notify_b = message_to<dkv::AppendEntries>(*commit_actions, 2);
        if (!notify_b) return;
        expect(notify_b->leader_commit == 1 && notify_b->entries.empty(),
               "commit notification to B did not carry leader_commit");
        const auto notified_b = cluster.b.node->on_append_entries(1, *notify_b);
        const auto* notified_b_actions =
            actions(notified_b, "B did not process commit notification");
        if (!notified_b_actions) return;
        expect(cluster.b.node->commit_index() == 1 &&
                   cluster.b.node->last_applied() == 1,
               "B did not apply the committed no-op");

        // C's first request was already outstanding when the commit advanced.
        auto appended_c = cluster.c.node->on_append_entries(1, *first_c);
        const auto* c_actions = actions(appended_c, "C did not process first append");
        if (!c_actions) return;
        const auto reply_c = message_to<dkv::AppendEntriesReply>(*c_actions, 1);
        if (!reply_c) return;
        expect(cluster.c.node->last_index() == 1 &&
                   cluster.c.node->commit_index() == 0,
               "C applied a no-op before learning its commit index");
        const auto recorded_c = cluster.a.node->on_append_reply(3, *first_c, *reply_c);
        if (!actions(recorded_c, "A did not record C's acknowledgment")) return;
        auto heartbeat = cluster.a.node->on_heartbeat_timeout();
        const auto* heartbeat_actions = actions(heartbeat, "A did not send heartbeat");
        if (!heartbeat_actions) return;
        expect(heartbeat_actions->schedule_heartbeat,
               "leader did not schedule the next heartbeat");
        const auto notify_c = message_to<dkv::AppendEntries>(*heartbeat_actions, 3);
        if (!notify_c) return;
        const auto notified_c = cluster.c.node->on_append_entries(1, *notify_c);
        expect(actions(notified_c, "C did not process heartbeat") &&
                   cluster.c.node->commit_index() == 1 &&
                   cluster.c.node->last_applied() == 1,
               "C did not apply the committed no-op after heartbeat");
    }

    void test_same_term_candidate_steps_down_for_leader() {
        ElectedCluster cluster;
        if (!cluster.ready()) return;
        const auto candidate = cluster.c.node->on_election_timeout();
        if (!actions(candidate, "C did not start election")) return;
        expect(cluster.c.node->role() == dkv::RaftRole::Candidate &&
                   cluster.c.node->term() == 1,
               "C did not become a candidate in term one");
        const auto elected = cluster.elect_a();
        if (!elected) return;
        const auto append = message_to<dkv::AppendEntries>(*elected, 3);
        if (!append) return;
        const auto accepted = cluster.c.node->on_append_entries(1, *append);
        const auto* accepted_actions =
            actions(accepted, "C did not accept same-term leader");
        if (!accepted_actions) return;
        const auto reply = message_to<dkv::AppendEntriesReply>(*accepted_actions, 1);
        expect(reply && reply->success &&
                   cluster.c.node->role() == dkv::RaftRole::Follower &&
                   cluster.c.node->known_leader() == 1 &&
                   accepted_actions->reset_election_timer,
               "C did not step down for valid same-term AppendEntries");
    }

    void test_client_write_applies_only_after_majority_ack() {
        ElectedCluster cluster;
        const auto elected = cluster.elect_a();
        if (!elected) return;
        const auto first = message_to<dkv::AppendEntries>(*elected, 2);
        if (!first) return;
        const auto appended = cluster.b.node->on_append_entries(1, *first);
        const auto* appended_actions = actions(appended, "B did not append no-op");
        if (!appended_actions) return;
        const auto ack = message_to<dkv::AppendEntriesReply>(*appended_actions, 1);
        if (!ack) return;
        const auto committed = cluster.a.node->on_append_reply(2, *first, *ack);
        const auto* committed_actions = actions(committed, "A did not commit no-op");
        if (!committed_actions) return;
        const auto notify = message_to<dkv::AppendEntries>(*committed_actions, 2);
        if (!notify) return;
        const auto notified = cluster.b.node->on_append_entries(1, *notify);
        const auto* notified_actions = actions(notified, "B did not apply no-op");
        if (!notified_actions) return;
        const auto notify_ack =
            message_to<dkv::AppendEntriesReply>(*notified_actions, 1);
        if (!notify_ack) return;
        const auto recorded = cluster.a.node->on_append_reply(2, *notify, *notify_ack);
        if (!actions(recorded, "A did not record heartbeat acknowledgment")) return;

        const auto proposed = cluster.a.node->propose({dkv::CommandType::Put, "x", "10"});
        const auto* proposed_actions = actions(proposed, "A did not accept PUT");
        if (!proposed_actions) return;
        expect(proposed_actions->proposed_index == 2 &&
                   cluster.a.node->last_index() == 2 &&
                   cluster.a.node->commit_index() == 1 &&
                   !cluster.a.node->get("x"),
               "PUT changed leader state before a majority acknowledgment");
        const auto put_b = message_to<dkv::AppendEntries>(*proposed_actions, 2);
        if (!put_b) return;
        const auto stored = cluster.b.node->on_append_entries(1, *put_b);
        const auto* stored_actions = actions(stored, "B did not store PUT");
        if (!stored_actions) return;
        const auto put_ack = message_to<dkv::AppendEntriesReply>(*stored_actions, 1);
        if (!put_ack) return;
        expect(put_ack->success && cluster.b.node->last_index() == 2 &&
                   cluster.b.node->commit_index() == 1 && !cluster.b.node->get("x"),
               "follower applied PUT before learning it was committed");

        const auto put_committed = cluster.a.node->on_append_reply(2, *put_b, *put_ack);
        const auto* put_commit_actions =
            actions(put_committed, "A did not process PUT acknowledgment");
        if (!put_commit_actions) return;
        expect(cluster.a.node->commit_index() == 2 &&
                   cluster.a.node->get("x") == "10" &&
                   put_commit_actions->applied ==
                       std::vector<dkv::AppliedLogEntry>{{2, dkv::ApplyResult::Applied}},
               "leader did not apply PUT after majority acknowledgment");
        const auto put_notify = message_to<dkv::AppendEntries>(*put_commit_actions, 2);
        if (!put_notify) return;
        const auto follower_committed = cluster.b.node->on_append_entries(1, *put_notify);
        expect(actions(follower_committed, "B did not receive PUT commit") &&
                   cluster.b.node->commit_index() == 2 &&
                   cluster.b.node->get("x") == "10",
               "follower did not apply committed PUT");
    }

    void test_higher_term_reply_steps_down_and_persists_term() {
        ElectedCluster cluster;
        const auto elected = cluster.elect_a();
        if (!elected) return;
        const auto pending = message_to<dkv::AppendEntries>(*elected, 2);
        if (!pending) return;

        const auto stepped_down =
            cluster.a.node->on_append_reply(2, *pending, {2, false});
        const auto* stepped_down_actions =
            actions(stepped_down, "A did not process higher-term reply");
        if (!stepped_down_actions) return;
        expect(cluster.a.node->role() == dkv::RaftRole::Follower &&
                   cluster.a.node->term() == 2 &&
                   !cluster.a.node->known_leader() &&
                   stepped_down_actions->reset_election_timer,
               "A did not step down on a higher-term reply");
        const auto rejected =
            cluster.a.node->propose({dkv::CommandType::Put, "x", "10"});
        expect(std::get_if<dkv::RaftNodeError>(&rejected) &&
                   std::get<dkv::RaftNodeError>(rejected) ==
                       dkv::RaftNodeError::NotLeader,
               "former leader accepted a client command");

        cluster.a.node.reset();
        auto reopened = dkv::RaftNode::open(1, {2, 3}, cluster.a.dir.path());
        auto* node = std::get_if<std::unique_ptr<dkv::RaftNode>>(&reopened);
        expect(node && *node && (*node)->term() == 2 &&
                   (*node)->role() == dkv::RaftRole::Follower &&
                   (*node)->last_index() == 1,
               "higher term or durable no-op was lost on restart");
    }

    void test_higher_term_vote_request_resets_timer_even_when_denied() {
        ElectedCluster cluster;
        const auto elected = cluster.elect_a();
        if (!elected) return;
        const auto denied = cluster.a.node->on_request_vote(2, {2, 2, 0, 0});
        const auto* denied_actions =
            actions(denied, "A did not process higher-term vote request");
        if (!denied_actions) return;
        const auto reply = message_to<dkv::RequestVoteReply>(*denied_actions, 2);
        expect(reply && !reply->vote_granted && reply->term == 2 &&
                   cluster.a.node->role() == dkv::RaftRole::Follower &&
                   denied_actions->reset_election_timer,
               "higher-term rejection did not step down and reset election timer");
    }

    void test_read_barrier_waits_for_majority() {
        ElectedCluster cluster;
        const auto elected = cluster.elect_a();
        if (!elected) return;
        const auto follower_barrier = cluster.b.node->read_barrier();
        expect(std::get_if<dkv::RaftNodeError>(&follower_barrier) &&
                   std::get<dkv::RaftNodeError>(follower_barrier) ==
                       dkv::RaftNodeError::NotLeader,
               "follower accepted a read barrier");
        const auto first = message_to<dkv::AppendEntries>(*elected, 2);
        if (!first) return;
        const auto barrier = cluster.a.node->read_barrier();
        const auto* barrier_actions = actions(barrier, "A did not propose read barrier");
        if (!barrier_actions) return;
        expect(barrier_actions->proposed_index == 2 &&
                   cluster.a.node->last_index() == 2 &&
                   cluster.a.node->last_applied() == 0,
               "read barrier was applied without a majority");

        const auto first_on_b = cluster.b.node->on_append_entries(1, *first);
        const auto* first_actions = actions(first_on_b, "B did not store first no-op");
        if (!first_actions) return;
        const auto first_ack = message_to<dkv::AppendEntriesReply>(*first_actions, 1);
        if (!first_ack) return;
        const auto first_committed =
            cluster.a.node->on_append_reply(2, *first, *first_ack);
        const auto* committed_actions =
            actions(first_committed, "A did not commit leadership no-op");
        if (!committed_actions) return;
        const auto barrier_append =
            message_to<dkv::AppendEntries>(*committed_actions, 2);
        if (!barrier_append) return;
        expect(cluster.a.node->commit_index() == 1 &&
                   barrier_append->entries.size() == 1 &&
                   barrier_append->entries[0].index == 2 &&
                   barrier_append->entries[0].command.type ==
                       dkv::CommandType::NoOp,
               "read barrier was not replicated as a current-term no-op");

        const auto barrier_on_b = cluster.b.node->on_append_entries(1, *barrier_append);
        const auto* stored_actions = actions(barrier_on_b, "B did not store barrier");
        if (!stored_actions) return;
        const auto barrier_ack =
            message_to<dkv::AppendEntriesReply>(*stored_actions, 1);
        if (!barrier_ack) return;
        const auto applied =
            cluster.a.node->on_append_reply(2, *barrier_append, *barrier_ack);
        const auto* applied_actions =
            actions(applied, "A did not process barrier acknowledgment");
        expect(applied_actions && cluster.a.node->commit_index() == 2 &&
                   cluster.a.node->last_applied() == 2 &&
                   applied_actions->applied ==
                       std::vector<dkv::AppliedLogEntry>{{2, dkv::ApplyResult::Applied}},
               "read barrier completed before or after the wrong commit point");
    }
}

int main() {
    test_election_noop_commit_and_follower_notification();
    test_same_term_candidate_steps_down_for_leader();
    test_client_write_applies_only_after_majority_ack();
    test_higher_term_reply_steps_down_and_persists_term();
    test_higher_term_vote_request_resets_timer_even_when_denied();
    test_read_barrier_waits_for_majority();
    if (failures != 0) std::cerr << failures << " Raft node tests failed\n";
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}

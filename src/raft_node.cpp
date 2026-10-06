#include "dkv/raft_node.hpp"

#include "dkv/persistent_log.hpp"
#include "dkv/persistent_metadata.hpp"

#include <stdexcept>
#include <utility>

namespace dkv {
    RaftNode::RaftNode(
        std::uint64_t self_id,
        std::array<std::uint64_t, 2> peer_ids,
        std::unique_ptr<PersistentMetadata> metadata,
        std::unique_ptr<PersistentLog> log)
        : self_id_(self_id),
          peer_ids_(peer_ids),
          metadata_(std::move(metadata)),
          log_(std::move(log)),
          election_(self_id, peer_ids, *metadata_, *log_) {}

    RaftNode::~RaftNode() = default;

    RaftNodeOpenResult RaftNode::open(
        std::uint64_t self_id,
        std::array<std::uint64_t, 2> peer_ids,
        const std::filesystem::path& data_dir) {
        if (peer_ids[0] == peer_ids[1] || peer_ids[0] == self_id ||
            peer_ids[1] == self_id) {
            return RaftNodeOpenError::InvalidConfiguration;
        }
        auto metadata_result = PersistentMetadata::open(data_dir / "raft.meta");
        auto* metadata =
            std::get_if<std::unique_ptr<PersistentMetadata>>(&metadata_result);
        if (!metadata) return RaftNodeOpenError::MetadataError;

        auto log_result = PersistentLog::open(data_dir / "raft.log");
        auto* log = std::get_if<std::unique_ptr<PersistentLog>>(&log_result);
        if (!log) return RaftNodeOpenError::LogError;

        return std::unique_ptr<RaftNode>{new RaftNode(
            self_id, peer_ids, std::move(*metadata), std::move(*log))};
    }

    std::uint64_t RaftNode::id() const noexcept { return self_id_; }
    RaftRole RaftNode::role() const noexcept { return election_.role(); }
    std::uint64_t RaftNode::term() const noexcept { return metadata_->state().current_term; }
    std::uint64_t RaftNode::last_index() const noexcept { return log_->last_index(); }
    std::uint64_t RaftNode::commit_index() const noexcept { return commit_index_; }
    std::uint64_t RaftNode::last_applied() const noexcept { return state_.last_applied(); }
    std::optional<std::uint64_t> RaftNode::known_leader() const noexcept {
        return known_leader_;
    }
    std::array<std::uint64_t, 2> RaftNode::peer_ids() const noexcept {
        return peer_ids_;
    }
    std::optional<PeerProgress> RaftNode::peer_progress(
        std::uint64_t peer_id) const noexcept {
        return leader_ && leader_->active() ? leader_->progress(peer_id) : std::nullopt;
    }
    std::optional<std::string> RaftNode::get(const std::string& key) const {
        return state_.get(key);
    }
    const AppliedRequest* RaftNode::request_result(const RequestId& id) const {
        return state_.request_result(id);
    }

    bool RaftNode::is_peer(std::uint64_t peer_id) const noexcept {
        return peer_id == peer_ids_[0] || peer_id == peer_ids_[1];
    }

    void RaftNode::drop_leadership() {
        leader_.reset();
        read_rounds_.clear();
        known_leader_.reset();
    }

    RaftNodeResult RaftNode::stop(RaftNodeError error) {
        stopped_ = true;
        drop_leadership();
        return error;
    }

    bool RaftNode::queue_append(std::uint64_t peer_id, RaftNodeActions& actions) {
        if (!leader_) return false;
        // Keep socket frames bounded even when the log has a long backlog.
        auto result = leader_->make_request(peer_id, 4);
        const auto* request = std::get_if<AppendEntries>(&result);
        if (!request) return false;
        actions.messages.push_back({peer_id, *request});
        return true;
    }

    bool RaftNode::apply_ready(RaftNodeActions& actions) {
        auto result = apply_committed(*log_, state_, commit_index_);
        auto* applied = std::get_if<std::vector<AppliedLogEntry>>(&result);
        if (!applied) return false;
        actions.applied = std::move(*applied);
        return true;
    }

    bool RaftNode::current_term_committed() const {
        if (commit_index_ == 0 || state_.last_applied() < commit_index_) return false;
        const auto entry = log_->entry_at(commit_index_);
        return entry && entry->term == term();
    }

    void RaftNode::start_waiting_reads(RaftNodeActions& actions) {
        if (!current_term_committed()) return;
        for (auto& [id, probe] : read_rounds_) {
            if (probe) continue;
            probe = AppendEntries{term(), self_id_, 0, 0, {}, commit_index_, id};
            for (const auto peer_id : peer_ids_) {
                actions.messages.push_back({peer_id, *probe});
            }
        }
    }

    RaftNodeResult RaftNode::on_election_timeout() {
        if (stopped_) return RaftNodeError::Stopped;
        const auto result = election_.start_election();
        if (const auto* error = std::get_if<ElectionStartError>(&result)) {
            if (*error == ElectionStartError::AlreadyLeader) return RaftNodeActions{};
            return stop(*error == ElectionStartError::TermExhausted
                            ? RaftNodeError::TermExhausted
                            : RaftNodeError::PersistenceError);
        }

        drop_leadership();
        const auto& request = std::get<RequestVote>(result);
        RaftNodeActions actions;
        actions.reset_election_timer = true;
        for (const auto peer_id : peer_ids_) {
            actions.messages.push_back({peer_id, request});
        }
        return actions;
    }

    RaftNodeResult RaftNode::on_heartbeat_timeout() {
        if (stopped_) return RaftNodeError::Stopped;
        if (!leader_) return RaftNodeActions{};
        if (!leader_->active()) {
            drop_leadership();
            return RaftNodeActions{};
        }
        RaftNodeActions actions;
        actions.schedule_heartbeat = true;
        for (const auto peer_id : peer_ids_) {
            if (!queue_append(peer_id, actions)) {
                return stop(RaftNodeError::InconsistentState);
            }
        }
        for (const auto& [id, probe] : read_rounds_) {
            if (!probe) continue;
            for (const auto peer_id : peer_ids_) {
                actions.messages.push_back({peer_id, *probe});
            }
        }
        return actions;
    }

    RaftNodeResult RaftNode::on_request_vote(
        std::uint64_t peer_id, const RequestVote& request) {
        if (stopped_) return RaftNodeError::Stopped;
        if (!is_peer(peer_id)) return RaftNodeError::InvalidPeer;
        if (request.candidate_id != peer_id) return RaftNodeError::InvalidRequest;

        const auto old_term = term();
        const auto result = handle_request_vote(*metadata_, *log_, request);
        if (std::holds_alternative<RequestVoteHandleError>(result)) {
            return stop(RaftNodeError::PersistenceError);
        }
        if (term() > old_term) {
            drop_leadership();
        }
        const auto reply = std::get<RequestVoteReply>(result);
        RaftNodeActions actions;
        actions.reset_election_timer = term() > old_term || reply.vote_granted;
        actions.messages.push_back({peer_id, reply});
        return actions;
    }

    RaftNodeResult RaftNode::on_vote_reply(
        std::uint64_t peer_id, const RequestVoteReply& reply) {
        if (stopped_) return RaftNodeError::Stopped;
        if (!is_peer(peer_id)) return RaftNodeError::InvalidPeer;

        const auto outcome = election_.handle_vote_reply(peer_id, reply);
        RaftNodeActions actions;
        if (outcome == VoteReplyOutcome::PersistenceError) {
            return stop(RaftNodeError::PersistenceError);
        }
        if (outcome == VoteReplyOutcome::SteppedDown) {
            drop_leadership();
            actions.reset_election_timer = true;
            return actions;
        }
        if (outcome != VoteReplyOutcome::BecameLeader) return actions;

        known_leader_ = self_id_;
        try {
            leader_ = std::make_unique<RaftLeaderReplication>(
                election_, *metadata_, *log_, commit_index_);
        } catch (const std::invalid_argument&) {
            return stop(RaftNodeError::InconsistentState);
        }
        const auto appended = leader_->append_command({CommandType::NoOp, "", ""});
        if (!std::holds_alternative<std::uint64_t>(appended)) {
            return stop(RaftNodeError::PersistenceError);
        }
        actions.schedule_heartbeat = true;
        for (const auto id : peer_ids_) {
            if (!queue_append(id, actions)) return stop(RaftNodeError::InconsistentState);
        }
        return actions;
    }

    RaftNodeResult RaftNode::on_append_entries(
        std::uint64_t peer_id, const AppendEntries& request) {
        if (stopped_) return RaftNodeError::Stopped;
        if (!is_peer(peer_id)) return RaftNodeError::InvalidPeer;
        if (request.leader_id != peer_id) return RaftNodeError::InvalidRequest;

        const auto old_term = term();
        const auto result = handle_append_entries(
            *metadata_, *log_, commit_index_, request);
        if (term() > old_term) {
            drop_leadership();
        }
        if (const auto* error = std::get_if<AppendEntriesHandleError>(&result)) {
            if (*error == AppendEntriesHandleError::InvalidRequest) {
                return RaftNodeError::InvalidRequest;
            }
            return stop(*error == AppendEntriesHandleError::PersistenceError
                            ? RaftNodeError::PersistenceError
                            : RaftNodeError::InconsistentState);
        }

        const auto reply = std::get<AppendEntriesReply>(result);
        RaftNodeActions actions;
        if (request.term != 0 && request.term == reply.term) {
            election_.observe_leader(request.term);
            drop_leadership();
            known_leader_ = peer_id;
            actions.reset_election_timer = true;
        }
        if (reply.success && !apply_ready(actions)) {
            return stop(RaftNodeError::InconsistentState);
        }
        actions.messages.push_back({peer_id, reply});
        return actions;
    }

    RaftNodeResult RaftNode::on_append_reply(
        std::uint64_t peer_id,
        const AppendEntries& sent_request,
        const AppendEntriesReply& reply) {
        if (stopped_) return RaftNodeError::Stopped;
        if (!is_peer(peer_id)) return RaftNodeError::InvalidPeer;
        RaftNodeActions actions;
        if (!leader_) {
            if (reply.term > term()) {
                if (metadata_->advance_term(reply.term) !=
                    PersistentMetadataUpdateResult::Persisted) {
                    return stop(RaftNodeError::PersistenceError);
                }
                drop_leadership();
                actions.reset_election_timer = true;
            }
            return actions;
        }

        const auto outcome = leader_->handle_reply(peer_id, sent_request, reply);
        if (outcome == AppendReplyOutcome::PersistenceError) {
            return stop(RaftNodeError::PersistenceError);
        }
        if (outcome == AppendReplyOutcome::InconsistentFollower) {
            return stop(RaftNodeError::InconsistentState);
        }
        if (outcome == AppendReplyOutcome::SteppedDown) {
            drop_leadership();
            actions.reset_election_timer = true;
            return actions;
        }
        if (outcome == AppendReplyOutcome::Retry) {
            if (!queue_append(peer_id, actions)) return stop(RaftNodeError::InconsistentState);
        } else if (outcome == AppendReplyOutcome::Committed) {
            if (!apply_ready(actions)) return stop(RaftNodeError::InconsistentState);
            for (const auto id : peer_ids_) {
                if (!queue_append(id, actions)) return stop(RaftNodeError::InconsistentState);
            }
            start_waiting_reads(actions);
        } else if (outcome == AppendReplyOutcome::Recorded) {
            const auto progress = leader_->progress(peer_id);
            if (!progress) return stop(RaftNodeError::InconsistentState);
            if (progress->next_index <= log_->last_index() &&
                !queue_append(peer_id, actions)) {
                return stop(RaftNodeError::InconsistentState);
            }
        }
        if (sent_request.read_context != 0 && reply.term == term() && reply.success) {
            const auto pending = read_rounds_.find(sent_request.read_context);
            if (pending != read_rounds_.end() && pending->second &&
                *pending->second == sent_request) {
                const auto index = pending->second->leader_commit;
                if (state_.last_applied() < index) {
                    return stop(RaftNodeError::InconsistentState);
                }
                actions.ready_reads.push_back({pending->first, index});
                read_rounds_.erase(pending);
            }
        }
        return actions;
    }

    RaftNodeResult RaftNode::propose(const Command& command) {
        if (stopped_) return RaftNodeError::Stopped;
        if (!leader_ || !leader_->active()) return RaftNodeError::NotLeader;
        if (command.type == CommandType::NoOp) return RaftNodeError::InvalidCommand;
        return append_as_leader(command);
    }

    RaftNodeResult RaftNode::read_barrier() {
        if (stopped_) return RaftNodeError::Stopped;
        if (!leader_ || !leader_->active()) return RaftNodeError::NotLeader;
        if (next_read_id_ == 0) return stop(RaftNodeError::InconsistentState);
        const auto id = next_read_id_++;
        read_rounds_.emplace(id, std::nullopt);
        RaftNodeActions actions;
        actions.started_read_id = id;
        start_waiting_reads(actions);
        return actions;
    }

    RaftNodeResult RaftNode::append_as_leader(const Command& command) {
        if (stopped_) return RaftNodeError::Stopped;
        if (!leader_ || !leader_->active()) return RaftNodeError::NotLeader;
        const auto result = leader_->append_command(command);
        if (const auto* error = std::get_if<LeaderReplicationError>(&result)) {
            if (*error == LeaderReplicationError::InvalidCommand) {
                return RaftNodeError::InvalidCommand;
            }
            return stop(*error == LeaderReplicationError::PersistenceError
                            ? RaftNodeError::PersistenceError
                            : RaftNodeError::InconsistentState);
        }
        RaftNodeActions actions;
        actions.proposed_index = std::get<std::uint64_t>(result);
        for (const auto id : peer_ids_) {
            if (!queue_append(id, actions)) return stop(RaftNodeError::InconsistentState);
        }
        return actions;
    }
}

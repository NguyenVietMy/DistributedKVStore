#include "dkv/raft_leader.hpp"

#include "dkv/persistent_log.hpp"
#include "dkv/persistent_metadata.hpp"
#include "dkv/raft_election.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace dkv {
    RaftLeaderReplication::RaftLeaderReplication(
        RaftElection& election,
        PersistentMetadata& metadata,
        PersistentLog& log,
        std::uint64_t& commit_index)
        : self_id_(election.self_id()),
          term_(metadata.state().current_term),
          election_(election),
          metadata_(metadata),
          log_(log),
          commit_index_(commit_index),
          peers_{{PeerState{election.peer_ids()[0], {log.last_index() + 1, 0}, std::nullopt},
                  PeerState{election.peer_ids()[1], {log.last_index() + 1, 0}, std::nullopt}}} {
        if (term_ == 0 ||
            log.last_index() >= std::numeric_limits<std::uint64_t>::max() - 1 ||
            commit_index > log.last_index() || election.role() != RaftRole::Leader ||
            metadata.state().voted_for != self_id_) {
            throw std::invalid_argument("invalid leader replication state");
        }
    }

    bool RaftLeaderReplication::active() const noexcept {
        return !stopped_ && election_.role() == RaftRole::Leader &&
               metadata_.state().current_term == term_ &&
               metadata_.state().voted_for == self_id_;
    }

    RaftLeaderReplication::PeerState* RaftLeaderReplication::find_peer(
        std::uint64_t peer_id) noexcept {
        for (auto& peer : peers_) {
            if (peer.id == peer_id) return &peer;
        }
        return nullptr;
    }

    const RaftLeaderReplication::PeerState* RaftLeaderReplication::find_peer(
        std::uint64_t peer_id) const noexcept {
        for (const auto& peer : peers_) {
            if (peer.id == peer_id) return &peer;
        }
        return nullptr;
    }

    std::optional<PeerProgress> RaftLeaderReplication::progress(
        std::uint64_t peer_id) const noexcept {
        const auto* peer = find_peer(peer_id);
        if (!peer) return std::nullopt;
        return peer->progress;
    }

    MakeAppendEntriesResult RaftLeaderReplication::make_request(
        std::uint64_t peer_id, std::size_t max_entries) {
        auto* peer = find_peer(peer_id);
        if (!peer) return LeaderReplicationError::UnknownPeer;
        if (!active()) return LeaderReplicationError::NotLeader;
        if (max_entries == 0) return LeaderReplicationError::InvalidBatchSize;
        if (commit_index_ > log_.last_index()) {
            return LeaderReplicationError::InconsistentState;
        }
        if (peer->pending) return *peer->pending;

        const auto next = peer->progress.next_index;
        if (next == 0 || next > log_.last_index() + 1) {
            return LeaderReplicationError::InconsistentState;
        }
        const auto previous_index = next - 1;
        std::uint64_t previous_term = 0;
        if (previous_index != 0) {
            const auto previous = log_.entry_at(previous_index);
            if (!previous) return LeaderReplicationError::InconsistentState;
            previous_term = previous->term;
        }

        AppendEntries request{term_, self_id_, previous_index, previous_term, {}, commit_index_};
        auto index = next;
        while (index <= log_.last_index() && request.entries.size() < max_entries) {
            const auto entry = log_.entry_at(index);
            if (!entry) return LeaderReplicationError::InconsistentState;
            request.entries.push_back(*entry);
            if (index == log_.last_index()) break;
            ++index;
        }
        peer->pending = request;
        return request;
    }

    LeaderAppendResult RaftLeaderReplication::append_command(const Command& command) {
        if (!active()) return LeaderReplicationError::NotLeader;
        if (log_.last_index() >= std::numeric_limits<std::uint64_t>::max() - 1) {
            return LeaderReplicationError::IndexExhausted;
        }
        const auto index = log_.last_index() + 1;
        const auto result = log_.append(LogEntry{index, term_, command});
        if (result == PersistentLogAppendResult::Appended) return index;
        if (result == PersistentLogAppendResult::InvalidEntry) {
            return LeaderReplicationError::InvalidCommand;
        }
        if (result == PersistentLogAppendResult::IoError) {
            stopped_ = true;
            return LeaderReplicationError::PersistenceError;
        }
        return LeaderReplicationError::InconsistentState;
    }

    bool RaftLeaderReplication::advance_commit_index() {
        for (auto index = log_.last_index(); index > commit_index_; --index) {
            const auto entry = log_.entry_at(index);
            if (!entry || entry->term != term_) continue;
            if (peers_[0].progress.match_index >= index ||
                peers_[1].progress.match_index >= index) {
                commit_index_ = index;
                return true;
            }
        }
        return false;
    }

    AppendReplyOutcome RaftLeaderReplication::handle_reply(
        std::uint64_t peer_id,
        const AppendEntries& sent_request,
        const AppendEntriesReply& reply) {
        auto* peer = find_peer(peer_id);
        if (!peer || !active()) return AppendReplyOutcome::Ignored;

        if (reply.term > term_) {
            stopped_ = true;
            if (metadata_.advance_term(reply.term) != PersistentMetadataUpdateResult::Persisted) {
                return AppendReplyOutcome::PersistenceError;
            }
            return AppendReplyOutcome::SteppedDown;
        }
        if (reply.term != term_ || !peer->pending || *peer->pending != sent_request) {
            return AppendReplyOutcome::Ignored;
        }

        peer->pending.reset();
        if (!reply.success) {
            const auto lowest_next = peer->progress.match_index + 1;
            if (peer->progress.next_index <= lowest_next) {
                return AppendReplyOutcome::InconsistentFollower;
            }
            --peer->progress.next_index;
            return AppendReplyOutcome::Retry;
        }

        const auto matched_index = sent_request.prev_log_index + sent_request.entries.size();
        peer->progress.match_index = std::max(peer->progress.match_index, matched_index);
        peer->progress.next_index = peer->progress.match_index + 1;
        if (advance_commit_index()) return AppendReplyOutcome::Committed;
        return AppendReplyOutcome::Recorded;
    }
}

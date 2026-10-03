#include "dkv/raft_election.hpp"

#include "dkv/persistent_log.hpp"
#include "dkv/persistent_metadata.hpp"

#include <cstddef>
#include <limits>
#include <stdexcept>

namespace dkv {
    RaftElection::RaftElection(std::uint64_t self_id,
                               std::array<std::uint64_t, 2> peer_ids,
                               PersistentMetadata& metadata,
                               const PersistentLog& log)
        : self_id_(self_id), peer_ids_(peer_ids), metadata_(metadata), log_(log) {
        if (peer_ids_[0] == peer_ids_[1] ||
            peer_ids_[0] == self_id_ || peer_ids_[1] == self_id_) {
            throw std::invalid_argument("Raft node IDs must be distinct");
        }
    }

    RaftRole RaftElection::role() const noexcept {
        if (role_ != RaftRole::Follower &&
            (metadata_.state().current_term != election_term_ ||
             metadata_.state().voted_for != self_id_)) {
            return RaftRole::Follower;
        }
        return role_;
    }

    unsigned RaftElection::granted_votes() const noexcept {
        return role() == RaftRole::Follower ? 0 : granted_votes_;
    }

    std::uint64_t RaftElection::self_id() const noexcept { return self_id_; }

    std::array<std::uint64_t, 2> RaftElection::peer_ids() const noexcept {
        return peer_ids_;
    }

    void RaftElection::observe_leader(std::uint64_t term) noexcept {
        if (term != 0 && term == metadata_.state().current_term) {
            become_follower();
        }
    }

    void RaftElection::become_follower() noexcept {
        role_ = RaftRole::Follower;
        election_term_ = 0;
        replied_ = {};
        granted_votes_ = 0;
    }

    bool RaftElection::refresh_role_from_metadata() noexcept {
        if (role_ != RaftRole::Follower && role() == RaftRole::Follower) {
            become_follower();
            return true;
        }
        return false;
    }

    ElectionStartResult RaftElection::start_election() {
        if (unusable_) return ElectionStartError::PersistenceError;
        refresh_role_from_metadata();
        if (role_ == RaftRole::Leader) return ElectionStartError::AlreadyLeader;

        const auto current_term = metadata_.state().current_term;
        if (current_term == std::numeric_limits<std::uint64_t>::max()) {
            return ElectionStartError::TermExhausted;
        }
        const auto next_term = current_term + 1;
        if (metadata_.advance_term(next_term) != PersistentMetadataUpdateResult::Persisted) {
            become_follower();
            unusable_ = true;
            return ElectionStartError::PersistenceError;
        }
        const auto vote_result = metadata_.vote_for(self_id_);
        if (vote_result != PersistentMetadataUpdateResult::Persisted &&
            vote_result != PersistentMetadataUpdateResult::Unchanged) {
            become_follower();
            unusable_ = true;
            return ElectionStartError::PersistenceError;
        }

        role_ = RaftRole::Candidate;
        election_term_ = next_term;
        replied_ = {};
        granted_votes_ = 1;
        return RequestVote{next_term, self_id_, log_.last_index(), log_.last_term()};
    }

    VoteReplyOutcome RaftElection::handle_vote_reply(
        std::uint64_t peer_id, const RequestVoteReply& reply) {
        if (unusable_) return VoteReplyOutcome::PersistenceError;
        if (refresh_role_from_metadata()) return VoteReplyOutcome::SteppedDown;

        std::size_t peer_index = peer_ids_.size();
        for (std::size_t i = 0; i < peer_ids_.size(); ++i) {
            if (peer_ids_[i] == peer_id) peer_index = i;
        }
        if (peer_index == peer_ids_.size()) return VoteReplyOutcome::Ignored;

        if (reply.term > metadata_.state().current_term) {
            become_follower();
            if (metadata_.advance_term(reply.term) != PersistentMetadataUpdateResult::Persisted) {
                unusable_ = true;
                return VoteReplyOutcome::PersistenceError;
            }
            return VoteReplyOutcome::SteppedDown;
        }
        if (role_ != RaftRole::Candidate || reply.term != election_term_ ||
            replied_[peer_index]) {
            return VoteReplyOutcome::Ignored;
        }

        replied_[peer_index] = true;
        if (!reply.vote_granted) return VoteReplyOutcome::Recorded;

        ++granted_votes_;
        if (granted_votes_ >= 2) {
            role_ = RaftRole::Leader;
            return VoteReplyOutcome::BecameLeader;
        }
        return VoteReplyOutcome::Recorded;
    }
}

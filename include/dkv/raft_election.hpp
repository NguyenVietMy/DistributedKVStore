#pragma once

#include "dkv/raft_vote.hpp"

#include <array>
#include <cstdint>
#include <variant>

namespace dkv {
    class PersistentLog;
    class PersistentMetadata;

    enum class RaftRole {
        Follower,
        Candidate,
        Leader
    };

    enum class ElectionStartError {
        AlreadyLeader,
        TermExhausted,
        PersistenceError
    };

    using ElectionStartResult = std::variant<RequestVote, ElectionStartError>;

    enum class VoteReplyOutcome {
        Ignored,
        Recorded,
        BecameLeader,
        SteppedDown,
        PersistenceError
    };

    // Candidate-side election state for a fixed three-node cluster.
    // The caller schedules timeouts and sends the returned RequestVote to both peers.
    class RaftElection {
    public:
        RaftElection(std::uint64_t self_id,
                     std::array<std::uint64_t, 2> peer_ids,
                     PersistentMetadata& metadata,
                     const PersistentLog& log);

        [[nodiscard]] ElectionStartResult start_election();
        [[nodiscard]] VoteReplyOutcome handle_vote_reply(
            std::uint64_t peer_id, const RequestVoteReply& reply);
        [[nodiscard]] RaftRole role() const noexcept;
        [[nodiscard]] unsigned granted_votes() const noexcept;

        RaftElection(const RaftElection&) = delete;
        RaftElection& operator=(const RaftElection&) = delete;
        RaftElection(RaftElection&&) = delete;
        RaftElection& operator=(RaftElection&&) = delete;

    private:
        void become_follower() noexcept;
        bool refresh_role_from_metadata() noexcept;

        std::uint64_t self_id_;
        std::array<std::uint64_t, 2> peer_ids_;
        PersistentMetadata& metadata_;
        const PersistentLog& log_;
        RaftRole role_{RaftRole::Follower};
        std::uint64_t election_term_{0};
        std::array<bool, 2> replied_{};
        unsigned granted_votes_{0};
        bool unusable_{false};
    };
}

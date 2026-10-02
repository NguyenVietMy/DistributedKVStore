#pragma once

#include "dkv/command.hpp"
#include "dkv/raft_append.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <variant>

namespace dkv {
    class PersistentLog;
    class PersistentMetadata;
    class RaftElection;

    struct PeerProgress {
        std::uint64_t next_index{1};
        std::uint64_t match_index{0};

        bool operator==(const PeerProgress&) const = default;
    };

    enum class LeaderReplicationError {
        NotLeader,
        UnknownPeer,
        InvalidBatchSize,
        InvalidCommand,
        IndexExhausted,
        PersistenceError,
        InconsistentState
    };

    using MakeAppendEntriesResult = std::variant<AppendEntries, LeaderReplicationError>;
    using LeaderAppendResult = std::variant<std::uint64_t, LeaderReplicationError>;

    enum class AppendReplyOutcome {
        Ignored,
        Retry,
        Recorded,
        Committed,
        SteppedDown,
        InconsistentFollower,
        PersistenceError
    };

    // Construct after RaftElection reports BecameLeader. The caller appends a
    // current-term no-op. At most one request is outstanding per follower;
    // make_request retries it unchanged. Pass the original request with each
    // reply. All methods run on the single Raft owner thread.
    class RaftLeaderReplication {
    public:
        RaftLeaderReplication(RaftElection& election,
                              PersistentMetadata& metadata,
                              PersistentLog& log,
                              std::uint64_t& commit_index);

        [[nodiscard]] MakeAppendEntriesResult make_request(
            std::uint64_t peer_id, std::size_t max_entries = 64);
        [[nodiscard]] LeaderAppendResult append_command(const Command& command);
        [[nodiscard]] AppendReplyOutcome handle_reply(
            std::uint64_t peer_id,
            const AppendEntries& sent_request,
            const AppendEntriesReply& reply);
        [[nodiscard]] std::optional<PeerProgress> progress(std::uint64_t peer_id) const noexcept;
        [[nodiscard]] bool active() const noexcept;

        RaftLeaderReplication(const RaftLeaderReplication&) = delete;
        RaftLeaderReplication& operator=(const RaftLeaderReplication&) = delete;
        RaftLeaderReplication(RaftLeaderReplication&&) = delete;
        RaftLeaderReplication& operator=(RaftLeaderReplication&&) = delete;

    private:
        struct PeerState {
            std::uint64_t id{0};
            PeerProgress progress;
            std::optional<AppendEntries> pending;
        };

        [[nodiscard]] PeerState* find_peer(std::uint64_t peer_id) noexcept;
        [[nodiscard]] const PeerState* find_peer(std::uint64_t peer_id) const noexcept;
        [[nodiscard]] bool advance_commit_index();

        std::uint64_t self_id_;
        std::uint64_t term_;
        RaftElection& election_;
        PersistentMetadata& metadata_;
        PersistentLog& log_;
        std::uint64_t& commit_index_;
        std::array<PeerState, 2> peers_;
        bool stopped_{false};
    };
}

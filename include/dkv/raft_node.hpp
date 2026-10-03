#pragma once

#include "dkv/raft_append.hpp"
#include "dkv/raft_apply.hpp"
#include "dkv/raft_election.hpp"
#include "dkv/raft_leader.hpp"

#include <array>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace dkv {
    class PersistentLog;
    class PersistentMetadata;

    using RaftMessage =
        std::variant<RequestVote, RequestVoteReply, AppendEntries, AppendEntriesReply>;

    struct OutboundRaftMessage {
        std::uint64_t peer_id{0};
        RaftMessage message;

        bool operator==(const OutboundRaftMessage&) const = default;
    };

    struct RaftNodeActions {
        std::vector<OutboundRaftMessage> messages;
        std::vector<AppliedLogEntry> applied;
        std::optional<std::uint64_t> proposed_index;
        bool reset_election_timer{false};
        bool schedule_heartbeat{false};
    };

    enum class RaftNodeError {
        InvalidPeer,
        InvalidRequest,
        NotLeader,
        InvalidCommand,
        TermExhausted,
        PersistenceError,
        InconsistentState,
        Stopped
    };

    using RaftNodeResult = std::variant<RaftNodeActions, RaftNodeError>;

    enum class RaftNodeOpenError {
        InvalidConfiguration,
        MetadataError,
        LogError
    };

    class RaftNode;
    using RaftNodeOpenResult =
        std::variant<std::unique_ptr<RaftNode>, RaftNodeOpenError>;

    // Processes one event at a time. The caller delivers messages and timer
    // events, then performs the returned send and timer actions.
    class RaftNode {
    public:
        ~RaftNode();

        [[nodiscard]] static RaftNodeOpenResult open(
            std::uint64_t self_id,
            std::array<std::uint64_t, 2> peer_ids,
            const std::filesystem::path& data_dir);

        [[nodiscard]] RaftNodeResult on_election_timeout();
        [[nodiscard]] RaftNodeResult on_heartbeat_timeout();
        [[nodiscard]] RaftNodeResult on_request_vote(
            std::uint64_t peer_id, const RequestVote& request);
        [[nodiscard]] RaftNodeResult on_vote_reply(
            std::uint64_t peer_id, const RequestVoteReply& reply);
        [[nodiscard]] RaftNodeResult on_append_entries(
            std::uint64_t peer_id, const AppendEntries& request);
        [[nodiscard]] RaftNodeResult on_append_reply(
            std::uint64_t peer_id,
            const AppendEntries& sent_request,
            const AppendEntriesReply& reply);
        // Proposes a PUT or DELETE. Completion belongs to the caller after
        // the returned index appears in applied; this call is not an ACK.
        [[nodiscard]] RaftNodeResult propose(const Command& command);
        // Appends a current-term no-op. A read may use the local state only
        // after this index appears in applied while leadership is unchanged.
        [[nodiscard]] RaftNodeResult read_barrier();

        [[nodiscard]] std::uint64_t id() const noexcept;
        [[nodiscard]] RaftRole role() const noexcept;
        [[nodiscard]] std::uint64_t term() const noexcept;
        [[nodiscard]] std::uint64_t last_index() const noexcept;
        [[nodiscard]] std::uint64_t commit_index() const noexcept;
        [[nodiscard]] std::uint64_t last_applied() const noexcept;
        [[nodiscard]] std::optional<std::uint64_t> known_leader() const noexcept;
        // Local applied state inspection; this is not a linearizable client read.
        [[nodiscard]] std::optional<std::string> get(const std::string& key) const;
        // Results are reconstructed by applying committed log entries after restart.
        [[nodiscard]] const AppliedRequest* request_result(const RequestId& id) const;

        RaftNode(const RaftNode&) = delete;
        RaftNode& operator=(const RaftNode&) = delete;
        RaftNode(RaftNode&&) = delete;
        RaftNode& operator=(RaftNode&&) = delete;

    private:
        RaftNode(std::uint64_t self_id,
                 std::array<std::uint64_t, 2> peer_ids,
                 std::unique_ptr<PersistentMetadata> metadata,
                 std::unique_ptr<PersistentLog> log);

        [[nodiscard]] bool is_peer(std::uint64_t peer_id) const noexcept;
        [[nodiscard]] bool queue_append(std::uint64_t peer_id, RaftNodeActions& actions);
        [[nodiscard]] bool apply_ready(RaftNodeActions& actions);
        [[nodiscard]] RaftNodeResult append_as_leader(const Command& command);
        [[nodiscard]] RaftNodeResult stop(RaftNodeError error);

        std::uint64_t self_id_;
        std::array<std::uint64_t, 2> peer_ids_;
        std::unique_ptr<PersistentMetadata> metadata_;
        std::unique_ptr<PersistentLog> log_;
        std::uint64_t commit_index_{0};
        KvStateMachine state_;
        RaftElection election_;
        std::unique_ptr<RaftLeaderReplication> leader_;
        std::optional<std::uint64_t> known_leader_;
        bool stopped_{false};
    };
}

#pragma once

#include <cstdint>
#include <variant>

namespace dkv {
    class PersistentLog;
    class PersistentMetadata;

    struct RequestVote {
        std::uint64_t term{0};
        std::uint64_t candidate_id{0};
        std::uint64_t last_log_index{0};
        std::uint64_t last_log_term{0};

        bool operator==(const RequestVote&) const = default;
    };

    struct RequestVoteReply {
        std::uint64_t term{0};
        bool vote_granted{false};

        bool operator==(const RequestVoteReply&) const = default;
    };

    enum class RequestVoteHandleError {
        PersistenceError
    };

    using RequestVoteHandleResult = std::variant<RequestVoteReply, RequestVoteHandleError>;

    // Returns a reply only after any required term/vote update is durable.
    // The caller handles role changes, membership checks, timers, and sending the reply.
    [[nodiscard]] RequestVoteHandleResult handle_request_vote(
        PersistentMetadata& metadata,
        const PersistentLog& log,
        const RequestVote& request);
}

#pragma once

#include "dkv/log_entry.hpp"

#include <cstdint>
#include <variant>
#include <vector>

namespace dkv {
    class PersistentLog;
    class PersistentMetadata;

    struct AppendEntries {
        std::uint64_t term{0};
        std::uint64_t leader_id{0};
        std::uint64_t prev_log_index{0};
        std::uint64_t prev_log_term{0};
        std::vector<LogEntry> entries;
        std::uint64_t leader_commit{0};
        // Nonzero only for a read-only heartbeat round. Echoed in replied_to.
        std::uint64_t read_context{0};

        bool operator==(const AppendEntries&) const = default;
    };

    struct AppendEntriesReply {
        std::uint64_t term{0};
        bool success{false};

        bool operator==(const AppendEntriesReply&) const = default;
    };

    enum class AppendEntriesHandleError {
        InvalidRequest,
        CommittedConflict,
        PersistenceError
    };

    using AppendEntriesHandleResult =
        std::variant<AppendEntriesReply, AppendEntriesHandleError>;

    // On success, commit_index advances only through entries proven to match the leader.
    // The caller owns role changes, membership checks, election timers, and application.
    [[nodiscard]] AppendEntriesHandleResult handle_append_entries(
        PersistentMetadata& metadata,
        PersistentLog& log,
        std::uint64_t& commit_index,
        const AppendEntries& request);
}

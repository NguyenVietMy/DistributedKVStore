#pragma once

#include "dkv/kv_state_machine.hpp"

#include <cstdint>
#include <variant>
#include <vector>

namespace dkv {
    class PersistentLog;

    struct AppliedLogEntry {
        std::uint64_t index{0};
        ApplyResult result{ApplyResult::Applied};

        bool operator==(const AppliedLogEntry&) const = default;
    };

    enum class ApplyCommittedError {
        CommitBeyondLog,
        StateAheadOfCommit,
        MissingEntry,
        InvalidCommand,
        UnexpectedIndex
    };

    using ApplyCommittedResult =
        std::variant<std::vector<AppliedLogEntry>, ApplyCommittedError>;

    // Applies only the committed prefix, starting after state.last_applied().
    // KeyNotFound is a valid result for DELETE and still advances last_applied.
    [[nodiscard]] ApplyCommittedResult apply_committed(
        const PersistentLog& log,
        KvStateMachine& state,
        std::uint64_t commit_index);
}

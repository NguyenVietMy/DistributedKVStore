#include "dkv/raft_apply.hpp"

#include "dkv/persistent_log.hpp"

#include <vector>

namespace dkv {
    ApplyCommittedResult apply_committed(
        const PersistentLog& log,
        KvStateMachine& state,
        std::uint64_t commit_index) {
        if (commit_index > log.last_index()) {
            return ApplyCommittedError::CommitBeyondLog;
        }
        if (state.last_applied() > commit_index) {
            return ApplyCommittedError::StateAheadOfCommit;
        }

        std::vector<AppliedLogEntry> applied;
        applied.reserve(static_cast<std::size_t>(commit_index - state.last_applied()));
        while (state.last_applied() < commit_index) {
            const auto index = state.last_applied() + 1;
            const auto entry = log.entry_at(index);
            if (!entry) return ApplyCommittedError::MissingEntry;
            const auto result = state.apply(index, entry->command, entry->term);
            if (result == ApplyResult::InvalidCommand) {
                return ApplyCommittedError::InvalidCommand;
            }
            if (result == ApplyResult::UnexpectedIndex) {
                return ApplyCommittedError::UnexpectedIndex;
            }
            applied.push_back(AppliedLogEntry{index, result});
        }
        return applied;
    }
}

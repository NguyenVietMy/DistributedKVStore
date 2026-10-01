#include "dkv/raft_append.hpp"

#include "dkv/log_entry_codec.hpp"
#include "dkv/persistent_log.hpp"
#include "dkv/persistent_metadata.hpp"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <variant>

namespace dkv {
    namespace {
        bool valid_entries(const AppendEntries& request) {
            if ((request.prev_log_index == 0) != (request.prev_log_term == 0) ||
                request.prev_log_term > request.term ||
                request.entries.size() >
                    std::numeric_limits<std::uint64_t>::max() - request.prev_log_index) {
                return false;
            }

            auto next_index = request.prev_log_index;
            auto previous_term = request.prev_log_term;
            for (const auto& entry : request.entries) {
                ++next_index;
                if (entry.index != next_index || entry.term < previous_term ||
                    entry.term > request.term ||
                    std::holds_alternative<LogEntryCodecError>(encode_log_entry(entry))) {
                    return false;
                }
                previous_term = entry.term;
            }
            return true;
        }
    }

    AppendEntriesHandleResult handle_append_entries(
        PersistentMetadata& metadata,
        PersistentLog& log,
        std::uint64_t& commit_index,
        const AppendEntries& request) {
        const auto term_result = metadata.advance_term(request.term);
        if (term_result == PersistentMetadataUpdateResult::IoError) {
            return AppendEntriesHandleError::PersistenceError;
        }

        const auto current_term = metadata.state().current_term;
        if (request.term == 0 || request.term < current_term) {
            return AppendEntriesReply{current_term, false};
        }
        if (!valid_entries(request) || commit_index > log.last_index()) {
            return AppendEntriesHandleError::InvalidRequest;
        }

        if (request.prev_log_index != 0) {
            const auto previous = log.entry_at(request.prev_log_index);
            if (!previous || previous->term != request.prev_log_term) {
                return AppendEntriesReply{current_term, false};
            }
        }

        for (const auto& entry : request.entries) {
            const auto existing = log.entry_at(entry.index);
            if (existing && existing->term == entry.term) {
                // Equal index and term imply the same command in a valid Raft history.
                if (existing->command != entry.command) {
                    return AppendEntriesHandleError::InvalidRequest;
                }
                continue;
            }
            if (existing) {
                if (entry.index <= commit_index) {
                    return AppendEntriesHandleError::CommittedConflict;
                }
                if (log.truncate_suffix(entry.index) != PersistentLogTruncateResult::Truncated) {
                    return AppendEntriesHandleError::PersistenceError;
                }
            }
            if (log.append(entry) != PersistentLogAppendResult::Appended) {
                return AppendEntriesHandleError::PersistenceError;
            }
        }

        const auto matched_index = request.prev_log_index + request.entries.size();
        commit_index = std::max(commit_index, std::min(request.leader_commit, matched_index));
        return AppendEntriesReply{current_term, true};
    }
}

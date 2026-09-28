#include "dkv/raft_vote.hpp"

#include "dkv/persistent_log.hpp"
#include "dkv/persistent_metadata.hpp"

namespace dkv {
    namespace {
        bool candidate_log_is_up_to_date(const RequestVote& request, const PersistentLog& log) {
            if (request.last_log_term != log.last_term()) {
                return request.last_log_term > log.last_term();
            }
            return request.last_log_index >= log.last_index();
        }
    }

    RequestVoteHandleResult handle_request_vote(
        PersistentMetadata& metadata,
        const PersistentLog& log,
        const RequestVote& request) {
        // advance_term also reports IoError if a previous replacement left the store uncertain.
        const auto term_result = metadata.advance_term(request.term);
        if (term_result == PersistentMetadataUpdateResult::IoError) {
            return RequestVoteHandleError::PersistenceError;
        }

        const auto& state = metadata.state();
        if (request.term == 0 || request.term < state.current_term) {
            return RequestVoteReply{state.current_term, false};
        }
        if ((state.voted_for && *state.voted_for != request.candidate_id) ||
            !candidate_log_is_up_to_date(request, log)) {
            return RequestVoteReply{state.current_term, false};
        }

        const auto result = metadata.vote_for(request.candidate_id);
        if (result == PersistentMetadataUpdateResult::Persisted ||
            result == PersistentMetadataUpdateResult::Unchanged) {
            return RequestVoteReply{metadata.state().current_term, true};
        }
        if (result == PersistentMetadataUpdateResult::VoteConflict) {
            return RequestVoteReply{metadata.state().current_term, false};
        }
        return RequestVoteHandleError::PersistenceError;
    }
}

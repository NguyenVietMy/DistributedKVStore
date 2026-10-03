#include "dkv/raft_apply.hpp"

#include "dkv/persistent_log.hpp"

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace {
    int failures = 0;

    void expect(bool condition, std::string_view message) {
        if (!condition) {
            std::cerr << "FAIL: " << message << '\n';
            ++failures;
        }
    }

    class TempDirectory {
    public:
        TempDirectory() {
            const auto name = (std::filesystem::temp_directory_path() / "dkv-apply-XXXXXX").string();
            std::vector<char> path(name.begin(), name.end());
            path.push_back('\0');
            if (char* created = ::mkdtemp(path.data())) path_ = created;
        }
        ~TempDirectory() {
            if (!path_.empty()) std::filesystem::remove_all(path_);
        }
        const std::filesystem::path& path() const { return path_; }
    private:
        std::filesystem::path path_;
    };

    void test_committed_prefix_and_replay() {
        TempDirectory dir;
        if (dir.path().empty()) {
            expect(false, "could not create temporary directory");
            return;
        }
        auto opened = dkv::PersistentLog::open(dir.path() / "raft.log");
        auto* log_pointer = std::get_if<std::unique_ptr<dkv::PersistentLog>>(&opened);
        expect(log_pointer && *log_pointer, "could not open test log");
        if (!log_pointer || !*log_pointer) return;
        auto& log = **log_pointer;

        expect(log.append({1, 1, {dkv::CommandType::Put, "x", "10"}}) ==
                   dkv::PersistentLogAppendResult::Appended,
               "could not append first command");
        expect(log.append({2, 1, {dkv::CommandType::Delete, "missing", ""}}) ==
                   dkv::PersistentLogAppendResult::Appended,
               "could not append missing-key delete");
        expect(log.append({3, 2, {dkv::CommandType::Put, "x", "20"}}) ==
                   dkv::PersistentLogAppendResult::Appended,
               "could not append uncommitted update");
        expect(log.append({4, 2, {dkv::CommandType::NoOp, "", ""}}) ==
                   dkv::PersistentLogAppendResult::Appended,
               "could not append no-op");

        dkv::KvStateMachine state;
        expect(dkv::apply_committed(log, state, 0) ==
                   dkv::ApplyCommittedResult{std::vector<dkv::AppliedLogEntry>{}} &&
               state.last_applied() == 0 && !state.get("x"),
               "uncommitted log was applied");
        expect(dkv::apply_committed(log, state, 2) ==
                   dkv::ApplyCommittedResult{std::vector<dkv::AppliedLogEntry>{
                       {1, dkv::ApplyResult::Applied},
                       {2, dkv::ApplyResult::KeyNotFound}}} &&
               state.last_applied() == 2 && state.get("x") == "10",
               "committed prefix was not applied in order");
        expect(dkv::apply_committed(log, state, 2) ==
                   dkv::ApplyCommittedResult{std::vector<dkv::AppliedLogEntry>{}},
               "repeated commit notification applied entries again");
        expect(dkv::apply_committed(log, state, 4) ==
                   dkv::ApplyCommittedResult{std::vector<dkv::AppliedLogEntry>{
                       {3, dkv::ApplyResult::Applied},
                       {4, dkv::ApplyResult::Applied}}} &&
               state.last_applied() == 4 && state.get("x") == "20",
               "later committed entries were not applied");
        expect(dkv::apply_committed(log, state, 5) ==
                   dkv::ApplyCommittedResult{dkv::ApplyCommittedError::CommitBeyondLog} &&
               state.last_applied() == 4,
               "commit beyond the log changed state");
        expect(dkv::apply_committed(log, state, 3) ==
                   dkv::ApplyCommittedResult{dkv::ApplyCommittedError::StateAheadOfCommit},
               "commit index moved behind last applied");

        dkv::KvStateMachine recovered;
        const auto replayed = dkv::apply_committed(log, recovered, 4);
        const auto* results = std::get_if<std::vector<dkv::AppliedLogEntry>>(&replayed);
        expect(results && results->size() == 4 && recovered.get("x") == "20",
               "committed log did not rebuild a fresh state machine");
    }

    void test_request_results_replay_from_log() {
        TempDirectory dir;
        expect(!dir.path().empty(), "could not create replay test directory");
        if (dir.path().empty()) return;
        dkv::RequestId id;
        id.bytes[0] = 0xa5;
        const dkv::Command first{dkv::CommandType::Put, "x", "old", id};
        const dkv::Command later{dkv::CommandType::Put, "x", "new"};
        const dkv::Command conflict{dkv::CommandType::Put, "x", "wrong", id};
        {
            auto opened = dkv::PersistentLog::open(dir.path() / "raft.log");
            auto* log = std::get_if<std::unique_ptr<dkv::PersistentLog>>(&opened);
            expect(log && *log, "could not open request replay log");
            if (!log || !*log) return;
            expect((*log)->append({1, 2, first}) ==
                   dkv::PersistentLogAppendResult::Appended, "could not append first request");
            expect((*log)->append({2, 2, later}) ==
                   dkv::PersistentLogAppendResult::Appended, "could not append later write");
            expect((*log)->append({3, 3, first}) ==
                   dkv::PersistentLogAppendResult::Appended, "could not append retry");
            expect((*log)->append({4, 3, conflict}) ==
                   dkv::PersistentLogAppendResult::Appended, "could not append conflict");
        }
        auto reopened = dkv::PersistentLog::open(dir.path() / "raft.log");
        auto* log = std::get_if<std::unique_ptr<dkv::PersistentLog>>(&reopened);
        expect(log && *log, "could not reopen request replay log");
        if (!log || !*log) return;
        dkv::KvStateMachine recovered;
        const auto applied = dkv::apply_committed(**log, recovered, 4);
        expect(applied == dkv::ApplyCommittedResult{
                    std::vector<dkv::AppliedLogEntry>{
                        {1, dkv::ApplyResult::Applied},
                        {2, dkv::ApplyResult::Applied},
                        {3, dkv::ApplyResult::Applied},
                        {4, dkv::ApplyResult::RequestConflict}}},
               "replayed retry or conflict had the wrong result");
        const auto* prior = recovered.request_result(id);
        expect(prior && prior->index == 1 && prior->term == 2 &&
                   prior->command == first && recovered.get("x") == "new",
               "replay did not preserve original request result and state");
    }
}

int main() {
    test_committed_prefix_and_replay();
    test_request_results_replay_from_log();
    if (failures != 0) {
        std::cerr << failures << " test check(s) failed\n";
        return 1;
    }
    std::cout << "SUCCESS\n";
}

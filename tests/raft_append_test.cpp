#include "dkv/raft_append.hpp"

#include "dkv/persistent_log.hpp"
#include "dkv/persistent_metadata.hpp"

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
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
            const auto name = (std::filesystem::temp_directory_path() / "dkv-append-XXXXXX").string();
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

    std::unique_ptr<dkv::PersistentMetadata> open_metadata(const std::filesystem::path& path) {
        auto result = dkv::PersistentMetadata::open(path);
        auto* metadata = std::get_if<std::unique_ptr<dkv::PersistentMetadata>>(&result);
        expect(metadata != nullptr && *metadata != nullptr, "could not open metadata");
        return metadata ? std::move(*metadata) : nullptr;
    }

    std::unique_ptr<dkv::PersistentLog> open_log(const std::filesystem::path& path) {
        auto result = dkv::PersistentLog::open(path);
        auto* log = std::get_if<std::unique_ptr<dkv::PersistentLog>>(&result);
        expect(log != nullptr && *log != nullptr, "could not open log");
        return log ? std::move(*log) : nullptr;
    }

    dkv::LogEntry entry(std::uint64_t index, std::uint64_t term, std::string_view value = {}) {
        if (value.empty()) return {index, term, {dkv::CommandType::NoOp, "", ""}};
        return {index, term, {dkv::CommandType::Put, "key", std::string{value}}};
    }

    void append(dkv::PersistentLog& log, const dkv::LogEntry& value) {
        expect(log.append(value) == dkv::PersistentLogAppendResult::Appended,
               "could not set up log entry");
    }

    void expect_reply(const dkv::AppendEntriesHandleResult& result,
                      dkv::AppendEntriesReply wanted, std::string_view message) {
        const auto* reply = std::get_if<dkv::AppendEntriesReply>(&result);
        expect(reply != nullptr && *reply == wanted, message);
    }

    void expect_error(const dkv::AppendEntriesHandleResult& result,
                      dkv::AppendEntriesHandleError wanted, std::string_view message) {
        const auto* error = std::get_if<dkv::AppendEntriesHandleError>(&result);
        expect(error != nullptr && *error == wanted, message);
    }

    void test_terms_and_previous_entry() {
        TempDirectory dir;
        if (dir.path().empty()) {
            expect(false, "could not create temporary directory");
            return;
        }
        auto metadata = open_metadata(dir.path() / "raft.meta");
        auto log = open_log(dir.path() / "raft.log");
        if (!metadata || !log) return;
        append(*log, entry(1, 1));
        append(*log, entry(2, 2));
        expect(metadata->advance_term(3) == dkv::PersistentMetadataUpdateResult::Persisted,
               "could not set current term");
        std::uint64_t commit_index = 1;

        expect_reply(dkv::handle_append_entries(*metadata, *log, commit_index,
                                                {2, 8, 2, 2, {}, 2}),
                     {3, false}, "stale term was accepted");
        expect_reply(dkv::handle_append_entries(*metadata, *log, commit_index,
                                                {4, 8, 2, 1, {}, 2}),
                     {4, false}, "wrong preceding term was accepted");
        expect(metadata->state() == dkv::RaftMetadata{4, std::nullopt},
               "higher term was not recorded after previous-entry mismatch");
        expect_reply(dkv::handle_append_entries(*metadata, *log, commit_index,
                                                {4, 8, 3, 2, {}, 2}),
                     {4, false}, "missing preceding entry was accepted");
        expect(log->last_index() == 2 && commit_index == 1,
               "rejected requests changed log or commit index");

        metadata.reset();
        metadata = open_metadata(dir.path() / "raft.meta");
        expect(metadata && metadata->state().current_term == 4,
               "higher term did not survive restart");
    }

    void test_heartbeat_commits_only_confirmed_prefix() {
        TempDirectory dir;
        if (dir.path().empty()) {
            expect(false, "could not create temporary directory");
            return;
        }
        auto metadata = open_metadata(dir.path() / "raft.meta");
        auto log = open_log(dir.path() / "raft.log");
        if (!metadata || !log) return;
        append(*log, entry(1, 1));
        append(*log, entry(2, 2));
        std::uint64_t commit_index = 0;

        expect_reply(dkv::handle_append_entries(*metadata, *log, commit_index,
                                                {3, 7, 1, 1, {}, 2}),
                     {3, true}, "valid heartbeat was rejected");
        expect(commit_index == 1, "heartbeat committed an unconfirmed suffix");
        expect_reply(dkv::handle_append_entries(*metadata, *log, commit_index,
                                                {3, 7, 2, 2, {}, 2}),
                     {3, true}, "heartbeat matching full log was rejected");
        expect(commit_index == 2, "heartbeat did not advance commit index");
        expect_reply(dkv::handle_append_entries(*metadata, *log, commit_index,
                                                {3, 7, 1, 1, {}, 0}),
                     {3, true}, "older heartbeat was rejected");
        expect(commit_index == 2, "commit index moved backwards");
        expect(log->last_index() == 2, "heartbeat truncated follower suffix");
    }

    void test_append_duplicate_and_recovery() {
        TempDirectory dir;
        if (dir.path().empty()) {
            expect(false, "could not create temporary directory");
            return;
        }
        auto metadata = open_metadata(dir.path() / "raft.meta");
        auto log = open_log(dir.path() / "raft.log");
        if (!metadata || !log) return;
        std::uint64_t commit_index = 0;
        expect_reply(dkv::handle_append_entries(*metadata, *log, commit_index,
                                                {1, 7, 0, 0, {entry(1, 1)}, 1}),
                     {1, true}, "first entry was not appended to an empty log");
        expect(commit_index == 1, "first entry was not committed");
        const dkv::AppendEntries request{2, 7, 1, 1,
                                         {entry(2, 2, "first"), entry(3, 2, "second")}, 3};

        expect_reply(dkv::handle_append_entries(*metadata, *log, commit_index, request),
                     {2, true}, "new entries were rejected");
        expect(commit_index == 3 && log->last_index() == 3,
               "entries were not appended and committed");
        expect_reply(dkv::handle_append_entries(*metadata, *log, commit_index, request),
                     {2, true}, "duplicate entries were rejected");
        expect(log->last_index() == 3, "duplicate request appended entries twice");
        expect_error(dkv::handle_append_entries(*metadata, *log, commit_index,
                                                {2, 7, 1, 1, {entry(2, 2, "different")}, 2}),
                     dkv::AppendEntriesHandleError::InvalidRequest,
                     "same index and term with different command was accepted");
        expect(log->entry_at(2) == entry(2, 2, "first") && commit_index == 3,
               "invalid duplicate changed committed state");

        log.reset();
        log = open_log(dir.path() / "raft.log");
        expect(log && log->entry_at(2) == entry(2, 2, "first") &&
               log->entry_at(3) == entry(3, 2, "second"),
               "acknowledged entries did not survive restart");
    }

    void test_conflict_repair_and_committed_protection() {
        TempDirectory dir;
        if (dir.path().empty()) {
            expect(false, "could not create temporary directory");
            return;
        }
        auto metadata = open_metadata(dir.path() / "raft.meta");
        auto log = open_log(dir.path() / "raft.log");
        if (!metadata || !log) return;
        append(*log, entry(1, 1));
        append(*log, entry(2, 2, "old"));
        append(*log, entry(3, 2, "old-suffix"));
        std::uint64_t commit_index = 2;

        expect_error(dkv::handle_append_entries(*metadata, *log, commit_index,
                                                {3, 7, 1, 1, {entry(2, 3, "new")}, 2}),
                     dkv::AppendEntriesHandleError::CommittedConflict,
                     "committed conflict was overwritten");
        expect(log->last_index() == 3 && log->entry_at(2) == entry(2, 2, "old") &&
               commit_index == 2, "committed conflict changed follower state");
        // The same request can repair index 2 only when it is uncommitted.
        commit_index = 1;
        expect_reply(dkv::handle_append_entries(*metadata, *log, commit_index,
                                                {3, 7, 1, 1, {entry(2, 3, "new")}, 2}),
                     {3, true}, "uncommitted conflict was not repaired");
        expect(log->last_index() == 2 && log->entry_at(2) == entry(2, 3, "new") &&
               commit_index == 2, "conflicting suffix was not replaced");

        log.reset();
        log = open_log(dir.path() / "raft.log");
        expect(log && log->last_index() == 2 && log->entry_at(2) == entry(2, 3, "new"),
               "repaired suffix did not survive restart");
    }

    void test_invalid_requests_and_persistence_failure() {
        TempDirectory dir;
        if (dir.path().empty()) {
            expect(false, "could not create temporary directory");
            return;
        }
        const auto metadata_dir = dir.path() / "metadata";
        std::filesystem::create_directory(metadata_dir);
        auto metadata = open_metadata(metadata_dir / "raft.meta");
        auto log = open_log(dir.path() / "raft.log");
        if (!metadata || !log) return;
        std::uint64_t commit_index = 0;

        expect_error(dkv::handle_append_entries(*metadata, *log, commit_index,
                                                {1, 7, 0, 1, {}, 0}),
                     dkv::AppendEntriesHandleError::InvalidRequest,
                     "invalid previous index and term were accepted");
        expect_error(dkv::handle_append_entries(*metadata, *log, commit_index,
                                                {1, 7, 0, 0,
                                                 {entry(1, 1), entry(3, 1)}, 2}),
                     dkv::AppendEntriesHandleError::InvalidRequest,
                     "nonsequential request was accepted");
        expect_error(dkv::handle_append_entries(*metadata, *log, commit_index,
                                                {2, 7, 0, 0,
                                                 {entry(1, 2), entry(2, 1)}, 2}),
                     dkv::AppendEntriesHandleError::InvalidRequest,
                     "entry terms moved backwards");
        expect(log->last_index() == 0 && commit_index == 0,
               "invalid request partly modified log");

        std::filesystem::remove_all(metadata_dir);
        expect_error(dkv::handle_append_entries(*metadata, *log, commit_index,
                                                {3, 7, 0, 0, {}, 0}),
                     dkv::AppendEntriesHandleError::PersistenceError,
                     "failed term persistence produced a reply");
        expect(metadata->state().current_term == 2,
               "failed term persistence changed current term");
    }
}

int main() {
    test_terms_and_previous_entry();
    test_heartbeat_commits_only_confirmed_prefix();
    test_append_duplicate_and_recovery();
    test_conflict_repair_and_committed_protection();
    test_invalid_requests_and_persistence_failure();
    if (failures != 0) {
        std::cerr << failures << " test check(s) failed\n";
        return 1;
    }
    std::cout << "SUCCESS\n";
}

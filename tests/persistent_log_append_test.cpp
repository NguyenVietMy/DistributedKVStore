#include "dkv/persistent_log.hpp"

#include <cerrno>
#include <cstddef>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <sys/types.h>
#include <unistd.h>
#include <utility>
#include <variant>

namespace {
    enum class FaultMode {
        None,
        PartialWrite,
        SyncError,
        SyncInterrupted,
        TruncateError,
        RollbackSyncError
    };

    struct FaultState {
        FaultMode mode{FaultMode::None};
        int write_calls{0};
        int sync_calls{0};
        int truncate_calls{0};
    } fault;

    int failures = 0;

    void inject(FaultMode mode) {
        fault = FaultState{mode};
    }

    void expect(bool condition, std::string_view message) {
        if (!condition) {
            std::cerr << "FAIL: " << message << '\n';
            ++failures;
        }
    }

    struct TempLog {
        std::filesystem::path path;

        explicit TempLog(std::string_view name)
            : path(std::filesystem::temp_directory_path() /
                   ("dkv-" + std::string(name) + "-" + std::to_string(::getpid()) + ".log")) {
            std::filesystem::remove(path);
        }

        ~TempLog() {
            std::error_code error;
            std::filesystem::remove(path, error);
        }
    };

    std::unique_ptr<dkv::PersistentLog> open_log(const std::filesystem::path& path) {
        auto result = dkv::PersistentLog::open(path);
        auto* log = std::get_if<std::unique_ptr<dkv::PersistentLog>>(&result);
        expect(log != nullptr && *log != nullptr, "could not open test log");
        return log != nullptr ? std::move(*log) : nullptr;
    }

    void expect_entry(const dkv::PersistentLog& log, const dkv::LogEntry& expected) {
        const auto actual = log.entry_at(expected.index);
        expect(actual.has_value() && *actual == expected, "log entry did not match");
    }
}

extern "C" ssize_t __real_pwrite(int fd, const void* buffer, size_t count, off_t offset);
extern "C" int __real_fdatasync(int fd);
extern "C" int __real_ftruncate(int fd, off_t size);

extern "C" ssize_t __wrap_pwrite(int fd, const void* buffer, size_t count, off_t offset) {
    ++fault.write_calls;
    if (fault.mode == FaultMode::PartialWrite || fault.mode == FaultMode::TruncateError) {
        if (fault.write_calls == 1) {
            return __real_pwrite(fd, buffer, 5, offset);
        }
        errno = ENOSPC;
        return -1;
    }
    return __real_pwrite(fd, buffer, count, offset);
}

extern "C" int __wrap_fdatasync(int fd) {
    ++fault.sync_calls;
    if (fault.mode == FaultMode::SyncInterrupted && fault.sync_calls == 1) {
        errno = EINTR;
        return -1;
    }
    if ((fault.mode == FaultMode::SyncError && fault.sync_calls == 1) ||
        (fault.mode == FaultMode::RollbackSyncError && fault.sync_calls <= 2)) {
        errno = EIO;
        return -1;
    }
    return __real_fdatasync(fd);
}

extern "C" int __wrap_ftruncate(int fd, off_t size) {
    ++fault.truncate_calls;
    if (fault.mode == FaultMode::TruncateError) {
        errno = EIO;
        return -1;
    }
    return __real_ftruncate(fd, size);
}

namespace {
    void test_multiple_appends_survive_reopen() {
        TempLog temp{"multiple-appends"};
        const dkv::LogEntry first{1, 4, {dkv::CommandType::Put, "name", "alice"}};
        const dkv::LogEntry second{2, 5, {dkv::CommandType::Delete, "name", ""}};
        const dkv::LogEntry third{3, 5, {dkv::CommandType::Put, "city", "boston"}};

        auto log = open_log(temp.path);
        if (!log) return;
        expect(log->append(first) == dkv::PersistentLogAppendResult::Appended,
               "first append failed");
        expect(log->append(second) == dkv::PersistentLogAppendResult::Appended,
               "second append failed");
        expect(log->last_index() == 2 && log->last_term() == 5,
               "two appended entries had the wrong last index or term");
        expect_entry(*log, first);
        expect_entry(*log, second);

        log.reset();
        log = open_log(temp.path);
        if (!log) return;
        expect_entry(*log, first);
        expect_entry(*log, second);
        expect(log->append(third) == dkv::PersistentLogAppendResult::Appended,
               "append after recovery failed");

        log.reset();
        log = open_log(temp.path);
        if (!log) return;
        expect(log->last_index() == 3 && log->last_term() == 5,
               "recovered log had the wrong last index or term");
        expect_entry(*log, first);
        expect_entry(*log, second);
        expect_entry(*log, third);
    }

    void test_partial_write_rolls_back_and_allows_retry() {
        TempLog temp{"partial-write"};
        const dkv::LogEntry first{1, 3, {dkv::CommandType::Put, "a", "one"}};
        const dkv::LogEntry second{2, 3, {dkv::CommandType::Put, "b", "two"}};
        auto log = open_log(temp.path);
        if (!log) return;
        if (log->append(first) != dkv::PersistentLogAppendResult::Appended) {
            expect(false, "setup append failed for partial-write test");
            return;
        }
        const auto original_size = std::filesystem::file_size(temp.path);

        inject(FaultMode::PartialWrite);
        const auto result = log->append(second);
        const int write_calls = fault.write_calls;
        const int truncate_calls = fault.truncate_calls;
        inject(FaultMode::None);

        expect(result == dkv::PersistentLogAppendResult::IoError,
               "partial write did not return IoError");
        expect(write_calls == 2 && truncate_calls == 1,
               "partial write did not reach the rollback path");
        expect(std::filesystem::file_size(temp.path) == original_size,
               "partial write left bytes beyond the previous log end");
        expect(log->last_index() == 1 && !log->entry_at(2).has_value(),
               "failed append changed in-memory entries");
        expect(log->append(second) == dkv::PersistentLogAppendResult::Appended,
               "append could not retry after successful rollback");

        log.reset();
        log = open_log(temp.path);
        if (!log) return;
        expect_entry(*log, first);
        expect_entry(*log, second);
    }

    void test_sync_error_rolls_back_and_allows_retry() {
        TempLog temp{"sync-error"};
        const dkv::LogEntry first{1, 3, {dkv::CommandType::Put, "a", "one"}};
        const dkv::LogEntry second{2, 3, {dkv::CommandType::Put, "b", "two"}};
        auto log = open_log(temp.path);
        if (!log) return;
        if (log->append(first) != dkv::PersistentLogAppendResult::Appended) {
            expect(false, "setup append failed for sync-error test");
            return;
        }
        const auto original_size = std::filesystem::file_size(temp.path);

        inject(FaultMode::SyncError);
        const auto result = log->append(second);
        const int sync_calls = fault.sync_calls;
        const int truncate_calls = fault.truncate_calls;
        inject(FaultMode::None);

        expect(result == dkv::PersistentLogAppendResult::IoError,
               "sync failure did not return IoError");
        expect(sync_calls == 2 && truncate_calls == 1,
               "sync failure did not sync the rollback");
        expect(std::filesystem::file_size(temp.path) == original_size,
               "sync failure left the attempted entry on disk");
        expect(log->last_index() == 1 && !log->entry_at(2).has_value(),
               "sync failure changed in-memory entries");
        expect(log->append(second) == dkv::PersistentLogAppendResult::Appended,
               "append could not retry after sync failure rollback");

        log.reset();
        log = open_log(temp.path);
        if (!log) return;
        expect_entry(*log, first);
        expect_entry(*log, second);
    }

    void test_interrupted_sync_is_retried() {
        TempLog temp{"sync-interrupted"};
        const dkv::LogEntry entry{1, 3, {dkv::CommandType::Put, "a", "one"}};
        auto log = open_log(temp.path);
        if (!log) return;

        inject(FaultMode::SyncInterrupted);
        const auto result = log->append(entry);
        const int sync_calls = fault.sync_calls;
        inject(FaultMode::None);

        expect(result == dkv::PersistentLogAppendResult::Appended,
               "interrupted sync prevented append");
        expect(sync_calls == 2, "interrupted sync was not retried");
        log.reset();
        log = open_log(temp.path);
        if (log) expect_entry(*log, entry);
    }

    void test_failed_truncation_stops_future_appends() {
        TempLog temp{"truncate-error"};
        const dkv::LogEntry first{1, 3, {dkv::CommandType::Put, "a", "one"}};
        const dkv::LogEntry second{2, 3, {dkv::CommandType::Put, "b", "two"}};
        auto log = open_log(temp.path);
        if (!log) return;
        if (log->append(first) != dkv::PersistentLogAppendResult::Appended) {
            expect(false, "setup append failed for truncate-error test");
            return;
        }

        inject(FaultMode::TruncateError);
        const auto result = log->append(second);
        const int truncate_calls = fault.truncate_calls;
        inject(FaultMode::None);
        expect(result == dkv::PersistentLogAppendResult::IoError && truncate_calls == 1,
               "failed truncation did not report IoError");
        expect(log->append(second) == dkv::PersistentLogAppendResult::IoError,
               "log accepted append after failed truncation");
        expect(fault.write_calls == 0,
               "log attempted another write after failed truncation");
    }

    void test_failed_rollback_sync_stops_future_appends() {
        TempLog temp{"rollback-sync-error"};
        const dkv::LogEntry first{1, 3, {dkv::CommandType::Put, "a", "one"}};
        const dkv::LogEntry second{2, 3, {dkv::CommandType::Put, "b", "two"}};
        auto log = open_log(temp.path);
        if (!log) return;
        if (log->append(first) != dkv::PersistentLogAppendResult::Appended) {
            expect(false, "setup append failed for rollback-sync test");
            return;
        }

        inject(FaultMode::RollbackSyncError);
        const auto result = log->append(second);
        const int sync_calls = fault.sync_calls;
        const int truncate_calls = fault.truncate_calls;
        inject(FaultMode::None);
        expect(result == dkv::PersistentLogAppendResult::IoError &&
               sync_calls == 2 && truncate_calls == 1,
               "failed rollback sync did not report IoError");
        expect(log->append(second) == dkv::PersistentLogAppendResult::IoError,
               "log accepted append after failed rollback sync");
        expect(fault.write_calls == 0,
               "log attempted another write after failed rollback sync");
    }
}

int main() {
    test_multiple_appends_survive_reopen();
    test_partial_write_rolls_back_and_allows_retry();
    test_sync_error_rolls_back_and_allows_retry();
    test_interrupted_sync_is_retried();
    test_failed_truncation_stops_future_appends();
    test_failed_rollback_sync_stops_future_appends();
    if (failures != 0) {
        std::cerr << failures << " test check(s) failed\n";
        return 1;
    }
    std::cout << "SUCCESS\n";
    return 0;
}

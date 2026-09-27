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
    enum class FaultMode { None, InterruptTruncate, FailTruncate, FailSync };

    struct FaultState {
        FaultMode mode{FaultMode::None};
        int truncate_calls{0};
        int sync_calls{0};
        int write_calls{0};
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
                   ("dkv-truncate-" + std::string(name) + "-" +
                    std::to_string(::getpid()) + ".log")) {
            std::filesystem::remove(path);
        }

        ~TempLog() {
            std::error_code error;
            std::filesystem::remove(path, error);
        }
    };

    const dkv::LogEntry first{1, 3, {dkv::CommandType::Put, "a", "one"}};
    const dkv::LogEntry second{2, 3, {dkv::CommandType::Put, "b", "two"}};
    const dkv::LogEntry third{3, 4, {dkv::CommandType::Delete, "a", ""}};

    std::unique_ptr<dkv::PersistentLog> open_log(const std::filesystem::path& path) {
        auto result = dkv::PersistentLog::open(path);
        auto* log = std::get_if<std::unique_ptr<dkv::PersistentLog>>(&result);
        expect(log != nullptr && *log != nullptr, "could not open test log");
        return log != nullptr ? std::move(*log) : nullptr;
    }

    bool append_entry(dkv::PersistentLog& log, const dkv::LogEntry& entry) {
        const bool appended = log.append(entry) == dkv::PersistentLogAppendResult::Appended;
        expect(appended, "could not append test entry");
        return appended;
    }

    void expect_entry(const dkv::PersistentLog& log, const dkv::LogEntry& expected) {
        const auto actual = log.entry_at(expected.index);
        expect(actual.has_value() && *actual == expected, "log entry did not match");
    }
}

extern "C" int __real_ftruncate(int fd, off_t size);
extern "C" int __real_fdatasync(int fd);
extern "C" ssize_t __real_pwrite(int fd, const void* buffer, size_t count, off_t offset);

extern "C" int __wrap_ftruncate(int fd, off_t size) {
    ++fault.truncate_calls;
    if (fault.mode == FaultMode::InterruptTruncate && fault.truncate_calls == 1) {
        errno = EINTR;
        return -1;
    }
    if (fault.mode == FaultMode::FailTruncate) {
        errno = EIO;
        return -1;
    }
    return __real_ftruncate(fd, size);
}

extern "C" int __wrap_fdatasync(int fd) {
    ++fault.sync_calls;
    if (fault.mode == FaultMode::FailSync) {
        errno = EIO;
        return -1;
    }
    return __real_fdatasync(fd);
}

extern "C" ssize_t __wrap_pwrite(int fd, const void* buffer, size_t count, off_t offset) {
    ++fault.write_calls;
    return __real_pwrite(fd, buffer, count, offset);
}

namespace {
    void test_middle_truncation_survives_reopen_and_reappend() {
        TempLog temp{"middle"};
        auto log = open_log(temp.path);
        if (!log || !append_entry(*log, first)) return;
        const auto size_after_first = std::filesystem::file_size(temp.path);
        if (!append_entry(*log, second) || !append_entry(*log, third)) return;

        inject(FaultMode::None);
        const auto result = log->truncate_suffix(2);
        expect(result == dkv::PersistentLogTruncateResult::Truncated,
               "middle truncation failed");
        expect(fault.truncate_calls == 1 && fault.sync_calls == 1,
               "middle truncation did not truncate and sync the file");
        expect(std::filesystem::file_size(temp.path) == size_after_first,
               "middle truncation kept the wrong number of file bytes");
        expect(log->last_index() == 1 && log->last_term() == 3,
               "middle truncation kept the wrong entries in memory");
        expect_entry(*log, first);
        expect(!log->entry_at(2).has_value(), "middle truncation retained index 2");

        log.reset();
        log = open_log(temp.path);
        if (!log) return;
        expect(log->last_index() == 1, "recovered truncated log had wrong last index");
        expect_entry(*log, first);
        const dkv::LogEntry replacement{2, 5, {dkv::CommandType::Put, "new", "value"}};
        if (!append_entry(*log, replacement)) return;

        log.reset();
        log = open_log(temp.path);
        if (!log) return;
        expect(log->last_index() == 2 && log->last_term() == 5,
               "replacement entry did not survive reopening");
        expect_entry(*log, first);
        expect_entry(*log, replacement);
    }

    void test_truncate_all_and_start_again() {
        TempLog temp{"all"};
        auto log = open_log(temp.path);
        if (!log || !append_entry(*log, first) || !append_entry(*log, second)) return;

        expect(log->truncate_suffix(1) == dkv::PersistentLogTruncateResult::Truncated,
               "truncating all entries failed");
        expect(std::filesystem::file_size(temp.path) == 0,
               "truncating all entries did not empty the file");
        expect(log->last_index() == 0 && log->last_term() == 0,
               "truncating all entries did not empty memory");

        log.reset();
        log = open_log(temp.path);
        if (!log) return;
        expect(log->last_index() == 0, "empty log did not survive reopening");
        if (!append_entry(*log, first)) return;
        log.reset();
        log = open_log(temp.path);
        if (log) expect_entry(*log, first);
    }

    void test_invalid_indexes_and_noop_preserve_file() {
        TempLog temp{"indexes"};
        auto log = open_log(temp.path);
        if (!log) return;
        expect(log->truncate_suffix(0) == dkv::PersistentLogTruncateResult::InvalidIndex,
               "empty log accepted index 0");
        expect(log->truncate_suffix(2) == dkv::PersistentLogTruncateResult::InvalidIndex,
               "empty log accepted index 2");
        expect(log->truncate_suffix(1) == dkv::PersistentLogTruncateResult::Truncated,
               "empty log rejected no-op truncation");

        if (!append_entry(*log, first)) return;
        const auto original_size = std::filesystem::file_size(temp.path);
        inject(FaultMode::None);
        expect(log->truncate_suffix(0) == dkv::PersistentLogTruncateResult::InvalidIndex,
               "nonempty log accepted index 0");
        expect(log->truncate_suffix(3) == dkv::PersistentLogTruncateResult::InvalidIndex,
               "nonempty log accepted out-of-range index");
        expect(log->truncate_suffix(2) == dkv::PersistentLogTruncateResult::Truncated,
               "nonempty log rejected no-op truncation");
        expect(fault.truncate_calls == 0 && fault.sync_calls == 0,
               "invalid or no-op truncation touched the file");
        expect(std::filesystem::file_size(temp.path) == original_size,
               "invalid or no-op truncation changed file size");
        expect_entry(*log, first);
    }

    void test_interrupted_truncation_is_retried() {
        TempLog temp{"interrupted"};
        auto log = open_log(temp.path);
        if (!log || !append_entry(*log, first) || !append_entry(*log, second)) return;

        inject(FaultMode::InterruptTruncate);
        const auto result = log->truncate_suffix(2);
        expect(result == dkv::PersistentLogTruncateResult::Truncated,
               "interrupted truncation failed");
        expect(fault.truncate_calls == 2 && fault.sync_calls == 1,
               "interrupted truncation was not retried and synced");
        log.reset();
        log = open_log(temp.path);
        if (log) {
            expect(log->last_index() == 1, "recovery retained truncated entry");
            expect_entry(*log, first);
        }
    }

    void test_truncate_failure_stops_future_mutations() {
        TempLog temp{"truncate-failure"};
        auto log = open_log(temp.path);
        if (!log || !append_entry(*log, first) || !append_entry(*log, second)) return;
        const auto original_size = std::filesystem::file_size(temp.path);

        inject(FaultMode::FailTruncate);
        const auto result = log->truncate_suffix(2);
        inject(FaultMode::None);
        expect(result == dkv::PersistentLogTruncateResult::IoError,
               "truncate failure did not report IoError");
        expect(std::filesystem::file_size(temp.path) == original_size,
               "failed truncation changed file size");
        expect(log->append(third) == dkv::PersistentLogAppendResult::IoError,
               "append succeeded after failed truncation");
        expect(fault.write_calls == 0, "append wrote after failed truncation");
        expect(log->truncate_suffix(1) == dkv::PersistentLogTruncateResult::IoError,
               "second truncation succeeded after failure");
    }

    void test_sync_failure_stops_future_mutations() {
        TempLog temp{"sync-failure"};
        auto log = open_log(temp.path);
        if (!log || !append_entry(*log, first) || !append_entry(*log, second)) return;

        inject(FaultMode::FailSync);
        const auto result = log->truncate_suffix(2);
        const int truncate_calls = fault.truncate_calls;
        const int sync_calls = fault.sync_calls;
        inject(FaultMode::None);
        expect(result == dkv::PersistentLogTruncateResult::IoError,
               "truncation sync failure did not report IoError");
        expect(truncate_calls == 1 && sync_calls == 1,
               "truncation did not attempt to sync the shortened file");
        expect(log->append(third) == dkv::PersistentLogAppendResult::IoError,
               "append succeeded after truncation sync failure");
        expect(fault.write_calls == 0, "append wrote after truncation sync failure");
        expect(log->truncate_suffix(1) == dkv::PersistentLogTruncateResult::IoError,
               "second truncation succeeded after sync failure");
    }
}

int main() {
    test_middle_truncation_survives_reopen_and_reappend();
    test_truncate_all_and_start_again();
    test_invalid_indexes_and_noop_preserve_file();
    test_interrupted_truncation_is_retried();
    test_truncate_failure_stops_future_mutations();
    test_sync_failure_stops_future_mutations();
    if (failures != 0) {
        std::cerr << failures << " test check(s) failed\n";
        return 1;
    }
    std::cout << "SUCCESS\n";
    return 0;
}

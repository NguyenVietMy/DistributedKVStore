#include "dkv/persistent_metadata.hpp"

#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <unistd.h>
#include <utility>
#include <variant>
#include <vector>

namespace {
    enum class FaultMode {
        None,
        PartialWrite,
        FileSyncError,
        FileSyncInterrupted,
        RenameError,
        DirectorySyncError,
        DirectorySyncInterrupted
    };

    struct FaultState {
        FaultMode mode{FaultMode::None};
        int writes{0};
        int file_syncs{0};
        int renames{0};
        int directory_syncs{0};
    } fault;

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
            const auto name = (std::filesystem::temp_directory_path() / "dkv-metadata-fault-XXXXXX").string();
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

    void expect_no_temp_files(const std::filesystem::path& dir) {
        std::size_t files = 0;
        for (const auto& entry : std::filesystem::directory_iterator(dir)) {
            ++files;
            expect(entry.path().filename() == "raft.meta", "failed update left a temporary file");
        }
        expect(files == 1, "metadata directory has an unexpected number of files");
    }
}

extern "C" ssize_t __real_write(int fd, const void* data, size_t size);
extern "C" int __real_fdatasync(int fd);
extern "C" int __real_rename(const char* from, const char* to);
extern "C" int __real_fsync(int fd);

extern "C" ssize_t __wrap_write(int fd, const void* data, size_t size) {
    ++fault.writes;
    if (fault.mode == FaultMode::PartialWrite) {
        if (fault.writes == 1) return __real_write(fd, data, 5);
        errno = ENOSPC;
        return -1;
    }
    return __real_write(fd, data, size);
}

extern "C" int __wrap_fdatasync(int fd) {
    ++fault.file_syncs;
    if (fault.file_syncs == 1 && fault.mode == FaultMode::FileSyncError) {
        errno = EIO;
        return -1;
    }
    if (fault.file_syncs == 1 && fault.mode == FaultMode::FileSyncInterrupted) {
        errno = EINTR;
        return -1;
    }
    return __real_fdatasync(fd);
}

extern "C" int __wrap_rename(const char* from, const char* to) {
    ++fault.renames;
    if (fault.mode == FaultMode::RenameError) {
        errno = EIO;
        return -1;
    }
    return __real_rename(from, to);
}

extern "C" int __wrap_fsync(int fd) {
    ++fault.directory_syncs;
    if (fault.directory_syncs == 1 && fault.mode == FaultMode::DirectorySyncError) {
        errno = EIO;
        return -1;
    }
    if (fault.directory_syncs == 1 && fault.mode == FaultMode::DirectorySyncInterrupted) {
        errno = EINTR;
        return -1;
    }
    return __real_fsync(fd);
}

namespace {
    void test_failure_before_replace(FaultMode mode) {
        TempDirectory dir;
        if (dir.path().empty()) {
            expect(false, "could not create temporary directory");
            return;
        }
        const auto path = dir.path() / "raft.meta";
        auto metadata = open_metadata(path);
        if (!metadata) return;
        using R = dkv::PersistentMetadataUpdateResult;
        if (metadata->advance_term(1) != R::Persisted) {
            expect(false, "could not persist initial metadata");
            return;
        }

        fault = FaultState{mode};
        const auto result = metadata->advance_term(2);
        const auto observed = fault;
        fault = FaultState{};

        expect(result == R::IoError, "failed update did not report an I/O error");
        expect(metadata->state() == dkv::RaftMetadata{1, std::nullopt},
               "failed update changed in-memory state");
        if (mode == FaultMode::PartialWrite) expect(observed.writes == 2, "partial write path was not exercised");
        if (mode == FaultMode::FileSyncError) expect(observed.file_syncs == 1, "file sync path was not exercised");
        if (mode == FaultMode::RenameError) expect(observed.renames == 1, "rename path was not exercised");

        auto reopened = open_metadata(path);
        if (reopened) expect(reopened->state() == dkv::RaftMetadata{1, std::nullopt},
                             "failed replacement changed recovered state");
        expect_no_temp_files(dir.path());

        if (mode == FaultMode::RenameError) {
            expect(metadata->advance_term(2) == R::IoError, "ambiguous rename failure allowed retry");
        } else {
            expect(metadata->advance_term(2) == R::Persisted,
                   "failure before replacement prevented a safe retry");
        }
    }

    void test_directory_sync_failure_blocks_updates() {
        TempDirectory dir;
        if (dir.path().empty()) return;
        const auto path = dir.path() / "raft.meta";
        auto metadata = open_metadata(path);
        if (!metadata) return;
        using R = dkv::PersistentMetadataUpdateResult;
        expect(metadata->advance_term(1) == R::Persisted, "could not persist initial term");

        fault = FaultState{FaultMode::DirectorySyncError};
        const auto result = metadata->advance_term(2);
        const auto observed = fault;
        fault = FaultState{};

        expect(result == R::IoError && observed.directory_syncs == 1,
               "directory sync failure was not reported");
        expect(metadata->state() == dkv::RaftMetadata{1, std::nullopt},
               "failed directory sync changed acknowledged state");
        expect(metadata->advance_term(3) == R::IoError,
               "metadata accepted an update after ambiguous replacement");
        fault = FaultState{};
        auto reopened = open_metadata(path);
        const auto recovery_syncs = fault;
        fault = FaultState{};
        if (reopened) expect(reopened->state() == dkv::RaftMetadata{2, std::nullopt},
                             "replacement was not visible before directory sync");
        expect(recovery_syncs.file_syncs == 1 && recovery_syncs.directory_syncs == 1,
               "reopening did not synchronize the recovered state");
        expect_no_temp_files(dir.path());
    }

    void test_interrupted_syncs_are_retried(FaultMode mode) {
        TempDirectory dir;
        if (dir.path().empty()) return;
        auto metadata = open_metadata(dir.path() / "raft.meta");
        if (!metadata) return;
        fault = FaultState{mode};
        const auto result = metadata->advance_term(1);
        const auto observed = fault;
        fault = FaultState{};
        expect(result == dkv::PersistentMetadataUpdateResult::Persisted,
               "interrupted sync was not retried");
        if (mode == FaultMode::FileSyncInterrupted) {
            expect(observed.file_syncs == 2, "file sync was not retried after EINTR");
        } else {
            expect(observed.directory_syncs == 2, "directory sync was not retried after EINTR");
        }
    }

    void test_recovery_requires_successful_sync(FaultMode mode) {
        TempDirectory dir;
        if (dir.path().empty()) return;
        const auto path = dir.path() / "raft.meta";
        auto metadata = open_metadata(path);
        if (!metadata) return;
        expect(metadata->advance_term(1) == dkv::PersistentMetadataUpdateResult::Persisted,
               "could not create recovery test metadata");
        metadata.reset();

        fault = FaultState{mode};
        auto result = dkv::PersistentMetadata::open(path);
        const auto observed = fault;
        fault = FaultState{};
        const auto* error = std::get_if<dkv::PersistentMetadataOpenError>(&result);
        expect(error != nullptr && *error == dkv::PersistentMetadataOpenError::IoError,
               "recovery accepted metadata that could not be synchronized");
        if (mode == FaultMode::FileSyncError) {
            expect(observed.file_syncs == 1, "recovery did not try to sync the file");
        } else {
            expect(observed.directory_syncs == 1, "recovery did not try to sync the directory");
        }
    }
}

int main() {
    test_failure_before_replace(FaultMode::PartialWrite);
    test_failure_before_replace(FaultMode::FileSyncError);
    test_failure_before_replace(FaultMode::RenameError);
    test_directory_sync_failure_blocks_updates();
    test_interrupted_syncs_are_retried(FaultMode::FileSyncInterrupted);
    test_interrupted_syncs_are_retried(FaultMode::DirectorySyncInterrupted);
    test_recovery_requires_successful_sync(FaultMode::FileSyncError);
    test_recovery_requires_successful_sync(FaultMode::DirectorySyncError);
    if (failures != 0) {
        std::cerr << failures << " test check(s) failed\n";
        return 1;
    }
    std::cout << "SUCCESS\n";
}

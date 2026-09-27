#include "dkv/persistent_metadata.hpp"

#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <unistd.h>
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
            const auto name = (std::filesystem::temp_directory_path() / "dkv-metadata-XXXXXX").string();
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
        if (auto* metadata = std::get_if<std::unique_ptr<dkv::PersistentMetadata>>(&result)) {
            return std::move(*metadata);
        }
        expect(false, "opening metadata failed");
        return nullptr;
    }

    void expect_open_error(const std::filesystem::path& path,
                           dkv::PersistentMetadataOpenError wanted,
                           std::string_view message) {
        auto result = dkv::PersistentMetadata::open(path);
        const auto* error = std::get_if<dkv::PersistentMetadataOpenError>(&result);
        expect(error != nullptr && *error == wanted, message);
    }

    void test_term_and_vote_survive_restart() {
        TempDirectory dir;
        expect(!dir.path().empty(), "could not create temporary directory");
        if (dir.path().empty()) return;
        const auto path = dir.path() / "raft.meta";
        auto metadata = open_metadata(path);
        if (!metadata) return;

        using R = dkv::PersistentMetadataUpdateResult;
        expect(metadata->state() == dkv::RaftMetadata{}, "new metadata is not empty");
        expect(metadata->vote_for(0) == R::InvalidTerm, "voted in term zero");
        expect(metadata->advance_term(1) == R::Persisted, "term one was not persisted");
        expect(metadata->vote_for(0) == R::Persisted, "node zero vote was not persisted");
        expect(metadata->vote_for(0) == R::Unchanged, "same vote was not idempotent");
        expect(metadata->vote_for(2) == R::VoteConflict, "second vote in term one was accepted");
        expect(metadata->advance_term(0) == R::StaleTerm, "term rollback was accepted");
        metadata.reset();

        metadata = open_metadata(path);
        if (!metadata) return;
        expect(metadata->state() == dkv::RaftMetadata{1, 0}, "term and vote did not survive restart");
        expect(metadata->advance_term(2) == R::Persisted, "new term was not persisted");
        expect(metadata->state() == dkv::RaftMetadata{2, std::nullopt}, "new term did not clear vote");
        expect(metadata->advance_term(2) == R::Unchanged, "same term was not idempotent");
        metadata.reset();

        metadata = open_metadata(path);
        if (!metadata) return;
        expect(metadata->state() == dkv::RaftMetadata{2, std::nullopt}, "cleared vote did not survive restart");
        expect(metadata->vote_for(3) == R::Persisted, "new term could not vote");
    }

    void test_bad_records_are_rejected() {
        TempDirectory dir;
        if (dir.path().empty()) {
            expect(false, "could not create temporary directory");
            return;
        }
        const auto path = dir.path() / "raft.meta";
        {
            std::ofstream file(path, std::ios::binary);
            file.put('D');
        }
        expect_open_error(path, dkv::PersistentMetadataOpenError::TruncatedRecord,
                          "truncated metadata was accepted");

        {
            auto metadata = open_metadata(dir.path() / "valid.meta");
            if (!metadata) return;
            expect(metadata->advance_term(4) == dkv::PersistentMetadataUpdateResult::Persisted,
                   "could not create valid metadata record");
        }
        const auto valid_path = dir.path() / "valid.meta";
        std::filesystem::copy_file(valid_path, path, std::filesystem::copy_options::overwrite_existing);
        {
            std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
            file.seekp(5);
            file.put('\x01');
        }
        expect_open_error(path, dkv::PersistentMetadataOpenError::CorruptRecord,
                          "checksum mismatch was accepted");

        std::filesystem::copy_file(valid_path, path, std::filesystem::copy_options::overwrite_existing);
        {
            std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
            file.seekp(4);
            file.put('\x02');
        }
        expect_open_error(path, dkv::PersistentMetadataOpenError::UnsupportedVersion,
                          "unsupported version was accepted");

        std::filesystem::copy_file(valid_path, path, std::filesystem::copy_options::overwrite_existing);
        {
            std::ofstream file(path, std::ios::app | std::ios::binary);
            file.put('x');
        }
        expect_open_error(path, dkv::PersistentMetadataOpenError::CorruptRecord,
                          "extra metadata bytes were accepted");
    }

    void test_invalid_parent_is_io_error() {
        TempDirectory dir;
        if (dir.path().empty()) return;
        expect_open_error(dir.path() / "missing" / "raft.meta",
                          dkv::PersistentMetadataOpenError::IoError,
                          "missing parent directory was accepted");
    }
}

int main() {
    test_term_and_vote_survive_restart();
    test_bad_records_are_rejected();
    test_invalid_parent_is_io_error();
    if (failures != 0) {
        std::cerr << failures << " test check(s) failed\n";
        return 1;
    }
    std::cout << "SUCCESS\n";
}

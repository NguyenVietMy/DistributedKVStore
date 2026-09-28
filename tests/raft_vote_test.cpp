#include "dkv/raft_vote.hpp"

#include "dkv/persistent_log.hpp"
#include "dkv/persistent_metadata.hpp"

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
            const auto name = (std::filesystem::temp_directory_path() / "dkv-vote-XXXXXX").string();
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
        expect(metadata != nullptr && *metadata != nullptr, "could not open test metadata");
        return metadata ? std::move(*metadata) : nullptr;
    }

    std::unique_ptr<dkv::PersistentLog> open_log(const std::filesystem::path& path) {
        auto result = dkv::PersistentLog::open(path);
        auto* log = std::get_if<std::unique_ptr<dkv::PersistentLog>>(&result);
        expect(log != nullptr && *log != nullptr, "could not open test log");
        return log ? std::move(*log) : nullptr;
    }

    void expect_reply(const dkv::RequestVoteHandleResult& result,
                      dkv::RequestVoteReply wanted,
                      std::string_view message) {
        const auto* reply = std::get_if<dkv::RequestVoteReply>(&result);
        expect(reply != nullptr && *reply == wanted, message);
    }

    void expect_persistence_error(const dkv::RequestVoteHandleResult& result,
                                  std::string_view message) {
        const auto* error = std::get_if<dkv::RequestVoteHandleError>(&result);
        expect(error != nullptr && *error == dkv::RequestVoteHandleError::PersistenceError,
               message);
    }

    void test_first_vote_repeated_vote_and_conflict() {
        TempDirectory dir;
        if (dir.path().empty()) {
            expect(false, "could not create temporary directory");
            return;
        }
        const auto metadata_path = dir.path() / "raft.meta";
        auto metadata = open_metadata(metadata_path);
        auto log = open_log(dir.path() / "raft.log");
        if (!metadata || !log) return;

        expect_reply(dkv::handle_request_vote(*metadata, *log, {0, 0, 0, 0}), {0, false},
                     "term-zero request was granted");
        expect_reply(dkv::handle_request_vote(*metadata, *log, {1, 0, 0, 0}), {1, true},
                     "first vote was not granted");
        expect(metadata->state() == dkv::RaftMetadata{1, 0},
               "first vote did not update term and candidate");

        metadata.reset();
        metadata = open_metadata(metadata_path);
        if (!metadata) return;
        expect(metadata->state() == dkv::RaftMetadata{1, 0},
               "granted vote did not survive reopening");
        expect_reply(dkv::handle_request_vote(*metadata, *log, {1, 0, 0, 0}), {1, true},
                     "repeated request from same candidate was denied");
        expect_reply(dkv::handle_request_vote(*metadata, *log, {1, 2, 0, 0}), {1, false},
                     "second candidate in same term was granted a vote");
        expect_reply(dkv::handle_request_vote(*metadata, *log, {2, 2, 0, 0}), {2, true},
                     "new term did not permit a new vote");
        expect_reply(dkv::handle_request_vote(*metadata, *log, {1, 0, 0, 0}), {2, false},
                     "stale term request was not rejected with current term");
    }

    void test_log_freshness_and_higher_term() {
        TempDirectory dir;
        if (dir.path().empty()) return;
        const auto metadata_path = dir.path() / "raft.meta";
        auto metadata = open_metadata(metadata_path);
        auto log = open_log(dir.path() / "raft.log");
        if (!metadata || !log) return;
        using R = dkv::PersistentMetadataUpdateResult;

        expect(log->append({1, 2, {dkv::CommandType::NoOp, "", ""}}) ==
                   dkv::PersistentLogAppendResult::Appended, "could not append first test entry");
        expect(log->append({2, 3, {dkv::CommandType::NoOp, "", ""}}) ==
                   dkv::PersistentLogAppendResult::Appended, "could not append second test entry");
        expect(metadata->advance_term(3) == R::Persisted, "could not set up term three");
        expect(metadata->vote_for(1) == R::Persisted, "could not set up term-three vote");

        expect_reply(dkv::handle_request_vote(*metadata, *log, {4, 7, 100, 2}), {4, false},
                     "longer log with older last term was accepted");
        expect(metadata->state() == dkv::RaftMetadata{4, std::nullopt},
               "higher term was not persisted or old vote was not cleared on rejection");
        metadata.reset();
        metadata = open_metadata(metadata_path);
        if (!metadata) return;
        expect(metadata->state() == dkv::RaftMetadata{4, std::nullopt},
               "rejected higher term did not survive reopening");
        expect_reply(dkv::handle_request_vote(*metadata, *log, {4, 7, 1, 3}), {4, false},
                     "shorter log with equal last term was accepted");
        expect(metadata->state().voted_for == std::nullopt,
               "stale candidate received a saved vote");
        expect_reply(dkv::handle_request_vote(*metadata, *log, {4, 7, 2, 3}), {4, true},
                     "equal log was not accepted");
        expect_reply(dkv::handle_request_vote(*metadata, *log, {5, 8, 1, 4}), {5, true},
                     "shorter log with newer last term was rejected");
        expect_reply(dkv::handle_request_vote(*metadata, *log, {6, 9, 3, 3}), {6, true},
                     "longer log with equal last term was rejected");
    }

    void test_persistence_failure_produces_no_reply() {
        TempDirectory dir;
        if (dir.path().empty()) return;
        const auto metadata_dir = dir.path() / "metadata";
        std::filesystem::create_directory(metadata_dir);
        auto metadata = open_metadata(metadata_dir / "raft.meta");
        auto log = open_log(dir.path() / "raft.log");
        if (!metadata || !log) return;

        std::filesystem::remove(metadata_dir);
        expect_persistence_error(dkv::handle_request_vote(*metadata, *log, {1, 2, 0, 0}),
                                 "failed term persistence produced a reply");
        expect(metadata->state() == dkv::RaftMetadata{},
               "failed term persistence changed state");

        std::filesystem::create_directory(metadata_dir);
        expect(metadata->advance_term(1) == dkv::PersistentMetadataUpdateResult::Persisted,
               "could not set up failed-vote test");
        std::filesystem::remove_all(metadata_dir);
        expect_persistence_error(dkv::handle_request_vote(*metadata, *log, {1, 2, 0, 0}),
                                 "failed vote persistence produced a reply");
        expect(metadata->state() == dkv::RaftMetadata{1, std::nullopt},
               "failed vote persistence changed state");
    }
}

int main() {
    test_first_vote_repeated_vote_and_conflict();
    test_log_freshness_and_higher_term();
    test_persistence_failure_produces_no_reply();
    if (failures != 0) {
        std::cerr << failures << " test check(s) failed\n";
        return 1;
    }
    std::cout << "SUCCESS\n";
}

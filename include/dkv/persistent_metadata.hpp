#pragma once

#include "dkv/raft_metadata.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <variant>

namespace dkv {
    enum class PersistentMetadataOpenError {
        IoError,
        TruncatedRecord,
        CorruptRecord,
        UnsupportedVersion
    };

    enum class PersistentMetadataUpdateResult {
        Persisted,
        Unchanged,
        StaleTerm,
        VoteConflict,
        InvalidTerm,
        IoError
    };

    class PersistentMetadata;
    using PersistentMetadataOpenResult =
        std::variant<std::unique_ptr<PersistentMetadata>, PersistentMetadataOpenError>;

    class PersistentMetadata {
    public:
        // A missing file starts at term zero with no vote; a damaged file is rejected.
        [[nodiscard]] static PersistentMetadataOpenResult open(const std::filesystem::path& path);
        [[nodiscard]] const RaftMetadata& state() const noexcept;
        // Persisted means the new file and its directory entry were synchronized.
        // After an ambiguous replacement failure, updates return IoError until reopened.
        [[nodiscard]] PersistentMetadataUpdateResult advance_term(std::uint64_t term);
        [[nodiscard]] PersistentMetadataUpdateResult vote_for(std::uint64_t candidate);

        PersistentMetadata(const PersistentMetadata&) = delete;
        PersistentMetadata& operator=(const PersistentMetadata&) = delete;
        PersistentMetadata(PersistentMetadata&&) = delete;
        PersistentMetadata& operator=(PersistentMetadata&&) = delete;

    private:
        explicit PersistentMetadata(std::filesystem::path path, RaftMetadata state);
        [[nodiscard]] PersistentMetadataUpdateResult persist(const RaftMetadata& next_state);

        std::filesystem::path path_;
        RaftMetadata state_;
        bool io_failed_{false};
    };
}

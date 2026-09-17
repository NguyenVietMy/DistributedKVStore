#pragma once

#include "dkv/log_entry.hpp"
#include <filesystem>
#include <memory>
#include <variant>
#include <cstdint>
#include <optional>
#include <vector>

namespace dkv {
    enum class PersistentLogOpenError {
        IoError,
        TruncatedRecord,
        CorruptRecord,
        NonSequentialIndex
    };

    enum class PersistentLogAppendResult {
        Appended,
        UnexpectedIndex,
        InvalidEntry,
        IoError
    };

    enum class PersistentLogTruncateResult {
        Truncated,
        InvalidIndex,
        IoError
    };

    class PersistentLog;
    using PersistentLogOpenResult = std::variant<std::unique_ptr<PersistentLog>, PersistentLogOpenError>;
    class PersistentLog {
        public:
            [[nodiscard]] PersistentLogAppendResult append(const LogEntry& entry);
            [[nodiscard]] std::optional<LogEntry> entry_at(std::uint64_t index) const;
            [[nodiscard]] std::uint64_t last_index() const noexcept;
            [[nodiscard]] std::uint64_t last_term() const noexcept;
            [[nodiscard]] static PersistentLogOpenResult open(const std::filesystem::path& path);
            [[nodiscard]] PersistentLogTruncateResult truncate_suffix(std::uint64_t first_index_to_remove);

            PersistentLog(const PersistentLog&) = delete;
            PersistentLog& operator=(const PersistentLog&) = delete;
            PersistentLog(PersistentLog&&) = delete;
            PersistentLog& operator=(PersistentLog&&) = delete;
            ~PersistentLog();
        private:
        
            struct StoredEntry {
                LogEntry log_entry;
                std::uint64_t byte_offset;
            };
            explicit PersistentLog(int file_descriptor);

            int file_descriptor_;
            std::vector<StoredEntry> entries_;
            std::uint64_t file_size_{0};
    };
}
#pragma once
#include "dkv/log_entry.hpp"
#include <cstddef>
#include <span>
#include <variant>
#include <vector>

namespace dkv {
    enum class LogEntryCodecError{
        InvalidEntry,
        FieldTooLarge,
        InputTooShort,
        InvalidMagic,
        UnsupportedVersion,
        InvalidLength,
        TruncatedRecord,
        ChecksumMismatch,
        InvalidCommand
    };

    struct DecodedLogEntry {
        LogEntry entry;
        std::size_t bytes_consumed{0};

        bool operator==(const DecodedLogEntry&) const = default;
    };

    using EncodeLogEntryResult = std::variant<std::vector<std::byte>, LogEntryCodecError>;
    using DecodeLogEntryResult = std::variant<DecodedLogEntry, LogEntryCodecError>;
    [[nodiscard]] EncodeLogEntryResult encode_log_entry(const LogEntry& entry);

    [[nodiscard]] DecodeLogEntryResult decode_log_entry(std::span<const std::byte> bytes);
}
#pragma once

#include "dkv/raft_metadata.hpp"

#include <array>
#include <cstddef>
#include <span>
#include <variant>

namespace dkv {
    inline constexpr std::size_t raft_metadata_record_size = 26;
    using RaftMetadataRecord = std::array<std::byte, raft_metadata_record_size>;

    enum class RaftMetadataCodecError {
        InvalidState,
        TruncatedRecord,
        InvalidLength,
        InvalidMagic,
        UnsupportedVersion,
        ChecksumMismatch,
        InvalidVote
    };

    using EncodeRaftMetadataResult = std::variant<RaftMetadataRecord, RaftMetadataCodecError>;
    using DecodeRaftMetadataResult = std::variant<RaftMetadata, RaftMetadataCodecError>;

    [[nodiscard]] EncodeRaftMetadataResult encode_raft_metadata(const RaftMetadata& state);
    [[nodiscard]] DecodeRaftMetadataResult decode_raft_metadata(std::span<const std::byte> bytes);
}

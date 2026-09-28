#include "dkv/raft_metadata_codec.hpp"

#include "dkv/crc32.hpp"

#include <cstdint>
#include <optional>

namespace dkv {
    namespace {
        // DKVM | version | term (u64) | vote-present | candidate (u64) | CRC32.
        // Integers are stored in big-endian order; CRC32 covers all preceding bytes.
        constexpr std::size_t checksum_offset = raft_metadata_record_size - sizeof(std::uint32_t);

        void put_u64(std::span<std::byte> bytes, std::uint64_t value) {
            for (std::size_t i = 0; i < 8; ++i) {
                bytes[i] = static_cast<std::byte>(value >> (56 - i * 8));
            }
        }

        std::uint64_t get_u64(std::span<const std::byte> bytes) {
            std::uint64_t value = 0;
            for (std::size_t i = 0; i < 8; ++i) {
                value = (value << 8) | std::to_integer<std::uint8_t>(bytes[i]);
            }
            return value;
        }
    }

    EncodeRaftMetadataResult encode_raft_metadata(const RaftMetadata& state) {
        if (state.current_term == 0 && state.voted_for) {
            return RaftMetadataCodecError::InvalidState;
        }

        RaftMetadataRecord bytes{};
        bytes[0] = std::byte{'D'};
        bytes[1] = std::byte{'K'};
        bytes[2] = std::byte{'V'};
        bytes[3] = std::byte{'M'};
        bytes[4] = std::byte{1};
        put_u64(std::span<std::byte>{bytes}.subspan(5, 8), state.current_term);
        bytes[13] = state.voted_for.has_value() ? std::byte{1} : std::byte{0};
        if (state.voted_for) {
            put_u64(std::span<std::byte>{bytes}.subspan(14, 8), *state.voted_for);
        }
        const auto checksum = crc32(std::span<const std::byte>{bytes}.first(checksum_offset));
        for (std::size_t i = 0; i < 4; ++i) {
            bytes[checksum_offset + i] = static_cast<std::byte>(checksum >> (24 - i * 8));
        }
        return bytes;
    }

    DecodeRaftMetadataResult decode_raft_metadata(std::span<const std::byte> bytes) {
        if (bytes.size() < raft_metadata_record_size) {
            return RaftMetadataCodecError::TruncatedRecord;
        }
        if (bytes.size() > raft_metadata_record_size) {
            return RaftMetadataCodecError::InvalidLength;
        }
        if (bytes[0] != std::byte{'D'} || bytes[1] != std::byte{'K'} ||
            bytes[2] != std::byte{'V'} || bytes[3] != std::byte{'M'}) {
            return RaftMetadataCodecError::InvalidMagic;
        }
        if (bytes[4] != std::byte{1}) {
            return RaftMetadataCodecError::UnsupportedVersion;
        }
        std::uint32_t stored_checksum = 0;
        for (std::size_t i = 0; i < 4; ++i) {
            stored_checksum = (stored_checksum << 8) |
                std::to_integer<std::uint8_t>(bytes[checksum_offset + i]);
        }
        if (stored_checksum != crc32(bytes.first(checksum_offset))) {
            return RaftMetadataCodecError::ChecksumMismatch;
        }
        const auto term = get_u64(bytes.subspan(5, 8));
        const auto presence = bytes[13];
        if (presence != std::byte{0} && presence != std::byte{1}) {
            return RaftMetadataCodecError::InvalidVote;
        }
        const auto candidate = get_u64(bytes.subspan(14, 8));
        if ((presence == std::byte{0} && candidate != 0) ||
            (presence == std::byte{1} && term == 0)) {
            return RaftMetadataCodecError::InvalidVote;
        }
        return RaftMetadata{term, presence == std::byte{1}
            ? std::optional<std::uint64_t>{candidate} : std::nullopt};
    }
}

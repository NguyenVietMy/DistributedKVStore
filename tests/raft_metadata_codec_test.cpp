#include "dkv/raft_metadata_codec.hpp"

#include "dkv/crc32.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <optional>
#include <span>
#include <string_view>
#include <variant>

namespace {
    int failures = 0;

    void expect(bool condition, std::string_view message) {
        if (!condition) {
            std::cerr << "FAIL: " << message << '\n';
            ++failures;
        }
    }

    void expect_decode_error(std::span<const std::byte> bytes,
                             dkv::RaftMetadataCodecError wanted,
                             std::string_view message) {
        const auto result = dkv::decode_raft_metadata(bytes);
        const auto* error = std::get_if<dkv::RaftMetadataCodecError>(&result);
        expect(error != nullptr && *error == wanted, message);
    }

    void update_checksum(dkv::RaftMetadataRecord& bytes) {
        const auto checksum = dkv::crc32(
            std::span<const std::byte>{bytes}.first(dkv::raft_metadata_record_size - 4));
        for (std::size_t i = 0; i < 4; ++i) {
            bytes[dkv::raft_metadata_record_size - 4 + i] =
                static_cast<std::byte>(checksum >> (24 - i * 8));
        }
    }

    void test_round_trip_and_layout() {
        const std::array states{
            dkv::RaftMetadata{0, std::nullopt},
            dkv::RaftMetadata{7, std::nullopt},
            dkv::RaftMetadata{7, 0},
            dkv::RaftMetadata{std::numeric_limits<std::uint64_t>::max(),
                              std::numeric_limits<std::uint64_t>::max()}
        };
        for (const auto& state : states) {
            const auto encoded = dkv::encode_raft_metadata(state);
            const auto* bytes = std::get_if<dkv::RaftMetadataRecord>(&encoded);
            expect(bytes != nullptr, "valid metadata did not encode");
            if (!bytes) continue;
            const auto decoded = dkv::decode_raft_metadata(*bytes);
            const auto* recovered = std::get_if<dkv::RaftMetadata>(&decoded);
            expect(recovered != nullptr && *recovered == state,
                   "metadata did not round-trip through codec");
        }

        const auto encoded = dkv::encode_raft_metadata(
            {0x0102030405060708ULL, 0x1112131415161718ULL});
        const auto* bytes = std::get_if<dkv::RaftMetadataRecord>(&encoded);
        if (!bytes) {
            expect(false, "could not encode layout test metadata");
            return;
        }
        expect((*bytes)[0] == std::byte{'D'} && (*bytes)[1] == std::byte{'K'} &&
                   (*bytes)[2] == std::byte{'V'} && (*bytes)[3] == std::byte{'M'} &&
                   (*bytes)[4] == std::byte{1}, "record marker or version was wrong");
        for (std::size_t i = 0; i < 8; ++i) {
            expect((*bytes)[5 + i] == static_cast<std::byte>(i + 1),
                   "term was not stored in big-endian order");
            expect((*bytes)[14 + i] == static_cast<std::byte>(0x11 + i),
                   "candidate was not stored in big-endian order");
        }
        expect((*bytes)[13] == std::byte{1}, "vote-presence flag was wrong");
    }

    void test_existing_record_format() {
        // Term 1, vote for node 0, encoded by the original metadata writer.
        constexpr std::array<std::uint8_t, dkv::raft_metadata_record_size> old_record{
            0x44, 0x4b, 0x56, 0x4d, 0x01,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
            0x01,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xa8, 0x1e, 0xe4, 0x2e
        };
        dkv::RaftMetadataRecord bytes{};
        for (std::size_t i = 0; i < bytes.size(); ++i) {
            bytes[i] = static_cast<std::byte>(old_record[i]);
        }
        const auto decoded = dkv::decode_raft_metadata(bytes);
        const auto* state = std::get_if<dkv::RaftMetadata>(&decoded);
        expect(state != nullptr && *state == dkv::RaftMetadata{1, 0},
               "codec could not read the original metadata format");
        const auto encoded = dkv::encode_raft_metadata({1, 0});
        const auto* rewritten = std::get_if<dkv::RaftMetadataRecord>(&encoded);
        expect(rewritten != nullptr && *rewritten == bytes,
               "codec changed the original metadata record bytes");
    }

    void test_invalid_sizes_and_state() {
        const auto invalid = dkv::encode_raft_metadata({0, 0});
        const auto* error = std::get_if<dkv::RaftMetadataCodecError>(&invalid);
        expect(error != nullptr && *error == dkv::RaftMetadataCodecError::InvalidState,
               "vote in term zero was encoded");

        const auto encoded = dkv::encode_raft_metadata({3, 1});
        const auto* bytes = std::get_if<dkv::RaftMetadataRecord>(&encoded);
        if (!bytes) return;
        expect_decode_error(std::span<const std::byte>{*bytes}.first(bytes->size() - 1),
                            dkv::RaftMetadataCodecError::TruncatedRecord,
                            "short record was accepted");
        std::array<std::byte, dkv::raft_metadata_record_size + 1> extra{};
        for (std::size_t i = 0; i < bytes->size(); ++i) extra[i] = (*bytes)[i];
        expect_decode_error(extra, dkv::RaftMetadataCodecError::InvalidLength,
                            "record with extra bytes was accepted");
    }

    void test_corrupt_records() {
        const auto encoded = dkv::encode_raft_metadata({3, 1});
        const auto* original = std::get_if<dkv::RaftMetadataRecord>(&encoded);
        if (!original) return;

        auto bytes = *original;
        bytes[0] = std::byte{'X'};
        expect_decode_error(bytes, dkv::RaftMetadataCodecError::InvalidMagic,
                            "wrong marker was accepted");

        bytes = *original;
        bytes[4] = std::byte{2};
        expect_decode_error(bytes, dkv::RaftMetadataCodecError::UnsupportedVersion,
                            "unknown version was accepted");

        bytes = *original;
        bytes[12] ^= std::byte{1};
        expect_decode_error(bytes, dkv::RaftMetadataCodecError::ChecksumMismatch,
                            "modified term passed checksum validation");

        bytes = *original;
        bytes[13] = std::byte{2};
        update_checksum(bytes);
        expect_decode_error(bytes, dkv::RaftMetadataCodecError::InvalidVote,
                            "invalid vote flag was accepted");

        bytes = *original;
        bytes[13] = std::byte{0};
        update_checksum(bytes);
        expect_decode_error(bytes, dkv::RaftMetadataCodecError::InvalidVote,
                            "absent vote with nonzero candidate was accepted");

        bytes = *original;
        for (std::size_t i = 5; i < 13; ++i) bytes[i] = std::byte{0};
        update_checksum(bytes);
        expect_decode_error(bytes, dkv::RaftMetadataCodecError::InvalidVote,
                            "vote in term zero was decoded");
    }
}

int main() {
    test_round_trip_and_layout();
    test_existing_record_format();
    test_invalid_sizes_and_state();
    test_corrupt_records();
    if (failures != 0) {
        std::cerr << failures << " test check(s) failed\n";
        return 1;
    }
    std::cout << "SUCCESS\n";
}

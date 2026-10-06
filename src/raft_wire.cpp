#include "dkv/raft_wire.hpp"

#include "dkv/crc32.hpp"
#include "dkv/log_entry_codec.hpp"

#include <cstdint>
#include <limits>
#include <utility>

namespace dkv {
    namespace {
        constexpr std::size_t checksum_size = 4;

        void put_u8(std::vector<std::byte>& out, std::uint8_t value) {
            out.push_back(static_cast<std::byte>(value));
        }

        void put_u32(std::vector<std::byte>& out, std::uint32_t value) {
            for (int shift = 24; shift >= 0; shift -= 8) {
                put_u8(out, static_cast<std::uint8_t>(value >> shift));
            }
        }

        void put_u64(std::vector<std::byte>& out, std::uint64_t value) {
            for (int shift = 56; shift >= 0; shift -= 8) {
                put_u8(out, static_cast<std::uint8_t>(value >> shift));
            }
        }

        struct Reader {
            std::span<const std::byte> input;
            std::size_t at{0};

            bool u8(std::uint8_t& value) {
                if (at >= input.size()) return false;
                value = std::to_integer<std::uint8_t>(input[at++]);
                return true;
            }

            bool u32(std::uint32_t& value) {
                if (input.size() - at < 4) return false;
                value = 0;
                for (int i = 0; i < 4; ++i) {
                    value = (value << 8) |
                            std::to_integer<std::uint32_t>(input[at++]);
                }
                return true;
            }

            bool u64(std::uint64_t& value) {
                if (input.size() - at < 8) return false;
                value = 0;
                for (int i = 0; i < 8; ++i) {
                    value = (value << 8) |
                            std::to_integer<std::uint64_t>(input[at++]);
                }
                return true;
            }
        };

        bool put_append(std::vector<std::byte>& out, const AppendEntries& request) {
            if (request.entries.size() > std::numeric_limits<std::uint32_t>::max()) {
                return false;
            }
            put_u64(out, request.term);
            put_u64(out, request.leader_id);
            put_u64(out, request.prev_log_index);
            put_u64(out, request.prev_log_term);
            put_u64(out, request.leader_commit);
            put_u64(out, request.read_context);
            put_u32(out, static_cast<std::uint32_t>(request.entries.size()));
            for (const auto& entry : request.entries) {
                const auto encoded = encode_log_entry(entry);
                const auto* bytes = std::get_if<std::vector<std::byte>>(&encoded);
                if (!bytes || bytes->size() > max_raft_frame_size - 4 - checksum_size ||
                    out.size() > max_raft_frame_size - bytes->size() - 4 - checksum_size) {
                    return false;
                }
                put_u32(out, static_cast<std::uint32_t>(bytes->size()));
                out.insert(out.end(), bytes->begin(), bytes->end());
            }
            return out.size() <= max_raft_frame_size - checksum_size;
        }

        bool read_append(Reader& reader, AppendEntries& request) {
            std::uint32_t count = 0;
            if (!reader.u64(request.term) || !reader.u64(request.leader_id) ||
                !reader.u64(request.prev_log_index) ||
                !reader.u64(request.prev_log_term) ||
                !reader.u64(request.leader_commit) ||
                !reader.u64(request.read_context) || !reader.u32(count)) {
                return false;
            }
            // Each record needs at least a four-byte length prefix.
            if (count > (reader.input.size() - reader.at) / 4) return false;
            request.entries.reserve(count);
            for (std::uint32_t i = 0; i < count; ++i) {
                std::uint32_t length = 0;
                if (!reader.u32(length) || length > reader.input.size() - reader.at) {
                    return false;
                }
                const auto decoded =
                    decode_log_entry(reader.input.subspan(reader.at, length));
                const auto* entry = std::get_if<DecodedLogEntry>(&decoded);
                if (!entry || entry->bytes_consumed != length) return false;
                request.entries.push_back(entry->entry);
                reader.at += length;
            }
            return true;
        }
    }

    EncodeRaftEnvelopeResult encode_raft_envelope(const RaftEnvelope& envelope) {
        if (envelope.sender_id == 0 ||
            (envelope.replied_to.has_value() !=
             std::holds_alternative<AppendEntriesReply>(envelope.message))) {
            return RaftWireError::InvalidEnvelope;
        }
        std::vector<std::byte> out;
        put_u8(out, 3); // wire version
        put_u8(out, static_cast<std::uint8_t>(envelope.message.index() + 1));
        put_u64(out, envelope.sender_id);
        if (const auto* value = std::get_if<RequestVote>(&envelope.message)) {
            put_u64(out, value->term);
            put_u64(out, value->candidate_id);
            put_u64(out, value->last_log_index);
            put_u64(out, value->last_log_term);
        } else if (const auto* value =
                       std::get_if<RequestVoteReply>(&envelope.message)) {
            put_u64(out, value->term);
            put_u8(out, value->vote_granted ? 1 : 0);
        } else if (const auto* value =
                       std::get_if<AppendEntries>(&envelope.message)) {
            if (!put_append(out, *value)) return RaftWireError::TooLarge;
        } else {
            const auto& reply = std::get<AppendEntriesReply>(envelope.message);
            put_u64(out, reply.term);
            put_u8(out, reply.success ? 1 : 0);
            if (!put_append(out, *envelope.replied_to)) return RaftWireError::TooLarge;
        }
        if (out.size() > max_raft_frame_size - checksum_size) {
            return RaftWireError::TooLarge;
        }
        put_u32(out, crc32(out));
        return out;
    }

    DecodeRaftEnvelopeResult decode_raft_envelope(std::span<const std::byte> bytes) {
        if (bytes.size() > max_raft_frame_size) return RaftWireError::TooLarge;
        if (bytes.size() < 10 + checksum_size) return RaftWireError::Truncated;
        if (bytes[0] != std::byte{3}) return RaftWireError::UnsupportedVersion;
        std::uint32_t expected_checksum = 0;
        for (const auto octet : bytes.last(checksum_size)) {
            expected_checksum = (expected_checksum << 8) |
                std::to_integer<std::uint32_t>(octet);
        }
        if (crc32(bytes.first(bytes.size() - checksum_size)) != expected_checksum) {
            return RaftWireError::ChecksumMismatch;
        }
        bytes = bytes.first(bytes.size() - checksum_size);
        Reader reader{bytes};
        std::uint8_t version = 0;
        std::uint8_t kind = 0;
        std::uint64_t sender = 0;
        if (!reader.u8(version) || !reader.u8(kind) || !reader.u64(sender)) {
            return RaftWireError::Truncated;
        }
        if (version != 3) return RaftWireError::UnsupportedVersion;
        if (sender == 0) return RaftWireError::InvalidEnvelope;
        RaftEnvelope envelope;
        envelope.sender_id = sender;
        switch (kind) {
        case 1: {
            RequestVote value;
            if (!reader.u64(value.term) || !reader.u64(value.candidate_id) ||
                !reader.u64(value.last_log_index) || !reader.u64(value.last_log_term)) {
                return RaftWireError::Truncated;
            }
            envelope.message = value;
            break;
        }
        case 2: {
            RequestVoteReply value;
            std::uint8_t granted = 0;
            if (!reader.u64(value.term) || !reader.u8(granted)) {
                return RaftWireError::Truncated;
            }
            if (granted > 1) return RaftWireError::InvalidMessage;
            value.vote_granted = granted == 1;
            envelope.message = value;
            break;
        }
        case 3: {
            AppendEntries value;
            if (!read_append(reader, value)) return RaftWireError::InvalidMessage;
            envelope.message = std::move(value);
            break;
        }
        case 4: {
            AppendEntriesReply value;
            std::uint8_t success = 0;
            if (!reader.u64(value.term) || !reader.u8(success)) {
                return RaftWireError::Truncated;
            }
            if (success > 1) return RaftWireError::InvalidMessage;
            value.success = success == 1;
            AppendEntries replied_to;
            if (!read_append(reader, replied_to)) return RaftWireError::InvalidMessage;
            envelope.message = value;
            envelope.replied_to = std::move(replied_to);
            break;
        }
        default:
            return RaftWireError::InvalidMessage;
        }
        if (reader.at != bytes.size()) return RaftWireError::TrailingBytes;
        return envelope;
    }
}

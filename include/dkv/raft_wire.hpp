#pragma once

#include "dkv/raft_node.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <variant>
#include <vector>

namespace dkv {
    // One TCP frame carries one envelope. AppendEntries replies echo the
    // request so the leader can reject stale or reordered acknowledgments.
    struct RaftEnvelope {
        std::uint64_t sender_id{0};
        RaftMessage message;
        std::optional<AppendEntries> replied_to;

        bool operator==(const RaftEnvelope&) const = default;
    };

    enum class RaftWireError {
        InvalidEnvelope,
        InvalidMessage,
        TooLarge,
        Truncated,
        UnsupportedVersion,
        TrailingBytes,
        ChecksumMismatch
    };

    constexpr std::size_t max_raft_frame_size = 8 * 1024 * 1024;
    using EncodeRaftEnvelopeResult =
        std::variant<std::vector<std::byte>, RaftWireError>;
    using DecodeRaftEnvelopeResult = std::variant<RaftEnvelope, RaftWireError>;

    [[nodiscard]] EncodeRaftEnvelopeResult encode_raft_envelope(
        const RaftEnvelope& envelope);
    [[nodiscard]] DecodeRaftEnvelopeResult decode_raft_envelope(
        std::span<const std::byte> bytes);
}

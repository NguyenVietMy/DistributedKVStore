#pragma once

#include "dkv/command.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <variant>
#include <vector>

namespace dkv {
    // Client frames share the Raft TCP listener but use distinct message kinds.
    constexpr std::size_t max_client_frame_size = 1024 * 1024 + 128;

    struct ClientWriteRequest {
        Command command;
        bool operator==(const ClientWriteRequest&) const = default;
    };

    enum class ClientWriteStatus : std::uint8_t {
        Committed = 1,
        NotLeader = 2,
        OutcomeUnknown = 3,
        InvalidRequest = 4,
        Busy = 5,
        InternalError = 6
    };

    struct ClientWriteReply {
        ClientWriteStatus status{ClientWriteStatus::InternalError};
        std::uint64_t term{0};
        std::uint64_t index{0};
        std::uint64_t leader_hint{0};
        bool operator==(const ClientWriteReply&) const = default;
    };

    enum class ClientWireError {
        InvalidMessage,
        TooLarge,
        Truncated,
        UnsupportedVersion
    };

    using EncodeClientResult = std::variant<std::vector<std::byte>, ClientWireError>;
    using DecodeClientRequestResult =
        std::variant<ClientWriteRequest, ClientWireError>;
    using DecodeClientReplyResult = std::variant<ClientWriteReply, ClientWireError>;

    [[nodiscard]] EncodeClientResult encode_client_request(
        const ClientWriteRequest& request);
    [[nodiscard]] DecodeClientRequestResult decode_client_request(
        std::span<const std::byte> bytes);
    [[nodiscard]] EncodeClientResult encode_client_reply(
        const ClientWriteReply& reply);
    [[nodiscard]] DecodeClientReplyResult decode_client_reply(
        std::span<const std::byte> bytes);
}

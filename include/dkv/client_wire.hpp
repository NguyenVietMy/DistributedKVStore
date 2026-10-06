#pragma once

#include "dkv/command.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <variant>
#include <vector>

namespace dkv {
    // Client frames share the Raft TCP listener but use distinct message kinds.
    constexpr std::size_t max_client_request_size = 1024 * 1024 + 128;
    constexpr std::size_t max_client_frame_size = 2 * 1024 * 1024 + 128;

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

    struct ClientReadRequest {
        std::string key;
        bool operator==(const ClientReadRequest&) const = default;
    };

    enum class ClientReadStatus : std::uint8_t {
        Found = 1,
        NotFound = 2,
        NotLeader = 3,
        Busy = 4,
        InvalidRequest = 5,
        InternalError = 6
    };

    struct ClientReadReply {
        ClientReadStatus status{ClientReadStatus::InternalError};
        std::uint64_t term{0};
        std::uint64_t index{0};
        std::uint64_t leader_hint{0};
        std::optional<std::string> value;
        bool operator==(const ClientReadReply&) const = default;
    };

    struct ClientStatusRequest {
        bool operator==(const ClientStatusRequest&) const = default;
    };

    enum class ClientStatusCode : std::uint8_t {
        Ok = 1,
        InvalidRequest = 2
    };

    enum class ClientNodeRole : std::uint8_t {
        Unknown = 0,
        Follower = 1,
        Candidate = 2,
        Leader = 3
    };

    struct ClientPeerStatus {
        std::uint64_t id{0};
        // Both indices are zero when this node is not tracking the peer.
        std::uint64_t next_index{0};
        std::uint64_t match_index{0};
        bool operator==(const ClientPeerStatus&) const = default;
    };

    struct ClientStatusReply {
        ClientStatusCode status{ClientStatusCode::InvalidRequest};
        std::uint64_t node_id{0};
        ClientNodeRole role{ClientNodeRole::Unknown};
        std::uint64_t term{0};
        std::uint64_t known_leader{0};
        std::uint64_t last_index{0};
        std::uint64_t commit_index{0};
        std::uint64_t last_applied{0};
        std::array<ClientPeerStatus, 2> peers{};
        bool operator==(const ClientStatusReply&) const = default;
    };

    enum class ClientWireError {
        InvalidMessage,
        TooLarge,
        Truncated,
        UnsupportedVersion,
        ChecksumMismatch
    };

    using EncodeClientResult = std::variant<std::vector<std::byte>, ClientWireError>;
    using DecodeClientRequestResult =
        std::variant<ClientWriteRequest, ClientWireError>;
    using DecodeClientReplyResult = std::variant<ClientWriteReply, ClientWireError>;
    using DecodeClientReadRequestResult =
        std::variant<ClientReadRequest, ClientWireError>;
    using DecodeClientReadReplyResult =
        std::variant<ClientReadReply, ClientWireError>;
    using DecodeClientStatusRequestResult =
        std::variant<ClientStatusRequest, ClientWireError>;
    using DecodeClientStatusReplyResult =
        std::variant<ClientStatusReply, ClientWireError>;

    [[nodiscard]] EncodeClientResult encode_client_request(
        const ClientWriteRequest& request);
    [[nodiscard]] DecodeClientRequestResult decode_client_request(
        std::span<const std::byte> bytes);
    [[nodiscard]] EncodeClientResult encode_client_reply(
        const ClientWriteReply& reply);
    [[nodiscard]] DecodeClientReplyResult decode_client_reply(
        std::span<const std::byte> bytes);
    [[nodiscard]] EncodeClientResult encode_client_read_request(
        const ClientReadRequest& request);
    [[nodiscard]] DecodeClientReadRequestResult decode_client_read_request(
        std::span<const std::byte> bytes);
    [[nodiscard]] EncodeClientResult encode_client_read_reply(
        const ClientReadReply& reply);
    [[nodiscard]] DecodeClientReadReplyResult decode_client_read_reply(
        std::span<const std::byte> bytes);
    [[nodiscard]] EncodeClientResult encode_client_status_request(
        const ClientStatusRequest& request);
    [[nodiscard]] DecodeClientStatusRequestResult decode_client_status_request(
        std::span<const std::byte> bytes);
    [[nodiscard]] EncodeClientResult encode_client_status_reply(
        const ClientStatusReply& reply);
    [[nodiscard]] DecodeClientStatusReplyResult decode_client_status_reply(
        std::span<const std::byte> bytes);
}

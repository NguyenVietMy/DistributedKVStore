#include "dkv/client_wire.hpp"

#include "dkv/command_codec.hpp"

#include <cstring>
#include <utility>

namespace dkv {
    namespace {
        constexpr std::byte version{1};
        constexpr std::byte request_kind{5};
        constexpr std::byte reply_kind{6};
        constexpr std::byte read_request_kind{7};
        constexpr std::byte read_reply_kind{8};
        constexpr std::byte status_request_kind{9};
        constexpr std::byte status_reply_kind{10};
        constexpr std::size_t reply_size = 27;
        constexpr std::size_t read_reply_size = 31;
        constexpr std::size_t status_reply_size = 100;

        void put_u32(std::vector<std::byte>& bytes, std::uint32_t value) {
            for (int shift = 24; shift >= 0; shift -= 8) {
                bytes.push_back(static_cast<std::byte>((value >> shift) & 0xff));
            }
        }

        std::uint32_t read_u32(std::span<const std::byte> bytes, std::size_t at) {
            std::uint32_t result = 0;
            for (std::size_t i = 0; i < 4; ++i) {
                result = (result << 8) |
                    std::to_integer<std::uint32_t>(bytes[at + i]);
            }
            return result;
        }

        void put_u64(std::vector<std::byte>& bytes, std::uint64_t value) {
            for (int shift = 56; shift >= 0; shift -= 8) {
                bytes.push_back(static_cast<std::byte>((value >> shift) & 0xff));
            }
        }

        std::uint64_t read_u64(std::span<const std::byte> bytes, std::size_t at) {
            std::uint64_t result = 0;
            for (std::size_t i = 0; i < 8; ++i) {
                result = (result << 8) |
                    std::to_integer<std::uint64_t>(bytes[at + i]);
            }
            return result;
        }

        bool valid_status(std::uint8_t value) {
            return value >= static_cast<std::uint8_t>(ClientWriteStatus::Committed) &&
                   value <= static_cast<std::uint8_t>(ClientWriteStatus::InternalError);
        }

        bool valid_read_status(std::uint8_t value) {
            return value >= static_cast<std::uint8_t>(ClientReadStatus::Found) &&
                   value <= static_cast<std::uint8_t>(ClientReadStatus::InternalError);
        }

        bool valid_status_reply(const ClientStatusReply& reply) {
            if (reply.status == ClientStatusCode::InvalidRequest) {
                return reply.node_id == 0 && reply.role == ClientNodeRole::Unknown &&
                       reply.term == 0 && reply.known_leader == 0 &&
                       reply.last_index == 0 && reply.commit_index == 0 &&
                       reply.last_applied == 0 &&
                       reply.peers == std::array<ClientPeerStatus, 2>{};
            }
            if (reply.status != ClientStatusCode::Ok || reply.node_id == 0 ||
                reply.role < ClientNodeRole::Follower ||
                reply.role > ClientNodeRole::Leader ||
                reply.last_applied > reply.commit_index ||
                reply.commit_index > reply.last_index ||
                reply.peers[0].id == 0 || reply.peers[1].id == 0 ||
                reply.peers[0].id == reply.peers[1].id ||
                reply.peers[0].id == reply.node_id ||
                reply.peers[1].id == reply.node_id) {
                return false;
            }
            if (reply.role == ClientNodeRole::Leader) {
                if (reply.known_leader != reply.node_id) return false;
                for (const auto& peer : reply.peers) {
                    if (peer.next_index == 0 || peer.match_index >= peer.next_index ||
                        peer.match_index > reply.last_index) return false;
                }
            } else {
                for (const auto& peer : reply.peers) {
                    if (peer.next_index != 0 || peer.match_index != 0) return false;
                }
            }
            return true;
        }
    }

    EncodeClientResult encode_client_request(const ClientWriteRequest& request) {
        if (request.command.type == CommandType::NoOp ||
            !request.command.request_id) {
            return ClientWireError::InvalidMessage;
        }
        auto encoded = encode_command(request.command);
        const auto* command = std::get_if<std::vector<std::byte>>(&encoded);
        if (!command) return ClientWireError::InvalidMessage;
        if (command->size() > max_client_request_size - 2) {
            return ClientWireError::TooLarge;
        }
        std::vector<std::byte> bytes{version, request_kind};
        bytes.insert(bytes.end(), command->begin(), command->end());
        return bytes;
    }

    DecodeClientRequestResult decode_client_request(
        std::span<const std::byte> bytes) {
        if (bytes.size() > max_client_request_size) return ClientWireError::TooLarge;
        if (bytes.size() < 2) return ClientWireError::Truncated;
        if (bytes[0] != version) return ClientWireError::UnsupportedVersion;
        if (bytes[1] != request_kind) return ClientWireError::InvalidMessage;
        const auto decoded = decode_command(bytes.subspan(2));
        const auto* command = std::get_if<Command>(&decoded);
        if (!command || command->type == CommandType::NoOp ||
            !command->request_id) {
            return ClientWireError::InvalidMessage;
        }
        return ClientWriteRequest{*command};
    }

    EncodeClientResult encode_client_reply(const ClientWriteReply& reply) {
        if (!valid_status(static_cast<std::uint8_t>(reply.status)) ||
            (reply.status == ClientWriteStatus::Committed && reply.index == 0)) {
            return ClientWireError::InvalidMessage;
        }
        std::vector<std::byte> bytes{version, reply_kind,
            static_cast<std::byte>(reply.status)};
        put_u64(bytes, reply.term);
        put_u64(bytes, reply.index);
        put_u64(bytes, reply.leader_hint);
        return bytes;
    }

    DecodeClientReplyResult decode_client_reply(std::span<const std::byte> bytes) {
        if (bytes.size() < reply_size) return ClientWireError::Truncated;
        if (bytes.size() != reply_size) return ClientWireError::InvalidMessage;
        if (bytes[0] != version) return ClientWireError::UnsupportedVersion;
        if (bytes[1] != reply_kind) return ClientWireError::InvalidMessage;
        const auto status = std::to_integer<std::uint8_t>(bytes[2]);
        if (!valid_status(status)) return ClientWireError::InvalidMessage;
        ClientWriteReply reply{static_cast<ClientWriteStatus>(status),
                               read_u64(bytes, 3), read_u64(bytes, 11),
                               read_u64(bytes, 19)};
        if (reply.status == ClientWriteStatus::Committed && reply.index == 0) {
            return ClientWireError::InvalidMessage;
        }
        return reply;
    }

    EncodeClientResult encode_client_read_request(
        const ClientReadRequest& request) {
        if (request.key.size() > max_client_request_size - 6) {
            return ClientWireError::TooLarge;
        }
        std::vector<std::byte> bytes{version, read_request_kind};
        put_u32(bytes, static_cast<std::uint32_t>(request.key.size()));
        for (const char character : request.key) {
            bytes.push_back(static_cast<std::byte>(
                static_cast<unsigned char>(character)));
        }
        return bytes;
    }

    DecodeClientReadRequestResult decode_client_read_request(
        std::span<const std::byte> bytes) {
        if (bytes.size() > max_client_request_size) return ClientWireError::TooLarge;
        if (bytes.size() < 6) return ClientWireError::Truncated;
        if (bytes[0] != version) return ClientWireError::UnsupportedVersion;
        if (bytes[1] != read_request_kind) return ClientWireError::InvalidMessage;
        const auto length = read_u32(bytes, 2);
        if (length != bytes.size() - 6) return ClientWireError::InvalidMessage;
        std::string key(length, '\0');
        if (length != 0) std::memcpy(key.data(), bytes.data() + 6, length);
        return ClientReadRequest{std::move(key)};
    }

    EncodeClientResult encode_client_read_reply(const ClientReadReply& reply) {
        const auto status = static_cast<std::uint8_t>(reply.status);
        if (!valid_read_status(status) ||
            (reply.status == ClientReadStatus::Found) != reply.value.has_value() ||
            ((reply.status == ClientReadStatus::Found ||
              reply.status == ClientReadStatus::NotFound) && reply.index == 0)) {
            return ClientWireError::InvalidMessage;
        }
        const auto value_size = reply.value ? reply.value->size() : 0;
        if (value_size > max_client_frame_size - read_reply_size) {
            return ClientWireError::TooLarge;
        }
        std::vector<std::byte> bytes{version, read_reply_kind,
                                     static_cast<std::byte>(status)};
        put_u64(bytes, reply.term);
        put_u64(bytes, reply.index);
        put_u64(bytes, reply.leader_hint);
        put_u32(bytes, static_cast<std::uint32_t>(value_size));
        if (reply.value) {
            for (const char character : *reply.value) {
                bytes.push_back(static_cast<std::byte>(
                    static_cast<unsigned char>(character)));
            }
        }
        return bytes;
    }

    DecodeClientReadReplyResult decode_client_read_reply(
        std::span<const std::byte> bytes) {
        if (bytes.size() > max_client_frame_size) return ClientWireError::TooLarge;
        if (bytes.size() < read_reply_size) return ClientWireError::Truncated;
        if (bytes[0] != version) return ClientWireError::UnsupportedVersion;
        if (bytes[1] != read_reply_kind) return ClientWireError::InvalidMessage;
        const auto status_byte = std::to_integer<std::uint8_t>(bytes[2]);
        if (!valid_read_status(status_byte)) return ClientWireError::InvalidMessage;
        const auto status = static_cast<ClientReadStatus>(status_byte);
        const auto length = read_u32(bytes, 27);
        if (length != bytes.size() - read_reply_size ||
            (status != ClientReadStatus::Found && length != 0)) {
            return ClientWireError::InvalidMessage;
        }
        const auto index = read_u64(bytes, 11);
        if ((status == ClientReadStatus::Found ||
             status == ClientReadStatus::NotFound) && index == 0) {
            return ClientWireError::InvalidMessage;
        }
        std::optional<std::string> value;
        if (status == ClientReadStatus::Found) {
            value.emplace(length, '\0');
            if (length != 0) {
                std::memcpy(value->data(), bytes.data() + read_reply_size, length);
            }
        }
        return ClientReadReply{status, read_u64(bytes, 3), index,
                               read_u64(bytes, 19), std::move(value)};
    }

    EncodeClientResult encode_client_status_request(const ClientStatusRequest&) {
        return std::vector<std::byte>{version, status_request_kind};
    }

    DecodeClientStatusRequestResult decode_client_status_request(
        std::span<const std::byte> bytes) {
        if (bytes.size() < 2) return ClientWireError::Truncated;
        if (bytes.size() != 2) return ClientWireError::InvalidMessage;
        if (bytes[0] != version) return ClientWireError::UnsupportedVersion;
        if (bytes[1] != status_request_kind) return ClientWireError::InvalidMessage;
        return ClientStatusRequest{};
    }

    EncodeClientResult encode_client_status_reply(const ClientStatusReply& reply) {
        if (!valid_status_reply(reply)) return ClientWireError::InvalidMessage;
        std::vector<std::byte> bytes{version, status_reply_kind,
                                     static_cast<std::byte>(reply.status),
                                     static_cast<std::byte>(reply.role)};
        put_u64(bytes, reply.node_id);
        put_u64(bytes, reply.term);
        put_u64(bytes, reply.known_leader);
        put_u64(bytes, reply.last_index);
        put_u64(bytes, reply.commit_index);
        put_u64(bytes, reply.last_applied);
        for (const auto& peer : reply.peers) {
            put_u64(bytes, peer.id);
            put_u64(bytes, peer.next_index);
            put_u64(bytes, peer.match_index);
        }
        return bytes;
    }

    DecodeClientStatusReplyResult decode_client_status_reply(
        std::span<const std::byte> bytes) {
        if (bytes.size() < status_reply_size) return ClientWireError::Truncated;
        if (bytes.size() != status_reply_size) return ClientWireError::InvalidMessage;
        if (bytes[0] != version) return ClientWireError::UnsupportedVersion;
        if (bytes[1] != status_reply_kind) return ClientWireError::InvalidMessage;
        ClientStatusReply reply;
        reply.status = static_cast<ClientStatusCode>(
            std::to_integer<std::uint8_t>(bytes[2]));
        reply.role = static_cast<ClientNodeRole>(
            std::to_integer<std::uint8_t>(bytes[3]));
        reply.node_id = read_u64(bytes, 4);
        reply.term = read_u64(bytes, 12);
        reply.known_leader = read_u64(bytes, 20);
        reply.last_index = read_u64(bytes, 28);
        reply.commit_index = read_u64(bytes, 36);
        reply.last_applied = read_u64(bytes, 44);
        for (std::size_t i = 0; i < reply.peers.size(); ++i) {
            const auto at = 52 + i * 24;
            reply.peers[i] = {read_u64(bytes, at), read_u64(bytes, at + 8),
                              read_u64(bytes, at + 16)};
        }
        if (!valid_status_reply(reply)) return ClientWireError::InvalidMessage;
        return reply;
    }
}

#include "dkv/client_wire.hpp"

#include "dkv/command_codec.hpp"

namespace dkv {
    namespace {
        constexpr std::byte version{1};
        constexpr std::byte request_kind{5};
        constexpr std::byte reply_kind{6};
        constexpr std::size_t reply_size = 27;

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
    }

    EncodeClientResult encode_client_request(const ClientWriteRequest& request) {
        if (request.command.type == CommandType::NoOp) {
            return ClientWireError::InvalidMessage;
        }
        auto encoded = encode_command(request.command);
        const auto* command = std::get_if<std::vector<std::byte>>(&encoded);
        if (!command) return ClientWireError::InvalidMessage;
        if (command->size() > max_client_frame_size - 2) {
            return ClientWireError::TooLarge;
        }
        std::vector<std::byte> bytes{version, request_kind};
        bytes.insert(bytes.end(), command->begin(), command->end());
        return bytes;
    }

    DecodeClientRequestResult decode_client_request(
        std::span<const std::byte> bytes) {
        if (bytes.size() > max_client_frame_size) return ClientWireError::TooLarge;
        if (bytes.size() < 2) return ClientWireError::Truncated;
        if (bytes[0] != version) return ClientWireError::UnsupportedVersion;
        if (bytes[1] != request_kind) return ClientWireError::InvalidMessage;
        const auto decoded = decode_command(bytes.subspan(2));
        const auto* command = std::get_if<Command>(&decoded);
        if (!command || command->type == CommandType::NoOp) {
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
}

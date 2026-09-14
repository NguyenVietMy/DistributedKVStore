#pragma once
#include "dkv/command.hpp"
#include <cstddef>
#include <span>
#include <variant>
#include <vector>

namespace dkv {
    enum class CommandCodecError {
        InvalidCommand,
        FieldTooLarge,
        InputTooShort,
        UnsupportedVersion,
        UnknownCommandType,
        TruncatedPayload,
        TrailingBytes
    };
    
    using EncodeCommandResult = std::variant<std::vector<std::byte>, CommandCodecError>;

    using DecodeCommandResult = std::variant<Command, CommandCodecError>;
    [[nodiscard]] EncodeCommandResult encode_command(const Command& command);

    [[nodiscard]] DecodeCommandResult decode_command(std::span<const std::byte> bytes);
}
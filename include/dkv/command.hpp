#pragma once
#include <array>
#include <compare>
#include <cstdint>
#include <optional>
#include <string>
namespace dkv {
    struct RequestId {
        std::array<std::uint8_t, 16> bytes{};
        auto operator<=>(const RequestId&) const = default;
    };

    enum class CommandType {
        Put,
        Delete,
        NoOp
    };
    struct Command {
        CommandType type{CommandType::NoOp};
        std::string key;
        std::string value;
        std::optional<RequestId> request_id;
        bool operator==(const Command&) const = default;
    };
}

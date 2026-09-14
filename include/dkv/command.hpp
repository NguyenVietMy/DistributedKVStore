#pragma once
#include <string>
namespace dkv {
    enum class CommandType {
        Put,
        Delete,
        NoOp
    };
    struct Command {
        CommandType type{CommandType::NoOp};
        std::string key;
        std::string value;
        bool operator==(const Command&) const = default;
    };
}
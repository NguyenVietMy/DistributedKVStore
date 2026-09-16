#pragma once
#include "dkv/command.hpp"
#include <cstdint>
namespace dkv{
    struct LogEntry{
        std::uint64_t index{0};
        std::uint64_t term{0};
        Command command;

        bool operator==(const LogEntry&) const = default;
    };
}
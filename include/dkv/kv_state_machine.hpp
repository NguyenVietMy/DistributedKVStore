#pragma once
#include "dkv/command.hpp"
#include <cstdint>
#include <map>
#include <optional>
#include <string>

namespace dkv {
    enum class ApplyResult{
        Applied,
        KeyNotFound,
        InvalidCommand,
        UnexpectedIndex
    };
    class KvStateMachine{
        public:
            ApplyResult apply(std::uint64_t log_index, const Command& command);
            std::optional<std::string> get(const std::string& key) const;
            std::uint64_t last_applied() const noexcept;
        private:
            std::map<std::string, std::string> data_;
            std::uint64_t last_applied_{0};
    };
}
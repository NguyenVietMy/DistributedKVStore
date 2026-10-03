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
        RequestConflict,
        InvalidCommand,
        UnexpectedIndex
    };
    struct AppliedRequest {
        Command command;
        ApplyResult result;
        std::uint64_t index;
        std::uint64_t term;
    };
    class KvStateMachine{
        public:
            ApplyResult apply(std::uint64_t log_index, const Command& command,
                              std::uint64_t log_term = 0);
            std::optional<std::string> get(const std::string& key) const;
            const AppliedRequest* request_result(const RequestId& id) const;
            std::uint64_t last_applied() const noexcept;
        private:
            std::map<std::string, std::string> data_;
            std::map<RequestId, AppliedRequest> applied_requests_;
            std::uint64_t last_applied_{0};
    };
}

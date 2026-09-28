#pragma once

#include <cstdint>
#include <optional>

namespace dkv {
    struct RaftMetadata {
        std::uint64_t current_term{0};
        std::optional<std::uint64_t> voted_for;

        bool operator==(const RaftMetadata&) const = default;
    };
}

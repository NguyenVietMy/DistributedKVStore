#pragma once
#include <cstddef>
#include <cstdint>
#include <span>

namespace dkv {
    [[nodiscard]] std::uint32_t crc32(std::span<const std::byte> bytes) noexcept;
}
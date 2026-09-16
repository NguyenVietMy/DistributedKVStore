#include "dkv/crc32.hpp"

namespace dkv {
    std::uint32_t crc32(std::span<const std::byte> bytes) noexcept{
        std::uint32_t checksum{0xFFFFFFFFu};
        for(const std::byte byte : bytes){
            checksum ^= std::to_integer<std::uint32_t>(byte);
            for(int bit = 0; bit < 8; ++bit){
                if((checksum & 1u) != 0){
                    checksum = (checksum >> 1) ^ 0xEDB88320u;
                }else{
                    checksum >>= 1;
                }
            }
        }
        return checksum ^ 0xFFFFFFFFu;
    }
}
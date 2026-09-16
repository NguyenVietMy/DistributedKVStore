#include "dkv/log_entry_codec.hpp"
#include "dkv/command_codec.hpp"
#include "dkv/crc32.hpp"
#include <array>
#include <cstdint>
#include <vector>
#include <limits>
#include <variant>

namespace dkv {
    namespace {
        constexpr std::array<std::byte, 4> magic{
            std::byte{0x44},
            std::byte{0x4B},
            std::byte{0x56},
            std::byte{0x4C}
        };
        constexpr std::byte formatVersion{1};
        constexpr std::size_t prefixSize{9};
        constexpr std::size_t entryMetadataSize{16};
        constexpr std::size_t checksumSize{4};

        void append_u32(std::vector<std::byte>& output, std::uint32_t value){
            for(int shift = 24; shift >=0; shift-=8){
                output.push_back(
                    static_cast<std::byte>((value>>shift) & 0xFFu)
                );
            }
        }
        void append_u64(std::vector<std::byte>& output, std::uint64_t value){
            for(int shift = 56; shift >= 0; shift -= 8){
                output.push_back(
                    static_cast<std::byte>((value >> shift) & 0xFFu)
                );
            }
        }
    }

    EncodeLogEntryResult encode_log_entry(const LogEntry& entry){
        if(entry.index == 0 || entry.term == 0){
            return LogEntryCodecError::InvalidEntry;
        }
        const auto encoded_command = encode_command(entry.command);
        const auto* command_bytes = std::get_if<std::vector<std::byte>>(&encoded_command);

        if(command_bytes == nullptr){
            const auto command_error = std::get<CommandCodecError>(encoded_command);
            if(command_error == CommandCodecError::FieldTooLarge){
                return LogEntryCodecError::FieldTooLarge;
            }
            return LogEntryCodecError::InvalidCommand;
        }

        const auto maximum_body_size = static_cast<std::size_t>(
            std::numeric_limits<std::uint32_t>::max()
        );

        if(command_bytes->size() > maximum_body_size - entryMetadataSize){
            return LogEntryCodecError::FieldTooLarge;
        }

        const auto body_size = static_cast<std::uint32_t>(
            entryMetadataSize + command_bytes->size()
        );

        std::vector<std::byte> bytes;
        bytes.reserve(prefixSize + body_size + checksumSize);

        bytes.insert(bytes.end(), magic.begin(), magic.end());
        bytes.push_back(formatVersion);
        append_u32(bytes, body_size);
        append_u64(bytes, entry.index);
        append_u64(bytes, entry.term);
        bytes.insert(bytes.end(), command_bytes->begin(), command_bytes->end());

        const std::uint32_t checksum = crc32(bytes);
        append_u32(bytes, checksum);
        return bytes;
    }
}
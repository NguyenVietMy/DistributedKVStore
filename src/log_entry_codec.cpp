#include "dkv/log_entry_codec.hpp"
#include "dkv/command_codec.hpp"
#include "dkv/crc32.hpp"
#include <array>
#include <cstdint>
#include <vector>
#include <limits>
#include <span>
#include <utility>
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
        constexpr std::size_t minimumEncodedCommandSize{10};

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

        std::uint32_t read_u32(
            std::span<const std::byte> input,
            std::size_t offset
        ){
            std::uint32_t value{0};

            for(std::size_t index = 0; index < 4; ++index){
                value = (value << 8) |
                    std::to_integer<std::uint32_t>(input[offset + index]);
            }

            return value;
        }

        std::uint64_t read_u64(
            std::span<const std::byte> input,
            std::size_t offset
        ){
            std::uint64_t value{0};

            for(std::size_t index = 0; index < 8; ++index){
                value = (value << 8) |
                    std::to_integer<std::uint64_t>(input[offset + index]);
            }

            return value;
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

    DecodeLogEntryResult decode_log_entry(std::span<const std::byte> bytes){
        if(bytes.size() < prefixSize){
            return LogEntryCodecError::InputTooShort;
        }

        for(std::size_t index = 0; index < magic.size(); ++index){
            if(bytes[index] != magic[index]){
                return LogEntryCodecError::InvalidMagic;
            }
        }

        if(bytes[magic.size()] != formatVersion){
            return LogEntryCodecError::UnsupportedVersion;
        }

        const auto body_size = static_cast<std::size_t>(
            read_u32(bytes, magic.size() + 1)
        );
        const std::size_t minimum_body_size =
            entryMetadataSize + minimumEncodedCommandSize;

        if(body_size < minimum_body_size){
            return LogEntryCodecError::InvalidLength;
        }

        const std::size_t bytes_after_prefix = bytes.size() - prefixSize;
        if(body_size > bytes_after_prefix){
            return LogEntryCodecError::TruncatedRecord;
        }

        const std::size_t bytes_after_body = bytes_after_prefix - body_size;
        if(bytes_after_body < checksumSize){
            return LogEntryCodecError::TruncatedRecord;
        }

        const std::size_t checksum_offset = prefixSize + body_size;
        const std::uint32_t stored_checksum =
            read_u32(bytes, checksum_offset);
        const std::uint32_t calculated_checksum =
            crc32(bytes.first(checksum_offset));

        if(stored_checksum != calculated_checksum){
            return LogEntryCodecError::ChecksumMismatch;
        }

        const std::uint64_t entry_index =
            read_u64(bytes, prefixSize);
        const std::uint64_t entry_term =
            read_u64(bytes, prefixSize + sizeof(std::uint64_t));

        if(entry_index == 0 || entry_term == 0){
            return LogEntryCodecError::InvalidEntry;
        }

        const std::size_t command_offset =
            prefixSize + entryMetadataSize;
        const std::size_t command_size =
            body_size - entryMetadataSize;
        const auto decoded_command = decode_command(
            bytes.subspan(command_offset, command_size)
        );
        const auto* command = std::get_if<Command>(&decoded_command);

        if(command == nullptr){
            return LogEntryCodecError::InvalidCommand;
        }

        const std::size_t record_size =
            checksum_offset + checksumSize;

        return DecodedLogEntry{
            LogEntry{
                entry_index,
                entry_term,
                *command
            },
            record_size
        };
    }
}
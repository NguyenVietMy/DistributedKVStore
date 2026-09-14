#include "dkv/command_codec.hpp"
#include <cstdint>
#include <limits>
#include <cstring>
#include <utility>

namespace dkv {
    namespace{
        constexpr std::byte formatVersion{1};
        constexpr std::byte putTag{1};
        constexpr std::byte deleteTag{2};
        constexpr std::byte noOpTag{3};
        constexpr std::size_t headerSize{10};

        void append_u32(std::vector<std::byte>& output, std::uint32_t value){
            output.push_back(
                static_cast<std::byte>((value >> 24) & 0xFFu)
            );
            output.push_back(
                static_cast<std::byte>((value >> 16) & 0xFFu)
            );
            output.push_back(
                static_cast<std::byte>((value >> 8) & 0xFFu)
            );
            output.push_back(
                static_cast<std::byte>(value & 0xFFu)
            );
        }

        std::uint32_t read_u32(std::span<const std::byte> input, std::size_t offset){
            std::uint32_t byte0 = std::to_integer<std::uint32_t>(input[offset]);
            std::uint32_t byte1 = std::to_integer<std::uint32_t>(input[offset + 1]);
            std::uint32_t byte2 = std::to_integer<std::uint32_t>(input[offset + 2]);
            std::uint32_t byte3 = std::to_integer<std::uint32_t>(input[offset + 3]);

            return (byte0 << 24) | (byte1 << 16) | (byte2 << 8) | byte3;
        }


        void append_string(std::vector<std::byte>& output, const std::string& input){
            for(char c: input){
                output.push_back(static_cast<std::byte>(static_cast<unsigned char>(c)));
            }
        }
        std::string read_string(std::span<const std::byte> input, std::size_t offset, std::size_t length){
            std::string result(length, '\0');
            if(length != 0){
                std::memcpy(
                    result.data(),
                    input.data() + offset,
                    length);
            }      
            return result;
        }

    }
    EncodeCommandResult encode_command(const Command& command){
        std::byte type_byte{};
        switch(command.type){
            case CommandType::Put:
                type_byte = putTag;
                break;
            case CommandType::Delete:
                if(!command.value.empty()){
                    return CommandCodecError::InvalidCommand;
                }
                type_byte = deleteTag;
                break;
            case CommandType::NoOp:
                if(!command.value.empty() || !command.key.empty()){
                    return CommandCodecError::InvalidCommand;
                }
                type_byte = noOpTag;
                break;
            default:
                return CommandCodecError::InvalidCommand;
        }
        const auto maximum_field_size = static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max());
        if(command.key.size() > maximum_field_size || command.value.size() > maximum_field_size){
            return CommandCodecError::FieldTooLarge;
        }
        const auto key_size = static_cast<std::uint32_t>(command.key.size());
        const auto value_size = static_cast<std::uint32_t>(command.value.size());

        std::vector<std::byte> bytes;
        bytes.reserve(headerSize + key_size + value_size);
        bytes.push_back(formatVersion);
        bytes.push_back(type_byte);
        append_u32(bytes, key_size);
        append_u32(bytes, value_size);
        append_string(bytes, command.key);
        append_string(bytes, command.value);
        return bytes;
    }
    
    DecodeCommandResult decode_command(std::span<const std::byte> bytes){
        if(bytes.size() < headerSize){
            return CommandCodecError::InputTooShort;
        }
        if(bytes[0] != formatVersion){
            return CommandCodecError::UnsupportedVersion;
        }
        CommandType command_type{CommandType::NoOp};
        
        if(bytes[1] == putTag){
            command_type = CommandType::Put;
        }else if(bytes[1] == deleteTag){
            command_type = CommandType::Delete;
        }else if(bytes[1] == noOpTag){
            command_type = CommandType::NoOp;
        }else{
            return CommandCodecError::UnknownCommandType;
        }

        const auto key_size = static_cast<std::size_t>(read_u32(bytes, 2));
        const auto value_size = static_cast<std::size_t>(read_u32(bytes, 6));
        const auto payload_size = bytes.size() - headerSize;

        if(key_size > payload_size){
            return CommandCodecError::TruncatedPayload;
        }
        if(value_size > payload_size - key_size){
            return CommandCodecError::TruncatedPayload;
        }
        if(value_size < payload_size - key_size){
            return CommandCodecError::TrailingBytes;
        }
        std::size_t key_offset = headerSize;
        std::size_t value_offset = headerSize + key_size;
        std::string key = read_string(bytes, key_offset, key_size);
        std::string value = read_string(bytes, value_offset, value_size);
        if(command_type == CommandType::Delete && !value.empty()){
            return CommandCodecError::InvalidCommand;
        }else if(command_type == CommandType::NoOp && (!value.empty() || !key.empty())){
            return CommandCodecError::InvalidCommand;
        }
        return Command{
            command_type,
            std::move(key),
            std::move(value)
        };
    }

}
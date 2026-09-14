#include "dkv/command_codec.hpp"
#include <cstdint>
#include <limits>

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

        void append_string(std::vector<std::byte>& output, const std::string& input){
            for(char c: input){
                output.push_back(static_cast<std::byte>(static_cast<unsigned char>(c)));
            }
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
}
#include "dkv/command_codec.cpp"
#include <iostream>

namespace{
    const dkv::Command command{
        dkv::CommandType::Put,
        "a",
        "bc",
    };
    std::vector<std::byte> bytes = dkv::encode_command(command);
}
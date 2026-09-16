#include "dkv/crc32.hpp"
#include <iostream>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace {
    int failures = 0;
    void expect(bool condition, std::string_view message){
        if(!condition){
            std::cerr << "FAIL: " << message << "\n";
            ++failures;
        }
    }

    void test_empty_input(){
        const std::vector<std::byte> bytes;
        expect(dkv::crc32(bytes) == 0x00000000u, "CRC-32 of empty input is incorrect");
    }

    void test_standard_check_value(){
        const std::string input{"123456789"};
        const auto bytes = std::as_bytes(std::span{input});
        expect(
            dkv::crc32(bytes) == 0xCBF43926u,
            "CRC-32 standard check value is incorrect"
        );
    }
}

int main(){
    test_empty_input();
    test_standard_check_value();

    if(failures != 0){
        std::cerr << failures << " test check(s) failed\n";
        return 1;
    }

    std::cout << "SUCCESS\n";
    return 0;
}
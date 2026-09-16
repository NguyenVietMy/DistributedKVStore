#include "dkv/log_entry_codec.hpp"

#include <cstddef>
#include <iostream>
#include <string_view>
#include <variant>
#include <vector>

namespace {

    int failures = 0;

    void expect(bool condition, std::string_view message){
        if(!condition){
            std::cerr << "FAIL: " << message << '\n';
            ++failures;
        }
    }

    void expect_encoded_entry(
        const dkv::LogEntry& entry,
        const std::vector<std::byte>& expected_bytes,
        std::string_view encoding_error_message,
        std::string_view byte_mismatch_message
    ){
        const auto result = dkv::encode_log_entry(entry);
        const auto* actual_bytes = std::get_if<std::vector<std::byte>>(&result);

        expect(actual_bytes != nullptr, encoding_error_message);
        if(actual_bytes == nullptr){
            return;
        }

        expect(*actual_bytes == expected_bytes, byte_mismatch_message);
    }

    void expect_encode_error(
        const dkv::LogEntry& entry,
        dkv::LogEntryCodecError expected_error,
        std::string_view message
    ){
        const auto result = dkv::encode_log_entry(entry);
        const auto* error = std::get_if<dkv::LogEntryCodecError>(&result);

        expect(error != nullptr, "encoding invalid log entry returned bytes");
        if(error == nullptr){
            return;
        }

        expect(*error == expected_error, message);
    }

    void test_put_encoding(){
        const dkv::LogEntry entry{
            1,
            1,
            dkv::Command{dkv::CommandType::Put, "a", "bc"}
        };
        const std::vector<std::byte> expected_bytes{
            std::byte{0x44}, std::byte{0x4B}, std::byte{0x56}, std::byte{0x4C},
            std::byte{0x01},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x1D},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x01},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x01},
            std::byte{0x01}, std::byte{0x01},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x01},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x02},
            std::byte{0x61}, std::byte{0x62}, std::byte{0x63},
            std::byte{0xD1}, std::byte{0x5E}, std::byte{0xEE}, std::byte{0x38}
        };

        expect_encoded_entry(
            entry,
            expected_bytes,
            "encoding Put log entry returned an error",
            "encoded Put log entry bytes do not match"
        );
    }

    void test_delete_encoding(){
        const dkv::LogEntry entry{
            1,
            1,
            dkv::Command{dkv::CommandType::Delete, "a", ""}
        };
        const std::vector<std::byte> expected_bytes{
            std::byte{0x44}, std::byte{0x4B}, std::byte{0x56}, std::byte{0x4C},
            std::byte{0x01},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x1B},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x01},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x01},
            std::byte{0x01}, std::byte{0x02},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x01},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
            std::byte{0x61},
            std::byte{0xE5}, std::byte{0x29}, std::byte{0x7F}, std::byte{0x0F}
        };

        expect_encoded_entry(
            entry,
            expected_bytes,
            "encoding Delete log entry returned an error",
            "encoded Delete log entry bytes do not match"
        );
    }

    void test_no_op_encoding(){
        const dkv::LogEntry entry{
            1,
            1,
            dkv::Command{}
        };
        const std::vector<std::byte> expected_bytes{
            std::byte{0x44}, std::byte{0x4B}, std::byte{0x56}, std::byte{0x4C},
            std::byte{0x01},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x1A},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x01},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x01},
            std::byte{0x01}, std::byte{0x03},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
            std::byte{0xB7}, std::byte{0x3C}, std::byte{0x54}, std::byte{0x38}
        };

        expect_encoded_entry(
            entry,
            expected_bytes,
            "encoding NoOp log entry returned an error",
            "encoded NoOp log entry bytes do not match"
        );
    }

    void test_invalid_index_and_term(){
        const dkv::LogEntry zero_index{
            0,
            1,
            dkv::Command{}
        };
        const dkv::LogEntry zero_term{
            1,
            0,
            dkv::Command{}
        };

        expect_encode_error(
            zero_index,
            dkv::LogEntryCodecError::InvalidEntry,
            "zero index did not report InvalidEntry"
        );
        expect_encode_error(
            zero_term,
            dkv::LogEntryCodecError::InvalidEntry,
            "zero term did not report InvalidEntry"
        );
    }

    void test_invalid_commands(){
        const dkv::LogEntry invalid_delete{
            1,
            1,
            dkv::Command{dkv::CommandType::Delete, "a", "value"}
        };
        const dkv::LogEntry invalid_no_op{
            1,
            1,
            dkv::Command{dkv::CommandType::NoOp, "a", ""}
        };

        expect_encode_error(
            invalid_delete,
            dkv::LogEntryCodecError::InvalidCommand,
            "invalid Delete did not report InvalidCommand"
        );
        expect_encode_error(
            invalid_no_op,
            dkv::LogEntryCodecError::InvalidCommand,
            "invalid NoOp did not report InvalidCommand"
        );
    }

}

int main(){
    test_put_encoding();
    test_delete_encoding();
    test_no_op_encoding();
    test_invalid_index_and_term();
    test_invalid_commands();

    if(failures != 0){
        std::cerr << failures << " test check(s) failed\n";
        return 1;
    }

    std::cout << "SUCCESS\n";
    return 0;
}

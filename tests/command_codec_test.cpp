#include "dkv/command_codec.hpp"

#include <cstddef>
#include <iostream>
#include <string>
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

    void expect_encoded_bytes(
        const dkv::Command& command,
        const std::vector<std::byte>& expected_bytes,
        std::string_view encoding_error_message,
        std::string_view byte_mismatch_message
    ){
        const auto result = dkv::encode_command(command);
        const auto* actual_bytes = std::get_if<std::vector<std::byte>>(&result);

        expect(actual_bytes != nullptr, encoding_error_message);
        if(actual_bytes == nullptr){
            return;
        }

        expect(*actual_bytes == expected_bytes, byte_mismatch_message);
    }

    void expect_invalid_command(const dkv::Command& command, std::string_view message){
        const auto result = dkv::encode_command(command);
        const auto* error = std::get_if<dkv::CommandCodecError>(&result);

        expect(
            error != nullptr && *error == dkv::CommandCodecError::InvalidCommand,
            message
        );
    }

    void expect_decode_error(
        const std::vector<std::byte>& bytes,
        dkv::CommandCodecError expected_error,
        std::string_view message
    ){
        const auto result = dkv::decode_command(bytes);
        const auto* error = std::get_if<dkv::CommandCodecError>(&result);

        expect(error != nullptr, "decoding invalid bytes returned a command");
        if(error == nullptr){
            return;
        }

        expect(*error == expected_error, message);
    }

    void test_put_encoding(){
        const dkv::Command command{
            dkv::CommandType::Put,
            "a",
            "bc"
        };
        const std::vector<std::byte> expected_bytes{
            std::byte{0x01}, std::byte{0x01},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x01},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x02},
            std::byte{0x61}, std::byte{0x62}, std::byte{0x63}
        };

        expect_encoded_bytes(
            command,
            expected_bytes,
            "encoding Put returned an error",
            "encoded Put bytes do not match"
        );
    }

    void test_delete_encoding(){
        const dkv::Command command{
            dkv::CommandType::Delete,
            "a",
            ""
        };
        const std::vector<std::byte> expected_bytes{
            std::byte{0x01}, std::byte{0x02},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x01},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
            std::byte{0x61}
        };

        expect_encoded_bytes(
            command,
            expected_bytes,
            "encoding Delete returned an error",
            "encoded Delete bytes do not match"
        );
    }

    void test_no_op_encoding(){
        const dkv::Command command{};
        const std::vector<std::byte> expected_bytes{
            std::byte{0x01}, std::byte{0x03},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00}
        };

        expect_encoded_bytes(
            command,
            expected_bytes,
            "encoding NoOp returned an error",
            "encoded NoOp bytes do not match"
        );
    }

    void test_invalid_commands(){
        const dkv::Command invalid_delete{
            dkv::CommandType::Delete,
            "a",
            "bc"
        };
        const dkv::Command no_op_with_key_and_value{
            dkv::CommandType::NoOp,
            "a",
            "a"
        };
        const dkv::Command no_op_with_key{
            dkv::CommandType::NoOp,
            "a",
            ""
        };
        const dkv::Command no_op_with_value{
            dkv::CommandType::NoOp,
            "",
            "a"
        };

        expect_invalid_command(
            invalid_delete,
            "Delete with a value should return InvalidCommand"
        );
        expect_invalid_command(
            no_op_with_key_and_value,
            "NoOp with a key and value should return InvalidCommand"
        );
        expect_invalid_command(
            no_op_with_key,
            "NoOp with a key should return InvalidCommand"
        );
        expect_invalid_command(
            no_op_with_value,
            "NoOp with a value should return InvalidCommand"
        );
    }

    void test_embedded_null_bytes(){
        const std::string binary_key{"a\0b", 3};
        const std::string binary_value{"c\0d", 3};
        const dkv::Command command{
            dkv::CommandType::Put,
            binary_key,
            binary_value
        };
        const std::vector<std::byte> expected_bytes{
            std::byte{0x01}, std::byte{0x01},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x03},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x03},
            std::byte{0x61}, std::byte{0x00}, std::byte{0x62},
            std::byte{0x63}, std::byte{0x00}, std::byte{0x64}
        };

        expect_encoded_bytes(
            command,
            expected_bytes,
            "encoding binary strings returned an error",
            "embedded null bytes were not preserved"
        );
    }
    void test_put_decoding(){
            const std::vector<std::byte> bytes{
                std::byte{0x01}, std::byte{0x01},
                std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x01},
                std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x02},
                std::byte{0x61}, std::byte{0x62}, std::byte{0x63}
            };
            const dkv::Command expected_command{
                dkv::CommandType::Put,
                "a",
                "bc"
            };

            const auto result = dkv::decode_command(bytes);
            const auto* command = std::get_if<dkv::Command>(&result);
            expect(command != nullptr, "decoding valid Put returned an error");
            if(command == nullptr){
                return;
            }
            
            expect(*command == expected_command, "decoded Put does not match");
    }
    void test_delete_decoding(){
        const std::vector<std::byte> bytes{
            std::byte{0x01}, std::byte{0x02}, 
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x01}, 
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, 
            std::byte{0x61}
        };
        const dkv::Command expected_command{
            dkv::CommandType::Delete,
            "a",
            ""
        };

        const auto result = dkv::decode_command(bytes);
        const auto* command = std::get_if<dkv::Command>(&result);
        expect(command != nullptr, "decoding valid Delete returned an error");
        if(command == nullptr){
            return;
        }

        expect(*command == expected_command, "decoded Delete does not match");
    }

    void test_noop_decoding(){
        const std::vector<std::byte> bytes{
            std::byte{0x01}, std::byte{0x03},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, 
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00}
        };

        const dkv::Command expected_command{
            dkv::CommandType::NoOp,
            "",
            ""
        };

        const auto result = dkv::decode_command(bytes);
        const auto* command = std::get_if<dkv::Command>(&result);

        expect(command != nullptr, "decoding valid NoOp returned an error");
        if(command == nullptr){
            return;
        }
        expect(*command == expected_command, "decoded NoOp does not match");

    }

    void test_wrong_format_version(){
        const std::vector<std::byte> bytes{
            std::byte{0x02}, std::byte{0x03},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, 
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00}
        };
        expect_decode_error(
            bytes,
            dkv::CommandCodecError::UnsupportedVersion,
            "wrong version did not report UnsupportedVersion"
        );
    }
    
    void test_short_input(){
        const std::vector<std::byte> bytes{
            std::byte{0x01}, std::byte{0x03},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, 
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
        };
        expect_decode_error(
            bytes,
            dkv::CommandCodecError::InputTooShort,
            "short input did not report InputTooShort"
        );
    }
    void test_unknown_command_type(){
        const std::vector<std::byte> bytes{
            std::byte{0x01}, std::byte{0x04},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, 
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00} 
        };
        expect_decode_error(
            bytes,
            dkv::CommandCodecError::UnknownCommandType,
            "unknown command type did not report UnknownCommandType"
        );
    }

    void test_truncated_payload(){
        const std::vector<std::byte> bytes{
            std::byte{0x01}, std::byte{0x01},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x01}, 
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
        };
        expect_decode_error(
            bytes,
            dkv::CommandCodecError::TruncatedPayload,
            "truncated payload did not report TruncatedPayload"
        );
    }

    void test_trailing_bytes(){
        const std::vector<std::byte> bytes{
            std::byte{0x01}, std::byte{0x01},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, 
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
            std::byte{0x00}
        };
        expect_decode_error(
            bytes,
            dkv::CommandCodecError::TrailingBytes,
            "trailing bytes did not report TrailingBytes"
        );
    }

    void test_invalid_delete_decode(){
        const std::vector<std::byte> bytes{
            std::byte{0x01}, std::byte{0x02},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, 
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x01},
            std::byte{0x61}
        };
        expect_decode_error(
            bytes,
            dkv::CommandCodecError::InvalidCommand,
            "invalid Delete did not report InvalidCommand"
        );
    }
    void test_invalid_noop_decode(){
        const std::vector<std::byte> bytes{
            std::byte{0x01}, std::byte{0x03},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, 
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x01},
            std::byte{0x61}
        };
        expect_decode_error(
            bytes,
            dkv::CommandCodecError::InvalidCommand,
            "invalid NoOp did not report InvalidCommand"
        );
    }
}

int main(){
    test_trailing_bytes();
    test_invalid_delete_decode();
    test_invalid_noop_decode();
    test_unknown_command_type();
    test_truncated_payload();
    test_short_input();
    test_wrong_format_version();
    test_noop_decoding();
    test_put_encoding();
    test_delete_encoding();
    test_no_op_encoding();
    test_invalid_commands();
    test_embedded_null_bytes();
    test_put_decoding();
    test_delete_decoding();
    if(failures != 0){
        std::cerr << failures << " test check(s) failed\n";
        return 1;
    }

    std::cout << "SUCCESS\n";
    return 0;
}

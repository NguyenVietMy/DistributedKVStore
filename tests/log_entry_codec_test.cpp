#include "dkv/log_entry_codec.hpp"
#include "dkv/crc32.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <span>
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

    std::vector<std::byte> encode_for_decoding(const dkv::LogEntry& entry){
        const auto result = dkv::encode_log_entry(entry);
        const auto* bytes = std::get_if<std::vector<std::byte>>(&result);

        expect(bytes != nullptr, "could not encode decoder test entry");
        if(bytes == nullptr){
            return {};
        }

        return *bytes;
    }

    void expect_decoded_entry(
        std::span<const std::byte> bytes,
        const dkv::LogEntry& expected_entry,
        std::size_t expected_bytes_consumed,
        std::string_view message
    ){
        const auto result = dkv::decode_log_entry(bytes);
        const auto* decoded = std::get_if<dkv::DecodedLogEntry>(&result);

        expect(decoded != nullptr, "decoding valid log entry returned an error");
        if(decoded == nullptr){
            return;
        }

        expect(decoded->entry == expected_entry, message);
        expect(
            decoded->bytes_consumed == expected_bytes_consumed,
            "decoded log entry reported incorrect bytes_consumed"
        );
    }

    void expect_decode_error(
        std::span<const std::byte> bytes,
        dkv::LogEntryCodecError expected_error,
        std::string_view message
    ){
        const auto result = dkv::decode_log_entry(bytes);
        const auto* error = std::get_if<dkv::LogEntryCodecError>(&result);

        expect(error != nullptr, "decoding invalid log record returned an entry");
        if(error == nullptr){
            return;
        }

        expect(*error == expected_error, message);
    }

    void write_u32(
        std::vector<std::byte>& bytes,
        std::size_t offset,
        std::uint32_t value
    ){
        for(int index = 0; index < 4; ++index){
            const int shift = 24 - (index * 8);
            bytes[offset + static_cast<std::size_t>(index)] =
                static_cast<std::byte>((value >> shift) & 0xFFu);
        }
    }

    void refresh_checksum(std::vector<std::byte>& bytes){
        const std::size_t checksum_offset = bytes.size() - 4;
        const std::span<const std::byte> protected_bytes{
            bytes.data(),
            checksum_offset
        };
        const std::uint32_t checksum = dkv::crc32(protected_bytes);

        write_u32(bytes, checksum_offset, checksum);
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

    void test_valid_decoding(){
        const std::array<dkv::LogEntry, 3> entries{
            dkv::LogEntry{
                1,
                2,
                dkv::Command{dkv::CommandType::Put, "a", "bc"}
            },
            dkv::LogEntry{
                2,
                2,
                dkv::Command{dkv::CommandType::Delete, "a", ""}
            },
            dkv::LogEntry{
                3,
                2,
                dkv::Command{}
            }
        };

        for(const dkv::LogEntry& entry : entries){
            const auto bytes = encode_for_decoding(entry);
            if(bytes.empty()){
                return;
            }

            expect_decoded_entry(
                bytes,
                entry,
                bytes.size(),
                "decoded log entry does not match original entry"
            );
        }
    }

    void test_embedded_null_decoding(){
        const dkv::LogEntry entry{
            4,
            2,
            dkv::Command{
                dkv::CommandType::Put,
                std::string{"a\0b", 3},
                std::string{"c\0d", 3}
            }
        };
        const auto bytes = encode_for_decoding(entry);
        if(bytes.empty()){
            return;
        }

        expect_decoded_entry(
            bytes,
            entry,
            bytes.size(),
            "decoder did not preserve embedded null bytes"
        );
    }

    void test_concatenated_records(){
        const dkv::LogEntry first_entry{
            1,
            1,
            dkv::Command{dkv::CommandType::Put, "a", "bc"}
        };
        const dkv::LogEntry second_entry{
            2,
            1,
            dkv::Command{dkv::CommandType::Delete, "a", ""}
        };
        const auto first_bytes = encode_for_decoding(first_entry);
        const auto second_bytes = encode_for_decoding(second_entry);
        if(first_bytes.empty() || second_bytes.empty()){
            return;
        }

        std::vector<std::byte> records = first_bytes;
        records.insert(
            records.end(),
            second_bytes.begin(),
            second_bytes.end()
        );

        const auto first_result = dkv::decode_log_entry(records);
        const auto* first =
            std::get_if<dkv::DecodedLogEntry>(&first_result);

        expect(first != nullptr, "decoder rejected first concatenated record");
        if(first == nullptr){
            return;
        }

        expect(first->entry == first_entry, "decoded first record is incorrect");
        expect(
            first->bytes_consumed == first_bytes.size(),
            "first concatenated record consumed incorrect byte count"
        );

        const std::span<const std::byte> all_bytes{records};
        const auto remaining = all_bytes.subspan(first->bytes_consumed);

        expect_decoded_entry(
            remaining,
            second_entry,
            second_bytes.size(),
            "decoded second record is incorrect"
        );
    }

    void test_input_too_short(){
        const std::vector<std::byte> bytes;

        expect_decode_error(
            bytes,
            dkv::LogEntryCodecError::InputTooShort,
            "short record did not report InputTooShort"
        );
    }

    void test_invalid_magic(){
        const dkv::LogEntry entry{1, 1, dkv::Command{}};
        auto bytes = encode_for_decoding(entry);
        if(bytes.empty()){
            return;
        }
        bytes[0] = std::byte{0x00};

        expect_decode_error(
            bytes,
            dkv::LogEntryCodecError::InvalidMagic,
            "invalid magic did not report InvalidMagic"
        );
    }

    void test_unsupported_version(){
        const dkv::LogEntry entry{1, 1, dkv::Command{}};
        auto bytes = encode_for_decoding(entry);
        if(bytes.empty()){
            return;
        }
        bytes[4] = std::byte{0x02};

        expect_decode_error(
            bytes,
            dkv::LogEntryCodecError::UnsupportedVersion,
            "unsupported version did not report UnsupportedVersion"
        );
    }

    void test_invalid_length(){
        const dkv::LogEntry entry{1, 1, dkv::Command{}};
        auto bytes = encode_for_decoding(entry);
        if(bytes.empty()){
            return;
        }
        write_u32(bytes, 5, 25);

        expect_decode_error(
            bytes,
            dkv::LogEntryCodecError::InvalidLength,
            "undersized body did not report InvalidLength"
        );
    }

    void test_truncated_record(){
        const dkv::LogEntry entry{1, 1, dkv::Command{}};
        auto bytes = encode_for_decoding(entry);
        if(bytes.empty()){
            return;
        }
        bytes.pop_back();

        expect_decode_error(
            bytes,
            dkv::LogEntryCodecError::TruncatedRecord,
            "truncated record did not report TruncatedRecord"
        );
    }

    void test_checksum_mismatch(){
        const dkv::LogEntry entry{
            1,
            1,
            dkv::Command{dkv::CommandType::Put, "a", "bc"}
        };
        auto bytes = encode_for_decoding(entry);
        if(bytes.empty()){
            return;
        }
        bytes[35] = std::byte{0x62};

        expect_decode_error(
            bytes,
            dkv::LogEntryCodecError::ChecksumMismatch,
            "corrupted record did not report ChecksumMismatch"
        );
    }

    void test_invalid_entry_decoding(){
        const dkv::LogEntry entry{1, 1, dkv::Command{}};
        auto zero_index = encode_for_decoding(entry);
        auto zero_term = encode_for_decoding(entry);
        if(zero_index.empty() || zero_term.empty()){
            return;
        }

        zero_index[16] = std::byte{0x00};
        refresh_checksum(zero_index);
        zero_term[24] = std::byte{0x00};
        refresh_checksum(zero_term);

        expect_decode_error(
            zero_index,
            dkv::LogEntryCodecError::InvalidEntry,
            "zero decoded index did not report InvalidEntry"
        );
        expect_decode_error(
            zero_term,
            dkv::LogEntryCodecError::InvalidEntry,
            "zero decoded term did not report InvalidEntry"
        );
    }

    void test_invalid_command_decoding(){
        const dkv::LogEntry entry{
            1,
            1,
            dkv::Command{dkv::CommandType::Put, "a", "bc"}
        };
        auto bytes = encode_for_decoding(entry);
        if(bytes.empty()){
            return;
        }

        bytes[26] = std::byte{0x04};
        refresh_checksum(bytes);

        expect_decode_error(
            bytes,
            dkv::LogEntryCodecError::InvalidCommand,
            "invalid nested command did not report InvalidCommand"
        );
    }

}

int main(){
    test_put_encoding();
    test_delete_encoding();
    test_no_op_encoding();
    test_invalid_index_and_term();
    test_invalid_commands();
    test_valid_decoding();
    test_embedded_null_decoding();
    test_concatenated_records();
    test_input_too_short();
    test_invalid_magic();
    test_unsupported_version();
    test_invalid_length();
    test_truncated_record();
    test_checksum_mismatch();
    test_invalid_entry_decoding();
    test_invalid_command_decoding();

    if(failures != 0){
        std::cerr << failures << " test check(s) failed\n";
        return 1;
    }

    std::cout << "SUCCESS\n";
    return 0;
}

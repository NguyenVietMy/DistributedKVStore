#include "dkv/persistent_log.hpp"
#include <iostream>
#include <string_view>
#include <unistd.h>
#include <filesystem>
#include <memory>
#include <string>
#include <variant>
#include <fstream>
#include "dkv/log_entry_codec.hpp"
#include <cstddef>
#include <vector>

namespace {
    int failures = 0;
    void expect(bool condition, std::string_view message){
        if(!condition){
            std::cerr << "FAIL: " << message << "\n";
            ++failures;
        }
    }

    void test_open_missing_file_returns_empty_log(){
        std::string unique_filename = "dkv-missing-" + std::to_string(::getpid()) + ".log";
        auto temp_path = std::filesystem::temp_directory_path() / unique_filename;
        std::filesystem::remove(temp_path);
        auto result = dkv::PersistentLog::open(temp_path);
        auto* pointer = std::get_if<std::unique_ptr<dkv::PersistentLog>>(&result);
        expect(pointer != nullptr && *pointer != nullptr, "opening missing file returned an error");

        if(pointer == nullptr || *pointer == nullptr){
            std::filesystem::remove(temp_path);
            return;
        }
        expect((*pointer)->last_index() == 0, "empty log had nonzero last index");
        expect((*pointer)->last_term() == 0, "empty log had nonzero last term");
        expect(!(*pointer)->entry_at(1).has_value(), "empty log returned an entry at index 1");
        pointer->reset();
        std::filesystem::remove(temp_path);
    }

    void test_open_empty_file_returns_empty_log(){
        std::string unique_filename = "dkv-empty-" + std::to_string(::getpid()) + ".log";
        const auto temp_path = std::filesystem::temp_directory_path() / unique_filename;
        std::filesystem::remove(temp_path);

        std::ofstream empty_file(
            temp_path,
            std::ios::binary | std::ios::trunc
        );
        expect(empty_file.is_open(), "could not create empty test file");
        if(!empty_file.is_open()){
            std::filesystem::remove(temp_path);
            return;
        }
        empty_file.close();

        auto result = dkv::PersistentLog::open(temp_path);
        auto* pointer = std::get_if<std::unique_ptr<dkv::PersistentLog>>(&result);
        expect(pointer != nullptr && *pointer != nullptr, "opening empty file returned an error");

        if(pointer == nullptr || *pointer == nullptr){
            std::filesystem::remove(temp_path);
            return;
        }
        expect((*pointer)->last_index() == 0, "empty log had nonzero last index");
        expect((*pointer)->last_term() == 0, "empty log had nonzero last term");
        expect(!(*pointer)->entry_at(1).has_value(), "empty log returned an entry at index 1");
        pointer->reset();
        std::filesystem::remove(temp_path);
    }

    void test_open_valid_file_recovers_entry(){
        std::string unique_filename = "dkv-valid-" + std::to_string(::getpid()) + ".log";
        const auto temp_path = std::filesystem::temp_directory_path() / unique_filename;
        std::filesystem::remove(temp_path);
        const dkv::LogEntry entry{
            1,
            7,
            dkv::Command{dkv::CommandType::Put, "name","alice"}
        };

        auto encoded = dkv::encode_log_entry(entry);
        auto bytes = std::get_if<std::vector<std::byte>>(&encoded);

        expect(bytes != nullptr, "could not encode recovery test entry");
        if(bytes == nullptr){
            std::filesystem::remove(temp_path);
            return;
        }
        std::ofstream output_file(
            temp_path,
            std::ios::binary | std::ios::trunc
        );

        expect(output_file.is_open(), "could not create valid test log");
        if(!output_file.is_open()){
            std::filesystem::remove(temp_path);
            return;
        }

        output_file.write(
            reinterpret_cast<const char*>(bytes->data()),
            static_cast<std::streamsize>(bytes->size())
        );

        expect(output_file.good(), "could not write valid test log");
        if(!output_file.good()){
            output_file.close();
            std::filesystem::remove(temp_path);
            return;
        }

        output_file.close();

        auto result = dkv::PersistentLog::open(temp_path);
        auto* pointer = std::get_if<std::unique_ptr<dkv::PersistentLog>>(&result);
        expect(pointer != nullptr && *pointer != nullptr, "opening valid file returned an error");

        if(pointer == nullptr || *pointer == nullptr){
            std::filesystem::remove(temp_path);
            return;
        }
        expect((*pointer)->last_index() == 1, "recovery failed with no entry");
        expect((*pointer)->last_term() == 7, "recovered log had incorrect last term");
        expect((*pointer)->entry_at(1).has_value(), "recovery failed, entry doesn't exist");
        const auto recovered_entry = (*pointer)->entry_at(1);
        expect(!(*pointer)->entry_at(2).has_value(), "recovery returned unexpected entry at index 2");
        if(recovered_entry.has_value()){
            expect(*recovered_entry == entry, "recovered entry does not match");
        }
        pointer->reset();
        std::filesystem::remove(temp_path);
    }

    void test_open_truncated_record(){
        std::string unique_filename = "dkv-truncated-" + std::to_string(::getpid()) + ".log";
        const auto temp_path = std::filesystem::temp_directory_path() / unique_filename;
        std::filesystem::remove(temp_path);

        const dkv::LogEntry entry{
            1,
            4,
            dkv::Command{dkv::CommandType::Put, "name", "alice"}
        };

        auto encoded = dkv::encode_log_entry(entry);
        auto* bytes = std::get_if<std::vector<std::byte>>(&encoded);
        expect(bytes != nullptr, "could not encode truncated-record test entry");
        if(bytes == nullptr){
            std::filesystem::remove(temp_path);
            return;
        }
        bytes->pop_back();
        std::ofstream output_file(
            temp_path,
            std::ios::binary | std::ios::trunc
        );

        expect(output_file.is_open(), "could not create truncated test log");
        if(!output_file.is_open()){
            std::filesystem::remove(temp_path);
            return;
        }
        output_file.write(
            reinterpret_cast<const char*>(bytes->data()),
            static_cast<std::streamsize>(bytes->size())
        );
        expect(output_file.good(), "could not write truncated test log.");
        if(!output_file.good()){
            output_file.close();
            std::filesystem::remove(temp_path);
            return;
        }
        output_file.close();

        auto result = dkv::PersistentLog::open(temp_path);
        auto error = std::get_if<dkv::PersistentLogOpenError>(&result);
        expect(error != nullptr, "opening truncated file did not return an error");
        if(error != nullptr){
            expect(*error == dkv::PersistentLogOpenError::TruncatedRecord, "did not output the correct error for truncated log");
        }
        std::filesystem::remove(temp_path);
    }

    void test_open_valid_file_recovers_multiple_entries(){
        std::string unique_filename = "dkv-multiple-" + std::to_string(::getpid()) + ".log";
        const auto temp_path = std::filesystem::temp_directory_path() / unique_filename;
        std::filesystem::remove(temp_path);

        const std::vector<dkv::LogEntry> entries{
            dkv::LogEntry{
                1,
                4,
                dkv::Command{dkv::CommandType::Put, "name", "alice"}
            },
            dkv::LogEntry{
                2,
                4,
                dkv::Command{dkv::CommandType::Put, "city", "boston"}
            },
            dkv::LogEntry{
                3,
                5,
                dkv::Command{dkv::CommandType::Delete, "name", ""}
            }
        };

        std::vector<std::byte> file_bytes;
        for(const dkv::LogEntry& entry : entries){
            auto encoded = dkv::encode_log_entry(entry);
            auto* bytes = std::get_if<std::vector<std::byte>>(&encoded);

            expect(bytes != nullptr, "could not encode multiple-entry recovery test");
            if(bytes == nullptr){
                std::filesystem::remove(temp_path);
                return;
            }

            file_bytes.insert(
                file_bytes.end(),
                bytes->begin(),
                bytes->end()
            );
        }

        std::ofstream output_file(
            temp_path,
            std::ios::binary | std::ios::trunc
        );

        expect(output_file.is_open(), "could not create multiple-entry test log");
        if(!output_file.is_open()){
            std::filesystem::remove(temp_path);
            return;
        }

        output_file.write(
            reinterpret_cast<const char*>(file_bytes.data()),
            static_cast<std::streamsize>(file_bytes.size())
        );

        expect(output_file.good(), "could not write multiple-entry test log");
        if(!output_file.good()){
            output_file.close();
            std::filesystem::remove(temp_path);
            return;
        }

        output_file.close();

        auto result = dkv::PersistentLog::open(temp_path);
        auto* pointer = std::get_if<std::unique_ptr<dkv::PersistentLog>>(&result);
        expect(pointer != nullptr && *pointer != nullptr, "opening multiple-entry file returned an error");

        if(pointer == nullptr || *pointer == nullptr){
            std::filesystem::remove(temp_path);
            return;
        }

        expect((*pointer)->last_index() == 3, "recovered log had incorrect last index");
        expect((*pointer)->last_term() == 5, "recovered log had incorrect last term");

        for(const dkv::LogEntry& entry : entries){
            const auto recovered_entry = (*pointer)->entry_at(entry.index);
            expect(recovered_entry.has_value(), "recovered log was missing an entry");
            if(recovered_entry.has_value()){
                expect(*recovered_entry == entry, "recovered log entry does not match");
            }
        }

        expect(!(*pointer)->entry_at(0).has_value(), "recovered log returned an entry at index 0");
        expect(!(*pointer)->entry_at(4).has_value(), "recovered log returned an entry past its end");
        pointer->reset();
        std::filesystem::remove(temp_path);
    }

    void test_open_corrupt_record_returns_error(){
        std::string unique_filename = "dkv-corrupt-" + std::to_string(::getpid()) + ".log";
        const auto temp_path = std::filesystem::temp_directory_path() / unique_filename;
        std::filesystem::remove(temp_path);

        const dkv::LogEntry entry{
            1,
            7,
            dkv::Command{dkv::CommandType::Put, "name", "alice"}
        };
        auto encoded = dkv::encode_log_entry(entry);
        auto* bytes = std::get_if<std::vector<std::byte>>(&encoded);

        expect(bytes != nullptr, "could not encode corrupt-record test entry");
        if(bytes == nullptr){
            std::filesystem::remove(temp_path);
            return;
        }

        bytes->back() ^= std::byte{0xFF};

        std::ofstream output_file(
            temp_path,
            std::ios::binary | std::ios::trunc
        );

        expect(output_file.is_open(), "could not create corrupt test log");
        if(!output_file.is_open()){
            std::filesystem::remove(temp_path);
            return;
        }

        output_file.write(
            reinterpret_cast<const char*>(bytes->data()),
            static_cast<std::streamsize>(bytes->size())
        );

        expect(output_file.good(), "could not write corrupt test log");
        if(!output_file.good()){
            output_file.close();
            std::filesystem::remove(temp_path);
            return;
        }

        output_file.close();

        auto result = dkv::PersistentLog::open(temp_path);
        auto* error = std::get_if<dkv::PersistentLogOpenError>(&result);
        expect(error != nullptr, "opening corrupt file returned a log");
        if(error != nullptr){
            expect(
                *error == dkv::PersistentLogOpenError::CorruptRecord,
                "corrupt file did not report CorruptRecord"
            );
        }

        std::filesystem::remove(temp_path);
    }

    void test_open_non_sequential_indexes_returns_error(){
        std::string unique_filename = "dkv-non-sequential-" + std::to_string(::getpid()) + ".log";
        const auto temp_path = std::filesystem::temp_directory_path() / unique_filename;
        std::filesystem::remove(temp_path);

        const dkv::LogEntry first_entry{
            1,
            3,
            dkv::Command{dkv::CommandType::Put, "a", "one"}
        };
        const dkv::LogEntry third_entry{
            3,
            3,
            dkv::Command{dkv::CommandType::Put, "b", "two"}
        };

        auto first_encoded = dkv::encode_log_entry(first_entry);
        auto third_encoded = dkv::encode_log_entry(third_entry);
        auto* first_bytes = std::get_if<std::vector<std::byte>>(&first_encoded);
        auto* third_bytes = std::get_if<std::vector<std::byte>>(&third_encoded);

        expect(first_bytes != nullptr, "could not encode first non-sequential test entry");
        expect(third_bytes != nullptr, "could not encode third non-sequential test entry");
        if(first_bytes == nullptr || third_bytes == nullptr){
            std::filesystem::remove(temp_path);
            return;
        }

        std::vector<std::byte> file_bytes = *first_bytes;
        file_bytes.insert(
            file_bytes.end(),
            third_bytes->begin(),
            third_bytes->end()
        );

        std::ofstream output_file(
            temp_path,
            std::ios::binary | std::ios::trunc
        );

        expect(output_file.is_open(), "could not create non-sequential test log");
        if(!output_file.is_open()){
            std::filesystem::remove(temp_path);
            return;
        }

        output_file.write(
            reinterpret_cast<const char*>(file_bytes.data()),
            static_cast<std::streamsize>(file_bytes.size())
        );

        expect(output_file.good(), "could not write non-sequential test log");
        if(!output_file.good()){
            output_file.close();
            std::filesystem::remove(temp_path);
            return;
        }

        output_file.close();

        auto result = dkv::PersistentLog::open(temp_path);
        auto* error = std::get_if<dkv::PersistentLogOpenError>(&result);
        expect(error != nullptr, "opening non-sequential file returned a log");
        if(error != nullptr){
            expect(
                *error == dkv::PersistentLogOpenError::NonSequentialIndex,
                "non-sequential file did not report NonSequentialIndex"
            );
        }

        std::filesystem::remove(temp_path);
    }

    void test_open_file_in_invalid_location_returns_io_error(){
        std::string unique_filename = "dkv-io-error-" + std::to_string(::getpid());
        const auto blocker_path = std::filesystem::temp_directory_path() / unique_filename;
        const auto log_path = blocker_path / "log";
        std::filesystem::remove(blocker_path);

        std::ofstream blocker_file(
            blocker_path,
            std::ios::binary | std::ios::trunc
        );
        expect(blocker_file.is_open(), "could not create I/O error test file");
        if(!blocker_file.is_open()){
            std::filesystem::remove(blocker_path);
            return;
        }
        blocker_file.close();

        auto result = dkv::PersistentLog::open(log_path);
        auto* error = std::get_if<dkv::PersistentLogOpenError>(&result);
        expect(error != nullptr, "opening invalid path returned a log");
        if(error != nullptr){
            expect(
                *error == dkv::PersistentLogOpenError::IoError,
                "invalid path did not report IoError"
            );
        }

        std::filesystem::remove(blocker_path);
    }
}

int main(){
    test_open_truncated_record();
    test_open_missing_file_returns_empty_log();
    test_open_empty_file_returns_empty_log();
    test_open_valid_file_recovers_entry();
    test_open_valid_file_recovers_multiple_entries();
    test_open_corrupt_record_returns_error();
    test_open_non_sequential_indexes_returns_error();
    test_open_file_in_invalid_location_returns_io_error();
    if(failures != 0){
        std::cerr << failures << " test check(s) failed\n";
        return 1;
    }
    std::cout << "SUCCESS\n";
    return 0;
}
#include "dkv/kv_state_machine.hpp"

#include <iostream>
#include <string>
#include <string_view>

namespace {

    int failures = 0;

    void expect(bool condition, std::string_view message) {
        if(!condition) {
            std::cerr << "FAIL: " << message << '\n';
            ++failures;
        }
    }

    void test_initial_state() {
        const dkv::KvStateMachine state{};

        expect(state.last_applied() == 0, "a new state machine should start at log index 0");
        expect(!state.get("missing").has_value(), "a missing key should return an empty optional");
    }

    void test_put_and_log_ordering() {
        dkv::KvStateMachine state{};
        const dkv::Command initial_put{
            dkv::CommandType::Put,
            "name",
            "Milo",
        };

        const auto initial_put_result = state.apply(1, initial_put);
        expect(initial_put_result == dkv::ApplyResult::Applied, "a Put at the next log index should be applied");
        expect(state.last_applied() == 1, "a successful Put should advance the applied index");
        expect(state.get("name") == "Milo", "a successful Put should store its value");

        const dkv::Command replacement_put{
            dkv::CommandType::Put,
            "name",
            "Elon",
        };

        const auto duplicate_result = state.apply(1, replacement_put);
        expect(duplicate_result == dkv::ApplyResult::UnexpectedIndex, "a duplicate log index should be rejected");
        expect(state.get("name") == "Milo", "a rejected duplicate should not change stored data");
        expect(state.last_applied() == 1, "a rejected duplicate should not advance the applied index");

        const auto gap_result = state.apply(3, replacement_put);
        expect(gap_result == dkv::ApplyResult::UnexpectedIndex, "a skipped log index should be rejected");
        expect(state.get("name") == "Milo", "a rejected gap should not change stored data");
        expect(state.last_applied() == 1, "a rejected gap should not advance the applied index");

        const auto replacement_result = state.apply(2, replacement_put);
        expect(replacement_result == dkv::ApplyResult::Applied, "a Put at the next log index should replace an existing value");
        expect(state.get("name") == "Elon", "a replacement Put should expose the new value");
        expect(state.last_applied() == 2, "a replacement Put should advance the applied index");
    }

    void test_delete() {
        dkv::KvStateMachine state{};
        const dkv::Command put{
            dkv::CommandType::Put,
            "name",
            "Milo",
        };
        const dkv::Command delete_name{
            dkv::CommandType::Delete,
            "name",
            {},
        };

        expect(state.apply(1, put) == dkv::ApplyResult::Applied, "Delete setup Put should be applied");

        const auto delete_result = state.apply(2, delete_name);
        expect(delete_result == dkv::ApplyResult::Applied, "deleting an existing key should be applied");
        expect(!state.get("name").has_value(), "a deleted key should no longer be present");
        expect(state.last_applied() == 2, "deleting an existing key should advance the applied index");

        const auto missing_delete_result = state.apply(3, delete_name);
        expect(missing_delete_result == dkv::ApplyResult::KeyNotFound, "deleting a missing key should return KeyNotFound");
        expect(state.last_applied() == 3, "deleting a missing key should still advance the applied index");
    }

    void test_no_op() {
        dkv::KvStateMachine state{};
        const dkv::Command put{
            dkv::CommandType::Put,
            "name",
            "Milo",
        };
        const dkv::Command no_op{};

        expect(state.apply(1, put) == dkv::ApplyResult::Applied, "NoOp setup Put should be applied");

        const auto no_op_result = state.apply(2, no_op);
        expect(no_op_result == dkv::ApplyResult::Applied, "a valid NoOp should be applied");
        expect(state.get("name") == "Milo", "a NoOp should not change stored data");
        expect(state.last_applied() == 2, "a NoOp should advance the applied index");
    }

    void test_invalid_commands() {
        dkv::KvStateMachine state{};
        const dkv::Command put{
            dkv::CommandType::Put,
            "name",
            "Milo",
        };

        expect(state.apply(1, put) == dkv::ApplyResult::Applied, "invalid-command setup Put should be applied");

        const dkv::Command invalid_delete{
            dkv::CommandType::Delete,
            "name",
            "unused value",
        };
        const auto invalid_delete_result = state.apply(2, invalid_delete);
        expect(invalid_delete_result == dkv::ApplyResult::InvalidCommand, "a Delete with a value should be rejected");
        expect(state.get("name") == "Milo", "an invalid Delete should not remove the key");
        expect(state.last_applied() == 1, "an invalid Delete should not advance the applied index");

        const dkv::Command no_op_with_key{
            dkv::CommandType::NoOp,
            "unexpected key",
            {},
        };
        const auto keyed_no_op_result = state.apply(2, no_op_with_key);
        expect(keyed_no_op_result == dkv::ApplyResult::InvalidCommand, "a NoOp with a key should be rejected");
        expect(state.last_applied() == 1, "an invalid keyed NoOp should not advance the applied index");

        const dkv::Command no_op_with_value{
            dkv::CommandType::NoOp,
            {},
            "unexpected value",
        };
        const auto valued_no_op_result = state.apply(2, no_op_with_value);
        expect(valued_no_op_result == dkv::ApplyResult::InvalidCommand, "a NoOp with a value should be rejected");
        expect(state.last_applied() == 1, "an invalid valued NoOp should not advance the applied index");
        expect(state.get("name") == "Milo", "invalid NoOps should not change stored data");

        const dkv::Command valid_no_op{};
        expect(state.apply(2, valid_no_op) == dkv::ApplyResult::Applied, "a valid command should still apply after invalid commands");
        expect(state.last_applied() == 2, "the valid command should advance the applied index");
    }

    void test_arbitrary_byte_strings() {
        dkv::KvStateMachine state{};
        const std::string binary_key{"key\0suffix", 10};
        const std::string binary_value{"value\0suffix", 12};
        const dkv::Command binary_put{
            dkv::CommandType::Put,
            binary_key,
            binary_value,
        };

        const auto put_result = state.apply(1, binary_put);
        expect(put_result == dkv::ApplyResult::Applied, "a Put should accept strings containing null bytes");

        const auto stored_value = state.get(binary_key);
        expect(stored_value.has_value(), "a binary key should be retrievable");
        if(stored_value.has_value()) {
            expect(*stored_value == binary_value, "a binary value should be preserved exactly");
        }

        const dkv::Command binary_delete{
            dkv::CommandType::Delete,
            binary_key,
            {},
        };
        expect(state.apply(2, binary_delete) == dkv::ApplyResult::Applied, "Delete should accept a key containing null bytes");
        expect(!state.get(binary_key).has_value(), "Delete should remove the complete binary key");
    }

}  // namespace

int main() {
    test_initial_state();
    test_put_and_log_ordering();
    test_delete();
    test_no_op();
    test_invalid_commands();
    test_arbitrary_byte_strings();

    if(failures != 0) {
        std::cerr << failures << " test check(s) failed\n";
        return 1;
    }

    std::cout << "SUCCESS\n";
    return 0;
}

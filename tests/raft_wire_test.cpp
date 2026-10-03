#include "dkv/raft_wire.hpp"

#include <cstdlib>
#include <iostream>
#include <string_view>
#include <variant>
#include <vector>

namespace {
    int failures = 0;

    void expect(bool condition, std::string_view message) {
        if (!condition) {
            std::cerr << "FAIL: " << message << '\n';
            ++failures;
        }
    }

    void round_trip(const dkv::RaftEnvelope& envelope) {
        const auto encoded = dkv::encode_raft_envelope(envelope);
        const auto* bytes = std::get_if<std::vector<std::byte>>(&encoded);
        expect(bytes != nullptr, "could not encode valid Raft message");
        if (!bytes) return;
        const auto decoded = dkv::decode_raft_envelope(*bytes);
        const auto* received = std::get_if<dkv::RaftEnvelope>(&decoded);
        expect(received && *received == envelope, "Raft wire round trip changed message");
        const auto truncated = dkv::decode_raft_envelope(
            std::span<const std::byte>(*bytes).first(bytes->size() - 1));
        expect(std::holds_alternative<dkv::RaftWireError>(truncated),
               "truncated Raft message was accepted");
    }

    void test_round_trips() {
        const dkv::AppendEntries append{
            3, 1, 1, 2,
            {{2, 3, {dkv::CommandType::Put, "x", "10"}},
             {3, 3, {dkv::CommandType::Delete, "y", ""}}},
            1};
        round_trip({1, dkv::RequestVote{3, 1, 2, 3}, std::nullopt});
        round_trip({2, dkv::RequestVoteReply{3, true}, std::nullopt});
        round_trip({1, append, std::nullopt});
        round_trip({2, dkv::AppendEntriesReply{3, true}, append});
    }

    void test_bad_envelopes() {
        expect(dkv::encode_raft_envelope(
                   {2, dkv::AppendEntriesReply{3, true}, std::nullopt}) ==
                   dkv::EncodeRaftEnvelopeResult{dkv::RaftWireError::InvalidEnvelope},
               "append reply without request was encoded");
        expect(dkv::encode_raft_envelope(
                   {0, dkv::RequestVote{3, 1, 0, 0}, std::nullopt}) ==
                   dkv::EncodeRaftEnvelopeResult{dkv::RaftWireError::InvalidEnvelope},
               "zero sender ID was encoded");

        auto encoded = dkv::encode_raft_envelope(
            {1, dkv::RequestVote{3, 1, 0, 0}, std::nullopt});
        auto bytes = std::get<std::vector<std::byte>>(encoded);
        bytes[0] = std::byte{2};
        expect(dkv::decode_raft_envelope(bytes) ==
                   dkv::DecodeRaftEnvelopeResult{dkv::RaftWireError::UnsupportedVersion},
               "unknown wire version was accepted");
        bytes[0] = std::byte{1};
        bytes.push_back(std::byte{0});
        expect(dkv::decode_raft_envelope(bytes) ==
                   dkv::DecodeRaftEnvelopeResult{dkv::RaftWireError::TrailingBytes},
               "trailing wire bytes were accepted");
    }
}

int main() {
    test_round_trips();
    test_bad_envelopes();
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}

#include "dkv/client_wire.hpp"
#include "dkv/crc32.hpp"

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

    void refresh_checksum(std::vector<std::byte>& bytes) {
        const auto checksum = dkv::crc32(
            std::span<const std::byte>(bytes).first(bytes.size() - 4));
        for (int i = 0; i < 4; ++i) {
            bytes[bytes.size() - 4 + i] =
                static_cast<std::byte>((checksum >> (24 - 8 * i)) & 0xff);
        }
    }

    template <typename Decode>
    void expect_corruption_rejected(const std::vector<std::byte>& bytes,
                                    Decode decode, std::string_view message) {
        auto corrupted = bytes;
        corrupted[bytes.size() - 5] ^= std::byte{1};
        expect(decode(corrupted) ==
                   decltype(decode(corrupted)){dkv::ClientWireError::ChecksumMismatch},
               message);
        corrupted = bytes;
        corrupted.back() ^= std::byte{1};
        expect(decode(corrupted) ==
                   decltype(decode(corrupted)){dkv::ClientWireError::ChecksumMismatch},
               message);
    }

    void test_requests() {
        dkv::RequestId id;
        id.bytes[0] = 0x42;
        for (const auto& command : {
                 dkv::Command{dkv::CommandType::Put, "x", "10", id},
                 dkv::Command{dkv::CommandType::Delete, "x", "", id}}) {
            const dkv::ClientWriteRequest request{command};
            const auto encoded = dkv::encode_client_request(request);
            const auto* bytes = std::get_if<std::vector<std::byte>>(&encoded);
            expect(bytes != nullptr, "could not encode write request");
            if (!bytes) continue;
            expect(dkv::decode_client_request(*bytes) ==
                       dkv::DecodeClientRequestResult{request},
                   "write request did not round trip");
            expect_corruption_rejected(*bytes, dkv::decode_client_request,
                                       "corrupted write request was accepted");
            auto truncated = *bytes;
            truncated.pop_back();
            expect(std::holds_alternative<dkv::ClientWireError>(
                       dkv::decode_client_request(truncated)),
                   "truncated write request was accepted");
        }
        expect(dkv::encode_client_request(
                   {{dkv::CommandType::NoOp, "", ""}}) ==
                   dkv::EncodeClientResult{dkv::ClientWireError::InvalidMessage},
               "client no-op was accepted");
        expect(dkv::encode_client_request(
                   {{dkv::CommandType::Put, "x", "10"}}) ==
                   dkv::EncodeClientResult{dkv::ClientWireError::InvalidMessage},
               "write without request ID was accepted");
    }

    void test_replies() {
        for (const auto status : {dkv::ClientWriteStatus::Committed,
                                  dkv::ClientWriteStatus::NotLeader,
                                  dkv::ClientWriteStatus::OutcomeUnknown}) {
            const dkv::ClientWriteReply reply{status, 3, 2, 1};
            const auto encoded = dkv::encode_client_reply(reply);
            const auto* bytes = std::get_if<std::vector<std::byte>>(&encoded);
            expect(bytes != nullptr, "could not encode write reply");
            if (!bytes) continue;
            expect(dkv::decode_client_reply(*bytes) ==
                       dkv::DecodeClientReplyResult{reply},
                   "write reply did not round trip");
            expect_corruption_rejected(*bytes, dkv::decode_client_reply,
                                       "corrupted write reply was accepted");
            auto malformed = *bytes;
            malformed[2] = std::byte{0};
            refresh_checksum(malformed);
            expect(dkv::decode_client_reply(malformed) ==
                       dkv::DecodeClientReplyResult{
                           dkv::ClientWireError::InvalidMessage},
                   "unknown write status was accepted");
        }
    }

    void test_read_messages() {
        const dkv::ClientReadRequest request{"empty-value"};
        const auto encoded_request = dkv::encode_client_read_request(request);
        const auto* request_bytes =
            std::get_if<std::vector<std::byte>>(&encoded_request);
        expect(request_bytes != nullptr, "could not encode GET request");
        if (request_bytes) {
            expect(dkv::decode_client_read_request(*request_bytes) ==
                       dkv::DecodeClientReadRequestResult{request},
                   "GET request did not round trip");
            expect_corruption_rejected(*request_bytes, dkv::decode_client_read_request,
                                       "corrupted GET request was accepted");
            auto malformed = *request_bytes;
            malformed.pop_back();
            expect(std::holds_alternative<dkv::ClientWireError>(
                       dkv::decode_client_read_request(malformed)),
                   "truncated GET key was accepted");
        }

        for (const auto& reply : {
                 dkv::ClientReadReply{dkv::ClientReadStatus::Found, 3, 4, 1,
                                      std::string{}},
                 dkv::ClientReadReply{dkv::ClientReadStatus::Found, 3, 4, 1,
                                      std::string{"value"}},
                 dkv::ClientReadReply{dkv::ClientReadStatus::NotFound, 3, 4, 1,
                                      std::nullopt},
                 dkv::ClientReadReply{dkv::ClientReadStatus::NotLeader, 3, 0, 1,
                                      std::nullopt}}) {
            const auto encoded = dkv::encode_client_read_reply(reply);
            const auto* bytes = std::get_if<std::vector<std::byte>>(&encoded);
            expect(bytes != nullptr, "could not encode GET reply");
            if (!bytes) continue;
            expect(dkv::decode_client_read_reply(*bytes) ==
                       dkv::DecodeClientReadReplyResult{reply},
                   "GET reply did not round trip");
            expect_corruption_rejected(*bytes, dkv::decode_client_read_reply,
                                       "corrupted GET reply was accepted");
        }
        expect(dkv::encode_client_read_reply(
                   {dkv::ClientReadStatus::Found, 3, 4, 1, std::nullopt}) ==
                   dkv::EncodeClientResult{dkv::ClientWireError::InvalidMessage},
               "GET found reply without a value was accepted");
    }

    void test_status_messages() {
        const dkv::ClientStatusRequest request;
        const auto encoded_request = dkv::encode_client_status_request(request);
        const auto* request_bytes =
            std::get_if<std::vector<std::byte>>(&encoded_request);
        expect(request_bytes && request_bytes->size() == 6 &&
                   dkv::decode_client_status_request(*request_bytes) ==
                       dkv::DecodeClientStatusRequestResult{request},
               "STATUS request did not round trip");
        if (request_bytes) {
            expect_corruption_rejected(*request_bytes, dkv::decode_client_status_request,
                                       "corrupted STATUS request was accepted");
            auto malformed = *request_bytes;
            malformed.push_back(std::byte{0});
            expect(dkv::decode_client_status_request(malformed) ==
                       dkv::DecodeClientStatusRequestResult{
                           dkv::ClientWireError::InvalidMessage},
                   "STATUS request with a payload was accepted");
            malformed = *request_bytes;
            malformed[0] = std::byte{1};
            expect(dkv::decode_client_status_request(malformed) ==
                       dkv::DecodeClientStatusRequestResult{
                           dkv::ClientWireError::UnsupportedVersion},
                   "previous STATUS wire version was accepted");
        }

        dkv::ClientStatusReply follower{dkv::ClientStatusCode::Ok, 2,
            dkv::ClientNodeRole::Follower, 3, 1, 4, 4, 4,
            {{{1, 0, 0}, {3, 0, 0}}}};
        dkv::ClientStatusReply candidate{dkv::ClientStatusCode::Ok, 3,
            dkv::ClientNodeRole::Candidate, 4, 0, 4, 4, 4,
            {{{1, 0, 0}, {2, 0, 0}}}};
        dkv::ClientStatusReply leader{dkv::ClientStatusCode::Ok, 1,
            dkv::ClientNodeRole::Leader, 5, 1, 5, 4, 4,
            {{{2, 6, 5}, {3, 5, 4}}}};
        for (const auto& reply : {follower, candidate, leader,
                                  dkv::ClientStatusReply{}}) {
            const auto encoded = dkv::encode_client_status_reply(reply);
            const auto* bytes = std::get_if<std::vector<std::byte>>(&encoded);
            expect(bytes && bytes->size() == 104,
                   "could not encode fixed-size STATUS reply");
            if (!bytes) continue;
            expect_corruption_rejected(*bytes, dkv::decode_client_status_reply,
                                       "corrupted STATUS reply was accepted");
            expect(dkv::decode_client_status_reply(*bytes) ==
                       dkv::DecodeClientStatusReplyResult{reply},
                   "STATUS reply did not round trip");
            auto truncated = *bytes;
            truncated.pop_back();
            expect(dkv::decode_client_status_reply(truncated) ==
                       dkv::DecodeClientStatusReplyResult{
                           dkv::ClientWireError::Truncated},
                   "truncated STATUS reply was accepted");
        }
        const auto encoded = dkv::encode_client_status_reply(leader);
        const auto* bytes = std::get_if<std::vector<std::byte>>(&encoded);
        if (bytes) {
            auto malformed = *bytes;
            malformed[3] = std::byte{9};
            refresh_checksum(malformed);
            expect(dkv::decode_client_status_reply(malformed) ==
                       dkv::DecodeClientStatusReplyResult{
                           dkv::ClientWireError::InvalidMessage},
                   "unknown STATUS role was accepted");
            malformed = *bytes;
            malformed[60] = std::byte{0};
            malformed[67] = std::byte{0};
            refresh_checksum(malformed);
            expect(dkv::decode_client_status_reply(malformed) ==
                       dkv::DecodeClientStatusReplyResult{
                           dkv::ClientWireError::InvalidMessage},
                   "leader STATUS without follower progress was accepted");
        }
        follower.peers[0].next_index = 5;
        expect(dkv::encode_client_status_reply(follower) ==
                   dkv::EncodeClientResult{dkv::ClientWireError::InvalidMessage},
               "follower advertised leader-only progress");
    }
}

int main() {
    test_requests();
    test_replies();
    test_read_messages();
    test_status_messages();
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}

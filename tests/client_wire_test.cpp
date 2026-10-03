#include "dkv/client_wire.hpp"

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

    void test_requests() {
        for (const auto& command : {
                 dkv::Command{dkv::CommandType::Put, "x", "10"},
                 dkv::Command{dkv::CommandType::Delete, "x", ""}}) {
            const dkv::ClientWriteRequest request{command};
            const auto encoded = dkv::encode_client_request(request);
            const auto* bytes = std::get_if<std::vector<std::byte>>(&encoded);
            expect(bytes != nullptr, "could not encode write request");
            if (!bytes) continue;
            expect(dkv::decode_client_request(*bytes) ==
                       dkv::DecodeClientRequestResult{request},
                   "write request did not round trip");
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
            auto malformed = *bytes;
            malformed[2] = std::byte{0};
            expect(dkv::decode_client_reply(malformed) ==
                       dkv::DecodeClientReplyResult{
                           dkv::ClientWireError::InvalidMessage},
                   "unknown write status was accepted");
        }
    }
}

int main() {
    test_requests();
    test_replies();
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}

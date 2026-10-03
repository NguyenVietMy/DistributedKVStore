#include "dkv/client_wire.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace {
    bool send_all(int fd, const std::byte* data, std::size_t size) {
        std::size_t done = 0;
        while (done < size) {
            const auto count = ::send(fd, data + done, size - done, MSG_NOSIGNAL);
            if (count < 0 && errno == EINTR) continue;
            if (count <= 0) return false;
            done += static_cast<std::size_t>(count);
        }
        return true;
    }

    bool receive_all(int fd, std::byte* data, std::size_t size) {
        std::size_t done = 0;
        while (done < size) {
            const auto count = ::recv(fd, data + done, size - done, 0);
            if (count < 0 && errno == EINTR) continue;
            if (count <= 0) return false;
            done += static_cast<std::size_t>(count);
        }
        return true;
    }

    std::array<std::byte, 4> prefix(std::size_t length) {
        return {static_cast<std::byte>((length >> 24) & 0xff),
                static_cast<std::byte>((length >> 16) & 0xff),
                static_cast<std::byte>((length >> 8) & 0xff),
                static_cast<std::byte>(length & 0xff)};
    }

    int run(std::uint16_t port, const dkv::EncodeClientResult& encoded,
            bool reading) {
        const auto* bytes = std::get_if<std::vector<std::byte>>(&encoded);
        if (!bytes) {
            std::cerr << "invalid or oversized request\n";
            return 2;
        }
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            std::cerr << "could not open socket\n";
            return 5;
        }
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(port);
        if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
            ::close(fd);
            std::cerr << "server unavailable\n";
            return 5;
        }
        timeval timeout{10, 0};
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
        auto header = prefix(bytes->size());
        if (!send_all(fd, header.data(), header.size()) ||
            !send_all(fd, bytes->data(), bytes->size()) ||
            !receive_all(fd, header.data(), header.size())) {
            ::close(fd);
            std::cerr << (reading ? "read unavailable; retry\n"
                                  : "outcome unknown: connection closed or timed out\n");
            return 4;
        }
        std::size_t length = 0;
        for (const auto octet : header) {
            length = (length << 8) | std::to_integer<unsigned>(octet);
        }
        if (length == 0 || length > dkv::max_client_frame_size) {
            ::close(fd);
            std::cerr << "invalid server reply\n";
            return 5;
        }
        std::vector<std::byte> reply_bytes(length);
        const bool received = receive_all(fd, reply_bytes.data(), length);
        ::close(fd);
        if (!received) {
            std::cerr << (reading ? "read unavailable; retry\n"
                                  : "outcome unknown: incomplete reply\n");
            return 4;
        }
        if (reading) {
            const auto decoded = dkv::decode_client_read_reply(reply_bytes);
            const auto* reply = std::get_if<dkv::ClientReadReply>(&decoded);
            if (!reply) {
                std::cerr << "invalid server reply\n";
                return 5;
            }
            switch (reply->status) {
            case dkv::ClientReadStatus::Found:
                std::cout << *reply->value << '\n';
                return 0;
            case dkv::ClientReadStatus::NotFound:
                std::cout << "(not found)\n";
                return 0;
            case dkv::ClientReadStatus::NotLeader:
                std::cout << "not leader";
                if (reply->leader_hint != 0) {
                    std::cout << "; try node " << reply->leader_hint;
                }
                std::cout << '\n';
                return 3;
            case dkv::ClientReadStatus::Busy:
                std::cout << "leader busy; retry later\n";
                return 5;
            case dkv::ClientReadStatus::InvalidRequest:
                std::cout << "invalid request\n";
                return 2;
            case dkv::ClientReadStatus::InternalError:
                std::cout << "read unavailable; retry\n";
                return 5;
            }
        }
        const auto decoded = dkv::decode_client_reply(reply_bytes);
        const auto* reply = std::get_if<dkv::ClientWriteReply>(&decoded);
        if (!reply) {
            std::cerr << "invalid server reply\n";
            return 5;
        }
        switch (reply->status) {
        case dkv::ClientWriteStatus::Committed:
            std::cout << "committed term " << reply->term
                      << " index " << reply->index << '\n';
            return 0;
        case dkv::ClientWriteStatus::NotLeader:
            std::cout << "not leader";
            if (reply->leader_hint != 0) {
                std::cout << "; try node " << reply->leader_hint;
            }
            std::cout << '\n';
            return 3;
        case dkv::ClientWriteStatus::OutcomeUnknown:
            std::cout << "outcome unknown for term " << reply->term
                      << " index " << reply->index << '\n';
            return 4;
        case dkv::ClientWriteStatus::InvalidRequest:
            std::cout << "invalid request\n";
            return 2;
        case dkv::ClientWriteStatus::Busy:
            std::cout << "leader busy; retry later\n";
            return 5;
        case dkv::ClientWriteStatus::InternalError:
            std::cout << "server error; outcome unknown\n";
            return 4;
        }
        return 5;
    }
}

int main(int argc, char** argv) {
    if (argc != 4 && argc != 5) {
        std::cerr << "usage: dkv_client PORT put KEY VALUE | delete KEY | get KEY\n";
        return 2;
    }
    unsigned port = 0;
    const std::string_view port_text = argv[1];
    const auto parsed = std::from_chars(
        port_text.data(), port_text.data() + port_text.size(), port);
    if (parsed.ec != std::errc{} ||
        parsed.ptr != port_text.data() + port_text.size() ||
        port == 0 || port > std::numeric_limits<std::uint16_t>::max()) {
        std::cerr << "invalid port\n";
        return 2;
    }
    const std::string_view operation = argv[2];
    if (operation == "put" && argc == 5) {
        return run(static_cast<std::uint16_t>(port),
                   dkv::encode_client_request(
                       {{dkv::CommandType::Put, argv[3], argv[4]}}), false);
    }
    if (operation == "delete" && argc == 4) {
        return run(static_cast<std::uint16_t>(port),
                   dkv::encode_client_request(
                       {{dkv::CommandType::Delete, argv[3], ""}}), false);
    }
    if (operation == "get" && argc == 4) {
        return run(static_cast<std::uint16_t>(port),
                   dkv::encode_client_read_request({argv[3]}), true);
    }
    std::cerr << "usage: dkv_client PORT put KEY VALUE | delete KEY | get KEY\n";
    return 2;
}

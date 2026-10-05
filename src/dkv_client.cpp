#include "dkv/client_wire.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/random.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <variant>
#include <vector>

namespace {
    using Clock = std::chrono::steady_clock;

    bool parse_port(std::string_view text, std::uint16_t& port) {
        unsigned value = 0;
        const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
        if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() ||
            value == 0 || value > std::numeric_limits<std::uint16_t>::max()) {
            return false;
        }
        port = static_cast<std::uint16_t>(value);
        return true;
    }

    int hex_digit(char character) {
        if (character >= '0' && character <= '9') return character - '0';
        if (character >= 'a' && character <= 'f') return character - 'a' + 10;
        if (character >= 'A' && character <= 'F') return character - 'A' + 10;
        return -1;
    }

    std::optional<dkv::RequestId> parse_request_id(std::string_view text) {
        if (text.size() != 32) return std::nullopt;
        dkv::RequestId id;
        for (std::size_t i = 0; i < id.bytes.size(); ++i) {
            const int high = hex_digit(text[2 * i]);
            const int low = hex_digit(text[2 * i + 1]);
            if (high < 0 || low < 0) return std::nullopt;
            id.bytes[i] = static_cast<std::uint8_t>((high << 4) | low);
        }
        return id;
    }

    std::optional<dkv::RequestId> generate_request_id() {
        dkv::RequestId id;
        std::size_t filled = 0;
        while (filled < id.bytes.size()) {
            const auto count = ::getrandom(id.bytes.data() + filled,
                                           id.bytes.size() - filled, 0);
            if (count < 0 && errno == EINTR) continue;
            if (count <= 0) return std::nullopt;
            filled += static_cast<std::size_t>(count);
        }
        return id;
    }

    std::string format_request_id(const dkv::RequestId& id) {
        constexpr char digits[] = "0123456789abcdef";
        std::string text;
        text.reserve(32);
        for (const auto octet : id.bytes) {
            text.push_back(digits[octet >> 4]);
            text.push_back(digits[octet & 0x0f]);
        }
        return text;
    }

    bool wait_ready(int fd, short events, Clock::time_point deadline) {
        while (Clock::now() < deadline) {
            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - Clock::now());
            const auto timeout = static_cast<int>(std::clamp<std::int64_t>(
                remaining.count() + 1, 1, std::numeric_limits<int>::max()));
            pollfd descriptor{fd, events, 0};
            const int result = ::poll(&descriptor, 1, timeout);
            if (result < 0 && errno == EINTR) continue;
            return result > 0 &&
                (descriptor.revents & (events | POLLERR | POLLHUP | POLLNVAL));
        }
        return false;
    }

    bool send_all(int fd, const std::byte* data, std::size_t size,
                  Clock::time_point deadline) {
        std::size_t done = 0;
        while (done < size) {
            if (!wait_ready(fd, POLLOUT, deadline)) return false;
            const auto count = ::send(fd, data + done, size - done,
                                      MSG_NOSIGNAL | MSG_DONTWAIT);
            if (count < 0 && (errno == EINTR || errno == EAGAIN ||
                              errno == EWOULDBLOCK)) continue;
            if (count <= 0) return false;
            done += static_cast<std::size_t>(count);
        }
        return true;
    }

    bool receive_all(int fd, std::byte* data, std::size_t size,
                     Clock::time_point deadline) {
        std::size_t done = 0;
        while (done < size) {
            if (!wait_ready(fd, POLLIN, deadline)) return false;
            const auto count = ::recv(fd, data + done, size - done, MSG_DONTWAIT);
            if (count < 0 && (errno == EINTR || errno == EAGAIN ||
                              errno == EWOULDBLOCK)) continue;
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

    enum class ExchangeError { SocketUnavailable, ServerUnavailable,
                               ConnectionLost, InvalidReply };
    using ExchangeResult = std::variant<dkv::ClientWriteReply,
                                        dkv::ClientReadReply, ExchangeError>;

    ExchangeResult exchange(std::uint16_t port, const std::vector<std::byte>& bytes,
                            bool reading, Clock::time_point deadline) {
        const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
        if (fd < 0) {
            return ExchangeError::SocketUnavailable;
        }
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(port);
        if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
            if (errno != EINPROGRESS || !wait_ready(fd, POLLOUT, deadline)) {
                ::close(fd);
                return ExchangeError::ServerUnavailable;
            }
            int socket_error = 0;
            socklen_t error_size = sizeof(socket_error);
            if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &error_size) != 0 ||
                socket_error != 0) {
                ::close(fd);
                return ExchangeError::ServerUnavailable;
            }
        }
        auto header = prefix(bytes.size());
        if (!send_all(fd, header.data(), header.size(), deadline) ||
            !send_all(fd, bytes.data(), bytes.size(), deadline) ||
            !receive_all(fd, header.data(), header.size(), deadline)) {
            ::close(fd);
            return ExchangeError::ConnectionLost;
        }
        std::size_t length = 0;
        for (const auto octet : header) {
            length = (length << 8) | std::to_integer<unsigned>(octet);
        }
        if (length == 0 || length > dkv::max_client_frame_size) {
            ::close(fd);
            return ExchangeError::InvalidReply;
        }
        std::vector<std::byte> reply_bytes(length);
        const bool received = receive_all(fd, reply_bytes.data(), length, deadline);
        ::close(fd);
        if (!received) {
            return ExchangeError::ConnectionLost;
        }
        if (reading) {
            const auto decoded = dkv::decode_client_read_reply(reply_bytes);
            const auto* reply = std::get_if<dkv::ClientReadReply>(&decoded);
            if (!reply) return ExchangeError::InvalidReply;
            return *reply;
        }
        const auto decoded = dkv::decode_client_reply(reply_bytes);
        const auto* reply = std::get_if<dkv::ClientWriteReply>(&decoded);
        if (!reply) return ExchangeError::InvalidReply;
        return *reply;
    }

    int show_result(const ExchangeResult& result, bool reading) {
        if (const auto* error = std::get_if<ExchangeError>(&result)) {
            switch (*error) {
            case ExchangeError::SocketUnavailable:
                std::cerr << "could not open socket\n";
                return 5;
            case ExchangeError::ServerUnavailable:
                std::cerr << "server unavailable\n";
                return 5;
            case ExchangeError::ConnectionLost:
                std::cerr << (reading ? "read unavailable; retry\n"
                                      : "outcome unknown: connection closed or timed out\n");
                return 4;
            case ExchangeError::InvalidReply:
                std::cerr << "invalid server reply\n";
                return 5;
            }
        }
        if (reading) {
            const auto* reply = std::get_if<dkv::ClientReadReply>(&result);
            if (!reply) return 5;
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
        const auto* reply = std::get_if<dkv::ClientWriteReply>(&result);
        if (!reply) return 5;
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

    int run_single(std::uint16_t port, const dkv::EncodeClientResult& encoded,
                   bool reading) {
        const auto* bytes = std::get_if<std::vector<std::byte>>(&encoded);
        if (!bytes) {
            std::cerr << "invalid or oversized request\n";
            return 2;
        }
        return show_result(exchange(port, *bytes, reading,
                                    Clock::now() + std::chrono::seconds(10)), reading);
    }

    int run_cluster(const std::array<std::uint16_t, 3>& ports,
                    const dkv::EncodeClientResult& encoded, bool reading) {
        const auto* bytes = std::get_if<std::vector<std::byte>>(&encoded);
        if (!bytes) {
            std::cerr << "invalid or oversized request\n";
            return 2;
        }
        const auto deadline = Clock::now() + std::chrono::seconds(8);
        std::size_t target = 0;
        std::array<bool, 3> visited{};
        while (Clock::now() < deadline) {
            visited[target] = true;
            const auto attempt_deadline = std::min(
                deadline, Clock::now() + std::chrono::seconds(1));
            const auto result = exchange(ports[target], *bytes, reading,
                                         attempt_deadline);
            std::uint64_t hint = 0;
            bool busy = false;
            if (const auto* reply = std::get_if<dkv::ClientReadReply>(&result)) {
                if (reply->status == dkv::ClientReadStatus::Found ||
                    reply->status == dkv::ClientReadStatus::NotFound ||
                    reply->status == dkv::ClientReadStatus::InvalidRequest) {
                    return show_result(result, reading);
                }
                hint = reply->leader_hint;
                busy = reply->status == dkv::ClientReadStatus::Busy;
            } else if (const auto* reply = std::get_if<dkv::ClientWriteReply>(&result)) {
                if (reply->status == dkv::ClientWriteStatus::Committed ||
                    reply->status == dkv::ClientWriteStatus::InvalidRequest) {
                    return show_result(result, reading);
                }
                hint = reply->leader_hint;
                busy = reply->status == dkv::ClientWriteStatus::Busy;
            }
            if (!busy) {
                const auto hinted = hint >= 1 && hint <= ports.size()
                    ? static_cast<std::size_t>(hint - 1) : ports.size();
                if (hinted < ports.size() && !visited[hinted]) {
                    target = hinted;
                } else {
                    std::size_t step = 1;
                    while (step < ports.size() &&
                           visited[(target + step) % ports.size()]) {
                        ++step;
                    }
                    if (step == ports.size()) {
                        visited.fill(false);
                        step = 1;
                    }
                    target = (target + step) % ports.size();
                }
            }
            const auto wait = std::min(std::chrono::milliseconds(75),
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    deadline - Clock::now()));
            if (wait.count() > 0) std::this_thread::sleep_for(wait);
        }
        std::cerr << (reading ? "read unavailable; retry\n"
                              : "outcome unknown; retry with the same request id\n");
        return 4;
    }
}

int main(int argc, char** argv) {
    constexpr const char* usage =
        "usage: dkv_client PORT [put KEY VALUE [REQUEST_ID] | "
        "delete KEY [REQUEST_ID] | get KEY]\n"
        "       dkv_client PORT1 PORT2 PORT3 [put KEY VALUE [REQUEST_ID] | "
        "delete KEY [REQUEST_ID] | get KEY]\n";
    if (argc < 4) {
        std::cerr << usage;
        return 2;
    }
    std::array<std::uint16_t, 3> ports{};
    if (!parse_port(argv[1], ports[0])) {
        std::cerr << "invalid port\n";
        return 2;
    }
    const std::string_view second = argv[2];
    const bool cluster = second != "put" && second != "delete" && second != "get";
    const int operation_at = cluster ? 4 : 2;
    if (cluster) {
        if (argc < 6) {
            std::cerr << usage;
            return 2;
        }
        if (!parse_port(argv[2], ports[1]) || !parse_port(argv[3], ports[2]) ||
            ports[0] == ports[1] || ports[0] == ports[2] || ports[1] == ports[2]) {
            std::cerr << "invalid or duplicate cluster port\n";
            return 2;
        }
    }
    const std::string_view operation = argv[operation_at];
    const auto execute = [&](const dkv::EncodeClientResult& encoded, bool reading) {
        return cluster ? run_cluster(ports, encoded, reading)
                       : run_single(ports[0], encoded, reading);
    };
    if ((operation == "put" &&
         (argc == operation_at + 3 || argc == operation_at + 4)) ||
        (operation == "delete" &&
         (argc == operation_at + 2 || argc == operation_at + 3))) {
        const bool explicit_id = (operation == "put" && argc == operation_at + 4) ||
                                 (operation == "delete" && argc == operation_at + 3);
        const auto id = explicit_id
            ? parse_request_id(argv[argc - 1]) : generate_request_id();
        if (!id) {
            std::cerr << (explicit_id ? "invalid request ID (expected 32 hex digits)\n"
                                      : "could not generate request ID\n");
            return 2;
        }
        std::cerr << "request id " << format_request_id(*id) << '\n';
        const dkv::Command command{
            operation == "put" ? dkv::CommandType::Put : dkv::CommandType::Delete,
            argv[operation_at + 1], operation == "put" ? argv[operation_at + 2] : "", id};
        return execute(dkv::encode_client_request({command}), false);
    }
    if (operation == "get" && argc == operation_at + 2) {
        return execute(dkv::encode_client_read_request({argv[operation_at + 1]}), true);
    }
    std::cerr << usage;
    return 2;
}

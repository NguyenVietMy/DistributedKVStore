#include "dkv/client_wire.hpp"
#include "dkv/raft_node.hpp"
#include "dkv/raft_wire.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <map>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace {
    using Clock = std::chrono::steady_clock;
    using namespace std::chrono_literals;

    class Socket {
    public:
        explicit Socket(int fd = -1) : fd_(fd) {}
        ~Socket() { if (fd_ >= 0) ::close(fd_); }
        Socket(const Socket&) = delete;
        Socket& operator=(const Socket&) = delete;
        int get() const { return fd_; }
        int release() { return std::exchange(fd_, -1); }

    private:
        int fd_;
    };

    bool parse_port(std::string_view text, std::uint16_t& port) {
        unsigned value = 0;
        const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
        if (result.ec != std::errc{} || result.ptr != text.data() + text.size() ||
            value == 0 || value > std::numeric_limits<std::uint16_t>::max()) {
            return false;
        }
        port = static_cast<std::uint16_t>(value);
        return true;
    }

    bool write_all(int fd, const std::byte* data, std::size_t size) {
        std::size_t done = 0;
        while (done < size) {
            const auto written = ::send(fd, data + done, size - done, MSG_NOSIGNAL);
            if (written < 0 && errno == EINTR) continue;
            if (written <= 0) return false;
            done += static_cast<std::size_t>(written);
        }
        return true;
    }

    bool read_all(int fd, std::byte* data, std::size_t size) {
        std::size_t done = 0;
        while (done < size) {
            const auto read = ::recv(fd, data + done, size - done, 0);
            if (read < 0 && errno == EINTR) continue;
            if (read <= 0) return false;
            done += static_cast<std::size_t>(read);
        }
        return true;
    }

    std::array<std::byte, 4> length_prefix(std::size_t size) {
        return {static_cast<std::byte>((size >> 24) & 0xff),
                static_cast<std::byte>((size >> 16) & 0xff),
                static_cast<std::byte>((size >> 8) & 0xff),
                static_cast<std::byte>(size & 0xff)};
    }

    std::optional<std::vector<std::byte>> receive_frame(int fd) {
        timeval timeout{0, 300000};
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        std::array<std::byte, 4> prefix{};
        if (!read_all(fd, prefix.data(), prefix.size())) return std::nullopt;
        std::size_t length = 0;
        for (const auto octet : prefix) {
            length = (length << 8) | std::to_integer<unsigned>(octet);
        }
        if (length == 0 || length > dkv::max_raft_frame_size) return std::nullopt;
        std::vector<std::byte> bytes(length);
        if (!read_all(fd, bytes.data(), bytes.size())) return std::nullopt;
        return bytes;
    }

    bool send_client_reply(int fd, const dkv::ClientWriteReply& reply) {
        const auto encoded = dkv::encode_client_reply(reply);
        const auto* bytes = std::get_if<std::vector<std::byte>>(&encoded);
        if (!bytes) return false;
        timeval timeout{0, 300000};
        ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
        const auto prefix = length_prefix(bytes->size());
        return write_all(fd, prefix.data(), prefix.size()) &&
               write_all(fd, bytes->data(), bytes->size());
    }

    bool send_client_read_reply(int fd, const dkv::ClientReadReply& reply) {
        const auto encoded = dkv::encode_client_read_reply(reply);
        const auto* bytes = std::get_if<std::vector<std::byte>>(&encoded);
        if (!bytes) return false;
        timeval timeout{0, 300000};
        ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
        const auto prefix = length_prefix(bytes->size());
        return write_all(fd, prefix.data(), prefix.size()) &&
               write_all(fd, bytes->data(), bytes->size());
    }

    enum class SendResult { Sent, Unavailable, InvalidMessage };

    SendResult send_frame(std::uint16_t port, const dkv::RaftEnvelope& envelope) {
        const auto encoded = dkv::encode_raft_envelope(envelope);
        const auto* bytes = std::get_if<std::vector<std::byte>>(&encoded);
        if (!bytes) return SendResult::InvalidMessage;
        Socket socket{::socket(AF_INET, SOCK_STREAM, 0)};
        if (socket.get() < 0) return SendResult::Unavailable;
        timeval timeout{0, 300000};
        ::setsockopt(socket.get(), SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(port);
        if (::connect(socket.get(), reinterpret_cast<sockaddr*>(&address),
                      sizeof(address)) != 0) {
            return SendResult::Unavailable;
        }
        const auto prefix = length_prefix(bytes->size());
        if (!write_all(socket.get(), prefix.data(), prefix.size()) ||
            !write_all(socket.get(), bytes->data(), bytes->size())) {
            return SendResult::Unavailable;
        }
        return SendResult::Sent;
    }

    Socket listen_on(std::uint16_t port) {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) return Socket{};
        int reuse = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(port);
        if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
            ::listen(fd, 16) != 0) {
            ::close(fd);
            return Socket{};
        }
        return Socket{fd};
    }

    class Server {
    public:
        Server(std::unique_ptr<dkv::RaftNode> node,
               std::array<std::uint16_t, 3> ports, int listener)
            : node_(std::move(node)), ports_(ports), listener_(listener) {
            reset_election_timer();
        }

        int run() {
            std::cout << "node " << node_->id() << " listening on 127.0.0.1:"
                      << ports_[node_->id() - 1] << std::endl;
            while (running_) {
                const auto now = Clock::now();
                const auto deadline = node_->role() == dkv::RaftRole::Leader
                                          ? heartbeat_deadline_ : election_deadline_;
                const auto remaining =
                    std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
                const int wait_ms = remaining.count() <= 0 ? 0 :
                    static_cast<int>(std::min<std::int64_t>(remaining.count(), 1000));
                std::array<pollfd, 2> fds{{
                    {listener_, POLLIN, 0},
                    {console_open_ ? STDIN_FILENO : -1, POLLIN, 0}}};
                const int ready = ::poll(fds.data(), fds.size(), wait_ms);
                if (ready < 0 && errno != EINTR) {
                    std::cerr << "poll failed\n";
                    fatal_ = true;
                    break;
                }
                if (ready > 0 && (fds[0].revents & POLLIN)) receive_one();
                if (ready > 0 && (fds[1].revents & POLLIN)) read_console();
                if (ready > 0 && (fds[1].revents & (POLLHUP | POLLERR))) {
                    console_open_ = false;
                }
                if (!running_) break;
                const auto after = Clock::now();
                if (node_->role() == dkv::RaftRole::Leader &&
                    after >= heartbeat_deadline_) {
                    process(node_->on_heartbeat_timeout());
                    heartbeat_deadline_ = Clock::now() + 100ms;
                } else if (node_->role() != dkv::RaftRole::Leader &&
                           after >= election_deadline_) {
                    process(node_->on_election_timeout());
                    reset_election_timer();
                }
            }
            fail_pending();
            return fatal_ ? 1 : 0;
        }

    private:
        struct PendingClient {
            std::unique_ptr<Socket> connection;
            std::uint64_t term;
            std::optional<std::string> read_key;
        };

        void reset_election_timer() {
            election_deadline_ = Clock::now() +
                std::chrono::milliseconds(election_delay_(random_));
        }

        void process(const dkv::RaftNodeResult& result,
                     const std::optional<dkv::AppendEntries>& replied_to = std::nullopt) {
            expire_pending();
            const auto* actions = std::get_if<dkv::RaftNodeActions>(&result);
            if (!actions) {
                const auto error = std::get<dkv::RaftNodeError>(result);
                std::cerr << "Raft event error " << static_cast<int>(error) << '\n';
                if (error == dkv::RaftNodeError::PersistenceError ||
                    error == dkv::RaftNodeError::InconsistentState ||
                    error == dkv::RaftNodeError::TermExhausted ||
                    error == dkv::RaftNodeError::Stopped) {
                    fail_pending();
                    fatal_ = true;
                    running_ = false;
                }
                return;
            }
            if (actions->reset_election_timer) reset_election_timer();
            if (actions->schedule_heartbeat) heartbeat_deadline_ = Clock::now() + 100ms;
            if (actions->proposed_index) {
                std::cout << "proposed index " << *actions->proposed_index << std::endl;
            }
            for (const auto& entry : actions->applied) {
                std::cout << "applied index " << entry.index << std::endl;
                const auto pending = pending_clients_.find(entry.index);
                if (pending != pending_clients_.end()) {
                    if (pending->second.read_key) {
                        const auto value = node_->get(*pending->second.read_key);
                        send_client_read_reply(pending->second.connection->get(),
                            {value ? dkv::ClientReadStatus::Found
                                   : dkv::ClientReadStatus::NotFound,
                             pending->second.term, entry.index, node_->id(), value});
                    } else {
                        send_client_reply(pending->second.connection->get(),
                            {dkv::ClientWriteStatus::Committed,
                             pending->second.term, entry.index, node_->id()});
                    }
                    pending_clients_.erase(pending);
                }
            }
            for (const auto& outbound : actions->messages) {
                const auto port = ports_[outbound.peer_id - 1];
                const auto correlation =
                    std::holds_alternative<dkv::AppendEntriesReply>(outbound.message)
                        ? replied_to : std::nullopt;
                const auto sent = send_frame(
                    port, {node_->id(), outbound.message, correlation});
                if (sent == SendResult::InvalidMessage) {
                    std::cerr << "could not encode Raft message\n";
                    fail_pending();
                    fatal_ = true;
                    running_ = false;
                    return;
                }
                // An unavailable peer is retried by the next election or heartbeat.
            }
        }

        void fail_pending() {
            for (auto& [index, pending] : pending_clients_) {
                if (pending.read_key) {
                    send_client_read_reply(pending.connection->get(),
                        {dkv::ClientReadStatus::InternalError,
                         pending.term, index, node_->known_leader().value_or(0),
                         std::nullopt});
                } else {
                    send_client_reply(pending.connection->get(),
                        {dkv::ClientWriteStatus::OutcomeUnknown,
                         pending.term, index, node_->known_leader().value_or(0)});
                }
            }
            pending_clients_.clear();
        }

        void expire_pending() {
            for (auto it = pending_clients_.begin(); it != pending_clients_.end();) {
                if (node_->role() == dkv::RaftRole::Leader &&
                    it->second.term == node_->term()) {
                    ++it;
                    continue;
                }
                if (it->second.read_key) {
                    send_client_read_reply(it->second.connection->get(),
                        {dkv::ClientReadStatus::NotLeader, node_->term(), 0,
                         node_->known_leader().value_or(0), std::nullopt});
                } else {
                    send_client_reply(it->second.connection->get(),
                        {dkv::ClientWriteStatus::OutcomeUnknown, it->second.term,
                         it->first, node_->known_leader().value_or(0)});
                }
                it = pending_clients_.erase(it);
            }
        }

        void handle_client_write(Socket& connection,
                                 std::span<const std::byte> bytes) {
            const auto decoded = dkv::decode_client_request(bytes);
            const auto* request = std::get_if<dkv::ClientWriteRequest>(&decoded);
            if (!request) {
                send_client_reply(connection.get(),
                    {dkv::ClientWriteStatus::InvalidRequest, node_->term(), 0, 0});
                return;
            }
            if (node_->role() != dkv::RaftRole::Leader) {
                send_client_reply(connection.get(),
                    {dkv::ClientWriteStatus::NotLeader, node_->term(), 0,
                     node_->known_leader().value_or(0)});
                return;
            }
            if (pending_clients_.size() >= 1024) {
                send_client_reply(connection.get(),
                    {dkv::ClientWriteStatus::Busy, node_->term(), 0, node_->id()});
                return;
            }
            const auto term = node_->term();
            auto proposed = node_->propose(request->command);
            const auto* actions = std::get_if<dkv::RaftNodeActions>(&proposed);
            if (!actions || !actions->proposed_index) {
                const auto error = std::get_if<dkv::RaftNodeError>(&proposed);
                const auto status = error && *error == dkv::RaftNodeError::NotLeader
                    ? dkv::ClientWriteStatus::NotLeader
                    : error && *error == dkv::RaftNodeError::InvalidCommand
                        ? dkv::ClientWriteStatus::InvalidRequest
                        : dkv::ClientWriteStatus::InternalError;
                send_client_reply(connection.get(),
                    {status, node_->term(), 0, node_->known_leader().value_or(0)});
                if (error && status == dkv::ClientWriteStatus::InternalError) {
                    process(proposed);
                }
                return;
            }
            pending_clients_.emplace(*actions->proposed_index,
                PendingClient{std::make_unique<Socket>(connection.release()),
                              term, std::nullopt});
            process(proposed);
        }

        void handle_client_read(Socket& connection,
                                std::span<const std::byte> bytes) {
            const auto decoded = dkv::decode_client_read_request(bytes);
            const auto* request = std::get_if<dkv::ClientReadRequest>(&decoded);
            if (!request) {
                send_client_read_reply(connection.get(),
                    {dkv::ClientReadStatus::InvalidRequest, node_->term(), 0, 0,
                     std::nullopt});
                return;
            }
            if (node_->role() != dkv::RaftRole::Leader) {
                send_client_read_reply(connection.get(),
                    {dkv::ClientReadStatus::NotLeader, node_->term(), 0,
                     node_->known_leader().value_or(0), std::nullopt});
                return;
            }
            if (pending_clients_.size() >= 1024) {
                send_client_read_reply(connection.get(),
                    {dkv::ClientReadStatus::Busy, node_->term(), 0, node_->id(),
                     std::nullopt});
                return;
            }
            const auto term = node_->term();
            auto proposed = node_->read_barrier();
            const auto* actions = std::get_if<dkv::RaftNodeActions>(&proposed);
            if (!actions || !actions->proposed_index) {
                const auto error = std::get_if<dkv::RaftNodeError>(&proposed);
                const auto status = error && *error == dkv::RaftNodeError::NotLeader
                    ? dkv::ClientReadStatus::NotLeader
                    : dkv::ClientReadStatus::InternalError;
                send_client_read_reply(connection.get(),
                    {status, node_->term(), 0, node_->known_leader().value_or(0),
                     std::nullopt});
                if (error && status == dkv::ClientReadStatus::InternalError) {
                    process(proposed);
                }
                return;
            }
            pending_clients_.emplace(*actions->proposed_index,
                PendingClient{std::make_unique<Socket>(connection.release()),
                              term, request->key});
            process(proposed);
        }

        void receive_one() {
            Socket connection{::accept(listener_, nullptr, nullptr)};
            if (connection.get() < 0) return;
            const auto bytes = receive_frame(connection.get());
            if (!bytes) return;
            if (bytes->size() >= 2 && (*bytes)[1] == std::byte{5}) {
                handle_client_write(connection, *bytes);
                return;
            }
            if (bytes->size() >= 2 && (*bytes)[1] == std::byte{7}) {
                handle_client_read(connection, *bytes);
                return;
            }
            const auto decoded = dkv::decode_raft_envelope(*bytes);
            const auto* envelope = std::get_if<dkv::RaftEnvelope>(&decoded);
            if (!envelope) return;
            const auto sender = envelope->sender_id;
            if (sender < 1 || sender > 3 || sender == node_->id()) return;
            if (const auto* value = std::get_if<dkv::RequestVote>(&envelope->message)) {
                process(node_->on_request_vote(sender, *value));
            } else if (const auto* value =
                           std::get_if<dkv::RequestVoteReply>(&envelope->message)) {
                process(node_->on_vote_reply(sender, *value));
            } else if (const auto* value =
                           std::get_if<dkv::AppendEntries>(&envelope->message)) {
                process(node_->on_append_entries(sender, *value), *value);
            } else if (const auto* value =
                           std::get_if<dkv::AppendEntriesReply>(&envelope->message)) {
                if (envelope->replied_to) {
                    process(node_->on_append_reply(
                        sender, *envelope->replied_to, *value));
                }
            }
        }

        void read_console() {
            std::array<char, 4096> bytes{};
            const auto count = ::read(STDIN_FILENO, bytes.data(), bytes.size());
            if (count == 0) {
                console_open_ = false;
                return;
            }
            if (count < 0) {
                if (errno != EINTR) console_open_ = false;
                return;
            }
            for (std::size_t i = 0; i < static_cast<std::size_t>(count); ++i) {
                const char character = bytes[i];
                if (discarding_line_) {
                    if (character == '\n') discarding_line_ = false;
                } else if (character == '\n') {
                    handle_console_line(console_line_);
                    console_line_.clear();
                    if (!running_) return;
                } else if (console_line_.size() < 1024 * 1024) {
                    console_line_.push_back(character);
                } else {
                    console_line_.clear();
                    discarding_line_ = true;
                    std::cout << "command too large\n";
                }
            }
        }

        void handle_console_line(const std::string& line) {
            std::istringstream input(line);
            std::string operation;
            std::string key;
            input >> operation;
            if (operation == "quit") {
                running_ = false;
            } else if (operation == "status") {
                const auto role = node_->role();
                std::cout << "term " << node_->term() << " role "
                          << (role == dkv::RaftRole::Leader ? "leader" :
                              role == dkv::RaftRole::Candidate ? "candidate" : "follower")
                          << " log " << node_->last_index()
                          << " commit " << node_->commit_index()
                          << " applied " << node_->last_applied() << std::endl;
            } else if (operation == "inspect" && input >> key) {
                const auto value = node_->get(key);
                std::cout << (value ? *value : "(not found)") << std::endl;
            } else if (operation == "put" && input >> key) {
                std::string value;
                std::getline(input >> std::ws, value);
                process(node_->propose({dkv::CommandType::Put, key, value}));
            } else if (operation == "delete" && input >> key) {
                process(node_->propose({dkv::CommandType::Delete, key, ""}));
            } else {
                std::cout << "commands: status, put KEY VALUE, inspect KEY, "
                             "delete KEY, quit\n";
            }
        }

        std::unique_ptr<dkv::RaftNode> node_;
        std::array<std::uint16_t, 3> ports_;
        int listener_;
        std::map<std::uint64_t, PendingClient> pending_clients_;
        std::mt19937 random_{std::random_device{}()};
        std::uniform_int_distribution<int> election_delay_{300, 600};
        Clock::time_point election_deadline_{};
        Clock::time_point heartbeat_deadline_{};
        std::string console_line_;
        bool discarding_line_{false};
        bool console_open_{true};
        bool running_{true};
        bool fatal_{false};
    };
}

int main(int argc, char** argv) {
    if (argc != 6) {
        std::cerr << "usage: dkv_node ID DATA_DIR PORT1 PORT2 PORT3\n";
        return 2;
    }
    unsigned id = 0;
    const std::string_view id_text = argv[1];
    const auto parsed = std::from_chars(
        id_text.data(), id_text.data() + id_text.size(), id);
    if (parsed.ec != std::errc{} ||
        parsed.ptr != id_text.data() + id_text.size() || id < 1 || id > 3) {
        std::cerr << "ID must be 1, 2, or 3\n";
        return 2;
    }
    std::array<std::uint16_t, 3> ports{};
    for (int i = 0; i < 3; ++i) {
        if (!parse_port(argv[i + 3], ports[i])) {
            std::cerr << "invalid port\n";
            return 2;
        }
    }
    if (ports[0] == ports[1] || ports[0] == ports[2] || ports[1] == ports[2]) {
        std::cerr << "ports must be distinct\n";
        return 2;
    }
    std::error_code error;
    std::filesystem::create_directories(argv[2], error);
    if (error) {
        std::cerr << "could not create data directory: " << error.message() << '\n';
        return 1;
    }
    std::array<std::uint64_t, 2> peers{};
    std::size_t at = 0;
    for (std::uint64_t peer = 1; peer <= 3; ++peer) {
        if (peer != id) peers[at++] = peer;
    }
    auto opened = dkv::RaftNode::open(id, peers, argv[2]);
    auto* node = std::get_if<std::unique_ptr<dkv::RaftNode>>(&opened);
    if (!node) {
        std::cerr << "could not open Raft storage\n";
        return 1;
    }
    auto listener = listen_on(ports[id - 1]);
    if (listener.get() < 0) {
        std::cerr << "could not listen on port " << ports[id - 1] << '\n';
        return 1;
    }
    return Server{std::move(*node), ports, listener.get()}.run();
}

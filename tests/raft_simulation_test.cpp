#include "dkv/persistent_log.hpp"
#include "dkv/raft_node.hpp"

#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace {
    constexpr std::uint64_t never = std::numeric_limits<std::uint64_t>::max();

    class Simulation {
    public:
        struct Message {
            std::uint64_t from;
            std::uint64_t to;
            dkv::RaftMessage body;
            std::optional<dkv::AppendEntries> replied_to;
            std::uint64_t due;
        };

        explicit Simulation(std::uint64_t seed) : seed_(seed), random_(seed) {
            auto name = (std::filesystem::temp_directory_path() /
                         "dkv-simulation-XXXXXX").string();
            std::vector<char> path(name.begin(), name.end());
            path.push_back('\0');
            if (char* created = ::mkdtemp(path.data())) root_ = created;
            require(!root_.empty(), "could not create temporary data directory");
            for (std::uint64_t id = 1; id <= 3; ++id) {
                std::filesystem::create_directory(directory(id));
                open_node(id);
            }
            check_safety();
        }

        ~Simulation() {
            for (auto& node : nodes_) node.reset();
            if (!root_.empty()) std::filesystem::remove_all(root_);
        }

        Simulation(const Simulation&) = delete;
        Simulation& operator=(const Simulation&) = delete;

        void require(bool condition, const std::string& explanation) const {
            if (!condition) {
                throw std::runtime_error("seed " + std::to_string(seed_) +
                                         ", tick " + std::to_string(now_) +
                                         ": " + explanation);
            }
        }

        void advance(std::uint64_t ticks) {
            for (std::uint64_t tick = 0; tick < ticks; ++tick) {
                ++now_;
                for (std::uint64_t id = 1; id <= 3; ++id) {
                    auto* node = nodes_[id - 1].get();
                    if (!node) continue;
                    if (node->role() == dkv::RaftRole::Leader &&
                        now_ >= heartbeat_deadlines_[id - 1]) {
                        process(id, node->on_heartbeat_timeout());
                    } else if (node->role() != dkv::RaftRole::Leader &&
                               now_ >= election_deadlines_[id - 1]) {
                        process(id, node->on_election_timeout());
                    }
                }
                deliver_due();
            }
        }

        void partition(std::uint64_t id) {
            for (std::uint64_t peer = 1; peer <= 3; ++peer) {
                if (peer == id) continue;
                links_[id - 1][peer - 1] = false;
                links_[peer - 1][id - 1] = false;
            }
        }

        void heal() {
            for (auto& row : links_) row.fill(true);
        }

        void crash(std::uint64_t id) {
            require(nodes_[id - 1] != nullptr, "crashed an offline node");
            nodes_[id - 1].reset();
            election_deadlines_[id - 1] = never;
            heartbeat_deadlines_[id - 1] = never;
            applied_seen_[id - 1] = 0;
            commit_seen_[id - 1] = 0;
            for (auto it = ready_reads_.begin(); it != ready_reads_.end();) {
                if (it->first == id) {
                    it = ready_reads_.erase(it);
                } else {
                    ++it;
                }
            }
            check_safety();
        }

        void restart(std::uint64_t id) {
            require(nodes_[id - 1] == nullptr, "restarted an online node");
            open_node(id);
            check_safety();
        }

        std::optional<Message> hold_message_from(std::uint64_t from) {
            auto it = messages_.begin();
            while (it != messages_.end() && it->from != from) ++it;
            if (it == messages_.end()) return std::nullopt;
            auto message = std::move(*it);
            messages_.erase(it);
            return message;
        }

        void release(Message message, std::uint64_t delay) {
            message.due = now_ + delay;
            messages_.push_back(std::move(message));
        }

        void duplicate_message_from(std::uint64_t from) {
            for (std::size_t i = 0; i < messages_.size(); ++i) {
                if (messages_[i].from != from) continue;
                auto duplicate = messages_[i];
                messages_.push_back(std::move(duplicate));
                return;
            }
            require(false, "nothing to duplicate from selected node");
        }

        void drop_message_from(std::uint64_t from) {
            auto it = messages_.begin();
            while (it != messages_.end() && it->from != from) ++it;
            require(it != messages_.end(), "nothing to drop from selected node");
            messages_.erase(it);
        }

        std::uint64_t start_read(std::uint64_t id) {
            require(nodes_[id - 1] != nullptr, "started read on an offline node");
            const auto result = nodes_[id - 1]->read_barrier();
            const auto* actions = std::get_if<dkv::RaftNodeActions>(&result);
            require(actions && actions->started_read_id, "leader rejected read");
            const auto read_id = *actions->started_read_id;
            process(id, result);
            return read_id;
        }

        bool read_completed(std::uint64_t id, std::uint64_t read_id) const {
            return ready_reads_.contains({id, read_id});
        }

        std::uint64_t propose(std::uint64_t id, const dkv::Command& command) {
            require(nodes_[id - 1] != nullptr, "proposed on an offline node");
            const auto result = nodes_[id - 1]->propose(command);
            const auto* actions = std::get_if<dkv::RaftNodeActions>(&result);
            require(actions && actions->proposed_index, "leader rejected proposal");
            const auto index = *actions->proposed_index;
            process(id, result);
            return index;
        }

        std::uint64_t leader_in(const std::vector<std::uint64_t>& ids) const {
            std::uint64_t leader = 0;
            for (const auto id : ids) {
                const auto* node = nodes_[id - 1].get();
                if (!node || node->role() != dkv::RaftRole::Leader) continue;
                require(leader == 0, "multiple leaders in selected nodes");
                leader = id;
            }
            require(leader != 0, "no leader in selected nodes");
            return leader;
        }

        const dkv::RaftNode& node(std::uint64_t id) const {
            require(nodes_[id - 1] != nullptr, "inspected an offline node");
            return *nodes_[id - 1];
        }

        std::optional<dkv::LogEntry> durable_entry(std::uint64_t id,
                                                   std::uint64_t index) const {
            auto opened = dkv::PersistentLog::open(directory(id) / "raft.log");
            auto* log = std::get_if<std::unique_ptr<dkv::PersistentLog>>(&opened);
            require(log && *log, "could not inspect durable log");
            return (*log)->entry_at(index);
        }

    private:
        std::filesystem::path directory(std::uint64_t id) const {
            return root_ / std::to_string(id);
        }

        static std::array<std::uint64_t, 2> peers(std::uint64_t id) {
            std::array<std::uint64_t, 2> result{};
            std::size_t next = 0;
            for (std::uint64_t peer = 1; peer <= 3; ++peer) {
                if (peer != id) result[next++] = peer;
            }
            return result;
        }

        void open_node(std::uint64_t id) {
            auto opened = dkv::RaftNode::open(id, peers(id), directory(id));
            auto* node = std::get_if<std::unique_ptr<dkv::RaftNode>>(&opened);
            require(node && *node, "could not open Raft node");
            nodes_[id - 1] = std::move(*node);
            reset_election_timer(id);
        }

        void reset_election_timer(std::uint64_t id) {
            const auto jitter = std::uniform_int_distribution<std::uint64_t>(0, 2)(random_);
            election_deadlines_[id - 1] = now_ + 10 + (id - 1) * 15 + jitter;
        }

        void process(std::uint64_t id, const dkv::RaftNodeResult& result,
                     const std::optional<dkv::AppendEntries>& replied_to = std::nullopt) {
            const auto* actions = std::get_if<dkv::RaftNodeActions>(&result);
            require(actions != nullptr, "Raft node returned an error");
            if (actions->reset_election_timer) reset_election_timer(id);
            if (actions->schedule_heartbeat) heartbeat_deadlines_[id - 1] = now_ + 3;

            auto applied = applied_seen_[id - 1];
            for (const auto& entry : actions->applied) {
                require(entry.index == ++applied, "entries applied out of order");
                require(entry.index <= nodes_[id - 1]->commit_index(),
                        "applied an entry before commitment");
            }
            require(applied == nodes_[id - 1]->last_applied(),
                    "last applied changed without an application event");
            applied_seen_[id - 1] = applied;

            for (const auto& read : actions->ready_reads) {
                require(nodes_[id - 1]->role() == dkv::RaftRole::Leader &&
                            nodes_[id - 1]->last_applied() >= read.index,
                        "read completed without an applied leader state");
                require(ready_reads_.emplace(id, read.id).second,
                        "read completed more than once");
            }

            for (const auto& outbound : actions->messages) {
                require(outbound.peer_id >= 1 && outbound.peer_id <= 3 &&
                            outbound.peer_id != id,
                        "Raft node addressed an invalid peer");
                std::optional<dkv::AppendEntries> correlation;
                if (std::holds_alternative<dkv::AppendEntriesReply>(outbound.message)) {
                    require(replied_to.has_value(), "reply lost its AppendEntries context");
                    correlation = replied_to;
                }
                const auto latency = std::uniform_int_distribution<std::uint64_t>(0, 2)(random_);
                messages_.push_back({id, outbound.peer_id, outbound.message,
                                     std::move(correlation), now_ + latency});
            }
            check_safety();
        }

        void deliver_due() {
            std::size_t delivered = 0;
            while (true) {
                std::vector<std::size_t> due;
                for (std::size_t i = 0; i < messages_.size(); ++i) {
                    if (messages_[i].due <= now_) due.push_back(i);
                }
                if (due.empty()) break;
                require(++delivered < 10000, "message delivery did not settle");
                const auto selected = std::uniform_int_distribution<std::size_t>(
                    0, due.size() - 1)(random_);
                auto message = std::move(messages_[due[selected]]);
                messages_.erase(messages_.begin() + static_cast<std::ptrdiff_t>(due[selected]));
                if (!nodes_[message.to - 1] ||
                    !links_[message.from - 1][message.to - 1]) {
                    continue;
                }

                dkv::RaftNodeResult result;
                if (const auto* vote = std::get_if<dkv::RequestVote>(&message.body)) {
                    result = nodes_[message.to - 1]->on_request_vote(message.from, *vote);
                } else if (const auto* reply =
                               std::get_if<dkv::RequestVoteReply>(&message.body)) {
                    result = nodes_[message.to - 1]->on_vote_reply(message.from, *reply);
                } else if (const auto* append =
                               std::get_if<dkv::AppendEntries>(&message.body)) {
                    result = nodes_[message.to - 1]->on_append_entries(message.from,
                                                                        *append);
                } else {
                    require(message.replied_to.has_value(),
                            "AppendEntries reply has no matching request");
                    result = nodes_[message.to - 1]->on_append_reply(
                        message.from, *message.replied_to,
                        std::get<dkv::AppendEntriesReply>(message.body));
                }
                const auto correlation = std::get_if<dkv::AppendEntries>(&message.body);
                process(message.to, result, correlation ?
                    std::optional<dkv::AppendEntries>{*correlation} : std::nullopt);
            }
        }

        void check_safety() {
            std::array<std::unique_ptr<dkv::PersistentLog>, 3> logs;
            for (std::uint64_t id = 1; id <= 3; ++id) {
                auto opened = dkv::PersistentLog::open(directory(id) / "raft.log");
                auto* log = std::get_if<std::unique_ptr<dkv::PersistentLog>>(&opened);
                require(log && *log, "could not reopen durable log");
                logs[id - 1] = std::move(*log);
            }

            for (std::uint64_t id = 1; id <= 3; ++id) {
                const auto* current = nodes_[id - 1].get();
                if (!current) continue;
                if (current->role() == dkv::RaftRole::Leader) {
                    const auto [it, inserted] = leaders_by_term_.emplace(current->term(), id);
                    require(inserted || it->second == id, "two leaders elected in one term");
                }
                require(current->last_applied() <= current->commit_index() &&
                        current->commit_index() <= current->last_index(),
                        "invalid applied/committed/log index ordering");
                require(current->commit_index() >= commit_seen_[id - 1],
                        "commit index moved backward");
                commit_seen_[id - 1] = current->commit_index();

                for (std::uint64_t index = 1; index <= current->commit_index(); ++index) {
                    const auto entry = logs[id - 1]->entry_at(index);
                    require(entry.has_value(), "committed entry absent from durable log");
                    const auto [it, inserted] = committed_.emplace(index, *entry);
                    require(inserted || it->second == *entry,
                            "different commands committed at the same index");
                }
            }
            for (const auto& [index, committed] : committed_) {
                unsigned replicas = 0;
                for (const auto& log : logs) {
                    if (log->entry_at(index) == committed) ++replicas;
                }
                require(replicas >= 2,
                        "committed entry was not durably stored by a majority");
            }
        }

        std::uint64_t seed_;
        std::mt19937_64 random_;
        std::filesystem::path root_;
        std::uint64_t now_{0};
        std::array<std::unique_ptr<dkv::RaftNode>, 3> nodes_;
        std::array<std::uint64_t, 3> election_deadlines_{};
        std::array<std::uint64_t, 3> heartbeat_deadlines_{};
        std::array<std::uint64_t, 3> applied_seen_{};
        std::array<std::uint64_t, 3> commit_seen_{};
        std::array<std::array<bool, 3>, 3> links_{{
            {{true, true, true}}, {{true, true, true}}, {{true, true, true}}}};
        std::vector<Message> messages_;
        std::map<std::uint64_t, std::uint64_t> leaders_by_term_;
        std::map<std::uint64_t, dkv::LogEntry> committed_;
        std::set<std::pair<std::uint64_t, std::uint64_t>> ready_reads_;
    };

    void partition_and_recovery(std::uint64_t seed) {
        Simulation simulation(seed);
        simulation.advance(60);
        const auto old_leader = simulation.leader_in({1, 2, 3});
        simulation.require(old_leader == 1, "first election chose an unexpected leader");
        simulation.require(simulation.node(1).commit_index() == 1,
                           "election no-op did not commit");

        const auto baseline = simulation.propose(
            old_leader, {dkv::CommandType::Put, "baseline", "kept"});
        simulation.duplicate_message_from(old_leader);
        simulation.drop_message_from(old_leader);
        simulation.advance(30);
        for (std::uint64_t id = 1; id <= 3; ++id) {
            simulation.require(simulation.node(id).commit_index() >= baseline &&
                               simulation.node(id).get("baseline") == "kept",
                               "baseline write did not reach every node");
        }

        const auto ghost = simulation.propose(
            old_leader, {dkv::CommandType::Put, "ghost", "must-disappear"});
        const auto ghost_entry = simulation.durable_entry(old_leader, ghost);
        simulation.require(ghost_entry && ghost_entry->command.key == "ghost",
                           "isolated proposal was not durably stored locally");
        auto delayed = simulation.hold_message_from(old_leader);
        simulation.require(delayed.has_value(), "no message available to delay");
        simulation.partition(old_leader);
        const auto isolated_read = simulation.start_read(old_leader);
        simulation.advance(90);
        simulation.require(simulation.node(old_leader).commit_index() < ghost &&
                           !simulation.node(old_leader).get("ghost") &&
                           !simulation.read_completed(old_leader, isolated_read),
                           "minority leader completed an isolated operation");

        const auto new_leader = simulation.leader_in({2, 3});
        simulation.require(simulation.node(new_leader).term() >
                               simulation.node(old_leader).term() &&
                           simulation.node(new_leader).commit_index() >= ghost,
                           "remaining majority did not elect and commit a new leader");
        const auto replacement = simulation.propose(
            new_leader, {dkv::CommandType::Put, "winner", "new-term"});
        simulation.advance(30);
        for (std::uint64_t id : {2ULL, 3ULL}) {
            simulation.require(simulation.node(id).commit_index() >= replacement &&
                               simulation.node(id).get("winner") == "new-term" &&
                               !simulation.node(id).get("ghost"),
                               "majority could not commit while old leader was isolated");
        }
        const auto majority_read = simulation.start_read(new_leader);
        simulation.advance(10);
        simulation.require(simulation.read_completed(new_leader, majority_read),
                           "new leader could not confirm a read with its majority");

        simulation.crash(old_leader);
        simulation.restart(old_leader);
        simulation.require(simulation.node(old_leader).last_index() == ghost &&
                           simulation.node(old_leader).commit_index() == 0,
                           "restart did not preserve the uncommitted suffix");
        simulation.heal();
        simulation.release(std::move(*delayed), 5);
        simulation.advance(100);
        const auto winning_entry = simulation.durable_entry(new_leader, ghost);
        simulation.require(winning_entry && winning_entry->term > 1,
                           "new leader did not replace the old suffix");
        for (std::uint64_t id = 1; id <= 3; ++id) {
            simulation.require(simulation.node(id).commit_index() >= replacement &&
                               simulation.node(id).get("baseline") == "kept" &&
                               simulation.node(id).get("winner") == "new-term" &&
                               !simulation.node(id).get("ghost") &&
                               simulation.durable_entry(id, ghost) == winning_entry,
                               "healed cluster did not converge on committed history");
        }
    }
}

int main(int argc, char** argv) {
    try {
        if (argc > 2) throw std::runtime_error("usage: raft_simulation_test [seed]");
        if (argc == 2) {
            std::uint64_t seed = 0;
            const std::string input = argv[1];
            const auto parsed = std::from_chars(input.data(), input.data() + input.size(), seed);
            if (parsed.ec != std::errc{} || parsed.ptr != input.data() + input.size()) {
                throw std::runtime_error("seed must be an unsigned integer");
            }
            partition_and_recovery(seed);
        } else {
            for (const auto seed : {1ULL, 7ULL, 42ULL, 12345ULL}) {
                partition_and_recovery(seed);
            }
        }
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

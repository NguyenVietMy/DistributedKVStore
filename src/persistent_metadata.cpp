#include "dkv/persistent_metadata.hpp"

#include "dkv/crc32.hpp"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <span>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace dkv {
    namespace {
        // DKVM | version | term (u64) | vote-present | candidate (u64) | CRC32.
        // Integers are stored in big-endian order; CRC32 covers all preceding bytes.
        constexpr std::size_t record_size = 26;
        constexpr std::size_t checksum_offset = record_size - sizeof(std::uint32_t);

        class UniqueFd {
        public:
            explicit UniqueFd(int fd = -1) : fd_(fd) {}
            ~UniqueFd() { if (fd_ >= 0) ::close(fd_); }
            UniqueFd(const UniqueFd&) = delete;
            UniqueFd& operator=(const UniqueFd&) = delete;
            int get() const noexcept { return fd_; }
            int release() noexcept { return std::exchange(fd_, -1); }
        private:
            int fd_;
        };

        std::filesystem::path parent_of(const std::filesystem::path& path) {
            return path.parent_path().empty() ? std::filesystem::path{"."} : path.parent_path();
        }

        void put_u64(std::span<std::byte> bytes, std::uint64_t value) {
            for (std::size_t i = 0; i < 8; ++i) {
                bytes[i] = static_cast<std::byte>(value >> (56 - i * 8));
            }
        }

        std::uint64_t get_u64(std::span<const std::byte> bytes) {
            std::uint64_t value = 0;
            for (std::size_t i = 0; i < 8; ++i) {
                value = (value << 8) | std::to_integer<std::uint8_t>(bytes[i]);
            }
            return value;
        }

        std::array<std::byte, record_size> encode(const RaftMetadata& state) {
            std::array<std::byte, record_size> bytes{};
            bytes[0] = std::byte{'D'};
            bytes[1] = std::byte{'K'};
            bytes[2] = std::byte{'V'};
            bytes[3] = std::byte{'M'};
            bytes[4] = std::byte{1};
            put_u64(std::span<std::byte>{bytes}.subspan(5, 8), state.current_term);
            bytes[13] = state.voted_for.has_value() ? std::byte{1} : std::byte{0};
            if (state.voted_for) {
                put_u64(std::span<std::byte>{bytes}.subspan(14, 8), *state.voted_for);
            }
            const auto checksum = crc32(std::span<const std::byte>{bytes}.first(checksum_offset));
            for (std::size_t i = 0; i < 4; ++i) {
                bytes[checksum_offset + i] = static_cast<std::byte>(checksum >> (24 - i * 8));
            }
            return bytes;
        }

        std::variant<RaftMetadata, PersistentMetadataOpenError> decode(
            const std::array<std::byte, record_size>& bytes) {
            if (bytes[0] != std::byte{'D'} || bytes[1] != std::byte{'K'} ||
                bytes[2] != std::byte{'V'} || bytes[3] != std::byte{'M'}) {
                return PersistentMetadataOpenError::CorruptRecord;
            }
            if (bytes[4] != std::byte{1}) {
                return PersistentMetadataOpenError::UnsupportedVersion;
            }
            std::uint32_t stored_checksum = 0;
            for (std::size_t i = 0; i < 4; ++i) {
                stored_checksum = (stored_checksum << 8) |
                    std::to_integer<std::uint8_t>(bytes[checksum_offset + i]);
            }
            if (stored_checksum != crc32(std::span<const std::byte>{bytes}.first(checksum_offset))) {
                return PersistentMetadataOpenError::CorruptRecord;
            }
            const auto term = get_u64(std::span<const std::byte>{bytes}.subspan(5, 8));
            const auto presence = bytes[13];
            if (presence != std::byte{0} && presence != std::byte{1}) {
                return PersistentMetadataOpenError::CorruptRecord;
            }
            const auto candidate = get_u64(std::span<const std::byte>{bytes}.subspan(14, 8));
            if ((presence == std::byte{0} && candidate != 0) ||
                (presence == std::byte{1} && term == 0)) {
                return PersistentMetadataOpenError::CorruptRecord;
            }
            return RaftMetadata{term, presence == std::byte{1}
                ? std::optional<std::uint64_t>{candidate} : std::nullopt};
        }

        bool read_exact(int fd, std::span<std::byte> output) {
            std::size_t read_count = 0;
            while (read_count < output.size()) {
                const auto n = ::read(fd, output.data() + read_count, output.size() - read_count);
                if (n > 0) {
                    read_count += static_cast<std::size_t>(n);
                } else if (n == -1 && errno == EINTR) {
                    continue;
                } else {
                    return false;
                }
            }
            return true;
        }

        bool write_exact(int fd, std::span<const std::byte> input) {
            std::size_t written = 0;
            while (written < input.size()) {
                const auto n = ::write(fd, input.data() + written, input.size() - written);
                if (n > 0) {
                    written += static_cast<std::size_t>(n);
                } else if (n == -1 && errno == EINTR) {
                    continue;
                } else {
                    return false;
                }
            }
            return true;
        }

        bool sync_file(int fd) {
            while (::fdatasync(fd) == -1) {
                if (errno != EINTR) return false;
            }
            return true;
        }

        bool sync_directory(int fd) {
            while (::fsync(fd) == -1) {
                if (errno != EINTR) return false;
            }
            return true;
        }
    }

    PersistentMetadata::PersistentMetadata(std::filesystem::path path, RaftMetadata state)
        : path_(std::move(path)), state_(state) {}

    PersistentMetadataOpenResult PersistentMetadata::open(const std::filesystem::path& path) {
        if (path.filename().empty()) return PersistentMetadataOpenError::IoError;

        UniqueFd directory{::open(parent_of(path).c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC)};
        if (directory.get() < 0) return PersistentMetadataOpenError::IoError;

        UniqueFd file{::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW)};
        if (file.get() < 0) {
            if (errno != ENOENT) return PersistentMetadataOpenError::IoError;
            return std::unique_ptr<PersistentMetadata>{new PersistentMetadata(path, {})};
        }
        struct stat file_info{};
        if (::fstat(file.get(), &file_info) == -1 || !S_ISREG(file_info.st_mode)) {
            return PersistentMetadataOpenError::IoError;
        }
        if (file_info.st_size < static_cast<off_t>(record_size)) {
            return PersistentMetadataOpenError::TruncatedRecord;
        }
        if (file_info.st_size != static_cast<off_t>(record_size)) {
            return PersistentMetadataOpenError::CorruptRecord;
        }
        std::array<std::byte, record_size> bytes{};
        if (!read_exact(file.get(), bytes)) return PersistentMetadataOpenError::IoError;
        auto decoded = decode(bytes);
        if (const auto* error = std::get_if<PersistentMetadataOpenError>(&decoded)) return *error;
        // An earlier process may have replaced the file but failed to sync its directory.
        // Certify the recovered state before the caller can use it for Raft decisions.
        if (!sync_file(file.get()) || !sync_directory(directory.get())) {
            return PersistentMetadataOpenError::IoError;
        }
        return std::unique_ptr<PersistentMetadata>{
            new PersistentMetadata(path, std::get<RaftMetadata>(decoded))};
    }

    const RaftMetadata& PersistentMetadata::state() const noexcept { return state_; }

    PersistentMetadataUpdateResult PersistentMetadata::advance_term(std::uint64_t term) {
        if (io_failed_) return PersistentMetadataUpdateResult::IoError;
        if (term < state_.current_term) return PersistentMetadataUpdateResult::StaleTerm;
        if (term == state_.current_term) return PersistentMetadataUpdateResult::Unchanged;
        return persist(RaftMetadata{term, std::nullopt});
    }

    PersistentMetadataUpdateResult PersistentMetadata::vote_for(std::uint64_t candidate) {
        if (io_failed_) return PersistentMetadataUpdateResult::IoError;
        if (state_.current_term == 0) return PersistentMetadataUpdateResult::InvalidTerm;
        if (state_.voted_for == candidate) return PersistentMetadataUpdateResult::Unchanged;
        if (state_.voted_for) return PersistentMetadataUpdateResult::VoteConflict;
        return persist(RaftMetadata{state_.current_term, candidate});
    }

    PersistentMetadataUpdateResult PersistentMetadata::persist(const RaftMetadata& next_state) {
        UniqueFd directory{::open(parent_of(path_).c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC)};
        if (directory.get() < 0) return PersistentMetadataUpdateResult::IoError;

        const auto template_path = path_.string() + ".tmp.XXXXXX";
        std::vector<char> temp_name(template_path.begin(), template_path.end());
        temp_name.push_back('\0');
        UniqueFd temp{::mkostemp(temp_name.data(), O_CLOEXEC)};
        if (temp.get() < 0) return PersistentMetadataUpdateResult::IoError;
        const auto discard_temp = [&] { ::unlink(temp_name.data()); };

        const auto bytes = encode(next_state);
        if (!write_exact(temp.get(), bytes) || !sync_file(temp.get())) {
            discard_temp();
            return PersistentMetadataUpdateResult::IoError;
        }
        if (::close(temp.release()) == -1) {
            discard_temp();
            return PersistentMetadataUpdateResult::IoError;
        }
        if (::rename(temp_name.data(), path_.c_str()) == -1) {
            discard_temp();
            io_failed_ = true;
            return PersistentMetadataUpdateResult::IoError;
        }
        if (!sync_directory(directory.get())) {
            io_failed_ = true;
            return PersistentMetadataUpdateResult::IoError;
        }
        state_ = next_state;
        return PersistentMetadataUpdateResult::Persisted;
    }
}

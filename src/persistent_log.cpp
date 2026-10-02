#include "dkv/persistent_log.hpp"
#include <unistd.h>
#include <cstdint>
#include <fcntl.h>
#include <utility>
#include <sys/stat.h>
#include <cerrno>
#include <cstddef>
#include <span>
#include "dkv/log_entry_codec.hpp"
#include <vector>
#include <limits>
#include <type_traits>

namespace dkv {
    namespace {
        bool read_exact(int file_descriptor, std::span<std::byte> output){
            std::size_t total_read = 0;
            while(total_read < output.size()){
                const ssize_t bytes_read = ::pread(
                    file_descriptor,
                    output.data() + total_read,
                    output.size() - total_read,
                    static_cast<off_t>(total_read)
                );
                if (bytes_read > 0) {
                    total_read += static_cast<std::size_t>(bytes_read);
                } else if (bytes_read == 0) {
                    return false;
                } else if (errno == EINTR) {
                    continue;
                } else {
                    return false;
                }
            }
            return true;
        }
        bool write_exact(int file_descriptor, std::span<const std::byte> input, std::uint64_t start_offset){
            std::size_t total_written = 0;
            while(total_written < input.size()){
                const ssize_t bytes_written = ::pwrite(
                    file_descriptor,
                    input.data() + total_written,
                    input.size() - total_written,
                    static_cast<off_t>(start_offset + total_written)
                );
                if(bytes_written > 0){
                    total_written+= static_cast<std::size_t>(bytes_written);
                }else if(bytes_written == -1 && errno == EINTR){
                    continue;
                }else{
                    return false;
                }
            }
            return true;
        }

        bool sync_file(int fd){
            for (;;) {
                if (::fdatasync(fd) == 0) return true;
                if (errno != EINTR) return false;
            }
        }
        bool sync_descriptor(int fd) {
            for (;;) {
                if (::fsync(fd) == 0) return true;
                if (errno != EINTR) return false;
            }
        }
        bool restore_file_size(int fd, off_t size) {
            while (::ftruncate(fd, size) == -1) {
                if (errno != EINTR) return false;
            }
            return sync_file(fd);
        }
    }
    PersistentLog::PersistentLog(int file_descriptor)
        : file_descriptor_(file_descriptor) {}
    
    PersistentLog::~PersistentLog(){
        if(file_descriptor_ >=0){
            ::close(file_descriptor_);
        }
    }

    std::uint64_t PersistentLog::last_index() const noexcept {
        return static_cast<std::uint64_t>(entries_.size());
    }

    std::uint64_t PersistentLog::last_term() const noexcept {
        if(entries_.empty()){
            return 0;
        }
        return entries_.back().log_entry.term;
    }

    std::optional<LogEntry> PersistentLog::entry_at(std::uint64_t index) const{
        std::uint64_t latest_index = last_index();
        if(index == 0 || index > latest_index){
            return std::nullopt;
        }
        return entries_[index - 1].log_entry;
    }

    PersistentLogOpenResult PersistentLog::open(const std::filesystem::path& path){
        int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
        if(fd == -1){
            return PersistentLogOpenError::IoError;
        }
        std::unique_ptr<PersistentLog> log{
            new PersistentLog(fd)
        };
        struct stat file_info{};

        if(::fstat(fd, &file_info) == -1){
            return PersistentLogOpenError::IoError;
        }
        if(file_info.st_size < 0){
            return PersistentLogOpenError::IoError;
        }

        // A newly created log's name is not durable until its parent directory
        // is synced. Do this on every open so a retry also certifies a file left
        // behind by an earlier failed directory sync.
        const auto parent = path.parent_path().empty()
            ? std::filesystem::path{"."} : path.parent_path();
        const int directory_fd = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (directory_fd == -1) {
            return PersistentLogOpenError::IoError;
        }
        const bool synced = sync_descriptor(fd) && sync_descriptor(directory_fd);
        const int close_result = ::close(directory_fd);
        if (!synced || close_result == -1) {
            return PersistentLogOpenError::IoError;
        }

        log->file_size_ = static_cast<std::uint64_t>(file_info.st_size);
        if(log->file_size_ == 0) {
            return PersistentLogOpenResult{std::move(log)};
        }

        const auto byte_count = static_cast<std::size_t>(log->file_size_);
        std::vector<std::byte> file_bytes(byte_count);
        if(!read_exact(fd, std::span<std::byte>{file_bytes})){
            return PersistentLogOpenError::IoError;
        }

        std::span<const std::byte> view{file_bytes};
        std::size_t offset = 0;
        while(offset < byte_count){
            auto remaining = view.subspan(offset);
            auto decode_result = decode_log_entry(remaining);
            const auto* codec_error = std::get_if<LogEntryCodecError>(&decode_result);
            if(codec_error != nullptr){
                if(*codec_error == LogEntryCodecError::InputTooShort || *codec_error == LogEntryCodecError::TruncatedRecord){
                    return PersistentLogOpenError::TruncatedRecord;
                }else{
                    return PersistentLogOpenError::CorruptRecord;
                }
            }
            auto decoded = std::get<DecodedLogEntry>(decode_result);
            if(decoded.bytes_consumed == 0 || decoded.bytes_consumed > remaining.size()){
                return PersistentLogOpenError::CorruptRecord;
            }
            if(decoded.entry.index != log->last_index() +1){
                return PersistentLogOpenError::NonSequentialIndex;
            }
            StoredEntry stored_entry{
                decoded.entry,
                offset
            };
            log->entries_.push_back(stored_entry);
            offset+= decoded.bytes_consumed;
        }
        return PersistentLogOpenResult{std::move(log)};
    }

    PersistentLogAppendResult PersistentLog::append(const LogEntry& entry){
        if (io_failed_) {
            return PersistentLogAppendResult::IoError;
        }
        if(entry.index != last_index()+1){
            return PersistentLogAppendResult::UnexpectedIndex;
        }
        const auto encoded = encode_log_entry(entry);
        const auto* bytes = std::get_if<std::vector<std::byte>>(&encoded);
        if(bytes == nullptr){
            return PersistentLogAppendResult::InvalidEntry;
        }
        const std::uint64_t start_offset = file_size_;
        const auto max_size = static_cast<std::uint64_t>(std::numeric_limits<off_t>::max());
        if (start_offset > max_size ||
            bytes->size() > max_size - start_offset) {
            return PersistentLogAppendResult::IoError;
        }
        static_assert(std::is_nothrow_move_constructible_v<StoredEntry>);
        StoredEntry stored_entry{entry, start_offset};
        entries_.reserve(entries_.size() + 1);
        if (!write_exact(file_descriptor_, std::span<const std::byte>{*bytes}, start_offset)) {
            if (!restore_file_size(file_descriptor_, static_cast<off_t>(start_offset))) {
                io_failed_ = true;
            }
            return PersistentLogAppendResult::IoError;
        }
        if (!sync_file(file_descriptor_)) {
            if (!restore_file_size(file_descriptor_, static_cast<off_t>(start_offset))) {
                io_failed_ = true;
            }
            return PersistentLogAppendResult::IoError;
        }
        entries_.push_back(std::move(stored_entry));
        file_size_+=bytes->size();
        return PersistentLogAppendResult::Appended;
    }
    PersistentLogTruncateResult PersistentLog::truncate_suffix(std::uint64_t first_index_to_remove){
        if (io_failed_) {
            return PersistentLogTruncateResult::IoError;
        }
        const std::uint64_t next_index = last_index() +1;
        if (first_index_to_remove == 0 || first_index_to_remove > next_index) {
            return PersistentLogTruncateResult::InvalidIndex;
        }
        if (first_index_to_remove == next_index) {
            return PersistentLogTruncateResult::Truncated; // Nothing to remove.
        }

        const std::uint64_t new_file_size = entries_[first_index_to_remove - 1].byte_offset;
        while(::ftruncate(file_descriptor_, static_cast<off_t>(new_file_size)) == -1){
            if(errno != EINTR){
                io_failed_ = true;
                return PersistentLogTruncateResult::IoError;
            }
        }
        if (!sync_file(file_descriptor_)) {
            io_failed_ = true;
            return PersistentLogTruncateResult::IoError;
        }
        entries_.resize(static_cast<std::size_t>(first_index_to_remove - 1));
        file_size_ = new_file_size;
        return PersistentLogTruncateResult::Truncated;
    }
}

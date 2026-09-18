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
}
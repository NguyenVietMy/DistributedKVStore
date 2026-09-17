#include "dkv/persistent_log.hpp"
#include <unistd.h>
#include <cstdint>
#include <fcntl.h>
#include <utility>

namespace dkv {
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
        int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC | 0644);
        if(fd == -1){
            return PersistentLogOpenError::IoError;
        }
        std::unique_ptr<PersistentLog> log{
            new PersistentLog(fd)
        };
        return PersistentLogOpenResult{std::move(log)};
    }
}
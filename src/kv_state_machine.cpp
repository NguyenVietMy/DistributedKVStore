#include "dkv/kv_state_machine.hpp"
#include "dkv/command.hpp"
namespace dkv {
    std::uint64_t KvStateMachine::last_applied() const noexcept{
        return last_applied_;
    }

    std::optional<std::string> KvStateMachine::get(const std::string& key) const {
        const auto it = data_.find(key);
        if(it == data_.end()){
            return std::nullopt;
        }
        return it->second;
    }

    const AppliedRequest* KvStateMachine::request_result(const RequestId& id) const {
        const auto it = applied_requests_.find(id);
        return it == applied_requests_.end() ? nullptr : &it->second;
    }

    ApplyResult KvStateMachine::apply(std::uint64_t log_index, const Command& command,
                                     std::uint64_t log_term){
        ApplyResult result{ApplyResult::Applied};
        if(log_index != last_applied_ + 1){
            result = ApplyResult::UnexpectedIndex;
            return result;
        }
        if ((command.type == CommandType::Delete && !command.value.empty()) ||
            (command.type == CommandType::NoOp &&
             (!command.key.empty() || !command.value.empty() || command.request_id)) ||
            (command.type != CommandType::Put && command.type != CommandType::Delete &&
             command.type != CommandType::NoOp)) {
            return ApplyResult::InvalidCommand;
        }
        if (command.request_id) {
            if (const auto* prior = request_result(*command.request_id)) {
                last_applied_ = log_index;
                return prior->command == command ? prior->result
                                                 : ApplyResult::RequestConflict;
            }
        }
        
        switch(command.type){
            case CommandType::Put:
                data_[command.key] = command.value;
                result = ApplyResult::Applied;
                break;
            
            case CommandType::Delete:
                if(!command.value.empty()){
                    result = ApplyResult::InvalidCommand;
                }else{
                    const auto removed = data_.erase(command.key);
                    if(removed == 1){
                        result = ApplyResult::Applied;
                    }else{
                        result = ApplyResult::KeyNotFound;
                    }
                }
                break;
            
            case CommandType::NoOp:
                if(!command.value.empty()|| !command.key.empty()){
                    result = ApplyResult::InvalidCommand;
                }else{
                    result = ApplyResult::Applied;
                }
                break;
            
            default:
                result = ApplyResult::InvalidCommand;
        }

        if(result == ApplyResult::InvalidCommand){
            return result;
        }
        last_applied_ = log_index;
        if (command.request_id) {
            applied_requests_.emplace(*command.request_id,
                                      AppliedRequest{command, result, log_index,
                                                     log_term});
        }
        return result;
    }
}

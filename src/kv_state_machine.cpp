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

    ApplyResult KvStateMachine::apply(std::uint64_t log_index, const Command& command){
        ApplyResult result{ApplyResult::Applied};
        if(log_index != last_applied_ + 1){
            result = ApplyResult::UnexpectedIndex;
            return result;
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
        return result;
    }
}
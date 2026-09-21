cmake_minimum_required(VERSION 3.20)

project(DistributedKVStore LANGUAGES CXX)

add_library(dkv_core 
    src/kv_state_machine.cpp 
    src/command_codec.cpp
    src/crc32.cpp
    src/log_entry_codec.cpp
    src/persistent_log.cpp
)

target_include_directories(dkv_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

target_compile_features(dkv_core PUBLIC cxx_std_20)

include(CTest)

if(BUILD_TESTING)
    add_executable(kv_state_machine_test tests/kv_state_machine_test.cpp)
    target_link_libraries(kv_state_machine_test PRIVATE dkv_core)
    add_test(NAME kv_state_machine_test COMMAND kv_state_machine_test)
    
    add_executable(command_codec_test tests/command_codec_test.cpp)
    target_link_libraries(command_codec_test PRIVATE dkv_core)
    add_test(NAME command_codec_test COMMAND command_codec_test)

    add_executable(crc32_test tests/crc32_test.cpp)
    target_link_libraries(crc32_test PRIVATE dkv_core)
    add_test(NAME crc32_test COMMAND crc32_test)

    add_executable(log_entry_codec_test tests/log_entry_codec_test.cpp)
    target_link_libraries(log_entry_codec_test PRIVATE dkv_core)
    add_test(NAME log_entry_codec_test COMMAND log_entry_codec_test)
    
    add_executable(persistent_log_test tests/persistent_log_test.cpp)
    target_link_libraries(persistent_log_test PRIVATE dkv_core)
    add_test(NAME persistent_log_test COMMAND persistent_log_test)

endif()
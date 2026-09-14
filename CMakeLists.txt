cmake_minimum_required(VERSION 3.20)

project(DistributedKVStore LANGUAGES CXX)

add_library(dkv_core 
    src/kv_state_machine.cpp 
    src/command_codec.cpp
)

target_include_directories(dkv_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

target_compile_features(dkv_core PUBLIC cxx_std_20)

include(CTest)

if(BUILD_TESTING)
    add_executable(kv_state_machine_test tests/kv_state_machine_test.cpp)
    target_link_libraries(kv_state_machine_test PRIVATE dkv_core)
    add_test(NAME kv_state_machine_test COMMAND kv_state_machine_test)

endif()
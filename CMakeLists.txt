cmake_minimum_required(VERSION 3.20)

project(DistributedKVStore LANGUAGES CXX)

add_library(dkv_core 
    src/kv_state_machine.cpp 
    src/command_codec.cpp
    src/crc32.cpp
    src/log_entry_codec.cpp
    src/persistent_log.cpp
    src/persistent_metadata.cpp
    src/raft_metadata_codec.cpp
    src/raft_vote.cpp
    src/raft_election.cpp
    src/raft_append.cpp
    src/raft_leader.cpp
    src/raft_apply.cpp
    src/raft_node.cpp
    src/raft_wire.cpp
    src/client_wire.cpp
)

target_include_directories(dkv_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

target_compile_features(dkv_core PUBLIC cxx_std_20)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_executable(dkv_node src/raft_server.cpp)
    target_link_libraries(dkv_node PRIVATE dkv_core)
    add_executable(dkv_client src/dkv_client.cpp)
    target_link_libraries(dkv_client PRIVATE dkv_core)
endif()

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
    
    add_executable(raft_metadata_codec_test tests/raft_metadata_codec_test.cpp)
    target_link_libraries(raft_metadata_codec_test PRIVATE dkv_core)
    add_test(NAME raft_metadata_codec_test COMMAND raft_metadata_codec_test)

    add_executable(persistent_metadata_test tests/persistent_metadata_test.cpp)
    target_link_libraries(persistent_metadata_test PRIVATE dkv_core)
    add_test(NAME persistent_metadata_test COMMAND persistent_metadata_test)

    add_executable(raft_election_test tests/raft_election_test.cpp)
    target_link_libraries(raft_election_test PRIVATE dkv_core)
    add_test(NAME raft_election_test COMMAND raft_election_test)

    add_executable(raft_append_test tests/raft_append_test.cpp)
    target_link_libraries(raft_append_test PRIVATE dkv_core)
    add_test(NAME raft_append_test COMMAND raft_append_test)

    add_executable(raft_leader_test tests/raft_leader_test.cpp)
    target_link_libraries(raft_leader_test PRIVATE dkv_core)
    add_test(NAME raft_leader_test COMMAND raft_leader_test)

    add_executable(raft_apply_test tests/raft_apply_test.cpp)
    target_link_libraries(raft_apply_test PRIVATE dkv_core)
    add_test(NAME raft_apply_test COMMAND raft_apply_test)

    add_executable(raft_node_test tests/raft_node_test.cpp)
    target_link_libraries(raft_node_test PRIVATE dkv_core)
    add_test(NAME raft_node_test COMMAND raft_node_test)

    add_executable(raft_simulation_test tests/raft_simulation_test.cpp)
    target_link_libraries(raft_simulation_test PRIVATE dkv_core)
    add_test(NAME raft_simulation_test COMMAND raft_simulation_test)

    add_executable(raft_wire_test tests/raft_wire_test.cpp)
    target_link_libraries(raft_wire_test PRIVATE dkv_core)
    add_test(NAME raft_wire_test COMMAND raft_wire_test)

    add_executable(client_wire_test tests/client_wire_test.cpp)
    target_link_libraries(client_wire_test PRIVATE dkv_core)
    add_test(NAME client_wire_test COMMAND client_wire_test)

    add_executable(raft_vote_test tests/raft_vote_test.cpp)
    target_link_libraries(raft_vote_test PRIVATE dkv_core)
    add_test(NAME raft_vote_test COMMAND raft_vote_test)

    add_executable(persistent_log_test tests/persistent_log_test.cpp)
    target_link_libraries(persistent_log_test PRIVATE dkv_core)
    add_test(NAME persistent_log_test COMMAND persistent_log_test)

    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        add_executable(persistent_metadata_fault_test tests/persistent_metadata_fault_test.cpp)
        target_link_libraries(persistent_metadata_fault_test PRIVATE dkv_core)
        target_link_options(persistent_metadata_fault_test PRIVATE
            "LINKER:--wrap=write"
            "LINKER:--wrap=fdatasync"
            "LINKER:--wrap=rename"
            "LINKER:--wrap=fsync"
        )
        add_test(NAME persistent_metadata_fault_test COMMAND persistent_metadata_fault_test)

        add_executable(persistent_log_append_test tests/persistent_log_append_test.cpp)
        target_link_libraries(persistent_log_append_test PRIVATE dkv_core)
        target_link_options(persistent_log_append_test PRIVATE
            "LINKER:--wrap=pwrite"
            "LINKER:--wrap=fdatasync"
            "LINKER:--wrap=fsync"
            "LINKER:--wrap=ftruncate"
        )
        add_test(NAME persistent_log_append_test COMMAND persistent_log_append_test)

        add_executable(persistent_log_truncate_test tests/persistent_log_truncate_test.cpp)
        target_link_libraries(persistent_log_truncate_test PRIVATE dkv_core)
        target_link_options(persistent_log_truncate_test PRIVATE
            "LINKER:--wrap=ftruncate"
            "LINKER:--wrap=fdatasync"
            "LINKER:--wrap=pwrite"
        )
        add_test(NAME persistent_log_truncate_test COMMAND persistent_log_truncate_test)

        find_package(Python3 COMPONENTS Interpreter)
        if(Python3_Interpreter_FOUND)
            add_test(NAME raft_server_test
                COMMAND ${Python3_EXECUTABLE}
                    ${CMAKE_CURRENT_SOURCE_DIR}/tests/raft_server_test.py
                    $<TARGET_FILE:dkv_node>)
            add_test(NAME client_server_test
                COMMAND ${Python3_EXECUTABLE}
                    ${CMAKE_CURRENT_SOURCE_DIR}/tests/client_server_test.py
                    $<TARGET_FILE:dkv_node>
                    $<TARGET_FILE:dkv_client>)
            add_test(NAME client_retry_test
                COMMAND ${Python3_EXECUTABLE}
                    ${CMAKE_CURRENT_SOURCE_DIR}/tests/client_retry_test.py
                    $<TARGET_FILE:dkv_client>)
            add_test(NAME raft_failover_test
                COMMAND ${Python3_EXECUTABLE}
                    ${CMAKE_CURRENT_SOURCE_DIR}/tests/raft_failover_test.py
                    $<TARGET_FILE:dkv_node>
                    $<TARGET_FILE:dkv_client>)
        endif()
    endif()

endif()

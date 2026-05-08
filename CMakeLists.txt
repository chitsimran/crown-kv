cmake_minimum_required(VERSION 3.20)
project(crown CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (WIN32)
    add_compile_definitions(_WIN32_WINNT=0x0600 WINVER=0x0600)
endif()

# ============================================================
# Dependencies
# ============================================================

# Let gRPC pull in protobuf itself — do NOT call find_package(Protobuf)
# separately, it causes target redefinition conflicts on Homebrew.
find_package(gRPC REQUIRED)

include(FetchContent)
set(FETCHCONTENT_UPDATES_DISCONNECTED ON)

find_package(nlohmann_json QUIET)
if (NOT nlohmann_json_FOUND)
    message(STATUS "nlohmann_json not found; fetching header-only dependency")
    FetchContent_Declare(
        nlohmann_json
        GIT_REPOSITORY https://github.com/nlohmann/json.git
        GIT_TAG v3.11.3
    )
    FetchContent_MakeAvailable(nlohmann_json)
endif()

# Convenient alias
set(GRPC_PLUGIN $<TARGET_FILE:gRPC::grpc_cpp_plugin>)

# ============================================================
# Proto code generation
# ============================================================

set(PROTO_DIR     "${CMAKE_CURRENT_SOURCE_DIR}/proto")
set(PROTO_FILE    "${PROTO_DIR}/replication.proto")
set(PROTO_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/proto_gen")

file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

set(PROTO_SRCS "${PROTO_GEN_DIR}/replication.pb.cc")
set(PROTO_HDRS "${PROTO_GEN_DIR}/replication.pb.h")
set(GRPC_SRCS  "${PROTO_GEN_DIR}/replication.grpc.pb.cc")
set(GRPC_HDRS  "${PROTO_GEN_DIR}/replication.grpc.pb.h")

add_custom_command(
    OUTPUT  ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
    COMMAND ${CMAKE_COMMAND} -E make_directory "${PROTO_GEN_DIR}"
    COMMAND protobuf::protoc
    ARGS
        --proto_path "${PROTO_DIR}"
        --experimental_allow_proto3_optional
        --cpp_out "${PROTO_GEN_DIR}"
        --grpc_out "${PROTO_GEN_DIR}"
        --plugin=protoc-gen-grpc=${GRPC_PLUGIN}
        "${PROTO_FILE}"
    DEPENDS ${PROTO_FILE} protobuf::protoc gRPC::grpc_cpp_plugin
    COMMENT "Generating protobuf and gRPC sources from replication.proto"
    VERBATIM
)

add_library(replication STATIC
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)
target_include_directories(replication PUBLIC ${PROTO_GEN_DIR})
target_link_libraries(replication PUBLIC
    gRPC::grpc++
)

# ============================================================
# Chain replication executable
# ============================================================
# add_library(replication STATIC
#     src/node/node.cpp
#     src/replication/replication_strategy.cpp
#     src/replication/common/chain_style_replication_support.cpp
#     src/replication/chain/chain_replication.cpp
#     src/replication/craq/craq_replication.cpp
#     src/replication/crown/crown_replication.cpp
# )

target_include_directories(replication PUBLIC
    ${CMAKE_SOURCE_DIR}/src
    ${PROTO_GEN_DIR}
)

# ============================================================
# Server executable
# ============================================================

add_executable(server
    src/server/server.cpp
    src/kv_store/kv_store.cpp
    src/replication/common/replication_common.cpp
    src/replication/chain/chain_replication.cpp
)
target_include_directories(server PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${PROTO_GEN_DIR}
)
target_link_libraries(server PRIVATE
    replication
)

# ============================================================
# Client executable
# ============================================================

add_executable(client
    src/client/client.cpp
)
target_include_directories(client PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${PROTO_GEN_DIR}
)
target_link_libraries(client PRIVATE
    replication
    nlohmann_json::nlohmann_json
)

# ============================================================
# Metadata store executable
# ============================================================

add_executable(metadata_store
    src/metadata_store/metadata_store.cpp
)
target_include_directories(metadata_store PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${PROTO_GEN_DIR}
)
target_link_libraries(metadata_store PRIVATE
    replication
)

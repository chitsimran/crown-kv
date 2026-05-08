#pragma once

#include "replication.pb.h"
#include "replication.grpc.pb.h"

#include <cstdint>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <optional>
#include <string>

namespace replication_common {

inline uint64_t fnv1a_64(const std::string& key) {
    uint64_t hash = 14695981039346656037ULL;
    for (unsigned char c : key) {
        hash ^= c;
        hash *= 1099511628211ULL;
    }
    return hash;
}

inline int head_index(const std::string& key, int ring_size) {
    if (ring_size <= 0) {
        return 0;
    }
    return static_cast<int>(fnv1a_64(key) % static_cast<uint64_t>(ring_size));
}

inline int tail_index(const std::string& key, int ring_size) {
    if (ring_size <= 0) {
        return 0;
    }
    uint64_t base = fnv1a_64(key) % static_cast<uint64_t>(ring_size);
    return static_cast<int>((base + ring_size - 1) % ring_size);
}

inline std::string address_from_node(const replication::NodeInfo& node) {
    return node.address() + ":" + std::to_string(node.port());
}

inline std::shared_ptr<replication::ReplicationService::Stub> make_replication_stub(
    const replication::NodeInfo& node) {
    auto channel = grpc::CreateChannel(address_from_node(node),
                                       grpc::InsecureChannelCredentials());
    return std::shared_ptr<replication::ReplicationService::Stub>(
        replication::ReplicationService::NewStub(channel).release());
}

inline bool parse_host_port(const std::string& input, std::string* host, int* port) {
    auto pos = input.rfind(':');
    if (pos == std::string::npos || pos == 0 || pos + 1 >= input.size()) {
        return false;
    }
    *host = input.substr(0, pos);
    try {
        *port = std::stoi(input.substr(pos + 1));
    } catch (...) {
        return false;
    }
    return true;
}

} // namespace replication_common

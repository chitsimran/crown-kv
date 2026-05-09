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

// Default channel arguments for every gRPC channel created in this codebase.
// Tuned for many small concurrent RPCs (forward + ack traffic on the chain):
//  - higher max concurrent streams to avoid HTTP/2 head-of-line blocking
//  - keepalive so idle channels don't get killed under load
grpc::ChannelArguments default_channel_args();

// Returns a cached channel for a given address. Channels are thread-safe; sharing
// them across stubs avoids reopening TCP/HTTP2 connections per call.
std::shared_ptr<grpc::Channel> get_or_create_channel(const std::string& address);

// Returns a cached ReplicationService stub. Safe to call from any thread.
std::shared_ptr<replication::ReplicationService::Stub> make_replication_stub(
    const replication::NodeInfo& node);
std::shared_ptr<replication::ReplicationService::Stub> make_replication_stub(
    const std::string& address);

// Returns a cached ClientAckService stub keyed by the client_addr in PutRequest.
std::shared_ptr<replication::ClientAckService::Stub> get_client_ack_stub(
    const std::string& client_addr);

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

// Toggle for verbose per-RPC logging in the replication layer. Off by default.
void set_verbose_logging(bool enabled);
bool verbose_logging_enabled();

} // namespace replication_common

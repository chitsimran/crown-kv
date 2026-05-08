#pragma once

#include "replication.pb.h"
#include "replication.grpc.pb.h"
#include "../kv_store/kv_store.h"
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>

using replication::GetResponse;
using replication::PutRequest;
using replication::PutResponse;
using replication::ReplicationService;

class Replication {
public:
    KVStore kv_store;
    
    // passed on by server in constructor // key -> head node
    std::unordered_map<char, std::shared_ptr<ReplicationService::Stub>> key_mapping;

    // passed on by server in constructor // key -> tail node
    std::unordered_map<char, std::shared_ptr<ReplicationService::Stub>> key_tail_mapping;

    std::unordered_map<int64_t, PutRequest> pending_acks; // request_id -> request
    std::mutex pending_mutex_;

    void add_to_pending_acks(PutRequest request);

    // asynchronously forward the put request to the next node, and add it to pending_acks until you receive an ack for it
    void forward_put(PutRequest request, const std::shared_ptr<ReplicationService::Stub>& next_stub);

    // if prev_stub is null, send ack to the client (source_ip in the request), else send ack to the prev node
    void send_ack(int64_t request_id, const std::shared_ptr<ReplicationService::Stub>& prev_stub);

    virtual PutResponse handle_put(PutRequest request) = 0;

    // in case of craq and crown check with Tail the latest version if you have a dirty version
    virtual GetResponse handle_get(std::string key) = 0;

    virtual void handle_ack(int64_t request_id) = 0;
};

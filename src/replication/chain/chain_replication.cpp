#include "chain_replication.h"

#include <grpcpp/grpcpp.h>

using replication::GetResponse;
using replication::NodeInfo;
using replication::PutResponse;

namespace {

std::shared_ptr<ReplicationService::Stub> BuildStub(
    const std::vector<NodeInfo>& membership, int index) {
    if (membership.empty() || index < 0 || index >= static_cast<int>(membership.size())) {
        return nullptr;
    }
    return replication_common::make_replication_stub(membership[index]);
}

} // namespace

void ChainReplication::update_membership(const std::vector<NodeInfo>& membership,
                                         const std::string& node_id) {
    int ring_size = static_cast<int>(membership.size());
    int node_index = 0;
    for (int i = 0; i < ring_size; ++i) {
        if (membership[i].node_id() == node_id) {
            node_index = i;
            break;
        }
    }

    std::shared_ptr<ReplicationService::Stub> prev_stub;
    std::shared_ptr<ReplicationService::Stub> next_stub;
    if (ring_size > 1) {
        int prev_index = (node_index + ring_size - 1) % ring_size;
        int next_index = (node_index + 1) % ring_size;
        prev_stub = BuildStub(membership, prev_index);
        next_stub = BuildStub(membership, next_index);
    }

    std::lock_guard<std::mutex> lock(ring_mutex_);
    membership_ = membership;
    ring_size_ = ring_size;
    node_index_ = node_index;
    prev_stub_ = prev_stub;
    next_stub_ = next_stub;
}

PutResponse ChainReplication::handle_put(PutRequest request) {
    PutResponse response;
    int ring_size = 0;
    int node_index = 0;
    std::shared_ptr<ReplicationService::Stub> next_stub;
    std::shared_ptr<ReplicationService::Stub> prev_stub;
    std::vector<NodeInfo> membership;
    {
        std::lock_guard<std::mutex> lock(ring_mutex_);
        ring_size = ring_size_;
        node_index = node_index_;
        membership = membership_;
        next_stub = next_stub_;
        prev_stub = prev_stub_;
    }

    if (ring_size == 0) {
        response.set_success(false);
        response.set_error("NO_MEMBERSHIP");
        return response;
    }

    int head_index = 0;
    int tail_index = ring_size - 1;

    if (request.version() == 0) {
        if (node_index != head_index) {
            response.set_success(false);
            std::string error = "WRONG_NODE";
            if (head_index >= 0 && head_index < static_cast<int>(membership.size())) {
                error += ":" + replication_common::address_from_node(membership[head_index]);
            }
            response.set_error(error);
            return response;
        }

        uint64_t assigned_version = kv_store.put(request.key(), request.value(), std::nullopt);
        request.set_version(assigned_version);
        response.set_success(true);
        response.set_version(assigned_version);

        if (ring_size == 1) {
            // Single node is both head and tail; only send to client, not backwards to self
            kv_store.mark_clean(request.key(), assigned_version);
            send_ack(request, nullptr);
            return response;
        }

        if (ring_size == 1 || !next_stub) {
            kv_store.mark_clean(request.key(), assigned_version);
            // Single-node ring: head == tail. prev_stub is null so this becomes a
            // CommitAck to the client.
            send_ack(request, prev_stub);
            return response;
        }

        if (next_stub) {
            forward_put(request, next_stub);
        }
        return response;
    }

    uint64_t latest_version = kv_store.get_latest_version(request.key());
    bool stale = request.version() <= latest_version;
    if (!stale) {
        kv_store.put(request.key(), request.value(), request.version());
    }

    if (ring_size == 1) {
        // Single node is both head and tail; only send to client, not backwards to self
        if (!stale) {
            kv_store.mark_clean(request.key(), request.version());
        }
        send_ack(request, nullptr);
        response.set_success(true);
        response.set_version(request.version());
        return response;
    }

    if (node_index == tail_index) {
        if (!stale) {
            kv_store.mark_clean(request.key(), request.version());
        }
        send_ack(request, prev_stub);
        send_ack(request, nullptr);
        response.set_success(true);
        response.set_version(request.version());
        return response;
    }

    if (next_stub) {
        forward_put(request, next_stub);
    }
    response.set_success(true);
    response.set_version(request.version());
    return response;
}

GetResponse ChainReplication::handle_get(std::string key) {
    GetResponse response;
    int ring_size = 0;
    int node_index = 0;
    {
        std::lock_guard<std::mutex> lock(ring_mutex_);
        ring_size = ring_size_;
        node_index = node_index_;
    }

    if (ring_size == 0) {
        response.set_error("NO_MEMBERSHIP");
        return response;
    }

    int tail_index = ring_size - 1;
    if (node_index != tail_index) {
        response.set_error("WRONG_NODE");
        return response;
    }

    auto value = kv_store.get(key);
    if (!value.has_value()) {
        response.set_error("NOT_FOUND");
        return response;
    }

    response.set_value(value.value());
    response.set_version(kv_store.get_latest_version(key));
    return response;
}

void ChainReplication::handle_ack(int64_t request_id) {
    PutRequest request;
    if (!find_pending_request(request_id, &request)) {
        return;
    }

    kv_store.mark_clean(request.key(), request.version());

    int ring_size = 0;
    int node_index = 0;
    std::shared_ptr<ReplicationService::Stub> prev_stub;
    {
        std::lock_guard<std::mutex> lock(ring_mutex_);
        ring_size = ring_size_;
        node_index = node_index_;
        prev_stub = prev_stub_;
    }

    if (ring_size == 0) {
        return;
    }

    int head_index = 0;
    if (node_index != head_index) {
        send_ack(request, prev_stub);
    }

    erase_pending_ack(request_id);
}

std::shared_ptr<ReplicationService::Stub> ChainReplication::get_next_stub() {
    std::lock_guard<std::mutex> lock(ring_mutex_);
    return next_stub_;
}

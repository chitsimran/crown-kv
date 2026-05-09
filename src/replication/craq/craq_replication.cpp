#include "craq_replication.h"

#include <grpcpp/grpcpp.h>

using replication::GetResponse;
using replication::NodeInfo;
using replication::PutResponse;
using replication::VersionQueryRequest;
using replication::VersionQueryResponse;

namespace {

std::shared_ptr<ReplicationService::Stub> BuildStub(
    const std::vector<NodeInfo>& membership, int index) {
    if (membership.empty() || index < 0 || index >= static_cast<int>(membership.size())) {
        return nullptr;
    }
    return replication_common::make_replication_stub(membership[index]);
}

} // namespace

CraqReplication::CleanState CraqReplication::get_committed_state(
    const std::string& key) {
    CleanState state;
    std::lock_guard<std::mutex> lock(KVStore::mutex_store_);
    auto it = KVStore::store_.find(key);
    if (it != KVStore::store_.end()) {
        state.version = it->second.verison;
        state.found = true;
    }
    return state;
}

bool CraqReplication::has_dirty(const std::string& key) {
    std::lock_guard<std::mutex> lock(KVStore::mutex_dirty_store_);
    auto it = KVStore::dirty_store_.find(key);
    return it != KVStore::dirty_store_.end() && !it->second.empty();
}

std::optional<Record> CraqReplication::get_dirty_record(const std::string& key,
                                                        uint64_t version) {
    std::lock_guard<std::mutex> lock(KVStore::mutex_dirty_store_);
    auto it = KVStore::dirty_store_.find(key);
    if (it == KVStore::dirty_store_.end()) {
        return std::nullopt;
    }
    auto rec_it = it->second.find(version);
    if (rec_it == it->second.end()) {
        return std::nullopt;
    }
    return rec_it->second;
}

void CraqReplication::set_epoch(uint64_t epoch) {
    epoch_.store(epoch);
}

void CraqReplication::update_membership(const std::vector<NodeInfo>& membership,
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

PutResponse CraqReplication::handle_put(PutRequest request) {
    PutResponse response;
    int ring_size = 0;
    int node_index = 0;
    std::vector<NodeInfo> membership;
    std::shared_ptr<ReplicationService::Stub> next_stub;
    std::shared_ptr<ReplicationService::Stub> prev_stub;
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

        if (node_index == tail_index || !next_stub) {
            kv_store.mark_clean(request.key(), assigned_version);
            add_to_pending_acks(request);
            if (prev_stub) {
                send_ack(request.request_id(), prev_stub);
            }
            send_ack(request.request_id(), nullptr);
            {
                std::lock_guard<std::mutex> lock(pending_mutex_);
                pending_acks.erase(request.request_id());
            }
            return response;
        }

        forward_put(request, next_stub);
        return response;
    }

    CleanState clean_state = get_committed_state(request.key());
    bool stale = request.version() <= clean_state.version;
    if (!stale) {
        kv_store.put(request.key(), request.value(), request.version());
    }

    if (node_index == tail_index || !next_stub) {
        if (!stale) {
            kv_store.mark_clean(request.key(), request.version());
        }
        add_to_pending_acks(request);
        if (prev_stub) {
            send_ack(request.request_id(), prev_stub);
        }
        send_ack(request.request_id(), nullptr);
        {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            pending_acks.erase(request.request_id());
        }
        response.set_success(true);
        response.set_version(request.version());
        return response;
    }

    forward_put(request, next_stub);
    response.set_success(true);
    response.set_version(request.version());
    return response;
}

GetResponse CraqReplication::handle_get(std::string key) {
    GetResponse response;
    int ring_size = 0;
    int node_index = 0;
    std::vector<NodeInfo> membership;
    {
        std::lock_guard<std::mutex> lock(ring_mutex_);
        ring_size = ring_size_;
        node_index = node_index_;
        membership = membership_;
    }

    if (ring_size == 0) {
        response.set_error("NO_MEMBERSHIP");
        return response;
    }

    if (!has_dirty(key)) {
        auto value = kv_store.get(key);
        if (!value.has_value()) {
            response.set_error("NOT_FOUND");
            return response;
        }
        CleanState clean_state = get_committed_state(key);
        response.set_value(value.value());
        response.set_version(clean_state.version);
        return response;
    }

    int tail_index = ring_size - 1;
    uint64_t committed_version = 0;
    if (node_index == tail_index) {
        committed_version = get_committed_state(key).version;
    } else {
        if (tail_index < 0 || tail_index >= static_cast<int>(membership.size())) {
            response.set_error("TAIL_UNAVAILABLE");
            return response;
        }
        auto tail_stub = replication_common::make_replication_stub(membership[tail_index]);
        VersionQueryRequest request;
        request.set_key(key);
        request.set_epoch(epoch_.load());
        VersionQueryResponse version_response;
        grpc::ClientContext context;
        grpc::Status status = tail_stub->VersionQuery(&context, request, &version_response);
        if (!status.ok()) {
            response.set_error("TAIL_UNAVAILABLE");
            return response;
        }
        if (!version_response.error().empty()) {
            response.set_error(version_response.error());
            return response;
        }
        committed_version = version_response.committed_version();
    }

    if (committed_version == 0) {
        response.set_error("NOT_FOUND");
        return response;
    }

    auto dirty_record = get_dirty_record(key, committed_version);
    if (dirty_record.has_value()) {
        response.set_value(dirty_record->value);
        response.set_version(committed_version);
        return response;
    }

    auto clean_value = kv_store.get(key);
    CleanState clean_state = get_committed_state(key);
    if (clean_value.has_value() && clean_state.version >= committed_version) {
        response.set_value(clean_value.value());
        response.set_version(clean_state.version);
        return response;
    }

    response.set_error("VERSION_NOT_FOUND");
    return response;
}

void CraqReplication::handle_ack(int64_t request_id) {
    PutRequest request;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        auto it = pending_acks.find(request_id);
        if (it == pending_acks.end()) {
            return;
        }
        request = it->second.request;
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
    if (node_index == head_index) {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_acks.erase(request_id);
        return;
    }

    send_ack(request_id, prev_stub);

    std::lock_guard<std::mutex> lock(pending_mutex_);
    pending_acks.erase(request_id);
}

std::shared_ptr<ReplicationService::Stub> CraqReplication::get_next_stub() {
    std::lock_guard<std::mutex> lock(ring_mutex_);
    return next_stub_;
}

VersionQueryResponse CraqReplication::handle_version_query(
    const VersionQueryRequest& request) {
    VersionQueryResponse response;
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
    CleanState clean_state = get_committed_state(request.key());
    response.set_committed_version(clean_state.version);
    if (!clean_state.found) {
        response.set_error("NOT_FOUND");
    }
    return response;
}

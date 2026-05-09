#pragma once

#include "replication.pb.h"
#include "replication.grpc.pb.h"
#include "../kv_store/kv_store.h"
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <vector>

using replication::GetResponse;
using replication::PutRequest;
using replication::PutResponse;
using replication::ReplicationService;

class Replication {
public:
    Replication();
    virtual ~Replication();

    struct PendingEntry {
        PutRequest request;
        std::chrono::steady_clock::time_point deadline;
    };

    KVStore kv_store;
    
    // passed on by server in constructor // key -> head node
    std::unordered_map<char, std::shared_ptr<ReplicationService::Stub>> key_mapping;

    // passed on by server in constructor // key -> tail node
    std::unordered_map<char, std::shared_ptr<ReplicationService::Stub>> key_tail_mapping;

    std::unordered_map<int64_t, PendingEntry> pending_acks; // request_id -> entry
    std::mutex pending_mutex_;
    std::function<void(const PutRequest&)> forward_failure_handler;

    void add_to_pending_acks(PutRequest request);
    void erase_pending_ack(int64_t request_id);

    size_t pending_count();
    std::vector<PutRequest> snapshot_pending_requests();
    std::vector<PutRequest> collect_expired_requests();

    // asynchronously forward the put request to the next node, and add it to pending_acks until you receive an ack for it
    void forward_put(PutRequest request, const std::shared_ptr<ReplicationService::Stub>& next_stub);

    // if prev_stub is null, send ack to the client (source_ip in the request), else send ack to the prev node
    void send_ack(int64_t request_id, const std::shared_ptr<ReplicationService::Stub>& prev_stub);

    void set_epoch(uint64_t epoch) { epoch_.store(epoch); }

    virtual PutResponse handle_put(PutRequest request) = 0;

    // in case of craq and crown check with Tail the latest version if you have a dirty version
    virtual GetResponse handle_get(std::string key) = 0;

    virtual void handle_ack(int64_t request_id) = 0;

    virtual std::shared_ptr<ReplicationService::Stub> get_next_stub() = 0;

protected:
    std::atomic<uint64_t> epoch_{0};

private:
    struct ForwardTask {
        PutRequest request;
        std::shared_ptr<ReplicationService::Stub> stub;
        std::function<void(const PutRequest&)> failure_handler;
    };

    void enqueue_forward_task(ForwardTask task);
    void forward_worker_loop();

    std::mutex forward_mutex_;
    std::condition_variable forward_cv_;
    std::deque<ForwardTask> forward_queue_;
    std::vector<std::thread> forward_workers_;
    bool forward_shutdown_ = false;
};

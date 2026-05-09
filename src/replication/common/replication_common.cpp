#include "replication_common.h"
#include "../replication.h"

#include <grpcpp/grpcpp.h>
#include <algorithm>
#include <iostream>
#include <mutex>
#include <sstream>

using replication::CommitAck;
using replication::CommitAckResponse;
using replication::PutResponse;
using replication::WriteAck;
using replication::WriteAckResponse;

namespace {

// Pending-ack retry deadline. Deliberately very conservative: retries should fire
// only when a write is truly stuck (e.g., the chain dropped a WriteAck during a
// reconfiguration). The head waits for an ack to traverse the entire ring, so the
// deadline must accommodate the slowest position; we use one value for all nodes.
constexpr auto kAckTimeout = std::chrono::milliseconds(300000); // 5 minutes
// gRPC deadline for individual ForwardPut RPCs. Wide enough that benchmark-induced
// slowness on the next node never trips DEADLINE_EXCEEDED.
constexpr auto kForwardRpcDeadline = std::chrono::milliseconds(60000); // 1 minute
constexpr size_t kForwardWorkerCount = 16;
constexpr size_t kMaxForwardQueueSize = 100000;
constexpr int kMaxRetryCount = 20;

std::mutex g_replication_log_mutex;

void LogReplicationEvent(const std::string& message) {
    std::lock_guard<std::mutex> lock(g_replication_log_mutex);
    std::cout << "[replication] " << message << std::endl;
}

} // namespace

Replication::Replication() {
    forward_workers_.reserve(kForwardWorkerCount);
    for (size_t i = 0; i < kForwardWorkerCount; ++i) {
        forward_workers_.emplace_back([this]() {
            forward_worker_loop();
        });
    }
}

Replication::~Replication() {
    {
        std::lock_guard<std::mutex> lock(forward_mutex_);
        forward_shutdown_ = true;
    }
    forward_cv_.notify_all();
    for (auto& worker : forward_workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void Replication::add_to_pending_acks(PutRequest request) {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    auto it = pending_acks.find(request.request_id());
    if (it != pending_acks.end()) {
        it->second.request = request;
        return;
    }
    PendingEntry entry;
    entry.request = request;
    entry.deadline = std::chrono::steady_clock::now() + kAckTimeout;
    entry.retry_count = 0;
    pending_acks.emplace(request.request_id(), std::move(entry));
}

void Replication::erase_pending_ack(int64_t request_id) {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    pending_acks.erase(request_id);
}

size_t Replication::pending_count() {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    return pending_acks.size();
}

std::vector<PutRequest> Replication::snapshot_pending_requests() {
    std::vector<PutRequest> snapshot;
    std::lock_guard<std::mutex> lock(pending_mutex_);
    snapshot.reserve(pending_acks.size());
    for (const auto& entry : pending_acks) {
        snapshot.push_back(entry.second.request);
    }
    std::sort(snapshot.begin(), snapshot.end(), [](const PutRequest& lhs,
                                                   const PutRequest& rhs) {
        if (lhs.key() == rhs.key()) {
            return lhs.version() < rhs.version();
        }
        return lhs.version() < rhs.version();
    });
    return snapshot;
}

std::vector<PutRequest> Replication::collect_expired_requests() {
    std::vector<PutRequest> expired;
    std::vector<int64_t> dropped_ids;
    auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        for (auto it = pending_acks.begin(); it != pending_acks.end(); ) {
            if (it->second.deadline > now) {
                ++it;
                continue;
            }
            if (it->second.retry_count >= kMaxRetryCount) {
                dropped_ids.push_back(it->first);
                it = pending_acks.erase(it);
                continue;
            }
            it->second.retry_count += 1;
            it->second.deadline = now + kAckTimeout;
            expired.push_back(it->second.request);
            ++it;
        }
    }
    if (!dropped_ids.empty()) {
        std::ostringstream out;
        out << "drop_orphan_pending count=" << dropped_ids.size()
            << " max_retries=" << kMaxRetryCount
            << " first_request_id=" << dropped_ids.front();
        LogReplicationEvent(out.str());
    }
    if (!expired.empty()) {
        std::ostringstream out;
        out << "retry_expired count=" << expired.size();
        LogReplicationEvent(out.str());
    }
    return expired;
}

void Replication::forward_put(PutRequest request,
                              const std::shared_ptr<ReplicationService::Stub>& next_stub) {
    if (!next_stub) {
        return;
    }
    request.set_epoch(epoch_.load());
    add_to_pending_acks(request);

    ForwardTask task;
    task.request = request;
    task.stub = next_stub;
    enqueue_forward_task(std::move(task));
}

void Replication::enqueue_forward_task(ForwardTask task) {
    {
        std::unique_lock<std::mutex> lock(forward_mutex_);
        forward_cv_.wait(lock, [this]() {
            return forward_shutdown_ || forward_queue_.size() < kMaxForwardQueueSize;
        });
        if (forward_shutdown_) {
            return;
        }
        forward_queue_.push_back(std::move(task));
    }
    forward_cv_.notify_one();
}

void Replication::forward_worker_loop() {
    while (true) {
        ForwardTask task;
        {
            std::unique_lock<std::mutex> lock(forward_mutex_);
            forward_cv_.wait(lock, [this]() {
                return forward_shutdown_ || !forward_queue_.empty();
            });
            if (forward_shutdown_ && forward_queue_.empty()) {
                return;
            }
            task = std::move(forward_queue_.front());
            forward_queue_.pop_front();
        }
        forward_cv_.notify_one();

        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + kForwardRpcDeadline);
        PutResponse response;
        grpc::Status status = task.stub->ForwardPut(&context, task.request, &response);
        if (!status.ok()) {
            std::ostringstream out;
            out << "forward_put_rpc_fail code=" << static_cast<int>(status.error_code())
                << " message=\"" << status.error_message() << "\""
                << " request_id=" << task.request.request_id()
                << " key=" << task.request.key()
                << " request_epoch=" << task.request.epoch();
            LogReplicationEvent(out.str());
        }
        // Failure/recovery is the metadata store's responsibility, derived from
        // heartbeats. Servers do not report peer failures based on RPC outcomes.
    }
}

void Replication::send_ack(int64_t request_id,
                           const std::shared_ptr<ReplicationService::Stub>& prev_stub) {
    PutRequest request;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        auto it = pending_acks.find(request_id);
        if (it == pending_acks.end()) {
            return;
        }
        request = it->second.request;
    }

    if (prev_stub) {
        WriteAck ack;
        ack.set_request_id(request.request_id());
        ack.set_key(request.key());
        ack.set_version(request.version());
        ack.set_epoch(request.epoch());

        WriteAckResponse response;
        grpc::ClientContext context;
        prev_stub->SendWriteAck(&context, ack, &response);
        return;
    }

    std::string host;
    int port = 0;
    if (!replication_common::parse_host_port(request.client_addr(), &host, &port)) {
        return;
    }
    auto channel = grpc::CreateChannel(request.client_addr(),
                                       grpc::InsecureChannelCredentials());
    auto stub = replication::ClientAckService::NewStub(channel);

    CommitAck ack;
    ack.set_key(request.key());
    ack.set_version(request.version());
    CommitAckResponse response;
    grpc::ClientContext context;
    stub->CommitAck(&context, ack, &response);
}

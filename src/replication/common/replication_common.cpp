#include "replication_common.h"
#include "../replication.h"

#include <grpcpp/grpcpp.h>
#include <algorithm>

using replication::CommitAck;
using replication::CommitAckResponse;
using replication::PutResponse;
using replication::WriteAck;
using replication::WriteAckResponse;

namespace {

constexpr auto kAckTimeout = std::chrono::milliseconds(5000);
constexpr size_t kForwardWorkerCount = 16;
constexpr size_t kMaxForwardQueueSize = 100000;

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
    pending_acks.emplace(request.request_id(),
                         PendingEntry{request, std::chrono::steady_clock::now() + kAckTimeout});
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
    auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(pending_mutex_);
    for (auto& entry : pending_acks) {
        if (entry.second.deadline <= now) {
            expired.push_back(entry.second.request);
            entry.second.deadline = now + kAckTimeout;
        }
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

    std::shared_ptr<ReplicationService::Stub> stub = next_stub;
    auto failure_handler = forward_failure_handler;
    enqueue_forward_task(ForwardTask{request, stub, failure_handler});
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
        context.set_deadline(std::chrono::system_clock::now() + kAckTimeout);
        PutResponse response;
        grpc::Status status = task.stub->ForwardPut(&context, task.request, &response);
        if (!status.ok() && task.failure_handler) {
            task.failure_handler(task.request);
        }
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

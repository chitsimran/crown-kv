#include "replication_common.h"
#include "../replication.h"

#include <grpcpp/grpcpp.h>
#include <atomic>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_map>

using replication::ClientAckService;
using replication::CommitAck;
using replication::CommitAckResponse;
using replication::PutResponse;
using replication::WriteAck;
using replication::WriteAckResponse;

namespace {

// Pending-ack retry deadline. Short enough that a stuck forward retries quickly,
// long enough that a healthy chain isn't spammed unnecessarily.
constexpr auto kAckTimeout = std::chrono::milliseconds(10000); // 10 seconds
// gRPC deadline for individual ForwardPut RPCs. Tight enough that a forward to a
// dead node fails fast (instead of holding a worker for a full minute), but wide
// enough to absorb benchmark-induced slowness on a healthy successor.
constexpr auto kForwardRpcDeadline = std::chrono::milliseconds(10000); // 10 seconds
// gRPC deadline for fire-and-forget ack RPCs (WriteAck/CommitAck). Long enough to
// survive transient stalls but short enough that slow consumers don't bloat the CQ.
constexpr auto kAckRpcDeadline = std::chrono::milliseconds(10000);
// 64 workers per node. Each forward is bounded by RPC latency to the successor
// (~ms), so 16 was capping CROWN at ~5K ops/s per link. With 64 we still fit
// comfortably under the channel's 1024 concurrent-stream limit and saturate
// successor processing instead of the forwarder pool.
constexpr size_t kForwardWorkerCount = 64;
constexpr size_t kMaxForwardQueueSize = 100000;
// After this many retries to a stuck successor, stop retrying. The entry stays
// in pending_acks; a subsequent Reconfigure (driven by the metadata store on
// failure detection) re-forwards through the new topology.
constexpr int kMaxRetryCount = 3;

std::atomic<bool> g_log_verbose{false};

std::mutex g_replication_log_mutex;

void LogReplicationEvent(const std::string& message) {
    if (!g_log_verbose.load(std::memory_order_relaxed)) {
        return;
    }
    std::lock_guard<std::mutex> lock(g_replication_log_mutex);
    std::cout << "[replication] " << message << std::endl;
}

// =============================================================================
// Channel and stub caches
// =============================================================================

std::mutex g_channel_cache_mu;
std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> g_channel_cache;

std::mutex g_repl_stub_cache_mu;
std::unordered_map<std::string,
                   std::shared_ptr<replication::ReplicationService::Stub>>
    g_repl_stub_cache;

std::mutex g_client_ack_stub_cache_mu;
std::unordered_map<std::string, std::shared_ptr<ClientAckService::Stub>>
    g_client_ack_stub_cache;

// =============================================================================
// Async ack pump: a single CompletionQueue with a worker thread that drains
// completed WriteAck/CommitAck RPCs. Ack traffic is fire-and-forget from the
// perspective of the caller — we only need to clean up the ClientContext and
// response objects when the call finishes.
// =============================================================================

struct AsyncAckCallBase {
    grpc::ClientContext context;
    grpc::Status status;
    virtual ~AsyncAckCallBase() = default;
};

struct WriteAckCall : AsyncAckCallBase {
    WriteAckResponse response;
    std::unique_ptr<grpc::ClientAsyncResponseReader<WriteAckResponse>> rpc;
};

struct CommitAckCall : AsyncAckCallBase {
    CommitAckResponse response;
    std::unique_ptr<grpc::ClientAsyncResponseReader<CommitAckResponse>> rpc;
};

class AsyncAckPump {
public:
    static AsyncAckPump& instance() {
        static AsyncAckPump pump;
        return pump;
    }

    grpc::CompletionQueue* queue() { return &cq_; }

private:
    AsyncAckPump() : worker_(&AsyncAckPump::run, this) {}

    ~AsyncAckPump() {
        cq_.Shutdown();
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    void run() {
        void* tag = nullptr;
        bool ok = false;
        while (cq_.Next(&tag, &ok)) {
            // The tag is a heap-allocated AsyncAckCallBase; just delete it.
            delete static_cast<AsyncAckCallBase*>(tag);
        }
    }

    grpc::CompletionQueue cq_;
    std::thread worker_;
};

void issue_async_write_ack(const std::shared_ptr<replication::ReplicationService::Stub>& stub,
                           const WriteAck& ack) {
    auto* call = new WriteAckCall();
    call->context.set_deadline(std::chrono::system_clock::now() + kAckRpcDeadline);
    call->rpc = stub->PrepareAsyncSendWriteAck(&call->context, ack,
                                               AsyncAckPump::instance().queue());
    call->rpc->StartCall();
    call->rpc->Finish(&call->response, &call->status, call);
}

void issue_async_commit_ack(const std::shared_ptr<ClientAckService::Stub>& stub,
                            const CommitAck& ack) {
    auto* call = new CommitAckCall();
    call->context.set_deadline(std::chrono::system_clock::now() + kAckRpcDeadline);
    call->rpc = stub->PrepareAsyncCommitAck(&call->context, ack,
                                            AsyncAckPump::instance().queue());
    call->rpc->StartCall();
    call->rpc->Finish(&call->response, &call->status, call);
}

} // namespace

// =============================================================================
// Public helpers (replication_common namespace)
// =============================================================================

namespace replication_common {

grpc::ChannelArguments default_channel_args() {
    grpc::ChannelArguments args;
    // HTTP/2 default is 100 concurrent streams; bumping this avoids ack/forward
    // traffic queueing behind each other on a shared channel.
    args.SetInt(GRPC_ARG_MAX_CONCURRENT_STREAMS, 1024);
    // Ping idle channels so they don't get killed under bursty load.
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 30000);
    args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5000);
    args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
    return args;
}

std::shared_ptr<grpc::Channel> get_or_create_channel(const std::string& address) {
    std::lock_guard<std::mutex> lock(g_channel_cache_mu);
    auto it = g_channel_cache.find(address);
    if (it != g_channel_cache.end()) {
        return it->second;
    }
    auto channel = grpc::CreateCustomChannel(address,
                                             grpc::InsecureChannelCredentials(),
                                             default_channel_args());
    g_channel_cache[address] = channel;
    return channel;
}

std::shared_ptr<replication::ReplicationService::Stub> make_replication_stub(
    const std::string& address) {
    {
        std::lock_guard<std::mutex> lock(g_repl_stub_cache_mu);
        auto it = g_repl_stub_cache.find(address);
        if (it != g_repl_stub_cache.end()) {
            return it->second;
        }
    }
    auto channel = get_or_create_channel(address);
    auto stub = std::shared_ptr<replication::ReplicationService::Stub>(
        replication::ReplicationService::NewStub(channel).release());
    std::lock_guard<std::mutex> lock(g_repl_stub_cache_mu);
    auto inserted = g_repl_stub_cache.emplace(address, stub);
    return inserted.first->second;
}

std::shared_ptr<replication::ReplicationService::Stub> make_replication_stub(
    const replication::NodeInfo& node) {
    return make_replication_stub(address_from_node(node));
}

std::shared_ptr<ClientAckService::Stub> get_client_ack_stub(
    const std::string& client_addr) {
    {
        std::lock_guard<std::mutex> lock(g_client_ack_stub_cache_mu);
        auto it = g_client_ack_stub_cache.find(client_addr);
        if (it != g_client_ack_stub_cache.end()) {
            return it->second;
        }
    }
    auto channel = get_or_create_channel(client_addr);
    auto stub = std::shared_ptr<ClientAckService::Stub>(
        ClientAckService::NewStub(channel).release());
    std::lock_guard<std::mutex> lock(g_client_ack_stub_cache_mu);
    auto inserted = g_client_ack_stub_cache.emplace(client_addr, stub);
    return inserted.first->second;
}

void set_verbose_logging(bool enabled) {
    g_log_verbose.store(enabled, std::memory_order_relaxed);
}

bool verbose_logging_enabled() {
    return g_log_verbose.load(std::memory_order_relaxed);
}

} // namespace replication_common

// =============================================================================
// Replication base class methods
// =============================================================================

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
    auto deadline = std::chrono::steady_clock::now() + kAckTimeout;
    std::lock_guard<std::mutex> lock(pending_mutex_);
    auto it = pending_acks.find(request.request_id());
    if (it != pending_acks.end()) {
        it->second.request = std::move(request);
        // Reset the deadline whenever the same entry is re-staged (e.g., a
        // Reconfigure-driven re-forward). Otherwise the entry could be marked
        // expired immediately after being re-issued.
        it->second.deadline = deadline;
        return;
    }
    PendingEntry entry;
    entry.request = std::move(request);
    entry.deadline = deadline;
    entry.retry_count = 0;
    pending_acks.emplace(entry.request.request_id(), std::move(entry));
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
    // Order is irrelevant: each request is forwarded independently and version
    // numbers carry the ordering information needed by downstream replicas.
    return snapshot;
}

std::vector<PutRequest> Replication::collect_expired_requests() {
    std::vector<PutRequest> expired;
    auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        for (auto& kv : pending_acks) {
            auto& entry = kv.second;
            if (entry.deadline > now) {
                continue;
            }
            // Cap retries. Beyond the limit we leave the entry untouched in the
            // map; Reconfigure will re-forward it through the new chain when
            // the metadata store detects the failure.
            if (entry.retry_count >= kMaxRetryCount) {
                continue;
            }
            entry.retry_count += 1;
            entry.deadline = now + kAckTimeout;
            expired.push_back(entry.request);
        }
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
    task.request = std::move(request);
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

void Replication::send_ack(const PutRequest& request,
                           const std::shared_ptr<ReplicationService::Stub>& prev_stub) {
    if (prev_stub) {
        WriteAck ack;
        ack.set_request_id(request.request_id());
        ack.set_key(request.key());
        ack.set_version(request.version());
        ack.set_epoch(request.epoch());
        issue_async_write_ack(prev_stub, ack);
        return;
    }

    // Going to the client. Validate the address shape first to avoid creating
    // bogus channels that would never connect.
    std::string host;
    int port = 0;
    if (!replication_common::parse_host_port(request.client_addr(), &host, &port)) {
        return;
    }
    auto stub = replication_common::get_client_ack_stub(request.client_addr());
    CommitAck ack;
    ack.set_key(request.key());
    ack.set_version(request.version());
    issue_async_commit_ack(stub, ack);
}

#pragma once

#include "replication.pb.h"
#include "replication.grpc.pb.h"
#include "../kv_store/kv_store.h"
#include <array>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
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
        int retry_count = 0;
    };

    KVStore kv_store;

    // passed on by server in constructor // key -> head node
    std::unordered_map<char, std::shared_ptr<ReplicationService::Stub>> key_mapping;

    // passed on by server in constructor // key -> tail node
    std::unordered_map<char, std::shared_ptr<ReplicationService::Stub>> key_tail_mapping;

    // Pending acks are sharded across kPendingShards stripes so concurrent
    // forward/handle_ack operations on different request_ids don't serialise on
    // a single mutex. This was the dominant per-node bottleneck under load:
    // the prior single-mutex design saw ~10K acquisitions/sec on the middle
    // node at 5K writes/sec, and contention there showed up as multi-second
    // p99 latency despite headroom in CPU/network.
    static constexpr size_t kPendingShards = 64;
    struct PendingShard {
        std::mutex mu;
        std::unordered_map<int64_t, PendingEntry> map;
    };
    std::array<PendingShard, kPendingShards> pending_shards;

    PendingShard& pending_shard_for(int64_t request_id) {
        return pending_shards[static_cast<uint64_t>(request_id) % kPendingShards];
    }

    void add_to_pending_acks(PutRequest request);
    void erase_pending_ack(int64_t request_id);
    // Copy the pending request for request_id into *out if present.
    // Returns true if an entry existed. Used by handle_ack paths so they can
    // run their post-ack work outside the stripe lock.
    bool find_pending_request(int64_t request_id, PutRequest* out);

    size_t pending_count();
    std::vector<PutRequest> snapshot_pending_requests();
    std::vector<PutRequest> collect_expired_requests();

    // asynchronously forward the put request to the next node, and add it to pending_acks until you receive an ack for it
    void forward_put(PutRequest request, const std::shared_ptr<ReplicationService::Stub>& next_stub);

    // Asynchronously forward without inserting into pending_acks. Used by
    // CROWN-chain-reads mode where there is no backward WriteAck cascade and
    // therefore no entry would ever be cleared. Caller accepts that a failed
    // forward leaves the write only partially replicated with no retry path —
    // the recovery story is the same as the old (fast) CROWN implementation:
    // none. Suitable for benchmarks; not for production.
    void forward_put_untracked(PutRequest request,
                               const std::shared_ptr<ReplicationService::Stub>& next_stub);

    // If prev_stub is null, fire a CommitAck to the client (request.client_addr()).
    // Otherwise, fire a WriteAck to the predecessor node. Both calls are async.
    void send_ack(const PutRequest& request,
                  const std::shared_ptr<ReplicationService::Stub>& prev_stub);

    void set_epoch(uint64_t epoch) { epoch_.store(epoch); }

    virtual PutResponse handle_put(PutRequest request) = 0;

    // in case of craq and crown check with Tail the latest version if you have a dirty version
    virtual GetResponse handle_get(std::string key) = 0;

    virtual void handle_ack(int64_t request_id) = 0;

    virtual std::shared_ptr<ReplicationService::Stub> get_next_stub() = 0;

protected:
    std::atomic<uint64_t> epoch_{0};

};

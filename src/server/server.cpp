#include "replication.grpc.pb.h"
#include "replication/chain/chain_replication.h"
#include "replication/craq/craq_replication.h"
#include "replication/crown/crown_replication.h"
#include <grpcpp/grpcpp.h>

#include <sys/resource.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using replication::HeartbeatRequest;
using replication::HeartbeatResponse;
using replication::GetRequest;
using replication::GetResponse;
using replication::MembershipRequest;
using replication::MembershipResponse;
using replication::MetadataService;
using replication::NodeInfo;
using replication::PutRequest;
using replication::PutResponse;
using replication::ReconfigureRequest;
using replication::ReconfigureResponse;
using replication::ReplicationService;
using replication::ServerStatsRequest;
using replication::ServerStatsResponse;
using replication::StateDumpRequest;
using replication::StateDumpResponse;
using replication::SyncCompleteRequest;
using replication::SyncCompleteResponse;
using replication::VersionQueryRequest;
using replication::VersionQueryResponse;
using replication::WriteAck;
using replication::WriteAckResponse;

namespace {

constexpr int kHeartbeatIntervalMs = 400;
constexpr int kRetryIntervalMs = 1000;

enum class NodeState {
    STARTING,
    NORMAL,
};

struct ServerState {
    std::mutex mutex;
    std::string node_id;
    uint64_t epoch = 0;
    std::string mode;
    std::vector<NodeInfo> membership;
    std::atomic<NodeState> state{NodeState::STARTING};
    // Lock-free reads for the hot RPC path. Updated inside state->mutex so
    // writers stay consistent; readers on the critical path skip the mutex.
    std::atomic<uint64_t> epoch_fast{0};
    std::atomic<int>      mode_fast{0}; // 0=none,1=CHAIN,2=CRAQ,3=CROWN
};

// Map mode string → mode_fast index (write-side only, not on hot path).
int ModeToFast(const std::string& mode) {
    if (mode == "CHAIN") return 1;
    if (mode == "CRAQ")  return 2;
    if (mode == "CROWN") return 3;
    return 0;
}

// Integer-indexed replication dispatch — avoids string comparison per RPC.
// Returns nullptr for unknown/unset mode (0).
constexpr const char* FastToModeStr(int m) {
    return m == 1 ? "CHAIN" : m == 2 ? "CRAQ" : m == 3 ? "CROWN" : "NONE";
}

// Verbose per-RPC logging is opt-in (--verbose). Default is silent — logging on
// every Put/ForwardPut/WriteAck under benchmark load is itself a hot-path cost.
std::atomic<bool> g_verbose_server_logging{false};

Replication* SelectReplicationForMode(const std::string& mode,
                                      ChainReplication* chain_replication,
                                      CraqReplication* craq_replication,
                                      CrownReplication* crown_replication);

// Install a new membership/epoch on every replication mode. If `epoch_advanced`
// is true (the local epoch actually moved forward), also re-forward any pending
// entries through the new chain — this is the recovery path for writes that
// were stuck behind a now-removed predecessor. We dedupe on epoch so concurrent
// triggers (Reconfigure RPC + heartbeat-driven RefreshMembership) don't both
// flood the queue with re-forwards.
inline void ApplyMembership(const std::vector<NodeInfo>& membership,
                            const std::string& node_id, const std::string& mode,
                            uint64_t epoch, bool epoch_advanced,
                            ChainReplication* chain, CraqReplication* craq,
                            CrownReplication* crown) {
    chain->update_membership(membership, node_id);
    chain->set_epoch(epoch);
    craq->update_membership(membership, node_id);
    craq->set_epoch(epoch);
    crown->update_membership(membership, node_id);
    crown->set_epoch(epoch);
    if (!epoch_advanced) {
        return;
    }
    Replication* replication = SelectReplicationForMode(mode, chain, craq, crown);
    if (!replication) {
        return;
    }
    auto next_stub = replication->get_next_stub();
    if (!next_stub) {
        return;
    }
    for (const auto& pending : replication->snapshot_pending_requests()) {
        replication->forward_put(pending, next_stub);
    }
}

std::mutex log_mutex;

void LogServerEvent(const std::string& event, const std::string& node_id,
                    const std::string& mode, uint64_t local_epoch,
                    const std::string& detail) {
    if (!g_verbose_server_logging.load(std::memory_order_relaxed)) {
        return;
    }
    std::lock_guard<std::mutex> lock(log_mutex);
    std::cout << "[" << event << "] node=" << node_id
              << " mode=" << mode
              << " local_epoch=" << local_epoch
              << " " << detail << std::endl;
}

class MetadataClient {
public:
    explicit MetadataClient(const std::string& address)
        : stub_(MetadataService::NewStub(
              replication_common::get_or_create_channel(address))) {}

    bool GetMembership(MembershipResponse* response) {
        MembershipRequest request;
        grpc::ClientContext context;
        grpc::Status status = stub_->GetMembership(&context, request, response);
        return status.ok();
    }

    bool SendHeartbeat(const std::string& node_id, uint64_t epoch, uint64_t* out_epoch) {
        HeartbeatRequest request;
        request.set_node_id(node_id);
        request.set_epoch(epoch);
        HeartbeatResponse response;
        grpc::ClientContext context;
        grpc::Status status = stub_->Heartbeat(&context, request, &response);
        if (!status.ok()) {
            return false;
        }
        *out_epoch = response.epoch();
        return response.alive();
    }

    bool SendSyncComplete(const std::string& node_id, uint64_t epoch) {
        SyncCompleteRequest request;
        request.set_node_id(node_id);
        request.set_epoch(epoch);
        SyncCompleteResponse response;
        grpc::ClientContext context;
        grpc::Status status = stub_->ReportSyncDone(&context, request, &response);
        return status.ok() && response.success();
    }

private:
    std::unique_ptr<MetadataService::Stub> stub_;
};

class NodeMetadataService final : public MetadataService::Service {
public:
    NodeMetadataService(ServerState* state, MetadataClient* metadata_client,
                                                ChainReplication* chain_replication,
                                                CraqReplication* craq_replication,
                                                CrownReplication* crown_replication)
                : state_(state),
                    metadata_client_(metadata_client),
                    chain_replication_(chain_replication),
                    craq_replication_(craq_replication),
                    crown_replication_(crown_replication) {}

    grpc::Status Reconfigure(grpc::ServerContext*, const ReconfigureRequest* request,
                             ReconfigureResponse* response) override {
        bool epoch_advanced = false;
        std::vector<NodeInfo> membership_copy;
        std::string node_id;
        std::string mode;
        bool needs_sync = false;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            // Drop stale-epoch reconfigurations.
            if (request->epoch() < state_->epoch) {
                response->set_success(false);
                response->set_node_id(state_->node_id);
                return grpc::Status::OK;
            }
            epoch_advanced = request->epoch() > state_->epoch;
            state_->epoch = request->epoch();
            state_->mode = request->mode();
            state_->epoch_fast.store(request->epoch(), std::memory_order_release);
            state_->mode_fast.store(ModeToFast(request->mode()), std::memory_order_release);
            state_->membership.assign(request->membership().begin(),
                                      request->membership().end());
            needs_sync = !request->added_node_id().empty() &&
                         request->added_node_id() == state_->node_id;
            membership_copy = state_->membership;
            node_id = state_->node_id;
            mode = state_->mode;
        }
        if (needs_sync) {
            SyncFromPeer(membership_copy, node_id, request->epoch());
            metadata_client_->SendSyncComplete(node_id, request->epoch());
        }
        ApplyMembership(membership_copy, node_id, mode, request->epoch(),
                        epoch_advanced, chain_replication_, craq_replication_,
                        crown_replication_);
        state_->state.store(NodeState::NORMAL);
        response->set_success(true);
        response->set_node_id(state_->node_id);
        return grpc::Status::OK;
    }

private:
    void SyncFromPeer(const std::vector<NodeInfo>& membership,
                      const std::string& node_id, uint64_t epoch) {
        for (const auto& member : membership) {
            if (member.node_id() == node_id) {
                continue;
            }
            auto stub = replication_common::make_replication_stub(member);
            if (!stub) {
                continue;
            }
            StateDumpRequest request;
            request.set_epoch(epoch);
            StateDumpResponse response;
            grpc::ClientContext context;
            grpc::Status status = stub->StateDump(&context, request, &response);
            if (!status.ok()) {
                continue;
            }
            std::unordered_map<std::string, std::string> values;
            std::unordered_map<std::string, uint64_t> versions;
            for (const auto& entry : response.kv_data()) {
                values[entry.first] = entry.second;
            }
            for (const auto& entry : response.version_data()) {
                versions[entry.first] = entry.second;
            }
            KVStore::apply_committed_snapshot(values, versions);
            return;
        }
    }

    ServerState* state_;
    MetadataClient* metadata_client_;
    ChainReplication* chain_replication_;
    CraqReplication* craq_replication_;
    CrownReplication* crown_replication_;
};

class ReplicationGatewayService final : public ReplicationService::Service {
public:
    ReplicationGatewayService(ServerState* state, ChainReplication* chain_replication,
                              CraqReplication* craq_replication,
                              CrownReplication* crown_replication,
                              MetadataClient* metadata_client)
        : state_(state),
          node_id_(state->node_id),
          chain_replication_(chain_replication),
          craq_replication_(craq_replication),
          crown_replication_(crown_replication),
          metadata_client_(metadata_client) {}

    grpc::Status Put(grpc::ServerContext*, const PutRequest* request,
                     PutResponse* response) override {
        if (state_->state.load() != NodeState::NORMAL) {
            response->set_success(false);
            response->set_error("SERVICE_UNAVAILABLE");
            return grpc::Status::OK;
        }
        uint64_t local_epoch = state_->epoch_fast.load(std::memory_order_acquire);
        int      mode_fast   = state_->mode_fast.load(std::memory_order_acquire);
        if (g_verbose_server_logging.load(std::memory_order_relaxed)) {
            std::ostringstream detail;
            detail << "request_id=" << request->request_id()
                   << " key=" << request->key()
                   << " version=" << request->version()
                   << " stamped_epoch=" << local_epoch
                   << " client_addr=" << request->client_addr();
            LogServerEvent("Put.recv", node_id_, FastToModeStr(mode_fast), local_epoch, detail.str());
        }
        Replication* replication = SelectReplicationFast(mode_fast);
        if (!replication) {
            response->set_success(false);
            response->set_error("WRONG_MODE");
            return grpc::Status::OK;
        }
        PutRequest stamped_request = *request;
        stamped_request.set_epoch(local_epoch);
        *response = replication->handle_put(stamped_request);
        if (g_verbose_server_logging.load(std::memory_order_relaxed)) {
            std::ostringstream detail;
            detail << "request_id=" << stamped_request.request_id()
                   << " key=" << stamped_request.key()
                   << " success=" << response->success()
                   << " version=" << response->version();
            if (!response->error().empty()) {
                detail << " error=" << response->error();
            }
            LogServerEvent("Put.resp", node_id_, FastToModeStr(mode_fast), local_epoch, detail.str());
        }
        return grpc::Status::OK;
    }

    grpc::Status ForwardPut(grpc::ServerContext*, const PutRequest* request,
                            PutResponse* response) override {
        uint64_t local_epoch = state_->epoch_fast.load(std::memory_order_acquire);
        int      mode_fast   = state_->mode_fast.load(std::memory_order_acquire);
        if (g_verbose_server_logging.load(std::memory_order_relaxed)) {
            std::ostringstream detail;
            detail << "request_id=" << request->request_id()
                   << " key=" << request->key()
                   << " version=" << request->version()
                   << " request_epoch=" << request->epoch()
                   << " client_addr=" << request->client_addr();
            LogServerEvent("ForwardPut.recv", node_id_, FastToModeStr(mode_fast), local_epoch, detail.str());
        }
        Replication* replication = SelectReplicationFast(mode_fast);
        if (!replication) {
            response->set_success(false);
            response->set_error("WRONG_MODE");
            return grpc::Status::OK;
        }
        if (request->epoch() < local_epoch) {
            response->set_success(false);
            response->set_error("STALE_EPOCH");
            return grpc::Status::OK;
        }
        if (request->epoch() > local_epoch) {
            RefreshMembershipAsync();
        }
        *response = replication->handle_put(*request);
        if (g_verbose_server_logging.load(std::memory_order_relaxed)) {
            std::ostringstream detail;
            detail << "request_id=" << request->request_id()
                   << " key=" << request->key()
                   << " success=" << response->success()
                   << " version=" << response->version();
            if (!response->error().empty()) {
                detail << " error=" << response->error();
            }
            LogServerEvent("ForwardPut.resp", node_id_, FastToModeStr(mode_fast), local_epoch, detail.str());
        }
        return grpc::Status::OK;
    }

    grpc::Status Get(grpc::ServerContext*, const GetRequest* request,
                     GetResponse* response) override {
        uint64_t local_epoch = state_->epoch_fast.load(std::memory_order_acquire);
        int      mode_fast   = state_->mode_fast.load(std::memory_order_acquire);
        Replication* replication = SelectReplicationFast(mode_fast);
        if (!replication) {
            response->set_error("WRONG_MODE");
            return grpc::Status::OK;
        }
        *response = replication->handle_get(request->key());
        return grpc::Status::OK;
    }

    grpc::Status SendWriteAck(grpc::ServerContext*, const WriteAck* request,
                              WriteAckResponse* response) override {
        uint64_t local_epoch = state_->epoch_fast.load(std::memory_order_acquire);
        int      mode_fast   = state_->mode_fast.load(std::memory_order_acquire);
        if (g_verbose_server_logging.load(std::memory_order_relaxed)) {
            std::ostringstream detail;
            detail << "request_id=" << request->request_id()
                   << " key=" << request->key()
                   << " version=" << request->version()
                   << " request_epoch=" << request->epoch();
            LogServerEvent("WriteAck.recv", node_id_, FastToModeStr(mode_fast), local_epoch, detail.str());
        }
        Replication* replication = SelectReplicationFast(mode_fast);
        if (!replication) {
            response->set_success(false);
            return grpc::Status::OK;
        }
        // WriteAcks flow backwards along the chain. We accept any epoch
        // (request->epoch() <= local_epoch is fine — the request was issued at
        // an older snapshot of the membership, and dropping the ack would orphan
        // the predecessor's pending entry). If the ack carries a newer epoch,
        // refresh asynchronously so we catch up.
        if (request->epoch() > local_epoch) {
            RefreshMembershipAsync();
        }
        replication->handle_ack(request->request_id());
        response->set_success(true);
        return grpc::Status::OK;
    }

    grpc::Status VersionQuery(grpc::ServerContext*, const VersionQueryRequest* request,
                              VersionQueryResponse* response) override {
        uint64_t local_epoch = state_->epoch_fast.load(std::memory_order_acquire);
        int      mode_fast   = state_->mode_fast.load(std::memory_order_acquire);
        if (request->epoch() < local_epoch) {
            response->set_error("STALE_EPOCH");
            return grpc::Status::OK;
        }
        if (request->epoch() > local_epoch) {
            RefreshMembershipAsync();
        }
        if (mode_fast == 2) {
            *response = craq_replication_->handle_version_query(*request);
        } else if (mode_fast == 3) {
            *response = crown_replication_->handle_version_query(*request);
        } else {
            response->set_error("WRONG_MODE");
        }
        return grpc::Status::OK;
    }

    grpc::Status GetServerStats(grpc::ServerContext*, const ServerStatsRequest*,
                                ServerStatsResponse* response) override {
        struct rusage ru;
        if (getrusage(RUSAGE_SELF, &ru) != 0) {
            return grpc::Status(grpc::StatusCode::INTERNAL, "getrusage failed");
        }
        uint64_t utime_us = static_cast<uint64_t>(ru.ru_utime.tv_sec) * 1000000ULL +
                            static_cast<uint64_t>(ru.ru_utime.tv_usec);
        uint64_t stime_us = static_cast<uint64_t>(ru.ru_stime.tv_sec) * 1000000ULL +
                            static_cast<uint64_t>(ru.ru_stime.tv_usec);
        auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        long ncpu = sysconf(_SC_NPROCESSORS_ONLN);
        response->set_utime_us(utime_us);
        response->set_stime_us(stime_us);
        response->set_monotonic_ns(static_cast<uint64_t>(now_ns));
        response->set_num_cpus(static_cast<uint32_t>(ncpu > 0 ? ncpu : 1));
        response->set_node_id(node_id_);
        return grpc::Status::OK;
    }

    grpc::Status StateDump(grpc::ServerContext*, const StateDumpRequest* request,
                           StateDumpResponse* response) override {
        uint64_t local_epoch = 0;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            local_epoch = state_->epoch;
        }
        if (request->epoch() < local_epoch) {
            // Signal staleness explicitly so the caller can distinguish "I am
            // empty" from "your snapshot request is stale". gRPC FAILED_PRECONDITION
            // is the correct status for "request was sensible but the system is
            // not in the right state to satisfy it".
            response->set_epoch(local_epoch);
            return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "STALE_EPOCH");
        }
        if (request->epoch() > local_epoch) {
            RefreshMembershipAsync();
        }
        for (const auto& entry : KVStore::snapshot_committed()) {
            (*response->mutable_kv_data())[entry.first] = entry.second.value;
            (*response->mutable_version_data())[entry.first] = entry.second.verison;
        }
        response->set_epoch(local_epoch);
        return grpc::Status::OK;
    }

private:
    Replication* SelectReplication(const std::string& mode) const {
        if (mode == "CHAIN") {
            return chain_replication_;
        }
        if (mode == "CRAQ") {
            return craq_replication_;
        }
        if (mode == "CROWN") {
            return crown_replication_;
        }
        return nullptr;
    }

    Replication* SelectReplicationFast(int mode_fast) const {
        switch (mode_fast) {
            case 1: return chain_replication_;
            case 2: return craq_replication_;
            case 3: return crown_replication_;
            default: return nullptr;
        }
    }

    // Pull membership from the metadata store. Used as a fallback when an RPC
    // arrives carrying an epoch newer than ours — typically that means we
    // missed (or are about to receive) a Reconfigure. We re-forward pending
    // entries here too so a lost Reconfigure RPC doesn't leave writes stuck.
    void RefreshMembershipAsync() {
        MembershipResponse membership_response;
        if (!metadata_client_->GetMembership(&membership_response)) {
            return;
        }
        std::vector<NodeInfo> membership_copy;
        std::string node_id;
        std::string mode;
        bool epoch_advanced = false;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            if (membership_response.epoch() <= state_->epoch) {
                return;
            }
            epoch_advanced = true;
            state_->epoch = membership_response.epoch();
            state_->mode = membership_response.mode();
            state_->epoch_fast.store(membership_response.epoch(), std::memory_order_release);
            state_->mode_fast.store(ModeToFast(membership_response.mode()), std::memory_order_release);
            state_->membership.assign(membership_response.membership().begin(),
                                      membership_response.membership().end());
            membership_copy = state_->membership;
            node_id = state_->node_id;
            mode = state_->mode;
        }
        ApplyMembership(membership_copy, node_id, mode,
                        membership_response.epoch(), epoch_advanced,
                        chain_replication_, craq_replication_,
                        crown_replication_);
    }

    ServerState* state_;
    std::string node_id_;
    ChainReplication* chain_replication_;
    CraqReplication* craq_replication_;
    CrownReplication* crown_replication_;
    MetadataClient* metadata_client_;
};

Replication* SelectReplicationForMode(const std::string& mode,
                                      ChainReplication* chain_replication,
                                      CraqReplication* craq_replication,
                                      CrownReplication* crown_replication) {
    if (mode == "CHAIN") {
        return chain_replication;
    }
    if (mode == "CRAQ") {
        return craq_replication;
    }
    if (mode == "CROWN") {
        return crown_replication;
    }
    return nullptr;
}

void RefreshMembership(ServerState* state, MetadataClient* client,
                       ChainReplication* chain_replication,
                       CraqReplication* craq_replication,
                       CrownReplication* crown_replication) {
    MembershipResponse response;
    if (!client->GetMembership(&response)) {
        return;
    }
    std::vector<NodeInfo> membership_copy;
    std::string node_id;
    std::string mode;
    bool epoch_advanced = false;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (response.epoch() < state->epoch) {
            return;
        }
        epoch_advanced = response.epoch() > state->epoch;
        state->epoch = response.epoch();
        state->mode = response.mode();
        state->epoch_fast.store(response.epoch(), std::memory_order_release);
        state->mode_fast.store(ModeToFast(response.mode()), std::memory_order_release);
        state->membership.assign(response.membership().begin(), response.membership().end());
        if (state->state.load() == NodeState::STARTING) {
            state->state.store(NodeState::NORMAL);
        }
        membership_copy = state->membership;
        node_id = state->node_id;
        mode = state->mode;
    }
    ApplyMembership(membership_copy, node_id, mode, response.epoch(),
                    epoch_advanced, chain_replication, craq_replication,
                    crown_replication);
}

void HeartbeatLoop(ServerState* state, MetadataClient* client,
                   ChainReplication* chain_replication, CraqReplication* craq_replication,
                   CrownReplication* crown_replication,
                   std::atomic<bool>* shutdown_flag) {
    while (!shutdown_flag->load()) {
        uint64_t master_epoch = 0;
        uint64_t local_epoch = 0;
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            local_epoch = state->epoch;
        }
        if (client->SendHeartbeat(state->node_id, local_epoch, &master_epoch)) {
            if (master_epoch > local_epoch) {
                RefreshMembership(state, client, chain_replication, craq_replication,
                                  crown_replication);
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(kHeartbeatIntervalMs));
    }
}

void RetryLoop(ServerState* state, ChainReplication* chain_replication,
               CraqReplication* craq_replication, CrownReplication* crown_replication,
               std::atomic<bool>* shutdown_flag) {
    while (!shutdown_flag->load()) {
        std::string mode;
        uint64_t local_epoch = 0;
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            mode = state->mode;
            local_epoch = state->epoch;
        }
        Replication* replication = SelectReplicationForMode(
            mode, chain_replication, craq_replication, crown_replication);
        if (replication) {
            auto expired = replication->collect_expired_requests();
            auto next_stub = replication->get_next_stub();
            if (!expired.empty()) {
                std::ostringstream detail;
                detail << "expired_count=" << expired.size()
                       << " pending_total=" << replication->pending_count()
                       << " has_next_stub=" << (next_stub ? 1 : 0);
                LogServerEvent("RetryLoop.fire", state->node_id, mode, local_epoch,
                               detail.str());
            }
            if (next_stub) {
                for (const auto& request : expired) {
                    replication->forward_put(request, next_stub);
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(kRetryIntervalMs));
    }
}

} // namespace

int main(int argc, char** argv) {
    std::string node_id;
    std::string listen_addr = "0.0.0.0:50051";
    std::string metadata_addr = "127.0.0.1:50050";

    bool verbose = false;
    bool crown_chain_reads = false;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--node-id" && i + 1 < argc) {
            node_id = argv[++i];
        } else if (arg == "--listen" && i + 1 < argc) {
            listen_addr = argv[++i];
        } else if (arg == "--metadata" && i + 1 < argc) {
            metadata_addr = argv[++i];
        } else if (arg == "--verbose" || arg == "-v") {
            verbose = true;
        } else if (arg == "--crown-chain-reads") {
            crown_chain_reads = true;
        }
    }

    if (node_id.empty()) {
        std::cerr << "--node-id is required" << std::endl;
        return 1;
    }

    g_verbose_server_logging.store(verbose, std::memory_order_relaxed);
    replication_common::set_verbose_logging(verbose);

    ServerState state;
    state.node_id = node_id;

    ChainReplication chain_replication;
    CraqReplication craq_replication;
    CrownReplication crown_replication;
    crown_replication.set_chain_mode(crown_chain_reads);
    std::cout << "CROWN read mode: "
              << (crown_chain_reads ? "chain (no dirty store)" : "craq (with dirty store)")
              << std::endl;

    MetadataClient metadata_client(metadata_addr);
    RefreshMembership(&state, &metadata_client, &chain_replication, &craq_replication,
                      &crown_replication);

    NodeMetadataService metadata_service(&state, &metadata_client, &chain_replication,
                                         &craq_replication, &crown_replication);
    ReplicationGatewayService replication_service(&state, &chain_replication,
                                                  &craq_replication, &crown_replication,
                                                  &metadata_client);
    grpc::ServerBuilder builder;
    // Match the channel-side concurrency tuning so a head node can drive many
    // outstanding ForwardPut streams to its successor without head-of-line
    // blocking inside the HTTP/2 layer.
    builder.AddChannelArgument(GRPC_ARG_MAX_CONCURRENT_STREAMS, 1024);
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS, 30000);
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5000);
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
    // Allow peers to ping as frequently as every 10 s. Without this the gRPC
    // default (5 min) causes ENHANCE_YOUR_CALM / GOAWAY for our 30 s keepalives.
    builder.AddChannelArgument(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 10000);
    builder.AddListeningPort(listen_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&metadata_service);
    builder.RegisterService(&replication_service);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    if (!server) {
        std::cerr << "Failed to start server on " << listen_addr << std::endl;
        return 1;
    }

    std::atomic<bool> shutdown_flag{false};
    std::thread heartbeat_thread(HeartbeatLoop, &state, &metadata_client, &chain_replication,
                                 &craq_replication, &crown_replication, &shutdown_flag);
    std::thread retry_thread(RetryLoop, &state, &chain_replication, &craq_replication,
                             &crown_replication, &shutdown_flag);

    std::cout << "Server listening on " << listen_addr << std::endl;
    server->Wait();

    shutdown_flag.store(true);
    heartbeat_thread.join();
    retry_thread.join();
    return 0;
}

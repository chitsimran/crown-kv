#include "replication.grpc.pb.h"
#include "replication/chain/chain_replication.h"
#include "replication/craq/craq_replication.h"
#include "replication/crown/crown_replication.h"
#include <grpcpp/grpcpp.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using replication::DrainAckRequest;
using replication::DrainAckResponse;
using replication::FreezeRequest;
using replication::FreezeResponse;
using replication::HeartbeatRequest;
using replication::HeartbeatResponse;
using replication::GetRequest;
using replication::GetResponse;
using replication::InflightRequest;
using replication::InflightResponse;
using replication::MembershipRequest;
using replication::MembershipResponse;
using replication::MetadataService;
using replication::NodeInfo;
using replication::PutRequest;
using replication::PutResponse;
using replication::ReconfigureRequest;
using replication::ReconfigureResponse;
using replication::ReportFailureRequest;
using replication::ReportFailureResponse;
using replication::ReplicationService;
using replication::StateDumpRequest;
using replication::StateDumpResponse;
using replication::SyncCompleteRequest;
using replication::SyncCompleteResponse;
using replication::VersionQueryRequest;
using replication::VersionQueryResponse;
using replication::WriteAck;
using replication::WriteAckResponse;

namespace {

constexpr int kHeartbeatIntervalMs = 500;
constexpr int kRetryIntervalMs = 1000;

enum class NodeState {
    STARTING,
    NORMAL,
    FROZEN,
    DRAINING_WAIT
};

struct ServerState {
    std::mutex mutex;
    std::string node_id;
    uint64_t epoch = 0;
    std::string mode;
    std::vector<NodeInfo> membership;
    std::vector<NodeInfo> pending_membership;
    std::atomic<NodeState> state{NodeState::STARTING};
    std::atomic<bool> drain_active{false};
    uint64_t drain_epoch = 0;
    std::unordered_map<std::string, int> forward_failures;
};

Replication* SelectReplicationForMode(const std::string& mode,
                                      ChainReplication* chain_replication,
                                      CraqReplication* craq_replication,
                                      CrownReplication* crown_replication);

std::mutex log_mutex;

void LogServerEvent(const std::string& event, const std::string& node_id,
                    const std::string& mode, uint64_t local_epoch,
                    const std::string& detail) {
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
              grpc::CreateChannel(address, grpc::InsecureChannelCredentials()))) {}

    bool GetMembership(MembershipResponse* response) {
        MembershipRequest request;
        grpc::ClientContext context;
        grpc::Status status = stub_->GetMembership(&context, request, response);
        return status.ok();
    }

    bool SendDrainAck(const std::string& node_id, uint64_t epoch) {
        DrainAckRequest request;
        request.set_node_id(node_id);
        request.set_epoch(epoch);
        DrainAckResponse response;
        grpc::ClientContext context;
        grpc::Status status = stub_->ReportDrainAck(&context, request, &response);
        return status.ok() && response.success();
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

    bool ReportFailure(const std::string& reporter_node_id,
                       const std::string& failed_node_id, uint64_t epoch) {
        ReportFailureRequest request;
        request.set_reporter_node_id(reporter_node_id);
        request.set_failed_node_id(failed_node_id);
        request.set_epoch(epoch);
        ReportFailureResponse response;
        grpc::ClientContext context;
        grpc::Status status = stub_->ReportFailure(&context, request, &response);
        return status.ok() && response.success();
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

    grpc::Status Freeze(grpc::ServerContext*, const FreezeRequest* request,
                        FreezeResponse* response) override {
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            if (request->epoch() < state_->epoch) {
                response->set_success(false);
                response->set_node_id(state_->node_id);
                return grpc::Status::OK;
            }
            state_->epoch = request->epoch();
            state_->pending_membership.assign(request->v_new().begin(), request->v_new().end());
            state_->state.store(NodeState::FROZEN);
            state_->drain_epoch = request->epoch();
        }
        StartDrain(request->epoch());
        response->set_success(true);
        response->set_node_id(state_->node_id);
        return grpc::Status::OK;
    }

    grpc::Status Reconfigure(grpc::ServerContext*, const ReconfigureRequest* request,
                             ReconfigureResponse* response) override {
        std::vector<NodeInfo> membership_copy;
        std::string node_id;
        bool needs_sync = false;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            state_->epoch = request->epoch();
            state_->mode = request->mode();
            state_->membership.assign(request->membership().begin(), request->membership().end());
            state_->pending_membership.clear();
            needs_sync = !request->added_node_id().empty() &&
                         request->added_node_id() == state_->node_id;
            membership_copy = state_->membership;
            node_id = state_->node_id;
        }
        if (needs_sync) {
            SyncFromPeer(membership_copy, node_id, request->epoch());
            metadata_client_->SendSyncComplete(node_id, request->epoch());
        }
        chain_replication_->update_membership(membership_copy, node_id);
        chain_replication_->set_epoch(request->epoch());
        craq_replication_->update_membership(membership_copy, node_id);
        craq_replication_->set_epoch(request->epoch());
        crown_replication_->update_membership(membership_copy, node_id);
        crown_replication_->set_epoch(request->epoch());
        Replication* replication = SelectReplicationForMode(
            request->mode(), chain_replication_, craq_replication_, crown_replication_);
        if (replication) {
            auto next_stub = replication->get_next_stub();
            if (next_stub) {
                for (const auto& pending : replication->snapshot_pending_requests()) {
                    replication->forward_put(pending, next_stub);
                }
            }
        }
        state_->state.store(NodeState::NORMAL);
        response->set_success(true);
        response->set_node_id(state_->node_id);
        return grpc::Status::OK;
    }

private:
    void StartDrain(uint64_t epoch) {
        bool already_active = state_->drain_active.exchange(true);
        if (already_active) {
            return;
        }
        InflightRequest inflight;
        inflight.set_origin_node_id(state_->node_id);
        inflight.set_epoch(epoch);
        SendOriginInflight(inflight);
    }

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

    void SendOriginInflight(const InflightRequest& inflight) {
        std::string mode;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            mode = state_->mode;
        }

        Replication* replication = SelectReplicationForMode(
            mode, chain_replication_, craq_replication_, crown_replication_);
        if (!replication) {
            state_->drain_active.store(false);
            return;
        }

        auto next_stub = DrainSuccessorStub(replication);
        if (next_stub) {
            while (replication->pending_count() > 0) {
                auto pending = replication->snapshot_pending_requests();
                for (const auto& request : pending) {
                    replication->forward_put(request, next_stub);
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }

            InflightResponse response;
            grpc::ClientContext context;
            next_stub->ForwardInflight(&context, inflight, &response);
        } else {
            state_->state.store(NodeState::DRAINING_WAIT);
            metadata_client_->SendDrainAck(state_->node_id, inflight.epoch());
        }

        state_->drain_active.store(false);
    }

    std::shared_ptr<ReplicationService::Stub> DrainSuccessorStub(Replication* replication) {
        std::vector<NodeInfo> pending_membership;
        std::vector<NodeInfo> current_membership;
        std::string node_id;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            pending_membership = state_->pending_membership;
            current_membership = state_->membership;
            node_id = state_->node_id;
        }

        bool removal = !pending_membership.empty() &&
                       pending_membership.size() < current_membership.size();
        if (!removal) {
            return replication->get_next_stub();
        }

        int ring_size = static_cast<int>(pending_membership.size());
        if (ring_size <= 1) {
            return nullptr;
        }
        for (int i = 0; i < ring_size; ++i) {
            if (pending_membership[i].node_id() == node_id) {
                return replication_common::make_replication_stub(
                    pending_membership[(i + 1) % ring_size]);
            }
        }
        return replication->get_next_stub();
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
        uint64_t local_epoch = 0;
        std::string mode;
        std::string node_id;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            local_epoch = state_->epoch;
            mode = state_->mode;
            node_id = state_->node_id;
        }
        {
            std::ostringstream detail;
            detail << "request_id=" << request->request_id()
                   << " key=" << request->key()
                   << " version=" << request->version()
                   << " request_epoch=" << request->epoch()
                   << " client_addr=" << request->client_addr();
            LogServerEvent("Put.recv", node_id, mode, local_epoch, detail.str());
        }
        Replication* replication = SelectReplication(mode);
        if (!replication) {
            response->set_success(false);
            response->set_error("WRONG_MODE");
            LogServerEvent("Put.reject", node_id, mode, local_epoch, "error=WRONG_MODE");
            return grpc::Status::OK;
        }
        if (request->epoch() < local_epoch) {
            response->set_success(false);
            response->set_error("STALE_EPOCH");
            LogServerEvent("Put.reject", node_id, mode, local_epoch, "error=STALE_EPOCH");
            return grpc::Status::OK;
        }
        if (request->epoch() > local_epoch) {
            RefreshMembershipAsync();
        }
        *response = replication->handle_put(*request);
        {
            std::ostringstream detail;
            detail << "request_id=" << request->request_id()
                   << " key=" << request->key()
                   << " success=" << response->success()
                   << " version=" << response->version();
            if (!response->error().empty()) {
                detail << " error=" << response->error();
            }
            LogServerEvent("Put.resp", node_id, mode, local_epoch, detail.str());
        }
        return grpc::Status::OK;
    }

    grpc::Status ForwardPut(grpc::ServerContext*, const PutRequest* request,
                            PutResponse* response) override {
        uint64_t local_epoch = 0;
        std::string mode;
        std::string node_id;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            local_epoch = state_->epoch;
            mode = state_->mode;
            node_id = state_->node_id;
        }
        {
            std::ostringstream detail;
            detail << "request_id=" << request->request_id()
                   << " key=" << request->key()
                   << " version=" << request->version()
                   << " request_epoch=" << request->epoch()
                   << " client_addr=" << request->client_addr();
            LogServerEvent("ForwardPut.recv", node_id, mode, local_epoch, detail.str());
        }
        Replication* replication = SelectReplication(mode);
        if (!replication) {
            response->set_success(false);
            response->set_error("WRONG_MODE");
            LogServerEvent("ForwardPut.reject", node_id, mode, local_epoch,
                           "error=WRONG_MODE");
            return grpc::Status::OK;
        }
        if (request->epoch() < local_epoch) {
            response->set_success(false);
            response->set_error("STALE_EPOCH");
            LogServerEvent("ForwardPut.reject", node_id, mode, local_epoch,
                           "error=STALE_EPOCH");
            return grpc::Status::OK;
        }
        if (request->epoch() > local_epoch) {
            RefreshMembershipAsync();
        }
        *response = replication->handle_put(*request);
        {
            std::ostringstream detail;
            detail << "request_id=" << request->request_id()
                   << " key=" << request->key()
                   << " success=" << response->success()
                   << " version=" << response->version();
            if (!response->error().empty()) {
                detail << " error=" << response->error();
            }
            LogServerEvent("ForwardPut.resp", node_id, mode, local_epoch, detail.str());
        }
        return grpc::Status::OK;
    }

    grpc::Status Get(grpc::ServerContext*, const GetRequest* request,
                     GetResponse* response) override {
        uint64_t local_epoch = 0;
        std::string mode;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            local_epoch = state_->epoch;
            mode = state_->mode;
        }
        Replication* replication = SelectReplication(mode);
        if (!replication) {
            response->set_error("WRONG_MODE");
            return grpc::Status::OK;
        }
        if (request->epoch() < local_epoch) {
            response->set_error("STALE_EPOCH");
            return grpc::Status::OK;
        }
        if (request->epoch() > local_epoch) {
            RefreshMembershipAsync();
        }
        *response = replication->handle_get(request->key());
        return grpc::Status::OK;
    }

    grpc::Status SendWriteAck(grpc::ServerContext*, const WriteAck* request,
                              WriteAckResponse* response) override {
        uint64_t local_epoch = 0;
        std::string mode;
        std::string node_id;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            local_epoch = state_->epoch;
            mode = state_->mode;
            node_id = state_->node_id;
        }
        {
            std::ostringstream detail;
            detail << "request_id=" << request->request_id()
                   << " key=" << request->key()
                   << " version=" << request->version()
                   << " request_epoch=" << request->epoch();
            LogServerEvent("WriteAck.recv", node_id, mode, local_epoch, detail.str());
        }
        Replication* replication = SelectReplication(mode);
        if (!replication) {
            response->set_success(false);
            LogServerEvent("WriteAck.reject", node_id, mode, local_epoch,
                           "error=WRONG_MODE");
            return grpc::Status::OK;
        }
        if (request->epoch() < local_epoch) {
            response->set_success(false);
            LogServerEvent("WriteAck.reject", node_id, mode, local_epoch,
                           "error=STALE_EPOCH");
            return grpc::Status::OK;
        }
        if (request->epoch() > local_epoch) {
            RefreshMembershipAsync();
        }
        replication->handle_ack(request->request_id());
        response->set_success(true);
        LogServerEvent("WriteAck.resp", node_id, mode, local_epoch, "success=true");
        return grpc::Status::OK;
    }

    grpc::Status ForwardInflight(grpc::ServerContext*, const InflightRequest* request,
                                 InflightResponse* response) override {
        response->set_success(true);
        std::string mode;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            mode = state_->mode;
        }
        Replication* replication = SelectReplication(mode);
        if (!replication) {
            return grpc::Status::OK;
        }

        HandleInflight(replication, *request);
        return grpc::Status::OK;
    }

    grpc::Status VersionQuery(grpc::ServerContext*, const VersionQueryRequest* request,
                              VersionQueryResponse* response) override {
        uint64_t local_epoch = 0;
        std::string mode;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            local_epoch = state_->epoch;
            mode = state_->mode;
        }
        if (request->epoch() < local_epoch) {
            response->set_error("STALE_EPOCH");
            return grpc::Status::OK;
        }
        if (request->epoch() > local_epoch) {
            RefreshMembershipAsync();
        }
        if (mode != "CRAQ" && mode != "CROWN") {
            response->set_error("WRONG_MODE");
            return grpc::Status::OK;
        }
        if (mode == "CRAQ") {
            *response = craq_replication_->handle_version_query(*request);
        } else {
            *response = crown_replication_->handle_version_query(*request);
        }
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
            response->set_epoch(local_epoch);
            return grpc::Status::OK;
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

    void HandleInflight(Replication* replication, const InflightRequest& inflight) {
        auto next_stub = DrainSuccessorStub(replication);
        if (next_stub) {
            while (replication->pending_count() > 0) {
                auto pending = replication->snapshot_pending_requests();
                for (const auto& request : pending) {
                    replication->forward_put(request, next_stub);
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }

        if (inflight.origin_node_id() == state_->node_id) {
            state_->state.store(NodeState::DRAINING_WAIT);
            metadata_client_->SendDrainAck(state_->node_id, inflight.epoch());
            return;
        }

        if (next_stub) {
            InflightResponse response;
            grpc::ClientContext context;
            next_stub->ForwardInflight(&context, inflight, &response);
        }
    }

    std::shared_ptr<ReplicationService::Stub> DrainSuccessorStub(Replication* replication) {
        std::vector<NodeInfo> pending_membership;
        std::vector<NodeInfo> current_membership;
        std::string node_id;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            pending_membership = state_->pending_membership;
            current_membership = state_->membership;
            node_id = state_->node_id;
        }

        bool removal = !pending_membership.empty() &&
                       pending_membership.size() < current_membership.size();
        if (!removal) {
            return replication->get_next_stub();
        }

        int ring_size = static_cast<int>(pending_membership.size());
        if (ring_size <= 1) {
            return nullptr;
        }
        for (int i = 0; i < ring_size; ++i) {
            if (pending_membership[i].node_id() == node_id) {
                return replication_common::make_replication_stub(
                    pending_membership[(i + 1) % ring_size]);
            }
        }
        return replication->get_next_stub();
    }

    void RefreshMembershipAsync() {
        MembershipResponse membership_response;
        if (!metadata_client_->GetMembership(&membership_response)) {
            return;
        }
        std::vector<NodeInfo> membership_copy;
        std::string node_id;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            if (membership_response.epoch() < state_->epoch) {
                return;
            }
            state_->epoch = membership_response.epoch();
            state_->mode = membership_response.mode();
            state_->membership.assign(membership_response.membership().begin(),
                                      membership_response.membership().end());
            membership_copy = state_->membership;
            node_id = state_->node_id;
        }
        chain_replication_->update_membership(membership_copy, node_id);
        chain_replication_->set_epoch(membership_response.epoch());
        craq_replication_->update_membership(membership_copy, node_id);
        craq_replication_->set_epoch(membership_response.epoch());
        crown_replication_->update_membership(membership_copy, node_id);
        crown_replication_->set_epoch(membership_response.epoch());
    }

    ServerState* state_;
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
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->epoch = response.epoch();
        state->mode = response.mode();
        state->membership.assign(response.membership().begin(), response.membership().end());
        if (state->state.load() == NodeState::STARTING) {
            state->state.store(NodeState::NORMAL);
        }
        membership_copy = state->membership;
        node_id = state->node_id;
    }
    chain_replication->update_membership(membership_copy, node_id);
    chain_replication->set_epoch(response.epoch());
    craq_replication->update_membership(membership_copy, node_id);
    craq_replication->set_epoch(response.epoch());
    crown_replication->update_membership(membership_copy, node_id);
    crown_replication->set_epoch(response.epoch());
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
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            mode = state->mode;
        }
        Replication* replication = SelectReplicationForMode(
            mode, chain_replication, craq_replication, crown_replication);
        if (replication) {
            auto expired = replication->collect_expired_requests();
            auto next_stub = replication->get_next_stub();
            if (next_stub) {
                for (const auto& request : expired) {
                    replication->forward_put(request, next_stub);
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(kRetryIntervalMs));
    }
}

std::string CurrentSuccessorNodeId(ServerState* state) {
    std::lock_guard<std::mutex> lock(state->mutex);
    int ring_size = static_cast<int>(state->membership.size());
    if (ring_size <= 1) {
        return "";
    }
    for (int i = 0; i < ring_size; ++i) {
        if (state->membership[i].node_id() == state->node_id) {
            return state->membership[(i + 1) % ring_size].node_id();
        }
    }
    return "";
}

void InstallForwardFailureHandler(ServerState* state, MetadataClient* metadata_client,
                                  Replication* replication) {
    replication->forward_failure_handler =
        [state, metadata_client](const PutRequest&) {
            std::string failed_node_id = CurrentSuccessorNodeId(state);
            if (failed_node_id.empty()) {
                return;
            }
            uint64_t epoch = 0;
            int failures = 0;
            {
                std::lock_guard<std::mutex> lock(state->mutex);
                epoch = state->epoch;
                failures = ++state->forward_failures[failed_node_id];
            }
            if (failures >= 3) {
                if (metadata_client->ReportFailure(state->node_id, failed_node_id, epoch)) {
                    std::lock_guard<std::mutex> lock(state->mutex);
                    state->forward_failures[failed_node_id] = 0;
                }
            }
        };
}

} // namespace

int main(int argc, char** argv) {
    std::string node_id;
    std::string listen_addr = "0.0.0.0:50051";
    std::string metadata_addr = "127.0.0.1:50050";

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--node-id" && i + 1 < argc) {
            node_id = argv[++i];
        } else if (arg == "--listen" && i + 1 < argc) {
            listen_addr = argv[++i];
        } else if (arg == "--metadata" && i + 1 < argc) {
            metadata_addr = argv[++i];
        }
    }

    if (node_id.empty()) {
        std::cerr << "--node-id is required" << std::endl;
        return 1;
    }

    ServerState state;
    state.node_id = node_id;

    ChainReplication chain_replication;
    CraqReplication craq_replication;
    CrownReplication crown_replication;

    MetadataClient metadata_client(metadata_addr);
    InstallForwardFailureHandler(&state, &metadata_client, &chain_replication);
    InstallForwardFailureHandler(&state, &metadata_client, &craq_replication);
    InstallForwardFailureHandler(&state, &metadata_client, &crown_replication);
    RefreshMembership(&state, &metadata_client, &chain_replication, &craq_replication,
                      &crown_replication);

    NodeMetadataService metadata_service(&state, &metadata_client, &chain_replication,
                                         &craq_replication, &crown_replication);
    ReplicationGatewayService replication_service(&state, &chain_replication,
                                                  &craq_replication, &crown_replication,
                                                  &metadata_client);
    grpc::ServerBuilder builder;
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

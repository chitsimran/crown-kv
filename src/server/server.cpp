#include "replication.grpc.pb.h"
#include "replication/chain/chain_replication.h"
#include "replication/craq/craq_replication.h"
#include "replication/crown/crown_replication.h"
#include <grpcpp/grpcpp.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using replication::DrainAckRequest;
using replication::DrainAckResponse;
using replication::FreezeRequest;
using replication::FreezeResponse;
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
using replication::VersionQueryRequest;
using replication::VersionQueryResponse;
using replication::WriteAck;
using replication::WriteAckResponse;

namespace {

constexpr int kHeartbeatIntervalMs = 500;

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
};

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
        }
        metadata_client_->SendDrainAck(state_->node_id, request->epoch());
        response->set_success(true);
        response->set_node_id(state_->node_id);
        return grpc::Status::OK;
    }

    grpc::Status Reconfigure(grpc::ServerContext*, const ReconfigureRequest* request,
                             ReconfigureResponse* response) override {
        std::vector<NodeInfo> membership_copy;
        std::string node_id;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            state_->epoch = request->epoch();
            state_->mode = request->mode();
            state_->membership.assign(request->membership().begin(), request->membership().end());
            state_->pending_membership.clear();
            state_->state.store(NodeState::NORMAL);
            membership_copy = state_->membership;
            node_id = state_->node_id;
        }
        chain_replication_->update_membership(membership_copy, node_id);
        craq_replication_->update_membership(membership_copy, node_id);
        craq_replication_->set_epoch(request->epoch());
        crown_replication_->update_membership(membership_copy, node_id);
        crown_replication_->set_epoch(request->epoch());
        response->set_success(true);
        response->set_node_id(state_->node_id);
        return grpc::Status::OK;
    }

private:
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
                                                            CrownReplication* crown_replication)
        : state_(state),
          chain_replication_(chain_replication),
                    craq_replication_(craq_replication),
                    crown_replication_(crown_replication) {}

    grpc::Status Put(grpc::ServerContext*, const PutRequest* request,
                     PutResponse* response) override {
        if (state_->state.load() == NodeState::FROZEN) {
            response->set_success(false);
            response->set_error("FROZEN");
            return grpc::Status::OK;
        }
        uint64_t local_epoch = 0;
        std::string mode;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            local_epoch = state_->epoch;
            mode = state_->mode;
        }
        Replication* replication = SelectReplication(mode);
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
        *response = replication->handle_put(*request);
        return grpc::Status::OK;
    }

    grpc::Status ForwardPut(grpc::ServerContext*, const PutRequest* request,
                            PutResponse* response) override {
        uint64_t local_epoch = 0;
        std::string mode;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            local_epoch = state_->epoch;
            mode = state_->mode;
        }
        Replication* replication = SelectReplication(mode);
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
        *response = replication->handle_put(*request);
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
        *response = replication->handle_get(request->key());
        return grpc::Status::OK;
    }

    grpc::Status SendWriteAck(grpc::ServerContext*, const WriteAck* request,
                              WriteAckResponse* response) override {
        uint64_t local_epoch = 0;
        std::string mode;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            local_epoch = state_->epoch;
            mode = state_->mode;
        }
        Replication* replication = SelectReplication(mode);
        if (!replication) {
            response->set_success(false);
            return grpc::Status::OK;
        }
        if (request->epoch() < local_epoch) {
            response->set_success(false);
            return grpc::Status::OK;
        }
        replication->handle_ack(request->request_id());
        response->set_success(true);
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

    ServerState* state_;
    ChainReplication* chain_replication_;
    CraqReplication* craq_replication_;
    CrownReplication* crown_replication_;
};

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
    RefreshMembership(&state, &metadata_client, &chain_replication, &craq_replication,
                      &crown_replication);

    NodeMetadataService metadata_service(&state, &metadata_client, &chain_replication,
                                         &craq_replication, &crown_replication);
    ReplicationGatewayService replication_service(&state, &chain_replication,
                                                  &craq_replication, &crown_replication);
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

    std::cout << "Server listening on " << listen_addr << std::endl;
    server->Wait();

    shutdown_flag.store(true);
    heartbeat_thread.join();
    return 0;
}

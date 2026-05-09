#include "replication.grpc.pb.h"
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

using replication::HeartbeatRequest;
using replication::HeartbeatResponse;
using replication::MembershipRequest;
using replication::MembershipResponse;
using replication::MetadataService;
using replication::NodeInfo;
using replication::ReconfigureRequest;
using replication::ReconfigureResponse;
using replication::SetModeRequest;
using replication::SetModeResponse;
using replication::SyncCompleteRequest;
using replication::SyncCompleteResponse;
using replication::AddMemberRequest;
using replication::AddMemberResponse;

namespace {

// Servers send heartbeats every 400ms (server.cpp::kHeartbeatIntervalMs). Match
// that here so the threshold math reflects reality. 10 misses -> 4 s of silence
// before a node is declared dead — short enough that the bench-with-kill scenario
// recovers in a reasonable time, long enough to ride out brief stalls.
constexpr int kHeartbeatIntervalMs = 400;
constexpr int kHeartbeatMissThreshold = 10; // 4 seconds

struct ClusterState {
    std::mutex mutex;
    uint64_t epoch = 1;
    std::string mode = "CHAIN";
    std::vector<NodeInfo> membership;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> last_heartbeat;
    std::atomic<bool> reconfig_in_progress{false};
};

void TriggerReconfigureRemove(ClusterState* state, const std::string& failed_node_id);
void TriggerReconfigureAdd(ClusterState* state, const NodeInfo& added_node);
void TriggerModeChange(ClusterState* state, const std::string& mode);

bool ParseHostPort(const std::string& text, std::string* host, int* port) {
    auto pos = text.rfind(':');
    if (pos == std::string::npos || pos == 0 || pos + 1 >= text.size()) {
        return false;
    }
    *host = text.substr(0, pos);
    try {
        *port = std::stoi(text.substr(pos + 1));
    } catch (...) {
        return false;
    }
    return true;
}

bool ParseMember(const std::string& text, NodeInfo* out) {
    auto pos = text.find('@');
    if (pos == std::string::npos || pos == 0 || pos + 1 >= text.size()) {
        return false;
    }
    std::string node_id = text.substr(0, pos);
    std::string addr = text.substr(pos + 1);
    std::string host;
    int port = 0;
    if (!ParseHostPort(addr, &host, &port)) {
        return false;
    }
    out->set_node_id(node_id);
    out->set_address(host);
    out->set_port(port);
    return true;
}

std::string ToAddress(const NodeInfo& info) {
    return info.address() + ":" + std::to_string(info.port());
}

std::vector<std::string> Split(const std::string& text, char delim) {
    std::vector<std::string> parts;
    std::stringstream stream(text);
    std::string part;
    while (std::getline(stream, part, delim)) {
        if (!part.empty()) {
            parts.push_back(part);
        }
    }
    return parts;
}

class MetadataStoreService final : public MetadataService::Service {
public:
    explicit MetadataStoreService(ClusterState* state) : state_(state) {}

    grpc::Status GetMembership(grpc::ServerContext*, const MembershipRequest*,
                               MembershipResponse* response) override {
        std::lock_guard<std::mutex> lock(state_->mutex);
        response->set_epoch(state_->epoch);
        response->set_mode(state_->mode);
        for (const auto& member : state_->membership) {
            *response->add_membership() = member;
        }
        return grpc::Status::OK;
    }

    grpc::Status SetMode(grpc::ServerContext*, const SetModeRequest* request,
                         SetModeResponse* response) override {
        const std::string mode = request->mode();
        if (mode != "CHAIN" && mode != "CRAQ" && mode != "CROWN") {
            response->set_success(false);
            response->set_error("UNKNOWN_MODE");
            return grpc::Status::OK;
        }

        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            if (state_->mode == mode) {
                response->set_success(true);
                response->set_epoch(state_->epoch);
                return grpc::Status::OK;
            }
        }

        std::thread(TriggerModeChange, state_, mode).detach();
        response->set_success(true);
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            response->set_epoch(state_->epoch + 1);
        }
        return grpc::Status::OK;
    }

    grpc::Status AddMember(grpc::ServerContext*, const AddMemberRequest* request,
                           AddMemberResponse* response) override {
        NodeInfo new_node;
        new_node.set_node_id(request->node_id());
        new_node.set_address(request->address());
        new_node.set_port(request->port());

        std::thread(TriggerReconfigureAdd, state_, new_node).detach();
        response->set_success(true);
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            response->set_new_epoch(state_->epoch + 1);
        }
        return grpc::Status::OK;
    }

    grpc::Status Heartbeat(grpc::ServerContext*, const HeartbeatRequest* request,
                           HeartbeatResponse* response) override {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->last_heartbeat[request->node_id()] = std::chrono::steady_clock::now();
        response->set_alive(true);
        response->set_epoch(state_->epoch);
        return grpc::Status::OK;
    }

    grpc::Status ReportSyncDone(grpc::ServerContext*, const SyncCompleteRequest*,
                                SyncCompleteResponse* response) override {
        response->set_success(true);
        return grpc::Status::OK;
    }

private:
    ClusterState* state_;
};

bool SendReconfigure(const NodeInfo& node, uint64_t epoch,
                     const std::vector<NodeInfo>& membership, const std::string& mode,
                     const std::string& removed_node_id,
                     const std::string& added_node_id = "") {
    auto channel = grpc::CreateChannel(ToAddress(node), grpc::InsecureChannelCredentials());
    auto stub = MetadataService::NewStub(channel);
    ReconfigureRequest request;
    request.set_epoch(epoch);
    request.set_mode(mode);
    request.set_removed_node_id(removed_node_id);
    request.set_added_node_id(added_node_id);
    for (const auto& member : membership) {
        *request.add_membership() = member;
    }
    ReconfigureResponse response;
    grpc::ClientContext context;
    // Bound the call so a slow/stuck node doesn't block the rollout to other
    // survivors. The handler is lightweight (mostly metadata + enqueueing
    // pending re-forwards), so 5 s is generous.
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(5000));
    grpc::Status status = stub->Reconfigure(&context, request, &response);
    return status.ok() && response.success();
}

void TriggerReconfigureRemove(ClusterState* state, const std::string& failed_node_id) {
    std::vector<NodeInfo> new_membership;
    uint64_t new_epoch = 0;
    std::string mode;

    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (state->reconfig_in_progress.exchange(true)) {
            std::cerr << "[metadata] TriggerReconfigureRemove skipped (already in progress)"
                      << " failed=" << failed_node_id << std::endl;
            return;
        }
        bool found_failed_node = false;
        for (const auto& member : state->membership) {
            if (member.node_id() != failed_node_id) {
                new_membership.push_back(member);
            } else {
                found_failed_node = true;
            }
        }
        if (!found_failed_node) {
            state->reconfig_in_progress.store(false);
            std::cerr << "[metadata] TriggerReconfigureRemove skipped (node not in membership)"
                      << " failed=" << failed_node_id << std::endl;
            return;
        }
        state->epoch += 1;
        new_epoch = state->epoch;
        state->membership = new_membership;
        state->last_heartbeat.erase(failed_node_id);
        mode = state->mode;
        state->reconfig_in_progress.store(false);
    }
    std::cerr << "[metadata] TriggerReconfigureRemove start"
              << " failed=" << failed_node_id
              << " new_epoch=" << new_epoch
              << " survivors=" << new_membership.size() << std::endl;

    // Survivors learn about the new ring via this single Reconfigure pass.
    // Each handler installs the new membership/epoch and re-forwards its
    // pending entries through the new chain. A node that misses this RPC
    // catches up via heartbeat-driven RefreshMembership in server.cpp.
    for (const auto& node : new_membership) {
        if (!SendReconfigure(node, new_epoch, new_membership, mode, failed_node_id)) {
            std::cerr << "[metadata] SendReconfigure failed node=" << node.node_id()
                      << " epoch=" << new_epoch
                      << " (will recover via heartbeat refresh)" << std::endl;
        }
    }
}

void TriggerReconfigureAdd(ClusterState* state, const NodeInfo& added_node) {
    std::vector<NodeInfo> new_membership;
    uint64_t new_epoch = 0;
    std::string mode;

    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (state->reconfig_in_progress.exchange(true)) {
            return;
        }
        new_membership = state->membership;
        for (const auto& member : new_membership) {
            if (member.node_id() == added_node.node_id()) {
                state->reconfig_in_progress.store(false);
                return;
            }
        }
        new_membership.push_back(added_node);
        state->epoch += 1;
        new_epoch = state->epoch;
        state->membership = new_membership;
        state->last_heartbeat[added_node.node_id()] = std::chrono::steady_clock::now();
        mode = state->mode;
        state->reconfig_in_progress.store(false);
    }

    for (const auto& node : new_membership) {
        SendReconfigure(node, new_epoch, new_membership, mode, "",
                        added_node.node_id());
    }
}

void TriggerModeChange(ClusterState* state, const std::string& mode) {
    std::vector<NodeInfo> membership;
    uint64_t new_epoch = 0;

    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (state->reconfig_in_progress.exchange(true)) {
            return;
        }
        state->epoch += 1;
        state->mode = mode;
        new_epoch = state->epoch;
        membership = state->membership;
        state->reconfig_in_progress.store(false);
    }

    for (const auto& node : membership) {
        SendReconfigure(node, new_epoch, membership, mode, "");
    }
}

void MonitorFailures(ClusterState* state, std::atomic<bool>* shutdown_flag) {
    while (!shutdown_flag->load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kHeartbeatIntervalMs));
        std::vector<std::pair<std::string, int64_t>> failed_nodes;
        auto now = std::chrono::steady_clock::now();
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            for (const auto& member : state->membership) {
                auto it = state->last_heartbeat.find(member.node_id());
                if (it == state->last_heartbeat.end()) {
                    continue;
                }
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - it->second);
                if (elapsed.count() > kHeartbeatIntervalMs * kHeartbeatMissThreshold) {
                    failed_nodes.emplace_back(member.node_id(), elapsed.count());
                }
            }
        }
        if (!failed_nodes.empty()) {
            std::cerr << "[metadata] MonitorFailures detected dead node="
                      << failed_nodes.front().first
                      << " elapsed_ms=" << failed_nodes.front().second
                      << " threshold_ms="
                      << (static_cast<int64_t>(kHeartbeatIntervalMs) * kHeartbeatMissThreshold)
                      << std::endl;
            TriggerReconfigureRemove(state, failed_nodes.front().first);
        }
    }
}

} // namespace

int main(int argc, char** argv) {
    std::string listen_addr = "0.0.0.0:50050";
    std::string mode = "CROWN";
    std::string members_arg;
    std::string add_member_arg;
    int add_after_ms = 0;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--listen" && i + 1 < argc) {
            listen_addr = argv[++i];
        } else if (arg == "--mode" && i + 1 < argc) {
            mode = argv[++i];
        } else if (arg == "--members" && i + 1 < argc) {
            members_arg = argv[++i];
        } else if (arg == "--add-member" && i + 1 < argc) {
            add_member_arg = argv[++i];
        } else if (arg == "--add-after-ms" && i + 1 < argc) {
            add_after_ms = std::stoi(argv[++i]);
        }
    }

    ClusterState state;
    state.mode = mode;

    if (!members_arg.empty()) {
        for (const auto& part : Split(members_arg, ',')) {
            NodeInfo node;
            if (!ParseMember(part, &node)) {
                std::cerr << "Invalid member entry: " << part << std::endl;
                return 1;
            }
            state.membership.push_back(node);
        }
    }

    MetadataStoreService service(&state);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(listen_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    if (!server) {
        std::cerr << "Failed to start metadata_store on " << listen_addr << std::endl;
        return 1;
    }

    std::atomic<bool> shutdown_flag{false};
    std::thread monitor_thread(MonitorFailures, &state, &shutdown_flag);
    std::thread add_thread;
    if (!add_member_arg.empty()) {
        NodeInfo added_node;
        if (!ParseMember(add_member_arg, &added_node)) {
            std::cerr << "Invalid add-member entry: " << add_member_arg << std::endl;
            return 1;
        }
        add_thread = std::thread([&state, added_node, add_after_ms]() {
            if (add_after_ms > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(add_after_ms));
            }
            TriggerReconfigureAdd(&state, added_node);
        });
    }

    std::cout << "metadata_store listening on " << listen_addr << std::endl;
    server->Wait();

    shutdown_flag.store(true);
    monitor_thread.join();
    if (add_thread.joinable()) {
        add_thread.join();
    }
    return 0;
}

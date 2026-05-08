#include "replication.grpc.pb.h"
#include <grpcpp/grpcpp.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using replication::DrainAckRequest;
using replication::DrainAckResponse;
using replication::FreezeRequest;
using replication::FreezeResponse;
using replication::HeartbeatRequest;
using replication::HeartbeatResponse;
using replication::MembershipRequest;
using replication::MembershipResponse;
using replication::MetadataService;
using replication::NodeInfo;
using replication::ReconfigureRequest;
using replication::ReconfigureResponse;
using replication::ReportFailureRequest;
using replication::ReportFailureResponse;
using replication::SetModeRequest;
using replication::SetModeResponse;
using replication::SyncCompleteRequest;
using replication::SyncCompleteResponse;

namespace {

constexpr int kHeartbeatIntervalMs = 500;
constexpr int kHeartbeatMissThreshold = 3;
constexpr int kFreezeAckTimeoutMs = 2000;

struct ClusterState {
    std::mutex mutex;
    uint64_t epoch = 1;
    std::string mode = "CHAIN";
    std::vector<NodeInfo> membership;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> last_heartbeat;
    std::unordered_set<std::string> drain_acks;
    uint64_t drain_epoch = 0;
    size_t expected_drain_acks = 0;
    std::condition_variable drain_cv;
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

    grpc::Status Heartbeat(grpc::ServerContext*, const HeartbeatRequest* request,
                           HeartbeatResponse* response) override {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->last_heartbeat[request->node_id()] = std::chrono::steady_clock::now();
        response->set_alive(true);
        response->set_epoch(state_->epoch);
        return grpc::Status::OK;
    }

    grpc::Status ReportDrainAck(grpc::ServerContext*, const DrainAckRequest* request,
                                DrainAckResponse* response) override {
        std::lock_guard<std::mutex> lock(state_->mutex);
        if (request->epoch() == state_->drain_epoch) {
            state_->drain_acks.insert(request->node_id());
            if (state_->expected_drain_acks > 0 &&
                state_->drain_acks.size() >= state_->expected_drain_acks) {
                state_->drain_cv.notify_all();
            }
        }
        response->set_success(true);
        return grpc::Status::OK;
    }

    grpc::Status ReportSyncDone(grpc::ServerContext*, const SyncCompleteRequest*,
                                SyncCompleteResponse* response) override {
        response->set_success(true);
        return grpc::Status::OK;
    }

    grpc::Status ReportFailure(grpc::ServerContext*, const ReportFailureRequest* request,
                               ReportFailureResponse* response) override {
        response->set_success(true);
        std::thread(TriggerReconfigureRemove, state_, request->failed_node_id()).detach();
        return grpc::Status::OK;
    }

private:
    ClusterState* state_;
};

bool SendFreeze(const NodeInfo& node, uint64_t epoch,
                const std::vector<NodeInfo>& new_membership) {
    auto channel = grpc::CreateChannel(ToAddress(node), grpc::InsecureChannelCredentials());
    auto stub = MetadataService::NewStub(channel);
    FreezeRequest request;
    request.set_epoch(epoch);
    for (const auto& member : new_membership) {
        *request.add_v_new() = member;
    }
    FreezeResponse response;
    grpc::ClientContext context;
    auto deadline = std::chrono::system_clock::now() +
                    std::chrono::milliseconds(kFreezeAckTimeoutMs);
    context.set_deadline(deadline);
    grpc::Status status = stub->Freeze(&context, request, &response);
    return status.ok() && response.success();
}

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
    grpc::Status status = stub->Reconfigure(&context, request, &response);
    return status.ok() && response.success();
}

void TriggerReconfigureRemove(ClusterState* state, const std::string& failed_node_id) {
    std::vector<NodeInfo> old_membership;
    std::vector<NodeInfo> new_membership;
    uint64_t old_epoch = 0;
    std::string mode;

    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (state->reconfig_in_progress.exchange(true)) {
            return;
        }
        old_epoch = state->epoch;
        mode = state->mode;
        old_membership = state->membership;
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
            return;
        }
        state->drain_epoch = old_epoch;
        state->expected_drain_acks = new_membership.size();
        state->drain_acks.clear();
    }

    std::vector<NodeInfo> freeze_confirmed_membership;
    for (const auto& node : old_membership) {
        if (node.node_id() == failed_node_id) {
            continue;
        }
        if (SendFreeze(node, old_epoch, new_membership)) {
            for (const auto& candidate : new_membership) {
                if (candidate.node_id() == node.node_id()) {
                    freeze_confirmed_membership.push_back(candidate);
                    break;
                }
            }
        }
    }

    if (freeze_confirmed_membership.empty()) {
        freeze_confirmed_membership = new_membership;
    }

    {
        std::unique_lock<std::mutex> lock(state->mutex);
        state->expected_drain_acks = freeze_confirmed_membership.size();
        if (state->expected_drain_acks > 0) {
            state->drain_cv.wait_for(
                lock, std::chrono::milliseconds(kFreezeAckTimeoutMs), [&]() {
                    return state->drain_acks.size() >= state->expected_drain_acks;
                });
        }
        state->epoch = old_epoch + 1;
        state->membership = freeze_confirmed_membership;
        state->last_heartbeat.erase(failed_node_id);
        std::unordered_set<std::string> live_ids;
        for (const auto& member : state->membership) {
            live_ids.insert(member.node_id());
        }
        for (const auto& member : old_membership) {
            if (live_ids.count(member.node_id()) == 0) {
                state->last_heartbeat.erase(member.node_id());
            }
        }
        state->reconfig_in_progress.store(false);
        new_membership = state->membership;
    }

    for (const auto& node : new_membership) {
        SendReconfigure(node, old_epoch + 1, new_membership, mode, failed_node_id);
    }
}

void TriggerReconfigureAdd(ClusterState* state, const NodeInfo& added_node) {
    std::vector<NodeInfo> old_membership;
    std::vector<NodeInfo> new_membership;
    uint64_t old_epoch = 0;
    std::string mode;

    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (state->reconfig_in_progress.exchange(true)) {
            return;
        }
        old_epoch = state->epoch;
        mode = state->mode;
        old_membership = state->membership;
        new_membership = state->membership;
        for (const auto& member : new_membership) {
            if (member.node_id() == added_node.node_id()) {
                state->reconfig_in_progress.store(false);
                return;
            }
        }
        new_membership.push_back(added_node);
        state->drain_epoch = old_epoch;
        state->expected_drain_acks = old_membership.size();
        state->drain_acks.clear();
    }

    for (const auto& node : old_membership) {
        SendFreeze(node, old_epoch, new_membership);
    }

    {
        std::unique_lock<std::mutex> lock(state->mutex);
        if (state->expected_drain_acks > 0) {
            state->drain_cv.wait_for(
                lock, std::chrono::milliseconds(kFreezeAckTimeoutMs), [&]() {
                    return state->drain_acks.size() >= state->expected_drain_acks;
                });
        }
        state->epoch = old_epoch + 1;
        state->membership = new_membership;
        state->last_heartbeat[added_node.node_id()] = std::chrono::steady_clock::now();
        state->reconfig_in_progress.store(false);
    }

    for (const auto& node : new_membership) {
        SendReconfigure(node, old_epoch + 1, new_membership, mode, "",
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
        std::vector<std::string> failed_nodes;
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
                    failed_nodes.push_back(member.node_id());
                }
            }
        }
        if (!failed_nodes.empty()) {
            TriggerReconfigureRemove(state, failed_nodes.front());
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

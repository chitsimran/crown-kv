#include "replication.grpc.pb.h"
#include "replication/common/replication_common.h"
#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using replication::ClientAckService;
using replication::CommitAck;
using replication::CommitAckResponse;
using replication::GetRequest;
using replication::GetResponse;
using replication::MembershipRequest;
using replication::MembershipResponse;
using replication::MetadataService;
using replication::NodeInfo;
using replication::PutRequest;
using replication::PutResponse;
using replication::ReplicationService;
using replication::SetModeRequest;
using replication::SetModeResponse;

namespace {

struct PendingKey {
    std::string key;
    uint64_t version = 0;

    bool operator==(const PendingKey& other) const {
        return version == other.version && key == other.key;
    }
};

struct PendingKeyHash {
    size_t operator()(const PendingKey& key) const {
        return std::hash<std::string>{}(key.key) ^ (std::hash<uint64_t>{}(key.version) << 1);
    }
};

struct PendingEntry {
    std::chrono::steady_clock::time_point sent;
};

struct ClientState {
    std::mutex mutex;
    std::condition_variable cv;
    std::unordered_map<PendingKey, PendingEntry, PendingKeyHash> pending;
    std::unordered_map<PendingKey, std::chrono::steady_clock::time_point, PendingKeyHash>
        committed_early;
    uint64_t completed = 0;
    uint64_t expected = 0;
    double total_latency_ms = 0.0;
    bool started = false;
    std::chrono::steady_clock::time_point start_time;
};

class CommitAckServiceImpl final : public ClientAckService::Service {
public:
    explicit CommitAckServiceImpl(ClientState* state) : state_(state) {}

    grpc::Status CommitAck(grpc::ServerContext*, const replication::CommitAck* request,
                           CommitAckResponse* response) override {
        auto now = std::chrono::steady_clock::now();
        std::unique_lock<std::mutex> lock(state_->mutex);
        PendingKey key{request->key(), request->version()};
        auto it = state_->pending.find(key);
        if (it != state_->pending.end()) {
            auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
                now - it->second.sent);
            state_->total_latency_ms += latency.count() / 1000.0;
            state_->completed += 1;
            state_->pending.erase(it);
            if (state_->completed >= state_->expected) {
                state_->cv.notify_all();
            }
        } else {
            state_->committed_early[key] = now;
        }
        response->set_success(true);
        return grpc::Status::OK;
    }

private:
    ClientState* state_;
};

struct MembershipState {
    uint64_t epoch = 0;
    std::string mode;
    std::vector<NodeInfo> members;
    std::vector<std::shared_ptr<ReplicationService::Stub>> stubs;
};

std::string MemberAddressAt(const MembershipState& membership, int index) {
    if (index < 0 || index >= static_cast<int>(membership.members.size())) {
        return "<invalid>";
    }
    return replication_common::address_from_node(membership.members[index]);
}

void PrintRouteDebug(const std::string& op, const MembershipState& membership,
                     const std::string& key, int index, int attempt) {
    std::cout << op << " route: mode=" << membership.mode
              << " epoch=" << membership.epoch
              << " key=" << key
              << " index=" << index
              << " target=" << MemberAddressAt(membership, index)
              << " attempt=" << attempt << std::endl;
}

bool FetchMembership(const std::unique_ptr<MetadataService::Stub>& stub,
                     MembershipState* state) {
    MembershipRequest request;
    MembershipResponse response;
    grpc::ClientContext context;
    grpc::Status status = stub->GetMembership(&context, request, &response);
    if (!status.ok()) {
        return false;
    }
    state->epoch = response.epoch();
    state->mode = response.mode();
    state->members.assign(response.membership().begin(), response.membership().end());
    state->stubs.clear();
    for (const auto& member : state->members) {
        state->stubs.push_back(replication_common::make_replication_stub(member));
    }
    return true;
}

std::vector<std::string> SplitWords(const std::string& line) {
    std::stringstream stream(line);
    std::vector<std::string> words;
    std::string word;
    while (stream >> word) {
        words.push_back(word);
    }
    return words;
}

std::string JoinWords(const std::vector<std::string>& words, size_t begin) {
    std::string joined;
    for (size_t i = begin; i < words.size(); ++i) {
        if (!joined.empty()) {
            joined += " ";
        }
        joined += words[i];
    }
    return joined;
}

int SelectWriteNodeIndex(const MembershipState& membership, const std::string& key) {
    int ring_size = static_cast<int>(membership.members.size());
    if (ring_size == 0) {
        return -1;
    }
    if (membership.mode == "CHAIN" || membership.mode == "CRAQ") {
        return 0;
    }
    return replication_common::head_index(key, ring_size);
}

int SelectReadNodeIndex(const MembershipState& membership, const std::string& key) {
    int ring_size = static_cast<int>(membership.members.size());
    if (ring_size == 0) {
        return -1;
    }
    if (membership.mode == "CHAIN") {
        return ring_size - 1;
    }
    return replication_common::head_index(key, ring_size);
}

bool SendPutWithRetry(const std::string& key, const std::string& value,
                      const std::string& client_addr, std::atomic<uint64_t>* request_id,
                      const std::unique_ptr<MetadataService::Stub>& metadata_stub,
                      MembershipState* membership, PutResponse* out_response) {
    for (int attempt = 0; attempt < 3; ++attempt) {
        int index = SelectWriteNodeIndex(*membership, key);
        if (index < 0 || index >= static_cast<int>(membership->stubs.size())) {
            std::cout << "put route error: mode=" << membership->mode
                      << " epoch=" << membership->epoch
                      << " key=" << key
                      << " selected_index=" << index
                      << " members=" << membership->members.size() << std::endl;
            return false;
        }
        PrintRouteDebug("put", *membership, key, index, attempt + 1);
        PutRequest request;
        request.set_request_id((*request_id)++);
        request.set_key(key);
        request.set_value(value);
        request.set_version(0);
        request.set_client_addr(client_addr);
        request.set_epoch(membership->epoch);

        PutResponse response;
        grpc::ClientContext context;
        grpc::Status status = membership->stubs[index]->Put(&context, request, &response);
        if (!status.ok()) {
            std::cout << "put rpc failed: code=" << status.error_code()
                      << " message=" << status.error_message() << std::endl;
            continue;
        }
        if (response.success()) {
            *out_response = response;
            return true;
        }
        std::cout << "put response error: " << response.error()
                  << " version=" << response.version() << std::endl;
        if (response.error().rfind("WRONG_NODE", 0) == 0 ||
            response.error() == "STALE_EPOCH") {
            std::cout << "refreshing membership after " << response.error() << std::endl;
            if (!FetchMembership(metadata_stub, membership)) {
                std::cout << "membership refresh failed" << std::endl;
                return false;
            }
            continue;
        }
        *out_response = response;
        return false;
    }
    return false;
}

bool SendGetWithRetry(const std::string& key,
                      const std::unique_ptr<MetadataService::Stub>& metadata_stub,
                      MembershipState* membership, GetResponse* out_response) {
    for (int attempt = 0; attempt < 3; ++attempt) {
        int index = SelectReadNodeIndex(*membership, key);
        if (index < 0 || index >= static_cast<int>(membership->stubs.size())) {
            std::cout << "get route error: mode=" << membership->mode
                      << " epoch=" << membership->epoch
                      << " key=" << key
                      << " selected_index=" << index
                      << " members=" << membership->members.size() << std::endl;
            return false;
        }
        PrintRouteDebug("get", *membership, key, index, attempt + 1);
        GetRequest request;
        request.set_key(key);
        request.set_epoch(membership->epoch);
        GetResponse response;
        grpc::ClientContext context;
        grpc::Status status = membership->stubs[index]->Get(&context, request, &response);
        if (!status.ok()) {
            std::cout << "get rpc failed: code=" << status.error_code()
                      << " message=" << status.error_message() << std::endl;
            continue;
        }
        if (response.error() == "WRONG_NODE" || response.error() == "STALE_EPOCH") {
            std::cout << "get response error: " << response.error()
                      << "; refreshing membership" << std::endl;
            if (!FetchMembership(metadata_stub, membership)) {
                std::cout << "membership refresh failed" << std::endl;
                return false;
            }
            continue;
        }
        *out_response = response;
        return true;
    }
    return false;
}

void PrintMembership(const MembershipState& membership) {
    std::cout << "Epoch: " << membership.epoch << std::endl;
    std::cout << "Mode: " << membership.mode << std::endl;
    std::cout << "Members:" << std::endl;
    for (const auto& member : membership.members) {
        std::cout << "  " << member.node_id() << " @ " << member.address() << ":"
                  << member.port() << std::endl;
    }
}

void PrintHelp() {
    std::cout
        << "Commands:\n"
        << "  members                    Show epoch, mode, and nodes\n"
        << "  mode                       Show current mode\n"
        << "  use CHAIN|CRAQ|CROWN       Change cluster mode via metadata_store\n"
        << "  put KEY VALUE              Write and wait for CommitAck\n"
        << "  put-async KEY VALUE        Write and return after head acceptance\n"
        << "  get KEY                    Read using current routing rules\n"
        << "  bench N [csv] [hot%] [hot-set%]\n"
        << "                             Single-threaded async open-loop writes from a\n"
        << "                             CSV produced by setup/generate_kv_dataset.py.\n"
        << "                             hot% is the share of writes targeting hot keys;\n"
        << "                             hot-set% is the share of unique keys in the hot\n"
        << "                             pool. The dataset is loaded once and cached.\n"
        << "  pending                    Show client-side uncommitted writes\n"
        << "  refresh                    Refetch membership from metadata_store\n"
        << "  help                       Show this help\n"
        << "  quit                       Exit\n";
}

bool SetClusterMode(const std::unique_ptr<MetadataService::Stub>& metadata_stub,
                    const std::string& mode, MembershipState* membership) {
    SetModeRequest request;
    request.set_mode(mode);
    SetModeResponse response;
    grpc::ClientContext context;
    grpc::Status status = metadata_stub->SetMode(&context, request, &response);
    if (!status.ok()) {
        std::cout << "mode change failed: " << status.error_message() << std::endl;
        return false;
    }
    if (!response.success()) {
        std::cout << "mode change error: " << response.error() << std::endl;
        return false;
    }

    for (int i = 0; i < 10; ++i) {
        if (!FetchMembership(metadata_stub, membership)) {
            return false;
        }
        if (membership->mode == mode) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return FetchMembership(metadata_stub, membership);
}

void RecordAcceptedWrite(ClientState* state, const PendingKey& pending_key,
                         const PendingEntry& entry) {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (!state->started) {
        state->started = true;
        state->start_time = entry.sent;
    }
    state->expected += 1;
    auto early_it = state->committed_early.find(pending_key);
    if (early_it != state->committed_early.end()) {
        auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
            early_it->second - entry.sent);
        state->total_latency_ms += latency.count() / 1000.0;
        state->committed_early.erase(early_it);
        state->completed += 1;
        state->cv.notify_all();
        return;
    }
    state->pending[pending_key] = entry;
}

bool WaitForCommit(ClientState* state, const PendingKey& pending_key, int timeout_ms) {
    std::unique_lock<std::mutex> lock(state->mutex);
    return state->cv.wait_for(lock, std::chrono::milliseconds(timeout_ms), [&]() {
        return state->pending.find(pending_key) == state->pending.end();
    });
}

// Loads a CSV produced by setup/generate_kv_dataset.py. The file has a header
// row "key,value" followed by alphabetic 10-byte keys and 64-byte values. We
// read every row eagerly so the bench command never builds keys on the fly.
bool LoadDatasetCsv(const std::string& path,
                    std::vector<std::pair<std::string, std::string>>* out) {
    std::ifstream file(path);
    if (!file.is_open()) {
        std::cerr << "failed to open dataset file: " << path << std::endl;
        return false;
    }
    out->clear();
    std::string line;
    bool header = true;
    while (std::getline(file, line)) {
        if (header) {
            header = false;
            continue;
        }
        if (line.empty()) {
            continue;
        }
        auto comma = line.find(',');
        if (comma == std::string::npos) {
            continue;
        }
        out->emplace_back(line.substr(0, comma), line.substr(comma + 1));
    }
    return !out->empty();
}

// Mirrors apply_hot_skew in setup/generate_kv_dataset.py: hot_share% of writes
// are drawn from the first hot_set_share% of the dataset (the "hot pool"), the
// remainder cycles through the rest. Hot rows come first, then cold rows —
// matching the python script so behavior is consistent across pre-baked CSVs
// and client-side skewing.
std::vector<std::pair<std::string, std::string>> BuildSkewedWorkload(
    const std::vector<std::pair<std::string, std::string>>& dataset,
    uint64_t total_count, int hot_share, int hot_set_share) {
    std::vector<std::pair<std::string, std::string>> workload;
    if (dataset.empty() || total_count == 0) {
        return workload;
    }
    workload.reserve(total_count);
    if (hot_share <= 0) {
        for (uint64_t i = 0; i < total_count; ++i) {
            workload.push_back(dataset[i % dataset.size()]);
        }
        return workload;
    }
    uint64_t hot_count = std::max<uint64_t>(1, total_count * hot_share / 100);
    uint64_t cold_count = total_count - hot_count;
    size_t hot_set_size = std::max<size_t>(
        1, dataset.size() * static_cast<size_t>(hot_set_share) / 100);
    if (hot_set_size > dataset.size()) {
        hot_set_size = dataset.size();
    }
    size_t cold_pool_size = dataset.size() - hot_set_size;

    for (uint64_t i = 0; i < hot_count; ++i) {
        workload.push_back(dataset[i % hot_set_size]);
    }
    for (uint64_t i = 0; i < cold_count; ++i) {
        if (cold_pool_size == 0) {
            workload.push_back(dataset[i % hot_set_size]);
        } else {
            workload.push_back(dataset[hot_set_size + (i % cold_pool_size)]);
        }
    }
    return workload;
}

void PrintClientStats(ClientState* state) {
    std::lock_guard<std::mutex> lock(state->mutex);
    double avg_latency = state->completed > 0 ? state->total_latency_ms / state->completed : 0.0;
    std::cout << "completed=" << state->completed << " expected=" << state->expected
              << " pending=" << state->pending.size()
              << " avg_latency_ms=" << avg_latency << std::endl;
}

void RunRepl(const std::unique_ptr<MetadataService::Stub>& metadata_stub,
             MembershipState* membership, const std::string& listen_addr,
             ClientState* state, std::atomic<uint64_t>* request_id) {
    PrintHelp();
    std::string line;
    while (true) {
        std::cout << "crown-kv(" << membership->mode << "@" << membership->epoch << ")> ";
        if (!std::getline(std::cin, line)) {
            break;
        }
        auto words = SplitWords(line);
        if (words.empty()) {
            continue;
        }
        const std::string& command = words[0];
        if (command == "quit" || command == "exit") {
            break;
        }
        if (command == "help") {
            PrintHelp();
        } else if (command == "members") {
            PrintMembership(*membership);
        } else if (command == "mode") {
            std::cout << membership->mode << " (epoch " << membership->epoch << ")"
                      << std::endl;
        } else if (command == "refresh") {
            if (FetchMembership(metadata_stub, membership)) {
                std::cout << "refreshed: " << membership->mode << " epoch "
                          << membership->epoch << std::endl;
            } else {
                std::cout << "refresh failed" << std::endl;
            }
        } else if (command == "use") {
            if (words.size() != 2) {
                std::cout << "usage: use CHAIN|CRAQ|CROWN" << std::endl;
                continue;
            }
            if (SetClusterMode(metadata_stub, words[1], membership)) {
                std::cout << "mode is now " << membership->mode << " at epoch "
                          << membership->epoch << std::endl;
            }
        } else if (command == "get") {
            if (words.size() != 2) {
                std::cout << "usage: get KEY" << std::endl;
                continue;
            }
            GetResponse response;
            if (!SendGetWithRetry(words[1], metadata_stub, membership, &response)) {
                std::cout << "get failed" << std::endl;
                continue;
            }
            if (!response.error().empty()) {
                std::cout << "error: " << response.error() << std::endl;
            } else {
                std::cout << response.value() << " (version " << response.version() << ")"
                          << std::endl;
            }
        } else if (command == "put" || command == "put-async") {
            if (words.size() < 3) {
                std::cout << "usage: " << command << " KEY VALUE" << std::endl;
                continue;
            }
            std::string value = JoinWords(words, 2);
            PendingEntry entry{std::chrono::steady_clock::now()};
            PutResponse response;
            if (!SendPutWithRetry(words[1], value, listen_addr, request_id, metadata_stub,
                                  membership, &response)) {
                std::cout << "put failed" << std::endl;
                continue;
            }
            if (!response.success()) {
                std::cout << "error: " << response.error() << std::endl;
                continue;
            }
            PendingKey pending_key{words[1], response.version()};
            RecordAcceptedWrite(state, pending_key, entry);
            std::cout << "accepted version " << response.version();
            if (command == "put") {
                if (WaitForCommit(state, pending_key, 10000)) {
                    std::cout << ", committed" << std::endl;
                } else {
                    std::cout << ", still pending" << std::endl;
                }
            } else {
                std::cout << ", pending commit" << std::endl;
            }
        } else if (command == "bench") {
            if (words.size() < 2) {
                std::cout << "usage: bench N [csv-path] [hot-share%] [hot-set-share%]\n"
                          << "  csv-path     defaults to setup/generated_kv_dataset/all_kv_pairs.csv\n"
                          << "  hot-share    0-100, percentage of writes targeting hot keys (default 0)\n"
                          << "  hot-set-share 1-100, percentage of unique keys in the hot pool (default 10)"
                          << std::endl;
                continue;
            }
            uint64_t count = std::stoull(words[1]);
            std::string csv_path = words.size() >= 3
                ? words[2]
                : std::string("setup/generated_kv_dataset/all_kv_pairs.csv");
            int hot_share = words.size() >= 4 ? std::stoi(words[3]) : 0;
            int hot_set_share = words.size() >= 5 ? std::stoi(words[4]) : 10;
            if (hot_share < 0 || hot_share > 100) {
                std::cout << "hot-share must be 0-100" << std::endl;
                continue;
            }
            if (hot_set_share < 1 || hot_set_share > 100) {
                std::cout << "hot-set-share must be 1-100" << std::endl;
                continue;
            }

            // Cache the dataset across bench invocations: re-reading 20K+ rows
            // from disk per call is wasteful and noisy. Reload only on path change.
            static std::string cached_path;
            static std::vector<std::pair<std::string, std::string>> cached_dataset;
            if (csv_path != cached_path || cached_dataset.empty()) {
                auto load_start = std::chrono::steady_clock::now();
                if (!LoadDatasetCsv(csv_path, &cached_dataset)) {
                    cached_path.clear();
                    cached_dataset.clear();
                    std::cout << "bench aborted: could not load dataset" << std::endl;
                    continue;
                }
                cached_path = csv_path;
                auto load_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - load_start).count();
                std::cout << "loaded " << cached_dataset.size() << " rows from "
                          << csv_path << " (" << load_ms << " ms)" << std::endl;
            }

            // Build the full workload before any timing starts. Once the timed
            // section opens, no more key/value materialization happens.
            auto build_start = std::chrono::steady_clock::now();
            std::vector<std::pair<std::string, std::string>> workload =
                BuildSkewedWorkload(cached_dataset, count, hot_share, hot_set_share);
            if (workload.size() != count) {
                std::cout << "bench aborted: workload empty (dataset size="
                          << cached_dataset.size() << ")" << std::endl;
                continue;
            }
            auto build_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - build_start).count();
            std::cout << "prepared workload: count=" << count
                      << " hot_share=" << hot_share << "%"
                      << " hot_set_share=" << hot_set_share << "%"
                      << " (" << build_ms << " ms)" << std::endl;

            uint64_t start_completed = 0;
            uint64_t accepted = 0;
            {
                std::lock_guard<std::mutex> lock(state->mutex);
                start_completed = state->completed;
            }

            struct AsyncPutCall {
                uint64_t index = 0;
                std::string key;
                PendingEntry entry;
                grpc::ClientContext context;
                PutResponse response;
                grpc::Status status;
                std::unique_ptr<grpc::ClientAsyncResponseReader<PutResponse>> rpc;
            };

            grpc::CompletionQueue completion_queue;
            std::vector<std::unique_ptr<AsyncPutCall>> calls;
            calls.reserve(count);
            uint64_t failed = 0;
            auto rpc_deadline = std::chrono::system_clock::now() + std::chrono::seconds(30);
            auto bench_start = std::chrono::steady_clock::now();
            for (uint64_t i = 0; i < count; ++i) {
                int index = SelectWriteNodeIndex(*membership, workload[i].first);
                if (index < 0 || index >= static_cast<int>(membership->stubs.size())) {
                    failed += 1;
                    if (failed <= 10) {
                        std::cout << "bench route error: mode=" << membership->mode
                                  << " epoch=" << membership->epoch
                                  << " key=" << workload[i].first
                                  << " selected_index=" << index
                                  << " members=" << membership->members.size() << std::endl;
                    }
                    continue;
                }

                auto call = std::make_unique<AsyncPutCall>();
                call->index = i;
                call->key = workload[i].first;
                call->entry = PendingEntry{std::chrono::steady_clock::now()};
                call->context.set_deadline(rpc_deadline);

                PutRequest request;
                request.set_request_id((*request_id)++);
                request.set_key(workload[i].first);
                request.set_value(workload[i].second);
                request.set_version(0);
                request.set_client_addr(listen_addr);
                request.set_epoch(membership->epoch);

                auto* tag = call.get();
                call->rpc = membership->stubs[index]->PrepareAsyncPut(
                    &call->context, request, &completion_queue);
                call->rpc->StartCall();
                call->rpc->Finish(&call->response, &call->status, tag);
                calls.push_back(std::move(call));
            }

            void* tag = nullptr;
            bool ok = false;
            uint64_t completed_acceptances = 0;
            while (completed_acceptances < calls.size() &&
                   completion_queue.Next(&tag, &ok)) {
                completed_acceptances += 1;
                auto* call = static_cast<AsyncPutCall*>(tag);
                if (!ok || !call->status.ok() || !call->response.success()) {
                    failed += 1;
                    if (failed <= 10) {
                        std::cout << "put failed at " << call->index;
                        if (!call->response.error().empty()) {
                            std::cout << ": " << call->response.error();
                        } else if (!call->status.ok()) {
                            std::cout << ": " << call->status.error_message();
                        }
                        std::cout << " target="
                                  << MemberAddressAt(*membership,
                                                     SelectWriteNodeIndex(*membership,
                                                                         call->key))
                                  << " mode=" << membership->mode
                                  << " epoch=" << membership->epoch;
                        std::cout << std::endl;
                    }
                    continue;
                }
                RecordAcceptedWrite(state, PendingKey{call->key, call->response.version()},
                                    call->entry);
                accepted += 1;
            }
            completion_queue.Shutdown();

            {
                std::unique_lock<std::mutex> lock(state->mutex);
                state->cv.wait(lock, [&]() {
                    return state->completed >= start_completed + accepted;
                });
            }
            auto elapsed = std::chrono::duration_cast<std::chrono::duration<double>>(
                std::chrono::steady_clock::now() - bench_start);
            std::cout << "committed " << accepted << " writes in " << elapsed.count()
                      << "s (" << (accepted / elapsed.count()) << " ops/sec)"
                      << ", failed " << failed
                      << ", async single-threaded" << std::endl;
        } else if (command == "pending") {
            PrintClientStats(state);
        } else {
            std::cout << "unknown command: " << command << std::endl;
        }
    }
}

} // namespace

int main(int argc, char** argv) {
    std::string metadata_addr = "127.0.0.1:50050";
    std::string listen_addr = "0.0.0.0:6000";
    std::string key_prefix = "key";
    std::string value_prefix = "value";
    std::string get_key;
    uint64_t put_count = 0;
    bool show_membership = false;
    bool repl = false;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--metadata" && i + 1 < argc) {
            metadata_addr = argv[++i];
        } else if (arg == "--listen" && i + 1 < argc) {
            listen_addr = argv[++i];
        } else if (arg == "--get-membership") {
            show_membership = true;
        } else if (arg == "--repl") {
            repl = true;
        } else if (arg == "--put-count" && i + 1 < argc) {
            put_count = static_cast<uint64_t>(std::stoull(argv[++i]));
        } else if (arg == "--key-prefix" && i + 1 < argc) {
            key_prefix = argv[++i];
        } else if (arg == "--value-prefix" && i + 1 < argc) {
            value_prefix = argv[++i];
        } else if (arg == "--get" && i + 1 < argc) {
            get_key = argv[++i];
        }
    }

    auto metadata_channel = grpc::CreateChannel(metadata_addr,
                                                grpc::InsecureChannelCredentials());
    auto metadata_stub = MetadataService::NewStub(metadata_channel);

    MembershipState membership;
    if (!FetchMembership(metadata_stub, &membership)) {
        std::cerr << "GetMembership failed" << std::endl;
        return 1;
    }

    if (repl) {
        ClientState state;
        CommitAckServiceImpl ack_service(&state);
        grpc::ServerBuilder builder;
        builder.AddListeningPort(listen_addr, grpc::InsecureServerCredentials());
        builder.RegisterService(&ack_service);
        std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
        if (!server) {
            std::cerr << "Failed to start client ack server" << std::endl;
            return 1;
        }
        std::atomic<uint64_t> request_id{1};
        RunRepl(metadata_stub, &membership, listen_addr, &state, &request_id);
        server->Shutdown();
        return 0;
    }

    if (show_membership) {
        PrintMembership(membership);
        if (put_count == 0 && get_key.empty()) {
            return 0;
        }
    }

    if (!get_key.empty()) {
        GetResponse response;
        if (!SendGetWithRetry(get_key, metadata_stub, &membership, &response)) {
            std::cerr << "Get failed" << std::endl;
            return 1;
        }
        if (!response.error().empty()) {
            std::cout << "Get error: " << response.error() << std::endl;
            return 0;
        }
        std::cout << "Value: " << response.value() << " (version " << response.version()
                  << ")" << std::endl;
        return 0;
    }

    if (put_count == 0) {
        std::cout << "Usage: client --put-count N [--metadata host:port] [--listen host:port]"
                  << std::endl;
        return 0;
    }

    ClientState state;
    CommitAckServiceImpl ack_service(&state);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(listen_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&ack_service);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    if (!server) {
        std::cerr << "Failed to start client ack server" << std::endl;
        return 1;
    }

    std::atomic<uint64_t> request_id{1};
    std::thread sender([&]() {
        for (uint64_t i = 0; i < put_count; ++i) {
            std::string key = key_prefix + std::to_string(i);
            std::string value = value_prefix + std::to_string(i);
            PutResponse response;
            PendingEntry entry{std::chrono::steady_clock::now()};
            if (!SendPutWithRetry(key, value, listen_addr, &request_id, metadata_stub,
                                  &membership, &response)) {
                std::cerr << "Put failed for key " << key << std::endl;
                continue;
            }
            if (!response.success()) {
                std::cerr << "Put error: " << response.error() << std::endl;
                continue;
            }
            PendingKey pending_key{key, response.version()};
            RecordAcceptedWrite(&state, pending_key, entry);
        }
    });

    sender.join();

    {
        std::unique_lock<std::mutex> lock(state.mutex);
        state.cv.wait(lock, [&]() { return state.completed >= state.expected; });
    }

    auto end_time = std::chrono::steady_clock::now();
    double elapsed_sec = 0.0;
    if (state.started) {
        elapsed_sec = std::chrono::duration_cast<std::chrono::duration<double>>(
            end_time - state.start_time)
            .count();
    }
    double throughput = elapsed_sec > 0.0 ? state.completed / elapsed_sec : 0.0;
    double avg_latency = state.completed > 0 ? state.total_latency_ms / state.completed : 0.0;

    std::cout << "Completed: " << state.completed << " / " << state.expected << std::endl;
    std::cout << "Throughput: " << throughput << " ops/sec" << std::endl;
    std::cout << "Average latency: " << avg_latency << " ms" << std::endl;

    server->Shutdown();
    return 0;
}

#include "replication.grpc.pb.h"
#include "client/throughput_window.h"
#include "replication/common/replication_common.h"
#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <system_error>
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
    uint64_t benchmark_id = 0;
};

struct CommitEvent {
    std::chrono::steady_clock::time_point committed_at;
    double latency_ms = 0.0;
    uint64_t completed_sequence = 0;
    uint64_t benchmark_id = 0;
};

struct KvRow {
    std::string key;
    std::string value;
};

struct WorkloadItem {
    std::string key;
    std::string value;
    int target_index = -1;
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
    uint64_t next_benchmark_id = 1;
    std::vector<CommitEvent> commit_events;
    std::atomic<uint64_t> live_writes{0};
    std::atomic<uint64_t> current_benchmark_id{0};
};

bool TryReleaseLiveWrite(ClientState* state, uint64_t benchmark_id) {
    if (state->current_benchmark_id.load(std::memory_order_acquire) != benchmark_id) {
        return false;
    }
    uint64_t live = state->live_writes.load(std::memory_order_relaxed);
    while (live > 0) {
        if (state->live_writes.compare_exchange_weak(
                live, live - 1, std::memory_order_acq_rel,
                std::memory_order_relaxed)) {
            return true;
        }
    }
    return false;
}

void RecordCommitLocked(ClientState* state, const PendingEntry& entry,
                        std::chrono::steady_clock::time_point committed_at) {
    auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
        committed_at - entry.sent);
    double latency_ms = latency.count() / 1000.0;
    state->total_latency_ms += latency_ms;
    state->completed += 1;
    state->commit_events.push_back(
        CommitEvent{committed_at, latency_ms, state->completed, entry.benchmark_id});
    TryReleaseLiveWrite(state, entry.benchmark_id);
    state->cv.notify_all();
}

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
            RecordCommitLocked(state_, it->second, now);
            state_->pending.erase(it);
            state_->cv.notify_all();
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

struct BenchmarkRouting {
    MembershipState membership;
    std::vector<int> target_indexes;
    std::mutex mutex;
    std::atomic<bool> refresh_in_progress{false};
    std::chrono::steady_clock::time_point last_refresh_attempt;
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

bool IsUnsignedIntegerString(const std::string& value) {
    if (value.empty()) {
        return false;
    }
    return std::all_of(value.begin(), value.end(), [](char c) {
        return c >= '0' && c <= '9';
    });
}

std::string ToLower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
        if (c >= 'A' && c <= 'Z') {
            return static_cast<char>(c - 'A' + 'a');
        }
        return static_cast<char>(c);
    });
    return value;
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
        << "  bench N [csv] [hot%] [hot-set%] [window-ms] [output-prefix] [max-outstanding] [open|closed]\n"
        << "                             Async writes from a\n"
        << "                             CSV produced by setup/generate_kv_dataset.py.\n"
        << "                             hot% is the share of writes targeting hot keys;\n"
        << "                             hot-set% is the share of unique keys in the hot\n"
        << "                             pool. window-ms defaults to 1000. Throughput CSV\n"
        << "                             and PNG default to bench_results/throughput_*.{csv,png}.\n"
        << "                             Key target nodes are precomputed before timing.\n"
        << "                             closed-loop is default; open-loop is available\n"
        << "                             with open or --open-loop. max-outstanding\n"
        << "                             defaults to 1000 for closed-loop and may be set\n"
        << "                             with --max-outstanding N.\n"
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
        RecordCommitLocked(state, entry, early_it->second);
        state->committed_early.erase(early_it);
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
bool LoadDatasetCsv(const std::string& path, std::vector<KvRow>* out) {
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
        out->push_back(KvRow{line.substr(0, comma), line.substr(comma + 1)});
    }
    return !out->empty();
}

std::string RouteCacheKey(const std::string& csv_path,
                          const MembershipState& membership) {
    std::ostringstream key;
    key << csv_path << '|' << membership.mode << '|' << membership.epoch;
    for (const auto& member : membership.members) {
        key << '|' << member.node_id() << '@'
            << replication_common::address_from_node(member);
    }
    return key.str();
}

std::vector<uint64_t> CountTargets(const std::vector<WorkloadItem>& rows,
                                   size_t member_count) {
    std::vector<uint64_t> counts(member_count, 0);
    for (const auto& row : rows) {
        if (row.target_index >= 0 &&
            row.target_index < static_cast<int>(counts.size())) {
            counts[row.target_index] += 1;
        }
    }
    return counts;
}

uint64_t SumCounts(const std::vector<uint64_t>& counts) {
    uint64_t total = 0;
    for (uint64_t count : counts) {
        total += count;
    }
    return total;
}

void PrintTargetCounts(const std::string& label,
                       const MembershipState& membership,
                       const std::vector<uint64_t>& counts,
                       const std::string& unit) {
    uint64_t total = SumCounts(counts);
    std::cout << label << " (mode=" << membership.mode
              << " epoch=" << membership.epoch
              << ", total=" << total << ")" << std::endl;
    size_t printable = std::min(counts.size(), membership.members.size());
    for (size_t i = 0; i < printable; ++i) {
        double pct = total > 0
            ? (100.0 * static_cast<double>(counts[i]) / static_cast<double>(total))
            : 0.0;
        std::ostringstream pct_text;
        pct_text << std::fixed << std::setprecision(2) << pct;
        std::cout << "  " << membership.members[i].node_id()
                  << " @ " << MemberAddressAt(membership, static_cast<int>(i))
                  << ": " << counts[i] << " " << unit
                  << " (" << pct_text.str() << "%)" << std::endl;
    }
    if (counts.size() > membership.members.size()) {
        uint64_t stale_total = 0;
        for (size_t i = membership.members.size(); i < counts.size(); ++i) {
            stale_total += counts[i];
        }
        if (stale_total > 0) {
            std::cout << "  <stale membership indexes>: " << stale_total << " "
                      << unit << std::endl;
        }
    }
}

void PrintIssuedTargetCounts(
    const std::string& label,
    const std::vector<std::pair<std::string, uint64_t>>& counts) {
    uint64_t total = 0;
    for (const auto& entry : counts) {
        total += entry.second;
    }
    std::cout << label << " (total=" << total << ")" << std::endl;
    for (const auto& entry : counts) {
        double pct = total > 0
            ? (100.0 * static_cast<double>(entry.second) / static_cast<double>(total))
            : 0.0;
        std::ostringstream pct_text;
        pct_text << std::fixed << std::setprecision(2) << pct;
        std::cout << "  " << entry.first << ": " << entry.second
                  << " writes (" << pct_text.str() << "%)" << std::endl;
    }
}

bool PrecomputeWriteRoutes(const std::vector<KvRow>& dataset,
                           const MembershipState& membership,
                           std::vector<WorkloadItem>* routed_dataset) {
    routed_dataset->clear();
    routed_dataset->reserve(dataset.size());
    bool ok = true;
    int printed_errors = 0;
    for (const auto& row : dataset) {
        int index = SelectWriteNodeIndex(membership, row.key);
        if (index < 0 || index >= static_cast<int>(membership.stubs.size())) {
            ok = false;
            if (printed_errors < 10) {
                std::cout << "route precompute error: mode=" << membership.mode
                          << " epoch=" << membership.epoch
                          << " key=" << row.key
                          << " selected_index=" << index
                          << " members=" << membership.members.size() << std::endl;
                printed_errors += 1;
            }
        }
        routed_dataset->push_back(WorkloadItem{row.key, row.value, index});
    }
    return ok;
}

std::vector<uint64_t> CountBenchmarkTargets(
    const BenchmarkRouting& routing, size_t member_count) {
    std::vector<uint64_t> counts(member_count, 0);
    for (const auto& target : routing.target_indexes) {
        int index = target;
        if (index >= 0 && index < static_cast<int>(counts.size())) {
            counts[index] += 1;
        }
    }
    return counts;
}

void RebuildBenchmarkRoutes(BenchmarkRouting* routing,
                            const std::vector<WorkloadItem>& workload,
                            const MembershipState& membership) {
    if (routing->target_indexes.size() != workload.size()) {
        routing->target_indexes.assign(workload.size(), -1);
    }
    for (size_t i = 0; i < workload.size(); ++i) {
        routing->target_indexes[i] =
            SelectWriteNodeIndex(membership, workload[i].key);
    }
}

void MaybeRefreshBenchmarkRouting(
    BenchmarkRouting* routing,
    const std::unique_ptr<MetadataService::Stub>& metadata_stub,
    MembershipState* repl_membership,
    const std::vector<WorkloadItem>& workload,
    const std::string& reason) {
    bool expected = false;
    if (!routing->refresh_in_progress.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel,
            std::memory_order_relaxed)) {
        return;
    }

    auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(routing->mutex);
        if (routing->last_refresh_attempt.time_since_epoch().count() != 0 &&
            now - routing->last_refresh_attempt < std::chrono::seconds(1)) {
            routing->refresh_in_progress.store(false, std::memory_order_release);
            return;
        }
        routing->last_refresh_attempt = now;
    }

    MembershipState refreshed;
    bool ok = FetchMembership(metadata_stub, &refreshed);
    if (ok) {
        std::lock_guard<std::mutex> lock(routing->mutex);
        uint64_t old_epoch = routing->membership.epoch;
        routing->membership = refreshed;
        *repl_membership = refreshed;
        RebuildBenchmarkRoutes(routing, workload, refreshed);
        std::cout << "bench refreshed routes after " << reason
                  << ": mode=" << refreshed.mode
                  << " epoch=" << refreshed.epoch
                  << " (was " << old_epoch << ")"
                  << " members=" << refreshed.members.size() << std::endl;
        PrintTargetCounts("updated write distribution", refreshed,
                          CountBenchmarkTargets(*routing, refreshed.members.size()),
                          "writes");
    } else {
        std::cout << "bench route refresh failed after " << reason << std::endl;
    }
    routing->refresh_in_progress.store(false, std::memory_order_release);
}

// Mirrors apply_hot_skew in setup/generate_kv_dataset.py: hot_share% of writes
// are drawn from the first hot_set_share% of the dataset (the "hot pool"), the
// remainder cycles through the rest. Hot rows come first, then cold rows —
// matching the python script so behavior is consistent across pre-baked CSVs
// and client-side skewing.
std::vector<WorkloadItem> BuildSkewedWorkload(
    const std::vector<WorkloadItem>& dataset,
    uint64_t total_count, int hot_share, int hot_set_share) {
    std::vector<WorkloadItem> workload;
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

uint64_t CountBenchmarkCommitsLocked(const ClientState* state, uint64_t benchmark_id,
                                     size_t first_event_index) {
    uint64_t count = 0;
    for (size_t i = first_event_index; i < state->commit_events.size(); ++i) {
        if (state->commit_events[i].benchmark_id == benchmark_id) {
            count += 1;
        }
    }
    return count;
}

std::vector<benchmark::CommitSample> SnapshotBenchmarkSamples(
    ClientState* state, uint64_t benchmark_id, size_t first_event_index,
    std::chrono::steady_clock::time_point bench_start) {
    std::lock_guard<std::mutex> lock(state->mutex);
    std::vector<benchmark::CommitSample> samples;
    for (size_t i = first_event_index; i < state->commit_events.size(); ++i) {
        const auto& event = state->commit_events[i];
        if (event.benchmark_id != benchmark_id) {
            continue;
        }
        benchmark::CommitSample sample;
        sample.time_sec = std::chrono::duration_cast<std::chrono::duration<double>>(
            event.committed_at - bench_start).count();
        if (sample.time_sec < 0.0) {
            sample.time_sec = 0.0;
        }
        sample.latency_ms = event.latency_ms;
        sample.completed_sequence = event.completed_sequence;
        samples.push_back(sample);
    }
    return samples;
}

std::string TimestampForFilename() {
    auto now = std::chrono::system_clock::now();
    std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    std::tm* tm = std::localtime(&now_time);
    if (!tm) {
        return "unknown_time";
    }
    std::ostringstream out;
    out << std::put_time(tm, "%Y%m%d_%H%M%S");
    return out.str();
}

std::pair<std::filesystem::path, std::filesystem::path> BenchmarkOutputPaths(
    const std::string& output_prefix) {
    std::filesystem::path prefix;
    if (output_prefix.empty()) {
        prefix = std::filesystem::path("bench_results") /
                 ("throughput_" + TimestampForFilename());
    } else {
        prefix = output_prefix;
    }

    std::filesystem::path csv_path = prefix;
    std::filesystem::path png_path = prefix;
    if (csv_path.extension() == ".csv") {
        png_path.replace_extension(".png");
    } else {
        csv_path += ".csv";
        png_path += ".png";
    }
    return {csv_path, png_path};
}

bool WriteThroughputCsv(const std::filesystem::path& csv_path,
                        const std::vector<benchmark::ThroughputWindowRow>& rows) {
    std::error_code ec;
    auto parent = csv_path.parent_path();
    if (!parent.empty()) {
        std::filesystem::create_directories(parent, ec);
        if (ec) {
            std::cerr << "failed to create benchmark output directory "
                      << parent.string() << ": " << ec.message() << std::endl;
            return false;
        }
    }

    std::ofstream out(csv_path);
    if (!out.is_open()) {
        std::cerr << "failed to write throughput CSV: " << csv_path.string() << std::endl;
        return false;
    }

    out << "time_sec,window_start_sec,window_end_sec,commits,throughput_ops_sec,"
           "cumulative_commits,failed,accepted,orphaned\n";
    out << std::fixed << std::setprecision(6);
    for (const auto& row : rows) {
        out << row.time_sec << ','
            << row.window_start_sec << ','
            << row.window_end_sec << ','
            << row.commits << ','
            << row.throughput_ops_sec << ','
            << row.cumulative_commits << ','
            << row.failed << ','
            << row.accepted << ','
            << row.orphaned << '\n';
    }
    return true;
}

std::string ShellQuote(const std::string& value) {
    std::string quoted = "'";
    for (char c : value) {
        if (c == '\'') {
            quoted += "'\\''";
        } else {
            quoted += c;
        }
    }
    quoted += "'";
    return quoted;
}

void TryPlotThroughputCsv(const std::filesystem::path& csv_path,
                          const std::filesystem::path& png_path) {
    std::string command = "python3 setup/plot_throughput.py " +
                          ShellQuote(csv_path.string()) + " " +
                          ShellQuote(png_path.string());
    int rc = std::system(command.c_str());
    if (rc == 0) {
        std::cout << "throughput plot: " << png_path.string() << std::endl;
        return;
    }
    std::cout << "throughput plot skipped; CSV is available at "
              << csv_path.string()
              << " (run `python3 setup/plot_throughput.py "
              << csv_path.string() << " " << png_path.string()
              << "` after installing matplotlib)" << std::endl;
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
                std::cout << "usage: bench N [csv-path] [hot-share%] [hot-set-share%] [window-ms] [output-prefix] [max-outstanding] [open|closed]\n"
                          << "  csv-path     defaults to setup/generated_kv_dataset/all_kv_pairs.csv\n"
                          << "  hot-share    0-100, percentage of writes targeting hot keys (default 0)\n"
                          << "  hot-set-share 1-100, percentage of unique keys in the hot pool (default 10)\n"
                          << "  window-ms    sliding throughput window in milliseconds (default 1000)\n"
                          << "  output-prefix defaults to bench_results/throughput_<timestamp>\n"
                          << "  max-outstanding caps live in-flight writes in closed-loop mode (default 1000)\n"
                          << "                  positional, --max-outstanding N, or --max-outstanding=N\n"
                          << "  open|closed   closed-loop is default; use open or --open-loop for open-loop"
                          << std::endl;
                continue;
            }
            uint64_t count = std::stoull(words[1]);
            std::string csv_path = "setup/generated_kv_dataset/all_kv_pairs.csv";
            int hot_share = 0;
            int hot_set_share = 10;
            int window_ms = 1000;
            std::string output_prefix;
            uint64_t max_outstanding = 1000;
            bool closed_loop = true;
            bool bad_bench_arg = false;

            auto is_bench_mode_arg = [](const std::string& arg) {
                std::string arg_lower = ToLower(arg);
                return arg_lower == "open" || arg_lower == "open-loop" ||
                       arg_lower == "--open-loop" || arg_lower == "closed" ||
                       arg_lower == "closed-loop" || arg_lower == "--closed-loop";
            };

            size_t arg_index = 2;
            if (arg_index < words.size() && !is_bench_mode_arg(words[arg_index])) {
                csv_path = words[arg_index++];
            }

            int numeric_optional_index = 0;
            for (; arg_index < words.size(); ++arg_index) {
                const std::string arg_lower = ToLower(words[arg_index]);
                if (arg_lower == "open" || arg_lower == "open-loop" ||
                    arg_lower == "--open-loop") {
                    closed_loop = false;
                } else if (arg_lower == "closed" || arg_lower == "closed-loop" ||
                           arg_lower == "--closed-loop") {
                    closed_loop = true;
                } else if (arg_lower == "--max-outstanding" ||
                           arg_lower == "--max_outstanding" ||
                           arg_lower == "--outstanding") {
                    if (arg_index + 1 >= words.size() ||
                        !IsUnsignedIntegerString(words[arg_index + 1])) {
                        std::cout << words[arg_index]
                                  << " requires a positive integer value" << std::endl;
                        bad_bench_arg = true;
                        break;
                    }
                    max_outstanding = std::stoull(words[++arg_index]);
                } else if (arg_lower.rfind("--max-outstanding=", 0) == 0 ||
                           arg_lower.rfind("--max_outstanding=", 0) == 0 ||
                           arg_lower.rfind("--outstanding=", 0) == 0) {
                    auto equals = words[arg_index].find('=');
                    std::string value = words[arg_index].substr(equals + 1);
                    if (!IsUnsignedIntegerString(value)) {
                        std::cout << "invalid max-outstanding value: "
                                  << value << std::endl;
                        bad_bench_arg = true;
                        break;
                    }
                    max_outstanding = std::stoull(value);
                } else if (IsUnsignedIntegerString(words[arg_index])) {
                    if (numeric_optional_index == 0) {
                        hot_share = std::stoi(words[arg_index]);
                    } else if (numeric_optional_index == 1) {
                        hot_set_share = std::stoi(words[arg_index]);
                    } else if (numeric_optional_index == 2) {
                        window_ms = std::stoi(words[arg_index]);
                    } else if (numeric_optional_index == 3) {
                        max_outstanding = std::stoull(words[arg_index]);
                    } else {
                        std::cout << "too many numeric bench options: "
                                  << words[arg_index] << std::endl;
                        bad_bench_arg = true;
                        break;
                    }
                    numeric_optional_index += 1;
                } else if (output_prefix.empty()) {
                    output_prefix = words[arg_index];
                } else {
                    std::cout << "unknown bench option: " << words[arg_index]
                              << std::endl;
                    bad_bench_arg = true;
                    break;
                }
            }
            if (bad_bench_arg) {
                continue;
            }
            if (hot_share < 0 || hot_share > 100) {
                std::cout << "hot-share must be 0-100" << std::endl;
                continue;
            }
            if (hot_set_share < 1 || hot_set_share > 100) {
                std::cout << "hot-set-share must be 1-100" << std::endl;
                continue;
            }
            if (window_ms <= 0) {
                std::cout << "window-ms must be positive" << std::endl;
                continue;
            }
            if (closed_loop && max_outstanding == 0) {
                std::cout << "max-outstanding must be positive" << std::endl;
                continue;
            }

            // Cache the dataset across bench invocations: re-reading 20K+ rows
            // from disk per call is wasteful and noisy. Reload only on path change.
            if (!FetchMembership(metadata_stub, membership)) {
                std::cout << "bench aborted: membership refresh failed" << std::endl;
                continue;
            }

            static std::string cached_path;
            static std::vector<KvRow> cached_dataset;
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

            static std::string cached_route_key;
            static std::vector<WorkloadItem> cached_routed_dataset;
            std::string route_key = RouteCacheKey(csv_path, *membership);
            if (route_key != cached_route_key ||
                cached_routed_dataset.size() != cached_dataset.size()) {
                auto route_start = std::chrono::steady_clock::now();
                if (!PrecomputeWriteRoutes(cached_dataset, *membership,
                                           &cached_routed_dataset)) {
                    cached_route_key.clear();
                    cached_routed_dataset.clear();
                    std::cout << "bench aborted: could not precompute write routes"
                              << std::endl;
                    continue;
                }
                cached_route_key = route_key;
                auto route_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - route_start).count();
                std::cout << "precomputed write targets for "
                          << cached_routed_dataset.size() << " keys"
                          << " (" << route_ms << " ms)" << std::endl;
                PrintTargetCounts("dataset target distribution", *membership,
                                  CountTargets(cached_routed_dataset,
                                               membership->members.size()),
                                  "keys");
            }

            // Build the full routed workload before any timing starts. Once the
            // timed section opens, no more key/value materialization or routing
            // hash calculation happens.
            auto build_start = std::chrono::steady_clock::now();
            std::vector<WorkloadItem> workload =
                BuildSkewedWorkload(cached_routed_dataset, count, hot_share,
                                    hot_set_share);
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
                      << " mode=" << (closed_loop ? "closed-loop" : "open-loop")
                      << (closed_loop ? " max_outstanding=" : "")
                      << (closed_loop ? std::to_string(max_outstanding) : "")
                      << " (" << build_ms << " ms)" << std::endl;
            PrintTargetCounts("planned write distribution", *membership,
                              CountTargets(workload, membership->members.size()),
                              "writes");

            BenchmarkRouting benchmark_routing;
            {
                std::lock_guard<std::mutex> lock(benchmark_routing.mutex);
                benchmark_routing.membership = *membership;
                RebuildBenchmarkRoutes(&benchmark_routing, workload, *membership);
            }

            uint64_t benchmark_id = 0;
            size_t first_commit_event = 0;
            {
                std::lock_guard<std::mutex> lock(state->mutex);
                benchmark_id = state->next_benchmark_id++;
                first_commit_event = state->commit_events.size();
                state->live_writes.store(0, std::memory_order_release);
                state->current_benchmark_id.store(benchmark_id,
                                                  std::memory_order_release);
            }

            struct AsyncPutCall {
                uint64_t index = 0;
                std::string key;
                int target_index = -1;
                std::string target_addr;
                std::string route_mode;
                uint64_t route_epoch = 0;
                PendingEntry entry;
                grpc::ClientContext context;
                PutResponse response;
                grpc::Status status;
                std::unique_ptr<grpc::ClientAsyncResponseReader<PutResponse>> rpc;
            };

            // Sharded async issue + drain. Each shard owns one CompletionQueue
            // with one issuer thread feeding it and one drainer thread consuming
            // tags. This removes the single-threaded bottleneck on both the
            // request-issue side and the response-drain side, and avoids
            // contention on a shared CQ.
            constexpr int kShardCount = 4;
            constexpr auto kRpcDeadline = std::chrono::seconds(30);

            std::vector<std::unique_ptr<grpc::CompletionQueue>> cqs;
            cqs.reserve(kShardCount);
            for (int i = 0; i < kShardCount; ++i) {
                cqs.push_back(std::make_unique<grpc::CompletionQueue>());
            }

            std::vector<std::unique_ptr<AsyncPutCall>> calls(count);
            std::atomic<uint64_t> issue_failed{0};
            std::atomic<uint64_t> rpc_failed{0};
            std::atomic<uint64_t> accepted_atomic{0};
            std::atomic<uint64_t> printed_failures{0};
            std::atomic<uint64_t> skipped_due_to_stall{0};
            std::atomic<bool> stop_issuing{false};

            std::mutex issued_counts_mutex;
            std::vector<std::pair<std::string, uint64_t>> issued_by_target;

            auto issuer_fn = [&](int shard_id) {
                auto& cq = *cqs[shard_id];
                size_t chunk = (count + kShardCount - 1) / kShardCount;
                size_t start = static_cast<size_t>(shard_id) * chunk;
                size_t end = std::min(start + chunk, static_cast<size_t>(count));
                for (size_t i = start; i < end; ++i) {
                    if (closed_loop) {
                        std::unique_lock<std::mutex> lock(state->mutex);
                        state->cv.wait(lock, [&]() {
                            return stop_issuing.load(std::memory_order_acquire) ||
                                   state->current_benchmark_id.load(
                                       std::memory_order_acquire) != benchmark_id ||
                                   state->live_writes.load(
                                       std::memory_order_acquire) < max_outstanding;
                        });
                        if (stop_issuing.load(std::memory_order_acquire) ||
                            state->current_benchmark_id.load(
                                std::memory_order_acquire) != benchmark_id) {
                            skipped_due_to_stall.fetch_add(end - i,
                                                           std::memory_order_relaxed);
                            break;
                        }
                        state->live_writes.fetch_add(1, std::memory_order_acq_rel);
                    }

                    int index = -1;
                    MembershipState route_membership;
                    std::shared_ptr<ReplicationService::Stub> route_stub;
                    {
                        std::lock_guard<std::mutex> lock(benchmark_routing.mutex);
                        if (i < benchmark_routing.target_indexes.size()) {
                            index = benchmark_routing.target_indexes[i];
                        }
                        route_membership = benchmark_routing.membership;
                        if (index >= 0 &&
                            index < static_cast<int>(route_membership.stubs.size())) {
                            route_stub = route_membership.stubs[index];
                        }
                    }
                    if (!route_stub) {
                        issue_failed.fetch_add(1);
                        if (closed_loop) {
                            std::lock_guard<std::mutex> lock(state->mutex);
                            TryReleaseLiveWrite(state, benchmark_id);
                            state->cv.notify_all();
                        }
                        if (printed_failures.fetch_add(1) < 10) {
                            std::cout << "bench route error: mode=" << route_membership.mode
                                      << " epoch=" << route_membership.epoch
                                      << " key=" << workload[i].key
                                      << " selected_index=" << index
                                      << " members=" << route_membership.members.size()
                                      << std::endl;
                        }
                        continue;
                    }

                    auto call = std::make_unique<AsyncPutCall>();
                    call->index = i;
                    call->key = workload[i].key;
                    call->target_index = index;
                    call->target_addr = MemberAddressAt(route_membership, index);
                    call->route_mode = route_membership.mode;
                    call->route_epoch = route_membership.epoch;
                    call->entry = PendingEntry{std::chrono::steady_clock::now(),
                                               benchmark_id};
                    // Per-call deadline so late-issued calls in a large bench
                    // get a fresh 30 s budget instead of sharing a deadline
                    // captured before the issue loop began.
                    call->context.set_deadline(
                        std::chrono::system_clock::now() + kRpcDeadline);

                    PutRequest request;
                    request.set_request_id((*request_id)++);
                    request.set_key(workload[i].key);
                    request.set_value(workload[i].value);
                    request.set_version(0);
                    request.set_client_addr(listen_addr);

                    auto* tag = call.get();
                    call->rpc = route_stub->PrepareAsyncPut(
                        &call->context, request, &cq);
                    call->rpc->StartCall();
                    call->rpc->Finish(&call->response, &call->status, tag);
                    if (!closed_loop) {
                        state->live_writes.fetch_add(1, std::memory_order_acq_rel);
                    }
                    {
                        std::lock_guard<std::mutex> lock(issued_counts_mutex);
                        auto it = std::find_if(
                            issued_by_target.begin(), issued_by_target.end(),
                            [&](const std::pair<std::string, uint64_t>& entry) {
                                return entry.first == call->target_addr;
                            });
                        if (it == issued_by_target.end()) {
                            issued_by_target.emplace_back(call->target_addr, 1);
                        } else {
                            it->second += 1;
                        }
                    }
                    calls[i] = std::move(call);
                }
            };

            auto drainer_fn = [&](int shard_id) {
                auto& cq = *cqs[shard_id];
                void* tag = nullptr;
                bool ok = false;
                while (cq.Next(&tag, &ok)) {
                    auto* call = static_cast<AsyncPutCall*>(tag);
                    if (!ok || !call->status.ok() || !call->response.success()) {
                        rpc_failed.fetch_add(1);
                        {
                            std::lock_guard<std::mutex> lock(state->mutex);
                            TryReleaseLiveWrite(state, benchmark_id);
                            state->cv.notify_all();
                        }
                        if (printed_failures.fetch_add(1) < 10) {
                            std::ostringstream out;
                            out << "put failed at " << call->index;
                            std::string reason;
                            if (!call->response.error().empty()) {
                                reason = call->response.error();
                                out << ": " << reason;
                            } else if (!call->status.ok()) {
                                reason = call->status.error_message();
                                out << ": " << reason;
                            }
                            out << " target=" << call->target_addr
                                << " mode=" << call->route_mode
                                << " epoch=" << call->route_epoch;
                            std::cout << out.str() << std::endl;
                        }
                        std::string refresh_reason =
                            !call->response.error().empty()
                                ? call->response.error()
                                : call->status.error_message();
                        MaybeRefreshBenchmarkRouting(
                            &benchmark_routing, metadata_stub, membership,
                            workload, refresh_reason.empty()
                                          ? std::string("put failure")
                                          : refresh_reason);
                        continue;
                    }
                    RecordAcceptedWrite(
                        state,
                        PendingKey{call->key, call->response.version()},
                        call->entry);
                    accepted_atomic.fetch_add(1);
                }
            };

            // Throughput-window origin is captured BEFORE issuance starts so
            // CommitAcks that arrive during the issue phase get plotted at
            // their real time rather than clamped to t=0 (which would bunch
            // them all into the first window). Drainers run concurrently with
            // issuers, so acks are recorded the moment they arrive.
            auto bench_start = std::chrono::steady_clock::now();

            // Drainers must be running before issuers so tags can be consumed
            // as soon as they complete (otherwise the gRPC client side queues
            // them up and we waste memory plus delay accept latency).
            std::vector<std::thread> drainers;
            drainers.reserve(kShardCount);
            for (int i = 0; i < kShardCount; ++i) {
                drainers.emplace_back(drainer_fn, i);
            }

            std::vector<std::thread> issuers;
            issuers.reserve(kShardCount);
            for (int i = 0; i < kShardCount; ++i) {
                issuers.emplace_back(issuer_fn, i);
            }

            // Watcher thread prints status every few seconds so a slow or
            // failing run never looks frozen. Critical when a ring node is
            // killed: ~N/3 calls sit in the CQ waiting for DEADLINE_EXCEEDED
            // (30 s per call), and without this, the user sees no output
            // between issue-end and the wait-for-CommitAck loop.
            std::atomic<bool> watcher_done{false};
            std::thread watcher([&]() {
                constexpr auto kIssueStallTimeout = std::chrono::seconds(15);
                constexpr auto kWatcherPoll = std::chrono::milliseconds(200);
                constexpr auto kWatcherPrint = std::chrono::seconds(3);
                uint64_t last_accounted = 0;
                uint64_t last_committed = 0;
                auto last_progress = std::chrono::steady_clock::now();
                auto last_print = last_progress;
                while (!watcher_done.load()) {
                    std::this_thread::sleep_for(kWatcherPoll);
                    if (watcher_done.load()) {
                        break;
                    }
                    uint64_t a = accepted_atomic.load();
                    uint64_t fi = issue_failed.load();
                    uint64_t fr = rpc_failed.load();
                    uint64_t skipped = skipped_due_to_stall.load();
                    uint64_t accounted = a + fi + fr;
                    uint64_t committed_now = 0;
                    {
                        std::lock_guard<std::mutex> lock(state->mutex);
                        committed_now = CountBenchmarkCommitsLocked(
                            state, benchmark_id, first_commit_event);
                    }
                    if (accounted > last_accounted || committed_now > last_committed) {
                        last_accounted = accounted;
                        last_committed = committed_now;
                        last_progress = std::chrono::steady_clock::now();
                    }
                    auto stalled = std::chrono::steady_clock::now() - last_progress;
                    uint64_t live = state->live_writes.load(std::memory_order_acquire);
                    auto now = std::chrono::steady_clock::now();
                    if (now - last_print >= kWatcherPrint) {
                        last_print = now;
                        std::cout << "bench drain: bench_mode="
                                  << (closed_loop ? "closed-loop" : "open-loop")
                                  << " accepted=" << a
                                  << " committed=" << committed_now
                                  << " rpc_failed=" << fr
                                  << " issue_failed=" << fi
                                  << " live=" << live
                                  << "/" << max_outstanding
                                  << " skipped=" << skipped
                                  << "/" << count
                                  << std::endl;
                    }
                    if (!stop_issuing.load(std::memory_order_acquire) &&
                        closed_loop && live > 0 && stalled >= kIssueStallTimeout) {
                        stop_issuing.store(true, std::memory_order_release);
                        state->cv.notify_all();
                        std::cout << "bench issuing stopped: no progress for "
                                  << std::chrono::duration_cast<std::chrono::seconds>(
                                         kIssueStallTimeout).count()
                                  << "s" << std::endl;
                    }
                }
            });

            for (auto& t : issuers) {
                t.join();
            }
            auto issue_end = std::chrono::steady_clock::now();

            for (auto& cq : cqs) {
                cq->Shutdown();
            }
            for (auto& t : drainers) {
                t.join();
            }

            watcher_done.store(true);
            watcher.join();

            uint64_t failed = issue_failed.load() + rpc_failed.load();
            uint64_t accepted = accepted_atomic.load();
            uint64_t skipped = skipped_due_to_stall.load();

            std::vector<std::pair<std::string, uint64_t>> issued_snapshot;
            {
                std::lock_guard<std::mutex> lock(issued_counts_mutex);
                issued_snapshot = issued_by_target;
            }
            PrintIssuedTargetCounts("issued writes by target node", issued_snapshot);

            // Wait for CommitAcks, but bail out if no progress is made for a
            // while. Without this, a single orphaned write (e.g. the original
            // head was killed mid-flight before forwarding could happen)
            // hangs the entire bench forever.
            constexpr auto kStallTimeout = std::chrono::seconds(15);
            constexpr auto kProgressTick = std::chrono::seconds(5);
            uint64_t orphaned = 0;
            auto last_progress = std::chrono::steady_clock::now();
            uint64_t last_seen = 0;
            while (true) {
                std::unique_lock<std::mutex> lock(state->mutex);
                bool met = state->cv.wait_for(lock, kProgressTick, [&]() {
                    return CountBenchmarkCommitsLocked(state, benchmark_id,
                                                       first_commit_event) >= accepted;
                });
                uint64_t completed_now = CountBenchmarkCommitsLocked(
                    state, benchmark_id, first_commit_event);
                lock.unlock();
                if (met) {
                    break;
                }
                auto now = std::chrono::steady_clock::now();
                if (completed_now > last_seen) {
                    last_seen = completed_now;
                    last_progress = now;
                }
                auto stalled = now - last_progress;
                uint64_t outstanding = accepted > completed_now
                    ? accepted - completed_now
                    : 0;
                std::cout << "bench wait: completed="
                          << completed_now
                          << "/" << accepted
                          << " outstanding=" << outstanding
                          << " stalled_for="
                          << std::chrono::duration_cast<std::chrono::seconds>(stalled).count()
                          << "s" << std::endl;
                if (stalled >= kStallTimeout) {
                    orphaned = outstanding;
                    std::cout << "bench giving up: " << orphaned
                              << " writes orphaned (no CommitAck after "
                              << std::chrono::duration_cast<std::chrono::seconds>(kStallTimeout).count()
                              << "s of no progress)" << std::endl;
                    break;
                }
            }
            auto elapsed = std::chrono::duration_cast<std::chrono::duration<double>>(
                std::chrono::steady_clock::now() - bench_start);
            auto issue_dur = std::chrono::duration_cast<std::chrono::duration<double>>(
                issue_end - bench_start);
            auto samples = SnapshotBenchmarkSamples(state, benchmark_id,
                                                     first_commit_event, bench_start);
            state->current_benchmark_id.store(0, std::memory_order_release);
            state->live_writes.store(0, std::memory_order_release);
            state->cv.notify_all();
            uint64_t committed = static_cast<uint64_t>(samples.size());
            orphaned = accepted > committed ? accepted - committed : 0;
            double last_commit_sec = 0.0;
            for (const auto& sample : samples) {
                last_commit_sec = std::max(last_commit_sec, sample.time_sec);
            }
            double throughput = last_commit_sec > 0.0 ? committed / last_commit_sec : 0.0;
            // Active throughput excludes the issue phase: from the moment the
            // last write was issued to the last CommitAck. Useful for comparing
            // protocols when issue overhead is non-trivial relative to total wall.
            double active_sec = last_commit_sec - issue_dur.count();
            double active_throughput = active_sec > 0.0 ? committed / active_sec : 0.0;
            double window_sec = window_ms / 1000.0;
            auto rows = benchmark::BuildThroughputWindows(
                samples, last_commit_sec, window_sec, failed, accepted, orphaned);
            auto [csv_output_path, png_output_path] = BenchmarkOutputPaths(output_prefix);
            if (WriteThroughputCsv(csv_output_path, rows)) {
                std::cout << "throughput csv: " << csv_output_path.string() << std::endl;
                TryPlotThroughputCsv(csv_output_path, png_output_path);
            }
            std::cout << "committed " << committed << " writes"
                      << " (" << (closed_loop ? "closed-loop" : "open-loop") << ")"
                      << " in " << last_commit_sec << "s"
                      << " (" << throughput << " ops/sec)"
                      << ", wall " << elapsed.count() << "s"
                      << ", issue " << issue_dur.count() << "s"
                      << (closed_loop ? ", max_outstanding " : "")
                      << (closed_loop ? std::to_string(max_outstanding) : "")
                      << ", post-issue " << active_sec << "s"
                      << " (" << active_throughput << " ops/sec)"
                      << ", failed " << failed
                      << ", skipped " << skipped
                      << ", orphaned " << orphaned
                      << std::endl;
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

    auto configure_ack_server = [](grpc::ServerBuilder& builder) {
        // Every CommitAck for the entire bench arrives at this server, so the
        // default sync config (single CQ, 1 poller) is the wall under load.
        builder.AddChannelArgument(GRPC_ARG_MAX_CONCURRENT_STREAMS, 1024);
        builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS, 30000);
        builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5000);
        builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
        builder.AddChannelArgument(
            GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 10000);
        builder.SetSyncServerOption(grpc::ServerBuilder::NUM_CQS, 4);
        builder.SetSyncServerOption(grpc::ServerBuilder::MIN_POLLERS, 4);
        builder.SetSyncServerOption(grpc::ServerBuilder::MAX_POLLERS, 16);
    };

    if (repl) {
        ClientState state;
        CommitAckServiceImpl ack_service(&state);
        grpc::ServerBuilder builder;
        configure_ack_server(builder);
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
    configure_ack_server(builder);
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

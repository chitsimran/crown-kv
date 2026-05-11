#pragma once

#include "../replication.h"
#include "../common/replication_common.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

class CrownReplication : public Replication {
public:
    CrownReplication() = default;

    void update_membership(const std::vector<replication::NodeInfo>& membership,
                           const std::string& node_id);

    PutResponse handle_put(PutRequest request) override;
    GetResponse handle_get(std::string key) override;
    void handle_ack(int64_t request_id) override;
    std::shared_ptr<ReplicationService::Stub> get_next_stub() override;

    replication::VersionQueryResponse handle_version_query(
        const replication::VersionQueryRequest& request);

    // When true, CROWN uses chain-style reads and versioning:
    //   - writes go straight to the committed store (no dirty entries)
    //   - reads only serve from the per-key tail (no version-query to tail)
    // When false (default), CROWN behaves like CRAQ: dirty+committed split,
    // any-node reads with version validation against the tail.
    void set_chain_mode(bool enabled) { chain_mode_.store(enabled); }
    bool chain_mode() const { return chain_mode_.load(); }

private:
    struct CleanState {
        uint64_t version = 0;
        bool found = false;
    };

    static CleanState get_committed_state(const std::string& key);
    static bool has_dirty(const std::string& key);
    static std::optional<Record> get_dirty_record(const std::string& key, uint64_t version);

    std::mutex ring_mutex_;
    int node_index_ = 0;
    int ring_size_ = 0;
    std::vector<replication::NodeInfo> membership_;
    std::shared_ptr<ReplicationService::Stub> prev_stub_;
    std::shared_ptr<ReplicationService::Stub> next_stub_;
    std::atomic<bool> chain_mode_{false};
};

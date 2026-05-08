#pragma once

#include "../replication.h"
#include "../common/replication_common.h"

#include <mutex>
#include <memory>
#include <vector>

class ChainReplication : public Replication {
public:
    ChainReplication() = default;

    void update_membership(const std::vector<replication::NodeInfo>& membership,
                           const std::string& node_id);

    PutResponse handle_put(PutRequest request) override;
    GetResponse handle_get(std::string key) override;
    void handle_ack(int64_t request_id) override;

private:
    std::mutex ring_mutex_;
    int node_index_ = 0;
    int ring_size_ = 0;
    std::vector<replication::NodeInfo> membership_;
    std::shared_ptr<ReplicationService::Stub> prev_stub_;
    std::shared_ptr<ReplicationService::Stub> next_stub_;
};

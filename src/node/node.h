#include "../proto/replication.pb.h"
#include "proto/replication.grpc.pb.h"

class Node {
private:
    int node_id;
    public:
    NodeAddress my_address;
    ReplicationMode mode = ReplicationMode::CHAIN;
    KeyRange head_range;
    KeyRange tail_range;
    bool _is_head = false;
    bool _is_tail = false;
    std::optional<std::unique_ptr<ReplicationService::Stub>> prev;
    std::optional<std::unique_ptr<ReplicationService::Stub>> next;
    std::vector<KeyRange> key_ranges;
public:
    bool is_head(std::string& key);

    bool is_tail(std::string& key);
};

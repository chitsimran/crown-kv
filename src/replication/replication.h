#include <iostream>
#include <mutex>

#include "../proto/replication.pb.h"
#include "proto/replication.grpc.pb.h"
#include "../kv_store/kv_store.h"

class Replication {
private:
    KVStore kv_store;
    std::unordered_map<char, std::unique_ptr<ReplicationService::Stub>> key_mapping;
public:
    void handle_put(PutRequest request) {};

    void handle_get(std::string key) {};

    void handle_ack(int64_t request_id) {};
};
#include "../src/kv_store/kv_store.h"
#include "../src/replication/chain/chain_replication.h"
#include "../src/replication/common/replication_common.h"
#include "../src/replication/crown/crown_replication.h"

#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

namespace {

replication::NodeInfo make_node(const std::string& node_id, int port) {
    replication::NodeInfo node;
    node.set_node_id(node_id);
    node.set_address("127.0.0.1");
    node.set_port(port);
    return node;
}

std::vector<replication::NodeInfo> make_membership(int count) {
    std::vector<replication::NodeInfo> membership;
    for (int i = 0; i < count; ++i) {
        membership.push_back(make_node("n" + std::to_string(i), 50051 + i));
    }
    return membership;
}

std::string key_with_head_not_zero(int ring_size) {
    for (int i = 0; i < 10000; ++i) {
        std::string key = "protocol-head-key-" + std::to_string(i);
        if (replication_common::head_index(key, ring_size) != 0) {
            return key;
        }
    }
    assert(false);
    return "";
}

std::string key_with_tail_not_zero(int ring_size) {
    for (int i = 0; i < 10000; ++i) {
        std::string key = "protocol-tail-key-" + std::to_string(i);
        if (replication_common::tail_index(key, ring_size) != 0) {
            return key;
        }
    }
    assert(false);
    return "";
}

void test_crown_single_node_put_commits_and_reads_clean_value() {
    CrownReplication crown;
    crown.update_membership(make_membership(1), "n0");
    crown.set_epoch(1);

    replication::PutRequest request;
    request.set_request_id(101);
    request.set_key("protocol-crown-single-key");
    request.set_value("committed");
    request.set_version(0);
    request.set_client_addr("invalid-client-address");
    request.set_epoch(1);

    auto put_response = crown.handle_put(request);
    assert(put_response.success());
    assert(put_response.version() == 1);

    auto get_response = crown.handle_get(request.key());
    assert(get_response.error().empty());
    assert(get_response.value() == "committed");
    assert(get_response.version() == 1);
}

void test_crown_rejects_client_write_at_non_head() {
    CrownReplication crown;
    crown.update_membership(make_membership(2), "n0");
    crown.set_epoch(1);

    replication::PutRequest request;
    request.set_request_id(102);
    request.set_key(key_with_head_not_zero(2));
    request.set_value("wrong-node");
    request.set_version(0);
    request.set_epoch(1);

    auto response = crown.handle_put(request);
    assert(!response.success());
    assert(response.error().rfind("WRONG_NODE", 0) == 0);
}

void test_crown_dirty_read_returns_committed_value() {
    const std::string key = "protocol-crown-dirty-read-key";
    uint64_t clean_version = KVStore::put(key, "clean", std::nullopt);
    KVStore::mark_clean(key, clean_version);
    KVStore::put(key, "dirty", clean_version + 1);

    CrownReplication crown;
    crown.update_membership(make_membership(1), "n0");
    crown.set_epoch(1);

    auto response = crown.handle_get(key);
    assert(response.error().empty());
    assert(response.value() == "clean");
    assert(response.version() == clean_version);
}

void test_chain_rejects_read_at_non_tail() {
    ChainReplication chain;
    chain.update_membership(make_membership(2), "n0");

    auto response = chain.handle_get(key_with_tail_not_zero(2));
    assert(response.error() == "WRONG_NODE");
}

void test_crown_ack_stops_at_head() {
    CrownReplication crown;
    crown.update_membership(make_membership(1), "n0");
    crown.set_epoch(1);

    replication::PutRequest request;
    request.set_request_id(103);
    request.set_key("protocol-crown-ack-key");
    request.set_value("acked");
    request.set_version(1);
    request.set_epoch(1);

    KVStore::put(request.key(), request.value(), request.version());
    crown.add_to_pending_acks(request);
    crown.handle_ack(request.request_id());

    assert(crown.pending_count() == 0);
    auto value = KVStore::get(request.key());
    assert(value.has_value());
    assert(value.value() == "acked");
}

} // namespace

int main() {
    test_crown_single_node_put_commits_and_reads_clean_value();
    test_crown_rejects_client_write_at_non_head();
    test_crown_dirty_read_returns_committed_value();
    test_chain_rejects_read_at_non_tail();
    test_crown_ack_stops_at_head();
    std::cout << "protocol_tests passed" << std::endl;
    return 0;
}

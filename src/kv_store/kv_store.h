#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <sys/types.h>
#include <unordered_map>
#include <mutex>
#include <utility>
#include <vector>

struct Record {
    uint64_t verison = 0;
    std::string value;
};

class CraqReplication;
class CrownReplication;

class KVStore {
private:
    friend class CraqReplication;
    friend class CrownReplication;
    static std::unordered_map<std::string, Record> store_;
    // when you receive an ack for a key, only put it in store if its version is higher than the current version in store
    static std::unordered_map<std::string, std::unordered_map<uint64_t, Record>> dirty_store_;
    static std::mutex mutex_store_;
    static std::mutex mutex_dirty_store_;
public:
    // only put if version > latest version
    // if version == null, it means the request is from client to head, so we can just assign it a new version that is higher than the current version in store
    static uint64_t put(const std::string& key, const std::string& value, std::optional<uint64_t> version);

    static std::optional<std::string> get(const std::string& key);

    // get the highest version (max in both store_ and dirty_store_) for a key
    static uint64_t get_latest_version(const std::string& key);

    // only put it in clean store if its version is higher than the current version in store
    static void mark_clean(const std::string& key, uint64_t version);

    static std::vector<std::pair<std::string, Record>> snapshot_committed();

    static void apply_committed_snapshot(
        const std::unordered_map<std::string, std::string>& values,
        const std::unordered_map<std::string, uint64_t>& versions);
};

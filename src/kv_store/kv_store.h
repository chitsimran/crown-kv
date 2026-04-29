#include <cstdint>
#include <iostream>
#include <string>
#include <sys/types.h>

struct Record {
    uint64_t verison = 0;
    std::string value;
};

class KVStore {
private:
    std::unordered_map<std::string, Record> store_;
    // when you receive an ack for a key, only put it in store if its version is higher than the current version in store
    std::unordered_map<std::string, std::unordered_map<uint64_t, Record>> dirty_store_;
    std::mutex mutex_store_;
    std::mutex mutex_dirty_store_;
public:
    static void put(const std::string& key, const std::string& value, int version);

    static std::optional<std::string> get(const std::string& key);

    static void mark_clean(const std::string& key, int version);
};

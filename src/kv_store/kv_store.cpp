#include "kv_store.h"

std::unordered_map<std::string, Record> KVStore::store_;
std::unordered_map<std::string, std::unordered_map<uint64_t, Record>> KVStore::dirty_store_;
std::mutex KVStore::mutex_store_;
std::mutex KVStore::mutex_dirty_store_;

uint64_t KVStore::put(const std::string& key, const std::string& value,
                      std::optional<uint64_t> version) {
    if (!version.has_value()) {
        uint64_t new_version = get_latest_version(key) + 1;
        Record record{new_version, value};
        {
            std::lock_guard<std::mutex> lock(mutex_dirty_store_);
            dirty_store_[key][new_version] = record;
        }
        return new_version;
    }

    uint64_t provided_version = version.value();
    uint64_t committed_version = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_store_);
        auto it = store_.find(key);
        if (it != store_.end()) {
            committed_version = it->second.verison;
        }
    }
    if (provided_version <= committed_version) {
        return committed_version;
    }

    Record record{provided_version, value};
    {
        std::lock_guard<std::mutex> lock(mutex_dirty_store_);
        dirty_store_[key][provided_version] = record;
    }
    return provided_version;
}

std::optional<std::string> KVStore::get(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_store_);
    auto it = store_.find(key);
    if (it == store_.end()) {
        return std::nullopt;
    }
    return it->second.value;
}

uint64_t KVStore::get_latest_version(const std::string& key) {
    uint64_t latest = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_store_);
        auto it = store_.find(key);
        if (it != store_.end()) {
            latest = it->second.verison;
        }
    }
    {
        std::lock_guard<std::mutex> lock(mutex_dirty_store_);
        auto it = dirty_store_.find(key);
        if (it != dirty_store_.end()) {
            for (const auto& entry : it->second) {
                if (entry.first > latest) {
                    latest = entry.first;
                }
            }
        }
    }
    return latest;
}

void KVStore::mark_clean(const std::string& key, uint64_t version) {
    std::optional<Record> record;
    {
        std::lock_guard<std::mutex> lock(mutex_dirty_store_);
        auto dirty_it = dirty_store_.find(key);
        if (dirty_it == dirty_store_.end()) {
            return;
        }
        auto record_it = dirty_it->second.find(version);
        if (record_it != dirty_it->second.end()) {
            record = record_it->second;
        }
        for (auto it = dirty_it->second.begin(); it != dirty_it->second.end();) {
            if (it->first <= version) {
                it = dirty_it->second.erase(it);
            } else {
                ++it;
            }
        }
    }

    if (!record.has_value()) {
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_store_);
    auto it = store_.find(key);
    if (it == store_.end() || record->verison > it->second.verison) {
        store_[key] = record.value();
    }
}

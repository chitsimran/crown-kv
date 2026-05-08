#include "kv_store.h"

std::unordered_map<std::string, Record> KVStore::store_;
std::unordered_map<std::string, std::unordered_map<uint64_t, Record>> KVStore::dirty_store_;
std::mutex KVStore::mutex_store_;
std::mutex KVStore::mutex_dirty_store_;

uint64_t KVStore::put(const std::string& key, const std::string& value,
                      std::optional<uint64_t> version) {
    if (!version.has_value()) {
        std::scoped_lock lock(mutex_store_, mutex_dirty_store_);
        uint64_t latest = 0;
        auto clean_it = store_.find(key);
        if (clean_it != store_.end()) {
            latest = clean_it->second.verison;
        }
        auto dirty_it = dirty_store_.find(key);
        if (dirty_it != dirty_store_.end()) {
            for (const auto& entry : dirty_it->second) {
                if (entry.first > latest) {
                    latest = entry.first;
                }
            }
        }
        uint64_t new_version = latest + 1;
        Record record{new_version, value};
        dirty_store_[key][new_version] = record;
        return new_version;
    }

    uint64_t provided_version = version.value();
    uint64_t committed_version = 0;
    std::scoped_lock lock(mutex_store_, mutex_dirty_store_);
    auto it = store_.find(key);
    if (it != store_.end()) {
        committed_version = it->second.verison;
    }
    if (provided_version <= committed_version) {
        return committed_version;
    }

    Record record{provided_version, value};
    dirty_store_[key][provided_version] = record;
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
    std::scoped_lock lock(mutex_store_, mutex_dirty_store_);
    auto it = store_.find(key);
    if (it != store_.end()) {
        latest = it->second.verison;
    }
    auto dirty_it = dirty_store_.find(key);
    if (dirty_it != dirty_store_.end()) {
        for (const auto& entry : dirty_it->second) {
            if (entry.first > latest) {
                latest = entry.first;
            }
        }
    }
    return latest;
}

void KVStore::mark_clean(const std::string& key, uint64_t version) {
    std::optional<Record> record;
    std::scoped_lock lock(mutex_store_, mutex_dirty_store_);
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

    if (!record.has_value()) {
        return;
    }

    auto clean_it = store_.find(key);
    if (clean_it == store_.end() || record->verison > clean_it->second.verison) {
        store_[key] = record.value();
    }
}

std::vector<std::pair<std::string, Record>> KVStore::snapshot_committed() {
    std::vector<std::pair<std::string, Record>> snapshot;
    std::lock_guard<std::mutex> lock(mutex_store_);
    snapshot.reserve(store_.size());
    for (const auto& entry : store_) {
        snapshot.push_back(entry);
    }
    return snapshot;
}

void KVStore::apply_committed_snapshot(
    const std::unordered_map<std::string, std::string>& values,
    const std::unordered_map<std::string, uint64_t>& versions) {
    std::scoped_lock lock(mutex_store_, mutex_dirty_store_);
    for (const auto& entry : values) {
        uint64_t version = 0;
        auto version_it = versions.find(entry.first);
        if (version_it != versions.end()) {
            version = version_it->second;
        }
        auto clean_it = store_.find(entry.first);
        if (clean_it == store_.end() || version > clean_it->second.verison) {
            store_[entry.first] = Record{version, entry.second};
        }
        auto dirty_it = dirty_store_.find(entry.first);
        if (dirty_it != dirty_store_.end()) {
            for (auto it = dirty_it->second.begin(); it != dirty_it->second.end();) {
                if (it->first <= version) {
                    it = dirty_it->second.erase(it);
                } else {
                    ++it;
                }
            }
            if (dirty_it->second.empty()) {
                dirty_store_.erase(dirty_it);
            }
        }
    }
}

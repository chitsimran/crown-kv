#include "kv_store.h"

std::array<KVStore::Stripe, KVStore::kStripeCount> KVStore::stripes_;

uint64_t KVStore::put(const std::string& key, const std::string& value,
                      std::optional<uint64_t> version) {
    auto& stripe = stripe_for(key);
    if (!version.has_value()) {
        std::lock_guard<std::mutex> lock(stripe.mu);
        uint64_t latest = 0;
        auto clean_it = stripe.committed.find(key);
        if (clean_it != stripe.committed.end()) {
            latest = clean_it->second.verison;
        }
        auto dirty_it = stripe.dirty.find(key);
        if (dirty_it != stripe.dirty.end()) {
            for (const auto& entry : dirty_it->second) {
                if (entry.first > latest) {
                    latest = entry.first;
                }
            }
        }
        uint64_t new_version = latest + 1;
        Record record{new_version, value};
        stripe.dirty[key][new_version] = record;
        return new_version;
    }

    uint64_t provided_version = version.value();
    uint64_t committed_version = 0;
    std::lock_guard<std::mutex> lock(stripe.mu);
    auto it = stripe.committed.find(key);
    if (it != stripe.committed.end()) {
        committed_version = it->second.verison;
    }
    if (provided_version <= committed_version) {
        return committed_version;
    }

    Record record{provided_version, value};
    stripe.dirty[key][provided_version] = record;
    return provided_version;
}

std::optional<std::string> KVStore::get(const std::string& key) {
    auto& stripe = stripe_for(key);
    std::lock_guard<std::mutex> lock(stripe.mu);
    auto it = stripe.committed.find(key);
    if (it == stripe.committed.end()) {
        return std::nullopt;
    }
    return it->second.value;
}

uint64_t KVStore::get_latest_version(const std::string& key) {
    auto& stripe = stripe_for(key);
    uint64_t latest = 0;
    std::lock_guard<std::mutex> lock(stripe.mu);
    auto it = stripe.committed.find(key);
    if (it != stripe.committed.end()) {
        latest = it->second.verison;
    }
    auto dirty_it = stripe.dirty.find(key);
    if (dirty_it != stripe.dirty.end()) {
        for (const auto& entry : dirty_it->second) {
            if (entry.first > latest) {
                latest = entry.first;
            }
        }
    }
    return latest;
}

void KVStore::mark_clean(const std::string& key, uint64_t version) {
    auto& stripe = stripe_for(key);
    std::optional<Record> record;
    std::lock_guard<std::mutex> lock(stripe.mu);
    auto dirty_it = stripe.dirty.find(key);
    if (dirty_it == stripe.dirty.end()) {
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

    auto clean_it = stripe.committed.find(key);
    if (clean_it == stripe.committed.end() || record->verison > clean_it->second.verison) {
        stripe.committed[key] = record.value();
    }
}

std::vector<std::pair<std::string, Record>> KVStore::snapshot_committed() {
    std::vector<std::pair<std::string, Record>> snapshot;
    for (auto& stripe : stripes_) {
        std::lock_guard<std::mutex> lock(stripe.mu);
        snapshot.reserve(snapshot.size() + stripe.committed.size());
        for (const auto& entry : stripe.committed) {
            snapshot.push_back(entry);
        }
    }
    return snapshot;
}

void KVStore::apply_committed_snapshot(
    const std::unordered_map<std::string, std::string>& values,
    const std::unordered_map<std::string, uint64_t>& versions) {
    for (const auto& entry : values) {
        const std::string& key = entry.first;
        auto& stripe = stripe_for(key);
        std::lock_guard<std::mutex> lock(stripe.mu);
        uint64_t version = 0;
        auto version_it = versions.find(key);
        if (version_it != versions.end()) {
            version = version_it->second;
        }
        auto clean_it = stripe.committed.find(key);
        if (clean_it == stripe.committed.end() || version > clean_it->second.verison) {
            stripe.committed[key] = Record{version, entry.second};
        }
        auto dirty_it = stripe.dirty.find(key);
        if (dirty_it != stripe.dirty.end()) {
            for (auto it = dirty_it->second.begin(); it != dirty_it->second.end();) {
                if (it->first <= version) {
                    it = dirty_it->second.erase(it);
                } else {
                    ++it;
                }
            }
            if (dirty_it->second.empty()) {
                stripe.dirty.erase(dirty_it);
            }
        }
    }
}

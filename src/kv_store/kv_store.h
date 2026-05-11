#pragma once

#include <array>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <sys/types.h>
#include <unordered_map>
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

    // Stripe count chosen so that, even with the bench pushing >5K ops/s,
    // expected contention per stripe is well under 1 op at a time. Powers of
    // two would let us mask instead of mod, but std::hash already returns a
    // well-distributed value.
    static constexpr size_t kStripeCount = 256;

    struct Stripe {
        std::mutex mu;
        std::unordered_map<std::string, Record> committed;
        std::unordered_map<std::string, std::unordered_map<uint64_t, Record>> dirty;
    };

    static std::array<Stripe, kStripeCount> stripes_;

    static Stripe& stripe_for(const std::string& key) {
        return stripes_[std::hash<std::string>{}(key) % kStripeCount];
    }

public:
    // only put if version > latest version
    // if version == null, it means the request is from client to head, so we can just assign it a new version that is higher than the current version in store
    static uint64_t put(const std::string& key, const std::string& value, std::optional<uint64_t> version);

    // Write directly to the committed store, bypassing the dirty/committed
    // distinction. Used by CROWN in chain-read mode (--crown-chain-reads):
    // there's no read-time validation against the tail, so values can be
    // visible immediately at every replica. This is single-lock, single-map
    // and avoids the dirty-map scan path entirely. version=nullopt → head
    // path (assign latest_committed+1). version=value → forwarded-put path.
    static uint64_t put_committed(const std::string& key, const std::string& value,
                                  std::optional<uint64_t> version);

    static std::optional<std::string> get(const std::string& key);

    // get the highest version (max in both committed and dirty) for a key
    static uint64_t get_latest_version(const std::string& key);

    // only put it in clean store if its version is higher than the current version in store
    static void mark_clean(const std::string& key, uint64_t version);

    static std::vector<std::pair<std::string, Record>> snapshot_committed();

    static void apply_committed_snapshot(
        const std::unordered_map<std::string, std::string>& values,
        const std::unordered_map<std::string, uint64_t>& versions);
};

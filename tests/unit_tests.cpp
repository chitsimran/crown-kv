#include "../src/kv_store/kv_store.h"
#include "../src/client/throughput_window.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {

void test_head_assigns_version_monotonically() {
    constexpr int kWrites = 64;
    const std::string key = "unit-concurrent-key";
    std::mutex versions_mutex;
    std::vector<uint64_t> versions;
    versions.reserve(kWrites);
    std::vector<std::thread> threads;

    for (int i = 0; i < kWrites; ++i) {
        threads.emplace_back([&versions, &versions_mutex, i]() {
            uint64_t version = KVStore::put("unit-concurrent-key",
                                            "value-" + std::to_string(i),
                                            std::nullopt);
            std::lock_guard<std::mutex> lock(versions_mutex);
            versions.push_back(version);
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    std::sort(versions.begin(), versions.end());
    assert(versions.size() == kWrites);
    for (int i = 0; i < kWrites; ++i) {
        assert(versions[i] == static_cast<uint64_t>(i + 1));
    }
    assert(KVStore::get_latest_version(key) == kWrites);
}

void test_mark_clean_moves_latest_committed_value() {
    const std::string key = "unit-clean-key";
    uint64_t version = KVStore::put(key, "clean-value", std::nullopt);
    KVStore::mark_clean(key, version);
    auto value = KVStore::get(key);
    assert(value.has_value());
    assert(value.value() == "clean-value");
}

void test_explicit_stale_write_does_not_roll_back_clean_value() {
    const std::string key = "unit-stale-explicit-key";
    uint64_t version = KVStore::put(key, "new-value", std::nullopt);
    KVStore::mark_clean(key, version);

    uint64_t returned_version = KVStore::put(key, "old-value", version);
    auto value = KVStore::get(key);

    assert(returned_version == version);
    assert(value.has_value());
    assert(value.value() == "new-value");
    assert(KVStore::get_latest_version(key) == version);
}

void test_latest_version_includes_dirty_versions() {
    const std::string key = "unit-latest-dirty-key";
    uint64_t clean_version = KVStore::put(key, "clean-value", std::nullopt);
    KVStore::mark_clean(key, clean_version);
    uint64_t dirty_version = clean_version + 3;

    uint64_t returned_version = KVStore::put(key, "dirty-value", dirty_version);

    assert(returned_version == dirty_version);
    assert(KVStore::get_latest_version(key) == dirty_version);
    auto value = KVStore::get(key);
    assert(value.has_value());
    assert(value.value() == "clean-value");
}

void test_mark_clean_ignores_missing_dirty_version() {
    const std::string key = "unit-missing-dirty-key";
    uint64_t version = KVStore::put(key, "committed-value", std::nullopt);
    KVStore::mark_clean(key, version);

    KVStore::mark_clean(key, version + 100);

    auto value = KVStore::get(key);
    assert(value.has_value());
    assert(value.value() == "committed-value");
    assert(KVStore::get_latest_version(key) == version);
}

void test_snapshot_round_trip_keeps_versions() {
    const std::string key = "unit-snapshot-key";
    uint64_t version = KVStore::put(key, "snapshot-value", std::nullopt);
    KVStore::mark_clean(key, version);

    auto snapshot = KVStore::snapshot_committed();
    bool found = false;
    for (const auto& entry : snapshot) {
        if (entry.first == key) {
            found = true;
            assert(entry.second.value == "snapshot-value");
            assert(entry.second.verison == version);
        }
    }
    assert(found);
}

void test_throughput_windows_count_commits_per_window() {
    std::vector<benchmark::CommitSample> samples = {
        benchmark::CommitSample{0.10, 1.0, 1},
        benchmark::CommitSample{0.20, 1.5, 2},
        benchmark::CommitSample{0.70, 2.0, 3},
        benchmark::CommitSample{1.10, 2.5, 4},
    };

    auto rows = benchmark::BuildThroughputWindows(samples, 1.10, 0.50, 7, 10, 6);

    assert(rows.size() == 3);
    assert(rows[0].commits == 2);
    assert(rows[0].throughput_ops_sec == 4.0);
    assert(rows[0].cumulative_commits == 2);
    assert(rows[1].commits == 1);
    assert(rows[1].throughput_ops_sec == 2.0);
    assert(rows[1].cumulative_commits == 3);
    assert(rows[2].commits == 1);
    assert(rows[2].throughput_ops_sec == 2.0);
    assert(rows[2].cumulative_commits == 4);
    assert(rows[2].failed == 7);
    assert(rows[2].accepted == 10);
    assert(rows[2].orphaned == 6);
}

void test_throughput_windows_include_zero_commit_gaps() {
    std::vector<benchmark::CommitSample> samples = {
        benchmark::CommitSample{0.10, 1.0, 1},
        benchmark::CommitSample{1.10, 1.0, 2},
    };

    auto rows = benchmark::BuildThroughputWindows(samples, 1.10, 0.50, 0, 2, 0);

    assert(rows.size() == 3);
    assert(rows[0].commits == 1);
    assert(rows[1].commits == 0);
    assert(rows[1].throughput_ops_sec == 0.0);
    assert(rows[1].cumulative_commits == 1);
    assert(rows[2].commits == 1);
}

} // namespace

int main() {
    test_head_assigns_version_monotonically();
    test_mark_clean_moves_latest_committed_value();
    test_explicit_stale_write_does_not_roll_back_clean_value();
    test_latest_version_includes_dirty_versions();
    test_mark_clean_ignores_missing_dirty_version();
    test_snapshot_round_trip_keeps_versions();
    test_throughput_windows_count_commits_per_window();
    test_throughput_windows_include_zero_commit_gaps();
    std::cout << "unit_tests passed" << std::endl;
    return 0;
}

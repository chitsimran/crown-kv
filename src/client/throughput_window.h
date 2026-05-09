#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <vector>

namespace benchmark {

struct CommitSample {
    double time_sec = 0.0;
    double latency_ms = 0.0;
    uint64_t completed_sequence = 0;
};

struct ThroughputWindowRow {
    double time_sec = 0.0;
    double window_start_sec = 0.0;
    double window_end_sec = 0.0;
    uint64_t commits = 0;
    double throughput_ops_sec = 0.0;
    uint64_t cumulative_commits = 0;
    uint64_t failed = 0;
    uint64_t accepted = 0;
    uint64_t orphaned = 0;
};

inline std::vector<ThroughputWindowRow> BuildThroughputWindows(
    std::vector<CommitSample> samples, double duration_sec, double window_sec,
    uint64_t failed, uint64_t accepted, uint64_t orphaned) {
    std::vector<ThroughputWindowRow> rows;
    if (window_sec <= 0.0) {
        return rows;
    }

    std::sort(samples.begin(), samples.end(), [](const CommitSample& lhs,
                                                 const CommitSample& rhs) {
        return lhs.time_sec < rhs.time_sec;
    });

    double end_time = std::max(duration_sec, samples.empty() ? 0.0 : samples.back().time_sec);
    if (end_time <= 0.0) {
        end_time = window_sec;
    }

    size_t sample_index = 0;
    uint64_t cumulative = 0;
    int window_count = static_cast<int>(std::ceil(end_time / window_sec));
    window_count = std::max(window_count, 1);

    for (int i = 0; i < window_count; ++i) {
        double start = i * window_sec;
        double end = start + window_sec;
        uint64_t commits = 0;
        while (sample_index < samples.size() && samples[sample_index].time_sec < end) {
            if (samples[sample_index].time_sec >= start) {
                commits += 1;
            }
            sample_index += 1;
        }
        cumulative += commits;

        ThroughputWindowRow row;
        row.time_sec = end;
        row.window_start_sec = start;
        row.window_end_sec = end;
        row.commits = commits;
        row.throughput_ops_sec = commits / window_sec;
        row.cumulative_commits = cumulative;
        row.failed = failed;
        row.accepted = accepted;
        row.orphaned = orphaned;
        rows.push_back(row);
    }

    return rows;
}

} // namespace benchmark

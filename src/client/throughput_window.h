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
    // Per-window latency aggregates (ms). Zero when the window has no commits.
    double latency_p50_ms = 0.0;
    double latency_p99_ms = 0.0;
    double latency_avg_ms = 0.0;
};

// Pick the percentile-rank value via nearest-rank: index = ceil(p * n) - 1,
// clamped to [0, n-1]. Caller must pass a sorted, non-empty vector.
inline double PercentileSorted(const std::vector<double>& sorted, double p) {
    if (sorted.empty()) {
        return 0.0;
    }
    double rank = std::ceil(p * static_cast<double>(sorted.size())) - 1.0;
    if (rank < 0.0) rank = 0.0;
    size_t idx = static_cast<size_t>(rank);
    if (idx >= sorted.size()) idx = sorted.size() - 1;
    return sorted[idx];
}

struct LatencySummary {
    double p50_ms = 0.0;
    double p95_ms = 0.0;
    double p99_ms = 0.0;
    double p999_ms = 0.0;
    double avg_ms = 0.0;
    double max_ms = 0.0;
};

inline LatencySummary SummarizeLatencies(const std::vector<CommitSample>& samples) {
    LatencySummary out;
    if (samples.empty()) {
        return out;
    }
    std::vector<double> latencies;
    latencies.reserve(samples.size());
    double sum = 0.0;
    for (const auto& s : samples) {
        latencies.push_back(s.latency_ms);
        sum += s.latency_ms;
    }
    std::sort(latencies.begin(), latencies.end());
    out.p50_ms  = PercentileSorted(latencies, 0.50);
    out.p95_ms  = PercentileSorted(latencies, 0.95);
    out.p99_ms  = PercentileSorted(latencies, 0.99);
    out.p999_ms = PercentileSorted(latencies, 0.999);
    out.avg_ms  = sum / static_cast<double>(latencies.size());
    out.max_ms  = latencies.back();
    return out;
}

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
        std::vector<double> window_latencies;
        double window_latency_sum = 0.0;
        while (sample_index < samples.size() && samples[sample_index].time_sec < end) {
            if (samples[sample_index].time_sec >= start) {
                commits += 1;
                double l = samples[sample_index].latency_ms;
                window_latencies.push_back(l);
                window_latency_sum += l;
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
        if (!window_latencies.empty()) {
            std::sort(window_latencies.begin(), window_latencies.end());
            row.latency_p50_ms = PercentileSorted(window_latencies, 0.50);
            row.latency_p99_ms = PercentileSorted(window_latencies, 0.99);
            row.latency_avg_ms = window_latency_sum /
                                 static_cast<double>(window_latencies.size());
        }
        rows.push_back(row);
    }

    return rows;
}

} // namespace benchmark

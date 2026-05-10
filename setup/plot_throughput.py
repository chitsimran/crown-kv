#!/usr/bin/env python3
"""Plot crown-kv benchmark throughput and latency CSVs."""

import argparse
import csv
import sys
from pathlib import Path


def read_points(csv_path: Path):
    times = []
    throughputs = []
    p50 = []
    p99 = []
    avg = []
    with csv_path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            times.append(float(row["time_sec"]))
            throughputs.append(float(row["throughput_ops_sec"]))
            p50.append(float(row.get("latency_p50_ms", 0) or 0))
            p99.append(float(row.get("latency_p99_ms", 0) or 0))
            avg.append(float(row.get("latency_avg_ms", 0) or 0))
    return times, throughputs, p50, p99, avg


def trailing_average(times, values, smooth_sec):
    smoothed = []
    start = 0
    running_sum = 0.0
    for end, current_time in enumerate(times):
        running_sum += values[end]
        while start < end and times[start] < current_time - smooth_sec:
            running_sum -= values[start]
            start += 1
        count = end - start + 1
        smoothed.append(running_sum / count)
    return smoothed


def main() -> int:
    parser = argparse.ArgumentParser(description="Plot crown-kv benchmark throughput")
    parser.add_argument("csv_path", type=Path)
    parser.add_argument("png_path", type=Path)
    parser.add_argument("--smooth-sec", type=float, default=3.0,
                        help="Trailing rolling-average window in seconds (default: 3)")
    args = parser.parse_args()

    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib is not installed; CSV was written but PNG was skipped",
              file=sys.stderr)
        return 2

    times, throughputs, p50, p99, avg = read_points(args.csv_path)
    if not times:
        print(f"no rows found in {args.csv_path}", file=sys.stderr)
        return 1

    smoothed = trailing_average(times, throughputs, args.smooth_sec)
    has_latency = any(v > 0 for v in p99)

    args.png_path.parent.mkdir(parents=True, exist_ok=True)

    if has_latency:
        fig, (ax_top, ax_bot) = plt.subplots(2, 1, figsize=(10, 8), sharex=True)
    else:
        fig, ax_top = plt.subplots(1, 1, figsize=(10, 5))
        ax_bot = None

    ax_top.plot(times, throughputs, linewidth=1.0, alpha=0.35,
                label="window throughput")
    ax_top.plot(times, smoothed, linewidth=2.4,
                label=f"{args.smooth_sec:g}s trailing average")
    ax_top.set_ylabel("Throughput (commits/s)")
    ax_top.set_title("CROWN-KV Commit Throughput")
    ax_top.legend()
    ax_top.grid(True, alpha=0.3)

    if ax_bot is not None:
        ax_bot.plot(times, p50, linewidth=1.6, label="p50")
        ax_bot.plot(times, avg, linewidth=1.2, alpha=0.7, label="avg")
        ax_bot.plot(times, p99, linewidth=1.6, label="p99")
        ax_bot.set_xlabel("Time since benchmark start (s)")
        ax_bot.set_ylabel("Latency (ms)")
        ax_bot.set_title("Per-window Commit Latency")
        ax_bot.legend()
        ax_bot.grid(True, alpha=0.3)
    else:
        ax_top.set_xlabel("Time since benchmark start (s)")

    plt.tight_layout()
    plt.savefig(args.png_path, dpi=150)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

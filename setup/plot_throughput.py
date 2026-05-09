#!/usr/bin/env python3
"""Plot crown-kv benchmark throughput CSVs."""

import argparse
import csv
import sys
from pathlib import Path


def read_points(csv_path: Path):
    times = []
    throughputs = []
    with csv_path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            times.append(float(row["time_sec"]))
            throughputs.append(float(row["throughput_ops_sec"]))
    return times, throughputs


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

    times, throughputs = read_points(args.csv_path)
    if not times:
        print(f"no rows found in {args.csv_path}", file=sys.stderr)
        return 1

    smoothed = trailing_average(times, throughputs, args.smooth_sec)

    args.png_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(10, 5))
    plt.plot(times, throughputs, linewidth=1.0, alpha=0.35, label="window throughput")
    plt.plot(times, smoothed, linewidth=2.4,
             label=f"{args.smooth_sec:g}s trailing average")
    plt.xlabel("Time since benchmark start (s)")
    plt.ylabel("Throughput (commits/s)")
    plt.title("CROWN-KV Commit Throughput")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(args.png_path, dpi=150)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

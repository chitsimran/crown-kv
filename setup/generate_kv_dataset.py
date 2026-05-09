#!/usr/bin/env python3

"""Generate a balanced key/value dataset for CROWN.

The generator uses the same FNV-1a 64-bit hash as the replication code.
It emits one combined CSV file containing alphabetic 10-byte keys and
64-byte values. The selected keys are balanced across the shared modulus
for ring sizes 3, 5, 7, and 9, so the same file works for those CROWN ring
sizes without storing any head index metadata.

Hot-key mode repeats a configurable subset of keys more often so the output
can be used as a skewed write workload as well as a plain KV seed file.
"""

from __future__ import annotations

import argparse
import csv
import math
import string
from collections import deque
from pathlib import Path
from typing import Deque, Dict, List, Optional, Tuple


FNV_OFFSET_BASIS = 14695981039346656037
FNV_PRIME = 1099511628211
BALANCE_MODULUS = 315  # lcm(3, 5, 7, 9)
ALPHABET = string.ascii_lowercase


def fnv1a_64(key: str) -> int:
    hash_value = FNV_OFFSET_BASIS
    for byte in key.encode("utf-8"):
        hash_value ^= byte
        hash_value = (hash_value * FNV_PRIME) & 0xFFFFFFFFFFFFFFFF
    return hash_value


def to_base26(value: int, width: int) -> str:
    chars = ["a"] * width
    for position in range(width - 1, -1, -1):
        chars[position] = ALPHABET[value % 26]
        value //= 26
    return "".join(chars)


def make_key(index: int) -> str:
    return to_base26(index, 10)


def make_value(index: int) -> str:
    return to_base26(index, 64)


def generate_balanced_rows(total_pairs: int, nodes: Optional[int] = None) -> List[Tuple[str, str]]:
    """Generate balanced key-value pairs.
    If nodes is specified, balance for that specific node count.
    Otherwise, balance for ring sizes 3, 5, 7, 9 using modulus 315.
    """
    modulus = nodes if nodes is not None else BALANCE_MODULUS
    buckets: Dict[int, Deque[Tuple[str, str]]] = {
        residue: deque() for residue in range(modulus)
    }
    rows: List[Tuple[str, str]] = []
    candidate = 0
    next_residue = 0

    while len(rows) < total_pairs:
        if buckets[next_residue]:
            rows.append(buckets[next_residue].popleft())
            next_residue = (next_residue + 1) % modulus
            continue

        key = make_key(candidate)
        residue = fnv1a_64(key) % modulus
        buckets[residue].append((key, make_value(candidate)))
        candidate += 1

    return rows


def apply_hot_skew(rows: List[Tuple[str, str]], hot_share: int, hot_set_share: int, nodes: Optional[int] = None) -> List[Tuple[str, str]]:
    if hot_share == 0:
        return rows

    total_rows = len(rows)
    hot_rows_count = max(1, math.floor(total_rows * hot_share / 100))
    cold_rows_count = total_rows - hot_rows_count

    # If nodes specified, partition rows by which node they hash to
    if nodes is not None:
        node_buckets = [[] for _ in range(nodes)]
        for key, value in rows:
            node_id = fnv1a_64(key) % nodes
            node_buckets[node_id].append((key, value))
        
        # Hot pool: keys from hot node (node 0 only)
        hot_node_id = 0
        hot_pool = node_buckets[hot_node_id]
        
        # Cold pool: distribute cold rows evenly across all nodes
        # Calculate how many rows to take from each node
        cold_per_node = cold_rows_count // nodes
        cold_pool = []
        for node_id in range(nodes):
            # Take up to cold_per_node rows from this node, cycling if needed
            node_data = node_buckets[node_id]
            for i in range(cold_per_node):
                cold_pool.append(node_data[i % len(node_data)] if node_data else rows[0])
        
        # Handle remainder
        remainder = cold_rows_count % nodes
        for i in range(remainder):
            node_id = i % nodes
            node_data = node_buckets[node_id]
            cold_pool.append(node_data[(cold_per_node + i) % len(node_data)] if node_data else rows[0])
    else:
        # Original behavior: first hot_set_share% of rows are hot pool
        hot_set_size = max(1, math.floor(total_rows * hot_set_share / 100))
        hot_pool = rows[:hot_set_size]
        cold_pool = rows[hot_set_size:]

    skewed_rows: List[Tuple[str, str]] = []
    for index in range(hot_rows_count):
        if hot_pool:
            key, value = hot_pool[index % len(hot_pool)]
            skewed_rows.append((key, value))

    for index in range(cold_rows_count):
        if cold_pool:
            key, value = cold_pool[index % len(cold_pool)]
            skewed_rows.append((key, value))

    return skewed_rows


def write_dataset_csv(path: Path, rows: List[Tuple[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["key", "value"])
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a balanced KV dataset")
    parser.add_argument(
        "--total-pairs",
        type=int,
        default=20000,
        help="Total number of KV pairs to generate",
    )
    parser.add_argument(
        "--hot-share",
        type=int,
        default=0,
        help="Percentage of rows to assign to hot keys (0-100)",
    )
    parser.add_argument(
        "--hot-set-share",
        type=int,
        default=10,
        help="Percentage of unique keys that belong to the hot set (1-100)",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default=Path(__file__).resolve().parent / "generated_kv_dataset" / "all_kv_pairs.csv",
        help="CSV file to write",
    )
    parser.add_argument(
        "--file-name",
        type=str,
        default="",
        help="File name to write inside setup/generated_kv_dataset/",
    )
    parser.add_argument(
        "--nodes",
        type=int,
        default=None,
        help="Number of nodes in the ring. If not specified, generates balanced data for 3, 5, 7, 9 nodes.",
    )
    args = parser.parse_args()

    if args.total_pairs <= 0:
        raise SystemExit("--total-pairs must be greater than zero")
    if not 0 <= args.hot_share <= 100:
        raise SystemExit("--hot-share must be between 0 and 100")
    if not 1 <= args.hot_set_share <= 100:
        raise SystemExit("--hot-set-share must be between 1 and 100")

    output_file = args.output_file
    if args.file_name:
        output_file = Path(__file__).resolve().parent / "generated_kv_dataset" / args.file_name

    rows = generate_balanced_rows(args.total_pairs, args.nodes)
    rows = apply_hot_skew(rows, args.hot_share, args.hot_set_share, args.nodes)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    write_dataset_csv(output_file, rows)

    print(f"Wrote {len(rows)} pairs to {output_file}")
    print("Balanced for ring sizes 3, 5, 7, and 9 using the repo's FNV-1a hash.")
    if args.hot_share > 0:
        print(f"Hot rows: {args.hot_share}% of the file, hot set: {args.hot_set_share}% of keys")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

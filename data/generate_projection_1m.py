#!/usr/bin/env python3
"""
Projection Dataset Generator (1,000,000 Rows Scalability Benchmark)
Big Data Final Project — Sistem Terpadu Pemantauan Spatio-Temporal Hotspot & Karhutla

Synthesizes a 1,000,000-row active fire dataset from the verified 100,000-row VIIRS
sample (or custom target count) for Phase 5 end-to-end scalability and throughput testing.
Uses an orthogonal projection suffix scheme (_p{rep}) to guarantee zero RowKey collisions.

Usage:
    python generate_projection_1m.py [output_path] [target_rows] [input_path]

Defaults:
    output_path = data/hotspot_1m.csv
    target_rows = 1000000
    input_path  = data/viirs_hotspot_100k.csv
"""

import sys
import os
import time
import csv

DEFAULT_TARGET_ROWS = 1_000_000


def generate_projection(input_path: str, output_path: str, target_rows: int = DEFAULT_TARGET_ROWS):
    if not os.path.isfile(input_path):
        raise FileNotFoundError(f"Input source file not found: {input_path}")

    print(f"[*] Starting projection generation...")
    print(f"    Source dataset: {input_path}")
    print(f"    Target output:  {output_path}")
    print(f"    Target rows:    {target_rows:,}")

    start_time = time.time()

    # Read base rows
    base_rows = []
    header = None
    with open(input_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            if row:
                base_rows.append(row)

    base_count = len(base_rows)
    if base_count == 0:
        raise ValueError(f"Input file {input_path} contains no data rows!")

    print(f"[*] Loaded {base_count:,} base records in {time.time() - start_time:.2f}s")

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    # Replicate to target_rows
    replications_needed = (target_rows + base_count - 1) // base_count
    written = 0

    with open(output_path, "w", encoding="utf-8", newline="\n") as f_out:
        writer = csv.writer(f_out)
        writer.writerow(header)

        for rep in range(replications_needed):
            for row in base_rows:
                if written >= target_rows:
                    break
                
                # For replications after 0, append orthogonal projection index (_p{rep}) to hotspot_id
                # to maintain rowkey uniqueness for HBase and downstream operations without colliding
                # with pre-existing _r1.._r9 chunks in the base dataset.
                if rep == 0:
                    hotspot_id = row[0]
                else:
                    hotspot_id = f"{row[0]}_p{rep}"

                new_row = [hotspot_id] + row[1:]
                writer.writerow(new_row)
                written += 1

                if written % 250_000 == 0:
                    elapsed = time.time() - start_time
                    print(f"    Progress: {written:,} / {target_rows:,} ({written/target_rows*100:.1f}%) in {elapsed:.2f}s")

    elapsed = time.time() - start_time
    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"[+] Successfully generated {written:,} rows in {elapsed:.2f}s")
    print(f"    Output file size: {file_size_mb:.2f} MB")
    print(f"    Output path: {output_path}")


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_input = os.path.join(script_dir, "viirs_hotspot_100k.csv")
    default_output = os.path.join(script_dir, "hotspot_1m.csv")

    output_path = sys.argv[1] if len(sys.argv) > 1 else default_output
    target_rows = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_TARGET_ROWS
    input_path = sys.argv[3] if len(sys.argv) > 3 else default_input

    generate_projection(input_path, output_path, target_rows)


if __name__ == "__main__":
    main()

import asyncio
import time
from pathlib import Path

import pandas as pd

from pipeline import load, load_sync

NUM_FILES = 5
TIERS = ["small", "medium"]
PARTS_DIR = Path("data/parts")


def split_file(source_file: Path, num_parts: int) -> list[Path]:
    """Split a single CSV into num_parts roughly equal files for the benchmark."""
    PARTS_DIR.mkdir(exist_ok=True)
    df = pd.read_csv(source_file, low_memory=False)
    chunk_size = len(df) // num_parts
    part_paths = []
    for i in range(num_parts):
        start = i * chunk_size
        end = None if i == num_parts - 1 else start + chunk_size
        output_path = PARTS_DIR / f"part_{i}.csv"
        df.iloc[start:end].to_csv(output_path, index=False)
        part_paths.append(output_path)
    return part_paths


def run_single_file_baseline():
    """Load each dataset tier once with plain pd.read_csv to establish a baseline."""
    print("-- sync baseline (single file) ------------------------")
    print(f"{'tier':<10} {'rows':>10} {'size':>8} {'time':>8}")
    print("-" * 42)

    for tier in TIERS:
        file_path = Path(f"data/nyc311_{tier}.csv")
        if not file_path.exists():
            print(f"{tier:<10} {'(no file)':>10}")
            continue
        file_size_mb = file_path.stat().st_size / 1e6
        start_time = time.perf_counter()
        df = pd.read_csv(str(file_path), low_memory=False)
        elapsed = time.perf_counter() - start_time
        print(f"{tier:<10} {len(df):>10,} {file_size_mb:>6.0f}MB {elapsed:>7.2f}s")


async def run_concurrent_vs_sequential():
    """
    Compare loading NUM_FILES files concurrently (via asyncio.gather + run_in_executor)
    against loading the same files one at a time. Both sides call load_sync so the
    comparison is apples-to-apples.
    """
    source_file = Path("data/nyc311_small.csv")
    if not source_file.exists():
        print("small tier not found, skipping")
        return

    print(f"\nsplitting {source_file.name} into {NUM_FILES} parts...")
    part_paths = split_file(source_file, NUM_FILES)
    rows_per_file = len(pd.read_csv(part_paths[0], low_memory=False))

    print(f"\n-- concurrent vs sequential ({NUM_FILES} files x ~{rows_per_file:,} rows) --")
    print(f"{'method':<28} {'total rows':>12} {'time':>8}")
    print("-" * 50)

    # Concurrent: all files submitted to the thread pool at once via asyncio.gather.
    start_time = time.perf_counter()
    results = await asyncio.gather(*[load(str(p)) for p in part_paths])
    concurrent_time = time.perf_counter() - start_time
    total_rows = sum(result.rows_kept for result in results)
    print(f"{'async (run_in_executor)':<28} {total_rows:>12,} {concurrent_time:>7.2f}s")

    # Sequential: same load_sync called one file at a time, no concurrency.
    start_time = time.perf_counter()
    for part_path in part_paths:
        load_sync(str(part_path))
    sequential_time = time.perf_counter() - start_time
    print(f"{'sync (sequential)':<28} {total_rows:>12,} {sequential_time:>7.2f}s")

    ratio = concurrent_time / sequential_time
    if ratio > 1:
        print(f"\n  async: {ratio:.1f}x slower than sequential")
    else:
        print(f"\n  async: {1/ratio:.1f}x faster than sequential")


async def main():
    run_single_file_baseline()
    await run_concurrent_vs_sequential()


if __name__ == "__main__":
    asyncio.run(main())

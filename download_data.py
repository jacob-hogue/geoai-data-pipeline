import requests
import time
from pathlib import Path

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"
PAGE_SIZE = 50_000
TIERS = {"small": 100_000, "medium": 500_000, "large": 2_000_000}


def download_tier(tier_name: str, target_rows: int):
    output_file = Path(f"data/nyc311_{tier_name}.csv")
    if output_file.exists():
        print(f"{tier_name}: already downloaded")
        return

    print(f"{tier_name}: fetching {target_rows:,} rows...")
    start_time = time.perf_counter()
    row_offset = 0
    rows_written = 0

    with output_file.open("w", encoding="utf-8") as f:
        while rows_written < target_rows:
            page_limit = min(PAGE_SIZE, target_rows - rows_written)
            response = requests.get(
                API_URL,
                params={"$limit": page_limit, "$offset": row_offset, "$order": ":id"},
                timeout=60,
            )
            response.raise_for_status()

            csv_lines = response.text.splitlines(keepends=True)
            if len(csv_lines) <= 1:
                break

            # Skip the header row on subsequent pages so it isn't repeated.
            f.writelines(csv_lines if row_offset == 0 else csv_lines[1:])
            rows_fetched = len(csv_lines) - 1
            rows_written += rows_fetched
            row_offset += rows_fetched
            print(f"  {rows_written:,}/{target_rows:,}", end="\r")

            if rows_fetched < page_limit:
                break

    file_size_mb = output_file.stat().st_size / 1e6
    elapsed = time.perf_counter() - start_time
    print(f"\n{tier_name}: {rows_written:,} rows, {file_size_mb:.1f}MB in {elapsed:.1f}s")


if __name__ == "__main__":
    Path("data").mkdir(exist_ok=True)
    for tier_name, target_rows in TIERS.items():
        download_tier(tier_name, target_rows)

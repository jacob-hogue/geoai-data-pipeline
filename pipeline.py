import asyncio
from dataclasses import dataclass, field

import pandas as pd


@dataclass
class LoadResult:
    df: pd.DataFrame
    rows_raw: int
    rows_kept: int
    missing_coords: int
    encoding_errors: int
    warnings: list[str] = field(default_factory=list)


COORDINATE_COLUMNS = ("latitude", "longitude")
REQUIRED_COLUMNS = {"unique_key", "created_date", "complaint_type", "borough", "latitude", "longitude"}

# Unicode replacement character (U+FFFD): pandas inserts this when a byte
# sequence cannot be decoded as UTF-8 (encoding_errors="replace" option).
ENCODING_ERROR_CHAR = "\ufffd"


def load_sync(path: str) -> LoadResult:
    df = pd.read_csv(path, low_memory=False, encoding="utf-8", encoding_errors="replace")
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Count how many cells contain the replacement character across the whole DataFrame.
    encoding_errors = int(
        df.apply(lambda col: col.astype(str).str.contains(ENCODING_ERROR_CHAR, na=False)).sum().sum()
    )

    total_rows = len(df)
    if all(col in df.columns for col in COORDINATE_COLUMNS):
        # pd.to_numeric with errors="coerce" turns both empty values and non-numeric
        # strings (e.g. "N/A") into NaN. notna() then filters those rows out.
        df = df[pd.to_numeric(df["latitude"], errors="coerce").notna()]
        df = df[pd.to_numeric(df["longitude"], errors="coerce").notna()]
    rows_dropped = total_rows - len(df)

    warnings = []
    absent_columns = REQUIRED_COLUMNS - set(df.columns)
    if absent_columns:
        warnings.append(f"schema drift, missing columns: {absent_columns}")

    return LoadResult(
        df=df,
        rows_raw=total_rows,
        rows_kept=len(df),
        missing_coords=rows_dropped,
        encoding_errors=encoding_errors,
        warnings=warnings,
    )


async def load(path: str) -> LoadResult:
    # Offload the blocking CSV read to a background thread so the event loop
    # stays free to handle other tasks while the file is being read and parsed.
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, load_sync, path)


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "data/nyc311_small.csv"
    result = asyncio.run(load(path))
    print(f"rows kept:      {result.rows_kept:,}")
    print(f"missing coords: {result.missing_coords:,}")
    print(f"encoding errs:  {result.encoding_errors:,}")
    if result.warnings:
        print(f"warnings:       {result.warnings}")
    print(result.df[["unique_key", "complaint_type", "borough", "latitude", "longitude"]].head())

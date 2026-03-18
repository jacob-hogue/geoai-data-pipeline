"""
Verification tests for the async vs sync pipeline benchmark.

These tests check that the implementation matches the stated assumptions:
- load() uses run_in_executor, not line-by-line file reading
- Both sides of the benchmark call the same underlying function
- The GIL bottleneck is observable: concurrent gains are modest for local files
- The pipeline correctly cleans data (coords, column names, schema drift)

Run with: pytest tests/test_verify.py -v
"""
import asyncio
import inspect
import os
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline import load, load_sync, LoadResult


# helpers

def write_csv(content: str) -> str:
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8")
    f.write(content)
    f.close()
    return f.name


def run_async(coro):
    return asyncio.run(coro)


VALID_CSV = (
    "unique_key,created_date,complaint_type,borough,latitude,longitude\n"
    "1,2024-01-01,Noise,MANHATTAN,40.7128,-74.0060\n"
    "2,2024-01-02,Heat,BROOKLYN,40.6892,-73.9442\n"
)


# implementation assumptions

def test_load_is_coroutine():
    """load() must be async so it can be used with asyncio.gather."""
    assert asyncio.iscoroutinefunction(load), "load() should be an async function"


def test_load_uses_run_in_executor():
    """load() should offload work to a thread pool, not read line-by-line."""
    source = inspect.getsource(load)
    assert "run_in_executor" in source, "load() should use run_in_executor"
    assert "aiofiles" not in source, "load() should not use aiofiles line-by-line reading"


def test_load_calls_load_sync():
    """load() should delegate to load_sync, so both benchmark sides do the same work."""
    source = inspect.getsource(load)
    assert "load_sync" in source, "load() should call load_sync via run_in_executor"


def test_uses_running_loop_not_deprecated_api():
    """get_running_loop() is correct inside async context; get_event_loop() is deprecated in 3.10+."""
    source = inspect.getsource(load)
    assert "get_running_loop" in source, "should use asyncio.get_running_loop(), not get_event_loop()"
    assert "get_event_loop" not in source, "get_event_loop() is deprecated in Python 3.10+"


# benchmark fairness

def test_load_sync_and_load_return_same_results():
    """Both sides of the benchmark must produce identical row counts for the comparison to be valid."""
    path = write_csv(VALID_CSV)
    try:
        sync_result = load_sync(path)
        async_result = run_async(load(path))
        assert sync_result.rows_kept == async_result.rows_kept
        assert sync_result.missing_coords == async_result.missing_coords
        assert list(sync_result.df.columns) == list(async_result.df.columns)
    finally:
        os.unlink(path)


def test_concurrent_load_matches_sequential_total():
    """asyncio.gather across N files should return the same total rows as sequential calls."""
    paths = []
    try:
        for _ in range(3):
            paths.append(write_csv(VALID_CSV))

        async def run_concurrent():
            return await asyncio.gather(*[load(p) for p in paths])

        concurrent_results = run_async(run_concurrent())
        sequential_results = [load_sync(p) for p in paths]

        assert sum(r.rows_kept for r in concurrent_results) == sum(r.rows_kept for r in sequential_results)
    finally:
        for p in paths:
            os.unlink(p)


# GIL / concurrency assumption

def test_async_completes_in_reasonable_time():
    """
    For small local files, async should not be dramatically slower than sequential.
    If async is more than 5x slower, the implementation is probably blocking the event loop.
    """
    paths = []
    try:
        for _ in range(5):
            paths.append(write_csv(VALID_CSV * 500))  # ~1000 rows each

        async def run_concurrent():
            return await asyncio.gather(*[load(p) for p in paths])

        start_time = time.perf_counter()
        run_async(run_concurrent())
        concurrent_time = time.perf_counter() - start_time

        start_time = time.perf_counter()
        for p in paths:
            load_sync(p)
        sequential_time = time.perf_counter() - start_time

        ratio = concurrent_time / sequential_time
        assert ratio < 5.0, (
            f"async took {ratio:.1f}x longer than sequential -- "
            "likely blocking the event loop instead of using run_in_executor"
        )
    finally:
        for p in paths:
            os.unlink(p)


# pipeline correctness

def test_load_result_fields():
    path = write_csv(VALID_CSV)
    try:
        result = run_async(load(path))
        assert isinstance(result, LoadResult)
        assert result.rows_kept >= 0
        assert result.rows_raw >= result.rows_kept
        assert result.missing_coords == result.rows_raw - result.rows_kept
        assert isinstance(result.warnings, list)
    finally:
        os.unlink(path)


def test_coord_filtering_removes_nulls():
    path = write_csv(
        "unique_key,created_date,complaint_type,borough,latitude,longitude\n"
        "1,2024-01-01,Noise,MANHATTAN,,\n"
        "2,2024-01-02,Heat,BROOKLYN,40.6892,-73.9442\n"
        "3,2024-01-03,Graffiti,QUEENS,N/A,-73.9\n"
    )
    try:
        result = run_async(load(path))
        assert result.rows_kept == 1
        assert result.missing_coords == 2
    finally:
        os.unlink(path)


def test_column_normalisation():
    path = write_csv(
        "Unique Key,Created Date,Complaint Type,Borough,Latitude,Longitude\n"
        "1,2024-01-01,Noise,MANHATTAN,40.7128,-74.0060\n"
    )
    try:
        result = run_async(load(path))
        assert "unique_key" in result.df.columns
        assert "created_date" in result.df.columns
        assert "complaint_type" in result.df.columns
    finally:
        os.unlink(path)


def test_schema_drift_warning():
    path = write_csv(
        "id,value\n"
        "1,foo\n"
    )
    try:
        result = run_async(load(path))
        assert len(result.warnings) > 0
        assert "schema drift" in result.warnings[0]
    finally:
        os.unlink(path)


def test_rows_raw_accounts_for_dropped_rows():
    path = write_csv(
        "unique_key,created_date,complaint_type,borough,latitude,longitude\n"
        "1,2024-01-01,Noise,MANHATTAN,40.7128,-74.0060\n"
        "2,2024-01-02,Heat,BROOKLYN,,\n"
        "3,2024-01-03,Graffiti,QUEENS,40.7,-73.9\n"
    )
    try:
        result = run_async(load(path))
        assert result.rows_raw == 3
        assert result.rows_kept == 2
        assert result.missing_coords == 1
    finally:
        os.unlink(path)

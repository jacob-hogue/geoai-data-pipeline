import asyncio
import os
import tempfile

import pytest

from pipeline import load


# helpers

def write_csv(content: str) -> str:
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8")
    f.write(content)
    f.close()
    return f.name


def run_async(coro):
    return asyncio.run(coro)


# tests

def test_loads_valid_rows():
    path = write_csv(
        "unique_key,created_date,complaint_type,borough,latitude,longitude\n"
        "1,2024-01-01,Noise,MANHATTAN,40.7128,-74.0060\n"
        "2,2024-01-02,Heat,BROOKLYN,40.6892,-73.9442\n"
    )
    try:
        result = run_async(load(path))
        assert result.rows_kept == 2
        assert result.missing_coords == 0
    finally:
        os.unlink(path)


def test_drops_missing_coords():
    path = write_csv(
        "unique_key,created_date,complaint_type,borough,latitude,longitude\n"
        "1,2024-01-01,Noise,MANHATTAN,,\n"
        "2,2024-01-02,Heat,BROOKLYN,40.6892,-73.9442\n"
        "3,2024-01-03,Graffiti,QUEENS,,\n"
    )
    try:
        result = run_async(load(path))
        assert result.rows_kept == 1
        assert result.missing_coords == 2
    finally:
        os.unlink(path)


def test_normalises_column_names():
    path = write_csv(
        "Unique Key,Created Date,Complaint Type,Borough,Latitude,Longitude\n"
        "1,2024-01-01,Noise,MANHATTAN,40.7128,-74.0060\n"
    )
    try:
        result = run_async(load(path))
        assert "unique_key" in result.df.columns
        assert "complaint_type" in result.df.columns
    finally:
        os.unlink(path)


def test_detects_schema_drift():
    path = write_csv(
        "id,date,type\n"
        "1,2024-01-01,Noise\n"
    )
    try:
        result = run_async(load(path))
        assert len(result.warnings) > 0
        assert "schema drift" in result.warnings[0]
    finally:
        os.unlink(path)


def test_handles_non_numeric_coords():
    path = write_csv(
        "unique_key,created_date,complaint_type,borough,latitude,longitude\n"
        "1,2024-01-01,Noise,MANHATTAN,N/A,-74.0060\n"
        "2,2024-01-02,Heat,BROOKLYN,40.6892,-73.9442\n"
    )
    try:
        result = run_async(load(path))
        assert result.rows_kept == 1
    finally:
        os.unlink(path)

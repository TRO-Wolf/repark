"""FACADE-3 step 1 createDataFrame release-baseline runner (one shape table + cProfile split)."""

from __future__ import annotations

import cProfile
import datetime
import io
import json
import os
import pstats
import statistics
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

ROW_COUNTS: tuple[int, ...] = (10_000, 100_000)
WARMUPS = 1
REPS = 3
FLOOR_REPEATS = 3
PROFILE_TOP = 15
IDLE_PROCESSES: tuple[str, ...] = ("cargo", "rustc", "maturin")

BASE_DATE = datetime.date(2024, 1, 1)
BASE_TS = datetime.datetime(2024, 1, 1, 12, 0, 0)

DDL_SCHEMA = "i BIGINT, f DOUBLE, s STRING, b BOOLEAN, d DATE, t TIMESTAMP, dc DECIMAL(38, 18)"


def load1() -> float:
    """The 1-minute load average."""
    return os.getloadavg()[0]


def wait_for_idle() -> None:
    """Block until no cargo/rustc/maturin process is running on the box."""
    while True:
        busy = any(
            subprocess.run(
                ["pgrep", "-x", name],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            ).returncode
            == 0
            for name in IDLE_PROCESSES
        )
        if not busy:
            return
        time.sleep(20)


def native_is_release() -> bool:
    """True when the installed native module was built without debug assertions."""
    import repark._native as native

    return bool(getattr(native, "__debug_assertions__", True)) is False


def build_session() -> Any:
    """One facade session at 8 shuffle partitions and UTC."""
    from repark import ReparkSession

    active = ReparkSession.getActiveSession()
    if active is not None:
        active.stop()
    return (
        ReparkSession.builder.appName("facade-3-cdf-baseline")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def row_values(index: int) -> tuple[Any, ...]:
    """The seven-column scalar row: int, float, string, bool, date, timestamp, decimal."""
    return (
        index,
        index + 0.5,
        f"s{index}",
        index % 2 == 0,
        BASE_DATE + datetime.timedelta(days=index % 365),
        BASE_TS + datetime.timedelta(seconds=index),
        Decimal(index).scaleb(-2),
    )


def nested_values(index: int) -> tuple[Any, ...]:
    """The nested row: an int, an int list, a dict cell, and a tuple (struct) cell."""
    return (index, [index, index + 1], {"k": index, "s": f"x{index}"}, (index, f"t{index}"))


def struct_schema() -> Any:
    """The explicit seven-column StructType matching :func:`row_values`."""
    from repark.spark.types import (
        BooleanType,
        DateType,
        DecimalType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("i", LongType(), True),
            StructField("f", DoubleType(), True),
            StructField("s", StringType(), True),
            StructField("b", BooleanType(), True),
            StructField("d", DateType(), True),
            StructField("t", TimestampType(), True),
            StructField("dc", DecimalType(38, 18), True),
        ]
    )


def build_shape(name: str, count: int) -> tuple[Any, Any]:
    """Materialize the (data, schema) fixture for one cell name and row count."""
    if name == "tuples":
        return [row_values(index) for index in range(count)], None
    if name == "rows":
        from repark.spark.row import Row

        return [
            Row(
                i=index,
                f=index + 0.5,
                s=f"s{index}",
                b=index % 2 == 0,
                d=BASE_DATE + datetime.timedelta(days=index % 365),
                t=BASE_TS + datetime.timedelta(seconds=index),
                dc=Decimal(index).scaleb(-2),
            )
            for index in range(count)
        ], None
    if name == "dicts":
        return [
            {
                "i": index,
                "f": index + 0.5,
                "s": f"s{index}",
                "b": index % 2 == 0,
                "d": BASE_DATE + datetime.timedelta(days=index % 365),
                "t": BASE_TS + datetime.timedelta(seconds=index),
                "dc": Decimal(index).scaleb(-2),
            }
            for index in range(count)
        ], None
    if name == "tuples_ddl":
        return [row_values(index) for index in range(count)], DDL_SCHEMA
    if name == "tuples_struct":
        return [row_values(index) for index in range(count)], struct_schema()
    if name == "nested":
        return [nested_values(index) for index in range(count)], None
    if name == "pandas":
        import pandas as pd
        import pyarrow as pa

        frame = pd.DataFrame(
            {
                "i": pd.Series(range(count), dtype="int64"),
                "f": pd.Series([index + 0.5 for index in range(count)], dtype="float64"),
                "s": pd.Series([f"s{index}" for index in range(count)], dtype="string"),
                "b": pd.Series([index % 2 == 0 for index in range(count)], dtype="bool"),
                "d": pd.Series(
                    [BASE_DATE + datetime.timedelta(days=index % 365) for index in range(count)],
                    dtype=pd.ArrowDtype(pa.date32()),
                ),
                "t": pd.Series(
                    [BASE_TS + datetime.timedelta(seconds=index) for index in range(count)],
                    dtype="datetime64[us]",
                ),
                "dc": pd.Series(
                    [Decimal(index).scaleb(-2) for index in range(count)],
                    dtype=pd.ArrowDtype(pa.decimal128(38, 18)),
                ),
            }
        )
        return frame, None
    if name == "polars":
        import polars as pl

        frame = pl.DataFrame(
            {
                "i": list(range(count)),
                "f": [index + 0.5 for index in range(count)],
                "s": [f"s{index}" for index in range(count)],
                "b": [index % 2 == 0 for index in range(count)],
                "d": [BASE_DATE + datetime.timedelta(days=index % 365) for index in range(count)],
                "t": [BASE_TS + datetime.timedelta(seconds=index) for index in range(count)],
                "dc": [Decimal(index).scaleb(-2) for index in range(count)],
            }
        )
        return frame, None
    raise ValueError(f"unknown shape {name}")


SHAPES: tuple[str, ...] = (
    "tuples",
    "rows",
    "dicts",
    "tuples_ddl",
    "tuples_struct",
    "nested",
    "pandas",
    "polars",
)


def run_cell(session: Any, data: Any, schema: Any, op: str) -> float:
    """One timed createDataFrame (or createDataFrame().count()) call in seconds."""
    started = time.perf_counter()
    frame = (
        session.createDataFrame(data, schema)
        if schema is not None
        else session.createDataFrame(data)
    )
    if op == "count":
        frame.count()
    return time.perf_counter() - started


def measure_cell(session: Any, name: str, count: int, op: str) -> dict[str, Any]:
    """Warmup + REPS medians for one shape/count/op cell."""
    data, schema = build_shape(name, count)
    wait_for_idle()
    for _ in range(WARMUPS):
        run_cell(session, data, schema, op)
    samples: list[float] = []
    load_start = load1()
    for _ in range(REPS):
        samples.append(run_cell(session, data, schema, op))
    load_end = load1()
    return {
        "cell": f"{name}/{count}/{op}",
        "median_ms": statistics.median(samples) * 1000.0,
        "min_ms": min(samples) * 1000.0,
        "spread_ms": (max(samples) - min(samples)) * 1000.0,
        "samples_ms": [sample * 1000.0 for sample in samples],
        "load_start": load_start,
        "load_end": load_end,
    }


def profile_shape(session: Any, name: str, count: int) -> dict[str, Any]:
    """cProfile one createDataFrame call; top functions by cumulative time."""
    data, schema = build_shape(name, count)
    profiler = cProfile.Profile()
    wait_for_idle()
    started = time.perf_counter()
    profiler.enable()
    frame = (
        session.createDataFrame(data, schema)
        if schema is not None
        else session.createDataFrame(data)
    )
    profiler.disable()
    wall_ms = (time.perf_counter() - started) * 1000.0
    buffer = io.StringIO()
    stats = pstats.Stats(profiler, stream=buffer).sort_stats("cumulative")
    stats.print_stats(PROFILE_TOP)
    return {
        "cell": f"{name}/{count}",
        "wall_ms": wall_ms,
        "top": buffer.getvalue(),
        "count_result": frame.count(),
    }


def measure_floor(session: Any) -> float:
    """Spread of FLOOR_REPEATS medians of the pandas create control at 1e5."""
    data, schema = build_shape("pandas", 100_000)
    medians: list[float] = []
    for _ in range(FLOOR_REPEATS):
        wait_for_idle()
        samples = [run_cell(session, data, schema, "count") for _ in range(REPS)]
        medians.append(statistics.median(samples) * 1000.0)
    return max(medians) - min(medians)


def machine_header() -> dict[str, Any]:
    """CPU/kernel/python/pyarrow/native facts recorded beside the numbers."""
    import pyarrow as pa

    import repark._native as native

    cpu = "unknown"
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.is_file():
        for line in cpuinfo.read_text(encoding="utf-8").splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    native_file = getattr(native, "__file__", "")
    native_size = Path(native_file).stat().st_size if native_file else 0
    return {
        "cpu": cpu,
        "kernel": os.uname().release,
        "python": sys.version.split()[0],
        "pyarrow": pa.__version__,
        "native_bytes": native_size,
        "release_proof": native_is_release(),
    }


def main() -> int:
    """Run the full battery and write JSON + markdown to --out."""
    out_json = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/tmp/facade-3-cdf-baseline.json")
    if not native_is_release():
        raise SystemExit("native module is not a release build; refusing to measure")
    session = build_session()
    header = machine_header()
    run_load_start = load1()
    cells: list[dict[str, Any]] = []
    for name in SHAPES:
        for count in ROW_COUNTS:
            for op in ("create", "count"):
                cell = measure_cell(session, name, count, op)
                cells.append(cell)
                print(
                    f"{cell['cell']:>28}  median {cell['median_ms']:9.2f} ms  "
                    f"spread {cell['spread_ms']:7.2f}  "
                    f"load {cell['load_start']:.2f}->{cell['load_end']:.2f}",
                    flush=True,
                )
    floor = measure_floor(session)
    slowest = sorted(
        (cell for cell in cells if cell["cell"].endswith("/100000/create")),
        key=lambda cell: cell["median_ms"],
        reverse=True,
    )[:3]
    profiles = [profile_shape(session, cell["cell"].split("/")[0], 100_000) for cell in slowest]
    payload = {
        "header": header,
        "run_load": [run_load_start, load1()],
        "floor_ms": floor,
        "cells": cells,
        "profiles": profiles,
    }
    out_json.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {out_json}")
    session.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

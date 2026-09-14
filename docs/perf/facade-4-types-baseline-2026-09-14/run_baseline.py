"""FACADE-4 step 0 baseline — type-conversion cost isolated per schema and per op.

Cells: (a) ``repark_type_to_arrow`` per schema plus a spy count of calls per
``createDataFrame`` / ``collect`` / ``to_arrow`` / ``show`` of a 1e5 frame;
(b) ``struct_type_from_arrow`` round-trip; (c) DDL parse and DDL write walls;
(d) cProfile cumulative share of conversion inside ``df.schema``,
``createDataFrame(pandas)`` and ``spark.read.csv(inferSchema)`` at 1e5.
Warmup + 5 reps, medians, an idle-box wait before every cell. Prints JSON.
"""

from __future__ import annotations

import contextlib
import cProfile
import io
import json
import pstats
import statistics
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

REPS = 5
CALLS_PER_SAMPLE = 200
ROWS = 100_000

CONVERSION_FUNCS = {
    "_arrow_type_to_repark",
    "_data_type_to_sql_type",
    "_datatype_to_ddl_token",
    "_engine_type",
    "_normalize_nested_sql_type_aliases",
    "_parse_atomic_token",
    "_parse_complex_or_atomic",
    "_parse_datatype_json_value",
    "_parse_datatype_string",
    "_parse_field_list",
    "_sql_type_to_arrow",
    "fromDDL",
    "jsonValue",
    "repark_type_to_arrow",
    "resolve_cell_rung",
    "resolve_column_type",
    "rung_to_engine_cast",
    "rung_to_spark_type",
    "rung_to_sql_cast",
    "simpleString",
    "struct_type_from_arrow",
    "toDDL",
    "try_rung",
    "typeName",
}


def wait_for_idle() -> float:
    """Block until no cargo/rustc/maturin runs; return the settled 1-minute load."""
    while True:
        busy = subprocess.run(
            ["pgrep", "-e", "cargo", "-e", "rustc", "-e", "maturin"],
            capture_output=True,
        )
        if busy.returncode != 0:
            break
        time.sleep(5)
    return float(Path("/proc/loadavg").read_text(encoding="ascii").split()[0])


def _swallow(fn: Any) -> Any:
    """Call ``fn`` ignoring exceptions so refusal paths are also timed."""

    def wrapped() -> Any:
        try:
            return fn()
        except Exception:
            return None

    return wrapped


def timed(fn: Any, reps: int = REPS) -> dict[str, float]:
    """Wall median of ``reps`` calls after one warmup call."""
    safe = _swallow(fn)
    safe()
    samples = []
    for _ in range(reps):
        start = time.perf_counter()
        safe()
        samples.append((time.perf_counter() - start) * 1e3)
    return {"median_ms": statistics.median(samples), "min_ms": min(samples)}


def per_call_us(fn: Any) -> float:
    """Median microseconds per call over reps batches of CALLS_PER_SAMPLE."""
    safe = _swallow(fn)
    safe()
    samples = []
    for _ in range(REPS):
        start = time.perf_counter()
        for _ in range(CALLS_PER_SAMPLE):
            safe()
        samples.append((time.perf_counter() - start) / CALLS_PER_SAMPLE * 1e6)
    return statistics.median(samples)


def _schemas() -> dict[str, Any]:
    from repark.spark import types as t

    nested_inner = t.MapType(
        t.StringType(),
        t.StructType([t.StructField("leaf", t.DecimalType(10, 2), False)]),
        False,
    )
    nested3 = t.StructType(
        [
            t.StructField("top", t.IntegerType(), False),
            t.StructField(
                "mid",
                t.ArrayType(
                    t.StructType(
                        [
                            t.StructField("m", nested_inner, True),
                            t.StructField("arr", t.ArrayType(t.LongType(), False), False),
                        ]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )
    cycle = [
        t.IntegerType(),
        t.LongType(),
        t.DoubleType(),
        t.StringType(),
        t.BooleanType(),
        t.DateType(),
        t.TimestampType(),
        t.DecimalType(10, 2),
        t.BinaryType(),
        t.FloatType(),
    ]
    return {
        "flat7": t.StructType(
            [
                t.StructField("i", t.IntegerType(), True),
                t.StructField("l", t.LongType(), True),
                t.StructField("f", t.DoubleType(), True),
                t.StructField("s", t.StringType(), True),
                t.StructField("b", t.BooleanType(), True),
                t.StructField("d", t.DateType(), True),
                t.StructField("t", t.TimestampType(), True),
            ]
        ),
        "wide50": t.StructType(
            [
                t.StructField(f"c{index}", cycle[index % len(cycle)], index % 3 != 0)
                for index in range(50)
            ]
        ),
        "nested3": nested3,
        "decimal_variants": t.StructType(
            [
                t.StructField("d10", t.DecimalType(10, 2), True),
                t.StructField("d38", t.DecimalType(38, 18), True),
                t.StructField("d38s0", t.DecimalType(38, 0), True),
            ]
        ),
        "timestamp_variants": t.StructType(
            [
                t.StructField("ltz", t.TimestampType(), True),
                t.StructField("ntz", t.TimestampNTZType(), True),
                t.StructField("tz", t.TimestampType(), True),
            ]
        ),
        "interval_char_varchar": t.StructType(
            [
                t.StructField("ym", t.YearMonthIntervalType(), True),
                t.StructField("dt", t.DayTimeIntervalType(), True),
                t.StructField("cal", t.CalendarIntervalType(), True),
                t.StructField("ch", t.CharType(8), True),
                t.StructField("vc", t.VarcharType(32), True),
            ]
        ),
    }


def _conversion_cumtime(prof: cProfile.Profile) -> float:
    """Total cumulative seconds across the conversion function set."""
    stats = pstats.Stats(prof)
    total = 0.0
    for (_file, _line, name), (_cc, _nc, _tt, cumtime, _callers) in stats.stats.items():
        if name in CONVERSION_FUNCS:
            total += cumtime
    return total


def _profile_op(fn: Any) -> dict[str, float]:
    """Profile one op; return wall ms and conversion cumulative ms."""
    prof = cProfile.Profile()
    start = time.perf_counter()
    prof.enable()
    _swallow(fn)()
    prof.disable()
    wall_ms = (time.perf_counter() - start) * 1e3
    return {"wall_ms": wall_ms, "conversion_cum_ms": _conversion_cumtime(prof) * 1e3}


def _pandas_frame() -> Any:
    import pandas as pd

    return pd.DataFrame(
        {
            "i": range(ROWS),
            "f": [float(index) + 0.5 for index in range(ROWS)],
            "s": ["s"] * ROWS,
            "b": [True] * ROWS,
            "l": range(ROWS),
            "d": pd.to_datetime("2024-01-02").date(),
            "t": pd.Timestamp("2024-01-02 03:04:05"),
        }
    )


def _csv_file(tmpdir: Path) -> str:
    path = tmpdir / "infer.csv"
    with path.open("w", encoding="utf-8") as handle:
        handle.write("i,f,s,b,d,t\n")
        for index in range(ROWS):
            handle.write(f"{index},{index}.5,s{index},true,2024-01-02,2024-01-02 03:04:05\n")
    return str(path)


def _explicit_nested_schema() -> Any:
    from repark.spark import types as t

    return t.StructType(
        [
            t.StructField("arr", t.ArrayType(t.IntegerType()), True),
            t.StructField("m", t.MapType(t.StringType(), t.IntegerType()), True),
            t.StructField(
                "st",
                t.StructType(
                    [t.StructField("x", t.LongType()), t.StructField("y", t.StringType())]
                ),
                True,
            ),
        ]
    )


def cell_a(session: Any, schemas: dict[str, Any]) -> dict[str, Any]:
    """Per-call cost of repark_type_to_arrow plus per-op call counts."""
    from repark.spark import types as t

    per_schema = {
        name: {"repark_type_to_arrow_us": per_call_us(lambda s=schema: t.repark_type_to_arrow(s))}
        for name, schema in schemas.items()
    }
    spy_counts: dict[str, int] = {}
    original = t.repark_type_to_arrow

    def spy(data_type: Any) -> Any:
        spy_counts["n"] = spy_counts.get("n", 0) + 1
        return original(data_type)

    t.repark_type_to_arrow = spy
    try:
        frame_pd = session.createDataFrame(_pandas_frame())
        nested_rows = [([1, 2], {"k": index}, (index, "s")) for index in range(ROWS)]
        ops = {
            "createDataFrame_pandas_1e5": lambda: session.createDataFrame(_pandas_frame()),
            "createDataFrame_nested_1e5": lambda: session.createDataFrame(
                nested_rows, _explicit_nested_schema()
            ),
            "collect_1e5": lambda: frame_pd.collect(),
            "to_arrow_1e5": lambda: frame_pd.to_arrow(),
            "show_1e5": lambda: frame_pd.show(),
            "df_schema": lambda: frame_pd.schema,
        }
        counts = {}
        for op_name, op in ops.items():
            if op_name == "show_1e5":
                spy_counts["n"] = 0
                with contextlib.redirect_stdout(io.StringIO()):
                    wall = timed(op, reps=3)
            else:
                spy_counts["n"] = 0
                wall = timed(op, reps=3)
            counts[op_name] = {
                "calls_per_op": spy_counts["n"] / 4,
                "wall_ms": wall["median_ms"],
            }
    finally:
        t.repark_type_to_arrow = original
    return {"per_schema": per_schema, "calls_per_op": counts}


def cell_b(schemas: dict[str, Any]) -> dict[str, Any]:
    """struct_type_from_arrow round-trip per schema."""
    import pyarrow as pa

    from repark.spark import types as t

    out = {}
    for name, schema in schemas.items():
        try:
            arrow_schema = pa.schema(
                [
                    pa.field(field.name, t.repark_type_to_arrow(field.dataType))
                    for field in schema.fields
                ]
            )
        except Exception as error:
            out[name] = {"refused": f"{type(error).__name__}: {error}"}
            continue

        def round_trip(s: Any = arrow_schema) -> Any:
            rebuilt = t.struct_type_from_arrow(s)
            return [t.repark_type_to_arrow(field.dataType) for field in rebuilt.fields]

        out[name] = {
            "struct_type_from_arrow_us": per_call_us(
                lambda s=arrow_schema: t.struct_type_from_arrow(s)
            ),
            "round_trip_us": per_call_us(round_trip),
        }
    return out


def cell_c(schemas: dict[str, Any]) -> dict[str, Any]:
    """DDL parse and DDL write walls per schema."""
    from repark.spark import types as t

    out = {}
    for name, schema in schemas.items():
        simple = schema.simpleString()
        try:
            ddl = schema.toDDL()
        except Exception:
            ddl = simple
        out[name] = {
            "fromddl_us": per_call_us(lambda text=simple: t.DataType.fromDDL(text)),
            "parse_datatype_string_us": per_call_us(
                lambda text=simple: t._parse_datatype_string(text)
            ),
            "fromddl_ddl_us": per_call_us(lambda text=ddl: t.DataType.fromDDL(text)),
            "simplestring_us": per_call_us(lambda s=schema: s.simpleString()),
            "toddl_us": per_call_us(lambda s=schema: s.toDDL()),
        }
    for atomic_name in ("timestamp", "decimal(38,18)", "char(8)", "interval day to second"):
        out[f"atomic_{atomic_name}"] = {
            "fromddl_us": per_call_us(lambda text=atomic_name: t.DataType.fromDDL(text)),
        }
    return out


def cell_d(session: Any, tmpdir: Path) -> dict[str, Any]:
    """cProfile share of conversion in end-to-end ops."""
    csv_path = _csv_file(tmpdir)
    pandas_frame = _pandas_frame()
    frame = session.createDataFrame(pandas_frame)
    ops = {
        "df_schema": lambda: frame.schema,
        "createDataFrame_pandas_1e5": lambda: session.createDataFrame(pandas_frame),
        "read_csv_infer_1e5": lambda: session.read.csv(
            csv_path, header=True, inferSchema=True
        ).collect(),
    }
    out = {}
    for op_name, op in ops.items():
        runs = [_profile_op(op) for _ in range(REPS)]
        out[op_name] = {
            "wall_ms": statistics.median(run["wall_ms"] for run in runs),
            "conversion_cum_ms": statistics.median(run["conversion_cum_ms"] for run in runs),
        }
    return out


def main() -> None:
    """Run all cells under idle-box waits and print JSON."""
    import repark._native as native
    from repark import ReparkSession

    env = {
        "debug_assertions": bool(native.__debug_assertions__),
        "rows": ROWS,
        "reps": REPS,
    }
    results: dict[str, Any] = {"env": env, "cells": {}}
    session = (
        ReparkSession.builder.appName("facade-4-baseline")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.timestampType", "TIMESTAMP_LTZ")
        .getOrCreate()
    )
    try:
        schemas = _schemas()
        with tempfile.TemporaryDirectory(prefix="facade4-baseline-") as tmp:
            tmpdir = Path(tmp)
            for cell_name, thunk in (
                ("a_repark_type_to_arrow", lambda: cell_a(session, schemas)),
                ("b_struct_type_from_arrow", lambda: cell_b(schemas)),
                ("c_ddl_parse_write", lambda: cell_c(schemas)),
                ("d_end_to_end_share", lambda: cell_d(session, tmpdir)),
            ):
                load = wait_for_idle()
                results["cells"][cell_name] = {"load_at_start": load, "data": thunk()}
    finally:
        session.stop()
    print(json.dumps(results, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

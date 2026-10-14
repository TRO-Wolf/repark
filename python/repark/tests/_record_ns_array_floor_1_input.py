"""NS-ARRAY-FLOOR-1 D1: record what each pandas and Arrow door receives.

Runs every door over a frame whose column is a timestamp_ns column, a
timestamptz_ns column, the U-1 widened shape, or a microsecond control, at
top level and inside an array. The probe function keeps what it saw and
answers a trivial integer frame, so an answer cell measures the input side
and an error cell names the door refusal.

Usage with a repark module under test, then with live Spark for the
microsecond control only:

    python _record_ns_array_floor_1_input.py --out e.json --md c.md
    JAVA_HOME=... SPARK_LOCAL_IP=127.0.0.1 python _record_ns_array_floor_1_input.py --live

pins: ns-array-floor-1/C-001, C-002
"""

from __future__ import annotations

import argparse
import contextlib
import json
import re
import sys
import tempfile
import traceback
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa

ZONE = "UTC"
SEED_NS = [1767323045123456789, None, 1767323045000000001, -1, -1500]
SEED_US = [1767323045123456, None, 1767323045000000, -1, -2]
KEYS = [1, 0, 1, 0, 1]

SOURCES_TO_SQL = {
    "ns": "ns",
    "tzns": "tzns",
    "coal": "coalesce(CAST(NULL AS TIMESTAMP), ns)",
    "case": "CASE WHEN id > 1 THEN NULL ELSE ns END",
    "us": "us",
}

LIVE_SOURCES = ["us"]

DOORS = [
    "grouped_applyin",
    "cogroup_applyin",
    "grouped_applyinarrow",
    "cogroup_applyinarrow",
    "mapinpan",
    "mapinarrow",
    "scalar_pandas_udf",
    "groupedagg_pandas_udf",
    "scalar_arrow_udf",
]


def scrub_path(path: str, roots: list[str]) -> str:
    for root in roots:
        if path.startswith(root):
            return path[len(root) :].lstrip("/\\")
    return Path(path).name


def scrub_text(text: str) -> str:
    scratch = tempfile.gettempdir()
    home = str(Path.home())
    out = []
    for token in text.split():
        stripped = token.lstrip("\"'(")
        if stripped.startswith(scratch) or stripped.startswith(home):
            out.append("<scratch>")
        else:
            out.append(token)
    return " ".join(out)


def package_roots() -> list[str]:
    roots = []
    for name in ("repark", "pyspark", "pandas", "pyarrow"):
        module = sys.modules.get(name)
        if module is not None and getattr(module, "__file__", None):
            roots.append(str(Path(str(module.__file__)).parent.parent) + "/")
    return roots


def error_record(error: BaseException) -> dict[str, Any]:
    roots = package_roots()
    frames = []
    for frame in traceback.extract_tb(error.__traceback__)[-8:]:
        frames.append(
            {
                "file": scrub_path(frame.filename, roots),
                "line": frame.lineno,
                "func": frame.name,
            }
        )
    condition = ""
    getter = getattr(error, "getErrorClass", None)
    if callable(getter):
        try:
            condition = str(getter())
        except Exception:
            condition = ""
    text = scrub_text(str(error))
    lines = [line for line in text.splitlines() if line.strip()]
    match = re.search(r"[A-Za-z_.]*Error\s*:\s*.{0,200}", text)
    return {
        "error": type(error).__name__,
        "condition": condition,
        "first_line": lines[0][:300] if lines else "",
        "inner_line": match.group(0)[:300] if match else "",
        "traceback": frames,
    }


def short_values(values: list[Any], limit: int = 3) -> list[str]:
    return [repr(value)[:80] for value in values[:limit]]


def open_repark_session(warehouse: str) -> Any:
    from repark import ReparkSession

    spark = (
        ReparkSession.builder.appName("ns-array-floor-1-input")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", ZONE)
        .getOrCreate()
    )
    spark.register_memory_catalog("ice", warehouse)
    spark.sql("CREATE NAMESPACE ice.ns")
    spark.sql(
        "CREATE TABLE ice.ns.src (id INT, k INT, ns timestamp_ns,"
        " tzns timestamptz_ns, us TIMESTAMP)"
        " USING iceberg TBLPROPERTIES ('format-version'='3')"
    )
    import pyarrow as pa

    table = pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5], pa.int32()),
            "k": pa.array(KEYS, pa.int32()),
            "ns": pa.array(SEED_NS, type=pa.timestamp("ns")),
            "tzns": pa.array(SEED_NS, type=pa.timestamp("ns", tz="UTC")),
            "us": pa.array(SEED_US, type=pa.timestamp("us", tz="UTC")),
        }
    )
    spark._ensure_alive().register_arrow_stream_as_temp_view("ns_seed", table)
    spark.table("ns_seed").writeTo("ice.ns.src").append()
    return spark


def open_live_session(safe: str | None = None) -> Any:
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("ns-array-floor-1-input-live")
        .master("local[1]")
        .config("spark.sql.session.timeZone", ZONE)
        .config("spark.sql.ansi.enabled", "false")
    )
    if safe is not None:
        builder = builder.config("spark.sql.execution.pandas.convertToArrowArraySafely", safe)
    return builder.getOrCreate()


def live_seed(spark: Any) -> Any:
    import datetime

    base = datetime.datetime(1970, 1, 1)
    stamps = []
    for micros in SEED_US:
        if micros is None:
            stamps.append(None)
        else:
            stamps.append(base + datetime.timedelta(microseconds=micros))
    rows = [(index + 1, KEYS[index], stamps[index]) for index in range(5)]
    return spark.createDataFrame(rows, "id int, k int, us timestamp")


def frame_for(spark: Any, source: str, level: str, live: bool) -> Any:
    if live:
        base = live_seed(spark)
        if level == "arr":
            from pyspark.sql import functions as spark_functions

            return base.select("k", spark_functions.array("us").alias("v"))
        return base.selectExpr("k", "us AS v")
    expr = SOURCES_TO_SQL[source]
    if level == "arr":
        return spark.sql(f"SELECT k, array({expr}) AS v FROM ice.ns.src")
    return spark.sql(f"SELECT k, {expr} AS v FROM ice.ns.src")


def input_arrow(frame: Any) -> str:
    try:
        return scrub_text(str(frame.toArrow().schema))
    except Exception as error:
        return "unreadable: " + scrub_text(str(error))[:120]


def udf_libs() -> tuple[Any, Any, Any]:
    try:
        from repark.spark.functions_arrow_udf import arrow_udf
        from repark.spark.functions_udf import pandas_udf

        return pandas_udf, arrow_udf, "GROUPED_AGG"
    except ImportError:
        from pyspark.sql.functions import PandasUDFType
        from pyspark.sql.functions import arrow_udf as spark_arrow_udf
        from pyspark.sql.functions import pandas_udf as spark_pandas_udf

        return spark_pandas_udf, spark_arrow_udf, PandasUDFType.GROUPED_AGG


def rows_repr(rows: Any) -> dict[str, Any]:
    items = [repr(row)[:100] for row in rows[:4]]
    return {"count": len(rows), "rows": items}


def run_grouped_applyin(frame: Any, seen: dict[str, Any]) -> Any:
    def func(pdf: Any) -> Any:
        seen["dtype"] = {k: str(v) for k, v in pdf.dtypes.astype(str).items()}
        seen["values"] = short_values(list(pdf["v"]))
        return pdf[["k"]]

    return frame.groupBy("k").applyInPandas(func, "k int").collect()


def run_cogroup_applyin(left: Any, right: Any, seen: dict[str, Any]) -> Any:
    def func(first: Any, second: Any) -> Any:
        seen["left_dtype"] = {k: str(v) for k, v in first.dtypes.astype(str).items()}
        seen["right_dtype"] = {k: str(v) for k, v in second.dtypes.astype(str).items()}
        seen["values"] = short_values(list(first["v"]))
        return first[["k"]]

    return left.groupBy("k").cogroup(right.groupBy("k")).applyInPandas(func, "k int").collect()


def run_grouped_applyinarrow(frame: Any, seen: dict[str, Any]) -> Any:
    def func(table: Any) -> Any:
        seen["arrow"] = scrub_text(str(table.schema))
        seen["values"] = short_values(table.column("v").to_pylist())
        return table.select(["k"])

    return frame.groupBy("k").applyInArrow(func, "k int").collect()


def run_cogroup_applyinarrow(left: Any, right: Any, seen: dict[str, Any]) -> Any:
    def func(first: Any, second: Any) -> Any:
        seen["left_arrow"] = scrub_text(str(first.schema))
        seen["right_arrow"] = scrub_text(str(second.schema))
        seen["values"] = short_values(first.column("v").to_pylist())
        return first.select(["k"])

    return left.groupBy("k").cogroup(right.groupBy("k")).applyInArrow(func, "k int").collect()


def run_mapinpan(frame: Any, seen: dict[str, Any]) -> Any:
    def func(batches: Any) -> Any:
        for pdf in batches:
            if "dtype" not in seen:
                seen["dtype"] = {k: str(v) for k, v in pdf.dtypes.astype(str).items()}
                seen["values"] = short_values(list(pdf["v"]))
            yield pdf[["k"]]

    return frame.mapInPandas(func, "k int").collect()


def run_mapinarrow(frame: Any, seen: dict[str, Any]) -> Any:
    import pyarrow as pa

    def func(batches: Any) -> Any:
        for batch in batches:
            if "arrow" not in seen:
                seen["arrow"] = scrub_text(str(batch.schema))
                seen["values"] = short_values(batch.column("v").to_pylist())
            yield pa.RecordBatch.from_arrays([batch.column("k")], names=["k"])

    return frame.mapInArrow(func, "k int").collect()


def run_scalar_pandas_udf(frame: Any, seen: dict[str, Any]) -> Any:
    pandas_udf, _, _ = udf_libs()

    def func(series: pd.Series) -> pd.Series:
        seen["dtype"] = str(series.dtype)
        seen["values"] = short_values(list(series))
        return pd.Series([1] * len(series), dtype="int64")

    udf = pandas_udf(func, "long")
    return frame.select(udf("v").alias("r")).collect()


def run_groupedagg_pandas_udf(frame: Any, seen: dict[str, Any]) -> Any:
    pandas_udf, _, token = udf_libs()

    def func(series: pd.Series) -> int:
        seen["dtype"] = str(series.dtype)
        seen["values"] = short_values(list(series))
        return len(series)

    udf = pandas_udf(func, "long", token)
    return frame.groupBy("k").agg(udf("v").alias("r")).collect()


def run_scalar_arrow_udf(frame: Any, seen: dict[str, Any]) -> Any:
    _, arrow_udf, _ = udf_libs()

    def func(array: pa.Array) -> pa.Array:
        seen["arrow"] = str(array.type)
        seen["values"] = short_values(array.to_pylist())
        return pa.array([1] * len(array), type=pa.int64())

    udf = arrow_udf(func, "long")
    return frame.select(udf("v").alias("r")).collect()


RUNNERS = {
    "grouped_applyin": run_grouped_applyin,
    "grouped_applyinarrow": run_grouped_applyinarrow,
    "mapinpan": run_mapinpan,
    "mapinarrow": run_mapinarrow,
    "scalar_pandas_udf": run_scalar_pandas_udf,
    "groupedagg_pandas_udf": run_groupedagg_pandas_udf,
    "scalar_arrow_udf": run_scalar_arrow_udf,
}

COGROUP_RUNNERS = {
    "cogroup_applyin": run_cogroup_applyin,
    "cogroup_applyinarrow": run_cogroup_applyinarrow,
}

_LIVE_SEEN: dict[str, dict[str, str]] = {}

PANDAS_FRAME_DOORS = ("grouped_applyin", "cogroup_applyin", "mapinpan")
PANDAS_SERIES_DOORS = ("scalar_pandas_udf", "groupedagg_pandas_udf")
ARROW_DOORS = ("grouped_applyinarrow", "cogroup_applyinarrow", "mapinarrow", "scalar_arrow_udf")


def live_pandas_dtype(spark: Any, level: str) -> str:
    frame = frame_for(spark, "us", level, True)

    def func(pdf: Any) -> Any:
        return pd.DataFrame({"k": pdf["k"], "seen": str(pdf["v"].dtype)})

    rows = frame.groupBy("k").applyInPandas(func, "k int, seen string").collect()
    return ",".join(sorted({str(row["seen"]) for row in rows}))


def live_arrow_schema(spark: Any, level: str) -> str:
    frame = frame_for(spark, "us", level, True)

    def func(batches: Any) -> Any:
        for batch in batches:
            yield pa.RecordBatch.from_arrays(
                [batch.column("k"), pa.array([str(batch.schema)] * batch.num_rows)],
                names=["k", "seen"],
            )

    rows = frame.mapInArrow(func, "k int, seen string").collect()
    return ",".join(sorted({str(row["seen"]) for row in rows}))


def fill_live_seen(cell: dict[str, Any], door: str, level: str) -> None:
    diag = _LIVE_SEEN.get(level, {})
    if door in PANDAS_FRAME_DOORS:
        cell["seen"] = {
            "dtype": diag.get("pandas", "unknown"),
            "via": "shared batch conversion diagnostic",
        }
    elif door in PANDAS_SERIES_DOORS:
        cell["seen"] = {
            "dtype": diag.get("pandas", "unknown"),
            "via": "derived: Series slice of the same converted batch",
        }
    elif door in ARROW_DOORS:
        cell["seen"] = {
            "arrow": diag.get("arrow", "unknown"),
            "via": "shared batch stream diagnostic",
        }


ALL_DOORS = DOORS


def measure_cell(spark: Any, door: str, source: str, level: str, live: bool) -> dict[str, Any]:
    cell: dict[str, Any] = {"door": door, "source": source, "level": level}
    try:
        frame = frame_for(spark, source, level, live)
        cell["input_arrow"] = input_arrow(frame)
    except Exception as error:
        cell["outcome"] = "frame_error"
        cell.update(error_record(error))
        return cell
    seen: dict[str, Any] = {}
    try:
        if door in COGROUP_RUNNERS:
            right = frame_for(spark, source, level, live)
            rows = COGROUP_RUNNERS[door](frame, right, seen)
        else:
            rows = RUNNERS[door](frame, seen)
        cell["outcome"] = "answer"
        cell["seen"] = seen
        if live:
            fill_live_seen(cell, door, level)
        cell["returned"] = rows_repr(rows)
    except Exception as error:
        cell["outcome"] = "error"
        cell["seen"] = seen
        cell.update(error_record(error))
    return cell


def write_markdown(path: str, meta: dict[str, Any], cells: list[dict[str, Any]]) -> None:
    lines = [
        "## NS-ARRAY-FLOOR-1 input cells",
        "",
        "Backend "
        + meta["label"]
        + " base "
        + meta["base_sha"]
        + " zone "
        + meta["zone"]
        + " live "
        + str(meta["live"])
        + " safe "
        + str(meta.get("safe_setting", "n/a"))
        + ".",
        "",
    ]
    if meta.get("banner"):
        lines.append("Banner " + meta["banner"])
        lines.append("")
    for door in ALL_DOORS:
        lines.append("### " + door)
        lines.append("")
        lines.append("| source | level | outcome | seen | returned_or_error |")
        lines.append("|---|---|---|---|---|")
        for cell in [c for c in cells if c["door"] == door]:
            seen = cell.get("seen", {})
            if cell["outcome"] == "answer":
                detail = repr(seen)[:160] + " -> " + repr(cell["returned"])[:120]
            else:
                detail = (
                    cell.get("error", "")
                    + " "
                    + cell.get("condition", "")
                    + " "
                    + cell.get("first_line", "")[:160]
                    + " / "
                    + cell.get("inner_line", "")[:160]
                )
            lines.append(
                "| "
                + cell["source"]
                + " | "
                + cell["level"]
                + " | "
                + cell["outcome"]
                + " | "
                + repr(seen)[:120]
                + " | "
                + detail.replace("|", "/")[:200]
                + " |"
            )
        lines.append("")
    Path(path).write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    parser.add_argument("--md", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--base-sha", default="")
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--safe-setting", default=None)
    args = parser.parse_args()
    if args.live:
        spark = open_live_session(args.safe_setting)
        meta: dict[str, Any] = {
            "label": args.label,
            "base_sha": args.base_sha,
            "zone": ZONE,
            "live": True,
            "safe_setting": (
                args.safe_setting
                if args.safe_setting is not None
                else str(spark.conf.get("spark.sql.execution.pandas.convertToArrowArraySafely"))
            ),
            "banner": "spark "
            + str(spark.version)
            + " zone "
            + str(spark.conf.get("spark.sql.session.timeZone")),
        }
        sources = LIVE_SOURCES
        for level in ("top", "arr"):
            try:
                _LIVE_SEEN[level] = {
                    "pandas": live_pandas_dtype(spark, level),
                    "arrow": live_arrow_schema(spark, level),
                }
            except Exception as error:
                _LIVE_SEEN[level] = {
                    "pandas": "diagnostic failed: " + type(error).__name__,
                    "arrow": "diagnostic failed: " + type(error).__name__,
                }
        meta["live_seen"] = _LIVE_SEEN
    else:
        warehouse = tempfile.mkdtemp(prefix="ns-array-floor-1-in-")
        spark = open_repark_session(warehouse)
        meta = {
            "label": args.label,
            "base_sha": args.base_sha,
            "zone": ZONE,
            "live": False,
            "safe_setting": "n/a",
        }
        sources = list(SOURCES_TO_SQL)
    cells = []
    for source in sources:
        for level in ("top", "arr"):
            for door in ALL_DOORS:
                cells.append(measure_cell(spark, door, source, level, args.live))
    payload = {"meta": meta, "cells": cells}
    Path(args.out).write_text(json.dumps(payload, indent=1), encoding="utf-8")
    write_markdown(args.md, meta, cells)
    with contextlib.suppress(Exception):
        spark.stop()


if __name__ == "__main__":
    main()

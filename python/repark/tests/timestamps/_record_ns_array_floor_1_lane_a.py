"""NS-ARRAY-FLOOR-1 F2: Lane A echo shape behind the 108 lost statements.

The function returns its input frame unchanged and the declared return
schema is the input frame's own schema. Sources are a plain timestamp_ns
column, coalesce(ns, NULL), and a microsecond control; on live Spark the
microsecond control plus a pandas frame built with datetime64[ns] values.
Ticks are read back as integers through an Arrow cast.

Usage with a repark module under test, then with live Spark:

    python _record_ns_array_floor_1_lane_a.py --out e.json --md c.md
    PYSPARK_PYTHON=... JAVA_HOME=... python _record_ns_array_floor_1_lane_a.py --live

pins: ns-array-floor-1/C-006, C-007
"""

from __future__ import annotations

import argparse
import contextlib
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

sys.path.insert(0, str(Path(__file__).parent))
import _record_ns_array_floor_1_input as base

ZONE = base.ZONE

SOURCES_TO_SQL = {
    "ns_plain": "ns",
    "ns_coal": "coalesce(ns, NULL)",
    "us": "us",
}

DOORS = [
    "grouped_applyin",
    "cogroup_applyin",
    "grouped_applyinarrow",
    "cogroup_applyinarrow",
    "mapinpan",
    "mapinarrow",
    "scalar_pandas_udf",
    "scalar_arrow_udf",
]


def open_session(warehouse: str, live: bool, safe: str | None = None) -> Any:
    if live:
        return base.open_live_session(safe)
    return base.open_repark_session(warehouse)


def seed_frame(spark: Any, source: str, live: bool) -> Any:
    if live:
        if source == "ns_pandas":
            import pandas as pd

            pdf = pd.DataFrame(
                {
                    "k": base.KEYS,
                    "v": pd.to_datetime(base.SEED_NS, unit="ns"),
                }
            )
            return spark.createDataFrame(pdf)
        import datetime

        stamps = []
        for micros in base.SEED_US:
            if micros is None:
                stamps.append(None)
            else:
                stamps.append(
                    datetime.datetime(1970, 1, 1) + datetime.timedelta(microseconds=micros)
                )
        rows = [(index + 1, base.KEYS[index], stamps[index]) for index in range(5)]
        base_frame = spark.createDataFrame(rows, "id int, k int, us timestamp")
        return base_frame.selectExpr("k", "us AS v")
    expr = SOURCES_TO_SQL[source]
    return spark.sql(f"SELECT k, {expr} AS v FROM ice.ns.src")


def schema_simple(schema: Any) -> list[list[str]]:
    return [[field.name, field.dataType.simpleString()] for field in schema.fields]


def result_ticks(table: Any) -> Any:
    return pc.cast(table.column("v"), pa.int64()).to_pylist()


def run_grouped_applyin(frame: Any, schema: Any) -> Any:
    def func(pdf: Any) -> Any:
        return pdf

    return frame.groupBy("k").applyInPandas(func, schema)


def run_cogroup_applyin(frame: Any, schema: Any) -> Any:
    def func(first: Any, second: Any) -> Any:
        return first

    return frame.groupBy("k").cogroup(frame.groupBy("k")).applyInPandas(func, schema)


def run_grouped_applyinarrow(frame: Any, schema: Any) -> Any:
    def func(table: Any) -> Any:
        return table

    return frame.groupBy("k").applyInArrow(func, schema)


def run_cogroup_applyinarrow(frame: Any, schema: Any) -> Any:
    def func(first: Any, second: Any) -> Any:
        return first

    return frame.groupBy("k").cogroup(frame.groupBy("k")).applyInArrow(func, schema)


def run_mapinpan(frame: Any, schema: Any) -> Any:
    def func(batches: Any) -> Any:
        yield from batches

    return frame.mapInPandas(func, schema)


def run_mapinarrow(frame: Any, schema: Any) -> Any:
    def func(batches: Any) -> Any:
        yield from batches

    return frame.mapInArrow(func, schema)


def run_scalar_pandas_udf(frame: Any, schema: Any, simple: list[list[str]]) -> Any:
    pandas_udf, _, _ = base.udf_libs()

    def func(series: pd.Series) -> pd.Series:
        return series

    udf = pandas_udf(func, simple[1][1])
    return frame.select(udf("v").alias("v"))


def run_scalar_arrow_udf(frame: Any, schema: Any, simple: list[list[str]]) -> Any:
    _, arrow_udf, _ = base.udf_libs()

    def func(array: pa.Array) -> pa.Array:
        return array

    udf = arrow_udf(func, simple[1][1])
    return frame.select(udf("v").alias("v"))


RUNNERS = {
    "grouped_applyin": run_grouped_applyin,
    "cogroup_applyin": run_cogroup_applyin,
    "grouped_applyinarrow": run_grouped_applyinarrow,
    "cogroup_applyinarrow": run_cogroup_applyinarrow,
    "mapinpan": run_mapinpan,
    "mapinarrow": run_mapinarrow,
    "scalar_pandas_udf": run_scalar_pandas_udf,
    "scalar_arrow_udf": run_scalar_arrow_udf,
}

SCALAR_DOORS = ("scalar_pandas_udf", "scalar_arrow_udf")


def measure_cell(spark: Any, door: str, source: str, live: bool) -> dict[str, Any]:
    cell: dict[str, Any] = {"door": door, "source": source}
    try:
        frame = seed_frame(spark, source, live)
        cell["input_arrow"] = base.input_arrow(frame)
        schema = frame.schema
        simple = schema_simple(schema)
        cell["schema_simple"] = simple
    except Exception as error:
        cell["outcome"] = "frame_error"
        cell.update(base.error_record(error))
        return cell
    try:
        if door in SCALAR_DOORS:
            result = RUNNERS[door](frame, schema, simple)
        else:
            result = RUNNERS[door](frame, schema)
        table = result.toArrow()
        cell["outcome"] = "answer"
        cell["result_arrow"] = base.scrub_text(str(table.schema))
        cell["ticks"] = result_ticks(table)
    except Exception as error:
        cell["outcome"] = "error"
        cell.update(base.error_record(error))
    return cell


def write_markdown(path: str, meta: dict[str, Any], cells: list[dict[str, Any]]) -> None:
    lines = [
        "## NS-ARRAY-FLOOR-1 Lane A echo cells",
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
    lines.append("| door | source | schema | outcome | ticks_or_error |")
    lines.append("|---|---|---|---|---|")
    for cell in cells:
        if cell["outcome"] == "answer":
            detail = repr(cell["schema_simple"]) + " ticks " + repr(cell["ticks"])[:220]
        else:
            detail = (
                cell.get("error", "")
                + " "
                + cell.get("condition", "")
                + " "
                + cell.get("first_line", "")[:200]
            )
        lines.append(
            "| "
            + cell["door"]
            + " | "
            + cell["source"]
            + " | "
            + repr(cell.get("schema_simple", ""))[:120]
            + " | "
            + cell["outcome"]
            + " | "
            + detail.replace("|", "/")[:280]
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
        spark = open_session("", True, args.safe_setting)
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
        sources = ["us", "ns_pandas"]
    else:
        warehouse = tempfile.mkdtemp(prefix="ns-array-floor-1-lane-a-")
        spark = open_session(warehouse, False)
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
        for door in DOORS:
            cells.append(measure_cell(spark, door, source, args.live))
    payload = {"meta": meta, "cells": cells}
    Path(args.out).write_text(json.dumps(payload, indent=1), encoding="utf-8")
    write_markdown(args.md, meta, cells)
    with contextlib.suppress(Exception):
        spark.stop()


if __name__ == "__main__":
    main()

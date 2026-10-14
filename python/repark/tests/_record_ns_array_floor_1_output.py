"""NS-ARRAY-FLOOR-1 D2: record what each door stores for a nanosecond output.

Every door that can return an array column returns datetime64[ns] values
under a declared array<timestamp> and array<timestamp_ntz>; a top-level
datetime64[ns] column under timestamp and timestamp_ntz is the control.
Result-frame ticks and read-back ticks are both recorded.

Usage with a repark module under test, then with live Spark:

    python _record_ns_array_floor_1_output.py --out e.json --md c.md
    JAVA_HOME=... SPARK_LOCAL_IP=127.0.0.1 python _record_ns_array_floor_1_output.py --live

pins: ns-array-floor-1/C-003, ns-array-floor-1/C-004, ns-array-floor-1/C-005
"""

from __future__ import annotations

import argparse
import contextlib
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

import pyarrow as pa

sys.path.insert(0, str(Path(__file__).parent))
import _record_ns_array_floor_1_input as base

ZONE = base.ZONE
SEED_PAIRS = [[1767323045123456789, 1767323045000000001], [1767323045000000001]]
SEED_TOP = [1767323045123456789, None, 1767323045000000001]
EXPECT_FLOOR_PAIRS = [[1767323045123456, 1767323045000000], [1767323045000000]]
EXPECT_FLOOR_TOP = [1767323045123456, None, 1767323045000000]

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

DECLARED = {
    ("array", "tz"): "v array<timestamp>",
    ("array", "ntz"): "v array<timestamp_ntz>",
    ("top", "tz"): "v timestamp",
    ("top", "ntz"): "v timestamp_ntz",
}


def ns_stamps(values: list[Any]) -> list[Any]:
    import pandas as pd

    return [pd.to_datetime(v, unit="ns") if v is not None else None for v in values]


def open_session(warehouse: str, live: bool) -> Any:
    if live:
        return base.open_live_session()
    return base.open_repark_session(warehouse)


def seed_frame(spark: Any, live: bool) -> Any:
    if live:
        rows = [(1, 1), (2, 0), (3, 1)]
        return spark.createDataFrame(rows, "id int, k int")
    spark._ensure_alive().register_arrow_stream_as_temp_view(
        "ns_out_seed",
        pa.table({"id": pa.array([1, 2, 3], pa.int32()), "k": pa.array([1, 0, 1], pa.int32())}),
    )
    return spark.table("ns_out_seed")


def datetime_to_micros(value: Any) -> int:
    import datetime

    if value.tzinfo is not None:
        value = value.astimezone(datetime.UTC).replace(tzinfo=None)
    delta = value - datetime.datetime(1970, 1, 1)
    return (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds


def tick_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    return datetime_to_micros(value)


def frame_ticks(table: Any) -> Any:
    column = table.column("v")
    if pa.types.is_list(column.type) or pa.types.is_large_list(column.type):
        out = []
        for row in column.to_pylist():
            if row is None:
                out.append(None)
            else:
                out.append([tick_value(v) for v in row])
        return out
    return [tick_value(v) for v in column.to_pylist()]


def store_roundtrip(spark: Any, frame: Any, ddl: str, live: bool) -> dict[str, Any]:
    record: dict[str, Any] = {}
    if live:
        record["stored_ticks"] = None
        record["store_note"] = "live cells read frame ticks only"
        return record
    name = "ns_out_" + str(next(store_roundtrip.counter))
    try:
        spark.sql(f"CREATE TABLE ice.ns.{name} (v {ddl}) USING iceberg")
        frame.select("v").writeTo(f"ice.ns.{name}").append()
        back = spark.sql(f"SELECT v FROM ice.ns.{name}").toArrow()
        record["stored_ticks"] = frame_ticks(back)
        record["stored_arrow"] = base.scrub_text(str(back.schema))
    except Exception as error:
        record["stored_ticks"] = None
        record["store_error"] = base.error_record(error)
    return record


store_roundtrip.counter = iter(range(1000000))


def run_grouped_applyin(frame: Any, kind: str, schema: str) -> Any:
    import pandas as pd

    if kind == "array":
        rows = ns_stamps([v for pair in SEED_PAIRS for v in pair])

        def func(pdf: Any) -> Any:
            return pd.DataFrame({"v": [rows]})

    else:
        series = pd.Series(ns_stamps(SEED_TOP), dtype="datetime64[ns]")

        def func(pdf: Any) -> Any:
            return pd.DataFrame({"v": series})

    return frame.groupBy("k").applyInPandas(func, schema)


def run_cogroup_applyin(frame: Any, kind: str, schema: str) -> Any:
    import pandas as pd

    other = frame

    if kind == "array":
        rows = ns_stamps([v for pair in SEED_PAIRS for v in pair])

        def func(first: Any, second: Any) -> Any:
            return pd.DataFrame({"v": [rows]})

    else:
        series = pd.Series(ns_stamps(SEED_TOP), dtype="datetime64[ns]")

        def func(first: Any, second: Any) -> Any:
            return pd.DataFrame({"v": series})

    grouped = frame.groupBy("k").cogroup(other.groupBy("k"))
    return grouped.applyInPandas(func, schema)


def run_grouped_applyinarrow(frame: Any, kind: str, schema: str) -> Any:
    if kind == "array":
        table = pa.table({"v": pa.array(SEED_PAIRS, type=pa.list_(pa.timestamp("ns")))})

        def func(batch: Any) -> Any:
            return table

    else:
        table = pa.table({"v": pa.array(SEED_TOP, type=pa.timestamp("ns"))})

        def func(batch: Any) -> Any:
            return table

    return frame.groupBy("k").applyInArrow(func, schema)


def run_cogroup_applyinarrow(frame: Any, kind: str, schema: str) -> Any:
    other = frame
    if kind == "array":
        table = pa.table({"v": pa.array(SEED_PAIRS, type=pa.list_(pa.timestamp("ns")))})

        def func(first: Any, second: Any) -> Any:
            return table

    else:
        table = pa.table({"v": pa.array(SEED_TOP, type=pa.timestamp("ns"))})

        def func(first: Any, second: Any) -> Any:
            return table

    grouped = frame.groupBy("k").cogroup(other.groupBy("k"))
    return grouped.applyInArrow(func, schema)


def run_mapinpan(frame: Any, kind: str, schema: str) -> Any:
    import pandas as pd

    if kind == "array":
        rows = ns_stamps([v for pair in SEED_PAIRS for v in pair])

        def func(batches: Any) -> Any:
            for _ in batches:
                pass
            yield pd.DataFrame({"v": [rows]})

    else:
        series = pd.Series(ns_stamps(SEED_TOP), dtype="datetime64[ns]")

        def func(batches: Any) -> Any:
            for _ in batches:
                pass
            yield pd.DataFrame({"v": series})

    return frame.mapInPandas(func, schema)


def run_mapinarrow(frame: Any, kind: str, schema: str) -> Any:
    if kind == "array":
        table = pa.table({"v": pa.array(SEED_PAIRS, type=pa.list_(pa.timestamp("ns")))})

        def func(batches: Any) -> Any:
            for _ in batches:
                pass
            yield from table.to_batches()

    else:
        table = pa.table({"v": pa.array(SEED_TOP, type=pa.timestamp("ns"))})

        def func(batches: Any) -> Any:
            for _ in batches:
                pass
            yield from table.to_batches()

    return frame.mapInArrow(func, schema)


def run_scalar_pandas_udf(frame: Any, kind: str, return_type: str) -> Any:
    import pandas as pd

    pandas_udf, _, _ = base.udf_libs()
    stamps = ns_stamps(SEED_TOP)
    first_two = stamps[:2]

    if kind == "array":

        def func(series: Any) -> Any:
            return pd.Series([first_two] * len(series))

    else:

        def func(series: Any) -> Any:
            return pd.Series((stamps * 2)[: len(series)], dtype="datetime64[ns]")

    udf = pandas_udf(func, return_type)
    return frame.select(udf("id").alias("v"))


def run_groupedagg_pandas_udf(frame: Any, kind: str, return_type: str) -> Any:
    pandas_udf, _, token = base.udf_libs()
    stamps = ns_stamps(SEED_TOP)

    if kind == "array":

        def func(series: Any) -> Any:
            return stamps[:2]

    else:

        def func(series: Any) -> Any:
            return stamps[0]

    udf = pandas_udf(func, return_type, token)
    return frame.groupBy("k").agg(udf("id").alias("v"))


def run_scalar_arrow_udf(frame: Any, kind: str, return_type: str) -> Any:
    _, arrow_udf, _ = base.udf_libs()

    if kind == "array":
        out = pa.array(
            [[1767323045123456789, 1767323045000000001]] * 3,
            type=pa.list_(pa.timestamp("ns")),
        )

        def func(array: Any) -> pa.Array:
            return out.slice(0, len(array))

    else:
        out = pa.array(SEED_TOP, type=pa.timestamp("ns"))

        def func(array: Any) -> pa.Array:
            return out.slice(0, len(array))

    udf = arrow_udf(func, return_type)
    return frame.select(udf("id").alias("v"))


RUNNERS = {
    "grouped_applyin": run_grouped_applyin,
    "cogroup_applyin": run_cogroup_applyin,
    "grouped_applyinarrow": run_grouped_applyinarrow,
    "cogroup_applyinarrow": run_cogroup_applyinarrow,
    "mapinpan": run_mapinpan,
    "mapinarrow": run_mapinarrow,
    "scalar_pandas_udf": run_scalar_pandas_udf,
    "groupedagg_pandas_udf": run_groupedagg_pandas_udf,
    "scalar_arrow_udf": run_scalar_arrow_udf,
}


def measure_cell(spark: Any, door: str, kind: str, declared: str, live: bool) -> dict[str, Any]:
    cell: dict[str, Any] = {"door": door, "kind": kind, "declared": declared}
    schema = DECLARED[(kind, declared)]
    return_type = schema.split(" ", 1)[1]
    cell["schema"] = schema
    try:
        frame = seed_frame(spark, live)
        if door in ("scalar_pandas_udf", "groupedagg_pandas_udf", "scalar_arrow_udf"):
            result = RUNNERS[door](frame, kind, return_type)
        else:
            result = RUNNERS[door](frame, kind, schema)
        table = result.toArrow()
        cell["outcome"] = "answer"
        cell["result_arrow"] = base.scrub_text(str(table.schema))
        cell["frame_ticks"] = frame_ticks(table)
        cell.update(store_roundtrip(spark, result, return_type, live))
    except Exception as error:
        cell["outcome"] = "error"
        cell.update(base.error_record(error))
    return cell


def write_markdown(path: str, meta: dict[str, Any], cells: list[dict[str, Any]]) -> None:
    lines = [
        "## NS-ARRAY-FLOOR-1 output cells",
        "",
        "Backend "
        + meta["label"]
        + " base "
        + meta["base_sha"]
        + " zone "
        + meta["zone"]
        + " live "
        + str(meta["live"])
        + ".",
        "",
    ]
    if meta.get("banner"):
        lines.append("Banner " + meta["banner"])
        lines.append("")
    lines.append("| door | kind | declared | outcome | ticks_or_error |")
    lines.append("|---|---|---|---|---|")
    for cell in cells:
        if cell["outcome"] == "answer":
            detail = "frame " + repr(cell["frame_ticks"])[:200]
            if cell.get("stored_ticks") is not None:
                detail += " stored " + repr(cell["stored_ticks"])[:200]
            elif cell.get("store_error"):
                detail += " store_error " + cell["store_error"]["error"]
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
            + cell["kind"]
            + " | "
            + cell["declared"]
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
    args = parser.parse_args()
    if args.live:
        spark = open_session("", True)
        meta: dict[str, Any] = {
            "label": args.label,
            "base_sha": args.base_sha,
            "zone": ZONE,
            "live": True,
            "banner": "spark "
            + str(spark.version)
            + " zone "
            + str(spark.conf.get("spark.sql.session.timeZone")),
        }
    else:
        warehouse = tempfile.mkdtemp(prefix="ns-array-floor-1-out-")
        spark = open_session(warehouse, False)
        meta = {
            "label": args.label,
            "base_sha": args.base_sha,
            "zone": ZONE,
            "live": False,
        }
    cells = []
    for door in DOORS:
        for kind in ("array", "top"):
            for declared in ("tz", "ntz"):
                cells.append(measure_cell(spark, door, kind, declared, args.live))
    payload = {"meta": meta, "cells": cells}
    Path(args.out).write_text(json.dumps(payload, indent=1), encoding="utf-8")
    write_markdown(args.md, meta, cells)
    with contextlib.suppress(Exception):
        spark.stop()


if __name__ == "__main__":
    main()

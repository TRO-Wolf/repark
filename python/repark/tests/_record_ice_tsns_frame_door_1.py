"""ICE-TSNS-FRAME-DOOR-1: record what Spark answers for a pandas nanosecond timestamp frame."""

from __future__ import annotations

import argparse
import json
import tempfile
import time
import warnings
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa

ZONES = ("UTC", "America/New_York")
SCHEMAS = (None, "c timestamp", "c timestamp_ntz")
SEED = "2026-01-02 03:04:05.123456789"
SEED_NANOS = 1767323045123456789
SEED_MICROS = 1767323045123456
PRE_EPOCH_NANOS = (-1, -1001, -999_999_999_999)
PRE_EPOCH_MICROS = (-1, -2, -1_000_000_000)
AWARE_SOURCES = ("pd_utc_ns", "pd_ny_ns", "pd_utc_ns_pre_epoch", "pd_utc_ns_notnull")
NAIVE_SOURCES = ("pd_naive_ns", "pd_naive_ns_pre_epoch", "pd_arrowdtype_ns")
MICROSECOND_SOURCES = ("pd_ny_us", "pd_tokyo_us", "pd_offset_us")
CATEGORY_SOURCES = ("pd_category_ns", "pd_category_utc_ns")
DISCLOSURE_SOURCES = ("pd_ny_ns_transitions", "pd_utc_ns_extremes", "pd_naive_ns_extremes")
EXTREME_NANOS = (-9_223_372_036_854_775_807, 9_223_372_036_854_775_807)
EXTREME_MICROS = (-9_223_372_036_854_776, 9_223_372_036_854_775)
TRANSITION_INSTANTS = {
    "America/New_York": (
        "2026-11-01 05:30:00.000000001",
        "2026-11-01 06:30:00.000000001",
        "2026-11-01 05:30:00.000001000",
        "2026-03-08 06:59:59.999999999",
        "2026-03-08 07:00:00.000000001",
    ),
    "Europe/London": (
        "2026-10-25 00:30:00.000000001",
        "2026-10-25 01:30:00.000000001",
        "2026-10-25 00:30:00.000001000",
        "2026-03-29 00:59:59.999999999",
        "2026-03-29 01:00:00.000000001",
    ),
    "Australia/Lord_Howe": (
        "2026-04-04 14:45:00.000000001",
        "2026-04-04 15:15:00.000000001",
        "2026-04-04 14:45:00.000001000",
        "2026-10-03 15:29:59.999999999",
        "2026-10-03 15:30:00.000000001",
    ),
}


def transition_frame(zone: str) -> pd.DataFrame:
    """Return the nanosecond instants around both transitions of ``zone``, aware in ``zone``."""
    instants = pd.Series(pd.to_datetime(list(TRANSITION_INSTANTS[zone]), format="ISO8601"))
    return pd.DataFrame(
        {"c": instants.astype("datetime64[ns]").dt.tz_localize("UTC").dt.tz_convert(zone)}
    )


def transition_nanos(zone: str) -> list[int]:
    """Return the int64 nanoseconds of the instants of ``transition_frame(zone)``."""
    return [int(value) for value in transition_frame(zone)["c"].astype("int64")]


def sources() -> dict[str, pd.DataFrame]:
    """Return every pandas frame of the matrix, keyed by its cell name."""
    naive = pd.Series(pd.to_datetime([SEED, None])).astype("datetime64[ns]")
    aware = naive.dt.tz_localize("UTC")
    pre_epoch = pd.Series(np.array(PRE_EPOCH_NANOS, dtype="int64")).astype("datetime64[ns]")
    extremes = pd.Series(np.array(EXTREME_NANOS, dtype="int64")).astype("datetime64[ns]")
    arrow_naive = pa.timestamp("ns")
    return {
        "pd_naive_ns": pd.DataFrame({"c": naive}),
        "pd_utc_ns": pd.DataFrame({"c": aware}),
        "pd_ny_ns": pd.DataFrame({"c": aware.dt.tz_convert("America/New_York")}),
        "pd_naive_ns_pre_epoch": pd.DataFrame({"c": pre_epoch}),
        "pd_utc_ns_pre_epoch": pd.DataFrame({"c": pre_epoch.dt.tz_localize("UTC")}),
        "pd_arrowdtype_ns": pd.DataFrame(
            {
                "c": pd.Series(
                    pa.array([SEED_NANOS, None], type=arrow_naive),
                    dtype=pd.ArrowDtype(arrow_naive),
                )
            }
        ),
        "pd_utc_ns_notnull": pd.DataFrame({"c": aware[:1]}),
        "pd_ny_us": pd.DataFrame(
            {"c": aware.dt.floor("us").astype(pd.DatetimeTZDtype("us", "America/New_York"))}
        ),
        "pd_tokyo_us": pd.DataFrame(
            {"c": aware.dt.floor("us").astype(pd.DatetimeTZDtype("us", "Asia/Tokyo"))}
        ),
        "pd_offset_us": pd.DataFrame(
            {"c": aware.dt.floor("us").astype(pd.DatetimeTZDtype("us", "+05:30"))}
        ),
        "pd_category_ns": pd.DataFrame({"c": naive.astype("category")}),
        "pd_category_utc_ns": pd.DataFrame({"c": aware.astype("category")}),
        "pd_ny_ns_transitions": transition_frame("America/New_York"),
        "pd_utc_ns_extremes": pd.DataFrame({"c": extremes.dt.tz_localize("UTC")}),
        "pd_naive_ns_extremes": pd.DataFrame({"c": extremes}),
    }


def cell_key(zone: str, source: str, schema: str | None) -> str:
    """Return the fixture key of one cell."""
    return f"{zone}|{source}|{schema or 'none'}"


def open_spark(zone: str, warehouse: Path) -> Any:
    """Open a local Spark session in ``zone`` over a private warehouse."""
    from pyspark.sql import SparkSession

    prior = SparkSession.getActiveSession()
    if prior is not None:
        prior.stop()
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("ice-tsns-frame-door-1-oracle")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.session.timeZone", zone)
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def one_cell(spark: Any, frame: pd.DataFrame, schema: str | None) -> dict[str, Any]:
    """Return the Arrow type, nullability, int64 values and warnings of one createDataFrame."""
    cell: dict[str, Any] = {}
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            made = (
                spark.createDataFrame(frame)
                if schema is None
                else spark.createDataFrame(frame, schema=schema)
            )
            field = made.schema.fields[0]
            table = made.toArrow()
            arrow_field = table.schema.field("c")
            cell["simple"] = field.dataType.simpleString()
            cell["arrow"] = str(arrow_field.type)
            cell["nullable"] = bool(field.nullable and arrow_field.nullable)
            cell["ints"] = table.column("c").combine_chunks().cast(pa.int64()).to_pylist()
            cell["status"] = "ok"
        except Exception as error:
            text = str(error).strip().splitlines()
            cell["status"] = "error"
            cell["error"] = f"{type(error).__name__}: {(text[0] if text else '')[:260]}"
        cell["warnings"] = sorted(
            {
                f"{warning.category.__name__}: {str(warning.message)[:120]}"
                for warning in caught
                if warning.category.__name__ != "ResourceWarning"
            }
        )
    return cell


def main() -> None:
    """Write the oracle JSON."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    warehouse = Path(tempfile.mkdtemp(prefix="ice-tsns-frame-door-1-"))
    cells: dict[str, Any] = {}
    version = ""
    for zone in ZONES:
        spark = open_spark(zone, warehouse)
        version = version or spark.version
        for source, frame in sources().items():
            for schema in SCHEMAS:
                cells[cell_key(zone, source, schema)] = one_cell(spark, frame, schema)
        spark.stop()
    payload = {
        "spark": version,
        "pandas": pd.__version__,
        "pyarrow": pa.__version__,
        "driver_zone": "/".join(time.tzname),
        "zones": list(ZONES),
        "cells": cells,
    }
    args.output.write_text(json.dumps(payload, indent=1) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()

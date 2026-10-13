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
MICROSECOND_SOURCES = ("pd_ny_us",)


def sources() -> dict[str, pd.DataFrame]:
    """Return every nanosecond pandas frame of the matrix, keyed by its cell name."""
    naive = pd.Series(pd.to_datetime([SEED, None])).astype("datetime64[ns]")
    aware = naive.dt.tz_localize("UTC")
    pre_epoch = pd.Series(np.array(PRE_EPOCH_NANOS, dtype="int64")).astype("datetime64[ns]")
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

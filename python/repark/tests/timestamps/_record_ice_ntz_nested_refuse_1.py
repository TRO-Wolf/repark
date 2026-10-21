"""ICE-NTZ-NESTED-REFUSE-1: record what Spark 4.1.2 stores through the nested NTZ doors.

Run with the ``record`` extra, a JVM and ``REPARK_PARITY_LIVE=1``. Writes
``ice_ntz_nested_refuse_1_spark.json`` beside this file.
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import _ice_ntz_nested_refuse_1_doors as doors
import _live_parity as live

CATALOG = "ntzr_record"
OUTPUT = Path(__file__).with_name("ice_ntz_nested_refuse_1_spark.json")


def record(warehouse: Path) -> dict[str, Any]:
    """Run every cell on live Spark in every zone and return the cells by key."""
    spark = live.build_spark_iceberg_engine(warehouse, catalog=CATALOG).session
    recorded: dict[str, Any] = {}
    try:
        doors.seed(spark, CATALOG)
        index = 0
        for zone in doors.ZONES:
            spark.conf.set("spark.sql.session.timeZone", zone)
            for door, source in doors.cells():
                index += 1
                table = f"{CATALOG}.ns.t{index}"
                recorded[f"{zone}|{door}|{source}"] = doors.run_cell(
                    spark, CATALOG, door, source, table
                )
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
    return recorded


def main() -> None:
    """Record the cells and write the JSON fixture."""
    with tempfile.TemporaryDirectory() as warehouse:
        cells = record(Path(warehouse))
    payload = {"spark": "4.1.2", "iceberg": "1.11.0", "cells": cells}
    OUTPUT.write_text(json.dumps(payload, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"{len(cells)} cells")


if __name__ == "__main__":
    main()

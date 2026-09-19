"""Record or re-check the ICE-OVERWRITE-MODE-1 transform-spec cells on live PySpark 4.1.2.

Usage (PySpark 4.1.2 interpreter, e.g. ``/tmp/sparkenv/bin/python``)::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
        /tmp/sparkenv/bin/python python/repark/tests/_record_ice_overwrite_mode_1_transform.py \
        --warehouse /tmp/owp-transform-wh record
    ... check

Each cell seeds ``(1,a,x),(2,b,y),(3,c,x)`` into a table whose spec holds a transform, runs
``writeTo(t).overwritePartitions()`` with the frame ``(1,z,x)`` in the static and the dynamic
session mode, and records the sorted rows and the last snapshot's operation. ``record`` prints
the fixture; ``check`` exits non-zero on the first cell that differs from
``ice_overwrite_mode_1_transform_spark_oracle.json``.

pins: ice-overwrite-mode-1/C-018
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

FIXTURE = Path(__file__).with_name("ice_overwrite_mode_1_transform_spark_oracle.json")
ICEBERG_SPARK_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
SPECS: dict[str, str] = {
    "ident_bucket": "PARTITIONED BY (cat, bucket(2, id))",
    "bucket_only": "PARTITIONED BY (bucket(4, id))",
    "truncate_cat": "PARTITIONED BY (truncate(1, cat))",
    "unpartitioned": "",
}
MODES: tuple[str, ...] = ("static", "dynamic")


def build_session(warehouse: Path) -> Any:
    """Build a local PySpark session with an InMemory Iceberg catalog ``sc``."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("record-ice-overwrite-mode-1-transform")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config("spark.sql.extensions", ICEBERG_SPARK_EXTENSIONS)
        .config("spark.sql.catalog.sc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.sc.catalog-impl", "org.apache.iceberg.inmemory.InMemoryCatalog")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return session


def record_cells(warehouse: Path) -> dict[str, Any]:
    """Run every cell on live Spark and return the fixture document."""
    shutil.rmtree(warehouse, ignore_errors=True)
    session = build_session(warehouse)
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns")
    cells: dict[str, Any] = {}
    for spec_name, spec in SPECS.items():
        for mode in MODES:
            for version in (2, 3):
                key = f"{spec_name}_{mode}_v{version}"
                table = f"sc.ns.{key}"
                session.conf.set("spark.sql.sources.partitionOverwriteMode", mode)
                session.sql(
                    f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg "
                    f"{spec} TBLPROPERTIES ('format-version'='{version}')"
                )
                session.sql(
                    f"INSERT INTO {table} VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')"
                )
                frame = session.createDataFrame(
                    [(1, "z", "x")], "id BIGINT, data STRING, cat STRING"
                )
                frame.writeTo(table).overwritePartitions()
                rows = session.sql(f"SELECT * FROM {table} ORDER BY id, data").collect()
                last = session.sql(
                    f"SELECT operation FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
                ).collect()
                cells[key] = {
                    "spec": spec,
                    "mode": mode,
                    "version": version,
                    "rows": [list(row) for row in rows],
                    "last_operation": last[0][0],
                }
    session.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
    banner = session.version
    session.stop()
    return {"banner": banner, "runtime": ICEBERG_SPARK_RUNTIME_GAV, "cells": cells}


def main() -> int:
    """Record the fixture to stdout, or check it against the committed file."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("mode", choices=("record", "check"))
    args = parser.parse_args()
    document = record_cells(args.warehouse)
    if args.mode == "record":
        print(json.dumps(document, indent=1))
        return 0
    committed = json.loads(FIXTURE.read_text(encoding="utf-8"))
    for key, cell in committed["cells"].items():
        if document["cells"].get(key) != cell:
            print(f"mismatch in {key}: {document['cells'].get(key)} != {cell}")
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

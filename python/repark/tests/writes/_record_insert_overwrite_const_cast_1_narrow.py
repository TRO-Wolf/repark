from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _insert_overwrite_const_cast_1_narrow_cells import build_cells, guarded, run_cell, setup

FIXTURE = Path(__file__).with_name("insert_overwrite_const_cast_1_narrow_spark_oracle.json")
NAMESPACE = "ice.ioc"


def open_session(engine: str, warehouse: str, iceberg_jar: str | None) -> tuple[Any, Any]:
    if engine == "spark":
        from pyspark.sql import SparkSession, functions

        builder = (
            SparkSession.builder.master("local[2]")
            .appName("insert-overwrite-const-cast-1-narrow")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.ice.type", "hadoop")
            .config("spark.sql.catalog.ice.warehouse", warehouse)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
        )
        if iceberg_jar:
            builder = builder.config("spark.jars", iceberg_jar)
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("OFF")
        return spark, functions
    import repark._native
    from repark.spark.sql import SparkSession, functions

    print(f"REPARK_NATIVE={repark._native.__file__}", file=sys.stderr)
    spark = (
        SparkSession.builder.appName("insert-overwrite-const-cast-1-narrow")
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    return spark, functions


def record(engine: str, warehouse: str, iceberg_jar: str | None, out: Path) -> None:
    spark, functions = open_session(engine, warehouse, iceberg_jar)
    setup(spark, NAMESPACE)
    results = {
        key: run_cell(spark, functions, NAMESPACE, key, cell) for key, cell in build_cells().items()
    }
    out.write_text(json.dumps(results, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"{engine}: {len(results)} cells -> {out}")


def combine(spark_path: Path, main_path: Path) -> None:
    spark = json.loads(spark_path.read_text(encoding="utf-8"))
    main = json.loads(main_path.read_text(encoding="utf-8"))
    cells = {
        key: {
            "spark": spark[key],
            "main": main[key],
            "expect": "spark" if guarded(key, cell) else "main",
        }
        for key, cell in build_cells().items()
    }
    FIXTURE.write_text(
        json.dumps({"cells": cells}, indent=1, sort_keys=True) + "\n", encoding="utf-8"
    )
    moved = sum(
        1 for cell in cells.values() if cell["expect"] == "spark" and cell["spark"] != cell["main"]
    )
    print(f"{len(cells)} cells, {moved} red on main -> {FIXTURE}")


def main() -> int:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("record")
    run.add_argument("--engine", choices=("spark", "repark"), required=True)
    run.add_argument("--warehouse", required=True)
    run.add_argument("--iceberg-jar")
    run.add_argument("--out", type=Path, required=True)
    merge = commands.add_parser("combine")
    merge.add_argument("--spark", type=Path, required=True)
    merge.add_argument("--main", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "record":
        record(args.engine, args.warehouse, args.iceberg_jar, args.out)
    else:
        combine(args.spark, args.main)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

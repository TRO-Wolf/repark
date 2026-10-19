"""Record Spark 4.1.2's answers for ICE-ROWID-ORDER-1: v3 row-id assignment order.

NOT a ``test_`` module: pytest never collects it. It runs two recordings on live
PySpark and writes them into
``python/repark-parity/fixtures/torture/data/ice_rowid_order_1/``: the a/b/c
recording (twelve runs of ``INSERT INTO t SELECT`` / literal ``VALUES`` / CTAS on
a v3 table partitioned by ``cat``, Hadoop and InMemory catalogs) goes to
``spark_rowid_abc_oracle.json``, and the eight-category recording (six runs per
configuration of ``INSERT INTO t SELECT`` over ``d, a, z, m, b, q, c, x``) goes
to ``spark_rowid_order_oracle.json``. Each run records partition to
``min(_row_id)`` (a/b/c) or the ``(partition, record_count, first_row_id)`` file
listing in ``first_row_id`` order (eight categories).

The a/b/c file also carries the draft recorder's ``*_16_concurrent_inserts``
storm cells, which belong to ICE-APPEND-RETRY-1 and are NOT reproduced here (see
``ice_occ_scoped_1/spark_occ_oracle4.json``). The order recording reproduces all
twelve configurations through :func:`record_order_cell`; pass ``fast=True`` to
re-record only the default configuration (adaptive on, 400 rows, hash
distribution), the cell the pins read.

Run with a PySpark 4.1.2 interpreter that can resolve the Iceberg runtime GAV from
``_oracle_pins``; ``REPARK_ORACLE_IVY`` optionally points ``spark.jars.ivy`` at a warm
cache::

    _record_ice_rowid_order_1.py [warehouse] [--fast]

``warehouse`` is created fresh (removed first) and defaults to env-driven scratch.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

FIXTURE_DIR = (
    Path(__file__).resolve().parents[2] / "repark-parity/fixtures/torture/data/ice_rowid_order_1"
)
ABC_FILE = FIXTURE_DIR / "spark_rowid_abc_oracle.json"
ORDER_FILE = FIXTURE_DIR / "spark_rowid_order_oracle.json"
ABC_RUNS = 12
ORDER_RUNS = 6
ABC_CATS = ("a", "b", "c")
ORDER_CATS = ("d", "a", "z", "m", "b", "q", "c", "x")
ORDER_ROWS = (400, 200000)
ORDER_MODES = ("hash", "none", "range")


def start_spark(warehouse: Path, aqe: bool, shuffle: str) -> Any:
    """Start one short-lived local Spark session with Hadoop and InMemory catalogs.

    Args:
        warehouse: The warehouse directory the Hadoop catalog serves.
        aqe: Whether adaptive query execution stays enabled.
        shuffle: The ``spark.sql.shuffle.partitions`` value.

    Returns:
        The PySpark session.
    """
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[8]")
        .appName("ice-rowid-order-1-oracle")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.sc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse / "wh"))
        .config("spark.sql.catalog.mc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.mc.catalog-impl", "org.apache.iceberg.inmemory.InMemoryCatalog")
        .config("spark.sql.catalog.mc.warehouse", str(warehouse / "mwh"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", shuffle)
        .config("spark.sql.adaptive.enabled", "true" if aqe else "false")
    )
    ivy = os.environ.get("REPARK_ORACLE_IVY")
    if ivy:
        builder = builder.config("spark.jars.ivy", ivy)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    return session


def stop_spark(spark: Any) -> None:
    """Stop the session and clear the active context so a second session can start.

    Args:
        spark: The live PySpark session.
    """
    spark.stop()
    from pyspark import SparkContext

    SparkContext._active_spark_context = None


def record_abc_cell(spark: Any, catalog: str, kind: str) -> dict[str, Any]:
    """Run one a/b/c shape twelve times and record every run's partition mapping.

    Args:
        spark: The live PySpark session with the ``s1`` source view registered.
        catalog: ``sc`` (Hadoop) or ``mc`` (InMemory).
        kind: ``select``, ``values`` or ``ctas``.

    Returns:
        The cell record with ``runs`` and the ``distinct`` repetition counts.
    """
    runs: list[Any] = []
    for index in range(ABC_RUNS):
        if kind == "select":
            table = f"{catalog}.ns.o{index}"
            spark.sql(
                f"CREATE TABLE {table} (id BIGINT, cat STRING, v DOUBLE) USING iceberg "
                "PARTITIONED BY (cat) TBLPROPERTIES ('format-version'='3')"
            )
            spark.sql(f"INSERT INTO {table} SELECT id, cat, v FROM s1")
        elif kind == "values":
            table = f"{catalog}.ns.v{index}"
            spark.sql(
                f"CREATE TABLE {table} (id BIGINT, cat STRING) USING iceberg "
                "PARTITIONED BY (cat) TBLPROPERTIES ('format-version'='3')"
            )
            spark.sql(
                f"INSERT INTO {table} VALUES (1, 'b'), (2, 'a'), (3, 'c'), "
                "(4, 'a'), (5, 'b'), (6, 'c')"
            )
        else:
            table = f"{catalog}.ns.c{index}"
            spark.sql(
                f"CREATE TABLE {table} USING iceberg PARTITIONED BY (cat) "
                "TBLPROPERTIES ('format-version'='3') AS SELECT id, cat, v FROM s1"
            )
        runs.append(
            sorted(
                [
                    list(row)
                    for row in spark.sql(
                        f"SELECT cat, min(_row_id) FROM {table} GROUP BY cat"
                    ).collect()
                ]
            )
        )
    distinct: dict[str, int] = {}
    for run in runs:
        distinct[json.dumps(run)] = distinct.get(json.dumps(run), 0) + 1
    return {"runs": runs, "distinct": distinct}


def record_abc(spark: Any) -> dict[str, Any]:
    """Record the eight a/b/c cells on the live session.

    Args:
        spark: The live PySpark session.

    Returns:
        The ``rowid_*`` cell mapping for the a/b/c oracle file.
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS mc.ns")
    spark.range(300).createOrReplaceTempView("r1")
    spark.sql(
        "SELECT id, CASE WHEN id % 3 = 0 THEN 'a' WHEN id % 3 = 1 THEN 'b' "
        "ELSE 'c' END AS cat, CAST(id AS DOUBLE) AS v FROM r1"
    ).createOrReplaceTempView("s1")
    cells: dict[str, Any] = {}
    for catalog in ("sc", "mc"):
        for kind in ("select", "values", "ctas"):
            cells[f"rowid_{catalog}_{kind}"] = record_abc_cell(spark, catalog, kind)
            print(catalog, kind, cells[f"rowid_{catalog}_{kind}"]["distinct"], flush=True)
        first = f"{catalog}.ns.o0"
        cells[f"rowid_{catalog}_select_files"] = [
            list(row)
            for row in spark.sql(
                f"SELECT partition.cat, record_count, first_row_id FROM {first}.files "
                "ORDER BY first_row_id"
            ).collect()
        ]
    return cells


def record_order_cell(spark: Any, rows: int, mode: str) -> dict[str, Any]:
    """Run one eight-category configuration six times and record every file listing.

    Args:
        spark: The live PySpark session with the ``src`` source view registered.
        rows: The source row count, 400 or 200000.
        mode: The ``write.distribution-mode``, ``hash``, ``none`` or ``range``.

    Returns:
        The cell record with ``distinct_file_orders`` and ``runs``.
    """
    runs: list[Any] = []
    for index in range(ORDER_RUNS):
        table = f"sc.ns.t_{rows}_{mode}_{index}"
        spark.sql(
            f"CREATE TABLE {table} (id BIGINT, cat STRING, v DOUBLE) USING iceberg "
            f"PARTITIONED BY (cat) TBLPROPERTIES ('format-version'='3', "
            f"'write.distribution-mode'='{mode}')"
        )
        spark.sql(f"INSERT INTO {table} SELECT id, cat, v FROM src")
        runs.append(
            [
                [row.cat, row.record_count, row.first_row_id]
                for row in spark.sql(
                    f"SELECT partition.cat AS cat, record_count, first_row_id "
                    f"FROM {table}.files ORDER BY first_row_id"
                ).collect()
            ]
        )
    orders = sorted({json.dumps([entry[0] for entry in run]) for run in runs})
    return {"distinct_file_orders": orders, "runs": runs}


def record_order(spark: Any, aqe: bool, fast: bool) -> dict[str, Any]:
    """Record the eight-category cells for one adaptive setting on the live session.

    Args:
        spark: The live PySpark session.
        aqe: The adaptive setting this session runs under.
        fast: When true, record only the default configuration (400 rows, hash).

    Returns:
        The cell mapping keyed ``aqe=<bool> rows=<n> dist=<mode>``.
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns")
    case = " ".join(
        f"WHEN id % {len(ORDER_CATS)} = {index} THEN '{cat}'"
        for index, cat in enumerate(ORDER_CATS)
    )
    cells: dict[str, Any] = {}
    row_counts = (400,) if fast else ORDER_ROWS
    for rows in row_counts:
        spark.range(rows).selectExpr(
            "id", f"CASE {case} END AS cat", "CAST(id AS DOUBLE) AS v"
        ).createOrReplaceTempView("src")
        for mode in ORDER_MODES:
            key = f"aqe={aqe} rows={rows} dist={mode}"
            cells[key] = record_order_cell(spark, rows, mode)
            print(key, cells[key]["distinct_file_orders"], flush=True)
    return cells


def main(warehouse: Path | None = None, fast: bool = False) -> None:
    """Record both row-id recordings and write the two fixture oracle files.

    Args:
        warehouse: The scratch warehouse root, created fresh. Defaults to
            env-driven scratch when omitted.
        fast: When true, record only the default order configuration.
    """
    root = warehouse or Path(tempfile.mkdtemp(prefix="repark-ice-rowid-order-1-"))
    if root.exists():
        shutil.rmtree(root)
    spark = start_spark(root, True, "4")
    abc_cells = record_abc(spark)
    truth = {
        "spark_version": spark.version,
        "iceberg_runtime": ICEBERG_SPARK_RUNTIME_GAV,
        "shuffle_partitions": "4",
        "cells": abc_cells,
    }
    stop_spark(spark)
    order_cells: dict[str, Any] = {}
    order_spark = truth["spark_version"]
    for aqe in (True, False):
        session = start_spark(root, aqe, "4")
        order_spark = session.version
        order_cells.update(record_order(session, aqe, fast))
        stop_spark(session)
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    ABC_FILE.write_text(json.dumps(truth, indent=1), encoding="utf-8")
    ORDER_FILE.write_text(
        json.dumps(
            {"cats": list(ORDER_CATS), "cells": order_cells, "spark": order_spark}, indent=1
        ),
        encoding="utf-8",
    )
    shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main(
        Path(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1] != "--fast" else None,
        "--fast" in sys.argv,
    )

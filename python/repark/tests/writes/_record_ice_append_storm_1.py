"""Record the ICE-APPEND-RETRY-1 insert-storm Spark oracle.

Runs sixteen barrier-released single-row inserts against one fresh unpartitioned
table per repetition, over six repetitions per catalog and format version, and
writes the normalized cells into ``spark_occ_oracle4.json``. Scratch warehouse
prefixes in Spark error text become the literal ``<warehouse>/`` before the
write, so the committed file carries no scratch paths.

Run with a PySpark 4.1.2 interpreter that resolves
``ICEBERG_SPARK_RUNTIME_GAV``; ``REPARK_ORACLE_IVY`` optionally points
``spark.jars.ivy`` at a warm cache. The warehouse is a fresh temp dir.

Not collected by pytest.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
import tempfile
import threading
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "repark-parity"
    / "fixtures"
    / "torture"
    / "data"
    / "ice_occ_scoped_1"
    / "spark_occ_oracle4.json"
)
EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
HADOOP_CATALOG = "sc"
MEMORY_CATALOG = "mc"
NAMESPACE = "ns"
CATALOG_CELLS = ((HADOOP_CATALOG, "hadoop"), (MEMORY_CATALOG, "inmemory"))
FORMAT_VERSIONS = ("2", "3")
REPETITIONS = 6
STATEMENT_COUNT = 16
WAREHOUSE_TOKEN = "<warehouse>/"


def spark_session(warehouse: Path) -> Any:
    """Start one local Spark session with Hadoop and InMemory Iceberg catalogs.

    Args:
        warehouse: The scratch root; the Hadoop catalog lives under ``wh`` and
            the InMemory catalog under ``mwh``.

    Returns:
        The live PySpark session.
    """
    from pyspark.sql import SparkSession

    hadoop = f"spark.sql.catalog.{HADOOP_CATALOG}"
    memory = f"spark.sql.catalog.{MEMORY_CATALOG}"
    builder = (
        SparkSession.builder.master("local[8]")
        .appName("repark-ice-append-storm-1-record")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config("spark.sql.extensions", EXTENSIONS)
        .config(hadoop, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{hadoop}.type", "hadoop")
        .config(f"{hadoop}.warehouse", str(warehouse / "wh"))
        .config(memory, "org.apache.iceberg.spark.SparkCatalog")
        .config(
            f"{memory}.catalog-impl",
            "org.apache.iceberg.inmemory.InMemoryCatalog",
        )
        .config(f"{memory}.warehouse", str(warehouse / "mwh"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
    )
    ivy = os.environ.get("REPARK_ORACLE_IVY")
    if ivy:
        builder = builder.config("spark.jars.ivy", ivy)
    return builder.getOrCreate()


def shorten_error(error: Exception) -> str:
    """Return the first Iceberg exception line of a failed statement, truncated.

    Args:
        error: The exception PySpark raised for one concurrent insert.

    Returns:
        The Iceberg exception head, or the second message line when the error
        carries no Iceberg class, capped at 200 characters.
    """
    match = re.search(r"(org\.apache\.iceberg\.exceptions\.\w+: [^\n]*)", str(error))
    if match is not None:
        return match.group(1)[:200]
    text = str(error)
    first = text.split("\n")[1] if "\n" in text else text
    return first[:200]


def run_statement(
    barrier: threading.Barrier,
    session: Any,
    statement: str,
    outcomes: list[str | None],
    index: int,
) -> None:
    """Release on the barrier with the other writers, then run one statement.

    Args:
        barrier: The barrier the sixteen writers wait on together.
        session: The shared PySpark session.
        statement: The single-row insert to run.
        outcomes: The per-index outcome slots this worker writes into.
        index: This worker's slot in ``outcomes``.
    """
    barrier.wait()
    try:
        session.sql(statement)
        outcomes[index] = "ok"
    except Exception as error:
        outcomes[index] = shorten_error(error)


def storm_repetition(
    spark: Any, catalog: str, format_version: str, repetition: int
) -> dict[str, Any]:
    """Run sixteen concurrent inserts on a fresh table and record the outcome.

    Args:
        spark: The live PySpark session.
        catalog: The catalog holding the storm table.
        format_version: The Iceberg format version, ``"2"`` or ``"3"``.
        repetition: The repetition index, carried in the table name so every
            repetition races on a fresh table.

    Returns:
        The repetition cell with ``committed``, ``of``, ``errors`` and
        ``rows_after`` (row count, distinct ids, snapshots).
    """
    table = f"{catalog}.{NAMESPACE}.ins{format_version}_{repetition}"
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, w INT) USING iceberg "
        f"TBLPROPERTIES ('format-version'='{format_version}')"
    )
    statements = [
        f"INSERT INTO {table} VALUES ({value}, {value})" for value in range(STATEMENT_COUNT)
    ]
    barrier = threading.Barrier(len(statements))
    outcomes: list[str | None] = [None] * len(statements)
    threads = [
        threading.Thread(target=run_statement, args=(barrier, spark, statement, outcomes, index))
        for index, statement in enumerate(statements)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    committed = sum(1 for outcome in outcomes if outcome == "ok")
    failures = [outcome for outcome in outcomes if outcome is not None and outcome != "ok"]
    errors = sorted({failure.split("\n")[0][:160] for failure in failures})
    counts = spark.sql(f"SELECT count(*), count(DISTINCT id) FROM {table}").collect()[0]
    snapshots = spark.sql(f"SELECT count(*) FROM {table}.snapshots").collect()[0][0]
    return {
        "committed": committed,
        "of": len(statements),
        "errors": errors,
        "rows_after": [counts[0], counts[1], snapshots],
    }


def normalize_prefix(text: str, warehouse: Path) -> str:
    """Replace the scratch warehouse prefixes in error text with the file token.

    Args:
        text: One recorded Spark error line.
        warehouse: The scratch root whose catalog prefixes become the token.

    Returns:
        The error line with both catalog prefixes replaced.
    """
    root = str(warehouse)
    return text.replace(f"{root}/wh/", WAREHOUSE_TOKEN).replace(f"{root}/mwh/", WAREHOUSE_TOKEN)


def main() -> None:
    """Record every repetition on both catalogs and write the fixture."""
    warehouse = Path(tempfile.mkdtemp(prefix="ice-append-storm-1-"))
    spark = spark_session(warehouse)
    try:
        spark.sparkContext.setLogLevel("ERROR")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {HADOOP_CATALOG}.{NAMESPACE}")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {MEMORY_CATALOG}.{NAMESPACE}")
        cells: dict[str, list[dict[str, Any]]] = {}
        for catalog, cell in CATALOG_CELLS:
            for format_version in FORMAT_VERSIONS:
                label = f"{cell}_v{format_version}_16_concurrent_inserts"
                repetitions = []
                for repetition in range(REPETITIONS):
                    outcome = storm_repetition(spark, catalog, format_version, repetition)
                    outcome["errors"] = [
                        normalize_prefix(text, warehouse) for text in outcome["errors"]
                    ]
                    repetitions.append(outcome)
                    print(label, outcome["committed"], "of", outcome["of"], flush=True)
                cells[label] = repetitions
        oracle = {
            "spark_version": spark.version,
            "iceberg_runtime": ICEBERG_SPARK_RUNTIME_GAV,
            "cells": cells,
        }
        FIXTURE.write_text(json.dumps(oracle, indent=1) + "\n", encoding="utf-8")
        print(f"wrote {FIXTURE}", flush=True)
    finally:
        spark.stop()
        shutil.rmtree(warehouse, ignore_errors=True)


if __name__ == "__main__":
    main()

"""Record or re-check the ICE-RTAS-BYNAME-1 Spark oracle cells on live PySpark 4.1.2.

Usage (PySpark 4.1.2 interpreter, e.g. ``/tmp/sparkenv/bin/python``)::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
        /tmp/sparkenv/bin/python python/repark/tests/_record_ice_rtas_byname_1_oracle.py \
        --warehouse /tmp/byname-oracle-wh record
    ... check

``record`` prints the fixture JSON to stdout; ``check`` re-derives every cell
and exits non-zero naming the first mismatch against the committed
``ice_rtas_byname_1_spark_oracle.json`` after normalizing the two
run-stamped summary keys (``app-id``, ``spark.app.id``). The Iceberg runtime
GAV comes from :mod:`_oracle_pins` (CP-8: never restate a version literal).

pins: ice-rtas-byname-1/C-001, C-002, C-003, C-005
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

FIXTURE = Path(__file__).with_name("ice_rtas_byname_1_spark_oracle.json")
VOLATILE_SUMMARY_KEYS = ("app-id", "spark.app.id")
ICEBERG_SPARK_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

BYNAME_SEQUENCE: tuple[tuple[str, str], ...] = (
    ("by_name", "INSERT INTO sc.ns.bn BY NAME SELECT * FROM swapped"),
    ("by_name_subset", "INSERT INTO sc.ns.bn BY NAME SELECT 'Bo' AS first_name, 2 AS n"),
    (
        "by_name_extra",
        "INSERT INTO sc.ns.bn BY NAME SELECT 'Cy' AS first_name, 'Z' AS last_name, 3 AS n,"
        " 4 AS extra",
    ),
    (
        "by_name_case",
        "INSERT INTO sc.ns.bn BY NAME SELECT 'Di' AS FIRST_NAME, 'Y' AS Last_Name, 5 AS N",
    ),
    (
        "by_name_dup",
        "INSERT INTO sc.ns.bn BY NAME SELECT 'E' AS first_name, 'F' AS first_name,"
        " 'G' AS last_name, 6 AS n",
    ),
    (
        "by_name_case_dup",
        "INSERT INTO sc.ns.bn BY NAME SELECT 'a' AS first_name, 'b' AS FIRST_NAME,"
        " 'c' AS last_name",
    ),
    ("positional", "INSERT INTO sc.ns.bn SELECT * FROM swapped"),
    ("by_name_values", "INSERT INTO sc.ns.bn BY NAME VALUES ('x', 'y', 7)"),
    (
        "by_name_column_list",
        "INSERT INTO sc.ns.bn (n, first_name) BY NAME SELECT 9 AS N, 'Q' AS FIRST_NAME",
    ),
    (
        "overwrite_by_name",
        "INSERT OVERWRITE sc.ns.bn BY NAME SELECT 'O' AS last_name, 'P' AS first_name, 8 AS n",
    ),
)

PARQUET_SEQUENCE: tuple[tuple[str, str], ...] = (
    ("pq_by_name", "INSERT INTO sc.ns.pq BY NAME SELECT * FROM swapped"),
    ("pq_subset", "INSERT INTO sc.ns.pq BY NAME SELECT 'Bo' AS first_name, 2 AS n"),
    (
        "pq_case",
        "INSERT INTO sc.ns.pq BY NAME SELECT 'Di' AS FIRST_NAME, 'Y' AS Last_Name, 5 AS N",
    ),
    (
        "pq_extra",
        "INSERT INTO sc.ns.pq BY NAME SELECT 'Cy' AS first_name, 4 AS extra",
    ),
    (
        "pq_dup",
        "INSERT INTO sc.ns.pq BY NAME SELECT 'E' AS first_name, 'F' AS first_name,"
        " 'G' AS last_name, 6 AS n",
    ),
    ("pq_values", "INSERT INTO sc.ns.pq BY NAME VALUES ('x', 'y', 7)"),
    ("pq_positional", "INSERT INTO sc.ns.pq SELECT * FROM swapped"),
    (
        "pq_overwrite",
        "INSERT OVERWRITE sc.ns.pq BY NAME SELECT 'O' AS last_name, 'P' AS first_name, 8 AS n",
    ),
)


def _spark_session(warehouse: Path) -> Any:
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[4]")
        .appName("ic-rtas-byname-1-oracle")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config("spark.jars.ivy", "/tmp/ic-build/.ivy2")
        .config("spark.sql.extensions", ICEBERG_SPARK_EXTENSIONS)
        .config("spark.sql.catalog.sc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _rows_of(spark: Any, sql: str) -> list[list[Any]]:
    return sorted(
        (list(row) for row in spark.sql(sql).collect()),
        key=repr,
    )


def record_byname(spark: Any) -> dict[str, Any]:
    spark.sql(
        "CREATE TABLE sc.ns.bn (first_name STRING, last_name STRING, n INT)"
        " USING iceberg TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql(
        "CREATE OR REPLACE TEMP VIEW swapped AS"
        " SELECT 'Smith' AS last_name, 'Ann' AS first_name, 1 AS n"
    )
    cells: dict[str, Any] = {}
    for label, sql in BYNAME_SEQUENCE:
        try:
            spark.sql(sql)
            cells[label] = {
                "sql": sql,
                "rows": _rows_of(spark, "SELECT first_name, last_name, n FROM sc.ns.bn"),
            }
        except Exception as error:
            cells[label] = {
                "sql": sql,
                "error_class": type(error).__name__,
                "error": str(error),
            }
    return cells


def record_parquet(spark: Any) -> dict[str, Any]:
    spark.sql("CREATE TABLE sc.ns.pq (first_name STRING, last_name STRING, n INT) USING parquet")
    cells: dict[str, Any] = {}
    for label, sql in PARQUET_SEQUENCE:
        try:
            spark.sql(sql)
            cells[label] = {
                "sql": sql,
                "rows": _rows_of(spark, "SELECT first_name, last_name, n FROM sc.ns.pq"),
            }
        except Exception as error:
            cells[label] = {
                "sql": sql,
                "error_class": type(error).__name__,
                "error": str(error),
            }
    return cells


def record_rtas(spark: Any) -> dict[str, Any]:
    spark.range(2000).createOrReplaceTempView("r")
    spark.sql(
        "CREATE TABLE sc.ns.rt USING iceberg PARTITIONED BY (p)"
        " TBLPROPERTIES ('format-version'='2')"
        " AS SELECT id, CAST(id % 3 AS INT) AS p FROM r"
    )
    spark.sql(
        "CREATE OR REPLACE TABLE sc.ns.rt USING iceberg PARTITIONED BY (p)"
        " TBLPROPERTIES ('format-version'='2')"
        " AS SELECT id, CAST(id % 5 AS INT) AS p FROM r WHERE id < 100"
    )
    snaps = [
        row.asDict()
        for row in spark.sql(
            "SELECT operation, summary FROM sc.ns.rt.snapshots ORDER BY committed_at"
        ).collect()
    ]
    spark.sql(
        "CREATE OR REPLACE TABLE sc.ns.rt2 USING iceberg TBLPROPERTIES ('format-version'='2')"
        " AS SELECT id FROM r WHERE id < 10"
    )
    snaps2 = [
        row.asDict()
        for row in spark.sql(
            "SELECT operation, summary FROM sc.ns.rt2.snapshots ORDER BY committed_at"
        ).collect()
    ]
    spark.sql(
        "CREATE OR REPLACE TABLE sc.ns.rt3 USING iceberg TBLPROPERTIES ('format-version'='2')"
        " AS SELECT id FROM r WHERE id < 0"
    )
    snaps3 = [
        row.asDict()
        for row in spark.sql(
            "SELECT operation, summary FROM sc.ns.rt3.snapshots ORDER BY committed_at"
        ).collect()
    ]
    spark.sql(
        "CREATE OR REPLACE TABLE sc.ns.rt3 USING iceberg TBLPROPERTIES ('format-version'='2')"
        " AS SELECT id FROM r WHERE id < 0"
    )
    snaps3b = [
        row.asDict()
        for row in spark.sql(
            "SELECT operation, summary FROM sc.ns.rt3.snapshots ORDER BY committed_at"
        ).collect()
    ]
    return {
        "ctas_then_rtas": snaps,
        "rtas_new_table": snaps2,
        "rtas_empty_new": snaps3,
        "rtas_empty_twice": snaps3b,
    }


def record_all(warehouse: Path) -> dict[str, Any]:
    if warehouse.exists():
        shutil.rmtree(warehouse)
    warehouse.mkdir(parents=True)
    spark = _spark_session(warehouse)
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns")
        return {
            "insert_by_name": record_byname(spark),
            "parquet_by_name": record_parquet(spark),
            "rtas_ops": record_rtas(spark),
        }
    finally:
        spark.stop()


def _normalized(cell: Any) -> Any:
    if isinstance(cell, dict):
        out: dict[str, Any] = {}
        for key, value in cell.items():
            if key == "summary" and isinstance(value, dict):
                value = {
                    summary_key: summary_value
                    for summary_key, summary_value in value.items()
                    if summary_key not in VOLATILE_SUMMARY_KEYS
                }
            out[key] = _normalized(value)
        return out
    if isinstance(cell, list):
        return [_normalized(item) for item in cell]
    return cell


def check_against_fixture(derived: dict[str, Any]) -> list[str]:
    expected = json.loads(FIXTURE.read_text(encoding="utf-8"))
    mismatches: list[str] = []
    for section in ("insert_by_name", "parquet_by_name", "rtas_ops"):
        for cell, want in expected[section].items():
            got = derived[section].get(cell)
            if _normalized(got) != _normalized(want):
                mismatches.append(
                    f"{section}.{cell}:\n  want={json.dumps(want, default=str)[:600]}"
                    f"\n  got ={json.dumps(got, default=str)[:600]}"
                )
    for section in ("insert_by_name", "parquet_by_name", "rtas_ops"):
        for cell in derived[section]:
            if cell not in expected[section]:
                mismatches.append(f"{section}.{cell}: extra cell not in fixture")
    return mismatches


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("mode", choices=("record", "check"))
    args = parser.parse_args(argv)
    derived = record_all(args.warehouse)
    if args.mode == "record":
        print(json.dumps(derived, indent=1, default=str))
        return 0
    mismatches = check_against_fixture(derived)
    if mismatches:
        print(f"{len(mismatches)} oracle cell(s) drifted from {FIXTURE}:", flush=True)
        for mismatch in mismatches:
            print(mismatch, flush=True)
        return 1
    print(f"oracle fixture {FIXTURE.name} reproduces on live Spark", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

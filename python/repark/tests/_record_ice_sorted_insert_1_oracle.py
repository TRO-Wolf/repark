"""Record the ICE-SORTED-INSERT-1 Spark oracle — per-file sortedness truth JSON.

NOT a ``test_`` module: pytest never collects it. It rebuilds the six run-19c
sort cells on live PySpark plus two new cells (``days(ts)`` transform order and
a float column holding NaN), records per-file ``records`` / ``sort_order_id`` /
``sorted`` into ``ice_sorted_insert_1_spark_oracle.json`` beside this file, and
copies the days-transform warehouse into ``fixtures/ice_sorted_insert_1/``. The
pin test copies it back to the same canonical path before ``register_table``,
so manifest file URIs stay valid.

Run it (one JVM at a time)::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        /tmp/sparkenv/bin/python python/repark/tests/_record_ice_sorted_insert_1_oracle.py

Re-running it re-derives every cell from live Spark and exits non-zero on drift;
it rewrites the truth JSON only when ``--rewrite`` is passed, so routine runs
verify rather than launder.

Spark basis: ``local[4]``, ``spark.driver.memory=2g``, UI off,
``spark.sql.session.timeZone=UTC``,
``org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`` (see ``_oracle_pins.py``).
"""

from __future__ import annotations

import datetime
import json
import math
import shutil
import sys
from pathlib import Path
from typing import Any

_CANONICAL_ROOT = Path("/tmp/repark-ice-sorted-insert-1")
_WAREHOUSE = _CANONICAL_ROOT / "wh"
_CATALOG = "rec"
_HERE = Path(__file__).resolve().parent
_TRUTH_PATH = _HERE / "ice_sorted_insert_1_spark_oracle.json"
_FIXTURE_DIR = _HERE / "fixtures" / "ice_sorted_insert_1"
_FIXTURE_TABLES = ("days",)


def _spark_session() -> Any:
    """The recorded basis, built once."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[4]")
        .appName("repark-ice-sorted-insert-1-record")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0",
        )
        .config("spark.jars.ivy", "/tmp/ic-build/.ivy2")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{_CATALOG}.warehouse", str(_WAREHOUSE))
        .getOrCreate()
    )


def _is_sorted_int(keys: list[tuple[Any, ...]], spec: list[tuple[bool, bool]]) -> bool:
    """Sortedness of int-or-None key tuples under (desc, nulls_first) per key."""

    def key_of(item: tuple[Any, ...]) -> tuple[tuple[int, int], ...]:
        out = []
        for value, (desc, nulls_first) in zip(item, spec, strict=True):
            is_null = value is None
            rank = 0 if (is_null == nulls_first) else 1
            out.append((rank, 0 if is_null else (-value if desc else value)))
        return tuple(out)

    return [key_of(k) for k in keys] == sorted(key_of(k) for k in keys)


def _is_sorted_float(keys: list[tuple[Any, ...]]) -> bool:
    """Sortedness of (float-or-None,) keys under ASC NULLS FIRST, NaN largest."""

    def key_of(item: tuple[Any, ...]) -> tuple[int, float]:
        (value,) = item
        if value is None:
            return (0, 0.0)
        if isinstance(value, float) and math.isnan(value):
            return (2, math.inf)
        return (1, float(value))

    return [key_of(k) for k in keys] == sorted(key_of(k) for k in keys)


def _json_row(row: dict[str, Any]) -> dict[str, Any]:
    """One head row with datetimes rendered as ISO text."""
    return {
        key: value.isoformat() if isinstance(value, datetime.datetime) else value
        for key, value in row.items()
    }


def _files(
    spark: Any,
    table: str,
    columns: list[str],
    kind: str,
    spec: list[tuple[bool, bool]] | None = None,
) -> list[dict[str, Any]]:
    """Per-file stamp and sortedness; heads only, keys never stored."""
    select = ", ".join(columns)
    by_file: dict[str, list[dict[str, Any]]] = {}
    for row in spark.sql(f"SELECT input_file_name() AS fp, {select} FROM {table}").collect():
        by_file.setdefault(row.fp, []).append({c: row[c] for c in columns})
    probe = spark.sql(f"SELECT * FROM {table}.files")
    stamps = {
        row.file_path.rsplit("/", 1)[-1]: (
            row.record_count,
            row.sort_order_id,
            str(row.partition) if "partition" in probe.columns else "",
        )
        for row in probe.collect()
    }
    out = []
    for path, values in by_file.items():
        if kind == "day":
            keys = [
                (
                    None if x["ts"] is None else (x["ts"].date() - datetime.date(1970, 1, 1)).days,
                    x["id"],
                )
                for x in values
            ]
            ordered = _is_sorted_int(keys, [(False, True), (False, True)])
        elif kind == "float":
            ordered = _is_sorted_float([(x["f"],) for x in values])
        else:
            ordered = _is_sorted_int([tuple(x[c] for c in columns) for x in values], spec or [])
        records, stamp, partition = stamps.get(path.rsplit("/", 1)[-1], (len(values), None, ""))
        out.append(
            {
                "records": records,
                "sort_order_id": stamp,
                "partition": partition,
                "first": [_json_row(v) for v in values[:3]],
                "sorted": ordered,
            }
        )
    return sorted(out, key=lambda f: (f["partition"], json.dumps(f["first"], default=str)))


def _sort_cell(
    spark: Any,
    name: str,
    ddl: str,
    order_sql: str,
    insert_sql: str,
    columns: list[str],
    kind: str,
    spec: list[tuple[bool, bool]] | None = None,
    door: str = "sql",
) -> dict[str, Any]:
    """Build one ordered table on Spark and record its files."""
    table = f"{_CATALOG}.ns.{name}"
    spark.sql(f"CREATE TABLE {table} {ddl}")
    if order_sql:
        spark.sql(f"ALTER TABLE {table} {order_sql}")
    if door == "sql":
        spark.sql(insert_sql.format(t=table))
    else:
        spark.sql(insert_sql).writeTo(table).append()
    return {
        "ddl": ddl,
        "order": order_sql,
        "insert": insert_sql,
        "door": door,
        "files": _files(spark, table, columns, kind, spec),
    }


def _record(spark: Any) -> dict[str, Any]:
    """Every oracle cell, derived from live Spark."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {_CATALOG}.ns")
    spark.range(2000).createOrReplaceTempView("r")
    cells: dict[str, Any] = {}
    cells["sort_partitioned_local"] = _sort_cell(
        spark,
        "so1",
        "(id BIGINT, p INT) USING iceberg PARTITIONED BY (p) TBLPROPERTIES ('format-version'='2')",
        "WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY id",
        "INSERT INTO {t} SELECT (id * 7919) % 2000 AS id, CAST(id % 2 AS INT) AS p FROM r",
        ["id"],
        "int",
        [(False, True)],
    )
    cells["sort_unpartitioned_desc"] = _sort_cell(
        spark,
        "so2",
        "(id BIGINT, s STRING) USING iceberg TBLPROPERTIES ('format-version'='2')",
        "WRITE ORDERED BY id DESC",
        "INSERT INTO {t} SELECT (id * 7919) % 2000 AS id, CAST(id AS STRING) FROM r",
        ["id"],
        "int",
        [(True, False)],
    )
    cells["sort_two_keys_nulls"] = _sort_cell(
        spark,
        "so3",
        "(a INT, b BIGINT) USING iceberg TBLPROPERTIES ('format-version'='2')",
        "WRITE ORDERED BY a ASC NULLS LAST, b DESC NULLS FIRST",
        "INSERT INTO {t} SELECT CASE WHEN id % 7 = 0 THEN NULL ELSE CAST(id % 5 AS INT)"
        " END AS a, CASE WHEN id % 11 = 0 THEN NULL ELSE (id * 31) % 97 END AS b FROM r",
        ["a", "b"],
        "int",
        [(False, False), (True, True)],
    )
    cells["sort_dataframe_door"] = _sort_cell(
        spark,
        "so4",
        "(id BIGINT, p INT) USING iceberg PARTITIONED BY (p) TBLPROPERTIES ('format-version'='2')",
        "WRITE ORDERED BY id",
        "SELECT (id * 7919) % 2000 AS id, CAST(id % 2 AS INT) AS p FROM r",
        ["id"],
        "int",
        [(False, True)],
        door="df",
    )
    cells["sort_distribution_none"] = _sort_cell(
        spark,
        "so5",
        "(id BIGINT, p INT) USING iceberg PARTITIONED BY (p) TBLPROPERTIES ('format-version'='2')",
        "WRITE LOCALLY ORDERED BY id",
        "INSERT INTO {t} SELECT (id * 7919) % 2000 AS id, CAST(id % 2 AS INT) AS p FROM r",
        ["id"],
        "int",
        [(False, True)],
    )
    cells["sort_transform_bucket"] = _sort_cell(
        spark,
        "so6",
        "(id BIGINT, p INT) USING iceberg TBLPROPERTIES ('format-version'='2')",
        "WRITE ORDERED BY bucket(4, id), id",
        "INSERT INTO {t} SELECT (id * 7919) % 2000 AS id, 1 AS p FROM r",
        ["id"],
        "int",
        [(False, True)],
    )
    cells["sort_transform_days"] = _sort_cell(
        spark,
        "so7",
        "(id BIGINT, ts TIMESTAMP) USING iceberg TBLPROPERTIES ('format-version'='2')",
        "WRITE ORDERED BY days(ts), id",
        "INSERT INTO {t} SELECT (id * 7919) % 2000 AS id,"
        " CAST(DATE_ADD(DATE '2026-01-01', CAST(id % 40 AS INT)) AS TIMESTAMP)"
        " AS ts FROM r",
        ["id", "ts"],
        "day",
    )
    cells["sort_float_nan"] = _sort_cell(
        spark,
        "so8",
        "(id BIGINT, f FLOAT) USING iceberg TBLPROPERTIES ('format-version'='2')",
        "WRITE ORDERED BY f",
        "INSERT INTO {t} SELECT id, CASE WHEN id % 13 = 0 THEN CAST('NaN' AS FLOAT)"
        " WHEN id % 17 = 0 THEN CAST(NULL AS FLOAT)"
        " ELSE CAST((id * 7919) % 2000 AS FLOAT) END AS f FROM r",
        ["id", "f"],
        "float",
    )
    return {
        "oracle": "PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0, local[4], UTC",
        "cells": cells,
    }


def _copy_fixtures() -> None:
    """Copy the days-transform warehouse into the fixture dir, keeping map.md."""
    for key in _FIXTURE_TABLES:
        src = _WAREHOUSE / "ns" / "so7"
        dest = _FIXTURE_DIR / key
        if dest.exists():
            shutil.rmtree(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(src, dest)


def _report_drift(recorded: dict[str, Any], payload: dict[str, Any]) -> None:
    """Print every truth-vs-live cell difference."""
    for name, live in payload["cells"].items():
        old = recorded["cells"].get(name)
        if old != live:
            print(f"  cell {name}: truth={json.dumps(old, default=str)[:400]}")
            print(f"  cell {name}: live ={json.dumps(live, default=str)[:400]}")


def main(argv: list[str]) -> int:
    """Build, record, verify against the checked-in truth, rewrite only on --rewrite."""
    if _CANONICAL_ROOT.exists():
        shutil.rmtree(_CANONICAL_ROOT)
    _WAREHOUSE.mkdir(parents=True)
    spark = _spark_session()
    try:
        payload = _record(spark)
    finally:
        spark.stop()
    if _TRUTH_PATH.exists() and "--rewrite" not in argv:
        recorded = json.loads(_TRUTH_PATH.read_text(encoding="utf-8"))
        if recorded == payload:
            print("oracle matches the checked-in truth")
            return 0
        print("ORACLE DRIFT versus the checked-in truth:")
        _report_drift(recorded, payload)
        return 1
    _TRUTH_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    _copy_fixtures()
    print(f"wrote {_TRUTH_PATH} and {_FIXTURE_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

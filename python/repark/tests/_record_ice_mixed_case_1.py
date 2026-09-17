"""Record the ICE-MIXED-CASE-1 Spark oracle and stamp the mixed-case fixture warehouse.

NOT a ``test_`` module: pytest never collects it. It creates a Spark-written
Iceberg table with mixed-case columns, runs every ledger cell on live PySpark
4.1.2 under ``spark.sql.caseSensitive`` false and true, and writes
``python/repark/tests/ice_mixed_case_1_spark_oracle.json``. It also leaves the
warehouse in place so the operator can copy it to
``python/repark-parity/fixtures/torture/data/ice_mixed_case_1``. Run it (needs
a JVM and ``pyspark``)::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        /tmp/ib-build/.venv/bin/python python/repark/tests/_record_ice_mixed_case_1.py \\
        /tmp/repark-ice-mixed-case-1

Exit code 0 writes the oracle JSON next to this driver. It never edits the
committed pin file ``test_ice_mixed_case_1.py``.
"""

from __future__ import annotations

import json
import shutil
import sys
import traceback
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ORACLE_PATH = Path(__file__).resolve().parent / "ice_mixed_case_1_spark_oracle.json"

_CREATE_TABLE = (
    "CREATE TABLE sc.ns.mc (userId BIGINT, eventName STRING, `Mixed Case` INT) "
    "USING iceberg TBLPROPERTIES ('format-version'='2', "
    "'write.delete.mode'='copy-on-write', 'write.update.mode'='copy-on-write', "
    "'write.merge.mode'='copy-on-write')"
)
_SEED_SQL = "INSERT INTO sc.ns.mc VALUES (1, 'a', 10), (2, 'b', 20)"
_RESET_SQLS = ["DELETE FROM sc.ns.mc", _SEED_SQL]
_READBACK_SQL = "SELECT userId, eventName, `Mixed Case` FROM sc.ns.mc ORDER BY userId"
_MCS_VIEW_SQL = (
    "CREATE OR REPLACE TEMP VIEW mcs AS SELECT CAST(2 AS BIGINT) AS userid, "
    "'b2' AS eventname, 21 AS `mixed case` UNION ALL "
    "SELECT CAST(3 AS BIGINT), 'c', 30"
)
_MCS2_VIEW_SQL = (
    "CREATE OR REPLACE TEMP VIEW mcs2 AS SELECT CAST(2 AS BIGINT) AS USERID, "
    "'b2' AS EVENTNAME, 21 AS `MIXED CASE` UNION ALL "
    "SELECT CAST(3 AS BIGINT), 'c', 30"
)


def _cells() -> dict[str, dict[str, Any]]:
    """Ledger cell id to the Spark recipe: setup SQL plus measured statements."""
    return {
        "MC-SEL-01": {
            "sql": ["SELECT userId, eventName, `Mixed Case` FROM sc.ns.mc ORDER BY userId"]
        },
        "MC-SEL-02": {
            "sql": ["SELECT userid, EVENTNAME, `Mixed Case` FROM sc.ns.mc WHERE USERID = 1"]
        },
        "MC-SEL-03": {
            "sql": ["SELECT `USERID`, `EVENTNAME`, `mixed case` FROM sc.ns.mc ORDER BY `USERID`"]
        },
        "MC-SEL-04": {
            "sql": ["SELECT `userId`, `eventName`, `Mixed Case` FROM sc.ns.mc ORDER BY `userId`"]
        },
        "MC-WHERE-01": {"sql": ["SELECT userId FROM sc.ns.mc WHERE EVENTNAME = 'a' ORDER BY userId"]},
        "MC-GRP-01": {
            "sql": [
                "SELECT EVENTNAME, COUNT(*) AS c FROM sc.ns.mc "
                "GROUP BY EVENTNAME ORDER BY EVENTNAME"
            ]
        },
        "MC-ORD-01": {"sql": ["SELECT userId FROM sc.ns.mc ORDER BY USERID DESC"]},
        "MC-JOIN-01": {
            "sql": [
                "SELECT a.userId, b.eventName FROM sc.ns.mc a JOIN sc.ns.mc b "
                "ON a.USERID = b.userid ORDER BY a.userId"
            ]
        },
        "MC-VIEW-01": {
            "sql": [
                "CREATE OR REPLACE TEMP VIEW mcv AS SELECT * FROM sc.ns.mc",
                "SELECT USERID, EVENTNAME FROM mcv WHERE userid = 1",
            ]
        },
        "MC-AMB-01": {
            "sql": [
                "CREATE OR REPLACE TEMP VIEW amb_l AS SELECT 1 AS a",
                "CREATE OR REPLACE TEMP VIEW amb_r AS SELECT 2 AS A",
                "SELECT a FROM amb_l JOIN amb_r ON amb_l.a = amb_r.A",
            ]
        },
        "MC-AMB-02": {
            "sql": [
                "CREATE OR REPLACE TEMP VIEW amb_l AS SELECT 1 AS a",
                "CREATE OR REPLACE TEMP VIEW amb_r AS SELECT 2 AS A",
                "SELECT A FROM amb_l JOIN amb_r ON amb_l.a = amb_r.A",
            ]
        },
        "MC-DF-01": {"df": {"op": "select", "cols": ["USERID"]}},
        "MC-DF-02": {"df": {"op": "filter", "cond": "USERID = 1"}},
        "MC-UPD-01": {
            "reset": True,
            "sql": ["UPDATE sc.ns.mc SET eventName = 'u' WHERE userId = 1", _READBACK_SQL],
        },
        "MC-UPD-02": {
            "reset": True,
            "sql": [
                "UPDATE sc.ns.mc SET UserId = UserId + 10 WHERE EVENTNAME = 'b'",
                _READBACK_SQL,
            ],
        },
        "MC-DEL-01": {
            "reset": True,
            "sql": ["DELETE FROM sc.ns.mc WHERE USERID = 2", _READBACK_SQL],
        },
        "MC-MRG-01": {
            "reset": True,
            "sql": [
                _MCS_VIEW_SQL,
                "MERGE INTO sc.ns.mc t USING mcs s ON t.`userId` = s.`userid` "
                "WHEN MATCHED THEN UPDATE SET eventName = s.`eventname` "
                "WHEN NOT MATCHED THEN INSERT (userId, eventName, `Mixed Case`) "
                "VALUES (s.`userid`, s.`eventname`, s.`mixed case`)",
                _READBACK_SQL,
            ],
        },
        "MC-MRG-02": {
            "reset": True,
            "sql": [
                _MCS2_VIEW_SQL,
                "MERGE INTO sc.ns.mc t USING mcs2 s ON t.userid = s.USERID "
                "WHEN MATCHED THEN UPDATE SET EventName = s.EVENTNAME "
                "WHEN NOT MATCHED THEN INSERT (userId, eventName, `Mixed Case`) "
                "VALUES (s.USERID, s.EVENTNAME, s.`MIXED CASE`)",
                _READBACK_SQL,
            ],
        },
        "MC-MRG-03": {
            "reset": True,
            "sql": [
                _MCS_VIEW_SQL,
                "MERGE INTO sc.ns.mc t USING mcs s ON t.userId = s.userid "
                "WHEN MATCHED THEN UPDATE SET * "
                "WHEN NOT MATCHED THEN INSERT *",
                _READBACK_SQL,
            ],
        },
        "MC-MRG-04": {
            "reset": True,
            "sql": [
                _MCS2_VIEW_SQL,
                "MERGE INTO sc.ns.mc t USING mcs2 s ON t.userid = s.USERID "
                "WHEN MATCHED THEN UPDATE SET eventName = s.EVENTNAME "
                "WHEN NOT MATCHED THEN INSERT (USERID, EVENTNAME, `mixed case`) "
                "VALUES (s.USERID, s.EVENTNAME, s.`MIXED CASE`)",
                _READBACK_SQL,
            ],
        },
        "MC-INS-01": {
            "reset": True,
            "sql": [
                "INSERT INTO sc.ns.mc (USERID, EVENTNAME, `mixed case`) VALUES (9, 'z', 90)",
                _READBACK_SQL,
            ],
        },
    }


def _spark_session(warehouse: str) -> Any:
    """One short-lived local Spark session over a Hadoop Iceberg catalog."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[4]")
        .appName("ice-mixed-case-1-record")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0")
        .config("spark.jars.ivy", "/tmp/ib-scratch/.ivy2")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.sc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def _norm(value: Any) -> Any:
    """JSON-safe normalization for recorded Spark scalars."""
    import datetime
    import decimal

    if isinstance(value, decimal.Decimal):
        return {"__decimal__": str(value)}
    if isinstance(value, (datetime.datetime, datetime.date)):
        return {"__iso__": value.isoformat()}
    if isinstance(value, float) and value != value:
        return "NaN"
    if isinstance(value, dict):
        return {str(key): _norm(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_norm(item) for item in value]
    return value


def _run_sql(spark: Any, sql: str) -> dict[str, Any]:
    """Run one Spark SQL string; rows plus columns, or error class and message."""
    try:
        frame = spark.sql(sql)
        columns = list(frame.columns)
        rows = [_norm(list(row)) for row in frame.collect()]
        return {"rows": rows, "columns": columns}
    except Exception as exc:
        name = type(exc).__name__
        text = "".join(traceback.format_exception_only(exc)).strip()
        return {"error_class": name, "error_message": text}


def _run_df(spark: Any, recipe: dict[str, Any]) -> dict[str, Any]:
    """Run one Spark DataFrame recipe; rows plus columns, or error class and message."""
    try:
        frame = spark.table("sc.ns.mc")
        if recipe["op"] == "select":
            frame = frame.select(*recipe["cols"])
        elif recipe["op"] == "filter":
            frame = frame.filter(recipe["cond"])
        else:
            raise ValueError(f"unknown df recipe {recipe!r}")
        columns = list(frame.columns)
        rows = sorted((_norm(list(row)) for row in frame.collect()), key=repr)
        return {"rows": rows, "columns": columns}
    except Exception as exc:
        name = type(exc).__name__
        text = "".join(traceback.format_exception_only(exc)).strip()
        return {"error_class": name, "error_message": text}


def _record_cell(spark: Any, cell: dict[str, Any]) -> dict[str, Any]:
    """Record one cell under both ``caseSensitive`` values."""
    recorded: dict[str, Any] = {}
    for flag in ("false", "true"):
        spark.conf.set("spark.sql.caseSensitive", flag)
        if cell.get("reset"):
            for sql in _RESET_SQLS:
                spark.sql(sql).collect()
        if "df" in cell:
            recorded[flag] = _run_df(spark, cell["df"])
            continue
        outcome: dict[str, Any] = {}
        for sql in cell["sql"]:
            outcome = _run_sql(spark, sql)
            if "error_class" in outcome:
                outcome["failed_sql"] = sql
                break
        recorded[flag] = outcome
    return recorded


def main(argv: list[str]) -> int:
    """Create the warehouse, record every cell, write the oracle JSON."""
    warehouse = argv[1] if len(argv) > 1 else "/tmp/repark-ice-mixed-case-1"
    shutil.rmtree(warehouse, ignore_errors=True)
    spark = _spark_session(warehouse)
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
        spark.sql(_CREATE_TABLE).collect()
        spark.sql(_SEED_SQL).collect()
        cells = _cells()
        recorded = {cell_id: _record_cell(spark, cell) for cell_id, cell in cells.items()}
        payload = {
            "spark": "4.1.2",
            "iceberg_runtime": "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0",
            "warehouse": warehouse,
            "create_table": _CREATE_TABLE,
            "seed": _SEED_SQL,
            "cells": recorded,
        }
        _ORACLE_PATH.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        print(f"wrote {_ORACLE_PATH} ({len(recorded)} cells); warehouse at {warehouse}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

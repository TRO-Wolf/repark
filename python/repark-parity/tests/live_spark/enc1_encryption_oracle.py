"""ENC-1 step 1b oracle: 23 Spark 4.1.2 + Iceberg 1.11.0 cells, no KMS configured."""

from __future__ import annotations

import datetime
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

from mb0_bench import build_session, iceberg_jar
from pyspark.sql import SparkSession

NAMESPACE = "local.enc1"
KEY = "review-test-key"
EXPECTED_CELLS = 23


def jvm_causes(exc: Exception) -> list[str]:
    chain: list[str] = []
    origin = getattr(exc, "_origin", None)
    if origin is None:
        origin = getattr(exc, "java_exception", None)
    seen = 0
    while origin is not None and seen < 10:
        try:
            chain.append(origin.getClass().getName())
        except Exception:
            chain.append(type(origin).__name__)
        try:
            origin = origin.getCause()
        except Exception:
            break
        seen += 1
    return chain


def error_of(exc: Exception) -> dict[str, Any]:
    condition = None
    sqlstate = None
    for name in ("getCondition", "getErrorClass"):
        if hasattr(exc, name):
            try:
                condition = getattr(exc, name)()
            except Exception:
                condition = None
            if condition:
                break
    if hasattr(exc, "getSqlState"):
        try:
            sqlstate = exc.getSqlState()
        except Exception:
            sqlstate = None
    java_exc = getattr(exc, "java_exception", None)
    if java_exc is not None:
        if not condition:
            for name in ("getCondition", "getErrorClass"):
                try:
                    value = getattr(java_exc, name)()
                except Exception:
                    value = None
                if value:
                    condition = value
                    break
        if sqlstate is None:
            try:
                sqlstate = java_exc.getSqlState()
            except Exception:
                sqlstate = None
    cause = exc.__cause__
    return {
        "class": condition or f"{type(exc).__module__}.{type(exc).__name__}",
        "condition": condition,
        "exception": f"{type(exc).__module__}.{type(exc).__name__}",
        "sqlstate": sqlstate,
        "causes": jvm_causes(exc),
        "python_cause": f"{type(cause).__module__}.{type(cause).__name__}: {cause}"
        if cause is not None
        else None,
        "text": str(exc),
    }


def run_sql(spark: SparkSession, statement: str) -> dict[str, Any]:
    try:
        frame = spark.sql(statement)
        rows = frame.collect() if frame is not None else []
        return {"outcome": "ok", "rows": [list(row) for row in rows]}
    except Exception as exc:
        return {"outcome": "error", "error": error_of(exc)}


def table_state(spark: SparkSession, table: str) -> dict[str, Any]:
    state: dict[str, Any] = {"exists": False}
    try:
        props = spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
    except Exception as exc:
        state["exists_error"] = error_of(exc)
        return state
    state["exists"] = True
    state["properties"] = {row["key"]: row["value"] for row in props}
    try:
        snaps = spark.sql(f"SELECT snapshot_id, operation FROM {table}.snapshots").collect()
        state["snapshots"] = [
            {"id": str(row["snapshot_id"]), "op": row["operation"]} for row in snaps
        ]
    except Exception as exc:
        state["snapshots_error"] = error_of(exc)
    try:
        files = spark.sql(f"SELECT file_path FROM {table}.files").collect()
        state["file_count"] = len(files)
    except Exception as exc:
        state["files_error"] = error_of(exc)
    try:
        count = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").collect()
        state["row_count"] = count[0]["n"]
    except Exception as exc:
        state["count_error"] = error_of(exc)
    return state


def create_keyed_statement(table: str) -> str:
    return (
        f"CREATE TABLE {table} (id INT, name STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '3', 'encryption.key-id' = '{KEY}')"
    )


def keyed_with_data(spark: SparkSession, name: str) -> tuple[str, dict[str, Any]]:
    table = f"{NAMESPACE}.{name}"
    run_sql(spark, f"CREATE TABLE {table} (id INT, name STRING) USING iceberg")
    setup = run_sql(spark, f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')")
    alter = run_sql(spark, f"ALTER TABLE {table} SET TBLPROPERTIES ('encryption.key-id' = '{KEY}')")
    return table, {"setup_insert": setup, "alter": alter}


def record(
    cells: list[dict[str, Any]],
    spark: SparkSession,
    cell: str,
    statement: str,
    result: dict[str, Any],
    table: str | None,
) -> None:
    entry: dict[str, Any] = {"cell": cell, "statement": statement}
    entry.update(result)
    if table is not None:
        entry["table_state"] = table_state(spark, table)
    cells.append(entry)
    print(f"recorded {cell}: {result.get('outcome')}", flush=True)


def record_all(spark: SparkSession) -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    table = f"{NAMESPACE}.e01_create"
    statement = create_keyed_statement(table)
    record(cells, spark, "E01", statement, run_sql(spark, statement), table)

    table = f"{NAMESPACE}.e02_values"
    run_sql(spark, create_keyed_statement(table))
    statement = f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')"
    record(cells, spark, "E02", statement, run_sql(spark, statement), table)

    table = f"{NAMESPACE}.e03_select"
    run_sql(spark, create_keyed_statement(table))
    statement = f"INSERT INTO {table} SELECT id, name FROM {NAMESPACE}.e02_values"
    record(cells, spark, "E03", statement, run_sql(spark, statement), table)

    table = f"{NAMESPACE}.e04_overwrite"
    run_sql(spark, create_keyed_statement(table))
    statement = f"INSERT OVERWRITE {table} SELECT 3, 'c'"
    record(cells, spark, "E04", statement, run_sql(spark, statement), table)

    table = f"{NAMESPACE}.e05_ctas"
    statement = (
        f"CREATE TABLE {table} USING iceberg "
        f"TBLPROPERTIES ('format-version' = '3', 'encryption.key-id' = '{KEY}') "
        f"AS SELECT 1 AS id, 'a' AS name"
    )
    record(cells, spark, "E05", statement, run_sql(spark, statement), table)

    table = f"{NAMESPACE}.e06_rtas"
    run_sql(spark, f"CREATE TABLE {table} (id INT, name STRING) USING iceberg")
    statement = (
        f"CREATE OR REPLACE TABLE {table} USING iceberg "
        f"TBLPROPERTIES ('format-version' = '3', 'encryption.key-id' = '{KEY}') "
        f"AS SELECT 1 AS id, 'a' AS name"
    )
    record(cells, spark, "E06", statement, run_sql(spark, statement), table)

    table = f"{NAMESPACE}.e07_merge"
    run_sql(spark, create_keyed_statement(table))
    statement = (
        f"MERGE INTO {table} AS t USING (SELECT 1 AS id, 'a' AS name) AS s "
        f"ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name "
        f"WHEN NOT MATCHED THEN INSERT *"
    )
    record(cells, spark, "E07", statement, run_sql(spark, statement), table)

    table = f"{NAMESPACE}.e08_update"
    run_sql(spark, create_keyed_statement(table))
    statement = f"UPDATE {table} SET name = 'z' WHERE id = 1"
    record(cells, spark, "E08", statement, run_sql(spark, statement), table)

    table = f"{NAMESPACE}.e09_delete_row"
    run_sql(spark, create_keyed_statement(table))
    statement = f"DELETE FROM {table} WHERE id = 1"
    record(cells, spark, "E09", statement, run_sql(spark, statement), table)

    table = f"{NAMESPACE}.e10_delete_all"
    run_sql(spark, create_keyed_statement(table))
    statement = f"DELETE FROM {table}"
    record(cells, spark, "E10", statement, run_sql(spark, statement), table)

    table = f"{NAMESPACE}.e11_truncate"
    run_sql(spark, create_keyed_statement(table))
    statement = f"TRUNCATE TABLE {table}"
    record(cells, spark, "E11", statement, run_sql(spark, statement), table)

    table = f"{NAMESPACE}.e12_alter_add"
    run_sql(spark, f"CREATE TABLE {table} (id INT, name STRING) USING iceberg")
    setup = run_sql(spark, f"INSERT INTO {table} VALUES (1, 'a')")
    alter = run_sql(spark, f"ALTER TABLE {table} SET TBLPROPERTIES ('encryption.key-id' = '{KEY}')")
    statement = f"INSERT INTO {table} VALUES (2, 'b')"
    result = run_sql(spark, statement)
    result["setup_insert"] = setup
    result["alter"] = alter
    record(cells, spark, "E12", statement, result, table)

    table = f"{NAMESPACE}.e13_df_append"
    run_sql(spark, create_keyed_statement(table))
    try:
        frame = spark.createDataFrame([(9, "w")], "id INT, name STRING")
        frame.writeTo(table).append()
        df_result: dict[str, Any] = {"outcome": "ok", "rows": []}
    except Exception as exc:
        df_result = {"outcome": "error", "error": error_of(exc)}
    record(
        cells,
        spark,
        "E13",
        f"spark.createDataFrame([(9, 'w')]).writeTo('{table}').append()",
        df_result,
        table,
    )

    table, setup = keyed_with_data(spark, "e14_rewrite_data")
    statement = "CALL local.system.rewrite_data_files(table => 'enc1.e14_rewrite_data')"
    result = run_sql(spark, statement)
    result.update(setup)
    record(cells, spark, "E14", statement, result, table)

    table, setup = keyed_with_data(spark, "e15_rewrite_manifests")
    statement = "CALL local.system.rewrite_manifests(table => 'enc1.e15_rewrite_manifests')"
    result = run_sql(spark, statement)
    result.update(setup)
    record(cells, spark, "E15", statement, result, table)

    table, setup = keyed_with_data(spark, "e16_expire")
    run_sql(spark, f"INSERT INTO {table} VALUES (3, 'c')")
    statement = "CALL local.system.expire_snapshots(table => 'enc1.e16_expire', retain_last => 1)"
    result = run_sql(spark, statement)
    result.update(setup)
    record(cells, spark, "E16", statement, result, table)

    table, setup = keyed_with_data(spark, "e17_orphans")
    statement = "CALL local.system.remove_orphan_files(table => 'enc1.e17_orphans')"
    result = run_sql(spark, statement)
    result.update(setup)
    record(cells, spark, "E17", statement, result, table)

    table, setup = keyed_with_data(spark, "e18_select")
    statement = f"SELECT * FROM {table} ORDER BY id"
    result = run_sql(spark, statement)
    result.update(setup)
    record(cells, spark, "E18", statement, result, table)

    table = f"{NAMESPACE}.e19_empty"
    create = (
        f"CREATE TABLE {table} (id INT, name STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '3', 'encryption.key-id' = '')"
    )
    create_result = run_sql(spark, create)
    statement = f"INSERT INTO {table} VALUES (1, 'a')"
    result = run_sql(spark, statement)
    result["create"] = create_result
    record(cells, spark, "E19", statement, result, table)

    table = f"{NAMESPACE}.e20_lookalike_a"
    create = (
        f"CREATE TABLE {table} (id INT, name STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '3', 'encryption.keyid' = '{KEY}')"
    )
    create_result = run_sql(spark, create)
    statement = f"INSERT INTO {table} VALUES (1, 'a')"
    result = run_sql(spark, statement)
    result["create"] = create_result
    record(cells, spark, "E20", statement, result, table)

    table = f"{NAMESPACE}.e21_lookalike_b"
    create = (
        f"CREATE TABLE {table} (id INT, name STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '3', 'encryption.key-id-x' = '{KEY}')"
    )
    create_result = run_sql(spark, create)
    statement = f"INSERT INTO {table} VALUES (1, 'a')"
    result = run_sql(spark, statement)
    result["create"] = create_result
    record(cells, spark, "E21", statement, result, table)

    table = f"{NAMESPACE}.e22_control"
    run_sql(spark, f"CREATE TABLE {table} (id INT, name STRING) USING iceberg")
    statement = f"INSERT INTO {table} VALUES (1, 'a')"
    record(cells, spark, "E22", statement, run_sql(spark, statement), table)

    table = f"{NAMESPACE}.e23_v2"
    create = (
        f"CREATE TABLE {table} (id INT, name STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '2', 'encryption.key-id' = '{KEY}')"
    )
    create_result = run_sql(spark, create)
    statement = f"INSERT INTO {table} VALUES (1, 'a')"
    result = run_sql(spark, statement)
    result["create"] = create_result
    record(cells, spark, "E23", statement, result, table)

    return cells


def preamble(spark: SparkSession) -> dict[str, Any]:
    return {
        "date": datetime.date.today().isoformat(),
        "spark": spark.version,
        "iceberg_jar": Path(str(iceberg_jar())).name,
        "catalog": "hadoop",
        "master": spark.sparkContext.master,
        "kms": "none configured",
    }


def scrub(text: str, warehouse: str, recorder: str) -> str:
    return text.replace(warehouse, "$WAREHOUSE").replace(recorder, "$RECORDER")


def main() -> int:
    warehouse = os.environ.get("ENC1_WAREHOUSE") or tempfile.mkdtemp(prefix="enc1-warehouse-")
    out = Path(os.environ.get("ENC1_OUT") or Path(__file__).with_suffix(".json"))
    spark = build_session(Path(warehouse))
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
    try:
        cells = record_all(spark)
        fresh_preamble = preamble(spark)
    finally:
        spark.stop()
    document = {"preamble": fresh_preamble, "cells": cells}
    text = json.dumps(document, indent=2, sort_keys=True)
    out.write_text(scrub(text, warehouse, str(Path(__file__))) + "\n")
    print(f"wrote {out} ({len(cells)} cells)", flush=True)
    return 0 if len(cells) == EXPECTED_CELLS else 1


if __name__ == "__main__":
    sys.exit(main())

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any


def build_session(engine: str, warehouse: str) -> tuple[Any, Any]:
    if engine == "spark":
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as spark_fn

        jar = os.environ["SPARK_ICEBERG_JAR"]
        sess = (
            SparkSession.builder.master("local[2]")
            .appName("iocast_step0_part1")
            .config("spark.jars", jar)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.ice.type", "hadoop")
            .config("spark.sql.catalog.ice.warehouse", warehouse)
            .config("spark.sql.session.timeZone", "America/New_York")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        sess.sparkContext.setLogLevel("OFF")
        return sess, spark_fn
    from repark.spark.sql import SparkSession
    from repark.spark.sql import functions as spark_fn

    sess = (
        SparkSession.builder.appName("iocast_step0_part1")
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", warehouse)
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    return sess, spark_fn


def err_summary(exc: Exception) -> dict[str, Any]:
    msg = str(exc).strip()
    first = msg.splitlines()[0] if msg else type(exc).__name__
    cond = None
    if "[" in first and "]" in first:
        s = first.find("[")
        e = first.find("]", s)
        if e > s + 1:
            cond = first[s + 1 : e]
    if cond is None and "[" in msg and "]" in msg:
        for line in msg.splitlines()[:5]:
            if "[" in line and "]" in line:
                s = line.find("[")
                e = line.find("]", s)
                if e > s + 1 and line[s + 1 : e].replace(".", "_").replace("_", "").isupper():
                    cond = line[s + 1 : e]
                    break
    return {
        "ok": False,
        "exc": type(exc).__name__,
        "cond": cond,
        "head": first[:200],
    }


def run_read(sess: Any, sql: str) -> dict[str, Any]:
    try:
        rows = [[str(v) for v in r] for r in sess.sql(sql).collect()]
        return {"ok": True, "rows": rows}
    except Exception as exc:
        return err_summary(exc)


def run_overwrite(
    sess: Any, table: str, col_type: str, seed_lit: str, select_sql: str
) -> dict[str, Any]:
    sess.sql(f"DROP TABLE IF EXISTS {table}").collect()
    sess.sql(f"CREATE TABLE {table} (x {col_type}) USING iceberg").collect()
    sess.sql(f"INSERT INTO {table} VALUES ({seed_lit})").collect()
    try:
        sess.sql(f"INSERT OVERWRITE {table} {select_sql}").collect()
        outcome: dict[str, Any] = {"ok": True}
    except Exception as exc:
        outcome = err_summary(exc)
    cnt = sess.sql(f"SELECT count(*) FROM {table}").collect()[0][0]
    outcome["rows_after"] = int(cnt)
    return outcome


def main() -> int:
    engine = sys.argv[1]
    out_path = Path(sys.argv[2])
    base_dir = Path(tempfile.gettempdir())
    wh = tempfile.mkdtemp(prefix=f"wh_p1_{engine}_", dir=str(base_dir))
    sess, _ = build_session(engine, wh)
    results: dict[str, Any] = {}
    try:
        sess.conf.set("spark.sql.ansi.enabled", "true")
        sess.sql("CREATE NAMESPACE IF NOT EXISTS ice.ns").collect()
        sess.sql(
            "CREATE TABLE ice.ns.real_empty (s STRING, d DOUBLE, b BIGINT, dt DATE, ntz "
            "TIMESTAMP_NTZ, flag BOOLEAN) USING iceberg"
        ).collect()
        sess.sql(
            "CREATE TABLE ice.ns.real_bad (s STRING, d DOUBLE, b BIGINT, flag BOOLEAN) USING "
            "iceberg"
        ).collect()
        sess.sql("INSERT INTO ice.ns.real_bad VALUES ('x', 123456.0, 4294967297, true)").collect()
        sess.sql("CREATE OR REPLACE TEMP VIEW tv_str AS SELECT 'x' AS s").collect()
        sess.sql(
            "CREATE OR REPLACE TEMP VIEW tv_dbl AS SELECT CAST(123456.0 AS DOUBLE) AS d"
        ).collect()
        sess.sql(
            "CREATE OR REPLACE TEMP VIEW tv_big AS SELECT CAST(4294967297 AS BIGINT) AS b"
        ).collect()
        sess.sql("CREATE OR REPLACE TEMP VIEW tv_date AS SELECT DATE '2024-01-01' AS dt").collect()
        sess.sql(
            "CREATE OR REPLACE TEMP VIEW tv_ntz AS SELECT TIMESTAMP_NTZ '2024-01-01 00:00:00' AS "
            "ntz"
        ).collect()

        cases: list[tuple[str, str, str, str]] = [
            ("cast_str_double_lit", "DOUBLE", "1.0", "SELECT CAST('x' AS DOUBLE) WHERE 1 = 0"),
            (
                "cast_str_double_subq",
                "DOUBLE",
                "1.0",
                "SELECT CAST(s AS DOUBLE) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            ),
            (
                "cast_str_double_nested_subq",
                "DOUBLE",
                "1.0",
                "SELECT CAST(s AS DOUBLE) FROM (SELECT s FROM (SELECT 'x' AS s) a) b WHERE 1 = 0",
            ),
            (
                "cast_str_double_cte",
                "DOUBLE",
                "1.0",
                "WITH cte AS (SELECT 'x' AS s) SELECT CAST(s AS DOUBLE) FROM cte WHERE 1 = 0",
            ),
            (
                "cast_str_double_tempview",
                "DOUBLE",
                "1.0",
                "SELECT CAST(s AS DOUBLE) FROM tv_str WHERE 1 = 0",
            ),
            (
                "cast_str_double_values1",
                "DOUBLE",
                "1.0",
                "SELECT CAST(s AS DOUBLE) FROM VALUES ('x') AS t(s) WHERE 1 = 0",
            ),
            (
                "cast_str_double_values2",
                "DOUBLE",
                "1.0",
                "SELECT CAST(s AS DOUBLE) FROM VALUES ('x'), ('y') AS t(s) WHERE 1 = 0",
            ),
            (
                "cast_str_double_union",
                "DOUBLE",
                "1.0",
                "SELECT CAST(s AS DOUBLE) FROM (SELECT 'x' AS s UNION ALL SELECT 'y' AS s) src "
                "WHERE 1 = 0",
            ),
            (
                "cast_str_double_expr",
                "DOUBLE",
                "1.0",
                "SELECT CAST(concat(s, 'y') AS DOUBLE) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            ),
            (
                "cast_str_double_limit0",
                "DOUBLE",
                "1.0",
                "SELECT CAST(s AS DOUBLE) FROM (SELECT 'x' AS s) src LIMIT 0",
            ),
            (
                "cast_str_double_inner_false",
                "DOUBLE",
                "1.0",
                "SELECT CAST(s AS DOUBLE) FROM (SELECT 'x' AS s WHERE 1 = 0) src",
            ),
            (
                "cast_str_double_lit_over_empty_tbl",
                "DOUBLE",
                "1.0",
                "SELECT CAST('x' AS DOUBLE) FROM ice.ns.real_empty",
            ),
            (
                "cast_str_double_subq_over_empty_tbl",
                "DOUBLE",
                "1.0",
                "SELECT CAST(s AS DOUBLE) FROM (SELECT 'x' AS s FROM ice.ns.real_empty) src",
            ),
            (
                "cast_str_double_unprojected",
                "DOUBLE",
                "1.0",
                "SELECT 1.0 FROM (SELECT CAST(s AS DOUBLE) AS d FROM (SELECT 'x' AS s) src) q "
                "WHERE 1 = 0",
            ),
            (
                "cast_str_double_named_struct_dead",
                "DOUBLE",
                "1.0",
                "SELECT named_struct('a', 1.0, 'b', CAST('x' AS DOUBLE)).a FROM (SELECT 1) t "
                "WHERE 1 = 0",
            ),
            (
                "cast_str_float_subq",
                "FLOAT",
                "CAST(1.0 AS FLOAT)",
                "SELECT CAST(s AS FLOAT) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            ),
            (
                "cast_str_ts_lit",
                "TIMESTAMP",
                "TIMESTAMP '2024-01-01 00:00:00'",
                "SELECT CAST('x' AS TIMESTAMP) WHERE 1 = 0",
            ),
            (
                "cast_str_ts_subq",
                "TIMESTAMP",
                "TIMESTAMP '2024-01-01 00:00:00'",
                "SELECT CAST(s AS TIMESTAMP) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            ),
            (
                "cast_str_ts_cte",
                "TIMESTAMP",
                "TIMESTAMP '2024-01-01 00:00:00'",
                "WITH cte AS (SELECT 'x' AS s) SELECT CAST(s AS TIMESTAMP) FROM cte WHERE 1 = 0",
            ),
            (
                "cast_str_ts_tempview",
                "TIMESTAMP",
                "TIMESTAMP '2024-01-01 00:00:00'",
                "SELECT CAST(s AS TIMESTAMP) FROM tv_str WHERE 1 = 0",
            ),
            (
                "cast_str_ntz_lit",
                "TIMESTAMP_NTZ",
                "TIMESTAMP_NTZ '2024-01-01 00:00:00'",
                "SELECT CAST('x' AS TIMESTAMP_NTZ) WHERE 1 = 0",
            ),
            (
                "cast_str_ntz_subq",
                "TIMESTAMP_NTZ",
                "TIMESTAMP_NTZ '2024-01-01 00:00:00'",
                "SELECT CAST(s AS TIMESTAMP_NTZ) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            ),
            (
                "cast_str_ntz_cte",
                "TIMESTAMP_NTZ",
                "TIMESTAMP_NTZ '2024-01-01 00:00:00'",
                "WITH cte AS (SELECT 'x' AS s) SELECT CAST(s AS TIMESTAMP_NTZ) FROM cte WHERE 1 "
                "= 0",
            ),
            (
                "cast_str_ntz_tempview",
                "TIMESTAMP_NTZ",
                "TIMESTAMP_NTZ '2024-01-01 00:00:00'",
                "SELECT CAST(s AS TIMESTAMP_NTZ) FROM tv_str WHERE 1 = 0",
            ),
            (
                "cast_dbl_dec_lit",
                "DECIMAL(5, 0)",
                "CAST(1 AS DECIMAL(5, 0))",
                "SELECT CAST(CAST(123456.0 AS DOUBLE) AS DECIMAL(5, 0)) WHERE 1 = 0",
            ),
            (
                "cast_dbl_dec_subq",
                "DECIMAL(5, 0)",
                "CAST(1 AS DECIMAL(5, 0))",
                "SELECT CAST(d AS DECIMAL(5, 0)) FROM (SELECT CAST(123456.0 AS DOUBLE) AS d) src "
                "WHERE 1 = 0",
            ),
            (
                "cast_dbl_dec_cte",
                "DECIMAL(5, 0)",
                "CAST(1 AS DECIMAL(5, 0))",
                "WITH cte AS (SELECT CAST(123456.0 AS DOUBLE) AS d) SELECT CAST(d AS DECIMAL(5, "
                "0)) FROM cte WHERE 1 = 0",
            ),
            (
                "cast_dbl_dec_tempview",
                "DECIMAL(5, 0)",
                "CAST(1 AS DECIMAL(5, 0))",
                "SELECT CAST(d AS DECIMAL(5, 0)) FROM tv_dbl WHERE 1 = 0",
            ),
            (
                "cast_dbl_nan_dec_subq",
                "DECIMAL(5, 0)",
                "CAST(1 AS DECIMAL(5, 0))",
                "SELECT CAST(d AS DECIMAL(5, 0)) FROM (SELECT CAST('NaN' AS DOUBLE) AS d) src "
                "WHERE 1 = 0",
            ),
            (
                "cast_flt_dec_subq",
                "DECIMAL(5, 0)",
                "CAST(1 AS DECIMAL(5, 0))",
                "SELECT CAST(d AS DECIMAL(5, 0)) FROM (SELECT CAST(123456.0 AS FLOAT) AS d) src "
                "WHERE 1 = 0",
            ),
            (
                "cast_str_int_subq",
                "INT",
                "1",
                "SELECT CAST(s AS INT) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            ),
            (
                "cast_str_int_cte",
                "INT",
                "1",
                "WITH cte AS (SELECT 'x' AS s) SELECT CAST(s AS INT) FROM cte WHERE 1 = 0",
            ),
            ("cast_str_int_tempview", "INT", "1", "SELECT CAST(s AS INT) FROM tv_str WHERE 1 = 0"),
            (
                "cast_str_bigint_subq",
                "BIGINT",
                "1L",
                "SELECT CAST(s AS BIGINT) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            ),
            (
                "cast_str_smallint_subq",
                "SMALLINT",
                "CAST(1 AS SMALLINT)",
                "SELECT CAST(s AS SMALLINT) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            ),
            (
                "cast_str_tinyint_subq",
                "TINYINT",
                "CAST(1 AS TINYINT)",
                "SELECT CAST(s AS TINYINT) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            ),
            (
                "cast_str_bool_subq",
                "BOOLEAN",
                "true",
                "SELECT CAST(s AS BOOLEAN) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            ),
            (
                "cast_str_date_subq",
                "DATE",
                "DATE '2024-01-01'",
                "SELECT CAST(s AS DATE) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            ),
            (
                "cast_str_date_tempview",
                "DATE",
                "DATE '2024-01-01'",
                "SELECT CAST(s AS DATE) FROM tv_str WHERE 1 = 0",
            ),
            (
                "cast_str_dec_bad_subq",
                "DECIMAL(5, 0)",
                "CAST(1 AS DECIMAL(5, 0))",
                "SELECT CAST(s AS DECIMAL(5, 0)) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            ),
            (
                "cast_str_dec_ovf_subq",
                "DECIMAL(5, 0)",
                "CAST(1 AS DECIMAL(5, 0))",
                "SELECT CAST(s AS DECIMAL(5, 0)) FROM (SELECT '123456' AS s) src WHERE 1 = 0",
            ),
            (
                "cast_big_int_subq",
                "INT",
                "1",
                "SELECT CAST(b AS INT) FROM (SELECT CAST(4294967297 AS BIGINT) AS b) src WHERE 1 "
                "= 0",
            ),
            ("cast_big_int_tempview", "INT", "1", "SELECT CAST(b AS INT) FROM tv_big WHERE 1 = 0"),
            (
                "cast_big_dec_subq",
                "DECIMAL(5, 0)",
                "CAST(1 AS DECIMAL(5, 0))",
                "SELECT CAST(b AS DECIMAL(5, 0)) FROM (SELECT CAST(123456 AS BIGINT) AS b) src "
                "WHERE 1 = 0",
            ),
            (
                "cast_dec_dec_subq",
                "DECIMAL(5, 0)",
                "CAST(1 AS DECIMAL(5, 0))",
                "SELECT CAST(d AS DECIMAL(5, 0)) FROM (SELECT CAST(123456 AS DECIMAL(10, 0)) AS "
                "d) src WHERE 1 = 0",
            ),
            (
                "cast_dbl_int_subq",
                "INT",
                "1",
                "SELECT CAST(d AS INT) FROM (SELECT CAST(1e20 AS DOUBLE) AS d) src WHERE 1 = 0",
            ),
            (
                "cast_bool_dec_subq",
                "DECIMAL(1, 1)",
                "CAST(0.0 AS DECIMAL(1, 1))",
                "SELECT CAST(b AS DECIMAL(1, 1)) FROM (SELECT true AS b) src WHERE 1 = 0",
            ),
            (
                "cast_map_subq",
                "MAP<STRING, INT>",
                "map('a', 1)",
                "SELECT CAST(m AS MAP<STRING, INT>) FROM (SELECT map('a', 'x') AS m) src WHERE 1 "
                "= 0",
            ),
            (
                "cast_arr_subq",
                "ARRAY<INT>",
                "array(1)",
                "SELECT CAST(a AS ARRAY<INT>) FROM (SELECT array('x') AS a) src WHERE 1 = 0",
            ),
            (
                "cast_struct_subq",
                "STRUCT<a: INT>",
                "named_struct('a', 1)",
                "SELECT CAST(st AS STRUCT<a: INT>) FROM (SELECT named_struct('a', 'x') AS st) "
                "src WHERE 1 = 0",
            ),
            (
                "illegal_date_to_int_subq",
                "INT",
                "1",
                "SELECT CAST(dt AS INT) FROM (SELECT DATE '2024-01-01' AS dt) src WHERE 1 = 0",
            ),
            (
                "illegal_date_to_int_tv",
                "INT",
                "1",
                "SELECT CAST(dt AS INT) FROM tv_date WHERE 1 = 0",
            ),
            (
                "illegal_try_date_to_int_subq",
                "INT",
                "1",
                "SELECT TRY_CAST(dt AS INT) FROM (SELECT DATE '2024-01-01' AS dt) src WHERE 1 = 0",
            ),
            (
                "illegal_int_to_date_subq",
                "DATE",
                "DATE '2024-01-01'",
                "SELECT CAST(n AS DATE) FROM (SELECT 1 AS n) src WHERE 1 = 0",
            ),
            (
                "illegal_ntz_to_bigint_subq",
                "BIGINT",
                "1L",
                "SELECT CAST(ntz AS BIGINT) FROM (SELECT TIMESTAMP_NTZ '2024-01-01 00:00:00' AS "
                "ntz) src WHERE 1 = 0",
            ),
            (
                "illegal_ntz_to_bigint_tv",
                "BIGINT",
                "1L",
                "SELECT CAST(ntz AS BIGINT) FROM tv_ntz WHERE 1 = 0",
            ),
            (
                "illegal_ntz_to_double_subq",
                "DOUBLE",
                "1.0",
                "SELECT CAST(ntz AS DOUBLE) FROM (SELECT TIMESTAMP_NTZ '2024-01-01 00:00:00' AS "
                "ntz) src WHERE 1 = 0",
            ),
            (
                "illegal_ntz_to_dec_subq",
                "DECIMAL(10, 0)",
                "CAST(1 AS DECIMAL(10, 0))",
                "SELECT CAST(ntz AS DECIMAL(10, 0)) FROM (SELECT TIMESTAMP_NTZ '2024-01-01 "
                "00:00:00' AS ntz) src WHERE 1 = 0",
            ),
            (
                "illegal_int_to_ntz_subq",
                "TIMESTAMP_NTZ",
                "TIMESTAMP_NTZ '2024-01-01 00:00:00'",
                "SELECT CAST(n AS TIMESTAMP_NTZ) FROM (SELECT 1 AS n) src WHERE 1 = 0",
            ),
            (
                "dead_case_false_lit",
                "DOUBLE",
                "1.0",
                "SELECT CASE WHEN false THEN CAST('x' AS DOUBLE) ELSE 1.0 END WHERE 1 = 0",
            ),
            (
                "dead_case_false_subq",
                "DOUBLE",
                "1.0",
                "SELECT CASE WHEN false THEN CAST(s AS DOUBLE) END FROM (SELECT 'x' AS s) src "
                "WHERE 1 = 0",
            ),
            (
                "dead_case_true_else_subq",
                "DOUBLE",
                "1.0",
                "SELECT CASE WHEN true THEN 1.0 ELSE CAST(s AS DOUBLE) END FROM (SELECT 'x' AS "
                "s) src WHERE 1 = 0",
            ),
            (
                "dead_case_1eq1_else_subq",
                "DOUBLE",
                "1.0",
                "SELECT CASE WHEN 1 = 1 THEN 1.0 ELSE CAST(s AS DOUBLE) END FROM (SELECT 'x' AS "
                "s) src WHERE 1 = 0",
            ),
            (
                "dead_case_simple_subq",
                "DOUBLE",
                "1.0",
                "SELECT CASE 1 WHEN 1 THEN 1.0 WHEN 2 THEN CAST(s AS DOUBLE) END FROM (SELECT "
                "'x' AS s) src WHERE 1 = 0",
            ),
            (
                "dead_case_live_branch_subq",
                "DOUBLE",
                "1.0",
                "SELECT CASE WHEN true THEN CAST(s AS DOUBLE) ELSE 1.0 END FROM (SELECT 'x' AS "
                "s) src WHERE 1 = 0",
            ),
            (
                "dead_case_col_cond_empty",
                "DOUBLE",
                "1.0",
                "SELECT CASE WHEN flag THEN 1.0 ELSE CAST('x' AS DOUBLE) END FROM "
                "ice.ns.real_empty",
            ),
            (
                "dead_if_false_subq",
                "DOUBLE",
                "1.0",
                "SELECT IF(false, CAST(s AS DOUBLE), 1.0) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            ),
            (
                "dead_if_true_else_subq",
                "DOUBLE",
                "1.0",
                "SELECT IF(true, 1.0, CAST(s AS DOUBLE)) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            ),
            (
                "dead_if_true_then_subq",
                "DOUBLE",
                "1.0",
                "SELECT IF(true, CAST(s AS DOUBLE), 1.0) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            ),
            (
                "dead_coalesce_1_subq",
                "DOUBLE",
                "1.0",
                "SELECT coalesce(1.0, CAST(s AS DOUBLE)) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            ),
            (
                "dead_coalesce_null_subq",
                "DOUBLE",
                "1.0",
                "SELECT coalesce(CAST(NULL AS DOUBLE), CAST(s AS DOUBLE)) FROM (SELECT 'x' AS s) "
                "src WHERE 1 = 0",
            ),
            (
                "dead_where_false_and_subq",
                "DOUBLE",
                "1.0",
                "SELECT 1.0 FROM (SELECT 'x' AS s) src WHERE false AND CAST(s AS DOUBLE) = 1.0",
            ),
            (
                "dead_where_cast_and_false_subq",
                "DOUBLE",
                "1.0",
                "SELECT 1.0 FROM (SELECT 'x' AS s) src WHERE CAST(s AS DOUBLE) = 1.0 AND false",
            ),
            (
                "dead_where_1eq0_and_subq",
                "DOUBLE",
                "1.0",
                "SELECT 1.0 FROM (SELECT 'x' AS s) src WHERE 1 = 0 AND CAST(s AS DOUBLE) = 1.0",
            ),
            (
                "dead_where_cast_and_1eq0_subq",
                "DOUBLE",
                "1.0",
                "SELECT 1.0 FROM (SELECT 'x' AS s) src WHERE CAST(s AS DOUBLE) = 1.0 AND 1 = 0",
            ),
            (
                "dead_where_true_or_subq",
                "DOUBLE",
                "1.0",
                "SELECT 1.0 FROM (SELECT 'x' AS s) src WHERE (true OR CAST(s AS DOUBLE) = 1.0) "
                "AND 1 = 0",
            ),
            (
                "dead_where_cast_or_true_subq",
                "DOUBLE",
                "1.0",
                "SELECT 1.0 FROM (SELECT 'x' AS s) src WHERE (CAST(s AS DOUBLE) = 1.0 OR true) "
                "AND 1 = 0",
            ),
            (
                "dead_case_false_ts_subq",
                "TIMESTAMP",
                "TIMESTAMP '2024-01-01 00:00:00'",
                "SELECT CASE WHEN false THEN CAST(s AS TIMESTAMP) END FROM (SELECT 'x' AS s) src "
                "WHERE 1 = 0",
            ),
            (
                "dead_case_false_ntz_subq",
                "TIMESTAMP_NTZ",
                "TIMESTAMP_NTZ '2024-01-01 00:00:00'",
                "SELECT CASE WHEN false THEN CAST(s AS TIMESTAMP_NTZ) END FROM (SELECT 'x' AS s) "
                "src WHERE 1 = 0",
            ),
            (
                "dead_case_false_dec_subq",
                "DECIMAL(5, 0)",
                "CAST(1 AS DECIMAL(5, 0))",
                "SELECT CASE WHEN false THEN CAST(d AS DECIMAL(5, 0)) END FROM (SELECT "
                "CAST(123456.0 AS DOUBLE) AS d) src WHERE 1 = 0",
            ),
            (
                "dead_case_false_int_subq",
                "INT",
                "1",
                "SELECT CASE WHEN false THEN CAST(s AS INT) END FROM (SELECT 'x' AS s) src WHERE "
                "1 = 0",
            ),
            (
                "ctrl_try_cast_double",
                "DOUBLE",
                "1.0",
                "SELECT TRY_CAST(s AS DOUBLE) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            ),
            (
                "ctrl_try_cast_ts",
                "TIMESTAMP",
                "TIMESTAMP '2024-01-01 00:00:00'",
                "SELECT TRY_CAST(s AS TIMESTAMP) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            ),
            (
                "ctrl_try_cast_ntz",
                "TIMESTAMP_NTZ",
                "TIMESTAMP_NTZ '2024-01-01 00:00:00'",
                "SELECT TRY_CAST(s AS TIMESTAMP_NTZ) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            ),
            (
                "ctrl_try_cast_dbl_dec",
                "DECIMAL(5, 0)",
                "CAST(1 AS DECIMAL(5, 0))",
                "SELECT TRY_CAST(d AS DECIMAL(5, 0)) FROM (SELECT CAST(123456.0 AS DOUBLE) AS d) "
                "src WHERE 1 = 0",
            ),
            (
                "ctrl_try_cast_int",
                "INT",
                "1",
                "SELECT TRY_CAST(s AS INT) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            ),
            (
                "ctrl_valid_double",
                "DOUBLE",
                "1.0",
                "SELECT CAST(s AS DOUBLE) FROM (SELECT '1.5' AS s) src WHERE 1 = 0",
            ),
            (
                "ctrl_valid_ts",
                "TIMESTAMP",
                "TIMESTAMP '2024-01-01 00:00:00'",
                "SELECT CAST(s AS TIMESTAMP) FROM (SELECT '2024-01-01 00:00:00' AS s) src WHERE "
                "1 = 0",
            ),
            (
                "ctrl_valid_ntz",
                "TIMESTAMP_NTZ",
                "TIMESTAMP_NTZ '2024-01-01 00:00:00'",
                "SELECT CAST(s AS TIMESTAMP_NTZ) FROM (SELECT '2024-01-01 00:00:00' AS s) src "
                "WHERE 1 = 0",
            ),
            (
                "ctrl_valid_dbl_dec",
                "DECIMAL(5, 0)",
                "CAST(1 AS DECIMAL(5, 0))",
                "SELECT CAST(d AS DECIMAL(5, 0)) FROM (SELECT CAST(12.0 AS DOUBLE) AS d) src "
                "WHERE 1 = 0",
            ),
            (
                "ctrl_valid_int",
                "INT",
                "1",
                "SELECT CAST(s AS INT) FROM (SELECT '42' AS s) src WHERE 1 = 0",
            ),
            (
                "ctrl_null_double",
                "DOUBLE",
                "1.0",
                "SELECT CAST(s AS DOUBLE) FROM (SELECT CAST(NULL AS STRING) AS s) src WHERE 1 = 0",
            ),
            (
                "ctrl_real_empty_double",
                "DOUBLE",
                "1.0",
                "SELECT CAST(s AS DOUBLE) FROM ice.ns.real_empty",
            ),
            (
                "ctrl_real_empty_ts",
                "TIMESTAMP",
                "TIMESTAMP '2024-01-01 00:00:00'",
                "SELECT CAST(s AS TIMESTAMP) FROM ice.ns.real_empty",
            ),
            (
                "ctrl_real_empty_ntz",
                "TIMESTAMP_NTZ",
                "TIMESTAMP_NTZ '2024-01-01 00:00:00'",
                "SELECT CAST(s AS TIMESTAMP_NTZ) FROM ice.ns.real_empty",
            ),
            (
                "ctrl_real_empty_dbl_dec",
                "DECIMAL(5, 0)",
                "CAST(1 AS DECIMAL(5, 0))",
                "SELECT CAST(d AS DECIMAL(5, 0)) FROM ice.ns.real_empty",
            ),
            ("ctrl_real_empty_int", "INT", "1", "SELECT CAST(s AS INT) FROM ice.ns.real_empty"),
            (
                "ctrl_real_bad_where_false_double",
                "DOUBLE",
                "1.0",
                "SELECT CAST(s AS DOUBLE) FROM ice.ns.real_bad WHERE 1 = 0",
            ),
            (
                "ctrl_real_bad_where_false_int",
                "INT",
                "1",
                "SELECT CAST(s AS INT) FROM ice.ns.real_bad WHERE 1 = 0",
            ),
            (
                "ctrl_real_bad_where_false_big_int",
                "INT",
                "1",
                "SELECT CAST(b AS INT) FROM ice.ns.real_bad WHERE 1 = 0",
            ),
            (
                "ctrl_store_implicit_1e19_empty",
                "BIGINT",
                "1L",
                "SELECT 1e19 FROM ice.ns.real_empty",
            ),
            (
                "ctrl_store_implicit_1e19_subq_false",
                "BIGINT",
                "1L",
                "SELECT d FROM (SELECT 1e19 AS d) src WHERE 1 = 0",
            ),
        ]

        idx = 0
        for ansi in ("true", "false"):
            sess.conf.set("spark.sql.ansi.enabled", ansi)
            for name, col_type, seed_lit, select_sql in cases:
                idx += 1
                read_res = run_read(sess, select_sql)
                ow_res = run_overwrite(sess, f"ice.ns.p1_{idx}", col_type, seed_lit, select_sql)
                results[f"{ansi}|{name}"] = {
                    "sql": select_sql,
                    "read": read_res,
                    "overwrite": ow_res,
                }
        out_path.write_text(json.dumps(results, indent=1), encoding="utf-8")
        print(f"done engine={engine} cells={len(results)}")
    finally:
        sess.stop()
        shutil.rmtree(wh, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

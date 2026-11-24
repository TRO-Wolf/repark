from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark import functions as spark_fn

CAST_INVALID = "[CAST_INVALID_INPUT]"
CAST_OVERFLOW = "[CAST_OVERFLOW]"
NUM_RANGE = "[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]"

OVERWRITE_SHAPE_PINS: tuple[tuple[str, str, str, str, str], ...] = (
    (
        "str_double_subq",
        "DOUBLE",
        "1.0",
        "SELECT CAST(s AS DOUBLE) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_double_nested_subq",
        "DOUBLE",
        "1.0",
        "SELECT CAST(s AS DOUBLE) FROM (SELECT s FROM (SELECT 'x' AS s) a) b WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_double_cte",
        "DOUBLE",
        "1.0",
        "WITH c AS (SELECT 'x' AS s) SELECT CAST(s AS DOUBLE) FROM c WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_double_tempview",
        "DOUBLE",
        "1.0",
        "SELECT CAST(s AS DOUBLE) FROM tv_bad_str WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_double_union",
        "DOUBLE",
        "1.0",
        "SELECT CAST(s AS DOUBLE) FROM (SELECT '1.0' AS s UNION ALL SELECT 'x' AS s) u WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_double_union_real_and_const",
        "DOUBLE",
        "1.0",
        "SELECT CAST(s AS DOUBLE) FROM "
        "(SELECT s FROM local.db.real_empty UNION ALL SELECT 'x' AS s) u WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_double_left_join",
        "DOUBLE",
        "1.0",
        "SELECT CAST(r.s AS DOUBLE) FROM (SELECT 1 AS id) l "
        "LEFT JOIN (SELECT 1 AS id, 'x' AS s) r ON l.id = r.id WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_double_groupby_having",
        "DOUBLE",
        "1.0",
        "SELECT CAST(s AS DOUBLE) FROM (SELECT 'x' AS s) q GROUP BY s HAVING 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_double_expr",
        "DOUBLE",
        "1.0",
        "SELECT CAST(concat(s, 'y') AS DOUBLE) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "try_cast_wrapping_bad_cast",
        "DOUBLE",
        "1.0",
        "SELECT CAST(TRY_CAST(CAST(s AS DOUBLE) AS INT) AS DOUBLE) "
        "FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_float_subq",
        "FLOAT",
        "CAST(1.0 AS FLOAT)",
        "SELECT CAST(s AS FLOAT) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_ts_lit",
        "TIMESTAMP",
        "TIMESTAMP '2024-01-01 00:00:00'",
        "SELECT CAST('x' AS TIMESTAMP) WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_ts_subq",
        "TIMESTAMP",
        "TIMESTAMP '2024-01-01 00:00:00'",
        "SELECT CAST(s AS TIMESTAMP) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_ts_cte",
        "TIMESTAMP",
        "TIMESTAMP '2024-01-01 00:00:00'",
        "WITH c AS (SELECT 'x' AS s) SELECT CAST(s AS TIMESTAMP) FROM c WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_ts_tempview",
        "TIMESTAMP",
        "TIMESTAMP '2024-01-01 00:00:00'",
        "SELECT CAST(s AS TIMESTAMP) FROM tv_bad_str WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_ntz_lit",
        "TIMESTAMP_NTZ",
        "CAST('2024-01-01 00:00:00' AS TIMESTAMP_NTZ)",
        "SELECT CAST('x' AS TIMESTAMP_NTZ) WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_ntz_subq",
        "TIMESTAMP_NTZ",
        "CAST('2024-01-01 00:00:00' AS TIMESTAMP_NTZ)",
        "SELECT CAST(s AS TIMESTAMP_NTZ) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_ntz_cte",
        "TIMESTAMP_NTZ",
        "CAST('2024-01-01 00:00:00' AS TIMESTAMP_NTZ)",
        "WITH c AS (SELECT 'x' AS s) SELECT CAST(s AS TIMESTAMP_NTZ) FROM c WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_ntz_tempview",
        "TIMESTAMP_NTZ",
        "CAST('2024-01-01 00:00:00' AS TIMESTAMP_NTZ)",
        "SELECT CAST(s AS TIMESTAMP_NTZ) FROM tv_bad_str WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "dbl_dec_lit",
        "DECIMAL(5, 0)",
        "CAST(1 AS DECIMAL(5, 0))",
        "SELECT CAST(1e30 AS DECIMAL(5, 0)) WHERE 1 = 0",
        NUM_RANGE,
    ),
    (
        "dbl_dec_subq",
        "DECIMAL(5, 0)",
        "CAST(1 AS DECIMAL(5, 0))",
        "SELECT CAST(d AS DECIMAL(5, 0)) FROM (SELECT 1e30 AS d) src WHERE 1 = 0",
        NUM_RANGE,
    ),
    (
        "dbl_dec_cte",
        "DECIMAL(5, 0)",
        "CAST(1 AS DECIMAL(5, 0))",
        "WITH c AS (SELECT 1e30 AS d) SELECT CAST(d AS DECIMAL(5, 0)) FROM c WHERE 1 = 0",
        NUM_RANGE,
    ),
    (
        "dbl_dec_tempview",
        "DECIMAL(5, 0)",
        "CAST(1 AS DECIMAL(5, 0))",
        "SELECT CAST(d AS DECIMAL(5, 0)) FROM tv_big_dbl WHERE 1 = 0",
        NUM_RANGE,
    ),
    (
        "flt_dec_subq",
        "DECIMAL(5, 0)",
        "CAST(1 AS DECIMAL(5, 0))",
        "SELECT CAST(f AS DECIMAL(5, 0)) FROM (SELECT CAST(1e20 AS FLOAT) AS f) src WHERE 1 = 0",
        NUM_RANGE,
    ),
    (
        "bool_dec_subq",
        "DECIMAL(1, 1)",
        "CAST(0.0 AS DECIMAL(1, 1))",
        "SELECT CAST(b AS DECIMAL(1, 1)) FROM (SELECT true AS b) src WHERE 1 = 0",
        NUM_RANGE,
    ),
    (
        "str_int_subq",
        "INT",
        "1",
        "SELECT CAST(s AS INT) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_bigint_subq",
        "BIGINT",
        "CAST(1 AS BIGINT)",
        "SELECT CAST(s AS BIGINT) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_smallint_subq",
        "SMALLINT",
        "CAST(1 AS SMALLINT)",
        "SELECT CAST(s AS SMALLINT) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_tinyint_subq",
        "TINYINT",
        "CAST(1 AS TINYINT)",
        "SELECT CAST(s AS TINYINT) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_bool_subq",
        "BOOLEAN",
        "true",
        "SELECT CAST(s AS BOOLEAN) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_date_subq",
        "DATE",
        "DATE '2024-01-01'",
        "SELECT CAST(s AS DATE) FROM (SELECT 'not-a-date' AS s) src WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_dec_bad_subq",
        "DECIMAL(10, 2)",
        "CAST(1.00 AS DECIMAL(10, 2))",
        "SELECT CAST(s AS DECIMAL(10, 2)) FROM (SELECT 'not-a-dec' AS s) src WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "str_dec_ovf_subq",
        "DECIMAL(5, 0)",
        "CAST(1 AS DECIMAL(5, 0))",
        "SELECT CAST(s AS DECIMAL(5, 0)) FROM (SELECT '999999' AS s) src WHERE 1 = 0",
        NUM_RANGE,
    ),
    (
        "big_int_subq",
        "INT",
        "1",
        "SELECT CAST(b AS INT) FROM (SELECT 2147483648 AS b) src WHERE 1 = 0",
        CAST_OVERFLOW,
    ),
    (
        "dbl_int_subq",
        "INT",
        "1",
        "SELECT CAST(d AS INT) FROM (SELECT 1e30 AS d) src WHERE 1 = 0",
        CAST_OVERFLOW,
    ),
    (
        "big_dec_subq",
        "DECIMAL(5, 0)",
        "CAST(1 AS DECIMAL(5, 0))",
        "SELECT CAST(b AS DECIMAL(5, 0)) FROM (SELECT 123456 AS b) src WHERE 1 = 0",
        NUM_RANGE,
    ),
    (
        "dec_dec_subq",
        "DECIMAL(5, 0)",
        "CAST(1 AS DECIMAL(5, 0))",
        "SELECT CAST(d AS DECIMAL(5, 0)) FROM (SELECT CAST(123456.00 AS DECIMAL(10, 2)) AS d) "
        "src WHERE 1 = 0",
        NUM_RANGE,
    ),
    (
        "map_cast_subq",
        "MAP<STRING, INT>",
        "map('a', 1)",
        "SELECT CAST(map('a', s) AS MAP<STRING, INT>) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "arr_cast_subq",
        "ARRAY<INT>",
        "array(1)",
        "SELECT CAST(array(s) AS ARRAY<INT>) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "struct_cast_subq",
        "STRUCT<a: INT>",
        "named_struct('a', 1)",
        "SELECT CAST(named_struct('a', s) AS STRUCT<a: INT>) "
        "FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "live_case_branch_subq",
        "DOUBLE",
        "1.0",
        "SELECT CASE WHEN true THEN CAST(s AS DOUBLE) ELSE 1.0 END "
        "FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "live_if_branch_subq",
        "DOUBLE",
        "1.0",
        "SELECT IF(true, CAST(s AS DOUBLE), 1.0) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        CAST_INVALID,
    ),
    (
        "live_coalesce_null_subq",
        "DOUBLE",
        "1.0",
        "SELECT coalesce(CAST(NULL AS DOUBLE), CAST(s AS DOUBLE)) "
        "FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        CAST_INVALID,
    ),
)

SHAPES: dict[str, dict[str, str]] = {
    "str_double": {
        "col_type": "DOUBLE",
        "seed": "1.0",
        "select_x": "SELECT CAST(s AS DOUBLE) AS x FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        "select_px": "SELECT 1 AS p, CAST(s AS DOUBLE) AS x FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        "cast_expr": "CAST('x' AS DOUBLE)",
        "valid_expr": "2.0",
        "expected": CAST_INVALID,
    },
    "str_ts": {
        "col_type": "TIMESTAMP",
        "seed": "TIMESTAMP '2024-01-01 00:00:00'",
        "select_x": "SELECT CAST(s AS TIMESTAMP) AS x FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        "select_px": "SELECT 1 AS p, CAST(s AS TIMESTAMP) AS x "
        "FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        "cast_expr": "CAST('x' AS TIMESTAMP)",
        "valid_expr": "TIMESTAMP '2024-01-02 00:00:00'",
        "expected": CAST_INVALID,
    },
    "str_ntz": {
        "col_type": "TIMESTAMP_NTZ",
        "seed": "CAST('2024-01-01 00:00:00' AS TIMESTAMP_NTZ)",
        "select_x": "SELECT CAST(s AS TIMESTAMP_NTZ) AS x FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        "select_px": "SELECT 1 AS p, CAST(s AS TIMESTAMP_NTZ) AS x "
        "FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        "cast_expr": "CAST('x' AS TIMESTAMP_NTZ)",
        "valid_expr": "CAST('2024-01-02 00:00:00' AS TIMESTAMP_NTZ)",
        "expected": CAST_INVALID,
    },
    "dbl_dec": {
        "col_type": "DECIMAL(5, 0)",
        "seed": "CAST(1 AS DECIMAL(5, 0))",
        "select_x": "SELECT CAST(d AS DECIMAL(5, 0)) AS x FROM (SELECT 1e30 AS d) src WHERE 1 = 0",
        "select_px": "SELECT 1 AS p, CAST(d AS DECIMAL(5, 0)) AS x "
        "FROM (SELECT 1e30 AS d) src WHERE 1 = 0",
        "cast_expr": "CAST(1e30 AS DECIMAL(5, 0))",
        "valid_expr": "CAST(2 AS DECIMAL(5, 0))",
        "expected": NUM_RANGE,
    },
    "str_int": {
        "col_type": "INT",
        "seed": "1",
        "select_x": "SELECT CAST(s AS INT) AS x FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        "select_px": "SELECT 1 AS p, CAST(s AS INT) AS x FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        "cast_expr": "CAST('x' AS INT)",
        "valid_expr": "2",
        "expected": CAST_INVALID,
    },
}

ALL_WRITE_DOORS: tuple[str, ...] = (
    "insert_overwrite",
    "insert_into",
    "insert_overwrite_by_name",
    "insert_overwrite_static_part",
    "insert_overwrite_dyn_part",
    "ctas",
    "rtas",
    "crtas",
    "writeTo_overwrite",
    "writeTo_overwritePartitions",
    "writeTo_append",
    "writeTo_create",
    "writeTo_replace",
    "saveAsTable_overwrite",
    "insertInto_overwrite",
    "insertInto_append",
    "parquet_overwrite",
    "merge_in_source",
    "merge_in_action_empty_src",
    "merge_on_false_and",
    "merge_on_cond",
    "update_where_1eq0_and",
    "update_where_cast_and_1eq0",
    "update_where_cast",
    "delete_where_1eq0_and",
    "delete_where_cast_and_1eq0",
    "delete_where_cast",
)

DOOR_FIXED_PINS: tuple[tuple[str, str], ...] = tuple(
    (shape, door)
    for shape in ("str_double", "str_ts", "str_ntz", "dbl_dec", "str_int")
    for door in ALL_WRITE_DOORS
    if not (
        shape == "str_double"
        and door
        in (
            "merge_in_action_empty_src",
            "merge_on_false_and",
            "merge_on_cond",
            "update_where_1eq0_and",
            "update_where_cast_and_1eq0",
            "update_where_cast",
            "delete_where_1eq0_and",
            "delete_where_cast_and_1eq0",
            "delete_where_cast",
        )
    )
    and not (
        shape in ("str_ts", "str_ntz", "str_int")
        and door in ("merge_on_cond", "update_where_cast", "delete_where_cast")
    )
)

ANSI_OFF_DOOR_CONTROLS: tuple[tuple[str, str], ...] = tuple(
    (shape, door)
    for shape in ("str_double", "str_ts", "str_ntz", "dbl_dec")
    for door in (
        "insert_overwrite",
        "insert_into",
        "insert_overwrite_by_name",
        "insert_overwrite_static_part",
        "insert_overwrite_dyn_part",
        "ctas",
        "rtas",
        "crtas",
        "writeTo_overwrite",
        "writeTo_overwritePartitions",
        "writeTo_append",
        "writeTo_create",
        "writeTo_replace",
        "saveAsTable_overwrite",
        "insertInto_overwrite",
        "insertInto_append",
        "parquet_overwrite",
        "merge_in_source",
        "merge_in_action_empty_src",
        "update_where_1eq0_and",
        "delete_where_1eq0_and",
    )
)

ANSI_ON_CONTROL_SOURCES: tuple[tuple[str, str, str, str, str], ...] = (
    (
        "dead_case_false_double",
        "DOUBLE",
        "1.0",
        "INSERT OVERWRITE {t} SELECT CASE WHEN false THEN CAST(s AS DOUBLE) ELSE 1.0 END "
        "FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        "0",
    ),
    (
        "dead_case_true_else_double",
        "DOUBLE",
        "1.0",
        "INSERT OVERWRITE {t} SELECT CASE WHEN true THEN 1.0 ELSE CAST(s AS DOUBLE) END "
        "FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        "0",
    ),
    (
        "dead_if_false_double",
        "DOUBLE",
        "1.0",
        "INSERT OVERWRITE {t} SELECT IF(false, CAST(s AS DOUBLE), 1.0) "
        "FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        "0",
    ),
    (
        "dead_coalesce_const_double",
        "DOUBLE",
        "1.0",
        "INSERT OVERWRITE {t} SELECT coalesce(1.0, CAST(s AS DOUBLE)) "
        "FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        "0",
    ),
    (
        "dead_where_false_and_ts",
        "TIMESTAMP",
        "TIMESTAMP '2024-01-01 00:00:00'",
        "INSERT OVERWRITE {t} SELECT TIMESTAMP '2024-01-01 00:00:00' FROM (SELECT 'x' AS s) src "
        "WHERE false AND CAST(s AS TIMESTAMP) = TIMESTAMP '2024-01-01 00:00:00'",
        "0",
    ),
    (
        "dead_where_true_or_ts",
        "TIMESTAMP",
        "TIMESTAMP '2024-01-01 00:00:00'",
        "INSERT OVERWRITE {t} SELECT TIMESTAMP '2024-01-01 00:00:00' FROM (SELECT 'x' AS s) src "
        "WHERE (true OR CAST(s AS TIMESTAMP) = TIMESTAMP '2024-01-01 00:00:00') AND 1 = 0",
        "0",
    ),
    (
        "limit0_ts",
        "TIMESTAMP",
        "TIMESTAMP '2024-01-01 00:00:00'",
        "INSERT OVERWRITE {t} SELECT CAST('x' AS TIMESTAMP) LIMIT 0",
        "0",
    ),
    (
        "unprojected_ts",
        "TIMESTAMP",
        "TIMESTAMP '2024-01-01 00:00:00'",
        "INSERT OVERWRITE {t} SELECT TIMESTAMP '2024-01-01 00:00:00' FROM "
        "(SELECT CAST(s AS TIMESTAMP) AS d FROM (SELECT 'x' AS s) a) q WHERE 1 = 0",
        "0",
    ),
    (
        "values_local_relation_double",
        "DOUBLE",
        "1.0",
        "INSERT OVERWRITE {t} SELECT CAST(s AS DOUBLE) FROM VALUES ('x') AS v(s) WHERE 1 = 0",
        "0",
    ),
    (
        "try_cast_double",
        "DOUBLE",
        "1.0",
        "INSERT OVERWRITE {t} SELECT TRY_CAST(s AS DOUBLE) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        "0",
    ),
    (
        "valid_cast_double",
        "DOUBLE",
        "1.0",
        "INSERT OVERWRITE {t} SELECT CAST(s AS DOUBLE) FROM (SELECT '1.5' AS s) src WHERE 1 = 0",
        "0",
    ),
    (
        "real_empty_double",
        "DOUBLE",
        "1.0",
        "INSERT OVERWRITE {t} SELECT CAST(s AS DOUBLE) FROM local.db.real_empty",
        "0",
    ),
    (
        "nan_to_decimal_insert_into",
        "DECIMAL(5, 0)",
        "CAST(1 AS DECIMAL(5, 0))",
        "INSERT INTO {t} SELECT CAST(d AS DECIMAL(5, 0)) "
        "FROM (SELECT CAST('NaN' AS DOUBLE) AS d) src WHERE 1 = 0",
        "1",
    ),
    (
        "inf_to_decimal_insert_into",
        "DECIMAL(5, 0)",
        "CAST(1 AS DECIMAL(5, 0))",
        "INSERT INTO {t} SELECT CAST(d AS DECIMAL(5, 0)) "
        "FROM (SELECT CAST('Infinity' AS DOUBLE) AS d) src WHERE 1 = 0",
        "1",
    ),
    (
        "update_set_where_false_ts",
        "TIMESTAMP",
        "TIMESTAMP '2024-01-01 00:00:00'",
        "UPDATE {t} SET x = CAST('x' AS TIMESTAMP) WHERE 1 = 0",
        "1",
    ),
)


@pytest.fixture
def iocast_session(tmp_path: Path) -> Iterator[tuple[ReparkSession, Path]]:
    session = (
        ReparkSession.builder.appName("insert-overwrite-const-cast-1")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", str(tmp_path / "wh"))
        .getOrCreate()
    )
    session.sql("CREATE NAMESPACE IF NOT EXISTS local.db").collect()
    session.sql("CREATE TABLE local.db.real_empty (s STRING) USING iceberg").collect()
    session.sql("CREATE OR REPLACE TEMP VIEW tv_bad_str AS SELECT 'x' AS s").collect()
    session.sql("CREATE OR REPLACE TEMP VIEW tv_big_dbl AS SELECT 1e30 AS d").collect()
    yield session, tmp_path
    session.stop()


def _create_flat_table(session: ReparkSession, name: str, col_type: str, seed: str | None) -> str:
    fq = f"local.db.{name}"
    session.sql(f"DROP TABLE IF EXISTS {fq}").collect()
    session.sql(f"CREATE TABLE {fq} (x {col_type}) USING iceberg").collect()
    if seed is not None:
        session.sql(f"INSERT INTO {fq} VALUES ({seed})").collect()
    return fq


def _create_part_table(session: ReparkSession, name: str, col_type: str, seed: str) -> str:
    fq = f"local.db.{name}"
    session.sql(f"DROP TABLE IF EXISTS {fq}").collect()
    session.sql(
        f"CREATE TABLE {fq} (p INT, x {col_type}) USING iceberg PARTITIONED BY (p)"
    ).collect()
    session.sql(f"INSERT INTO {fq} VALUES (1, {seed}), (2, {seed})").collect()
    return fq


def _table_exists(session: ReparkSession, fq: str) -> bool:
    try:
        session.sql(f"SELECT * FROM {fq}").collect()
    except Exception:
        return False
    return True


def _row_count(session: ReparkSession, fq: str) -> int:
    return session.sql(f"SELECT * FROM {fq}").toArrow().num_rows


def _run_write_door(
    session: ReparkSession,
    root: Path,
    door: str,
    shape: dict[str, str],
    tag: str,
) -> tuple[str | None, Path | None, int]:
    col_type = shape["col_type"]
    seed = shape["seed"]
    select_x = shape["select_x"]
    select_px = shape["select_px"]
    cast_expr = shape["cast_expr"]
    valid_expr = shape["valid_expr"]

    if door == "parquet_overwrite":
        out_dir = root / f"pq_{tag}"
        session.sql(f"SELECT {seed} AS x").write.mode("overwrite").parquet(str(out_dir))
        session.sql(select_x).write.mode("overwrite").parquet(str(out_dir))
        return None, out_dir, 1

    if door in ("ctas", "writeTo_create"):
        fq = f"local.db.t_{tag}"
        session.sql(f"DROP TABLE IF EXISTS {fq}").collect()
        if door == "ctas":
            session.sql(f"CREATE TABLE {fq} USING iceberg AS {select_x}").collect()
        else:
            session.sql(select_x).writeTo(fq).create()
        return fq, None, 0

    if door in (
        "insert_overwrite_static_part",
        "insert_overwrite_dyn_part",
        "writeTo_overwritePartitions",
    ):
        fq = _create_part_table(session, f"t_{tag}", col_type, seed)
        if door == "insert_overwrite_static_part":
            session.sql(f"INSERT OVERWRITE {fq} PARTITION (p = 1) {select_x}").collect()
        elif door == "insert_overwrite_dyn_part":
            session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            try:
                session.sql(f"INSERT OVERWRITE {fq} {select_px}").collect()
            finally:
                session.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
        else:
            session.sql(select_px).writeTo(fq).overwritePartitions()
        return fq, None, 2

    fq = _create_flat_table(session, f"t_{tag}", col_type, seed)
    if door == "insert_overwrite":
        session.sql(f"INSERT OVERWRITE {fq} {select_x}").collect()
    elif door == "insert_into":
        session.sql(f"INSERT INTO {fq} {select_x}").collect()
    elif door == "insert_overwrite_by_name":
        session.sql(f"INSERT OVERWRITE {fq} BY NAME {select_x}").collect()
    elif door == "rtas":
        session.sql(f"REPLACE TABLE {fq} USING iceberg AS {select_x}").collect()
    elif door == "crtas":
        session.sql(f"CREATE OR REPLACE TABLE {fq} USING iceberg AS {select_x}").collect()
    elif door == "writeTo_overwrite":
        session.sql(select_x).writeTo(fq).overwrite(spark_fn.lit(True))
    elif door == "writeTo_append":
        session.sql(select_x).writeTo(fq).append()
    elif door == "writeTo_replace":
        session.sql(select_x).writeTo(fq).replace()
    elif door == "saveAsTable_overwrite":
        session.sql(select_x).write.mode("overwrite").saveAsTable(fq)
    elif door == "insertInto_overwrite":
        session.sql(select_x).write.insertInto(fq, overwrite=True)
    elif door == "insertInto_append":
        session.sql(select_x).write.insertInto(fq, overwrite=False)
    elif door == "merge_in_source":
        session.sql(
            f"MERGE INTO {fq} t USING ({select_x}) s ON t.x = s.x WHEN MATCHED THEN DELETE"
        ).collect()
    elif door == "merge_in_action_empty_src":
        session.sql(
            f"MERGE INTO {fq} t USING (SELECT {seed} AS x WHERE 1 = 0) s "
            f"ON t.x = s.x WHEN MATCHED THEN UPDATE SET x = {cast_expr}"
        ).collect()
    elif door == "merge_on_false_and":
        session.sql(
            f"MERGE INTO {fq} t USING (SELECT {seed} AS x) s "
            f"ON false AND t.x = {cast_expr} WHEN MATCHED THEN DELETE"
        ).collect()
    elif door == "merge_on_cond":
        session.sql(
            f"MERGE INTO {fq} t USING (SELECT {seed} AS x) s "
            f"ON t.x = {cast_expr} WHEN MATCHED THEN DELETE"
        ).collect()
    elif door == "update_where_1eq0_and":
        session.sql(f"UPDATE {fq} SET x = {valid_expr} WHERE 1 = 0 AND x = {cast_expr}").collect()
    elif door == "update_where_cast_and_1eq0":
        session.sql(f"UPDATE {fq} SET x = {valid_expr} WHERE x = {cast_expr} AND 1 = 0").collect()
    elif door == "update_where_cast":
        session.sql(f"UPDATE {fq} SET x = {valid_expr} WHERE x = {cast_expr}").collect()
    elif door == "delete_where_1eq0_and":
        session.sql(f"DELETE FROM {fq} WHERE 1 = 0 AND x = {cast_expr}").collect()
    elif door == "delete_where_cast_and_1eq0":
        session.sql(f"DELETE FROM {fq} WHERE x = {cast_expr} AND 1 = 0").collect()
    elif door == "delete_where_cast":
        session.sql(f"DELETE FROM {fq} WHERE x = {cast_expr}").collect()
    else:
        raise ValueError(door)
    return fq, None, 1


def _prepare_target(
    session: ReparkSession,
    root: Path,
    door: str,
    shape: dict[str, str],
    tag: str,
) -> tuple[str | None, Path | None, int]:
    if door == "parquet_overwrite":
        return None, root / f"pq_{tag}", 1
    if door in ("ctas", "writeTo_create"):
        return f"local.db.t_{tag}", None, 0
    if door in (
        "insert_overwrite_static_part",
        "insert_overwrite_dyn_part",
        "writeTo_overwritePartitions",
    ):
        return f"local.db.t_{tag}", None, 2
    return f"local.db.t_{tag}", None, 1


@pytest.mark.parametrize(
    ("name", "col_type", "seed", "source_sql", "expected_class"),
    OVERWRITE_SHAPE_PINS,
    ids=[row[0] for row in OVERWRITE_SHAPE_PINS],
)
def test_overwrite_const_cast_refuses_and_preserves_rows(
    iocast_session: tuple[ReparkSession, Path],
    name: str,
    col_type: str,
    seed: str,
    source_sql: str,
    expected_class: str,
) -> None:
    session, _ = iocast_session
    session.conf.set("spark.sql.ansi.enabled", "true")
    fq = _create_flat_table(session, f"ow_{name}", col_type, seed)
    with pytest.raises(Exception) as exc_info:
        session.sql(f"INSERT OVERWRITE {fq} {source_sql}").collect()
    assert expected_class in str(exc_info.value)
    assert _row_count(session, fq) == 1


@pytest.mark.parametrize(
    ("shape_name", "door"),
    DOOR_FIXED_PINS,
    ids=[f"{shape}_{door}" for shape, door in DOOR_FIXED_PINS],
)
def test_write_door_const_cast_refuses_before_mutating(
    iocast_session: tuple[ReparkSession, Path],
    shape_name: str,
    door: str,
) -> None:
    session, root = iocast_session
    session.conf.set("spark.sql.ansi.enabled", "true")
    shape = SHAPES[shape_name]
    tag = f"fix_{shape_name}_{door}".lower()
    fq, out_dir, expected_rows = _prepare_target(session, root, door, shape, tag)
    with pytest.raises(Exception) as exc_info:
        _run_write_door(session, root, door, shape, tag)
    assert shape["expected"] in str(exc_info.value)
    if out_dir is not None:
        assert session.read.parquet(str(out_dir)).toArrow().num_rows == expected_rows
    elif fq is not None:
        if expected_rows == 0:
            assert not _table_exists(session, fq)
        else:
            assert _row_count(session, fq) == expected_rows


@pytest.mark.parametrize(
    ("shape_name", "door"),
    ANSI_OFF_DOOR_CONTROLS,
    ids=[f"off_{shape}_{door}" for shape, door in ANSI_OFF_DOOR_CONTROLS],
)
def test_ansi_off_allows_empty_const_cast_writes(
    iocast_session: tuple[ReparkSession, Path],
    shape_name: str,
    door: str,
) -> None:
    session, root = iocast_session
    session.conf.set("spark.sql.ansi.enabled", "false")
    shape = SHAPES[shape_name]
    tag = f"off_{shape_name}_{door}".lower()
    fq, out_dir, _ = _run_write_door(session, root, door, shape, tag)
    if out_dir is not None:
        assert session.read.parquet(str(out_dir)).toArrow().num_rows == 0
    elif fq is not None:
        assert _table_exists(session, fq)


@pytest.mark.parametrize(
    ("name", "col_type", "seed", "statement_tpl", "expected_rows"),
    ANSI_ON_CONTROL_SOURCES,
    ids=[row[0] for row in ANSI_ON_CONTROL_SOURCES],
)
def test_ansi_on_dead_branches_and_controls_succeed(
    iocast_session: tuple[ReparkSession, Path],
    name: str,
    col_type: str,
    seed: str,
    statement_tpl: str,
    expected_rows: str,
) -> None:
    session, _ = iocast_session
    session.conf.set("spark.sql.ansi.enabled", "true")
    fq = _create_flat_table(session, f"ctrl_{name}", col_type, seed)
    session.sql(statement_tpl.format(t=fq)).collect()
    assert _row_count(session, fq) == int(expected_rows)


def test_ansi_capture_across_temp_views_and_dataframes(
    iocast_session: tuple[ReparkSession, Path],
) -> None:
    session, _ = iocast_session
    session.conf.set("spark.sql.ansi.enabled", "false")
    session.sql("CREATE OR REPLACE TEMP VIEW tv_cap_off_src AS SELECT 'x' AS s").collect()
    session.sql(
        "CREATE OR REPLACE TEMP VIEW tv_cap_off_cast AS "
        "SELECT CAST(s AS DOUBLE) AS x FROM (SELECT 'x' AS s) q WHERE 1 = 0"
    ).collect()
    df_off: Any = session.sql("SELECT CAST(s AS DOUBLE) AS x FROM (SELECT 'x' AS s) q WHERE 1 = 0")

    session.conf.set("spark.sql.ansi.enabled", "true")
    df_on: Any = session.sql("SELECT CAST(s AS DOUBLE) AS x FROM (SELECT 'x' AS s) q WHERE 1 = 0")

    fq1 = _create_flat_table(session, "cap_tv_off_cast_on", "DOUBLE", "1.0")
    with pytest.raises(Exception) as exc1:
        session.sql(
            f"INSERT OVERWRITE {fq1} SELECT CAST(s AS DOUBLE) FROM tv_cap_off_src WHERE 1 = 0"
        ).collect()
    assert CAST_INVALID in str(exc1.value)
    assert _row_count(session, fq1) == 1

    fq2 = _create_flat_table(session, "cap_tv_cast_off_on", "DOUBLE", "1.0")
    session.sql(f"INSERT OVERWRITE {fq2} SELECT x FROM tv_cap_off_cast").collect()
    assert _row_count(session, fq2) == 0

    fq3 = _create_flat_table(session, "cap_df_off_on", "DOUBLE", "1.0")
    df_off.writeTo(fq3).overwrite(spark_fn.lit(True))
    assert _row_count(session, fq3) == 0

    session.conf.set("spark.sql.ansi.enabled", "false")
    fq4 = _create_flat_table(session, "cap_df_on_off", "DOUBLE", "1.0")
    with pytest.raises(Exception) as exc4:
        df_on.writeTo(fq4).overwrite(spark_fn.lit(True))
    assert CAST_INVALID in str(exc4.value)
    assert _row_count(session, fq4) == 1

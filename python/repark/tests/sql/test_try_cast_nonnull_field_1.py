from __future__ import annotations

import datetime
import math
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from _try_cast_nonnull_field_1_cells import ANSI_MODES, SWEEP_TRY_CAST_CELLS

from repark._native import PyReparkSession
from repark.spark import SparkSession
from repark.spark import functions as sf
from repark.spark import types as st
from repark.spark.session import _reset_active_session_for_tests

pytestmark = pytest.mark.xdist_group("try_cast_nonnull_field_1")


def normalize_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Inf" if value > 0 else "-Inf"
        return round(value, 6)
    if isinstance(value, Decimal):
        return f"Dec({value})"
    if isinstance(value, (datetime.datetime, datetime.date, datetime.timedelta)):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    if isinstance(value, (list, tuple)):
        return [normalize_value(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): normalize_value(val)
            for key, val in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    return str(value)


def normalize_arrow_value(value: Any, arrow_type: pa.DataType) -> Any:
    if value is None:
        return None
    if pa.types.is_map(arrow_type):
        return {
            str(normalize_arrow_value(key, arrow_type.key_type)): normalize_arrow_value(
                val, arrow_type.item_type
            )
            for key, val in sorted(value, key=lambda pair: str(pair[0]))
        }
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return [normalize_arrow_value(item, arrow_type.value_type) for item in value]
    if pa.types.is_struct(arrow_type) and isinstance(value, dict):
        return {
            field.name: normalize_arrow_value(value.get(field.name), field.type)
            for field in arrow_type
        }
    return normalize_value(value)


def normalize_type(type_text: str) -> str:
    return type_text.lower().replace(" ", "")


def sort_rows(rows: list[list[Any]]) -> list[list[Any]]:
    return sorted(
        rows,
        key=lambda row: [(0, "") if cell is None else (1, repr(cell)) for cell in row],
    )


def capture_frame(fn: Callable[[], Any]) -> dict[str, Any]:
    try:
        df = fn()
        schema = df.schema
        types = [normalize_type(field.dataType.simpleString()) for field in schema.fields]
        dtypes = [normalize_type(dtype) for _, dtype in df.dtypes]
        nulls = [bool(field.nullable) for field in schema.fields]
        rows = [[normalize_value(cell) for cell in row] for row in df.collect()]
        table = df.to_arrow()
        arrow_nulls = [bool(field.nullable) for field in table.schema]
        arrow_rows = [
            [
                normalize_arrow_value(table.column(col)[row].as_py(), table.schema.field(col).type)
                for col in range(table.num_columns)
            ]
            for row in range(table.num_rows)
        ]
        return {
            "ok": True,
            "types": types,
            "dtypes": dtypes,
            "nulls": nulls,
            "rows": rows,
            "arrow_nulls": arrow_nulls,
            "arrow_rows": arrow_rows,
        }
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def capture_native(session: PyReparkSession, sql: str) -> dict[str, Any]:
    try:
        table = pa.table(session.sql(sql))
        nulls = [bool(field.nullable) for field in table.schema]
        rows = [
            [
                normalize_arrow_value(table.column(col)[row].as_py(), table.schema.field(col).type)
                for col in range(table.num_columns)
            ]
            for row in range(table.num_rows)
        ]
        return {"ok": True, "nulls": nulls, "rows": rows}
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def assert_captured_matches(
    cell: dict[str, Any],
    expected_rows: list[list[Any]],
    expected_types: list[str],
    expected_nulls: list[bool],
    label: str,
    *,
    unordered: bool = False,
) -> None:
    assert cell["ok"], f"{label}: {cell.get('error')}"
    assert cell["types"] == expected_types, f"{label}: types {cell['types']} != {expected_types}"
    assert cell["dtypes"] == expected_types, f"{label}: dtypes {cell['dtypes']} != {expected_types}"
    assert cell["nulls"] == expected_nulls, f"{label}: nulls {cell['nulls']} != {expected_nulls}"
    actual_rows = sort_rows(cell["rows"]) if unordered else cell["rows"]
    target_rows = sort_rows(expected_rows) if unordered else expected_rows
    assert actual_rows == target_rows, f"{label}: collect {actual_rows} != {target_rows}"
    assert cell["arrow_nulls"] == expected_nulls, (
        f"{label}: arrow nulls {cell['arrow_nulls']} != {expected_nulls}"
    )
    actual_arrow_rows = sort_rows(cell["arrow_rows"]) if unordered else cell["arrow_rows"]
    assert actual_arrow_rows == target_rows, (
        f"{label}: arrow rows {actual_arrow_rows} != {target_rows}"
    )


def build_session(warehouse: Path, ansi: str) -> SparkSession:
    _reset_active_session_for_tests()
    return (
        SparkSession.builder.appName(f"try-cast-nonnull-field-1-{ansi}")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.ansi.enabled", ansi)
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", str(warehouse))
        .getOrCreate()
    )


def write_parquet_fixtures(root: Path) -> tuple[Path, Path]:
    root.mkdir(parents=True, exist_ok=True)
    single_schema = pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("b", pa.int64(), nullable=False),
            pa.field("s", pa.string(), nullable=False),
            pa.field("s_ok", pa.string(), nullable=False),
            pa.field("s_dt", pa.string(), nullable=False),
        ]
    )
    single_tbl = pa.table(
        {
            "id": [1, 2],
            "b": [4294967297, 1],
            "s": ["abc", "7"],
            "s_ok": ["11", "7"],
            "s_dt": ["notadt", "2024-01-15"],
        },
        schema=single_schema,
    )
    single_path = root / "nn_single.parquet"
    pq.write_table(single_tbl, str(single_path))

    prune_dir = root / "nn_prune"
    prune_dir.mkdir(parents=True, exist_ok=True)
    prune_schema = pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("k_int", pa.int32(), nullable=False),
            pa.field("s", pa.string(), nullable=False),
        ]
    )
    parts = [
        ("part0.parquet", [1, 2], [1, 2], ["1", "2"]),
        ("part1.parquet", [3, 4], [100, 101], ["abc", "101"]),
        ("part2.parquet", [5, 6], [1000, 1001], ["1000", "1001"]),
    ]
    for filename, ids, keys, strings in parts:
        table = pa.table({"id": ids, "k_int": keys, "s": strings}, schema=prune_schema)
        pq.write_table(table, str(prune_dir / filename))
    return single_path, prune_dir


def seed_tables(spark: SparkSession, pq_single: Path, pq_prune: Path) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.sw").collect()
    spark.sql(
        """
        CREATE TABLE ice.sw.nn (
            id INT NOT NULL,
            i INT NOT NULL,
            b BIGINT NOT NULL,
            s STRING NOT NULL,
            s_bool STRING NOT NULL,
            s_ts STRING NOT NULL,
            s_dt STRING NOT NULL,
            s_map STRING NOT NULL,
            d DOUBLE NOT NULL,
            d_nan DOUBLE NOT NULL,
            i_max INT NOT NULL,
            ts_old TIMESTAMP NOT NULL
        ) USING iceberg
        """
    ).collect()
    spark.sql(
        """
        INSERT INTO ice.sw.nn VALUES
            (1, 10, 4294967297, 'abc', 'notabool', 'notats', 'notadt', 'a:1,b:2',
             1.5, CAST('NaN' AS DOUBLE), 2147483647, TIMESTAMP '1500-06-15 12:00:00'),
            (2, 7, 1, '7', 'true', '2024-01-01 02:30:00', '2024-01-15', 'k:9',
             2.25, 1.5, 1, TIMESTAMP '2024-01-01 02:30:00')
        """
    ).collect()
    spark.sql(
        "CREATE TABLE ice.sw.nn_ok (id INT NOT NULL, s_ok STRING NOT NULL) USING iceberg"
    ).collect()
    spark.sql("INSERT INTO ice.sw.nn_ok VALUES (1, '11'), (2, '7')").collect()
    spark.sql(
        "CREATE TABLE ice.sw.prune (id INT NOT NULL, k_int INT NOT NULL, s STRING NOT NULL) "
        "USING iceberg"
    ).collect()
    spark.sql("INSERT INTO ice.sw.prune VALUES (1, 1, '1'), (2, 2, '2')").collect()
    spark.sql("INSERT INTO ice.sw.prune VALUES (3, 100, 'abc'), (4, 101, '101')").collect()
    spark.sql("INSERT INTO ice.sw.prune VALUES (5, 1000, '1000'), (6, 1001, '1001')").collect()
    spark.read.parquet(str(pq_single)).createOrReplaceTempView("pq_nn")
    spark.read.parquet(str(pq_prune)).createOrReplaceTempView("pq_prune")
    mem_schema = st.StructType(
        [
            st.StructField("id", st.IntegerType(), False),
            st.StructField("s", st.StringType(), False),
        ]
    )
    spark.createDataFrame([(1, "abc"), (2, "7")], schema=mem_schema).createOrReplaceTempView(
        "mem_nn"
    )
    spark.sql("CREATE OR REPLACE TEMP VIEW tv_nn AS SELECT id, s FROM ice.sw.nn").collect()


DOOR_SQL_CASES: tuple[tuple[str, str, list[list[Any]], list[str], list[bool]], ...] = (
    (
        "inline_union_abs",
        "SELECT abs(TRY_CAST(s AS INT)) AS r FROM (SELECT 'x' AS s UNION ALL SELECT '7')",
        [[None], [7]],
        ["int"],
        [True],
    ),
    (
        "inline_union_add1",
        "SELECT TRY_CAST(s AS INT) + 1 AS r FROM (SELECT 'x' AS s UNION ALL SELECT '7')",
        [[None], [8]],
        ["int"],
        [True],
    ),
    (
        "cast_shield_eq",
        "SELECT id, TRY_CAST(b AS INT) = 1 AS r FROM ice.sw.nn ORDER BY id",
        [[1, None], [2, True]],
        ["int", "boolean"],
        [False, True],
    ),
    (
        "hof_transform_abs",
        "SELECT id, transform(array(1, 2), e -> abs(TRY_CAST(s AS INT)) + e) AS r "
        "FROM ice.sw.nn ORDER BY id",
        [[1, [None, None]], [2, [8, 9]]],
        ["int", "array<int>"],
        [False, False],
    ),
    (
        "join_on_abs_try",
        "SELECT a.id, a.r FROM (SELECT id, abs(TRY_CAST(s AS INT)) AS r FROM ice.sw.nn) a "
        "LEFT JOIN (SELECT 7 AS k) b ON a.r = b.k ORDER BY a.id",
        [[1, None], [2, 7]],
        ["int", "int"],
        [False, True],
    ),
    (
        "union_abs_try",
        "SELECT id, abs(TRY_CAST(s AS INT)) AS r FROM ice.sw.nn "
        "UNION ALL SELECT 3 AS id, 9 AS r ORDER BY id",
        [[1, None], [2, 7], [3, 9]],
        ["int", "int"],
        [False, True],
    ),
    (
        "merged_proj_map_limit",
        "SELECT id, map('k', x)['k'] AS r "
        "FROM (SELECT id, TRY_CAST(s AS INT) AS x FROM ice.sw.nn LIMIT 10) q ORDER BY id",
        [[1, None], [2, 7]],
        ["int", "int"],
        [False, True],
    ),
    (
        "merged_proj_map_where",
        "SELECT id, map('k', x)['k'] AS r "
        "FROM (SELECT id, TRY_CAST(s AS INT) AS x FROM ice.sw.nn) q WHERE id > 0 ORDER BY id",
        [[1, None], [2, 7]],
        ["int", "int"],
        [False, True],
    ),
    (
        "parquet_coalesce_abs",
        "SELECT id, abs(TRY_CAST(coalesce(s, 'x') AS INT)) AS r FROM pq_nn ORDER BY id",
        [[1, None], [2, 7]],
        ["int", "int"],
        [True, True],
    ),
    (
        "mem_coalesce_abs",
        "SELECT id, abs(TRY_CAST(coalesce(s, 'x') AS INT)) AS r FROM mem_nn ORDER BY id",
        [[1, None], [2, 7]],
        ["int", "int"],
        [True, True],
    ),
)


def build_select_expr_abs(spark: SparkSession) -> Any:
    return spark.table("ice.sw.nn").selectExpr("id", "abs(TRY_CAST(s AS INT)) AS r").orderBy("id")


def build_select_expr_add1(spark: SparkSession) -> Any:
    return spark.table("ice.sw.nn").selectExpr("id", "TRY_CAST(s AS INT) + 1 AS r").orderBy("id")


def build_select_expr_len(spark: SparkSession) -> Any:
    return (
        spark.table("ice.sw.nn")
        .selectExpr("id", "length(TRY_CAST(TRY_CAST(s AS INT) AS STRING)) AS r")
        .orderBy("id")
    )


def build_fexpr_abs(spark: SparkSession) -> Any:
    return (
        spark.table("ice.sw.nn")
        .select("id", sf.expr("abs(TRY_CAST(s AS INT))").alias("r"))
        .orderBy("id")
    )


def build_fexpr_add1(spark: SparkSession) -> Any:
    return (
        spark.table("ice.sw.nn")
        .select("id", sf.expr("TRY_CAST(s AS INT) + 1").alias("r"))
        .orderBy("id")
    )


def build_fexpr_date_add(spark: SparkSession) -> Any:
    return (
        spark.table("ice.sw.nn")
        .select("id", sf.expr("date_add(TRY_CAST(s_dt AS DATE), 1)").alias("r"))
        .orderBy("id")
    )


def build_col_try_cast_abs(spark: SparkSession) -> Any:
    return (
        spark.table("ice.sw.nn")
        .select("id", sf.abs(sf.col("s").try_cast("int")).alias("r"))
        .orderBy("id")
    )


def build_col_try_cast_add1(spark: SparkSession) -> Any:
    return (
        spark.table("ice.sw.nn")
        .select("id", (sf.col("s").try_cast("int") + sf.lit(1)).alias("r"))
        .orderBy("id")
    )


def build_col_try_cast_len(spark: SparkSession) -> Any:
    return (
        spark.table("ice.sw.nn")
        .select("id", sf.length(sf.col("s").try_cast("int").try_cast("string")).alias("r"))
        .orderBy("id")
    )


def build_col_try_cast_neg_abs(spark: SparkSession) -> Any:
    return (
        spark.table("ice.sw.nn")
        .select("id", sf.abs(-sf.col("s").try_cast("int")).alias("r"))
        .orderBy("id")
    )


def build_col_try_cast_pq_coalesce(spark: SparkSession) -> Any:
    return (
        spark.table("pq_nn")
        .select("id", sf.abs(sf.coalesce(sf.col("s"), sf.lit("x")).try_cast("int")).alias("r"))
        .orderBy("id")
    )


def build_col_try_cast_mem_coalesce(spark: SparkSession) -> Any:
    return (
        spark.table("mem_nn")
        .select("id", sf.abs(sf.coalesce(sf.col("s"), sf.lit("x")).try_cast("int")).alias("r"))
        .orderBy("id")
    )


DF_DOOR_CASES: tuple[
    tuple[str, Callable[[SparkSession], Any], list[list[Any]], list[str], list[bool]],
    ...,
] = (
    ("selectExpr_abs", build_select_expr_abs, [[1, None], [2, 7]], ["int", "int"], [False, True]),
    ("selectExpr_add1", build_select_expr_add1, [[1, None], [2, 8]], ["int", "int"], [False, True]),
    ("selectExpr_len", build_select_expr_len, [[1, None], [2, 1]], ["int", "int"], [False, True]),
    ("fexpr_abs", build_fexpr_abs, [[1, None], [2, 7]], ["int", "int"], [False, True]),
    ("fexpr_add1", build_fexpr_add1, [[1, None], [2, 8]], ["int", "int"], [False, True]),
    (
        "fexpr_date_add",
        build_fexpr_date_add,
        [[1, None], [2, "2024-01-16"]],
        ["int", "date"],
        [False, True],
    ),
    (
        "col_try_cast_abs",
        build_col_try_cast_abs,
        [[1, None], [2, 7]],
        ["int", "int"],
        [False, True],
    ),
    (
        "col_try_cast_add1",
        build_col_try_cast_add1,
        [[1, None], [2, 8]],
        ["int", "int"],
        [False, True],
    ),
    (
        "col_try_cast_len",
        build_col_try_cast_len,
        [[1, None], [2, 1]],
        ["int", "int"],
        [False, True],
    ),
    (
        "col_try_cast_neg_abs",
        build_col_try_cast_neg_abs,
        [[1, None], [2, 7]],
        ["int", "int"],
        [False, True],
    ),
    (
        "col_try_cast_pq_coalesce",
        build_col_try_cast_pq_coalesce,
        [[1, None], [2, 7]],
        ["int", "int"],
        [True, True],
    ),
    (
        "col_try_cast_mem_coalesce",
        build_col_try_cast_mem_coalesce,
        [[1, None], [2, 7]],
        ["int", "int"],
        [True, True],
    ),
)

NATIVE_CASES: tuple[tuple[str, str, list[list[Any]], list[bool]], ...] = (
    (
        "mem_round_try_dbl",
        "SELECT id, round(TRY_CAST(s AS DOUBLE), 1) AS r FROM nn_mem ORDER BY id",
        [[1, None], [2, 7.0]],
        [False, True],
    ),
    (
        "mem_round_neg_try_dbl",
        "SELECT id, round(-TRY_CAST(s AS DOUBLE), 1) AS r FROM nn_mem ORDER BY id",
        [[1, None], [2, -7.0]],
        [False, True],
    ),
    (
        "mem_date_part_try_dt",
        "SELECT id, date_part('year', TRY_CAST(s_dt AS DATE)) AS r FROM nn_mem ORDER BY id",
        [[1, None], [2, 2024]],
        [False, True],
    ),
    (
        "mem_strpos_try_str",
        "SELECT id, strpos(TRY_CAST(TRY_CAST(s AS INT) AS VARCHAR), '7') AS r "
        "FROM nn_mem ORDER BY id",
        [[1, None], [2, 1]],
        [False, True],
    ),
    (
        "mem_cast_shield_eq",
        "SELECT id, TRY_CAST(b AS INT) = 1 AS r FROM nn_mem ORDER BY id",
        [[1, None], [2, True]],
        [False, True],
    ),
    (
        "pq_coalesce_round",
        "SELECT id, round(TRY_CAST(coalesce(s, 'x') AS DOUBLE), 1) AS r FROM pq_nat ORDER BY id",
        [[1, None], [2, 7.0]],
        [True, True],
    ),
    (
        "pq_coalesce_date_part",
        "SELECT id, date_part('year', TRY_CAST(coalesce(s_dt, 'x') AS DATE)) AS r "
        "FROM pq_nat ORDER BY id",
        [[1, None], [2, 2024]],
        [True, True],
    ),
)


def measure_writes(spark: SparkSession, ansi: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    tag = f"w_{ansi}"
    spark.sql(f"CREATE TABLE ice.sw.dst_{tag} (id INT NOT NULL, r INT) USING iceberg").collect()
    try:
        spark.sql(
            f"INSERT INTO ice.sw.dst_{tag} SELECT id, abs(TRY_CAST(s AS INT)) FROM ice.sw.nn"
        ).collect()
        out["insert_nullable"] = capture_frame(
            lambda: spark.sql(f"SELECT id, r FROM ice.sw.dst_{tag} ORDER BY id")
        )
    except Exception as exc:
        out["insert_nullable"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

    for cname, expr in (
        ("ctas_abs", "abs(TRY_CAST(s AS INT))"),
        ("ctas_add", "TRY_CAST(s AS INT) + 1"),
    ):
        try:
            spark.sql(
                f"CREATE TABLE ice.sw.{cname}_{tag} USING iceberg AS "
                f"SELECT id, {expr} AS r FROM ice.sw.nn"
            ).collect()
            out[cname] = capture_frame(
                lambda n=cname: spark.sql(f"SELECT id, r FROM ice.sw.{n}_{tag} ORDER BY id")
            )
        except Exception as exc:
            out[cname] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

    spark.sql(
        f"CREATE TABLE ice.sw.dst_nn_ok_{tag} (id INT NOT NULL, r INT NOT NULL) USING iceberg"
    ).collect()
    try:
        spark.sql(
            f"INSERT INTO ice.sw.dst_nn_ok_{tag} "
            "SELECT id, abs(TRY_CAST(s_ok AS INT)) FROM ice.sw.nn_ok"
        ).collect()
        out["insert_nonnull_valid"] = capture_frame(
            lambda: spark.sql(f"SELECT id, r FROM ice.sw.dst_nn_ok_{tag} ORDER BY id")
        )
    except Exception as exc:
        out["insert_nonnull_valid"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

    spark.sql(
        f"CREATE TABLE ice.sw.dst_nn_fail_{tag} (id INT NOT NULL, r INT NOT NULL) USING iceberg"
    ).collect()
    try:
        spark.sql(
            f"INSERT INTO ice.sw.dst_nn_fail_{tag} "
            "SELECT id, abs(TRY_CAST(s AS INT)) FROM ice.sw.nn"
        ).collect()
        out["insert_nonnull_with_null_error"] = ""
    except Exception as exc:
        out["insert_nonnull_with_null_error"] = f"{type(exc).__name__}: {exc}"

    spark.sql(
        f"CREATE TABLE ice.sw.upd_{tag} (id INT NOT NULL, s STRING NOT NULL, r INT) USING iceberg"
    ).collect()
    spark.sql(f"INSERT INTO ice.sw.upd_{tag} VALUES (1, 'abc', 0), (2, '7', 0)").collect()
    try:
        spark.sql(f"UPDATE ice.sw.upd_{tag} SET r = abs(TRY_CAST(s AS INT))").collect()
        out["update"] = capture_frame(
            lambda: spark.sql(f"SELECT id, r FROM ice.sw.upd_{tag} ORDER BY id")
        )
    except Exception as exc:
        out["update"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

    spark.sql(f"CREATE TABLE ice.sw.mrg_{tag} (id INT NOT NULL, r INT) USING iceberg").collect()
    spark.sql(f"INSERT INTO ice.sw.mrg_{tag} VALUES (1, 0), (2, 0)").collect()
    try:
        spark.sql(
            f"MERGE INTO ice.sw.mrg_{tag} t USING ice.sw.nn s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET r = abs(TRY_CAST(s.s AS INT))"
        ).collect()
        out["merge"] = capture_frame(
            lambda: spark.sql(f"SELECT id, r FROM ice.sw.mrg_{tag} ORDER BY id")
        )
    except Exception as exc:
        out["merge"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    return out


def measure_pushdown(spark: SparkSession) -> dict[str, Any]:
    out: dict[str, Any] = {}
    ice_sqls = (
        ("ice_control", "SELECT id FROM ice.sw.prune WHERE k_int > 500 ORDER BY id", True),
        (
            "ice_try_widening",
            "SELECT id FROM ice.sw.prune WHERE TRY_CAST(k_int AS BIGINT) > 500 ORDER BY id",
            True,
        ),
        (
            "ice_compound",
            "SELECT id FROM ice.sw.prune "
            "WHERE TRY_CAST(k_int AS BIGINT) > 500 AND abs(TRY_CAST(s AS INT)) > 500 ORDER BY id",
            False,
        ),
    )
    for key, sql, unwrapped in ice_sqls:
        out[key] = {
            "cell": capture_frame(lambda q=sql: spark.sql(q)),
            "explain": "\n".join(str(row[1]) for row in spark.sql(f"EXPLAIN {sql}").collect()),
            "unwrapped": unwrapped,
        }

    pq_sqls = (
        ("pq_control", "SELECT id FROM pq_prune WHERE k_int > 500 ORDER BY id", [[5], [6]], True),
        (
            "pq_try_widening",
            "SELECT id FROM pq_prune WHERE TRY_CAST(k_int AS BIGINT) > 500 ORDER BY id",
            [[5], [6]],
            True,
        ),
        (
            "pq_compound",
            "SELECT id FROM pq_prune "
            "WHERE TRY_CAST(k_int AS BIGINT) > 500 AND abs(TRY_CAST(s AS INT)) > 500 ORDER BY id",
            [[5], [6]],
            False,
        ),
        (
            "pq_dynamic_join",
            "SELECT a.id FROM (SELECT id, TRY_CAST(k_int AS BIGINT) AS kb FROM pq_prune) a "
            "JOIN (SELECT CAST(1001 AS BIGINT) AS kb) b ON a.kb = b.kb ORDER BY a.id",
            [[6]],
            True,
        ),
    )
    for key, sql, expected, unwrapped in pq_sqls:
        out[key] = {
            "cell": capture_frame(lambda q=sql: spark.sql(q)),
            "expected": expected,
            "explain": "\n".join(str(row[1]) for row in spark.sql(f"EXPLAIN {sql}").collect()),
            "analyze": "\n".join(
                str(row[1]) for row in spark.sql(f"EXPLAIN ANALYZE {sql}").collect()
            ),
            "unwrapped": unwrapped,
        }
    return out


def measure_mode(root: Path, ansi: str) -> dict[str, Any]:
    pq_single, pq_prune = write_parquet_fixtures(root / f"pq_{ansi}")
    spark = build_session(root / f"wh_{ansi}", ansi)
    try:
        seed_tables(spark, pq_single, pq_prune)
        sweep = {
            cell_id: capture_frame(lambda q=sql: spark.sql(q))
            for cell_id, sql, _, _, _ in SWEEP_TRY_CAST_CELLS
        }
        sql_edges = {
            case_id: capture_frame(lambda q=sql: spark.sql(q))
            for case_id, sql, _, _, _ in DOOR_SQL_CASES
        }
        df_doors = {
            case_id: capture_frame(lambda fn=builder: fn(spark))
            for case_id, builder, _, _, _ in DF_DOOR_CASES
        }

        other = "true" if ansi == "false" else "false"
        spark.sql(f"SET spark.sql.ansi.enabled = {other}").collect()
        tv_cross = capture_frame(
            lambda: spark.sql(
                "SELECT id FROM tv_nn WHERE map('k', TRY_CAST(s AS INT))['k'] > 0 ORDER BY id"
            )
        )
        spark.sql(f"SET spark.sql.ansi.enabled = {ansi}").collect()

        try:
            ea_rows = spark.sql(
                "EXPLAIN ANALYZE SELECT id, abs(TRY_CAST(s AS INT)) AS r FROM ice.sw.nn ORDER BY id"
            ).collect()
            explain_analyze: dict[str, Any] = {"ok": bool(ea_rows)}
        except Exception as exc:
            explain_analyze = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

        writes = measure_writes(spark, ansi)
        pushdown = measure_pushdown(spark)
        return {
            "sweep": sweep,
            "sql_edges": sql_edges,
            "df_doors": df_doors,
            "tv_cross": tv_cross,
            "explain_analyze": explain_analyze,
            "writes": writes,
            "pushdown": pushdown,
        }
    finally:
        spark.stop()
        _reset_active_session_for_tests()


def measure_native(root: Path) -> dict[str, Any]:
    pq_single, pq_prune = write_parquet_fixtures(root / "nat_pq")
    session = PyReparkSession.native()
    session.register_arrow_stream_as_temp_view("nn_mem", pq.read_table(str(pq_single)))
    session.create_or_replace_temp_view("pq_nat", session.read_parquet(str(pq_single)))
    session.create_or_replace_temp_view("pq_prune_nat", session.read_parquet(str(pq_prune)))
    cells = {case_id: capture_native(session, sql) for case_id, sql, _, _ in NATIVE_CASES}
    explain_tbl = pa.table(
        session.sql(
            "EXPLAIN SELECT id FROM pq_prune_nat WHERE TRY_CAST(k_int AS BIGINT) > 500 ORDER BY id"
        )
    )
    explain_text = "\n".join(
        str(explain_tbl.column(1)[row].as_py()) for row in range(explain_tbl.num_rows)
    )
    return {"cells": cells, "explain": explain_text}


@pytest.fixture(scope="module")
def measured(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    root = tmp_path_factory.mktemp("try_cast_nonnull_field_1")
    return {
        "modes": {ansi: measure_mode(root, ansi) for ansi in ANSI_MODES},
        "native": measure_native(root),
    }


@pytest.mark.parametrize("ansi", ANSI_MODES, ids=lambda mode: f"ansi={mode}")
@pytest.mark.parametrize(
    ("cell_id", "_sql", "expected_rows", "expected_types", "expected_nulls"),
    SWEEP_TRY_CAST_CELLS,
    ids=[row[0] for row in SWEEP_TRY_CAST_CELLS],
)
def test_sweep_try_cast_cells(
    measured: dict[str, Any],
    ansi: str,
    cell_id: str,
    _sql: str,
    expected_rows: list[list[Any]],
    expected_types: list[str],
    expected_nulls: list[bool],
) -> None:
    assert_captured_matches(
        measured["modes"][ansi]["sweep"][cell_id],
        expected_rows,
        expected_types,
        expected_nulls,
        f"ansi={ansi}:{cell_id}",
    )


UNORDERED_DOOR_SQL_CASES: frozenset[str] = frozenset({"inline_union_abs", "inline_union_add1"})


@pytest.mark.parametrize("ansi", ANSI_MODES, ids=lambda mode: f"ansi={mode}")
@pytest.mark.parametrize(
    ("case_id", "_sql", "expected_rows", "expected_types", "expected_nulls"),
    DOOR_SQL_CASES,
    ids=[row[0] for row in DOOR_SQL_CASES],
)
def test_sql_edge_and_source_doors(
    measured: dict[str, Any],
    ansi: str,
    case_id: str,
    _sql: str,
    expected_rows: list[list[Any]],
    expected_types: list[str],
    expected_nulls: list[bool],
) -> None:
    assert_captured_matches(
        measured["modes"][ansi]["sql_edges"][case_id],
        expected_rows,
        expected_types,
        expected_nulls,
        f"ansi={ansi}:{case_id}",
        unordered=case_id in UNORDERED_DOOR_SQL_CASES,
    )


@pytest.mark.parametrize("ansi", ANSI_MODES, ids=lambda mode: f"ansi={mode}")
@pytest.mark.parametrize(
    ("case_id", "_builder", "expected_rows", "expected_types", "expected_nulls"),
    DF_DOOR_CASES,
    ids=[row[0] for row in DF_DOOR_CASES],
)
def test_dataframe_doors(
    measured: dict[str, Any],
    ansi: str,
    case_id: str,
    _builder: Callable[[SparkSession], Any],
    expected_rows: list[list[Any]],
    expected_types: list[str],
    expected_nulls: list[bool],
) -> None:
    assert_captured_matches(
        measured["modes"][ansi]["df_doors"][case_id],
        expected_rows,
        expected_types,
        expected_nulls,
        f"ansi={ansi}:{case_id}",
    )


@pytest.mark.parametrize("ansi", ANSI_MODES, ids=lambda mode: f"ansi={mode}")
def test_temp_view_cross_ansi_and_explain_analyze(
    measured: dict[str, Any],
    ansi: str,
) -> None:
    mode = measured["modes"][ansi]
    assert_captured_matches(
        mode["tv_cross"],
        [[2]],
        ["int"],
        [False],
        f"ansi={ansi}:tv_cross_map",
    )
    assert mode["explain_analyze"]["ok"], mode["explain_analyze"]


@pytest.mark.parametrize("ansi", ANSI_MODES, ids=lambda mode: f"ansi={mode}")
def test_iceberg_writes_insert_ctas_update_merge(
    measured: dict[str, Any],
    ansi: str,
) -> None:
    writes = measured["modes"][ansi]["writes"]
    assert_captured_matches(
        writes["insert_nullable"], [[1, None], [2, 7]], ["int", "int"], [False, True], "insert"
    )
    assert_captured_matches(
        writes["ctas_abs"], [[1, None], [2, 7]], ["int", "int"], [True, True], "ctas_abs"
    )
    assert_captured_matches(
        writes["ctas_add"], [[1, None], [2, 8]], ["int", "int"], [True, True], "ctas_add"
    )
    assert_captured_matches(
        writes["insert_nonnull_valid"],
        [[1, 11], [2, 7]],
        ["int", "int"],
        [False, False],
        "insert_nonnull_valid",
    )
    assert "non-nullable" in writes["insert_nonnull_with_null_error"]
    assert_captured_matches(
        writes["update"], [[1, None], [2, 7]], ["int", "int"], [False, True], "update"
    )
    assert_captured_matches(
        writes["merge"], [[1, None], [2, 7]], ["int", "int"], [False, True], "merge"
    )


@pytest.mark.parametrize("ansi", ANSI_MODES, ids=lambda mode: f"ansi={mode}")
def test_pushdown_pruning_and_explain_hygiene(
    measured: dict[str, Any],
    ansi: str,
) -> None:
    pushdown = measured["modes"][ansi]["pushdown"]
    for key in ("ice_control", "ice_try_widening", "ice_compound"):
        entry = pushdown[key]
        assert_captured_matches(entry["cell"], [[5], [6]], ["int"], [False], key)
        assert "N=1" in entry["explain"], f"Iceberg pruning lost for {key}:\n{entry['explain']}"
        if entry["unwrapped"]:
            assert "__repark_nullable_field__" not in entry["explain"], entry["explain"]
    for key in ("pq_control", "pq_try_widening", "pq_compound", "pq_dynamic_join"):
        entry = pushdown[key]
        assert_captured_matches(entry["cell"], entry["expected"], ["int"], [True], key)
        assert "files_ranges_pruned_statistics=3 total → 1 matched" in entry["analyze"], (
            f"Parquet pruning lost for {key}:\n{entry['analyze']}"
        )
        if entry["unwrapped"]:
            assert "__repark_nullable_field__" not in entry["explain"], entry["explain"]


@pytest.mark.parametrize(
    ("case_id", "_sql", "expected_rows", "expected_nulls"),
    NATIVE_CASES,
    ids=[row[0] for row in NATIVE_CASES],
)
def test_native_door_try_cast(
    measured: dict[str, Any],
    case_id: str,
    _sql: str,
    expected_rows: list[list[Any]],
    expected_nulls: list[bool],
) -> None:
    cell = measured["native"]["cells"][case_id]
    assert cell["ok"], f"{case_id}: {cell.get('error')}"
    assert cell["nulls"] == expected_nulls, f"{case_id}: {cell['nulls']} != {expected_nulls}"
    assert cell["rows"] == expected_rows, f"{case_id}: {cell['rows']} != {expected_rows}"
    assert "__repark_nullable_field__" not in measured["native"]["explain"]

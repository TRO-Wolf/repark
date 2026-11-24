from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import tempfile
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


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
    if isinstance(value, (list, tuple)):
        return [normalize_value(item) for item in value]
    if isinstance(value, dict):
        return {
            str(k): normalize_value(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))
        }
    return str(value)


def normalize_type(type_text: str) -> str:
    return type_text.lower().replace(" ", "")


def first_line(exc: BaseException) -> str:
    text = str(exc).strip()
    if not text:
        return type(exc).__name__
    return text.splitlines()[0][:240]


def capture_df(df: Any, is_spark: bool) -> dict[str, Any]:
    try:
        schema = df.schema
        dtypes = [[name, normalize_type(dtype)] for name, dtype in df.dtypes]
        schema_fields = [
            {
                "name": field.name,
                "type": normalize_type(field.dataType.simpleString()),
                "nullable": bool(field.nullable),
            }
            for field in schema.fields
        ]
    except Exception as exc:
        return {
            "kind": "plan_error",
            "error_type": type(exc).__name__,
            "first": first_line(exc),
        }

    try:
        rows = [[normalize_value(cell) for cell in row] for row in df.collect()]
        collect_res: dict[str, Any] = {"ok": True, "rows": rows}
    except Exception as exc:
        collect_res = {
            "ok": False,
            "error_type": type(exc).__name__,
            "first": first_line(exc),
        }

    arrow_res: dict[str, Any] | None = None
    if not is_spark and hasattr(df, "to_arrow"):
        try:
            table = df.to_arrow()
            arrow_res = {
                "ok": True,
                "nulls": [bool(field.nullable) for field in table.schema],
                "types": [str(field.type) for field in table.schema],
                "rows": [
                    [normalize_value(table.column(c)[r].as_py()) for c in range(table.num_columns)]
                    for r in range(table.num_rows)
                ],
            }
        except Exception as exc:
            arrow_res = {
                "ok": False,
                "error_type": type(exc).__name__,
                "first": first_line(exc),
            }

    if collect_res["ok"] and (arrow_res is None or arrow_res["ok"]):
        kind = "ok"
    elif (not collect_res["ok"]) and (arrow_res is None or not arrow_res["ok"]):
        kind = "exec_error"
    else:
        kind = "split_error"

    return {
        "kind": kind,
        "dtypes": dtypes,
        "schema_fields": schema_fields,
        "collect": collect_res,
        "arrow": arrow_res,
    }


def capture_call(fn: Any, is_spark: bool) -> dict[str, Any]:
    try:
        df = fn()
    except Exception as exc:
        return {
            "kind": "plan_error",
            "error_type": type(exc).__name__,
            "first": first_line(exc),
        }
    return capture_df(df, is_spark)


def capture_explain(spark: Any, sql: str, is_spark: bool) -> dict[str, Any]:
    if is_spark:
        try:
            rows = spark.sql(f"EXPLAIN EXTENDED {sql}").collect()
            plan_text = "\n".join(str(row[0]) for row in rows)
            return {"ok": True, "plan": plan_text}
        except Exception as exc:
            return {"ok": False, "first": first_line(exc)}
    try:
        rows = spark.sql(f"EXPLAIN {sql}").collect()
        plan_lines = [f"{row[0]}:\n{row[1]}" for row in rows]
        plan_text = "\n".join(plan_lines)
    except Exception as exc:
        return {"ok": False, "first": first_line(exc)}
    analyze_text = ""
    try:
        a_rows = spark.sql(f"EXPLAIN ANALYZE {sql}").collect()
        analyze_text = "\n".join(f"{row[0]}:\n{row[1]}" for row in a_rows)
    except Exception as exc:
        analyze_text = f"ERROR: {first_line(exc)}"
    return {"ok": True, "plan": plan_text, "analyze": analyze_text}


def make_spark_session(engine: str, warehouse: Path, ansi: str) -> tuple[Any, Any, Any]:
    if engine == "spark":
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as sf
        from pyspark.sql import types as st

        jar = os.environ["SPARK_ICEBERG_JAR"]
        spark = (
            SparkSession.builder.master("local[2]")
            .appName(f"trycast-step0-{ansi}")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "America/New_York")
            .config("spark.sql.ansi.enabled", ansi)
            .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.ice.type", "hadoop")
            .config("spark.sql.catalog.ice.warehouse", str(warehouse))
            .config("spark.jars", jar)
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        return spark, sf, st

    from repark import ReparkSession
    from repark.spark import functions as sf
    from repark.spark import types as st
    from repark.spark.session import _reset_active_session_for_tests

    _reset_active_session_for_tests()
    spark = (
        ReparkSession.builder.appName(f"trycast-step0-{ansi}")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.ansi.enabled", ansi)
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", str(warehouse))
        .getOrCreate()
    )
    return spark, sf, st


def write_parquet_fixtures(root: Path) -> tuple[Path, Path]:
    root.mkdir(parents=True, exist_ok=True)
    single_schema = pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("i", pa.int32(), nullable=False),
            pa.field("b", pa.int64(), nullable=False),
            pa.field("s", pa.string(), nullable=False),
            pa.field("s_ok", pa.string(), nullable=False),
            pa.field("s_dt", pa.string(), nullable=False),
            pa.field("s_bool", pa.string(), nullable=False),
            pa.field("d_nan", pa.float64(), nullable=False),
            pa.field("i_max", pa.int32(), nullable=False),
        ]
    )
    single_tbl = pa.table(
        {
            "id": [1, 2],
            "i": [10, 7],
            "b": [4294967297, 1],
            "s": ["abc", "7"],
            "s_ok": ["11", "7"],
            "s_dt": ["notadt", "2024-01-15"],
            "s_bool": ["notabool", "true"],
            "d_nan": [float("nan"), 1.5],
            "i_max": [2147483647, 1],
        },
        schema=single_schema,
    )
    single_path = root / "nn_single.parquet"
    pq.write_table(single_tbl, str(single_path))

    prune_dir = root / "nn_prune_parquet"
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
    for fname, ids, kints, ss in parts:
        tbl = pa.table({"id": ids, "k_int": kints, "s": ss}, schema=prune_schema)
        pq.write_table(tbl, str(prune_dir / fname))
    return single_path, prune_dir


def setup_iceberg_tables(spark: Any) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.tc")
    for tbl in (
        "nn",
        "prune",
        "dst_null",
        "dst_nn_ok",
        "dst_nn_fail",
        "ctas_abs",
        "ctas_add",
        "ctas_ok",
        "upd_tgt",
        "mrg_tgt",
    ):
        spark.sql(f"DROP TABLE IF EXISTS ice.tc.{tbl}")

    spark.sql(
        """
        CREATE TABLE ice.tc.nn (
            id INT NOT NULL,
            i INT NOT NULL,
            b BIGINT NOT NULL,
            s STRING NOT NULL,
            s_ok STRING NOT NULL,
            s_dt STRING NOT NULL,
            s_bool STRING NOT NULL,
            d_nan DOUBLE NOT NULL,
            i_max INT NOT NULL
        ) USING iceberg
        """
    )
    spark.sql(
        """
        INSERT INTO ice.tc.nn VALUES
            (
                1, 10, 4294967297, 'abc', '11', 'notadt', 'notabool',
                CAST('NaN' AS DOUBLE), 2147483647
            ),
            (2, 7, 1, '7', '7', '2024-01-15', 'true', 1.5, 1)
        """
    )

    spark.sql(
        """
        CREATE TABLE ice.tc.prune (
            id INT NOT NULL,
            k_int INT NOT NULL,
            s STRING NOT NULL
        ) USING iceberg
        """
    )
    spark.sql("INSERT INTO ice.tc.prune VALUES (1, 1, '1'), (2, 2, '2')")
    spark.sql("INSERT INTO ice.tc.prune VALUES (3, 100, 'abc'), (4, 101, '101')")
    spark.sql("INSERT INTO ice.tc.prune VALUES (5, 1000, '1000'), (6, 1001, '1001')")


def run_mode(engine: str, ansi: str, root: Path) -> dict[str, Any]:
    is_spark = engine == "spark"
    wh = root / f"wh_{ansi}"
    pq_single, pq_prune = write_parquet_fixtures(root / f"pq_{ansi}")
    spark, sf, st = make_spark_session(engine, wh, ansi)
    out: dict[str, Any] = {
        "has_F_try_cast": hasattr(sf, "try_cast"),
        "cells": {},
        "pushdown": {},
        "writes": {},
    }
    try:
        setup_iceberg_tables(spark)
        ice_df = spark.table("ice.tc.nn")
        pq_df = spark.read.parquet(str(pq_single))
        pq_df.createOrReplaceTempView("pq_nn")
        pq_prune_df = spark.read.parquet(str(pq_prune))
        pq_prune_df.createOrReplaceTempView("pq_prune")

        mem_schema = st.StructType(
            [
                st.StructField("id", st.IntegerType(), False),
                st.StructField("i", st.IntegerType(), False),
                st.StructField("b", st.LongType(), False),
                st.StructField("s", st.StringType(), False),
                st.StructField("s_ok", st.StringType(), False),
                st.StructField("s_dt", st.StringType(), False),
                st.StructField("s_bool", st.StringType(), False),
                st.StructField("d_nan", st.DoubleType(), False),
                st.StructField("i_max", st.IntegerType(), False),
            ]
        )
        mem_df = spark.createDataFrame(
            [
                (1, 10, 4294967297, "abc", "11", "notadt", "notabool", float("nan"), 2147483647),
                (2, 7, 1, "7", "7", "2024-01-15", "true", 1.5, 1),
            ],
            schema=mem_schema,
        )
        mem_df.createOrReplaceTempView("mem_nn")

        sources = [
            ("ice", "ice.tc.nn", ice_df),
            ("pq", "pq_nn", pq_df),
            ("mem", "mem_nn", mem_df),
        ]

        cells = out["cells"]
        for src_tag, tbl_name, df in sources:
            cells[f"door/sql/{src_tag}/abs_try_int"] = capture_call(
                lambda t=tbl_name: spark.sql(
                    f"SELECT id, abs(TRY_CAST(s AS INT)) AS r FROM {t} ORDER BY id"
                ),
                is_spark,
            )
            cells[f"door/sql/{src_tag}/add1_try_int"] = capture_call(
                lambda t=tbl_name: spark.sql(
                    f"SELECT id, TRY_CAST(s AS INT) + 1 AS r FROM {t} ORDER BY id"
                ),
                is_spark,
            )
            cells[f"door/sql/{src_tag}/neg_try_int_bare"] = capture_call(
                lambda t=tbl_name: spark.sql(
                    f"SELECT id, -TRY_CAST(s AS INT) AS r FROM {t} ORDER BY id"
                ),
                is_spark,
            )
            cells[f"door/sql/{src_tag}/abs_neg_try_int"] = capture_call(
                lambda t=tbl_name: spark.sql(
                    f"SELECT id, abs(-TRY_CAST(s AS INT)) AS r FROM {t} ORDER BY id"
                ),
                is_spark,
            )
            cells[f"door/sql/{src_tag}/bare_try_int"] = capture_call(
                lambda t=tbl_name: spark.sql(
                    f"SELECT id, TRY_CAST(s AS INT) AS r FROM {t} ORDER BY id"
                ),
                is_spark,
            )
            cells[f"door/sql/{src_tag}/all_valid_abs_try_int"] = capture_call(
                lambda t=tbl_name: spark.sql(
                    f"SELECT id, abs(TRY_CAST(s_ok AS INT)) AS r FROM {t} ORDER BY id"
                ),
                is_spark,
            )
            cells[f"door/sql/{src_tag}/cast_shield_eq_try_int"] = capture_call(
                lambda t=tbl_name: spark.sql(
                    f"SELECT id, TRY_CAST(b AS INT) = 1 AS r FROM {t} ORDER BY id"
                ),
                is_spark,
            )
            cells[f"door/sql/{src_tag}/cast_shield_where_try_int"] = capture_call(
                lambda t=tbl_name: spark.sql(
                    f"SELECT id FROM {t} WHERE TRY_CAST(b AS INT) = 1 ORDER BY id"
                ),
                is_spark,
            )
            cells[f"door/sql/{src_tag}/abs_try_coalesce_int"] = capture_call(
                lambda t=tbl_name: spark.sql(
                    f"SELECT id, abs(TRY_CAST(coalesce(s, 'x') AS INT)) AS r FROM {t} ORDER BY id"
                ),
                is_spark,
            )
            cells[f"door/sql/{src_tag}/merged_proj_map_limit"] = capture_call(
                lambda t=tbl_name: spark.sql(
                    f"SELECT id, map('k', x)['k'] AS r "
                    f"FROM (SELECT id, TRY_CAST(s AS INT) AS x FROM {t} LIMIT 10) q ORDER BY id"
                ),
                is_spark,
            )
            cells[f"door/sql/{src_tag}/merged_proj_map_where"] = capture_call(
                lambda t=tbl_name: spark.sql(
                    f"SELECT id, map('k', x)['k'] AS r "
                    f"FROM (SELECT id, TRY_CAST(s AS INT) AS x FROM {t}) q WHERE id > 0 ORDER BY id"
                ),
                is_spark,
            )
            cells[f"door/sql/{src_tag}/merged_proj_hof_limit"] = capture_call(
                lambda t=tbl_name: spark.sql(
                    f"SELECT id, transform(array(1, 2), e -> x + e) AS r "
                    f"FROM (SELECT id, TRY_CAST(s AS INT) AS x FROM {t} LIMIT 10) q ORDER BY id"
                ),
                is_spark,
            )
            cells[f"door/sql/{src_tag}/merged_proj_abs"] = capture_call(
                lambda t=tbl_name: spark.sql(
                    f"SELECT id, abs(x) AS r "
                    f"FROM (SELECT id, TRY_CAST(s AS INT) AS x FROM {t} LIMIT 10) q ORDER BY id"
                ),
                is_spark,
            )
            cells[f"door/sql/{src_tag}/hof_aggregate_try_init"] = capture_call(
                lambda t=tbl_name: spark.sql(
                    "SELECT id, aggregate(array(1, 2), TRY_CAST(s AS INT), "
                    f"(acc, x) -> acc + x) AS r FROM {t} ORDER BY id"
                ),
                is_spark,
            )
            cells[f"door/sql/{src_tag}/hof_transform_abs_try"] = capture_call(
                lambda t=tbl_name: spark.sql(
                    f"SELECT id, transform(array(1, 2), e -> abs(TRY_CAST(s AS INT)) + e) AS r "
                    f"FROM {t} ORDER BY id"
                ),
                is_spark,
            )
            cells[f"door/sql/{src_tag}/join_on_abs_try"] = capture_call(
                lambda t=tbl_name: spark.sql(
                    f"SELECT a.id, a.r FROM "
                    f"(SELECT id, abs(TRY_CAST(s AS INT)) AS r FROM {t}) a "
                    f"LEFT JOIN (SELECT 7 AS k) b ON a.r = b.k ORDER BY a.id"
                ),
                is_spark,
            )
            cells[f"door/sql/{src_tag}/union_abs_try"] = capture_call(
                lambda t=tbl_name: spark.sql(
                    f"SELECT id, abs(TRY_CAST(s AS INT)) AS r FROM {t} "
                    f"UNION ALL SELECT 3 AS id, 9 AS r ORDER BY id"
                ),
                is_spark,
            )

            cells[f"door/selectExpr/{src_tag}/abs_try_int"] = capture_call(
                lambda d=df: d.selectExpr("id", "abs(TRY_CAST(s AS INT)) AS r").orderBy("id"),
                is_spark,
            )
            cells[f"door/selectExpr/{src_tag}/add1_try_int"] = capture_call(
                lambda d=df: d.selectExpr("id", "TRY_CAST(s AS INT) + 1 AS r").orderBy("id"),
                is_spark,
            )
            cells[f"door/selectExpr/{src_tag}/len_try_str"] = capture_call(
                lambda d=df: d.selectExpr(
                    "id", "length(TRY_CAST(TRY_CAST(s AS INT) AS STRING)) AS r"
                ).orderBy("id"),
                is_spark,
            )

            cells[f"door/fexpr/{src_tag}/abs_try_int"] = capture_call(
                lambda d=df: d.select("id", sf.expr("abs(TRY_CAST(s AS INT))").alias("r")).orderBy(
                    "id"
                ),
                is_spark,
            )
            cells[f"door/fexpr/{src_tag}/add1_try_int"] = capture_call(
                lambda d=df: d.select("id", sf.expr("TRY_CAST(s AS INT) + 1").alias("r")).orderBy(
                    "id"
                ),
                is_spark,
            )
            cells[f"door/fexpr/{src_tag}/date_add_try_dt"] = capture_call(
                lambda d=df: d.select(
                    "id", sf.expr("date_add(TRY_CAST(s_dt AS DATE), 1)").alias("r")
                ).orderBy("id"),
                is_spark,
            )

            cells[f"door/col_try_cast/{src_tag}/abs_try_int"] = capture_call(
                lambda d=df: d.select("id", sf.abs(sf.col("s").try_cast("int")).alias("r")).orderBy(
                    "id"
                ),
                is_spark,
            )
            cells[f"door/col_try_cast/{src_tag}/add1_try_int"] = capture_call(
                lambda d=df: d.select(
                    "id", (sf.col("s").try_cast("int") + sf.lit(1)).alias("r")
                ).orderBy("id"),
                is_spark,
            )
            cells[f"door/col_try_cast/{src_tag}/len_try_str"] = capture_call(
                lambda d=df: d.select(
                    "id", sf.length(sf.col("s").try_cast("int").try_cast("string")).alias("r")
                ).orderBy("id"),
                is_spark,
            )
            cells[f"door/col_try_cast/{src_tag}/abs_neg_try_int"] = capture_call(
                lambda d=df: d.select(
                    "id", sf.abs(-sf.col("s").try_cast("int")).alias("r")
                ).orderBy("id"),
                is_spark,
            )
            cells[f"door/col_try_cast/{src_tag}/abs_try_coalesce_int"] = capture_call(
                lambda d=df: d.select(
                    "id",
                    sf.abs(sf.coalesce(sf.col("s"), sf.lit("x")).try_cast("int")).alias("r"),
                ).orderBy("id"),
                is_spark,
            )
            cells[f"door/col_try_cast/{src_tag}/bare_try_int"] = capture_call(
                lambda d=df: d.select("id", sf.col("s").try_cast("int").alias("r")).orderBy("id"),
                is_spark,
            )

            cells[f"sep/{src_tag}/case_no_else_array_remove"] = capture_call(
                lambda t=tbl_name: spark.sql(
                    f"SELECT id, array_remove(array(1, 7), CASE WHEN id = 999 THEN i END) AS r "
                    f"FROM {t} ORDER BY id"
                ),
                is_spark,
            )
            cells[f"sep/{src_tag}/array_remove_null_elem"] = capture_call(
                lambda t=tbl_name: spark.sql(
                    "SELECT id, array_remove(array(1, 2), "
                    f"CASE WHEN id = 1 THEN NULL ELSE 1 END) AS r FROM {t} ORDER BY id"
                ),
                is_spark,
            )
            cells[f"sep/{src_tag}/array_remove_try_int"] = capture_call(
                lambda t=tbl_name: spark.sql(
                    f"SELECT id, array_remove(array(1, 7), TRY_CAST(s AS INT)) AS r "
                    f"FROM {t} ORDER BY id"
                ),
                is_spark,
            )
            cells[f"sep/{src_tag}/make_dt_interval_nan_col"] = capture_call(
                lambda t=tbl_name: spark.sql(
                    f"SELECT id, make_dt_interval(i, 0, 0, d_nan) AS r FROM {t} ORDER BY id"
                ),
                is_spark,
            )
            cells[f"sep/{src_tag}/make_dt_interval_ovf_col"] = capture_call(
                lambda t=tbl_name: spark.sql(
                    f"SELECT id, make_dt_interval(i_max, 0, 0, 0.0) AS r FROM {t} ORDER BY id"
                ),
                is_spark,
            )

        cells["door/sql/inline_union/abs_try_int"] = capture_call(
            lambda: spark.sql(
                "SELECT abs(TRY_CAST(s AS INT)) AS r FROM (SELECT 'x' AS s UNION ALL SELECT '7')"
            ),
            is_spark,
        )
        cells["door/sql/inline_union/add1_try_int"] = capture_call(
            lambda: spark.sql(
                "SELECT TRY_CAST(s AS INT) + 1 AS r FROM (SELECT 'x' AS s UNION ALL SELECT '7')"
            ),
            is_spark,
        )
        cells["door/sql/inline_union/neg_try_int"] = capture_call(
            lambda: spark.sql(
                "SELECT -TRY_CAST(s AS INT) AS r FROM (SELECT 'x' AS s UNION ALL SELECT '7')"
            ),
            is_spark,
        )

        spark.sql("CREATE OR REPLACE TEMP VIEW tv_nn AS SELECT id, s FROM ice.tc.nn")
        cells["door/sql/temp_view_filter_abs_try"] = capture_call(
            lambda: spark.sql("SELECT id FROM tv_nn WHERE abs(TRY_CAST(s AS INT)) > 0 ORDER BY id"),
            is_spark,
        )
        other_ansi = "true" if ansi == "false" else "false"
        spark.sql(f"SET spark.sql.ansi.enabled = {other_ansi}")
        cells["door/sql/temp_view_cross_ansi_filter_abs_try"] = capture_call(
            lambda: spark.sql("SELECT id FROM tv_nn WHERE abs(TRY_CAST(s AS INT)) > 0 ORDER BY id"),
            is_spark,
        )
        cells["door/sql/temp_view_cross_ansi_filter_map_try"] = capture_call(
            lambda: spark.sql(
                "SELECT id FROM tv_nn WHERE map('k', TRY_CAST(s AS INT))['k'] > 0 ORDER BY id"
            ),
            is_spark,
        )
        spark.sql(f"SET spark.sql.ansi.enabled = {ansi}")
        cells["door/sql/explain_analyze_abs_try"] = (
            {"kind": "ok", "note": "spark_skip"}
            if is_spark
            else capture_call(
                lambda: spark.sql(
                    "EXPLAIN ANALYZE SELECT id, abs(TRY_CAST(s AS INT)) AS r "
                    "FROM ice.tc.nn ORDER BY id"
                ),
                is_spark,
            )
        )

        pushdown_queries = {
            "ice/control_k_gt_500": "SELECT id FROM ice.tc.prune WHERE k_int > 500 ORDER BY id",
            "ice/try_widening_gt_500": (
                "SELECT id FROM ice.tc.prune WHERE TRY_CAST(k_int AS BIGINT) > 500 ORDER BY id"
            ),
            "ice/try_str_gt_500": (
                "SELECT id FROM ice.tc.prune WHERE TRY_CAST(s AS INT) > 500 ORDER BY id"
            ),
            "ice/where_abs_try_gt_5": (
                "SELECT id FROM ice.tc.prune WHERE abs(TRY_CAST(s AS INT)) > 5 ORDER BY id"
            ),
            "ice/compound_prune_and_abs": (
                "SELECT id FROM ice.tc.prune "
                "WHERE TRY_CAST(k_int AS BIGINT) > 500 "
                "AND abs(TRY_CAST(s AS INT)) > 500 ORDER BY id"
            ),
            "ice/dynamic_join_bare_try": (
                "SELECT a.id FROM (SELECT id, TRY_CAST(k_int AS BIGINT) AS kb FROM ice.tc.prune) a "
                "JOIN (SELECT CAST(1001 AS BIGINT) AS kb) b ON a.kb = b.kb ORDER BY a.id"
            ),
            "pq/control_k_gt_500": "SELECT id FROM pq_prune WHERE k_int > 500 ORDER BY id",
            "pq/try_widening_gt_500": (
                "SELECT id FROM pq_prune WHERE TRY_CAST(k_int AS BIGINT) > 500 ORDER BY id"
            ),
            "pq/try_str_gt_500": (
                "SELECT id FROM pq_prune WHERE TRY_CAST(s AS INT) > 500 ORDER BY id"
            ),
            "pq/where_abs_try_gt_5": (
                "SELECT id FROM pq_prune WHERE abs(TRY_CAST(s AS INT)) > 5 ORDER BY id"
            ),
            "pq/compound_prune_and_abs": (
                "SELECT id FROM pq_prune "
                "WHERE TRY_CAST(k_int AS BIGINT) > 500 "
                "AND abs(TRY_CAST(s AS INT)) > 500 ORDER BY id"
            ),
            "pq/dynamic_join_bare_try": (
                "SELECT a.id FROM (SELECT id, TRY_CAST(k_int AS BIGINT) AS kb FROM pq_prune) a "
                "JOIN (SELECT CAST(1001 AS BIGINT) AS kb) b ON a.kb = b.kb ORDER BY a.id"
            ),
        }
        for pkey, psql in pushdown_queries.items():
            out["pushdown"][pkey] = {
                "result": capture_call(lambda q=psql: spark.sql(q), is_spark),
                "explain": capture_explain(spark, psql, is_spark),
            }

        writes = out["writes"]

        spark.sql("CREATE TABLE ice.tc.dst_null (id INT NOT NULL, r INT) USING iceberg")
        try:
            spark.sql(
                "INSERT INTO ice.tc.dst_null SELECT id, abs(TRY_CAST(s AS INT)) FROM ice.tc.nn"
            ).collect()
            writes["insert_nullable_abs_try"] = capture_df(
                spark.sql("SELECT id, r FROM ice.tc.dst_null ORDER BY id"), is_spark
            )
        except Exception as exc:
            writes["insert_nullable_abs_try"] = {
                "kind": "write_error",
                "error_type": type(exc).__name__,
                "first": first_line(exc),
            }

        spark.sql("CREATE TABLE ice.tc.dst_nn_ok (id INT NOT NULL, r INT NOT NULL) USING iceberg")
        try:
            spark.sql(
                "INSERT INTO ice.tc.dst_nn_ok SELECT id, abs(TRY_CAST(s_ok AS INT)) FROM ice.tc.nn"
            ).collect()
            writes["insert_nonnull_all_valid"] = capture_df(
                spark.sql("SELECT id, r FROM ice.tc.dst_nn_ok ORDER BY id"), is_spark
            )
        except Exception as exc:
            writes["insert_nonnull_all_valid"] = {
                "kind": "write_error",
                "error_type": type(exc).__name__,
                "first": first_line(exc),
            }

        spark.sql("CREATE TABLE ice.tc.dst_nn_fail (id INT NOT NULL, r INT NOT NULL) USING iceberg")
        try:
            spark.sql(
                "INSERT INTO ice.tc.dst_nn_fail SELECT id, abs(TRY_CAST(s AS INT)) FROM ice.tc.nn"
            ).collect()
            writes["insert_nonnull_with_null"] = capture_df(
                spark.sql("SELECT id, r FROM ice.tc.dst_nn_fail ORDER BY id"), is_spark
            )
        except Exception as exc:
            writes["insert_nonnull_with_null"] = {
                "kind": "write_error",
                "error_type": type(exc).__name__,
                "first": first_line(exc),
            }

        for cname, cexpr in [
            ("ctas_abs", "abs(TRY_CAST(s AS INT))"),
            ("ctas_add", "TRY_CAST(s AS INT) + 1"),
            ("ctas_ok", "abs(TRY_CAST(s_ok AS INT))"),
        ]:
            try:
                spark.sql(
                    f"CREATE TABLE ice.tc.{cname} USING iceberg AS "
                    f"SELECT id, {cexpr} AS r FROM ice.tc.nn"
                ).collect()
                writes[cname] = capture_df(
                    spark.sql(f"SELECT id, r FROM ice.tc.{cname} ORDER BY id"), is_spark
                )
            except Exception as exc:
                writes[cname] = {
                    "kind": "write_error",
                    "error_type": type(exc).__name__,
                    "first": first_line(exc),
                }

        spark.sql(
            "CREATE TABLE ice.tc.upd_tgt (id INT NOT NULL, s STRING NOT NULL, r INT) USING iceberg"
        )
        spark.sql("INSERT INTO ice.tc.upd_tgt VALUES (1, 'abc', 0), (2, '7', 0)")
        try:
            spark.sql("UPDATE ice.tc.upd_tgt SET r = abs(TRY_CAST(s AS INT))").collect()
            writes["update_abs_try"] = capture_df(
                spark.sql("SELECT id, r FROM ice.tc.upd_tgt ORDER BY id"), is_spark
            )
        except Exception as exc:
            writes["update_abs_try"] = {
                "kind": "write_error",
                "error_type": type(exc).__name__,
                "first": first_line(exc),
            }

        spark.sql("CREATE TABLE ice.tc.mrg_tgt (id INT NOT NULL, r INT) USING iceberg")
        spark.sql("INSERT INTO ice.tc.mrg_tgt VALUES (1, 0), (2, 0)")
        try:
            spark.sql(
                """
                MERGE INTO ice.tc.mrg_tgt t
                USING ice.tc.nn s
                ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET r = abs(TRY_CAST(s.s AS INT))
                """
            ).collect()
            writes["merge_abs_try"] = capture_df(
                spark.sql("SELECT id, r FROM ice.tc.mrg_tgt ORDER BY id"), is_spark
            )
        except Exception as exc:
            writes["merge_abs_try"] = {
                "kind": "write_error",
                "error_type": type(exc).__name__,
                "first": first_line(exc),
            }
    finally:
        spark.stop()
    return out


def run_native(root: Path) -> dict[str, Any]:
    import repark
    from repark._native import PyReparkSession

    pq_single, pq_prune = write_parquet_fixtures(root / "pq_native")
    session = PyReparkSession.native()
    session.register_arrow_stream_as_temp_view("nn_mem", pq.read_table(str(pq_single)))
    session.create_or_replace_temp_view("pq_nat", session.read_parquet(str(pq_single)))
    session.create_or_replace_temp_view("pq_prune_nat", session.read_parquet(str(pq_prune)))

    queries = {
        "mem/try_int_raw": "SELECT id, TRY_CAST(s AS INT) AS r FROM nn_mem ORDER BY id",
        "mem/abs_try_int": "SELECT id, abs(TRY_CAST(s AS INT)) AS r FROM nn_mem ORDER BY id",
        "mem/round_try_dbl": (
            "SELECT id, round(TRY_CAST(s AS DOUBLE), 1) AS r FROM nn_mem ORDER BY id"
        ),
        "mem/round_neg_try_dbl": (
            "SELECT id, round(-TRY_CAST(s AS DOUBLE), 1) AS r FROM nn_mem ORDER BY id"
        ),
        "mem/date_part_try_dt": (
            "SELECT id, date_part('year', TRY_CAST(s_dt AS DATE)) AS r FROM nn_mem ORDER BY id"
        ),
        "mem/date_trunc_try_dt": (
            "SELECT id, date_trunc('year', TRY_CAST(s_dt AS DATE)) AS r FROM nn_mem ORDER BY id"
        ),
        "mem/strpos_try_str": (
            "SELECT id, strpos(TRY_CAST(TRY_CAST(s AS INT) AS VARCHAR), '7') AS r "
            "FROM nn_mem ORDER BY id"
        ),
        "mem/cast_try_smallint": (
            "SELECT id, CAST(TRY_CAST(s AS INT) AS SMALLINT) AS r FROM nn_mem ORDER BY id"
        ),
        "mem/all_valid_round_try_dbl": (
            "SELECT id, round(TRY_CAST(s_ok AS DOUBLE), 1) AS r FROM nn_mem ORDER BY id"
        ),
        "mem/cast_shield_eq_try_int": (
            "SELECT id, TRY_CAST(b AS INT) = 1 AS r FROM nn_mem ORDER BY id"
        ),
        "mem/merged_proj_round": (
            "SELECT id, round(x, 1) AS r "
            "FROM (SELECT id, TRY_CAST(s AS DOUBLE) AS x FROM nn_mem LIMIT 10) q ORDER BY id"
        ),
        "pq/round_try_coalesce_dbl": (
            "SELECT id, round(TRY_CAST(coalesce(s, 'x') AS DOUBLE), 1) AS r FROM pq_nat ORDER BY id"
        ),
        "pq/date_part_try_coalesce_dt": (
            "SELECT id, date_part('year', TRY_CAST(coalesce(s_dt, 'x') AS DATE)) AS r "
            "FROM pq_nat ORDER BY id"
        ),
        "pq/prune_control": "SELECT id FROM pq_prune_nat WHERE k_int > 500 ORDER BY id",
        "pq/prune_try_widening": (
            "SELECT id FROM pq_prune_nat WHERE TRY_CAST(k_int AS BIGINT) > 500 ORDER BY id"
        ),
        "sep/array_remove_try_int": (
            "SELECT id, array_remove(make_array(1, 7), TRY_CAST(s AS INT)) AS r "
            "FROM nn_mem ORDER BY id"
        ),
    }

    res: dict[str, Any] = {"native_file": repark._native.__file__, "cells": {}, "explains": {}}
    for key, sql in queries.items():
        try:
            tbl = pa.table(session.sql(sql))
            res["cells"][key] = {
                "kind": "ok",
                "types": [str(f.type) for f in tbl.schema],
                "nulls": [bool(f.nullable) for f in tbl.schema],
                "rows": [
                    [normalize_value(tbl.column(c)[r].as_py()) for c in range(tbl.num_columns)]
                    for r in range(tbl.num_rows)
                ],
            }
        except Exception as exc:
            res["cells"][key] = {
                "kind": "error",
                "error_type": type(exc).__name__,
                "first": first_line(exc),
            }

    for key in ("pq/prune_control", "pq/prune_try_widening"):
        try:
            tbl = pa.table(session.sql(f"EXPLAIN {queries[key]}"))
            lines = [
                f"{tbl.column(0)[r].as_py()}:\n{tbl.column(1)[r].as_py()}"
                for r in range(tbl.num_rows)
            ]
            res["explains"][key] = "\n".join(lines)
        except Exception as exc:
            res["explains"][key] = f"ERROR: {first_line(exc)}"
    return res


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=("repark", "spark"), required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--work-root", type=Path, required=True)
    args = parser.parse_args()

    args.work_root.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix=f"trycast_extra_{args.engine}_", dir=str(args.work_root)))
    payload: dict[str, Any] = {"engine": args.engine, "modes": {}}
    if args.engine == "repark":
        import repark

        payload["native_file"] = repark._native.__file__
    try:
        for ansi in ("false", "true"):
            payload["modes"][ansi] = run_mode(args.engine, ansi, root)
        if args.engine == "repark":
            payload["native"] = run_native(root)
    finally:
        shutil.rmtree(root, ignore_errors=True)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, indent=2, sort_keys=True))
    print(f"WROTE {args.out}", flush=True)


if __name__ == "__main__":
    main()

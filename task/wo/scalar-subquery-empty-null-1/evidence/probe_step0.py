from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any


def norm_val(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple)):
        return [norm_val(item) for item in value]
    return str(value)


def run_df_spark(frame: Any) -> dict[str, Any]:
    schema_info = [
        {
            "name": field.name,
            "type": field.dataType.simpleString(),
            "nullable": bool(field.nullable),
        }
        for field in frame.schema.fields
    ]
    rows = [[norm_val(cell) for cell in row] for row in frame.collect()]
    return {"ok": True, "rows": rows, "schema": schema_info}


def run_call_spark(fn: Callable[[], Any]) -> dict[str, Any]:
    try:
        return run_df_spark(fn())
    except BaseException as exc:
        return {
            "ok": False,
            "error": type(exc).__name__ + ": " + str(exc).splitlines()[0][:240],
        }


def run_df_repark(frame: Any) -> dict[str, Any]:
    df_schema = [
        {
            "name": field.name,
            "type": field.dataType.simpleString(),
            "nullable": bool(field.nullable),
        }
        for field in frame.schema.fields
    ]
    arrow_tbl = frame.to_arrow()
    arrow_schema = [
        {"name": field.name, "type": str(field.type), "nullable": field.nullable}
        for field in arrow_tbl.schema
    ]
    nonnull_with_nulls = [
        field.name
        for index, field in enumerate(arrow_tbl.schema)
        if (not field.nullable) and arrow_tbl.column(index).null_count > 0
    ]
    rows = [
        [norm_val(arrow_tbl.column(col)[row].as_py()) for col in range(arrow_tbl.num_columns)]
        for row in range(arrow_tbl.num_rows)
    ]
    result: dict[str, Any] = {
        "ok": True,
        "rows": rows,
        "df_schema": df_schema,
        "arrow_schema": arrow_schema,
    }
    if nonnull_with_nulls:
        result["nonnull_with_nulls"] = nonnull_with_nulls
    return result


def run_call_repark(fn: Callable[[], Any]) -> dict[str, Any]:
    try:
        return run_df_repark(fn())
    except BaseException as exc:
        return {
            "ok": False,
            "error": type(exc).__name__ + ": " + str(exc).splitlines()[0][:240],
        }


def run_native_sql(native_session: Any, sql: str) -> dict[str, Any]:
    from repark.spark.dataframe import DataFrame

    try:
        frame = DataFrame(native_session.sql(sql), native_session, {"alive": True})
        arrow_tbl = frame.to_arrow()
        arrow_schema = [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in arrow_tbl.schema
        ]
        nonnull_with_nulls = [
            field.name
            for index, field in enumerate(arrow_tbl.schema)
            if (not field.nullable) and arrow_tbl.column(index).null_count > 0
        ]
        rows = [
            [norm_val(arrow_tbl.column(col)[row].as_py()) for col in range(arrow_tbl.num_columns)]
            for row in range(arrow_tbl.num_rows)
        ]
        result: dict[str, Any] = {"ok": True, "rows": rows, "arrow_schema": arrow_schema}
        if nonnull_with_nulls:
            result["nonnull_with_nulls"] = nonnull_with_nulls
        return result
    except BaseException as exc:
        return {
            "ok": False,
            "error": type(exc).__name__ + ": " + str(exc).splitlines()[0][:240],
        }


def run_native_exec(native_session: Any, sql: str) -> Any:
    from repark.spark.dataframe import DataFrame

    return DataFrame(native_session.sql(sql), native_session, {"alive": True}).to_arrow()


def setup_tables(exec_fn: Callable[[str], Any], catalog: str, native_ansi: bool = False) -> None:
    if native_ansi:
        exec_fn(f"CREATE SCHEMA IF NOT EXISTS {catalog}.ns")
    else:
        exec_fn(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.ns")
    str_type = "VARCHAR" if native_ansi else "STRING"
    using = "" if native_ansi else " USING iceberg"
    exec_fn(
        f"CREATE TABLE {catalog}.ns.empty_nn (id INT NOT NULL, b INT NOT NULL, "
        f"s {str_type} NOT NULL, flag BOOLEAN NOT NULL, d DATE NOT NULL){using}"
    )
    exec_fn(f"CREATE TABLE {catalog}.ns.l_nn (id INT NOT NULL, v INT NOT NULL){using}")
    exec_fn(f"INSERT INTO {catalog}.ns.l_nn VALUES (1, 10), (2, 20)")
    exec_fn(f"CREATE TABLE {catalog}.ns.r_nn (id INT NOT NULL, b INT NOT NULL){using}")
    exec_fn(f"INSERT INTO {catalog}.ns.r_nn VALUES (2, 20)")


def extra_sql_cells(catalog: str) -> dict[str, str]:
    empty = f"{catalog}.ns.empty_nn"
    left = f"{catalog}.ns.l_nn"
    return {
        "cte_with_scalar_subq_isnull": (
            f"WITH c AS (SELECT id, (SELECT b FROM {empty}) AS r FROM {left}) "
            "SELECT id, r IS NULL AS is_n, coalesce(r, 42) AS c FROM c ORDER BY id"
        ),
        "cte_with_scalar_subq_where_isnull": (
            f"WITH c AS (SELECT id, (SELECT b FROM {empty}) AS r FROM {left}) "
            "SELECT id FROM c WHERE r IS NULL ORDER BY id"
        ),
        "cte_empty_in_scalar_subq_isnull": (
            f"WITH e AS (SELECT b FROM {empty}) SELECT (SELECT b FROM e) IS NULL AS r"
        ),
        "cte_empty_in_scalar_subq_coalesce": (
            f"WITH e AS (SELECT b FROM {empty}) "
            f"SELECT id, coalesce((SELECT b FROM e), 42) AS c FROM {left} ORDER BY id"
        ),
        "cte_nested_scalar_subq_isnull": (
            f"WITH c1 AS (SELECT b FROM {empty}), c2 AS (SELECT (SELECT b FROM c1) AS x) "
            "SELECT x IS NULL AS r FROM c2"
        ),
        "cte_count_star_control": (
            f"WITH c AS (SELECT (SELECT COUNT(*) FROM {empty}) AS cnt) "
            "SELECT cnt, cnt IS NULL AS is_n FROM c"
        ),
        "union_with_empty_subq": (
            f"SELECT r FROM (SELECT (SELECT b FROM {empty}) AS r "
            "UNION ALL SELECT 1 AS r) t ORDER BY r NULLS FIRST"
        ),
        "cast_date_subq_to_ts_isnull": (
            f"SELECT CAST((SELECT d FROM {empty}) AS TIMESTAMP) IS NULL AS r"
        ),
        "cast_date_subq_to_ts_val": f"SELECT CAST((SELECT d FROM {empty}) AS TIMESTAMP) AS r",
        "nested_scalar_subq_isnull": (
            f"SELECT (SELECT (SELECT b FROM {empty}) FROM {left} LIMIT 1) IS NULL AS r"
        ),
        "nested_scalar_subq_outer_empty": (f"SELECT (SELECT (SELECT 1) FROM {empty}) IS NULL AS r"),
        "in_list_single_scalar_subq": (
            f"SELECT id FROM {left} WHERE id IN ((SELECT b FROM {empty})) ORDER BY id"
        ),
        "scalar_subq_in_having": (
            f"SELECT id FROM {left} GROUP BY id HAVING (SELECT b FROM {empty}) IS NULL ORDER BY id"
        ),
        "scalar_subq_in_join_on": (
            f"SELECT l.id FROM {left} l INNER JOIN {left} r "
            f"ON l.id = r.id AND (SELECT b FROM {empty}) IS NULL ORDER BY l.id"
        ),
        "scalar_subq_lit_control": "SELECT (SELECT 1) IS NULL AS r",
        "scalar_subq_values_control": "SELECT (SELECT c FROM (VALUES (1)) AS t(c)) IS NULL AS r",
    }


def run_cached_cell(session: Any, empty: str, left: str) -> Any:
    cached = session.sql(f"SELECT id, (SELECT b FROM {empty}) AS r FROM {left}").cache()
    try:
        return cached.filter("r IS NULL").orderBy("id")
    finally:
        cached.unpersist()


def run_df_door_cells(
    session: Any,
    funcs: Any,
    run_call: Callable[[Callable[[], Any]], dict[str, Any]],
    catalog: str,
) -> dict[str, dict[str, Any]]:
    empty = f"{catalog}.ns.empty_nn"
    left = f"{catalog}.ns.l_nn"
    left_df = session.table(left).orderBy("id")
    empty_df = session.table(empty)
    out: dict[str, dict[str, Any]] = {}

    out["fexpr_withcol_isnull"] = run_call(
        lambda: left_df.withColumn("r", funcs.expr("(SELECT 1 WHERE false) IS NULL")).select(
            "id", "r"
        )
    )
    out["fexpr_withcol_isnotnull"] = run_call(
        lambda: left_df.withColumn("r", funcs.expr("(SELECT 1 WHERE false) IS NOT NULL")).select(
            "id", "r"
        )
    )
    out["fexpr_withcol_coalesce"] = run_call(
        lambda: left_df.withColumn("r", funcs.expr("coalesce((SELECT 1 WHERE false), 42)")).select(
            "id", "r"
        )
    )
    out["fexpr_withcol_bare"] = run_call(
        lambda: left_df.withColumn("r", funcs.expr("(SELECT 1 WHERE false)")).select("id", "r")
    )
    out["fexpr_withcol_cast_date_ts"] = run_call(
        lambda: left_df.withColumn(
            "r", funcs.expr("CAST((SELECT DATE '2024-01-01' WHERE false) AS TIMESTAMP)")
        ).select("id", "r")
    )
    out["fexpr_withcol_case_isnull"] = run_call(
        lambda: left_df.withColumn(
            "r",
            funcs.expr("(CASE WHEN id = 1 THEN (SELECT 1 WHERE false) ELSE 1 END) IS NULL"),
        ).select("id", "r")
    )
    out["fexpr_withcol_coalesce_id"] = run_call(
        lambda: left_df.withColumn("r", funcs.expr("coalesce((SELECT 1 WHERE false), id)")).select(
            "id", "r"
        )
    )
    out["fexpr_withcol_count_star_control"] = run_call(
        lambda: left_df.withColumn(
            "r", funcs.expr("(SELECT COUNT(*) FROM (SELECT 1 WHERE false)) IS NULL")
        ).select("id", "r")
    )
    out["fexpr_filter_col_isnull"] = run_call(
        lambda: left_df.filter(funcs.expr("(SELECT 1 WHERE false) IS NULL")).select("id")
    )
    out["fexpr_filter_col_isnotnull"] = run_call(
        lambda: left_df.filter(funcs.expr("(SELECT 1 WHERE false) IS NOT NULL")).select("id")
    )
    out["fexpr_filter_col_tbl_isnull"] = run_call(
        lambda: left_df.filter(funcs.expr(f"(SELECT b FROM {empty}) IS NULL")).select("id")
    )
    out["filter_str_inline_isnull"] = run_call(
        lambda: left_df.filter("(SELECT 1 WHERE false) IS NULL").select("id")
    )
    out["filter_str_inline_isnotnull"] = run_call(
        lambda: left_df.filter("(SELECT 1 WHERE false) IS NOT NULL").select("id")
    )
    out["filter_str_tbl_isnull"] = run_call(
        lambda: left_df.filter(f"(SELECT b FROM {empty}) IS NULL").select("id")
    )
    out["filter_str_tbl_isnotnull"] = run_call(
        lambda: left_df.filter(f"(SELECT b FROM {empty}) IS NOT NULL").select("id")
    )
    out["filter_str_count_having_isnull"] = run_call(
        lambda: left_df.filter(
            f"(SELECT COUNT(*) FROM {empty} HAVING COUNT(*) > 0) IS NULL"
        ).select("id")
    )
    out["filter_str_count_star_control"] = run_call(
        lambda: left_df.filter(f"(SELECT COUNT(*) FROM {empty}) IS NULL").select("id")
    )
    out["selectexpr_inline_isnull"] = run_call(
        lambda: left_df.selectExpr("id", "(SELECT 1 WHERE false) IS NULL AS r")
    )
    out["selectexpr_inline_coalesce"] = run_call(
        lambda: left_df.selectExpr("id", "coalesce((SELECT 1 WHERE false), 42) AS r")
    )
    out["selectexpr_tbl_isnull"] = run_call(
        lambda: left_df.selectExpr("id", f"(SELECT b FROM {empty}) IS NULL AS r")
    )
    out["selectexpr_tbl_coalesce"] = run_call(
        lambda: left_df.selectExpr("id", f"coalesce((SELECT b FROM {empty}), 42) AS r")
    )
    out["selectexpr_tbl_bare"] = run_call(
        lambda: left_df.selectExpr("id", f"(SELECT b FROM {empty}) AS r")
    )
    if hasattr(empty_df, "scalar"):
        scalar_col = empty_df.select("b").scalar()
        out["withcol_scalar_isnull"] = run_call(
            lambda: left_df.withColumn("r", scalar_col.isNull()).select("id", "r")
        )
        out["withcol_scalar_coalesce"] = run_call(
            lambda: left_df.withColumn("r", funcs.coalesce(scalar_col, funcs.lit(42))).select(
                "id", "r"
            )
        )
        out["withcol_scalar_bare"] = run_call(
            lambda: left_df.withColumn("r", scalar_col).select("id", "r")
        )
        out["filter_scalar_isnull"] = run_call(
            lambda: left_df.filter(scalar_col.isNull()).select("id")
        )
        out["filter_scalar_isnotnull"] = run_call(
            lambda: left_df.filter(scalar_col.isNotNull()).select("id")
        )

    session.sql(
        f"CREATE OR REPLACE TEMP VIEW tv_subq_sql AS "
        f"SELECT id, (SELECT b FROM {empty}) AS r FROM {left}"
    ).collect()
    out["tempview_sql_subq_select"] = run_call(
        lambda: session.sql(
            "SELECT id, r, r IS NULL AS is_n, coalesce(r, 42) AS c FROM tv_subq_sql ORDER BY id"
        )
    )
    out["tempview_sql_subq_where_isnull"] = run_call(
        lambda: session.sql("SELECT id FROM tv_subq_sql WHERE r IS NULL ORDER BY id")
    )
    session.sql(f"SELECT id, (SELECT b FROM {empty}) AS r FROM {left}").createOrReplaceTempView(
        "tv_subq_df"
    )
    out["tempview_df_subq_select"] = run_call(
        lambda: session.sql(
            "SELECT id, r, r IS NULL AS is_n, coalesce(r, 42) AS c FROM tv_subq_df ORDER BY id"
        )
    )
    out["tempview_df_subq_where_isnull"] = run_call(
        lambda: session.sql("SELECT id FROM tv_subq_df WHERE r IS NULL ORDER BY id")
    )
    orig_ansi = session.conf.get("spark.sql.ansi.enabled")
    flipped = "true" if orig_ansi == "false" else "false"
    session.conf.set("spark.sql.ansi.enabled", flipped)
    out["tempview_df_ansi_flip"] = run_call(
        lambda: session.sql(
            "SELECT id, r IS NULL AS is_n, coalesce(r, 42) AS c FROM tv_subq_df ORDER BY id"
        )
    )
    session.conf.set("spark.sql.ansi.enabled", orig_ansi)
    out["cached_subq_where_isnull"] = run_call(lambda: run_cached_cell(session, empty, left))
    out["spark_explode_empty_isnull"] = run_call(
        lambda: session.sql("SELECT (SELECT explode(filter(array(1), x -> x > 5))) IS NULL AS r")
    )
    return out


def run_prepared_repark(
    exec_fn: Callable[[str], Any],
    query_fn: Callable[[str], dict[str, Any]],
    catalog: str,
) -> dict[str, dict[str, Any]]:
    empty = f"{catalog}.ns.empty_nn"
    left = f"{catalog}.ns.l_nn"
    out: dict[str, dict[str, Any]] = {}
    for key, prep_sql, exec_sql in (
        (
            "prep_isnull_select",
            f"PREPARE p1 AS SELECT (SELECT b FROM {empty}) IS NULL AS r",
            "EXECUTE p1",
        ),
        (
            "prep_isnull_where",
            f"PREPARE p2 AS SELECT id FROM {left} "
            f"WHERE (SELECT b FROM {empty}) IS NULL ORDER BY id",
            "EXECUTE p2",
        ),
        (
            "prep_coalesce_param",
            f"PREPARE p3(INT) AS SELECT id, coalesce((SELECT b FROM {empty}), $1) AS r "
            f"FROM {left} ORDER BY id",
            "EXECUTE p3(42)",
        ),
        ("prep_bare", f"PREPARE p4 AS SELECT (SELECT b FROM {empty}) AS r", "EXECUTE p4"),
    ):
        try:
            exec_fn(prep_sql)
            out[key] = query_fn(exec_sql)
        except BaseException as exc:
            out[key] = {
                "ok": False,
                "error": type(exc).__name__ + ": " + str(exc).splitlines()[0][:240],
            }
    return out


def run_prepared_spark(session: Any, catalog: str) -> dict[str, dict[str, Any]]:
    empty = f"{catalog}.ns.empty_nn"
    left = f"{catalog}.ns.l_nn"
    return {
        "prep_isnull_select": run_call_spark(
            lambda: session.sql(f"EXECUTE IMMEDIATE 'SELECT (SELECT b FROM {empty}) IS NULL AS r'")
        ),
        "prep_isnull_where": run_call_spark(
            lambda: session.sql(
                f"EXECUTE IMMEDIATE 'SELECT id FROM {left} "
                f"WHERE (SELECT b FROM {empty}) IS NULL ORDER BY id'"
            )
        ),
        "prep_coalesce_param": run_call_spark(
            lambda: session.sql(
                f"EXECUTE IMMEDIATE 'SELECT id, coalesce((SELECT b FROM {empty}), 42) AS r "
                f"FROM {left} ORDER BY id'"
            )
        ),
        "prep_bare": run_call_spark(
            lambda: session.sql(f"EXECUTE IMMEDIATE 'SELECT (SELECT b FROM {empty}) AS r'")
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=["repark", "spark"], required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    tmp = Path(tempfile.mkdtemp(prefix="step0_subq_"))
    warehouse = tmp / "wh"
    catalog = "ice"
    sql_cells = extra_sql_cells(catalog)
    payload: dict[str, Any] = {"engine": args.engine}

    try:
        if args.engine == "spark":
            from pyspark.sql import SparkSession
            from pyspark.sql import functions as spark_functions

            builder = (
                SparkSession.builder.master("local[2]")
                .appName("step0-subq-spark")
                .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.ice.type", "hadoop")
                .config("spark.sql.catalog.ice.warehouse", str(warehouse))
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.ansi.enabled", "false")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
            )
            jar = os.environ.get("SPARK_ICEBERG_JAR")
            if jar:
                builder = builder.config("spark.jars", jar).config(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                )
            session = builder.getOrCreate()
            session.sparkContext.setLogLevel("ERROR")
            setup_tables(lambda q: session.sql(q).collect(), catalog)
            for ansi in ("false", "true"):
                session.conf.set("spark.sql.ansi.enabled", ansi)
                payload[f"spark_sql_ansi_{ansi}"] = {
                    key: run_call_spark(lambda q=query: session.sql(q))
                    for key, query in sql_cells.items()
                }
                payload[f"spark_df_ansi_{ansi}"] = run_df_door_cells(
                    session, spark_functions, run_call_spark, catalog
                )
            session.conf.set("spark.sql.ansi.enabled", "false")
            payload["spark_prepared"] = run_prepared_spark(session, catalog)
            session.stop()
        else:
            from repark import _native
            from repark.spark import SparkSession
            from repark.spark import functions as spark_functions

            session = (
                SparkSession.builder.appName("step0-subq-repark")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate()
            )
            session.register_memory_catalog(catalog, str(warehouse / "spark"))
            setup_tables(lambda q: session.sql(q).collect(), catalog)
            for ansi in ("false", "true"):
                session.conf.set("spark.sql.ansi.enabled", ansi)
                payload[f"spark_sql_ansi_{ansi}"] = {
                    key: run_call_repark(lambda q=query: session.sql(q))
                    for key, query in sql_cells.items()
                }
                payload[f"spark_df_ansi_{ansi}"] = run_df_door_cells(
                    session, spark_functions, run_call_repark, catalog
                )
            session.conf.set("spark.sql.ansi.enabled", "false")
            payload["spark_prepared"] = run_prepared_repark(
                lambda q: session.sql(q).collect(),
                lambda q: run_call_repark(lambda: session.sql(q)),
                catalog,
            )
            session.stop()

            native = _native.PyReparkSession.native()
            native.register_memory_catalog(catalog, str(warehouse / "native"))
            setup_tables(lambda q: run_native_exec(native, q), catalog, native_ansi=True)
            payload["native_sql"] = {
                key: run_native_sql(native, query) for key, query in sql_cells.items()
            }
            payload["native_prepared"] = run_prepared_repark(
                lambda q: run_native_exec(native, q),
                lambda q: run_native_sql(native, q),
                catalog,
            )
            run_native_exec(
                native,
                f"CREATE VIEW v_nat_subq AS "
                f"SELECT id, (SELECT b FROM {catalog}.ns.empty_nn) AS r FROM {catalog}.ns.l_nn",
            )
            payload["native_view"] = {
                "native_view_subq_select": run_native_sql(
                    native,
                    "SELECT id, r, r IS NULL AS is_n, coalesce(r, 42) AS c "
                    "FROM v_nat_subq ORDER BY id",
                ),
                "native_view_subq_where_isnull": run_native_sql(
                    native, "SELECT id FROM v_nat_subq WHERE r IS NULL ORDER BY id"
                ),
                "native_view_describe": run_native_sql(native, "DESCRIBE v_nat_subq"),
            }
        Path(args.out).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()

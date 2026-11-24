from __future__ import annotations

import argparse
import datetime
import json
import math
import os
import shutil
import tempfile
from decimal import Decimal
from pathlib import Path
from typing import Any


def normalize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, str)):
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
    if isinstance(value, list):
        return [normalize_value(item) for item in value]
    if isinstance(value, tuple):
        return [normalize_value(item) for item in value]
    if isinstance(value, dict):
        return {
            str(k): normalize_value(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))
        }
    return str(value)


def normalize_type(type_text: str) -> str:
    return type_text.lower().replace(" ", "")


def capture_query(spark: Any, sql: str, check_arrow: bool) -> dict[str, Any]:
    df = None
    try:
        df = spark.sql(sql)
        schema = df.schema
        types = [normalize_type(field.dataType.simpleString()) for field in schema.fields]
        schema_nulls = [bool(field.nullable) for field in schema.fields]
    except Exception as exc:
        msg = str(exc)
        return {
            "kind": "plan_error",
            "error_type": type(exc).__name__,
            "first": (msg.splitlines()[0] if msg else "")[:240],
        }

    collect_result: dict[str, Any]
    try:
        rows = [[normalize_value(cell) for cell in row] for row in df.collect()]
        collect_result = {"ok": True, "rows": rows}
    except Exception as exc:
        msg = str(exc)
        collect_result = {
            "ok": False,
            "error_type": type(exc).__name__,
            "first": (msg.splitlines()[0] if msg else "")[:240],
        }

    arrow_result: dict[str, Any] | None = None
    if check_arrow and hasattr(df, "to_arrow"):
        try:
            table = df.to_arrow()
            arrow_nulls = [bool(field.nullable) for field in table.schema]
            arrow_rows = [
                [normalize_value(table.column(c)[r].as_py()) for c in range(table.num_columns)]
                for r in range(table.num_rows)
            ]
            arrow_result = {"ok": True, "nulls": arrow_nulls, "rows": arrow_rows}
        except Exception as exc:
            msg = str(exc)
            arrow_result = {
                "ok": False,
                "error_type": type(exc).__name__,
                "first": (msg.splitlines()[0] if msg else "")[:240],
            }

    if collect_result["ok"] and (arrow_result is None or arrow_result["ok"]):
        kind = "ok"
    elif (not collect_result["ok"]) and (arrow_result is None or not arrow_result["ok"]):
        kind = "exec_error"
    else:
        kind = "split_error"

    return {
        "kind": kind,
        "types": types,
        "schema_nulls": schema_nulls,
        "collect": collect_result,
        "arrow": arrow_result,
    }


def make_session(engine: str, warehouse: Path, ansi: str) -> Any:
    if engine == "spark":
        from pyspark.sql import SparkSession

        jar = os.environ["SPARK_ICEBERG_JAR"]
        spark = (
            SparkSession.builder.master("local[2]")
            .appName(f"ws2-sweep-{ansi}")
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
        return spark

    from repark import ReparkSession
    from repark.spark.session import _reset_active_session_for_tests

    _reset_active_session_for_tests()
    return (
        ReparkSession.builder.appName(f"ws2-sweep-{ansi}")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.ansi.enabled", ansi)
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", str(warehouse))
        .getOrCreate()
    )


def setup_tables(spark: Any) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.sw")
    for tbl in ("nn", "nn_r", "empty_nn", "null_vals"):
        spark.sql(f"DROP TABLE IF EXISTS ice.sw.{tbl}")

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
    )
    spark.sql(
        """
        INSERT INTO ice.sw.nn VALUES
            (
                1, 10, 4294967297, 'abc', 'notabool', 'notats', 'notadt',
                'a:1,b:2', 1.5, CAST('NaN' AS DOUBLE), 2147483647,
                TIMESTAMP '1500-06-15 12:00:00'
            ),
            (
                2, 7, 1, '7', 'true', '2024-01-01 02:30:00', '2024-01-15',
                'k:9', 2.25, 1.5, 1,
                TIMESTAMP '2024-01-01 02:30:00'
            )
        """
    )

    spark.sql(
        """
        CREATE TABLE ice.sw.nn_r (
            id INT NOT NULL,
            i INT NOT NULL,
            b BIGINT NOT NULL,
            s STRING NOT NULL,
            d DOUBLE NOT NULL,
            fl BOOLEAN NOT NULL,
            ts TIMESTAMP NOT NULL,
            dt DATE NOT NULL
        ) USING iceberg
        """
    )
    spark.sql(
        """
        INSERT INTO ice.sw.nn_r VALUES
            (2, 7, 1, '7', 2.25, true, TIMESTAMP '2024-01-01 02:30:00', DATE '2024-01-15'),
            (3, 9, 2, '9', 3.5, false, TIMESTAMP '2024-02-01 05:00:00', DATE '2024-02-01')
        """
    )

    spark.sql(
        """
        CREATE TABLE ice.sw.empty_nn (
            id INT NOT NULL,
            i INT NOT NULL,
            b BIGINT NOT NULL,
            s STRING NOT NULL,
            d DOUBLE NOT NULL,
            fl BOOLEAN NOT NULL,
            ts TIMESTAMP NOT NULL,
            dt DATE NOT NULL
        ) USING iceberg
        """
    )

    spark.sql(
        """
        CREATE TABLE ice.sw.null_vals (
            v INT
        ) USING iceberg
        """
    )
    spark.sql("INSERT INTO ice.sw.null_vals VALUES (2), (NULL)")


def id_col_for(tbl: str) -> str:
    return "l.id" if "LEFT JOIN" in tbl else "id"


def build_queries() -> dict[str, str]:
    queries: dict[str, str] = {}

    int_wrappers = {
        "try_int": ("ice.sw.nn", "TRY_CAST(s AS INT)"),
        "neg_try_int": ("ice.sw.nn", "-TRY_CAST(s AS INT)"),
        "case_no_else": ("ice.sw.nn", "CASE WHEN id = 999 THEN i END"),
        "nullif_self": ("ice.sw.nn", "NULLIF(i, i)"),
        "left_join": ("ice.sw.nn l LEFT JOIN ice.sw.nn_r r ON l.id = r.id", "r.i"),
        "subq_empty": ("ice.sw.nn", "(SELECT i FROM ice.sw.empty_nn)"),
    }

    int_functions = [
        ("math/abs", "abs({x})"),
        ("math/round", "round({x}, 0)"),
        ("math/bround", "bround({x}, 0)"),
        ("math/floor", "floor({x})"),
        ("math/ceil", "ceil({x})"),
        ("math/sqrt", "sqrt({x})"),
        ("math/ln", "ln({x})"),
        ("math/pow", "pow({x}, 2)"),
        ("math/pmod", "pmod({x}, 5)"),
        ("math/signum", "signum({x})"),
        ("math/bin", "bin({x})"),
        ("math/greatest", "greatest({x}, {x})"),
        ("math/least", "least({x}, {x})"),
        ("math/add_1", "{x} + 1"),
        ("math/sub_1", "{x} - 1"),
        ("math/mul_2", "{x} * 2"),
        ("cast/to_smallint", "CAST({x} AS SMALLINT)"),
        ("cast/to_bigint", "CAST({x} AS BIGINT)"),
        ("cast/to_string", "CAST({x} AS STRING)"),
        ("cast/to_double", "CAST({x} AS DOUBLE)"),
        ("cast/try_smallint", "TRY_CAST({x} AS SMALLINT)"),
        ("bit/bitwise_not", "~{x}"),
        ("bit/bit_count", "bit_count({x})"),
        ("bit/bit_get", "bit_get({x}, 0)"),
        ("bit/shiftleft", "shiftleft({x}, 1)"),
        ("bit/shiftright", "shiftright({x}, 1)"),
        ("str/chr", "chr({x})"),
        ("str/cast_len", "length(CAST({x} AS STRING))"),
        ("str/cast_upper", "upper(CAST({x} AS STRING))"),
        ("str/cast_concat", "concat(CAST({x} AS STRING), '!')"),
        ("coll/array", "array({x})"),
        ("coll/array_contains", "array_contains(array(1, 7), {x})"),
        ("coll/array_remove", "array_remove(array(1, 7), {x})"),
        ("coll/slice", "slice(array(1, 2, 3), {x}, 1)"),
        ("coll/named_struct_field", "named_struct('a', {x}).a"),
        ("coll/map_get", "map('k', {x})['k']"),
        ("coll/transform", "transform(array(1, 2), e -> e + {x})"),
        ("coll/exists", "exists(array(1, 7), e -> e = {x})"),
        ("cond/coalesce1", "coalesce({x})"),
        ("cond/coalesce2", "coalesce({x}, {x})"),
        ("cond/nvl", "nvl({x}, {x})"),
        ("cond/nvl2", "nvl2({x}, {x}, {x})"),
        ("cond/ifnull", "ifnull({x}, {x})"),
        ("cond/nullif", "nullif({x}, 999)"),
        ("cond/case_then", "CASE WHEN {id} > 0 THEN {x} ELSE 0 END"),
        ("cond/if_then", "IF({id} > 0, {x}, 0)"),
        ("win/first_value", "first_value({x}) OVER (ORDER BY {id})"),
        ("win/last_value", "last_value({x}) OVER (ORDER BY {id})"),
        ("win/lag", "lag({x}) OVER (ORDER BY {id})"),
        ("win/lead", "lead({x}) OVER (ORDER BY {id})"),
        ("win/sum_over", "sum({x}) OVER (ORDER BY {id})"),
    ]

    for wname, (tbl, expr) in int_wrappers.items():
        idc = id_col_for(tbl)
        for fname, tmpl in int_functions:
            body = tmpl.format(x=expr, id=idc)
            queries[f"{wname}|{fname}"] = (
                f"SELECT {idc} AS id, {body} AS r FROM {tbl} ORDER BY {idc}"
            )

        for aname, atmpl in [
            ("agg/sum", "sum({x})"),
            ("agg/min", "min({x})"),
            ("agg/max", "max({x})"),
            ("agg/count", "count({x})"),
        ]:
            body = atmpl.format(x=expr)
            queries[f"{wname}|{aname}"] = f"SELECT {body} AS r FROM {tbl}"

    str_wrappers = {
        "try_str": ("ice.sw.nn", "TRY_CAST(TRY_CAST(s AS INT) AS STRING)"),
        "left_join_str": ("ice.sw.nn l LEFT JOIN ice.sw.nn_r r ON l.id = r.id", "r.s"),
        "subq_empty_str": ("ice.sw.nn", "(SELECT s FROM ice.sw.empty_nn)"),
        "case_no_else_str": ("ice.sw.nn", "CASE WHEN id = 999 THEN s END"),
        "nullif_str": ("ice.sw.nn", "NULLIF(s, s)"),
    }
    str_functions = [
        ("str/length", "length({x})"),
        ("str/octet_length", "octet_length({x})"),
        ("str/bit_length", "bit_length({x})"),
        ("str/ascii", "ascii({x})"),
        ("str/upper", "upper({x})"),
        ("str/lower", "lower({x})"),
        ("str/substring", "substring({x}, 1, 2)"),
        ("str/concat", "concat({x}, 'z')"),
        ("str/reverse", "reverse({x})"),
        ("str/initcap", "initcap({x})"),
        ("str/startswith", "startswith({x}, '7')"),
        ("str/split_part", "split_part({x}, ':', 1)"),
        ("str/base64", "base64({x})"),
        ("str/instr", "instr({x}, '7')"),
        ("str/format_string", "format_string({x}, 1)"),
        ("coll/str_to_map", "str_to_map({x})"),
        ("coll/array_join", "array_join(array({x}), ',')"),
    ]
    for wname, (tbl, expr) in str_wrappers.items():
        idc = id_col_for(tbl)
        for fname, tmpl in str_functions:
            body = tmpl.format(x=expr)
            queries[f"{wname}|{fname}"] = (
                f"SELECT {idc} AS id, {body} AS r FROM {tbl} ORDER BY {idc}"
            )

    dt_wrappers = {
        "try_ts_str": ("ice.sw.nn", "TRY_CAST(s_ts AS TIMESTAMP)"),
        "try_dt_str": ("ice.sw.nn", "TRY_CAST(s_dt AS DATE)"),
        "try_ts_int": ("ice.sw.nn", "TRY_CAST(TRY_CAST(s AS INT) AS TIMESTAMP)"),
        "left_join_ts": ("ice.sw.nn l LEFT JOIN ice.sw.nn_r r ON l.id = r.id", "r.ts"),
        "subq_empty_ts": ("ice.sw.nn", "(SELECT ts FROM ice.sw.empty_nn)"),
    }
    dt_functions = [
        ("dt/date_part_hour", "date_part('hour', {x})"),
        ("dt/date_part_year", "date_part('year', {x})"),
        ("dt/extract_year", "extract(YEAR FROM {x})"),
        ("dt/hour", "hour({x})"),
        ("dt/year", "year({x})"),
        ("dt/date_trunc_year", "date_trunc('year', {x})"),
        ("dt/date_add", "date_add({x}, 1)"),
        ("dt/date_sub", "date_sub({x}, 1)"),
        ("dt/date_diff", "date_diff({x}, DATE '2024-01-01')"),
        ("dt/last_day", "last_day({x})"),
        ("dt/unix_date", "unix_date({x})"),
    ]
    for wname, (tbl, expr) in dt_wrappers.items():
        idc = id_col_for(tbl)
        for fname, tmpl in dt_functions:
            if wname == "try_dt_str" and fname in ("dt/date_part_hour", "dt/hour"):
                continue
            if wname in ("try_ts_str", "try_ts_int", "left_join_ts", "subq_empty_ts") and fname in (
                "dt/unix_date",
            ):
                continue
            body = tmpl.format(x=expr)
            queries[f"{wname}|{fname}"] = (
                f"SELECT {idc} AS id, {body} AS r FROM {tbl} ORDER BY {idc}"
            )

    dbl_wrappers = {
        "try_dbl": ("ice.sw.nn", "TRY_CAST(s AS DOUBLE)"),
        "neg_try_dbl": ("ice.sw.nn", "-TRY_CAST(s AS DOUBLE)"),
    }
    dbl_functions = [
        ("math/abs", "abs({x})"),
        ("math/round", "round({x}, 1)"),
        ("math/bround", "bround({x}, 1)"),
        ("math/floor", "floor({x})"),
        ("math/ceil", "ceil({x})"),
        ("math/sqrt", "sqrt({x})"),
        ("cast/to_int", "CAST({x} AS INT)"),
        ("cast/to_string", "CAST({x} AS STRING)"),
        ("str/cast_len", "length(CAST({x} AS STRING))"),
    ]
    for wname, (tbl, expr) in dbl_wrappers.items():
        idc = id_col_for(tbl)
        for fname, tmpl in dbl_functions:
            body = tmpl.format(x=expr)
            queries[f"{wname}|{fname}"] = (
                f"SELECT {idc} AS id, {body} AS r FROM {tbl} ORDER BY {idc}"
            )

    bool_wrappers = {
        "try_bool": ("ice.sw.nn", "TRY_CAST(s_bool AS BOOLEAN)"),
        "not_try_bool": ("ice.sw.nn", "NOT TRY_CAST(s_bool AS BOOLEAN)"),
        "left_join_bool": ("ice.sw.nn l LEFT JOIN ice.sw.nn_r r ON l.id = r.id", "r.fl"),
        "subq_empty_bool": ("ice.sw.nn", "(SELECT fl FROM ice.sw.empty_nn)"),
    }
    bool_functions = [
        ("bool/raw", "{x}"),
        ("bool/cast_int", "CAST({x} AS INT)"),
        ("bool/cast_str", "CAST({x} AS STRING)"),
        ("bool/cast_str_len", "length(CAST({x} AS STRING))"),
        ("bool/cast_int_abs", "abs(CAST({x} AS INT))"),
        ("bool/nvl2", "nvl2({x}, {x}, {x})"),
        ("bool/coalesce2", "coalesce({x}, {x})"),
        ("bool/nullif", "nullif({x}, false)"),
        ("bool/array_contains", "array_contains(array(true), {x})"),
        ("bool/bit_count", "bit_count({x})"),
    ]
    for wname, (tbl, expr) in bool_wrappers.items():
        idc = id_col_for(tbl)
        for fname, tmpl in bool_functions:
            body = tmpl.format(x=expr)
            queries[f"{wname}|{fname}"] = (
                f"SELECT {idc} AS id, {body} AS r FROM {tbl} ORDER BY {idc}"
            )

    extra = {
        "dfbug/array_remove_null_elem": (
            "SELECT id, array_remove(array(1, 2), CASE WHEN id = 1 THEN NULL ELSE 1 END) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        "dfbug/make_dt_interval_nan_col": (
            "SELECT id, make_dt_interval(i, 0, 0, d_nan) AS r FROM ice.sw.nn ORDER BY id"
        ),
        "dfbug/make_dt_interval_ovf_col": (
            "SELECT id, make_dt_interval(i_max, 0, 0, 0.0) AS r FROM ice.sw.nn ORDER BY id"
        ),
        "dfbug/in_subq_nullable_raw": (
            "SELECT id, (id IN (SELECT v FROM ice.sw.null_vals)) AS r FROM ice.sw.nn ORDER BY id"
        ),
        "dfbug/in_subq_nullable_isnull": (
            "SELECT id, (id IN (SELECT v FROM ice.sw.null_vals)) IS NULL AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        "dfbug/date_trunc_out_of_nanos": (
            "SELECT id, date_trunc('year', ts_old) AS r FROM ice.sw.nn ORDER BY id"
        ),
        "dfbug/exists_null_elem": (
            "SELECT id, exists(array(1, CAST(NULL AS INT)), x -> x > 2) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        "dfbug/subq_empty_isnull_where": (
            "SELECT id FROM ice.sw.nn WHERE (SELECT b FROM ice.sw.empty_nn) IS NULL ORDER BY id"
        ),
        "dfbug/subq_empty_coalesce_42": (
            "SELECT id, coalesce((SELECT b FROM ice.sw.empty_nn), 42L) AS r "
            "FROM ice.sw.nn ORDER BY id"
        ),
        "dfbug/cast_wide_try_abs": (
            "SELECT id, abs(CAST(TRY_CAST(s AS INT) AS BIGINT)) AS r FROM ice.sw.nn ORDER BY id"
        ),
        "dfbug/cast_wide_neg_try_abs": (
            "SELECT id, abs(CAST(-TRY_CAST(s AS INT) AS BIGINT)) AS r FROM ice.sw.nn ORDER BY id"
        ),
        "suf/pos_1L": "SELECT +1L AS r",
        "suf/1L_plus_1L": "SELECT 1L + 1L AS r",
        "suf/subq_pos_1S_tiny": (
            "SELECT CAST(r AS TINYINT) AS r2 FROM (SELECT CAST(+1S AS SMALLINT) AS r) t"
        ),
        "suf/cte_pos_1S_tiny": (
            "WITH c AS (SELECT CAST(+1S AS SMALLINT) AS r) SELECT CAST(r AS TINYINT) AS r2 FROM c"
        ),
        "suf/subq_pos_bigL_sm": (
            "SELECT CAST(r AS SMALLINT) AS r2 FROM (SELECT CAST(+4294967297L AS INT) AS r) t"
        ),
        "suf/neg_pos_bigL_int": "SELECT CAST(-+4294967297L AS INT) AS r",
        "suf/pos_neg_bigL_int": "SELECT CAST(+-4294967297L AS INT) AS r",
        "suf/neg_pos_129S_tiny": "SELECT CAST(-+129S AS TINYINT) AS r",
        "suf/null_bigint_plus_1L": "SELECT CAST(NULL AS BIGINT) + 1L AS r",
    }
    queries.update(extra)
    return queries


def run(engine: str, out_path: Path) -> None:
    native_file = None
    if engine == "repark":
        import repark

        native_file = repark._native.__file__
        print(f"repark._native.__file__ = {native_file}", flush=True)

    queries = build_queries()
    root = Path(tempfile.mkdtemp(prefix=f"ws2_sweep_{engine}_"))
    results: dict[str, Any] = {
        "engine": engine,
        "native_file": native_file,
        "query_count": len(queries),
        "modes": {},
    }
    try:
        for ansi in ("false", "true"):
            wh = root / f"wh_{ansi}"
            spark = make_session(engine, wh, ansi)
            setup_tables(spark)
            mode_out: dict[str, Any] = {}
            for key, sql in queries.items():
                mode_out[key] = capture_query(spark, sql, check_arrow=(engine == "repark"))
            results["modes"][ansi] = mode_out
            spark.stop()
    finally:
        shutil.rmtree(root, ignore_errors=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results, indent=2, sort_keys=True))
    print(f"WROTE {out_path} ({len(queries)} queries x 2 ANSI modes)", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=("repark", "spark"), required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    run(args.engine, args.out)


if __name__ == "__main__":
    main()

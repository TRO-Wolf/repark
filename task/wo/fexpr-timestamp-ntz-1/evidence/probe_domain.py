from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

ZONES = ("America/New_York", "Asia/Kolkata", "UTC")
ANSI_MODES = ("false", "true")
T = "TIMESTAMP '2024-01-01 02:00:00'"

NTZ_SHAPES: tuple[tuple[str, str, str], ...] = (
    ("cast_t_ntz", f"CAST({T} AS TIMESTAMP_NTZ)", "TIMESTAMP_LTZ"),
    ("ttype_ntz_lit", T, "TIMESTAMP_NTZ"),
    ("to_timestamp_ntz", "to_timestamp_ntz('2024-01-01 02:00:00')", "TIMESTAMP_LTZ"),
    ("make_timestamp_ntz", "make_timestamp_ntz(2024, 1, 1, 2, 0, 0)", "TIMESTAMP_LTZ"),
    ("cast_str_ntz", "CAST('2024-01-01 02:00:00' AS TIMESTAMP_NTZ)", "TIMESTAMP_LTZ"),
)

CARD_CASES_LTZ: tuple[tuple[str, str], ...] = (
    ("cast_t_ntz", f"CAST({T} AS TIMESTAMP_NTZ)"),
    ("try_cast_t_ntz", f"TRY_CAST({T} AS TIMESTAMP_NTZ)"),
    ("colon_t_ntz", f"{T}::TIMESTAMP_NTZ"),
    ("cast_tz_ntz", "CAST(TIMESTAMP '2024-01-01 02:00:00Z' AS TIMESTAMP_NTZ)"),
    ("cast_str_ntz", "CAST('2024-01-01 02:00:00' AS TIMESTAMP_NTZ)"),
    ("cast_str_off_ntz", "CAST('2024-01-01 02:00:00+05:30' AS TIMESTAMP_NTZ)"),
    ("try_cast_str_ntz", "TRY_CAST('2024-01-01 02:00:00' AS TIMESTAMP_NTZ)"),
    ("cast_bad_str_ntz", "CAST('bad' AS TIMESTAMP_NTZ)"),
    ("try_cast_bad_str_ntz", "TRY_CAST('bad' AS TIMESTAMP_NTZ)"),
    ("cast_date_ntz", "CAST(DATE '2024-01-01' AS TIMESTAMP_NTZ)"),
    ("cast_int_ntz", "CAST(0 AS TIMESTAMP_NTZ)"),
    ("cast_t_ntz_to_bigint", f"CAST(CAST({T} AS TIMESTAMP_NTZ) AS BIGINT)"),
    ("to_timestamp_ntz_str", "to_timestamp_ntz('2024-01-01 02:00:00')"),
    ("to_timestamp_ntz_t", f"to_timestamp_ntz({T})"),
    ("make_timestamp_ntz", "make_timestamp_ntz(2024, 1, 1, 2, 0, 0)"),
    ("ntz_literal", "TIMESTAMP_NTZ '2024-01-01 02:00:00'"),
)

CARD_CASES_NTZ_TTYPE: tuple[tuple[str, str], ...] = (
    ("ttype_ntz_lit", T),
    ("ttype_ntz_lit_z", "TIMESTAMP '2024-01-01 02:00:00Z'"),
    ("ttype_ntz_lit_off", "TIMESTAMP '2024-01-01 02:00:00+05:30'"),
    ("ttype_ntz_lit_gap", "TIMESTAMP '2024-03-10 02:30:00'"),
    ("ttype_ntz_cast_str", "CAST('2024-01-01 02:00:00' AS TIMESTAMP)"),
    ("ttype_ntz_cast_str_z", "CAST('2024-01-01 02:00:00Z' AS TIMESTAMP)"),
    ("ttype_ntz_try_cast_str", "TRY_CAST('2024-01-01 02:00:00' AS TIMESTAMP)"),
    ("ttype_ntz_try_cast_bad", "TRY_CAST('bad' AS TIMESTAMP)"),
    ("ttype_ntz_cast_date", "CAST(DATE '2024-01-01' AS TIMESTAMP)"),
    ("ttype_ntz_cast_null", "CAST(NULL AS TIMESTAMP)"),
    ("ttype_ntz_cast_int", "CAST(0 AS TIMESTAMP)"),
    ("ttype_ntz_cast_ltz", f"CAST({T} AS TIMESTAMP_LTZ)"),
    ("ttype_ntz_cast_str_ltz", "CAST('2024-01-01 02:00:00' AS TIMESTAMP_LTZ)"),
    ("ttype_ntz_to_timestamp", "to_timestamp('2024-01-01 02:00:00')"),
    ("ttype_ntz_lit_to_bigint", f"CAST({T} AS BIGINT)"),
)

COL_CASES_LTZ: tuple[tuple[str, str], ...] = (
    ("col_ts_cast_ntz", "CAST(ts AS TIMESTAMP_NTZ)"),
    ("col_ts_try_cast_ntz", "TRY_CAST(ts AS TIMESTAMP_NTZ)"),
    ("col_s_cast_ntz", "CAST(s AS TIMESTAMP_NTZ)"),
    ("col_d_cast_ntz", "CAST(d AS TIMESTAMP_NTZ)"),
)

COL_CASES_NTZ_TTYPE: tuple[tuple[str, str], ...] = (
    ("ttype_ntz_col_s_cast_ts", "CAST(s AS TIMESTAMP)"),
    ("ttype_ntz_col_ts_cast_ts", "CAST(ts AS TIMESTAMP)"),
    ("ttype_ntz_col_d_cast_ts", "CAST(d AS TIMESTAMP)"),
    ("ttype_ntz_col_s_cast_ltz", "CAST(s AS TIMESTAMP_LTZ)"),
)

UNALIASED_CASES: tuple[tuple[str, str, str], ...] = (
    ("unaliased_z", "TIMESTAMP '2024-01-01 02:00:00Z'", "TIMESTAMP_LTZ"),
    ("unaliased_off", "TIMESTAMP '2024-01-01 02:00:00+05:30'", "TIMESTAMP_LTZ"),
    ("unaliased_zoneless", T, "TIMESTAMP_LTZ"),
    ("unaliased_cast_ntz", f"CAST({T} AS TIMESTAMP_NTZ)", "TIMESTAMP_LTZ"),
    ("unaliased_ttype_ntz", T, "TIMESTAMP_NTZ"),
    ("unaliased_ntz_lit", "TIMESTAMP_NTZ '2024-01-01 02:00:00'", "TIMESTAMP_LTZ"),
)

NEIGHBOUR_CASES: tuple[tuple[str, str, str], ...] = (
    ("nb_ltz_zoneless", T, "TIMESTAMP_LTZ"),
    ("nb_ltz_z", "TIMESTAMP '2024-01-01 02:00:00Z'", "TIMESTAMP_LTZ"),
    ("nb_ltz_off", "TIMESTAMP '2024-01-01 02:00:00+05:30'", "TIMESTAMP_LTZ"),
    ("nb_ltz_gap", "TIMESTAMP '2024-03-10 02:30:00'", "TIMESTAMP_LTZ"),
    ("nb_ltz_overlap", "TIMESTAMP '2024-11-03 01:30:00'", "TIMESTAMP_LTZ"),
    ("nb_cast_str_ltz", "CAST('2024-01-01 02:00:00' AS TIMESTAMP_LTZ)", "TIMESTAMP_LTZ"),
    ("nb_cast_z_ltz", "CAST('2024-01-01 02:00:00Z' AS TIMESTAMP_LTZ)", "TIMESTAMP_LTZ"),
    (
        "nb_cast_ntz_ltz",
        "CAST(TIMESTAMP_NTZ '2024-01-01 02:00:00' AS TIMESTAMP_LTZ)",
        "TIMESTAMP_LTZ",
    ),
    ("nb_try_cast_ltz", "TRY_CAST('2024-01-01 02:00:00' AS TIMESTAMP_LTZ)", "TIMESTAMP_LTZ"),
    ("nb_date_lit", "DATE '2024-01-01'", "TIMESTAMP_LTZ"),
    ("nb_cast_str_date", "CAST('2024-01-01' AS DATE)", "TIMESTAMP_LTZ"),
    ("nb_cast_date_ts", "CAST(DATE '2024-01-01' AS TIMESTAMP)", "TIMESTAMP_LTZ"),
    ("nb_cast_date_ntz", "CAST(DATE '2024-01-01' AS TIMESTAMP_NTZ)", "TIMESTAMP_LTZ"),
    ("nb_ltz_plus_1h", f"{T} + INTERVAL 1 HOUR", "TIMESTAMP_LTZ"),
    ("nb_ntz_plus_1h", "TIMESTAMP_NTZ '2024-01-01 02:00:00' + INTERVAL 1 HOUR", "TIMESTAMP_LTZ"),
    ("nb_date_plus_1d", "DATE '2024-01-01' + INTERVAL 1 DAY", "TIMESTAMP_LTZ"),
    ("nb_cast_ntz_plus_1h", f"CAST({T} AS TIMESTAMP_NTZ) + INTERVAL 1 HOUR", "TIMESTAMP_LTZ"),
    ("nb_ttype_ltz_explicit_lit", T, "TIMESTAMP_LTZ"),
    ("nb_ttype_ltz_explicit_cast", "CAST('2024-01-01 02:00:00' AS TIMESTAMP)", "TIMESTAMP_LTZ"),
)


def err_tag(exc: BaseException) -> str:
    text = str(exc)
    match = re.search(r"\[([A-Z][A-Z0-9_.]+)\]", text)
    if match:
        return f"ERR:{match.group(1)}"
    return f"ERR:{type(exc).__name__}:{text.splitlines()[0][:120]}"


def new_session(engine: str, warehouse: Path, timestamp_type: str, app: str) -> tuple[Any, Any]:
    if engine == "spark":
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as sf

        jar = os.environ["SPARK_ICEBERG_JAR"]
        session = (
            SparkSession.builder.master("local[2]")
            .appName(app)
            .config("spark.jars", jar)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.ice.type", "hadoop")
            .config("spark.sql.catalog.ice.warehouse", str(warehouse))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.timestampType", timestamp_type)
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("ERROR")
        return session, sf
    from repark import ReparkSession
    from repark.spark import functions as sf
    from repark.spark.session import _reset_active_session_for_tests

    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName(app)
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", str(warehouse))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.timestampType", timestamp_type)
        .getOrCreate()
    )
    return session, sf


def stop_session(engine: str, session: Any) -> None:
    session.stop()
    if engine == "repark":
        from repark.spark.session import _reset_active_session_for_tests

        _reset_active_session_for_tests()


def eval_frame(frame: Any, col_name: str, sf: Any) -> dict[str, Any]:
    try:
        dtype = dict(frame.dtypes)[col_name]
        projected = frame.select(sf.col(col_name).cast("string").alias("s"))
        val = projected.collect()[0][0]
        extra: dict[str, Any] = {"dtype": dtype, "str": val}
        if dtype == "timestamp":
            ep = frame.select(sf.unix_timestamp(sf.col(col_name)).alias("u")).collect()[0][0]
            extra["unix_s"] = ep
        return extra
    except Exception as exc:
        return {"err": err_tag(exc)}


def run_door(session: Any, sf: Any, base: Any, door: str, expr_sql: str) -> dict[str, Any]:
    try:
        if door == "sql":
            df = session.sql(f"SELECT {expr_sql} AS v")
        elif door == "select":
            df = base.select(sf.expr(expr_sql).alias("v"))
        elif door == "withColumn":
            df = base.withColumn("v", sf.expr(expr_sql)).select("v")
        elif door == "selectExpr":
            df = base.selectExpr(f"{expr_sql} AS v")
        else:
            raise ValueError(door)
        return eval_frame(df, "v", sf)
    except Exception as exc:
        return {"err": err_tag(exc)}


def run_col_door(session: Any, sf: Any, base: Any, door: str, expr_sql: str) -> dict[str, Any]:
    try:
        if door == "sql":
            base.createOrReplaceTempView("step0_cols")
            df = session.sql(f"SELECT {expr_sql} AS v FROM step0_cols")
        elif door == "select":
            df = base.select(sf.expr(expr_sql).alias("v"))
        elif door == "withColumn":
            df = base.withColumn("v", sf.expr(expr_sql)).select("v")
        elif door == "selectExpr":
            df = base.selectExpr(f"{expr_sql} AS v")
        else:
            raise ValueError(door)
        return eval_frame(df, "v", sf)
    except Exception as exc:
        return {"err": err_tag(exc)}


def measure_unaliased(session: Any, sf: Any, base: Any, door: str, expr_sql: str) -> dict[str, Any]:
    try:
        if door == "sql":
            df = session.sql(f"SELECT {expr_sql}")
        elif door == "select":
            df = base.select(sf.expr(expr_sql))
        elif door == "selectExpr":
            df = base.selectExpr(expr_sql)
        else:
            raise ValueError(door)
        col_name, dtype = df.dtypes[0]
        val = df.select(sf.col(f"`{col_name}`").cast("string")).collect()[0][0]
        return {"name": col_name, "dtype": dtype, "str": val}
    except Exception as exc:
        return {"err": err_tag(exc)}


def measure_zone_change(session: Any, sf: Any, expr_sql: str, target_zone: str) -> dict[str, Any]:
    try:
        session.conf.set("spark.sql.session.timeZone", "America/New_York")
        base = session.sql("SELECT 1 AS id")
        expr_col = sf.expr(expr_sql)
        df = base.withColumn("v", expr_col).withColumn(
            "ntz", sf.expr("TIMESTAMP_NTZ '2024-01-01 02:00:00'")
        )
        session.conf.set("spark.sql.session.timeZone", target_zone)
        dtype = dict(df.dtypes)["v"]
        out: dict[str, Any] = {"dtype": dtype}
        for label, col_obj in (
            ("str", sf.col("v").cast("string")),
            ("hour", sf.hour(sf.col("v"))),
            ("fmt", sf.date_format(sf.col("v"), "yyyy-MM-dd HH:mm")),
            ("unix", sf.unix_timestamp(sf.col("v"))),
            ("eq_ntz", sf.col("v") == sf.col("ntz")),
        ):
            try:
                out[label] = df.select(col_obj.alias("x")).collect()[0][0]
            except Exception as exc:
                out[label] = err_tag(exc)
        return out
    except Exception as exc:
        return {"err": err_tag(exc)}
    finally:
        session.conf.set("spark.sql.session.timeZone", "UTC")


def measure_mixing_and_filter(session: Any, sf: Any, expr_sql: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    base = session.sql(
        "SELECT 1 AS id, TIMESTAMP_NTZ '2024-01-01 02:00:00' AS ntz, "
        "TIMESTAMP_NTZ '2024-01-01 03:00:00' AS ntz_later"
    )
    try:
        df_expr = session.sql("SELECT 1 AS id").select(sf.expr(expr_sql).alias("v"))
        df_ntz = session.sql("SELECT TIMESTAMP_NTZ '2024-01-01 02:00:00' AS v")
        unioned = df_expr.union(df_ntz)
        u_dtype = unioned.dtypes[0][1]
        u_vals = [r[0] for r in unioned.select(sf.col("v").cast("string")).collect()]
        out["union"] = {"dtype": u_dtype, "vals": u_vals}
    except Exception as exc:
        out["union"] = {"err": err_tag(exc)}

    for label, fn in (
        (
            "coalesce_v_ntz",
            lambda: base.select(sf.coalesce(sf.expr(expr_sql), sf.col("ntz")).alias("v")),
        ),
        (
            "coalesce_null_v",
            lambda: base.select(
                sf.coalesce(sf.expr("CAST(NULL AS TIMESTAMP_NTZ)"), sf.expr(expr_sql)).alias("v")
            ),
        ),
        (
            "coalesce_ntz_v",
            lambda: base.select(sf.coalesce(sf.col("ntz"), sf.expr(expr_sql)).alias("v")),
        ),
        (
            "case_v_ntz",
            lambda: base.select(
                sf.when(sf.col("id") == 1, sf.expr(expr_sql)).otherwise(sf.col("ntz")).alias("v")
            ),
        ),
        (
            "eq_ntz",
            lambda: base.select((sf.expr(expr_sql) == sf.col("ntz")).alias("v")),
        ),
        (
            "lt_ntz_later",
            lambda: base.select((sf.expr(expr_sql) < sf.col("ntz_later")).alias("v")),
        ),
        (
            "eq_null_safe",
            lambda: base.select(sf.expr(f"({expr_sql}) <=> ntz").alias("v")),
        ),
    ):
        try:
            df = fn()
            out[label] = eval_frame(df, "v", sf)
        except Exception as exc:
            out[label] = {"err": err_tag(exc)}

    try:
        left = session.sql("SELECT 1 AS lid").withColumn("v", sf.expr(expr_sql))
        right = session.sql("SELECT 2 AS rid, TIMESTAMP_NTZ '2024-01-01 02:00:00' AS ntz")
        joined = left.join(right, left["v"] == right["ntz"], "inner")
        out["join"] = {
            "count": joined.count(),
            "v_dtype": dict(joined.dtypes)["v"],
            "ntz_dtype": dict(joined.dtypes)["ntz"],
        }
    except Exception as exc:
        out["join"] = {"err": err_tag(exc)}

    for label, fn in (
        ("filter_col_eq_fexpr", lambda: base.filter(sf.col("ntz") == sf.expr(expr_sql))),
        ("filter_fexpr_pred", lambda: base.filter(sf.expr(f"ntz = ({expr_sql})"))),
        ("filter_sql_str", lambda: base.filter(f"ntz = ({expr_sql})")),
    ):
        try:
            out[label] = {"count": fn().count()}
        except Exception as exc:
            out[label] = {"err": err_tag(exc)}

    return out


def read_parquet_raw(path: Path) -> dict[str, Any]:
    files = sorted(path.glob("*.parquet")) if path.is_dir() else [path]
    table = pq.read_table(files)
    field = table.schema.field("v")
    col = table.column("v")
    raw_i64 = (
        col.cast(pa.int64()).to_pylist() if pa.types.is_timestamp(field.type) else col.to_pylist()
    )
    return {"arrow_type": str(field.type), "raw_i64": raw_i64}


def readback_table(session: Any, sf: Any, table_name: str, zone: str) -> dict[str, Any]:
    session.conf.set("spark.sql.session.timeZone", zone)
    df = session.table(table_name)
    dtype = dict(df.dtypes)["v"]
    row = df.select(
        sf.col("v").cast("string").alias("s"),
        sf.hour(sf.col("v")).alias("h"),
    ).collect()[0]
    return {"dtype": dtype, "str": row[0], "hour": row[1]}


def readback_parquet(session: Any, sf: Any, path: Path, zone: str) -> dict[str, Any]:
    session.conf.set("spark.sql.session.timeZone", zone)
    df = session.read.parquet(str(path))
    dtype = dict(df.dtypes)["v"]
    row = df.select(
        sf.col("v").cast("string").alias("s"),
        sf.hour(sf.col("v")).alias("h"),
    ).collect()[0]
    return {"dtype": dtype, "str": row[0], "hour": row[1]}


def measure_writes(
    engine: str,
    artifacts: Path,
    ansi: str,
) -> dict[str, Any]:
    results: dict[str, Any] = {}
    wh_ltz = artifacts / f"wh_ltz_ansi_{ansi}"
    wh_ntz = artifacts / f"wh_ntz_ansi_{ansi}"
    pq_root = artifacts / f"pq_ansi_{ansi}"
    for path in (wh_ltz, wh_ntz, pq_root):
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True)

    write_cases: tuple[tuple[str, str, str, str], ...] = (
        ("cast_t_ntz_fexpr", f"CAST({T} AS TIMESTAMP_NTZ)", "TIMESTAMP_LTZ", "fexpr"),
        ("cast_t_ntz_sql", f"CAST({T} AS TIMESTAMP_NTZ)", "TIMESTAMP_LTZ", "sql"),
        ("ttype_ntz_lit_fexpr", T, "TIMESTAMP_NTZ", "fexpr"),
        ("ttype_ntz_lit_sql", T, "TIMESTAMP_NTZ", "sql"),
        (
            "to_timestamp_ntz_fexpr",
            "to_timestamp_ntz('2024-01-01 02:00:00')",
            "TIMESTAMP_LTZ",
            "fexpr",
        ),
        (
            "make_timestamp_ntz_fexpr",
            "make_timestamp_ntz(2024, 1, 1, 2, 0, 0)",
            "TIMESTAMP_LTZ",
            "fexpr",
        ),
        (
            "cast_str_ntz_fexpr",
            "CAST('2024-01-01 02:00:00' AS TIMESTAMP_NTZ)",
            "TIMESTAMP_LTZ",
            "fexpr",
        ),
    )

    for ttype, wh in (("TIMESTAMP_LTZ", wh_ltz), ("TIMESTAMP_NTZ", wh_ntz)):
        session, sf = new_session(engine, wh, ttype, f"step0-writes-{ttype}-{ansi}")
        try:
            session.sql("CREATE NAMESPACE IF NOT EXISTS ice.db").collect()
            session.conf.set("spark.sql.ansi.enabled", ansi)
            for name, expr_sql, case_ttype, door in write_cases:
                if case_ttype != ttype:
                    continue
                cell: dict[str, Any] = {}
                session.conf.set("spark.sql.session.timeZone", "America/New_York")
                for col_type, suffix in (("TIMESTAMP_NTZ", "ntz"), ("TIMESTAMP_LTZ", "ltz")):
                    tbl = f"ice.db.{name}_{suffix}"
                    try:
                        session.sql(
                            f"CREATE TABLE {tbl} (id INT, v {col_type}) USING iceberg"
                        ).collect()
                        if door == "sql":
                            session.sql(
                                f"INSERT INTO {tbl} SELECT 1 AS id, {expr_sql} AS v"
                            ).collect()
                        else:
                            df = session.sql("SELECT 1 AS id").withColumn("v", sf.expr(expr_sql))
                            df.writeTo(tbl).append()
                        cell[f"ice_{suffix}_ny"] = readback_table(
                            session, sf, tbl, "America/New_York"
                        )
                        cell[f"ice_{suffix}_utc"] = readback_table(session, sf, tbl, "UTC")
                    except Exception as exc:
                        cell[f"ice_{suffix}"] = {"err": err_tag(exc)}
                    finally:
                        session.conf.set("spark.sql.session.timeZone", "America/New_York")

                pq_dir = pq_root / name
                try:
                    if door == "sql":
                        df = session.sql(f"SELECT 1 AS id, {expr_sql} AS v")
                    else:
                        df = session.sql("SELECT 1 AS id").withColumn("v", sf.expr(expr_sql))
                    df.write.mode("overwrite").parquet(str(pq_dir))
                    cell["pq_raw"] = read_parquet_raw(pq_dir)
                    cell["pq_ny"] = readback_parquet(session, sf, pq_dir, "America/New_York")
                    cell["pq_utc"] = readback_parquet(session, sf, pq_dir, "UTC")
                except Exception as exc:
                    cell["pq"] = {"err": err_tag(exc)}
                finally:
                    session.conf.set("spark.sql.session.timeZone", "America/New_York")
                results[name] = cell
        finally:
            stop_session(engine, session)

    return results


def cross_read_artifacts(engine: str, target_artifacts: Path) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for ansi in ANSI_MODES:
        ansi_out: dict[str, Any] = {}
        pq_root = target_artifacts / f"pq_ansi_{ansi}"
        for ttype, wh_name in (
            ("TIMESTAMP_LTZ", f"wh_ltz_ansi_{ansi}"),
            ("TIMESTAMP_NTZ", f"wh_ntz_ansi_{ansi}"),
        ):
            wh = target_artifacts / wh_name
            if not wh.exists():
                continue
            session, sf = new_session(engine, wh, "TIMESTAMP_LTZ", f"step0-xread-{ttype}-{ansi}")
            try:
                session.conf.set("spark.sql.ansi.enabled", ansi)
                db_dir = wh / "db"
                if db_dir.exists():
                    for tbl_dir in sorted(db_dir.iterdir()):
                        if not tbl_dir.is_dir():
                            continue
                        tbl = f"ice.db.{tbl_dir.name}"
                        try:
                            ansi_out[f"ice_{tbl_dir.name}_ny"] = readback_table(
                                session, sf, tbl, "America/New_York"
                            )
                            ansi_out[f"ice_{tbl_dir.name}_utc"] = readback_table(
                                session, sf, tbl, "UTC"
                            )
                        except Exception as exc:
                            ansi_out[f"ice_{tbl_dir.name}"] = {"err": err_tag(exc)}
                if ttype == "TIMESTAMP_LTZ" and pq_root.exists():
                    for pq_dir in sorted(pq_root.iterdir()):
                        if not pq_dir.is_dir():
                            continue
                        try:
                            ansi_out[f"pq_{pq_dir.name}_ny"] = readback_parquet(
                                session, sf, pq_dir, "America/New_York"
                            )
                            ansi_out[f"pq_{pq_dir.name}_utc"] = readback_parquet(
                                session, sf, pq_dir, "UTC"
                            )
                        except Exception as exc:
                            ansi_out[f"pq_{pq_dir.name}"] = {"err": err_tag(exc)}
            finally:
                stop_session(engine, session)
        out[ansi] = ansi_out
    return out


def measure_all(engine: str, artifacts: Path) -> dict[str, Any]:
    data: dict[str, Any] = {
        "engine": engine,
        "card_cells": {},
        "col_cells": {},
        "unaliased_cells": {},
        "zone_change_cells": {},
        "mixing_cells": {},
        "neighbour_cells": {},
        "write_cells": {},
        "runtime_conf_cells": {},
    }
    dummy_wh = artifacts / "wh_probe"
    dummy_wh.mkdir(parents=True, exist_ok=True)

    for ttype in ("TIMESTAMP_LTZ", "TIMESTAMP_NTZ"):
        session, sf = new_session(engine, dummy_wh, ttype, f"step0-main-{ttype}")
        try:
            for zone in ZONES:
                session.conf.set("spark.sql.session.timeZone", zone)
                for ansi in ANSI_MODES:
                    session.conf.set("spark.sql.ansi.enabled", ansi)
                    base = session.sql("SELECT 1 AS id")
                    col_base = session.sql(
                        f"SELECT 1 AS id, {T} AS ts, "
                        "'2024-01-01 02:00:00' AS s, DATE '2024-01-01' AS d"
                    )
                    cases = CARD_CASES_LTZ if ttype == "TIMESTAMP_LTZ" else CARD_CASES_NTZ_TTYPE
                    for name, expr_sql in cases:
                        for door in ("sql", "select", "withColumn", "selectExpr"):
                            key = f"{name}|{door}|{zone}|ansi={ansi}"
                            data["card_cells"][key] = run_door(session, sf, base, door, expr_sql)

                    col_cases = COL_CASES_LTZ if ttype == "TIMESTAMP_LTZ" else COL_CASES_NTZ_TTYPE
                    for name, expr_sql in col_cases:
                        for door in ("sql", "select", "withColumn", "selectExpr"):
                            key = f"{name}|{door}|{zone}|ansi={ansi}"
                            data["col_cells"][key] = run_col_door(
                                session, sf, col_base, door, expr_sql
                            )

                    for name, expr_sql, case_ttype in UNALIASED_CASES:
                        if case_ttype != ttype:
                            continue
                        for door in ("sql", "select", "selectExpr"):
                            key = f"{name}|{door}|{zone}|ansi={ansi}"
                            data["unaliased_cells"][key] = measure_unaliased(
                                session, sf, base, door, expr_sql
                            )

                    for name, expr_sql, case_ttype in NEIGHBOUR_CASES:
                        if case_ttype != ttype:
                            continue
                        for door in ("sql", "select"):
                            key = f"{name}|{door}|{zone}|ansi={ansi}"
                            data["neighbour_cells"][key] = run_door(
                                session, sf, base, door, expr_sql
                            )

                    for shape_name, expr_sql, shape_ttype in NTZ_SHAPES:
                        if shape_ttype != ttype:
                            continue
                        key = f"{shape_name}|{zone}|ansi={ansi}"
                        data["mixing_cells"][key] = measure_mixing_and_filter(session, sf, expr_sql)

            for ansi in ANSI_MODES:
                session.conf.set("spark.sql.ansi.enabled", ansi)
                for shape_name, expr_sql, shape_ttype in (
                    *NTZ_SHAPES,
                    ("ntz_control", "TIMESTAMP_NTZ '2024-01-01 02:00:00'", "TIMESTAMP_LTZ"),
                ):
                    if shape_ttype != ttype:
                        continue
                    for target_zone in ("America/New_York", "Asia/Kolkata", "UTC"):
                        key = f"{shape_name}|NY->{target_zone}|ansi={ansi}"
                        data["zone_change_cells"][key] = measure_zone_change(
                            session, sf, expr_sql, target_zone
                        )

            if ttype == "TIMESTAMP_LTZ":
                session.conf.set("spark.sql.session.timeZone", "America/New_York")
                session.conf.set("spark.sql.ansi.enabled", "true")
                session.conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ")
                base = session.sql("SELECT 1 AS id")
                for door in ("sql", "select", "selectExpr"):
                    data["runtime_conf_cells"][f"conf_set_ntz|{door}"] = run_door(
                        session, sf, base, door, T
                    )
                session.conf.set("spark.sql.timestampType", "TIMESTAMP_LTZ")
        finally:
            stop_session(engine, session)

    for ansi in ANSI_MODES:
        data["write_cells"][ansi] = measure_writes(engine, artifacts, ansi)

    return data


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=("spark", "repark"), required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--cross-read", type=Path, default=None)
    args = parser.parse_args()

    if args.cross_read is not None:
        result = cross_read_artifacts(args.engine, args.cross_read)
    else:
        args.artifacts.mkdir(parents=True, exist_ok=True)
        result = measure_all(args.engine, args.artifacts)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(f"WROTE {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

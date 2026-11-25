from __future__ import annotations

import json
import os
import re
import shutil
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any


def build_session(engine: str, warehouse: str) -> tuple[Any, Any]:
    if engine == "spark":
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as spark_fn

        jar = os.environ["SPARK_ICEBERG_JAR"]
        sess = (
            SparkSession.builder.master("local[2]")
            .appName("iocast_edge_probe")
            .config("spark.jars", jar)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        sess.sparkContext.setLogLevel("OFF")
        return sess, spark_fn
    from repark import SparkSession
    from repark.spark import functions as spark_fn

    sess = (
        SparkSession.builder.config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    return sess, spark_fn


def parse_args(argv: list[str]) -> tuple[str, Path]:
    if len(argv) == 3 and argv[0] == "--engine":
        return argv[1], Path(argv[2])
    if len(argv) == 2 and argv[0].startswith("--engine="):
        return argv[0].split("=", 1)[1], Path(argv[1])
    if len(argv) == 2 and argv[0] in ("spark", "repark"):
        return argv[0], Path(argv[1])
    if len(argv) == 1:
        return "repark", Path(argv[0])
    raise SystemExit("usage: probe_edge.py [--engine spark|repark] <out.json>")


def err_cond(message: str) -> str | None:
    match = re.search(r"\[([A-Z][A-Z0-9_.]*)\]", message)
    return match.group(1) if match else None


def fresh_dbl(spark: Any, name: str, seed: bool = True) -> str:
    fq = f"local.db.{name}"
    spark.sql(f"DROP TABLE IF EXISTS {fq}").collect()
    spark.sql(f"CREATE TABLE {fq} (x DOUBLE) USING iceberg").collect()
    if seed:
        spark.sql(f"INSERT INTO {fq} VALUES (1.0)").collect()
    return fq


def record_cell(
    out: dict[str, Any],
    spark: Any,
    name: str,
    action: Callable[[], Any],
    fq: str,
) -> None:
    try:
        action()
        count = spark.sql(f"SELECT * FROM {fq}").count()
        out[name] = {"ok": True, "cond": None, "rows": count}
    except Exception as exc:
        msg = str(exc)
        count = spark.sql(f"SELECT * FROM {fq}").count()
        out[name] = {"ok": False, "cond": err_cond(msg), "head": msg[:140], "rows": count}


def run_all(engine: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    wh = tempfile.mkdtemp(prefix=f"wh_edge_{engine}_", dir=tempfile.gettempdir())
    spark, spark_fn = build_session(engine, wh)
    try:
        spark.conf.set("spark.sql.ansi.enabled", "true")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db").collect()
        spark.sql("CREATE TABLE local.db.real_empty (s STRING) USING iceberg").collect()

        for ansi_val in ("true", "false"):
            spark.conf.set("spark.sql.ansi.enabled", ansi_val)
            fq = fresh_dbl(spark, f"e_union_rc_{ansi_val}")
            record_cell(
                out,
                spark,
                f"{ansi_val}|union_real_and_const",
                lambda f=fq: spark.sql(
                    f"INSERT OVERWRITE {f} SELECT CAST(s AS DOUBLE) FROM "
                    "(SELECT s FROM local.db.real_empty UNION ALL SELECT 'x' AS s) u WHERE 1 = 0"
                ).collect(),
                fq,
            )
            fq = fresh_dbl(spark, f"e_union_vb_{ansi_val}")
            record_cell(
                out,
                spark,
                f"{ansi_val}|union_valid_and_bad",
                lambda f=fq: spark.sql(
                    f"INSERT OVERWRITE {f} SELECT CAST(s AS DOUBLE) FROM "
                    "(SELECT '1.0' AS s UNION ALL SELECT 'x' AS s) u WHERE 1 = 0"
                ).collect(),
                fq,
            )
            fq = fresh_dbl(spark, f"e_ljoin_{ansi_val}")
            record_cell(
                out,
                spark,
                f"{ansi_val}|left_join_const_right",
                lambda f=fq: spark.sql(
                    f"INSERT OVERWRITE {f} SELECT CAST(r.s AS DOUBLE) FROM "
                    "(SELECT 1 AS id) l LEFT JOIN (SELECT 1 AS id, 'x' AS s) r "
                    "ON l.id = r.id WHERE 1 = 0"
                ).collect(),
                fq,
            )
            fq = fresh_dbl(spark, f"e_gb_{ansi_val}")
            record_cell(
                out,
                spark,
                f"{ansi_val}|groupby_const_having_false",
                lambda f=fq: spark.sql(
                    f"INSERT OVERWRITE {f} SELECT CAST(s AS DOUBLE) FROM "
                    "(SELECT 'x' AS s) q GROUP BY s HAVING 1 = 0"
                ).collect(),
                fq,
            )
            fq = fresh_dbl(spark, f"e_trywrap_{ansi_val}")
            record_cell(
                out,
                spark,
                f"{ansi_val}|try_cast_wrapping_cast",
                lambda f=fq: spark.sql(
                    f"INSERT OVERWRITE {f} SELECT CAST(TRY_CAST(CAST(s AS DOUBLE) AS INT) "
                    "AS DOUBLE) FROM (SELECT 'x' AS s) q WHERE 1 = 0"
                ).collect(),
                fq,
            )
            fq = fresh_dbl(spark, f"e_valcast_{ansi_val}")
            record_cell(
                out,
                spark,
                f"{ansi_val}|values_cast_where_false",
                lambda f=fq: spark.sql(
                    f"INSERT OVERWRITE {f} SELECT x FROM "
                    "(SELECT * FROM VALUES (CAST('x' AS DOUBLE)) AS v(x)) q WHERE 1 = 0"
                ).collect(),
                fq,
            )
            fq = fresh_dbl(spark, f"e_upd_dbl_{ansi_val}", seed=False)
            record_cell(
                out,
                spark,
                f"{ansi_val}|update_empty_no_where",
                lambda f=fq: spark.sql(
                    f"UPDATE {f} SET x = CAST(CAST('x' AS TIMESTAMP) AS DOUBLE)"
                ).collect(),
                fq,
            )
            fq = f"local.db.e_upd_ts_{ansi_val}"
            spark.sql(f"DROP TABLE IF EXISTS {fq}").collect()
            spark.sql(f"CREATE TABLE {fq} (x TIMESTAMP) USING iceberg").collect()
            record_cell(
                out,
                spark,
                f"{ansi_val}|update_empty_ts_no_where",
                lambda f=fq: spark.sql(f"UPDATE {f} SET x = CAST('x' AS TIMESTAMP)").collect(),
                fq,
            )
            fq = f"local.db.e_del_fcol_{ansi_val}"
            spark.sql(f"DROP TABLE IF EXISTS {fq}").collect()
            spark.sql(f"CREATE TABLE {fq} (x TIMESTAMP) USING iceberg").collect()
            spark.sql(f"INSERT INTO {fq} VALUES (TIMESTAMP '2024-01-01 00:00:00')").collect()
            record_cell(
                out,
                spark,
                f"{ansi_val}|where_false_and_col_ts",
                lambda f=fq: spark.sql(
                    f"DELETE FROM {f} WHERE false AND x = CAST('x' AS TIMESTAMP)"
                ).collect(),
                fq,
            )
            fq = f"local.db.e_lit_ts_lim0_{ansi_val}"
            spark.sql(f"DROP TABLE IF EXISTS {fq}").collect()
            spark.sql(f"CREATE TABLE {fq} (x TIMESTAMP) USING iceberg").collect()
            spark.sql(f"INSERT INTO {fq} VALUES (TIMESTAMP '2024-01-01 00:00:00')").collect()
            record_cell(
                out,
                spark,
                f"{ansi_val}|lit_ts_limit0",
                lambda f=fq: spark.sql(
                    f"INSERT OVERWRITE {f} SELECT CAST('x' AS TIMESTAMP) LIMIT 0"
                ).collect(),
                fq,
            )

        spark.conf.set("spark.sql.ansi.enabled", "false")
        spark.sql("CREATE OR REPLACE TEMP VIEW tv_off_src AS SELECT 'x' AS s").collect()
        spark.sql(
            "CREATE OR REPLACE TEMP VIEW tv_off_cast AS "
            "SELECT CAST(s AS DOUBLE) AS x FROM (SELECT 'x' AS s) q WHERE 1 = 0"
        ).collect()
        df_off = spark.sql("SELECT CAST(s AS DOUBLE) AS x FROM (SELECT 'x' AS s) q WHERE 1 = 0")

        spark.conf.set("spark.sql.ansi.enabled", "true")
        df_on = spark.sql("SELECT CAST(s AS DOUBLE) AS x FROM (SELECT 'x' AS s) q WHERE 1 = 0")

        fq = fresh_dbl(spark, "e_tv_off_cast_on")
        record_cell(
            out,
            spark,
            "cap|tv_created_off_cast_on",
            lambda f=fq: spark.sql(
                f"INSERT OVERWRITE {f} SELECT CAST(s AS DOUBLE) FROM tv_off_src WHERE 1 = 0"
            ).collect(),
            fq,
        )
        fq = fresh_dbl(spark, "e_tv_cast_off_on")
        record_cell(
            out,
            spark,
            "cap|tv_cast_created_off_read_on",
            lambda f=fq: spark.sql(f"INSERT OVERWRITE {f} SELECT x FROM tv_off_cast").collect(),
            fq,
        )
        fq = fresh_dbl(spark, "e_df_off_on")
        record_cell(
            out,
            spark,
            "cap|df_built_off_written_on",
            lambda f=fq: df_off.writeTo(f).overwrite(spark_fn.lit(True)),
            fq,
        )
        spark.conf.set("spark.sql.ansi.enabled", "false")
        fq = fresh_dbl(spark, "e_df_on_off")
        record_cell(
            out,
            spark,
            "cap|df_built_on_written_off",
            lambda f=fq: df_on.writeTo(f).overwrite(spark_fn.lit(True)),
            fq,
        )
    finally:
        spark.stop()
        shutil.rmtree(wh, ignore_errors=True)
    return out


def main() -> int:
    engine, out_path = parse_args(sys.argv[1:])
    out = run_all(engine)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"done engine={engine} cells={len(out)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

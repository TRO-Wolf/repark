from __future__ import annotations

import json
import re
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from repark import SparkSession
from repark.spark import functions as spark_fn


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


def run_all() -> dict[str, Any]:
    out: dict[str, Any] = {}
    with tempfile.TemporaryDirectory() as tmp:
        spark = (
            SparkSession.builder.config("spark.sql.ansi.enabled", "true")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", str(Path(tmp) / "wh"))
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
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
        spark.stop()
    return out


def main() -> int:
    Path(sys.argv[1]).write_text(json.dumps(run_all(), indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

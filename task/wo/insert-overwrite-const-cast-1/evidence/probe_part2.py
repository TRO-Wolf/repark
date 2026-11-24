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
            .appName("iocast_step0_part2")
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
        SparkSession.builder.appName("iocast_step0_part2")
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
    for line in msg.splitlines()[:5]:
        if "[" in line and "]" in line:
            s = line.find("[")
            e = line.find("]", s)
            cand = line[s + 1 : e]
            if e > s + 1 and cand.replace(".", "").replace("_", "").isupper():
                cond = cand
                break
    return {"ok": False, "exc": type(exc).__name__, "cond": cond, "head": first[:180]}


def table_state(sess: Any, table: str) -> Any:
    try:
        rows = [[str(v) for v in r] for r in sess.sql(f"SELECT * FROM {table}").collect()]
        rows.sort()
        return {"exists": True, "count": len(rows), "rows": rows[:5]}
    except Exception:
        return {"exists": False, "count": None, "rows": None}


def run_door_action(action: Any) -> dict[str, Any]:
    try:
        res = action()
        if hasattr(res, "collect"):
            res.collect()
        return {"ok": True}
    except Exception as exc:
        return err_summary(exc)


def fresh_unpart(
    sess: Any, seq_box: list[int], col_type: str, seed_lit: str, seed: bool = True
) -> str:
    seq_box[0] += 1
    table = f"ice.ns.d_{seq_box[0]}"
    sess.sql(f"CREATE TABLE {table} (x {col_type}) USING iceberg").collect()
    if seed:
        sess.sql(f"INSERT INTO {table} VALUES ({seed_lit})").collect()
    return table


def fresh_part(sess: Any, seq_box: list[int], col_type: str, seed_lit: str) -> str:
    seq_box[0] += 1
    table = f"ice.ns.dp_{seq_box[0]}"
    sess.sql(
        f"CREATE TABLE {table} (p INT, x {col_type}) USING iceberg PARTITIONED BY (p)"
    ).collect()
    sess.sql(f"INSERT INTO {table} VALUES (1, {seed_lit}), (2, {seed_lit})").collect()
    return table


def main() -> int:
    engine = sys.argv[1]
    out_path = Path(sys.argv[2])
    base_dir = Path(tempfile.gettempdir())
    wh = tempfile.mkdtemp(prefix=f"wh_p2_{engine}_", dir=str(base_dir))
    pq_root = Path(tempfile.mkdtemp(prefix=f"pq_p2_{engine}_", dir=str(base_dir)))
    sess, spark_fn = build_session(engine, wh)
    results: dict[str, Any] = {}
    try:
        sess.conf.set("spark.sql.ansi.enabled", "true")
        sess.sql("CREATE NAMESPACE IF NOT EXISTS ice.ns").collect()
        sess.sql("CREATE TABLE ice.ns.real_empty (s STRING, d DOUBLE) USING iceberg").collect()

        shapes = [
            (
                "str_double",
                "DOUBLE",
                "1.0",
                "SELECT CAST(s AS DOUBLE) AS x FROM (SELECT 'x' AS s) src WHERE 1 = 0",
                "SELECT 1 AS p, CAST(s AS DOUBLE) AS x FROM (SELECT 'x' AS s) src WHERE 1 = 0",
                "CAST('x' AS DOUBLE)",
            ),
            (
                "str_ts",
                "TIMESTAMP",
                "TIMESTAMP '2024-01-01 00:00:00'",
                "SELECT CAST(s AS TIMESTAMP) AS x FROM (SELECT 'x' AS s) src WHERE 1 = 0",
                "SELECT 1 AS p, CAST(s AS TIMESTAMP) AS x FROM (SELECT 'x' AS s) src WHERE 1 = 0",
                "CAST('x' AS TIMESTAMP)",
            ),
            (
                "str_ntz",
                "TIMESTAMP_NTZ",
                "TIMESTAMP_NTZ '2024-01-01 00:00:00'",
                "SELECT CAST(s AS TIMESTAMP_NTZ) AS x FROM (SELECT 'x' AS s) src WHERE 1 = 0",
                "SELECT 1 AS p, CAST(s AS TIMESTAMP_NTZ) AS x FROM (SELECT 'x' AS s) src WHERE 1 "
                "= 0",
                "CAST('x' AS TIMESTAMP_NTZ)",
            ),
            (
                "dbl_dec",
                "DECIMAL(5, 0)",
                "CAST(1 AS DECIMAL(5, 0))",
                "SELECT CAST(d AS DECIMAL(5, 0)) AS x FROM (SELECT CAST(123456.0 AS DOUBLE) AS "
                "d) src WHERE 1 = 0",
                "SELECT 1 AS p, CAST(d AS DECIMAL(5, 0)) AS x FROM (SELECT CAST(123456.0 AS "
                "DOUBLE) AS d) src WHERE 1 = 0",
                "CAST(CAST(123456.0 AS DOUBLE) AS DECIMAL(5, 0))",
            ),
            (
                "str_int",
                "INT",
                "1",
                "SELECT CAST(s AS INT) AS x FROM (SELECT 'x' AS s) src WHERE 1 = 0",
                "SELECT 1 AS p, CAST(s AS INT) AS x FROM (SELECT 'x' AS s) src WHERE 1 = 0",
                "CAST('x' AS INT)",
            ),
            (
                "dead_case_double",
                "DOUBLE",
                "1.0",
                "SELECT CASE WHEN false THEN CAST(s AS DOUBLE) ELSE 1.0 END AS x FROM (SELECT "
                "'x' AS s) src WHERE 1 = 0",
                "SELECT 1 AS p, CASE WHEN false THEN CAST(s AS DOUBLE) ELSE 1.0 END AS x FROM "
                "(SELECT 'x' AS s) src WHERE 1 = 0",
                "CASE WHEN false THEN CAST('x' AS DOUBLE) ELSE 1.0 END",
            ),
            (
                "real_empty_double",
                "DOUBLE",
                "1.0",
                "SELECT CAST(s AS DOUBLE) AS x FROM ice.ns.real_empty",
                "SELECT 1 AS p, CAST(s AS DOUBLE) AS x FROM ice.ns.real_empty",
                None,
            ),
        ]

        seq_box = [0]

        for ansi in ("true", "false"):
            sess.conf.set("spark.sql.ansi.enabled", ansi)
            sess.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
            for sname, col_type, seed_lit, sel1, selp, lit_cast in shapes:
                t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                r = run_door_action(lambda t=t, sel1=sel1: sess.sql(f"INSERT OVERWRITE {t} {sel1}"))
                r["after"] = table_state(sess, t)
                results[f"{ansi}|{sname}|insert_overwrite"] = r

                t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                r = run_door_action(lambda t=t, sel1=sel1: sess.sql(f"INSERT INTO {t} {sel1}"))
                r["after"] = table_state(sess, t)
                results[f"{ansi}|{sname}|insert_into"] = r

                t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                r = run_door_action(
                    lambda t=t, sel1=sel1: sess.sql(f"INSERT OVERWRITE {t} BY NAME {sel1}")
                )
                r["after"] = table_state(sess, t)
                results[f"{ansi}|{sname}|insert_overwrite_by_name"] = r

                t = fresh_part(sess, seq_box, col_type, seed_lit)
                r = run_door_action(
                    lambda t=t, sel1=sel1: sess.sql(
                        f"INSERT OVERWRITE {t} PARTITION (p = 1) {sel1}"
                    )
                )
                r["after"] = table_state(sess, t)
                results[f"{ansi}|{sname}|insert_overwrite_static_part"] = r

                sess.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
                t = fresh_part(sess, seq_box, col_type, seed_lit)
                r = run_door_action(lambda t=t, selp=selp: sess.sql(f"INSERT OVERWRITE {t} {selp}"))
                r["after"] = table_state(sess, t)
                results[f"{ansi}|{sname}|insert_overwrite_dyn_part"] = r
                sess.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

                seq_box[0] += 1
                t_ctas = f"ice.ns.ctas_{seq_box[0]}"
                r = run_door_action(
                    lambda t_ctas=t_ctas, sel1=sel1: sess.sql(
                        f"CREATE TABLE {t_ctas} USING iceberg AS {sel1}"
                    )
                )
                r["after"] = table_state(sess, t_ctas)
                results[f"{ansi}|{sname}|ctas"] = r

                t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                r = run_door_action(
                    lambda t=t, sel1=sel1: sess.sql(f"REPLACE TABLE {t} USING iceberg AS {sel1}")
                )
                r["after"] = table_state(sess, t)
                results[f"{ansi}|{sname}|rtas"] = r

                t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                r = run_door_action(
                    lambda t=t, sel1=sel1: sess.sql(
                        f"CREATE OR REPLACE TABLE {t} USING iceberg AS {sel1}"
                    )
                )
                r["after"] = table_state(sess, t)
                results[f"{ansi}|{sname}|crtas"] = r

                t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                r = run_door_action(
                    lambda t=t, sel1=sel1: sess.sql(sel1).writeTo(t).overwrite(spark_fn.lit(True))
                )
                r["after"] = table_state(sess, t)
                results[f"{ansi}|{sname}|writeTo_overwrite"] = r

                t = fresh_part(sess, seq_box, col_type, seed_lit)
                r = run_door_action(
                    lambda t=t, selp=selp: sess.sql(selp).writeTo(t).overwritePartitions()
                )
                r["after"] = table_state(sess, t)
                results[f"{ansi}|{sname}|writeTo_overwritePartitions"] = r

                t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                r = run_door_action(lambda t=t, sel1=sel1: sess.sql(sel1).writeTo(t).append())
                r["after"] = table_state(sess, t)
                results[f"{ansi}|{sname}|writeTo_append"] = r

                seq_box[0] += 1
                t_w2c = f"ice.ns.w2c_{seq_box[0]}"
                r = run_door_action(
                    lambda t_w2c=t_w2c, sel1=sel1: sess.sql(sel1).writeTo(t_w2c).create()
                )
                r["after"] = table_state(sess, t_w2c)
                results[f"{ansi}|{sname}|writeTo_create"] = r

                t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                r = run_door_action(lambda t=t, sel1=sel1: sess.sql(sel1).writeTo(t).replace())
                r["after"] = table_state(sess, t)
                results[f"{ansi}|{sname}|writeTo_replace"] = r

                t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                r = run_door_action(
                    lambda t=t, sel1=sel1: sess.sql(sel1).write.mode("overwrite").saveAsTable(t)
                )
                r["after"] = table_state(sess, t)
                results[f"{ansi}|{sname}|saveAsTable_overwrite"] = r

                t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                r = run_door_action(
                    lambda t=t, sel1=sel1: sess.sql(sel1).write.insertInto(t, overwrite=True)
                )
                r["after"] = table_state(sess, t)
                results[f"{ansi}|{sname}|insertInto_overwrite"] = r

                t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                r = run_door_action(
                    lambda t=t, sel1=sel1: sess.sql(sel1).write.insertInto(t, overwrite=False)
                )
                r["after"] = table_state(sess, t)
                results[f"{ansi}|{sname}|insertInto_append"] = r

                pq_dir = pq_root / f"pq_{ansi}_{sname}"
                sess.sql(f"SELECT {seed_lit} AS x").write.mode("overwrite").parquet(str(pq_dir))
                r = run_door_action(
                    lambda pq_dir=pq_dir, sel1=sel1: (
                        sess.sql(sel1).write.mode("overwrite").parquet(str(pq_dir))
                    )
                )
                try:
                    pq_rows = [
                        [str(v) for v in row] for row in sess.read.parquet(str(pq_dir)).collect()
                    ]
                    r["after"] = {"exists": True, "count": len(pq_rows), "rows": pq_rows[:5]}
                except Exception as exc:
                    r["after"] = err_summary(exc)
                results[f"{ansi}|{sname}|parquet_overwrite"] = r

                t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                merge_src_sql = (
                    f"MERGE INTO {t} tgt USING ({sel1}) src ON tgt.x = src.x "
                    f"WHEN MATCHED THEN UPDATE SET tgt.x = src.x "
                    f"WHEN NOT MATCHED THEN INSERT (x) VALUES (src.x)"
                )
                r = run_door_action(lambda sql=merge_src_sql: sess.sql(sql))
                r["after"] = table_state(sess, t)
                results[f"{ansi}|{sname}|merge_in_source"] = r

                if lit_cast is not None:
                    t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                    r = run_door_action(
                        lambda t=t, lc=lit_cast, sl=seed_lit: sess.sql(
                            f"MERGE INTO {t} tgt USING (SELECT {sl} AS x WHERE 1 = 0) src "
                            f"ON tgt.x = src.x "
                            f"WHEN MATCHED THEN UPDATE SET tgt.x = {lc} "
                            f"WHEN NOT MATCHED THEN INSERT (x) VALUES ({lc})"
                        )
                    )
                    r["after"] = table_state(sess, t)
                    results[f"{ansi}|{sname}|merge_in_action_empty_src"] = r

                    t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                    r = run_door_action(
                        lambda t=t, lc=lit_cast, sl=seed_lit: sess.sql(
                            f"MERGE INTO {t} tgt USING (SELECT {sl} AS x) src "
                            f"ON false AND tgt.x = {lc} "
                            f"WHEN MATCHED THEN DELETE"
                        )
                    )
                    r["after"] = table_state(sess, t)
                    results[f"{ansi}|{sname}|merge_on_false_and"] = r

                    t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                    r = run_door_action(
                        lambda t=t, lc=lit_cast, sl=seed_lit: sess.sql(
                            f"MERGE INTO {t} tgt USING (SELECT {sl} AS x) src "
                            f"ON tgt.x = {lc} "
                            f"WHEN MATCHED THEN DELETE"
                        )
                    )
                    r["after"] = table_state(sess, t)
                    results[f"{ansi}|{sname}|merge_on_cond"] = r

                    t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                    r = run_door_action(
                        lambda t=t, lc=lit_cast: sess.sql(f"UPDATE {t} SET x = {lc} WHERE 1 = 0")
                    )
                    r["after"] = table_state(sess, t)
                    results[f"{ansi}|{sname}|update_set_where_false"] = r

                    t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                    r = run_door_action(
                        lambda t=t, lc=lit_cast, sl=seed_lit: sess.sql(
                            f"UPDATE {t} SET x = {sl} WHERE 1 = 0 AND x = {lc}"
                        )
                    )
                    r["after"] = table_state(sess, t)
                    results[f"{ansi}|{sname}|update_where_1eq0_and"] = r

                    t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                    r = run_door_action(
                        lambda t=t, lc=lit_cast, sl=seed_lit: sess.sql(
                            f"UPDATE {t} SET x = {sl} WHERE x = {lc} AND 1 = 0"
                        )
                    )
                    r["after"] = table_state(sess, t)
                    results[f"{ansi}|{sname}|update_where_cast_and_1eq0"] = r

                    t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                    r = run_door_action(
                        lambda t=t, lc=lit_cast, sl=seed_lit: sess.sql(
                            f"UPDATE {t} SET x = {sl} WHERE x = {lc}"
                        )
                    )
                    r["after"] = table_state(sess, t)
                    results[f"{ansi}|{sname}|update_where_cast"] = r

                    t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                    r = run_door_action(
                        lambda t=t, lc=lit_cast: sess.sql(
                            f"DELETE FROM {t} WHERE 1 = 0 AND x = {lc}"
                        )
                    )
                    r["after"] = table_state(sess, t)
                    results[f"{ansi}|{sname}|delete_where_1eq0_and"] = r

                    t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                    r = run_door_action(
                        lambda t=t, lc=lit_cast: sess.sql(
                            f"DELETE FROM {t} WHERE x = {lc} AND 1 = 0"
                        )
                    )
                    r["after"] = table_state(sess, t)
                    results[f"{ansi}|{sname}|delete_where_cast_and_1eq0"] = r

                    t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                    r = run_door_action(
                        lambda t=t, lc=lit_cast: sess.sql(f"DELETE FROM {t} WHERE x = {lc}")
                    )
                    r["after"] = table_state(sess, t)
                    results[f"{ansi}|{sname}|delete_where_cast"] = r

            nonempty_cases = [
                (
                    "ne_str_double_2row",
                    "DOUBLE",
                    "9.0",
                    "SELECT CAST(s AS DOUBLE) AS x FROM VALUES ('1.0'), ('x') AS t(s)",
                ),
                (
                    "ne_str_ts_2row",
                    "TIMESTAMP",
                    "TIMESTAMP '2024-01-01 00:00:00'",
                    "SELECT CAST(s AS TIMESTAMP) AS x FROM VALUES ('2024-02-02 00:00:00'), ('x') "
                    "AS t(s)",
                ),
                (
                    "ne_str_ntz_2row",
                    "TIMESTAMP_NTZ",
                    "TIMESTAMP_NTZ '2024-01-01 00:00:00'",
                    "SELECT CAST(s AS TIMESTAMP_NTZ) AS x FROM VALUES ('2024-02-02 00:00:00'), "
                    "('x') AS t(s)",
                ),
                (
                    "ne_dbl_dec_2row",
                    "DECIMAL(5, 0)",
                    "CAST(9 AS DECIMAL(5, 0))",
                    "SELECT CAST(d AS DECIMAL(5, 0)) AS x FROM VALUES (CAST(1.0 AS DOUBLE)), "
                    "(CAST(123456.0 AS DOUBLE)) AS t(d)",
                ),
                (
                    "ne_str_int_2row",
                    "INT",
                    "9",
                    "SELECT CAST(s AS INT) AS x FROM VALUES ('1'), ('x') AS t(s)",
                ),
                (
                    "ne_str_double_multibatch",
                    "DOUBLE",
                    "9.0",
                    "SELECT CAST(CASE WHEN id = 19999 THEN 'x' ELSE '1.0' END AS DOUBLE) AS x "
                    "FROM range(20000)",
                ),
            ]
            for nname, col_type, seed_lit, nsel in nonempty_cases:
                t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                r = run_door_action(lambda t=t, nsel=nsel: sess.sql(f"INSERT OVERWRITE {t} {nsel}"))
                r["after"] = table_state(sess, t)
                results[f"{ansi}|{nname}|insert_overwrite"] = r

                t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                r = run_door_action(lambda t=t, nsel=nsel: sess.sql(f"INSERT INTO {t} {nsel}"))
                r["after"] = table_state(sess, t)
                results[f"{ansi}|{nname}|insert_into"] = r

                t = fresh_unpart(sess, seq_box, col_type, seed_lit)
                r = run_door_action(
                    lambda t=t, nsel=nsel: sess.sql(f"REPLACE TABLE {t} USING iceberg AS {nsel}")
                )
                r["after"] = table_state(sess, t)
                results[f"{ansi}|{nname}|rtas"] = r

                pq_dir = pq_root / f"pq_ne_{ansi}_{nname}"
                sess.sql(f"SELECT {seed_lit} AS x").write.mode("overwrite").parquet(str(pq_dir))
                r = run_door_action(
                    lambda pq_dir=pq_dir, nsel=nsel: (
                        sess.sql(nsel).write.mode("overwrite").parquet(str(pq_dir))
                    )
                )
                try:
                    pq_rows = [
                        [str(v) for v in row] for row in sess.read.parquet(str(pq_dir)).collect()
                    ]
                    r["after"] = {"exists": True, "count": len(pq_rows), "rows": pq_rows[:5]}
                except Exception as exc:
                    r["after"] = err_summary(exc)
                results[f"{ansi}|{nname}|parquet_overwrite"] = r

        out_path.write_text(json.dumps(results, indent=1), encoding="utf-8")
        print(f"done engine={engine} cells={len(results)}")
    finally:
        sess.stop()
        shutil.rmtree(wh, ignore_errors=True)
        shutil.rmtree(pq_root, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

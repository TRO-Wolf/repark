from __future__ import annotations

import json
import re
import sys
import tempfile
from pathlib import Path
from typing import Any

TYPES = {
    "DOUBLE": "DOUBLE",
    "FLOAT": "FLOAT",
    "INT": "INT",
    "BIGINT": "BIGINT",
    "DECIMAL": "DECIMAL(10,2)",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
    "NTZ": "TIMESTAMP_NTZ",
}

SEED = {
    "DOUBLE": "CAST(1.5 AS DOUBLE)",
    "FLOAT": "CAST(1.5 AS FLOAT)",
    "INT": "CAST(7 AS INT)",
    "BIGINT": "CAST(7 AS BIGINT)",
    "DECIMAL": "CAST(1.25 AS DECIMAL(10,2))",
    "BOOLEAN": "true",
    "DATE": "DATE'2024-01-02'",
    "TIMESTAMP": "TIMESTAMP'2024-01-02 03:04:05'",
    "NTZ": "TIMESTAMP_NTZ'2024-01-02 03:04:05'",
}

SHAPES = {
    "s1_fromless_literal": "SELECT CAST('x' AS {T}) WHERE 1 = 0",
    "s2_subquery": "SELECT CAST(s AS {T}) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
    "s3_nested_subquery": (
        "SELECT CAST(s AS {T}) FROM (SELECT s FROM (SELECT 'x' AS s) a) b WHERE 1 = 0"
    ),
    "s4_inner_cast_projected": (
        "SELECT d FROM (SELECT CAST(s AS {T}) AS d FROM (SELECT 'x' AS s) a) b WHERE 1 = 0"
    ),
    "s5_inner_filter": "SELECT CAST(s AS {T}) FROM (SELECT 'x' AS s WHERE 1 = 0) src",
    "s6_cte": "WITH c AS (SELECT 'x' AS s) SELECT CAST(s AS {T}) FROM c WHERE 1 = 0",
    "s7_values_leaf_literal_cast": "SELECT CAST('x' AS {T}) FROM VALUES (1) AS v(a) WHERE 1 = 0",
    "s8_renamed_alias": (
        "SELECT CAST(r AS {T}) AS y FROM (SELECT s AS r FROM (SELECT 'x' AS s) a) b WHERE 1 = 0"
    ),
    "s9_where_false": "SELECT CAST(s AS {T}) FROM (SELECT 'abc' AS s) src WHERE false",
    "s10_two_filters": "SELECT CAST(s AS {T}) FROM (SELECT 'x' AS s) src WHERE 1 = 0 AND 2 = 2",
    "o1_values_column": "SELECT CAST(s AS {T}) FROM VALUES ('x') AS v(s) WHERE 1 = 0",
    "o2_limit0": "SELECT CAST(s AS {T}) FROM (SELECT 'x' AS s) src LIMIT 0",
    "o3_union": (
        "SELECT CAST(s AS {T}) FROM (SELECT 'x' AS s UNION ALL SELECT 'y' AS s) src WHERE 1 = 0"
    ),
    "o4_join": (
        "SELECT CAST(a.s AS {T}) FROM (SELECT 'x' AS s) a JOIN (SELECT 1 AS k) b ON true WHERE 1 "
        "= 0"
    ),
    "o5_aggregate": "SELECT CAST(s AS {T}) FROM (SELECT 'x' AS s) src GROUP BY s HAVING 1 = 0",
    "o6_case_dead": (
        "SELECT CASE WHEN false THEN CAST(s AS {T}) END FROM (SELECT 'x' AS s) src WHERE 1 = 0"
    ),
    "o7_try_cast": "SELECT TRY_CAST(s AS {T}) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
    "o8_scalar_subquery": "SELECT (SELECT CAST(s AS {T}) FROM (SELECT 'x' AS s) q) WHERE 1 = 0",
    "o9_exists": (
        "SELECT CAST(s AS {T}) FROM (SELECT 'x' AS s) src WHERE 1 = 0 AND EXISTS (SELECT 1)"
    ),
    "o10_real_table": "SELECT CAST(s AS {T}) FROM {SRC} WHERE 1 = 0",
    "o11_temp_view": "SELECT CAST(s AS {T}) FROM tv_x WHERE 1 = 0",
    "o12_distinct": "SELECT DISTINCT CAST(s AS {T}) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
    "o13_window": (
        "SELECT CAST(s AS {T}) FROM (SELECT 'x' AS s, ROW_NUMBER() OVER (ORDER BY 1) AS r) src "
        "WHERE 1 = 0"
    ),
    "o14_valid_value": "SELECT CAST(s AS {T}) FROM (SELECT '1' AS s) src WHERE 1 = 0",
    "o15_null_literal": "SELECT CAST(NULL AS {T}) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
}

FURTHER = {
    "f1_ntz_subquery": (
        "NTZ",
        "SELECT CAST(s AS TIMESTAMP_NTZ) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
    ),
    "f2_ntz_literal": ("NTZ", "SELECT CAST('x' AS TIMESTAMP_NTZ) WHERE 1 = 0"),
    "f3_dec_from_double_subquery": (
        "DECIMAL",
        "SELECT CAST(d AS DECIMAL(10,2)) FROM (SELECT 1e20 AS d) src WHERE 1 = 0",
    ),
    "f4_dec_from_double_literal": ("DECIMAL", "SELECT CAST(1e20 AS DECIMAL(10,2)) WHERE 1 = 0"),
}

SHAPE_TYPES = ["DOUBLE", "FLOAT", "TIMESTAMP", "NTZ", "INT", "BIGINT", "DECIMAL", "BOOLEAN", "DATE"]
DOOR_TYPES = ["DOUBLE", "TIMESTAMP", "NTZ", "INT"]


def build_session(engine: str, warehouse: str) -> tuple[Any, Any]:
    if engine == "spark":
        import os

        from pyspark.sql import SparkSession, functions

        spark = (
            SparkSession.builder.master("local[2]")
            .appName("iocnarrow_step0")
            .config("spark.jars", os.environ["SPARK_ICEBERG_JAR"])
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.ice.type", "hadoop")
            .config("spark.sql.catalog.ice.warehouse", warehouse)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("OFF")
        return spark, functions
    import repark._native

    print(f"REPARK_NATIVE={repark._native.__file__}", file=sys.stderr)
    from repark.spark.sql import SparkSession, functions

    spark = (
        SparkSession.builder.appName("iocnarrow_step0")
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    return spark, functions


def err_class(message: str) -> str:
    found = re.search(r"\[([A-Z][A-Z0-9_.]*)\]", message)
    return found.group(1) if found else "GENERIC:" + message.splitlines()[0][:90]


def rows_of(spark: Any, table: str) -> list[list[str]] | None:
    try:
        return sorted(
            [[str(v) for v in row] for row in spark.sql(f"SELECT * FROM {table}").collect()]
        )
    except Exception:
        return None


def prepare(spark: Any, table: str, type_key: str, partitioned: bool) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {table}").collect()
    column = TYPES[type_key]
    if partitioned:
        spark.sql(
            f"CREATE TABLE {table} (x {column}, p INT) USING iceberg PARTITIONED BY (p)"
        ).collect()
        spark.sql(f"INSERT INTO {table} SELECT {SEED[type_key]}, 1").collect()
        spark.sql(f"INSERT INTO {table} SELECT {SEED[type_key]}, 2").collect()
    else:
        spark.sql(f"CREATE TABLE {table} (x {column}) USING iceberg").collect()
        spark.sql(f"INSERT INTO {table} SELECT {SEED[type_key]}").collect()


def door_action(spark: Any, functions: Any, door: str, table: str, source: str) -> None:
    if door == "sql_ow":
        spark.sql(f"INSERT OVERWRITE {table} {source}").collect()
    elif door == "sql_ow_table":
        spark.sql(f"INSERT OVERWRITE TABLE {table} {source}").collect()
    elif door == "sql_ow_static_part":
        spark.sql(f"INSERT OVERWRITE {table} PARTITION (p = 1) {source}").collect()
    elif door == "sql_ow_dynamic_part":
        spark.sql(f"INSERT OVERWRITE {table} PARTITION (p) SELECT *, 1 FROM ({source}) q").collect()
    elif door == "sql_ow_dynamic_mode":
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        try:
            spark.sql(f"INSERT OVERWRITE {table} SELECT *, 1 FROM ({source}) q").collect()
        finally:
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
    elif door == "v2_overwrite_cond":
        spark.sql(source).toDF("x").writeTo(table).overwrite(functions.lit(True))
    elif door == "v2_overwrite_partitions":
        spark.sql(f"SELECT *, 1 AS p FROM ({source}) q").toDF("x", "p").writeTo(
            table
        ).overwritePartitions()
    elif door == "v1_insert_into_overwrite":
        spark.sql(source).toDF("x").write.mode("overwrite").insertInto(table)
    elif door == "v1_save_as_table_overwrite":
        spark.sql(source).toDF("x").write.mode("overwrite").saveAsTable(table)
    elif door == "ctl_sql_insert_into":
        spark.sql(f"INSERT INTO {table} {source}").collect()
    elif door == "ctl_sql_ctas":
        spark.sql(f"DROP TABLE IF EXISTS {table}").collect()
        spark.sql(f"CREATE TABLE {table} USING iceberg AS {source}").collect()
    elif door == "ctl_sql_rtas":
        spark.sql(f"CREATE OR REPLACE TABLE {table} USING iceberg AS {source}").collect()
    elif door == "ctl_v2_append":
        spark.sql(source).toDF("x").writeTo(table).append()
    elif door == "ctl_v2_create_or_replace":
        spark.sql(source).toDF("x").writeTo(table).using("iceberg").createOrReplace()
    elif door == "ctl_v1_save_as_table_append":
        spark.sql(source).toDF("x").write.mode("append").saveAsTable(table)
    elif door == "ctl_read":
        spark.sql(source).collect()
    else:
        raise ValueError(door)


PARTITIONED_DOORS = {
    "sql_ow_static_part",
    "sql_ow_dynamic_part",
    "sql_ow_dynamic_mode",
    "v2_overwrite_partitions",
}

DOORS = [
    "sql_ow",
    "sql_ow_table",
    "sql_ow_static_part",
    "sql_ow_dynamic_part",
    "sql_ow_dynamic_mode",
    "v2_overwrite_cond",
    "v2_overwrite_partitions",
    "v1_insert_into_overwrite",
    "v1_save_as_table_overwrite",
    "ctl_sql_insert_into",
    "ctl_sql_ctas",
    "ctl_sql_rtas",
    "ctl_v2_append",
    "ctl_v2_create_or_replace",
    "ctl_v1_save_as_table_append",
    "ctl_read",
]


def cells() -> list[tuple[str, str, str, str, str]]:
    out: list[tuple[str, str, str, str, str]] = []
    for ansi in ("true", "false"):
        for type_key in ("DOUBLE", "TIMESTAMP"):
            out.append((f"card|{ansi}|{type_key}", ansi, "sql_ow", type_key, SHAPES["s2_subquery"]))
        for name, (type_key, source) in FURTHER.items():
            out.append((f"further|{ansi}|{name}", ansi, "sql_ow", type_key, source))
        for door in DOORS:
            for type_key in DOOR_TYPES:
                out.append(
                    (f"door|{ansi}|{door}|{type_key}", ansi, door, type_key, SHAPES["s2_subquery"])
                )
        for shape, source in SHAPES.items():
            for type_key in SHAPE_TYPES:
                for door in ("sql_ow", "ctl_read"):
                    out.append(
                        (f"shape|{ansi}|{shape}|{type_key}|{door}", ansi, door, type_key, source)
                    )
    return out


def table_name(index: int) -> str:
    return f"ice.s0.t{index}"


def run(engine: str, warehouse: str, out_path: Path) -> None:
    spark, functions = build_session(engine, warehouse)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.s0").collect()
    spark.sql("DROP TABLE IF EXISTS ice.s0.src_x").collect()
    spark.sql("CREATE TABLE ice.s0.src_x (s STRING) USING iceberg").collect()
    spark.sql("INSERT INTO ice.s0.src_x VALUES ('x')").collect()
    spark.sql("CREATE OR REPLACE TEMP VIEW tv_x AS SELECT 'x' AS s")
    results: dict[str, Any] = {}
    for index, (key, ansi, door, type_key, template) in enumerate(cells()):
        table = table_name(index)
        source = template.format(T=TYPES[type_key], SRC="ice.s0.src_x")
        spark.conf.set("spark.sql.ansi.enabled", "true")
        if door != "ctl_read":
            prepare(spark, table, type_key, door in PARTITIONED_DOORS)
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        try:
            door_action(spark, functions, door, table, source)
            outcome: dict[str, Any] = {"ok": True}
        except Exception as exc:
            outcome = {"ok": False, "cls": err_class(str(exc))}
        spark.conf.set("spark.sql.ansi.enabled", "true")
        outcome["rows"] = None if door == "ctl_read" else rows_of(spark, table)
        outcome["table"] = None if door == "ctl_read" else table
        outcome["sql"] = source
        results[key] = outcome
    out_path.write_text(json.dumps(results, indent=1, sort_keys=True), encoding="utf-8")
    print(f"{engine}: {len(results)} cells -> {out_path}")


def readback(warehouse: str, in_path: Path, out_path: Path) -> None:
    spark, _ = build_session("spark", warehouse)
    results = json.loads(in_path.read_text(encoding="utf-8"))
    read: dict[str, Any] = {}
    for key, outcome in results.items():
        read[key] = None if outcome["table"] is None else rows_of(spark, outcome["table"])
    out_path.write_text(json.dumps(read, indent=1, sort_keys=True), encoding="utf-8")
    mismatches = [key for key, outcome in results.items() if outcome["rows"] != read[key]]
    print(f"readback: {len(read)} tables, engine-vs-spark row mismatches={len(mismatches)}")
    for key in mismatches[:40]:
        print("  MISMATCH", key, results[key]["rows"], read[key])


def main() -> int:
    mode = sys.argv[1]
    if mode == "readback":
        readback(sys.argv[2], Path(sys.argv[3]), Path(sys.argv[4]))
        return 0
    out_path = Path(sys.argv[2])
    warehouse = (
        sys.argv[3]
        if len(sys.argv) > 3
        else tempfile.mkdtemp(prefix=f"s0_{mode}_", dir=str(out_path.parent))
    )
    run(mode, warehouse, out_path)
    print(f"WAREHOUSE={warehouse}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

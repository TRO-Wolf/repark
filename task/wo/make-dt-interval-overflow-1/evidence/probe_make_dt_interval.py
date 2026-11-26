from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import io
import json
import math
import os
import re
import shutil
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

ROWS: list[tuple[int, int | None, int | None, int | None, float | None]] = [
    (1, 1, 2, 3, 4.5),
    (2, 2147483647, 0, 0, 0.0),
    (3, 0, 2147483647, 0, 0.0),
    (4, 0, 0, 2147483647, 0.0),
    (5, 106751991, 4, 0, 54.775807),
    (6, 106751991, 4, 0, 54.775808),
    (7, 0, 0, 0, float("nan")),
    (8, 0, 0, 0, float("inf")),
    (9, -1, -2, -3, -4.5),
    (10, 0, 0, 0, 0.1234567),
    (11, -2147483648, 0, 0, 0.0),
    (12, 0, -2147483648, 0, 0.0),
]
NULL_ROW: tuple[int, None, None, None, None] = (13, None, None, None, None)

LITERALS: list[str] = [
    "make_dt_interval()",
    "make_dt_interval(0)",
    "make_dt_interval(1)",
    "make_dt_interval(-1)",
    "make_dt_interval(106751991)",
    "make_dt_interval(106751992)",
    "make_dt_interval(-106751991)",
    "make_dt_interval(-106751992)",
    "make_dt_interval(2147483647)",
    "make_dt_interval(-2147483648)",
    "make_dt_interval(NULL)",
    "make_dt_interval(CAST(NULL AS INT))",
    "make_dt_interval(0, 1)",
    "make_dt_interval(0, -1)",
    "make_dt_interval(0, 25)",
    "make_dt_interval(1, -24)",
    "make_dt_interval(0, 2147483647)",
    "make_dt_interval(0, -2147483648)",
    "make_dt_interval(106751991, 4)",
    "make_dt_interval(106751991, 5)",
    "make_dt_interval(2147483647, 2147483647)",
    "make_dt_interval(0, NULL)",
    "make_dt_interval(0, 0, 1)",
    "make_dt_interval(0, 0, -1)",
    "make_dt_interval(0, 0, 2147483647)",
    "make_dt_interval(0, 0, -2147483648)",
    "make_dt_interval(1, 2, 3)",
    "make_dt_interval(106751991, 4, 0)",
    "make_dt_interval(106751991, 4, 1)",
    "make_dt_interval(0, 0, NULL)",
    "make_dt_interval(1, 2, 3, 4.5)",
    "make_dt_interval(0, 0, 0, 0.0)",
    "make_dt_interval(0, 0, 0, 1.5)",
    "make_dt_interval(0, 0, 0, -1.5)",
    "make_dt_interval(0, 0, 0, 0.1)",
    "make_dt_interval(0, 0, 0, 0.12)",
    "make_dt_interval(0, 0, 0, 0.123)",
    "make_dt_interval(0, 0, 0, 0.1234)",
    "make_dt_interval(0, 0, 0, 0.12345)",
    "make_dt_interval(0, 0, 0, 0.123456)",
    "make_dt_interval(0, 0, 0, 0.1234567)",
    "make_dt_interval(0, 0, 0, 0.1234565)",
    "make_dt_interval(0, 0, 0, 0.0000005)",
    "make_dt_interval(0, 0, 0, -0.0000005)",
    "make_dt_interval(0, 0, 0, 0.00000049)",
    "make_dt_interval(0, 0, 0, 59.999999)",
    "make_dt_interval(0, 0, 0, 59.9999995)",
    "make_dt_interval(0, 0, 0, 60)",
    "make_dt_interval(0, 0, 0, 3600.5)",
    "make_dt_interval(0, 0, 0, 86400)",
    "make_dt_interval(0, 0, 0, 999999999999.999999)",
    "make_dt_interval(0, 0, 0, -999999999999.999999)",
    "make_dt_interval(0, 0, 0, 1000000000000)",
    "make_dt_interval(0, 0, 0, 1000000000000.5)",
    "make_dt_interval(106751991, 4, 0, 54.775807)",
    "make_dt_interval(106751991, 4, 0, 54.775808)",
    "make_dt_interval(-106751991, -4, 0, -54.775808)",
    "make_dt_interval(-106751991, -4, 0, -54.775809)",
    "make_dt_interval(2147483647, 2147483647, 2147483647, 0.0)",
    "make_dt_interval(1, 2147483647, 0, 0)",
    "make_dt_interval(-1, 2147483647, 0, 0)",
    "make_dt_interval(0, 0, 0, NULL)",
    "make_dt_interval(NULL, 0, 0, 0.0)",
    "make_dt_interval(0, 0, 0, CAST('NaN' AS DOUBLE))",
    "make_dt_interval(0, 0, 0, CAST('Infinity' AS DOUBLE))",
    "make_dt_interval(0, 0, 0, CAST('-Infinity' AS DOUBLE))",
    "make_dt_interval(0, 0, 0, 1.5D)",
    "make_dt_interval(0, 0, 0, 1.5E0)",
    "make_dt_interval(0, 0, 0, 0.1234567D)",
    "make_dt_interval(0, 0, 0, 0.1234565D)",
    "make_dt_interval(0, 0, 0, 1E12D)",
    "make_dt_interval(0, 0, 0, 5)",
    "make_dt_interval(0, 0, 0, CAST(1.5 AS FLOAT))",
    "make_dt_interval(0, 0, 0, CAST(0.1 AS FLOAT))",
    "make_dt_interval(0, 0, 0, CAST(4.5 AS DECIMAL(38, 10)))",
    "make_dt_interval(0, 0, 0, '1.5')",
    "make_dt_interval(CAST(1 AS BIGINT))",
    "make_dt_interval(CAST(4294967297 AS BIGINT))",
    "make_dt_interval(CAST(1 AS SMALLINT), CAST(2 AS TINYINT))",
    "make_dt_interval(1.5)",
    "make_dt_interval('1')",
    "make_dt_interval(1, 2, 3, 4, 5)",
]

CONSUMERS: list[str] = [
    "TIMESTAMP'2024-01-01 00:00:00' + make_dt_interval(1, 2, 3, 4.5)",
    "TIMESTAMP'2024-01-01 00:00:00' - make_dt_interval(1, 2, 3, 4.5)",
    "TIMESTAMP_NTZ'2024-01-01 00:00:00' + make_dt_interval(1, 2, 3, 4.5)",
    "DATE'2024-01-01' + make_dt_interval(1)",
    "make_dt_interval(1, 2, 3, 4.5) + make_dt_interval(0, 1)",
    "make_dt_interval(1) - make_dt_interval(2)",
    "-make_dt_interval(1, 2)",
    "make_dt_interval(1) * 2",
    "make_dt_interval(1) / 2",
    "make_dt_interval(106751991) + make_dt_interval(1)",
    "make_dt_interval(1) > make_dt_interval(0, 23)",
    "make_dt_interval(1) = make_dt_interval(0, 24)",
    "CAST(make_dt_interval(1, 2, 3, 4.5) AS STRING)",
    "CAST(make_dt_interval(-1, -2, -3, -4.5) AS STRING)",
    "CAST(make_dt_interval(0, 2147483647) AS STRING)",
    "extract(DAY FROM make_dt_interval(1, 2, 3, 4.5))",
    "extract(HOUR FROM make_dt_interval(1, 2, 3, 4.5))",
    "extract(MINUTE FROM make_dt_interval(1, 2, 3, 4.5))",
    "extract(SECOND FROM make_dt_interval(1, 2, 3, 4.5))",
    "typeof(make_dt_interval(1))",
    "make_dt_interval(1) IS NULL",
    "coalesce(make_dt_interval(NULL), make_dt_interval(2))",
    "CASE WHEN true THEN make_dt_interval(1) END",
    "make_dt_interval(1, 2, 3, 4.5) IN (make_dt_interval(1, 2, 3, 4.5))",
]

NEIGHBOURS: list[str] = [
    "make_interval(1, 2, 3, 4, 5, 6, 7.5)",
    "make_interval(0, 0, 0, 2147483647)",
    "make_interval()",
    "make_ym_interval(1, 2)",
    "make_ym_interval(2147483647, 0)",
    "make_ym_interval()",
    "INTERVAL '1' DAY",
    "INTERVAL '1 02:03:04.5' DAY TO SECOND",
    "INTERVAL '2' HOUR",
    "INTERVAL 1 DAY",
    "TIMESTAMP'2024-01-01 00:00:00' + INTERVAL '1' DAY",
    "TIMESTAMP'2024-01-02 00:00:00' - TIMESTAMP'2024-01-01 00:00:00'",
    "try_make_interval(0, 0, 0, 2147483647)",
    "try_make_interval(1, 2, 3, 4, 5, 6, 7.5)",
    "try_make_dt_interval(1)",
    "try_make_ym_interval(1, 2)",
    "try_add(make_dt_interval(106751991), make_dt_interval(1))",
]

SECONDS_TYPES: list[str] = [
    *(
        f"make_dt_interval(0, 0, 0, CAST(1 AS DECIMAL({p}, {s})))"
        for p, s in (
            (11, 0),
            (12, 0),
            (13, 0),
            (13, 1),
            (18, 6),
            (18, 7),
            (19, 7),
            (38, 30),
        )
    ),
    "make_dt_interval(0, 0, 0, CAST(1 AS BIGINT))",
    "make_dt_interval(0, 0, 0, CAST(1 AS INT))",
    "make_dt_interval(0, 0, 0, CAST(1 AS SMALLINT))",
    "make_dt_interval(0, 0, 0, CAST(999999999999.9999995 AS DECIMAL(19, 7)))",
    "make_dt_interval(0, 0, 0, CAST(-999999999999.9999995 AS DECIMAL(19, 7)))",
    "make_dt_interval(106751991, 5, -60, 0)",
    "make_dt_interval(-106751991, -5, 60, 0)",
    "make_dt_interval(0, 0, 0, 0.1234564999D)",
    "make_dt_interval(0, 0, 0, 1.0000005D)",
    "make_dt_interval(0, 0, 0, 2.5E-7D)",
    "make_dt_interval(0, 0, 0, 5.0E-7D)",
    "make_dt_interval(0, 0, 0, -5.0E-7D)",
    "make_dt_interval(0, 0, 0, 999999999999.9999D)",
    "make_dt_interval(0, 0, 0, -1E12D)",
    "make_dt_interval(0, 0, 0, 1E-300D)",
]

COLUMN_EXPRS: list[str] = [
    "make_dt_interval(d, h, m, s)",
    "make_dt_interval(d)",
    "make_dt_interval(0, h)",
    "make_dt_interval(0, 0, m)",
    "make_dt_interval(0, 0, 0, s)",
]

COLUMN_CONSUMERS: list[str] = [
    "ts + make_dt_interval(d, h, m, s)",
    "CAST(make_dt_interval(d, h, m, s) AS STRING)",
    "make_dt_interval(d, h, m, s) > make_dt_interval(0)",
]

AGGREGATES: list[str] = [
    "sum(make_dt_interval(d, h, m, s))",
    "avg(make_dt_interval(d, h, m, s))",
    "min(make_dt_interval(d, h, m, s))",
    "max(make_dt_interval(d, h, m, s))",
    "count(make_dt_interval(d, h, m, s))",
]

FN_CASES: list[tuple[str, list[Any]]] = [
    ("fn()", []),
    ("fn(1)", [1]),
    ("fn(2147483647)", [2147483647]),
    ("fn(0,2147483647)", [0, 2147483647]),
    ("fn(1,2,3,4.5)", [1, 2, 3, 4.5]),
    ("fn(0,0,0,0.1234567)", [0, 0, 0, 0.1234567]),
    ("fn(d)", ["d"]),
    ("fn(lit0,h)", [0, "h"]),
    ("fn(d,h,m,s)", ["d", "h", "m", "s"]),
    ("fn(lit0,lit0,lit0,s)", [0, 0, 0, "s"]),
]

MEM_IDS = (1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12)
FILE_IDS = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
ALL_IDS = (*FILE_IDS, 13)


def render_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Inf" if value > 0 else "-Inf"
        return repr(value)
    if isinstance(value, dt.timedelta):
        return f"td({value.days},{value.seconds},{value.microseconds})"
    if isinstance(value, Decimal):
        return f"Dec({value})"
    if isinstance(value, (list, tuple)):
        return [render_value(item) for item in value]
    return f"{type(value).__name__}:{value}"


def error_class(text: str) -> str:
    found = re.search(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]", text)
    return found.group(1) if found else ""


def first_line(exc: BaseException) -> str:
    text = str(exc).strip()
    if not text:
        return type(exc).__name__
    return text.splitlines()[0][:300]


def capture_df(df: Any) -> dict[str, Any]:
    try:
        dtypes = [[name, dtype.replace(" ", "")] for name, dtype in df.dtypes]
        nulls = [bool(field.nullable) for field in df.schema.fields]
    except Exception as exc:
        return {"kind": "error", "cls": error_class(str(exc)), "first": first_line(exc)}
    try:
        rows = [[render_value(cell) for cell in row] for row in df.collect()]
    except Exception as exc:
        return {
            "kind": "error",
            "cls": error_class(str(exc)),
            "first": first_line(exc),
            "dtypes": dtypes,
            "nulls": nulls,
        }
    return {"kind": "ok", "dtypes": dtypes, "nulls": nulls, "rows": rows}


def capture(fn: Any) -> dict[str, Any]:
    try:
        df = fn()
    except Exception as exc:
        return {"kind": "error", "cls": error_class(str(exc)), "first": first_line(exc)}
    return capture_df(df)


def make_session(engine: str, warehouse: Path, ansi: str) -> tuple[Any, Any, Any]:
    if engine == "spark":
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as sf
        from pyspark.sql import types as st

        spark = (
            SparkSession.builder.master("local[2]")
            .appName(f"makedt-step0-{ansi}")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", ansi)
            .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.ice.type", "hadoop")
            .config("spark.sql.catalog.ice.warehouse", str(warehouse))
            .config("spark.jars", os.environ["SPARK_ICEBERG_JAR"])
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
        ReparkSession.builder.appName(f"makedt-step0-{ansi}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", str(warehouse))
        .getOrCreate()
    )
    return spark, sf, st


def source_rows(with_null: bool) -> list[tuple[Any, ...]]:
    ts = dt.datetime(2024, 1, 1, 0, 0, 0)
    rows = [(*row, ts) for row in ROWS]
    if with_null:
        rows.append((*NULL_ROW, ts))
    return rows


def write_parquet_source(path: Path) -> None:
    rows = source_rows(with_null=True)
    schema = pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("d", pa.int32()),
            pa.field("h", pa.int32()),
            pa.field("m", pa.int32()),
            pa.field("s", pa.float64()),
            pa.field("ts", pa.timestamp("us", tz="UTC")),
        ]
    )
    columns = list(zip(*rows, strict=True))
    table = pa.table(
        {name: list(col) for name, col in zip(schema.names, columns, strict=True)},
        schema=schema,
    )
    pq.write_table(table, str(path))


def ice_values(rows: list[tuple[Any, ...]]) -> str:
    parts = []
    for row in rows:
        cells = []
        for value in row[:5]:
            if value is None:
                cells.append("NULL")
            elif isinstance(value, float) and math.isnan(value):
                cells.append("CAST('NaN' AS DOUBLE)")
            elif isinstance(value, float) and math.isinf(value):
                cells.append("CAST('Infinity' AS DOUBLE)")
            elif isinstance(value, float):
                cells.append(f"{value!r}D")
            else:
                cells.append(str(value))
        cells.append("TIMESTAMP'2024-01-01 00:00:00'")
        parts.append("(" + ", ".join(cells) + ")")
    return ", ".join(parts)


def setup_sources(spark: Any, st: Any, root: Path) -> None:
    schema = st.StructType(
        [
            st.StructField("id", st.IntegerType(), False),
            st.StructField("d", st.IntegerType(), False),
            st.StructField("h", st.IntegerType(), False),
            st.StructField("m", st.IntegerType(), False),
            st.StructField("s", st.DoubleType(), False),
            st.StructField("ts", st.TimestampType(), False),
        ]
    )
    utc_rows = [
        (*row[:5], row[5].replace(tzinfo=dt.UTC)) for row in source_rows(False) if row[0] in MEM_IDS
    ]
    spark.createDataFrame(utc_rows, schema).createOrReplaceTempView("mem")
    pq_path = root / "src.parquet"
    write_parquet_source(pq_path)
    spark.read.parquet(str(pq_path)).createOrReplaceTempView("pq")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.md")
    for name, nn in (("nn", " NOT NULL"), ("nl", "")):
        spark.sql(f"DROP TABLE IF EXISTS ice.md.{name}")
        spark.sql(
            f"CREATE TABLE ice.md.{name} (id INT NOT NULL, d INT{nn}, h INT{nn}, "
            f"m INT{nn}, s DOUBLE{nn}, ts TIMESTAMP{nn}) USING iceberg"
        )
        spark.sql(f"INSERT INTO ice.md.{name} VALUES {ice_values(source_rows(name == 'nl'))}")


def literal_cells(spark: Any, sf: Any, out: dict[str, Any]) -> None:
    base = spark.sql("SELECT 1 AS id")
    for expr in LITERALS + CONSUMERS + NEIGHBOURS + SECONDS_TYPES:
        out[f"sql|lit|{expr}"] = capture(lambda e=expr: spark.sql(f"SELECT {e} AS r"))
    for expr in LITERALS:
        out[f"selectExpr|lit|{expr}"] = capture(lambda e=expr: base.selectExpr("id", f"{e} AS r"))
        out[f"expr|lit|{expr}"] = capture(lambda e=expr: base.select("id", sf.expr(e).alias("r")))


def view_cell(spark: Any, body: str) -> Any:
    spark.sql(f"CREATE OR REPLACE TEMP VIEW v_md AS {body}")
    return spark.sql("SELECT * FROM v_md")


def column_cells(spark: Any, sf: Any, out: dict[str, Any]) -> None:
    sources = (
        ("mem", MEM_IDS),
        ("pq", ALL_IDS),
        ("ice.md.nn", FILE_IDS),
        ("ice.md.nl", ALL_IDS),
    )
    for src, ids in sources:
        for expr in COLUMN_EXPRS:
            for row_id in ids:
                stmt = f"SELECT id, {expr} AS r FROM {src} WHERE id = {row_id}"
                out[f"sql|{src}#{row_id}|{expr}"] = capture(lambda q=stmt: spark.sql(q))
    for expr in COLUMN_EXPRS + COLUMN_CONSUMERS:
        for row_id in (1, 2, 3, 7):
            stmt = f"SELECT id, {expr} AS r FROM mem WHERE id = {row_id}"
            out[f"view|mem#{row_id}|{expr}"] = capture(lambda q=stmt: view_cell(spark, q))
            out[f"selectExpr|mem#{row_id}|{expr}"] = capture(
                lambda e=expr, i=row_id: (
                    spark.table("mem").where(f"id = {i}").selectExpr("id", f"{e} AS r")
                )
            )
            out[f"expr|mem#{row_id}|{expr}"] = capture(
                lambda e=expr, i=row_id: (
                    spark.table("mem").where(f"id = {i}").select("id", sf.expr(e).alias("r"))
                )
            )
    for expr in COLUMN_CONSUMERS:
        for row_id in (1, 2, 3, 7):
            stmt = f"SELECT id, {expr} AS r FROM mem WHERE id = {row_id}"
            out[f"sql|mem#{row_id}|{expr}"] = capture(lambda q=stmt: spark.sql(q))
    for expr in AGGREGATES:
        for ids in ("1, 9", "1, 3", "1, 7"):
            stmt = f"SELECT {expr} AS r FROM mem WHERE id IN ({ids})"
            out[f"sql|mem#{ids}|{expr}"] = capture(lambda q=stmt: spark.sql(q))


def capture_value(fn: Any) -> dict[str, Any]:
    try:
        return {"kind": "ok", "rows": [[render_value(fn())]], "dtypes": [], "nulls": []}
    except Exception as exc:
        return {"kind": "error", "cls": error_class(str(exc)), "first": first_line(exc)}


def describe_view(spark: Any) -> Any:
    spark.sql("CREATE OR REPLACE TEMP VIEW v_desc AS SELECT make_dt_interval(1) AS r")
    return spark.sql("DESCRIBE v_desc").select("col_name", "data_type")


def blast_cells(spark: Any, sf: Any, out: dict[str, Any]) -> None:
    lit_df = "SELECT 1 AS id, make_dt_interval(1, 2, 3, 4.5) AS r"
    ts_base = "TIMESTAMP'2023-12-31 00:00:00'"
    stmts = {
        "ts-ts col": f"SELECT id, ts - {ts_base} AS r FROM mem WHERE id = 1",
        "sum(ts-ts)": f"SELECT sum(ts - {ts_base}) AS r FROM mem WHERE id IN (1, 2)",
        "union": "SELECT make_dt_interval(1) AS r UNION ALL SELECT make_dt_interval(0, 1) AS r",
        "cast-to-dt": "SELECT CAST(make_dt_interval(1) AS INTERVAL DAY TO SECOND) AS r",
    }
    for label, stmt in stmts.items():
        out[f"blast|sql|{label}"] = capture(lambda q=stmt: spark.sql(q))
    out["blast|describe|view"] = capture(lambda: describe_view(spark))
    out["blast|createDataFrame|timedelta"] = capture(
        lambda: spark.createDataFrame([(1, dt.timedelta(days=1, microseconds=5))], ["id", "r"])
    )
    out["blast|roundtrip|collect+schema"] = capture(
        lambda: spark.createDataFrame(spark.sql(lit_df).collect(), spark.sql(lit_df).schema)
    )
    out["blast|schema|json"] = capture_value(lambda: spark.sql(lit_df).schema.json())
    out["blast|schema|simpleString"] = capture_value(
        lambda: spark.sql(lit_df).schema.simpleString()
    )
    out["blast|toPandas|r"] = capture_value(lambda: str(spark.sql(lit_df).toPandas()["r"].tolist()))
    out["blast|col.cast|string"] = capture(
        lambda: spark.sql(lit_df).select(sf.col("r").cast("string").alias("r"))
    )


def fn_args(sf: Any, args: list[Any]) -> list[Any]:
    return [sf.col(a) if isinstance(a, str) else sf.lit(a) for a in args]


def fn_cells(spark: Any, sf: Any, out: dict[str, Any]) -> None:
    for label, args in FN_CASES:
        uses_cols = any(isinstance(a, str) for a in args)
        row_ids = (1, 2, 3, 7) if uses_cols else (1,)
        for row_id in row_ids:
            out[f"fn|mem#{row_id}|{label}"] = capture(
                lambda a=args, i=row_id: (
                    spark.table("mem")
                    .where(f"id = {i}")
                    .select("id", sf.make_dt_interval(*fn_args(sf, a)).alias("r"))
                )
            )


def schema_tree(df: Any) -> str:
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.printSchema()
    return buffer.getvalue().strip()


def write_cells(spark: Any, root: Path, engine: str, ansi: str, out: dict[str, Any]) -> None:
    stmts = {
        "lit": "SELECT 1 AS id, make_dt_interval(1, 2, 3, 4.5) AS r",
        "col": "SELECT id, make_dt_interval(d, h, m, s) AS r FROM mem WHERE id IN (1, 3, 9)",
        "nan": "SELECT id, make_dt_interval(d, h, m, s) AS r FROM mem WHERE id IN (1, 7)",
    }
    for label, stmt in stmts.items():
        out[f"printSchema|{label}"] = capture_tree(spark, stmt)
        pq_dir = root / f"out_{engine}_{ansi}_{label}.parquet"
        shutil.rmtree(pq_dir, ignore_errors=True)
        pq_dir.unlink(missing_ok=True)
        try:
            spark.sql(stmt).write.parquet(str(pq_dir))
            files = [pq_dir] if pq_dir.is_file() else sorted(pq_dir.glob("*.parquet"))
            stored = [str(f.type) for f in pq.read_schema(str(files[0]))] if files else []
            out[f"write.parquet|{label}"] = {
                "kind": "ok",
                "stored": stored,
                "path": str(pq_dir),
            }
        except Exception as exc:
            out[f"write.parquet|{label}"] = {
                "kind": "error",
                "cls": error_class(str(exc)),
                "first": first_line(exc),
            }
        table = f"ice.md.ctas_{engine}_{ansi}_{label}"
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
            spark.sql(f"CREATE TABLE {table} USING iceberg AS {stmt}")
            out[f"ctas|{label}"] = {
                "kind": "ok",
                "readback": capture(lambda t=table: spark.table(t)),
            }
        except Exception as exc:
            out[f"ctas|{label}"] = {
                "kind": "error",
                "cls": error_class(str(exc)),
                "first": first_line(exc),
            }


def capture_tree(spark: Any, stmt: str) -> dict[str, Any]:
    try:
        return {"kind": "ok", "tree": schema_tree(spark.sql(stmt))}
    except Exception as exc:
        return {"kind": "error", "cls": error_class(str(exc)), "first": first_line(exc)}


def native_cells(root: Path) -> dict[str, Any]:
    from repark._native import PyReparkSession

    session = PyReparkSession.native()
    session.create_or_replace_temp_view("pq", session.read_parquet(str(root / "src.parquet")))
    queries = {
        "lit(1,2,3,4.5)": "SELECT make_dt_interval(1, 2, 3, 4.5) AS r",
        "lit(2147483647)": "SELECT make_dt_interval(2147483647) AS r",
        "lit(0,2147483647)": "SELECT make_dt_interval(0, 2147483647) AS r",
        "pq#2(d)": "SELECT id, make_dt_interval(d) AS r FROM pq WHERE id = 2",
        "pq#3(0,h)": "SELECT id, make_dt_interval(0, h) AS r FROM pq WHERE id = 3",
        "pq#7(s)": "SELECT id, make_dt_interval(0, 0, 0, s) AS r FROM pq WHERE id = 7",
    }
    res: dict[str, Any] = {}
    for key, sql in queries.items():
        try:
            tbl = pa.table(session.sql(sql))
            res[f"native|{key}"] = {
                "kind": "ok",
                "types": [str(f.type) for f in tbl.schema],
                "nulls": [bool(f.nullable) for f in tbl.schema],
                "rows": [[render_value(v) for v in r.values()] for r in tbl.to_pylist()],
            }
        except Exception as exc:
            res[f"native|{key}"] = {"kind": "error", "first": first_line(exc)}
    return res


def readback(spark: Any, paths: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for path in paths:
        if path.endswith(".parquet"):
            out[f"readback|{path}"] = capture(lambda p=path: spark.read.parquet(p))
        else:
            out[f"readback|{path}"] = capture(lambda t=path: spark.table(t))
    return out


def run(args: argparse.Namespace) -> dict[str, Any]:
    root = args.work_root
    root.mkdir(parents=True, exist_ok=True)
    spark, sf, st = make_session(args.engine, root / "wh", args.ansi)
    payload: dict[str, Any] = {"engine": args.engine, "ansi": args.ansi, "cells": {}}
    if args.engine == "repark":
        import repark._native

        payload["native_file"] = repark._native.__file__
    if args.readback:
        payload["cells"] = readback(spark, json.loads(args.readback.read_text()))
        return payload
    cells: dict[str, Any] = payload["cells"]
    setup_sources(spark, st, root)
    literal_cells(spark, sf, cells)
    column_cells(spark, sf, cells)
    fn_cells(spark, sf, cells)
    blast_cells(spark, sf, cells)
    write_cells(spark, root, args.engine, args.ansi, cells)
    if args.engine == "repark":
        cells.update(native_cells(root))
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=("repark", "spark"), required=True)
    parser.add_argument("--ansi", choices=("false", "true"), required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--readback", type=Path)
    args = parser.parse_args()
    payload = run(args)
    args.out.write_text(json.dumps(payload, indent=1, sort_keys=True))
    print(f"cells={len(payload['cells'])} out={args.out}")


if __name__ == "__main__":
    main()

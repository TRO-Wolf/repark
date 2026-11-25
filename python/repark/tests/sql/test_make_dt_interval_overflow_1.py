from __future__ import annotations

import contextlib
import datetime as dt
import io
import json
import math
import re
from collections.abc import Callable, Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from repark.spark import SparkSession
from repark.spark import functions as sf
from repark.spark import types as st
from repark.spark.session import _reset_active_session_for_tests

pytestmark = pytest.mark.xdist_group("make_dt_interval_overflow_1")

ANSI_MODES = ("false", "true")
CELLS: dict[str, dict[str, dict[str, Any]]] = json.loads(
    Path(__file__).with_name("make_dt_interval_overflow_1_spark_oracle.json").read_text()
)
ROWS: list[tuple[Any, ...]] = [
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
    (13, None, None, None, None),
]
FN_ARGS: dict[str, list[Any]] = {
    "fn()": [],
    "fn(1)": [1],
    "fn(2147483647)": [2147483647],
    "fn(0,2147483647)": [0, 2147483647],
    "fn(1,2,3,4.5)": [1, 2, 3, 4.5],
    "fn(0,0,0,0.1234567)": [0, 0, 0, 0.1234567],
    "fn(d)": ["d"],
    "fn(lit0,h)": [0, "h"],
    "fn(d,h,m,s)": ["d", "h", "m", "s"],
    "fn(lit0,lit0,lit0,s)": [0, 0, 0, "s"],
}
MEM_IDS = (1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12)
TS = dt.datetime(2024, 1, 1, 0, 0, 0)


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


def capture_frame(build: Callable[[], Any]) -> dict[str, Any]:
    try:
        frame = build()
        result_type = frame.dtypes[-1][1].replace(" ", "")
        nullable = bool(frame.schema.fields[-1].nullable)
        rows = [[render_value(cell) for cell in row] for row in frame.collect()]
    except Exception as exc:
        return {"kind": "error", "text": str(exc)}
    return {"kind": "ok", "type": result_type, "nullable": nullable, "rows": rows}


def capture_text(build: Callable[[], Any]) -> dict[str, Any]:
    try:
        return {"kind": "ok", "text": str(build())}
    except Exception as exc:
        return {"kind": "error", "text": str(exc)}


def build_session(warehouse: Path, ansi: str) -> SparkSession:
    _reset_active_session_for_tests()
    return (
        SparkSession.builder.appName(f"make-dt-interval-overflow-1-{ansi}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", str(warehouse))
        .getOrCreate()
    )


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, float) and math.isnan(value):
        return "CAST('NaN' AS DOUBLE)"
    if isinstance(value, float) and math.isinf(value):
        return "CAST('Infinity' AS DOUBLE)"
    if isinstance(value, float):
        return f"{value!r}D"
    return str(value)


def ice_values(rows: list[tuple[Any, ...]]) -> str:
    rendered = []
    for row in rows:
        cells = [sql_literal(value) for value in row]
        cells.append("TIMESTAMP'2024-01-01 00:00:00'")
        rendered.append("(" + ", ".join(cells) + ")")
    return ", ".join(rendered)


def write_parquet_source(path: Path) -> None:
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
    columns = list(zip(*ROWS, strict=True))
    data = {name: list(column) for name, column in zip(schema.names[:5], columns, strict=True)}
    data["ts"] = [TS.replace(tzinfo=dt.UTC)] * len(ROWS)
    pq.write_table(pa.table(data, schema=schema), str(path))


def seed_sources(spark: SparkSession, root: Path) -> None:
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
    mem_rows = [(*row, TS.replace(tzinfo=dt.UTC)) for row in ROWS if row[0] in MEM_IDS]
    spark.createDataFrame(mem_rows, schema).createOrReplaceTempView("mem")
    pq_path = root / "src.parquet"
    write_parquet_source(pq_path)
    spark.read.parquet(str(pq_path)).createOrReplaceTempView("pq")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.md")
    for name, not_null in (("nn", " NOT NULL"), ("nl", "")):
        spark.sql(
            f"CREATE TABLE ice.md.{name} (id INT NOT NULL, d INT{not_null}, h INT{not_null}, "
            f"m INT{not_null}, s DOUBLE{not_null}, ts TIMESTAMP{not_null}) USING iceberg"
        )
        rows = [row for row in ROWS if name == "nl" or row[1] is not None]
        spark.sql(f"INSERT INTO ice.md.{name} VALUES {ice_values(rows)}")


def source_frame(spark: SparkSession, source: str) -> Any:
    name, _, row_id = source.partition("#")
    return spark.table(name).where(f"id = {row_id}")


def view_frame(spark: SparkSession, body: str) -> Any:
    spark.sql(f"CREATE OR REPLACE TEMP VIEW v_md AS {body}")
    return spark.sql("SELECT * FROM v_md")


def fn_frame(spark: SparkSession, source: str, label: str) -> Any:
    args = [sf.col(a) if isinstance(a, str) else sf.lit(a) for a in FN_ARGS[label]]
    return source_frame(spark, source).select("id", sf.make_dt_interval(*args).alias("r"))


def sql_frame(spark: SparkSession, source: str, expr: str) -> Any:
    if source == "lit":
        return spark.sql(f"SELECT {expr} AS r")
    name, _, ids = source.partition("#")
    if "," in ids:
        return spark.sql(f"SELECT {expr} AS r FROM {name} WHERE id IN ({ids})")
    return spark.sql(f"SELECT id, {expr} AS r FROM {name} WHERE id = {ids}")


def frame_base(spark: SparkSession, source: str) -> Any:
    if source == "lit":
        return spark.sql("SELECT 1 AS id")
    return source_frame(spark, source)


def build_cell(spark: SparkSession, door: str, source: str, expr: str) -> Callable[[], Any]:
    builders: dict[str, Callable[[], Any]] = {
        "sql": lambda: sql_frame(spark, source, expr),
        "selectExpr": lambda: frame_base(spark, source).selectExpr("id", f"{expr} AS r"),
        "expr": lambda: frame_base(spark, source).select("id", sf.expr(expr).alias("r")),
        "view": lambda: view_frame(
            spark, f"SELECT id, {expr} AS r FROM {source.replace('#', ' WHERE id = ')}"
        ),
        "fn": lambda: fn_frame(spark, source, expr),
    }
    return builders[door]


def tree_line(spark: SparkSession, stmt: str) -> str:
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        spark.sql(stmt).printSchema()
    return buffer.getvalue().strip().splitlines()[-1]


def ctas(spark: SparkSession, label: str, stmt: str) -> Any:
    table = f"ice.md.ctas_{label}"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"CREATE TABLE {table} USING iceberg AS {stmt}")
    return spark.table(table)


WRITE_STATEMENTS = {
    "lit": "SELECT 1 AS id, make_dt_interval(1, 2, 3, 4.5) AS r",
    "col": "SELECT id, make_dt_interval(d, h, m, s) AS r FROM mem WHERE id IN (1, 3, 9)",
    "nan": "SELECT id, make_dt_interval(d, h, m, s) AS r FROM mem WHERE id IN (1, 7)",
}


def capture_cell(spark: SparkSession, key: str) -> dict[str, Any]:
    door, source, expr = [*key.split("|", 2), ""][:3]
    if door == "printSchema":
        return capture_text(lambda: tree_line(spark, WRITE_STATEMENTS[source]))
    if door == "ctas":
        return capture_frame(lambda: ctas(spark, source, WRITE_STATEMENTS[source]))
    if door == "blast":
        return capture_frame(lambda: spark.sql(expr))
    return capture_frame(build_cell(spark, door, source, expr))


def assert_matches(key: str, ansi: str, expected: dict[str, Any], got: dict[str, Any]) -> None:
    label = f"{key} (ansi {ansi})"
    if expected["kind"] == "error":
        assert got["kind"] == "error", f"{label}: Spark raises {expected['cls']}, got {got}"
        assert expected["cls"] in got["text"], f"{label}: {got['text'][:300]}"
        return
    assert got["kind"] == "ok", f"{label}: Spark answers, got {got['text'][:300]}"
    for field in ("type", "nullable", "rows", "text"):
        if field in expected:
            assert got[field] == expected[field], f"{label}: {field} {got[field]!r}"


@pytest.fixture(scope="module", params=ANSI_MODES)
def measured(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[tuple[str, dict[str, dict[str, Any]]]]:
    ansi = request.param
    root = tmp_path_factory.mktemp(f"make_dt_interval_{ansi}")
    spark = build_session(root / "wh", ansi)
    seed_sources(spark, root)
    results = {key: capture_cell(spark, key) for key in sorted(CELLS)}
    _reset_active_session_for_tests()
    yield ansi, results


@pytest.mark.parametrize("key", sorted(CELLS))
def test_make_dt_interval_cell(measured: tuple[str, dict[str, dict[str, Any]]], key: str) -> None:
    ansi, results = measured
    assert_matches(key, ansi, CELLS[key][ansi], results[key])

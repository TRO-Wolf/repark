"""pins: cutover-schema-1/C-001, C-002, C-003, C-004, C-005, C-006"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

import _live_parity as lp
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from _sql_harden_cutover_programs import write_bronze_parquet
from _sql_harden_cutover_run import _metadata_facts, apply_dedup, sql_arrow

_SPARK_DEDUP_SCHEMA: list[list[Any]] = [
    ["id", "string", True],
    ["amount", "decimal128(10, 4)", True],
    ["units", "int32", False],
    ["note", "string", False],
    ["part", "int32", True],
]

_SPARK_DEDUP_ROWS: list[tuple[Any, ...]] = [
    ("A", Decimal("0.0000"), 0, "unknown", 10),
    ("B", Decimal("2.5000"), 2, "keep", 20),
]


def _schema_cells(table: pa.Table) -> list[list[Any]]:
    return [[field.name, str(field.type), field.nullable] for field in table.schema]


def _write_nested_parquet(path: Path) -> None:
    schema = pa.schema(
        [
            pa.field(
                "top",
                pa.struct([pa.field("inner", pa.string(), nullable=False)]),
                nullable=False,
            ),
            pa.field(
                "vals",
                pa.list_(pa.field("element", pa.int32(), nullable=False)),
                nullable=False,
            ),
        ]
    )
    table = pa.table(
        {
            "top": pa.array([{"inner": "x"}, {"inner": "y"}], type=schema[0].type),
            "vals": pa.array([[1, 2], [3]], type=schema[1].type),
        },
        schema=schema,
    )
    pq.write_table(table, str(path), compression="snappy")


def _spark_ctas_schema(tmp_path: Path, select: str, stem: str) -> list[list[Any]]:
    from repark import ReparkSession

    warehouse = tmp_path
    parquet = warehouse / "bronze.parquet"
    write_bronze_parquet(parquet)
    session = ReparkSession.builder.appName("cutover-schema-1-ctas").getOrCreate()
    try:
        session.register_memory_catalog("ice", warehouse)
        sql_arrow(session, f"CREATE NAMESPACE ice.cut LOCATION '{warehouse / 'cut'}'")
        session.read.format("parquet").load(str(parquet)).createOrReplaceTempView("staging_view")
        sql_arrow(session, f"CREATE TABLE ice.cut.{stem} USING iceberg AS {select}")
        facts = _metadata_facts(warehouse, stem)
    finally:
        session.stop()
    for fact in facts:
        if fact[0] == "schema":
            return fact[1]
    raise AssertionError("ctas metadata carries no schema fact")


def test_read_parquet_reports_every_field_nullable(tmp_path: Path) -> None:
    from repark import ReparkSession

    parquet = tmp_path / "bronze.parquet"
    write_bronze_parquet(parquet)
    session = ReparkSession.builder.appName("cutover-schema-1-read").getOrCreate()
    try:
        cells = _schema_cells(session.read.parquet(str(parquet)).to_arrow())
    finally:
        session.stop()
    assert cells == [
        ["id", "string", True],
        ["ingestion_timestamp", "timestamp[us]", True],
        ["amount", "decimal128(10, 4)", True],
        ["units", "int32", True],
        ["note", "string", True],
        ["part", "int32", True],
    ]


def test_read_parquet_relaxes_nested_fields(tmp_path: Path) -> None:
    from repark import ReparkSession

    parquet = tmp_path / "nested.parquet"
    _write_nested_parquet(parquet)
    session = ReparkSession.builder.appName("cutover-schema-1-nested").getOrCreate()
    try:
        table = session.read.parquet(str(parquet)).to_arrow()
    finally:
        session.stop()
    assert table.schema.field("top").nullable is True
    assert table.schema.field("vals").nullable is True
    inner = next(field for field in table.schema.field("top").type if field.name == "inner")
    assert str(inner.type) == "string"
    assert inner.nullable is True
    assert table.schema.field("vals").type.value_field.nullable is True


def test_read_csv_and_json_report_nullable_fields(tmp_path: Path) -> None:
    from repark import ReparkSession

    csv_path = tmp_path / "data.csv"
    csv_path.write_text("a,b\n1,x\n2,y\n", encoding="utf-8")
    json_path = tmp_path / "data.json"
    json_path.write_text('{"a": 1, "b": "x"}\n{"a": 2, "b": "y"}\n', encoding="utf-8")
    session = ReparkSession.builder.appName("cutover-schema-1-text").getOrCreate()
    try:
        csv_cells = _schema_cells(
            session.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(str(csv_path))
            .to_arrow()
        )
        json_cells = _schema_cells(session.read.json(str(json_path)).to_arrow())
    finally:
        session.stop()
    assert [cell[0] for cell in csv_cells] == ["a", "b"]
    assert all(cell[2] for cell in csv_cells)
    assert [cell[0] for cell in json_cells] == ["a", "b"]
    assert all(cell[2] for cell in json_cells)


def test_ctas_stores_every_column_optional(tmp_path: Path) -> None:
    schema = _spark_ctas_schema(tmp_path, "SELECT * FROM staging_view", "t1")
    assert schema == [
        ["id", "string", False],
        ["ingestion_timestamp", "timestamp", False],
        ["amount", "decimal(10,4)", False],
        ["units", "int", False],
        ["note", "string", False],
        ["part", "int", False],
    ]


def test_ctas_of_coalesce_stores_optional(tmp_path: Path) -> None:
    select = (
        "SELECT coalesce(units, 0) AS u, 'lit' AS s, coalesce(id, 'z') AS nid FROM staging_view"
    )
    schema = _spark_ctas_schema(tmp_path, select, "t2")
    assert schema == [["u", "long", False], ["s", "string", False], ["nid", "string", False]]


def test_dedup_arrow_schema_matches_spark(tmp_path: Path) -> None:
    from repark import ReparkSession, Window, functions
    from repark.spark import types

    parquet = tmp_path / "bronze.parquet"
    write_bronze_parquet(parquet)
    session = ReparkSession.builder.appName("cutover-schema-1-dedup").getOrCreate()
    try:
        frame = session.read.format("parquet").load(str(parquet))
        table = (
            apply_dedup(frame, functions, types, Window)
            .select("id", "amount", "units", "note", "part")
            .to_arrow()
        )
    finally:
        session.stop()
    assert _schema_cells(table) == _SPARK_DEDUP_SCHEMA
    assert sorted(tuple(row.values()) for row in table.to_pylist()) == _SPARK_DEDUP_ROWS


def test_decimal_cast_of_non_null_is_nullable() -> None:
    from repark import ReparkSession

    session = ReparkSession.builder.appName("cutover-schema-1-cast").getOrCreate()
    try:
        table = session.sql(
            "SELECT CAST(1 AS DECIMAL(10,4)) AS d, CAST(1 AS INT) AS i, CAST(1 AS STRING) AS s"
        ).to_arrow()
    finally:
        session.stop()
    assert _schema_cells(table) == [
        ["d", "decimal128(10, 4)", True],
        ["i", "int32", False],
        ["s", "string", False],
    ]


def test_ansi_door_cast_keeps_datafusion_nullability() -> None:
    import repark

    table = repark.sql("SELECT CAST(1 AS DECIMAL(10,4)) AS d").to_arrow()
    assert _schema_cells(table) == [["d", "decimal128(10, 4)", False]]


def test_tighten_derived_ctas_still_refuses(tmp_path: Path) -> None:
    from repark import ReparkSession
    from repark.errors import AnalysisException

    warehouse = tmp_path
    session = ReparkSession.builder.appName("cutover-schema-1-se1").getOrCreate()
    try:
        session.register_memory_catalog("ice", warehouse)
        sql_arrow(session, f"CREATE NAMESPACE ice.cut LOCATION '{warehouse / 'cut'}'")
        frame = session.createDataFrame([("AAA", 1), ("BBB", 2)], ["sym", "tick"])
        frame.declareSorted("sym", "tick", tightenNulls=True).createOrReplaceTempView("tight_view")
        with pytest.raises(AnalysisException, match="tightenNulls"):
            sql_arrow(
                session,
                "CREATE TABLE ice.cut.tight_ctas USING iceberg AS SELECT * FROM tight_view",
            )
    finally:
        session.stop()


def test_read_parquet_relaxes_only_to_depth_32(tmp_path: Path) -> None:
    from repark import ReparkSession

    parquet = tmp_path / "deep40.parquet"
    inner: pa.DataType = pa.int32()
    for level in range(40):
        inner = pa.struct([pa.field(f"n{level}", inner, nullable=False)])
    schema = pa.schema([pa.field("root", inner, nullable=False)])
    value: Any = 7
    for level in range(40):
        value = {f"n{level}": value}
    pq.write_table(pa.table({"root": pa.array([value], type=inner)}, schema=schema), str(parquet))
    session = ReparkSession.builder.appName("cutover-schema-1-nulldepth").getOrCreate()
    try:
        read_back = session.read.parquet(str(parquet)).to_arrow().schema
    finally:
        session.stop()
    flags = [read_back.field("root").nullable]
    current = read_back.field("root").type
    for _ in range(40):
        child = current[0]
        flags.append(child.nullable)
        current = child.type
    assert flags.index(False) == 34


def test_read_parquet_tz_naive_timestamp_reports_string_dtype(tmp_path: Path) -> None:
    from repark import ReparkSession

    parquet = tmp_path / "bronze.parquet"
    write_bronze_parquet(parquet)
    session = ReparkSession.builder.appName("cutover-schema-1-tsntz").getOrCreate()
    try:
        frame = session.read.parquet(str(parquet))
        dtypes = frame.dtypes
        simple = frame.schema.simpleString()
        arrow_type = str(frame.to_arrow().schema.field("ingestion_timestamp").type)
    finally:
        session.stop()
    assert ("ingestion_timestamp", "string") in dtypes
    assert "ingestion_timestamp:string" in simple
    assert arrow_type == "timestamp[us]"


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_read_parquet_matches_oracle_nullability(
    tmp_path: Path, spark_engine: lp.Engine
) -> None:
    from repark import ReparkSession

    parquet = tmp_path / "bronze.parquet"
    write_bronze_parquet(parquet)
    session = ReparkSession.builder.appName("cutover-schema-1-live-read").getOrCreate()
    try:
        repark_cells = _schema_cells(session.read.parquet(str(parquet)).to_arrow())
    finally:
        session.stop()
    spark_cells = _schema_cells(
        spark_engine.arrow_of(spark_engine.session.read.parquet(str(parquet)))
    )
    assert repark_cells == spark_cells
    assert all(cell[2] for cell in repark_cells)


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_cast_rules_match_oracle(spark_engine: lp.Engine) -> None:
    from repark import ReparkSession

    text = "SELECT CAST(1 AS DECIMAL(10,4)) AS d, CAST(1 AS INT) AS i, CAST(1 AS STRING) AS s"
    session = ReparkSession.builder.appName("cutover-schema-1-live-cast").getOrCreate()
    try:
        repark_cells = _schema_cells(session.sql(text).to_arrow())
    finally:
        session.stop()
    spark_cells = _schema_cells(spark_engine.arrow_of(spark_engine.session.sql(text)))
    assert repark_cells == spark_cells


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_dedup_schema_matches_oracle(tmp_path: Path, spark_engine: lp.Engine) -> None:
    from pyspark.sql import Window as SparkWindow
    from pyspark.sql import functions as spark_functions
    from pyspark.sql import types as spark_types

    from repark import ReparkSession, Window
    from repark import functions as repark_functions
    from repark.spark import types as repark_types

    parquet = tmp_path / "bronze.parquet"
    write_bronze_parquet(parquet)
    session = ReparkSession.builder.appName("cutover-schema-1-live-dedup").getOrCreate()
    try:
        repark_table = (
            apply_dedup(
                session.read.format("parquet").load(str(parquet)),
                repark_functions,
                repark_types,
                Window,
            )
            .select("id", "amount", "units", "note", "part")
            .to_arrow()
        )
    finally:
        session.stop()
    spark_table = (
        apply_dedup(
            spark_engine.session.read.parquet(str(parquet)),
            spark_functions,
            spark_types,
            SparkWindow,
        )
        .select("id", "amount", "units", "note", "part")
        .toArrow()
    )
    assert _schema_cells(repark_table) == _schema_cells(spark_table)
    assert sorted(repark_table.to_pylist(), key=repr) == sorted(spark_table.to_pylist(), key=repr)

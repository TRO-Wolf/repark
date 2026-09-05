"""pins: nullability-2/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008"""

from __future__ import annotations

import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import _live_parity as lp
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

_CAST_VALUE_ROWS: list[tuple[str, str, str, bool, Any]] = [
    ("str_to_int", "SELECT CAST('1' AS INT) AS v", "int32", True, 1),
    ("str_to_bigint", "SELECT CAST('1' AS BIGINT) AS v", "int64", True, 1),
    ("str_to_smallint", "SELECT CAST('1' AS SMALLINT) AS v", "int16", True, 1),
    ("str_to_double", "SELECT CAST('1.5' AS DOUBLE) AS v", "double", True, 1.5),
    ("str_to_float", "SELECT CAST('1.5' AS FLOAT) AS v", "float", True, 1.5),
    ("str_to_bool", "SELECT CAST('true' AS BOOLEAN) AS v", "bool", True, True),
    (
        "str_to_date",
        "SELECT CAST(s AS DATE) AS v FROM (SELECT '2020-01-01' AS s) t",
        "date32[day]",
        True,
        datetime.date(2020, 1, 1),
    ),
    (
        "str_to_ts",
        "SELECT CAST(s AS TIMESTAMP) AS v FROM (SELECT '2020-01-01 00:00:00' AS s) t",
        "timestamp[us, tz=UTC]",
        True,
        datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC),
    ),
    (
        "ts_to_int",
        "SELECT CAST(TIMESTAMP '2020-01-01 00:00:00' AS INT) AS v",
        "int32",
        True,
        1577836800,
    ),
    ("double_to_int", "SELECT CAST(1.5 AS INT) AS v", "int32", True, 1),
    ("double_to_long", "SELECT CAST(1.5 AS BIGINT) AS v", "int64", True, 1),
    ("float_to_int", "SELECT CAST(CAST(1.5 AS FLOAT) AS INT) AS v", "int32", True, 1),
    ("float_to_short", "SELECT CAST(CAST(1.5 AS FLOAT) AS SMALLINT) AS v", "int16", True, 1),
    ("float_to_byte", "SELECT CAST(CAST(1.5 AS FLOAT) AS TINYINT) AS v", "int8", True, 1),
    (
        "date_to_ts",
        "SELECT CAST(DATE '2020-01-01' AS TIMESTAMP) AS v",
        "timestamp[us, tz=UTC]",
        False,
        datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC),
    ),
    (
        "ts_to_date",
        "SELECT CAST(TIMESTAMP '2020-01-01 00:00:00' AS DATE) AS v",
        "date32[day]",
        False,
        datetime.date(2020, 1, 1),
    ),
    (
        "ts_to_long",
        "SELECT CAST(TIMESTAMP '2020-01-01 00:00:00' AS BIGINT) AS v",
        "int64",
        False,
        1577836800,
    ),
    ("bigint_to_int", "SELECT CAST(CAST(1 AS BIGINT) AS INT) AS v", "int32", False, 1),
    ("int_to_bigint", "SELECT CAST(1 AS BIGINT) AS v", "int64", False, 1),
    ("int_to_smallint", "SELECT CAST(1 AS SMALLINT) AS v", "int16", False, 1),
    ("int_to_tinyint", "SELECT CAST(1 AS TINYINT) AS v", "int8", False, 1),
    ("int_to_string", "SELECT CAST(1 AS STRING) AS v", "string", False, "1"),
    ("int_to_double", "SELECT CAST(1 AS DOUBLE) AS v", "double", False, 1.0),
    ("int_to_float", "SELECT CAST(1 AS FLOAT) AS v", "float", False, 1.0),
    ("bool_to_int", "SELECT CAST(true AS INT) AS v", "int32", False, 1),
    ("int_to_bool", "SELECT CAST(1 AS BOOLEAN) AS v", "bool", False, True),
    ("bool_to_string", "SELECT CAST(true AS STRING) AS v", "string", False, "true"),
    (
        "date_to_string",
        "SELECT CAST(DATE '2020-01-01' AS STRING) AS v",
        "string",
        False,
        "2020-01-01",
    ),
    ("dec_to_int", "SELECT CAST(CAST(1 AS DECIMAL(10,0)) AS INT) AS v", "int32", True, 1),
    ("dec_to_long", "SELECT CAST(CAST(1 AS DECIMAL(10,0)) AS BIGINT) AS v", "int64", True, 1),
    ("dec_to_short", "SELECT CAST(CAST(1 AS DECIMAL(10,0)) AS SMALLINT) AS v", "int16", True, 1),
    ("dec_to_byte", "SELECT CAST(CAST(1 AS DECIMAL(10,0)) AS TINYINT) AS v", "int8", True, 1),
    ("dec_to_double", "SELECT CAST(CAST(1 AS DECIMAL(10,0)) AS DOUBLE) AS v", "double", False, 1.0),
    ("dec_to_string", "SELECT CAST(CAST(1 AS DECIMAL(10,0)) AS STRING) AS v", "string", False, "1"),
    (
        "ts_to_dec",
        "SELECT CAST(TIMESTAMP '2020-01-01 00:00:00' AS DECIMAL(20,0)) AS v",
        "decimal128(20, 0)",
        True,
        Decimal("1577836800"),
    ),
]

_CAST_FLAG_ROWS: list[tuple[str, str, bool]] = [
    ("str_bad_to_int_o2", "SELECT CAST('abc' AS INT) AS v", True),
    ("ts_to_short_o2", "SELECT CAST(TIMESTAMP '2020-01-01 00:00:00' AS SMALLINT) AS v", True),
    ("ts_to_byte_o2", "SELECT CAST(TIMESTAMP '2020-01-01 00:00:00' AS TINYINT) AS v", True),
]

_ARITH_ROWS: list[tuple[str, str, str, bool, bool, Any]] = [
    (
        "dec_add",
        "SELECT CAST(1 AS DECIMAL(10,0)) + CAST(1 AS DECIMAL(10,0)) AS v",
        "decimal128(11, 0)",
        False,
        True,
        Decimal("2"),
    ),
    (
        "dec_sub",
        "SELECT CAST(1 AS DECIMAL(10,0)) - CAST(1 AS DECIMAL(10,0)) AS v",
        "decimal128(11, 0)",
        False,
        True,
        Decimal("0"),
    ),
    (
        "dec_mul",
        "SELECT CAST(999 AS DECIMAL(10,0)) * CAST(999 AS DECIMAL(10,0)) AS v",
        "decimal128(21, 0)",
        False,
        True,
        Decimal("998001"),
    ),
    (
        "dec_col_add",
        "SELECT a + b AS v FROM (SELECT CAST(1 AS DECIMAL(10,0)) AS a,"
        " CAST(1 AS DECIMAL(10,0)) AS b) t",
        "decimal128(11, 0)",
        False,
        True,
        Decimal("2"),
    ),
    (
        "dec_col_mul",
        "SELECT a * b AS v FROM (SELECT CAST(999 AS DECIMAL(10,0)) AS a,"
        " CAST(999 AS DECIMAL(10,0)) AS b) t",
        "decimal128(21, 0)",
        False,
        True,
        Decimal("998001"),
    ),
    (
        "dec38_add",
        "SELECT CAST(1 AS DECIMAL(38,0)) + CAST(1 AS DECIMAL(38,0)) AS v",
        "decimal128(38, 0)",
        False,
        True,
        Decimal("2"),
    ),
    (
        "dec38_sub",
        "SELECT CAST(1 AS DECIMAL(38,0)) - CAST(1 AS DECIMAL(38,0)) AS v",
        "decimal128(38, 0)",
        False,
        True,
        Decimal("0"),
    ),
    (
        "dec38_mul",
        "SELECT CAST(1 AS DECIMAL(38,20)) * CAST(1 AS DECIMAL(38,20)) AS v",
        "decimal128(38, 6)",
        False,
        True,
        Decimal("1.000000"),
    ),
    (
        "float_literal_add",
        "SELECT 1.5 + 2.5 AS v",
        "decimal128(3, 1)",
        False,
        True,
        Decimal("4.0"),
    ),
]

_ARITH_CONTROLS: list[tuple[str, str, bool, Any]] = [
    ("int_add_int", "SELECT 9 + 9 AS v", False, 18),
    ("int_mul_int", "SELECT 9 * 9 AS v", False, 81),
    ("bigint_add_bigint", "SELECT CAST(9 AS BIGINT) + CAST(9 AS BIGINT) AS v", False, 18),
    ("mixed_mul", "SELECT 5 * CAST(1 AS DECIMAL(10,0)) AS v", True, Decimal("5")),
    (
        "dec_div",
        "SELECT CAST(1 AS DECIMAL(10,0)) / CAST(3 AS DECIMAL(10,0)) AS v",
        True,
        Decimal("0.33333333333"),
    ),
    (
        "dec_rem",
        "SELECT CAST(10 AS DECIMAL(10,0)) % CAST(3 AS DECIMAL(10,0)) AS v",
        True,
        Decimal("1"),
    ),
    ("dbl_add", "SELECT CAST(1.5 AS DOUBLE) + CAST(2.5 AS DOUBLE) AS v", False, 4.0),
]

_BOOL_DEC_ROWS: list[tuple[str, str, str, bool, Any]] = [
    (
        "bool_true_to_dec",
        "SELECT CAST(true AS DECIMAL(10,2)) AS v",
        "decimal128(10, 2)",
        False,
        Decimal("1.00"),
    ),
    (
        "bool_false_to_dec",
        "SELECT CAST(false AS DECIMAL(10,2)) AS v",
        "decimal128(10, 2)",
        False,
        Decimal("0.00"),
    ),
    (
        "bool_true_to_dec_1_0",
        "SELECT CAST(true AS DECIMAL(1,0)) AS v",
        "decimal128(1, 0)",
        False,
        Decimal("1"),
    ),
]

_NSE_SQL_ROWS: list[tuple[str, str, Any]] = [
    ("nse_sql_nulls", "SELECT (NULL <=> NULL) AS v", True),
    ("nse_sql_vals", "SELECT (1 <=> 1) AS v", True),
    ("nse_sql_mixed", "SELECT (1 <=> NULL) AS v", False),
]


def _spark_session(ansi: str) -> Any:
    from repark import ReparkSession

    active = ReparkSession.getActiveSession()
    if active is not None:
        active.stop()
    return (
        ReparkSession.builder.appName("nullability-2")
        .config("spark.sql.ansi.enabled", ansi)
        .getOrCreate()
    )


def test_cast_nullability_matches_spark() -> None:
    for ansi in ("true", "false"):
        session = _spark_session(ansi)
        try:
            for name, text, arrow_type, nullable, value in _CAST_VALUE_ROWS:
                table = session.sql(text).to_arrow()
                field = table.schema[0]
                assert (name, str(field.type), field.nullable, table.column(0).to_pylist()) == (
                    name,
                    arrow_type,
                    nullable,
                    [value],
                )
            for name, text, nullable in _CAST_FLAG_ROWS:
                logical = session.sql(text).schema.fields[0]
                assert (name, logical.nullable) == (name, nullable)
        finally:
            session.stop()


def test_valid_literal_cast_to_date_or_ts_stays_nonnull() -> None:
    session = _spark_session("true")
    try:
        typed = session.sql(
            "SELECT DATE '2020-01-01' AS d, TIMESTAMP '2020-01-01 00:00:00' AS t"
        ).to_arrow()
        assert [field.nullable for field in typed.schema] == [False, False]
        literal = session.sql(
            "SELECT CAST('2020-01-01' AS DATE) AS d, CAST('2020-01-01 00:00:00' AS TIMESTAMP) AS t"
        ).to_arrow()
        assert [field.nullable for field in literal.schema] == [False, False]
        invalid = session.sql("SELECT CAST('abc' AS DATE) AS d").schema.fields[0]
        assert invalid.nullable is True
    finally:
        session.stop()


def test_cast_nullability_native_door_keeps_datafusion() -> None:
    import repark

    table = repark.sql(
        "SELECT CAST('1' AS INT) AS i, CAST(DATE '2020-01-01' AS TIMESTAMP) AS dt"
    ).to_arrow()
    cells = [[field.name, str(field.type), field.nullable] for field in table.schema]
    assert cells == [
        ["i", "int32", False],
        ["dt", "timestamp[ns]", False],
    ]


def test_decimal_arithmetic_nullability_follows_ansi() -> None:
    for ansi, position in (("true", 3), ("false", 4)):
        session = _spark_session(ansi)
        try:
            for name, text, arrow_type, on_nullable, off_nullable, value in _ARITH_ROWS:
                table = session.sql(text).to_arrow()
                field = table.schema[0]
                expected = (on_nullable, off_nullable)[position - 3]
                assert (name, str(field.type), field.nullable, table.column(0).to_pylist()) == (
                    name,
                    arrow_type,
                    expected,
                    [value],
                )
            for name, text, nullable, value in _ARITH_CONTROLS:
                table = session.sql(text).to_arrow()
                field = table.schema[0]
                assert (name, field.nullable, table.column(0).to_pylist()) == (
                    name,
                    nullable,
                    [value],
                )
        finally:
            session.stop()


def test_bool_to_decimal_served_on_both_doors() -> None:
    import repark

    for ansi in ("true", "false"):
        session = _spark_session(ansi)
        try:
            for name, text, arrow_type, nullable, value in _BOOL_DEC_ROWS:
                table = session.sql(text).to_arrow()
                field = table.schema[0]
                assert (name, str(field.type), field.nullable, table.column(0).to_pylist()) == (
                    name,
                    arrow_type,
                    nullable,
                    [value],
                )
            narrow = session.sql("SELECT CAST(true AS DECIMAL(2,2)) AS v")
            assert narrow.schema.fields[0].nullable is True
            if ansi == "true":
                with pytest.raises(Exception, match="NUMERIC_VALUE_OUT_OF_RANGE"):
                    narrow.to_arrow()
            else:
                table = narrow.to_arrow()
                assert table.column(0).to_pylist() == [None]
                assert table.schema[0].nullable is True
        finally:
            session.stop()
    native = repark.sql(
        "SELECT CAST(true AS DECIMAL(10,2)) AS v, CAST(true AS DECIMAL(1,0)) AS w"
    ).to_arrow()
    assert [(str(field.type), field.nullable) for field in native.schema] == [
        ("decimal128(10, 2)", False),
        ("decimal128(1, 0)", False),
    ]
    assert native.column(0).to_pylist() == [Decimal("1.00")]
    assert native.column(1).to_pylist() == [Decimal("1")]
    with pytest.raises(Exception, match="NUMERIC_VALUE_OUT_OF_RANGE"):
        repark.sql("SELECT CAST(true AS DECIMAL(2,2)) AS v").to_arrow()


def test_null_safe_equal_is_non_null() -> None:
    from repark import functions as repark_functions

    for ansi in ("true", "false"):
        session = _spark_session(ansi)
        try:
            for name, text, value in _NSE_SQL_ROWS:
                frame = session.sql(text)
                table = frame.to_arrow()
                field = table.schema[0]
                assert (name, str(field.type), field.nullable, table.column(0).to_pylist()) == (
                    name,
                    "bool",
                    False,
                    [value],
                )
                assert frame.schema.fields[0].nullable is False
            df = session.sql("SELECT 1 AS a, NULL AS b")
            out = df.select(
                repark_functions.col("a").eqNullSafe(repark_functions.col("b")).alias("v")
            )
            out_field = out.to_arrow().schema[0]
            assert (str(out_field.type), out_field.nullable) == ("bool", False)
            assert out.schema.fields[0].nullable is False
        finally:
            session.stop()


def test_null_safe_equal_written_schema(tmp_path: Path) -> None:
    from repark import functions as repark_functions

    source = tmp_path / "nse_src.parquet"
    pq.write_table(
        pa.table(
            {
                "a": pa.array([1], type=pa.int32()),
                "b": pa.array([None], type=pa.int32()),
            }
        ),
        str(source),
    )
    session = _spark_session("true")
    try:
        out = session.read.parquet(str(source)).select(
            repark_functions.col("a").eqNullSafe(repark_functions.col("b")).alias("v")
        )
        assert out.to_arrow().schema[0].nullable is False
        out.write.parquet(str(tmp_path / "nse.parquet"))
    finally:
        session.stop()
    assert pq.read_schema(str(tmp_path / "nse.parquet")).field("v").nullable is False


def test_null_safe_equal_ctas_stores_optional(tmp_path: Path) -> None:
    from _sql_harden_cutover_run import _metadata_facts, sql_arrow

    warehouse = tmp_path
    session = _spark_session("true")
    try:
        session.register_memory_catalog("ice", warehouse)
        sql_arrow(session, f"CREATE NAMESPACE ice.cut LOCATION '{warehouse / 'cut'}'")
        sql_arrow(
            session,
            "CREATE TABLE ice.cut.nse USING iceberg AS SELECT (NULL <=> NULL) AS v",
        )
        facts = _metadata_facts(warehouse, "nse")
    finally:
        session.stop()
    for fact in facts:
        if fact[0] == "schema":
            assert fact[1] == [["v", "boolean", False]]
            return
    raise AssertionError("ctas metadata carries no schema fact")


def test_reader_relax_covers_depth_40(tmp_path: Path) -> None:
    parquet = tmp_path / "deep40.parquet"
    inner: pa.DataType = pa.int32()
    for level in range(40):
        inner = pa.struct([pa.field(f"n{level}", inner, nullable=False)])
    schema = pa.schema([pa.field("root", inner, nullable=False)])
    value: Any = 7
    for level in range(40):
        value = {f"n{level}": value}
    pq.write_table(pa.table({"root": pa.array([value], type=inner)}, schema=schema), str(parquet))
    session = _spark_session("true")
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
    assert flags == [True] * 41


def test_deep_read_refuses_past_arrow_footer_limit(tmp_path: Path) -> None:
    parquet = tmp_path / "deep80.parquet"
    inner: pa.DataType = pa.int32()
    for level in range(80):
        inner = pa.struct([pa.field(f"n{level}", inner, nullable=False)])
    schema = pa.schema([pa.field("root", inner, nullable=False)])
    value: Any = 7
    for level in range(80):
        value = {f"n{level}": value}
    pq.write_table(pa.table({"root": pa.array([value], type=inner)}, schema=schema), str(parquet))
    session = _spark_session("true")
    try:
        with pytest.raises(Exception, match="DepthLimitReached"):
            session.read.parquet(str(parquet)).to_arrow()
    finally:
        session.stop()


def test_tz_naive_timestamp_dtype(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    import datetime as datetime_module

    from repark.spark.types import StructField, StructType, TimestampNTZType

    parquet = tmp_path / "bronze.parquet"
    schema = pa.schema(
        [
            pa.field("id", pa.string(), nullable=False),
            pa.field("ingestion_timestamp", pa.timestamp("us"), nullable=False),
        ]
    )
    pq.write_table(
        pa.table(
            {
                "id": pa.array(["A"], type=pa.string()),
                "ingestion_timestamp": pa.array([1_000_000], type=pa.timestamp("us")),
            },
            schema=schema,
        ),
        str(parquet),
    )
    session = _spark_session("true")
    try:
        frame = session.read.parquet(str(parquet))
        assert frame.dtypes == [("id", "string"), ("ingestion_timestamp", "timestamp_ntz")]
        assert frame.schema.simpleString() == "struct<id:string,ingestion_timestamp:timestamp_ntz>"
        arrow_type = str(frame.to_arrow().schema.field("ingestion_timestamp").type)
        assert arrow_type == "timestamp[us]"
        frame.printSchema()
        printed, _ = capsys.readouterr()
        assert printed == (
            "root\n"
            " |-- id: string (nullable = true)\n"
            " |-- ingestion_timestamp: timestamp_ntz (nullable = true)\n\n"
        )
        created = session.createDataFrame(
            [(datetime_module.datetime(2020, 1, 1),)],
            StructType([StructField("ts", TimestampNTZType(), True)]),
        )
        assert created.dtypes == [("ts", "timestamp_ntz")]
        aware = session.sql("SELECT CAST('2020-01-01 00:00:00' AS TIMESTAMP) AS v")
        assert aware.dtypes == [("v", "timestamp")]
    finally:
        session.stop()


def test_csv_json_timestamp_reads_keep_string_dtype(tmp_path: Path) -> None:
    csv_path = tmp_path / "ts.csv"
    csv_path.write_text("id,ts\nA,2020-01-01 00:00:00\n")
    json_path = tmp_path / "ts.json"
    json_path.write_text('{"id": "A", "ts": "2020-01-01T00:00:00"}\n')
    session = _spark_session("true")
    try:
        assert session.read.csv(str(csv_path)).dtypes == [("_c0", "string"), ("_c1", "string")]
        assert session.read.json(str(json_path)).dtypes == [("id", "string"), ("ts", "string")]
    finally:
        session.stop()


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_cast_matrix_matches_oracle(spark_engine: lp.Engine) -> None:
    session = _spark_session("true")
    try:
        for name, text, _arrow_type, _nullable, _ in _CAST_VALUE_ROWS:
            repark_table = session.sql(text).to_arrow()
            spark_table = spark_engine.arrow_of(spark_engine.session.sql(text))
            assert (name, str(repark_table.schema[0].type)) == (
                name,
                str(spark_table.schema[0].type),
            )
            assert repark_table.schema[0].nullable == spark_table.schema[0].nullable
    finally:
        session.stop()


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_arith_bool_nse_match_oracle(spark_engine: lp.Engine) -> None:
    from repark import functions as repark_functions

    session = _spark_session("false")
    try:
        with lp.spark_session_conf(spark_engine, (("spark.sql.ansi.enabled", "false"),)):
            for name, text, _, _, _, _ in _ARITH_ROWS:
                repark_field = session.sql(text).to_arrow().schema[0]
                spark_field = spark_engine.arrow_of(spark_engine.session.sql(text)).schema[0]
                assert (name, repark_field.nullable) == (name, spark_field.nullable)
            for name, text, _, _, _ in _BOOL_DEC_ROWS:
                repark_field = session.sql(text).to_arrow().schema[0]
                spark_field = spark_engine.arrow_of(spark_engine.session.sql(text)).schema[0]
                assert (name, str(repark_field.type), repark_field.nullable) == (
                    name,
                    str(spark_field.type),
                    spark_field.nullable,
                )
            nse = session.sql("SELECT (NULL <=> NULL) AS v").to_arrow().schema[0]
            oracle = spark_engine.arrow_of(
                spark_engine.session.sql("SELECT (NULL <=> NULL) AS v")
            ).schema[0]
            assert (str(nse.type), nse.nullable) == (str(oracle.type), oracle.nullable)
            repark_df = session.sql("SELECT 1 AS a, NULL AS b").select(
                repark_functions.col("a").eqNullSafe(repark_functions.col("b")).alias("v")
            )
            spark_df = spark_engine.session.sql("SELECT 1 AS a, NULL AS b").select(
                spark_engine.functions.col("a")
                .eqNullSafe(spark_engine.functions.col("b"))
                .alias("v")
            )
            assert repark_df.to_arrow().schema[0].nullable is False
            assert spark_engine.arrow_of(spark_df).schema[0].nullable is False
    finally:
        session.stop()


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_depth_40_and_tsntz_match_oracle(tmp_path: Path, spark_engine: lp.Engine) -> None:
    parquet = tmp_path / "deep40.parquet"
    inner: pa.DataType = pa.int32()
    for level in range(40):
        inner = pa.struct([pa.field(f"n{level}", inner, nullable=False)])
    schema = pa.schema([pa.field("root", inner, nullable=False)])
    value: Any = 7
    for level in range(40):
        value = {f"n{level}": value}
    pq.write_table(pa.table({"root": pa.array([value], type=inner)}, schema=schema), str(parquet))
    session = _spark_session("true")
    try:
        repark_frame = session.read.parquet(str(parquet))
        repark_flags = [repark_frame.schema.fields[0].nullable]
        spark_frame = spark_engine.session.read.parquet(str(parquet))
        spark_field = spark_frame.schema.fields[0]
        spark_flags = [spark_field.nullable]
        repark_type = repark_frame.to_arrow().schema.field("root").type
        for _ in range(40):
            repark_child = repark_type[0]
            repark_flags.append(repark_child.nullable)
            repark_type = repark_child.type
            spark_field = spark_field.dataType.fields[0]
            spark_flags.append(spark_field.nullable)
        bronze = tmp_path / "bronze.parquet"
        bronze_schema = pa.schema(
            [
                pa.field("id", pa.string(), nullable=False),
                pa.field("ingestion_timestamp", pa.timestamp("us"), nullable=False),
            ]
        )
        pq.write_table(
            pa.table(
                {
                    "id": pa.array(["A"], type=pa.string()),
                    "ingestion_timestamp": pa.array([1_000_000], type=pa.timestamp("us")),
                },
                schema=bronze_schema,
            ),
            str(bronze),
        )
        repark_dtypes = session.read.parquet(str(bronze)).dtypes
        spark_dtypes = spark_engine.session.read.parquet(str(bronze)).dtypes
    finally:
        session.stop()
    assert repark_flags == [True] * 41
    assert spark_flags == [True] * 41
    assert repark_dtypes == spark_dtypes
    assert ("ingestion_timestamp", "timestamp_ntz") in repark_dtypes

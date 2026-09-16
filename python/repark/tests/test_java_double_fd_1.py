"""JAVA-DOUBLE-FD-1 pins: JDK-longhand text, HALF_UP %f, suffixed casts."""

from __future__ import annotations

import math

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-java-double-fd-1").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def spark_nonansi() -> ReparkSession:
    session = (
        ReparkSession.builder.appName("pytest-java-double-fd-1-off")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _table(frame: object) -> pa.Table:
    return frame.to_arrow()  # type: ignore[attr-defined]


def _check_double(table: pa.Table, column: str, expected: float) -> None:
    values = table.column(column).to_pylist()
    assert len(values) == 1
    value = values[0]
    assert isinstance(value, float)
    if math.isnan(expected):
        assert math.isnan(value)
    elif expected == 0.0:
        assert value == 0.0
        assert math.copysign(1.0, value) == math.copysign(1.0, expected)
    else:
        assert value == expected
    assert table.schema.field(column).type == pa.float64()


def test_spark_door_fd_longhand_sql(spark: ReparkSession) -> None:
    """J10-fd cells answer JDK-longhand text on the SQL door, typed string."""
    table = _table(
        spark.sql(
            "SELECT CAST(CAST('8.41E21' AS DOUBLE) AS STRING) AS a, "
            "CAST(CAST('1.0E23' AS DOUBLE) AS STRING) AS b, "
            "CAST(CAST('2.0E-3' AS DOUBLE) AS STRING) AS c, "
            "CAST(CAST('5.0E-324' AS DOUBLE) AS STRING) AS d"
        )
    )
    assert table.column("a").to_pylist() == ["8.409999999999999E21"]
    assert table.column("b").to_pylist() == ["9.999999999999999E22"]
    assert table.column("c").to_pylist() == ["0.002"]
    assert table.column("d").to_pylist() == ["4.9E-324"]
    assert table.schema.field("a").type == pa.string()
    table = _table(
        spark.sql(
            "SELECT CAST(CAST('2.2250738585072014E-308' AS DOUBLE) AS STRING) AS a, "
            "CAST(CAST('9.007199254740993E15' AS DOUBLE) AS STRING) AS b, "
            "CAST(CAST('1.1' AS DOUBLE) AS STRING) AS c"
        )
    )
    assert table.column("a").to_pylist() == ["2.2250738585072014E-308"]
    assert table.column("b").to_pylist() == ["9.007199254740992E15"]
    assert table.column("c").to_pylist() == ["1.1"]
    assert table.schema.field("a").type == pa.string()


def test_facade_fd_longhand_col_cast(spark: ReparkSession) -> None:
    """J10-fd cells answer JDK-longhand text through F.col cast, typed string."""
    frame = spark.sql(
        "SELECT CAST('8.41E21' AS DOUBLE) AS a, CAST('1.0E23' AS DOUBLE) AS b, "
        "CAST('5.0E-324' AS DOUBLE) AS c, CAST('1.1' AS DOUBLE) AS d"
    ).select(
        F.col("a").cast("string").alias("a"),
        F.col("b").cast("string").alias("b"),
        F.col("c").cast("string").alias("c"),
        F.col("d").cast("string").alias("d"),
    )
    table = _table(frame)
    assert table.column("a").to_pylist() == ["8.409999999999999E21"]
    assert table.column("b").to_pylist() == ["9.999999999999999E22"]
    assert table.column("c").to_pylist() == ["4.9E-324"]
    assert table.column("d").to_pylist() == ["1.1"]
    assert table.schema.field("a").type == pa.string()


def test_spark_door_format_string_f_half_up(spark: ReparkSession) -> None:
    """J10-format-string-f answers Java Formatter HALF_UP on both doors."""
    table = _table(
        spark.sql(
            "SELECT format_string('%f', CAST('1.0E7' AS DOUBLE)) AS a, "
            "format_string('%.2f', CAST('0.125' AS DOUBLE)) AS b"
        )
    )
    assert table.column("a").to_pylist() == ["10000000.000000"]
    assert table.column("b").to_pylist() == ["0.13"]
    frame = spark.range(1).select(
        F.format_string("%f", F.lit(1.0e7)).alias("a"),
        F.format_string("%.2f", F.lit(0.125)).alias("b"),
    )
    table = _table(frame)
    assert table.column("a").to_pylist() == ["10000000.000000"]
    assert table.column("b").to_pylist() == ["0.13"]


_CAST_CASES: list[tuple[str, float]] = [
    ("'Inf'", math.inf),
    ("'inf'", math.inf),
    ("'-Inf'", -math.inf),
    ("'+inf'", math.inf),
    ("'Infinity'", math.inf),
    ("'-infinity'", -math.inf),
    ("'INFINITY'", math.inf),
    ("'NaN'", math.nan),
    ("'nan'", math.nan),
    ("'NAN'", math.nan),
    ("' Inf '", math.inf),
    ("'1e2'", 100.0),
    ("'-0'", -0.0),
    ("'1d'", 1.0),
    ("'1f'", 1.0),
    ("'1.5D'", 1.5),
    ("'+1'", 1.0),
    ("'.5'", 0.5),
    ("'1.'", 1.0),
    ("' 1.0 '", 1.0),
]


def test_spark_door_cast_accepting_shapes(spark: ReparkSession) -> None:
    """Every accepting DEGI-cast cell answers its value on the SQL door, typed double."""
    for literal, expected in _CAST_CASES:
        table = _table(spark.sql(f"SELECT CAST({literal} AS DOUBLE) AS r"))
        _check_double(table, "r", expected)


def test_spark_door_cast_accepting_shapes_nonansi(spark_nonansi: ReparkSession) -> None:
    """Every accepting DEGI-cast cell answers its value with ANSI off, typed double."""
    for literal, expected in _CAST_CASES:
        table = _table(spark_nonansi.sql(f"SELECT CAST({literal} AS DOUBLE) AS r"))
        _check_double(table, "r", expected)


def test_facade_cast_accepting_shapes(spark: ReparkSession) -> None:
    """Every accepting DEGI-cast cell answers its value through Column.cast."""
    for literal, expected in _CAST_CASES:
        frame = spark.sql(f"SELECT {literal} AS x").select(F.col("x").cast("double").alias("r"))
        _check_double(_table(frame), "r", expected)


def test_facade_cast_accepting_shapes_nonansi(spark_nonansi: ReparkSession) -> None:
    """Every accepting DEGI-cast cell answers through Column.cast with ANSI off."""
    for literal, expected in _CAST_CASES:
        frame = spark_nonansi.sql(f"SELECT {literal} AS x").select(
            F.col("x").cast("double").alias("r")
        )
        _check_double(_table(frame), "r", expected)


def test_cast_hex_refused(spark: ReparkSession) -> None:
    """DEGI-cast-hex refuses '0x10' with CAST_INVALID_INPUT under ANSI."""
    with pytest.raises(Exception, match="CAST_INVALID_INPUT"):
        spark.sql("SELECT CAST('0x10' AS DOUBLE) AS r").to_arrow()
    frame = spark.sql("SELECT '0x10' AS x").select(F.col("x").cast("double").alias("r"))
    with pytest.raises(Exception, match="CAST_INVALID_INPUT"):
        frame.to_arrow()


def test_cast_hex_null_nonansi(spark_nonansi: ReparkSession) -> None:
    """DEGI-cast-hex answers NULL with ANSI off on both doors, typed double."""
    table = _table(spark_nonansi.sql("SELECT CAST('0x10' AS DOUBLE) AS r"))
    assert table.column("r").to_pylist() == [None]
    assert table.schema.field("r").type == pa.float64()
    frame = spark_nonansi.sql("SELECT '0x10' AS x").select(F.col("x").cast("double").alias("r"))
    table = _table(frame)
    assert table.column("r").to_pylist() == [None]


def test_cast_suffix_float_target(spark: ReparkSession) -> None:
    """Java-suffixed text casts to FLOAT on both doors, typed float."""
    table = _table(spark.sql("SELECT CAST('1d' AS FLOAT) AS r"))
    assert table.column("r").to_pylist() == [1.0]
    assert table.schema.field("r").type == pa.float32()
    frame = spark.sql("SELECT '1f' AS x").select(F.col("x").cast("float").alias("r"))
    table = _table(frame)
    assert table.column("r").to_pylist() == [1.0]
    assert table.schema.field("r").type == pa.float32()


def test_typed_literals_above_decimal38_stay_nonnull(spark: ReparkSession) -> None:
    """R-17c-2: >DECIMAL(38) typed literals fold to non-null float fields."""
    cases = [
        ("SELECT 1e38D AS v", pa.float64(), 1e38),
        ("SELECT 1e200D AS v", pa.float64(), 1e200),
        ("SELECT 1e38F AS v", pa.float32(), 1e38),
    ]
    for sql, data_type, expected in cases:
        table = _table(spark.sql(sql))
        value = table.column("v").to_pylist()[0]
        assert math.isclose(value, expected, rel_tol=1e-6), sql
        assert table.schema.field("v").type == data_type, sql
        assert table.schema.field("v").nullable is False, sql
    frame = spark.range(1).select(F.expr("1e200D").alias("v"))
    table = _table(frame)
    assert table.column("v").to_pylist() == [1e200]
    assert table.schema.field("v").type == pa.float64()
    assert table.schema.field("v").nullable is False


def _check_string(table: pa.Table, column: str, expected: str) -> None:
    values = table.column(column).to_pylist()
    assert values == [expected]
    assert table.schema.field(column).type == pa.string()


def test_spark_door_format_string_nan_takes_no_sign(spark: ReparkSession) -> None:
    """Q19-fmt-0..3: sign, space and paren flags never prefix NaN."""
    for literal in [
        "SELECT format_string('%+f', CAST('NaN' AS DOUBLE)) AS v",
        "SELECT format_string('% f', CAST('NaN' AS DOUBLE)) AS v",
        "SELECT format_string('%f', -CAST('NaN' AS DOUBLE)) AS v",
        "SELECT format_string('%(f', -CAST('NaN' AS DOUBLE)) AS v",
    ]:
        _check_string(_table(spark.sql(literal)), "v", "NaN")
    frame = spark.range(1).select(
        F.format_string("%+f", F.lit(float("nan"))).alias("v"),
        F.format_string("% f", F.lit(float("nan"))).alias("w"),
    )
    table = _table(frame)
    _check_string(table, "v", "NaN")
    _check_string(table, "w", "NaN")


def test_spark_door_format_string_infinity_sign_controls(spark: ReparkSession) -> None:
    """Q19-fmt-6..7: infinity keeps Java sign handling on both doors."""
    table = _table(spark.sql("SELECT format_string('%+f', CAST('-Infinity' AS DOUBLE)) AS v"))
    _check_string(table, "v", "-Infinity")
    table = _table(spark.sql("SELECT format_string('%(f', CAST('-Infinity' AS DOUBLE)) AS v"))
    _check_string(table, "v", "(Infinity)")
    frame = spark.range(1).select(
        F.format_string("%+f", F.lit(float("-inf"))).alias("v"),
    )
    _check_string(_table(frame), "v", "-Infinity")


def test_spark_door_format_string_upper_f_refuses(spark: ReparkSession) -> None:
    """Q19-fmt-4..5: %F is not a Java conversion and refuses on both doors."""
    for literal in [
        "SELECT format_string('%F', -CAST('NaN' AS DOUBLE)) AS v",
        "SELECT format_string('%+F', CAST('NaN' AS DOUBLE)) AS v",
    ]:
        with pytest.raises(Exception, match="Conversion = 'F'"):
            spark.sql(literal).to_arrow()
    frame = spark.range(1).select(F.format_string("%F", F.lit(float("nan"))).alias("v"))
    with pytest.raises(Exception, match="Conversion = 'F'"):
        frame.to_arrow()


def test_spark_door_format_string_trailing_text_control(spark: ReparkSession) -> None:
    """Q19-fmt-14: trailing literal text after one verb answers upstream."""
    table = _table(spark.sql("SELECT format_string('%-10.2f|', CAST('3.14159' AS DOUBLE)) AS v"))
    _check_string(table, "v", "3.14      |")


_SFX_ROWS = [("1d",), ("1f",), ("1.5D",), ("-2.5F",), ("1dd",), ("0x10",)]
_SFX_OK = "x IN ('1d','1f','1.5D','-2.5F')"


def _sfx(session: ReparkSession) -> None:
    session.createDataFrame(_SFX_ROWS, ["x"]).createOrReplaceTempView("sfx")


def _check_float(table: pa.Table, column: str, expected: list) -> None:
    values = table.column(column).to_pylist()
    assert values == expected
    assert table.schema.field(column).type == pa.float64()
    assert table.schema.field(column).nullable is True


def test_spark_door_cast_suffix_column_ansi_error(spark: ReparkSession) -> None:
    """Q19-col-double-ansi: a bad row raises CAST_INVALID_INPUT on a real column."""
    _sfx(spark)
    with pytest.raises(Exception, match="CAST_INVALID_INPUT"):
        spark.sql("SELECT CAST(x AS DOUBLE) AS v FROM sfx").to_arrow()


def test_spark_door_cast_suffix_column_nonansi(spark_nonansi: ReparkSession) -> None:
    """Q19-col-double-nonansi: bad rows answer NULL on a real column."""
    _sfx(spark_nonansi)
    table = _table(spark_nonansi.sql("SELECT CAST(x AS DOUBLE) AS v FROM sfx"))
    _check_float(table, "v", [1.0, 1.0, 1.5, -2.5, None, None])


def test_q19_suffix_col_ok(spark: ReparkSession, spark_nonansi: ReparkSession) -> None:
    """Q19 ok cells, both ANSI settings: suffixed text casts over real columns."""
    for session in (spark, spark_nonansi):
        _sfx(session)
        table = _table(session.sql(f"SELECT CAST(x AS DOUBLE) AS v FROM sfx WHERE {_SFX_OK}"))
        _check_float(table, "v", [1.0, 1.0, 1.5, -2.5])
        table = _table(session.sql(f"SELECT CAST(x AS FLOAT) AS v FROM sfx WHERE {_SFX_OK}"))
        assert table.column("v").to_pylist() == [1.0, 1.0, 1.5, -2.5]
        assert table.schema.field("v").type == pa.float32()
        assert table.schema.field("v").nullable is True
        table = _table(session.sql("SELECT CAST(repeat('1d', 1) AS DOUBLE) AS v"))
        _check_float(table, "v", [1.0])


def test_facade_cast_suffix_column_ansi_error(spark: ReparkSession) -> None:
    """Q19-col-double-ansi through Column.cast."""
    _sfx(spark)
    frame = spark.table("sfx").select(F.col("x").cast("double").alias("v"))
    with pytest.raises(Exception, match="CAST_INVALID_INPUT"):
        frame.to_arrow()


def test_facade_cast_suffix_column_nonansi(spark_nonansi: ReparkSession) -> None:
    """Q19-col-double-nonansi through Column.cast."""
    _sfx(spark_nonansi)
    frame = spark_nonansi.table("sfx").select(F.col("x").cast("double").alias("v"))
    _check_float(_table(frame), "v", [1.0, 1.0, 1.5, -2.5, None, None])


def test_facade_cast_suffix_column_ok(spark: ReparkSession, spark_nonansi: ReparkSession) -> None:
    """Q19 ok cells, both ANSI settings, through Column.cast."""
    for session in (spark, spark_nonansi):
        _sfx(session)
        frame = session.sql(f"SELECT x FROM sfx WHERE {_SFX_OK}").select(
            F.col("x").cast("double").alias("v")
        )
        _check_float(_table(frame), "v", [1.0, 1.0, 1.5, -2.5])
        frame = session.sql(f"SELECT x FROM sfx WHERE {_SFX_OK}").select(
            F.col("x").cast("float").alias("v")
        )
        table = _table(frame)
        assert table.column("v").to_pylist() == [1.0, 1.0, 1.5, -2.5]
        assert table.schema.field("v").type == pa.float32()
        frame = session.sql("SELECT repeat('1d', 1) AS x").select(
            F.col("x").cast("double").alias("v")
        )
        _check_float(_table(frame), "v", [1.0])


def test_spark_door_format_string_alt_forces_point(spark: ReparkSession) -> None:
    """Q19-fmt-8..13: # always prints the decimal point, HALF_UP first."""
    for literal, expected in [
        ("SELECT format_string('%#.0f', CAST('1.0' AS DOUBLE)) AS v", "1."),
        ("SELECT format_string('%#.0f', CAST('0.0' AS DOUBLE)) AS v", "0."),
        ("SELECT format_string('%#.0f', CAST('2.5' AS DOUBLE)) AS v", "3."),
        ("SELECT format_string('%#f', CAST('1.0' AS DOUBLE)) AS v", "1.000000"),
        ("SELECT format_string('%.0f', CAST('2.5' AS DOUBLE)) AS v", "3"),
        ("SELECT format_string('%010.2f', CAST('-3.14159' AS DOUBLE)) AS v", "-000003.14"),
    ]:
        _check_string(_table(spark.sql(literal)), "v", expected)
    frame = spark.range(1).select(
        F.format_string("%#.0f", F.lit(2.5)).alias("v"),
        F.format_string("%.0f", F.lit(2.5)).alias("w"),
    )
    table = _table(frame)
    _check_string(table, "v", "3.")
    _check_string(table, "w", "3")

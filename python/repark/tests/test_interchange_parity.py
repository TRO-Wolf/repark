"""G-INT interchange parity — ``toPandas`` / ``createDataFrame`` / ``to_polars``.

Oracle: live PySpark 4.1.2 (zulu-17, ``spark.sql.session.timeZone=UTC``,
``spark.sql.execution.arrow.pyspark.enabled=true``) measured 2026-07-27. Routine CI is JVM-free;
goldens are pinned inline from that oracle. Value AND type are asserted on the Arrow path
(``to_arrow``) and on the pandas/polars export dtypes — never only via ``show()``.
"""

from __future__ import annotations

import datetime as dt
import math
from decimal import Decimal

import pyarrow as pa
import pytest

from repark import ReparkSession, Row


@pytest.fixture
def spark() -> ReparkSession:
    return ReparkSession.builder.appName("pytest-interchange").getOrCreate()


# ==================================================================================================
# Shared typed fixture (SQL CAST) — produces the INT-001 type matrix on both engines
# ==================================================================================================


def _typed_frame_with_nulls(spark: ReparkSession) -> object:
    """Three-row typed frame: non-null, all-null, non-null. ORDER BY i32 pins row order."""
    return spark.sql(
        """
        SELECT * FROM (
          SELECT
            CAST(1 AS INT) AS i32,
            CAST(10 AS BIGINT) AS i64,
            CAST(1.5 AS DOUBLE) AS f64,
            CAST(12.34 AS DECIMAL(10,2)) AS dec,
            CAST('hello' AS STRING) AS s,
            CAST(true AS BOOLEAN) AS b,
            CAST('2024-01-15' AS DATE) AS d,
            CAST('2024-01-15 12:30:00' AS TIMESTAMP) AS ts,
            CAST(1 AS INT) AS ord
          UNION ALL
          SELECT
            CAST(NULL AS INT) AS i32,
            CAST(NULL AS BIGINT) AS i64,
            CAST(NULL AS DOUBLE) AS f64,
            CAST(NULL AS DECIMAL(10,2)) AS dec,
            CAST(NULL AS STRING) AS s,
            CAST(NULL AS BOOLEAN) AS b,
            CAST(NULL AS DATE) AS d,
            CAST(NULL AS TIMESTAMP) AS ts,
            CAST(2 AS INT) AS ord
          UNION ALL
          SELECT
            CAST(-2 AS INT) AS i32,
            CAST(-20 AS BIGINT) AS i64,
            CAST(-0.5 AS DOUBLE) AS f64,
            CAST(-1.00 AS DECIMAL(10,2)) AS dec,
            CAST('world' AS STRING) AS s,
            CAST(false AS BOOLEAN) AS b,
            CAST('2020-06-01' AS DATE) AS d,
            CAST('2020-06-01 00:00:00' AS TIMESTAMP) AS ts,
            CAST(3 AS INT) AS ord
        ) t
        ORDER BY ord
        """
    ).drop("ord")


def _typed_frame_no_nulls(spark: ReparkSession) -> object:
    """Two-row typed frame with no nulls — pandas keeps int32/int64/bool (live Spark oracle)."""
    return spark.sql(
        """
        SELECT * FROM (
          SELECT
            CAST(1 AS INT) AS i32,
            CAST(10 AS BIGINT) AS i64,
            CAST(1.5 AS DOUBLE) AS f64,
            CAST(12.34 AS DECIMAL(10,2)) AS dec,
            CAST('hello' AS STRING) AS s,
            CAST(true AS BOOLEAN) AS b,
            CAST('2024-01-15' AS DATE) AS d,
            CAST('2024-01-15 12:30:00' AS TIMESTAMP) AS ts,
            CAST(1 AS INT) AS ord
          UNION ALL
          SELECT
            CAST(-2 AS INT) AS i32,
            CAST(-20 AS BIGINT) AS i64,
            CAST(-0.5 AS DOUBLE) AS f64,
            CAST(-1.00 AS DECIMAL(10,2)) AS dec,
            CAST('world' AS STRING) AS s,
            CAST(false AS BOOLEAN) AS b,
            CAST('2020-06-01' AS DATE) AS d,
            CAST('2020-06-01 00:00:00' AS TIMESTAMP) AS ts,
            CAST(2 AS INT) AS ord
        ) t
        ORDER BY ord
        """
    ).drop("ord")


# ==================================================================================================
# INT-001 — toPandas / to_pandas: value AND dtype per column type
# ==================================================================================================


def _assert_timestamp_wall_clock(value: object, expected: dt.datetime) -> None:
    """Compare a timestamp cell as naive wall-clock UTC (repark ns / Spark us,tz=UTC)."""
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    assert isinstance(value, dt.datetime)
    if value.tzinfo is not None:
        value = value.astimezone(dt.UTC).replace(tzinfo=None)
    assert value.replace(tzinfo=None) == expected


def test_to_pandas_with_nulls_values_and_dtypes(spark: ReparkSession) -> None:
    """Live Spark 4.1.2 Arrow-on: nulls promote int→float64, bool→object; decimal object+Decimal.

    Measured under TZ=UTC / arrow.pyspark.enabled=true. repark routes to_pandas through
    ``to_arrow().to_pandas()`` — the same path Arrow-enabled PySpark uses.

    INT-001 matrix (C1-Q-004): every column has Arrow type + value cells for all three rows,
    including i64 / f64 / ts (previously soft-pinned only).
    """
    pd = pytest.importorskip("pandas")
    frame = _typed_frame_with_nulls(spark)
    # Arrow path first (value AND type — load-bearing; never only show()).
    arrow = frame.to_arrow()
    assert arrow.schema.field("i32").type == pa.int32()
    assert arrow.schema.field("i64").type == pa.int64()
    assert arrow.schema.field("f64").type == pa.float64()
    assert arrow.schema.field("dec").type == pa.decimal128(10, 2)
    # SQL string literals may land as string or string_view; both are Utf8 family.
    string_type = arrow.schema.field("s").type
    assert (
        pa.types.is_string(string_type)
        or pa.types.is_large_string(string_type)
        or (hasattr(pa.types, "is_string_view") and pa.types.is_string_view(string_type))
    ), f"expected string family, got {string_type!r}"
    assert arrow.schema.field("b").type == pa.bool_()
    assert arrow.schema.field("d").type == pa.date32()
    # repark: timestamp[ns] (no tz); Spark: timestamp[us, tz=UTC]. Wall-clock equal under UTC.
    ts_type = arrow.schema.field("ts").type
    assert pa.types.is_timestamp(ts_type)
    assert ts_type.unit in {"ns", "us", "ms"}
    # repark has no tz on the SQL CAST path; Spark Arrow-on is UTC — pin either.
    assert ts_type.tz in (None, "UTC")

    rows = arrow.to_pylist()
    assert len(rows) == 3
    # Row 0 — full type matrix values
    assert rows[0]["i32"] == 1
    assert rows[0]["i64"] == 10
    assert rows[0]["f64"] == 1.5
    assert rows[0]["dec"] == Decimal("12.34")
    assert rows[0]["s"] == "hello"
    assert rows[0]["b"] is True
    assert rows[0]["d"] == dt.date(2024, 1, 15)
    _assert_timestamp_wall_clock(rows[0]["ts"], dt.datetime(2024, 1, 15, 12, 30, 0))
    # Row 1 — all-null value cells (every column)
    assert rows[1]["i32"] is None
    assert rows[1]["i64"] is None
    assert rows[1]["f64"] is None
    assert rows[1]["dec"] is None
    assert rows[1]["s"] is None
    assert rows[1]["b"] is None
    assert rows[1]["d"] is None
    assert rows[1]["ts"] is None
    # Row 2 — negative / second non-null
    assert rows[2]["i32"] == -2
    assert rows[2]["i64"] == -20
    assert rows[2]["f64"] == -0.5
    assert rows[2]["dec"] == Decimal("-1.00")
    assert rows[2]["s"] == "world"
    assert rows[2]["b"] is False
    assert rows[2]["d"] == dt.date(2020, 6, 1)
    _assert_timestamp_wall_clock(rows[2]["ts"], dt.datetime(2020, 6, 1, 0, 0, 0))

    pdf = frame.to_pandas()
    assert list(pdf.columns) == ["i32", "i64", "f64", "dec", "s", "b", "d", "ts"]
    # Null-bearing ints → float64 + nan (live Spark oracle, Arrow on and off).
    assert pdf["i32"].dtype == "float64"
    assert pdf["i64"].dtype == "float64"
    assert pdf["f64"].dtype == "float64"
    assert pdf["i32"].tolist()[0] == 1.0
    assert math.isnan(pdf["i32"].tolist()[1])
    assert pdf["i32"].tolist()[2] == -2.0
    assert pdf["i64"].tolist()[0] == 10.0
    assert math.isnan(pdf["i64"].tolist()[1])
    assert pdf["i64"].tolist()[2] == -20.0
    assert pdf["f64"].tolist()[0] == 1.5
    assert math.isnan(pdf["f64"].tolist()[1])
    assert pdf["f64"].tolist()[2] == -0.5
    # decimal stays object with Decimal / None
    assert pdf["dec"].dtype == object
    assert pdf["dec"].tolist() == [Decimal("12.34"), None, Decimal("-1.00")]
    # string → pandas StringDtype (na_value=nan)
    assert isinstance(pdf["s"].dtype, type(pd.StringDtype()))
    assert pdf["s"].tolist()[0] == "hello"
    assert pdf["s"].tolist()[2] == "world"
    assert pdf["s"].isna().tolist() == [False, True, False]
    # bool with nulls → object
    assert pdf["b"].dtype == object
    assert pdf["b"].tolist() == [True, None, False]
    # date → object with datetime.date
    assert pdf["d"].dtype == object
    assert pdf["d"].tolist() == [dt.date(2024, 1, 15), None, dt.date(2020, 6, 1)]
    # timestamp → datetime64[ns]
    assert str(pdf["ts"].dtype).startswith("datetime64")
    assert pdf["ts"].tolist()[0] == pd.Timestamp("2024-01-15 12:30:00")
    assert pd.isna(pdf["ts"].tolist()[1])
    assert pdf["ts"].tolist()[2] == pd.Timestamp("2020-06-01 00:00:00")


def test_to_pandas_no_nulls_preserves_numpy_ints_and_bool(spark: ReparkSession) -> None:
    """Without nulls, live Spark keeps int32/int64/bool numpy dtypes (Arrow-enabled toPandas).

    Also pins the Arrow path (value AND type) for the no-null matrix — C1-Q-004.
    """
    pd = pytest.importorskip("pandas")
    frame = _typed_frame_no_nulls(spark)
    arrow = frame.to_arrow()
    assert arrow.schema.field("i32").type == pa.int32()
    assert arrow.schema.field("i64").type == pa.int64()
    assert arrow.schema.field("f64").type == pa.float64()
    assert arrow.schema.field("dec").type == pa.decimal128(10, 2)
    string_type = arrow.schema.field("s").type
    assert (
        pa.types.is_string(string_type)
        or pa.types.is_large_string(string_type)
        or (hasattr(pa.types, "is_string_view") and pa.types.is_string_view(string_type))
    )
    assert arrow.schema.field("b").type == pa.bool_()
    assert arrow.schema.field("d").type == pa.date32()
    assert pa.types.is_timestamp(arrow.schema.field("ts").type)
    rows = arrow.to_pylist()
    assert rows[0]["i32"] == 1 and rows[0]["i64"] == 10 and rows[0]["f64"] == 1.5
    assert rows[0]["dec"] == Decimal("12.34") and rows[0]["s"] == "hello" and rows[0]["b"] is True
    assert rows[0]["d"] == dt.date(2024, 1, 15)
    _assert_timestamp_wall_clock(rows[0]["ts"], dt.datetime(2024, 1, 15, 12, 30, 0))
    assert rows[1]["i32"] == -2 and rows[1]["i64"] == -20 and rows[1]["f64"] == -0.5
    assert rows[1]["dec"] == Decimal("-1.00") and rows[1]["s"] == "world" and rows[1]["b"] is False
    assert rows[1]["d"] == dt.date(2020, 6, 1)
    _assert_timestamp_wall_clock(rows[1]["ts"], dt.datetime(2020, 6, 1, 0, 0, 0))

    pdf = frame.to_pandas()
    assert pdf["i32"].dtype == "int32"
    assert pdf["i64"].dtype == "int64"
    assert pdf["f64"].dtype == "float64"
    assert pdf["b"].dtype == "bool"
    assert pdf["i32"].tolist() == [1, -2]
    assert pdf["i64"].tolist() == [10, -20]
    assert pdf["f64"].tolist() == [1.5, -0.5]
    assert pdf["b"].tolist() == [True, False]
    assert pdf["dec"].tolist() == [Decimal("12.34"), Decimal("-1.00")]
    assert pdf["s"].tolist() == ["hello", "world"]
    assert pdf["d"].tolist() == [dt.date(2024, 1, 15), dt.date(2020, 6, 1)]
    assert pdf["ts"].tolist() == [
        pd.Timestamp("2024-01-15 12:30:00"),
        pd.Timestamp("2020-06-01 00:00:00"),
    ]


def test_to_pandas_camelcase_alias_is_same_method(spark: ReparkSession) -> None:
    pytest.importorskip("pandas")
    from repark import DataFrame

    assert DataFrame.toPandas is DataFrame.to_pandas
    pdf = spark.sql("SELECT CAST(5 AS INT) AS n").toPandas()
    assert pdf["n"].tolist() == [5]
    assert pdf["n"].dtype == "int32"


# ==================================================================================================
# INT-002 — createDataFrame from pandas / dicts / Rows (value+type vs live Spark)
# ==================================================================================================


def test_create_dataframe_from_dicts_value_and_arrow_type(spark: ReparkSession) -> None:
    """Live Spark: dict list → LongType + StringType (Arrow int64 + string), all nullable."""
    frame = spark.createDataFrame([{"id": 1, "name": "x"}, {"id": 2, "name": "y"}])
    table = frame.to_arrow()
    assert table.schema.field("id").type == pa.int64()
    assert table.schema.field("name").type == pa.string()
    assert table.to_pylist() == [{"id": 1, "name": "x"}, {"id": 2, "name": "y"}]
    # collect path (value, not only show)
    assert [row.asDict() for row in frame.collect()] == [
        {"id": 1, "name": "x"},
        {"id": 2, "name": "y"},
    ]


def test_create_dataframe_from_tuples_with_schema_names(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1, "x"), (2, "y")], schema=["id", "name"])
    table = frame.to_arrow()
    assert table.schema.field("id").type == pa.int64()
    assert table.schema.field("name").type == pa.string()
    assert table.to_pylist() == [{"id": 1, "name": "x"}, {"id": 2, "name": "y"}]


def test_create_dataframe_from_rows_value_and_arrow_type(spark: ReparkSession) -> None:
    """Live Spark accepts list[Row] → same LongType/StringType as dicts."""
    frame = spark.createDataFrame([Row(id=1, name="x"), Row(id=2, name="y")])
    table = frame.to_arrow()
    assert table.schema.field("id").type == pa.int64()
    assert table.schema.field("name").type == pa.string()
    assert table.to_pylist() == [{"id": 1, "name": "x"}, {"id": 2, "name": "y"}]


def test_create_dataframe_from_pandas_value_and_arrow_type(spark: ReparkSession) -> None:
    """Live Spark 4.1.2: Int64→Long, str→String, float64→Double; NaN/NA→null on Arrow path."""
    pd = pytest.importorskip("pandas")
    source = pd.DataFrame(
        {
            "i": pd.Series([1, None, 3], dtype="Int64"),
            "s": ["a", None, "c"],
            "f": [1.0, 2.5, float("nan")],
        }
    )
    frame = spark.createDataFrame(source)
    table = frame.to_arrow()
    assert table.schema.field("i").type == pa.int64()
    assert table.schema.field("s").type == pa.string()
    assert table.schema.field("f").type == pa.float64()
    assert table.to_pylist() == [
        {"i": 1, "s": "a", "f": 1.0},
        {"i": None, "s": None, "f": 2.5},
        {"i": 3, "s": "c", "f": None},
    ]


def test_create_dataframe_date_timestamp_decimal_literals(spark: ReparkSession) -> None:
    """Live Spark infers DateType / TimestampType / DecimalType(38,18) from Python scalars."""
    frame = spark.createDataFrame(
        [
            (
                dt.date(2024, 1, 1),
                dt.datetime(2024, 1, 1, 12, 0, 0),
                Decimal("1.23"),
            )
        ],
        ["d", "ts", "dec"],
    )
    table = frame.to_arrow()
    assert table.schema.field("d").type == pa.date32()
    assert pa.types.is_timestamp(table.schema.field("ts").type)
    assert table.schema.field("dec").type == pa.decimal128(38, 18)
    rows = table.to_pylist()
    assert rows[0]["d"] == dt.date(2024, 1, 1)
    assert rows[0]["dec"] == Decimal("1.230000000000000000")
    # wall-clock 12:00 (UTC session; repark timestamp[ns] naive)
    ts_value = rows[0]["ts"]
    if hasattr(ts_value, "to_pydatetime"):
        ts_value = ts_value.to_pydatetime()
    assert ts_value.replace(tzinfo=None) == dt.datetime(2024, 1, 1, 12, 0, 0)


def test_create_dataframe_nulls_in_tuples(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1, None), (None, "x")], ["a", "b"])
    table = frame.to_arrow()
    assert table.schema.field("a").type == pa.int64()
    assert table.schema.field("b").type == pa.string()
    assert table.to_pylist() == [{"a": 1, "b": None}, {"a": None, "b": "x"}]


def test_create_dataframe_empty_list_requires_schema(spark: ReparkSession) -> None:
    """Empty list needs names; columns are typed nulls (CAST NULL AS VARCHAR) — C2-L-003."""
    from repark.errors import PySparkValueError

    with pytest.raises(PySparkValueError, match="empty data requires a schema"):
        spark.createDataFrame([])
    empty = spark.createDataFrame([], schema=["a", "b"])
    table = empty.to_arrow()
    assert table.num_rows == 0
    assert table.column_names == ["a", "b"]
    # Bare Null types are forbidden — stable string family for name-only empty frames.
    assert (
        pa.types.is_string(table.schema.field("a").type)
        or pa.types.is_large_string(table.schema.field("a").type)
        or pa.types.is_string_view(table.schema.field("a").type)
    )
    assert (
        pa.types.is_string(table.schema.field("b").type)
        or pa.types.is_large_string(table.schema.field("b").type)
        or pa.types.is_string_view(table.schema.field("b").type)
    )


def test_create_dataframe_all_null_column_has_typed_arrow(spark: ReparkSession) -> None:
    """All-null columns must not emit bare Null Arrow types (C2-L-003)."""
    frame = spark.createDataFrame([(None, 1), (None, 2)], ["a", "b"])
    table = frame.to_arrow()
    assert table.schema.field("b").type == pa.int64()
    assert not pa.types.is_null(table.schema.field("a").type)
    assert (
        pa.types.is_string(table.schema.field("a").type)
        or pa.types.is_large_string(table.schema.field("a").type)
        or pa.types.is_string_view(table.schema.field("a").type)
    )
    assert table.to_pylist() == [{"a": None, "b": 1}, {"a": None, "b": 2}]

    only_null = spark.createDataFrame([(None,), (None,)], ["x"])
    only_table = only_null.to_arrow()
    assert not pa.types.is_null(only_table.schema.field("x").type)
    assert only_table.to_pylist() == [{"x": None}, {"x": None}]


def test_create_dataframe_pandas_typed_all_null_preserves_arrow_types(
    spark: ReparkSession,
) -> None:
    """pandas all-null typed columns must not collapse to VARCHAR (C3-Q-001).

    Untyped ``None`` tuples stay string (C2-L-003); source dtypes on pandas/polars are load-bearing.
    Mutation of all-null CAST → always-VARCHAR reds this pin on Arrow type
    (value alone is insufficient).
    """
    pd = pytest.importorskip("pandas")
    source = pd.DataFrame(
        {
            "i": pd.Series([pd.NA, pd.NA], dtype="Int64"),
            "f": pd.Series([float("nan"), float("nan")], dtype="float64"),
            "b": pd.Series([pd.NA, pd.NA], dtype="boolean"),
            "s": pd.Series([pd.NA, pd.NA], dtype="string"),
            "ts": pd.to_datetime([pd.NaT, pd.NaT]),
        }
    )
    table = spark.createDataFrame(source).to_arrow()
    assert table.schema.field("i").type == pa.int64()
    assert table.schema.field("f").type == pa.float64()
    assert table.schema.field("b").type == pa.bool_()
    assert (
        pa.types.is_string(table.schema.field("s").type)
        or pa.types.is_large_string(table.schema.field("s").type)
        or pa.types.is_string_view(table.schema.field("s").type)
    )
    assert pa.types.is_timestamp(table.schema.field("ts").type)
    assert table.num_rows == 2
    rows = table.to_pylist()
    assert rows == [
        {"i": None, "f": None, "b": None, "s": None, "ts": None},
        {"i": None, "f": None, "b": None, "s": None, "ts": None},
    ]
    # schema reorder must carry dtype-matched null casts with the names (not drop types).
    reordered = spark.createDataFrame(source, schema=["ts", "s", "b", "f", "i"]).to_arrow()
    assert reordered.column_names == ["ts", "s", "b", "f", "i"]
    assert pa.types.is_timestamp(reordered.schema.field("ts").type)
    assert reordered.schema.field("i").type == pa.int64()


def test_create_dataframe_integer_all_null_width_stable_vs_non_null(
    spark: ReparkSession,
) -> None:
    """Narrow Int32/Int16/Int8 all-null must not become int32 while non-null is int64 (C4-Q-001).

    VALUES emits bare Python int → Arrow int64 for every non-null integer cell. All-null CAST
    must match that width so null occupancy cannot change the schema (data-dependent type lie).
    """
    pd = pytest.importorskip("pandas")
    pl = pytest.importorskip("polars")

    for pandas_dtype in ("Int32", "Int16", "Int8"):
        all_null = pd.DataFrame({"i": pd.Series([pd.NA, pd.NA], dtype=pandas_dtype)})
        with_value = pd.DataFrame({"i": pd.Series([1, pd.NA], dtype=pandas_dtype)})
        null_table = spark.createDataFrame(all_null).to_arrow()
        value_table = spark.createDataFrame(with_value).to_arrow()
        assert null_table.schema.field("i").type == pa.int64(), pandas_dtype
        assert value_table.schema.field("i").type == pa.int64(), pandas_dtype
        assert null_table.to_pylist() == [{"i": None}, {"i": None}]
        assert value_table.to_pylist() == [{"i": 1}, {"i": None}]

    for polars_dtype in (pl.Int32, pl.Int16, pl.Int8):
        all_null_pl = pl.DataFrame({"i": pl.Series("i", [None, None], dtype=polars_dtype)})
        with_value_pl = pl.DataFrame({"i": pl.Series("i", [1, None], dtype=polars_dtype)})
        null_table = spark.createDataFrame(all_null_pl).to_arrow()
        value_table = spark.createDataFrame(with_value_pl).to_arrow()
        assert null_table.schema.field("i").type == pa.int64(), str(polars_dtype)
        assert value_table.schema.field("i").type == pa.int64(), str(polars_dtype)
        assert null_table.to_pylist() == [{"i": None}, {"i": None}]
        assert value_table.to_pylist() == [{"i": 1}, {"i": None}]


def test_create_dataframe_pandas_arrow_dtype_all_null_timestamp_date(
    spark: ReparkSession,
) -> None:
    """pandas ArrowDtype timestamp/date all-null must not collapse to VARCHAR (C4-L-002).

    ``str(ArrowDtype)`` is ``timestamp[ns][pyarrow]`` / ``date32[day][pyarrow]`` — not
    ``datetime64`` — so a datetime64-only map silently typed them as string.
    """
    pd = pytest.importorskip("pandas")
    source = pd.DataFrame(
        {
            "ts": pd.Series([None, None], dtype=pd.ArrowDtype(pa.timestamp("ns"))),
            "d": pd.Series([None, None], dtype=pd.ArrowDtype(pa.date32())),
            "f": pd.Series([None, None], dtype=pd.ArrowDtype(pa.float64())),
        }
    )
    table = spark.createDataFrame(source).to_arrow()
    assert pa.types.is_timestamp(table.schema.field("ts").type)
    assert table.schema.field("d").type == pa.date32()
    assert table.schema.field("f").type == pa.float64()
    assert table.to_pylist() == [
        {"ts": None, "d": None, "f": None},
        {"ts": None, "d": None, "f": None},
    ]


def test_create_dataframe_pandas_polars_date_decimal_datetime_all_null(
    spark: ReparkSession,
) -> None:
    """Pin C3 date/decimal/Datetime all-null CAST arms (C4-Q-003 mutation-proof).

    Deleting the DATE/DECIMAL/TIMESTAMP branches must red on Arrow type (value-only is green
    under VARCHAR). Covers pandas object-date / Arrow decimal and polars Date/Datetime/Decimal.
    """
    pd = pytest.importorskip("pandas")
    pl = pytest.importorskip("polars")

    pandas_source = pd.DataFrame(
        {
            "d": pd.Series([None, None], dtype=pd.ArrowDtype(pa.date32())),
            "dec": pd.Series([None, None], dtype=pd.ArrowDtype(pa.decimal128(10, 2))),
            "ts": pd.to_datetime([pd.NaT, pd.NaT]),
        }
    )
    pandas_table = spark.createDataFrame(pandas_source).to_arrow()
    assert pandas_table.schema.field("d").type == pa.date32()
    assert pandas_table.schema.field("dec").type == pa.decimal128(38, 18)
    assert pa.types.is_timestamp(pandas_table.schema.field("ts").type)
    assert pandas_table.to_pylist() == [
        {"d": None, "dec": None, "ts": None},
        {"d": None, "dec": None, "ts": None},
    ]

    polars_source = pl.DataFrame(
        {
            "d": pl.Series("d", [None, None], dtype=pl.Date),
            "ts": pl.Series("ts", [None, None], dtype=pl.Datetime("us")),
            "dec": pl.Series("dec", [None, None], dtype=pl.Decimal(precision=10, scale=2)),
        }
    )
    polars_table = spark.createDataFrame(polars_source).to_arrow()
    assert polars_table.schema.field("d").type == pa.date32()
    assert pa.types.is_timestamp(polars_table.schema.field("ts").type)
    assert polars_table.schema.field("dec").type == pa.decimal128(38, 18)
    assert polars_table.to_pylist() == [
        {"d": None, "ts": None, "dec": None},
        {"d": None, "ts": None, "dec": None},
    ]


def test_create_dataframe_list_all_nan_nat_preserves_arrow_types(
    spark: ReparkSession,
) -> None:
    """All-NaN / all-NaT on list/dict/Row paths must not erase to VARCHAR (C4-L-001).

    Normalize turns NaN/NaT into None; without a pre-normalize witness scan the VALUES path
    would emit CAST(NULL AS VARCHAR). Pure None stays string (C2-L-003).
    """
    np = pytest.importorskip("numpy")
    from repark.row import Row

    # Tuple path — float NaN → DOUBLE, value AND type.
    nan_table = spark.createDataFrame(
        [(float("nan"),), (float("nan"),)],
        ["x"],
    ).to_arrow()
    assert nan_table.schema.field("x").type == pa.float64()
    assert nan_table.to_pylist() == [{"x": None}, {"x": None}]

    # numpy NaT / datetime64 NaT → TIMESTAMP (type pin; prior pin was value-only on single NaT).
    nat_table = spark.createDataFrame(
        [(np.datetime64("NaT", "ns"),), (np.datetime64("NaT", "ns"),)],
        ["ts"],
    ).to_arrow()
    assert pa.types.is_timestamp(nat_table.schema.field("ts").type)
    assert nat_table.to_pylist() == [{"ts": None}, {"ts": None}]

    # dict path — same witnesses.
    dict_table = spark.createDataFrame(
        [{"x": float("nan"), "ts": np.datetime64("NaT", "ns")}] * 2
    ).to_arrow()
    assert dict_table.schema.field("x").type == pa.float64()
    assert pa.types.is_timestamp(dict_table.schema.field("ts").type)
    assert dict_table.to_pylist() == [{"x": None, "ts": None}, {"x": None, "ts": None}]

    # Row path.
    row_table = spark.createDataFrame([Row(x=float("nan")), Row(x=float("nan"))]).to_arrow()
    assert row_table.schema.field("x").type == pa.float64()
    assert row_table.to_pylist() == [{"x": None}, {"x": None}]

    # Pure None remains VARCHAR (C2-L-003) — control that we did not over-type.
    none_table = spark.createDataFrame([(None,), (None,)], ["x"]).to_arrow()
    assert not pa.types.is_null(none_table.schema.field("x").type)
    assert (
        pa.types.is_string(none_table.schema.field("x").type)
        or pa.types.is_large_string(none_table.schema.field("x").type)
        or pa.types.is_string_view(none_table.schema.field("x").type)
    )


def test_create_dataframe_polars_typed_all_null_preserves_arrow_types(
    spark: ReparkSession,
) -> None:
    """polars all-null typed columns keep dtype-matched Arrow types (C3-Q-001)."""
    pl = pytest.importorskip("polars")
    source = pl.DataFrame(
        {
            "i": pl.Series("i", [None, None], dtype=pl.Int64),
            "f": pl.Series("f", [None, None], dtype=pl.Float64),
            "b": pl.Series("b", [None, None], dtype=pl.Boolean),
            "s": pl.Series("s", [None, None], dtype=pl.String),
        }
    )
    table = spark.createDataFrame(source).to_arrow()
    assert table.schema.field("i").type == pa.int64()
    assert table.schema.field("f").type == pa.float64()
    assert table.schema.field("b").type == pa.bool_()
    assert (
        pa.types.is_string(table.schema.field("s").type)
        or pa.types.is_large_string(table.schema.field("s").type)
        or pa.types.is_string_view(table.schema.field("s").type)
    )
    assert table.to_pylist() == [
        {"i": None, "f": None, "b": None, "s": None},
        {"i": None, "f": None, "b": None, "s": None},
    ]


def test_create_dataframe_namedtuple_uses_fields_as_names(spark: ReparkSession) -> None:
    """namedtuple / NamedTuple with schema=None uses _fields, not _1/_2 (C3-Q-002 / C3-L-001)."""
    from collections import namedtuple
    from typing import NamedTuple

    Point = namedtuple("Point", ["x", "y"])
    table = spark.createDataFrame([Point(1, 2), Point(3, 4)]).to_arrow()
    assert table.column_names == ["x", "y"]
    assert table.schema.field("x").type == pa.int64()
    assert table.to_pylist() == [{"x": 1, "y": 2}, {"x": 3, "y": 4}]

    class Person(NamedTuple):
        id: int
        name: str

    typed = spark.createDataFrame([Person(1, "a"), Person(2, "b")]).to_arrow()
    assert typed.column_names == ["id", "name"]
    assert typed.to_pylist() == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

    # Explicit disjoint schema is pure positional rename (same as dict/Row).
    renamed = spark.createDataFrame([Point(1, 2)], schema=["a", "b"]).to_arrow()
    assert renamed.column_names == ["a", "b"]
    assert renamed.to_pylist() == [{"a": 1, "b": 2}]

    # Plain tuples keep default _1/_2 when schema is omitted.
    plain = spark.createDataFrame([(9, 8)]).to_arrow()
    assert plain.column_names == ["_1", "_2"]


def test_create_dataframe_schema_str_and_non_sequence_refuse(spark: ReparkSession) -> None:
    """schema= list/tuple of str, StructType, or DDL string — not bare non-DDL str / set / dict.

    A non-DDL str of length == width would otherwise character-iterate into silent wrong column
    names (e.g. schema=\"ab\" → columns a, b). set/dict order is nondeterministic.
    R-PARITY3: real DDL strings like ``'a INT, b STRING'`` are accepted (separate pins).
    """
    from repark.errors import PySparkTypeError

    with pytest.raises(PySparkTypeError, match=r"DDL|not str|character-iterated"):
        spark.createDataFrame([(1, 2)], schema="ab")
    with pytest.raises(PySparkTypeError, match=r"list/tuple|StructType|DDL"):
        spark.createDataFrame([(1,)], schema={"id"})  # type: ignore[arg-type]
    with pytest.raises(PySparkTypeError, match=r"list/tuple|StructType|DDL"):
        spark.createDataFrame([(1,)], schema={"id": "INT"})  # type: ignore[arg-type]
    with pytest.raises(PySparkTypeError, match="names must be str"):
        spark.createDataFrame([(1, 2)], schema=[1, 2])  # type: ignore[list-item]
    # Tuple of names remains accepted.
    ok = spark.createDataFrame([(1, "x")], schema=("id", "name")).to_arrow()
    assert ok.column_names == ["id", "name"]
    assert ok.to_pylist() == [{"id": 1, "name": "x"}]


def test_create_dataframe_numpy_datetime64_ns_is_timestamp(spark: ReparkSession) -> None:
    """numpy.datetime64[ns] must not become epoch int via .item() (C3-L-002).

    Unit ns returns int from .item(); without an explicit datetime64 branch, VALUES would emit
    integer SQL and Arrow int64 — silent type/value corruption vs TIMESTAMP.
    """
    np = pytest.importorskip("numpy")
    cell = np.datetime64("2024-01-15T12:00:00", "ns")
    assert isinstance(cell.item(), int)  # precondition: the foot-gun exists
    table = spark.createDataFrame([(cell,)], ["ts"]).to_arrow()
    assert pa.types.is_timestamp(table.schema.field("ts").type)
    _assert_timestamp_wall_clock(table.to_pylist()[0]["ts"], dt.datetime(2024, 1, 15, 12, 0, 0))

    # NaT → SQL NULL on the timestamp path (type AND value — C4-L-001 strengthens value-only).
    nat_table = spark.createDataFrame([(np.datetime64("NaT", "ns"),)], ["ts"]).to_arrow()
    assert pa.types.is_timestamp(nat_table.schema.field("ts").type)
    assert nat_table.to_pylist()[0]["ts"] is None

    # Coarser units still land as timestamp/date correctly.
    us = spark.createDataFrame([(np.datetime64("2024-01-15T12:30:00", "us"),)], ["ts"]).to_arrow()
    assert pa.types.is_timestamp(us.schema.field("ts").type)
    _assert_timestamp_wall_clock(us.to_pylist()[0]["ts"], dt.datetime(2024, 1, 15, 12, 30, 0))


def test_create_dataframe_empty_pandas_cannot_infer_schema(spark: ReparkSession) -> None:
    """Live Spark: empty pandas without StructType → CANNOT_INFER_EMPTY_SCHEMA (C1-L-003).

    repark is VALUES-only (no StructType schema path), so empty pandas/polars always fail loud
    rather than accepting name-only schema and silently losing dtypes / emitting untyped NULLs.
    """
    pd = pytest.importorskip("pandas")
    from repark.errors import PySparkValueError

    empty = pd.DataFrame({"i": pd.Series([], dtype="Int64"), "s": pd.Series([], dtype="string")})
    with pytest.raises(PySparkValueError, match="CANNOT_INFER_EMPTY_SCHEMA"):
        spark.createDataFrame(empty)
    with pytest.raises(PySparkValueError, match="CANNOT_INFER_EMPTY_SCHEMA"):
        spark.createDataFrame(empty, schema=["i", "s"])


def test_create_dataframe_empty_polars_cannot_infer_schema(spark: ReparkSession) -> None:
    pl = pytest.importorskip("polars")
    from repark.errors import PySparkValueError

    empty = pl.DataFrame(schema={"i": pl.Int64, "s": pl.String})
    with pytest.raises(PySparkValueError, match="CANNOT_INFER_EMPTY_SCHEMA"):
        spark.createDataFrame(empty)
    with pytest.raises(PySparkValueError, match="CANNOT_INFER_EMPTY_SCHEMA"):
        spark.createDataFrame(empty, schema=["i", "s"])


def test_create_dataframe_tz_aware_datetime_converts_to_utc(spark: ReparkSession) -> None:
    """tz-aware datetime must convert to UTC absolute time, not strip tz wall-clock (C1-Q-001)."""
    # 12:00 in US/Eastern (UTC-5 in January) → 17:00 UTC naive.
    eastern = dt.timezone(dt.timedelta(hours=-5))
    aware = dt.datetime(2024, 1, 15, 12, 0, 0, tzinfo=eastern)
    frame = spark.createDataFrame([(aware,)], ["ts"])
    table = frame.to_arrow()
    assert pa.types.is_timestamp(table.schema.field("ts").type)
    _assert_timestamp_wall_clock(table.to_pylist()[0]["ts"], dt.datetime(2024, 1, 15, 17, 0, 0))


def test_create_dataframe_pandas_timestamp_tz_and_naive(spark: ReparkSession) -> None:
    """pandas Timestamp / datetime64 path must preserve absolute time (C2-Q-001).

    Pure ``datetime`` was pinned in C1; the interchange entry is ``createDataFrame(pandas)``.
    Mutation of the Timestamp→pydatetime normalize (strip tz / null) must red this pin.
    """
    pd = pytest.importorskip("pandas")
    # tz-aware Timestamp column (Etc/GMT+5 == UTC-5 fixed offset via tz_localize).
    aware = pd.DataFrame(
        {
            "ts": pd.to_datetime(["2024-01-15 12:00:00"]).tz_localize("Etc/GMT+5"),
        }
    )
    table_aware = spark.createDataFrame(aware).to_arrow()
    assert pa.types.is_timestamp(table_aware.schema.field("ts").type)
    # 12:00 at GMT+5 (== UTC-5) → 17:00 UTC wall clock.
    _assert_timestamp_wall_clock(
        table_aware.to_pylist()[0]["ts"], dt.datetime(2024, 1, 15, 17, 0, 0)
    )

    naive_ts = pd.DataFrame({"ts": pd.to_datetime(["2024-01-15 12:30:00", "2020-06-01 00:00:00"])})
    table_naive = spark.createDataFrame(naive_ts).to_arrow()
    assert pa.types.is_timestamp(table_naive.schema.field("ts").type)
    rows = table_naive.to_pylist()
    _assert_timestamp_wall_clock(rows[0]["ts"], dt.datetime(2024, 1, 15, 12, 30, 0))
    _assert_timestamp_wall_clock(rows[1]["ts"], dt.datetime(2020, 6, 1, 0, 0, 0))

    # pd.NaT → SQL NULL on the timestamp column.
    with_nat = pd.DataFrame({"ts": pd.to_datetime(["2024-01-15 12:00:00", "NaT"])})
    nat_table = spark.createDataFrame(with_nat).to_arrow()
    nat_rows = nat_table.to_pylist()
    _assert_timestamp_wall_clock(nat_rows[0]["ts"], dt.datetime(2024, 1, 15, 12, 0, 0))
    assert nat_rows[1]["ts"] is None


def test_create_dataframe_decimal_scientific_notation_fixed_point(spark: ReparkSession) -> None:
    """Decimal str() may be scientific (1E-10); VALUES must emit fixed-point (C1-L-004)."""
    frame = spark.createDataFrame(
        [(Decimal("1E-10"), Decimal("1.23"), Decimal("1e-18"))],
        ["tiny", "plain", "eps"],
    )
    table = frame.to_arrow()
    assert table.schema.field("tiny").type == pa.decimal128(38, 18)
    rows = table.to_pylist()
    assert rows[0]["tiny"] == Decimal("0.000000000100000000")
    assert rows[0]["plain"] == Decimal("1.230000000000000000")
    assert rows[0]["eps"] == Decimal("0.000000000000000001")


def test_create_dataframe_decimal_outside_scale_refuses(spark: ReparkSession) -> None:
    """DECIMAL(38,18) envelope: under-scale / over-magnitude refuse loud (C2-L-002)."""
    from repark.errors import PySparkValueError

    with pytest.raises(PySparkValueError, match="outside DECIMAL\\(38, 18\\) scale"):
        spark.createDataFrame([(Decimal("1E-19"),)], ["d"])
    with pytest.raises(PySparkValueError, match="outside DECIMAL\\(38, 18\\) scale"):
        spark.createDataFrame([(Decimal("1.1234567890123456789"),)], ["d"])
    with pytest.raises(PySparkValueError, match="exceeds DECIMAL\\(38, 18\\) magnitude"):
        spark.createDataFrame([(Decimal(10) ** 25,)], ["d"])
    with pytest.raises(PySparkValueError, match="exceeds DECIMAL\\(38, 18\\) magnitude"):
        spark.createDataFrame([(Decimal(10) ** 20,)], ["d"])
    # Boundary: 10**20 - 1e-18 is still representable (20 integer digits max is exclusive 10**20).
    ok = spark.createDataFrame([(Decimal("1e-18"), Decimal("-1e-18"))], ["pos", "neg"])
    table = ok.to_arrow()
    assert table.schema.field("pos").type == pa.decimal128(38, 18)
    assert table.to_pylist()[0]["pos"] == Decimal("0.000000000000000001")


def test_create_dataframe_string_quote_and_escape(spark: ReparkSession) -> None:
    """SQL string literals must escape single quotes (C1-SEC-002 / C2-SEC-002)."""
    frame = spark.createDataFrame(
        [("O'Brien", "a''b"), ("plain", "x"), ("'", "''"), ("a'b'c", "end'")],
        ["name", "raw"],
    )
    table = frame.to_arrow()
    assert table.to_pylist() == [
        {"name": "O'Brien", "raw": "a''b"},
        {"name": "plain", "raw": "x"},
        {"name": "'", "raw": "''"},
        {"name": "a'b'c", "raw": "end'"},
    ]


def test_create_dataframe_dict_missing_key_null_fills(spark: ReparkSession) -> None:
    """r21 T1: schema=None dict lists Spark key-union null-fill (supersedes C1-L-001 refuse).

    Live Spark 4.1.2: missing keys on later/earlier rows become NULL; they no longer refuse.
    Name-list schema longer than source width still fails loud (length mismatch).
    """
    from repark.errors import PySparkValueError

    table = spark.createDataFrame([{"id": 1, "name": "x"}, {"id": 2}]).to_arrow()
    assert table.column_names == ["id", "name"]
    assert table.to_pylist() == [{"id": 1, "name": "x"}, {"id": 2, "name": None}]
    assert table.schema.field("id").type == pa.int64()
    # schema longer than first-row keys → length mismatch (unified bind; still fail-loud).
    with pytest.raises(PySparkValueError, match="schema length"):
        spark.createDataFrame([{"id": 1}], schema=["id", "name"])


def test_create_dataframe_row_missing_field_fails_loud(spark: ReparkSession) -> None:
    from repark.errors import PySparkValueError

    with pytest.raises(PySparkValueError, match="missing field"):
        spark.createDataFrame([Row(id=1, name="x"), Row(id=2)])
    with pytest.raises(PySparkValueError, match="schema length"):
        spark.createDataFrame([Row(id=1)], schema=["id", "name"])


def test_create_dataframe_pandas_schema_renames_positionally(spark: ReparkSession) -> None:
    """schema=[new names] renames columns positionally — never name-keyed silent null (C1-L-001)."""
    pd = pytest.importorskip("pandas")
    source = pd.DataFrame({"x": [1, 2], "y": ["a", "b"]})
    frame = spark.createDataFrame(source, schema=["id", "name"])
    table = frame.to_arrow()
    assert table.column_names == ["id", "name"]
    assert table.to_pylist() == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def test_create_dataframe_polars_schema_renames_positionally(spark: ReparkSession) -> None:
    pl = pytest.importorskip("polars")
    source = pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})
    frame = spark.createDataFrame(source, schema=["id", "name"])
    table = frame.to_arrow()
    assert table.column_names == ["id", "name"]
    assert table.to_pylist() == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def test_create_dataframe_schema_reorder_by_name_across_entry_points(
    spark: ReparkSession,
) -> None:
    """schema=['b','a'] on named sources reorders by name — no pandas/dict value swap (C2-L-001)."""
    pd = pytest.importorskip("pandas")
    pl = pytest.importorskip("polars")
    expected = [{"b": 2, "a": 1}]

    pandas_out = spark.createDataFrame(
        pd.DataFrame({"a": [1], "b": [2]}), schema=["b", "a"]
    ).to_arrow()
    assert pandas_out.column_names == ["b", "a"]
    assert pandas_out.to_pylist() == expected

    dict_out = spark.createDataFrame([{"a": 1, "b": 2}], schema=["b", "a"]).to_arrow()
    assert dict_out.column_names == ["b", "a"]
    assert dict_out.to_pylist() == expected

    row_out = spark.createDataFrame([Row(a=1, b=2)], schema=["b", "a"]).to_arrow()
    assert row_out.column_names == ["b", "a"]
    assert row_out.to_pylist() == expected

    polars_out = spark.createDataFrame(
        pl.DataFrame({"a": [1], "b": [2]}), schema=["b", "a"]
    ).to_arrow()
    assert polars_out.column_names == ["b", "a"]
    assert polars_out.to_pylist() == expected

    # Tuples have no source names — positional only (document + pin).
    tuple_out = spark.createDataFrame([(1, 2)], schema=["b", "a"]).to_arrow()
    assert tuple_out.to_pylist() == [{"b": 1, "a": 2}]

    # namedtuple / NamedTuple have _fields — reorder by name like dict/Row (C6-L-001).
    # Positional-only bind would emit {b:1,a:2} and silently disagree with every named path.
    from collections import namedtuple
    from typing import NamedTuple

    Point = namedtuple("Point", ["a", "b"])
    namedtuple_out = spark.createDataFrame([Point(1, 2)], schema=["b", "a"]).to_arrow()
    assert namedtuple_out.column_names == ["b", "a"]
    assert namedtuple_out.to_pylist() == expected
    assert namedtuple_out.schema.field("b").type == pa.int64()
    assert namedtuple_out.schema.field("a").type == pa.int64()

    class Pair(NamedTuple):
        a: int
        b: int

    typed_out = spark.createDataFrame([Pair(1, 2)], schema=["b", "a"]).to_arrow()
    assert typed_out.column_names == ["b", "a"]
    assert typed_out.to_pylist() == expected


def test_create_dataframe_schema_subset_fails_loud_across_entry_points(
    spark: ReparkSession,
) -> None:
    """schema shorter than source width fails on dict and pandas alike (C2-L-001)."""
    pd = pytest.importorskip("pandas")
    from collections import namedtuple

    from repark.errors import PySparkValueError

    with pytest.raises(PySparkValueError, match="schema length"):
        spark.createDataFrame([{"a": 1, "b": 2, "c": 99}], schema=["a", "b"])
    with pytest.raises(PySparkValueError, match="schema length"):
        spark.createDataFrame(pd.DataFrame({"a": [1], "b": [2], "c": [99]}), schema=["a", "b"])
    with pytest.raises(PySparkValueError, match="partially overlaps"):
        spark.createDataFrame([{"a": 1, "b": 2}], schema=["a", "x"])
    # namedtuple partial overlap must fail loud too — not positional fail-open (C6-L-001).
    Point = namedtuple("Point", ["a", "b"])
    with pytest.raises(PySparkValueError, match="partially overlaps"):
        spark.createDataFrame([Point(1, 2)], schema=["a", "x"])


def test_create_dataframe_dict_later_row_extra_keys_union(spark: ReparkSession) -> None:
    """r21 T1: later-row extra keys join the key-union as new columns (Spark 4.1.2 oracle).

    Supersedes C2-L-004 refuse. First-row keys sorted then new keys appended.
    """
    table = spark.createDataFrame([{"a": 1}, {"a": 2, "secret": 99}]).to_arrow()
    assert table.column_names == ["a", "secret"]
    assert table.to_pylist() == [{"a": 1, "secret": None}, {"a": 2, "secret": 99}]
    assert table.schema.field("a").type == pa.int64()
    assert table.schema.field("secret").type == pa.int64()


def test_create_dataframe_refuses_string_as_row_iterable(spark: ReparkSession) -> None:
    """Non-list/tuple row elements must not character-iterate (C1-Q-002)."""
    from repark.errors import PySparkTypeError

    with pytest.raises(PySparkTypeError, match="homogeneous list/tuple"):
        spark.createDataFrame([("ab",), "cd"], schema=["s"])
    with pytest.raises(PySparkTypeError, match="element type str"):
        spark.createDataFrame(["ab", "cd"])


def test_create_dataframe_ragged_rows_fail_loud(spark: ReparkSession) -> None:
    """Ragged tuple widths raise (C1-Q-006)."""
    from repark.errors import PySparkValueError

    with pytest.raises(PySparkValueError, match="ragged rows"):
        spark.createDataFrame([(1, "a"), (2,)], schema=["id", "name"])


def test_create_dataframe_dict_list_must_be_homogeneous(spark: ReparkSession) -> None:
    """Mixed dict / non-dict lists fail loud (C1-L-005)."""
    from repark.errors import PySparkTypeError

    with pytest.raises(PySparkTypeError, match="homogeneous"):
        spark.createDataFrame([{"id": 1}, (2,)])


def test_create_dataframe_inf_and_timedelta_fail_loud(spark: ReparkSession) -> None:
    """Infinite floats and pandas Timedelta refuse loud (C2-Q-002 / C4-Q-002).

    All-null timedelta/duration must refuse too — not soft-succeed as VARCHAR while non-null
    Timedelta raises (fail-open type lie).
    """
    pd = pytest.importorskip("pandas")
    pl = pytest.importorskip("polars")
    from repark.errors import PySparkTypeError

    with pytest.raises(PySparkTypeError, match="infinite float"):
        spark.createDataFrame([(float("inf"),)], ["x"])
    with pytest.raises(PySparkTypeError, match="infinite float"):
        spark.createDataFrame([(float("-inf"),)], ["x"])
    # Non-null and all-null timedelta dtypes both refuse at the dtype map (C2-Q-002 / C4-Q-002).
    with pytest.raises(PySparkTypeError, match=r"timedelta|duration|Timedelta"):
        spark.createDataFrame(pd.DataFrame({"td": [pd.Timedelta(days=1)]}))
    with pytest.raises(PySparkTypeError, match=r"timedelta|duration|Timedelta"):
        spark.createDataFrame(
            pd.DataFrame({"td": pd.Series([pd.NaT, pd.NaT], dtype="timedelta64[ns]")})
        )
    with pytest.raises(PySparkTypeError, match=r"Duration|duration"):
        spark.createDataFrame(
            pl.DataFrame({"td": pl.Series("td", [None, None], dtype=pl.Duration("us"))})
        )
    # Tuple-path Timedelta cell (no frame dtype) still refuses at normalize.
    with pytest.raises(PySparkTypeError, match="Timedelta"):
        spark.createDataFrame([(pd.Timedelta(days=1),)], ["td"])


def test_create_dataframe_numpy_datetime64_date_unit_null_occupancy_stable(
    spark: ReparkSession,
) -> None:
    """numpy.datetime64 calendar units must not flip DATE↔TIMESTAMP by null occupancy (C3-Q-001).

    Non-null unit ``D``/``W``/``M``/``Y`` normalize to ``datetime.date`` → DATE Arrow. All-null
    ``NaT`` with the same unit must witness DATE too — not TIMESTAMP (prior path forced
    ``saw_timestamp`` for every ``datetime64``).
    """
    np = pytest.importorskip("numpy")

    for unit in ("D", "W", "M", "Y"):
        non_null = np.datetime64("2024-01-15", unit)
        assert isinstance(non_null.item(), dt.date)  # precondition: date-like unit
        with_value = spark.createDataFrame([(non_null,)], ["d"]).to_arrow()
        assert pa.types.is_date(with_value.schema.field("d").type), unit
        # Wall-clock for unit D is exact; coarser units may truncate (Y/M/W) — pin D value.
        if unit == "D":
            assert with_value.to_pylist() == [{"d": dt.date(2024, 1, 15)}]

        all_null = spark.createDataFrame(
            [(np.datetime64("NaT", unit),), (np.datetime64("NaT", unit),)],
            ["d"],
        ).to_arrow()
        assert pa.types.is_date(all_null.schema.field("d").type), unit
        assert all_null.to_pylist() == [{"d": None}, {"d": None}]
        # Occupancy stability: all-null type == with-value type.
        assert all_null.schema.field("d").type == with_value.schema.field("d").type

    # Finer units still land as TIMESTAMP (regression guard for the ns path).
    ns_null = spark.createDataFrame([(np.datetime64("NaT", "ns"),)], ["ts"]).to_arrow()
    assert pa.types.is_timestamp(ns_null.schema.field("ts").type)
    assert ns_null.to_pylist() == [{"ts": None}]


def test_create_dataframe_numpy_timedelta64_refuses(spark: ReparkSession) -> None:
    """numpy.timedelta64 must not become int via .item() (C3-L-001).

    Unit ``ns`` returns int from ``.item()``; without an explicit refuse, VALUES emits a bare
    integer (silent duration→count). Coarser units return ``datetime.timedelta`` which is also
    unsupported on the VALUES path — refuse the numpy type uniformly.
    """
    np = pytest.importorskip("numpy")
    from repark.errors import PySparkTypeError

    ns_cell = np.timedelta64(3, "ns")
    assert isinstance(ns_cell.item(), int)  # precondition: the foot-gun exists
    with pytest.raises(PySparkTypeError, match=r"timedelta64|timedelta"):
        spark.createDataFrame([(ns_cell,)], ["td"])
    with pytest.raises(PySparkTypeError, match=r"timedelta64|timedelta"):
        spark.createDataFrame([(np.timedelta64("NaT", "ns"),)], ["td"])
    with pytest.raises(PySparkTypeError, match=r"timedelta64|timedelta"):
        spark.createDataFrame([(np.timedelta64(1, "D"),)], ["td"])


def test_create_dataframe_pandas_interval_dtype_refuses(spark: ReparkSession) -> None:
    """pandas IntervalDtype must not fail-open as BIGINT/DOUBLE (C3-L-002).

    ``str(IntervalDtype).lower().startswith(\"int\")`` is true (\"interval…\"), so the integer
    all-null arm silently typed interval columns as BIGINT. Float-closed intervals hit the
    ``\"float\" in text`` DOUBLE arm. Dtype map runs for every pandas column (not only all-null),
    so a non-null int-interval frame is enough to pin the startswith(\"int\") foot-gun; all-null
    float-interval pins the float soft-map. Tuple-path Interval cells also refuse.
    """
    import numpy as np

    pd = pytest.importorskip("pandas")
    from repark.errors import PySparkTypeError

    int_interval = pd.IntervalDtype(subtype="int64")
    assert str(int_interval).lower().startswith("int")  # precondition: the foot-gun exists
    # Non-null int-interval column — dtype map refuses before VALUES (C3-L-002).
    with pytest.raises(PySparkTypeError, match=r"Interval|interval"):
        spark.createDataFrame(
            pd.DataFrame({"iv": pd.arrays.IntervalArray.from_tuples([(0, 1), (1, 2)])})
        )
    # All-null float-interval (pandas cannot materialize all-null int-interval easily).
    float_null = pd.arrays.IntervalArray.from_arrays([np.nan, np.nan], [np.nan, np.nan])
    assert "float" in str(float_null.dtype).lower()
    with pytest.raises(PySparkTypeError, match=r"Interval|interval"):
        spark.createDataFrame(pd.DataFrame({"iv": float_null}))
    # Tuple-path Interval cell (no frame dtype) also refuses at normalize.
    with pytest.raises(PySparkTypeError, match=r"Interval|interval"):
        spark.createDataFrame([(pd.Interval(0, 1),)], ["iv"])


def test_create_dataframe_polars_binary_time_all_null_refuses(
    spark: ReparkSession,
) -> None:
    """Unsupported polars Binary/Time must refuse all-null — not soft VARCHAR (C3-L-003).

    r21 T1: nested List/Struct/Array are accepted via Arrow; Binary/Time/Object still refuse
    so all-null cannot soft-succeed as VARCHAR while non-null cells raise elsewhere.
    """
    pl = pytest.importorskip("polars")
    from repark.errors import PySparkTypeError

    for dtype in (pl.Binary, pl.Time):
        frame = pl.DataFrame({"c": pl.Series("c", [None, None], dtype=dtype)})
        with pytest.raises(
            PySparkTypeError,
            match=r"binary|time|Binary|Time|object",
        ):
            spark.createDataFrame(frame)


def test_create_dataframe_pandas_datetime64_ms_stays_timestamp(
    spark: ReparkSession,
) -> None:
    """datetime64[ms] all-null must stay TIMESTAMP — not DATE via unit-m substring (C4-Q-001).

    ``\"datetime64[m]\" in \"datetime64[ms]\"`` is False (closed bracket), so the calendar-unit
    DATE arm does not fire for ms/us/ns/s. Pin Arrow type + value + null-occupancy equality so a
    future open-prefix match (``datetime64[m`` without ``]``) cannot silently flip DATE.
    """
    pd = pytest.importorskip("pandas")

    # Precondition: closed-bracket form does not treat ms as unit m.
    assert "datetime64[m]" not in "datetime64[ms]"
    assert "datetime64[m" in "datetime64[ms]"  # open prefix would be the foot-gun

    for unit in ("ms", "us", "ns", "s"):
        all_null = pd.DataFrame({"ts": pd.Series([pd.NaT, pd.NaT], dtype=f"datetime64[{unit}]")})
        with_value = pd.DataFrame(
            {
                "ts": pd.Series(
                    [pd.Timestamp("2024-01-15 12:00:00"), pd.NaT],
                    dtype=f"datetime64[{unit}]",
                )
            }
        )
        null_table = spark.createDataFrame(all_null).to_arrow()
        value_table = spark.createDataFrame(with_value).to_arrow()
        assert pa.types.is_timestamp(null_table.schema.field("ts").type), unit
        assert pa.types.is_timestamp(value_table.schema.field("ts").type), unit
        assert not pa.types.is_date(null_table.schema.field("ts").type), unit
        assert null_table.schema.field("ts").type == value_table.schema.field("ts").type
        assert null_table.to_pylist() == [{"ts": None}, {"ts": None}]
        assert value_table.to_pylist()[1] == {"ts": None}
        assert value_table.to_pylist()[0]["ts"] is not None


def test_create_dataframe_pandas_period_dtype_refuses(spark: ReparkSession) -> None:
    """PeriodDtype must not fail-open as VARCHAR/DATE while non-null Period raises (C4-Q-002).

    ``period[M]`` previously fell through to VARCHAR; ``period[D]`` hit the date arm via
    ``endswith(\"[d]\")`` → DATE32 — both soft successes while a Period cell TypeError'd.
    """
    pd = pytest.importorskip("pandas")
    from repark.errors import PySparkTypeError

    # Precondition: bare endswith("[d]") would classify period[D] as date without the refuse.
    assert str(pd.PeriodDtype("D")).lower().endswith("[d]")

    with pytest.raises(PySparkTypeError, match=r"Period|period"):
        spark.createDataFrame(pd.DataFrame({"p": pd.Series([pd.NaT, pd.NaT], dtype="period[M]")}))
    with pytest.raises(PySparkTypeError, match=r"Period|period"):
        spark.createDataFrame(pd.DataFrame({"p": pd.Series([pd.NaT, pd.NaT], dtype="period[D]")}))
    with pytest.raises(PySparkTypeError, match=r"Period|period"):
        spark.createDataFrame(pd.DataFrame({"p": pd.Series([pd.Period("2024-01", freq="M")])}))
    # Tuple-path Period cell (no frame dtype) refuses at normalize.
    with pytest.raises(PySparkTypeError, match=r"Period|period"):
        spark.createDataFrame([(pd.Period("2024-01", freq="M"),)], ["p"])


def test_create_dataframe_pandas_category_null_occupancy_stable(
    spark: ReparkSession,
) -> None:
    """Categorical all-null must follow categories.dtype — not VARCHAR vs int64 (C4-Q-003).

    Non-null int categories emit bare Python int → Arrow int64. All-null CategoricalDtype with
    the same categories must CAST BIGINT, not soft VARCHAR (null-occupancy type lie).
    """
    pd = pytest.importorskip("pandas")

    int_categories = pd.CategoricalDtype(categories=[1, 2, 3])
    all_null = pd.DataFrame({"c": pd.Series([None, None], dtype=int_categories)})
    with_value = pd.DataFrame({"c": pd.Series([1, None], dtype=int_categories)})
    null_table = spark.createDataFrame(all_null).to_arrow()
    value_table = spark.createDataFrame(with_value).to_arrow()
    assert null_table.schema.field("c").type == pa.int64()
    assert value_table.schema.field("c").type == pa.int64()
    assert null_table.schema.field("c").type == value_table.schema.field("c").type
    assert null_table.to_pylist() == [{"c": None}, {"c": None}]
    assert value_table.to_pylist() == [{"c": 1}, {"c": None}]

    # String categories: both sides stay string-family (not int/date). Exact string vs
    # string_view can still differ between CAST(NULL AS VARCHAR) and string literals — that
    # is the pre-existing all-null VARCHAR path (C2-L-003), not the int occupancy flip.
    str_categories = pd.CategoricalDtype(categories=["a", "b"])
    str_null = spark.createDataFrame(
        pd.DataFrame({"c": pd.Series([None, None], dtype=str_categories)})
    ).to_arrow()
    str_value = spark.createDataFrame(
        pd.DataFrame({"c": pd.Series(["a", None], dtype=str_categories)})
    ).to_arrow()
    for table in (str_null, str_value):
        field_type = table.schema.field("c").type
        assert (
            pa.types.is_string(field_type)
            or pa.types.is_large_string(field_type)
            or pa.types.is_string_view(field_type)
        )
    assert str_null.to_pylist() == [{"c": None}, {"c": None}]
    assert str_value.to_pylist() == [{"c": "a"}, {"c": None}]


def test_create_dataframe_pandas_arrow_time_binary_all_null_refuses(
    spark: ReparkSession,
) -> None:
    """pandas ArrowDtype time/binary all-null must refuse — not VARCHAR (C4-Q-004).

    r21 T1: nested list/struct ArrowDtype lands via ``pa.Table.from_pandas``; time/binary
    still refuse so all-null cannot soft-succeed as VARCHAR.
    """
    pd = pytest.importorskip("pandas")
    from repark.errors import PySparkTypeError

    cases = [
        pa.time64("us"),
        pa.time32("ms"),
        pa.binary(),
        pa.large_binary(),
    ]
    for arrow_type in cases:
        frame = pd.DataFrame({"c": pd.Series([None, None], dtype=pd.ArrowDtype(arrow_type))})
        with pytest.raises(
            PySparkTypeError,
            match=r"time|binary|Arrow",
        ):
            spark.createDataFrame(frame)


def test_create_dataframe_pandas_datetime64_minute_not_month(
    spark: ReparkSession,
) -> None:
    """datetime64 unit ``m`` (minute) must not map as calendar month ``M`` (C5-Q-001 / C5-L-001).

    Lowercasing the dtype text collapses numpy ``M`` (month → DATE) and ``m`` (minute →
    TIMESTAMP) into the same spelling. Pin the dtype mapper case-sensitively, list-path NaT[m]
    TIMESTAMP occupancy, and that ms still stays TIMESTAMP (closed-bracket residual).
    """
    np = pytest.importorskip("numpy")
    from repark.session import _null_sql_for_pandas_dtype

    # Mapper: case-sensitive unit — minute TIMESTAMP, month DATE (would fail if text.lower()).
    assert "TIMESTAMP" in _null_sql_for_pandas_dtype(np.dtype("datetime64[m]")).upper()
    assert "DATE" in _null_sql_for_pandas_dtype(np.dtype("datetime64[M]")).upper()
    assert "DATE" in _null_sql_for_pandas_dtype(np.dtype("datetime64[D]")).upper()
    assert "TIMESTAMP" in _null_sql_for_pandas_dtype(np.dtype("datetime64[ms]")).upper()

    # List path: all-null NaT[m] witnesses TIMESTAMP (not DATE); occupancy matches non-null.
    null_table = spark.createDataFrame(
        [(np.datetime64("NaT", "m"),), (np.datetime64("NaT", "m"),)],
        ["ts"],
    ).to_arrow()
    value_table = spark.createDataFrame(
        [(np.datetime64("2024-01-15T12:00", "m"),), (np.datetime64("NaT", "m"),)],
        ["ts"],
    ).to_arrow()
    assert pa.types.is_timestamp(null_table.schema.field("ts").type)
    assert pa.types.is_timestamp(value_table.schema.field("ts").type)
    assert not pa.types.is_date(null_table.schema.field("ts").type)
    assert null_table.schema.field("ts").type == value_table.schema.field("ts").type
    assert null_table.to_pylist() == [{"ts": None}, {"ts": None}]
    assert value_table.to_pylist()[0]["ts"] is not None
    assert value_table.to_pylist()[1] == {"ts": None}


def test_create_dataframe_pandas_complex_dtype_refuses(spark: ReparkSession) -> None:
    """complex64/128 must refuse all-null and non-null — not VARCHAR fail-open (C5-Q-002).

    Previously the dtype map fell through to CAST(NULL AS VARCHAR) while non-null complex
    cells raised at the SQL literal boundary (null-occupancy fail-open if nan→None).
    """
    pd = pytest.importorskip("pandas")
    from repark.errors import PySparkTypeError

    for dtype in ("complex64", "complex128"):
        with pytest.raises(PySparkTypeError, match=r"complex"):
            spark.createDataFrame(pd.DataFrame({"c": pd.Series([None, None], dtype=dtype)}))
        with pytest.raises(PySparkTypeError, match=r"complex"):
            spark.createDataFrame(pd.DataFrame({"c": pd.Series([1 + 2j, None], dtype=dtype)}))
    # Tuple / list path Python complex.
    with pytest.raises(PySparkTypeError, match=r"complex"):
        spark.createDataFrame([(1 + 2j,)], ["c"])


def test_create_dataframe_pandas_sparse_null_occupancy_stable(
    spark: ReparkSession,
) -> None:
    """Sparse[int64|bool|object] null occupancy stable (C5-Q-003 / C6-Q-001).

    ``Sparse[int64, nan]`` does not startswith(\"int\") and previously soft-mapped VARCHAR while
    non-null sparse cells typed as int64/bool (null-occupancy Arrow flip — C5-SAF-002).
    ``Sparse[object]`` unwraps to object → VARCHAR via the dtype map and previously skipped the
    object-cell NaN/NaT witness gate (top-level object only), so all-null NaN became string while
    with-value float/timestamp became DOUBLE/TIMESTAMP (C6-Q-001).
    """
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")

    int_dtype = pd.SparseDtype("int64", np.nan)
    int_null = pd.DataFrame({"i": pd.Series([np.nan, np.nan], dtype=int_dtype)})
    int_value = pd.DataFrame({"i": pd.Series([1, np.nan], dtype=int_dtype)})
    null_table = spark.createDataFrame(int_null).to_arrow()
    value_table = spark.createDataFrame(int_value).to_arrow()
    assert null_table.schema.field("i").type == pa.int64()
    assert value_table.schema.field("i").type == pa.int64()
    assert null_table.schema.field("i").type == value_table.schema.field("i").type
    assert null_table.to_pylist() == [{"i": None}, {"i": None}]
    assert value_table.to_pylist() == [{"i": 1}, {"i": None}]

    bool_dtype = pd.SparseDtype(bool, np.nan)
    bool_null = pd.DataFrame({"b": pd.Series([np.nan, np.nan], dtype=bool_dtype)})
    bool_value = pd.DataFrame({"b": pd.Series([True, np.nan], dtype=bool_dtype)})
    b_null = spark.createDataFrame(bool_null).to_arrow()
    b_value = spark.createDataFrame(bool_value).to_arrow()
    assert b_null.schema.field("b").type == pa.bool_()
    assert b_value.schema.field("b").type == pa.bool_()
    assert b_null.schema.field("b").type == b_value.schema.field("b").type
    assert b_null.to_pylist() == [{"b": None}, {"b": None}]
    assert b_value.to_pylist() == [{"b": True}, {"b": None}]

    # Sparse[object]: all-null NaN must match with-value float → DOUBLE, not string (C6-Q-001).
    # (pandas SparseDtype(object, nan) stores fill as float nan; witness path must still run.)
    object_dtype = pd.SparseDtype(object, np.nan)
    obj_null = pd.DataFrame({"x": pd.Series([np.nan, np.nan], dtype=object_dtype)})
    obj_value = pd.DataFrame({"x": pd.Series([1.5, np.nan], dtype=object_dtype)})
    o_null = spark.createDataFrame(obj_null).to_arrow()
    o_value = spark.createDataFrame(obj_value).to_arrow()
    assert pa.types.is_floating(o_null.schema.field("x").type)
    assert pa.types.is_floating(o_value.schema.field("x").type)
    assert o_null.schema.field("x").type == o_value.schema.field("x").type
    assert o_null.to_pylist() == [{"x": None}, {"x": None}]
    assert o_value.to_pylist() == [{"x": 1.5}, {"x": None}]


def test_create_dataframe_pandas_object_nan_nat_witnesses(
    spark: ReparkSession,
) -> None:
    """object-dtype all-null must witness NaN/NaT like the list path (C5-SAF-001).

    object → CAST(NULL AS VARCHAR) alone lies: raw float NaN is DOUBLE on the list path and
    pandas NaT is TIMESTAMP. Pure None object columns stay VARCHAR (C2-L-003).
    """
    pd = pytest.importorskip("pandas")

    obj_nan = pd.DataFrame({"x": pd.Series([float("nan"), float("nan")], dtype=object)})
    list_nan = spark.createDataFrame([(float("nan"),), (float("nan"),)], ["x"]).to_arrow()
    pandas_nan = spark.createDataFrame(obj_nan).to_arrow()
    assert pa.types.is_floating(pandas_nan.schema.field("x").type)
    assert pandas_nan.schema.field("x").type == list_nan.schema.field("x").type
    assert pandas_nan.to_pylist() == [{"x": None}, {"x": None}]

    obj_nat = pd.DataFrame({"x": pd.Series([pd.NaT, pd.NaT], dtype=object)})
    list_nat = spark.createDataFrame([(pd.NaT,), (pd.NaT,)], ["x"]).to_arrow()
    pandas_nat = spark.createDataFrame(obj_nat).to_arrow()
    assert pa.types.is_timestamp(pandas_nat.schema.field("x").type)
    assert pandas_nat.schema.field("x").type == list_nat.schema.field("x").type
    assert pandas_nat.to_pylist() == [{"x": None}, {"x": None}]

    # Pure None object: no typed witness → VARCHAR/string-family (not double/timestamp).
    obj_none = spark.createDataFrame(
        pd.DataFrame({"x": pd.Series([None, None], dtype=object)})
    ).to_arrow()
    field_type = obj_none.schema.field("x").type
    assert (
        pa.types.is_string(field_type)
        or pa.types.is_large_string(field_type)
        or pa.types.is_string_view(field_type)
    )
    assert obj_none.to_pylist() == [{"x": None}, {"x": None}]


# ==================================================================================================
# INT-003 — to_polars value+dtype; round-trip to_polars → createDataFrame
# ==================================================================================================


def test_to_polars_value_and_dtype(spark: ReparkSession) -> None:
    pl = pytest.importorskip("polars")
    frame = _typed_frame_no_nulls(spark)
    pldf = frame.to_polars()
    assert isinstance(pldf, pl.DataFrame)
    assert pldf.schema["i32"] == pl.Int32
    assert pldf.schema["i64"] == pl.Int64
    assert pldf.schema["f64"] == pl.Float64
    assert pldf.schema["dec"] == pl.Decimal(precision=10, scale=2)
    assert pldf.schema["s"] == pl.String
    assert pldf.schema["b"] == pl.Boolean
    assert pldf.schema["d"] == pl.Date
    assert pldf.schema["ts"] == pl.Datetime(time_unit="ns", time_zone=None)
    assert pldf.select("i32").to_series().to_list() == [1, -2]
    assert pldf.select("i64").to_series().to_list() == [10, -20]
    assert pldf.select("f64").to_series().to_list() == [1.5, -0.5]
    assert pldf.select("s").to_series().to_list() == ["hello", "world"]
    assert pldf.select("b").to_series().to_list() == [True, False]
    assert pldf.select("d").to_series().to_list() == [dt.date(2024, 1, 15), dt.date(2020, 6, 1)]
    assert pldf.select("dec").to_series().to_list() == [Decimal("12.34"), Decimal("-1.00")]
    ts_values = pldf.select("ts").to_series().to_list()
    assert ts_values[0] == dt.datetime(2024, 1, 15, 12, 30, 0)
    assert ts_values[1] == dt.datetime(2020, 6, 1, 0, 0, 0)


def test_to_polars_with_nulls_value_and_dtype(spark: ReparkSession) -> None:
    """INT-003 nulls coverage (C1-Q-005) — value AND dtype on the null-bearing typed matrix."""
    pl = pytest.importorskip("polars")
    frame = _typed_frame_with_nulls(spark)
    # Arrow path first (value AND type).
    arrow = frame.to_arrow()
    assert arrow.schema.field("i32").type == pa.int32()
    assert arrow.schema.field("i64").type == pa.int64()
    rows = arrow.to_pylist()
    assert rows[1]["i32"] is None and rows[1]["s"] is None and rows[1]["ts"] is None

    pldf = frame.to_polars()
    assert pldf.schema["i32"] == pl.Int32
    assert pldf.schema["i64"] == pl.Int64
    assert pldf.schema["f64"] == pl.Float64
    assert pldf.schema["dec"] == pl.Decimal(precision=10, scale=2)
    assert pldf.schema["s"] == pl.String
    assert pldf.schema["b"] == pl.Boolean
    assert pldf.schema["d"] == pl.Date
    assert pldf.schema["ts"] == pl.Datetime(time_unit="ns", time_zone=None)
    assert pldf.select("i32").to_series().to_list() == [1, None, -2]
    assert pldf.select("i64").to_series().to_list() == [10, None, -20]
    assert pldf.select("f64").to_series().to_list() == [1.5, None, -0.5]
    assert pldf.select("dec").to_series().to_list() == [Decimal("12.34"), None, Decimal("-1.00")]
    assert pldf.select("s").to_series().to_list() == ["hello", None, "world"]
    assert pldf.select("b").to_series().to_list() == [True, None, False]
    assert pldf.select("d").to_series().to_list() == [
        dt.date(2024, 1, 15),
        None,
        dt.date(2020, 6, 1),
    ]
    ts_values = pldf.select("ts").to_series().to_list()
    assert ts_values[0] == dt.datetime(2024, 1, 15, 12, 30, 0)
    assert ts_values[1] is None
    assert ts_values[2] == dt.datetime(2020, 6, 1, 0, 0, 0)


def test_to_polars_round_trip_create_dataframe_value_identity(spark: ReparkSession) -> None:
    """Round-trip values via createDataFrame(polars) — identity on value, dtype where VALUES allows.

    createDataFrame materializes through SQL VALUES, so Python ``int`` always lands as int64
    (see ``test_to_polars_round_trip_int32_widens_to_int64_divergence``). Date / string / bool /
    float64 / int64 values round-trip bit-for-bit on the Arrow path.
    """
    pl = pytest.importorskip("polars")
    source = spark.createDataFrame(
        [
            (1, "a", 1.5, True, dt.date(2024, 1, 15)),
            (2, "b", 2.5, False, dt.date(2020, 6, 1)),
        ],
        ["i64", "s", "f64", "b", "d"],
    )
    pldf = source.to_polars()
    assert pldf.schema["i64"] == pl.Int64
    round_trip = spark.createDataFrame(pldf)
    out = round_trip.to_arrow()
    assert out.schema.field("i64").type == pa.int64()
    assert out.schema.field("s").type == pa.string()
    assert out.schema.field("f64").type == pa.float64()
    assert out.schema.field("b").type == pa.bool_()
    assert out.schema.field("d").type == pa.date32()
    assert out.to_pylist() == source.to_arrow().to_pylist()
    # polars dtypes after round-trip match for this set (all VALUES-stable).
    again = round_trip.to_polars()
    assert again.schema == pldf.schema
    assert again.to_dicts() == pldf.to_dicts()


def test_to_polars_round_trip_int32_widens_to_int64_divergence(spark: ReparkSession) -> None:
    """Pin for registry row TY-4 — semantics live only there.

    See ``docs/spark-sql-iceberg-parity.md`` §4
    [TY-4](../../../docs/spark-sql-iceberg-parity.md#ty-4--createdataframe-widens-arrow-int32-to-int64).
    Strengthened pin (C1-Q-007): Arrow type on both sides of the round-trip, not only polars dtype.
    """
    pl = pytest.importorskip("polars")
    source = spark.sql(
        "SELECT * FROM ("
        "  SELECT CAST(1 AS INT) AS i32 UNION ALL SELECT CAST(2 AS INT) AS i32"
        ") t ORDER BY i32"
    )
    assert source.to_arrow().schema.field("i32").type == pa.int32()
    pldf = source.to_polars()
    assert pldf.schema["i32"] == pl.Int32
    assert pldf.select("i32").to_series().to_list() == [1, 2]
    round_trip_df = spark.createDataFrame(pldf)
    assert round_trip_df.to_arrow().schema.field("i32").type == pa.int64()  # widened on Arrow
    round_trip = round_trip_df.to_polars().sort("i32")
    assert round_trip.select("i32").to_series().to_list() == [1, 2]
    assert round_trip.schema["i32"] == pl.Int64  # widened — the divergence


def test_to_polars_round_trip_decimal_precision_widens_divergence(spark: ReparkSession) -> None:
    """Pin for registry row TY-5 — semantics live only there.

    See ``docs/spark-sql-iceberg-parity.md`` §4
    [TY-5](../../../docs/spark-sql-iceberg-parity.md#ty-5--createdataframe-widens-decimal-precision-and-scale).
    Strengthened pin (C1-Q-007): source Arrow type, polars dtype, and round-trip Arrow+polars
    dtypes are all asserted; numeric value identity is independent of order.
    """
    pl = pytest.importorskip("polars")
    source = spark.sql(
        "SELECT * FROM ("
        "  SELECT CAST(12.34 AS DECIMAL(10,2)) AS dec "
        "  UNION ALL SELECT CAST(-1.00 AS DECIMAL(10,2)) AS dec"
        ") t ORDER BY dec"
    )
    assert source.to_arrow().schema.field("dec").type == pa.decimal128(10, 2)
    pldf = source.to_polars()
    assert pldf.schema["dec"] == pl.Decimal(precision=10, scale=2)
    assert sorted(pldf.select("dec").to_series().to_list()) == [
        Decimal("-1.00"),
        Decimal("12.34"),
    ]
    round_trip = spark.createDataFrame(pldf)
    out = round_trip.to_arrow()
    assert out.schema.field("dec").type == pa.decimal128(38, 18)
    # numeric value preserved (scale padded); order-insensitive — VALUES has no ORDER BY
    assert sorted(row["dec"] for row in out.to_pylist()) == [
        Decimal("-1.000000000000000000"),
        Decimal("12.340000000000000000"),
    ]
    again = round_trip.to_polars()
    assert again.schema["dec"] == pl.Decimal(precision=38, scale=18)

"""r24 A3 QUAL-03 — native cast vocabulary locksteps with types.py primitives.

Parametrized over every ``types.py`` class that claims a primitive Arrow mapping
via ``_engine_type`` and is in the facade cast allowlist. Covers ``cast`` and
``try_cast``, string tokens + type objects, and residual unknown → AnalysisException
on the native path (Python allowlist still raises ValueError for hostile tokens —
generator security gate).
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from repark import functions as F  # noqa: N812 — PySpark idiom
from repark.errors import AnalysisException, ParseException
from repark.session import ReparkSession
from repark.types import (
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    TimestampType,
)

# types.py primitives that claim cast via _engine_type + are in the facade allowlist.
_PRIMITIVE_CAST_TYPES: list[tuple[str, DataType, pa.DataType]] = [
    ("string", StringType(), pa.string()),
    ("boolean", BooleanType(), pa.bool_()),
    ("byte", ByteType(), pa.int8()),
    ("short", ShortType(), pa.int16()),
    ("int", IntegerType(), pa.int32()),
    ("long", LongType(), pa.int64()),
    ("float", FloatType(), pa.float32()),
    ("double", DoubleType(), pa.float64()),
    ("date", DateType(), pa.date32()),
    ("timestamp", TimestampType(), pa.timestamp("us", tz="UTC")),
    ("binary", BinaryType(), pa.binary()),
    ("decimal(10,4)", DecimalType(10, 4), pa.decimal128(10, 4)),
]

# Alias tokens the facade/native both accept (Spark short forms).
_ALIAS_TOKENS: list[tuple[str, pa.DataType]] = [
    ("tinyint", pa.int8()),
    ("smallint", pa.int16()),
    ("integer", pa.int32()),
    ("bigint", pa.int64()),
]


@pytest.fixture
def spark() -> ReparkSession:
    """Per-test local session (no AWS); plays with autouse active-session isolate."""
    return ReparkSession.builder.master("local[1]").appName("test_a3_cast_vocab").getOrCreate()


@pytest.mark.parametrize(
    ("token", "type_obj", "arrow_type"),
    _PRIMITIVE_CAST_TYPES,
    ids=[row[0] for row in _PRIMITIVE_CAST_TYPES],
)
def test_cast_primitive_type_object_and_token(
    spark: ReparkSession,
    token: str,
    type_obj: DataType,
    arrow_type: pa.DataType,
) -> None:
    """Every types.py primitive that claims cast works as type object and string token."""
    frame = spark.range(1).select(F.col("id").alias("v"))
    via_obj = frame.select(F.col("v").cast(type_obj).alias("c")).to_arrow()
    via_token = frame.select(F.col("v").cast(token).alias("c")).to_arrow()
    assert via_obj.schema.field("c").type == arrow_type, (
        f"cast({type_obj!r}) schema {via_obj.schema.field('c').type} != {arrow_type}"
    )
    assert via_token.schema.field("c").type == arrow_type, (
        f"cast({token!r}) schema {via_token.schema.field('c').type} != {arrow_type}"
    )


@pytest.mark.parametrize(
    ("token", "type_obj", "arrow_type"),
    _PRIMITIVE_CAST_TYPES,
    ids=[f"try_{row[0]}" for row in _PRIMITIVE_CAST_TYPES],
)
def test_try_cast_primitive_type_object_and_token(
    spark: ReparkSession,
    token: str,
    type_obj: DataType,
    arrow_type: pa.DataType,
) -> None:
    """try_cast accepts the same vocabulary; schema matches cast."""
    frame = spark.range(1).select(F.col("id").alias("v"))
    via_obj = frame.select(F.col("v").try_cast(type_obj).alias("c")).to_arrow()
    via_token = frame.select(F.col("v").try_cast(token).alias("c")).to_arrow()
    assert via_obj.schema.field("c").type == arrow_type
    assert via_token.schema.field("c").type == arrow_type


@pytest.mark.parametrize(("token", "arrow_type"), _ALIAS_TOKENS, ids=[t for t, _ in _ALIAS_TOKENS])
def test_cast_alias_tokens(spark: ReparkSession, token: str, arrow_type: pa.DataType) -> None:
    """Spark short-form aliases (tinyint/smallint/integer/bigint) reach native."""
    frame = spark.range(1)
    table = frame.select(F.col("id").cast(token).alias("c")).to_arrow()
    assert table.schema.field("c").type == arrow_type


def test_cast_unknown_type_raises_parse_exception_at_facade_allowlist(
    spark: ReparkSession,
) -> None:
    """Hostile / unknown tokens refuse at the Python allowlist as ``ParseException``.

    The allowlist is the security control (generator unnest gate C4-SEC-001); the
    exception *class* is Spark parity. Live PySpark 4.1.2 oracle, recorded r24 morning::

        col.cast("notatype")  -> ParseException  AnalysisException=True ValueError=False
        col.cast("varchar")   -> ParseException  AnalysisException=True ValueError=False

    ``ParseException`` subclasses ``AnalysisException``, so the Spark idiom
    ``except AnalysisException`` catches a bad cast on repark too — a bare ``ValueError``
    (the pre-rider behavior) would not be. Renamed with its assertion per rule 11.
    """
    for bad in ("notatype", "varchar"):
        with pytest.raises(ParseException, match=r"unknown cast type"):
            _ = F.col("id").cast(bad)
        # The parity idiom users actually write.
        with pytest.raises(AnalysisException, match=r"unknown cast type"):
            _ = F.col("id").cast(bad)
    with pytest.raises(ParseException, match=r"unknown cast type"):
        _ = F.col("id").try_cast("varchar")
    # Refusal must NOT be a bare ValueError — that is what diverged from Spark.
    assert not issubclass(ParseException, ValueError)


def test_native_parse_residual_is_analysis_exception(spark: ReparkSession) -> None:
    """Native ``PyColumn.cast`` residual is AnalysisException, not ValueError (QUAL-03).

    Bypass the facade allowlist by calling the native handle with an unknown token.
    """
    col = F.col("id")
    with pytest.raises(AnalysisException, match=r"unknown cast type"):
        _ = col._inner.cast("notatype")  # type: ignore[attr-defined]
    with pytest.raises(AnalysisException, match=r"unknown cast type"):
        _ = col._inner.try_cast("varchar")  # type: ignore[attr-defined]


def test_float_byte_short_binary_roundtrip_values(spark: ReparkSession) -> None:
    """The four previously-broken tokens produce correct values (not only schemas)."""
    frame = spark.createDataFrame([(1, "ab")], ["n", "s"])
    out = frame.select(
        F.col("n").cast("float").alias("f"),
        F.col("n").cast("byte").alias("b"),
        F.col("n").cast("short").alias("sh"),
        F.col("s").cast(BinaryType()).alias("bin"),
    ).to_arrow()
    rows: list[dict[str, Any]] = out.to_pylist()
    assert rows[0]["f"] == pytest.approx(1.0)
    assert rows[0]["b"] == 1
    assert rows[0]["sh"] == 1
    assert rows[0]["bin"] == b"ab"
    assert out.schema.field("f").type == pa.float32()
    assert out.schema.field("b").type == pa.int8()
    assert out.schema.field("sh").type == pa.int16()
    assert out.schema.field("bin").type == pa.binary()


def test_try_cast_width_overflow_yields_null(spark: ReparkSession) -> None:
    """try_cast to narrow integers nulls on overflow; cast stays fail-loud (octo C4-Q-001)."""
    frame = spark.createDataFrame([(200, 40_000)], ["b_src", "s_src"])
    tried = frame.select(
        F.col("b_src").try_cast("byte").alias("b"),
        F.col("s_src").try_cast("short").alias("s"),
    ).to_arrow()
    rows = tried.to_pylist()
    assert rows[0]["b"] is None
    assert rows[0]["s"] is None
    # Strict cast must not silently wrap (Arrow cast error → engine exception).
    with pytest.raises(Exception, match=r"Cast error|Int8|Int16"):
        _ = frame.select(F.col("b_src").cast("byte")).to_arrow()

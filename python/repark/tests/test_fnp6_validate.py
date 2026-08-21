"""FNP-6c — ``validate_utf8`` / ``try_validate_utf8`` / ``assert_true``.

All three share one shape: inspect a value, hand it back when acceptable, and fail loudly or
yield NULL when not. None of them computes anything.

**The structural note that matters for the UTF-8 pair.** An Arrow ``Utf8`` array cannot hold
invalid UTF-8 — Rust's ``&str`` forbids it — so on a string column these are tautologies. The case
that can actually fail is **binary**, and that is how ``datafusion-spark``'s ``is_valid_utf8``
already behaves. Spark's own strings are ``UTF8String`` byte arrays that *can* carry invalid
sequences, so a Spark program can reach these on a STRING column where repark cannot. That is a
difference in value representation, not a behaviour choice, and these rows exercise the binary
path where the two engines genuinely agree.

Ledger: ``task/fnp-6c-validate-ledger.md``.
"""

from __future__ import annotations

import pytest

from repark.errors import PySparkException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("fnp6-validate").getOrCreate()


def _bytes_frame():
    # X'616263' is "abc"; X'61FF62' has a lone 0xFF, which is not valid UTF-8 anywhere.
    return _session().sql("SELECT X'616263' AS good, X'61FF62' AS bad")


def test_try_validate_utf8_decodes_valid_bytes() -> None:
    got = (
        _bytes_frame()
        .select(F.try_validate_utf8("good").alias("r"))
        .toArrow()
        .column("r")
        .to_pylist()
    )
    assert got == ["abc"]


def test_try_validate_utf8_yields_null_on_invalid_bytes() -> None:
    """NULL rather than an error is the whole difference from ``validate_utf8``."""
    got = (
        _bytes_frame()
        .select(F.try_validate_utf8("bad").alias("r"))
        .toArrow()
        .column("r")
        .to_pylist()
    )
    assert got == [None]


def test_validate_utf8_passes_valid_bytes_through() -> None:
    got = (
        _bytes_frame().select(F.validate_utf8("good").alias("r")).toArrow().column("r").to_pylist()
    )
    assert got == ["abc"]


def test_validate_utf8_raises_on_invalid_bytes_with_sparks_error_class() -> None:
    with pytest.raises(PySparkException, match="INVALID_UTF8_STRING"):
        _bytes_frame().select(F.validate_utf8("bad").alias("r")).toArrow()


def test_utf8_pair_agrees_across_doors() -> None:
    frame = _bytes_frame()
    frame.createOrReplaceTempView("fnp6c_v")
    spark = _session()

    facade = frame.select(F.try_validate_utf8("bad").alias("r")).toArrow()
    door = spark.sql("SELECT try_validate_utf8(bad) AS r FROM fnp6c_v").toArrow()
    assert facade.column("r").to_pylist() == door.column("r").to_pylist()
    assert facade.schema.field("r").type == door.schema.field("r").type


def test_assert_true_returns_null_when_the_condition_holds() -> None:
    table = _session().range(1).select(F.assert_true(F.lit(True)).alias("r")).toArrow()
    assert table.column("r").to_pylist() == [None]
    assert str(table.schema.field("r").type) == "null"


def test_assert_true_raises_on_false() -> None:
    with pytest.raises(PySparkException, match="assert_true"):
        _session().range(1).select(F.assert_true(F.lit(False)).alias("r")).toArrow()


def test_assert_true_raises_on_null_because_null_is_not_true() -> None:
    """Spark raises on a NULL condition too — only ``true`` passes, and NULL is not true."""
    with pytest.raises(PySparkException, match="assert_true"):
        (
            _session()
            .range(1)
            .select(F.assert_true(F.lit(None).cast("boolean")).alias("r"))
            .toArrow()
        )


def test_assert_true_uses_the_caller_supplied_message() -> None:
    with pytest.raises(PySparkException, match="rows must be positive"):
        (
            _session()
            .range(1)
            .select(F.assert_true(F.lit(False), "rows must be positive").alias("r"))
            .toArrow()
        )

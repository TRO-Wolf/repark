"""SQP-1 facade controls — Spark string-literal escapes and ``CAST … AS BINARY``.

Live PySpark 4.1.2 oracle (``<pyspark-4.1.2-oracle>``); the charter ledger
``task/ledgers/staging/sqp-1-spark-string-literals-ledger.md`` holds the transcript.

The facade is a CONTROL for this unit: a Python string carries no SQL-lexer escapes, so
``F.lit(r"\\d")`` was already the regex ``\\d`` before the fix and stays so. What the fix changes
is the SQL door: ``spark.sql("… '\\\\d' …")`` now reaches the engine as ``\\d`` too, so the two
doors AGREE. The ``.cast("binary")`` facade path is the equality control for the SQL BINARY cast.

pins: sqp-1-spark-string-literals/C-007
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


@pytest.fixture
def spark() -> ReparkSession:
    """A facade session for the SQP-1 controls."""
    session = ReparkSession.builder.appName("pytest-sqp-1").getOrCreate()
    yield session
    session.stop()


def _table(frame: object) -> pa.Table:
    """Collect a frame on the Arrow path (value AND type)."""
    return frame.to_arrow()  # type: ignore[attr-defined]


def test_facade_regexp_count_is_unchanged_and_matches_the_sql_door(spark: ReparkSession) -> None:
    """The facade control holds at 1, and the SQL door now equals it.

    ``F.lit(r"\\d")`` is the regex ``\\d`` — a Python string, never touched by the SQL lexer — so
    ``regexp_count("a1", r"\\d")`` is 1 before and after. ``spark.sql`` with ``'\\\\d'`` now
    reaches the engine as ``\\d`` and returns the same 1, where before the fix it was ``\\\\d``
    and returned 0.
    """
    facade = _table(
        spark.range(1).select(
            F.regexp_count(F.lit("a1"), F.lit(r"\d")).alias("c"),
        )
    )
    assert facade.column("c").to_pylist() == [1]

    sql = _table(spark.sql(r"SELECT regexp_count('a1', '\\d') AS c"))
    assert sql.column("c").to_pylist() == [1], "the SQL door now agrees with the facade"

    # And the raw ``'\d'`` spelling reaches the engine as ``d`` (no digit in 'a1'), where before
    # the fix it was the two chars ``\d`` — the changed-answer direction.
    raw = _table(spark.sql(r"SELECT regexp_count('a1', '\d') AS c"))
    assert raw.column("c").to_pylist() == [0]


def test_facade_cast_binary_equals_the_sql_cast(spark: ReparkSession) -> None:
    """``F.lit("abc").cast("binary")`` equals ``CAST('abc' AS BINARY)`` in value AND Arrow type."""
    facade = _table(spark.range(1).select(F.lit("abc").cast("binary").alias("b")))
    sql = _table(spark.sql("SELECT CAST('abc' AS BINARY) AS b"))

    assert facade.column("b").to_pylist() == [b"abc"]
    assert sql.column("b").to_pylist() == [b"abc"]
    assert facade.schema.field("b").type == pa.binary()
    assert sql.schema.field("b").type == pa.binary()
    assert facade.schema.field("b").type == sql.schema.field("b").type


def test_sql_door_escape_reaches_the_binary_cast(spark: ReparkSession) -> None:
    """B15: the SQL escape composes with the cast — ``hex(CAST('\\t' AS BINARY))`` is ``09``."""
    table = _table(spark.sql(r"SELECT hex(CAST('\t' AS BINARY)) AS h"))
    assert table.column("h").to_pylist() == ["09"]


def test_double_quoted_literal_is_an_identifier(spark: ReparkSession) -> None:
    """BL-9 (registry §7). ``"abc"`` is a double-quoted identifier, not a STRING — reds when the
    FNP-4b fix makes it a Spark STRING literal."""
    with pytest.raises((AnalysisException, PySparkException), match=r"abc"):
        spark.sql('SELECT "abc" AS s').to_arrow()


def test_escaped_string_literals_flag_has_no_carrier(spark: ReparkSession) -> None:
    """BL-10 (registry §7). There is no carrier for ``escapedStringLiterals=true``; the door
    always processes escapes (the ``false`` behaviour), so ``'\\d'`` is ``d`` — reds when a
    carrier lands and the ``true`` mode keeps the backslash."""
    table = _table(spark.sql(r"SELECT '\d' AS s, length('\d') AS n"))
    assert table.column("s").to_pylist() == ["d"]
    assert table.column("n").to_pylist() == [1]


def test_numeric_to_binary_refuses(spark: ReparkSession) -> None:
    """BL-11 (registry §7). ``CAST(1 AS BINARY)`` refuses in every mode; repark has no ANSI-off
    big-endian encoding path — reds when that path lands."""
    with pytest.raises((AnalysisException, PySparkException), match=r"DATATYPE_MISMATCH"):
        spark.sql("SELECT CAST(1 AS BINARY) AS b").to_arrow()

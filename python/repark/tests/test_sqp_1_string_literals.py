"""SQP-1 facade controls — Spark string-literal escapes and ``CAST … AS BINARY``.

Live PySpark 4.1.2 oracle (``<pyspark-4.1.2-oracle>``); the charter ledger
``task/ledgers/staging/sqp-1-spark-string-literals-ledger.md`` holds the transcript.

The facade was a CONTROL for this unit's first cycle: a Python string carries no SQL-lexer escapes,
so ``F.lit(r"\\d")`` was already the regex ``\\d`` before the fix and stays so. What that cycle
changed is the SQL door: ``spark.sql("… '\\\\d' …")`` now reaches the engine as ``\\d`` too, so the
two doors AGREE. ``.cast("binary")`` is the equality control for the SQL BINARY cast.

**Cycle-2 (C-013) makes the facade a CHANGE, not only a control.** The Spark door's front door
Spark-unescapes every statement entering it — facade-generated SQL included — so a facade embed of
a value carrying a backslash (or a leading apostrophe) is only correct if it is spelled the way a
Spark user would, through the one helper ``repark.spark._idents.sql_string_literal``. The cycle-2
pins below carry such values through the enumerated embed paths.

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


# ---------------------------------------------------------------------------
# SQP-1 cycle-2 (C-013). The facade embeds every data value as a Spark-canonical
# literal through one helper (`repark.spark._idents.sql_string_literal`). Each
# pin carries a backslash — or a leading apostrophe — in a Python value: RED on
# 37b84b0, where the raw quote-doubled embed let the Spark door escape-process
# the backslash (a silent wrong value) or crash on the apostrophe (BigQuery's
# triple-quote lexer); GREEN once the value is spelled the way a Spark user
# would.
#
# pins: sqp-1-spark-string-literals/C-013
# ---------------------------------------------------------------------------

_BACKSLASH = "p\\q"  # the Python string p\q — one literal backslash, NOT an escape


def test_sql_literal_renders_a_backslash_as_a_spark_literal() -> None:
    """C-013: the VALUES-based ``createDataFrame`` cell renderer (``session._funcs._sql_literal``)
    spells a backslash value the Spark-canonical way — ``'p\\q'`` doubled to ``'p\\\\q'`` so the
    Spark door folds it back to ``p\\q``, not the escape-processed ``pq``. White-box because the
    shipped ``createDataFrame`` builds normal data through Arrow, not this VALUES SQL path; the
    renderer is still a live embed site that must route through the one helper. Reds if the helper
    stops doubling backslashes or this site stops calling it."""
    from repark.spark.session._funcs import _sql_literal

    assert _sql_literal(_BACKSLASH) == "'p\\\\q'"


def test_lit_backslash_survives_the_aggregate_embed(spark: ReparkSession) -> None:
    """C-013: ``F.lit('p\\q')`` mixed with an aggregate takes the ``_lit_sql_expr`` embed path; the
    backslash survives as Spark's ``p\\q``, not ``pq``."""
    table = _table(
        spark.range(3).select(F.lit(_BACKSLASH).alias("l"), F.count(F.lit(1)).alias("n"))
    )
    assert table.column("l").to_pylist() == [_BACKSLASH]
    assert table.column("n").to_pylist() == [3]


def test_unpivot_backslash_column_value(spark: ReparkSession) -> None:
    """C-013: ``unpivot`` embeds each source column NAME as a literal (``_sql_string_literal``); a
    column named with a backslash surfaces in the ``variable`` column verbatim, not
    escape-processed."""
    frame = spark.createDataFrame([(1, 10, 20)], ["id", "a\\b", "c"])
    rows = frame.unpivot("id", ["a\\b", "c"], "variable", "value").to_arrow().to_pylist()
    variables = {row["variable"] for row in rows}
    assert variables == {"a\\b", "c"}


def test_stop_words_remover_backslash_and_apostrophe(spark: ReparkSession) -> None:
    """C-013 (+ C1-F2): ``StopWordsRemover`` embeds each stop word as a literal. A backslash stop
    word matches and is removed (RED before — the door folded ``\\b`` to a backspace, so it matched
    nothing); a stop word beginning with an apostrophe does not crash (RED before — the door lexed
    ``'''tis'`` as an unterminated triple-quoted string)."""
    from repark.spark.ml.feature import StopWordsRemover

    frame = spark.createDataFrame([(["a\\b", "keep", "'tis"],)], ["words"])
    remover = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords=["a\\b", "'tis"])
    out = remover.transform(frame).collect()
    assert list(out[0].asDict()["filtered"]) == ["keep"]


def test_string_indexer_round_trips_a_backslash_label(spark: ReparkSession) -> None:
    """C-013: ``StringIndexer`` / ``IndexToString`` embed each label as a literal; a label with a
    backslash round-trips (RED before — the label ``a\\d`` reached the engine as ``ad``)."""
    from repark.spark.ml.feature import IndexToString, StringIndexer

    frame = spark.createDataFrame([("a\\d",), ("b",)], ["cat"])
    model = StringIndexer(inputCol="cat", outputCol="idx").fit(frame)
    indexed = model.transform(frame)
    restored = IndexToString(inputCol="idx", outputCol="orig", labels=model.labels).transform(
        indexed
    )
    origs = set(restored.select("orig").to_arrow().column("orig").to_pylist())
    assert "a\\d" in origs


def test_out_of_range_unicode_escape_is_one_replacement(spark: ReparkSession) -> None:
    r"""BL-12 (registry §7). An out-of-range ``\U`` (past U+10FFFF) becomes a SINGLE ``?`` here,
    where Spark's Java UTF-8 encoder emits two (``length('\U00110000')`` = 2, ``3F3F``). Reds when
    repark reproduces the 2-char Java artifact.

    pins: sqp-1-spark-string-literals/C-011
    """
    table = _table(spark.sql(r"SELECT '\U00110000' AS v, length('\U00110000') AS n"))
    assert table.column("v").to_pylist() == ["?"]
    assert table.column("n").to_pylist() == [1]

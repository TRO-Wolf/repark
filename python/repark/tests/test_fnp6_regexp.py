"""FNP-6a — ``regexp_extract_all`` and ``regexp_substr``, over machinery repark already wrote.

These are the first NEW kernels of the campaign: FNP-1/2/3/5 moved 34 names without writing one,
because the engine already had them. It does not have these.

What made them cheap is that the hard part was paid for earlier. ``spark_regexp.rs`` already
implements Java's ``Matcher.find()`` stepping — an empty match is reported where a previous
non-empty match ended, and empty matches advance by a UTF-16 unit — plus the ASCII binding for
Java's ``\\d``/``\\w``/``\\s`` (the ``regex`` crate's are Unicode). Both kernels reuse that walk
rather than re-deriving it.

**Two conventions Spark deliberately keeps apart**, and so do these:

* ``regexp_extract_all`` returns an EMPTY ARRAY when nothing matches. NULL means a NULL input.
* ``regexp_substr`` returns NULL when nothing matches — unlike ``regexp_extract``, whose
  empty-string convention cannot tell "matched empty" from "did not match".

**Oracle.** Python's ``re`` module, not repark's own output. The patterns here are ones where
Python and Java agree, so ``re`` is a fair judge; the Java-specific divergences (empty-match
stepping, ASCII classes) are already pinned by ``regexp_count``'s own tests.

Ledger: ``task/fnp-6a-regexp-ledger.md``.
"""

from __future__ import annotations

import re

import pytest

from repark.spark import functions as F  # noqa: N812 — PySpark idiom

PAIRS = r"(\d+)-(\d+)"


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("fnp6-regexp").getOrCreate()


def _frame():
    return _session().createDataFrame([("100-200, 300-400",), ("nope",), (None,)], "s string")


@pytest.mark.parametrize("group", [0, 1, 2])
def test_extract_all_matches_the_re_oracle(group: int) -> None:
    """Every match's group, checked against Python's ``re`` rather than against ourselves."""
    got = (
        _frame()
        .select(F.regexp_extract_all("s", F.lit(PAIRS), group).alias("r"))
        .toArrow()
        .column("r")
        .to_pylist()
    )
    expected = [
        [match.group(group) for match in re.finditer(PAIRS, "100-200, 300-400")],
        [match.group(group) for match in re.finditer(PAIRS, "nope")],
        None,
    ]
    assert got == expected


def test_extract_all_distinguishes_no_match_from_null_input() -> None:
    """Empty array and NULL mean different things, and the kernel must not conflate them."""
    got = (
        _frame()
        .select(F.regexp_extract_all("s", F.lit(PAIRS)).alias("r"))
        .toArrow()
        .column("r")
        .to_pylist()
    )
    assert got[1] == [], "no match must be an empty array"
    assert got[2] is None, "a NULL input must stay NULL"


def test_extract_all_returns_an_array_of_strings() -> None:
    table = _frame().select(F.regexp_extract_all("s", F.lit(PAIRS)).alias("r")).toArrow()
    assert str(table.schema.field("r").type) == "list<item: string>"


def test_substr_returns_null_on_no_match_not_empty_string() -> None:
    """The distinction from ``regexp_extract``, which returns '' — pinned so it cannot drift."""
    got = (
        _frame()
        .select(F.regexp_substr("s", F.lit(r"\d+")).alias("r"))
        .toArrow()
        .column("r")
        .to_pylist()
    )
    assert got == ["100", None, None]
    # Was `is not ""` (F632-suppressed): an identity test that only caught the empty string by the
    # accident of interning, and would have read as passing for any other falsy value. SEM-6.
    assert got[1] is None, "no match is NULL for regexp_substr, not an empty string"


def test_substr_matches_the_re_oracle() -> None:
    frame = _session().createDataFrame([("café x9",), ("abc",)], "s string")
    got = (
        frame.select(F.regexp_substr("s", F.lit(r"[0-9]+")).alias("r"))
        .toArrow()
        .column("r")
        .to_pylist()
    )
    expected = [
        (re.search(r"[0-9]+", "café x9") or [None]) and re.search(r"[0-9]+", "café x9").group(0),
        None,
    ]
    assert got == expected


def test_both_agree_with_the_sql_door() -> None:
    """C-012: a new kernel is registered AND dispatched, so both doors reach the same one."""
    spark = _session()
    frame = _frame()
    frame.createOrReplaceTempView("fnp6_v")

    for facade_column, sql in [
        (F.regexp_extract_all("s", F.lit(PAIRS)), f"regexp_extract_all(s, '{PAIRS}')"),
        # `[0-9]+` rather than `\d+`: SQL-literal backslash handling is a separate open
        # residual (STATUS "Known correctness issues"), and this row is about door agreement.
        (F.regexp_substr("s", F.lit("[0-9]+")), "regexp_substr(s, '[0-9]+')"),
    ]:
        facade = frame.select(facade_column.alias("r")).toArrow()
        door = spark.sql(f"SELECT {sql} AS r FROM fnp6_v").toArrow()
        assert facade.column("r").to_pylist() == door.column("r").to_pylist(), sql
        assert facade.schema.field("r").type == door.schema.field("r").type, sql


def test_extract_all_reuses_the_java_matcher_stepping() -> None:
    """The walk is shared with ``regexp_count``, so an empty-matching pattern agrees with it.

    ``[0-9]*`` on ``2026`` matches at every position plus the end — Java's stepping, which the
    ``regex`` crate's ``find_iter`` does not reproduce. Counting and collecting must not disagree.

    ``idx=0`` is named explicitly (SEM-1, 2026-08-21). ``[0-9]*`` has no capture group, and the
    two-argument default is now Spark's group 1, which RAISES on such a pattern — this test is
    about the stepping walk, not about the group default, so it asks for the whole match.
    """
    frame = _session().createDataFrame([("2026",)], "s string")
    out = frame.select(
        F.regexp_count("s", F.lit("[0-9]*")).alias("n"),
        F.size(F.regexp_extract_all("s", F.lit("[0-9]*"), 0)).alias("collected"),
    ).toArrow()
    assert out.column("n").to_pylist() == out.column("collected").to_pylist()

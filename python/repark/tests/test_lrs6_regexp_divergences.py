"""LRS-6 — the regexp divergences this campaign measured but did not close.

Both are BACKLOG registry rows (``RE-1``, ``RE-2``), and both pins **codify today's behavior** so
the unit that fixes each one turns its pin red on purpose — the registry's own rule for a BACKLOG
row (``docs/spark-sql-iceberg-parity.md`` §7).

Every Spark value below came from a live PySpark 4.1.2 (design §7).

Ledger: ``task/lrs-6-regexp-measured-ledger.md``.
"""

from __future__ import annotations

from repark.spark import functions as F  # noqa: N812 — PySpark idiom

ASTRAL = "\U0001f389ab"


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("lrs6-regexp").getOrCreate()


def _sql(text: str):
    return _session().sql(text).collect()[0][0]


def test_re1_extract_all_two_argument_form_returns_group_zero() -> None:
    """**Spark returns ``['a', 'b']``.** repark returns the whole match on both doors.

    This is the highest-value row the sweep found: a silently wrong answer on ordinary input, not
    an edge case. It is not fixed here because the campaign's invariant is that no working query
    changes its result — changing it is a deliberate decision, taken with the three-argument form
    and ``regexp_substr`` checked in the same change.
    """
    assert _sql("SELECT regexp_extract_all('a1b2', '([a-z])([0-9])') AS r") == ["a1", "b2"]
    facade = (
        _session()
        .range(1)
        .select(F.regexp_extract_all(F.lit("a1b2"), F.lit("([a-z])([0-9])")).alias("r"))
        .collect()[0][0]
    )
    assert facade == ["a1", "b2"], "the facade and the door agree with each other, not with Spark"


def test_re1_the_explicit_group_index_already_agrees_with_spark() -> None:
    """Only the DEFAULT diverges. Naming the index gives Spark's answer on both sides, which is
    what makes the fix a one-line change rather than a rewrite — and what makes it a decision
    rather than a bug hunt.
    """
    assert _sql("SELECT regexp_extract_all('a1b2', '([a-z])([0-9])', 1) AS r") == ["a", "b"]
    assert _sql("SELECT regexp_extract_all('a1b2', '([a-z])([0-9])', 0) AS r") == ["a1", "b2"]


def test_re2_zero_width_matches_skip_the_mid_surrogate_position() -> None:
    """**Spark returns 5 for both.** Java's ``Matcher`` finds an empty match at every UTF-16
    code-unit index, including the one INSIDE a surrogate pair.

    repark's ``regexp_count`` walks UTF-16 and is already right; the collector walks Unicode
    scalars, because a mid-surrogate offset is not a byte boundary and Rust's ``&str`` cannot
    address one. The two disagreeing is the measured fact this pin holds.
    """
    assert _sql(f"SELECT regexp_count('{ASTRAL}', '') AS r") == 5
    assert _sql(f"SELECT regexp_extract_all('{ASTRAL}', '', 0) AS r") == ["", "", "", ""]
    assert _sql(f"SELECT regexp_extract_all('{ASTRAL}', 'b*', 0) AS r") == ["", "", "b", ""]


def test_re2_substr_of_an_empty_pattern_on_astral_text_is_empty_not_null() -> None:
    """Spark returns NULL here; repark returns the empty string. Same root cause as above."""
    assert _sql(f"SELECT regexp_substr('{ASTRAL}', '') AS r") == ""


def test_bmp_text_already_agrees_with_spark_everywhere() -> None:
    """The bound on RE-2: both divergences are confined to supplementary-plane text. On BMP input
    every one of these matches Spark exactly, which is why the row is narrow rather than a general
    statement that repark's regex engine differs.
    """
    assert _sql("SELECT regexp_count('ab', '') AS r") == 3
    assert _sql("SELECT regexp_extract_all('ab', '', 0) AS r") == ["", "", ""]
    assert _sql("SELECT regexp_extract_all('ab', 'b*', 0) AS r") == ["", "b", ""]

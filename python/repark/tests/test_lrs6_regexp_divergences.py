"""LRS-6 — the regexp divergences this campaign measured but did not close.

**RE-1 is closed (SEM-1, 2026-08-21)** and its two pins left this file with it: the two-argument
``regexp_extract_all`` now defaults to capture group 1, and
``test_sem1_extract_all_group_default.py`` owns those assertions. It went exactly as this file's
contract promised — the pin was red on purpose the moment the default moved.

What remains here is ``RE-2``, a BACKLOG registry row whose pins **codify today's behavior** so the
unit that fixes it turns them red on purpose — the registry's own rule for a BACKLOG row
(``docs/spark-sql-iceberg-parity.md`` §7).

Every Spark value below came from a live PySpark 4.1.2 (design §7).

Ledger: ``task/lrs-6-regexp-measured-ledger.md``.
"""

from __future__ import annotations

ASTRAL = "\U0001f389ab"


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("lrs6-regexp").getOrCreate()


def _sql(text: str):
    return _session().sql(text).collect()[0][0]


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


def test_re3_substr_of_a_zero_width_match_is_empty_not_null() -> None:
    """**Spark returns NULL for every zero-width match**, on any text; repark returns ``''``.

    Split out of RE-2 on 2026-08-21 (SEM-5). It had been filed under a surrogate-position heading,
    but plain ASCII shows the same difference, so the surrogate framing was wrong — the cause is
    the empty match, not where it sits. The two controls below bound the row: a pattern that truly
    does not match already returns NULL, and a non-empty match is already correct.
    """
    assert _sql("SELECT regexp_substr('ab', '') AS r") == ""
    assert _sql("SELECT regexp_substr('a1b2', '[0-9]*') AS r") == ""
    assert _sql("SELECT regexp_substr('ab', 'b*') AS r") == ""
    assert _sql(f"SELECT regexp_substr('{ASTRAL}', '') AS r") == ""
    # Controls — already Spark's answers, and they are what keep the row narrow.
    assert _sql("SELECT regexp_substr('ab', 'x') AS r") is None
    assert _sql("SELECT regexp_substr('a1b2', '[0-9]+') AS r") == "1"


def test_bmp_counting_and_collecting_already_agree_with_spark() -> None:
    """The bound on RE-2 — and it is narrower than this file first claimed.

    **Corrected 2026-08-21 (SEM-5).** This test's docstring used to read "both divergences are
    confined to supplementary-plane text". That is true of the COUNT, which is what these
    assertions cover, and false of ``regexp_substr``, which returns ``''`` for a zero-width match
    on plain ASCII as well. The substr half is now its own row, RE-3, and the claim here is
    restricted to what it actually tested.
    """
    assert _sql("SELECT regexp_count('ab', '') AS r") == 3
    assert _sql("SELECT regexp_extract_all('ab', '', 0) AS r") == ["", "", ""]
    assert _sql("SELECT regexp_extract_all('ab', 'b*', 0) AS r") == ["", "b", ""]

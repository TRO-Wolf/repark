"""SEM-1 — the two-argument ``regexp_extract_all`` defaults to capture group 1, as Spark does.

**This unit changes what a working query returns.** `regexp_extract_all('a1b2',
'([a-z])([0-9])')` returned `['a1', 'b2']` — the whole match — and now returns `['a', 'b']`.
Registry row `RE-1` recorded that difference and is retired by this commit; the pin that codified
the old answer (`test_lrs6_regexp_divergences.py`) flips here, on purpose.

The default lives in exactly one place — `extract_rows` in
`crates/repark-functions/src/spark_regexp.rs` — and one knob serves both doors, because the facade
passes no default of its own: `functions_expr.regexp_extract_all` omits the third argument entirely
when the caller omits it.

Every expected value is Spark's own answer from a live PySpark 4.1.2, not read back out of repark.
The pattern with no capture group is the case that matters most: Spark's default of 1 makes it an
ERROR, where repark used to return the matches.

Ledger: ``task/sem-1-extract-all-group-default-ledger.md``.
"""

from __future__ import annotations

import pytest

PAIRS = "([a-z])([0-9])"


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("sem1-group-default").getOrCreate()


def _sql(query: str):
    return _session().sql(query).toArrow().column("r").to_pylist()


def test_the_facade_two_argument_form_returns_group_one() -> None:
    """Spark: ``['a', 'b']``. This tree returned ``['a1', 'b2']`` before this unit."""
    from repark.spark import functions as F  # noqa: N812 — PySpark idiom

    got = (
        _session()
        .createDataFrame([("a1b2",)], "s string")
        .select(F.regexp_extract_all("s", F.lit(PAIRS)).alias("r"))
        .toArrow()
        .column("r")
        .to_pylist()
    )
    assert got == [["a", "b"]]


def test_the_sql_door_two_argument_form_returns_group_one() -> None:
    """One knob, both doors — the door is not defaulted separately anywhere."""
    assert _sql(f"SELECT regexp_extract_all('a1b2','{PAIRS}') AS r") == [["a", "b"]]


@pytest.mark.parametrize(
    ("idx", "expected"),
    [(0, ["a1", "b2"]), (1, ["a", "b"]), (2, ["1", "2"])],
)
def test_an_explicit_index_still_means_what_it_meant(idx: int, expected: list[str]) -> None:
    """Only the DEFAULT moved. ``idx=0`` still asks for the whole match and still gets it, which is
    the migration path for anyone who wanted the old two-argument answer.
    """
    assert _sql(f"SELECT regexp_extract_all('a1b2','{PAIRS}', {idx}) AS r") == [expected]


@pytest.mark.parametrize("pattern", ["[0-9]*", "", "b"])
def test_a_pattern_with_no_capture_group_now_raises(pattern: str) -> None:
    """The consequence of Spark's default, and the reason this unit has collateral.

    With a default of 1 and no group 1 to take, Spark raises rather than returning matches:
    ``Expects group index between 0 and 0, but got 1``. Two tests in this repository were written
    against the old default and started failing as RUNTIME ERRORS rather than assertion diffs —
    ``test_fnp6_regexp.py`` and ``test_fnp_critic_remediation.py``; both now pass ``idx=0``
    explicitly, because each is about the stepping walk, not the group default.
    """
    with pytest.raises(Exception) as caught:
        _sql(f"SELECT regexp_extract_all('a1b2','{pattern}') AS r")
    assert "REGEX_GROUP_INDEX" in str(caught.value)
    assert "between 0 and 0, but got 1" in str(caught.value)


def test_regexp_substr_is_untouched_by_the_shared_default() -> None:
    """``regexp_substr`` shares ``extract_rows`` but binds the group as ``_group`` and never reads
    it — it always returns the whole match. Spark agrees at ``'a1'``, before and after.
    """
    assert _sql(f"SELECT regexp_substr('a1b2','{PAIRS}') AS r") == ["a1"]
    assert _sql("SELECT regexp_substr('a1b2','[a-z]([0-9])') AS r") == ["a1"]


def test_a_single_group_pattern_takes_that_group() -> None:
    """Spark: ``['1', '2']`` — the one group, not the whole match."""
    assert _sql("SELECT regexp_extract_all('a1b2','[a-z]([0-9])') AS r") == [["1", "2"]]

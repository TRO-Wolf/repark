"""SEM-1 — the two-argument ``regexp_extract_all`` defaults to capture group 1, as Spark does.

One knob serves both doors: the default lives in ``extract_rows`` in
``crates/repark-functions/src/spark_regexp.rs``, and the facade passes no default of its own.
Every expected value is Spark's own answer from live PySpark 4.1.2, not read back out of repark.
With a pattern that has no capture group, Spark's default of 1 makes the call raise.
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
    """pins: sem-1-spark-answer-parity/C-002

    Spark: ``['a', 'b']``.
    """
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
    """pins: sem-1-spark-answer-parity/C-002

    One knob, both doors — the door is not defaulted separately anywhere.
    """
    assert _sql(f"SELECT regexp_extract_all('a1b2','{PAIRS}') AS r") == [["a", "b"]]


@pytest.mark.parametrize(
    ("idx", "expected"),
    [(0, ["a1", "b2"]), (1, ["a", "b"]), (2, ["1", "2"])],
)
def test_an_explicit_index_still_means_what_it_meant(idx: int, expected: list[str]) -> None:
    """pins: sem-1-spark-answer-parity/C-002

    Only the default moved: ``idx=0`` still asks for the whole match.
    """
    assert _sql(f"SELECT regexp_extract_all('a1b2','{PAIRS}', {idx}) AS r") == [expected]


@pytest.mark.parametrize("pattern", ["[0-9]*", "", "b"])
def test_a_pattern_with_no_capture_group_now_raises(pattern: str) -> None:
    """pins: sem-1-spark-answer-parity/C-002, C-003

    With a default of 1 and no group 1 to take, Spark raises rather than returning matches:
    ``Expects group index between 0 and 0, but got 1``.
    """
    with pytest.raises(Exception) as caught:
        _sql(f"SELECT regexp_extract_all('a1b2','{pattern}') AS r")
    assert "REGEX_GROUP_INDEX" in str(caught.value)
    assert "between 0 and 0, but got 1" in str(caught.value)


def test_regexp_substr_is_untouched_by_the_shared_default() -> None:
    """pins: sem-1-spark-answer-parity/C-002

    ``regexp_substr`` shares ``extract_rows`` but never reads the group — it always returns
    the whole match (Spark agrees at ``'a1'``).
    """
    assert _sql(f"SELECT regexp_substr('a1b2','{PAIRS}') AS r") == ["a1"]
    assert _sql("SELECT regexp_substr('a1b2','[a-z]([0-9])') AS r") == ["a1"]


def test_a_single_group_pattern_takes_that_group() -> None:
    """Spark: ``['1', '2']`` — the one group, not the whole match."""
    assert _sql("SELECT regexp_extract_all('a1b2','[a-z]([0-9])') AS r") == [["1", "2"]]


def test_null_inputs_propagate() -> None:
    """pins: sem-1-spark-answer-parity/C-002, C-010"""
    assert _sql("SELECT regexp_extract_all(CAST(NULL AS STRING), '([a-z])') AS r") == [None]
    assert _sql("SELECT regexp_extract_all('a1b2', CAST(NULL AS STRING)) AS r") == [None]
    assert _sql(f"SELECT regexp_extract_all('a1b2', '{PAIRS}', CAST(NULL AS INT)) AS r") == [None]

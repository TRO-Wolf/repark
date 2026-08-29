"""LRS-6 — RE-2 divergence pins, measured but not closed (BACKLOG row).

RE-2's pins codify today's behavior; the unit that fixes it turns them red on purpose — the
registry's rule for a BACKLOG row (``docs/spark-sql-iceberg-parity.md`` §7). Every Spark value
below came from a live PySpark 4.1.2.
"""

from __future__ import annotations

ASTRAL = "\U0001f389ab"


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("lrs6-regexp").getOrCreate()


def _sql(text: str):
    return _session().sql(text).collect()[0][0]


def test_re2_zero_width_matches_skip_the_mid_surrogate_position() -> None:
    """Spark returns 5 for both: Java's ``Matcher`` finds an empty match at every UTF-16
    code-unit index, including inside a surrogate pair. repark's ``regexp_count`` walks UTF-16
    and is right; the collector walks Unicode scalars because Rust's ``&str`` cannot address a
    mid-surrogate offset.
    """
    assert _sql(f"SELECT regexp_count('{ASTRAL}', '') AS r") == 5
    assert _sql(f"SELECT regexp_extract_all('{ASTRAL}', '', 0) AS r") == ["", "", "", ""]
    assert _sql(f"SELECT regexp_extract_all('{ASTRAL}', 'b*', 0) AS r") == ["", "", "b", ""]


def test_bmp_counting_and_collecting_already_agree_with_spark() -> None:
    """BMP counting and collecting agree with Spark: these zero-width divergences are confined
    to supplementary-plane text.
    """
    assert _sql("SELECT regexp_count('ab', '') AS r") == 3
    assert _sql("SELECT regexp_extract_all('ab', '', 0) AS r") == ["", "", ""]
    assert _sql("SELECT regexp_extract_all('ab', 'b*', 0) AS r") == ["", "b", ""]

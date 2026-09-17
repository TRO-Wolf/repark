"""LRS-6 — RE-2 divergence pins, measured but not closed (BACKLOG row).

RE-2's pins codify today's behavior; the unit that fixes it turns them red on purpose — the
registry's rule for a BACKLOG row (``docs/spark-sql-iceberg-parity.md`` §7). Every Spark value
below came from a live PySpark 4.1.2. JAVA-REGEX-FEATURES-1 round 2 closed RE-2, so its pin
moved to ``test_java_regex_features_1.py``; what stays here is the BMP agreement pin.
"""

from __future__ import annotations


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("lrs6-regexp").getOrCreate()


def _sql(text: str):
    return _session().sql(text).collect()[0][0]


def test_bmp_counting_and_collecting_already_agree_with_spark() -> None:
    """BMP counting and collecting agree with Spark, as does supplementary-plane text since
    RE-2 closed.
    """
    assert _sql("SELECT regexp_count('ab', '') AS r") == 3
    assert _sql("SELECT regexp_extract_all('ab', '', 0) AS r") == ["", "", ""]
    assert _sql("SELECT regexp_extract_all('ab', 'b*', 0) AS r") == ["", "b", ""]

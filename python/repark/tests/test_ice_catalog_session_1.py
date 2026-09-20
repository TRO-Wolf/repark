"""ICE-CATALOG-SESSION-1: catalog and session SQL against the recorded Spark answers."""

from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, UnsupportedOperationException


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-ice-catalog-session-1").getOrCreate()
    session.register_memory_catalog("sc", tmp_path / "sc")
    session.sql("CREATE NAMESPACE sc.ns")
    session.sql("CREATE TABLE sc.ns.t_cache (a INT) USING iceberg")
    session.sql("INSERT INTO sc.ns.t_cache VALUES (1)")
    return session


def test_refresh_table_answers_empty(spark: ReparkSession) -> None:
    """N-7 mechanism: ``REFRESH TABLE`` rebuilds the provider and answers zero rows."""
    answered = spark.sql("REFRESH TABLE sc.ns.t_cache").to_arrow()
    assert answered.num_rows == 0
    assert spark.sql("SELECT * FROM sc.ns.t_cache").to_arrow().num_rows == 1


def test_refresh_missing_table_raises_table_not_found(spark: ReparkSession) -> None:
    """N-7: ``REFRESH TABLE`` on a missing table refuses ``TABLE_OR_VIEW_NOT_FOUND``."""
    with pytest.raises(AnalysisException, match="TABLE_OR_VIEW_NOT_FOUND"):
        spark.sql("REFRESH TABLE sc.ns.nothere").to_arrow()


def test_refresh_without_table_keyword_ok(spark: ReparkSession) -> None:
    """P-6: ``REFRESH`` without ``TABLE`` is accepted."""
    assert spark.sql("REFRESH sc.ns.t_cache").to_arrow().num_rows == 0


def test_cache_table_then_write_then_read_sees_the_write(spark: ReparkSession) -> None:
    """N-8: SQL ``CACHE TABLE`` sets ``isCached`` and a write invalidates it."""
    spark.sql("CACHE TABLE sc.ns.t_cache").to_arrow()
    assert spark.catalog.isCached("sc.ns.t_cache") is True
    spark.sql("INSERT INTO sc.ns.t_cache VALUES (2)").to_arrow()
    assert sorted(
        row["a"] for row in spark.sql("SELECT * FROM sc.ns.t_cache").to_arrow().to_pylist()
    ) == [1, 2]
    assert spark.catalog.isCached("sc.ns.t_cache") is False


def test_uncache_table_clears_is_cached(spark: ReparkSession) -> None:
    """SQL ``UNCACHE TABLE`` releases the entry ``CACHE TABLE`` made."""
    spark.sql("CACHE TABLE sc.ns.t_cache").to_arrow()
    assert spark.catalog.isCached("sc.ns.t_cache") is True
    spark.sql("UNCACHE TABLE sc.ns.t_cache").to_arrow()
    assert spark.catalog.isCached("sc.ns.t_cache") is False


def test_uncache_missing_table_raises_table_not_found(spark: ReparkSession) -> None:
    """C-021 mechanism: ``UNCACHE TABLE`` on a missing table refuses loud."""
    with pytest.raises(AnalysisException, match="TABLE_OR_VIEW_NOT_FOUND"):
        spark.sql("UNCACHE TABLE sc.ns.nothere").to_arrow()


def test_uncache_if_exists_missing_is_ok(spark: ReparkSession) -> None:
    """C-021 mechanism: ``UNCACHE TABLE IF EXISTS`` tolerates a missing table."""
    assert spark.sql("UNCACHE TABLE IF EXISTS sc.ns.nothere").to_arrow().num_rows == 0


def test_cache_as_select_stays_not_implemented(spark: ReparkSession) -> None:
    """P-6: ``CACHE TABLE ... AS SELECT`` keeps the engine's loud refusal."""
    with pytest.raises(UnsupportedOperationException, match="CACHE TABLE"):
        spark.sql("CACHE TABLE sc.ns.t_cache AS SELECT 1").to_arrow()

"""Drop-in no-op / accepted-ignored disclosure surface (WG-4 Clause 2, OTH-010).

PySpark methods repark accepts for source compatibility but does not reproduce Spark's effect:
``SparkSession.builder.master(...)`` (warn-once). ``spark.catalog.clearCache()`` is a real drop of
session cache MemTables (Q11) — not a no-op; see ``test_cache_persist.py``.
``DataFrame.show(vertical=True)`` is implemented (R-PARITY3 ``-RECORD`` layout). This pins each
remaining disclosure decision; the full rationale table lives in
``docs/spark-sql-iceberg-parity.md`` §8. Needs the compiled wheel.
"""

from __future__ import annotations

import logging
import warnings
from pathlib import Path

import pytest

from repark import ReparkSession
from repark import dataframe as dataframe_module
from repark import session as session_module
from repark.spark.catalog import Catalog


@pytest.fixture(autouse=True)
def _rearm_dropin_warnings() -> None:
    """Re-arm the process-once disclosure warnings so warn-once is deterministic per test."""
    session_module._reset_dropin_warnings_for_tests()
    dataframe_module._reset_dropin_warnings_for_tests()
    yield
    session_module._reset_dropin_warnings_for_tests()
    dataframe_module._reset_dropin_warnings_for_tests()


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-dropin").getOrCreate()
    session.register_memory_catalog("cat", tmp_path)
    session.sql("CREATE NAMESPACE cat.ns")
    session.sql("CREATE TABLE cat.ns.t AS SELECT 1 AS id, 'a' AS name")
    return session


def test_clear_cache_is_real_drop_without_warning(spark: ReparkSession) -> None:
    # Q11: clearCache is a REAL drop of session cache MemTables, not a silent no-op. It must
    # NOT warn (migration scripts call it every run). Behavior pins live in test_cache_persist.py.
    docstring = Catalog.clear_cache.__doc__ or ""
    assert "MemTable" in docstring or "cache" in docstring.lower()
    assert "no-op" not in docstring
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert spark.catalog.clearCache() is None


def test_show_vertical_true_no_longer_warns(
    spark: ReparkSession, capsys: pytest.CaptureFixture[str]
) -> None:
    # R-PARITY3: vertical layout is real; no OTH-010 warn on spark style.
    frame = spark.sql("SELECT id, name FROM cat.ns.t")
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        frame.show(vertical=True)
    out = capsys.readouterr().out
    assert "-RECORD 0" in out
    assert "id" in out


def test_show_vertical_false_does_not_warn(spark: ReparkSession) -> None:
    # vertical=False is the horizontal grid repark actually renders — no divergence, no warning.
    frame = spark.sql("SELECT id, name FROM cat.ns.t")
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        frame.show()
        frame.show(vertical=False)


def test_show_does_not_log_row_data_at_info(
    spark: ReparkSession, caplog: pytest.LogCaptureFixture
) -> None:
    # SEC-008: show() prints the table to stdout (PySpark parity) but must NOT leak row data / PII
    # into INFO logs. INFO gets only a row-count breadcrumb; the full render is DEBUG-only.
    frame = spark.sql("SELECT 1 AS id, 'SENTINEL_PII_CELL' AS name")
    with caplog.at_level(logging.DEBUG, logger="repark.spark.dataframe"):
        frame.show()

    info = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
    debug = [r.getMessage() for r in caplog.records if r.levelno == logging.DEBUG]

    assert any("show(1 rows)" in m for m in info), f"missing INFO breadcrumb; got {info!r}"
    assert not any("SENTINEL_PII_CELL" in m for m in info), "row data leaked to INFO (SEC-008)"
    assert any("SENTINEL_PII_CELL" in m for m in debug), "full render should be present at DEBUG"


def test_master_warns_once(tmp_path: Path) -> None:
    # .master(url) is recorded but ignored (single-node); the first call warns so a cluster URL
    # is not silently downgraded (warn-once).
    with pytest.warns(UserWarning, match="single-node"):
        builder = ReparkSession.builder.master("spark://example:7077")

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        builder.master("local[4]")


def test_set_log_level_is_documented_silent_noop(spark: ReparkSession) -> None:
    # spark.sparkContext.setLogLevel is accepted for source compatibility; engine logging is
    # tracing-based. Silent no-op (same OTH-010 class as clearCache — jobs call it every run).
    from repark.spark.session import SparkContext

    docstring = SparkContext.setLogLevel.__doc__ or ""
    assert "tracing" in docstring or "no-op" in docstring.lower() or "Silent" in docstring
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert spark.sparkContext.setLogLevel("WARN") is None


def test_spark_version_discloses_repark_not_spark_release(spark: ReparkSession) -> None:
    # spark.version returns repark-<dist>, not Spark's "4.1.2". Scripts log it; must not parse.
    from repark import __version__

    assert spark.version == f"repark-{__version__}"
    assert "4.1.2" not in spark.version


def test_with_columns_lateral_alias_divergence_disclosed(spark: ReparkSession) -> None:
    """DIVERGENCE: Spark lateral column aliases in withColumns; repark raises on both orders.

    Live PySpark 4.1.2 on ``(a=1, b=2, c=3)``:

    - ``{"x": col("a")+1, "y": col("x")}`` → columns ``[a,b,c,x,y]``, ``x=2, y=2`` (a later
      NEW name resolves an earlier NEW name laterally).
    - The REVERSE dict order ``{"y": col("x"), "x": col("a")+1}`` raises AnalysisException.

    repark has no lateral-alias resolution: BOTH orders raise. Load-bearing: if the forward
    order ever stops raising, repark has (partially) converged and this disclosure plus the
    withColumns docstring must be updated with fresh recordings.
    """
    from repark import functions as F  # noqa: N812 — PySpark idiom
    from repark.errors import AnalysisException

    df = spark.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
    with pytest.raises(AnalysisException):
        df.withColumns({"x": F.col("a") + 1, "y": F.col("x")}).collect()
    with pytest.raises(AnalysisException):
        df.withColumns({"y": F.col("x"), "x": F.col("a") + 1}).collect()

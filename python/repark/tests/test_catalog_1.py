"""CATALOG-1 facade pins: the session catalog is ``spark_catalog``, like Spark.

Every expected text is Spark 4.1.2 + Iceberg 1.11, measured 2026-09-26 (probes under
``target/probe-catalog-1/``). The harness-shaped session configures ``hc`` through the builder
and registers ``sc`` after build, as the scoreboard's RePark leg does.

pins: catalog-1/C-001, C-002
"""

from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark.session import _reset_active_session_for_tests

IN_MEMORY_CATALOG = "org.apache.iceberg.inmemory.InMemoryCatalog"


def _build(tmp_path: Path, pairs: dict[str, str] | None = None) -> ReparkSession:
    """A harness-shaped session: ``hc`` configured at build, ``sc`` registered after."""
    _reset_active_session_for_tests()
    builder = (
        ReparkSession.builder.appName("pytest-catalog-1")
        .config("spark.sql.catalog.hc.type", "hadoop")
        .config("spark.sql.catalog.hc.warehouse", str(tmp_path / "hc"))
    )
    for key, value in (pairs or {}).items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    session.register_memory_catalog("sc", str(tmp_path))
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns")
    session.sql("CREATE NAMESPACE IF NOT EXISTS hc.ns")
    return session


@pytest.fixture()
def spark(tmp_path: Path) -> ReparkSession:
    """The harness-shaped session, stopped after the test."""
    session = _build(tmp_path)
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _rows(spark: ReparkSession, sql: str) -> list[list[object]]:
    """Collect a SQL answer as nested lists."""
    return [list(row) for row in spark.sql(sql).collect()]


def _current(spark: ReparkSession) -> list[list[object]]:
    """``current_catalog()`` and ``current_database()`` as one row."""
    return _rows(spark, "SELECT current_catalog(), current_database()")


def test_current_catalog_cell_is_spark_catalog(spark: ReparkSession) -> None:
    """CAT-CURRENT-CATALOG: ``SELECT current_catalog()`` is ``[["spark_catalog"]]``.

    pins: catalog-1/C-001
    """
    assert _rows(spark, "SELECT current_catalog()") == [["spark_catalog"]]
    assert _current(spark) == [["spark_catalog", "default"]]
    assert spark.catalog.currentCatalog() == "spark_catalog"
    assert spark.catalog.currentDatabase() == "default"


def test_a_session_without_catalog_blocks_starts_in_spark_catalog(tmp_path: Path) -> None:
    """A bare session and a session with one block both start in ``spark_catalog``.

    pins: catalog-1/C-001
    """
    _reset_active_session_for_tests()
    bare = ReparkSession.builder.appName("pytest-catalog-1-bare").getOrCreate()
    try:
        assert _current(bare) == [["spark_catalog", "default"]]
    finally:
        bare.stop()
    single = (
        ReparkSession.builder.appName("pytest-catalog-1-single")
        .config("spark.sql.catalog.only.catalog-impl", IN_MEMORY_CATALOG)
        .config("spark.sql.catalog.only.warehouse", str(tmp_path))
        .getOrCreate()
    )
    try:
        assert _current(single) == [["spark_catalog", "default"]]
        assert single.catalog.currentCatalog() == "spark_catalog"
    finally:
        single.stop()
        _reset_active_session_for_tests()


def test_register_memory_catalog_registers_and_nothing_else(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """A later ``register_memory_catalog`` leaves the current catalog where it is.

    pins: catalog-1/C-001
    """
    spark.register_memory_catalog("later", str(tmp_path / "later"))
    assert _current(spark) == [["spark_catalog", "default"]]
    assert "later" in {catalog.name for catalog in spark.catalog.listCatalogs()}


def test_show_catalogs_lists_the_session_catalog(spark: ReparkSession) -> None:
    """SHOW CATALOGS and listCatalogs list ``hc``, ``sc`` and ``spark_catalog``.

    pins: catalog-1/C-002
    """
    assert _rows(spark, "SHOW CATALOGS") == [["hc"], ["sc"], ["spark_catalog"]]
    assert [c.name for c in spark.catalog.listCatalogs()] == ["hc", "sc", "spark_catalog"]


def test_spark_catalog_is_not_an_alias_of_another_catalog(spark: ReparkSession) -> None:
    """``spark_catalog.ns.t`` names the session catalog, never ``sc``.

    pins: catalog-1/C-002
    """
    spark.sql("CREATE TABLE sc.ns.t0 (id INT) USING iceberg")
    with pytest.raises(AnalysisException):
        spark.sql("SELECT * FROM spark_catalog.ns.t0").collect()
    with pytest.raises(AnalysisException):
        spark.sql("SELECT * FROM ns.t0").collect()
    assert spark.catalog.tableExists("spark_catalog.ns.t0") is False

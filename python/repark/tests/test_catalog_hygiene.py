"""Catalog hygiene fast-follows (#100): bare-session listTables regression pin."""

from __future__ import annotations


def test_bare_session_list_tables_lists_temps_never_raises() -> None:
    """Fast-follow for #100: a fresh session (no catalogs, no `default` schema) must list
    temp views on no-arg listTables() — PySpark's `default` db always exists, so bare
    listTables() never raises SCHEMA_NOT_FOUND. Explicit missing names still raise."""
    import pytest

    from repark import ReparkSession
    from repark.errors import AnalysisException

    session = ReparkSession.builder.appName("bare-listtables").getOrCreate()
    try:
        assert session.catalog.listTables() == []
        session.sql("SELECT 1 AS x").createOrReplaceTempView("tmp_bare_v")
        names = [t.name for t in session.catalog.listTables()]
        assert "tmp_bare_v" in names
        with pytest.raises(AnalysisException, match="SCHEMA_NOT_FOUND"):
            session.catalog.listTables("definitely_missing_db")
    finally:
        session.stop()

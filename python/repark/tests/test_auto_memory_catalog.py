"""R-AUTO-MEMCAT — the bare-session default memory catalog (duckdb ``:memory:`` analogue).

A ``builder.getOrCreate()`` auto-registers a session-scoped in-memory Iceberg catalog under
``spark_catalog`` with the ``default`` namespace seeded, so first-session bare-name flows work
with zero config. Since catalog-1 the session catalog always exists, like Spark's: it registers
beside configured catalog blocks and beside a different ``spark.sql.defaultCatalog``. A user
``spark.sql.catalog.spark_catalog.*`` block takes its place, and
``repark.sql.autoMemoryCatalog=false`` turns it off. The decision is the engine's
(``auto_session_catalog_wanted``). The temp warehouse dies with ``stop()``.

pins: catalog-1/C-002

MUTATION: drop the ``auto_session_catalog_wanted`` call →
``test_bare_session_bare_name_round_trip`` red.
MUTATION: drop the ``create_namespace`` seed → same test red (no ``default`` namespace).
MUTATION: drop the stop() cleanup → ``test_stop_removes_auto_warehouse`` red.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from repark.spark.session import ReparkSession


@pytest.fixture()
def _fresh_session_slot() -> None:
    """Ensure no active session leaks between tests (module uses bare getOrCreate)."""
    import repark.spark.session as session_mod

    if session_mod._active_session is not None:
        session_mod._active_session.stop()
    yield
    if session_mod._active_session is not None:
        session_mod._active_session.stop()


def test_bare_session_bare_name_round_trip(_fresh_session_slot: None) -> None:
    """Bare getOrCreate → saveAsTable/table/DROP TABLE with bare names, zero config."""
    spark = ReparkSession.builder.getOrCreate()
    assert spark.catalog.currentCatalog() == "spark_catalog"
    assert spark.catalog.currentDatabase() == "default"
    assert [c.name for c in spark.catalog.listCatalogs()] == ["spark_catalog"]

    spark.range(5).write.saveAsTable("automemcat_t")
    assert spark.table("automemcat_t").count() == 5
    spark.sql("DROP TABLE automemcat_t")
    spark.stop()


def test_configured_catalogs_sit_beside_the_session_catalog(
    _fresh_session_slot: None, tmp_path: Path
) -> None:
    """A builder-configured catalog registers beside ``spark_catalog``; current stays there."""
    spark = (
        ReparkSession.builder.config("spark.sql.catalog.mine", "memory")
        .config("spark.sql.catalog.mine.warehouse", str(tmp_path))
        .getOrCreate()
    )
    names = [c.name for c in spark.catalog.listCatalogs()]
    assert names == ["mine", "spark_catalog"]
    assert spark.catalog.currentCatalog() == "spark_catalog"
    spark.range(2).write.saveAsTable("beside_t")
    assert spark.catalog.tableExists("spark_catalog.default.beside_t")
    spark.stop()


def test_a_user_session_catalog_block_takes_the_auto_catalog_place(
    _fresh_session_slot: None, tmp_path: Path
) -> None:
    """``spark.sql.catalog.spark_catalog.*`` is the session catalog; no temp warehouse."""
    spark = (
        ReparkSession.builder.config(
            "spark.sql.catalog.spark_catalog.catalog-impl",
            "org.apache.iceberg.inmemory.InMemoryCatalog",
        )
        .config("spark.sql.catalog.spark_catalog.warehouse", str(tmp_path))
        .getOrCreate()
    )
    assert [c.name for c in spark.catalog.listCatalogs()] == ["spark_catalog"]
    assert "auto_catalog_warehouse" not in spark._alive_token
    spark.sql("CREATE NAMESPACE spark_catalog.user_ns")
    spark.sql("CREATE TABLE spark_catalog.user_ns.t (id INT) USING iceberg")
    assert any(tmp_path.rglob("*.metadata.json"))
    spark.stop()


def test_opt_out_knob_suppresses_auto(_fresh_session_slot: None) -> None:
    """``repark.sql.autoMemoryCatalog=false`` restores the empty-catalog bare session."""
    spark = ReparkSession.builder.config("repark.sql.autoMemoryCatalog", "false").getOrCreate()
    assert [c.name for c in spark.catalog.listCatalogs()] == []
    spark.stop()


def test_a_foreign_default_catalog_keeps_the_session_catalog(_fresh_session_slot: None) -> None:
    """A default naming no configured catalog still gets ``spark_catalog`` registered."""
    spark = ReparkSession.builder.config("spark.sql.defaultCatalog", "glue").getOrCreate()
    assert [c.name for c in spark.catalog.listCatalogs()] == ["spark_catalog"]
    assert spark.catalog.currentCatalog() == "glue"
    spark.stop()


def test_stop_removes_auto_warehouse(_fresh_session_slot: None) -> None:
    """The session-scoped temp warehouse vanishes on stop() (:memory: semantics)."""
    spark = ReparkSession.builder.getOrCreate()
    warehouse = spark._alive_token["auto_catalog_warehouse"]
    warehouse_path = Path(warehouse.name)
    spark.range(3).write.saveAsTable("automemcat_gone")
    assert warehouse_path.exists()
    spark.stop()
    assert not warehouse_path.exists()


def test_second_session_starts_clean(_fresh_session_slot: None) -> None:
    """After stop(), a new bare session sees no tables from the prior one (:memory:)."""
    first = ReparkSession.builder.getOrCreate()
    first.range(2).write.saveAsTable("automemcat_ephemeral")
    assert first.table("automemcat_ephemeral").count() == 2
    first.stop()

    second = ReparkSession.builder.getOrCreate()
    with pytest.raises(Exception, match=r"automemcat_ephemeral|not found|Table"):
        second.table("automemcat_ephemeral").count()
    second.stop()

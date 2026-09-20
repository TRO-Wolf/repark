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


def _catalog_names(spark: ReparkSession) -> list[str]:
    """The registered catalog names via ``SHOW CATALOGS``."""
    return [row["catalog"] for row in spark.sql("SHOW CATALOGS").to_arrow().to_pylist()]


def _table_properties(spark: ReparkSession, table: str) -> dict[str, str]:
    """Parse the ``Table Properties`` row of ``DESCRIBE TABLE EXTENDED``."""
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").to_arrow().to_pylist()
    raw = next(row["data_type"] for row in rows if row["col_name"] == "Table Properties")
    assert raw.startswith("[") and raw.endswith("]")
    return dict(pair.split("=", 1) for pair in raw[1:-1].split(",") if "=" in pair)


@pytest.fixture
def runtime_catalog(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-ice-catalog-session-1-rt").getOrCreate()
    session.register_memory_catalog("sc", tmp_path / "sc")
    return session


def test_runtime_catalog_registration_then_create(
    runtime_catalog: ReparkSession, tmp_path: Path
) -> None:
    """C-024 mechanism: one-at-a-time ``conf.set`` registers at first complete block."""
    spark = runtime_catalog
    warehouse = str(tmp_path / "rtwh")
    spark.conf.set("spark.sql.catalog.rt", "org.apache.iceberg.spark.SparkCatalog")
    assert "rt" not in _catalog_names(spark)
    spark.conf.set("spark.sql.catalog.rt.type", "memory")
    assert "rt" not in _catalog_names(spark)
    spark.conf.set("spark.sql.catalog.rt.warehouse", warehouse)
    assert "rt" in _catalog_names(spark)
    spark.sql("CREATE NAMESPACE rt.ns").to_arrow()
    spark.sql("CREATE TABLE rt.ns.t (a INT) USING iceberg").to_arrow()
    assert spark.sql("SELECT * FROM rt.ns.t").to_arrow().num_rows == 0


def test_late_table_default_key_lands_on_create(
    runtime_catalog: ReparkSession, tmp_path: Path
) -> None:
    """H-02 pin: a ``table-default`` key set after registration lands on ``CREATE``."""
    spark = runtime_catalog
    spark.conf.set("spark.sql.catalog.rd", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.rd.type", "memory")
    spark.conf.set("spark.sql.catalog.rd.warehouse", str(tmp_path / "rdwh"))
    spark.conf.set("spark.sql.catalog.rd.table-default.k1", "d1")
    spark.sql("CREATE NAMESPACE rd.ns").to_arrow()
    spark.sql("CREATE TABLE rd.ns.t (a INT) USING iceberg").to_arrow()
    assert _table_properties(spark, "rd.ns.t")["k1"] == "d1"


def test_table_override_beats_user_property(runtime_catalog: ReparkSession, tmp_path: Path) -> None:
    """N-12: ``table-override.k`` beats user ``TBLPROPERTIES k``."""
    spark = runtime_catalog
    spark.conf.set("spark.sql.catalog.ro", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.ro.type", "memory")
    spark.conf.set("spark.sql.catalog.ro.warehouse", str(tmp_path / "rowh"))
    spark.conf.set("spark.sql.catalog.ro.table-override.k2", "o2")
    spark.sql("CREATE NAMESPACE ro.ns").to_arrow()
    spark.sql("CREATE TABLE ro.ns.t (a INT) USING iceberg TBLPROPERTIES ('k2'='user')").to_arrow()
    assert _table_properties(spark, "ro.ns.t")["k2"] == "o2"


def test_user_property_beats_table_default(runtime_catalog: ReparkSession, tmp_path: Path) -> None:
    """N-12: user ``TBLPROPERTIES k`` beats ``table-default.k``."""
    spark = runtime_catalog
    spark.conf.set("spark.sql.catalog.ru", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.ru.type", "memory")
    spark.conf.set("spark.sql.catalog.ru.warehouse", str(tmp_path / "ruwh"))
    spark.conf.set("spark.sql.catalog.ru.table-default.k1", "d1")
    spark.sql("CREATE NAMESPACE ru.ns").to_arrow()
    spark.sql("CREATE TABLE ru.ns.t (a INT) USING iceberg TBLPROPERTIES ('k1'='user')").to_arrow()
    assert _table_properties(spark, "ru.ns.t")["k1"] == "user"


def test_both_default_and_override_resolves_to_override(
    runtime_catalog: ReparkSession, tmp_path: Path
) -> None:
    """N-12: default + override on one key resolves to the override."""
    spark = runtime_catalog
    spark.conf.set("spark.sql.catalog.rb", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.rb.type", "memory")
    spark.conf.set("spark.sql.catalog.rb.warehouse", str(tmp_path / "rbwh"))
    spark.conf.set("spark.sql.catalog.rb.table-default.k1", "d1")
    spark.conf.set("spark.sql.catalog.rb.table-override.k1", "o1")
    spark.sql("CREATE NAMESPACE rb.ns").to_arrow()
    spark.sql("CREATE TABLE rb.ns.t (a INT) USING iceberg").to_arrow()
    assert _table_properties(spark, "rb.ns.t")["k1"] == "o1"


def test_use_updates_facade_state(spark: ReparkSession) -> None:
    """C-028 mechanism: SQL ``USE`` moves the facade box and the engine together."""
    spark.sql("USE sc.ns").to_arrow()
    assert spark.catalog.currentCatalog() == "sc"
    assert spark.catalog.currentDatabase() == "ns"
    answered = spark.sql("SELECT current_catalog(), current_schema()").to_arrow().to_pylist()
    assert answered == [{"current_catalog()": "sc", "current_schema()": "ns"}]


def test_set_current_catalog_updates_engine_state(spark: ReparkSession) -> None:
    """C-028 mechanism: ``setCurrentCatalog`` moves the engine too."""
    spark.catalog.setCurrentCatalog("sc")
    answered = spark.sql("SELECT current_catalog()").to_arrow().to_pylist()
    assert answered == [{"current_catalog()": "sc"}]
    spark.catalog.setCurrentDatabase("ns")
    assert spark.catalog.currentDatabase() == "ns"
    answered = spark.sql("SELECT current_schema()").to_arrow().to_pylist()
    assert answered == [{"current_schema()": "ns"}]


def test_set_datafusion_catalog_keys_updates_facade_state(spark: ReparkSession) -> None:
    """C-028 mechanism: ``SET datafusion.catalog.*`` syncs the facade box."""
    spark.conf.set("datafusion.catalog.default_catalog", "sc")
    assert spark.catalog.currentCatalog() == "sc"
    spark.conf.set("datafusion.catalog.default_schema", "ns")
    assert spark.catalog.currentDatabase() == "ns"


def test_hadoop_type_registers_memory_catalog(
    runtime_catalog: ReparkSession, tmp_path: Path
) -> None:
    """C-025 mechanism: ``type=hadoop`` aliases to a memory catalog (INDEX 25)."""
    spark = runtime_catalog
    spark.conf.set("spark.sql.catalog.hd", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.hd.type", "hadoop")
    spark.conf.set("spark.sql.catalog.hd.warehouse", str(tmp_path / "hdwh"))
    assert "hd" in _catalog_names(spark)
    spark.sql("CREATE NAMESPACE hd.ns").to_arrow()
    spark.sql("CREATE TABLE hd.ns.t (a INT) USING iceberg").to_arrow()
    spark.sql("INSERT INTO hd.ns.t VALUES (1)").to_arrow()
    assert spark.sql("SELECT * FROM hd.ns.t").to_arrow().to_pylist() == [{"a": 1}]


def test_inmemory_catalog_impl_registers_memory_catalog(
    runtime_catalog: ReparkSession, tmp_path: Path
) -> None:
    """C-026 mechanism: an ``InMemoryCatalog`` class aliases to a memory catalog."""
    spark = runtime_catalog
    spark.conf.set(
        "spark.sql.catalog.im.catalog-impl",
        "org.apache.iceberg.memory.InMemoryCatalog",
    )
    spark.conf.set("spark.sql.catalog.im.warehouse", str(tmp_path / "imwh"))
    assert "im" in _catalog_names(spark)
    spark.sql("CREATE NAMESPACE im.ns").to_arrow()
    spark.sql("CREATE TABLE im.ns.t (a INT) USING iceberg").to_arrow()
    assert spark.sql("SELECT * FROM im.ns.t").to_arrow().num_rows == 0

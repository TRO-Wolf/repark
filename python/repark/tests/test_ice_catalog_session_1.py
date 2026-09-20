"""ICE-CATALOG-SESSION-1: catalog and session SQL against the recorded Spark answers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, UnsupportedOperationException

_ORACLE: dict[str, Any] = json.loads(
    (Path(__file__).parent / "ice_catalog_session_1_oracle.json").read_text(encoding="utf-8")
)


def _cell_obs(name: str) -> dict[str, Any]:
    """The recorded Spark observation for one inventory cell."""
    return _ORACLE["cells"][name]["obs"]


def _probe(name: str) -> Any:
    """One recorded Spark probe answer (the replay truth where the harness sorts)."""
    return _ORACLE["probes"][name]


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
    assert [[name] for name in _catalog_names(spark)] == [["rt"], ["sc"], ["spark_catalog"]]
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


def test_current_catalog_answers_spark_catalog(spark: ReparkSession) -> None:
    """C-012: ``CAT-CURRENT-CATALOG`` replays EQUAL at session start."""
    answered = spark.sql("SELECT current_catalog()").to_arrow().to_pylist()
    assert [[row["current_catalog()"]] for row in answered] == _cell_obs("CAT-CURRENT-CATALOG")[
        "current"
    ]
    assert spark.sql("SELECT current_schema()").to_arrow().to_pylist() == [
        {"current_schema()": "default"}
    ]


def test_use_catalog_leaves_namespace_empty(runtime_catalog: ReparkSession) -> None:
    """C-013: catalog-only ``USE`` leaves the namespace empty (N-2)."""
    spark = runtime_catalog
    spark.sql("USE sc").to_arrow()
    assert spark.catalog.currentCatalog() == "sc"
    assert spark.sql("SELECT current_schema()").to_arrow().to_pylist() == [{"current_schema()": ""}]


def test_current_database_alias(spark: ReparkSession) -> None:
    """C-013: ``current_database()`` tracks ``current_schema()`` through ``USE``."""
    spark.sql("USE sc.ns").to_arrow()
    assert spark.sql("SELECT current_database()").to_arrow().to_pylist() == [
        {"current_database()": "ns"}
    ]


def test_use_catalog_ns_cell(runtime_catalog: ReparkSession) -> None:
    """C-014: ``CAT-USE-CATALOG-NS`` replays EQUAL."""
    spark = runtime_catalog
    spark.sql("CREATE NAMESPACE sc.ns").to_arrow()
    spark.sql("USE sc.ns").to_arrow()
    spark.sql("CREATE TABLE uc_t_cat_use_catalog_ns (a INT) USING iceberg").to_arrow()
    rows = spark.sql("SHOW TABLES").to_arrow().to_pylist()
    assert [[row["namespace"], row["tableName"], row["isTemporary"]] for row in rows] == _cell_obs(
        "CAT-USE-CATALOG-NS"
    )["rows"]
    current = spark.sql("SELECT current_catalog(), current_schema()").to_arrow().to_pylist()
    assert [[row["current_catalog()"], row["current_schema()"]] for row in current] == _cell_obs(
        "CAT-USE-CATALOG-NS"
    )["current"]


def test_two_part_name_resolves_against_current_catalog(
    runtime_catalog: ReparkSession,
) -> None:
    """C-014: a two-part name resolves against the current catalog after ``USE``."""
    spark = runtime_catalog
    spark.sql("CREATE NAMESPACE sc.ns").to_arrow()
    spark.sql("USE sc").to_arrow()
    spark.sql("CREATE TABLE ns.two (a INT) USING iceberg").to_arrow()
    spark.sql("INSERT INTO ns.two VALUES (1), (2)").to_arrow()
    assert spark.sql("SELECT * FROM ns.two ORDER BY a").to_arrow().to_pylist() == [
        {"a": 1},
        {"a": 2},
    ]


def test_show_catalogs_lists_registered(runtime_catalog: ReparkSession, tmp_path: Path) -> None:
    """C-015: ``CAT-SHOW-CATALOGS`` replays EQUAL."""
    spark = runtime_catalog
    spark.register_memory_catalog("hc", tmp_path / "hc")
    assert [[name] for name in _catalog_names(spark)] == _cell_obs("CAT-SHOW-CATALOGS")["rows"]


def test_show_catalogs_like_filters(runtime_catalog: ReparkSession, tmp_path: Path) -> None:
    """C-015: ``SHOW CATALOGS LIKE`` filters with glob semantics."""
    spark = runtime_catalog
    spark.register_memory_catalog("hc", tmp_path / "hc")
    assert [
        row["catalog"] for row in spark.sql("SHOW CATALOGS LIKE 'h*'").to_arrow().to_pylist()
    ] == ["hc"]
    assert [
        row["catalog"] for row in spark.sql("SHOW CATALOGS LIKE 's*'").to_arrow().to_pylist()
    ] == ["sc", "spark_catalog"]


def test_show_tables_after_use_lists_current_namespace(spark: ReparkSession) -> None:
    """C-016: ``SHOW TABLES`` answers the Spark shape for the current namespace."""
    spark.sql("USE sc.ns").to_arrow()
    rows = spark.sql("SHOW TABLES").to_arrow().to_pylist()
    assert [[row["namespace"], row["tableName"], row["isTemporary"]] for row in rows] == [
        ["ns", "t_cache", False]
    ]


def test_show_columns_is_declaration_order(spark: ReparkSession) -> None:
    """C-017: ``D-SHOW-COLUMNS`` replays EQUAL in declaration order (N-6, not the harness sort)."""
    spark.sql(
        "CREATE TABLE sc.ns.t_cols (id INT, data STRING, cat STRING) USING iceberg"
    ).to_arrow()
    for statement in (
        "SHOW COLUMNS IN sc.ns.t_cols",
        "SHOW COLUMNS IN t_cols",
        "SHOW COLUMNS FROM sc.ns.t_cols",
    ):
        spark.sql("USE sc.ns").to_arrow()
        rows = spark.sql(statement).to_arrow().to_pylist()
        assert [[row["col_name"]] for row in rows] == _probe("F.show_columns")


def test_show_columns_beats_information_schema(spark: ReparkSession) -> None:
    """C-017: the ``SHOW COLUMNS`` intercept wins even with ``information_schema`` on."""
    spark.sql(
        "CREATE TABLE sc.ns.t_cols (id INT, data STRING, cat STRING) USING iceberg"
    ).to_arrow()
    spark.conf.set("datafusion.catalog.information_schema", "true")
    rows = spark.sql("SHOW COLUMNS IN sc.ns.t_cols").to_arrow().to_pylist()
    assert [[row["col_name"]] for row in rows] == _probe("F.show_columns")


def test_show_namespaces_bare_uses_current_catalog(spark: ReparkSession) -> None:
    """C-018: bare ``SHOW NAMESPACES`` lists the current catalog."""
    spark.sql("USE sc.ns").to_arrow()
    for statement in ("SHOW NAMESPACES", "SHOW SCHEMAS", "SHOW DATABASES"):
        rows = spark.sql(statement).to_arrow().to_pylist()
        assert [[row["namespace"]] for row in rows] == [["ns"]]


def test_refresh_table_cell(spark: ReparkSession) -> None:
    """C-019: ``CAT-REFRESH-TABLE`` replays EQUAL."""
    spark.sql("CREATE TABLE sc.ns.t_cat_refresh_table (a INT) USING iceberg").to_arrow()
    spark.sql("INSERT INTO sc.ns.t_cat_refresh_table VALUES (1)").to_arrow()
    assert spark.sql("REFRESH TABLE sc.ns.t_cat_refresh_table").to_arrow().num_rows == 0
    assert _cell_obs("CAT-REFRESH-TABLE")["ok"] is True


def test_cache_table_cell(spark: ReparkSession) -> None:
    """C-020: ``CAT-CACHE-TABLE`` replays EQUAL."""
    spark.sql("CREATE TABLE sc.ns.t_cat_cache_table (a INT) USING iceberg").to_arrow()
    spark.sql("INSERT INTO sc.ns.t_cat_cache_table VALUES (1)").to_arrow()
    spark.sql("CACHE TABLE sc.ns.t_cat_cache_table").to_arrow()
    spark.sql("INSERT INTO sc.ns.t_cat_cache_table VALUES (2)").to_arrow()
    rows = spark.sql("SELECT * FROM sc.ns.t_cat_cache_table ORDER BY a").to_arrow().to_pylist()
    assert [[row["a"]] for row in rows] == _cell_obs("CAT-CACHE-TABLE")["rows"]


def test_uncache_missing_table(spark: ReparkSession) -> None:
    """C-021: ``UNCACHE TABLE`` on a missing table refuses with the Spark text."""
    with pytest.raises(AnalysisException) as caught:
        spark.sql("UNCACHE TABLE sc.ns.nothere").to_arrow()
    text = str(caught.value)
    assert "TABLE_OR_VIEW_NOT_FOUND" in text
    assert "current_schema()" in text


def test_rename_to_two_part_cell(spark: ReparkSession) -> None:
    """C-022: ``D-RENAME-TABLE-SHORT`` replays EQUAL."""
    spark.sql(
        "CREATE TABLE sc.ns.t_d_rename_table_short (id INT, data STRING, cat STRING) USING iceberg"
    ).to_arrow()
    spark.sql(
        "INSERT INTO sc.ns.t_d_rename_table_short VALUES"
        " (0, 'd0', 'a'), (1, 'd1', 'b'), (2, 'd2', 'a')"
    ).to_arrow()
    spark.sql(
        "ALTER TABLE sc.ns.t_d_rename_table_short RENAME TO ns.u_d_rename_table_short"
    ).to_arrow()
    rows = (
        spark.sql("SELECT * FROM sc.ns.u_d_rename_table_short ORDER BY id").to_arrow().to_pylist()
    )
    assert [[row["id"], row["data"], row["cat"]] for row in rows] == _cell_obs(
        "D-RENAME-TABLE-SHORT"
    )["data"]


def test_rename_across_catalogs_still_refuses(
    runtime_catalog: ReparkSession, tmp_path: Path
) -> None:
    """C-022: a three-part cross-catalog ``RENAME TO`` still refuses."""
    spark = runtime_catalog
    spark.register_memory_catalog("hc", tmp_path / "hc")
    spark.sql("CREATE NAMESPACE sc.ns").to_arrow()
    spark.sql("CREATE NAMESPACE hc.ns").to_arrow()
    spark.sql("CREATE TABLE sc.ns.t (a INT) USING iceberg").to_arrow()
    with pytest.raises(AnalysisException, match="cannot move across catalogs"):
        spark.sql("ALTER TABLE sc.ns.t RENAME TO hc.ns.t2").to_arrow()


def test_call_no_catalog_cell(spark: ReparkSession) -> None:
    """C-023: ``P-CALL-NO-CATALOG`` replays EQUAL (S2/S0 are the 3rd/1st live ids)."""
    spark.sql("CREATE TABLE sc.ns.t_call (id INT, c STRING, d STRING) USING iceberg").to_arrow()
    spark.sql(
        "INSERT INTO sc.ns.t_call VALUES (1, 'a', 'x'), (2, 'b', 'y'), (6, 'f', 'x')"
    ).to_arrow()
    spark.sql("INSERT INTO sc.ns.t_call VALUES (7, 'g', 'y')").to_arrow()
    spark.sql("INSERT INTO sc.ns.t_call VALUES (8, 'h', 'z')").to_arrow()
    snaps = [
        row["snapshot_id"]
        for row in spark.sql("SELECT snapshot_id FROM sc.ns.t_call.snapshots ORDER BY committed_at")
        .to_arrow()
        .to_pylist()
    ]
    spark.sql("USE sc").to_arrow()
    answered = spark.sql(f"CALL system.rollback_to_snapshot('ns.t_call', {snaps[0]})").to_arrow()
    assert answered.schema.names == _cell_obs("P-CALL-NO-CATALOG")["out.cols"]
    rows = answered.to_pylist()
    assert [rows[0]["previous_snapshot_id"], rows[0]["current_snapshot_id"]] == [snaps[2], snaps[0]]
    data = spark.sql("SELECT * FROM sc.ns.t_call ORDER BY id").to_arrow().to_pylist()
    assert [[row["id"], row["c"], row["d"]] for row in data] == _cell_obs("P-CALL-NO-CATALOG")[
        "data"
    ]


def test_hadoop_type_cell(runtime_catalog: ReparkSession, tmp_path: Path) -> None:
    """C-025: ``CAT-TYPE-HADOOP`` replays EQUAL."""
    spark = runtime_catalog
    spark.conf.set("spark.sql.catalog.c_hadoop", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.c_hadoop.type", "hadoop")
    spark.conf.set("spark.sql.catalog.c_hadoop.warehouse", str(tmp_path / "hdwh"))
    spark.sql("CREATE NAMESPACE IF NOT EXISTS c_hadoop.n1").to_arrow()
    spark.sql("CREATE TABLE c_hadoop.n1.t (a INT) USING iceberg").to_arrow()
    spark.sql("INSERT INTO c_hadoop.n1.t VALUES (1)").to_arrow()
    rows = spark.sql("SELECT * FROM c_hadoop.n1.t").to_arrow().to_pylist()
    assert [[row["a"]] for row in rows] == _cell_obs("CAT-TYPE-HADOOP")["rows"]


def test_catalog_impl_inmemory_cell(runtime_catalog: ReparkSession, tmp_path: Path) -> None:
    """C-026: ``CAT-CATALOG-IMPL-INMEMORY`` replays EQUAL."""
    spark = runtime_catalog
    spark.conf.set(
        "spark.sql.catalog.c_inmem.catalog-impl",
        "org.apache.iceberg.inmemory.InMemoryCatalog",
    )
    spark.conf.set("spark.sql.catalog.c_inmem.warehouse", str(tmp_path / "imwh"))
    spark.sql("CREATE NAMESPACE IF NOT EXISTS c_inmem.n1").to_arrow()
    assert _cell_obs("CAT-CATALOG-IMPL-INMEMORY")["ok"] is True
    rows = spark.sql("SHOW NAMESPACES IN c_inmem").to_arrow().to_pylist()
    assert [[row["namespace"]] for row in rows] == [["n1"]]


def test_table_default_override_cell(runtime_catalog: ReparkSession, tmp_path: Path) -> None:
    """C-027: ``CAT-TABLE-DEFAULT-OVERRIDE`` replays EQUAL."""
    spark = runtime_catalog
    spark.conf.set("spark.sql.catalog.c_tdef", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.c_tdef.type", "memory")
    spark.conf.set("spark.sql.catalog.c_tdef.warehouse", str(tmp_path / "tdwh"))
    spark.conf.set("spark.sql.catalog.c_tdef.table-default.k1", "d1")
    spark.conf.set("spark.sql.catalog.c_tdef.table-override.k2", "o2")
    spark.conf.set("spark.sql.catalog.c_tdef.table-default.write.format.default", "parquet")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS c_tdef.n1").to_arrow()
    spark.sql("CREATE TABLE c_tdef.n1.t (a INT) USING iceberg").to_arrow()
    props = _table_properties(spark, "c_tdef.n1.t")
    for key, value in _cell_obs("CAT-TABLE-DEFAULT-OVERRIDE")["props"]:
        assert props[key] == value


def test_show_catalogs_lists_untouched_configured_catalog(tmp_path: Path) -> None:
    """EAGER-1 pin: a build-time-configured, never-touched catalog still lists."""
    spark = (
        ReparkSession.builder.appName("pytest-ice-catalog-session-1-eager")
        .config("spark.sql.catalog.hc.type", "memory")
        .config("spark.sql.catalog.hc.warehouse", str(tmp_path / "hcwh"))
        .getOrCreate()
    )
    assert "hc" in _catalog_names(spark)


def test_hadoop_alias_writes_uuid_metadata_names(tmp_path: Path) -> None:
    """HADOOP-1 pin: the aliased catalog writes UUID metadata names, no version-hint."""
    import glob
    import os
    import re

    spark = ReparkSession.builder.appName("pytest-ice-catalog-session-1-hd").getOrCreate()
    warehouse = str(tmp_path / "hdwh")
    spark.conf.set("spark.sql.catalog.hd.type", "hadoop")
    spark.conf.set("spark.sql.catalog.hd.warehouse", warehouse)
    spark.sql("CREATE NAMESPACE hd.ns").to_arrow()
    spark.sql("CREATE TABLE hd.ns.t (a INT) USING iceberg").to_arrow()
    spark.sql("INSERT INTO hd.ns.t VALUES (1)").to_arrow()
    metas = sorted(glob.glob(f"{warehouse}/**/*.metadata.json", recursive=True))
    assert len(metas) >= 1
    for meta in metas:
        assert (
            re.fullmatch(
                r"[0-9]+-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
                r"\.metadata\.json",
                os.path.basename(meta),
            )
            is not None
        )
    assert glob.glob(f"{warehouse}/**/version-hint*", recursive=True) == []

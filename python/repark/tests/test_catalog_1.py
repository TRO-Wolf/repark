"""CATALOG-1 facade pins: the session catalog is ``spark_catalog``, like Spark.

Every expected text is Spark 4.1.2 + Iceberg 1.11, measured 2026-09-26 (probes under
``target/probe-catalog-1/``). The harness-shaped session configures ``hc`` through the builder
and registers ``sc`` after build, as the scoreboard's RePark leg does.

pins: catalog-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
"""

from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    UnsupportedOperationException,
)
from repark.spark.session import _reset_active_session_for_tests

IN_MEMORY_CATALOG = "org.apache.iceberg.inmemory.InMemoryCatalog"
CATALOG_NOT_FOUND_NOPE = (
    "[CATALOG_NOT_FOUND] The catalog `nope` not found. Consider to set the SQL config "
    '"spark.sql.catalog.nope" to a catalog plugin. SQLSTATE: 42P08'
)


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


def _both_kinds_message(
    name: str, type_value: str = "memory", impl: str = IN_MEMORY_CATALOG
) -> str:
    """Spark's both-keys refusal text for one catalog block."""
    return (
        f"Cannot create catalog {name}, both type and catalog-impl are set: "
        f"type={type_value}, catalog-impl={impl}"
    )


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


def test_default_catalog_cell_follows_the_runtime_conf(spark: ReparkSession) -> None:
    """CAT-DEFAULT-CATALOG: a runtime default moves two-part names, then unset restores.

    pins: catalog-1/C-003
    """
    spark.conf.set("spark.sql.defaultCatalog", "sc")
    try:
        assert _current(spark) == [["sc", ""]]
        spark.sql("CREATE TABLE ns.dc_t (id INT) USING iceberg")
        assert _rows(spark, "SELECT count(*) FROM sc.ns.dc_t") == [[0]]
    finally:
        spark.conf.unset("spark.sql.defaultCatalog")
    assert _current(spark) == [["spark_catalog", "default"]]
    with pytest.raises(AnalysisException):
        spark.sql("SELECT count(*) FROM ns.dc_t").collect()


def test_the_runtime_default_catalog_drives_the_dataframe_door(spark: ReparkSession) -> None:
    """table / writeTo / saveAsTable / tableExists / listDatabases follow the runtime default.

    pins: catalog-1/C-003
    """
    spark.conf.set("spark.sql.defaultCatalog", "sc")
    try:
        assert spark.catalog.currentCatalog() == "sc"
        assert spark.catalog.currentDatabase() == ""
        spark.sql("CREATE TABLE ns.dc_t (id INT) USING iceberg")
        assert spark.table("ns.dc_t").count() == 0
        spark.createDataFrame([(1,)], "id INT").writeTo("ns.dc_w").create()
        assert _rows(spark, "SELECT * FROM sc.ns.dc_w") == [[1]]
        spark.createDataFrame([(2,)], "id INT").write.format("iceberg").saveAsTable("ns.dc_s")
        assert _rows(spark, "SELECT * FROM sc.ns.dc_s") == [[2]]
        assert spark.catalog.tableExists("ns.dc_t") is True
        assert sorted(d.name for d in spark.catalog.listDatabases()) == ["ns"]
        assert sorted(t.name for t in spark.catalog.listTables("ns")) == ["dc_s", "dc_t", "dc_w"]
        assert _rows(spark, "SHOW NAMESPACES") == [["ns"]]
    finally:
        spark.conf.unset("spark.sql.defaultCatalog")


def test_use_pins_the_current_catalog_against_the_default_conf(spark: ReparkSession) -> None:
    """After ``USE`` the default conf no longer moves the current catalog, set or unset.

    pins: catalog-1/C-003
    """
    spark.sql("USE hc.ns")
    assert _current(spark) == [["hc", "ns"]]
    spark.conf.set("spark.sql.defaultCatalog", "sc")
    assert _current(spark) == [["hc", "ns"]]
    spark.conf.unset("spark.sql.defaultCatalog")
    assert _current(spark) == [["hc", "ns"]]
    spark.sql("USE sc")
    assert _current(spark) == [["sc", ""]]
    spark.conf.unset("spark.sql.defaultCatalog")
    assert _current(spark) == [["sc", ""]]
    spark.sql("USE spark_catalog.default")
    assert _current(spark) == [["spark_catalog", "default"]]


def test_set_current_catalog_pins_against_the_default_conf(spark: ReparkSession) -> None:
    """``setCurrentCatalog`` pins the current catalog like ``USE`` against the default conf.

    pins: catalog-1/C-003
    """
    spark.catalog.setCurrentCatalog("sc")
    assert _current(spark)[0][0] == "sc"
    spark.conf.set("spark.sql.defaultCatalog", "hc")
    assert _current(spark)[0][0] == "sc"
    spark.conf.unset("spark.sql.defaultCatalog")
    assert _current(spark)[0][0] == "sc"
    spark.sql("USE spark_catalog.default")
    assert _current(spark) == [["spark_catalog", "default"]]


def test_the_default_catalog_conf_at_build_is_the_first_current_catalog(tmp_path: Path) -> None:
    """``spark.sql.defaultCatalog`` on the builder sets the first current catalog.

    pins: catalog-1/C-003
    """
    session = _build(tmp_path, {"spark.sql.defaultCatalog": "sc"})
    try:
        assert _current(session) == [["sc", ""]]
        assert _rows(session, "SHOW NAMESPACES") == [["ns"]]
        session.sql("CREATE TABLE ns.bt (id INT) USING iceberg")
        assert _rows(session, "SELECT count(*) FROM sc.ns.bt") == [[0]]
        assert session.catalog.currentCatalog() == "sc"
        assert sorted(d.name for d in session.catalog.listDatabases()) == ["ns"]
    finally:
        session.stop()
        _reset_active_session_for_tests()


def test_use_catalog_ns_cell_and_the_final_reset(spark: ReparkSession) -> None:
    """CAT-USE-CATALOG-NS: ``USE sc.ns``, unqualified CREATE/SHOW, the final reset works.

    pins: catalog-1/C-005
    """
    spark.sql("USE sc.ns")
    try:
        spark.sql("CREATE TABLE uc_t (id INT) USING iceberg")
        assert _rows(spark, "SHOW TABLES LIKE 'uc_t'") == [["ns", "uc_t", False]]
        assert _current(spark) == [["sc", "ns"]]
    finally:
        spark.sql("USE spark_catalog.default")
    assert _current(spark) == [["spark_catalog", "default"]]


def test_use_forms_answer_as_spark(spark: ReparkSession) -> None:
    """Every ``USE`` form lands where Spark lands; bad forms refuse SCHEMA_NOT_FOUND.

    pins: catalog-1/C-005
    """
    for sql, catalog, namespace in (
        ("USE sc", "sc", ""),
        ("USE sc.ns", "sc", "ns"),
        ("USE ns", "sc", "ns"),
        ("USE sc", "sc", "ns"),
        ("USE hc", "hc", ""),
        ("USE ns", "hc", "ns"),
        ("USE spark_catalog", "spark_catalog", "default"),
        ("USE sc.ns", "sc", "ns"),
        ("USE spark_catalog.default", "spark_catalog", "default"),
    ):
        spark.sql(sql)
        assert _current(spark) == [[catalog, namespace]], sql
    for sql, rendered in (
        ("USE zz.yy", "`spark_catalog`.`zz`.`yy`"),
        ("USE zz", "`spark_catalog`.`zz`"),
        ("USE zz.yy.xx", "`spark_catalog`.`zz`.`yy`.`xx`"),
        ("USE sc.nope", "`sc`.`nope`"),
        ("USE hc.ns.x", "`hc`.`ns`.`x`"),
    ):
        with pytest.raises(AnalysisException) as caught:
            spark.sql(sql).collect()
        assert f"[SCHEMA_NOT_FOUND] The schema {rendered} cannot be found." in str(caught.value), (
            sql
        )
        assert caught.value.getCondition() == "SCHEMA_NOT_FOUND", sql
        assert caught.value.getSqlState() == "42704", sql
    assert _current(spark) == [["spark_catalog", "default"]]


def test_type_memory_cell_refuses_at_first_use(spark: ReparkSession, tmp_path: Path) -> None:
    """CAT-TYPE-MEMORY: a runtime ``type=memory`` block refuses at first use, like Spark.

    pins: catalog-1/C-006
    """
    spark.conf.set("spark.sql.catalog.c_mem", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.c_mem.type", "memory")
    spark.conf.set("spark.sql.catalog.c_mem.warehouse", str(tmp_path / "c_mem"))
    assert "c_mem" not in {catalog.name for catalog in spark.catalog.listCatalogs()}
    assert _rows(spark, "SHOW CATALOGS") == [["hc"], ["sc"], ["spark_catalog"]]
    for sql in (
        "CREATE NAMESPACE IF NOT EXISTS c_mem.n1",
        "USE c_mem",
        "USE c_mem.n1",
        "SHOW NAMESPACES IN c_mem",
        "SELECT * FROM c_mem.n1.t",
    ):
        with pytest.raises(UnsupportedOperationException) as caught:
            spark.sql(sql).collect()
        assert str(caught.value) == "Unknown catalog type: memory", sql
        assert caught.value.getCondition() is None, sql
        assert caught.value.getSqlState() is None, sql
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.catalog.setCurrentCatalog("c_mem")
    assert str(caught.value) == "Unknown catalog type: memory"
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.catalog.tableExists("c_mem.n1.t")
    assert str(caught.value) == "Unknown catalog type: memory"
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.sql("SHOW NAMESPACES IN c_mem").collect()
    assert str(caught.value) == "Unknown catalog type: memory"
    assert _current(spark) == [["spark_catalog", "default"]]
    assert spark.catalog.currentCatalog() == "spark_catalog"
    assert sorted(d.name for d in spark.catalog.listDatabases()) == ["default"]


def test_type_memory_on_the_builder_door_builds_and_refuses_at_first_use(
    tmp_path: Path,
) -> None:
    """A builder ``type=memory`` block builds quietly; a later long form replaces it.

    pins: catalog-1/C-006
    """
    session = _build(
        tmp_path,
        {
            "spark.sql.catalog.c_mem": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.c_mem.type": "memory",
            "spark.sql.catalog.c_mem.warehouse": str(tmp_path / "c_mem"),
        },
    )
    try:
        assert "c_mem" not in {c.name for c in session.catalog.listCatalogs()}
        for sql in ("USE c_mem", "SHOW NAMESPACES IN c_mem", "SELECT * FROM c_mem.n1.t"):
            with pytest.raises(UnsupportedOperationException) as caught:
                session.sql(sql).collect()
            assert str(caught.value) == "Unknown catalog type: memory", sql
        assert _current(session) == [["spark_catalog", "default"]]
        session.conf.unset("spark.sql.catalog.c_mem.type")
        session.conf.set("spark.sql.catalog.c_mem.warehouse", str(tmp_path / "c_mem"))
        session.conf.set("spark.sql.catalog.c_mem.catalog-impl", IN_MEMORY_CATALOG)
        session.sql("CREATE NAMESPACE c_mem.n1")
        assert "c_mem" in {c.name for c in session.catalog.listCatalogs()}
        assert session.catalog.tableExists("c_mem.n1.t") is False
    finally:
        session.stop()
        _reset_active_session_for_tests()


def test_the_catalog_impl_long_form_works_on_both_doors(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """``catalog-impl=InMemoryCatalog`` registers on the runtime and builder doors.

    pins: catalog-1/C-006
    """
    spark.conf.set("spark.sql.catalog.c_impl", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.c_impl.catalog-impl", IN_MEMORY_CATALOG)
    spark.conf.set("spark.sql.catalog.c_impl.warehouse", str(tmp_path / "c_impl"))
    spark.sql("CREATE NAMESPACE c_impl.n1")
    spark.sql("CREATE TABLE c_impl.n1.t (id INT) USING iceberg")
    assert _rows(spark, "SELECT count(*) FROM c_impl.n1.t") == [[0]]
    built = _build(
        tmp_path,
        {
            "spark.sql.catalog.c_impl_b.catalog-impl": IN_MEMORY_CATALOG,
            "spark.sql.catalog.c_impl_b.warehouse": str(tmp_path / "c_impl_b"),
        },
    )
    try:
        built.sql("CREATE NAMESPACE c_impl_b.n1")
        assert "c_impl_b" in {c.name for c in built.catalog.listCatalogs()}
    finally:
        built.stop()
        _reset_active_session_for_tests()


def test_both_kind_keys_refuse_at_first_use_on_both_doors(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """``type`` beside ``catalog-impl`` refuses at first use on both doors, like Spark.

    pins: catalog-1/C-007
    """
    spark.conf.set("spark.sql.catalog.c_both", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.c_both.type", "memory")
    spark.conf.set("spark.sql.catalog.c_both.catalog-impl", IN_MEMORY_CATALOG)
    spark.conf.set("spark.sql.catalog.c_both.warehouse", str(tmp_path / "c_both"))
    expected = _both_kinds_message("c_both")
    for sql in (
        "CREATE NAMESPACE IF NOT EXISTS c_both.n1",
        "USE c_both",
        "SHOW NAMESPACES IN c_both",
        "SELECT * FROM c_both.n1.t",
    ):
        with pytest.raises(IllegalArgumentException) as caught:
            spark.sql(sql).collect()
        assert str(caught.value) == expected, sql
        assert caught.value.getCondition() is None, sql
        assert caught.value.getSqlState() is None, sql
    with pytest.raises(IllegalArgumentException) as caught:
        spark.catalog.setCurrentCatalog("c_both")
    assert str(caught.value) == expected
    with pytest.raises(IllegalArgumentException) as caught:
        spark.catalog.tableExists("c_both.n1.t")
    assert str(caught.value) == expected
    assert _current(spark) == [["spark_catalog", "default"]]
    built = _build(
        tmp_path,
        {
            "spark.sql.catalog.c_both_b.type": "memory",
            "spark.sql.catalog.c_both_b.catalog-impl": IN_MEMORY_CATALOG,
            "spark.sql.catalog.c_both_b.warehouse": str(tmp_path / "c_both_b"),
        },
    )
    try:
        with pytest.raises(IllegalArgumentException) as caught:
            built.sql("SHOW NAMESPACES IN c_both_b").collect()
        assert str(caught.value) == _both_kinds_message("c_both_b")
    finally:
        built.stop()
        _reset_active_session_for_tests()


def test_the_catalog_extensions_opt_in_restores_the_memory_type(tmp_path: Path) -> None:
    """``repark.sql.catalogExtensions=true`` makes ``type=memory`` the memory kind again.

    pins: catalog-1/C-008
    """
    session = _build(
        tmp_path,
        {
            "repark.sql.catalogExtensions": "true",
            "spark.sql.catalog.c.type": "memory",
            "spark.sql.catalog.c.warehouse": str(tmp_path / "c"),
            "spark.sql.catalog.c_pair.type": "memory",
            "spark.sql.catalog.c_pair.catalog-impl": IN_MEMORY_CATALOG,
            "spark.sql.catalog.c_pair.warehouse": str(tmp_path / "c_pair"),
        },
    )
    try:
        session.sql("CREATE NAMESPACE c.n1")
        session.sql("CREATE NAMESPACE c_pair.n1")
        session.conf.set("spark.sql.catalog.c_rt.type", "memory")
        session.conf.set("spark.sql.catalog.c_rt.warehouse", str(tmp_path / "c_rt"))
        session.sql("CREATE NAMESPACE c_rt.n1")
        assert "c_rt" in {c.name for c in session.catalog.listCatalogs()}
    finally:
        session.stop()
        _reset_active_session_for_tests()
    runtime_opt_in = _build(tmp_path)
    try:
        runtime_opt_in.conf.set("repark.sql.catalogExtensions", "true")
        runtime_opt_in.conf.set("spark.sql.catalog.c_rs.type", "memory")
        runtime_opt_in.conf.set("spark.sql.catalog.c_rs.warehouse", str(tmp_path / "c_rs"))
        runtime_opt_in.sql("CREATE NAMESPACE c_rs.n1")
    finally:
        runtime_opt_in.stop()
        _reset_active_session_for_tests()
    with pytest.raises(IllegalArgumentException, match="different catalog kinds"):
        _build(
            tmp_path,
            {
                "repark.sql.catalogExtensions": "true",
                "spark.sql.catalog.c_bad.type": "memory",
                "spark.sql.catalog.c_bad.catalog-impl": ("org.apache.iceberg.aws.glue.GlueCatalog"),
                "spark.sql.catalog.c_bad.warehouse": str(tmp_path / "c_bad"),
            },
        )


def test_a_default_catalog_naming_no_catalog_answers_catalog_not_found(tmp_path: Path) -> None:
    """A default that names no catalog builds; its first resolution is CATALOG_NOT_FOUND.

    pins: catalog-1/C-004
    """
    session = _build(tmp_path, {"spark.sql.defaultCatalog": "nope"})
    try:
        assert _rows(session, "SELECT 1") == [[1]]
        assert _rows(session, "SHOW CATALOGS") == [["hc"], ["sc"], ["spark_catalog"]]
        for sql in (
            "SELECT current_catalog()",
            "SHOW NAMESPACES",
            "CREATE TABLE ns.mt (id INT) USING iceberg",
            "USE sc",
        ):
            with pytest.raises(AnalysisException) as caught:
                session.sql(sql).collect()
            assert CATALOG_NOT_FOUND_NOPE in str(caught.value), sql
        for call in (session.catalog.currentCatalog, session.catalog.listDatabases):
            with pytest.raises(AnalysisException) as caught:
                call()
            assert str(caught.value) == CATALOG_NOT_FOUND_NOPE
        session.sql("CREATE TABLE sc.ns.three (id INT) USING iceberg")
        assert _rows(session, "SELECT count(*) FROM sc.ns.three") == [[0]]
        session.conf.unset("spark.sql.defaultCatalog")
        assert _current(session) == [["spark_catalog", "default"]]
    finally:
        session.stop()
        _reset_active_session_for_tests()

"""R-CURCAT-FACADE / G-INT catalog surface — current catalog + list* over existing primitives.

Oracle: live PySpark 4.1.2 (zulu-17) measured 2026-07-27 (G-INT) and re-measured 2026-07-29
(overnight N1). Live V2 shapes:

* ``listDatabases()`` → list of Database(name, catalog, description, locationUri)
* ``getDatabase(db)`` → one Database; ``locationUri`` / ``description`` filled from namespace
  metadata (unlike listDatabases FA-2); missing schema → AnalysisException SCHEMA_NOT_FOUND;
  bare and ``catalog.db`` (incl. ``spark_catalog`` alias) spellings
* ``listTables(db)`` → list of Table(name, catalog, namespace as list[str], description,
  tableType, isTemporary); missing schema → AnalysisException SCHEMA_NOT_FOUND; temps always
  included
* ``currentCatalog()`` → ``spark_catalog`` by default; ``currentDatabase()`` → ``default``
* ``listCatalogs()`` → CatalogMetadata(name, description)
* ``tableExists``: True for ``db.table`` (2-part under current catalog) and temp views;
  False for missing table / missing db (never raises for absence)
* ``setCurrentCatalog(unknown)`` → CATALOG_NOT_FOUND; ``setCurrentDatabase(unknown)`` →
  SCHEMA_NOT_FOUND
* ``spark.sql.defaultCatalog`` defaults to ``spark_catalog``

RePark is **two-part-namespace** Iceberg (``catalog.namespace.table`` three-part identifiers).
R-CURCAT implements the Catalog list/current methods **facade-only** (no engine USE /
bare SHOW NAMESPACES) over:

* ``SHOW NAMESPACES IN`` — ``listDatabases`` / ``databaseExists`` / ``setCurrentDatabase``
* ``DESCRIBE NAMESPACE`` — ``getDatabase`` (location/comment; ``listDatabases`` stays FA-2)
* live Iceberg ``list_iceberg_table_names`` + session temp views — ``listTables``
  (not global ``information_schema.tables``)
"""

from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkTypeError, UnsupportedOperationException
from repark.spark.catalog import Catalog, CatalogMetadata, Database, Table


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-catalog-surface").getOrCreate()
    session.register_memory_catalog("glue_catalog", tmp_path)
    session.sql("CREATE NAMESPACE glue_catalog.ns1")
    session.sql("CREATE TABLE glue_catalog.ns1.entity AS SELECT 1 AS id, 'a' AS name")
    session.sql("CREATE NAMESPACE glue_catalog.ns2")
    return session


# ==================================================================================================
# Methods already on the facade — tableExists / dropTempView / clearCache
# ==================================================================================================


def test_table_exists_three_part_and_temp_view(spark: ReparkSession) -> None:
    """Present Iceberg table True; missing table/namespace False; temp view by bare name."""
    assert spark.catalog.tableExists("glue_catalog.ns1.entity") is True
    assert spark.catalog.table_exists("glue_catalog.ns1.entity") is True  # snake_case alias
    assert spark.catalog.tableExists("glue_catalog.ns1.nope") is False
    # Missing namespace → False (live Spark tableExists also False for missing db.table)
    assert spark.catalog.tableExists("glue_catalog.no_such_ns.t") is False
    assert spark.catalog.tableExists("tv") is False
    spark.sql("SELECT 1 AS n").createOrReplaceTempView("tv")
    assert spark.catalog.tableExists("tv") is True
    assert spark.catalog.dropTempView("tv") is True
    assert spark.catalog.tableExists("tv") is False


def test_table_exists_unknown_catalog_raises(spark: ReparkSession) -> None:
    """Unregistered catalog fails loud — a silent False would mask a wiring bug."""
    with pytest.raises(RuntimeError, match="unknown catalog"):
        spark.catalog.tableExists("nope.ns.t")


def test_table_exists_two_part_under_current_catalog(spark: ReparkSession) -> None:
    """R-CURCAT: 2-part ``db.table`` resolves under currentCatalog (flips G-INT divergence)."""
    spark.catalog.setCurrentCatalog("glue_catalog")
    assert spark.catalog.tableExists("ns1.entity") is True
    assert spark.catalog.tableExists("ns1.nope") is False


def test_table_exists_one_part_under_current_database(spark: ReparkSession) -> None:
    """R-CURCAT: bare name falls back to currentCatalog.currentDatabase.name after temps."""
    spark.catalog.setCurrentCatalog("glue_catalog")
    spark.catalog.setCurrentDatabase("ns1")
    assert spark.catalog.tableExists("entity") is True
    assert spark.catalog.tableExists("nope") is False


def test_catalog_facade_public_surface() -> None:
    """Pin the *implemented* public Catalog surface (camelCase + snake_case aliases)."""
    public = {name for name in dir(Catalog) if not name.startswith("_")}
    assert public == {
        "clearCache",
        "clear_cache",
        # r23 C6: SQL-UDF catalog surface
        "registerFunction",
        "register_function",
        "functionExists",
        "function_exists",
        "currentCatalog",
        "currentDatabase",
        "current_catalog",
        "current_database",
        "databaseExists",
        "database_exists",
        "getDatabase",
        "get_database",
        "dropTempView",
        "drop_temp_view",
        "listCatalogs",
        "listDatabases",
        "listTables",
        "list_catalogs",
        "list_databases",
        "list_tables",
        "setCurrentCatalog",
        "setCurrentDatabase",
        "set_current_catalog",
        "set_current_database",
        "tableExists",
        "table_exists",
    }


def test_clear_cache_and_drop_temp_view_aliases(spark: ReparkSession) -> None:
    spark.sql("SELECT 1 AS n").createOrReplaceTempView("v")
    assert spark.catalog.drop_temp_view("v") is True
    assert spark.catalog.dropTempView("v") is False  # already gone
    assert spark.catalog.clearCache() is None
    assert spark.catalog.clear_cache() is None


# ==================================================================================================
# R-CURCAT — current catalog / database / list* success pins
# ==================================================================================================


def test_current_catalog_defaults_then_tracks_register(spark: ReparkSession) -> None:
    """After register_memory_catalog, currentCatalog is the registered catalog (facade flip)."""
    assert spark.catalog.currentCatalog() == "glue_catalog"
    assert spark.catalog.current_catalog() == "glue_catalog"
    assert spark.catalog.currentDatabase() == "default"


def test_set_current_catalog_and_list_catalogs(spark: ReparkSession) -> None:
    catalogs = spark.catalog.listCatalogs()
    # R-AUTO-MEMCAT: the auto-registered spark_catalog is listed alongside the user catalog
    # (live PySpark also always lists spark_catalog); currentCatalog still flipped to the
    # user registration.
    assert set(catalogs) == {
        CatalogMetadata(name="glue_catalog", description=None),
        CatalogMetadata(name="spark_catalog", description=None),
    }
    assert catalogs[0]._fields == ("name", "description")
    spark.catalog.setCurrentCatalog("glue_catalog")  # idempotent
    assert spark.catalog.currentCatalog() == "glue_catalog"


def test_set_current_catalog_unknown_raises(spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException, match="CATALOG_NOT_FOUND"):
        spark.catalog.setCurrentCatalog("no_such_catalog_xyz")


def test_set_current_catalog_and_database_reject_non_str(spark: ReparkSession) -> None:
    """Non-str arguments raise PySparkTypeError (never coerce None→'None')."""
    with pytest.raises(PySparkTypeError, match="catalogName"):
        spark.catalog.setCurrentCatalog(None)  # type: ignore[arg-type]
    with pytest.raises(PySparkTypeError, match="dbName"):
        spark.catalog.setCurrentDatabase(123)  # type: ignore[arg-type]
    with pytest.raises(PySparkTypeError, match="dbName"):
        spark.catalog.databaseExists(None)  # type: ignore[arg-type]
    with pytest.raises(PySparkTypeError, match="dbName"):
        spark.catalog.getDatabase(None)  # type: ignore[arg-type]
    with pytest.raises(PySparkTypeError, match="tableName"):
        spark.catalog.tableExists(None)  # type: ignore[arg-type]


def test_list_databases_isolated_per_current_catalog(tmp_path: Path) -> None:
    """listDatabases only lists namespaces of currentCatalog (not a global mix)."""
    spark = ReparkSession.builder.appName("iso-cat").getOrCreate()
    try:
        spark.register_memory_catalog("cat_a", tmp_path / "a")
        spark.register_memory_catalog("cat_b", tmp_path / "b")
        spark.sql("CREATE NAMESPACE cat_a.only_a")
        spark.sql("CREATE NAMESPACE cat_b.only_b")
        spark.catalog.setCurrentCatalog("cat_a")
        assert [db.name for db in spark.catalog.listDatabases()] == ["only_a"]
        spark.catalog.setCurrentCatalog("cat_b")
        assert [db.name for db in spark.catalog.listDatabases()] == ["only_b"]
        assert spark.catalog.tableExists("only_b.ghost") is False
        spark.sql("CREATE TABLE cat_b.only_b.t AS SELECT 1 AS id")
        assert spark.catalog.tableExists("only_b.t") is True
        spark.catalog.setCurrentCatalog("cat_a")
        assert spark.catalog.tableExists("only_b.t") is False
    finally:
        spark.stop()


def test_list_databases_field_shape(spark: ReparkSession) -> None:
    """Database namedtuple fields match live 4.1.2; description/locationUri None (SHOW gap)."""
    spark.catalog.setCurrentCatalog("glue_catalog")
    dbs = spark.catalog.listDatabases()
    by_name = {db.name: db for db in dbs}
    assert set(by_name) == {"ns1", "ns2"}
    ns1 = by_name["ns1"]
    assert ns1 == Database(name="ns1", catalog="glue_catalog", description=None, locationUri=None)
    assert ns1._fields == ("name", "catalog", "description", "locationUri")


def test_list_databases_like_pattern(spark: ReparkSession) -> None:
    spark.catalog.setCurrentCatalog("glue_catalog")
    names = [db.name for db in spark.catalog.listDatabases(pattern="ns1")]
    assert names == ["ns1"]


def test_list_tables_filter_pattern(spark: ReparkSession) -> None:
    """listTables pattern uses Spark filterPattern (* wildcards, | alts) — C1-Q-001 pin."""
    spark.catalog.setCurrentCatalog("glue_catalog")
    spark.sql("CREATE TABLE glue_catalog.ns1.other AS SELECT 2 AS id")
    assert sorted(t.name for t in spark.catalog.listTables("ns1", pattern="entity")) == ["entity"]
    assert sorted(t.name for t in spark.catalog.listTables("ns1", pattern="*ent*")) == ["entity"]
    assert sorted(t.name for t in spark.catalog.listTables("ns1", pattern="entity|other")) == [
        "entity",
        "other",
    ]
    # Temps also filtered by the same pattern.
    spark.sql("SELECT 1 AS n").createOrReplaceTempView("tv_entity")
    temps = [t.name for t in spark.catalog.listTables("ns1", pattern="tv_*") if t.isTemporary]
    assert temps == ["tv_entity"]


def test_database_exists(spark: ReparkSession) -> None:
    spark.catalog.setCurrentCatalog("glue_catalog")
    assert spark.catalog.databaseExists("ns1") is True
    assert spark.catalog.database_exists("ns2") is True
    assert spark.catalog.databaseExists("nope_db") is False
    assert spark.catalog.databaseExists("glue_catalog.ns1") is True


def test_get_database_bare_and_qualified_shape(spark: ReparkSession) -> None:
    """getDatabase field shape (live 4.1.2 Database); bare and qualified spellings agree.

    A LOCATION-less namespace has ``locationUri is None`` / ``description is None`` — Spark's
    ``makeDatabase`` uses ``orNull`` when ``comment`` / ``location`` metadata is absent.
    """
    spark.catalog.setCurrentCatalog("glue_catalog")
    bare = spark.catalog.getDatabase("ns1")
    qualified = spark.catalog.get_database("glue_catalog.ns1")
    expected = Database(name="ns1", catalog="glue_catalog", description=None, locationUri=None)
    assert bare == expected
    assert qualified == expected
    assert bare._fields == ("name", "catalog", "description", "locationUri")
    aliased = spark.catalog.getDatabase("spark_catalog.ns1")
    assert aliased == expected


def test_get_database_returns_location_and_comment(tmp_path: Path) -> None:
    """getDatabase fills locationUri/description; listDatabases stays FA-2 (None)."""
    spark = ReparkSession.builder.appName("pytest-getdatabase-loc").getOrCreate()
    try:
        spark.register_memory_catalog("glue_catalog", tmp_path)
        spark.sql(
            "CREATE NAMESPACE glue_catalog.located COMMENT 'y3 comment' "
            "LOCATION 's3://bucket/y3/located'"
        )
        spark.catalog.setCurrentCatalog("glue_catalog")
        db = spark.catalog.getDatabase("located")
        assert db == Database(
            name="located",
            catalog="glue_catalog",
            description="y3 comment",
            locationUri="s3://bucket/y3/located",
        )
        qualified = spark.catalog.getDatabase("glue_catalog.located")
        assert qualified == db
        for listed in spark.catalog.listDatabases():
            if listed.name == "located":
                assert listed.locationUri is None
                assert listed.description is None
    finally:
        spark.stop()


def test_get_database_missing_raises_schema_not_found(spark: ReparkSession) -> None:
    """Missing namespace: AnalysisException + SCHEMA_NOT_FOUND via DESCRIBE (not SHOW).

    Existence is DESCRIBE's. The getDatabase message must match the DESCRIBE NAMESPACE
    sibling on the same name — restoring a SHOW ``_namespace_exists`` precheck (different
    text) or dropping DESCRIBE without an existence check turns this red.
    """
    spark.catalog.setCurrentCatalog("glue_catalog")
    with pytest.raises(AnalysisException) as described:
        spark.sql("DESCRIBE NAMESPACE glue_catalog.no_such_ns_xyz").to_arrow()
    described_text = str(described.value)
    assert "[SCHEMA_NOT_FOUND]" in described_text
    assert "no_such_ns_xyz" in described_text

    with pytest.raises(AnalysisException) as bare:
        spark.catalog.getDatabase("no_such_ns_xyz")
    assert str(bare.value) == described_text

    with pytest.raises(AnalysisException) as qualified:
        spark.catalog.getDatabase("glue_catalog.no_such_ns_xyz")
    assert str(qualified.value) == described_text


def test_get_database_does_not_show_precheck() -> None:
    """Q-001 / SEC-001: get_database must not SHOW-list existence (error swallow)."""
    import ast
    import inspect
    import textwrap

    tree = ast.parse(textwrap.dedent(inspect.getsource(Catalog.get_database)))
    call_names = [
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    ]
    assert "_namespace_exists" not in call_names
    literals: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            literals.append(node.value)
        elif isinstance(node, ast.JoinedStr):
            for value in node.values:
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    literals.append(value.value)
    assert any("DESCRIBE NAMESPACE" in literal for literal in literals)


def test_get_database_location_uri_matches_describe_probe(spark: ReparkSession) -> None:
    """getDatabase.locationUri equals DESCRIBE Location on the same memory session."""
    from _acceptance import probe_namespace_location_via_describe

    spark.sql(
        "CREATE NAMESPACE glue_catalog.located_q002 COMMENT 'y3 q002' "
        "LOCATION 's3://bucket/y3-q002/located'"
    )
    spark.catalog.setCurrentCatalog("glue_catalog")
    via_api = spark.catalog.getDatabase("located_q002").locationUri
    via_describe = probe_namespace_location_via_describe(spark, "glue_catalog", "located_q002")
    assert via_api == via_describe == "s3://bucket/y3-q002/located"


def test_set_current_database(spark: ReparkSession) -> None:
    spark.catalog.setCurrentCatalog("glue_catalog")
    spark.catalog.setCurrentDatabase("ns1")
    assert spark.catalog.currentDatabase() == "ns1"
    with pytest.raises(AnalysisException, match="SCHEMA_NOT_FOUND"):
        spark.catalog.setCurrentDatabase("nope_db_xyz")


def test_list_tables_field_shape_and_temps(spark: ReparkSession) -> None:
    """listTables includes MANAGED Iceberg table + TEMPORARY views; field shapes match oracle."""
    spark.catalog.setCurrentCatalog("glue_catalog")
    spark.sql("SELECT 1 AS n").createOrReplaceTempView("tv_oracle")
    tables = spark.catalog.listTables("ns1")
    by_name = {table.name: table for table in tables}
    assert "entity" in by_name
    entity = by_name["entity"]
    assert entity == Table(
        name="entity",
        catalog="glue_catalog",
        namespace=["ns1"],
        description=None,
        tableType="MANAGED",
        isTemporary=False,
    )
    assert entity._fields == (
        "name",
        "catalog",
        "namespace",
        "description",
        "tableType",
        "isTemporary",
    )
    assert "tv_oracle" in by_name
    temp = by_name["tv_oracle"]
    assert temp.isTemporary is True
    assert temp.tableType == "TEMPORARY"
    assert temp.catalog is None
    assert temp.namespace == []


def test_list_tables_missing_schema_raises(spark: ReparkSession) -> None:
    spark.catalog.setCurrentCatalog("glue_catalog")
    with pytest.raises(AnalysisException, match="SCHEMA_NOT_FOUND"):
        spark.catalog.listTables("missing_schema_xyz")


def test_list_tables_no_arg_uses_current_database(spark: ReparkSession) -> None:
    spark.catalog.setCurrentCatalog("glue_catalog")
    spark.catalog.setCurrentDatabase("ns1")
    names = {table.name for table in spark.catalog.listTables()}
    assert "entity" in names


def test_default_catalog_from_builder_config(tmp_path: Path) -> None:
    """spark.sql.defaultCatalog builder conf seeds currentCatalog before register."""
    session = (
        ReparkSession.builder.appName("default-cat")
        .config("spark.sql.defaultCatalog", "glue_catalog")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.type", "memory")
        .config("spark.sql.catalog.glue_catalog.warehouse", str(tmp_path))
        .getOrCreate()
    )
    try:
        assert session.catalog.currentCatalog() == "glue_catalog"
        assert "glue_catalog" in {c.name for c in session.catalog.listCatalogs()}
    finally:
        session.stop()


# ==================================================================================================
# Remaining disclosed divergences
# ==================================================================================================


def test_show_tables_in_not_implemented_divergence(spark: ReparkSession) -> None:
    """Pin for registry row ST-1 — semantics live only there.

    See ``docs/spark-sql-iceberg-parity.md`` §2.4
    [ST-1](../../../docs/spark-sql-iceberg-parity.md#st-1--show-tables-in-is-unimplemented).
    """
    with pytest.raises(UnsupportedOperationException, match="SHOW TABLES"):
        spark.sql("SHOW TABLES IN glue_catalog.ns1")


def test_list_databases_location_uri_none_divergence(spark: ReparkSession) -> None:
    """Pin for registry row FA-2 — semantics live only there.

    See ``docs/spark-sql-iceberg-parity.md`` §5
    [FA-2](../../../docs/spark-sql-iceberg-parity.md#fa-2--listdatabases-leaves-description-and-locationuri-as-none).
    """
    spark.catalog.setCurrentCatalog("glue_catalog")
    for db in spark.catalog.listDatabases():
        assert db.locationUri is None
        assert db.description is None


def test_show_namespaces_lists_registered_namespaces(spark: ReparkSession) -> None:
    """SQL sibling of listDatabases: SHOW NAMESPACES IN catalog returns the namespace column."""
    import pyarrow as pa

    table = spark.sql("SHOW NAMESPACES IN glue_catalog").to_arrow()
    names = sorted(row["namespace"] for row in table.to_pylist())
    assert names == ["ns1", "ns2"]
    assert table.schema.field("namespace").type == pa.string()

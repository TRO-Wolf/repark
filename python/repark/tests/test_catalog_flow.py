"""The source publish job's path, end to end from Python — the acceptance kernel.

Mirrors the real script's ``ensure_silver_table_exists`` + ``upsert_silver_df`` flow against the
in-memory Iceberg catalog: temp view → ``tableExists`` → CTAS (``USING iceberg`` +
``TBLPROPERTIES``) → ``MERGE INTO … UPDATE SET * / INSERT *`` → ``dropTempView`` /
``clearCache``. Requires the compiled wheel (``maturin develop``) — the real boundary, no mocks.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession

FQ_TABLE = "glue_catalog.example_silver.entity"

ICEBERG_TABLE_PROPERTIES = """
    'format-version' = 2,
    'write.delete.mode' = 'copy-on-write',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write'
"""


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-catalog").getOrCreate()
    session.register_memory_catalog("glue_catalog", tmp_path)
    session.sql("CREATE NAMESPACE glue_catalog.example_silver")
    return session


def _rows(spark: ReparkSession) -> list[dict[str, object]]:
    return spark.sql(f"SELECT id, name FROM {FQ_TABLE} ORDER BY id").to_arrow().to_pylist()


def test_silver_publish_flow(spark: ReparkSession) -> None:
    # ensure_silver_table_exists: temp view + tableExists gate + CTAS
    df = spark.sql("SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b'")
    df.createOrReplaceTempView("staging_view")
    assert not spark.catalog.tableExists(FQ_TABLE)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {FQ_TABLE} USING iceberg "
        f"TBLPROPERTIES ({ICEBERG_TABLE_PROPERTIES}) AS SELECT * FROM staging_view"
    )
    spark.catalog.clearCache()
    assert spark.catalog.tableExists(FQ_TABLE)
    assert spark.catalog.dropTempView("staging_view")

    # upsert_silver_df: the literal MERGE text the script generates
    updates = spark.sql("SELECT 2 AS id, 'bee' AS name UNION ALL SELECT 3, 'c'")
    updates.createOrReplaceTempView("staging_view")
    spark.sql(
        f"MERGE INTO {FQ_TABLE} AS Target USING staging_view AS Source "
        "ON Target.id = Source.id "
        "WHEN MATCHED THEN UPDATE SET * "
        "WHEN NOT MATCHED THEN INSERT *"
    )
    spark.catalog.clearCache()
    assert spark.catalog.dropTempView("staging_view")
    spark.catalog.clearCache()

    assert _rows(spark) == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "bee"},
        {"id": 3, "name": "c"},
    ]


def test_ctas_partitioned_by_end_to_end(spark: ReparkSession) -> None:
    # U1 (audit P0-1 / BUG-008+OTH-001): CTAS `PARTITIONED BY` must land a PARTITIONED table,
    # never a silently dropped one. Arrow export path (value AND type), a partition-filtered
    # read, and a read-back-after-reregister.
    table_name = "glue_catalog.example_silver.events"
    spark.sql(
        "SELECT 1 AS id, 'a' AS category UNION ALL SELECT 2, 'b' UNION ALL SELECT 3, 'a'"
    ).createOrReplaceTempView("iv_part_src")
    spark.sql(
        f"CREATE TABLE {table_name} USING iceberg PARTITIONED BY (category) "
        "AS SELECT * FROM iv_part_src"
    )

    table = spark.sql(f"SELECT id, category FROM {table_name} ORDER BY id").to_arrow()
    assert table.to_pylist() == [
        {"id": 1, "category": "a"},
        {"id": 2, "category": "b"},
        {"id": 3, "category": "a"},
    ]
    assert table.schema.field("id").type == pa.int64()
    assert table.schema.field("category").type == pa.string()

    # A partition-predicate read returns exactly the matching partition's rows.
    filtered = spark.sql(f"SELECT id FROM {table_name} WHERE category = 'a' ORDER BY id").to_arrow()
    assert filtered.to_pylist() == [{"id": 1}, {"id": 3}]

    # Read-back after re-register: cache cleared, identical rows.
    spark.catalog.clearCache()
    again = spark.sql(f"SELECT id, category FROM {table_name} ORDER BY id").to_arrow()
    assert again.to_pylist() == table.to_pylist()


def test_merge_partitioned_by_end_to_end(spark: ReparkSession) -> None:
    # WG-1 (A4): MERGE INTO an identity-partitioned table through the public `spark.sql`
    # facade. Arrow export path (value AND type): a matched UPDATE and a not-matched INSERT
    # (the star forms, the publish-job shape) commit through the fanout; a partition-predicate
    # read proves the inserted row is correctly partitioned.
    table_name = "glue_catalog.example_silver.part_entity"
    spark.sql(
        "SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b' UNION ALL SELECT 3, 'c'"
    ).createOrReplaceTempView("iv_part_src")
    spark.sql(
        f"CREATE TABLE {table_name} USING iceberg PARTITIONED BY (id) AS SELECT * FROM iv_part_src"
    )
    spark.sql("SELECT 2 AS id, 'bee' AS name UNION ALL SELECT 4, 'dee'").createOrReplaceTempView(
        "iv_updates"
    )
    spark.sql(
        f"MERGE INTO {table_name} AS Target USING iv_updates AS Source "
        "ON Target.id = Source.id "
        "WHEN MATCHED THEN UPDATE SET * "
        "WHEN NOT MATCHED THEN INSERT *"
    )

    table = spark.sql(f"SELECT id, name FROM {table_name} ORDER BY id").to_arrow()
    assert table.to_pylist() == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "bee"},
        {"id": 3, "name": "c"},
        {"id": 4, "name": "dee"},
    ]
    assert table.schema.field("id").type == pa.int64()
    assert table.schema.field("name").type == pa.string()

    # The inserted row (id=4) landed in its own partition — a partition-predicate read returns it
    # alone (an empty result would mean the merge wrote it unpartitioned / under the wrong key).
    filtered = spark.sql(f"SELECT name FROM {table_name} WHERE id = 4").to_arrow()
    assert filtered.to_pylist() == [{"name": "dee"}]


def test_temp_view_is_replaceable_and_droppable(spark: ReparkSession) -> None:
    spark.sql("SELECT 1 AS n").createOrReplaceTempView("v")
    assert spark.sql("SELECT n FROM v").to_arrow().to_pylist() == [{"n": 1}]
    # Re-registering under the same name replaces (the "OR REPLACE" contract).
    spark.sql("SELECT 2 AS n").createOrReplaceTempView("v")
    assert spark.sql("SELECT n FROM v").to_arrow().to_pylist() == [{"n": 2}]
    assert spark.catalog.dropTempView("v")
    # Dropping a missing view reports False, never raises (PySpark semantics).
    assert not spark.catalog.dropTempView("v")


def test_table_exists_semantics(spark: ReparkSession) -> None:
    # Absent table in a real namespace → False; absent namespace → False (PySpark).
    assert not spark.catalog.tableExists("glue_catalog.example_silver.nope")
    assert not spark.catalog.tableExists("glue_catalog.no_such_namespace.t")
    # One-part names check temp views.
    assert not spark.catalog.tableExists("v")
    spark.sql("SELECT 1 AS n").createOrReplaceTempView("v")
    assert spark.catalog.tableExists("v")
    # An unregistered catalog is an error — a silent False would mask a wiring bug.
    with pytest.raises(RuntimeError, match="unknown catalog"):
        spark.catalog.tableExists("nope.ns.t")


def test_snake_case_spellings_match(spark: ReparkSession) -> None:
    # The RePark-native snake_case spellings are the same functions as the PySpark camelCase.
    assert spark.catalog.table_exists is not None
    catalog = spark.catalog
    assert type(catalog).tableExists is type(catalog).table_exists
    assert type(catalog).dropTempView is type(catalog).drop_temp_view
    assert type(catalog).clearCache is type(catalog).clear_cache


def test_config_driven_catalog_publish_flow(tmp_path: Path) -> None:
    """The source publish job's config block drives the catalog via `.config(...)` alone.

    Mirrors the measured block shape (bare `SparkCatalog` class key, `catalog-impl`/warehouse/
    `io-impl`) but swaps `type = memory` for the real Glue `catalog-impl`, so the config map
    alone registers the catalog at `getOrCreate` — no `register_memory_catalog` call — proving
    the `spark.sql.catalog.<name>.*` mapping drives a real catalog end to end (namespace →
    CTAS → MERGE round-trip).
    """
    spark = (
        ReparkSession.builder.appName("process-silver")
        .config("spark.sql.catalog.glue_alt", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_alt.type", "memory")
        .config("spark.sql.catalog.glue_alt.warehouse", str(tmp_path))
        .config("spark.sql.catalog.glue_alt.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .getOrCreate()
    )
    spark.sql("CREATE NAMESPACE glue_alt.example_silver")
    table = "glue_alt.example_silver.entity"

    # ensure_silver_table_exists: temp view + tableExists gate + CTAS.
    spark.sql("SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b'").createOrReplaceTempView(
        "staging_view"
    )
    assert not spark.catalog.tableExists(table)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} USING iceberg "
        f"TBLPROPERTIES ({ICEBERG_TABLE_PROPERTIES}) AS SELECT * FROM staging_view"
    )
    assert spark.catalog.tableExists(table)
    assert spark.catalog.dropTempView("staging_view")

    # upsert_silver_df: the literal MERGE star text the script generates.
    spark.sql("SELECT 2 AS id, 'bee' AS name UNION ALL SELECT 3, 'c'").createOrReplaceTempView(
        "staging_view"
    )
    spark.sql(
        f"MERGE INTO {table} AS Target USING staging_view AS Source "
        "ON Target.id = Source.id "
        "WHEN MATCHED THEN UPDATE SET * "
        "WHEN NOT MATCHED THEN INSERT *"
    )
    assert spark.catalog.dropTempView("staging_view")

    rows = spark.sql(f"SELECT id, name FROM {table} ORDER BY id").to_arrow().to_pylist()
    assert rows == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "bee"},
        {"id": 3, "name": "c"},
    ]


def test_repark_prefixed_catalog_config_registers_identically(tmp_path: Path) -> None:
    # The repark-native `repark.sql.catalog.<name>.*` spelling drives the same registration
    # path as the Spark spelling — CTAS round-trip proves it.
    spark = (
        ReparkSession.builder.appName("repark-native-config")
        .config("repark.sql.catalog.native_cat.type", "memory")
        .config("repark.sql.catalog.native_cat.warehouse", str(tmp_path))
        .getOrCreate()
    )
    spark.sql("CREATE NAMESPACE native_cat.ns")
    spark.sql("SELECT 1 AS id").createOrReplaceTempView("iv_src")
    spark.sql("CREATE TABLE native_cat.ns.t AS SELECT * FROM iv_src")
    assert spark.catalog.tableExists("native_cat.ns.t")
    assert spark.sql("SELECT id FROM native_cat.ns.t").to_arrow().to_pylist() == [{"id": 1}]


def test_create_namespace_with_location_places_ctas_data_there(tmp_path: Path) -> None:
    # ADV-1: programmatic create_namespace threads a `location` property (SQL CREATE NAMESPACE
    # cannot), so a CTAS into the namespace writes under that path — proving the full
    # facade -> PyO3 -> session -> catalog path threads the location the acceptance harness
    # relies on. A dropped location would fall back to $TMPDIR and this rglob would be empty.
    spark = ReparkSession.builder.appName("pytest-create-namespace").getOrCreate()
    spark.register_memory_catalog("glue_catalog", tmp_path)
    namespace_location = tmp_path / "custom_ns_location"
    spark.create_namespace("glue_catalog", "silver", location=str(namespace_location))

    spark.sql("CREATE TABLE glue_catalog.silver.t AS SELECT 1 AS id")
    assert spark.sql("SELECT id FROM glue_catalog.silver.t").to_arrow().to_pylist() == [{"id": 1}]
    assert any(namespace_location.rglob("*.parquet")), (
        "CTAS data must land under the namespace `location` set by create_namespace"
    )


def test_ctas_into_location_uri_only_namespace_places_data_there(tmp_path: Path) -> None:
    # U2 (audit BUG-001): a PRE-EXISTING real Glue database surfaces through the fork's catalog
    # with ONLY the `location_uri` property (fork glue/utils.rs maps Glue's canonical
    # locationUri to it) — no RePark-written `location` — so CTAS location resolution must
    # fall back to `location_uri`. The single-key shape is constructed via `WITH DBPROPERTIES`
    # (the mirror is unidirectional, so no `location` twin is synthesized). Without the
    # fallback the memory catalog would silently place the data under $TMPDIR and the rglob
    # below would be empty. Value AND Arrow type checked on the `to_arrow` export.
    spark = ReparkSession.builder.appName("pytest-location-uri-only").getOrCreate()
    spark.register_memory_catalog("glue_catalog", tmp_path)
    glue_db_location = tmp_path / "pre_existing_glue_db"
    spark.sql(
        f"CREATE NAMESPACE glue_catalog.legacy "
        f"WITH DBPROPERTIES ('location_uri' = '{glue_db_location}')"
    )

    spark.sql("CREATE TABLE glue_catalog.legacy.t AS SELECT 1 AS id")
    table = spark.sql("SELECT id FROM glue_catalog.legacy.t").to_arrow()
    assert table.to_pylist() == [{"id": 1}]
    assert table.schema.field("id").type == pa.int32()
    assert any(glue_db_location.rglob("*.parquet")), (
        "CTAS data must land under the namespace `location_uri` (the pre-existing Glue DB shape)"
    )


def test_sql_create_namespace_location_places_ctas_data_there(tmp_path: Path) -> None:
    # WG-5 (ADV-2 residual): SQL `CREATE NAMESPACE … LOCATION` sets the namespace warehouse
    # path through the public `spark.sql` facade. A CTAS into the namespace lands its data
    # under that path (value AND Arrow type checked on the `to_arrow` export); an empty rglob
    # would mean the SQL LOCATION was dropped and the data fell back to $TMPDIR.
    spark = ReparkSession.builder.appName("pytest-sql-create-namespace").getOrCreate()
    spark.register_memory_catalog("glue_catalog", tmp_path)
    namespace_location = tmp_path / "sql_ns_location"
    spark.sql(f"CREATE NAMESPACE glue_catalog.silver LOCATION '{namespace_location}'")

    spark.sql("CREATE TABLE glue_catalog.silver.t AS SELECT 1 AS id")
    table = spark.sql("SELECT id FROM glue_catalog.silver.t").to_arrow()
    assert table.to_pylist() == [{"id": 1}]
    assert table.schema.field("id").type == pa.int32()
    assert any(namespace_location.rglob("*.parquet")), (
        "CTAS data must land under the namespace `location` set by SQL CREATE NAMESPACE … LOCATION"
    )


def test_conflicting_prefix_spellings_raise(tmp_path: Path) -> None:
    # The same property under both spellings with DIFFERENT values is a fail-loud conflict
    # naming both keys — never an iteration-order-dependent silent pick. Raw values must not
    # appear in the error (catalog props can carry credentials).
    path_a = str(tmp_path / "a")
    path_b = str(tmp_path / "b")
    with pytest.raises(RuntimeError, match=r"conflicting catalog config") as raised:
        (
            ReparkSession.builder.config("spark.sql.catalog.c.warehouse", path_a)
            .config("repark.sql.catalog.c.warehouse", path_b)
            .config("spark.sql.catalog.c.type", "memory")
            .getOrCreate()
        )
    message = str(raised.value)
    assert "spark.sql.catalog.c.warehouse" in message
    assert "repark.sql.catalog.c.warehouse" in message
    assert path_a not in message and path_b not in message


def test_config_bad_catalog_block_raises_at_get_or_create(tmp_path: Path) -> None:
    # A malformed catalog block fails loud at getOrCreate (build-time parse), naming the key.
    with pytest.raises(RuntimeError, match=r"spark\.sql\.catalog\.glue_alt\.type"):
        (
            ReparkSession.builder.config("spark.sql.catalog.glue_alt.type", "hive")
            .config("spark.sql.catalog.glue_alt.warehouse", str(tmp_path))
            .getOrCreate()
        )

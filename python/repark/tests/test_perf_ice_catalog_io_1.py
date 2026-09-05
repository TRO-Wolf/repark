from __future__ import annotations

import os
import time
from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession, _native

_CACHE_KEY = "repark.iceberg.metadataCache"
_ENTRIES_KEY = "repark.iceberg.metadataCacheEntries"
_CACHE_KEY_ALT = "repark.iceberg.metadata_cache"
_ENTRIES_KEY_ALT = "repark.iceberg.metadata_cache_entries"
_MANIFEST_KEY = "repark.iceberg.manifestCacheBytes"
_MANIFEST_KEY_ALT = "repark.iceberg.manifest_cache_bytes"
_ALLOW_CREATE_V3_KEY = "repark.sql.allowCreateFormatVersion3"
_UPGRADE_TO_V3 = "ALTER TABLE ice.ns.up SET TBLPROPERTIES ('format-version' = '3')"
_ACCEPTANCE_ENV = "REPARK_AWS_ACCEPTANCE"
_MANIFEST_TARGET_MS = 20.0
_MANY_APPENDS = 48
_MANY_ROWS = 480


def _census(spark: ReparkSession) -> tuple[bool, int, int, int, int]:
    return _native.iceberg_metadata_cache_census(spark._ensure_alive())


def _body_fetches(spark: ReparkSession) -> int:
    return _census(spark)[3]


def _session(app: str, tmp_path: Path, **conf: str) -> ReparkSession:
    builder = ReparkSession.builder.appName(app)
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    warehouse = tmp_path / app
    warehouse.mkdir(parents=True, exist_ok=True)
    spark.register_memory_catalog("ice", str(warehouse))
    spark.sql("CREATE NAMESPACE ice.ns")
    return spark


def _range_view(spark: ReparkSession, rows: int) -> None:
    spark.sql(
        f"SELECT value AS id, CAST(value % 7 AS INT) AS vi FROM range(0, {rows}) AS r(value)"
    ).createOrReplaceTempView("src")


def _seed(spark: ReparkSession, table: str, rows: int = 300) -> None:
    _range_view(spark, rows)
    spark.sql(f"CREATE TABLE ice.ns.{table} (id BIGINT, vi INT) USING iceberg").to_arrow()
    spark.sql(f"INSERT INTO ice.ns.{table} SELECT * FROM src").to_arrow()


def _scalar(spark: ReparkSession, sql: str) -> int:
    return int(spark.sql(sql).to_arrow().column("c")[0].as_py())


def test_a_repeated_read_on_an_unmoved_table_parses_no_metadata_document(
    tmp_path: Path,
) -> None:
    spark = _session("census_read", tmp_path)
    _seed(spark, "t")
    spark.sql("SELECT count(*) AS c FROM ice.ns.t").to_arrow()

    before = _body_fetches(spark)
    spark.sql("SELECT count(*) AS c FROM ice.ns.t").to_arrow()
    spark.sql("SELECT id FROM ice.ns.t WHERE vi = 3").to_arrow()

    assert _body_fetches(spark) == before, (
        "two statements on an unmoved pointer must parse no metadata document"
    )


def test_dml_parses_no_metadata_document_beyond_the_commit_it_writes(
    tmp_path: Path,
) -> None:
    spark = _session("census_dml", tmp_path)
    _seed(spark, "t")
    spark.sql("SELECT * FROM src WHERE id < 20").createOrReplaceTempView("small")
    spark.sql("SELECT count(*) AS c FROM ice.ns.t").to_arrow()

    for statement in (
        "INSERT INTO ice.ns.t SELECT * FROM small",
        "DELETE FROM ice.ns.t WHERE id % 97 = 5",
        "UPDATE ice.ns.t SET vi = vi + 1 WHERE id % 89 = 7",
        "MERGE INTO ice.ns.t t USING small s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET t.vi = s.vi + 2 "
        "WHEN NOT MATCHED THEN INSERT *",
    ):
        before = _body_fetches(spark)
        spark.sql(statement).to_arrow()
        assert _body_fetches(spark) == before, (
            f"{statement.split()[0]} must parse no metadata document"
        )


def test_a_commit_is_visible_to_the_statement_that_follows_it(tmp_path: Path) -> None:
    spark = _session("census_stale", tmp_path)
    _seed(spark, "t", rows=10)
    assert _scalar(spark, "SELECT count(*) AS c FROM ice.ns.t") == 10

    spark.sql("INSERT INTO ice.ns.t VALUES (99, 1)").to_arrow()

    assert _scalar(spark, "SELECT count(*) AS c FROM ice.ns.t") == 11
    assert _scalar(spark, "SELECT count(*) AS c FROM ice.ns.t WHERE id = 99") == 1


def test_a_schema_change_is_seen_by_the_statement_that_follows_it(tmp_path: Path) -> None:
    spark = _session("census_schema", tmp_path)
    _seed(spark, "t", rows=10)
    assert len(spark.sql("SELECT * FROM ice.ns.t").to_arrow().schema) == 2

    spark.sql("ALTER TABLE ice.ns.t ADD COLUMNS (grade INT)").to_arrow()

    assert len(spark.sql("SELECT * FROM ice.ns.t").to_arrow().schema) == 3


def test_the_knob_off_reconstructs_the_pre_unit_load_path(tmp_path: Path) -> None:
    spark = _session("census_off", tmp_path, **{_CACHE_KEY: "false"})
    _seed(spark, "t", rows=10)

    enabled, _hits, _misses, fetches, entries = _census(spark)

    assert enabled is False
    assert (fetches, entries) == (0, 0)
    assert _scalar(spark, "SELECT count(*) AS c FROM ice.ns.t") == 10


def test_the_retained_location_bound_is_trimmed_at_the_statement_door(
    tmp_path: Path,
) -> None:
    spark = _session("census_bound", tmp_path, **{_CACHE_KEY: "true", _ENTRIES_KEY: "3"})
    _seed(spark, "t", rows=10)

    for index in range(12):
        spark.sql(f"CREATE TABLE ice.ns.t{index} (id BIGINT, vi INT) USING iceberg").to_arrow()
        assert _census(spark)[4] <= 4, "the statement door must hold the retained-entry bound"

    assert _scalar(spark, "SELECT count(*) AS c FROM ice.ns.t") == 10


@pytest.mark.parametrize(
    ("key", "value"),
    [
        (_CACHE_KEY, "yes"),
        (_ENTRIES_KEY, "0"),
        (_ENTRIES_KEY, "many"),
        (_MANIFEST_KEY, "many"),
        (_MANIFEST_KEY, "-1"),
    ],
)
def test_a_bad_cache_knob_fails_loud_naming_the_key(key: str, value: str) -> None:
    with pytest.raises(Exception, match=key.replace(".", r"\.")):
        ReparkSession.builder.appName("census_bad").config(key, value).getOrCreate()


@pytest.mark.parametrize(
    ("alias", "canonical", "value"),
    [
        (_CACHE_KEY_ALT, _CACHE_KEY, "yes"),
        (_ENTRIES_KEY_ALT, _ENTRIES_KEY, "0"),
        (_MANIFEST_KEY_ALT, _MANIFEST_KEY, "many"),
    ],
)
def test_a_bad_underscore_alias_names_the_key_the_user_set_and_the_canonical_one(
    alias: str, canonical: str, value: str
) -> None:
    with pytest.raises(Exception) as caught:
        ReparkSession.builder.appName("census_alias").config(alias, value).getOrCreate()
    message = str(caught.value)
    assert alias in message, "the message must name the key the user actually set"
    assert canonical in message, "and the canonical spelling it aliases"


def test_the_metadata_cache_bound_is_a_statement_door_clear_not_a_per_load_bound(
    tmp_path: Path,
) -> None:
    spark = _session("census_scope", tmp_path, **{_CACHE_KEY: "true", _ENTRIES_KEY: "1"})
    _range_view(spark, 4)
    for index in range(8):
        spark.sql(f"CREATE TABLE ice.ns.u{index} AS SELECT * FROM src").to_arrow()

    union = " UNION ALL ".join(f"SELECT id FROM ice.ns.u{index}" for index in range(8))
    assert _scalar(spark, f"SELECT count(*) AS c FROM ({union})") == 32
    assert _census(spark)[4] == 8, (
        "one statement over N tables retains N: the bound is checked at the statement door"
    )

    assert _scalar(spark, "SELECT count(*) AS c FROM ice.ns.u3") == 4
    assert _census(spark)[4] <= 1, "the next statement door brings it back under the bound"


def _build_many(spark: ReparkSession, table: str, appends: int = _MANY_APPENDS) -> None:
    per = _MANY_ROWS // appends
    spark.sql(f"CREATE TABLE ice.ns.{table} (id BIGINT, vi INT) USING iceberg").to_arrow()
    for index in range(appends):
        low = index * per
        high = low + per
        spark.sql(
            f"INSERT INTO ice.ns.{table} SELECT * FROM src WHERE id >= {low} AND id < {high}"
        ).to_arrow()


def _second_statement_ms(spark: ReparkSession, table: str) -> float:
    sql = f"SELECT count(id) AS c FROM ice.ns.{table}"
    spark.sql(sql).to_arrow()
    start = time.perf_counter()
    spark.sql(sql).to_arrow()
    return (time.perf_counter() - start) * 1000.0


def test_many_manifests_answer_equal_to_one_merged_manifest(tmp_path: Path) -> None:
    spark = _session("manifests", tmp_path)
    _range_view(spark, _MANY_ROWS)
    _build_many(spark, "t_many")
    _build_many(spark, "t_many_merged")
    spark.sql("CALL ice.system.rewrite_manifests(table => 'ns.t_many_merged')").to_arrow()

    many = _scalar(spark, "SELECT count(id) AS c FROM ice.ns.t_many")
    merged = _scalar(spark, "SELECT count(id) AS c FROM ice.ns.t_many_merged")

    assert many == merged == _MANY_ROWS


@pytest.mark.skipif(
    _native.__debug_assertions__,
    reason="the wall-clock target holds on a release module only; the delete-manifest pins "
    "guard the cache on every build",
)
def test_the_second_statement_on_a_many_manifest_table_is_under_the_target(
    tmp_path: Path,
) -> None:
    spark = _session("manifest_timing", tmp_path, **{_MANIFEST_KEY: "33554432"})
    _range_view(spark, _MANY_ROWS)
    _build_many(spark, "t_many")

    assert _second_statement_ms(spark, "t_many") <= _MANIFEST_TARGET_MS


def _manifest_files(warehouse: Path) -> set[Path]:
    return set(warehouse.rglob("*.avro"))


def _delete_manifests(warehouse: Path) -> int:
    paths = _manifest_files(warehouse)
    for path in paths:
        path.unlink()
    return len(paths)


def test_an_explicit_session_answers_from_the_shared_cache_after_manifests_vanish(
    tmp_path: Path,
) -> None:
    spark = _session("manifest_explicit", tmp_path, **{_MANIFEST_KEY: "33554432"})
    _seed(spark, "t", rows=10)
    assert _scalar(spark, "SELECT count(id) AS c FROM ice.ns.t") == 10

    removed = _delete_manifests(tmp_path / "manifest_explicit")

    assert removed > 0, "the pin means nothing without a manifest to delete"
    assert _scalar(spark, "SELECT count(id) AS c FROM ice.ns.t") == 10


def test_a_default_session_reopens_manifests_after_they_vanish(tmp_path: Path) -> None:
    spark = _session("manifest_default", tmp_path)
    _seed(spark, "t", rows=10)
    assert _scalar(spark, "SELECT count(id) AS c FROM ice.ns.t") == 10

    removed = _delete_manifests(tmp_path / "manifest_default")

    assert removed > 0, "the pin means nothing without a manifest to delete"
    with pytest.raises(Exception, match="manifest"):
        spark.sql("SELECT count(id) AS c FROM ice.ns.t").to_arrow()


def test_zero_manifest_bytes_makes_a_repeated_read_open_manifests_again(
    tmp_path: Path,
) -> None:
    spark = _session("manifest_off", tmp_path, **{_MANIFEST_KEY: "0"})
    _seed(spark, "t", rows=10)
    assert _scalar(spark, "SELECT count(id) AS c FROM ice.ns.t") == 10

    removed = _delete_manifests(tmp_path / "manifest_off")

    assert removed > 0, "the pin means nothing without a manifest to delete"
    with pytest.raises(Exception, match="manifest"):
        spark.sql("SELECT count(id) AS c FROM ice.ns.t").to_arrow()


def test_after_rewrite_and_expire_the_next_read_needs_only_new_manifest_paths(
    tmp_path: Path,
) -> None:
    spark = _session("manifest_rewrite", tmp_path, **{_MANIFEST_KEY: "33554432"})
    _range_view(spark, _MANY_ROWS)
    _build_many(spark, "t")
    assert _scalar(spark, "SELECT count(id) AS c FROM ice.ns.t") == _MANY_ROWS

    warehouse = tmp_path / "manifest_rewrite"
    before = _manifest_files(warehouse)
    spark.sql("CALL ice.system.rewrite_manifests(table => 'ns.t')").to_arrow()
    after = _manifest_files(warehouse)

    assert after - before, "rewrite must write new manifest paths or the pin is vacuous"

    spark.sql(
        "CALL ice.system.expire_snapshots("
        "table => 'ns.t', older_than => 9999999999999, retain_last => 1)"
    ).to_arrow()

    assert not any(path.exists() for path in before), (
        "expire must remove every pre-rewrite manifest path from disk"
    )
    assert _scalar(spark, "SELECT count(id) AS c FROM ice.ns.t") == _MANY_ROWS


def test_a_merge_after_a_commit_matches_the_committed_row(tmp_path: Path) -> None:
    spark = _session("manifest_merge", tmp_path, **{_MANIFEST_KEY: "33554432"})
    _seed(spark, "t", rows=10)
    spark.sql("INSERT INTO ice.ns.t VALUES (99, 1)").to_arrow()
    spark.sql("SELECT 99 AS id, 0 AS vi").createOrReplaceTempView("one")
    spark.sql(
        "MERGE INTO ice.ns.t t USING one s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.vi = 100"
    ).to_arrow()

    assert _scalar(spark, "SELECT vi AS c FROM ice.ns.t WHERE id = 99") == 100


def test_a_dropped_and_recreated_table_answers_its_own_rows(tmp_path: Path) -> None:
    spark = _session("manifest_drop", tmp_path, **{_MANIFEST_KEY: "33554432"})
    _seed(spark, "t", rows=10)
    assert _scalar(spark, "SELECT count(id) AS c FROM ice.ns.t") == 10

    spark.sql("DROP TABLE ice.ns.t").to_arrow()
    _seed(spark, "t", rows=20)

    assert _scalar(spark, "SELECT count(id) AS c FROM ice.ns.t") == 20


def test_a_registered_table_stays_correct_across_a_commit(tmp_path: Path) -> None:
    spark = _session("manifest_register", tmp_path, **{_MANIFEST_KEY: "33554432"})
    _seed(spark, "t", rows=10)
    pointers = sorted((tmp_path / "manifest_register").rglob("*.metadata.json"))

    assert pointers, "the seed must write a metadata pointer to adopt"

    spark.sql(
        f"CALL ice.system.register_table(table => 'ns.adopted', metadata_file => '{pointers[-1]}')"
    ).to_arrow()

    assert _scalar(spark, "SELECT count(id) AS c FROM ice.ns.adopted") == 10

    spark.sql("INSERT INTO ice.ns.adopted VALUES (99, 1)").to_arrow()

    assert _scalar(spark, "SELECT count(id) AS c FROM ice.ns.adopted") == 11


def test_a_tiny_byte_budget_still_answers_across_many_tables(tmp_path: Path) -> None:
    spark = _session("manifest_bound", tmp_path, **{_MANIFEST_KEY: "512"})
    _range_view(spark, 4)
    for index in range(8):
        spark.sql(f"CREATE TABLE ice.ns.b{index} AS SELECT * FROM src").to_arrow()

    for index in range(8):
        assert _scalar(spark, f"SELECT count(id) AS c FROM ice.ns.b{index}") == 4


def test_time_travel_reads_the_pinned_snapshot_with_the_cache_on(
    tmp_path: Path,
) -> None:
    spark = _session("manifest_timetravel", tmp_path, **{_MANIFEST_KEY: "33554432"})
    _seed(spark, "t", rows=10)
    first = spark._testing_list_snapshots("ice.ns.t")[-1][0]
    spark.sql("INSERT INTO ice.ns.t VALUES (99, 1)").to_arrow()

    assert _scalar(spark, f"SELECT count(id) AS c FROM ice.ns.t VERSION AS OF {first}") == 10
    assert _scalar(spark, "SELECT count(id) AS c FROM ice.ns.t") == 11


def test_branch_reads_answer_with_the_cache_on(tmp_path: Path) -> None:
    spark = _session("manifest_branch", tmp_path, **{_MANIFEST_KEY: "33554432"})
    _seed(spark, "t", rows=10)
    first = spark._testing_list_snapshots("ice.ns.t")[-1][0]
    spark.sql(f"CREATE BRANCH kept IN ice.ns.t AS OF VERSION {first}").to_arrow()
    spark.sql("INSERT INTO ice.ns.t VALUES (99, 1)").to_arrow()

    assert _scalar(spark, "SELECT count(id) AS c FROM ice.ns.t VERSION AS OF 'kept'") == 10
    assert _scalar(spark, "SELECT count(id) AS c FROM ice.ns.t") == 11


def _seed_v2_upgrade_table(spark: ReparkSession) -> None:
    spark.sql(
        "CREATE TABLE ice.ns.up (id INT, name STRING) USING iceberg "
        "TBLPROPERTIES ('format-version' = '2')"
    ).to_arrow()
    spark.sql("INSERT INTO ice.ns.up VALUES (1, 'a'), (2, 'b'), (3, 'c')").to_arrow()


def _lineage_triples(spark: ReparkSession, table: str) -> list[tuple[int, int | None, int | None]]:
    arrow = spark.sql(
        f"SELECT id, _row_id, _last_updated_sequence_number FROM {table} ORDER BY id"
    ).to_arrow()
    assert arrow.schema.field("_row_id").type == pa.int64(), arrow.schema
    assert arrow.schema.field("_last_updated_sequence_number").type == pa.int64(), arrow.schema
    return list(
        zip(
            [int(value) for value in arrow.column("id").to_pylist()],
            arrow.column("_row_id").to_pylist(),
            arrow.column("_last_updated_sequence_number").to_pylist(),
            strict=True,
        )
    )


def test_with_the_knob_on_an_upgraded_table_reads_null_lineage_for_carried_rows(
    tmp_path: Path,
) -> None:
    """Detector for PERF-CATALOG-LINEAGE-CACHE-1: pins today's wrong answer, reds on the fix."""
    spark = _session(
        "manifest_lineage_on",
        tmp_path,
        **{_ALLOW_CREATE_V3_KEY: "true", _MANIFEST_KEY: "33554432"},
    )
    _seed_v2_upgrade_table(spark)
    assert _scalar(spark, "SELECT count(id) AS c FROM ice.ns.up") == 3
    spark.sql(_UPGRADE_TO_V3).to_arrow()
    spark.sql("INSERT INTO ice.ns.up VALUES (4, 'd'), (5, 'e')").to_arrow()

    assert _lineage_triples(spark, "ice.ns.up") == [
        (1, None, None),
        (2, None, None),
        (3, None, None),
        (4, 0, 2),
        (5, 1, 2),
    ]


def test_with_the_knob_off_an_upgraded_table_reads_assigned_lineage_for_carried_rows(
    tmp_path: Path,
) -> None:
    """Sibling control: the same upgrade without the shared cache serves assigned lineage."""
    spark = _session(
        "manifest_lineage_off",
        tmp_path,
        **{_ALLOW_CREATE_V3_KEY: "true", _MANIFEST_KEY: "0"},
    )
    _seed_v2_upgrade_table(spark)
    assert _scalar(spark, "SELECT count(id) AS c FROM ice.ns.up") == 3
    spark.sql(_UPGRADE_TO_V3).to_arrow()
    spark.sql("INSERT INTO ice.ns.up VALUES (4, 'd'), (5, 'e')").to_arrow()

    assert _lineage_triples(spark, "ice.ns.up") == [
        (1, 2, 1),
        (2, 3, 1),
        (3, 4, 1),
        (4, 0, 2),
        (5, 1, 2),
    ]


@pytest.mark.skipif(
    os.environ.get(_ACCEPTANCE_ENV) != "1",
    reason="Glue leg runs under the AWS acceptance gate only",
)
@pytest.mark.skip(
    reason="PERF-ICE-CATALOG-IO-1 part 2 for Glue is fork-gated: GlueCatalogBuilder takes no "
    "with_table_metadata_cache at fork pin 189a73ed (fork ask F-CATIO-AWS)."
)
def test_glue_parses_no_metadata_document_for_an_unchanged_pointer() -> None:
    raise AssertionError("unreachable while the leg is skipped")


@pytest.mark.skipif(
    os.environ.get(_ACCEPTANCE_ENV) != "1",
    reason="S3 Tables leg runs under the AWS acceptance gate only",
)
@pytest.mark.skip(
    reason="PERF-ICE-CATALOG-IO-1 part 2 for S3 Tables is fork-gated: S3TablesCatalogBuilder "
    "takes no with_table_metadata_cache at fork pin 189a73ed (fork ask F-CATIO-AWS)."
)
def test_s3tables_parses_no_metadata_document_for_an_unchanged_pointer() -> None:
    raise AssertionError("unreachable while the leg is skipped")

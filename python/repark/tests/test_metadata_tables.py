"""I2 / R-METADATA-TABLES oracle — Spark-style Iceberg metadata tables.

Resolution only: ``cat.ns.tbl.snapshots`` (+ history/files/manifests/partitions/refs/
entries/metadata_log_entries/all_* family) and ``spark.table("<tbl>.files")`` rewrite
onto the fork's ``table$meta`` DataFusion providers (R142).

Pins:
- column NAMES + Arrow types for core tables (schema from fork inspect sources)
- row content sanity on a ≥3-snapshot fixture
- real table literally named ``files`` wins over suffix interpretation
- DML targeting a metadata table is loud
- AS OF + metadata composition is out of scope v1 (loud disclose)
- known fork residues (empty ``partition`` on unpartitioned; readable_metrics by name)

Fork pin ``4723104b``:
- ``MetadataTableType`` — ``crates/iceberg/src/inspect/metadata_table.rs:35-121``
- DF ``name.split_once('$')`` — ``integrations/datafusion/src/schema.rs:151-171``
- snapshots schema — ``inspect/snapshots.rs:49-73``
- history schema — ``inspect/history.rs:50-63``
- files/data_file columns — ``inspect/data_file.rs:67-`` + ``files.rs``
- unpartitioned empty ``partition`` divergence — ``inspect/partitions.rs:31-37`` (R142)
- readable_metrics by name — ``inspect/readable_metrics.rs`` / entries/files (R142)

Local memory-catalog only (no AWS, no docker).
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException

TABLE = "mem.ns.events"
COW = """
    'format-version' = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write'
"""

# Fork inspect/snapshots.rs:49-73 (Iceberg → Arrow).
SNAPSHOTS_SCHEMA: list[tuple[str, str]] = [
    ("committed_at", "timestamp[us, tz=UTC]"),
    ("snapshot_id", "int64"),
    ("parent_id", "int64"),
    ("operation", "string"),
    ("manifest_list", "string"),
    ("summary", "map<string, string>"),
]

# Fork inspect/history.rs:50-63.
HISTORY_SCHEMA: list[tuple[str, str]] = [
    ("made_current_at", "timestamp[us, tz=UTC]"),
    ("snapshot_id", "int64"),
    ("parent_id", "int64"),
    ("is_current_ancestor", "bool"),
]

# Fork inspect/refs.rs:47-66.
REFS_SCHEMA_NAMES = [
    "name",
    "type",
    "snapshot_id",
    "max_reference_age_in_ms",
    "min_snapshots_to_keep",
    "max_snapshot_age_in_ms",
]

# Core files-family top-level names (fork inspect/data_file.rs) + readable_metrics.
FILES_CORE_NAMES = [
    "content",
    "file_path",
    "file_format",
    "spec_id",
    "partition",
    "record_count",
    "file_size_in_bytes",
]


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-metadata-tables").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def _schema_names_types(table: pa.Table) -> list[tuple[str, str]]:
    return [(field.name, str(field.type)) for field in table.schema]


def _schema_names(table: pa.Table) -> list[str]:
    return [field.name for field in table.schema]


@pytest.fixture
def multi_snapshot(spark: ReparkSession) -> dict[str, object]:
    """≥3 snapshots via CTAS + INSERT + MERGE (same shape as I1 fixture)."""
    spark.sql(
        f"CREATE TABLE {TABLE} USING iceberg TBLPROPERTIES ({COW}) AS "
        "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)"
    )
    snaps = spark._testing_list_snapshots(TABLE)
    assert len(snaps) >= 1
    s1 = snaps[-1][0]

    spark.sql(f"INSERT INTO {TABLE} SELECT 4 AS id, 'd' AS name")
    snaps = spark._testing_list_snapshots(TABLE)
    s2 = snaps[-1][0]
    assert s2 != s1

    spark.sql(
        "SELECT 2 AS id, 'bee' AS name UNION ALL SELECT 5 AS id, 'e' AS name"
    ).createOrReplaceTempView("upd")
    spark.sql(
        f"MERGE INTO {TABLE} AS t USING upd AS s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET t.name = s.name "
        "WHEN NOT MATCHED THEN INSERT *"
    )
    snaps = spark._testing_list_snapshots(TABLE)
    s3 = snaps[-1][0]
    assert s3 != s2
    assert len(snaps) >= 3

    spark._testing_create_ref(TABLE, "tag", "tag_s1", s1)
    spark._testing_create_ref(TABLE, "branch", "branch_s2", s2)

    return {
        "s1": s1,
        "s2": s2,
        "s3": s3,
        "ids_current": [1, 2, 3, 4, 5],
        "snapshot_count": len(snaps),
    }


def test_snapshots_schema_and_count(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    arrow = spark.sql(f"SELECT * FROM {TABLE}.snapshots").to_arrow()
    names_types = _schema_names_types(arrow)
    # Map Arrow rendering may include entry/key_value naming; pin names + scalar types strictly
    # and accept map type containing string→string.
    assert [name for name, _ in names_types] == [name for name, _ in SNAPSHOTS_SCHEMA]
    for (name, expected_type), (_, actual_type) in zip(SNAPSHOTS_SCHEMA, names_types, strict=True):
        if name == "summary":
            assert "map" in actual_type and "string" in actual_type
        elif name == "committed_at":
            assert "timestamp" in actual_type
        else:
            assert actual_type == expected_type, f"{name}: {actual_type} != {expected_type}"
    assert arrow.num_rows >= 3
    assert arrow.num_rows == multi_snapshot["snapshot_count"]
    assert set(arrow.column("snapshot_id").to_pylist()) >= {
        multi_snapshot["s1"],
        multi_snapshot["s2"],
        multi_snapshot["s3"],
    }


def test_snapshots_count_show_and_partial_projection(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """r25 morning critic pin — the user-reported empty-projection class at the facade.

    ``.count()`` plans a zero-column projection over the metadata provider (the exact
    "Physical … N vs (logical) 0" Internal-error class); the styled ``.show()`` routes
    through ``count()`` first, which is how the report surfaced. Values AND types pinned
    on the Arrow path per the divergence-class rule.
    """
    df = spark.sql(f"SELECT * FROM {TABLE}.snapshots")
    assert df.count() == multi_snapshot["snapshot_count"]
    df.show()  # plain style must not raise
    spark.conf.set("repark.display.style", "polars")
    try:
        df.show()  # styled renderer calls count() — the reported repro
    finally:
        spark.conf.set("repark.display.style", "spark")

    partial = spark.sql(f"SELECT snapshot_id, operation FROM {TABLE}.snapshots").to_arrow()
    assert _schema_names(partial) == ["snapshot_id", "operation"]
    assert str(partial.schema.field("snapshot_id").type) == "int64"
    assert str(partial.schema.field("operation").type) == "string"
    assert set(partial.column("snapshot_id").to_pylist()) >= {
        multi_snapshot["s1"],
        multi_snapshot["s2"],
        multi_snapshot["s3"],
    }
    # Single-column projection also count()s correctly (1-field logical schema).
    assert (
        spark.sql(f"SELECT snapshot_id FROM {TABLE}.snapshots").count()
        == multi_snapshot["snapshot_count"]
    )


def test_history_is_current_ancestor(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    _ = multi_snapshot
    arrow = spark.sql(f"SELECT * FROM {TABLE}.history").to_arrow()
    names_types = _schema_names_types(arrow)
    assert [name for name, _ in names_types] == [name for name, _ in HISTORY_SCHEMA]
    for (name, expected_type), (_, actual_type) in zip(HISTORY_SCHEMA, names_types, strict=True):
        if name == "made_current_at":
            assert "timestamp" in actual_type
        else:
            assert actual_type == expected_type, f"{name}: {actual_type} != {expected_type}"
    ancestors = arrow.column("is_current_ancestor").to_pylist()
    assert any(value is True for value in ancestors)
    assert arrow.num_rows >= 3


def test_files_record_count_sums_to_table(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    arrow = spark.sql(f"SELECT * FROM {TABLE}.files").to_arrow()
    names = _schema_names(arrow)
    for core in FILES_CORE_NAMES:
        assert core in names, f"files schema missing {core}: {names}"
    # readable_metrics is appended (fork files.rs / readable_metrics.rs).
    assert "readable_metrics" in names
    counts = arrow.column("record_count").to_pylist()
    total = sum(int(value) for value in counts if value is not None)
    assert total == len(multi_snapshot["ids_current"])


def test_spark_table_dot_files_same_path(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """``spark.table("x.files")`` uses the same SQL path as ``SELECT * FROM x.files``."""
    _ = multi_snapshot
    via_sql = spark.sql(f"SELECT * FROM {TABLE}.files").to_arrow()
    via_table = spark.table(f"{TABLE}.files").to_arrow()
    assert _schema_names(via_sql) == _schema_names(via_table)
    assert via_sql.num_rows == via_table.num_rows
    sql_counts = via_sql.column("record_count").to_pylist()
    table_counts = via_table.column("record_count").to_pylist()
    assert sql_counts == table_counts


def test_manifests_partitions_refs_entries_metadata_log(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    _ = multi_snapshot
    manifests = spark.sql(f"SELECT * FROM {TABLE}.manifests").to_arrow()
    assert "path" in _schema_names(manifests)
    assert "added_snapshot_id" in _schema_names(manifests)
    assert manifests.num_rows >= 1

    partitions = spark.sql(f"SELECT * FROM {TABLE}.partitions").to_arrow()
    # Unpartitioned: fork KEEPS empty-struct partition column (Java drops it) — R142 divergence.
    part_names = _schema_names(partitions)
    assert "partition" in part_names, (
        "known fork residue: unpartitioned partitions table keeps empty partition struct "
        "(inspect/partitions.rs:31-37; Java drops it)"
    )
    assert "record_count" in part_names
    assert partitions.num_rows >= 1

    refs = spark.sql(f"SELECT * FROM {TABLE}.refs").to_arrow()
    assert _schema_names(refs)[:3] == REFS_SCHEMA_NAMES[:3]
    ref_names = set(refs.column("name").to_pylist())
    assert "main" in ref_names or "main" in {str(n).lower() for n in ref_names}
    assert "tag_s1" in ref_names
    assert "branch_s2" in ref_names

    entries = spark.sql(f"SELECT * FROM {TABLE}.entries").to_arrow()
    assert entries.num_rows >= 1
    # readable_metrics present; compare interior fields by name (fork field-id order residue).
    # C1-Q-001: interior pin must go red if the struct is empty or missing leaf names.
    assert "readable_metrics" in _schema_names(entries), (
        f"entries schema missing readable_metrics: {_schema_names(entries)}"
    )
    metrics_field = entries.schema.field("readable_metrics")
    assert pa.types.is_struct(metrics_field.type)
    interior = [metrics_field.type[i].name for i in range(metrics_field.type.num_fields)]
    assert len(interior) >= 1, "readable_metrics must expose at least one leaf field"
    # Unpartitioned id/name table → leaf columns appear by name (order not asserted).
    assert "id" in interior or "name" in interior, (
        f"readable_metrics interior must include id or name by name, got {interior}"
    )

    meta_log = spark.sql(f"SELECT * FROM {TABLE}.metadata_log_entries").to_arrow()
    assert "timestamp" in _schema_names(meta_log)
    assert "file" in _schema_names(meta_log)
    assert meta_log.num_rows >= 1


def test_all_family_resolves(spark: ReparkSession, multi_snapshot: dict[str, object]) -> None:
    """all_* family routes the same as current-snapshot siblings (fork MetadataTableType)."""
    _ = multi_snapshot
    for suffix in (
        "all_files",
        "all_data_files",
        "all_delete_files",
        "all_entries",
        "all_manifests",
        "data_files",
        "delete_files",
    ):
        arrow = spark.sql(f"SELECT * FROM {TABLE}.{suffix}").to_arrow()
        # all_delete_files / delete_files may be empty on COW-only fixture — schema still resolves.
        assert arrow.schema is not None
        assert len(_schema_names(arrow)) >= 1
    # C2-Q-003: all_files must be at least as large as current-snapshot files (not a hollow stub).
    current_files = spark.sql(f"SELECT * FROM {TABLE}.files").to_arrow()
    all_files = spark.sql(f"SELECT * FROM {TABLE}.all_files").to_arrow()
    assert all_files.num_rows >= current_files.num_rows
    assert "record_count" in _schema_names(all_files)


def test_real_table_named_files_wins(spark: ReparkSession) -> None:
    """A real table literally named ``files`` must not be rewritten as metadata."""
    spark.sql(
        f"CREATE TABLE mem.ns.files USING iceberg TBLPROPERTIES ({COW}) AS "
        "SELECT 42 AS x, 'real' AS label"
    )
    arrow = spark.sql("SELECT * FROM mem.ns.files").to_arrow()
    assert _schema_names(arrow) == ["x", "label"]
    assert arrow.column("x").to_pylist() == [42]
    # Metadata of a different base table still works.
    spark.sql(f"CREATE TABLE mem.ns.base USING iceberg TBLPROPERTIES ({COW}) AS SELECT 1 AS id")
    meta = spark.sql("SELECT * FROM mem.ns.base.files").to_arrow()
    assert "record_count" in _schema_names(meta)
    # C1-L-003: metadata of the real table named ``files`` still resolves.
    files_snaps = spark.sql("SELECT * FROM mem.ns.files.snapshots").to_arrow()
    assert "snapshot_id" in _schema_names(files_snaps)
    assert files_snaps.num_rows >= 1
    # C1-Q-003: INSERT into real ``files`` must not be blocked as metadata DML.
    spark.sql("INSERT INTO mem.ns.files SELECT 7 AS x, 'more' AS label")
    after = spark.sql("SELECT * FROM mem.ns.files").to_arrow()
    assert after.num_rows == 2


def test_dml_on_metadata_table_loud(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    _ = multi_snapshot
    with pytest.raises(AnalysisException) as raised:
        spark.sql(f"INSERT INTO {TABLE}.snapshots SELECT 1").to_arrow()
    message = str(raised.value).lower()
    assert "read-only" in message or "metadata table" in message

    with pytest.raises(AnalysisException) as raised:
        spark.sql(f"DELETE FROM {TABLE}.history").to_arrow()
    message = str(raised.value).lower()
    assert "read-only" in message or "metadata table" in message

    with pytest.raises(AnalysisException) as raised:
        spark.sql(
            f"MERGE INTO {TABLE}.files AS t USING (SELECT 1 AS id) s ON true "
            "WHEN MATCHED THEN DELETE"
        ).to_arrow()
    message = str(raised.value).lower()
    assert "read-only" in message or "metadata table" in message

    # C1-Q-003: UPDATE + CTAS targets also refuse loud.
    with pytest.raises(AnalysisException) as raised:
        spark.sql(f"UPDATE {TABLE}.refs SET name = 'x'").to_arrow()
    message = str(raised.value).lower()
    assert "read-only" in message or "metadata table" in message

    with pytest.raises(AnalysisException) as raised:
        spark.sql(f"CREATE TABLE {TABLE}.entries AS SELECT 1 AS id").to_arrow()
    message = str(raised.value).lower()
    assert "read-only" in message or "metadata table" in message


def test_as_of_composition_loud_out_of_scope(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    s1 = multi_snapshot["s1"]
    with pytest.raises(AnalysisException) as raised:
        spark.sql(f"SELECT * FROM {TABLE}.snapshots VERSION AS OF {s1}").to_arrow()
    message = str(raised.value).lower()
    assert "not supported" in message or "time travel" in message or "as of" in message

    # C1-L-002: parenthesized composition still refuses.
    with pytest.raises(AnalysisException) as raised:
        spark.sql(f"SELECT * FROM ({TABLE}.snapshots) VERSION AS OF {s1}").to_arrow()
    message = str(raised.value).lower()
    assert "not supported" in message or "time travel" in message or "as of" in message

    # C3-Q-001: TIMESTAMP / SYSTEM_* forms refuse loud (not only VERSION).
    with pytest.raises(AnalysisException) as raised:
        spark.sql(
            f"SELECT * FROM {TABLE}.snapshots TIMESTAMP AS OF '2099-01-01 00:00:00'"
        ).to_arrow()
    message = str(raised.value).lower()
    assert "not supported" in message or "time travel" in message or "as of" in message

    with pytest.raises(AnalysisException) as raised:
        spark.sql(f"SELECT * FROM {TABLE}.files FOR SYSTEM_VERSION AS OF {s1}").to_arrow()
    message = str(raised.value).lower()
    assert "not supported" in message or "time travel" in message or "as of" in message


def test_fq_column_named_files_not_rewritten(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """C1-L-001: a column literally named ``files`` must not rewrite as metadata."""
    _ = multi_snapshot
    spark.sql(
        f"CREATE TABLE mem.ns.with_files_col USING iceberg TBLPROPERTIES ({COW}) AS "
        "SELECT 1 AS id, 'blob' AS files"
    )
    arrow = spark.sql("SELECT mem.ns.with_files_col.files FROM mem.ns.with_files_col").to_arrow()
    assert _schema_names(arrow) == ["files"]
    assert arrow.column("files").to_pylist() == ["blob"]


def test_join_metadata_files(spark: ReparkSession, multi_snapshot: dict[str, object]) -> None:
    """C2-Q-001: JOIN to a metadata table rewrites and returns rows."""
    _ = multi_snapshot
    # SELECT * — narrow projections off files metadata hit Arrow FFI shape issues on some
    # paths (fork/DF residue); full-row JOIN still proves the `$` rewrite on JOIN positions.
    arrow = spark.sql(f"SELECT * FROM {TABLE} e JOIN {TABLE}.files f ON true").to_arrow()
    assert arrow.num_rows >= 1
    names = _schema_names(arrow)
    assert "id" in names
    assert "record_count" in names


def test_real_table_named_snapshots_wins(spark: ReparkSession) -> None:
    """C2-Q-004: a real table literally named ``snapshots`` must not rewrite as metadata."""
    spark.sql(f"CREATE TABLE mem.ns.snapshots USING iceberg TBLPROPERTIES ({COW}) AS SELECT 7 AS k")
    arrow = spark.sql("SELECT * FROM mem.ns.snapshots").to_arrow()
    assert _schema_names(arrow) == ["k"]
    assert arrow.column("k").to_pylist() == [7]


def test_truncate_metadata_loud(spark: ReparkSession, multi_snapshot: dict[str, object]) -> None:
    """C2-Q-002: TRUNCATE TABLE on a metadata path refuses loud."""
    _ = multi_snapshot
    with pytest.raises(AnalysisException) as raised:
        spark.sql(f"TRUNCATE TABLE {TABLE}.files").to_arrow()
    message = str(raised.value).lower()
    assert "read-only" in message or "metadata table" in message


def test_create_view_metadata_loud(spark: ReparkSession, multi_snapshot: dict[str, object]) -> None:
    """C5-Q-001: CREATE VIEW targeting a metadata path refuses loud."""
    _ = multi_snapshot
    with pytest.raises(AnalysisException) as raised:
        spark.sql(f"CREATE VIEW {TABLE}.files AS SELECT 1 AS id").to_arrow()
    message = str(raised.value).lower()
    assert "read-only" in message or "metadata table" in message


def test_drop_alter_metadata_loud(spark: ReparkSession, multi_snapshot: dict[str, object]) -> None:
    """C6-Q-001: DROP/ALTER TABLE on a metadata path refuses loud."""
    _ = multi_snapshot
    with pytest.raises(AnalysisException) as raised:
        spark.sql(f"DROP TABLE {TABLE}.files").to_arrow()
    message = str(raised.value).lower()
    assert "read-only" in message or "metadata table" in message

    with pytest.raises(AnalysisException) as raised:
        spark.sql(f"ALTER TABLE {TABLE}.snapshots ADD COLUMNS (x INT)").to_arrow()
    message = str(raised.value).lower()
    assert "read-only" in message or "metadata table" in message


def test_unpartitioned_partition_column_divergence(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """Pin fork residue: empty partition column kept on unpartitioned tables (Java drops it)."""
    _ = multi_snapshot
    files = spark.sql(f"SELECT * FROM {TABLE}.files").to_arrow()
    assert "partition" in _schema_names(files)
    partition_field = files.schema.field("partition")
    assert pa.types.is_struct(partition_field.type)
    # Unpartitioned → empty struct (0 fields).
    assert partition_field.type.num_fields == 0, (
        "fork keeps empty-struct partition on unpartitioned files table "
        "(inspect/files.rs unpartitioned divergence / R142)"
    )

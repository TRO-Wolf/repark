"""R-MAINTENANCE-CALL oracle — Spark ``CALL catalog.system.<proc>(…)``.

Seven procedures: expire_snapshots, rewrite_data_files, rewrite_position_delete_files,
remove_orphan_files, rewrite_manifests (MW-6), rollback_to_snapshot, and register_table
(V3-1 adoption). Unknown names refuse loud listing the supported set.

Oracle discipline: Arrow ``to_arrow`` value AND type pins (docs/testing.md divergence-class).
Result schemas pin Spark names and types.

Fork pin ``4723104b``:
- expire: ``transaction/expire_snapshots.rs`` + ``expire_cleanup.rs``
- rewrite: ``maintenance/rewrite_data_files.rs``
- rollback: ``transaction/manage_snapshots.rs:164-167``
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import IllegalArgumentException, PySparkException, UnsupportedOperationException

TABLE = "mem.ns.events"
COW = """
    'format-version' = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write'
"""


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-maintenance-call").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def _arrow_ids(table: pa.Table) -> list[int]:
    return sorted(int(value) for value in table.column("id").to_pylist() if value is not None)


def _schema_names(table: pa.Table) -> list[str]:
    return [field.name for field in table.schema]


@pytest.fixture
def multi_snapshot(spark: ReparkSession) -> dict[str, object]:
    """≥3 snapshots + tag at s1 for expire safety pin."""
    spark.sql(
        f"CREATE TABLE {TABLE} USING iceberg TBLPROPERTIES ({COW}) AS "
        "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)"
    )
    snaps = spark._testing_list_snapshots(TABLE)
    s1, s1_ts = snaps[-1]

    spark.sql(f"INSERT INTO {TABLE} SELECT 4 AS id, 'd' AS name")
    snaps = spark._testing_list_snapshots(TABLE)
    s2, s2_ts = snaps[-1]

    spark.sql(f"INSERT INTO {TABLE} SELECT 5 AS id, 'e' AS name")
    snaps = spark._testing_list_snapshots(TABLE)
    s3, s3_ts = snaps[-1]
    assert len(snaps) >= 3

    spark._testing_create_ref(TABLE, "tag", "tag_s1", s1)

    return {
        "s1": s1,
        "s1_ts": s1_ts,
        "s2": s2,
        "s2_ts": s2_ts,
        "s3": s3,
        "s3_ts": s3_ts,
        "ids_s1": [1, 2, 3],
        "ids_s2": [1, 2, 3, 4],
        "ids_s3": [1, 2, 3, 4, 5],
    }


def test_rollback_to_snapshot_restores_multiset(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    s1 = multi_snapshot["s1"]
    s3 = multi_snapshot["s3"]
    before = spark.sql(f"SELECT id, name FROM {TABLE} ORDER BY id").to_arrow()
    assert _arrow_ids(before) == multi_snapshot["ids_s3"]

    result = spark.sql(
        f"CALL mem.system.rollback_to_snapshot(table => 'ns.events', snapshot_id => {s1})"
    ).to_arrow()
    assert _schema_names(result) == ["previous_snapshot_id", "current_snapshot_id"]
    assert result.schema.field("previous_snapshot_id").type == pa.int64()
    assert result.schema.field("current_snapshot_id").type == pa.int64()
    # C1-Q-003: both result columns load-bearing.
    assert result.column("previous_snapshot_id")[0].as_py() == s3
    assert result.column("current_snapshot_id")[0].as_py() == s1

    after = spark.sql(f"SELECT id, name FROM {TABLE} ORDER BY id").to_arrow()
    assert _arrow_ids(after) == multi_snapshot["ids_s1"]
    assert after.schema.field("id").type == pa.int32()
    assert after.schema.field("name").type == pa.string()


def test_expire_snapshots_keeps_tag_reachable(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """Load-bearing R133 safety: tag-reachable s1 survives expire with retain_last=1.

    C1-Q-001 dual probe: untagged intermediate s2 must expire (proves expire ran);
    a no-op CALL would keep s1 *and* s2 and still pass a s1-only pin.
    """
    import time

    s1 = multi_snapshot["s1"]
    s2 = multi_snapshot["s2"]
    # Far-future older_than so age would expire every snapshot; retain_last keeps main head;
    # tag alone must keep s1.
    older_than_ms = int(time.time() * 1000) + 86_400_000
    result = spark.sql(
        f"CALL mem.system.expire_snapshots("
        f"table => 'ns.events', older_than => {older_than_ms}, retain_last => 1)"
    ).to_arrow()
    # MW-1: Spark's full six-column result, in Spark's order. The fork returns all content files
    # in ONE funnel; `classify_content_files` rebuilds the data/delete split from the manifest
    # entries' own content type. Measured on a live Spark 4.0.1 + Iceberg 1.10.0 oracle.
    assert _schema_names(result) == [
        "deleted_data_files_count",
        "deleted_position_delete_files_count",
        "deleted_equality_delete_files_count",
        "deleted_manifest_files_count",
        "deleted_manifest_lists_count",
        "deleted_statistics_files_count",
    ]
    # All six are bigint and NULLABLE — Spark declares them so (jar `OUTPUT_TYPE`, `iconst_1`
    # per StructField), unlike its two rewrite procedures. Matched per procedure, not by one rule.
    for name in _schema_names(result):
        assert result.schema.field(name).type == pa.int64()
        assert result.schema.field(name).nullable

    # s1 still resolvable via VERSION AS OF (tag kept it).
    pinned = spark.sql(f"SELECT id FROM {TABLE} VERSION AS OF {s1} ORDER BY id").to_arrow()
    assert _arrow_ids(pinned) == multi_snapshot["ids_s1"]

    # Untagged intermediate must be gone (mutation-proof that expire applied).
    with pytest.raises((UnsupportedOperationException, PySparkException)):
        spark.sql(f"SELECT id FROM {TABLE} VERSION AS OF {s2}").to_arrow()

    # Current read still works (main head retained).
    current = spark.sql(f"SELECT id FROM {TABLE} ORDER BY id").to_arrow()
    assert len(_arrow_ids(current)) >= 1


def test_expire_snapshots_keeps_branch_reachable(spark: ReparkSession) -> None:
    """C5-Q-001: branch-reachable snapshot survives expire (facade dual probe)."""
    import time

    table = "mem.ns.branch_exp"
    spark.sql(
        f"CREATE TABLE {table} USING iceberg TBLPROPERTIES ({COW}) AS "
        "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)"
    )
    snaps = spark._testing_list_snapshots(table)
    s1 = snaps[-1][0]
    spark.sql(f"INSERT INTO {table} SELECT 4 AS id, 'd' AS name")
    snaps = spark._testing_list_snapshots(table)
    s2 = snaps[-1][0]
    spark.sql(f"INSERT INTO {table} SELECT 5 AS id, 'e' AS name")
    spark._testing_create_ref(table, "branch", "audit", s1)

    older_than_ms = int(time.time() * 1000) + 86_400_000
    spark.sql(
        f"CALL mem.system.expire_snapshots("
        f"table => 'ns.branch_exp', older_than => {older_than_ms}, retain_last => 1)"
    ).to_arrow()

    pinned = spark.sql(f"SELECT id FROM {table} VERSION AS OF {s1} ORDER BY id").to_arrow()
    assert _arrow_ids(pinned) == [1, 2, 3]
    with pytest.raises((UnsupportedOperationException, PySparkException)):
        spark.sql(f"SELECT id FROM {table} VERSION AS OF {s2}").to_arrow()
    current = spark.sql(f"SELECT id FROM {table} ORDER BY id").to_arrow()
    assert len(_arrow_ids(current)) >= 1


def test_rewrite_data_files_preserves_multiset_and_reduces_files(spark: ReparkSession) -> None:
    table = "mem.ns.compact"
    spark.sql(
        f"CREATE TABLE {table} USING iceberg TBLPROPERTIES ({COW}) AS SELECT 1 AS id, 'a' AS name"
    )
    for index in range(2, 7):
        spark.sql(f"INSERT INTO {table} SELECT {index} AS id, 'x' AS name")

    before = spark.sql(f"SELECT id, name FROM {table} ORDER BY id").to_arrow()
    before_ids = _arrow_ids(before)
    assert before_ids == [1, 2, 3, 4, 5, 6]

    files_before = spark.sql(f"SELECT * FROM {table}.files").to_arrow()
    n_files_before = files_before.num_rows
    assert n_files_before >= 5

    result = spark.sql("CALL mem.system.rewrite_data_files(table => 'ns.compact')").to_arrow()
    # MW-2: Spark's five, in Spark's order, all non-nullable — measured on a live Spark 4.0.1
    # + Iceberg 1.10.0 oracle.
    assert _schema_names(result) == [
        "rewritten_data_files_count",
        "added_data_files_count",
        "rewritten_bytes_count",
        "failed_data_files_count",
        "removed_delete_files_count",
    ]
    assert result.schema.field("rewritten_data_files_count").type == pa.int32()
    assert result.schema.field("added_data_files_count").type == pa.int32()
    assert result.schema.field("rewritten_bytes_count").type == pa.int64()
    assert result.schema.field("failed_data_files_count").type == pa.int32()
    assert result.schema.field("removed_delete_files_count").type == pa.int32()
    for field in result.schema:
        assert not field.nullable, f"Spark declares {field.name} non-nullable"
    rewritten = result.column("rewritten_data_files_count")[0].as_py()
    assert rewritten >= 2
    # Spark reports 0 here whenever `remove-dangling-deletes` is off, and its default is off
    # (`RewriteDataFiles.REMOVE_DANGLING_DELETES_DEFAULT`). This procedure refuses the options
    # map, so the non-default path is unreachable and the zero is a real count.
    assert result.column("removed_delete_files_count")[0].as_py() == 0

    after = spark.sql(f"SELECT id, name FROM {table} ORDER BY id").to_arrow()
    assert _arrow_ids(after) == before_ids
    assert after.schema.field("id").type == pa.int32()
    assert after.schema.field("name").type == pa.string()

    files_after = spark.sql(f"SELECT * FROM {table}.files").to_arrow()
    assert files_after.num_rows < n_files_before


def test_unknown_procedure_lists_supported(spark: ReparkSession) -> None:
    with pytest.raises(
        (UnsupportedOperationException, PySparkException),
        match=r"register_table",
    ):
        spark.sql("CALL mem.system.not_a_real_proc(table => 'ns.events')")


def test_register_table_adopts_and_returns_spark_columns(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """V3-1 — facade door. Spark's three nullable BIGINT columns, then the adopted table reads."""
    owned = tmp_path / "owned"
    spark.sql(f"CREATE NAMESPACE mem.owned LOCATION '{owned}'")
    spark.sql(
        f"CREATE TABLE mem.owned.src USING iceberg TBLPROPERTIES ({COW}) AS "
        "SELECT 1 AS id, 'a' AS name"
    )
    metadata_dir = owned / "src" / "metadata"
    metadata_files = sorted(metadata_dir.glob("*.metadata.json"))
    assert metadata_files, f"engine-created table must write metadata under {metadata_dir}"
    metadata_file = metadata_files[-1]
    result = spark.sql(
        "CALL mem.system.register_table("
        f"table => 'owned.adopted', metadata_file => '{metadata_file}')"
    ).to_arrow()
    assert _schema_names(result) == [
        "current_snapshot_id",
        "total_records_count",
        "total_data_files_count",
    ]
    assert result.schema.field("current_snapshot_id").nullable
    assert result.schema.field("total_records_count").nullable
    assert result.schema.field("total_data_files_count").nullable
    assert result.schema.field("current_snapshot_id").type == pa.int64()
    assert result.schema.field("total_records_count").type == pa.int64()
    assert result.schema.field("total_data_files_count").type == pa.int64()
    assert result.column("total_records_count")[0].as_py() == 1
    adopted = spark.sql("SELECT id FROM mem.owned.adopted").to_arrow()
    assert _arrow_ids(adopted) == [1]


def _plant_orphan(table_dir: Path, name: str, age_days: float) -> Path:
    """Write one unreferenced file into the table's data directory, aged ``age_days`` old."""
    data_dir = table_dir / "data"
    assert data_dir.is_dir(), f"a fresh table must carry a data directory under {table_dir}"
    path = data_dir / name
    path.write_bytes(b"not really parquet")
    aged = time.time() - age_days * 24 * 60 * 60
    os.utime(path, (aged, aged))
    return path


def _orphan_names(result: pa.Table) -> set[str]:
    """The file names of an orphan listing, without their directory prefix."""
    return {Path(location).name for location in result.column("orphan_file_location").to_pylist()}


def test_remove_orphan_files_defaults_older_than_to_three_days(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """A bare call lists with Spark's ``older_than`` default of now minus three days.

    The planted 10-day-old orphan is returned and deleted; the planted 1-day-old orphan is
    kept. Registry rows ORPHAN-1/ORPHAN-2 (retired 2026-09-22, Spark parity).

    pins: ipi-30-orphan-1/C-001, C-002
    """
    owned = tmp_path / "owned"
    spark.sql(f"CREATE NAMESPACE mem.owned LOCATION '{owned}'")
    spark.sql(
        f"CREATE TABLE mem.owned.events USING iceberg TBLPROPERTIES ({COW}) "
        "AS SELECT 1 AS id, 'a' AS name"
    )
    table_dir = owned / "events"
    old = _plant_orphan(table_dir, "orphan-old.parquet", 10)
    young = _plant_orphan(table_dir, "orphan-young.parquet", 1)
    result = spark.sql("CALL mem.system.remove_orphan_files(table => 'owned.events')").to_arrow()
    assert _schema_names(result) == ["orphan_file_location"]
    assert result.schema.field("orphan_file_location").type == pa.string()
    assert not result.schema.field("orphan_file_location").nullable
    assert _orphan_names(result) == {"orphan-old.parquet"}
    assert not old.exists()
    assert young.exists()
    live = spark.sql("SELECT id FROM mem.owned.events").to_arrow()
    assert _arrow_ids(live) == [1]


def test_remove_orphan_files_deletes_by_default(spark: ReparkSession, tmp_path: Path) -> None:
    """A bare call deletes; ``dry_run => true`` lists the same rows and keeps the files.

    Registry rows ORPHAN-1/ORPHAN-2 (retired 2026-09-22, Spark parity).

    pins: ipi-30-orphan-1/C-002, C-003
    """
    owned = tmp_path / "owned"
    spark.sql(f"CREATE NAMESPACE mem.owned LOCATION '{owned}'")
    spark.sql(
        f"CREATE TABLE mem.owned.events USING iceberg TBLPROPERTIES ({COW}) "
        "AS SELECT 1 AS id, 'a' AS name"
    )
    table_dir = owned / "events"
    first = _plant_orphan(table_dir, "orphan-first.parquet", 10)
    second = _plant_orphan(table_dir, "orphan-second.parquet", 10)
    listed = spark.sql(
        "CALL mem.system.remove_orphan_files(table => 'owned.events', dry_run => true)"
    ).to_arrow()
    assert _schema_names(listed) == ["orphan_file_location"]
    assert _orphan_names(listed) == {"orphan-first.parquet", "orphan-second.parquet"}
    assert first.exists()
    assert second.exists()
    deleted = spark.sql("CALL mem.system.remove_orphan_files(table => 'owned.events')").to_arrow()
    assert _orphan_names(deleted) == {"orphan-first.parquet", "orphan-second.parquet"}
    assert not first.exists()
    assert not second.exists()


def test_remove_orphan_files_floor_matches_spark(spark: ReparkSession, tmp_path: Path) -> None:
    """MW-3: the 24-hour floor is PARITY with Spark, not a stricter posture.

    Measured across the boundary on the oracle: ``now`` refuses, ``now - 23h`` refuses,
    ``now - 25h`` runs. Java enforces it in ``RemoveOrphanFilesProcedure`` rather than the
    Action API, which is why this engine carries it in the CALL router too.

    pins: ipi-30-orphan-1/C-004
    """
    owned = tmp_path / "owned"
    spark.sql(f"CREATE NAMESPACE mem.owned LOCATION '{owned}'")
    spark.sql(
        f"CREATE TABLE mem.owned.events USING iceberg TBLPROPERTIES ({COW}) "
        "AS SELECT 1 AS id, 'a' AS name"
    )
    now_ms = int(time.time() * 1000)
    hour_ms = 60 * 60 * 1000
    with pytest.raises(
        (UnsupportedOperationException, PySparkException),
        match=r"less than 24 hours",
    ):
        spark.sql(
            "CALL mem.system.remove_orphan_files("
            f"table => 'owned.events', older_than => {now_ms - 23 * hour_ms})"
        )
    # The control: just outside the floor, it runs.
    spark.sql(
        "CALL mem.system.remove_orphan_files("
        f"table => 'owned.events', older_than => {now_ms - 25 * hour_ms})"
    ).to_arrow()


def test_rewrite_sort_strategy_refuses_loud(spark: ReparkSession) -> None:
    """Strategy sort on an unsorted table is the fork's message, named and positional."""
    spark.sql(
        f"CREATE TABLE {TABLE} USING iceberg TBLPROPERTIES ({COW}) AS SELECT 1 AS id, 'a' AS name"
    )
    expected = (
        "Cannot sort data without a valid sort order, "
        "table 'ns.events' is unsorted and no sort order is provided"
    )
    with pytest.raises(IllegalArgumentException) as caught:
        spark.sql("CALL mem.system.rewrite_data_files(table => 'ns.events', strategy => 'sort')")
    assert str(caught.value) == expected
    with pytest.raises(IllegalArgumentException) as caught:
        spark.sql("CALL mem.system.rewrite_data_files('ns.events', 'sort')")
    assert str(caught.value) == expected


def test_positional_rollback_args(spark: ReparkSession, multi_snapshot: dict[str, object]) -> None:
    """Spark docs: positional args accepted (cite iceberg spark-procedures Usage)."""
    s1 = multi_snapshot["s1"]
    result = spark.sql(f"CALL mem.system.rollback_to_snapshot('ns.events', {s1})").to_arrow()
    assert result.column("current_snapshot_id")[0].as_py() == s1
    after = spark.sql(f"SELECT id FROM {TABLE} ORDER BY id").to_arrow()
    assert _arrow_ids(after) == multi_snapshot["ids_s1"]


def test_remove_orphan_files_sweeps_a_fallback_table_but_never_the_shared_root(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """Owner ruling Q-55-6: a fallback table's own directory is sweepable; the root is not.

    ``register_memory_catalog`` carries a ``TempFallbackAllowed`` policy, so a namespace created
    with no ``location`` places its tables at ``<warehouse>/repark_ctas/<catalog>/<ns>/<table>``.
    Sweeping that table's own directory runs like Spark: the 10-day-old orphan is listed and
    deleted. A ``location`` at the shared root or at the warehouse still refuses, deleting nothing.

    pins: ipi-30-orphan-guard-narrow-1/C-001, C-002, C-004
    """
    spark.sql(
        f"CREATE TABLE {TABLE} USING iceberg TBLPROPERTIES ({COW}) AS SELECT 1 AS id, 'a' AS name"
    )
    table_dir = tmp_path / "repark_ctas" / "mem" / "ns" / "events"
    orphan = _plant_orphan(table_dir, "orphan-file.parquet", 10)
    for location in (tmp_path, tmp_path / "repark_ctas"):
        with pytest.raises(
            (UnsupportedOperationException, PySparkException),
            match=r"shared CTAS fallback root",
        ):
            spark.sql(
                "CALL mem.system.remove_orphan_files("
                f"table => 'ns.events', location => '{location}')"
            )
        assert orphan.exists()
    result = spark.sql("CALL mem.system.remove_orphan_files(table => 'ns.events')").to_arrow()
    assert _orphan_names(result) == {"orphan-file.parquet"}
    assert not orphan.exists()
    live = spark.sql(f"SELECT id FROM {TABLE}").to_arrow()
    assert _arrow_ids(live) == [1]


def _manifest_count(spark: ReparkSession, table: str) -> int:
    return spark.sql(f"SELECT path FROM {table}.manifests").to_arrow().num_rows


def test_rewrite_manifests_compacts_like_spark(spark: ReparkSession) -> None:
    """MW-6 — facade door. Spark's two non-nullable ``int`` columns, and Spark's counts.

    Oracle — live Spark 4.0.1 + Iceberg 1.10.0, five single-row appends into an unpartitioned v2
    table: ``rewritten_manifests_count=5``, ``added_manifests_count=1``, manifests 5 → 1, and the
    row set unchanged. The schema is also the Iceberg 1.10.0 jar's ``OUTPUT_TYPE`` constant.

    pins: mw-6-rewrite-manifests/C-001, C-002, C-003
    """
    table = "mem.ns.man"
    spark.sql(
        f"CREATE TABLE {table} USING iceberg TBLPROPERTIES ({COW}) AS SELECT 1 AS id, 'a' AS name"
    )
    for index in range(2, 6):
        spark.sql(f"INSERT INTO {table} SELECT {index} AS id, 'x' AS name")
    assert _manifest_count(spark, table) == 5

    before = _arrow_ids(spark.sql(f"SELECT id, name FROM {table} ORDER BY id").to_arrow())
    result = spark.sql("CALL mem.system.rewrite_manifests(table => 'ns.man')").to_arrow()
    assert _schema_names(result) == ["rewritten_manifests_count", "added_manifests_count"]
    assert result.schema.field("rewritten_manifests_count").type == pa.int32()
    assert result.schema.field("added_manifests_count").type == pa.int32()
    for field in result.schema:
        assert not field.nullable, f"Spark declares {field.name} non-nullable"
    assert result.column("rewritten_manifests_count")[0].as_py() == 5
    assert result.column("added_manifests_count")[0].as_py() == 1

    assert _manifest_count(spark, table) == 1
    after = spark.sql(f"SELECT id, name FROM {table} ORDER BY id").to_arrow()
    assert _arrow_ids(after) == before
    assert after.schema.field("id").type == pa.int32()
    assert after.schema.field("name").type == pa.string()


def test_rewrite_manifests_no_op_returns_zeros(spark: ReparkSession) -> None:
    """MW-6 — nothing to rewrite is two zeros, not an error and not a new snapshot.

    Oracle — live Spark 4.0.1: the second call on a freshly rewritten table returns ``0, 0`` and
    the snapshot list does not grow (Spark's ``targetNumManifests == 1 && matching.size() == 1``).

    pins: mw-6-rewrite-manifests/C-004
    """
    table = "mem.ns.noop"
    spark.sql(
        f"CREATE TABLE {table} USING iceberg TBLPROPERTIES ({COW}) AS SELECT 1 AS id, 'a' AS name"
    )
    for index in range(2, 6):
        spark.sql(f"INSERT INTO {table} SELECT {index} AS id, 'x' AS name")
    spark.sql("CALL mem.system.rewrite_manifests(table => 'ns.noop')").to_arrow()
    snapshots_before = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots").to_arrow().num_rows

    result = spark.sql("CALL mem.system.rewrite_manifests(table => 'ns.noop')").to_arrow()
    assert result.column("rewritten_manifests_count")[0].as_py() == 0
    assert result.column("added_manifests_count")[0].as_py() == 0
    after = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots").to_arrow().num_rows
    assert after == snapshots_before, "a no-op rewrite commits no snapshot"


def test_rewrite_manifests_spec_id_selects_and_use_caching_is_accepted(
    spark: ReparkSession,
) -> None:
    """ICE-RM-DELETES-1 — the argument surface.

    Spark takes ``table``, ``use_caching`` and ``spec_id``. ``use_caching`` caches Spark's own
    manifest DataFrame and changed no count on the oracle, so this engine accepts it and does
    nothing with it. ``spec_id`` selects which partition spec to rewrite; the current id runs
    like the default call and an unknown id raises ``IllegalArgumentException``
    (``Invalid spec id``, Spark's own text).

    pins: ice-rm-deletes-1/C-003, C-004
    """
    table = "mem.ns.args"
    spark.sql(
        f"CREATE TABLE {table} USING iceberg TBLPROPERTIES ({COW}) AS SELECT 1 AS id, 'a' AS name"
    )
    for index in range(2, 6):
        spark.sql(f"INSERT INTO {table} SELECT {index} AS id, 'x' AS name")

    result = spark.sql(
        "CALL mem.system.rewrite_manifests(table => 'ns.args', use_caching => true)"
    ).to_arrow()
    assert result.column("rewritten_manifests_count")[0].as_py() == 5
    assert result.column("added_manifests_count")[0].as_py() == 1

    compacted = spark.sql(
        "CALL mem.system.rewrite_manifests(table => 'ns.args', spec_id => 0)"
    ).to_arrow()
    assert compacted.column("rewritten_manifests_count")[0].as_py() == 0
    assert compacted.column("added_manifests_count")[0].as_py() == 0

    with pytest.raises(
        IllegalArgumentException,
        match=r"Invalid spec id 99",
    ):
        spark.sql("CALL mem.system.rewrite_manifests(table => 'ns.args', spec_id => 99)")

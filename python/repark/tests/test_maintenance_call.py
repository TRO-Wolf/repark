"""I3 / R-MAINTENANCE-CALL oracle — Spark ``CALL catalog.system.<proc>(…)``.

Three procedures v1 (fork-backed, LOCAL memory catalog only):

1. ``expire_snapshots`` — R133 + cleanup; tag-reachable snapshots survive.
2. ``rewrite_data_files`` — R135 bin-pack; row multiset preserved; file count drops.
3. ``rollback_to_snapshot`` — R98 ManageSnapshots.rollback_to; read = old multiset.

Unknown / ``remove_orphan_files`` refuse loud listing supported procs.

Oracle discipline: Arrow ``to_arrow`` value AND type pins (docs/testing.md
divergence-class). Result schemas pin Spark names where the fork exposes honest
metrics; expire content-file count is a disclosed divergence (fork buckets all
content together).

Fork pin ``4723104b``:
- expire: ``transaction/expire_snapshots.rs`` + ``expire_cleanup.rs``
- rewrite: ``maintenance/rewrite_data_files.rs``
- rollback: ``transaction/manage_snapshots.rs:164-167``
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import PySparkException, UnsupportedOperationException

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
    assert after.schema.field("id").type == pa.int64()
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
    # Divergence schema: combined content under deleted_data_files_count; pos/eq ABSENT.
    assert _schema_names(result) == [
        "deleted_data_files_count",
        "deleted_manifest_files_count",
        "deleted_manifest_lists_count",
        "deleted_statistics_files_count",
    ]
    assert result.schema.field("deleted_data_files_count").type == pa.int64()

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
    assert _schema_names(result) == [
        "rewritten_data_files_count",
        "added_data_files_count",
        "rewritten_bytes_count",
        "failed_data_files_count",
    ]
    assert result.schema.field("rewritten_data_files_count").type == pa.int32()
    assert result.schema.field("added_data_files_count").type == pa.int32()
    assert result.schema.field("rewritten_bytes_count").type == pa.int64()
    rewritten = result.column("rewritten_data_files_count")[0].as_py()
    assert rewritten >= 2

    after = spark.sql(f"SELECT id, name FROM {table} ORDER BY id").to_arrow()
    assert _arrow_ids(after) == before_ids
    assert after.schema.field("id").type == pa.int64()
    assert after.schema.field("name").type == pa.string()

    files_after = spark.sql(f"SELECT * FROM {table}.files").to_arrow()
    assert files_after.num_rows < n_files_before


def test_unknown_procedure_lists_supported(spark: ReparkSession) -> None:
    with pytest.raises(
        (UnsupportedOperationException, PySparkException),
        match=r"expire_snapshots|rewrite_data_files|not supported",
    ):
        spark.sql("CALL mem.system.not_a_real_proc(table => 'ns.events')")


def test_remove_orphan_files_refuses_loud(spark: ReparkSession) -> None:
    with pytest.raises(
        (UnsupportedOperationException, PySparkException),
        match=r"remove_orphan_files",
    ):
        spark.sql("CALL mem.system.remove_orphan_files(table => 'ns.events')")


def test_rewrite_sort_strategy_refuses_loud(spark: ReparkSession) -> None:
    spark.sql(
        f"CREATE TABLE {TABLE} USING iceberg TBLPROPERTIES ({COW}) AS SELECT 1 AS id, 'a' AS name"
    )
    with pytest.raises(
        (UnsupportedOperationException, PySparkException),
        match=r"sort|not supported|binpack|R135",
    ):
        spark.sql("CALL mem.system.rewrite_data_files(table => 'ns.events', strategy => 'sort')")
    # C1-L-001: positional strategy must refuse (never silent binpack).
    with pytest.raises(
        (UnsupportedOperationException, PySparkException),
        match=r"sort|not supported|binpack|R135",
    ):
        spark.sql("CALL mem.system.rewrite_data_files('ns.events', 'sort')")


def test_positional_rollback_args(spark: ReparkSession, multi_snapshot: dict[str, object]) -> None:
    """Spark docs: positional args accepted (cite iceberg spark-procedures Usage)."""
    s1 = multi_snapshot["s1"]
    # Re-advance past s1 first if already rolled back by another test — fixture is function-scoped.
    result = spark.sql(f"CALL mem.system.rollback_to_snapshot('ns.events', {s1})").to_arrow()
    assert result.column("current_snapshot_id")[0].as_py() == s1
    after = spark.sql(f"SELECT id FROM {TABLE} ORDER BY id").to_arrow()
    assert _arrow_ids(after) == multi_snapshot["ids_s1"]

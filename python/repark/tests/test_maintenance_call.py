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

import time
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
    assert after.schema.field("id").type == pa.int64()
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


def test_remove_orphan_files_requires_an_explicit_older_than(spark: ReparkSession) -> None:
    """MW-3 / registry row ORPHAN-1.

    Spark defaults ``older_than`` to ``now - 3 days`` and runs; this engine refuses. The
    procedure deletes files with no rollback, so the most dangerous argument must not be the one
    the caller never typed.
    """
    with pytest.raises(
        (UnsupportedOperationException, PySparkException),
        match=r"requires an explicit `older_than`",
    ):
        spark.sql("CALL mem.system.remove_orphan_files(table => 'ns.events')")


def test_remove_orphan_files_dry_run_is_the_default(spark: ReparkSession, tmp_path: Path) -> None:
    """MW-3 / registry row ORPHAN-2.

    Spark's ``dry_run`` defaults to false and DELETES. This engine defaults it to true. The
    result shape is Spark's either way: one row per orphan, ``orphan_file_location`` a
    non-nullable string, measured on a live Spark 4.0.1 + Iceberg 1.10.0 oracle.

    A table with no orphans lists none — the zero-row control that proves the column shape is
    real rather than an artefact of the fixture.
    """
    owned = tmp_path / "owned"
    spark.sql(f"CREATE NAMESPACE mem.owned LOCATION '{owned}'")
    spark.sql(
        f"CREATE TABLE mem.owned.events USING iceberg TBLPROPERTIES ({COW}) "
        "AS SELECT 1 AS id, 'a' AS name"
    )
    older_than_ms = int(time.time() * 1000) - 2 * 24 * 60 * 60 * 1000
    result = spark.sql(
        "CALL mem.system.remove_orphan_files("
        f"table => 'owned.events', older_than => {older_than_ms})"
    ).to_arrow()
    assert _schema_names(result) == ["orphan_file_location"]
    assert result.schema.field("orphan_file_location").type == pa.string()
    assert not result.schema.field("orphan_file_location").nullable
    assert result.num_rows == 0


def test_remove_orphan_files_floor_matches_spark(spark: ReparkSession, tmp_path: Path) -> None:
    """MW-3: the 24-hour floor is PARITY with Spark, not a stricter posture.

    Measured across the boundary on the oracle: ``now`` refuses, ``now - 23h`` refuses,
    ``now - 25h`` runs. Java enforces it in ``RemoveOrphanFilesProcedure`` rather than the
    Action API, which is why this engine carries it in the CALL router too.
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
    result = spark.sql(f"CALL mem.system.rollback_to_snapshot('ns.events', {s1})").to_arrow()
    assert result.column("current_snapshot_id")[0].as_py() == s1
    after = spark.sql(f"SELECT id FROM {TABLE} ORDER BY id").to_arrow()
    assert _arrow_ids(after) == multi_snapshot["ids_s1"]


def test_remove_orphan_files_refuses_the_shared_ctas_fallback_root(
    spark: ReparkSession,
) -> None:
    """MW-3: a table in the shared CTAS fallback root is not sweepable.

    ``register_memory_catalog`` carries a ``TempFallbackAllowed`` policy, so a namespace created
    with no ``location`` places its tables at ``<warehouse>/repark_ctas/<catalog>/<ns>/<table>``
    (A13: ``warehouse`` is the catalog argument, not the process temp dir). That path is still
    derived from NAMES under the warehouse, so two processes sharing one warehouse share the
    directory. Orphan removal deletes what one table's metadata does not reference.
    """
    spark.sql(
        f"CREATE TABLE {TABLE} USING iceberg TBLPROPERTIES ({COW}) AS SELECT 1 AS id, 'a' AS name"
    )
    older_than_ms = int(time.time() * 1000) - 2 * 24 * 60 * 60 * 1000
    with pytest.raises(
        (UnsupportedOperationException, PySparkException),
        match=r"shared CTAS fallback root",
    ):
        spark.sql(
            "CALL mem.system.remove_orphan_files("
            f"table => 'ns.events', older_than => {older_than_ms})"
        )


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
    assert after.schema.field("id").type == pa.int64()
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


def test_rewrite_manifests_spec_id_refuses_and_use_caching_is_accepted(
    spark: ReparkSession,
) -> None:
    """MW-6 / registry row MANIFEST-2 — the argument surface.

    Spark takes ``table``, ``use_caching`` and ``spec_id``. ``use_caching`` caches Spark's own
    manifest DataFrame and changed no count on the oracle, so this engine accepts it and does
    nothing with it. ``spec_id`` selects which partition spec to rewrite; this engine always
    rewrites the current one and refuses the argument rather than accepting one value of it.

    pins: mw-6-rewrite-manifests/C-007, C-008
    """
    table = "mem.ns.args"
    spark.sql(
        f"CREATE TABLE {table} USING iceberg TBLPROPERTIES ({COW}) AS SELECT 1 AS id, 'a' AS name"
    )
    for index in range(2, 6):
        spark.sql(f"INSERT INTO {table} SELECT {index} AS id, 'x' AS name")

    with pytest.raises(
        (UnsupportedOperationException, PySparkException),
        match=r"spec_id",
    ):
        spark.sql("CALL mem.system.rewrite_manifests(table => 'ns.args', spec_id => 0)")

    result = spark.sql(
        "CALL mem.system.rewrite_manifests(table => 'ns.args', use_caching => true)"
    ).to_arrow()
    assert result.column("rewritten_manifests_count")[0].as_py() == 5
    assert result.column("added_manifests_count")[0].as_py() == 1

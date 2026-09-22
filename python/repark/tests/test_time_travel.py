"""Oracle — snapshot / timestamp / branch / tag reads.

Pins each snapshot's row multiset AND Arrow schema via ``collect`` / ``to_arrow``; composition
with filter/projection; current-read unaffected after time-travel reads. Reader options + SQL
spellings. Local memory-catalog only (no AWS, no docker).

Fork pin ``4723104b``:
- ``IcebergStaticTableProvider::try_new_from_table_snapshot`` —
  ``crates/integrations/datafusion/src/table/mod.rs``
- ``snapshot_id_as_of_time`` (``<=``) — ``crates/iceberg/src/inspect/metadata_log_entries.rs``
- ManageSnapshots create_branch/tag — ``crates/iceberg/src/transaction/manage_snapshots.rs``

``VERSION AS OF`` accepts branch/tag names (Spark Iceberg docs, "Time travel").

pins: ice-metadata-cols-1/C-011, C-012, C-013, C-014
"""

from __future__ import annotations

import re
import time
from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, IllegalArgumentException, UnsupportedOperationException

TABLE = "mem.ns.events"
SELECTOR_TABLE = "mem.ns.tt_selector"
COW = """
    'format-version' = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write'
"""
SNAPSHOT_ID_REFUSAL = (
    "Time travel option `snapshot-id` is no longer supported, "
    "use Spark built-in `versionAsOf` instead"
)
AS_OF_TIMESTAMP_REFUSAL = (
    "Time travel option `as-of-timestamp` (in millis) is no longer supported, "
    "use Spark built-in `timestampAsOf` instead (properly formatted timestamp)"
)
TAG_REFUSAL = (
    "Time travel option `tag` is no longer supported, use Spark built-in `versionAsOf` instead"
)


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-time-travel").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def _arrow_ids(table: pa.Table) -> list[int]:
    """Sorted id multiset from an Arrow table (value pin)."""
    ids = table.column("id").to_pylist()
    return sorted(int(value) for value in ids if value is not None)


def _schema_names_types(table: pa.Table) -> list[tuple[str, str]]:
    """Arrow field name + type string pins (divergence-class: value AND type)."""
    return [(field.name, str(field.type)) for field in table.schema]


def _rows_id_data_cat(table: pa.Table) -> list[tuple[int, str, str]]:
    """Sorted full-row pin for the (id, data, cat) selector tables (value pin)."""
    rows = table.select(["id", "data", "cat"]).to_pylist()
    return sorted((int(row["id"]), str(row["data"]), str(row["cat"])) for row in rows)


@pytest.fixture
def multi_snapshot(spark: ReparkSession) -> dict[str, object]:
    """Build ≥3 snapshots (CTAS, append, MERGE) + one tag + one branch.

    Returns snapshot ids, timestamps, and expected id multisets per pin.
    """
    spark.sql(
        f"CREATE TABLE {TABLE} USING iceberg TBLPROPERTIES ({COW}) AS "
        "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)"
    )
    snaps = spark._testing_list_snapshots(TABLE)
    assert len(snaps) >= 1
    s1, s1_ts = snaps[-1]

    spark.sql(f"INSERT INTO {TABLE} SELECT 4 AS id, 'd' AS name")
    snaps = spark._testing_list_snapshots(TABLE)
    s2, s2_ts = snaps[-1]
    assert s2 != s1

    # Snapshot 3 via MERGE (matched update + not-matched insert) — third write shape.
    spark.sql(
        "SELECT 2 AS id, 'bee' AS name UNION ALL SELECT 5 AS id, 'e' AS name"
    ).createOrReplaceTempView("upd")
    spark.sql(
        f"MERGE INTO {TABLE} AS t USING upd AS s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET t.name = s.name "
        "WHEN NOT MATCHED THEN INSERT *"
    )
    snaps = spark._testing_list_snapshots(TABLE)
    s3, s3_ts = snaps[-1]
    assert s3 != s2
    assert len(snaps) >= 3

    spark._testing_create_ref(TABLE, "tag", "tag_s1", s1)
    spark._testing_create_ref(TABLE, "branch", "branch_s2", s2)

    return {
        "s1": s1,
        "s1_ts": s1_ts,
        "s2": s2,
        "s2_ts": s2_ts,
        "s3": s3,
        "s3_ts": s3_ts,
        "ids_s1": [1, 2, 3],
        "ids_s2": [1, 2, 3, 4],
        # MERGE: 2→bee, +5; still has 1,3,4
        "ids_s3": [1, 2, 3, 4, 5],
    }


@pytest.fixture
def selector_snapshots(spark: ReparkSession) -> dict[str, object]:
    """Three snapshots over (id, data, cat): s0 holds two rows, s1 three, s2 drops id 1.

    Mirrors the R-TT-*SELECTOR harness cells (``cells_read.py`` ``base3``) so the recorded
    rows replay against the memory catalog.
    """
    spark.sql(
        f"CREATE TABLE {SELECTOR_TABLE} USING iceberg TBLPROPERTIES ({COW}) AS "
        "SELECT * FROM (VALUES (1, 'a', 'x'), (2, 'b', 'y')) AS t(id, data, cat)"
    )
    s0 = spark._testing_list_snapshots(SELECTOR_TABLE)[-1][0]
    spark.sql(f"INSERT INTO {SELECTOR_TABLE} SELECT 3 AS id, 'c' AS data, 'x' AS cat")
    s1, s1_ts = spark._testing_list_snapshots(SELECTOR_TABLE)[-1]
    assert s1 != s0
    time.sleep(0.02)
    spark.sql(f"DELETE FROM {SELECTOR_TABLE} WHERE id = 1")
    s2, s2_ts = spark._testing_list_snapshots(SELECTOR_TABLE)[-1]
    assert s2 != s1
    assert int(s1_ts) < int(s2_ts)  # type: ignore[arg-type]
    return {"s0": s0, "s1": s1, "s1_ts": s1_ts, "s2": s2}


def test_sql_version_as_of_snapshot_id(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    s1 = multi_snapshot["s1"]
    s2 = multi_snapshot["s2"]
    arrow = spark.sql(f"SELECT id, name FROM {TABLE} VERSION AS OF {s1} ORDER BY id").to_arrow()
    assert _arrow_ids(arrow) == multi_snapshot["ids_s1"]
    assert _schema_names_types(arrow) == [("id", "int32"), ("name", "string")], (
        "VALUES-inferred ids are INT (int32) on the Iceberg/Arrow path"
    )

    arrow2 = spark.sql(
        f"SELECT id FROM {TABLE} FOR SYSTEM_VERSION AS OF {s2} ORDER BY id"
    ).to_arrow()
    assert _arrow_ids(arrow2) == multi_snapshot["ids_s2"]


def test_sql_timestamp_as_of(spark: ReparkSession, multi_snapshot: dict[str, object]) -> None:
    s1_ts = int(multi_snapshot["s1_ts"])  # type: ignore[arg-type]
    s2_ts = int(multi_snapshot["s2_ts"])  # type: ignore[arg-type]
    s3_ts = int(multi_snapshot["s3_ts"])  # type: ignore[arg-type]
    assert s1_ts < s2_ts <= s3_ts

    arrow = spark.sql(f"SELECT id FROM {TABLE} TIMESTAMP AS OF {s1_ts} ORDER BY id").to_arrow()
    assert _arrow_ids(arrow) == multi_snapshot["ids_s3"]

    arrow_sys = spark.sql(
        f"SELECT id FROM {TABLE} FOR SYSTEM_TIME AS OF {s1_ts} ORDER BY id"
    ).to_arrow()
    assert _arrow_ids(arrow_sys) == multi_snapshot["ids_s3"]

    arrow_s2 = spark.sql(f"SELECT id FROM {TABLE} TIMESTAMP AS OF {s2_ts} ORDER BY id").to_arrow()
    assert _arrow_ids(arrow_s2) == multi_snapshot["ids_s3"]

    arrow_s3 = spark.sql(f"SELECT id FROM {TABLE} TIMESTAMP AS OF {s3_ts} ORDER BY id").to_arrow()
    assert _arrow_ids(arrow_s3) == multi_snapshot["ids_s3"]

    mid = s1_ts + max(1, (s2_ts - s1_ts) // 2)
    if mid < s2_ts:
        arrow_mid = spark.sql(
            f"SELECT id FROM {TABLE} TIMESTAMP AS OF {mid} ORDER BY id"
        ).to_arrow()
        assert _arrow_ids(arrow_mid) == multi_snapshot["ids_s3"]

    with pytest.raises(IllegalArgumentException, match=r"snapshot older than"):
        spark.sql(f"SELECT * FROM {TABLE} TIMESTAMP AS OF {s1_ts // 1000 - 3600}").to_arrow()


def test_sql_version_as_of_branch_and_tag(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    arrow_tag = spark.sql(f"SELECT id FROM {TABLE} VERSION AS OF 'tag_s1' ORDER BY id").to_arrow()
    assert _arrow_ids(arrow_tag) == multi_snapshot["ids_s1"]

    arrow_branch = spark.sql(
        f"SELECT id FROM {TABLE} VERSION AS OF 'branch_s2' ORDER BY id"
    ).to_arrow()
    assert _arrow_ids(arrow_branch) == multi_snapshot["ids_s2"]


def test_unknown_snapshot_and_ref_name_the_pin(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    _ = multi_snapshot
    with pytest.raises(IllegalArgumentException, match="999999999"):
        spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF 999999999").to_arrow()
    with pytest.raises(IllegalArgumentException, match="no_such_ref"):
        spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF 'no_such_ref'").to_arrow()


def test_filter_projection_composition(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    s1 = multi_snapshot["s1"]
    arrow = spark.sql(
        f"SELECT id FROM {TABLE} VERSION AS OF {s1} WHERE id >= 2 ORDER BY id"
    ).to_arrow()
    assert _arrow_ids(arrow) == [2, 3]
    assert _schema_names_types(arrow) == [("id", "int32")]


def test_current_read_unaffected_after_time_travel(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    s1 = multi_snapshot["s1"]
    _ = spark.sql(f"SELECT id FROM {TABLE} VERSION AS OF {s1}").to_arrow()
    current = spark.sql(f"SELECT id FROM {TABLE} ORDER BY id").to_arrow()
    assert _arrow_ids(current) == multi_snapshot["ids_s3"]


def test_reader_option_snapshot_id(spark: ReparkSession, multi_snapshot: dict[str, object]) -> None:
    s1 = multi_snapshot["s1"]
    with pytest.raises(IllegalArgumentException, match=re.escape(SNAPSHOT_ID_REFUSAL)):
        spark.read.format("iceberg").option("snapshot-id", str(s1)).load(TABLE)


def test_reader_option_as_of_timestamp(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    s1_ts = multi_snapshot["s1_ts"]
    with pytest.raises(IllegalArgumentException, match=re.escape(AS_OF_TIMESTAMP_REFUSAL)):
        spark.read.format("iceberg").option("as-of-timestamp", str(s1_ts)).load(TABLE)


def test_reader_option_branch_and_tag(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    arrow_branch = (
        spark.read.format("iceberg")
        .option("branch", "branch_s2")
        .load(TABLE)
        .select("id")
        .to_arrow()
    )
    assert _arrow_ids(arrow_branch) == multi_snapshot["ids_s2"]

    with pytest.raises(IllegalArgumentException, match=re.escape(TAG_REFUSAL)):
        spark.read.format("iceberg").option("tag", "tag_s1").load(TABLE)


def test_reader_option_combinations_refuse_legacy_first(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    s1 = multi_snapshot["s1"]
    s1_ts = multi_snapshot["s1_ts"]
    refusing_pairs = [
        ((("snapshot-id", str(s1)), ("branch", "branch_s2")), SNAPSHOT_ID_REFUSAL),
        ((("snapshot-id", str(s1)), ("tag", "tag_s1")), SNAPSHOT_ID_REFUSAL),
        ((("snapshot-id", str(s1)), ("as-of-timestamp", str(s1_ts))), SNAPSHOT_ID_REFUSAL),
        ((("branch", "branch_s2"), ("tag", "tag_s1")), TAG_REFUSAL),
        ((("as-of-timestamp", str(s1_ts)), ("branch", "branch_s2")), AS_OF_TIMESTAMP_REFUSAL),
        ((("as-of-timestamp", str(s1_ts)), ("tag", "tag_s1")), AS_OF_TIMESTAMP_REFUSAL),
    ]
    for ((key_a, value_a), (key_b, value_b)), expected in refusing_pairs:
        with pytest.raises(IllegalArgumentException, match=re.escape(expected)):
            spark.read.format("iceberg").option(key_a, value_a).option(key_b, value_b).load(TABLE)
    with pytest.raises(IllegalArgumentException, match=re.escape(SNAPSHOT_ID_REFUSAL)):
        (
            spark.read.format("iceberg")
            .option("snapshot-id", str(s1))
            .option("branch", "branch_s2")
            .option("tag", "tag_s1")
            .load(TABLE)
        )


def test_incremental_snapshot_bounds_reach_the_incremental_scan(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """ICE-CHANGELOG-1: the window options are served; Spark's two refusals hold.

    pins: ice-changelog-1/C-004, C-007
    """
    s1 = multi_snapshot["s1"]
    s3 = multi_snapshot["s3"]
    with pytest.raises(IllegalArgumentException, match=r"is not a parent ancestor of end snapshot"):
        (
            spark.read.format("iceberg")
            .option("start-snapshot-id", str(s3))
            .option("end-snapshot-id", str(s1))
            .load(TABLE)
        )
    with pytest.raises(IllegalArgumentException, match=r"Cannot set only `end-snapshot-id`"):
        spark.read.format("iceberg").option("end-snapshot-id", str(s1)).load(TABLE)


def test_write_to_branch_unsupported(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """Write path stays CURRENT-only — branch writer option fails loud."""
    _ = multi_snapshot
    frame = spark.sql("SELECT 99 AS id, 'x' AS name")
    with pytest.raises(UnsupportedOperationException, match=r"branch|current-snapshot"):
        frame.writeTo(TABLE).option("branch", "branch_s2").append()


def _tt_registrations(spark: ReparkSession) -> list[str]:
    """Ephemeral ``__repark_tt_*`` names currently registered on the session."""
    spark._ensure_information_schema()
    return (
        spark.sql(
            "SELECT table_name FROM information_schema.tables WHERE table_name LIKE '__repark_tt_%'"
        )
        .to_arrow()
        .column("table_name")
        .to_pylist()
    )


def test_time_travel_temp_views_hidden_from_list_tables(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """Ephemeral ``__repark_tt_*`` pins must not surface in Catalog.listTables.

    The two producers differ: the SQL rewrite releases its pins once the statement is planned,
    while the reader-options path keeps one registration that backs the returned DataFrame —
    which keeps this pin non-vacuous. The real table's positive membership is asserted first, so
    an empty listing cannot green the filter assertion.
    """
    s1 = multi_snapshot["s1"]
    before = _tt_registrations(spark)

    _ = spark.sql(f"SELECT id FROM {TABLE} VERSION AS OF {s1}").to_arrow()
    assert _tt_registrations(spark) == before, (
        "the SQL time-travel rewrite must release its ephemeral pins once the statement is planned"
    )

    _ = spark.read.format("iceberg").option("versionAsOf", str(s1)).load(TABLE).to_arrow()
    after_read = _tt_registrations(spark)
    assert len(after_read) > len(before), (
        "the reader-options pin must still be registered — otherwise the listTables assertion "
        f"below is vacuous (before={before}, after={after_read})"
    )

    ns_listed = [table.name for table in spark.catalog.listTables("ns")]
    listed = [*ns_listed, *(table.name for table in spark.catalog.listTables())]
    # Positive membership first: an empty listing would green the leak assertion below wrongly.
    assert TABLE.rsplit(".", 1)[-1] in ns_listed, (
        f"listTables must still list the real table it is filtering around: {ns_listed}"
    )
    leaked = [name for name in listed if str(name).startswith("__repark_tt_")]
    assert leaked == [], f"time-travel temp views leaked into listTables: {leaked}"


def test_two_part_identifier_expands_for_time_travel(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """Two-part ``ns.table`` expands under current catalog so VERSION AS OF works."""
    s1 = multi_snapshot["s1"]
    arrow = spark.sql(f"SELECT id FROM ns.events VERSION AS OF {s1} ORDER BY id").to_arrow()
    assert _arrow_ids(arrow) == multi_snapshot["ids_s1"]


def test_write_to_tag_unsupported(spark: ReparkSession, multi_snapshot: dict[str, object]) -> None:
    """WriterV2.tag option refuses loud (same CURRENT-only contract as branch)."""
    _ = multi_snapshot
    frame = spark.sql("SELECT 99 AS id, 'x' AS name")
    with pytest.raises(UnsupportedOperationException, match=r"tag|current-snapshot"):
        frame.writeTo(TABLE).option("tag", "tag_s1").append()


def test_negative_snapshot_id_sql_is_recognized(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """Unary-minus snapshot ids must be rewritten (Iceberg ids are signed i64).

    An unknown negative id must still be *named* by the error, not dropped to a generic SQL
    parse failure.
    """
    _ = multi_snapshot
    with pytest.raises(IllegalArgumentException, match=r"-999999999999"):
        spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF -999999999999").to_arrow()


def test_timestamp_rfc3339_zulu_sql(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """TIMESTAMP AS OF accepts RFC3339 / Zulu wall-clock strings."""
    s1_ts = int(multi_snapshot["s1_ts"])  # type: ignore[arg-type]
    # Millisecond precision: second-truncation lands before s1_ts and trips the
    # earlier-than-first guard.
    from datetime import UTC, datetime

    seconds, millis = divmod(s1_ts, 1000)
    zulu = datetime.fromtimestamp(seconds, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S") + f".{millis:03d}Z"
    arrow = spark.sql(f"SELECT id FROM {TABLE} TIMESTAMP AS OF '{zulu}' ORDER BY id").to_arrow()
    assert _arrow_ids(arrow) == multi_snapshot["ids_s1"]


def test_read_iceberg_table_mutex_kwargs(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """Direct ``read_iceberg_table`` kwargs enforce mutual exclusion (Rust into_spec)."""
    s1 = multi_snapshot["s1"]
    with pytest.raises(AnalysisException, match=r"mutually exclusive"):
        spark.read_iceberg_table(TABLE, snapshot_id=int(s1), branch="branch_s2")  # type: ignore[arg-type]


def test_empty_branch_option_fails_loud(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """Empty branch pin and any tag pin fail loud (not silent current-snapshot)."""
    _ = multi_snapshot
    with pytest.raises(AnalysisException):
        spark.read.format("iceberg").option("branch", "").load(TABLE).to_arrow()
    with pytest.raises(IllegalArgumentException, match=re.escape(TAG_REFUSAL)):
        spark.read.format("iceberg").option("tag", "   ").load(TABLE).to_arrow()


def test_cte_version_as_of(spark: ReparkSession, multi_snapshot: dict[str, object]) -> None:
    """WITH … AS (SELECT … VERSION AS OF) rewrites inside CTEs."""
    s1 = multi_snapshot["s1"]
    arrow = spark.sql(
        f"WITH q AS (SELECT id FROM {TABLE} VERSION AS OF {s1}) SELECT id FROM q ORDER BY id"
    ).to_arrow()
    assert _arrow_ids(arrow) == multi_snapshot["ids_s1"]


def test_snapshot_id_overflow_refuses_before_parsing(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """Out-of-range snapshot-id still refuses with the legacy text (refusal precedes parsing)."""
    _ = multi_snapshot
    with pytest.raises(IllegalArgumentException, match=re.escape(SNAPSHOT_ID_REFUSAL)):
        spark.read.format("iceberg").option("snapshot-id", str(2**63)).load(TABLE)


def test_ctas_from_version_as_of(spark: ReparkSession, multi_snapshot: dict[str, object]) -> None:
    """CTAS source may pin VERSION AS OF (historical materialize)."""
    s1 = multi_snapshot["s1"]
    hist = "mem.ns.tt_hist"
    spark.sql(
        f"CREATE TABLE {hist} USING iceberg TBLPROPERTIES ({COW}) AS "
        f"SELECT id, name FROM {TABLE} VERSION AS OF {s1}"
    )
    arrow = spark.sql(f"SELECT id FROM {hist} ORDER BY id").to_arrow()
    assert _arrow_ids(arrow) == multi_snapshot["ids_s1"]


def test_merge_using_version_as_of_source(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """MERGE USING (SELECT … VERSION AS OF) pins the source snapshot."""
    s1 = multi_snapshot["s1"]
    target = "mem.ns.tt_merge_tgt"
    spark.sql(
        f"CREATE TABLE {target} USING iceberg TBLPROPERTIES ({COW}) AS "
        "SELECT * FROM (VALUES (1, 'old'), (9, 'x')) AS t(id, name)"
    )
    spark.sql(
        f"MERGE INTO {target} AS t "
        f"USING (SELECT id, name FROM {TABLE} VERSION AS OF {s1}) s "
        "ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET t.name = s.name "
        "WHEN NOT MATCHED THEN INSERT *"
    )
    arrow = spark.sql(f"SELECT id, name FROM {target} ORDER BY id").to_arrow()
    assert _arrow_ids(arrow) == [1, 2, 3, 9]
    # id=1 updated from s1 ('a'); id=2,3 inserted from s1; id=9 kept.
    names = {
        int(i): n
        for i, n in zip(
            arrow.column("id").to_pylist(),
            arrow.column("name").to_pylist(),
            strict=True,
        )
    }
    assert names[1] == "a"
    assert names[9] == "x"


def test_insert_select_version_as_of(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """INSERT…SELECT source may VERSION AS OF (rewrite runs on full statement)."""
    s1 = multi_snapshot["s1"]
    dest = "mem.ns.tt_insert_dst"
    spark.sql(
        f"CREATE TABLE {dest} USING iceberg TBLPROPERTIES ({COW}) AS "
        "SELECT * FROM (VALUES (0, 'z')) AS t(id, name)"
    )
    spark.sql(f"INSERT INTO {dest} SELECT id, name FROM {TABLE} VERSION AS OF {s1}")
    arrow = spark.sql(f"SELECT id FROM {dest} ORDER BY id").to_arrow()
    assert _arrow_ids(arrow) == [0, 1, 2, 3]


def test_subquery_version_as_of(spark: ReparkSession, multi_snapshot: dict[str, object]) -> None:
    s1 = multi_snapshot["s1"]
    arrow = spark.sql(
        f"SELECT id FROM (SELECT id FROM {TABLE} VERSION AS OF {s1}) q ORDER BY id"
    ).to_arrow()
    assert _arrow_ids(arrow) == multi_snapshot["ids_s1"]


def test_reader_option_case_insensitive_snapshot_id(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    s1 = multi_snapshot["s1"]
    with pytest.raises(IllegalArgumentException, match=re.escape(SNAPSHOT_ID_REFUSAL)):
        spark.read.format("iceberg").option("SNAPSHOT-ID", str(s1)).load(TABLE)


def test_branch_option_trims_whitespace(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """Reader branch/tag options trim padding."""
    _ = multi_snapshot
    arrow = (
        spark.read.format("iceberg")
        .option("branch", "  branch_s2  ")
        .load(TABLE)
        .select("id")
        .to_arrow()
    )
    assert _arrow_ids(arrow) == multi_snapshot["ids_s2"]


def test_schema_at_snapshot_not_current_schema(spark: ReparkSession, tmp_path: Path) -> None:
    """Pinned snapshot schema is the snapshot's schema, not current (static provider pin).

    A post-hoc filter on the *current* table would expose evolved columns at old pins; VERSION
    AS OF s1 must keep the pre-evolution field set.
    """
    table = "mem.ns.schema_tt"
    spark.sql(
        f"CREATE TABLE {table} USING iceberg TBLPROPERTIES ({COW}) AS "
        "SELECT * FROM (VALUES (1, 'a')) AS t(id, name)"
    )
    s1 = spark._testing_list_snapshots(table)[-1][0]
    spark.sql(
        f"CREATE OR REPLACE TABLE {table} USING iceberg TBLPROPERTIES ({COW}) AS "
        "SELECT 1 AS id, 'a' AS name, 9 AS extra"
    )
    s2 = spark._testing_list_snapshots(table)[-1][0]
    assert s2 != s1

    arrow_s1 = spark.sql(f"SELECT * FROM {table} VERSION AS OF {s1}").to_arrow()
    assert _schema_names_types(arrow_s1) == [("id", "int32"), ("name", "string")]
    assert "extra" not in arrow_s1.column_names
    assert _arrow_ids(arrow_s1) == [1]

    arrow_s2 = spark.sql(f"SELECT * FROM {table} VERSION AS OF {s2}").to_arrow()
    assert [name for name, _ in _schema_names_types(arrow_s2)] == ["id", "name", "extra"]
    assert arrow_s2.column("extra").to_pylist() == [9]

    current = spark.sql(f"SELECT * FROM {table}").to_arrow()
    assert "extra" in current.column_names


def test_system_version_as_of_string_ref(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """FOR SYSTEM_VERSION AS OF '<ref>' resolves branch/tag (same as VERSION AS OF)."""
    arrow = spark.sql(
        f"SELECT id FROM {TABLE} FOR SYSTEM_VERSION AS OF 'tag_s1' ORDER BY id"
    ).to_arrow()
    assert _arrow_ids(arrow) == multi_snapshot["ids_s1"]


def test_time_travel_options_rejected_on_parquet(
    spark: ReparkSession, multi_snapshot: dict[str, object], tmp_path: Path
) -> None:
    """snapshot-id / branch on format('parquet') stay loud (not silently ignored)."""
    _ = multi_snapshot
    path = tmp_path / "x.parquet"
    spark.sql("SELECT 1 AS id").write.mode("overwrite").parquet(str(path))
    with pytest.raises(AnalysisException, match=r"iceberg|time travel"):
        spark.read.format("parquet").option("snapshot-id", "1").load(str(path))


def test_multi_table_version_as_of_join(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """Two VERSION AS OF clauses in one JOIN rewrite independently (right-to-left splice)."""
    s1 = multi_snapshot["s1"]
    s2 = multi_snapshot["s2"]
    other = "mem.ns.events_b"
    spark.sql(
        f"CREATE TABLE {other} USING iceberg TBLPROPERTIES ({COW}) AS "
        "SELECT * FROM (VALUES (10, 'z')) AS t(id, name)"
    )
    snaps = spark._testing_list_snapshots(other)
    other_s1 = snaps[-1][0]
    arrow = spark.sql(
        f"SELECT a.id AS a_id, b.id AS b_id FROM {TABLE} VERSION AS OF {s1} a "
        f"JOIN {other} VERSION AS OF {other_s1} b ON 1=1 ORDER BY a_id"
    ).to_arrow()
    # Cross join: |s1| rows x 1 right row.
    assert sorted(int(v) for v in arrow.column("a_id").to_pylist()) == multi_snapshot["ids_s1"]
    assert all(int(v) == 10 for v in arrow.column("b_id").to_pylist())
    assert arrow.num_rows == len(multi_snapshot["ids_s1"])  # type: ignore[arg-type]
    arrow_s2 = spark.sql(
        f"SELECT a.id FROM {TABLE} VERSION AS OF {s2} a "
        f"JOIN {other} VERSION AS OF {other_s1} b ON 1=1 ORDER BY id"
    ).to_arrow()
    assert _arrow_ids(arrow_s2) == multi_snapshot["ids_s2"]


def test_snapshot_id_selector_reads_pinned_snapshot(
    spark: ReparkSession, selector_snapshots: dict[str, object]
) -> None:
    """R-TT-SNAPSHOT-ID-SELECTOR: SELECT * FROM t.snapshot_id_<id> reads that snapshot."""
    s0 = selector_snapshots["s0"]
    arrow = spark.sql(f"SELECT * FROM {SELECTOR_TABLE}.snapshot_id_{s0}").to_arrow()
    assert _rows_id_data_cat(arrow) == [(1, "a", "x"), (2, "b", "y")]
    assert _schema_names_types(arrow) == [("id", "int32"), ("data", "string"), ("cat", "string")]


def test_at_timestamp_selector_reads_pinned_snapshot(
    spark: ReparkSession, selector_snapshots: dict[str, object]
) -> None:
    """R-TT-AT-TIMESTAMP-SELECTOR: SELECT * FROM t.at_timestamp_<ms> reads as of that ms."""
    s1_ts = selector_snapshots["s1_ts"]
    arrow = spark.sql(f"SELECT * FROM {SELECTOR_TABLE}.at_timestamp_{s1_ts}").to_arrow()
    assert _rows_id_data_cat(arrow) == [(1, "a", "x"), (2, "b", "y"), (3, "c", "x")]
    assert _schema_names_types(arrow) == [("id", "int32"), ("data", "string"), ("cat", "string")]


def test_snapshot_id_selector_bad_suffix_refuses(
    spark: ReparkSession, selector_snapshots: dict[str, object]
) -> None:
    """Unparsable numeric selector suffixes refuse typed, never table-not-found."""
    _ = selector_snapshots
    with pytest.raises(IllegalArgumentException, match=r"invalid snapshot_id selector"):
        spark.sql(f"SELECT * FROM {SELECTOR_TABLE}.snapshot_id_abc").to_arrow()
    with pytest.raises(IllegalArgumentException, match=r"must be an integer snapshot id"):
        spark.sql(f"SELECT * FROM {SELECTOR_TABLE}.snapshot_id_").to_arrow()
    with pytest.raises(IllegalArgumentException, match=r"invalid at_timestamp selector"):
        spark.sql(f"SELECT * FROM {SELECTOR_TABLE}.at_timestamp_xyz").to_arrow()
    with pytest.raises(IllegalArgumentException, match=r"must be an integer millisecond"):
        spark.sql(f"SELECT * FROM {SELECTOR_TABLE}.at_timestamp_").to_arrow()


def test_branch_selector_still_resolves_as_branch_ref(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """t.branch_<name> / t.tag_<name> keep resolving (numeric selectors change nothing)."""
    arrow_branch = spark.sql(f"SELECT id FROM {TABLE}.branch_branch_s2 ORDER BY id").to_arrow()
    assert _arrow_ids(arrow_branch) == multi_snapshot["ids_s2"]
    arrow_tag = spark.sql(f"SELECT id FROM {TABLE}.tag_tag_s1 ORDER BY id").to_arrow()
    assert _arrow_ids(arrow_tag) == multi_snapshot["ids_s1"]


def test_branch_metadata_composition_still_errors(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """t.branch_b.files keeps its current error (A-7: behavior unchanged, not success)."""
    _ = multi_snapshot
    with pytest.raises(AnalysisException, match=r"compound identifier"):
        spark.sql(f"SELECT count(*) FROM {TABLE}.branch_b.files").to_arrow()

"""I1 / R-TIME-TRAVEL oracle — snapshot / timestamp / branch / tag reads.

Pins each snapshot's row multiset AND Arrow schema via ``collect`` / ``to_arrow``;
composition with filter/projection; current-read unaffected after time-travel reads.
Reader options + SQL spellings. Local memory-catalog only (no AWS, no docker).

Oracle discipline: fork cite + local multiset pins (docs/testing.md divergence-class rules).
Fork pin ``4723104b``:
- ``IcebergStaticTableProvider::try_new_from_table_snapshot`` —
  ``crates/integrations/datafusion/src/table/mod.rs:420``
- ``snapshot_id_as_of_time`` (``<=``) —
  ``crates/iceberg/src/inspect/metadata_log_entries.rs:129-138``
- ManageSnapshots create_branch/tag —
  ``crates/iceberg/src/transaction/manage_snapshots.rs:90-108``

``VERSION AS OF`` accepts branch/tag names (Spark Iceberg docs — Time travel /
"You can use branch or tag names with VERSION AS OF").
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, UnsupportedOperationException

TABLE = "mem.ns.events"
COW = """
    'format-version' = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write'
"""


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


def test_sql_version_as_of_snapshot_id(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    s1 = multi_snapshot["s1"]
    s2 = multi_snapshot["s2"]
    arrow = spark.sql(f"SELECT id, name FROM {TABLE} VERSION AS OF {s1} ORDER BY id").to_arrow()
    assert _arrow_ids(arrow) == multi_snapshot["ids_s1"]
    # VALUES-inferred ids are BIGINT (int64) on the Iceberg/Arrow path — pin type, not only names.
    assert _schema_names_types(arrow) == [("id", "int64"), ("name", "string")]

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
    assert _arrow_ids(arrow) == multi_snapshot["ids_s1"]

    arrow_sys = spark.sql(
        f"SELECT id FROM {TABLE} FOR SYSTEM_TIME AS OF {s1_ts} ORDER BY id"
    ).to_arrow()
    assert _arrow_ids(arrow_sys) == multi_snapshot["ids_s1"]

    # Latest-match pin: as-of exactly s2_ts must yield s2, not s1 (mutation-proof for
    # first-match-break / non-latest history walk — octo C1-Q-001 / C1-L-001).
    arrow_s2 = spark.sql(f"SELECT id FROM {TABLE} TIMESTAMP AS OF {s2_ts} ORDER BY id").to_arrow()
    assert _arrow_ids(arrow_s2) == multi_snapshot["ids_s2"]

    arrow_s3 = spark.sql(f"SELECT id FROM {TABLE} TIMESTAMP AS OF {s3_ts} ORDER BY id").to_arrow()
    assert _arrow_ids(arrow_s3) == multi_snapshot["ids_s3"]

    # Mid-interval: (s1_ts, s2_ts) → still s1 under timestamp_ms <= as_of (C1-L-002).
    mid = s1_ts + max(1, (s2_ts - s1_ts) // 2)
    if mid < s2_ts:
        arrow_mid = spark.sql(
            f"SELECT id FROM {TABLE} TIMESTAMP AS OF {mid} ORDER BY id"
        ).to_arrow()
        assert _arrow_ids(arrow_mid) == multi_snapshot["ids_s1"]

    with pytest.raises(AnalysisException, match=r"earlier|no Iceberg snapshot"):
        spark.sql(f"SELECT * FROM {TABLE} TIMESTAMP AS OF {s1_ts - 1}").to_arrow()


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
    with pytest.raises(AnalysisException, match="999999999"):
        spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF 999999999").to_arrow()
    with pytest.raises(AnalysisException, match="no_such_ref"):
        spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF 'no_such_ref'").to_arrow()


def test_filter_projection_composition(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    s1 = multi_snapshot["s1"]
    arrow = spark.sql(
        f"SELECT id FROM {TABLE} VERSION AS OF {s1} WHERE id >= 2 ORDER BY id"
    ).to_arrow()
    assert _arrow_ids(arrow) == [2, 3]
    assert _schema_names_types(arrow) == [("id", "int64")]


def test_current_read_unaffected_after_time_travel(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    s1 = multi_snapshot["s1"]
    _ = spark.sql(f"SELECT id FROM {TABLE} VERSION AS OF {s1}").to_arrow()
    current = spark.sql(f"SELECT id FROM {TABLE} ORDER BY id").to_arrow()
    assert _arrow_ids(current) == multi_snapshot["ids_s3"]


def test_reader_option_snapshot_id(spark: ReparkSession, multi_snapshot: dict[str, object]) -> None:
    s1 = multi_snapshot["s1"]
    arrow = (
        spark.read.format("iceberg")
        .option("snapshot-id", str(s1))
        .load(TABLE)
        .select("id")
        .to_arrow()
    )
    assert _arrow_ids(arrow) == multi_snapshot["ids_s1"]
    assert "id" in arrow.column_names


def test_reader_option_as_of_timestamp(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    s1_ts = multi_snapshot["s1_ts"]
    arrow = (
        spark.read.format("iceberg")
        .option("as-of-timestamp", str(s1_ts))
        .load(TABLE)
        .select("id")
        .to_arrow()
    )
    assert _arrow_ids(arrow) == multi_snapshot["ids_s1"]


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

    arrow_tag = (
        spark.read.format("iceberg").option("tag", "tag_s1").load(TABLE).select("id").to_arrow()
    )
    assert _arrow_ids(arrow_tag) == multi_snapshot["ids_s1"]


def test_reader_options_mutually_exclusive(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    s1 = multi_snapshot["s1"]
    s1_ts = multi_snapshot["s1_ts"]
    exclusive_pairs = [
        (("snapshot-id", str(s1)), ("branch", "branch_s2")),
        (("snapshot-id", str(s1)), ("tag", "tag_s1")),
        (("snapshot-id", str(s1)), ("as-of-timestamp", str(s1_ts))),
        (("branch", "branch_s2"), ("tag", "tag_s1")),
        (("as-of-timestamp", str(s1_ts)), ("branch", "branch_s2")),
        (("as-of-timestamp", str(s1_ts)), ("tag", "tag_s1")),
    ]
    for (key_a, value_a), (key_b, value_b) in exclusive_pairs:
        with pytest.raises(AnalysisException, match=r"mutually exclusive"):
            (spark.read.format("iceberg").option(key_a, value_a).option(key_b, value_b).load(TABLE))
    # Triple pin also fails loud (octo C8-Q-001).
    with pytest.raises(AnalysisException, match=r"mutually exclusive"):
        (
            spark.read.format("iceberg")
            .option("snapshot-id", str(s1))
            .option("branch", "branch_s2")
            .option("tag", "tag_s1")
            .load(TABLE)
        )


def test_incremental_snapshot_bounds_still_loud(spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException, match=r"incremental|start-snapshot-id"):
        spark.read.format("iceberg").option("start-snapshot-id", "1").load(TABLE)
    with pytest.raises(AnalysisException, match=r"incremental|end-snapshot-id"):
        spark.read.format("iceberg").option("end-snapshot-id", "1").load(TABLE)


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
    """I1 ephemeral ``__repark_tt_*`` pins must not surface in Catalog.listTables (C1-Q-002).

    Two halves, because the two producers of the prefix now behave differently:

    * the **SQL rewrite** releases its pins once the statement is planned (the H-1b
      ephemeral-view leak fix — they used to survive the statement, and survive its failure),
      so a ``VERSION AS OF`` query adds nothing to the introspection surface at all;
    * the **reader-options** path (``option("snapshot-id", …)``) still registers one, because
      that view backs the DataFrame it hands back. That registration is what keeps this pin
      non-vacuous: listTables must filter a prefix that is really there.

    The final step asserts positive membership of the real table before asserting the absence of
    the prefix, so an empty listing cannot green the filter assertion.
    """
    s1 = multi_snapshot["s1"]
    before = _tt_registrations(spark)

    _ = spark.sql(f"SELECT id FROM {TABLE} VERSION AS OF {s1}").to_arrow()
    assert _tt_registrations(spark) == before, (
        "the SQL time-travel rewrite must release its ephemeral pins once the statement is planned"
    )

    _ = spark.read.format("iceberg").option("snapshot-id", str(s1)).load(TABLE).to_arrow()
    after_read = _tt_registrations(spark)
    assert len(after_read) > len(before), (
        "the reader-options pin must still be registered — otherwise the listTables assertion "
        f"below is vacuous (before={before}, after={after_read})"
    )

    ns_listed = [table.name for table in spark.catalog.listTables("ns")]
    listed = [*ns_listed, *(table.name for table in spark.catalog.listTables())]
    # Positive membership FIRST: an empty (or broken) listing would green the leak assertion
    # below for entirely the wrong reason.
    assert TABLE.rsplit(".", 1)[-1] in ns_listed, (
        f"listTables must still list the real table it is filtering around: {ns_listed}"
    )
    leaked = [name for name in listed if str(name).startswith("__repark_tt_")]
    assert leaked == [], f"time-travel temp views leaked into listTables: {leaked}"


def test_two_part_identifier_expands_for_time_travel(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """F1: two-part ``ns.table`` expands under current catalog so VERSION AS OF works.

    Pre-F1 the engine required literal three-part names (fail-loud residual). Free-SQL
    bare/two-part expansion via ``resolve_table_name`` makes Spark-style two-part TT legal.
    """
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

    When the id is unknown, the error must still *name* the negative pin (octo C2-L-001) —
    not fall through to a generic SQL parse failure that drops the AS OF clause.
    """
    _ = multi_snapshot
    with pytest.raises(AnalysisException, match=r"-999999999999"):
        spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF -999999999999").to_arrow()


def test_timestamp_rfc3339_zulu_sql(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """TIMESTAMP AS OF accepts RFC3339 / Zulu wall-clock strings (octo C3-Q-001)."""
    s1_ts = int(multi_snapshot["s1_ts"])  # type: ignore[arg-type]
    # Convert epoch ms → Zulu string with millisecond precision (second-truncation would
    # land *before* s1_ts and trip the earlier-than-first guard).
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
    """Empty branch/tag pins fail loud (not silent current-snapshot)."""
    _ = multi_snapshot
    with pytest.raises(AnalysisException):
        spark.read.format("iceberg").option("branch", "").load(TABLE).to_arrow()
    with pytest.raises(AnalysisException):
        spark.read.format("iceberg").option("tag", "   ").load(TABLE).to_arrow()


def test_cte_version_as_of(spark: ReparkSession, multi_snapshot: dict[str, object]) -> None:
    """WITH … AS (SELECT … VERSION AS OF) rewrites inside CTEs (octo C7)."""
    s1 = multi_snapshot["s1"]
    arrow = spark.sql(
        f"WITH q AS (SELECT id FROM {TABLE} VERSION AS OF {s1}) SELECT id FROM q ORDER BY id"
    ).to_arrow()
    assert _arrow_ids(arrow) == multi_snapshot["ids_s1"]


def test_snapshot_id_overflow_is_analysis_exception(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """snapshot-id outside i64 → AnalysisException (not bare OverflowError) — C7-Q-001."""
    _ = multi_snapshot
    with pytest.raises(AnalysisException, match=r"64-bit|snapshot-id"):
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
    """MERGE USING (SELECT … VERSION AS OF) pins the source snapshot (octo C6)."""
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
    arrow = (
        spark.read.format("iceberg")
        .option("SNAPSHOT-ID", str(s1))
        .load(TABLE)
        .select("id")
        .to_arrow()
    )
    assert _arrow_ids(arrow) == multi_snapshot["ids_s1"]


def test_branch_option_trims_whitespace(
    spark: ReparkSession, multi_snapshot: dict[str, object]
) -> None:
    """Reader branch/tag options trim padding (octo C5-Q-001)."""
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

    Post-hoc filter on the *current* table would expose evolved columns at old pins.
    RTAS widens the schema; VERSION AS OF s1 must keep the pre-evolution field set
    (octo C4-L-001 / C4-Q-001).
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
    assert _schema_names_types(arrow_s1) == [("id", "int64"), ("name", "string")]
    assert "extra" not in arrow_s1.column_names
    assert _arrow_ids(arrow_s1) == [1]

    arrow_s2 = spark.sql(f"SELECT * FROM {table} VERSION AS OF {s2}").to_arrow()
    assert [name for name, _ in _schema_names_types(arrow_s2)] == ["id", "name", "extra"]
    assert arrow_s2.column("extra").to_pylist() == [9]

    # Current read matches s2 schema.
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
    # Sanity: s2 pin on left still distinct when requested.
    arrow_s2 = spark.sql(
        f"SELECT a.id FROM {TABLE} VERSION AS OF {s2} a "
        f"JOIN {other} VERSION AS OF {other_s1} b ON 1=1 ORDER BY id"
    ).to_arrow()
    assert _arrow_ids(arrow_s2) == multi_snapshot["ids_s2"]

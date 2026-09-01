"""REF facade pins: ref-branch-tag-wap/C-002, C-003, C-004, C-005, C-007.
pins: rp-5-fork-repin/C-004
"""

from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import PySparkException, UnsupportedOperationException

TABLE = "mem.ns.events"


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """A session with a memory catalog holding one seeded Iceberg table."""
    session = ReparkSession.builder.appName("pytest-ref-branch-tag-wap").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    session.sql(f"CREATE TABLE {TABLE} (id INT, name STRING) USING iceberg")
    session.sql(f"INSERT INTO {TABLE} SELECT 1 AS id, 'a' AS name")
    return session


def _refs_row(spark: ReparkSession, name: str) -> dict[str, object]:
    """Return the ``refs`` metadata row for ``name`` as a column-to-value mapping."""
    table = spark.sql(f"SELECT * FROM {TABLE}.refs WHERE name = '{name}'").to_arrow()
    assert table.num_rows == 1, f"expected one refs row for {name}, got {table.num_rows}"
    return {column: table.column(column).to_pylist()[0] for column in table.schema.names}


def test_branch_snapshot_retention_takes_both_halves(spark: ReparkSession) -> None:
    """``WITH SNAPSHOT RETENTION n SNAPSHOTS k DAYS`` writes both retention fields."""
    spark.sql(
        f"ALTER TABLE {TABLE} CREATE BRANCH audit RETAIN 5 DAYS "
        "WITH SNAPSHOT RETENTION 3 SNAPSHOTS 7 DAYS"
    )
    row = _refs_row(spark, "audit")
    assert row["type"] == "BRANCH"
    assert row["max_reference_age_in_ms"] == 432_000_000
    assert row["min_snapshots_to_keep"] == 3
    assert row["max_snapshot_age_in_ms"] == 604_800_000


def test_reversed_snapshot_retention_order_refuses(spark: ReparkSession) -> None:
    """Spark's parser rejects the reversed order, so the facade refuses it too."""
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.sql(
            f"ALTER TABLE {TABLE} CREATE BRANCH rev WITH SNAPSHOT RETENTION 7 DAYS 3 SNAPSHOTS"
        )
    assert "SNAPSHOT RETENTION" in str(caught.value)


def test_branch_and_tag_read_selectors_resolve_the_ref(spark: ReparkSession) -> None:
    """``t.branch_b`` and ``t.tag_v`` read the ref, and the plain name still reads main."""
    spark.sql(f"ALTER TABLE {TABLE} CREATE BRANCH audit")
    spark.sql(f"ALTER TABLE {TABLE} CREATE TAG v1")
    spark.sql(f"INSERT INTO {TABLE} SELECT 2 AS id, 'b' AS name")

    branch_ids = spark.sql(f"SELECT id FROM {TABLE}.branch_audit").to_arrow()
    tag_ids = spark.sql(f"SELECT id FROM {TABLE}.tag_v1").to_arrow()
    main_ids = spark.sql(f"SELECT id FROM {TABLE}").to_arrow()
    assert branch_ids.column("id").to_pylist() == [1]
    assert tag_ids.column("id").to_pylist() == [1]
    assert sorted(main_ids.column("id").to_pylist()) == [1, 2]


@pytest.mark.parametrize(
    ("suffix", "ref_name"),
    [("branch_nope", "nope"), ("tag_missing", "missing")],
)
def test_missing_ref_selector_refuses_naming_the_ref(
    spark: ReparkSession, suffix: str, ref_name: str
) -> None:
    """A selector for a ref that does not exist refuses and names it, branch or tag."""
    with pytest.raises(PySparkException) as caught:
        spark.sql(f"SELECT id FROM {TABLE}.{suffix}").to_arrow()
    assert ref_name in str(caught.value)
    assert "compound identifier" not in str(caught.value)


def test_ref_selector_on_the_read_side_of_dml_is_a_read(spark: ReparkSession) -> None:
    """A selector in a source relation, a USING operand or a predicate subquery is a read."""
    spark.sql(f"ALTER TABLE {TABLE} CREATE TAG v1")
    spark.sql(f"INSERT INTO {TABLE} SELECT 2 AS id, 'b' AS name")
    spark.sql(f"ALTER TABLE {TABLE} CREATE BRANCH b")
    spark.sql(f"INSERT INTO {TABLE} SELECT 3 AS id, 'c' AS name")

    spark.sql("CREATE TABLE mem.ns.into_dst (id INT, name STRING) USING iceberg")
    spark.sql(f"INSERT INTO mem.ns.into_dst SELECT id, name FROM {TABLE}.branch_b")
    into_rows = spark.sql("SELECT id FROM mem.ns.into_dst").to_arrow()
    assert sorted(into_rows.column("id").to_pylist()) == [1, 2]

    spark.sql("CREATE TABLE mem.ns.mrg_dst (id INT, name STRING) USING iceberg")
    spark.sql(
        f"MERGE INTO mem.ns.mrg_dst d USING {TABLE}.tag_v1 s ON d.id = s.id "
        "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)"
    )
    merged = spark.sql("SELECT id FROM mem.ns.mrg_dst").to_arrow()
    assert sorted(merged.column("id").to_pylist()) == [1]

    spark.sql(f"CREATE TABLE mem.ns.del_dst AS SELECT id, name FROM {TABLE}")
    spark.sql(f"DELETE FROM mem.ns.del_dst WHERE id IN (SELECT id FROM {TABLE}.tag_v1)")
    remaining = spark.sql("SELECT id FROM mem.ns.del_dst").to_arrow()
    assert sorted(remaining.column("id").to_pylist()) == [2, 3]


def test_write_to_branch_refusal_claims_the_target_only(spark: ReparkSession) -> None:
    spark.sql(f"ALTER TABLE {TABLE} CREATE BRANCH b")
    spark.sql(f"ALTER TABLE {TABLE} CREATE TAG v1")
    spark.sql(f"INSERT INTO {TABLE}.branch_b SELECT id, name FROM {TABLE}.tag_v1")
    branch_rows = spark.sql(f"SELECT id FROM {TABLE}.branch_b").to_arrow()
    assert sorted(branch_rows.column("id").to_pylist()) == [1, 1]
    main = spark.sql(f"SELECT id FROM {TABLE}").to_arrow()
    assert main.column("id").to_pylist() == [1]


def test_write_to_branch_refuses_naming_the_fork_write_path(spark: ReparkSession) -> None:
    spark.sql(f"ALTER TABLE {TABLE} CREATE BRANCH audit")
    spark.sql(f"INSERT INTO {TABLE}.branch_audit SELECT 2 AS id, 'b' AS name")
    rows = spark.sql(f"SELECT id FROM {TABLE}.branch_audit").to_arrow()
    assert sorted(rows.column("id").to_pylist()) == [1, 2]
    main = spark.sql(f"SELECT id FROM {TABLE}").to_arrow()
    assert sorted(main.column("id").to_pylist()) == [1]


def test_write_to_tag_refuses_like_spark(spark: ReparkSession) -> None:
    spark.sql(f"ALTER TABLE {TABLE} CREATE TAG v1")
    with pytest.raises(Exception, match="Cannot write to table with time travel"):
        spark.sql(f"INSERT INTO {TABLE}.tag_v1 SELECT 2 AS id, 'b' AS name")


@pytest.mark.parametrize(
    "call",
    [
        "fast_forward(table => 'ns.events', branch => 'main', to => 'audit')",
        "publish_changes(table => 'ns.events', wap_id => 'w1')",
        "cherrypick_snapshot(table => 'ns.events', snapshot_id => 1)",
    ],
)
def test_wap_publish_procedures_refuse_loud(spark: ReparkSession, call: str) -> None:
    """No WAP publish procedure is implemented; each refusal lists what is."""
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.sql(f"CALL mem.system.{call}")
    assert "not supported" in str(caught.value)


@pytest.mark.parametrize("key", ["spark.wap.branch", "spark.wap.id"])
def test_wap_session_conf_is_fail_closed(spark: ReparkSession, key: str) -> None:
    """The WAP session confs cannot be set, so no write is silently redirected."""
    with pytest.raises(PySparkException) as caught:
        spark.sql(f"SET {key} = 'audit'")
    assert "spark" in str(caught.value)
    assert "Could not find config namespace" in str(caught.value)

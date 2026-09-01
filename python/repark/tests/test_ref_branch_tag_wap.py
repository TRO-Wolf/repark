"""REF — facade rows for branch / tag retention and the refused write and WAP doors.

Oracle: live PySpark 4.1.2 + Iceberg 1.11.0 (Hadoop catalog), measured 2026-09-01. The
retention values below are the oracle's own ``refs`` rows, not arithmetic done here.

pins: ref-branch-tag-wap/C-003, C-004, C-005
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


def test_missing_ref_selector_refuses_naming_the_ref(spark: ReparkSession) -> None:
    """A selector for a ref that does not exist refuses and names it."""
    with pytest.raises(PySparkException) as caught:
        spark.sql(f"SELECT id FROM {TABLE}.branch_nope").to_arrow()
    assert "nope" in str(caught.value)


def test_write_to_branch_refuses_naming_the_fork_write_path(spark: ReparkSession) -> None:
    """The refusal names the surface that is still missing, not a stale fork pin."""
    spark.sql(f"ALTER TABLE {TABLE} CREATE BRANCH audit")
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.sql(f"INSERT INTO {TABLE}.branch_audit SELECT 2 AS id, 'b' AS name")
    message = str(caught.value)
    assert "iceberg-datafusion" in message
    assert "33be9a0" in message
    assert "b009ac1" not in message, "the superseded fork pin must not be cited"


def test_write_to_tag_refuses_like_spark(spark: ReparkSession) -> None:
    """Spark refuses a write through a tag; so does this door."""
    spark.sql(f"ALTER TABLE {TABLE} CREATE TAG v1")
    with pytest.raises(UnsupportedOperationException):
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

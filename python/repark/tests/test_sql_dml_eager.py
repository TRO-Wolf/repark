"""Bare `spark.sql` DML executes eagerly (F-BR-2) — PySpark parity.

`spark.sql("INSERT INTO …")` / `DELETE FROM` / `UPDATE` apply at ``sql()`` time like PySpark, even
when the returned DataFrame is never collected; collecting it does not re-apply (exactly-once). A
runtime DML failure surfaces at ``sql()`` time as the base :class:`PySparkException` (WG-3), never
:class:`AnalysisException`/:class:`ParseException`. Every value check is on the export path the
migrated caller reads — ``to_arrow`` — with the Arrow **type** pinned too, never ``show``. Requires
the compiled wheel (``maturin develop``) — the real facade boundary, no mocks.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, ParseException, PySparkException

TABLE = "cat.ns.t"


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-dml-eager").getOrCreate()
    session.register_memory_catalog("cat", tmp_path)
    session.sql("CREATE NAMESPACE cat.ns")
    # `id` is a 64-bit integer (SQL integer literals are int64), `name` a string.
    session.sql(f"CREATE TABLE {TABLE} AS SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b'")
    return session


def _table(spark: ReparkSession) -> pa.Table:
    return spark.sql(f"SELECT id, name FROM {TABLE} ORDER BY id").to_arrow()


def test_bare_sql_insert_applies_without_collect(spark: ReparkSession) -> None:
    # The returned DataFrame is never collected — the F-BR-2 trap (pre-fix: a silent no-op through
    # the primary facade). Post-fix the write applies at sql() time (PySpark-eager).
    spark.sql(f"INSERT INTO {TABLE} VALUES (3, 'c')")

    table = _table(spark)
    assert table.to_pylist() == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
        {"id": 3, "name": "c"},
    ]
    # Value AND Arrow type on the export path (to_arrow), never show.
    assert table.schema.field("id").type == pa.int64()
    assert table.schema.field("name").type == pa.string()


def test_bare_sql_delete_applies_without_collect(spark: ReparkSession) -> None:
    spark.sql(f"DELETE FROM {TABLE} WHERE id = 1")

    table = _table(spark)
    assert table.to_pylist() == [{"id": 2, "name": "b"}]
    assert table.schema.field("id").type == pa.int64()


def test_bare_sql_update_applies_without_collect(spark: ReparkSession) -> None:
    spark.sql(f"UPDATE {TABLE} SET name = 'z' WHERE id = 2")

    table = _table(spark)
    assert table.to_pylist() == [{"id": 1, "name": "a"}, {"id": 2, "name": "z"}]
    assert table.schema.field("name").type == pa.string()


def test_bare_sql_insert_applies_exactly_once_when_collected(spark: ReparkSession) -> None:
    # The no-double-apply trap the naive fix creates: eager at sql() AND a later collect on the
    # returned DataFrame must NOT insert a second copy.
    expected = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}]
    returned = spark.sql(f"INSERT INTO {TABLE} VALUES (3, 'c')")

    # Already applied before the returned DataFrame is touched.
    assert _table(spark).to_pylist() == expected

    # Collecting the returned DataFrame must not re-run the INSERT.
    returned.collect()
    assert _table(spark).to_pylist() == expected


def test_bare_sql_failing_dml_raises_base_pyspark_exception_at_sql_time(
    spark: ReparkSession,
) -> None:
    # A DML whose per-row CAST fails at RUNTIME. The source is a temp-view column (not a literal,
    # so it is not constant-folded to a plan error at analysis time). Post-fix it surfaces at
    # sql() time (eager), and the WG-3 taxonomy classifies a runtime failure as the base
    # PySparkException — NOT an AnalysisException/ParseException (neither a parse nor an analysis
    # error).
    spark.sql("SELECT 'abc' AS s").createOrReplaceTempView("bad_src")
    with pytest.raises(PySparkException) as raised:
        spark.sql(f"INSERT INTO {TABLE} SELECT CAST(s AS INT) AS id, s AS name FROM bad_src")
    assert not isinstance(raised.value, (AnalysisException, ParseException))
    # The failed write committed nothing — the table is unchanged.
    assert _table(spark).to_pylist() == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def test_bare_sql_empty_insert_overwrite_wipes_table(spark: ReparkSession) -> None:
    """C3-Q-002: bare ``spark.sql`` empty INSERT OVERWRITE must wipe (facade shipping path).

    Engine unit pins cover repark-sql only; this pins the Python ``spark.sql`` entry point so a
    regression that skips the empty-OW intercept on the facade would go red (value+type on Arrow).
    """
    prior = _table(spark)
    assert prior.num_rows == 2
    # Never collect the returned DataFrame — wipe must apply at sql() time (F-BR-2 + C1-Q-001).
    # Source is an independent empty VALUES projection (not self-scan) so the pin isolates the
    # empty-OW intercept from same-table read/write edge cases.
    spark.sql(
        f"INSERT OVERWRITE {TABLE} SELECT * FROM (VALUES (1, 'x')) AS v(id, name) WHERE false"
    )
    table = _table(spark)
    assert table.to_pylist() == [], "empty INSERT OVERWRITE must wipe all rows (not a silent no-op)"
    assert table.schema.field("id").type == pa.int64()
    assert table.schema.field("name").type == pa.string()


def test_bare_sql_call_unknown_procedure_refuses_loud(spark: ReparkSession) -> None:
    """C3-L-001 residual: unknown CALL procedures fail loud listing supported ones.

    I3 wired expire_snapshots / rewrite_data_files / rollback_to_snapshot; unknown +
    remove_orphan_files remain loud-unsupported on the facade shipping path.
    """
    from repark.errors import UnsupportedOperationException

    with pytest.raises(
        (UnsupportedOperationException, PySparkException),
        match=r"not supported|expire_snapshots|rewrite_data_files",
    ):
        spark.sql("CALL cat.system.not_a_real_proc(table => 'ns.t')")
    # Must not mutate the seeded table as a side effect of the refuse.
    assert _table(spark).to_pylist() == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
    ]


def test_bare_sql_empty_overwrite_incompatible_schema_does_not_wipe(
    spark: ReparkSession,
) -> None:
    """C5-Q-001: empty INSERT OVERWRITE with wrong column count must not wipe prior rows."""
    with pytest.raises(PySparkException, match=r"Column count|column|schema|field"):
        spark.sql(f"INSERT OVERWRITE {TABLE} SELECT 'x' AS only_wrong WHERE false")
    assert _table(spark).to_pylist() == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
    ]


def test_bare_sql_branch_tag_replace_round_trip(spark: ReparkSession) -> None:
    """r25 T2: CREATE OR REPLACE / bare REPLACE BRANCH|TAG re-pin on the facade ``spark.sql`` path.

    Supersedes I5 loud-refuse pin ``test_bare_sql_branch_tag_replace_refuses_loud``.
    """
    prior = _table(spark).to_pylist()
    # CREATE OR REPLACE when absent = create at current — both spellings, BRANCH and TAG
    # (the `TAG … IN <table>` form kept from the superseded refuse-loud pin; morning critic).
    spark.sql(f"CREATE OR REPLACE BRANCH audit_branch IN {TABLE}")
    spark.sql(f"CREATE OR REPLACE TAG t1 IN {TABLE}")
    spark.sql(f"ALTER TABLE {TABLE} CREATE OR REPLACE TAG t1")
    # Bare REPLACE re-pins existing refs (still current after no new commits).
    spark.sql(f"ALTER TABLE {TABLE} REPLACE BRANCH audit_branch")
    spark.sql(f"ALTER TABLE {TABLE} REPLACE TAG t1")
    # Data multiset unchanged by ref DDL.
    assert _table(spark).to_pylist() == prior, "REPLACE BRANCH|TAG must not mutate table rows"
    # Time-travel via the replaced branch still reads current rows.
    branch_rows = (
        spark.sql(f"SELECT id FROM {TABLE} VERSION AS OF 'audit_branch' ORDER BY id")
        .to_arrow()
        .column("id")
        .to_pylist()
    )
    assert branch_rows == [1, 2]

"""ICE-PROCEDURES-1 binder pins: positional and mixed CALL forms, exact refusals."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException

_RDF_COLS = [
    "rewritten_data_files_count",
    "added_data_files_count",
    "rewritten_bytes_count",
    "failed_data_files_count",
    "removed_delete_files_count",
]

_RPD_COLS = [
    "rewritten_delete_files_count",
    "added_delete_files_count",
    "rewritten_bytes_count",
    "added_bytes_count",
]


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """Memory catalog session for binder pins."""
    session = ReparkSession.builder.appName("pytest-procedures-1").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def _result_row(spark: ReparkSession, sql: str) -> tuple[list[str], list[int]]:
    """Run one CALL and return its column names and first row."""
    batch = spark.sql(sql).to_arrow()
    names = batch.schema.names
    return (names, [int(batch.column(name)[0].as_py()) for name in names])


def _snapshot_ids(spark: ReparkSession, table: str) -> list[int]:
    """Return snapshot ids in commit order."""
    batch = spark.sql(
        f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at, snapshot_id"
    ).to_arrow()
    return [int(value) for value in batch.column("snapshot_id").to_pylist()]


def _live_ids(spark: ReparkSession, table: str) -> list[int]:
    """Return live ids in ascending order."""
    batch = spark.sql(f"SELECT id FROM {table} ORDER BY id").to_arrow()
    return [int(value) for value in batch.column("id").to_pylist()]


def test_pos_rdf_positional_form_compacts(spark: ReparkSession) -> None:
    """Four positionals bind in declared order; NULL is unset. pins: ice-procedures-1/C-001."""
    spark.sql("CREATE TABLE mem.ns.posrdf (id BIGINT) USING iceberg")
    spark.sql("INSERT INTO mem.ns.posrdf VALUES (1)")
    spark.sql("INSERT INTO mem.ns.posrdf VALUES (2)")
    cols, row = _result_row(
        spark,
        "CALL mem.system.rewrite_data_files('ns.posrdf', 'binpack', NULL, "
        "map('min-input-files', '2'))",
    )
    assert cols == _RDF_COLS
    assert row[0] == 2
    assert row[1] == 1
    assert row[2] > 0
    assert row[3] == 0
    assert row[4] == 0
    assert _live_ids(spark, "mem.ns.posrdf") == [1, 2]


def test_pos_rpd_positional_options_bind(spark: ReparkSession) -> None:
    """Two positionals bind the map as options. pins: ice-procedures-1/C-002."""
    spark.sql("CREATE TABLE mem.ns.posrpd (id BIGINT) USING iceberg")
    spark.sql("INSERT INTO mem.ns.posrpd VALUES (1)")
    spark.sql("INSERT INTO mem.ns.posrpd VALUES (2)")
    cols, row = _result_row(
        spark,
        "CALL mem.system.rewrite_position_delete_files('ns.posrpd', map('rewrite-all', 'true'))",
    )
    assert cols == _RPD_COLS
    assert row == [0, 0, 0, 0]
    assert _live_ids(spark, "mem.ns.posrpd") == [1, 2]


def test_call_mixed_args_rollback_binds(spark: ReparkSession) -> None:
    """Mixed positional table plus named snapshot_id rolls back. pins: ice-procedures-1/C-003."""
    spark.sql("CREATE TABLE mem.ns.mx (id BIGINT) USING iceberg")
    spark.sql("INSERT INTO mem.ns.mx VALUES (1)")
    spark.sql("INSERT INTO mem.ns.mx VALUES (2)")
    ids = _snapshot_ids(spark, "mem.ns.mx")
    assert len(ids) == 2
    cols, row = _result_row(
        spark, f"CALL mem.system.rollback_to_snapshot('ns.mx', snapshot_id => {ids[0]})"
    )
    assert cols == ["previous_snapshot_id", "current_snapshot_id"]
    assert row == [ids[1], ids[0]]
    assert _live_ids(spark, "mem.ns.mx") == [1]


def test_bind_duplicate_binding_refuses(spark: ReparkSession) -> None:
    """One parameter bound positionally and by name refuses loud. pins: ice-procedures-1/C-004."""
    with pytest.raises(
        AnalysisException,
        match=re.escape("CALL argument `strategy` is bound twice (positionally and by name)"),
    ):
        spark.sql(
            "CALL mem.system.rewrite_data_files('ns.dup', 'binpack', strategy => 'sort')"
        ).to_arrow()


def test_bind_unknown_argument_refuses(spark: ReparkSession) -> None:
    """Unknown branch refuses with the declared allowed list. pins: ice-procedures-1/C-005."""
    with pytest.raises(
        AnalysisException,
        match=re.escape(
            "unknown CALL argument `branch`; allowed: table, strategy, sort_order, options, "
            "where, remove-dangling-deletes"
        ),
    ):
        spark.sql("CALL mem.system.rewrite_data_files(table => 'ns.x', branch => 'b1')").to_arrow()


def test_bind_missing_required_refuses(spark: ReparkSession) -> None:
    """Missing table refuses naming the declared position. pins: ice-procedures-1/C-006."""
    with pytest.raises(
        AnalysisException,
        match=re.escape("CALL argument `table` is required (named `table => …` or positional #0)"),
    ):
        spark.sql("CALL mem.system.rewrite_data_files(strategy => 'binpack')").to_arrow()


def test_bind_excess_positional_refuses(spark: ReparkSession) -> None:
    """Over-arity refuses; extras stay named-only. pins: ice-procedures-1/C-007."""
    with pytest.raises(
        AnalysisException,
        match=re.escape("CALL accepts at most 5 positional argument(s); got 6"),
    ):
        spark.sql(
            "CALL mem.system.rewrite_data_files('a', 'b', 'c', map('k', 'v'), 'd', 'e')"
        ).to_arrow()
    with pytest.raises(
        AnalysisException,
        match=re.escape("CALL accepts at most 3 positional argument(s); got 4"),
    ):
        spark.sql("CALL mem.system.rewrite_position_delete_files('a', NULL, NULL, NULL)").to_arrow()


def test_bind_null_is_unset(spark: ReparkSession) -> None:
    """Named NULL sort_order with binpack succeeds. pins: ice-procedures-1/C-008."""
    spark.sql("CREATE TABLE mem.ns.nnu (id BIGINT) USING iceberg")
    spark.sql("INSERT INTO mem.ns.nnu VALUES (1)")
    cols, _row = _result_row(
        spark,
        "CALL mem.system.rewrite_data_files(table => 'ns.nnu', strategy => 'binpack', "
        "sort_order => NULL)",
    )
    assert cols == _RDF_COLS


def test_not_yet_wired_params_stay_loud(spark: ReparkSession) -> None:
    """sort_by keeps its exact refusal. pins: ice-procedures-1/C-009, C-020."""
    spark.sql("CREATE TABLE mem.ns.nyw (id BIGINT) USING iceberg")
    spark.sql("INSERT INTO mem.ns.nyw VALUES (1)")
    with pytest.raises(
        AnalysisException,
        match=re.escape("unknown CALL argument `sort_by`; allowed: table, use_caching, spec_id"),
    ):
        spark.sql(
            "CALL mem.system.rewrite_manifests(table => 'ns.nyw', sort_by => array('id'))"
        ).to_arrow()


_EXPIRE_COLS = [
    "deleted_data_files_count",
    "deleted_position_delete_files_count",
    "deleted_equality_delete_files_count",
    "deleted_manifest_files_count",
    "deleted_manifest_lists_count",
    "deleted_statistics_files_count",
]


def _seed_expire_fixture(spark: ReparkSession, table: str) -> list[int]:
    """Create the mk3-plus-delete shape and return snapshot ids in commit order."""
    spark.sql(
        f"CREATE TABLE mem.ns.{table} (id BIGINT, data STRING, cat STRING) USING iceberg "
        "PARTITIONED BY (cat)"
    )
    spark.sql(f"INSERT INTO mem.ns.{table} VALUES (1, 'a', 'x'), (2, 'b', 'y'), (6, 'f', 'x')")
    spark.sql(f"INSERT INTO mem.ns.{table} VALUES (3, 'c', 'x'), (7, 'g', 'x')")
    spark.sql(f"INSERT INTO mem.ns.{table} VALUES (4, 'd', 'x'), (5, 'e', 'y'), (8, 'h', 'x')")
    spark.sql(f"DELETE FROM mem.ns.{table} WHERE id = 1")
    return _snapshot_ids(spark, f"mem.ns.{table}")


def test_expire_snapshot_ids_expires_exactly_those(spark: ReparkSession) -> None:
    """snapshot_ids expires exactly those ids. pins: ice-procedures-1/C-013."""
    ids = _seed_expire_fixture(spark, "exs")
    assert len(ids) == 4
    cols, row = _result_row(
        spark,
        f"CALL mem.system.expire_snapshots(table => 'ns.exs', snapshot_ids => array({ids[0]}))",
    )
    assert cols == _EXPIRE_COLS
    assert row == [0, 0, 0, 0, 1, 0]
    assert _snapshot_ids(spark, "mem.ns.exs") == ids[1:]


def test_expire_accept_and_ignore_trio_equals_plain(spark: ReparkSession) -> None:
    """Ignored expire args equal the plain older_than row. pins: ice-procedures-1/C-014."""
    _seed_expire_fixture(spark, "ex0")
    cols, plain = _result_row(
        spark,
        "CALL mem.system.expire_snapshots(table => 'ns.ex0', "
        "older_than => TIMESTAMP '2999-01-01 00:00:00')",
    )
    assert cols == _EXPIRE_COLS
    assert plain == [1, 0, 0, 1, 3, 0]
    for table, argument in (
        ("ex1", "stream_results => true"),
        ("ex2", "max_concurrent_deletes => 2"),
        ("ex3", "clean_expired_metadata => true"),
    ):
        _seed_expire_fixture(spark, table)
        cols, row = _result_row(
            spark,
            f"CALL mem.system.expire_snapshots(table => 'ns.{table}', "
            f"older_than => TIMESTAMP '2999-01-01 00:00:00', {argument})",
        )
        assert cols == _EXPIRE_COLS
        assert row == plain


def test_rpd_where_rewrites_matching_partition(spark: ReparkSession) -> None:
    """RPD where compacts only the matching partition. pins: ice-procedures-1/C-012."""
    spark.sql(
        "CREATE TABLE mem.ns.rpdw (id BIGINT, data STRING, cat STRING) USING iceberg "
        "PARTITIONED BY (cat) TBLPROPERTIES ('write.delete.mode' = 'merge-on-read', "
        "'write.merge.mode' = 'merge-on-read')"
    )
    spark.sql("INSERT INTO mem.ns.rpdw VALUES (1, 'a', 'x'), (2, 'b', 'y'), (6, 'f', 'x')")
    spark.sql("INSERT INTO mem.ns.rpdw VALUES (3, 'c', 'x'), (7, 'g', 'x')")
    spark.sql("INSERT INTO mem.ns.rpdw VALUES (4, 'd', 'x'), (5, 'e', 'y'), (8, 'h', 'x')")
    spark.sql("DELETE FROM mem.ns.rpdw WHERE id = 1")
    spark.sql("DELETE FROM mem.ns.rpdw WHERE id = 3")
    spark.sql("DELETE FROM mem.ns.rpdw WHERE id = 4")
    cols, row = _result_row(
        spark,
        "CALL mem.system.rewrite_position_delete_files(table => 'ns.rpdw', "
        "where => 'cat = \"x\"', options => map('rewrite-all', 'true'))",
    )
    assert cols == _RPD_COLS
    assert row[0] == 3
    assert row[1] == 3
    assert row[2] > 0
    assert row[3] > 0
    assert _live_ids(spark, "mem.ns.rpdw") == [2, 5, 6, 7, 8]


def test_dangling_extra_and_precedence_kept(spark: ReparkSession) -> None:
    """Quoted dangling-deletes extra binds; NULL map key wins. pins: ice-procedures-1/C-010."""
    spark.sql(
        "CREATE TABLE mem.ns.dng (id BIGINT) USING iceberg "
        "TBLPROPERTIES ('write.delete.mode' = 'merge-on-read')"
    )
    spark.sql("INSERT INTO mem.ns.dng VALUES (1)")
    spark.sql("INSERT INTO mem.ns.dng VALUES (2)")
    cols, _row = _result_row(
        spark,
        "CALL mem.system.rewrite_data_files(table => 'ns.dng', 'remove-dangling-deletes' => true)",
    )
    assert cols == _RDF_COLS
    cols, row = _result_row(
        spark,
        "CALL mem.system.rewrite_data_files(table => 'ns.dng', "
        "options => map('remove-dangling-deletes', NULL), 'remove-dangling-deletes' => true)",
    )
    assert cols == _RDF_COLS
    assert row[4] == 0

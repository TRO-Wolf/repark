"""ICE-CHANGELOG-1 — incremental append reads, ``t.changes`` and ``create_changelog_view``.

Oracle: ``ice_changelog_1_spark_oracle.json`` — 36 cells recorded on live PySpark 4.1.2 +
iceberg-spark-runtime-4.1_2.13:1.11.0 (``local[1]``, InMemoryCatalog ``sc``) by the
orchestrator's run-25c measurement (cells ``cells_qc4.py``, harness ``harness.py``; provenance
and SHA-256 in ``map.md``). ``_record_ice_changelog_1_oracle.py`` beside this file re-derives it
against live Spark under ``REPARK_PARITY_LIVE=1``.

Every cell rebuilds the recorded fixture — ``(1,'a','x'),(2,'b','y')`` then ``(3,'c','x')`` then
``(4,'d','y'),(5,'e','x')``, three append snapshots — and compares the recorded observations:
column names with their Spark type spelling, rows sorted by ``repr`` (as the recorder sorted
them), and for a refusal the exception class and Spark's message.

pins: ice-changelog-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
FIXTURE: list[dict[str, Any]] = json.loads(
    Path(__file__).with_name("ice_changelog_1_spark_oracle.json").read_text(encoding="utf-8")
)
CELLS: dict[str, dict[str, Any]] = {cell["id"]: cell for cell in FIXTURE}
CATALOG = "sc"
NAMESPACE = "ns"
MOR = (
    ", 'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read'"
    ", 'write.merge.mode'='merge-on-read'"
)


@pytest.fixture
def spark(tmp_path: Path) -> Any:
    """Yield a facade session with an in-memory Iceberg catalog named ``sc``."""
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-ice-changelog-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog(CATALOG, tmp_path / "wh")
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NAMESPACE}")
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _table(cell_id: str) -> str:
    """Return the per-cell table identifier."""
    return f"{CATALOG}.{NAMESPACE}.t_{cell_id.lower().replace('-', '_')}"


def _snaps(session: Any, table: str) -> list[int]:
    """Snapshot ids in ``(committed_at, snapshot_id)`` order, as the recorder collected them."""
    arrow = session.sql(
        f"SELECT snapshot_id, committed_at FROM {table}.snapshots"
    ).to_arrow()
    pairs = sorted(
        zip(
            arrow.column("snapshot_id").to_pylist(),
            arrow.column("committed_at").to_pylist(),
            strict=True,
        ),
        key=lambda pair: (pair[1], pair[0]),
    )
    return [pair[0] for pair in pairs]


def _seed(
    session: Any, table: str, version: str = "2", part: str = "", props: str = ""
) -> list[int]:
    """Create the recorded fixture table and its three append snapshots."""
    session.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg {part} "
        f"TBLPROPERTIES ('format-version'='{version}'{props})"
    )
    session.sql(f"INSERT INTO {table} VALUES (1, 'a', 'x'), (2, 'b', 'y')")
    session.sql(f"INSERT INTO {table} VALUES (3, 'c', 'x')")
    session.sql(f"INSERT INTO {table} VALUES (4, 'd', 'y'), (5, 'e', 'x')")
    return _snaps(session, table)


def _cols(frame: Any) -> list[list[str]]:
    """Column name / Spark type-spelling pairs, shaped like the recorded ``cols``."""
    return [[field.name, field.dataType.simpleString()] for field in frame.schema.fields]


def _rows(frame: Any) -> list[list[Any]]:
    """Collect a frame as plain lists sorted by ``repr``, as the recorder sorted them."""
    arrow = frame.to_arrow()
    columns = [arrow.column(index).to_pylist() for index in range(arrow.num_columns)]
    rows = ([column[index] for column in columns] for index in range(arrow.num_rows))
    return sorted(rows, key=repr)


def _assert_ok(cell_id: str, frame: Any) -> None:
    """Compare one recorded ``ok`` cell's columns and rows against ``frame``."""
    cell = CELLS[cell_id]
    assert cell["status"] == "ok", cell
    observed = cell["obs"]
    if "cols" in observed:
        assert _cols(frame) == observed["cols"]
    assert _rows(frame) == observed["rows"]


def _without_ids(message: str) -> str:
    """Blank the per-run snapshot ids so a recorded message compares across runs."""
    return re.sub(r"-?\d{4,}", "<id>", message)


def _assert_error(cell_id: str, raised: Exception) -> None:
    """Compare one recorded ``error`` cell's exception class and message against ``raised``."""
    cell = CELLS[cell_id]
    assert cell["status"] == "error", cell
    assert type(raised).__name__ == cell["error"]["type"], raised
    assert _without_ids(cell["error"]["msg"]) in _without_ids(str(raised)), raised


def _incremental(session: Any, table: str, **options: Any) -> Any:
    """Build an Iceberg read with the given reader options."""
    reader = session.read.format("iceberg")
    for key, value in options.items():
        reader = reader.option(key.replace("_", "-"), str(value))
    return reader.load(table)


def test_incremental_window_between_two_snapshots(spark: Any) -> None:
    """QI-S0-S2 — ``(S0, S2]`` returns only the rows the two later appends added.

    pins: ice-changelog-1/C-001
    """
    table = _table("QI-S0-S2")
    ids = _seed(spark, table)
    frame = _incremental(
        spark, table, start_snapshot_id=ids[0], end_snapshot_id=ids[2]
    )
    _assert_ok("QI-S0-S2", frame)


def test_incremental_window_open_end_runs_to_the_current_snapshot(spark: Any) -> None:
    """QI-S0-OPEN — a start with no end reads to the table's current snapshot.

    pins: ice-changelog-1/C-002
    """
    table = _table("QI-S0-OPEN")
    ids = _seed(spark, table)
    _assert_ok("QI-S0-OPEN", _incremental(spark, table, start_snapshot_id=ids[0]))


def test_incremental_start_equal_to_end_refuses(spark: Any) -> None:
    """QI-S1-S1 — an exclusive start that equals the end is not a parent ancestor.

    pins: ice-changelog-1/C-007
    """
    table = _table("QI-S1-S1")
    ids = _seed(spark, table)
    with pytest.raises(Exception) as raised:  # noqa: B017
        _incremental(
            spark, table, start_snapshot_id=ids[1], end_snapshot_id=ids[1]
        ).to_arrow()
    _assert_error("QI-S1-S1", raised.value)


def test_incremental_end_without_start_refuses(spark: Any) -> None:
    """QI-END-ONLY — Java's ``incrementalAppendScanBoundaries`` refusal.

    pins: ice-changelog-1/C-004
    """
    table = _table("QI-END-ONLY")
    ids = _seed(spark, table)
    with pytest.raises(Exception) as raised:  # noqa: B017
        _incremental(spark, table, end_snapshot_id=ids[1]).to_arrow()
    _assert_error("QI-END-ONLY", raised.value)


def test_incremental_unknown_start_refuses(spark: Any) -> None:
    """QI-START-UNKNOWN — an id that is no snapshot of this table.

    pins: ice-changelog-1/C-007
    """
    table = _table("QI-START-UNKNOWN")
    _seed(spark, table)
    with pytest.raises(Exception) as raised:  # noqa: B017
        _incremental(spark, table, start_snapshot_id=12345).to_arrow()
    _assert_error("QI-START-UNKNOWN", raised.value)


def test_incremental_start_after_end_refuses(spark: Any) -> None:
    """QI-START-AFTER-END — a start newer than the end is not a parent ancestor.

    pins: ice-changelog-1/C-007
    """
    table = _table("QI-START-AFTER-END")
    ids = _seed(spark, table)
    with pytest.raises(Exception) as raised:  # noqa: B017
        _incremental(
            spark, table, start_snapshot_id=ids[2], end_snapshot_id=ids[0]
        ).to_arrow()
    _assert_error("QI-START-AFTER-END", raised.value)


def test_incremental_composes_with_projection_and_filter(spark: Any) -> None:
    """QI-PROJECT-FILTER — a filter and a projection compose over the window.

    pins: ice-changelog-1/C-008
    """
    table = _table("QI-PROJECT-FILTER")
    ids = _seed(spark, table)
    frame = (
        _incremental(spark, table, start_snapshot_id=ids[0])
        .where("cat = 'x'")
        .select("id")
    )
    _assert_ok("QI-PROJECT-FILTER", frame)


def test_incremental_with_version_as_of_refuses(spark: Any) -> None:
    """QI-WITH-VERSIONASOF — a time-travel pin beside a window is refused.

    pins: ice-changelog-1/C-006
    """
    table = _table("QI-WITH-VERSIONASOF")
    ids = _seed(spark, table)
    with pytest.raises(Exception) as raised:  # noqa: B017
        (
            spark.read.format("iceberg")
            .option("start-snapshot-id", str(ids[0]))
            .option("versionAsOf", str(ids[1]))
            .load(table)
            .to_arrow()
        )
    _assert_error("QI-WITH-VERSIONASOF", raised.value)


def test_incremental_over_a_copy_on_write_delete_skips_it(spark: Any) -> None:
    """QI-OVER-DELETE-COW — a non-append snapshot in the range contributes nothing.

    pins: ice-changelog-1/C-003
    """
    table = _table("QI-OVER-DELETE-COW")
    _seed(spark, table)
    spark.sql(f"DELETE FROM {table} WHERE id = 3")
    spark.sql(f"INSERT INTO {table} VALUES (6, 'f', 'x')")
    ids = _snaps(spark, table)
    _assert_ok(
        "QI-OVER-DELETE-COW", _incremental(spark, table, start_snapshot_id=ids[0])
    )


def test_incremental_over_a_merge_on_read_delete_skips_it(spark: Any) -> None:
    """QI-OVER-DELETE-MOR — the same, on a merge-on-read table.

    pins: ice-changelog-1/C-003
    """
    table = _table("QI-OVER-DELETE-MOR")
    _seed(spark, table, props=MOR)
    spark.sql(f"DELETE FROM {table} WHERE id = 3")
    spark.sql(f"INSERT INTO {table} VALUES (6, 'f', 'x')")
    ids = _snaps(spark, table)
    _assert_ok(
        "QI-OVER-DELETE-MOR", _incremental(spark, table, start_snapshot_id=ids[0])
    )


def test_incremental_ending_at_a_delete_snapshot_does_not_raise(spark: Any) -> None:
    """QI-END-AT-DELETE — an overwrite snapshot as the window's end is legal.

    pins: ice-changelog-1/C-003
    """
    table = _table("QI-END-AT-DELETE")
    _seed(spark, table)
    spark.sql(f"DELETE FROM {table} WHERE id = 3")
    ids = _snaps(spark, table)
    _assert_ok(
        "QI-END-AT-DELETE",
        _incremental(spark, table, start_snapshot_id=ids[0], end_snapshot_id=ids[3]),
    )


def test_incremental_over_an_insert_overwrite_skips_it(spark: Any) -> None:
    """QI-OVER-OVERWRITE — an INSERT OVERWRITE in the range contributes nothing.

    pins: ice-changelog-1/C-003
    """
    table = _table("QI-OVER-OVERWRITE")
    _seed(spark, table)
    spark.sql(f"INSERT OVERWRITE {table} VALUES (9, 'z', 'x')")
    ids = _snaps(spark, table)
    _assert_ok(
        "QI-OVER-OVERWRITE", _incremental(spark, table, start_snapshot_id=ids[0])
    )


def test_incremental_over_a_partitioned_table(spark: Any) -> None:
    """QI-PARTITIONED — the window is unaffected by partitioning.

    pins: ice-changelog-1/C-001
    """
    table = _table("QI-PARTITIONED")
    ids = _seed(spark, table, part="PARTITIONED BY (cat)")
    _assert_ok(
        "QI-PARTITIONED",
        _incremental(spark, table, start_snapshot_id=ids[0], end_snapshot_id=ids[2]),
    )


def test_incremental_over_a_v3_table(spark: Any) -> None:
    """QI-V3 — format v3 reads the same rows.

    pins: ice-changelog-1/C-001
    """
    table = _table("QI-V3")
    ids = _seed(spark, table, version="3")
    _assert_ok("QI-V3", _incremental(spark, table, start_snapshot_id=ids[0]))


def test_incremental_reads_the_end_snapshots_schema(spark: Any) -> None:
    """QI-SCHEMA-EVOLVED — a column added inside the range is projected, null for older rows.

    pins: ice-changelog-1/C-008
    """
    table = _table("QI-SCHEMA-EVOLVED")
    _seed(spark, table)
    spark.sql(f"ALTER TABLE {table} ADD COLUMN extra INT")
    spark.sql(f"INSERT INTO {table} VALUES (6, 'f', 'x', 60)")
    ids = _snaps(spark, table)
    _assert_ok("QI-SCHEMA-EVOLVED", _incremental(spark, table, start_snapshot_id=ids[0]))


def _changes_fixture(spark: Any, cell_id: str, **seed_options: Any) -> str:
    """Seed the recorded fixture table and return its identifier."""
    table = _table(cell_id)
    _seed(spark, table, **seed_options)
    return table


def _view_name(cell_id: str) -> str:
    """The view name the recorder passed as ``changelog_view``."""
    return f"v_{cell_id.lower().replace('-', '_')}"


def _clv_fixture(spark: Any, cell_id: str, props: str = "") -> tuple[str, str, str]:
    """Build the recorded ``create_changelog_view`` fixture: three appends, an UPDATE, a DELETE."""
    table = _table(cell_id)
    _seed(spark, table, props=props)
    spark.sql(f"UPDATE {table} SET data = 'u' WHERE id = 2")
    spark.sql(f"DELETE FROM {table} WHERE id = 3")
    short = table.split(".", 1)[1]
    return table, short, _view_name(cell_id)


def _call_rows(frame: Any) -> list[list[Any]]:
    """Collect a CALL result in row order (the recorder did not sort ``out.rows``)."""
    arrow = frame.to_arrow()
    columns = [arrow.column(index).to_pylist() for index in range(arrow.num_columns)]
    return [[column[index] for column in columns] for index in range(arrow.num_rows)]


def _assert_call(cell_id: str, frame: Any) -> None:
    """Compare one recorded cell's ``out.cols`` / ``out.rows`` against a CALL result."""
    observed = CELLS[cell_id]["obs"]
    assert _cols(frame) == observed["out.cols"]
    assert _call_rows(frame) == observed["out.rows"]


def test_changes_relation_whole_history(spark: Any) -> None:
    """QI-CHANGES-DEFAULT — ``t.changes`` with no window covers the whole table history.

    pins: ice-changelog-1/C-009
    """
    table = _changes_fixture(spark, "QI-CHANGES-DEFAULT")
    frame = spark.sql(
        f"SELECT id, data, _change_type, _change_ordinal FROM {table}.changes"
    )
    _assert_ok("QI-CHANGES-DEFAULT", frame)


def test_changes_relation_schema(spark: Any) -> None:
    """QI-CHANGES-COLS — the user columns plus the three reserved change columns.

    pins: ice-changelog-1/C-009
    """
    table = _changes_fixture(spark, "QI-CHANGES-COLS")
    frame = spark.sql(
        f"SELECT id, data, cat, _change_type, _change_ordinal FROM {table}.changes"
    )
    _assert_ok("QI-CHANGES-COLS", frame)
    full = spark.sql(f"SELECT * FROM {table}.changes")
    assert [name for name, _ in _cols(full)] == [
        "id",
        "data",
        "cat",
        "_change_type",
        "_change_ordinal",
        "_commit_snapshot_id",
    ]


def test_changes_relation_over_a_whole_file_merge_on_read_delete(spark: Any) -> None:
    """QI-CHANGES-MOR-DELETE — a delete that removes a whole file reads as DELETE rows.

    pins: ice-changelog-1/C-009
    """
    table = _changes_fixture(spark, "QI-CHANGES-MOR-DELETE", props=MOR)
    spark.sql(f"DELETE FROM {table} WHERE id = 3")
    frame = spark.sql(f"SELECT id, _change_type, _change_ordinal FROM {table}.changes")
    _assert_ok("QI-CHANGES-MOR-DELETE", frame)


def test_changes_relation_over_a_copy_on_write_update(spark: Any) -> None:
    """QI-CHANGES-COW-UPDATE — a rewritten file yields DELETE+INSERT for every row in it.

    The relation is RAW: the carryover pair ``(1,a)`` stays, because carryover removal is the
    procedure's, never the relation's.

    pins: ice-changelog-1/C-009
    """
    table = _changes_fixture(spark, "QI-CHANGES-COW-UPDATE")
    spark.sql(f"UPDATE {table} SET data = 'u' WHERE id = 2")
    frame = spark.sql(
        f"SELECT id, data, _change_type, _change_ordinal FROM {table}.changes"
    )
    _assert_ok("QI-CHANGES-COW-UPDATE", frame)


def test_changes_relation_reader_window(spark: Any) -> None:
    """QI-CHANGES-READER-OPEN — the reader door takes the window options too.

    pins: ice-changelog-1/C-010
    """
    table = _changes_fixture(spark, "QI-CHANGES-READER-OPEN")
    ids = _snaps(spark, table)
    frame = (
        spark.read.format("iceberg")
        .option("start-snapshot-id", str(ids[1]))
        .load(f"{table}.changes")
        .select("id", "_change_type", "_change_ordinal")
    )
    _assert_ok("QI-CHANGES-READER-OPEN", frame)


def test_changes_relation_v3_deletion_vector(spark: Any) -> None:
    """QI-CHANGES-V3-DV — the same answers on format v3.

    pins: ice-changelog-1/C-009
    """
    table = _changes_fixture(spark, "QI-CHANGES-V3-DV", version="3", props=MOR)
    spark.sql(f"DELETE FROM {table} WHERE id = 3")
    frame = spark.sql(f"SELECT id, _change_type, _change_ordinal FROM {table}.changes")
    _assert_ok("QI-CHANGES-V3-DV", frame)


def test_create_changelog_view_default(spark: Any) -> None:
    """QC-DEFAULT — carryover removal keeps 8 of the 10 raw rows.

    pins: ice-changelog-1/C-011, C-014
    """
    _table_name, short, view = _clv_fixture(spark, "QC-DEFAULT")
    frame = spark.sql(
        f"CALL {CATALOG}.system.create_changelog_view("
        f"table => '{short}', changelog_view => '{view}')"
    )
    _assert_call("QC-DEFAULT", frame)
    rows = spark.sql(f"SELECT id, data, _change_type, _change_ordinal FROM {view}")
    assert _rows(rows) == CELLS["QC-DEFAULT"]["obs"]["view"]


def test_create_changelog_view_schema(spark: Any) -> None:
    """QC-SCHEMA — the view carries the changelog schema.

    pins: ice-changelog-1/C-014
    """
    _table_name, short, view = _clv_fixture(spark, "QC-SCHEMA")
    frame = spark.sql(
        f"CALL {CATALOG}.system.create_changelog_view("
        f"table => '{short}', changelog_view => '{view}')"
    )
    _assert_call("QC-SCHEMA", frame)
    empty = spark.sql(f"SELECT * FROM {view} LIMIT 0")
    assert _rows(empty) == CELLS["QC-SCHEMA"]["obs"]["view"]
    assert [name for name, _ in _cols(empty)] == [
        "id",
        "data",
        "cat",
        "_change_type",
        "_change_ordinal",
        "_commit_snapshot_id",
    ]


def test_create_changelog_view_net_changes(spark: Any) -> None:
    """QC-NET — each surviving row once, carrying the LAST touching ordinal.

    pins: ice-changelog-1/C-012, C-014
    """
    _table_name, short, view = _clv_fixture(spark, "QC-NET")
    frame = spark.sql(
        f"CALL {CATALOG}.system.create_changelog_view("
        f"table => '{short}', changelog_view => '{view}', net_changes => true)"
    )
    _assert_call("QC-NET", frame)
    rows = spark.sql(f"SELECT id, data, _change_type, _change_ordinal FROM {view}")
    assert _rows(rows) == CELLS["QC-NET"]["obs"]["view"]


def test_create_changelog_view_compute_updates(spark: Any) -> None:
    """QC-UPDATES-IDENT — one ordinal's DELETE+INSERT pair into UPDATE_BEFORE/UPDATE_AFTER.

    pins: ice-changelog-1/C-013, C-014
    """
    _table_name, short, view = _clv_fixture(spark, "QC-UPDATES-IDENT")
    frame = spark.sql(
        f"CALL {CATALOG}.system.create_changelog_view("
        f"table => '{short}', changelog_view => '{view}', compute_updates => true, "
        "identifier_columns => array('id'))"
    )
    _assert_call("QC-UPDATES-IDENT", frame)
    rows = spark.sql(f"SELECT id, data, _change_type, _change_ordinal FROM {view}")
    assert _rows(rows) == CELLS["QC-UPDATES-IDENT"]["obs"]["view"]


def test_create_changelog_view_duplicate_identifier_values(spark: Any) -> None:
    """QC-SQL-UPDATE-DUP — a non-unique identifier column answers as Spark's does.

    pins: ice-changelog-1/C-013
    """
    _table_name, short, view = _clv_fixture(spark, "QC-SQL-UPDATE-DUP")
    frame = spark.sql(
        f"CALL {CATALOG}.system.create_changelog_view("
        f"table => '{short}', changelog_view => '{view}', compute_updates => true, "
        "identifier_columns => array('cat'))"
    )
    _assert_call("QC-SQL-UPDATE-DUP", frame)
    rows = spark.sql(f"SELECT id, data, _change_type, _change_ordinal FROM {view}")
    assert _rows(rows) == CELLS["QC-SQL-UPDATE-DUP"]["obs"]["view"]


def test_create_changelog_view_without_identifier_columns_refuses(spark: Any) -> None:
    """QC-UPDATES-NO-IDENT — ``compute_updates`` with no identifier columns raises.

    pins: ice-changelog-1/C-014
    """
    _table_name, short, view = _clv_fixture(spark, "QC-UPDATES-NO-IDENT")
    with pytest.raises(Exception) as raised:  # noqa: B017
        spark.sql(
            f"CALL {CATALOG}.system.create_changelog_view("
            f"table => '{short}', changelog_view => '{view}', compute_updates => true)"
        )
    _assert_error("QC-UPDATES-NO-IDENT", raised.value)


def test_create_changelog_view_net_changes_with_updates_refuses(spark: Any) -> None:
    """QC-NET-AND-UPDATES — Java's ``Not support net changes with update images``.

    pins: ice-changelog-1/C-014
    """
    _table_name, short, view = _clv_fixture(spark, "QC-NET-AND-UPDATES")
    with pytest.raises(Exception) as raised:  # noqa: B017
        spark.sql(
            f"CALL {CATALOG}.system.create_changelog_view("
            f"table => '{short}', changelog_view => '{view}', net_changes => true, "
            "compute_updates => true, identifier_columns => array('id'))"
        )
    _assert_error("QC-NET-AND-UPDATES", raised.value)


def test_create_changelog_view_snapshot_window(spark: Any) -> None:
    """QC-OPT-RANGE — ``options => map('start-snapshot-id', …, 'end-snapshot-id', …)``.

    The ordinal restarts at 0 inside the window.

    pins: ice-changelog-1/C-010, C-014
    """
    table, short, view = _clv_fixture(spark, "QC-OPT-RANGE")
    ids = _snaps(spark, table)
    frame = spark.sql(
        f"CALL {CATALOG}.system.create_changelog_view("
        f"table => '{short}', changelog_view => '{view}', "
        f"options => map('start-snapshot-id', '{ids[1]}', 'end-snapshot-id', '{ids[3]}'))"
    )
    _assert_call("QC-OPT-RANGE", frame)
    rows = spark.sql(f"SELECT id, data, _change_type, _change_ordinal FROM {view}")
    assert _rows(rows) == CELLS["QC-OPT-RANGE"]["obs"]["view"]


def test_create_changelog_view_start_timestamp_zero(spark: Any) -> None:
    """QC-OPT-TS — ``start-timestamp = 0`` is before every snapshot, so nothing is excluded.

    pins: ice-changelog-1/C-010
    """
    _table_name, short, view = _clv_fixture(spark, "QC-OPT-TS")
    frame = spark.sql(
        f"CALL {CATALOG}.system.create_changelog_view("
        f"table => '{short}', changelog_view => '{view}', "
        "options => map('start-timestamp', '0'))"
    )
    _assert_call("QC-OPT-TS", frame)
    rows = spark.sql(f"SELECT id, data, _change_type, _change_ordinal FROM {view}")
    assert _rows(rows) == CELLS["QC-OPT-TS"]["obs"]["view"]


def test_create_changelog_view_on_a_merge_on_read_table_refuses_at_read(spark: Any) -> None:
    """QC-MOR — the CALL succeeds and the VIEW READ raises, as Spark's lazy view does.

    pins: ice-changelog-1/C-015
    """
    _table_name, short, view = _clv_fixture(spark, "QC-MOR", props=MOR)
    frame = spark.sql(
        f"CALL {CATALOG}.system.create_changelog_view("
        f"table => '{short}', changelog_view => '{view}')"
    )
    _assert_call("QC-MOR", frame)
    with pytest.raises(Exception) as raised:  # noqa: B017
        spark.sql(f"SELECT id, data, _change_type, _change_ordinal FROM {view}").to_arrow()
    _assert_error("QC-MOR", raised.value)


def test_create_changelog_view_default_name_is_backticked(spark: Any) -> None:
    """QC-DEFAULT-NAME — the returned name carries backticks; the view is addressable without.

    pins: ice-changelog-1/C-014
    """
    _table_name, short, _view = _clv_fixture(spark, "QC-DEFAULT-NAME")
    frame = spark.sql(
        f"CALL {CATALOG}.system.create_changelog_view(table => '{short}')"
    )
    _assert_call("QC-DEFAULT-NAME", frame)
    rows = spark.sql("SELECT count(*) FROM t_qc_default_name_changes")
    assert _rows(rows) == CELLS["QC-DEFAULT-NAME"]["obs"]["view"]

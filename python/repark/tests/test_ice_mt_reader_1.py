"""IPI-23-MT-READER-1 — the DataFrame reader loads Iceberg metadata tables.

``spark.read.format("iceberg").load("<cat>.<ns>.<t>.<meta>")`` (and
``read.table``) answers what SQL ``SELECT * FROM <cat>.<ns>.<t>.<meta>``
answers, and with ``versionAsOf`` / ``timestampAsOf`` what SQL
``SELECT * FROM …​.<meta> VERSION AS OF`` / ``TIMESTAMP AS OF`` answers.
Every answering test compares the reader against the SQL door on the same
table (rows and column names) and, where the recorded cells measured them,
the absolute field name, ``dataType.simpleString()``, ``nullable`` and rows
of the reader frame; every refusal compares the reader's class and text
against the SQL door's. Near misses pin today's behaviour:
plain loads, branch/tag/snapshot-id selectors, a real table named
``snapshots``, the unknown-suffix error, and the legacy-option refusals.

Oracle: recorded Spark 4.1.2 + Iceberg 1.11.0 inventory cells
``R-DF-LOAD-META`` (``load(t.snapshots)`` rows ``append, append,
overwrite``) and ``R-DF-LOAD-META-FILES-VERSIONASOF``
(``versionAsOf`` on ``load(t.files)`` rows ``[2]``), replayed in
``sb-mt``; the live tier replays the reader/SQL equality on live Spark.

pins: ipi-23-mt-reader-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
pins: ipi-23-mt-reader-1/C-008, C-009, C-010, C-011, C-012, C-013, C-014, C-015
pins: ipi-23-mt-reader-1/C-016, C-017, C-018, C-019, C-020, C-021, C-022
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, IllegalArgumentException
from repark.spark.session import _reset_active_session_for_tests

CATALOG = "mt"
NAMESPACE = "ns"
SEED_DDL = "(id BIGINT, data STRING, cat STRING)"
FIRST_APPEND = "(1, 'a', 'x'), (2, 'b', 'y')"
SECOND_APPEND = "(3, 'c', 'x')"
SNAPSHOT_ID_MSG = (
    "Time travel option `snapshot-id` is no longer supported, "
    "use Spark built-in `versionAsOf` instead"
)
AS_OF_TIMESTAMP_MSG = (
    "Time travel option `as-of-timestamp` (in millis) is no longer supported, "
    "use Spark built-in `timestampAsOf` instead (properly formatted timestamp)"
)
TAG_MSG = (
    "Time travel option `tag` is no longer supported, use Spark built-in `versionAsOf` instead"
)
LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — live Spark cell skipped (routine CI is JVM-free)"


@pytest.fixture
def spark(tmp_path: Path) -> Any:
    """Yield a facade session with an in-memory Iceberg catalog named ``mt``."""
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("test-ice-mt-reader-1").getOrCreate()
    session.register_memory_catalog(CATALOG, tmp_path / "wh")
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NAMESPACE}")
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _seeded(session: Any, name: str) -> str:
    """Create the two-append table, delete id 1, return its three-part name."""
    table = f"{CATALOG}.{NAMESPACE}.{name}"
    session.sql(
        f"CREATE TABLE {table} {SEED_DDL} USING iceberg TBLPROPERTIES ('format-version'='2')"
    )
    session.sql(f"INSERT INTO {table} VALUES {FIRST_APPEND}")
    session.sql(f"INSERT INTO {table} VALUES {SECOND_APPEND}")
    session.sql(f"DELETE FROM {table} WHERE id = 1")
    return table


def _snapshot_ids(session: Any, table: str) -> list[int]:
    """Snapshot ids oldest first via the table's own snapshots table."""
    rows = session.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at")
    return [int(row[0]) for row in rows.collect()]


def _committed_at(session: Any, table: str) -> list[Any]:
    """Snapshot commit instants oldest first via the snapshots table."""
    rows = session.sql(f"SELECT committed_at FROM {table}.snapshots ORDER BY committed_at")
    return [row[0] for row in rows.collect()]


def _frame_rows(frame: Any) -> list[list[Any]]:
    """Collect a frame as plain lists sorted by rendered form."""
    return sorted((list(row) for row in frame.collect()), key=repr)


def _frame_cols(frame: Any) -> list[str]:
    """Collect a frame's schema field names in order."""
    return [field.name for field in frame.schema.fields]


def _live_rows(frame: Any) -> list[list[Any]]:
    """Collect a live-Spark frame as plain lists sorted by rendered form."""
    return sorted((list(row) for row in frame.collect()), key=repr)


def _iceberg_reader(session: Any) -> Any:
    """Return the session's ``format("iceberg")`` reader."""
    return session.read.format("iceberg")


def test_load_snapshots_equals_sql(spark: Any) -> None:
    """Cell ``R-DF-LOAD-META``: ``load(t.snapshots)`` equals the SQL door.

    pins: ipi-23-mt-reader-1/C-001
    """
    table = _seeded(spark, "t_load_snapshots")
    reader = _iceberg_reader(spark).load(f"{table}.snapshots")
    sql = spark.sql(f"SELECT * FROM {table}.snapshots")
    assert _frame_rows(reader) == _frame_rows(sql)
    assert _frame_cols(reader) == _frame_cols(sql)
    operations = spark.sql(f"SELECT operation FROM {table}.snapshots ORDER BY committed_at")
    assert [row[0] for row in operations.collect()] == ["append", "append", "overwrite"]
    operation_frame = reader.select("operation")
    assert [
        (field.name, field.dataType.simpleString(), field.nullable)
        for field in operation_frame.schema.fields
    ] == [("operation", "string", True)]
    assert sorted(row[0] for row in operation_frame.collect()) == [
        "append",
        "append",
        "overwrite",
    ]


def test_load_files_history_refs_equal_sql(spark: Any) -> None:
    """``load(t.<meta>)`` equals the SQL door for files, history, refs.

    pins: ipi-23-mt-reader-1/C-002
    """
    table = _seeded(spark, "t_load_meta")
    for suffix in ("files", "history", "refs"):
        reader = _iceberg_reader(spark).load(f"{table}.{suffix}")
        sql = spark.sql(f"SELECT * FROM {table}.{suffix}")
        assert _frame_rows(reader) == _frame_rows(sql), suffix
        assert _frame_cols(reader) == _frame_cols(sql), suffix
    files_fields = [
        (field.name, field.dataType.simpleString(), field.nullable)
        for field in _iceberg_reader(spark).load(f"{table}.files").schema.fields
    ]
    assert ("record_count", "bigint", False) in files_fields


def test_table_api_snapshots_equals_sql(spark: Any) -> None:
    """``read.table(t.snapshots)`` equals the SQL door, rows and columns.

    pins: ipi-23-mt-reader-1/C-003
    """
    table = _seeded(spark, "t_table_meta")
    reader = spark.read.table(f"{table}.snapshots")
    sql = spark.sql(f"SELECT * FROM {table}.snapshots")
    assert _frame_rows(reader) == _frame_rows(sql)
    assert _frame_cols(reader) == _frame_cols(sql)
    assert sorted(row[0] for row in reader.select("operation").collect()) == [
        "append",
        "append",
        "overwrite",
    ]


def test_load_version_as_of_equals_sql(spark: Any) -> None:
    """``versionAsOf`` on ``load(t.<meta>)`` equals SQL ``VERSION AS OF``.

    pins: ipi-23-mt-reader-1/C-004
    """
    table = _seeded(spark, "t_load_version")
    first = _snapshot_ids(spark, table)[0]
    for suffix in ("files", "snapshots", "entries", "manifests"):
        reader = _iceberg_reader(spark).option("versionAsOf", first).load(f"{table}.{suffix}")
        sql = spark.sql(f"SELECT * FROM {table}.{suffix} VERSION AS OF {first}")
        assert _frame_rows(reader) == _frame_rows(sql), suffix
        assert _frame_cols(reader) == _frame_cols(sql), suffix
    record_frame = (
        _iceberg_reader(spark)
        .option("versionAsOf", first)
        .load(f"{table}.files")
        .select("record_count")
    )
    assert [
        (field.name, field.dataType.simpleString(), field.nullable)
        for field in record_frame.schema.fields
    ] == [("record_count", "bigint", False)]
    assert _frame_rows(record_frame) == [[2]]


def test_load_timestamp_as_of_equals_sql(spark: Any) -> None:
    """``timestampAsOf`` on ``load(t.files)`` equals SQL ``TIMESTAMP AS OF``.

    pins: ipi-23-mt-reader-1/C-005
    """
    table = _seeded(spark, "t_load_ts")
    stamp = _committed_at(spark, table)[1].strftime("%Y-%m-%d %H:%M:%S.%f")
    reader = _iceberg_reader(spark).option("timestampAsOf", stamp).load(f"{table}.files")
    sql = spark.sql(f"SELECT * FROM {table}.files TIMESTAMP AS OF '{stamp}'")
    assert _frame_rows(reader) == _frame_rows(sql)
    assert _frame_cols(reader) == _frame_cols(sql)
    assert _frame_rows(reader.select("record_count")) == [[1], [2]]


def test_reader_as_of_refusals_match_sql(spark: Any) -> None:
    """Reader AS OF refusals carry the SQL door's class and exact text.

    pins: ipi-23-mt-reader-1/C-006
    """
    table = _seeded(spark, "t_reader_refusals")
    with pytest.raises(IllegalArgumentException) as sql_nope:
        spark.sql(f"SELECT count(*) FROM {table}.files VERSION AS OF 'nope'").collect()
    with pytest.raises(IllegalArgumentException) as reader_nope:
        _iceberg_reader(spark).option("versionAsOf", "nope").load(f"{table}.files").collect()
    assert str(reader_nope.value) == str(sql_nope.value)
    assert str(reader_nope.value) == (
        "Cannot find matching snapshot ID or reference name for version nope"
    )
    with pytest.raises(IllegalArgumentException) as sql_old:
        sql_old_query = f"SELECT count(*) FROM {table}.files TIMESTAMP AS OF '2000-01-01 00:00:00'"
        spark.sql(sql_old_query).collect()
    with pytest.raises(IllegalArgumentException) as reader_old:
        reader_old_frame = _iceberg_reader(spark).option("timestampAsOf", "2000-01-01 00:00:00")
        reader_old_frame.load(f"{table}.files").collect()
    assert str(reader_old.value) == str(sql_old.value)
    assert str(reader_old.value) == "Cannot find a snapshot older than 2000-01-01T00:00:00+00:00"
    first = _snapshot_ids(spark, table)[0]
    with pytest.raises(AnalysisException) as sql_all:
        spark.sql(f"SELECT count(*) FROM {table}.all_files VERSION AS OF {first}").collect()
    with pytest.raises(AnalysisException) as reader_all:
        _iceberg_reader(spark).option("versionAsOf", first).load(f"{table}.all_files").collect()
    assert str(reader_all.value) == str(sql_all.value)
    assert "Cannot select snapshot in table: ALL_FILES" in str(reader_all.value)


def test_reader_unknown_numeric_as_of_answers_empty(spark: Any) -> None:
    """Unknown numeric ``versionAsOf`` answers empty with the SQL schema.

    pins: ipi-23-mt-reader-1/C-007
    """
    table = _seeded(spark, "t_reader_empty")
    reader = _iceberg_reader(spark).option("versionAsOf", 999_999_999).load(f"{table}.files")
    sql = spark.sql(f"SELECT * FROM {table}.files VERSION AS OF 999999999")
    assert _frame_rows(reader) == []
    assert _frame_rows(reader) == _frame_rows(sql)
    assert _frame_cols(reader) == _frame_cols(sql)
    assert ("record_count", "bigint", False) in [
        (field.name, field.dataType.simpleString(), field.nullable)
        for field in reader.schema.fields
    ]


def test_load_plain_and_version_as_of_unchanged(spark: Any) -> None:
    """Near miss: plain and pinned loads of the base table keep their rows.

    pins: ipi-23-mt-reader-1/C-008
    """
    table = _seeded(spark, "t_load_plain")
    first = _snapshot_ids(spark, table)[0]
    current = _frame_rows(_iceberg_reader(spark).load(table))
    assert current == _frame_rows(spark.sql(f"SELECT * FROM {table}"))
    assert current == [[2, "b", "y"], [3, "c", "x"]]
    pinned = _frame_rows(_iceberg_reader(spark).option("versionAsOf", first).load(table))
    assert pinned == _frame_rows(spark.sql(f"SELECT * FROM {table} VERSION AS OF {first}"))
    assert pinned == [[1, "a", "x"], [2, "b", "y"]]


def test_load_branch_tag_snapshot_id_selectors_unchanged(spark: Any) -> None:
    """Near miss: selector loads equal the SQL door as they do today.

    pins: ipi-23-mt-reader-1/C-009
    """
    table = _seeded(spark, "t_load_selectors")
    first, second = _snapshot_ids(spark, table)[:2]
    spark.sql(f"ALTER TABLE {table} CREATE TAG t0 AS OF VERSION {first}")
    spark.sql(f"ALTER TABLE {table} CREATE BRANCH b0 AS OF VERSION {second}")
    expected = {
        "branch_b0": [[1, "a", "x"], [2, "b", "y"], [3, "c", "x"]],
        "tag_t0": [[1, "a", "x"], [2, "b", "y"]],
        f"snapshot_id_{first}": [[1, "a", "x"], [2, "b", "y"]],
    }
    for suffix in ("branch_b0", "tag_t0", f"snapshot_id_{first}"):
        reader = _iceberg_reader(spark).load(f"{table}.{suffix}")
        sql = spark.sql(f"SELECT * FROM {table}.{suffix}")
        assert _frame_rows(reader) == _frame_rows(sql), suffix
        assert _frame_cols(reader) == _frame_cols(sql), suffix
        assert _frame_rows(reader) == expected[suffix], suffix


def test_load_table_named_snapshots_reads_real_table(spark: Any) -> None:
    """Near miss: a real table named ``snapshots`` reads its own rows.

    pins: ipi-23-mt-reader-1/C-010
    """
    table = f"{CATALOG}.{NAMESPACE}.snapshots"
    spark.sql(f"CREATE TABLE {table} (id BIGINT) USING iceberg")
    spark.sql(f"INSERT INTO {table} VALUES (1), (2)")
    assert _frame_rows(_iceberg_reader(spark).load(table)) == [[1], [2]]


def test_load_unknown_suffix_keeps_error_text(spark: Any) -> None:
    """Near miss: a four-part unknown suffix keeps its error text.

    pins: ipi-23-mt-reader-1/C-011
    """
    table = _seeded(spark, "t_load_nope")
    with pytest.raises(AnalysisException) as reader_nope:
        _iceberg_reader(spark).load(f"{table}.nope").collect()
    assert str(reader_nope.value) == (
        "Error during planning: Unsupported compound identifier "
        f"'{table}.nope'. Expected 1, 2 or 3 parts, got 4"
    )


def test_legacy_options_on_metadata_keep_refusal_texts(spark: Any) -> None:
    """Near miss: legacy options refuse on a metadata path with #800 texts.

    pins: ipi-23-mt-reader-1/C-012
    """
    table = _seeded(spark, "t_legacy_meta")
    first = _snapshot_ids(spark, table)[0]
    cases = [
        ("snapshot-id", first, SNAPSHOT_ID_MSG),
        ("as-of-timestamp", 1_700_000_000_000, AS_OF_TIMESTAMP_MSG),
        ("tag", "t0", TAG_MSG),
    ]
    for key, value, message in cases:
        with pytest.raises(IllegalArgumentException) as caught:
            _iceberg_reader(spark).option(key, value).load(f"{table}.files")
        assert str(caught.value) == message, key


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_live_spark_reader_matches_sql(tmp_path: Path) -> None:
    """Live leg: on Spark itself the reader answers what SQL answers.

    pins: ipi-23-mt-reader-1/C-013
    """
    import _live_parity as live_parity

    oracle = live_parity.build_spark_iceberg_engine(tmp_path / "spark-wh", catalog="livemt")
    session = oracle.session
    session.sql("CREATE NAMESPACE IF NOT EXISTS livemt.ns")
    table = "livemt.ns.t_live"
    session.sql(f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg")
    session.sql(f"INSERT INTO {table} VALUES (1, 'a', 'x'), (2, 'b', 'y')")
    session.sql(f"INSERT INTO {table} VALUES (3, 'c', 'x')")
    session.sql(f"DELETE FROM {table} WHERE id = 1")
    first = session.sql(
        f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at, snapshot_id LIMIT 1"
    ).collect()[0][0]
    reader_snaps = session.read.format("iceberg").load(f"{table}.snapshots").select("operation")
    sql_snaps = session.sql(f"SELECT operation FROM {table}.snapshots ORDER BY committed_at")
    assert _live_rows(reader_snaps) == _live_rows(sql_snaps)
    assert sorted(row[0] for row in reader_snaps.collect()) == [
        "append",
        "append",
        "overwrite",
    ]
    assert [
        (field.name, field.dataType.simpleString(), field.nullable)
        for field in reader_snaps.schema.fields
    ] == [("operation", "string", True)]
    reader_files = session.read.format("iceberg").option("versionAsOf", first)
    reader_files = reader_files.load(f"{table}.files").select("record_count")
    sql_files = session.sql(f"SELECT record_count FROM {table}.files VERSION AS OF {first}")
    assert _live_rows(reader_files) == _live_rows(sql_files)
    assert _live_rows(reader_files) == [[2]]
    assert [
        (field.name, field.dataType.simpleString(), field.nullable)
        for field in reader_files.schema.fields
    ] == [("record_count", "bigint", False)]


def test_load_case_twin_table_matches_sql_door(spark: Any) -> None:
    """V-001: a case-twin table fails like the SQL door, backticks included.

    pins: ipi-23-mt-reader-1/C-014
    """
    _seeded(spark, "t_casetwin")
    twin = f"{CATALOG}.{NAMESPACE}.T_CASETWIN"
    with pytest.raises(AnalysisException) as reader_twin:
        _iceberg_reader(spark).load(f"{twin}.snapshots").collect()
    with pytest.raises(AnalysisException) as sql_twin:
        spark.sql(f"SELECT operation FROM {twin}.snapshots").collect()
    assert str(reader_twin.value) == str(sql_twin.value)


def test_quoted_dollar_missing_table_refuses_all_files(spark: Any) -> None:
    """V-002: the missing-table dollar spelling refuses before the load.

    pins: ipi-23-mt-reader-1/C-015
    """
    table = _seeded(spark, "t_dollar_refuse")
    first = _snapshot_ids(spark, table)[0]
    missing_query = (
        f'SELECT count(*) FROM {CATALOG}.{NAMESPACE}."missing$all_files" VERSION AS OF 1'
    )
    existing_query = (
        f'SELECT count(*) FROM {CATALOG}.{NAMESPACE}."t_dollar_refuse$all_files" VERSION AS OF 1'
    )
    with pytest.raises(AnalysisException) as missing:
        spark.sql(missing_query).collect()
    with pytest.raises(AnalysisException) as existing_dollar:
        spark.sql(existing_query).collect()
    with pytest.raises(AnalysisException) as existing_dotted:
        spark.sql(f"SELECT count(*) FROM {table}.all_files VERSION AS OF {first}").collect()
    assert str(missing.value) == str(existing_dollar.value)
    assert str(missing.value) == str(existing_dotted.value)
    assert missing.value.getSqlState() == existing_dollar.value.getSqlState()
    assert missing.value.getSqlState() == existing_dotted.value.getSqlState()
    assert "Cannot select snapshot in table: ALL_FILES" in str(missing.value)


def test_load_uppercase_suffix_equals_sql(spark: Any) -> None:
    """Sweep: an uppercase suffix serves on both doors alike.

    pins: ipi-23-mt-reader-1/C-016
    """
    table = _seeded(spark, "t_upper_suffix")
    reader = _iceberg_reader(spark).load(f"{table}.SNAPSHOTS")
    sql = spark.sql(f"SELECT * FROM {table}.SNAPSHOTS")
    assert _frame_rows(reader) == _frame_rows(sql)
    assert _frame_cols(reader) == _frame_cols(sql)
    assert sorted(row[0] for row in reader.select("operation").collect()) == [
        "append",
        "append",
        "overwrite",
    ]


def test_load_missing_parent_matches_sql_door(spark: Any) -> None:
    """Sweep: missing table and missing namespace fail alike on both doors.

    pins: ipi-23-mt-reader-1/C-017
    """
    _seeded(spark, "t_missing_parent")
    missing_table = f"{CATALOG}.{NAMESPACE}.missing"
    missing_ns = f"{CATALOG}.nope.t_missing_parent"
    for name in (missing_table, missing_ns):
        with pytest.raises(AnalysisException) as reader_missing:
            _iceberg_reader(spark).load(f"{name}.snapshots").collect()
        with pytest.raises(AnalysisException) as sql_missing:
            spark.sql(f"SELECT * FROM {name}.snapshots").collect()
        assert str(reader_missing.value) == str(sql_missing.value), name


def test_load_all_types_without_as_of_equals_sql(spark: Any) -> None:
    """Sweep: every ``all_*`` table serves on both doors alike.

    pins: ipi-23-mt-reader-1/C-018
    """
    table = _seeded(spark, "t_all_noasof")
    spellings = (
        "all_files",
        "all_data_files",
        "all_delete_files",
        "all_entries",
        "all_manifests",
    )
    for suffix in spellings:
        reader = _iceberg_reader(spark).load(f"{table}.{suffix}")
        sql = spark.sql(f"SELECT * FROM {table}.{suffix}")
        assert _frame_rows(reader) == _frame_rows(sql), suffix
        assert _frame_cols(reader) == _frame_cols(sql), suffix


def test_load_quoted_dollar_spelling_equals_sql(spark: Any) -> None:
    """Sweep: the quoted ``t$suffix`` spelling serves on both doors alike.

    pins: ipi-23-mt-reader-1/C-019
    """
    _seeded(spark, "t_dollar_nosof")
    reader = _iceberg_reader(spark).load(f'{CATALOG}.{NAMESPACE}."t_dollar_nosof$snapshots"')
    sql = spark.sql(f'SELECT * FROM {CATALOG}.{NAMESPACE}."t_dollar_nosof$snapshots"')
    assert _frame_rows(reader) == _frame_rows(sql)
    assert _frame_cols(reader) == _frame_cols(sql)


def test_load_uppercase_suffix_as_of_equals_sql(spark: Any) -> None:
    """Sweep: an uppercase suffix with ``versionAsOf`` serves on both doors.

    pins: ipi-23-mt-reader-1/C-020
    """
    table = _seeded(spark, "t_upper_asof")
    first = _snapshot_ids(spark, table)[0]
    reader = _iceberg_reader(spark).option("versionAsOf", first).load(f"{table}.FILES")
    sql = spark.sql(f"SELECT * FROM {table}.FILES VERSION AS OF {first}")
    assert _frame_rows(reader) == _frame_rows(sql)
    assert _frame_cols(reader) == _frame_cols(sql)
    assert _frame_rows(reader.select("record_count")) == [[2]]


def test_load_missing_parent_as_of_matches_sql_door(spark: Any) -> None:
    """Sweep: missing parents and unknown suffix fail alike with AS OF.

    pins: ipi-23-mt-reader-1/C-021
    """
    table = _seeded(spark, "t_missing_asof")
    first = _snapshot_ids(spark, table)[0]
    names = (
        f"{CATALOG}.{NAMESPACE}.missing.files",
        f"{CATALOG}.nope.t_missing_asof.files",
        f"{table}.nope",
    )
    for name in names:
        with pytest.raises(AnalysisException) as reader_missing:
            _iceberg_reader(spark).option("versionAsOf", first).load(name).collect()
        with pytest.raises(AnalysisException) as sql_missing:
            spark.sql(f"SELECT * FROM {name} VERSION AS OF {first}").collect()
        assert str(reader_missing.value) == str(sql_missing.value), name


def test_load_all_types_as_of_refuse_alike(spark: Any) -> None:
    """Sweep: every ``all_*`` refusal matches the SQL door's class and text.

    pins: ipi-23-mt-reader-1/C-022
    """
    table = _seeded(spark, "t_all_asof")
    first = _snapshot_ids(spark, table)[0]
    spellings = ("all_data_files", "all_delete_files", "all_entries", "all_manifests")
    for suffix in spellings:
        upper = suffix.upper()
        with pytest.raises(AnalysisException) as reader_refusal:
            reader_pinned = _iceberg_reader(spark).option("versionAsOf", first)
            reader_pinned.load(f"{table}.{suffix}").collect()
        with pytest.raises(AnalysisException) as sql_refusal:
            spark.sql(f"SELECT count(*) FROM {table}.{suffix} VERSION AS OF {first}").collect()
        assert str(reader_refusal.value) == str(sql_refusal.value), suffix
        assert f"Cannot select snapshot in table: {upper}" in str(reader_refusal.value), suffix

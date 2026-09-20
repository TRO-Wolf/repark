"""IPI-21 + IPI-25 + IPI-42 — the three small parser shapes over finished behaviour.

Three inventory rows, nine cells, one mechanism: RePark's pre-parse seam refused a keyword
on the way to behaviour that already worked.

* **IPI-42** — ``CREATE BRANCH|TAG IF NOT EXISTS`` and ``DROP BRANCH|TAG IF EXISTS``.
* **IPI-25** — ``REPLACE TABLE`` and ``REPLACE TABLE … AS SELECT``.
* **IPI-21** — ``DROP TABLE … PURGE``, with the ``gc.enabled=false`` refusal.

Oracle: the run-25/26 inventory harness cells recorded against live PySpark 4.1.2 +
``iceberg-spark-runtime-4.1_2.13:1.11.0`` (``/tmp/oc-worker/nc-inventory/matrix.json``,
cells ``D-REF-CREATE-BRANCH-IF-NOT-EXISTS``, ``D-REF-TAG-IF-NOT-EXISTS``,
``D-REF-DROP-BRANCH-IF-EXISTS``, ``D-REF-DROP-TAG-IF-EXISTS``, ``D-REPLACE``, ``D-RTAS``,
``D-RTAS-TIME-TRAVEL``, ``D-DROP-TABLE-PURGE``, ``TP-GC-DISABLED-PURGE``, and the
regression cells ``D-DROP-TABLE``, ``D-DROP-TABLE-NO-PURGE``, ``D-DROP-TABLE-IF-EXISTS``),
plus the probe ``/tmp/oc-worker/qe/probe/p3.json`` keys ``E.*`` for the shapes no cell
covers (a missing ``REPLACE TABLE``, ``CREATE OR REPLACE BRANCH IF NOT EXISTS``, the
guardless refusals, a guard that must still create or still drop, and ``PURGE`` composed
with ``IF EXISTS``).

The four recorded ref cells all happen to exercise the *no-op* branch, so a guard that
always no-ops would pass them; the conditional pins below close that. The recorded
``CREATE BRANCH IF NOT EXISTS`` cell also pins its ref at the table's current snapshot,
so a guard implemented as "replace if different" would pass it; the pins here pin the
branch at an OLDER snapshot first, so a replace moves it and the assertion reds.

pins: ipi-21-25-42-small-parser/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008,
C-009, C-010
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, ParseException
from repark.spark.session import _reset_active_session_for_tests

CATALOG = "sc"
NAMESPACE = "ns"
SEED_DDL = "(id BIGINT, data STRING, cat STRING)"
SEED_ROWS = "(0, 'd0', 'a'), (1, 'd1', 'b'), (2, 'd2', 'a')"
SEED_DATA = [[0, "d0", "a"], [1, "d1", "b"], [2, "d2", "a"]]
GC_DISABLED_REFUSAL = "Cannot purge table: GC is disabled (deleting files may corrupt other tables)"


@pytest.fixture
def spark(tmp_path: Path) -> Any:
    """Yield a facade session with an in-memory Iceberg catalog named ``sc``."""
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("test-ice-small-parser-1").getOrCreate()
    session.register_memory_catalog(CATALOG, tmp_path / "wh")
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NAMESPACE}")
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _create(session: Any, name: str, properties: str = "") -> str:
    """Create one empty seed-shaped Iceberg table and return its three-part name."""
    table = f"{CATALOG}.{NAMESPACE}.{name}"
    tail = f", {properties}" if properties else ""
    session.sql(
        f"CREATE TABLE {table} {SEED_DDL} USING iceberg "
        f"TBLPROPERTIES ('format-version'='2'{tail})"
    )
    return table


def _seeded_once(session: Any, name: str, properties: str = "") -> str:
    """Create and seed a table with one append, mirroring the inventory's ``mk`` + ``seed``."""
    table = _create(session, name, properties)
    session.sql(f"INSERT INTO {table} VALUES {SEED_ROWS}")
    return table


def _seeded_twice(session: Any, name: str) -> str:
    """Create and seed a table with two appends, so a ref can sit at the older snapshot."""
    table = _create(session, name)
    session.sql(f"INSERT INTO {table} VALUES (0, 'd0', 'a'), (1, 'd1', 'b')")
    session.sql(f"INSERT INTO {table} VALUES (2, 'd2', 'a')")
    return table


def _snapshot_ids(session: Any, table: str) -> list[int]:
    """Snapshot ids in ``(committed_at, snapshot_id)`` order."""
    rows = session.sql(f"SELECT snapshot_id, committed_at FROM {table}.snapshots").collect()
    return [row[0] for row in sorted(rows, key=lambda row: (row[1], row[0]))]


def _refs(session: Any, table: str) -> list[list[Any]]:
    """Every snapshot ref as sorted ``[name, type, snapshot_id]`` rows."""
    rows = session.sql(f"SELECT name, type, snapshot_id FROM {table}.refs").collect()
    return sorted([row[0], row[1], row[2]] for row in rows)


def _ref_names(session: Any, table: str) -> list[str]:
    """Just the ref names, sorted."""
    return sorted(row[0] for row in _refs(session, table))


def _operations(session: Any, table: str) -> list[str]:
    """Snapshot operations in commit order."""
    rows = session.sql(
        f"SELECT operation, snapshot_id, committed_at FROM {table}.snapshots"
    ).collect()
    return [row[0] for row in sorted(rows, key=lambda row: (row[2], row[1]))]


def _schema(frame: Any) -> list[tuple[str, str]]:
    """``(name, simpleString)`` per field of a frame's schema."""
    return [(field.name, field.dataType.simpleString()) for field in frame.schema.fields]


def _rows(session: Any, query: str) -> list[list[Any]]:
    """Collect one query as sorted plain lists."""
    return sorted((list(row) for row in session.sql(query).collect()), key=repr)


def _data_paths(session: Any, table: str) -> list[str]:
    """Every data-file path the table reaches, through the ``all_files`` metadata table."""
    return [row[0] for row in session.sql(f"SELECT file_path FROM {table}.all_files").collect()]


def _exist(paths: list[str]) -> list[bool]:
    """Whether each recorded path is still on disk."""
    return [Path(path.replace("file://", "")).exists() for path in paths]


def test_create_branch_if_not_exists_is_noop(spark: Any) -> None:
    """Cell ``D-REF-CREATE-BRANCH-IF-NOT-EXISTS``: no error, no change, ref not moved."""
    table = _seeded_twice(spark, "t_branch_guard")
    first, second = _snapshot_ids(spark, table)[:2]
    spark.sql(f"ALTER TABLE {table} CREATE BRANCH b1 AS OF VERSION {first}")
    before = _refs(spark, table)

    spark.sql(f"ALTER TABLE {table} CREATE BRANCH IF NOT EXISTS b1")

    after = _refs(spark, table)
    assert after == before
    assert ["b1", "BRANCH", first] in after
    assert ["main", "BRANCH", second] in after


def test_create_tag_if_not_exists_with_version_is_noop(spark: Any) -> None:
    """Cell ``D-REF-TAG-IF-NOT-EXISTS``: the ``AS OF VERSION`` is ignored on an existing tag."""
    table = _seeded_twice(spark, "t_tag_guard")
    first, second = _snapshot_ids(spark, table)[:2]
    spark.sql(f"ALTER TABLE {table} CREATE TAG t1")
    before = _refs(spark, table)

    spark.sql(f"ALTER TABLE {table} CREATE TAG IF NOT EXISTS t1 AS OF VERSION {first}")

    after = _refs(spark, table)
    assert after == before
    assert ["t1", "TAG", second] in after


def test_create_branch_if_not_exists_missing_creates_it(spark: Any) -> None:
    """Probe ``E.create_branch_if_not_exists_missing``: the guard is not a blanket no-op."""
    table = _seeded_twice(spark, "t_branch_new")
    current = _snapshot_ids(spark, table)[-1]

    spark.sql(f"ALTER TABLE {table} CREATE BRANCH IF NOT EXISTS bnew")

    assert ["bnew", "BRANCH", current] in _refs(spark, table)


def test_create_tag_if_not_exists_missing_creates_it(spark: Any) -> None:
    """The TAG half of the same conditional — a tag-only no-op would pass the cells."""
    table = _seeded_twice(spark, "t_tag_new")
    current = _snapshot_ids(spark, table)[-1]

    spark.sql(f"ALTER TABLE {table} CREATE TAG IF NOT EXISTS tnew")

    assert ["tnew", "TAG", current] in _refs(spark, table)


def test_create_tag_if_not_exists_missing_honours_as_of_version(spark: Any) -> None:
    """The version is ignored only when the ref already exists; a new tag honours it."""
    table = _seeded_twice(spark, "t_tag_new_version")
    first = _snapshot_ids(spark, table)[0]

    spark.sql(f"ALTER TABLE {table} CREATE TAG IF NOT EXISTS tver AS OF VERSION {first}")

    assert ["tver", "TAG", first] in _refs(spark, table)


def test_drop_branch_if_exists_missing_is_noop(spark: Any) -> None:
    """Cell ``D-REF-DROP-BRANCH-IF-EXISTS``: no error, refs unchanged."""
    table = _seeded_twice(spark, "t_drop_branch_missing")
    before = _refs(spark, table)

    spark.sql(f"ALTER TABLE {table} DROP BRANCH IF EXISTS nope")

    assert _refs(spark, table) == before


def test_drop_tag_if_exists_missing_is_noop(spark: Any) -> None:
    """Cell ``D-REF-DROP-TAG-IF-EXISTS``: no error, refs unchanged."""
    table = _seeded_twice(spark, "t_drop_tag_missing")
    before = _refs(spark, table)

    spark.sql(f"ALTER TABLE {table} DROP TAG IF EXISTS nope")

    assert _refs(spark, table) == before


def test_drop_branch_if_exists_present_drops_it(spark: Any) -> None:
    """Probe ``E.drop_branch_if_exists_present``: the guard still drops what is there."""
    table = _seeded_twice(spark, "t_drop_branch_present")
    spark.sql(f"ALTER TABLE {table} CREATE BRANCH b1")
    assert "b1" in _ref_names(spark, table)

    spark.sql(f"ALTER TABLE {table} DROP BRANCH IF EXISTS b1")

    assert _ref_names(spark, table) == ["main"]


def test_drop_tag_if_exists_present_drops_it(spark: Any) -> None:
    """The TAG half of the same conditional."""
    table = _seeded_twice(spark, "t_drop_tag_present")
    spark.sql(f"ALTER TABLE {table} CREATE TAG t1")
    assert "t1" in _ref_names(spark, table)

    spark.sql(f"ALTER TABLE {table} DROP TAG IF EXISTS t1")

    assert _ref_names(spark, table) == ["main"]


def test_create_branch_existing_without_guard_raises(spark: Any) -> None:
    """Probe ``E.create_branch_existing_no_if``: the unguarded form still refuses."""
    table = _seeded_twice(spark, "t_branch_dup")
    spark.sql(f"ALTER TABLE {table} CREATE BRANCH b1")

    with pytest.raises(Exception) as caught:
        spark.sql(f"ALTER TABLE {table} CREATE BRANCH b1")

    assert "already exists" in str(caught.value)


def test_drop_branch_missing_without_guard_raises(spark: Any) -> None:
    """Probe ``E.drop_branch_missing_no_if``: the unguarded drop still refuses."""
    table = _seeded_twice(spark, "t_branch_drop_loud")

    with pytest.raises(Exception) as caught:
        spark.sql(f"ALTER TABLE {table} DROP BRANCH nope")

    assert "does not exist" in str(caught.value)


def test_unknown_trailing_clause_still_refuses(spark: Any) -> None:
    """Narrowing the refusal must not delete it: an unknown trailing word still refuses."""
    table = _seeded_twice(spark, "t_trailing")
    first = _snapshot_ids(spark, table)[0]

    with pytest.raises(Exception) as caught:
        spark.sql(
            f"ALTER TABLE {table} CREATE TAG t9 AS OF VERSION {first} RETAIN 7 DAYS EXTRA"
        )

    message = str(caught.value)
    assert "trailing clause after the supported form" in message
    assert '(got word "EXTRA")' in message
    assert _ref_names(spark, table) == ["main"]


def test_or_replace_with_if_not_exists_refuses(spark: Any) -> None:
    """Probe ``E.create_or_replace_branch_if_not_exists`` / ``E.replace_branch_if_not_exists``.

    Spark's own parser rejects the combination (``mismatched input 'NOT'``), so
    ``IF NOT EXISTS`` attaches to the plain ``CREATE BRANCH|TAG`` form alone. RePark refuses
    with a parse-class error rather than accepting it and quietly picking one meaning.
    """
    table = _seeded_twice(spark, "t_or_replace_guard")
    spark.sql(f"ALTER TABLE {table} CREATE BRANCH b1")
    before = _refs(spark, table)

    for sql in (
        f"ALTER TABLE {table} CREATE OR REPLACE BRANCH IF NOT EXISTS b1",
        f"ALTER TABLE {table} REPLACE BRANCH IF NOT EXISTS b1",
        f"ALTER TABLE {table} CREATE OR REPLACE TAG IF NOT EXISTS t1",
    ):
        with pytest.raises(ParseException) as caught:
            spark.sql(sql)
        assert "IF NOT EXISTS" in str(caught.value)

    assert _refs(spark, table) == before


def test_top_level_in_forms_accept_the_guard(spark: Any) -> None:
    """The ``… IN catalog.namespace.table`` spellings take the same infix guard."""
    table = _seeded_twice(spark, "t_top_level")
    current = _snapshot_ids(spark, table)[-1]

    spark.sql(f"CREATE BRANCH IF NOT EXISTS b2 IN {table}")
    assert ["b2", "BRANCH", current] in _refs(spark, table)

    spark.sql(f"DROP TAG IF EXISTS t2 IN {table}")
    assert _ref_names(spark, table) == ["b2", "main"]

    spark.sql(f"DROP BRANCH IF EXISTS b2 IN {table}")
    assert _ref_names(spark, table) == ["main"]


def test_replace_table_column_list(spark: Any) -> None:
    """Cell ``D-REPLACE``: the column-def form leaves no ref, no new snapshot and no rows."""
    table = _seeded_once(spark, "t_replace")

    spark.sql(f"REPLACE TABLE {table} (k INT, v STRING) USING iceberg")

    frame = spark.sql(f"SELECT * FROM {table}")
    assert _schema(frame) == [("k", "int"), ("v", "string")]
    assert frame.collect() == []
    assert _refs(spark, table) == []
    assert _operations(spark, table) == ["append"]


def test_replace_table_as_select(spark: Any) -> None:
    """Cell ``D-RTAS``: the second snapshot is an ``overwrite``, never a second ``append``."""
    table = _seeded_once(spark, "t_rtas")

    spark.sql(f"REPLACE TABLE {table} USING iceberg AS SELECT id * 10 AS id FROM range(2)")

    frame = spark.sql(f"SELECT * FROM {table}")
    assert _schema(frame) == [("id", "bigint")]
    assert _operations(spark, table) == ["append", "overwrite"]
    current = _snapshot_ids(spark, table)[-1]
    assert _refs(spark, table) == [["main", "BRANCH", current]]
    assert _rows(spark, f"SELECT * FROM {table}") == [[0], [10]]


def test_replace_table_preserves_time_travel(spark: Any) -> None:
    """Cell ``D-RTAS-TIME-TRAVEL``: the pre-replace snapshot is still readable."""
    table = _seeded_once(spark, "t_rtas_tt")
    first = _snapshot_ids(spark, table)[0]

    spark.sql(f"REPLACE TABLE {table} USING iceberg AS SELECT 99 AS z")

    assert _rows(spark, f"SELECT * FROM {table} VERSION AS OF {first}") == SEED_DATA


def test_replace_table_missing_raises_table_not_found(spark: Any) -> None:
    """Probe ``E.replace_missing`` / ``E.replace_missing_rtas``: condition AND SQLSTATE.

    ``REPLACE TABLE`` is the one spelling that requires the table to already exist; a bare
    token rewrite to ``CREATE OR REPLACE TABLE`` would silently create it instead.
    """
    missing = f"{CATALOG}.{NAMESPACE}.nope"
    missing_rtas = f"{CATALOG}.{NAMESPACE}.nope_rtas"

    for sql, name in (
        (f"REPLACE TABLE {missing} (k INT, v STRING) USING iceberg", missing),
        (f"REPLACE TABLE {missing_rtas} USING iceberg AS SELECT 1 AS i", missing_rtas),
    ):
        with pytest.raises(AnalysisException) as caught:
            spark.sql(sql)
        message = str(caught.value)
        assert "[TABLE_OR_VIEW_NOT_FOUND]" in message
        assert "SQLSTATE: 42P01" in message
        assert not spark.catalog.tableExists(name)


def test_create_or_replace_table_unchanged(spark: Any) -> None:
    """Regression: both ``CREATE OR REPLACE`` paths keep their recorded snapshot stamps."""
    column_def = _seeded_once(spark, "t_cor_columns")
    spark.sql(f"CREATE OR REPLACE TABLE {column_def} (k INT, v STRING) USING iceberg")
    assert _operations(spark, column_def) == ["append"]
    assert _refs(spark, column_def) == []
    assert spark.sql(f"SELECT * FROM {column_def}").collect() == []

    rtas = _seeded_once(spark, "t_cor_rtas")
    spark.sql(f"CREATE OR REPLACE TABLE {rtas} USING iceberg AS SELECT id * 10 AS id FROM range(2)")
    assert _operations(spark, rtas) == ["append", "overwrite"]
    assert _rows(spark, f"SELECT * FROM {rtas}") == [[0], [10]]

    fresh = f"{CATALOG}.{NAMESPACE}.t_cor_fresh"
    spark.sql(f"CREATE OR REPLACE TABLE {fresh} (k INT) USING iceberg")
    assert spark.catalog.tableExists(fresh)


def test_drop_expander_emits_purge(spark: Any) -> None:
    """The Python ``DROP`` expander must qualify the name and keep ``PURGE`` byte for byte."""
    qualified = f"`{CATALOG}`.`{NAMESPACE}`.`t_x`"
    cases = {
        f"DROP TABLE {CATALOG}.{NAMESPACE}.t_x PURGE": f"DROP TABLE {qualified} PURGE",
        f"DROP TABLE IF EXISTS {CATALOG}.{NAMESPACE}.t_x PURGE": (
            f"DROP TABLE IF EXISTS {qualified} PURGE"
        ),
        f"DROP TABLE {CATALOG}.{NAMESPACE}.t_x": f"DROP TABLE {qualified}",
        f"DROP TABLE IF EXISTS {CATALOG}.{NAMESPACE}.t_x": f"DROP TABLE IF EXISTS {qualified}",
    }
    for sql, expected in cases.items():
        assert spark._expand_bare_table_names_in_sql(sql) == expected


def test_drop_table_purge_deletes_data_files(spark: Any) -> None:
    """Cell ``D-DROP-TABLE-PURGE``: the recorded data files are gone afterwards."""
    table = _seeded_once(spark, "t_purge")
    paths = _data_paths(spark, table)
    assert paths and all(_exist(paths))

    spark.sql(f"DROP TABLE {table} PURGE")

    assert _exist(paths) == [False] * len(paths)
    assert not spark.catalog.tableExists(table)


def test_drop_table_if_exists_purge_present_deletes_data_files(spark: Any) -> None:
    """``PURGE`` composes with ``IF EXISTS`` on a table that is there."""
    table = _seeded_once(spark, "t_purge_if_exists")
    paths = _data_paths(spark, table)
    assert paths and all(_exist(paths))

    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")

    assert _exist(paths) == [False] * len(paths)
    assert not spark.catalog.tableExists(table)


def test_drop_table_if_exists_purge_missing_is_ok(spark: Any) -> None:
    """Probe ``E.drop_table_if_exists_purge``: a missing table with the guard is a no-op."""
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{NAMESPACE}.t_never PURGE")


def test_drop_table_purge_missing_raises_table_not_found(spark: Any) -> None:
    """Probe ``E.drop_table_purge_missing``: ``PURGE`` does not change the missing contract."""
    with pytest.raises(AnalysisException) as caught:
        spark.sql(f"DROP TABLE {CATALOG}.{NAMESPACE}.t_never PURGE")

    message = str(caught.value)
    assert "[TABLE_OR_VIEW_NOT_FOUND]" in message
    assert "SQLSTATE: 42P01" in message


def test_drop_table_without_purge_keeps_data_files(spark: Any) -> None:
    """Cell ``D-DROP-TABLE-NO-PURGE``: plain ``DROP TABLE`` keeps every data file.

    This is the regression pin that matters most — a purge-by-default bug destroys user data
    and nothing else in the suite would catch it.
    """
    table = _seeded_once(spark, "t_no_purge")
    paths = _data_paths(spark, table)
    assert paths and all(_exist(paths))

    spark.sql(f"DROP TABLE {table}")

    assert _exist(paths) == [True] * len(paths)
    assert not spark.catalog.tableExists(table)


def test_drop_table_if_exists_without_purge_keeps_data_files(spark: Any) -> None:
    """``IF EXISTS`` alone never implies ``PURGE``."""
    table = _seeded_once(spark, "t_no_purge_if_exists")
    paths = _data_paths(spark, table)

    spark.sql(f"DROP TABLE IF EXISTS {table}")

    assert _exist(paths) == [True] * len(paths)
    assert not spark.catalog.tableExists(table)


def test_drop_table_purge_gc_disabled_refuses(spark: Any) -> None:
    """Cell ``TP-GC-DISABLED-PURGE``: Spark refuses the purge, so RePark must too.

    Spark raises ``org.apache.iceberg.exceptions.ValidationException`` with this text; RePark
    raises ``AnalysisException`` with the same text (the class gap is IPI-51). Shipping
    without the guard would delete the user's data files where Spark keeps them.
    """
    table = _seeded_once(spark, "t_gc_disabled", properties="'gc.enabled'='false'")
    paths = _data_paths(spark, table)
    assert paths and all(_exist(paths))

    with pytest.raises(AnalysisException) as caught:
        spark.sql(f"DROP TABLE {table} PURGE")

    assert GC_DISABLED_REFUSAL in str(caught.value)
    assert _exist(paths) == [True] * len(paths)
    assert spark.catalog.tableExists(table)


def test_drop_table_purge_gc_enabled_true_still_purges(spark: Any) -> None:
    """The guard reads the property, it does not refuse every table that sets it."""
    table = _seeded_once(spark, "t_gc_enabled", properties="'gc.enabled'='true'")
    paths = _data_paths(spark, table)

    spark.sql(f"DROP TABLE {table} PURGE")

    assert _exist(paths) == [False] * len(paths)

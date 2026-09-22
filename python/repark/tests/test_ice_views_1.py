"""IPI-40 views PR1 battery — the catalog door and the read path.

``CREATE [OR REPLACE] VIEW``, ``DROP VIEW``, ``SHOW VIEWS`` and ``SELECT`` from a
view on the memory catalog, with Spark's error contract. Every test runs offline
and under ``REPARK_PARITY_LIVE=1``; the pinned values below are the packet's
measured Spark answers (M-1, M-2, M-7, M-8, M-9, M-10).

pins: ice-views-1/C-004, C-006, C-007, C-008, C-009, C-011, C-012, C-013, C-014, C-015, C-016
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException


@pytest.fixture()
def spark(tmp_path: Path) -> ReparkSession:
    """Memory catalog ``sc`` with namespace ``ns`` and table ``t(id, data)``."""
    session = ReparkSession.builder.appName("pytest-ice-views-1").getOrCreate()
    session.register_memory_catalog("sc", tmp_path)
    session.sql("CREATE NAMESPACE sc.ns")
    session.sql(
        "CREATE TABLE sc.ns.t AS SELECT * FROM "
        "(VALUES (1, 'd1'), (2, 'd2'), (0, 'd0')) AS t(id, data)"
    )
    return session


def _rows(frame: Any) -> list[list[Any]]:
    """Collect a frame to plain nested lists for golden comparison."""
    return [list(row) for row in frame.collect()]


def test_create_view_and_select(spark: ReparkSession) -> None:
    """D-VIEW-CREATE — a created view reads its body rows. pins: ice-views-1/C-016."""
    spark.sql("CREATE VIEW sc.ns.vw AS SELECT id, data FROM sc.ns.t WHERE id > 0")
    assert _rows(spark.sql("SELECT * FROM sc.ns.vw ORDER BY id")) == [[1, "d1"], [2, "d2"]]


def test_create_or_replace_view_second_wins(spark: ReparkSession) -> None:
    """D-VIEW-CREATE-OR-REPLACE — the second body wins. pins: ice-views-1/C-016."""
    spark.sql("CREATE VIEW sc.ns.v AS SELECT data FROM sc.ns.t WHERE id > 0")
    spark.sql("CREATE OR REPLACE VIEW sc.ns.v AS SELECT data FROM sc.ns.t")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v ORDER BY data")) == [
        ["d0"],
        ["d1"],
        ["d2"],
    ]


def test_view_version_log_after_replace(spark: ReparkSession, tmp_path: Path) -> None:
    """D-5/A-14 — replace appends a version and moves current (metadata JSON)."""
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM sc.ns.t")
    spark.sql("CREATE OR REPLACE VIEW sc.ns.v AS SELECT data FROM sc.ns.t")
    files = sorted((tmp_path / "ns" / "v" / "metadata").glob("*.json"))
    assert files, "pins: ice-views-1/C-009"
    meta = json.loads(Path(files[-1]).read_text(encoding="utf-8"))
    versions = meta["versions"]
    assert len(versions) == 2, "pins: ice-views-1/C-009"
    by_id = {version["version-id"]: version for version in versions}
    current = meta["current-version-id"]
    assert current == max(by_id), "pins: ice-views-1/C-009"
    assert by_id[current]["representations"][0]["sql"] == "SELECT data FROM sc.ns.t", (
        "pins: ice-views-1/C-009"
    )
    log = meta["version-log"]
    assert log[-1]["version-id"] == current, "pins: ice-views-1/C-009"


def test_show_views_and_drop(spark: ReparkSession) -> None:
    """D-VIEW-SHOW-DROP — SHOW VIEWS lists, LIKE filters, DROP removes."""
    spark.sql("CREATE VIEW sc.ns.vs_abc AS SELECT id FROM sc.ns.t")
    spark.sql("CREATE VIEW sc.ns.other_v AS SELECT id FROM sc.ns.t")
    assert _rows(spark.sql("SHOW VIEWS IN sc.ns LIKE 'vs_*'")) == [["ns", "vs_abc", False]]
    spark.sql("DROP VIEW sc.ns.vs_abc")
    assert _rows(spark.sql("SHOW VIEWS IN sc.ns")) == [["ns", "other_v", False]]
    spark.sql("DROP VIEW sc.ns.other_v")
    assert _rows(spark.sql("SHOW VIEWS IN sc.ns")) == []


def test_create_view_if_not_exists_is_noop(spark: ReparkSession) -> None:
    """V-IF-NOT-EXISTS — the existing body is not replaced. pins: ice-views-1/C-004."""
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM sc.ns.t WHERE id > 0")
    spark.sql("CREATE VIEW IF NOT EXISTS sc.ns.v AS SELECT data FROM sc.ns.t")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v ORDER BY id")) == [[1], [2]]


def test_view_body_with_version_as_of_branch(spark: ReparkSession) -> None:
    """V-TIME-TRAVEL-INSIDE — a branch body reads [[1],[2]]. pins: ice-views-1/C-011."""
    spark.sql("CREATE TABLE sc.ns.n AS SELECT * FROM (VALUES (1), (2)) AS t(id)")
    spark.sql("CREATE VIEW sc.ns.vb AS SELECT id FROM sc.ns.n VERSION AS OF 'main'")
    assert _rows(spark.sql("SELECT * FROM sc.ns.vb ORDER BY id")) == [[1], [2]]


def test_view_body_with_version_as_of_snapshot_stays_pinned(
    spark: ReparkSession,
) -> None:
    """A-7 snapshot pin — pinned rows survive a later INSERT. pins: ice-views-1/C-011."""
    spark.sql("CREATE TABLE sc.ns.n AS SELECT * FROM (VALUES (1), (2)) AS t(id)")
    snapshot_id = spark.sql("SELECT snapshot_id FROM sc.ns.n.snapshots").collect()[0][0]
    spark.sql(f"CREATE VIEW sc.ns.vs AS SELECT id FROM sc.ns.n VERSION AS OF {snapshot_id}")
    spark.sql("INSERT INTO sc.ns.n VALUES (3)")
    assert _rows(spark.sql("SELECT * FROM sc.ns.n ORDER BY id")) == [[1], [2], [3]]
    assert _rows(spark.sql("SELECT * FROM sc.ns.vs ORDER BY id")) == [[1], [2]]


def test_view_over_view(spark: ReparkSession) -> None:
    """M-10 — a view over a view reads through. pins: ice-views-1/C-012."""
    spark.sql("CREATE VIEW sc.ns.v1 AS SELECT id, data FROM sc.ns.t WHERE id > 0")
    spark.sql("CREATE VIEW sc.ns.v2 AS SELECT id FROM sc.ns.v1 WHERE id > 1")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v2 ORDER BY id")) == [[2]]


def test_view_body_resolves_stored_namespace(spark: ReparkSession) -> None:
    """A-14/mutation 9 — unqualified body names use the view's namespace."""
    spark.sql("CREATE NAMESPACE sc.ns2")
    spark.sql("CREATE TABLE sc.ns2.t AS SELECT * FROM (VALUES (9)) AS t(id)")
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id, data FROM t WHERE id > 0")
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns2")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v ORDER BY id")) == [
        [1, "d1"],
        [2, "d2"],
    ], "pins: ice-views-1/C-006"


def test_nested_view_depth_guard(spark: ReparkSession) -> None:
    """D-7 — 100 nested views read; the 101st trips the typed guard on both doors."""
    spark.sql("CREATE VIEW sc.ns.w0 AS SELECT id FROM sc.ns.t")
    for level in range(1, 100):
        spark.sql(f"CREATE VIEW sc.ns.w{level} AS SELECT id FROM sc.ns.w{level - 1}")
    assert _rows(spark.sql("SELECT * FROM sc.ns.w50 ORDER BY id")) == [[0], [1], [2]]
    assert _rows(spark.sql("SELECT * FROM sc.ns.w99 ORDER BY id")) == [[0], [1], [2]]
    spark.sql("CREATE VIEW sc.ns.w100 AS SELECT id FROM sc.ns.w99")
    with pytest.raises(AnalysisException, match=r"\[VIEW_NESTED_DEPTH_LIMIT\]"):
        spark.sql("SELECT * FROM sc.ns.w100 ORDER BY id").collect()
    with pytest.raises(AnalysisException, match=r"\[VIEW_NESTED_DEPTH_LIMIT\]"):
        spark.sql("CREATE VIEW sc.ns.w101 AS SELECT id FROM sc.ns.w100")


def test_show_tables_excludes_views(spark: ReparkSession) -> None:
    """M-2/A-6 — the working table listing omits views. pins: ice-views-1/C-014."""
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM sc.ns.t")
    assert [table.name for table in spark.catalog.listTables("sc.ns")] == ["t"]


def test_view_error_contract(spark: ReparkSession) -> None:
    """M-8/D-9 — message, SQLSTATE and condition on every reachable PR1 row."""
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM sc.ns.t")
    view_not_found_sentence = (
        "The table or view `sc`.`ns`.`v` cannot be found. "
        "Verify the spelling and correctness of the schema and catalog. "
        "If you did not qualify the name with a schema, verify the current_schema() "
        "output, or qualify the name with the correct schema and catalog. "
        "To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS."
    )
    cases = [
        (
            "CREATE VIEW sc.ns.v AS SELECT id FROM sc.ns.t",
            "VIEW_ALREADY_EXISTS",
            "Cannot create view ns.v because it already exists.",
            "42P07",
        ),
        (
            "DROP VIEW sc.ns.missing",
            "VIEW_NOT_FOUND",
            "The view ns.missing cannot be found",
            "42P01",
        ),
        (
            "DROP TABLE sc.ns.v",
            "TABLE_OR_VIEW_NOT_FOUND",
            view_not_found_sentence,
            "42P01",
        ),
        (
            "DROP VIEW sc.ns.t",
            "VIEW_NOT_FOUND",
            "The view ns.t cannot be found",
            "42P01",
        ),
        (
            "INSERT INTO sc.ns.v VALUES (1)",
            "TABLE_OR_VIEW_NOT_FOUND",
            view_not_found_sentence,
            "42P01",
        ),
        (
            "DELETE FROM sc.ns.v WHERE id = 1",
            "TABLE_OR_VIEW_NOT_FOUND",
            view_not_found_sentence,
            "42P01",
        ),
        (
            "UPDATE sc.ns.v SET id = 9 WHERE id = 1",
            "TABLE_OR_VIEW_NOT_FOUND",
            view_not_found_sentence,
            "42P01",
        ),
        (
            "INSERT INTO sc.ns.v BY NAME SELECT id FROM sc.ns.t",
            "TABLE_OR_VIEW_NOT_FOUND",
            view_not_found_sentence,
            "42P01",
        ),
    ]
    for statement, condition, message, sqlstate in cases:
        with pytest.raises(AnalysisException) as caught:
            spark.sql(statement)
        text = str(caught.value)
        assert f"[{condition}]" in text, f"pins: ice-views-1/C-007: {statement}"
        assert message in text, f"pins: ice-views-1/C-007: {statement}"
        assert f"SQLSTATE: {sqlstate}" in text, f"pins: ice-views-1/C-007: {statement}"
    assert _rows(spark.sql("SELECT * FROM sc.ns.v ORDER BY id")) == [[0], [1], [2]]


def test_alter_view_as_refuses(spark: ReparkSession) -> None:
    """M-7/A-12 — Spark's exact refusal with a null condition."""
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM sc.ns.t")
    with pytest.raises(AnalysisException) as caught:
        spark.sql("ALTER VIEW sc.ns.v AS SELECT id FROM sc.ns.t")
    text = str(caught.value)
    assert (
        "ALTER VIEW <viewName> AS is not supported. Use CREATE OR REPLACE VIEW instead" in text
    ), "pins: ice-views-1/C-008"
    assert "[" not in text.split("Error during planning:")[-1], "pins: ice-views-1/C-008"


def test_drop_namespace_on_view_only_namespace_refuses(spark: ReparkSession) -> None:
    """A-15 — a view-only namespace is non-empty. pins: ice-views-1/C-013."""
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM sc.ns.t")
    spark.sql("DROP TABLE sc.ns.t")
    with pytest.raises(AnalysisException, match="not empty"):
        spark.sql("DROP NAMESPACE sc.ns")


def test_drop_view_if_exists_missing_stays_equal(spark: ReparkSession) -> None:
    """V-DROP-IF-EXISTS — missing plus IF EXISTS stays a quiet success."""
    spark.sql("DROP VIEW IF EXISTS sc.ns.missing")


def test_drop_view_one_part_round_trips_current_namespace(spark: ReparkSession) -> None:
    """A-13 symmetric — one-part DROP VIEW resolves like one-part CREATE VIEW."""
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns")
    spark.sql("CREATE VIEW dropme AS SELECT id FROM sc.ns.t")
    spark.sql("CREATE VIEW keepme AS SELECT id FROM sc.ns.t")
    assert _rows(spark.sql("SHOW VIEWS IN sc.ns LIKE 'dropme'")) == [["ns", "dropme", False]]
    spark.sql("DROP VIEW dropme")
    assert _rows(spark.sql("SHOW VIEWS IN sc.ns LIKE 'dropme'")) == []
    assert _rows(spark.sql("SHOW VIEWS IN sc.ns LIKE 'keepme'")) == [["ns", "keepme", False]]
    spark.sql("DROP VIEW IF EXISTS dropme, keepme")
    assert _rows(spark.sql("SHOW VIEWS IN sc.ns")) == []
    spark.sql("DROP VIEW IF EXISTS dropme")


def test_view_cte_earlier_sibling_shadows_catalog_table(spark: ReparkSession) -> None:
    """V-005 — a CTE body sees its earlier siblings, not the same-named table."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql(
        "CREATE VIEW sc.ns.v AS WITH a AS (SELECT 1 AS id), "
        "b AS (SELECT id FROM a) SELECT id FROM b"
    )
    assert _rows(spark.sql("SELECT * FROM sc.ns.v")) == [[1]]


def test_view_cte_forward_reference_reads_catalog_table(spark: ReparkSession) -> None:
    """V-005 — a CTE does not see a later sibling; the catalog table answers."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql(
        "CREATE VIEW sc.ns.v AS WITH b AS (SELECT id FROM a), "
        "a AS (SELECT 1 AS id) SELECT id FROM b"
    )
    assert _rows(spark.sql("SELECT * FROM sc.ns.v")) == [[7]]


def test_view_recursive_cte_self_reference_is_the_cte(spark: ReparkSession) -> None:
    """V-005 — a WITH RECURSIVE self-reference is the CTE, not the table."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql(
        "CREATE VIEW sc.ns.v AS WITH RECURSIVE a AS (SELECT CAST(1 AS BIGINT) AS id "
        "UNION ALL SELECT id + 1 FROM a WHERE id < 3) SELECT id FROM a"
    )
    assert _rows(spark.sql("SELECT * FROM sc.ns.v ORDER BY id")) == [[1], [2], [3]]


def test_view_nested_with_inside_cte_still_shadows(spark: ReparkSession) -> None:
    """V-005 — an inner WITH inside a CTE body still shadows the table name."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql(
        "CREATE VIEW sc.ns.v AS WITH b AS (WITH a AS (SELECT 5 AS id) "
        "SELECT id FROM a) SELECT id FROM b"
    )
    assert _rows(spark.sql("SELECT * FROM sc.ns.v")) == [[5]]


def test_view_cte_body_bare_name_uses_stored_namespace(spark: ReparkSession) -> None:
    """V-005 — a CTE body's bare table ref stays on the view's namespace."""
    spark.sql("CREATE NAMESPACE sc.ns2")
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql("CREATE TABLE sc.ns2.a AS SELECT * FROM (VALUES (9)) AS t(id)")
    spark.sql("CREATE VIEW sc.ns.v AS WITH b AS (SELECT id FROM a) SELECT id FROM b")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v")) == [[7]]
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns2")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v")) == [[7]]


def test_view_body_is_not_null_subquery_qualifies(spark: ReparkSession) -> None:
    """V-001 r4 — an IS NOT NULL scalar subquery qualifies its bare table."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql("CREATE NAMESPACE sc.ns2")
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM t WHERE (SELECT id FROM a) IS NOT NULL")
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns2")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v ORDER BY id")) == [[0], [1], [2]]


def test_view_body_join_on_subquery_qualifies(spark: ReparkSession) -> None:
    """V-001 r4 — a JOIN ON constraint subquery qualifies its bare table."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql("CREATE NAMESPACE sc.ns2")
    spark.sql(
        "CREATE VIEW sc.ns.v AS SELECT t.id FROM t JOIN t AS u "
        "ON u.id = t.id AND t.id < (SELECT id FROM a) - 5"
    )
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns2")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v ORDER BY id")) == [[0], [1]]


def test_view_body_function_arg_subquery_qualifies(spark: ReparkSession) -> None:
    """V-001 r4 — a subquery inside a function argument qualifies its bare table."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql("CREATE NAMESPACE sc.ns2")
    spark.sql("CREATE VIEW sc.ns.v AS SELECT abs((SELECT id FROM a)) AS x")
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns2")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v")) == [[7]]


def test_view_body_order_by_subquery_qualifies(spark: ReparkSession) -> None:
    """V-001 r4 — an ORDER BY subquery qualifies its bare table."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql("CREATE NAMESPACE sc.ns2")
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM t ORDER BY abs(id - (SELECT id FROM a))")
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns2")
    assert sorted(_rows(spark.sql("SELECT * FROM sc.ns.v"))) == [[0], [1], [2]]


def test_view_body_like_subquery_qualifies(spark: ReparkSession) -> None:
    """V-001 r4 — a LIKE pattern subquery qualifies its bare table."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql("CREATE NAMESPACE sc.ns2")
    spark.sql(
        "CREATE VIEW sc.ns.v AS SELECT id FROM t "
        "WHERE CAST(id + 5 AS STRING) LIKE (SELECT CAST(id AS STRING) FROM a)"
    )
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns2")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v")) == [[2]]


def test_view_body_depth_three_subquery_qualifies(spark: ReparkSession) -> None:
    """V-001 r4 — a third-depth subquery still qualifies its bare table."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql("CREATE NAMESPACE sc.ns2")
    spark.sql("CREATE VIEW sc.ns.v AS SELECT abs((SELECT (SELECT id FROM a))) AS x")
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns2")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v")) == [[7]]


def test_view_body_cte_shadows_in_function_arg_subquery(spark: ReparkSession) -> None:
    """V-001 r4 — a CTE still shadows the catalog table inside a subquery."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql("CREATE NAMESPACE sc.ns2")
    spark.sql(
        "CREATE VIEW sc.ns.v AS WITH a AS (SELECT 1 AS id) SELECT abs((SELECT id FROM a)) AS x"
    )
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns2")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v")) == [[1]]


def test_view_body_qualified_name_left_alone(spark: ReparkSession) -> None:
    """V-001 r4 — an already-qualified name in a subquery is left alone."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql("CREATE NAMESPACE sc.ns2")
    spark.sql("CREATE VIEW sc.ns.v AS SELECT abs((SELECT id FROM sc.ns.a)) AS x")
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns2")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v")) == [[7]]

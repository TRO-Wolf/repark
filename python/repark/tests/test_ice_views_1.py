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

import pyarrow as pa
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


def test_use_resolves_show_views_and_bare_create_drop(spark: ReparkSession) -> None:
    """USE supplies the catalog for SHOW and the namespace for CREATE and DROP."""
    spark.sql("USE sc.ns")
    spark.sql("CREATE VIEW vb AS SELECT id FROM sc.ns.t")
    frame = spark.sql("SHOW VIEWS IN ns")
    assert [(field.name, field.type) for field in frame.to_arrow().schema] == [
        ("namespace", pa.string()),
        ("viewName", pa.string()),
        ("isTemporary", pa.bool_()),
    ]
    assert _rows(frame) == [["ns", "vb", False]]
    assert _rows(spark.sql("SELECT * FROM sc.ns.vb ORDER BY id")) == [[0], [1], [2]]
    spark.sql("DROP VIEW vb")
    assert _rows(spark.sql("SHOW VIEWS IN ns")) == []


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


def test_view_body_in_subquery_qualifies(spark: ReparkSession) -> None:
    """V-006 — an IN subquery qualifies its bare table."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql("CREATE NAMESPACE sc.ns2")
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM t WHERE id IN (SELECT id - 5 FROM a)")
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns2")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v ORDER BY id")) == [[2]]


def test_view_body_not_in_subquery_qualifies(spark: ReparkSession) -> None:
    """V-006 — a NOT IN subquery qualifies its bare table."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql("CREATE NAMESPACE sc.ns2")
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM t WHERE id NOT IN (SELECT id - 5 FROM a)")
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns2")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v ORDER BY id")) == [[0], [1]]


def test_view_body_exists_subquery_qualifies(spark: ReparkSession) -> None:
    """V-006 — an EXISTS subquery qualifies its bare table."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql("CREATE NAMESPACE sc.ns2")
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM t WHERE EXISTS (SELECT 1 FROM a WHERE id = 7)")
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns2")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v ORDER BY id")) == [[0], [1], [2]]


def test_view_body_case_when_subquery_qualifies(spark: ReparkSession) -> None:
    """V-006 — a CASE WHEN condition subquery qualifies its bare table."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql("CREATE NAMESPACE sc.ns2")
    spark.sql(
        "CREATE VIEW sc.ns.v AS SELECT id FROM t WHERE CASE WHEN (SELECT id FROM a) = 7 "
        "THEN id = 0 ELSE false END"
    )
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns2")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v ORDER BY id")) == [[0]]


def test_view_body_having_subquery_qualifies(spark: ReparkSession) -> None:
    """V-006 — a HAVING subquery qualifies its bare table."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql("CREATE NAMESPACE sc.ns2")
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM t GROUP BY id HAVING (SELECT id FROM a) = 7")
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns2")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v ORDER BY id")) == [[0], [1], [2]]


def test_view_body_derived_table_qualifies(spark: ReparkSession) -> None:
    """V-006 — a derived table qualifies its bare table."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql("CREATE NAMESPACE sc.ns2")
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM (SELECT id FROM a) AS d")
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns2")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v ORDER BY id")) == [[7]]


def test_view_body_union_arm_qualifies(spark: ReparkSession) -> None:
    """V-006 — a UNION ALL arm qualifies its bare table."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql("CREATE NAMESPACE sc.ns2")
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM t WHERE id = 0 UNION ALL SELECT id FROM a")
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns2")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v ORDER BY id")) == [[0], [7]]


def test_view_body_bare_scalar_subquery_qualifies(spark: ReparkSession) -> None:
    """V-006 — a bare scalar subquery qualifies its bare table."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql("CREATE NAMESPACE sc.ns2")
    spark.sql("CREATE VIEW sc.ns.v AS SELECT (SELECT id FROM a) AS x")
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns2")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v ORDER BY x")) == [[7]]


def test_view_body_cte_shadows_in_in_subquery(spark: ReparkSession) -> None:
    """V-006 — a bare name inside an IN subquery is still the CTE."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql("CREATE NAMESPACE sc.ns2")
    spark.sql(
        "CREATE VIEW sc.ns.v AS WITH a AS (SELECT 1 AS id) SELECT id FROM t "
        "WHERE id IN (SELECT id FROM a)"
    )
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns2")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v ORDER BY id")) == [[1]]


def test_view_body_cte_shadows_in_derived_table(spark: ReparkSession) -> None:
    """V-006 — a bare name inside a derived table is still the CTE."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql("CREATE NAMESPACE sc.ns2")
    spark.sql(
        "CREATE VIEW sc.ns.v AS WITH a AS (SELECT 1 AS id) SELECT id FROM (SELECT id FROM a) AS d"
    )
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns2")
    assert _rows(spark.sql("SELECT * FROM sc.ns.v ORDER BY id")) == [[1]]


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        pytest.param(
            "SELECT id FROM t WHERE (SELECT id FROM a) IS NULL",
            [],
            id="is-null",
        ),
        pytest.param(
            "SELECT id FROM t WHERE id IS DISTINCT FROM (SELECT id - 5 FROM a)",
            [[0], [1]],
            id="is-distinct-from",
        ),
        pytest.param(
            "SELECT id FROM t WHERE ((SELECT id FROM a) = 7) IS TRUE",
            [[0], [1], [2]],
            id="is-true",
        ),
        pytest.param(
            "SELECT id FROM t WHERE id BETWEEN (SELECT id - 6 FROM a) AND 2",
            [[1], [2]],
            id="between",
        ),
        pytest.param(
            "SELECT id FROM t WHERE CAST(id AS STRING) ILIKE "
            "(SELECT CAST(id - 5 AS STRING) FROM a)",
            [[2]],
            id="ilike",
        ),
        pytest.param(
            "SELECT id FROM t WHERE CAST(id AS STRING) RLIKE "
            "(SELECT CAST(id - 5 AS STRING) FROM a)",
            [[2]],
            id="rlike",
        ),
        pytest.param(
            "SELECT CAST((SELECT id FROM a) AS INT) AS x",
            [[7]],
            id="cast",
        ),
        pytest.param(
            "SELECT -(SELECT id FROM a) AS x",
            [[-7]],
            id="negation",
        ),
        pytest.param(
            "SELECT id FROM t WHERE NOT ((SELECT id FROM a) = 7)",
            [],
            id="not",
        ),
        pytest.param(
            "SELECT id FROM t WHERE id + (SELECT id FROM a) = 9",
            [[2]],
            id="arithmetic",
        ),
        pytest.param(
            "SELECT id FROM t WHERE id IN (0, (SELECT id - 5 FROM a))",
            [[0], [2]],
            id="in-list",
        ),
        pytest.param(
            "SELECT CASE WHEN id = 0 THEN 0 ELSE (SELECT id FROM a) END AS x FROM t",
            [[0], [7], [7]],
            id="case-else",
        ),
        pytest.param(
            "SELECT CASE (SELECT id FROM a) WHEN 7 THEN id END AS x FROM t",
            [[0], [1], [2]],
            id="case-operand",
        ),
        pytest.param(
            "SELECT coalesce((SELECT id FROM a), 0) AS x",
            [[7]],
            id="coalesce",
        ),
        pytest.param(
            "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM a WHERE id = 7)",
            [],
            id="not-exists",
        ),
        pytest.param(
            "SELECT id + 7 FROM t WHERE id = 0 INTERSECT SELECT id FROM a",
            [[7]],
            id="intersect",
        ),
        pytest.param(
            "SELECT id FROM t EXCEPT SELECT id - 7 FROM a",
            [[1], [2]],
            id="except",
        ),
        pytest.param(
            "WITH a AS (SELECT 1 AS id) SELECT id FROM t "
            "WHERE EXISTS (SELECT 1 FROM a WHERE id = 1)",
            [[0], [1], [2]],
            id="cte-shadows-in-exists",
        ),
        pytest.param(
            "WITH a AS (SELECT 1 AS id) SELECT id FROM t WHERE id = 0 UNION ALL SELECT id FROM a",
            [[0], [1]],
            id="cte-shadows-in-union-arm",
        ),
        pytest.param(
            "WITH c AS (SELECT (SELECT id FROM a) AS id) SELECT id FROM c",
            [[7]],
            id="subquery-inside-cte-body",
        ),
        pytest.param(
            "SELECT id FROM t WHERE id IS NOT DISTINCT FROM (SELECT id - 5 FROM a)",
            [[2]],
            id="not-distinct-from",
        ),
        pytest.param(
            "SELECT id FROM t WHERE ((SELECT id FROM a) = 8) IS FALSE",
            [[0], [1], [2]],
            id="is-false",
        ),
        pytest.param(
            "SELECT id FROM t WHERE ((SELECT id FROM a) = 7) IS NOT TRUE",
            [],
            id="is-not-true",
        ),
        pytest.param(
            "SELECT id FROM t WHERE ((SELECT id FROM a) = 7) IS NOT FALSE",
            [[0], [1], [2]],
            id="is-not-false",
        ),
        pytest.param(
            "SELECT id FROM t WHERE ((SELECT id FROM a) = 7) IS UNKNOWN",
            [],
            id="is-unknown",
        ),
        pytest.param(
            "SELECT id FROM t WHERE ((SELECT id FROM a) = 7) IS NOT UNKNOWN",
            [[0], [1], [2]],
            id="is-not-unknown",
        ),
        pytest.param(
            "SELECT ceil((SELECT id FROM a)) AS x",
            [[7]],
            id="ceil",
        ),
        pytest.param(
            "SELECT floor((SELECT id FROM a)) AS x",
            [[7]],
            id="floor",
        ),
        pytest.param(
            "SELECT substring((SELECT cast(id AS string) FROM a), 1, 1) AS x",
            [["7"]],
            id="substring",
        ),
        pytest.param(
            "SELECT position((SELECT cast(id AS string) FROM a) IN 'x7y') AS x",
            [[2]],
            id="position",
        ),
        pytest.param(
            "SELECT overlay((SELECT cast(id AS string) FROM a) PLACING 'a' FROM 1 FOR 1) AS x",
            [["a"]],
            id="overlay",
        ),
        pytest.param(
            "SELECT trim((SELECT cast(id AS string) FROM a)) AS x",
            [["7"]],
            id="trim",
        ),
        pytest.param(
            "SELECT extract(YEAR FROM (SELECT date '2020-01-01' FROM a)) AS x",
            [[2020]],
            id="extract",
        ),
        pytest.param(
            "SELECT try_cast((SELECT id FROM a) AS INT) AS x",
            [[7]],
            id="try-cast",
        ),
        pytest.param(
            "SELECT (SELECT id FROM a)::INT AS x",
            [[7]],
            id="double-colon-cast",
        ),
        pytest.param(
            "SELECT t.id, l.y FROM t, LATERAL (SELECT id + t.id AS y FROM a) l",
            [[0, 7], [1, 8], [2, 9]],
            id="lateral",
        ),
        pytest.param(
            "SELECT t.id FROM (t JOIN a ON t.id + 7 = a.id)",
            [[0]],
            id="nested-join",
        ),
        pytest.param(
            "SELECT t.id FROM t, a WHERE t.id + 7 = a.id",
            [[0]],
            id="comma-join",
        ),
        pytest.param(
            "SELECT t.id FROM t CROSS JOIN a",
            [[0], [1], [2]],
            id="cross-join",
        ),
        pytest.param(
            "SELECT id FROM t LEFT SEMI JOIN a ON t.id + 7 = a.id",
            [[0]],
            id="left-semi-join",
        ),
        pytest.param(
            "SELECT id FROM t LEFT ANTI JOIN a ON t.id + 7 = a.id",
            [[1], [2]],
            id="left-anti-join",
        ),
        pytest.param(
            "SELECT t.id FROM t JOIN t u ON t.id = u.id AND u.id < (SELECT id - 6 FROM a)",
            [[0]],
            id="subquery-in-join-on",
        ),
        pytest.param(
            "SELECT if(id = 0, (SELECT id FROM a), id) AS x FROM t",
            [[1], [2], [7]],
            id="if-argument",
        ),
        pytest.param(
            "SELECT array((SELECT id FROM a), 1) AS x",
            [[[7, 1]]],
            id="array-argument",
        ),
        pytest.param(
            "SELECT max(id) + (SELECT id FROM a) AS x FROM t",
            [[9]],
            id="aggregate-operand",
        ),
        pytest.param(
            "SELECT struct((SELECT id FROM a) AS f) AS x",
            [[{"f": 7}]],
            id="named-struct-field",
        ),
        pytest.param(
            "SELECT array(10, 20, 30)[(SELECT id - 6 FROM a)] AS x",
            [[20]],
            id="subscript-index",
        ),
    ],
)
def test_view_body_expression_position_qualifies(
    spark: ReparkSession, body: str, expected: list[list[Any]]
) -> None:
    """V-007 — a subquery inside each pinned expression position qualifies its table."""
    spark.sql("CREATE TABLE sc.ns.a AS SELECT * FROM (VALUES (7)) AS t(id)")
    spark.sql("CREATE NAMESPACE sc.ns2")
    spark.sql(f"CREATE VIEW sc.ns.v AS {body}")
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns2")
    assert sorted(_rows(spark.sql("SELECT * FROM sc.ns.v"))) == expected

"""IPI-40 views PR4 battery — SHOW TBLPROPERTIES on an Iceberg view.

``SHOW TBLPROPERTIES <view> [(key)]`` answers from the view's stored metadata:
the reserved rows ``location``/``provider``/``format-version`` plus the stored
properties, or a single keyed row (a case-sensitive miss answers Spark's
``does not have property`` sentence instead of raising). A name that is a
table or cannot resolve falls through to the upstream planning refusal, and a
name that is neither table nor view answers the PR2 TABLE_OR_VIEW_NOT_FOUND.
Measured against Spark 4.1.2 + Iceberg 1.11.0
(/tmp/oc-worker/run27/ticks-xo-opus3/064/MEASURED-showprops.md); the no-key row
order is Java map iteration order, so every no-key pin compares sorted.

pins: ice-views-1/C-017
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException

SHOW_VARIABLE_UNSUPPORTED = "SHOW [VARIABLE] is not supported unless information_schema is enabled"
SHOW_NAME_PARSE_ERROR = (
    "could not parse CREATE NAMESPACE: sql parser error: Expected: identifier, found: EOF"
)
NO_CONDITION = None


def _table_or_view_not_found(name: str) -> str:
    """The PR2-style full refusal for a name that is neither table nor view."""
    return (
        f"[TABLE_OR_VIEW_NOT_FOUND] The table or view `sc`.`ns`.`{name}` "
        "cannot be found. Verify the spelling and correctness of the schema and "
        "catalog. If you did not qualify the name with a schema, verify the "
        "current_schema() output, or qualify the name with the correct schema and "
        "catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP "
        "TABLE IF EXISTS. SQLSTATE: 42P01"
    )


@pytest.fixture()
def spark(tmp_path: Path) -> ReparkSession:
    """Memory catalog ``sc`` with namespace ``ns`` and table ``t(id, data)``."""
    session = ReparkSession.builder.appName("pytest-ice-views-4-showprops").getOrCreate()
    session.register_memory_catalog("sc", tmp_path)
    session.sql("CREATE NAMESPACE sc.ns")
    session.sql("CREATE TABLE sc.ns.t (id BIGINT, data STRING) USING iceberg")
    session.sql("INSERT INTO sc.ns.t VALUES (1, 'a'), (2, 'b')")
    return session


def _rows(frame: Any) -> list[list[Any]]:
    """Collect a frame to plain nested lists for golden comparison."""
    return [list(row) for row in frame.collect()]


def test_show_tblproperties_key_cell(spark: ReparkSession) -> None:
    """V-SHOW-TBLPROPERTIES — the measured cell: one row, two string columns."""
    spark.sql("CREATE VIEW sc.ns.v TBLPROPERTIES ('k'='v') AS SELECT id FROM sc.ns.t")
    table = spark.sql("SHOW TBLPROPERTIES sc.ns.v ('k')").to_arrow()
    assert table.schema.names == ["key", "value"]
    assert [field.type for field in table.schema] == [pa.string()] * 2
    assert [field.nullable for field in table.schema] == [False, False]
    assert _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.v ('k')")) == [["k", "v"]]


def test_no_key_lists_reserved_then_stored_sorted(spark: ReparkSession, tmp_path: Path) -> None:
    """E1 — the reserved three plus the stored properties, compared sorted."""
    spark.sql("CREATE VIEW sc.ns.v TBLPROPERTIES ('k'='v', 'a'='b') AS SELECT id FROM sc.ns.t")
    rows = sorted(_rows(spark.sql("SHOW TBLPROPERTIES sc.ns.v")))
    assert rows == [
        ["a", "b"],
        ["format-version", "1"],
        ["k", "v"],
        ["location", str(tmp_path / "ns" / "v")],
        ["provider", "iceberg"],
    ]


def test_no_key_without_stored_properties_is_reserved_only(
    spark: ReparkSession,
    tmp_path: Path,
) -> None:
    """E2 — a property-less view answers exactly the three reserved rows."""
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM sc.ns.t")
    rows = sorted(_rows(spark.sql("SHOW TBLPROPERTIES sc.ns.v")))
    assert rows == [
        ["format-version", "1"],
        ["location", str(tmp_path / "ns" / "v")],
        ["provider", "iceberg"],
    ]


def test_missing_key_answers_the_spark_sentence(spark: ReparkSession) -> None:
    """E3 — an absent key is a row, not an error."""
    spark.sql("CREATE VIEW sc.ns.v TBLPROPERTIES ('k'='v') AS SELECT id FROM sc.ns.t")
    assert _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.v ('nope')")) == [
        ["nope", "View sc.ns.v does not have property: nope"]
    ]


def test_key_lookup_is_case_sensitive(spark: ReparkSession) -> None:
    """E4 — 'K' does not hit stored 'k'."""
    spark.sql("CREATE VIEW sc.ns.v TBLPROPERTIES ('k'='v') AS SELECT id FROM sc.ns.t")
    assert _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.v ('K')")) == [
        ["K", "View sc.ns.v does not have property: K"]
    ]


def test_reserved_keys_answer_the_reserved_values(spark: ReparkSession) -> None:
    """E5 — 'provider' and 'format-version' hit the reserved rows."""
    spark.sql("CREATE VIEW sc.ns.v TBLPROPERTIES ('k'='v') AS SELECT id FROM sc.ns.t")
    assert _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.v ('provider')")) == [["provider", "iceberg"]]
    assert _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.v ('format-version')")) == [
        ["format-version", "1"]
    ]


def test_unquoted_and_dotted_keys(spark: ReparkSession) -> None:
    """E6 — (k) and (a.b) spellings reach the same stored keys."""
    spark.sql("CREATE VIEW sc.ns.v TBLPROPERTIES ('k'='v', 'a.b'='c') AS SELECT id FROM sc.ns.t")
    assert _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.v (k)")) == [["k", "v"]]
    assert _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.v (a.b)")) == [["a.b", "c"]]


def test_missing_view_is_table_or_view_not_found(spark: ReparkSession) -> None:
    """E7 — a missing name answers the full PR2 refusal, with or without a key."""
    for tail in ("", " ('k')"):
        with pytest.raises(AnalysisException) as caught:
            spark.sql(f"SHOW TBLPROPERTIES sc.ns.definitely_absent{tail}")
        expected = f"Error during planning: {_table_or_view_not_found('definitely_absent')}"
        assert str(caught.value) == expected
        assert caught.value.getCondition() == "TABLE_OR_VIEW_NOT_FOUND"
        assert caught.value.getSqlState() == "42P01"


def test_alter_view_set_then_show_reflects_updates(spark: ReparkSession, tmp_path: Path) -> None:
    """E8 — ALTER VIEW SET TBLPROPERTIES updates what SHOW answers."""
    spark.sql("CREATE VIEW sc.ns.v TBLPROPERTIES ('k'='v') AS SELECT id FROM sc.ns.t")
    spark.sql("ALTER VIEW sc.ns.v SET TBLPROPERTIES ('k'='v2', 'j'='u')")
    assert _rows(spark.sql("SHOW TBLPROPERTIES sc.ns.v ('k')")) == [["k", "v2"]]
    rows = sorted(_rows(spark.sql("SHOW TBLPROPERTIES sc.ns.v")))
    assert rows == [
        ["format-version", "1"],
        ["j", "u"],
        ["k", "v2"],
        ["location", str(tmp_path / "ns" / "v")],
        ["provider", "iceberg"],
    ]


def test_bare_and_two_part_names_do_not_follow_use(spark: ReparkSession) -> None:
    """E9 — bare and two-part names do not follow USE.

    Spark answers ``[["k","v"]]`` for both; repark resolves short names
    through the planner defaults, which USE does not move, so the name falls
    through to the upstream SHOW refusal. The residue is pinned, not fixed.
    """
    spark.sql("CREATE VIEW sc.ns.v TBLPROPERTIES ('k'='v') AS SELECT id FROM sc.ns.t")
    spark.sql("CREATE NAMESPACE sc.other")
    spark.catalog.setCurrentCatalog("sc")
    spark.catalog.setCurrentDatabase("ns")
    with pytest.raises(AnalysisException) as caught:
        spark.sql("SHOW TBLPROPERTIES v ('k')")
    assert str(caught.value) == f"Error during planning: {SHOW_VARIABLE_UNSUPPORTED}"
    assert caught.value.getCondition() == NO_CONDITION
    assert caught.value.getSqlState() == NO_CONDITION
    spark.catalog.setCurrentDatabase("other")
    with pytest.raises(AnalysisException) as caught:
        spark.sql("SHOW TBLPROPERTIES ns.v ('k')")
    assert str(caught.value) == f"Error during planning: {SHOW_VARIABLE_UNSUPPORTED}"
    assert caught.value.getCondition() == NO_CONDITION
    assert caught.value.getSqlState() == NO_CONDITION


def test_show_tblproperties_on_a_table_falls_through(spark: ReparkSession) -> None:
    """Near-miss a — a table keeps the upstream SHOW planning refusal."""
    for tail in ("", " ('k')"):
        with pytest.raises(AnalysisException) as caught:
            spark.sql(f"SHOW TBLPROPERTIES sc.ns.t{tail}")
        assert str(caught.value) == f"Error during planning: {SHOW_VARIABLE_UNSUPPORTED}"
        assert caught.value.getCondition() == NO_CONDITION
        assert caught.value.getSqlState() == NO_CONDITION


def test_show_views_and_show_tables_unchanged(spark: ReparkSession) -> None:
    """Near-miss b — the neighboring SHOW doors answer as before."""
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM sc.ns.t")
    views = spark.sql("SHOW VIEWS IN sc.ns").to_arrow()
    assert views.schema.names == ["namespace", "viewName", "isTemporary"]
    assert _rows(spark.sql("SHOW VIEWS IN sc.ns")) == [["ns", "v", False]]
    tables = spark.sql("SHOW TABLES IN sc.ns").to_arrow()
    assert tables.schema.names == ["namespace", "tableName", "isTemporary"]
    assert _rows(spark.sql("SHOW TABLES IN sc.ns")) == [["ns", "t", False]]


def test_tblpropertiesx_is_not_recognized(spark: ReparkSession) -> None:
    """Near-miss c — the head word must be exactly TBLPROPERTIES."""
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM sc.ns.t")
    with pytest.raises(AnalysisException) as caught:
        spark.sql("SHOW TBLPROPERTIESX sc.ns.v")
    assert str(caught.value) == f"Error during planning: {SHOW_VARIABLE_UNSUPPORTED}"
    assert caught.value.getCondition() == NO_CONDITION
    assert caught.value.getSqlState() == NO_CONDITION


def test_show_table_extended_falls_through(spark: ReparkSession) -> None:
    """Near-miss d — SHOW TABLE EXTENDED retains the upstream refusal."""
    with pytest.raises(AnalysisException) as caught:
        spark.sql("SHOW TABLE EXTENDED IN sc.ns LIKE t")
    assert str(caught.value) == f"Error during planning: {SHOW_VARIABLE_UNSUPPORTED}"
    assert caught.value.getCondition() == NO_CONDITION
    assert caught.value.getSqlState() == NO_CONDITION


def test_show_tblproperties_requires_a_name(spark: ReparkSession) -> None:
    """Near-miss e — a missing name returns the parser refusal."""
    with pytest.raises(AnalysisException) as caught:
        spark.sql("SHOW TBLPROPERTIES")
    assert str(caught.value) == f"Error during planning: {SHOW_NAME_PARSE_ERROR}"
    assert caught.value.getCondition() == NO_CONDITION
    assert caught.value.getSqlState() == NO_CONDITION

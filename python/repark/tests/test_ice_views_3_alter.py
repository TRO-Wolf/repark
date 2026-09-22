"""IPI-40 views PR3 battery — ALTER VIEW on an Iceberg view.

``ALTER VIEW <name> SET TBLPROPERTIES (...)``, ``ALTER VIEW <name> UNSET
TBLPROPERTIES [IF EXISTS] (...)`` and ``ALTER VIEW <name> RENAME TO <name>``
run against the view catalog with Spark's measured answers (Spark 4.1.2 +
Iceberg 1.11.0, MEASURED-alter.md). ``ALTER VIEW ... AS`` stays refused, and
the table ALTER path is untouched.

pins: ice-views-1/C-017
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException

CATALOG_OPERATION_UNSUPPORTED = (
    "[UNSUPPORTED_FEATURE.CATALOG_OPERATION] The feature is not supported: "
    "Catalog `sc` does not support views. SQLSTATE: 0A000"
)

VIEW_ALREADY_EXISTS_VB = (
    "[VIEW_ALREADY_EXISTS] Cannot create view ns.vb because it already exists.\n"
    "Choose a different name, drop or replace the existing object, or add the "
    "IF NOT EXISTS clause to tolerate pre-existing objects. SQLSTATE: 42P07"
)


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
    session = ReparkSession.builder.appName("pytest-ice-views-3-alter").getOrCreate()
    session.register_memory_catalog("sc", tmp_path)
    session.sql("CREATE NAMESPACE sc.ns")
    session.sql("CREATE TABLE sc.ns.t (id BIGINT, data STRING) USING iceberg")
    session.sql("INSERT INTO sc.ns.t VALUES (1, 'a'), (2, 'b')")
    return session


def _rows(frame: Any) -> list[list[Any]]:
    """Collect a frame to plain nested lists for golden comparison."""
    return [list(row) for row in frame.collect()]


def _table_properties(spark: ReparkSession, table: str) -> dict[str, str]:
    """Parse the ``Table Properties`` row of ``DESCRIBE TABLE EXTENDED``."""
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").to_arrow().to_pylist()
    raw = next(row["data_type"] for row in rows if row["col_name"] == "Table Properties")
    assert raw.startswith("[") and raw.endswith("]")
    return dict(pair.split("=", 1) for pair in raw[1:-1].split(",") if "=" in pair)


def test_alter_view_unset_tblproperties_cell(spark: ReparkSession) -> None:
    """V-ALTER-UNSET — UNSET removes the stored property; the view still reads."""
    spark.sql("CREATE VIEW sc.ns.v TBLPROPERTIES ('k'='v') AS SELECT id FROM sc.ns.t")
    spark.sql("ALTER VIEW sc.ns.v UNSET TBLPROPERTIES ('k')")
    assert sorted(_rows(spark.sql("SELECT * FROM sc.ns.v"))) == [[1], [2]]


def test_alter_view_set_tblproperties_and_rename_cell(spark: ReparkSession) -> None:
    """D-VIEW-ALTER-PROPS — SET then RENAME; the new name reads the empty body."""
    spark.sql("CREATE TABLE sc.ns.te (id BIGINT, data STRING) USING iceberg")
    spark.sql("CREATE VIEW sc.ns.va AS SELECT id FROM sc.ns.te")
    spark.sql("ALTER VIEW sc.ns.va SET TBLPROPERTIES ('k'='v')")
    spark.sql("ALTER VIEW sc.ns.va RENAME TO sc.ns.vb")
    assert _rows(spark.sql("SELECT * FROM sc.ns.vb")) == []


def test_unset_missing_key_without_if_exists_refuses(spark: ReparkSession) -> None:
    """E1 — plain AnalysisException, no condition, and the view still reads."""
    spark.sql("CREATE VIEW sc.ns.v TBLPROPERTIES ('k'='v') AS SELECT id FROM sc.ns.t")
    with pytest.raises(AnalysisException) as caught:
        spark.sql("ALTER VIEW sc.ns.v UNSET TBLPROPERTIES ('nope')")
    text = str(caught.value)
    assert "Cannot remove property that is not set: 'nope'" in text
    assert "[" not in text.split("Error during planning:")[-1]
    assert sorted(_rows(spark.sql("SELECT * FROM sc.ns.v"))) == [[1], [2]]


def test_unset_missing_key_with_if_exists_noops(spark: ReparkSession) -> None:
    """E2 — UNSET IF EXISTS on an absent key is a quiet success."""
    spark.sql("CREATE VIEW sc.ns.v TBLPROPERTIES ('k'='v') AS SELECT id FROM sc.ns.t")
    spark.sql("ALTER VIEW sc.ns.v UNSET TBLPROPERTIES IF EXISTS ('nope')")
    assert sorted(_rows(spark.sql("SELECT * FROM sc.ns.v"))) == [[1], [2]]


def test_set_on_missing_view_refuses_catalog_operation(spark: ReparkSession) -> None:
    """E3 — SET on a missing view answers the measured catalog-operation refusal."""
    with pytest.raises(AnalysisException) as caught:
        spark.sql("ALTER VIEW sc.ns.missing SET TBLPROPERTIES ('k'='v')")
    assert CATALOG_OPERATION_UNSUPPORTED in str(caught.value)


def test_set_on_table_refuses_and_leaves_properties(spark: ReparkSession) -> None:
    """E4 — ALTER VIEW on a table answers the same refusal and alters nothing."""
    properties_before = _table_properties(spark, "sc.ns.t")
    with pytest.raises(AnalysisException) as caught:
        spark.sql("ALTER VIEW sc.ns.t SET TBLPROPERTIES ('k'='v')")
    assert CATALOG_OPERATION_UNSUPPORTED in str(caught.value)
    assert _table_properties(spark, "sc.ns.t") == properties_before
    assert sorted(_rows(spark.sql("SELECT * FROM sc.ns.t ORDER BY id"))) == [
        [1, "a"],
        [2, "b"],
    ]


def test_rename_to_existing_view_refuses(spark: ReparkSession) -> None:
    """E5 — collision answers VIEW_ALREADY_EXISTS; both views stay readable."""
    spark.sql("CREATE VIEW sc.ns.va AS SELECT id FROM sc.ns.t")
    spark.sql("CREATE VIEW sc.ns.vb AS SELECT data FROM sc.ns.t")
    with pytest.raises(AnalysisException) as caught:
        spark.sql("ALTER VIEW sc.ns.va RENAME TO sc.ns.vb")
    assert VIEW_ALREADY_EXISTS_VB in str(caught.value)
    assert sorted(_rows(spark.sql("SELECT * FROM sc.ns.va"))) == [[1], [2]]
    assert sorted(_rows(spark.sql("SELECT * FROM sc.ns.vb"))) == [["a"], ["b"]]


def test_rename_missing_view_is_table_or_view_not_found(spark: ReparkSession) -> None:
    """E6 — renaming a missing name is the full PR2 TABLE_OR_VIEW_NOT_FOUND."""
    with pytest.raises(AnalysisException) as caught:
        spark.sql("ALTER VIEW sc.ns.missing RENAME TO sc.ns.w")
    assert _table_or_view_not_found("missing") in str(caught.value)


def test_rename_table_with_alter_view_refuses(spark: ReparkSession) -> None:
    """E7 — a table name answers the ALTER TABLE redirect; the table survives."""
    with pytest.raises(AnalysisException) as caught:
        spark.sql("ALTER VIEW sc.ns.t RENAME TO sc.ns.w")
    assert "Cannot rename a table with ALTER VIEW. Please use ALTER TABLE instead." in str(
        caught.value
    )
    assert sorted(_rows(spark.sql("SELECT * FROM sc.ns.t ORDER BY id"))) == [
        [1, "a"],
        [2, "b"],
    ]


def test_old_name_is_gone_after_rename(spark: ReparkSession) -> None:
    """E8 — the old name is gone: DESCRIBE answers TABLE_OR_VIEW_NOT_FOUND."""
    spark.sql("CREATE VIEW sc.ns.vo AS SELECT id FROM sc.ns.t")
    spark.sql("ALTER VIEW sc.ns.vo RENAME TO sc.ns.vr")
    with pytest.raises(AnalysisException) as caught:
        spark.sql("DESCRIBE sc.ns.vo").collect()
    assert _table_or_view_not_found("vo") in str(caught.value)
    with pytest.raises(AnalysisException) as caught:
        spark.sql("SELECT * FROM sc.ns.vo").collect()
    assert "table 'sc.ns.vo' not found" in str(caught.value)
    assert sorted(_rows(spark.sql("SELECT * FROM sc.ns.vr"))) == [[1], [2]]


def test_rename_to_bare_target_reports_cross_catalog_move(
    spark: ReparkSession,
) -> None:
    """E9 — a bare target resolves under the planner default ``datafusion``.

    Spark answers ``to=spark_catalog``; repark resolves the bare target through
    ``datafusion.catalog.default_catalog``, which stays ``datafusion`` in this
    session. The residue is pinned, not fixed.
    """
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM sc.ns.t")
    with pytest.raises(AnalysisException) as caught:
        spark.sql("ALTER VIEW sc.ns.v RENAME TO w")
    assert "Cannot move view between catalogs: from=sc and to=datafusion" in str(caught.value)


def test_alter_view_as_still_refuses(spark: ReparkSession) -> None:
    """Near-miss a — the ALTER VIEW ... AS refusal is verbatim and first."""
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM sc.ns.t")
    with pytest.raises(AnalysisException) as caught:
        spark.sql("ALTER VIEW sc.ns.v AS SELECT 1")
    text = str(caught.value)
    assert "ALTER VIEW <viewName> AS is not supported. Use CREATE OR REPLACE VIEW instead" in text
    assert "[" not in text.split("Error during planning:")[-1]


def test_alter_table_set_tblproperties_unchanged(spark: ReparkSession) -> None:
    """Near-miss b — the real table ALTER path still sets properties."""
    spark.sql("ALTER TABLE sc.ns.t SET TBLPROPERTIES ('k'='v')")
    assert _table_properties(spark, "sc.ns.t").get("k") == "v"


def test_bare_alter_view_without_verb_is_not_swallowed(spark: ReparkSession) -> None:
    """Near-miss c — ALTER VIEW with no verb keeps the engine's parse refusal."""
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM sc.ns.t")
    with pytest.raises(AnalysisException):
        spark.sql("ALTER VIEW sc.ns.v")
    assert sorted(_rows(spark.sql("SELECT * FROM sc.ns.v"))) == [[1], [2]]


def test_alter_views_and_viewx_are_not_recognized(spark: ReparkSession) -> None:
    """Near-miss d — the head word must be exactly VIEW."""
    spark.sql("CREATE VIEW sc.ns.v AS SELECT id FROM sc.ns.t")
    with pytest.raises(AnalysisException):
        spark.sql("ALTER VIEWS sc.ns.v SET TBLPROPERTIES ('k'='v')")
    with pytest.raises(AnalysisException):
        spark.sql("ALTER VIEWX sc.ns.v SET TBLPROPERTIES ('k'='v')")
    assert sorted(_rows(spark.sql("SELECT * FROM sc.ns.v"))) == [[1], [2]]

"""IPI-40 views PR5 battery — SHOW CREATE TABLE on an Iceberg view.

``SHOW CREATE TABLE <view>`` answers Spark 4.1.2's single ``CREATE VIEW`` string:
the stored column names (with ``COMMENT`` for a documented column), the view
comment, the reserved plus stored properties sorted by key without ``comment``,
and the stored SQL text verbatim. Measured against Spark 4.1.2 + Iceberg 1.11.0
(``/tmp/oc-worker/qe/probe/p2.json`` keys ``C.show_create`` and
``C.v2.show_create``). Near misses keep the answers measured on main a6e8bcda.
Unmeasured residues: D-VIEW-SHOWCREATE-1.

pins: ice-views-1/C-017
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, ParseException

V2_DDL = (
    "CREATE VIEW sc.ns.v2 (i COMMENT 'the id', d) COMMENT 'view doc' "
    "TBLPROPERTIES ('k'='v') AS SELECT id, data FROM sc.ns.t"
)
INVALID_SHOW_CREATE_TABLE = (
    "[INVALID_STATEMENT_OR_CLAUSE] The statement or clause: SHOW CREATE TABLE is not valid. "
    "SQLSTATE: 42601"
)
SHOW_CREATE_UNSUPPORTED = (
    "Error during planning: SHOW CREATE TABLE is not supported unless information_schema is enabled"
)


@pytest.fixture()
def spark(tmp_path: Path) -> ReparkSession:
    """Memory catalog ``sc`` with namespace ``ns`` and an empty table ``t(id, data)``."""
    session = ReparkSession.builder.appName("pytest-ice-views-5-showcreate").getOrCreate()
    session.register_memory_catalog("sc", tmp_path)
    session.sql("CREATE NAMESPACE sc.ns")
    session.sql("CREATE TABLE sc.ns.t (id BIGINT, data STRING) USING iceberg")
    return session


def _show_create(frame: Any) -> str:
    """Pin the one-row, one-column SHOW CREATE TABLE answer and return its text."""
    table = frame.to_arrow()
    assert table.schema == pa.schema([pa.field("createtab_stmt", pa.string(), nullable=False)])
    rows = table.to_pylist()
    assert len(rows) == 1
    return rows[0]["createtab_stmt"]


def _v2_text(tmp_path: Path) -> str:
    """The measured C.v2.show_create answer, with this fixture's view location."""
    return (
        "CREATE VIEW sc.ns.v2 (\n  i COMMENT 'the id',\n  d)\nCOMMENT 'view doc'\n"
        "TBLPROPERTIES (\n  'format-version' = '1',\n  'k' = 'v',\n"
        f"  'location' = '{tmp_path / 'ns' / 'v2'}',\n  'provider' = 'iceberg')\n"
        "AS\nSELECT id, data FROM sc.ns.t\n"
    )


def _assert_error(
    error: Exception,
    kind: type[Exception],
    message: str,
    condition: str | None,
    sql_state: str | None,
) -> None:
    """Pin an error's exact class, full text, condition and SQLSTATE."""
    assert type(error) is kind
    assert str(error) == message
    assert error.getCondition() == condition
    assert error.getSqlState() == sql_state


def test_show_create_table_on_view(spark: ReparkSession, tmp_path: Path) -> None:
    """V-SHOW-CREATE — the measured v2 cell: column doc, view comment, sorted properties."""
    spark.sql(V2_DDL)
    assert _show_create(spark.sql("SHOW CREATE TABLE sc.ns.v2")) == _v2_text(tmp_path)


def test_show_create_table_on_plain_view(spark: ReparkSession, tmp_path: Path) -> None:
    """The measured v1 cell: no column docs, no COMMENT line, reserved properties only."""
    spark.sql("CREATE VIEW sc.ns.v1 AS SELECT id, data FROM sc.ns.t WHERE id > 0")
    assert _show_create(spark.sql("SHOW CREATE TABLE sc.ns.v1")) == (
        "CREATE VIEW sc.ns.v1 (\n  id,\n  data)\nTBLPROPERTIES (\n  'format-version' = '1',\n"
        f"  'location' = '{tmp_path / 'ns' / 'v1'}',\n  'provider' = 'iceberg')\n"
        "AS\nSELECT id, data FROM sc.ns.t WHERE id > 0\n"
    )


def test_bare_name_follows_use(spark: ReparkSession, tmp_path: Path) -> None:
    """P-SP-BARE-NAME precedent — ``USE sc.ns`` then a bare name answers the qualified text."""
    spark.sql(V2_DDL)
    spark.sql("USE sc.ns")
    assert _show_create(spark.sql("SHOW CREATE TABLE v2")) == _v2_text(tmp_path)


def test_two_part_name_follows_use(spark: ReparkSession, tmp_path: Path) -> None:
    """P-SP-TWO-PART precedent — ``USE sc`` then ``ns.v2`` answers the qualified text."""
    spark.sql(V2_DDL)
    spark.sql("USE sc")
    assert _show_create(spark.sql("SHOW CREATE TABLE ns.v2")) == _v2_text(tmp_path)


def test_stored_body_is_rendered_verbatim(spark: ReparkSession, tmp_path: Path) -> None:
    """The body is the stored SQL text, never the namespace-qualified read form."""
    spark.sql("USE sc.ns")
    spark.sql("CREATE VIEW sc.ns.b AS SELECT id FROM t")
    assert _show_create(spark.sql("SHOW CREATE TABLE sc.ns.b")) == (
        "CREATE VIEW sc.ns.b (\n  id)\nTBLPROPERTIES (\n  'format-version' = '1',\n"
        f"  'location' = '{tmp_path / 'ns' / 'b'}',\n  'provider' = 'iceberg')\n"
        "AS\nSELECT id FROM t\n"
    )


def test_quotes_in_column_doc_and_view_comment(spark: ReparkSession, tmp_path: Path) -> None:
    """A ``'`` in a column doc or the view comment renders backslash-escaped (declared)."""
    spark.sql(
        "CREATE VIEW sc.ns.q (i COMMENT 'it\\'s') COMMENT 'o\\'clock' AS SELECT id FROM sc.ns.t"
    )
    assert _show_create(spark.sql("SHOW CREATE TABLE sc.ns.q")) == (
        "CREATE VIEW sc.ns.q (\n  i COMMENT 'it\\'s')\nCOMMENT 'o\\'clock'\n"
        "TBLPROPERTIES (\n  'format-version' = '1',\n"
        f"  'location' = '{tmp_path / 'ns' / 'q'}',\n  'provider' = 'iceberg')\n"
        "AS\nSELECT id FROM sc.ns.t\n"
    )


def test_table_keeps_the_table_text(spark: ReparkSession, tmp_path: Path) -> None:
    """Near miss — a table still answers main's CREATE TABLE text."""
    assert _show_create(spark.sql("SHOW CREATE TABLE sc.ns.t")) == (
        "CREATE TABLE sc.ns.t (\n  id BIGINT,\n  data STRING)\nUSING iceberg\n"
        f"LOCATION '{tmp_path / 'ns' / 't'}'\n"
        "TBLPROPERTIES (\n  'current-snapshot-id' = 'none',\n  'format' = 'iceberg/parquet',\n"
        "  'format-version' = '2',\n  'write.parquet.compression-codec' = 'zstd')\n"
    )


def test_missing_name_is_table_or_view_not_found(spark: ReparkSession) -> None:
    """Near miss — a missing name keeps main's full TABLE_OR_VIEW_NOT_FOUND answer."""
    with pytest.raises(AnalysisException) as caught:
        spark.sql("SHOW CREATE TABLE sc.ns.nope")
    _assert_error(
        caught.value,
        AnalysisException,
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view `sc`.`ns`.`nope` "
        "cannot be found. Verify the spelling and correctness of the schema and catalog. If you "
        "did not qualify the name with a schema, verify the current_schema() output, or qualify "
        "the name with the correct schema and catalog. To tolerate the error on drop use DROP "
        "VIEW IF EXISTS or DROP TABLE IF EXISTS. SQLSTATE: 42P01",
        "TABLE_OR_VIEW_NOT_FOUND",
        "42P01",
    )


def test_session_temp_view_shadowing_a_bare_name_falls_through(spark: ReparkSession) -> None:
    """Near miss — a bare name that resolves to a session temp view keeps main's refusal."""
    spark.sql(V2_DDL)
    spark.createDataFrame([(1,)], ["x"]).createOrReplaceTempView("v2")
    spark.sql("USE sc.ns")
    with pytest.raises(AnalysisException) as caught:
        spark.sql("SHOW CREATE TABLE v2")
    _assert_error(caught.value, AnalysisException, SHOW_CREATE_UNSUPPORTED, None, None)


def test_view_as_serde_keeps_the_main_parse_error(spark: ReparkSession) -> None:
    """Near miss — ``AS SERDE`` on a view keeps main's answer (Spark's is unmeasured)."""
    spark.sql(V2_DDL)
    with pytest.raises(ParseException) as caught:
        spark.sql("SHOW CREATE TABLE sc.ns.v2 AS SERDE")
    _assert_error(
        caught.value,
        ParseException,
        'SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 28")',
        None,
        None,
    )


@pytest.mark.parametrize(
    "sql",
    ["SHOW CREATE TABLE", "SHOW CREATE TABLE sc.ns.v2 extra"],
    ids=["no-name", "trailing-token"],
)
def test_malformed_forms_keep_the_parse_error(spark: ReparkSession, sql: str) -> None:
    """Near miss — no name or trailing tokens keep main's INVALID_STATEMENT_OR_CLAUSE."""
    spark.sql(V2_DDL)
    with pytest.raises(ParseException) as caught:
        spark.sql(sql)
    _assert_error(
        caught.value,
        ParseException,
        INVALID_SHOW_CREATE_TABLE,
        "INVALID_STATEMENT_OR_CLAUSE",
        "42601",
    )


def test_comment_set_through_alter_view_renders(spark: ReparkSession, tmp_path: Path) -> None:
    """p5 vc.after_set.show_create — a comment set by ALTER VIEW is the COMMENT line."""
    spark.sql(V2_DDL)
    spark.sql("ALTER VIEW sc.ns.v2 SET TBLPROPERTIES ('comment'='x')")
    assert _show_create(spark.sql("SHOW CREATE TABLE sc.ns.v2")) == (
        "CREATE VIEW sc.ns.v2 (\n  i COMMENT 'the id',\n  d)\nCOMMENT 'x'\n"
        "TBLPROPERTIES (\n  'format-version' = '1',\n  'k' = 'v',\n"
        f"  'location' = '{tmp_path / 'ns' / 'v2'}',\n  'provider' = 'iceberg')\n"
        "AS\nSELECT id, data FROM sc.ns.t\n"
    )


def test_properties_set_and_unset_after_creation_render_sorted(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """p5b vs.show_create.1/.2 — keys set later sort among the reserved ones; UNSET removes k."""
    spark.sql(
        "CREATE VIEW sc.ns.vs (i COMMENT 'the id', d) COMMENT 'view doc' "
        "TBLPROPERTIES ('k'='v') AS SELECT id, data FROM sc.ns.t"
    )
    location = tmp_path / "ns" / "vs"
    spark.sql("ALTER VIEW sc.ns.vs SET TBLPROPERTIES ('a'='1', 'z'='2')")
    assert _show_create(spark.sql("SHOW CREATE TABLE sc.ns.vs")) == (
        "CREATE VIEW sc.ns.vs (\n  i COMMENT 'the id',\n  d)\nCOMMENT 'view doc'\n"
        "TBLPROPERTIES (\n  'a' = '1',\n  'format-version' = '1',\n  'k' = 'v',\n"
        f"  'location' = '{location}',\n  'provider' = 'iceberg',\n  'z' = '2')\n"
        "AS\nSELECT id, data FROM sc.ns.t\n"
    )
    spark.sql("ALTER VIEW sc.ns.vs UNSET TBLPROPERTIES ('k')")
    assert _show_create(spark.sql("SHOW CREATE TABLE sc.ns.vs")) == (
        "CREATE VIEW sc.ns.vs (\n  i COMMENT 'the id',\n  d)\nCOMMENT 'view doc'\n"
        "TBLPROPERTIES (\n  'a' = '1',\n  'format-version' = '1',\n"
        f"  'location' = '{location}',\n  'provider' = 'iceberg',\n  'z' = '2')\n"
        "AS\nSELECT id, data FROM sc.ns.t\n"
    )

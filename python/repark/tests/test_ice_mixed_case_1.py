"""ICE-MIXED-CASE-1 — the Spark door resolves mixed-case columns case-insensitively.

pins: ice-mixed-case-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010,
C-013, C-014, C-015, C-016

A Spark-created Iceberg table ``(userId BIGINT, eventName STRING,
`Mixed Case` INT)`` adopted into RePark must answer Spark 4.1.2 on the SQL
door for every ledger cell: SELECT list, WHERE, GROUP BY / ORDER BY, self-JOIN
ON, UPDATE, DELETE, MERGE (ON / SET / INSERT cols and values, ``UPDATE SET *``
/ ``INSERT *``), and ``INSERT INTO t (cols)``, plus a temp view of a camelCase
frame, quoted wrong-case spellings, ``caseSensitive=true``, and the
``a``/``A`` ambiguity refusal. The DataFrame door cells pin the existing
Python matching unchanged.

**Oracle basis.** ``ice_mixed_case_1_spark_oracle.json`` beside this module,
recorded from live PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0 by
``_record_ice_mixed_case_1.py`` (NOT a test module; never collected), 21 cells
under ``spark.sql.caseSensitive`` false and true. Successes record ``columns``
(requested spelling — Spark echoes the query spelling, not the stored name)
plus ``rows``; failures record ``error_class`` plus ``error_message``. The
table itself is the checked-in warehouse
``python/repark-parity/fixtures/torture/data/ice_mixed_case_1``, copied onto
exactly ``/tmp/repark-ice-mixed-case-1/ns/mc`` (absolute paths are baked into
the metadata) and adopted via ``register_table``; every test re-copies fresh
because the mutating cells move the table forward.

**Live tier.** Under ``REPARK_PARITY_LIVE=1`` each cell's statements re-run on
live Spark against a private warehouse copy and must equal the recorded oracle
(``repark == golden == live Spark``); without the flag the live tests SKIP.

Two deliberate message-shape notes. The ambiguity refusal carries Spark's
``[AMBIGUOUS_REFERENCE]`` tag and sentence (the same shape
``test_filter_predicate_rewrite.py`` pins for the DataFrame door). A wrong-case
reference under ``caseSensitive=true`` refuses loud with DataFusion's ``No
field named`` wording — the resolution outcome matches Spark (refuse), the
sentence stays engine-native; only the error class is pinned there.

Declared ``caseSensitive=true`` divergence (registry ID-1): the parser folds
unquoted identifiers before planning, so under ``true`` even an exact-case
unquoted spelling (``userId``) arrives as ``userid`` and refuses. Exact case
under ``true`` needs backticks. ``test_sql_door_unquoted_exact_case_refuses_case_sensitive``
pins the refusal; ``test_sql_door_backticked_exact_case_succeeds_case_sensitive``
pins the backticked success against the recorded ``true`` oracle rows.

**Run 21b measurement.** The oracle's ``measured_21b`` block is the
orchestrator's ``probe_mc.py`` recording (PySpark 4.1.2 + Iceberg 1.11.0,
2026-09-17), copied verbatim: the V-01 alias cells, the V-02 outer/inner cells,
the V-04 JOIN USING cells and the L-08 case-twin cells. The ``measured``
fixture rebuilds the probe's tables on a fresh memory catalog. Case twins
refuse ``[AMBIGUOUS_REFERENCE]`` with ``SQLSTATE: 42704`` and one option per
matching field in the requested spelling; the fork cannot load a twin Iceberg
schema, so the L-08 Iceberg cells pin RePark's refusal at adoption (declared)
and a twin frame carries the resolution rule.
"""

from __future__ import annotations

import json
import os
import shutil
import time
import traceback
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException

_REPO_ROOT = Path(__file__).resolve().parents[3]
_FIXTURE_SRC = _REPO_ROOT / "python/repark-parity/fixtures/torture/data/ice_mixed_case_1/ns"
_ORACLE = json.loads(
    (_REPO_ROOT / "python/repark/tests/ice_mixed_case_1_spark_oracle.json").read_text(
        encoding="utf-8"
    )
)
_DEST_ROOT = Path("/tmp/repark-ice-mixed-case-1")
_CATALOG = "mc_case"
_NAMESPACE = "ns"
_TABLE = "mc"
_FQ_TABLE = f"{_CATALOG}.{_NAMESPACE}.{_TABLE}"
_ADOPT_METADATA = str(_DEST_ROOT / "ns/mc/metadata/v2.metadata.json")
_LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
_LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — live Spark cell skipped (routine CI is JVM-free)"
_CASE_SENSITIVE_KEY = "spark.sql.caseSensitive"

_CELL_SQL: dict[str, list[str]] = {
    "MC-SEL-01": ["SELECT userId, eventName, `Mixed Case` FROM {CAT}.ns.mc ORDER BY userId"],
    "MC-SEL-02": ["SELECT userid, EVENTNAME, `Mixed Case` FROM {CAT}.ns.mc WHERE USERID = 1"],
    "MC-SEL-03": ["SELECT `USERID`, `EVENTNAME`, `mixed case` FROM {CAT}.ns.mc ORDER BY `USERID`"],
    "MC-SEL-04": ["SELECT `userId`, `eventName`, `Mixed Case` FROM {CAT}.ns.mc ORDER BY `userId`"],
    "MC-WHERE-01": ["SELECT userId FROM {CAT}.ns.mc WHERE EVENTNAME = 'a' ORDER BY userId"],
    "MC-GRP-01": [
        "SELECT EVENTNAME, COUNT(*) AS c FROM {CAT}.ns.mc GROUP BY EVENTNAME ORDER BY EVENTNAME"
    ],
    "MC-ORD-01": ["SELECT userId FROM {CAT}.ns.mc ORDER BY USERID DESC"],
    "MC-JOIN-01": [
        "SELECT a.userId, b.eventName FROM {CAT}.ns.mc a "
        "JOIN {CAT}.ns.mc b ON a.USERID = b.userid ORDER BY a.userId"
    ],
    "MC-VIEW-01": [
        "SELECT USERID, EVENTNAME FROM mcv WHERE userid = 1",
    ],
    "MC-AMB-01": [
        "SELECT a FROM amb_l JOIN amb_r ON amb_l.a = amb_r.A",
    ],
    "MC-AMB-02": [
        "SELECT A FROM amb_l JOIN amb_r ON amb_l.a = amb_r.A",
    ],
    "MC-UPD-01": [
        "UPDATE {CAT}.ns.mc SET eventName = 'u' WHERE userId = 1",
        "SELECT userId, eventName, `Mixed Case` FROM {CAT}.ns.mc ORDER BY userId",
    ],
    "MC-UPD-02": [
        "UPDATE {CAT}.ns.mc SET UserId = UserId + 10 WHERE EVENTNAME = 'b'",
        "SELECT userId, eventName, `Mixed Case` FROM {CAT}.ns.mc ORDER BY userId",
    ],
    "MC-DEL-01": [
        "DELETE FROM {CAT}.ns.mc WHERE USERID = 2",
        "SELECT userId, eventName, `Mixed Case` FROM {CAT}.ns.mc ORDER BY userId",
    ],
    "MC-MRG-01": [
        "MERGE INTO {CAT}.ns.mc t USING mcs s ON t.`userId` = s.`userid` "
        "WHEN MATCHED THEN UPDATE SET eventName = s.`eventname` "
        "WHEN NOT MATCHED THEN INSERT (userId, eventName, `Mixed Case`) "
        "VALUES (s.`userid`, s.`eventname`, s.`mixed case`)",
        "SELECT userId, eventName, `Mixed Case` FROM {CAT}.ns.mc ORDER BY userId",
    ],
    "MC-MRG-02": [
        "MERGE INTO {CAT}.ns.mc t USING mcs2 s ON t.userid = s.USERID "
        "WHEN MATCHED THEN UPDATE SET EventName = s.EVENTNAME "
        "WHEN NOT MATCHED THEN INSERT (userId, eventName, `Mixed Case`) "
        "VALUES (s.USERID, s.EVENTNAME, s.`MIXED CASE`)",
        "SELECT userId, eventName, `Mixed Case` FROM {CAT}.ns.mc ORDER BY userId",
    ],
    "MC-MRG-03": [
        "MERGE INTO {CAT}.ns.mc t USING mcs s ON t.userId = s.userid "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
        "SELECT userId, eventName, `Mixed Case` FROM {CAT}.ns.mc ORDER BY userId",
    ],
    "MC-MRG-04": [
        "MERGE INTO {CAT}.ns.mc t USING mcs2 s ON t.userid = s.USERID "
        "WHEN MATCHED THEN UPDATE SET eventName = s.EVENTNAME "
        "WHEN NOT MATCHED THEN INSERT (USERID, EVENTNAME, `mixed case`) "
        "VALUES (s.USERID, s.EVENTNAME, s.`MIXED CASE`)",
        "SELECT userId, eventName, `Mixed Case` FROM {CAT}.ns.mc ORDER BY userId",
    ],
    "MC-INS-01": [
        "INSERT INTO {CAT}.ns.mc (USERID, EVENTNAME, `mixed case`) VALUES (9, 'z', 90)",
        "SELECT userId, eventName, `Mixed Case` FROM {CAT}.ns.mc ORDER BY userId",
    ],
}

_READ_CELLS = [
    "MC-SEL-01",
    "MC-SEL-02",
    "MC-SEL-03",
    "MC-SEL-04",
    "MC-WHERE-01",
    "MC-GRP-01",
    "MC-ORD-01",
    "MC-JOIN-01",
    "MC-VIEW-01",
]
_MUTATING_CELLS = [
    "MC-UPD-01",
    "MC-UPD-02",
    "MC-DEL-01",
    "MC-MRG-01",
    "MC-MRG-02",
    "MC-MRG-03",
    "MC-MRG-04",
    "MC-INS-01",
]
_AMBIGUOUS_CELLS = ["MC-AMB-01", "MC-AMB-02"]
_TRUE_SUCCESS_CELLS = ["MC-SEL-04"]
_TRUE_UNQUOTED_EXACT_REFUSES_CELLS = ["MC-SEL-01", "MC-UPD-01", "MC-MRG-01"]
_TRUE_BACKTICK_SQL: dict[str, list[str]] = {
    "MC-SEL-01": ["SELECT `userId`, `eventName`, `Mixed Case` FROM {CAT}.ns.mc ORDER BY `userId`"],
    "MC-UPD-01": [
        "UPDATE {CAT}.ns.mc SET `eventName` = 'u' WHERE `userId` = 1",
        "SELECT `userId`, `eventName`, `Mixed Case` FROM {CAT}.ns.mc ORDER BY `userId`",
    ],
    "MC-MRG-01": [
        "MERGE INTO {CAT}.ns.mc t USING mcs s ON t.`userId` = s.`userid` "
        "WHEN MATCHED THEN UPDATE SET `eventName` = s.`eventname` "
        "WHEN NOT MATCHED THEN INSERT (`userId`, `eventName`, `Mixed Case`) "
        "VALUES (s.`userid`, s.`eventname`, s.`mixed case`)",
        "SELECT `userId`, `eventName`, `Mixed Case` FROM {CAT}.ns.mc ORDER BY `userId`",
    ],
}
_TRUE_FAILURE_CELLS = [
    "MC-SEL-02",
    "MC-SEL-03",
    "MC-WHERE-01",
    "MC-GRP-01",
    "MC-ORD-01",
    "MC-JOIN-01",
    "MC-VIEW-01",
    "MC-UPD-02",
    "MC-DEL-01",
    "MC-MRG-02",
    "MC-MRG-03",
    "MC-MRG-04",
    "MC-INS-01",
]


class _DirLock:
    """Cross-process lock so concurrent facade tests do not clobber the fixture copy."""

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        while True:
            try:
                self.path.mkdir()
                return
            except FileExistsError:
                if time.monotonic() - started > 120:
                    raise TimeoutError(f"fixture lock {path} held for 2 minutes") from None
                time.sleep(0.025)

    def close(self) -> None:
        with suppress(OSError):
            self.path.rmdir()


@contextmanager
def _materialize() -> Iterator[None]:
    """Copy the Spark-written warehouse onto its baked path, fresh for one test."""
    lock = _DirLock(Path(str(_DEST_ROOT) + ".lock"))
    try:
        if _DEST_ROOT.exists():
            shutil.rmtree(_DEST_ROOT)
        shutil.copytree(_FIXTURE_SRC, _DEST_ROOT / "ns")
        yield
    finally:
        lock.close()


@pytest.fixture
def session() -> Iterator[ReparkSession]:
    """A fresh facade session over a fresh fixture copy with the table adopted."""
    with _materialize():
        spark = ReparkSession.builder.appName("pytest-ice-mixed-case-1").getOrCreate()
        spark.register_memory_catalog(_CATALOG, str(_DEST_ROOT))
        spark.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}").collect()
        spark.sql(
            f"CALL {_CATALOG}.system.register_table("
            f"table => '{_NAMESPACE}.{_TABLE}', metadata_file => '{_ADOPT_METADATA}')"
        ).collect()
        yield spark
        spark.stop()


_LIVE_SETUP_SQL: dict[str, list[str]] = {
    "MC-VIEW-01": ["CREATE OR REPLACE TEMP VIEW mcv AS SELECT * FROM {CAT}.ns.mc"],
    "MC-AMB-01": [
        "CREATE OR REPLACE TEMP VIEW amb_l AS SELECT 1 AS a",
        "CREATE OR REPLACE TEMP VIEW amb_r AS SELECT 2 AS A",
    ],
    "MC-AMB-02": [
        "CREATE OR REPLACE TEMP VIEW amb_l AS SELECT 1 AS a",
        "CREATE OR REPLACE TEMP VIEW amb_r AS SELECT 2 AS A",
    ],
    "MC-MRG-01": [
        "CREATE OR REPLACE TEMP VIEW mcs AS SELECT CAST(2 AS BIGINT) AS userid, "
        "'b2' AS eventname, 21 AS `mixed case` UNION ALL "
        "SELECT CAST(3 AS BIGINT), 'c', 30",
    ],
    "MC-MRG-03": [
        "CREATE OR REPLACE TEMP VIEW mcs AS SELECT CAST(2 AS BIGINT) AS userid, "
        "'b2' AS eventname, 21 AS `mixed case` UNION ALL "
        "SELECT CAST(3 AS BIGINT), 'c', 30",
    ],
    "MC-MRG-02": [
        "CREATE OR REPLACE TEMP VIEW mcs2 AS SELECT CAST(2 AS BIGINT) AS USERID, "
        "'b2' AS EVENTNAME, 21 AS `MIXED CASE` UNION ALL "
        "SELECT CAST(3 AS BIGINT), 'c', 30",
    ],
    "MC-MRG-04": [
        "CREATE OR REPLACE TEMP VIEW mcs2 AS SELECT CAST(2 AS BIGINT) AS USERID, "
        "'b2' AS EVENTNAME, 21 AS `MIXED CASE` UNION ALL "
        "SELECT CAST(3 AS BIGINT), 'c', 30",
    ],
}


def _setup_cell_views(spark: Any, cell_id: str, catalog: str) -> None:
    """Create the temp views a cell needs through the DataFrame door.

    SQL ``CREATE TEMP VIEW`` is not a RePark surface, so the RePark pin
    builds the views from frames. The live tier does not use this helper:
    stored column case depends on the registration path, so the live tier
    replays ``_LIVE_SETUP_SQL`` (the record script's DDL verbatim) instead.
    """
    if cell_id == "MC-VIEW-01":
        spark.sql(f"SELECT * FROM {catalog}.ns.mc").createOrReplaceTempView("mcv")
    if cell_id in ("MC-AMB-01", "MC-AMB-02"):
        spark.sql("SELECT 1 AS a").createOrReplaceTempView("amb_l")
        spark.sql("SELECT 2 AS A").createOrReplaceTempView("amb_r")
    if cell_id in ("MC-MRG-01", "MC-MRG-03"):
        spark.sql(
            "SELECT CAST(2 AS BIGINT) AS userid, 'b2' AS eventname, 21 AS `mixed case` "
            "UNION ALL SELECT CAST(3 AS BIGINT), 'c', 30"
        ).createOrReplaceTempView("mcs")
    if cell_id in ("MC-MRG-02", "MC-MRG-04"):
        spark.sql(
            "SELECT CAST(2 AS BIGINT) AS USERID, 'b2' AS EVENTNAME, 21 AS `MIXED CASE` "
            "UNION ALL SELECT CAST(3 AS BIGINT), 'c', 30"
        ).createOrReplaceTempView("mcs2")


def _run_statements(spark: ReparkSession, cell_id: str, catalog: str) -> pa.Table | None:
    """Run a cell's statements; the last statement's Arrow output, or None on DML tail."""
    _setup_cell_views(spark, cell_id, catalog)
    table: pa.Table | None = None
    for sql in _CELL_SQL[cell_id]:
        frame = spark.sql(sql.format(CAT=catalog))
        try:
            table = frame.to_arrow()
        except Exception:
            table = None
    return table


def _expected(cell_id: str, flag: str) -> dict[str, Any]:
    """The recorded Spark answer for one cell under one ``caseSensitive`` value."""
    return _ORACLE["cells"][cell_id][flag]


_STORED_NAMES: dict[str, str] = {
    "userid": "userId",
    "eventname": "eventName",
    "mixed case": "Mixed Case",
}

_EXPECTED_TYPES: dict[str, str] = {
    "userId": "int64",
    "eventName": "string",
    "Mixed Case": "int32",
    "c": "int64",
    "a": "int32",
    "A": "int32",
}


def _assert_success(table: pa.Table, cell_id: str, flag: str) -> None:
    """A RePark Arrow output equals the recorded Spark answer: order, rows, types.

    Column-name case is compared case-insensitively: Spark echoes the requested
    spelling while the SQL door outputs the stored name (declared in the module
    docstring); ``test_sql_door_wrong_case_outputs_stored_names`` pins that.
    """
    recorded = _expected(cell_id, flag)
    assert "rows" in recorded, f"{cell_id} (caseSensitive={flag}) must succeed on Spark"
    assert [name.lower() for name in table.column_names] == [
        name.lower() for name in recorded["columns"]
    ]
    actual_rows = [tuple(row.values()) for row in table.to_pylist()]
    assert actual_rows == [tuple(row) for row in recorded["rows"]]
    for name in table.column_names:
        assert str(table.schema.field(name).type) == _EXPECTED_TYPES[name], name


def test_sql_door_wrong_case_outputs_stored_names(session: ReparkSession) -> None:
    """Wrong-case references resolve but the SQL door outputs the stored name.

    Spark echoes the requested spelling (``USERID``); RePark outputs ``userId``.
    This pins the declared divergence until output naming converges.
    """
    table = session.sql(f"SELECT userid, EVENTNAME FROM {_FQ_TABLE} WHERE USERID = 1").to_arrow()
    assert table.column_names == ["userId", "eventName"]
    grouped = session.sql(
        f"SELECT EVENTNAME, COUNT(*) AS c FROM {_FQ_TABLE} GROUP BY EVENTNAME ORDER BY EVENTNAME"
    ).to_arrow()
    assert grouped.column_names == ["eventName", "c"]


@pytest.mark.parametrize("cell_id", _READ_CELLS + _MUTATING_CELLS)
def test_sql_door_answers_spark_case_insensitive(session: ReparkSession, cell_id: str) -> None:
    """Read and mutating cells answer the recorded Spark answer (default flag)."""
    table = _run_statements(session, cell_id, _CATALOG)
    assert table is not None
    _assert_success(table, cell_id, "false")


@pytest.mark.parametrize("cell_id", _AMBIGUOUS_CELLS)
def test_sql_door_ambiguous_reference_matches_spark_shape(
    session: ReparkSession, cell_id: str
) -> None:
    """A case-only collision refuses with Spark's ``[AMBIGUOUS_REFERENCE]`` sentence."""
    with pytest.raises(AnalysisException) as excinfo:
        _run_statements(session, cell_id, _CATALOG)
    message = str(excinfo.value)
    recorded = _expected(cell_id, "false")["error_message"]
    sentence = recorded[recorded.index("[AMBIGUOUS_REFERENCE]") : recorded.index("; line")]
    assert sentence.endswith("SQLSTATE: 42704")
    assert sentence in message


@pytest.mark.parametrize("cell_id", _TRUE_SUCCESS_CELLS)
def test_sql_door_exact_spelling_succeeds_case_sensitive(
    session: ReparkSession, cell_id: str
) -> None:
    """Backticked exact-case references resolve with ``caseSensitive=true``."""
    session.conf.set(_CASE_SENSITIVE_KEY, "true")
    try:
        table = _run_statements(session, cell_id, _CATALOG)
    finally:
        session.conf.set(_CASE_SENSITIVE_KEY, "false")
    assert table is not None
    _assert_success(table, cell_id, "true")


@pytest.mark.parametrize("cell_id", sorted(_TRUE_BACKTICK_SQL))
def test_sql_door_backticked_exact_case_succeeds_case_sensitive(
    session: ReparkSession, cell_id: str
) -> None:
    """Backticked exact-case spellings answer the recorded ``true`` oracle rows."""
    session.conf.set(_CASE_SENSITIVE_KEY, "true")
    try:
        _setup_cell_views(session, cell_id, _CATALOG)
        table: pa.Table | None = None
        for sql in _TRUE_BACKTICK_SQL[cell_id]:
            table = session.sql(sql.format(CAT=_CATALOG)).to_arrow()
    finally:
        session.conf.set(_CASE_SENSITIVE_KEY, "false")
    assert table is not None
    _assert_success(table, cell_id, "true")


@pytest.mark.parametrize("cell_id", _TRUE_UNQUOTED_EXACT_REFUSES_CELLS)
def test_sql_door_unquoted_exact_case_refuses_case_sensitive(
    session: ReparkSession, cell_id: str
) -> None:
    """Unquoted exact-case refuses with ``caseSensitive=true`` (declared).

    The parser folds unquoted identifiers before planning, so the exact case
    never reaches resolution. Backticks are the exact-case spelling; Spark
    resolving the unquoted form is the declared divergence (registry ID-1).
    """
    session.conf.set(_CASE_SENSITIVE_KEY, "true")
    try:
        with pytest.raises(AnalysisException):
            _run_statements(session, cell_id, _CATALOG)
    finally:
        session.conf.set(_CASE_SENSITIVE_KEY, "false")


@pytest.mark.parametrize("cell_id", _TRUE_FAILURE_CELLS)
def test_sql_door_wrong_case_refuses_case_sensitive(session: ReparkSession, cell_id: str) -> None:
    """Wrong-case references refuse loud with ``caseSensitive=true`` (Spark refuses)."""
    session.conf.set(_CASE_SENSITIVE_KEY, "true")
    try:
        with pytest.raises(AnalysisException):
            _run_statements(session, cell_id, _CATALOG)
    finally:
        session.conf.set(_CASE_SENSITIVE_KEY, "false")


def test_sql_set_statement_drives_case_sensitive(session: ReparkSession) -> None:
    """``SET spark.sql.caseSensitive`` flips resolution like ``conf.set`` does."""
    session.sql("SET spark.sql.caseSensitive = true").collect()
    try:
        with pytest.raises(AnalysisException):
            session.sql(f"SELECT userid FROM {_FQ_TABLE} WHERE USERID = 1").to_arrow()
        with pytest.raises(AnalysisException):
            session.sql(f"SELECT userId FROM {_FQ_TABLE} ORDER BY userId").to_arrow()
        table = session.sql(f"SELECT `userId` FROM {_FQ_TABLE} ORDER BY `userId`").to_arrow()
        assert table.to_pylist() == [{"userId": 1}, {"userId": 2}]
    finally:
        session.sql("SET spark.sql.caseSensitive = false").collect()
    table = session.sql(f"SELECT USERID FROM {_FQ_TABLE} ORDER BY USERID").to_arrow()
    assert table.column_names == ["userId"]
    assert table.to_pylist() == [{"userId": 1}, {"userId": 2}]


def test_dataframe_door_cells_unchanged(session: ReparkSession) -> None:
    """The DataFrame door keeps its existing case-insensitive matching for the cells."""
    selected = session.sql(f"SELECT * FROM {_FQ_TABLE}").select("USERID").to_arrow()
    assert selected.column_names == ["USERID"]
    assert sorted(selected.to_pylist(), key=repr) == [{"USERID": 1}, {"USERID": 2}]
    filtered = session.sql(f"SELECT * FROM {_FQ_TABLE}").filter("USERID = 1").to_arrow()
    assert filtered.column_names == ["userId", "eventName", "Mixed Case"]
    assert filtered.to_pylist() == [{"userId": 1, "eventName": "a", "Mixed Case": 10}]


def _live_spark_table(live: Any, warehouse: Path, catalog: str) -> None:
    """Copy the fixture to a private warehouse and expose it to live Spark."""
    dest = warehouse / "ns"
    if warehouse.exists():
        shutil.rmtree(warehouse)
    shutil.copytree(_FIXTURE_SRC, dest)
    live.session.conf.set(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
    live.session.conf.set(f"spark.sql.catalog.{catalog}.type", "hadoop")
    live.session.conf.set(f"spark.sql.catalog.{catalog}.warehouse", str(warehouse))


def _live_outcome(live: Any, cell_id: str, catalog: str, flag: str) -> dict[str, Any]:
    """Run one cell on live Spark; rows plus columns, or error class and message."""
    live.session.conf.set("spark.sql.caseSensitive", flag)
    try:
        for setup in _LIVE_SETUP_SQL.get(cell_id, []):
            live.session.sql(setup.format(CAT=catalog)).collect()
        table: Any = None
        for sql in _CELL_SQL[cell_id]:
            table = live.session.sql(sql.format(CAT=catalog)).toArrow()
        assert table is not None
        return {"rows": table.to_pylist(), "columns": table.column_names}
    except Exception as exc:
        name = type(exc).__name__
        text = "".join(traceback.format_exception_only(exc)).strip()
        return {"error_class": name, "error_message": text}


def test_live_spark_dataframe_recipes_match_the_recorded_oracle(
    tmp_path: Path, spark_engine: Any
) -> None:
    """Live Spark DataFrame ``select``/``filter`` re-derive the DF oracle cells."""
    if not _LIVE:
        pytest.skip(_LIVE_SKIP)
    _live_spark_table(spark_engine, tmp_path / "mc_live_df_wh", "mclivedf")
    frame = spark_engine.session.table("mclivedf.ns.mc")
    selected = frame.select("USERID").toArrow()
    recorded = _ORACLE["cells"]["MC-DF-01"]["false"]
    assert selected.column_names == recorded["columns"]
    assert sorted(selected.to_pylist(), key=repr) == [
        dict(zip(recorded["columns"], row, strict=True)) for row in recorded["rows"]
    ]
    filtered = spark_engine.session.table("mclivedf.ns.mc").filter("USERID = 1").toArrow()
    recorded = _ORACLE["cells"]["MC-DF-02"]["false"]
    assert filtered.column_names == recorded["columns"]
    assert filtered.to_pylist() == [
        dict(zip(recorded["columns"], row, strict=True)) for row in recorded["rows"]
    ]


@pytest.mark.parametrize("cell_id", sorted(_CELL_SQL))
@pytest.mark.parametrize("flag", ["false", "true"])
def test_live_spark_still_matches_the_recorded_oracle(
    cell_id: str, flag: str, tmp_path: Path, spark_engine: Any
) -> None:
    """Live PySpark 4.1.2 re-derives the recorded oracle (drift detector)."""
    if not _LIVE:
        pytest.skip(_LIVE_SKIP)
    _live_spark_table(spark_engine, tmp_path / "mc_live_wh", "mclive")
    spark_engine.session.sql("DELETE FROM mclive.ns.mc").collect()
    spark_engine.session.sql("INSERT INTO mclive.ns.mc VALUES (1, 'a', 10), (2, 'b', 20)").collect()
    live = _live_outcome(spark_engine, cell_id, "mclive", flag)
    recorded = _expected(cell_id, flag)
    if "rows" in recorded:
        assert live.get("rows") == [
            dict(zip(recorded["columns"], row, strict=True)) for row in recorded["rows"]
        ]
        assert live.get("columns") == recorded["columns"]
    else:
        assert live.get("error_class") == recorded["error_class"]
        assert recorded["error_message"].splitlines()[0][:80] in live.get("error_message", "")


_MEASURED = _ORACLE["measured_21b"]
_MEASURED_CELLS = _MEASURED["cells"]
_MEASURED_QUERY_CELLS = [
    "V01_alias_same_name_select",
    "V01_alias_same_name_expr_order",
    "V01_alias_same_name_agg_having",
    "V01_alias_upper_of_upper",
    "V02_outer_mixed_case_with_failing_inner",
    "V02_control_outer_only",
    "V04_join_using_select",
]
_MEASURED_TWIN_CELLS = [
    "L08_qualified_twin",
    "L08_bare_twin",
    "L08_bare_twin_lower",
    "L08_star_twin",
]
_MEASURED_SETUP = [
    "CREATE NAMESPACE {CAT}.ns",
    "CREATE TABLE {CAT}.ns.mc (`userId` INT, `eventName` STRING)",
    "INSERT INTO {CAT}.ns.mc VALUES (1, 'a'), (2, 'b')",
    "CREATE TABLE {CAT}.ns.other (name STRING)",
    "INSERT INTO {CAT}.ns.other VALUES ('a')",
    "CREATE TABLE {CAT}.ns.ja (`userId` INT, x INT)",
    "INSERT INTO {CAT}.ns.ja VALUES (1, 10), (2, 20)",
    "CREATE TABLE {CAT}.ns.jb (`userId` INT, y INT)",
    "INSERT INTO {CAT}.ns.jb VALUES (1, 100)",
    "CREATE TABLE {CAT}.ns.jt (`userId` INT, x INT, y INT)",
]
_MEASURED_CATALOG = "mc21"
_TWIN_METADATA = (
    _REPO_ROOT / "python/repark-parity/fixtures/torture/data/ice_mixed_case_1/twin_v3.metadata.json"
)


@pytest.fixture
def measured(tmp_path: Path) -> Iterator[ReparkSession]:
    """A fresh session with the probe's ``mc`` / ``other`` / ``ja`` / ``jb`` / ``jt`` tables."""
    spark = ReparkSession.builder.appName("pytest-ice-mixed-case-1-21b").getOrCreate()
    spark.register_memory_catalog(_MEASURED_CATALOG, str(tmp_path / "wh"))
    for sql in _MEASURED_SETUP:
        spark.sql(sql.format(CAT=_MEASURED_CATALOG)).collect()
    yield spark
    spark.stop()


def _measured_sql(cell_id: str) -> str:
    """The recorded statement, pointed at the pin's catalog instead of the probe's ``sc``."""
    return _MEASURED_CELLS[cell_id]["sql"].replace("sc.ns.", f"{_MEASURED_CATALOG}.ns.")


def _sorted_rows(table: pa.Table) -> list[list[Any]]:
    """Rows as lists, sorted by ``repr`` exactly as the probe sorted them."""
    return sorted(([*row.values()] for row in table.to_pylist()), key=repr)


@pytest.mark.parametrize("cell_id", _MEASURED_QUERY_CELLS)
def test_measured_query_cells_answer_spark(measured: ReparkSession, cell_id: str) -> None:
    """V-01 / V-02 / V-04 query cells answer the run-21b Spark measurement."""
    recorded = _MEASURED_CELLS[cell_id]
    assert recorded["outcome"] == "ok"
    table = measured.sql(_measured_sql(cell_id)).to_arrow()
    assert [name.lower() for name in table.column_names] == [
        name.lower() for name in recorded["columns"]
    ]
    assert _sorted_rows(table) == recorded["rows"]


@pytest.mark.xfail(
    strict=True,
    reason=(
        "BLOCKED-ON-FORK F-DML-FIELD-ID-1, pre-existing on origin/main 71482620: INSERT … SELECT "
        "over a join of two Iceberg scans writes NULL for the right-side column"
    ),
)
def test_measured_join_using_insert_answers_spark(measured: ReparkSession) -> None:
    """V-04: ``INSERT … SELECT … JOIN … USING (USERID)`` writes the Spark rows."""
    recorded = _MEASURED_CELLS["V04_join_using_insert"]
    assert recorded["outcome"] == "ok"
    measured.sql(_measured_sql("V04_join_using_insert")).collect()
    table = measured.sql(f"SELECT * FROM {_MEASURED_CATALOG}.ns.jt ORDER BY userId").to_arrow()
    assert _sorted_rows(table) == recorded["rows"]


@pytest.mark.parametrize("cell_id", _MEASURED_TWIN_CELLS)
def test_measured_case_twin_table_refuses_at_adoption(
    measured: ReparkSession, cell_id: str, tmp_path: Path
) -> None:
    """Spark loads a case-twin Iceberg table and refuses the reference; RePark refuses the load.

    Declared (registry ICE-MIXED-CASE-1 twin row): the fork's schema index
    rejects ``id`` + ``ID`` while parsing the metadata, so every L-08 cell
    refuses loud at adoption instead of at resolution. Never a silent answer.
    """
    recorded = _MEASURED_CELLS[cell_id]
    assert recorded["outcome"] == "error"
    assert recorded["class"] == "AnalysisException"
    assert _MEASURED_CELLS["twin_add"]["schema"] == ["id", "ID"]
    metadata = tmp_path / "tw" / "metadata" / "v3.metadata.json"
    metadata.parent.mkdir(parents=True)
    shutil.copyfile(_TWIN_METADATA, metadata)
    with pytest.raises(Exception, match="Cannot build lower case index: id and ID collide"):
        measured.sql(
            f"CALL {_MEASURED_CATALOG}.system.register_table("
            f"table => 'ns.tw', metadata_file => '{metadata}')"
        ).collect()
    with pytest.raises(AnalysisException):
        measured.sql(_measured_sql(cell_id)).collect()


@pytest.mark.parametrize(
    ("sql", "reference", "options"),
    [
        ("SELECT t.ID FROM twv AS t", "`t`.`ID`", "[`t`.`ID`, `t`.`ID`]"),
        ("SELECT ID FROM twv", "`ID`", "[`twv`.`ID`, `twv`.`ID`]"),
        ("SELECT id FROM twv", "`id`", "[`twv`.`id`, `twv`.`id`]"),
        ("SELECT t.id FROM twv AS t", "`t`.`id`", "[`t`.`id`, `t`.`id`]"),
    ],
)
def test_case_twin_reference_is_ambiguous_exact_or_not(
    measured: ReparkSession, sql: str, reference: str, options: str
) -> None:
    """L-08: any reference to a name with an ASCII case twin refuses like Spark.

    The measured Spark rule (``L08_*`` cells): exact or case-variant, bare or
    qualified, one option per matching field in the requested spelling,
    ``SQLSTATE: 42704``. A frame is the twin carrier here because the fork
    cannot load a twin Iceberg schema (the adoption pin above).
    """
    measured.sql("SELECT 1 AS id, 0 AS `ID`").createOrReplaceTempView("twv")
    with pytest.raises(AnalysisException) as excinfo:
        measured.sql(sql).to_arrow()
    assert (
        f"[AMBIGUOUS_REFERENCE] Reference {reference} is ambiguous, could be: {options}. "
        "SQLSTATE: 42704"
    ) in str(excinfo.value)


def test_join_using_insert_folds_and_writes_the_left_columns(measured: ReparkSession) -> None:
    """V-04 fold half: the INSERT plans (no ``No field named userid``) and writes the key row.

    The strict xfail above carries the right-side NULL defect; this pin keeps the
    fold itself green so the V-04 fix cannot regress while that defect is open.
    """
    measured.sql(_measured_sql("V04_join_using_insert")).collect()
    table = measured.sql(f"SELECT userId, x FROM {_MEASURED_CATALOG}.ns.jt").to_arrow()
    recorded = _MEASURED_CELLS["V04_join_using_insert"]["rows"]
    assert _sorted_rows(table) == [row[:2] for row in recorded]

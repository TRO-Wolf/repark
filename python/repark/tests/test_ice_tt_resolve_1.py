"""ICE-TT-RESOLVE-1 — Spark's time-travel resolution on every door.

Replays the 94 recorded Spark cells (47 shapes on format versions 2 and 3,
``ice_tt_resolve_1_spark_oracle.json``, live PySpark 4.1.2 + Iceberg 1.11.0)
against RePark: reader ``versionAsOf`` / ``timestampAsOf`` options through
both ``load`` and ``table``, the SQL ``TIMESTAMP AS OF`` /
``FOR SYSTEM_TIME AS OF`` cells on the facade ``spark.sql`` door, and each
answering cell's ``AS OF`` expression as a bare ``repark.sql`` select on the
native door (the existing beside-spark.sql idiom; the native door has no
table binding, so table-level ANSI time travel is pinned in
``crates/repark-sql/src/tests.rs``). Rows assert against the fixture;
refusals assert the exception class plus the message or condition token. One
table per format version serves every cell; dynamic values (snapshot ids,
mids, epoch seconds) resolve from this run's own snapshot log. The live tier
replays the same shapes on live Spark under ``REPARK_PARITY_LIVE=1`` and
re-derives the fixture.

pins: ice-tt-resolve-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
"""

from __future__ import annotations

import datetime
import json
import os
import re
import time
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, IllegalArgumentException, ParseException
from repark.spark.session import _reset_active_session_for_tests

LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — the live Spark re-derivation is skipped (CI is JVM-free)"
FIXTURE: dict[str, Any] = json.loads(
    Path(__file__).with_name("ice_tt_resolve_1_spark_oracle.json").read_text(encoding="utf-8")
)
CELLS: dict[str, dict[str, Any]] = {cell["id"]: cell for cell in FIXTURE["cells"]}
UTC = datetime.UTC
COMMIT_GAP = 2.2
S0_ROWS = [(1, "a", "x"), (2, "b", "y")]
S1_ROWS = [(1, "a", "x"), (2, "b", "y"), (3, "c", "x")]
S2_ROWS = [(2, "b", "y"), (3, "c", "x")]
SPEC_MSG = (
    "[INVALID_TIME_TRAVEL_SPEC] Cannot specify both version and timestamp "
    "when time travelling the table. SQLSTATE: 42K0E"
)
SNAPID_MSG = (
    "Time travel option `snapshot-id` is no longer supported, "
    "use Spark built-in `versionAsOf` instead"
)
ASOF_MSG = (
    "Time travel option `as-of-timestamp` (in millis) is no longer supported, "
    "use Spark built-in `timestampAsOf` instead (properly formatted timestamp)"
)
BRANCH_MSG = "Can't time travel in branch"
INSTANT_RE = re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?")


def _cell_error(cell_id: str) -> dict[str, Any]:
    """Return the recorded error payload for an error cell."""
    error = CELLS[cell_id]["error"]
    assert isinstance(error, dict)
    return error


def _stable_token(cell_id: str) -> str:
    """Return the recorded message minus run-varying instants."""
    return INSTANT_RE.sub("", _cell_error(cell_id)["msg"]).strip()


def _rows_of(frame: Any) -> list[tuple[Any, ...]]:
    """Collect id, data, cat rows ordered by id."""
    table = frame.select("id", "data", "cat").to_arrow()
    rows = list(
        zip(
            table.column("id").to_pylist(),
            table.column("data").to_pylist(),
            table.column("cat").to_pylist(),
            strict=True,
        )
    )
    rows.sort(key=lambda row: row[0])
    return rows


def _seed_table(spark: Any, table: str, version: int) -> dict[str, Any]:
    """Build the three-snapshot shape with tag t0 on S0 and branch b0 on S1."""
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version'='{version}')"
    )
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a', 'x'), (2, 'b', 'y')")
    time.sleep(COMMIT_GAP)
    spark.sql(f"INSERT INTO {table} VALUES (3, 'c', 'x')")
    time.sleep(COMMIT_GAP)
    spark.sql(f"DELETE FROM {table} WHERE id = 1")
    snaps = spark._testing_list_snapshots(table)
    assert len(snaps) == 3
    ids = [int(pair[0]) for pair in snaps]
    stamps = [int(pair[1]) for pair in snaps]
    assert stamps[1] - stamps[0] >= 2000
    assert stamps[2] - stamps[1] >= 2000
    spark._testing_create_ref(table, "tag", "t0", ids[0])
    spark._testing_create_ref(table, "branch", "b0", ids[1])
    mid_ms = stamps[0] + (stamps[1] - stamps[0]) // 2
    assert stamps[0] < mid_ms < stamps[1]
    sec = stamps[0] // 1000 + 1
    assert sec * 1000 > stamps[0]
    assert sec * 1000 + 500 < stamps[1]
    mid = datetime.datetime.fromtimestamp(mid_ms / 1000, tz=UTC)
    ny_zone = datetime.timezone(datetime.timedelta(hours=-4))
    return {
        "s0": ids[0],
        "s1": ids[1],
        "mid_str": mid.strftime("%Y-%m-%d %H:%M:%S.%f"),
        "mid_t": mid.strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "mid_ny": mid.astimezone(ny_zone).strftime("%Y-%m-%d %H:%M:%S.%f"),
        "s1_str": datetime.datetime.fromtimestamp(stamps[1] / 1000, tz=UTC).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        ),
        "sec": sec,
        "mid_ms": mid_ms,
        "s2_ms": stamps[2],
    }


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """One warehouse directory shared by every session in this module."""
    return tmp_path_factory.mktemp("tt_wh")


@pytest.fixture(scope="module")
def seeded(warehouse: Path) -> dict[str, Any]:
    """Seed both format-version tables once; tests adopt them by metadata file."""
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-ice-tt-resolve-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog("mem", warehouse / "wh")
    session.sql("CREATE NAMESPACE mem.ns")
    seeds = {version: _seed_table(session, f"mem.ns.tt_v{version}", version) for version in (2, 3)}
    metas = {version: _latest_metadata(warehouse, version) for version in (2, 3)}
    session.stop()
    _reset_active_session_for_tests()
    return {"seeds": seeds, "metas": metas, "warehouse": warehouse}


def _latest_metadata(warehouse: Path, version: int) -> str:
    """Return the newest metadata file of a seeded table, discovered by walk."""
    candidates = sorted((warehouse / "wh").rglob(f"tt_v{version}/metadata/*.metadata.json"))
    assert candidates, f"no metadata files for tt_v{version} under {warehouse}"
    return str(candidates[-1])


def _adopt_tables(session: Any, seeded: dict[str, Any]) -> None:
    """Register the seeded tables on a fresh session from their metadata files."""
    session.register_memory_catalog("mem", seeded["warehouse"] / "wh")
    session.sql("CREATE NAMESPACE mem.ns")
    for version in (2, 3):
        session.sql(
            "CALL mem.system.register_table("
            f"table => 'ns.tt_v{version}', metadata_file => '{seeded['metas'][version]}')"
        ).collect()


@pytest.fixture()
def spark(seeded: dict[str, Any]) -> Any:
    """UTC facade session with the seeded tables adopted."""
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-ice-tt-resolve-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    _adopt_tables(session, seeded)
    yield session
    session.stop()


@pytest.fixture()
def spark_ny(spark: Any, seeded: dict[str, Any]) -> Any:
    """New York facade session with the seeded tables adopted."""
    session = spark.newSession()
    session.conf.set("spark.sql.session.timeZone", "America/New_York")
    _adopt_tables(session, seeded)
    yield session
    session.stop()


def _facade_table(version: int) -> str:
    """Catalog table name on the facade sessions."""
    return f"mem.ns.tt_v{version}"


def _reader_option_frames(
    spark: Any, table: str, options: list[tuple[str, Any]], entry: str
) -> Any:
    """Apply reader options and read through load or table."""
    reader = spark.read.format("iceberg")
    for key, value in options:
        reader = reader.option(key, value)
    if entry == "load":
        return reader.load(table)
    return reader.table(table)


def _reader_ok_cases() -> list[tuple[str, int, str]]:
    """Every passing reader cell on both versions and both reader entry points."""
    cells = [
        "TT-DF-VAS-INT",
        "TT-DF-VAS-STR",
        "TT-DF-VAS-TAG",
        "TT-DF-VAS-BRANCH",
        "TT-DF-VAS-MAIN",
        "TT-DF-VAS-LOWER",
        "TT-DF-VAS-TABLE-API",
        "TT-DF-VAS-TAG-TABLE-API",
        "TT-DF-TAS-STR",
        "TT-DF-TAS-STR-EXACT",
        "TT-DF-TAS-STR-T",
        "TT-DF-TAS-DATE",
        "TT-DF-TAS-EPOCH-SEC",
        "TT-DF-TAS-EPOCH-MS",
        "TT-DF-TAS-NY",
        "TT-DF-TAS-TABLE-API",
    ]
    return [
        (cell, version, entry)
        for cell in cells
        for version in (2, 3)
        for entry in ("load", "table")
    ]


def _reader_options(cell: str, seed: dict[str, Any]) -> list[tuple[str, Any]]:
    """Build this run's reader options for a passing cell."""
    if cell == "TT-DF-VAS-INT":
        return [("versionAsOf", seed["s0"])]
    if cell == "TT-DF-VAS-STR":
        return [("versionAsOf", str(seed["s0"]))]
    if cell == "TT-DF-VAS-TAG":
        return [("versionAsOf", "t0")]
    if cell == "TT-DF-VAS-BRANCH":
        return [("versionAsOf", "b0")]
    if cell == "TT-DF-VAS-MAIN":
        return [("versionAsOf", "main")]
    if cell == "TT-DF-VAS-LOWER":
        return [("versionasof", seed["s0"])]
    if cell == "TT-DF-VAS-TABLE-API":
        return [("versionAsOf", seed["s0"])]
    if cell == "TT-DF-VAS-TAG-TABLE-API":
        return [("versionAsOf", "t0")]
    if cell == "TT-DF-TAS-STR":
        return [("timestampAsOf", seed["mid_str"])]
    if cell == "TT-DF-TAS-STR-EXACT":
        return [("timestampAsOf", seed["s1_str"])]
    if cell == "TT-DF-TAS-STR-T":
        return [("timestampAsOf", seed["mid_t"])]
    if cell == "TT-DF-TAS-DATE":
        return [("timestampAsOf", "2999-01-01")]
    if cell == "TT-DF-TAS-EPOCH-SEC":
        return [("timestampAsOf", str(seed["sec"]))]
    if cell == "TT-DF-TAS-EPOCH-MS":
        return [("timestampAsOf", str(seed["mid_ms"]))]
    if cell == "TT-DF-TAS-NY":
        return [("timestampAsOf", seed["mid_ny"])]
    if cell == "TT-DF-TAS-TABLE-API":
        return [("timestampAsOf", seed["mid_str"])]
    raise AssertionError(f"unknown reader cell {cell!r}")


def _reader_ok_expected(cell: str, version: int) -> list[tuple[Any, ...]]:
    """Fixture rows for a passing reader cell."""
    return [tuple(row) for row in CELLS[f"{cell}-V{version}"]["obs"]["rows"]]


@pytest.mark.parametrize("cell,version,entry", _reader_ok_cases())
def test_reader_cells(
    spark: Any, spark_ny: Any, seeded: dict[str, Any], cell: str, version: int, entry: str
) -> None:
    """Reader cells answer the fixture on load and table. pins: ice-tt-resolve-1/C-002"""
    seed = seeded["seeds"][version]
    session = spark_ny if cell == "TT-DF-TAS-NY" else spark
    frame = _reader_option_frames(
        session, _facade_table(version), _reader_options(cell, seed), entry
    )
    assert _rows_of(frame) == _reader_ok_expected(cell, version)
    assert frame.to_arrow().column_names == ["id", "data", "cat"]


def _reader_err_cases() -> list[tuple[str, int, str]]:
    """Every refusing reader cell on both versions and both entries, branch selector on load."""
    cells = [
        "TT-DF-VAS-MISSING-ERR",
        "TT-DF-VAS-UNKNOWN-REF-ERR",
        "TT-DF-TAS-BEFORE-ERR",
        "TT-DF-TAS-GARBAGE-ERR",
        "TT-DF-VAS-TAS-ERR",
        "TT-DF-VAS-SNAPID-ERR",
        "TT-DF-VAS-SNAPID-SAME",
        "TT-DF-VAS-BRANCH-OPT",
        "TT-DF-TAS-ASOF-ERR",
    ]
    cases = [
        (cell, version, entry)
        for cell in cells
        for version in (2, 3)
        for entry in ("load", "table")
    ]
    cases.extend([("TT-DF-VAS-SELECTOR-ERR", version, "load") for version in (2, 3)])
    return cases


def _reader_err_options(cell: str, seed: dict[str, Any]) -> tuple[list[tuple[str, Any]], bool]:
    """Build this run's refusing reader options plus whether the table carries a branch suffix."""
    if cell == "TT-DF-VAS-MISSING-ERR":
        return [("versionAsOf", 12345)], False
    if cell == "TT-DF-VAS-UNKNOWN-REF-ERR":
        return [("versionAsOf", "nope")], False
    if cell == "TT-DF-TAS-BEFORE-ERR":
        return [("timestampAsOf", "2000-01-01 00:00:00")], False
    if cell == "TT-DF-TAS-GARBAGE-ERR":
        return [("timestampAsOf", "not a ts")], False
    if cell == "TT-DF-VAS-TAS-ERR":
        return [("versionAsOf", seed["s0"]), ("timestampAsOf", seed["mid_str"])], False
    if cell in ("TT-DF-VAS-SNAPID-ERR", "TT-DF-VAS-SNAPID-SAME"):
        other = seed["s0"] if cell == "TT-DF-VAS-SNAPID-SAME" else seed["s1"]
        return [("versionAsOf", seed["s0"]), ("snapshot-id", other)], False
    if cell == "TT-DF-VAS-BRANCH-OPT":
        return [("versionAsOf", seed["s0"]), ("branch", "b0")], False
    if cell == "TT-DF-TAS-ASOF-ERR":
        return [("timestampAsOf", seed["mid_str"]), ("as-of-timestamp", seed["mid_ms"])], False
    if cell == "TT-DF-VAS-SELECTOR-ERR":
        return [("versionAsOf", seed["s0"])], True
    raise AssertionError(f"unknown reader cell {cell!r}")


def _reader_err_expectation(cell: str, version: int) -> tuple[type[BaseException], str]:
    """Exception class plus exact message or stable token for a refusing reader cell."""
    suffix = f"-V{version}"
    if cell in ("TT-DF-VAS-MISSING-ERR", "TT-DF-VAS-UNKNOWN-REF-ERR", "TT-DF-TAS-BEFORE-ERR"):
        return IllegalArgumentException, _cell_error(cell + suffix)["msg"]
    if cell == "TT-DF-TAS-GARBAGE-ERR":
        return AnalysisException, _cell_error(cell + suffix)["msg"]
    if cell == "TT-DF-VAS-TAS-ERR":
        return AnalysisException, SPEC_MSG
    if cell in ("TT-DF-VAS-SNAPID-ERR", "TT-DF-VAS-SNAPID-SAME"):
        return IllegalArgumentException, SNAPID_MSG
    if cell in ("TT-DF-VAS-BRANCH-OPT", "TT-DF-VAS-SELECTOR-ERR"):
        return IllegalArgumentException, BRANCH_MSG
    if cell == "TT-DF-TAS-ASOF-ERR":
        return IllegalArgumentException, ASOF_MSG
    raise AssertionError(f"unknown reader cell {cell!r}")


@pytest.mark.parametrize("cell,version,entry", _reader_err_cases())
def test_reader_refusals(
    spark: Any, seeded: dict[str, Any], cell: str, version: int, entry: str
) -> None:
    """Reader refusals raise the fixture class and message. pins: ice-tt-resolve-1/C-003"""
    seed = seeded["seeds"][version]
    options, branch_suffix = _reader_err_options(cell, seed)
    table = _facade_table(version) + (".branch_b0" if branch_suffix else "")
    expectation, message = _reader_err_expectation(cell, version)
    with pytest.raises(expectation, match=re.escape(message)):
        _reader_option_frames(spark, table, options, entry).collect()


def _facade_query(cell: str, table: str, seed: dict[str, Any]) -> str:
    """Build this run's facade SQL for a query cell."""
    head = f"SELECT id, data, cat FROM {table} "
    tail = " ORDER BY id"
    if cell == "TT-SQL-EXPR-CAST":
        return head + f"TIMESTAMP AS OF CAST('{seed['mid_str']}' AS TIMESTAMP)" + tail
    if cell == "TT-SQL-LIT-TS":
        return head + f"TIMESTAMP AS OF TIMESTAMP '{seed['mid_str']}'" + tail
    if cell == "TT-SQL-STR":
        return head + f"TIMESTAMP AS OF '{seed['mid_str']}'" + tail
    if cell == "TT-SQL-EPOCH-INT":
        return head + f"TIMESTAMP AS OF {seed['sec']}" + tail
    if cell == "TT-SQL-EPOCH-MS-INT":
        return head + f"TIMESTAMP AS OF {seed['mid_ms']}" + tail
    if cell == "TT-SQL-EPOCH-DEC":
        return head + f"TIMESTAMP AS OF {seed['sec']}.5" + tail
    if cell == "TT-SQL-CURRENT-TS":
        return head + "TIMESTAMP AS OF current_timestamp()" + tail
    if cell == "TT-SQL-EXPR-ARITH":
        return (
            head
            + f"TIMESTAMP AS OF CAST('{seed['mid_str']}' AS TIMESTAMP) - INTERVAL 1 DAYS"
            + tail
        )
    if cell == "TT-SQL-TO-TS":
        return head + f"TIMESTAMP AS OF to_timestamp('{seed['mid_str']}')" + tail
    if cell == "TT-SQL-DATE-LIT":
        return head + "TIMESTAMP AS OF DATE '2999-01-01'" + tail
    if cell == "TT-SQL-FST-INT":
        return head + f"FOR SYSTEM_TIME AS OF {seed['sec']}" + tail
    if cell == "TT-SQL-FST-EXPR":
        return head + f"FOR SYSTEM_TIME AS OF CAST('{seed['mid_str']}' AS TIMESTAMP)" + tail
    if cell == "TT-SQL-STR-GARBAGE-ERR":
        return head + "TIMESTAMP AS OF 'garbage'" + tail
    if cell == "TT-SQL-COLREF-ERR":
        return head + "TIMESTAMP AS OF id" + tail
    if cell == "TT-SQL-SUBQ-ERR":
        return head + f"TIMESTAMP AS OF (SELECT CAST('{seed['mid_str']}' AS TIMESTAMP))" + tail
    if cell == "TT-SQL-NULL-ERR":
        return head + "TIMESTAMP AS OF NULL" + tail
    if cell == "TT-SQL-RAND-ERR":
        return (
            head + f"TIMESTAMP AS OF CAST('{seed['mid_str']}' AS TIMESTAMP)"
            " + make_interval(0,0,0,0,0,0,rand())" + tail
        )
    if cell == "TT-SQL-EXPR-NY":
        return head + f"TIMESTAMP AS OF CAST('{seed['mid_ny']}' AS TIMESTAMP)" + tail
    if cell == "TT-SQL-STR-NY":
        return head + f"TIMESTAMP AS OF '{seed['mid_ny']}'" + tail
    if cell == "TT-SQL-EPOCH-NY":
        return head + f"TIMESTAMP AS OF {seed['sec']}" + tail
    if cell == "TT-SQL-VAS-EXPR":
        return head + f"VERSION AS OF {seed['s0']} + 0" + tail
    raise AssertionError(f"unknown query cell {cell!r}")


def _query_ok_cases() -> list[tuple[str, int]]:
    """Every answering query cell on both versions."""
    cells = [
        "TT-SQL-EXPR-CAST",
        "TT-SQL-LIT-TS",
        "TT-SQL-STR",
        "TT-SQL-EPOCH-INT",
        "TT-SQL-EPOCH-MS-INT",
        "TT-SQL-EPOCH-DEC",
        "TT-SQL-CURRENT-TS",
        "TT-SQL-TO-TS",
        "TT-SQL-DATE-LIT",
        "TT-SQL-FST-INT",
        "TT-SQL-FST-EXPR",
        "TT-SQL-SUBQ-ERR",
        "TT-SQL-EXPR-NY",
        "TT-SQL-STR-NY",
        "TT-SQL-EPOCH-NY",
    ]
    return [(cell, version) for cell in cells for version in (2, 3)]


def _query_expected(cell: str, version: int) -> list[tuple[Any, ...]]:
    """Fixture rows for an answering query cell."""
    return [tuple(row) for row in CELLS[f"{cell}-V{version}"]["obs"]["rows"]]


@pytest.mark.parametrize("cell,version", _query_ok_cases())
def test_facade_sql_cells(
    spark: Any, spark_ny: Any, seeded: dict[str, Any], cell: str, version: int
) -> None:
    """Facade SQL cells answer the fixture. pins: ice-tt-resolve-1/C-004"""
    seed = seeded["seeds"][version]
    session = (
        spark_ny
        if cell in ("TT-SQL-EXPR-NY", "TT-SQL-STR-NY", "TT-SQL-EPOCH-NY")
        else spark
    )
    frame = session.sql(_facade_query(cell, _facade_table(version), seed))
    assert _rows_of(frame) == _query_expected(cell, version)


def _query_err_cases() -> list[tuple[str, int]]:
    """Every refusing query cell on both versions."""
    cells = [
        "TT-SQL-EXPR-ARITH",
        "TT-SQL-STR-GARBAGE-ERR",
        "TT-SQL-COLREF-ERR",
        "TT-SQL-NULL-ERR",
        "TT-SQL-RAND-ERR",
        "TT-SQL-VAS-EXPR",
    ]
    return [(cell, version) for cell in cells for version in (2, 3)]


def _query_err_expectation(cell: str, version: int) -> tuple[type[BaseException], str]:
    """Exception class plus stable token for a refusing query cell."""
    suffix = f"-V{version}"
    if cell == "TT-SQL-EXPR-ARITH":
        return IllegalArgumentException, _stable_token(cell + suffix)
    if cell in ("TT-SQL-STR-GARBAGE-ERR", "TT-SQL-NULL-ERR"):
        return AnalysisException, _cell_error(cell + suffix)["getErrorClass"]
    if cell == "TT-SQL-COLREF-ERR":
        return ParseException, _cell_error(cell + suffix)["msg"].split("\n")[0]
    if cell == "TT-SQL-RAND-ERR":
        return AnalysisException, _cell_error(cell + suffix)["getErrorClass"]
    if cell == "TT-SQL-VAS-EXPR":
        return ParseException, "+"
    raise AssertionError(f"unknown query cell {cell!r}")


@pytest.mark.parametrize("cell,version", _query_err_cases())
def test_facade_sql_refusals(spark: Any, seeded: dict[str, Any], cell: str, version: int) -> None:
    """Facade SQL refusals raise the fixture class and token. pins: ice-tt-resolve-1/C-006"""
    seed = seeded["seeds"][version]
    expectation, token = _query_err_expectation(cell, version)
    with pytest.raises(expectation, match=re.escape(token)):
        spark.sql(_facade_query(cell, _facade_table(version), seed)).collect()


def _native_expr_and_expected(cell: str, seed: dict[str, Any]) -> tuple[str, Any]:
    """Bare native select of an answering cell's expression plus its value."""
    from decimal import Decimal

    mid = datetime.datetime.fromtimestamp(seed["mid_ms"] / 1000, tz=UTC).replace(tzinfo=None)
    if cell == "TT-SQL-EXPR-CAST":
        return f"SELECT CAST('{seed['mid_str']}' AS TIMESTAMP) AS v", mid
    if cell == "TT-SQL-LIT-TS":
        return f"SELECT TIMESTAMP '{seed['mid_str']}' AS v", mid
    if cell == "TT-SQL-STR":
        return f"SELECT '{seed['mid_str']}' AS v", seed["mid_str"]
    if cell in ("TT-SQL-EPOCH-INT", "TT-SQL-FST-INT"):
        return f"SELECT {seed['sec']} AS v", seed["sec"]
    if cell == "TT-SQL-EPOCH-MS-INT":
        return f"SELECT {seed['mid_ms']} AS v", seed["mid_ms"]
    if cell == "TT-SQL-EPOCH-DEC":
        return f"SELECT {seed['sec']}.5 AS v", Decimal(f"{seed['sec']}.5")
    if cell == "TT-SQL-CURRENT-TS":
        return "SELECT current_timestamp() AS v", None
    if cell == "TT-SQL-DATE-LIT":
        return "SELECT DATE '2999-01-01' AS v", datetime.date(2999, 1, 1)
    if cell == "TT-SQL-FST-EXPR":
        return f"SELECT CAST('{seed['mid_str']}' AS TIMESTAMP) AS v", mid
    if cell == "TT-SQL-SUBQ-ERR":
        return f"SELECT (SELECT CAST('{seed['mid_str']}' AS TIMESTAMP)) AS v", mid
    raise AssertionError(f"unknown native cell {cell!r}")


@pytest.mark.parametrize(
    "cell",
    [
        "TT-SQL-EXPR-CAST",
        "TT-SQL-LIT-TS",
        "TT-SQL-STR",
        "TT-SQL-EPOCH-INT",
        "TT-SQL-EPOCH-MS-INT",
        "TT-SQL-EPOCH-DEC",
        "TT-SQL-CURRENT-TS",
        "TT-SQL-DATE-LIT",
        "TT-SQL-FST-INT",
        "TT-SQL-FST-EXPR",
        "TT-SQL-SUBQ-ERR",
    ],
)
def test_native_expr_cells(seeded: dict[str, Any], cell: str) -> None:
    """Native bare selects pin each answering expression. pins: ice-tt-resolve-1/C-005"""
    import repark

    seed = seeded["seeds"][2]
    asked, expected = _native_expr_and_expected(cell, seed)
    got = repark.sql(asked).to_arrow().column("v").to_pylist()[0]
    if cell == "TT-SQL-CURRENT-TS":
        assert isinstance(got, datetime.datetime)
        import calendar

        now = calendar.timegm(got.timetuple()) + got.microsecond / 1_000_000
        assert now >= seed["s2_ms"] / 1000
        return
    assert got == expected


def _live_seed(session: Any, table: str, version: int) -> dict[str, Any]:
    """Seed the shared shape on live Spark and resolve this run's values."""
    session.sql(f"DROP TABLE IF EXISTS {table}")
    session.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version'='{version}')"
    )
    session.sql(f"INSERT INTO {table} VALUES (1, 'a', 'x'), (2, 'b', 'y')")
    time.sleep(COMMIT_GAP)
    session.sql(f"INSERT INTO {table} VALUES (3, 'c', 'x')")
    time.sleep(COMMIT_GAP)
    session.sql(f"DELETE FROM {table} WHERE id = 1")
    log = session.sql(
        f"SELECT snapshot_id, committed_at FROM {table}.snapshots ORDER BY committed_at"
    ).collect()
    ids = [int(row["snapshot_id"]) for row in log]
    stamps = [_live_utc(row["committed_at"]) for row in log]
    session.sql(f"ALTER TABLE {table} CREATE TAG t0 AS OF VERSION {ids[0]}")
    session.sql(f"ALTER TABLE {table} CREATE BRANCH b0 AS OF VERSION {ids[1]}")
    mid = stamps[0] + (stamps[1] - stamps[0]) / 2
    ny_zone = datetime.timezone(datetime.timedelta(hours=-4))
    return {
        "s0": ids[0],
        "s1": ids[1],
        "mid_str": mid.strftime("%Y-%m-%d %H:%M:%S.%f"),
        "mid_t": mid.strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "mid_ny": mid.astimezone(ny_zone).strftime("%Y-%m-%d %H:%M:%S.%f"),
        "s1_str": stamps[1].strftime("%Y-%m-%d %H:%M:%S.%f"),
        "sec": int(mid.timestamp()),
        "mid_ms": int(mid.timestamp() * 1000),
    }


def _live_utc(value: Any) -> Any:
    """Normalize a live Spark timestamp cell to UTC."""
    if value.tzinfo is not None:
        return value.astimezone(UTC)
    return value.replace(tzinfo=UTC)


def _live_rows(frame: Any) -> list[list[Any]]:
    """Collect a live Spark frame to fixture-comparable rows ordered by id."""
    names = list(frame.schema.fieldNames())
    rows = [[row[name] for name in names] for row in frame.collect()]
    rows.sort(key=lambda row: row[0])
    return rows


LIVE_TABLE_RE = re.compile(r"(?:ttlive\.ns\.t_v\d|sc\.ns\.t_[a-z0-9_]+)")
LIVE_POSITION_RE = re.compile(r"position \d+")
LIVE_POS_RE = re.compile(r"pos \d+")
LIVE_SQL_ECHO_RE = re.compile(r"== SQL (?:\(line 1, position \{P\}\) )?==\n[^\n]*\n[ \-]*\^+")


def _live_normalize(message: str) -> str:
    """Replace run-varying ids, instants, table echoes, positions, and SQL echo blocks."""
    tabled = LIVE_TABLE_RE.sub("{T}", message)
    placed = LIVE_POSITION_RE.sub("position {P}", tabled)
    posed = LIVE_POS_RE.sub("pos {P}", placed)
    echoed = LIVE_SQL_ECHO_RE.sub("{SQL}", posed)
    return INSTANT_RE.sub("{TS}", re.sub(r"\b\d{10,}\b", "{SID}", echoed))


def _live_error_info(error: BaseException) -> tuple[str, Any, Any, str]:
    """Reduce a live failure to type, condition, error class, and normalized message."""
    condition = None
    error_class = None
    for attr, slot in (("getCondition", "condition"), ("getErrorClass", "error_class")):
        try:
            value = getattr(error, attr)()
        except Exception:
            value = None
        if slot == "condition":
            condition = value
        else:
            error_class = value
    return type(error).__name__, condition, error_class, _live_normalize(str(error).strip())


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_live_cells_rederive_the_fixture(tmp_path: Path) -> None:
    """Live Spark replays every shape and matches the fixture. pins: ice-tt-resolve-1/C-008"""
    import _live_parity as live_parity

    os.environ["TZ"] = "UTC"
    time.tzset()
    warehouse = tmp_path / "live_wh"
    engine = live_parity.build_spark_iceberg_engine(warehouse, catalog="ttlive")
    session = engine.session
    namespace_ready = False
    session.sql("CREATE NAMESPACE IF NOT EXISTS ttlive.ns")
    namespace_ready = True
    try:
        for version in (2, 3):
            table = f"ttlive.ns.t_v{version}"
            seed = _live_seed(session, table, version)
            for cell_id, cell in CELLS.items():
                if not cell_id.endswith(f"-V{version}"):
                    continue
                shape = cell_id.rsplit("-V", 1)[0]
                if shape.startswith("TT-DF-"):
                    action = shape[len("TT-DF-") :]
                    with live_parity.spark_session_conf(
                        engine,
                        (("spark.sql.session.timeZone", "America/New_York"),)
                        if action == "TAS-NY"
                        else (),
                    ):
                        try:
                            if action in ("VAS-TABLE-API", "VAS-TAG-TABLE-API"):
                                ref = "t0" if action == "VAS-TAG-TABLE-API" else seed["s0"]
                                frame = session.read.option("versionAsOf", ref).table(table)
                            elif action == "TAS-TABLE-API":
                                frame = session.read.option("timestampAsOf", seed["mid_str"]).table(
                                    table
                                )
                            elif action == "VAS-SELECTOR-ERR":
                                frame = (
                                    session.read.format("iceberg")
                                    .option("versionAsOf", seed["s0"])
                                    .load(table + ".branch_b0")
                                )
                            else:
                                frame = _live_reader_frame(session, table, seed, action)
                            rows = _live_rows(frame.select("id", "data", "cat"))
                        except Exception as error:
                            _assert_live_error(cell, _live_error_info(error))
                            continue
                    if cell["status"] != "ok":
                        raise AssertionError(
                            f"{cell_id}: live Spark answered but the fixture refuses"
                        )
                    assert rows == cell["obs"]["rows"], f"{cell_id}: live rows differ from fixture"
                else:
                    action = shape[len("TT-SQL-") :]
                    template = _live_template(action)
                    with live_parity.spark_session_conf(
                        engine,
                        (("spark.sql.session.timeZone", "America/New_York"),)
                        if action in ("EXPR-NY", "STR-NY", "EPOCH-NY")
                        else (),
                    ):
                        try:
                            rows = _live_rows(session.sql(_fill_template(template, table, seed)))
                        except Exception as error:
                            _assert_live_error(cell, _live_error_info(error))
                            continue
                    if cell["status"] != "ok":
                        raise AssertionError(
                            f"{cell_id}: live Spark answered but the fixture refuses"
                        )
                    assert rows == cell["obs"]["rows"], f"{cell_id}: live rows differ from fixture"
    finally:
        if namespace_ready:
            for version in (2, 3):
                session.sql(f"DROP TABLE IF EXISTS ttlive.ns.t_v{version}")
            session.sql("DROP NAMESPACE IF EXISTS ttlive.ns CASCADE")


def _assert_live_error(cell: dict[str, Any], info: tuple[str, Any, Any, str]) -> None:
    """Check a live refusal against the fixture error shape."""
    if cell["status"] != "error":
        raise AssertionError(f"{cell['id']}: live Spark refused but the fixture answers")
    want = cell["error"]
    got_type, got_condition, got_class, got_msg = info
    assert got_type == want["type"], f"{cell['id']}: live error {got_type!r} != {want['type']!r}"
    assert got_condition == want["getCondition"], f"{cell['id']}: condition drift"
    assert got_class == want["getErrorClass"], f"{cell['id']}: error-class drift"
    assert got_msg == _live_normalize(want["msg"]), f"{cell['id']}: message drift"


def _live_reader_frame(session: Any, table: str, seed: dict[str, Any], action: str) -> Any:
    """Run one live reader shape through format iceberg load."""
    base = session.read.format("iceberg")
    if action == "VAS-INT":
        return base.option("versionAsOf", seed["s0"]).load(table)
    if action == "VAS-STR":
        return base.option("versionAsOf", str(seed["s0"])).load(table)
    if action == "VAS-TAG":
        return base.option("versionAsOf", "t0").load(table)
    if action == "VAS-BRANCH":
        return base.option("versionAsOf", "b0").load(table)
    if action == "VAS-MAIN":
        return base.option("versionAsOf", "main").load(table)
    if action == "VAS-MISSING-ERR":
        return base.option("versionAsOf", 12345).load(table)
    if action == "VAS-UNKNOWN-REF-ERR":
        return base.option("versionAsOf", "nope").load(table)
    if action == "VAS-LOWER":
        return base.option("versionasof", seed["s0"]).load(table)
    if action == "TAS-STR":
        return base.option("timestampAsOf", seed["mid_str"]).load(table)
    if action == "TAS-STR-EXACT":
        return base.option("timestampAsOf", seed["s1_str"]).load(table)
    if action == "TAS-STR-T":
        return base.option("timestampAsOf", seed["mid_t"]).load(table)
    if action == "TAS-DATE":
        return base.option("timestampAsOf", "2999-01-01").load(table)
    if action == "TAS-BEFORE-ERR":
        return base.option("timestampAsOf", "2000-01-01 00:00:00").load(table)
    if action == "TAS-GARBAGE-ERR":
        return base.option("timestampAsOf", "not a ts").load(table)
    if action == "TAS-EPOCH-SEC":
        return base.option("timestampAsOf", str(seed["sec"])).load(table)
    if action == "TAS-EPOCH-MS":
        return base.option("timestampAsOf", str(seed["mid_ms"])).load(table)
    if action == "TAS-NY":
        return base.option("timestampAsOf", seed["mid_ny"]).load(table)
    if action == "VAS-TAS-ERR":
        return (
            base.option("versionAsOf", seed["s0"])
            .option("timestampAsOf", seed["mid_str"])
            .load(table)
        )
    if action == "VAS-SNAPID-ERR":
        return base.option("versionAsOf", seed["s0"]).option("snapshot-id", seed["s1"]).load(table)
    if action == "VAS-SNAPID-SAME":
        return base.option("versionAsOf", seed["s0"]).option("snapshot-id", seed["s0"]).load(table)
    if action == "VAS-BRANCH-OPT":
        return base.option("versionAsOf", seed["s0"]).option("branch", "b0").load(table)
    if action == "TAS-ASOF-ERR":
        return (
            base.option("timestampAsOf", seed["mid_str"])
            .option("as-of-timestamp", seed["mid_ms"])
            .load(table)
        )
    raise AssertionError(f"unknown live reader action {action!r}")


def _live_template(action: str) -> str:
    """Return the Spark SQL template for a live query shape."""
    templates = {
        "EXPR-CAST": "SELECT * FROM {T} TIMESTAMP AS OF CAST('{M1}' AS TIMESTAMP)",
        "LIT-TS": "SELECT * FROM {T} TIMESTAMP AS OF TIMESTAMP '{M1}'",
        "STR": "SELECT * FROM {T} TIMESTAMP AS OF '{M1}'",
        "EPOCH-INT": "SELECT * FROM {T} TIMESTAMP AS OF {SEC1}",
        "EPOCH-MS-INT": "SELECT * FROM {T} TIMESTAMP AS OF {MS1}",
        "EPOCH-DEC": "SELECT * FROM {T} TIMESTAMP AS OF {SEC1}.5",
        "CURRENT-TS": "SELECT * FROM {T} TIMESTAMP AS OF current_timestamp()",
        "EXPR-ARITH": (
            "SELECT * FROM {T} TIMESTAMP AS OF CAST('{M1}' AS TIMESTAMP) - INTERVAL 1 DAYS"
        ),
        "TO-TS": "SELECT * FROM {T} TIMESTAMP AS OF to_timestamp('{M1}')",
        "DATE-LIT": "SELECT * FROM {T} TIMESTAMP AS OF DATE '2999-01-01'",
        "FST-INT": "SELECT * FROM {T} FOR SYSTEM_TIME AS OF {SEC1}",
        "FST-EXPR": "SELECT * FROM {T} FOR SYSTEM_TIME AS OF CAST('{M1}' AS TIMESTAMP)",
        "STR-GARBAGE-ERR": "SELECT * FROM {T} TIMESTAMP AS OF 'garbage'",
        "COLREF-ERR": "SELECT * FROM {T} TIMESTAMP AS OF id",
        "SUBQ-ERR": "SELECT * FROM {T} TIMESTAMP AS OF (SELECT CAST('{M1}' AS TIMESTAMP))",
        "NULL-ERR": "SELECT * FROM {T} TIMESTAMP AS OF NULL",
        "RAND-ERR": (
            "SELECT * FROM {T} TIMESTAMP AS OF CAST('{M1}' AS TIMESTAMP)"
            " + make_interval(0,0,0,0,0,0,rand())"
        ),
        "EXPR-NY": "SELECT * FROM {T} TIMESTAMP AS OF CAST('{M1NY}' AS TIMESTAMP)",
        "STR-NY": "SELECT * FROM {T} TIMESTAMP AS OF '{M1NY}'",
        "EPOCH-NY": "SELECT * FROM {T} TIMESTAMP AS OF {SEC1}",
        "VAS-EXPR": "SELECT * FROM {T} VERSION AS OF {S0} + 0",
    }
    return templates[action]


def _fill_template(template: str, table: str, seed: dict[str, Any]) -> str:
    """Fill a live query template from a live seed."""
    return template.format(
        T=table,
        S0=seed["s0"],
        S1=seed["s1"],
        M1=seed["mid_str"],
        M1NY=seed["mid_ny"],
        SEC1=seed["sec"],
        MS1=seed["mid_ms"],
    )

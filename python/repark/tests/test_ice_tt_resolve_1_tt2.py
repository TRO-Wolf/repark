"""ICE-TT-RESOLVE-1 TT2 critic cells — offline pins on the two-snapshot shape.

Companion to ``test_ice_tt_resolve_1``: the 94-cell suite owns the shared
seed, so this module seeds the two-snapshot critic shape per test instead.
Fixtures stay local because ruff F401 fires on fixture-only imports.

pins: ice-tt-resolve-1/C-002, C-003
"""

from __future__ import annotations

import datetime
import json
import re
import time
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, IllegalArgumentException, PySparkException
from repark.spark.session import _reset_active_session_for_tests

FIXTURE: dict[str, Any] = json.loads(
    Path(__file__).with_name("ice_tt_resolve_1_spark_oracle.json").read_text(encoding="utf-8")
)
CELLS: dict[str, dict[str, Any]] = {cell["id"]: cell for cell in FIXTURE["cells"]}
UTC = datetime.UTC
COMMIT_GAP = 2.2


def _cell_error(cell_id: str) -> dict[str, Any]:
    """Fixture error payload for a refusing cell."""
    return CELLS[cell_id]["error"]


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


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """One warehouse directory shared by every session in this module."""
    return tmp_path_factory.mktemp("tt2_wh")


@pytest.fixture()
def spark(warehouse: Path) -> Any:
    """UTC facade session on a fresh memory catalog."""
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-ice-tt-resolve-1-tt2")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog("mem", warehouse / "wh")
    session.sql("CREATE NAMESPACE mem.ns")
    yield session
    session.stop()


@pytest.fixture()
def spark_ny(spark: Any) -> Any:
    """New York facade session sharing the catalog."""
    session = spark.newSession()
    session.conf.set("spark.sql.session.timeZone", "America/New_York")
    yield session
    session.stop()


def _seed_tt2_table(session: Any, table: str, version: int) -> dict[str, Any]:
    """Build the two-snapshot critic shape with tag t0 and branch b0 on S0."""
    session.sql(f"DROP TABLE IF EXISTS {table}")
    session.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version'='{version}')"
    )
    session.sql(f"INSERT INTO {table} VALUES (1, 'a', 'x'), (2, 'b', 'y')")
    time.sleep(COMMIT_GAP)
    session.sql(f"INSERT INTO {table} VALUES (3, 'c', 'x')")
    snaps = session._testing_list_snapshots(table)
    assert len(snaps) == 2
    ids = [int(pair[0]) for pair in snaps]
    stamps = [int(pair[1]) for pair in snaps]
    assert stamps[1] - stamps[0] >= 2000
    session._testing_create_ref(table, "tag", "t0", ids[0])
    session._testing_create_ref(table, "branch", "b0", ids[0])
    mid_ms = stamps[0] + (stamps[1] - stamps[0]) // 2
    mid = datetime.datetime.fromtimestamp(mid_ms / 1000, tz=UTC)
    return {"s0": ids[0], "mid_str": mid.strftime("%Y-%m-%d %H:%M:%S.%f")}


def _tt2_query(cell: str, table: str, seed: dict[str, Any]) -> str:
    """Build this run's facade SQL for a TT2 non-determinism cell."""
    mid = seed["mid_str"]
    if cell == "TT2-RANDOM-ERR":
        return (
            f"SELECT * FROM {table} TIMESTAMP AS OF CAST('{mid}' AS TIMESTAMP)"
            " + make_interval(0,0,0,0,0,0,random())"
        )
    if cell == "TT2-UUID-ERR":
        return (
            f"SELECT * FROM {table} TIMESTAMP AS OF CAST(concat('{mid}', "
            "substr(uuid(), 1, 0)) AS TIMESTAMP)"
        )
    if cell == "TT2-WITH-RAND-SUBQ":
        return (
            f"SELECT * FROM {table} TIMESTAMP AS OF (WITH z AS (SELECT rand() AS r) "
            f"SELECT CAST('{mid}' AS TIMESTAMP) + make_interval(0,0,0,0,0,0,r) FROM z)"
        )
    raise AssertionError(f"unknown TT2 cell {cell!r}")


@pytest.mark.parametrize("cell", ["TT2-RANDOM-ERR", "TT2-UUID-ERR", "TT2-WITH-RAND-SUBQ"])
@pytest.mark.parametrize("version", [2, 3])
def test_facade_sql_tt2_nondeterministic(spark: Any, cell: str, version: int) -> None:
    """TT2 volatile expressions refuse by plan volatility. pins: ice-tt-resolve-1/C-003"""
    table = f"mem.ns.tt2_v{version}"
    seed = _seed_tt2_table(spark, table, version)
    if cell == "TT2-WITH-RAND-SUBQ":
        with pytest.raises(AnalysisException):
            spark.sql(_tt2_query(cell, table, seed)).collect()
    else:
        with pytest.raises(AnalysisException, match=re.escape(_cell_error(cell)["getErrorClass"])):
            spark.sql(_tt2_query(cell, table, seed)).collect()


def _tt2_scalar(session: Any, sql: str) -> list[Any]:
    """Collect a single-row scalar SELECT as plain values."""
    frame = session.sql(sql).to_arrow().to_pylist()
    assert len(frame) == 1
    return list(frame[0].values())


def _tt2_rows(cell: str) -> list[tuple[Any, ...]]:
    """Fixture rows for a passing TT2 cell."""
    return [tuple(row) for row in CELLS[cell]["obs"]["rows"]]


_TT2_CAST_OK = [
    (
        "TT2-CAST-PLUS0000",
        "SELECT unix_timestamp(CAST('2020-06-01 00:00:00+0000' AS TIMESTAMP))",
        "NY",
    ),
    (
        "TT2-CAST-DST-GAP",
        "SELECT unix_timestamp(CAST('2026-03-08 02:30:00' AS TIMESTAMP))",
        "NY",
    ),
    (
        "TT2-CAST-DST-OVERLAP",
        "SELECT unix_timestamp(CAST('2026-11-01 01:30:00' AS TIMESTAMP))",
        "NY",
    ),
    (
        "TT2-CAST-NANOS",
        "SELECT CAST(CAST('2020-06-01 00:00:00.123456789' AS TIMESTAMP) AS STRING)",
        "UTC",
    ),
]

_TT2_CAST_ERR = [
    (
        "TT2-CAST-DATE-Z",
        "SELECT CAST(CAST('2020-06-01Z' AS TIMESTAMP) AS STRING),"
        " unix_timestamp(CAST('2020-06-01Z' AS TIMESTAMP))",
    ),
    (
        "TT2-CAST-NOSEC-Z",
        "SELECT unix_timestamp(CAST('2020-06-01T00:00Z' AS TIMESTAMP))",
    ),
]

_TT2_CAST_GAP = [
    (
        "TT2-CAST-SHORT",
        "SELECT unix_timestamp(CAST('2020-6-1 1:2:3' AS TIMESTAMP))",
    ),
    (
        "TT2-CAST-YEAR-ONLY",
        "SELECT unix_timestamp(CAST('2020' AS TIMESTAMP))",
    ),
]


@pytest.mark.parametrize("cell,sql,zone", _TT2_CAST_OK)
def test_facade_sql_tt2_cast_cells(
    spark: Any, spark_ny: Any, cell: str, sql: str, zone: str
) -> None:
    """TT2 string casts answer through the engine CAST. pins: ice-tt-resolve-1/C-002"""
    session = spark_ny if zone == "NY" else spark
    assert tuple(_tt2_scalar(session, sql)) == _tt2_rows(cell)[0]


@pytest.mark.parametrize("cell,sql", _TT2_CAST_ERR)
def test_facade_sql_tt2_cast_refusals(spark_ny: Any, cell: str, sql: str) -> None:
    """TT2 Z-suffixed casts refuse CAST_INVALID_INPUT. pins: ice-tt-resolve-1/C-002"""
    with pytest.raises(PySparkException, match=re.escape(_cell_error(cell)["getErrorClass"])):
        spark_ny.sql(sql).to_arrow()


@pytest.mark.parametrize("cell,sql", _TT2_CAST_GAP)
@pytest.mark.xfail(
    strict=True,
    reason="engine Spark CAST refuses short/year-only strings the oracle accepts; "
    "cast finding, do not special-case time travel",
)
def test_facade_sql_tt2_cast_engine_gap(spark: Any, cell: str, sql: str) -> None:
    """TT2 short/year-only casts pin the Spark answer. pins: ice-tt-resolve-1/C-002"""
    assert tuple(_tt2_scalar(spark, sql)) == _tt2_rows(cell)[0]


@pytest.mark.parametrize("version", [2, 3])
def test_facade_sql_tt2_date_z_future(spark: Any, version: int) -> None:
    """TT2 Z-suffixed AS OF refuses INPUT. pins: ice-tt-resolve-1/C-003"""
    table = f"mem.ns.tt2z_v{version}"
    _seed_tt2_table(spark, table, version)
    with pytest.raises(
        AnalysisException,
        match=re.escape(_cell_error("TT2-DATE-Z-FUTURE")["getErrorClass"]),
    ):
        spark.sql(f"SELECT * FROM {table} TIMESTAMP AS OF '2999-01-01Z'").collect()


@pytest.mark.parametrize("version", [2, 3])
def test_reader_tt2_timestamp_as_of_refusal(spark: Any, version: int) -> None:
    """TT2 reader timestampAsOf Z-strings refuse INPUT. pins: ice-tt-resolve-1/C-002"""
    table = f"mem.ns.tt2tas_v{version}"
    _seed_tt2_table(spark, table, version)
    with pytest.raises(
        AnalysisException,
        match=re.escape(_cell_error("TT2-TAS-DATE-Z")["getErrorClass"]),
    ):
        spark.read.format("iceberg").option("timestampAsOf", "2999-01-01Z").load(table).collect()


@pytest.mark.parametrize("version", [2, 3])
@pytest.mark.xfail(
    strict=True,
    reason="engine Spark CAST refuses no-seconds walls the oracle answers; "
    "cast finding, do not special-case time travel",
)
def test_reader_tt2_timestamp_as_of_nosec(spark: Any, version: int) -> None:
    """TT2 no-seconds timestampAsOf pins the Spark rows. pins: ice-tt-resolve-1/C-002"""
    table = f"mem.ns.tt2tas_v{version}"
    _seed_tt2_table(spark, table, version)
    frame = (
        spark.read.format("iceberg")
        .option("timestampAsOf", "2999-01-01 00:00")
        .load(table)
        .select("id", "data", "cat")
        .orderBy("id")
    )
    assert _rows_of(frame) == [tuple(row) for row in CELLS["TT2-TAS-NOSEC"]["obs"]["rows"]]


@pytest.mark.parametrize("version", [2, 3])
def test_facade_sql_tt2_alias_join(spark: Any, version: int) -> None:
    """TT2 aliased self-join pins both sides rows. pins: ice-tt-resolve-1/C-002"""
    table = f"mem.ns.tt2alias_v{version}"
    seed = _seed_tt2_table(spark, table, version)
    mid = seed["mid_str"]
    frame = spark.sql(
        f"SELECT a.id, b.id FROM {table} TIMESTAMP AS OF CAST('{mid}' AS TIMESTAMP) a "
        f"FULL OUTER JOIN {table} TIMESTAMP AS OF '2261-01-01' b ON a.id = b.id "
        "ORDER BY b.id"
    )
    arrow = frame.to_arrow()
    rows = list(zip(arrow.column(0).to_pylist(), arrow.column(1).to_pylist(), strict=True))
    assert rows == _tt2_rows("TT2-SQL-ALIAS-JOIN")


@pytest.mark.parametrize("version", [2, 3])
def test_facade_sql_tt2_alias_as(spark: Any, version: int) -> None:
    """TT2 AS-alias on the pinned relation answers S0. pins: ice-tt-resolve-1/C-002"""
    table = f"mem.ns.tt2alias_as_v{version}"
    seed = _seed_tt2_table(spark, table, version)
    frame = spark.sql(
        f"SELECT t2.id FROM {table} TIMESTAMP AS OF '{seed['mid_str']}' AS t2 "
        "WHERE t2.id > 0 ORDER BY t2.id"
    )
    ids = [(value,) for value in frame.to_arrow().column(0).to_pylist()]
    assert ids == _tt2_rows("TT2-SQL-ALIAS-AS")


@pytest.mark.parametrize("version", [2, 3])
def test_facade_sql_tt2_version_alias(spark: Any, version: int) -> None:
    """TT2 bare alias on VERSION AS OF answers S0. pins: ice-tt-resolve-1/C-002"""
    table = f"mem.ns.tt2aliasver_v{version}"
    seed = _seed_tt2_table(spark, table, version)
    frame = spark.sql(f"SELECT t2.id FROM {table} VERSION AS OF {seed['s0']} t2 ORDER BY t2.id")
    ids = [(value,) for value in frame.to_arrow().column(0).to_pylist()]
    assert ids == _tt2_rows("TT2-SQL-VERSION-ALIAS")


@pytest.mark.parametrize("version", [2, 3])
def test_reader_tt2_version_as_of_on_tag_selector(spark: Any, version: int) -> None:
    """TT2 versionAsOf on a tag selector refuses. pins: ice-tt-resolve-1/C-002"""
    table = f"mem.ns.tt2seltag_v{version}"
    seed = _seed_tt2_table(spark, table, version)
    with pytest.raises(
        IllegalArgumentException,
        match=re.escape(_cell_error("TT2-TAS-TAG-SELECTOR-ERR")["msg"]),
    ):
        (
            spark.read.format("iceberg")
            .option("versionAsOf", seed["s0"])
            .load(table + ".tag_t0")
            .collect()
        )


@pytest.mark.parametrize("version", [2, 3])
def test_reader_tt2_timestamp_as_of_on_branch_selector(spark: Any, version: int) -> None:
    """TT2 timestampAsOf on a branch selector refuses. pins: ice-tt-resolve-1/C-002"""
    table = f"mem.ns.tt2selbranch_v{version}"
    seed = _seed_tt2_table(spark, table, version)
    with pytest.raises(
        IllegalArgumentException,
        match=re.escape(_cell_error("TT2-TAS-TS-TAG-SELECTOR-ERR")["msg"]),
    ):
        (
            spark.read.format("iceberg")
            .option("timestampAsOf", seed["mid_str"])
            .load(table + ".branch_b0")
            .collect()
        )


@pytest.mark.parametrize("version", [2, 3])
def test_facade_sql_tt2_ts_tag_selector(spark: Any, version: int) -> None:
    """TT2 SQL time travel on a tag selector refuses. pins: ice-tt-resolve-1/C-002"""
    table = f"mem.ns.tt2sqltag_v{version}"
    _seed_tt2_table(spark, table, version)
    with pytest.raises(
        IllegalArgumentException,
        match=re.escape(_cell_error("TT2-SQL-TS-TAG-SELECTOR")["msg"]),
    ):
        spark.sql(f"SELECT * FROM {table}.tag_t0 TIMESTAMP AS OF '2999-01-01'").collect()


@pytest.mark.parametrize("version", [2, 3])
def test_facade_sql_tt2_vas_branch_selector(spark: Any, version: int) -> None:
    """TT2 SQL version travel on a branch selector refuses. pins: ice-tt-resolve-1/C-002"""
    table = f"mem.ns.tt2sqlbranch_v{version}"
    seed = _seed_tt2_table(spark, table, version)
    with pytest.raises(
        IllegalArgumentException,
        match=re.escape(_cell_error("TT2-SQL-VAS-BRANCH-SELECTOR")["msg"]),
    ):
        spark.sql(f"SELECT * FROM {table}.branch_b0 VERSION AS OF {seed['s0']}").collect()

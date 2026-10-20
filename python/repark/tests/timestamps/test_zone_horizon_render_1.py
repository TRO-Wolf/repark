"""ZONE-HORIZON-RENDER-1 — an instant after 2099 renders at Spark's wall clock.

chrono-tz tabulates zone transitions up to 2099 and Java applies a zone's final rule for
ever. Before this unit only the wall-clock to instant direction of the ``TIMESTAMP``
literal read the shared horizon, so an instant after 2099 in a shifting zone rendered at
standard time. Every cell reads its expected answer from
``zone_horizon_render_1_spark_oracle.json``: six session zones x the years 2099, 2100,
2104, 2500 and 9999 x a January and a July instant plus both sides of each transition x
46 instant functions, the same walls x 16 wall-clock functions, and a CSV and a JSON write
per zone.

``zone_horizon_render_1_residue.json`` lists the cells RePark still answers differently.
Each either differs the same way in the 2099 control or belongs to a named class: the
built-in ``extract`` and ``date_part`` never read the session zone; ``from_utc_timestamp``
and ``to_utc_timestamp`` are an upstream kernel this unit does not own, so they still read
standard time after 2099; ``make_timestamp``, the ``TIMESTAMP_NTZ`` literal and
``unix_timestamp`` stop at the nanosecond range (2262); and ``months_between`` reads the
wall clock on a transition day where Spark reads elapsed seconds. A
residue cell that starts to agree with Spark turns its zone's pin red, so the list cannot
go stale.

The live tier re-runs the committed probes on live Spark and asserts the fixture.

pins: zone-horizon-render-1/C-001, C-002, C-005, C-006, C-007, C-008, C-009, C-010
"""

from __future__ import annotations

import datetime
import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from _record_zone_horizon_render_1 import ZONES, answer, cell_key, oracle_cells, record

from repark import ReparkSession

_HERE = Path(__file__).resolve().parent
_ORACLE: dict[str, Any] = json.loads(
    (_HERE / "zone_horizon_render_1_spark_oracle.json").read_text(encoding="utf-8")
)
_RESIDUE: dict[str, list[str]] = json.loads(
    (_HERE / "zone_horizon_render_1_residue.json").read_text(encoding="utf-8")
)
_CELLS: list[dict[str, Any]] = oracle_cells(_ORACLE)
_CONTROL_YEAR = 2099
_ZONE_BLIND = frozenset(
    ("extract_hour", "extract_minute", "extract_doy", "date_part_hour", "date_part_day")
)
_UPSTREAM_SHIFT = frozenset(
    (
        "from_utc_timestamp",
        "to_utc_timestamp",
        "from_utc_timestamp_wall",
        "to_utc_timestamp_wall",
    )
)
_NANOSECOND_RANGE = frozenset(
    (
        "make_timestamp",
        "make_timestamp_zone",
        "ntz_to_ltz",
        "convert_from_utc",
        "convert_to_utc",
        "unix_timestamp",
    )
)
_NANOSECOND_LAST_YEAR = 2262
_ELAPSED_IN_DAY = frozenset(("months_between",))
_CARD_ZONES = ("America/New_York", "Australia/Sydney", "Australia/Lord_Howe", "Asia/Kolkata")
_CARD_YEARS = (2099, 2100, 2104, 2500, 9999)


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """A fresh facade session with ANSI on; each test sets the zone it needs."""
    session = ReparkSession.builder.appName("zone-horizon-render-1").getOrCreate()
    session.conf.set("spark.sql.ansi.enabled", "true")
    yield session


def _residue_keys() -> set[str]:
    """Return every residue cell key, over all zones."""
    return {key for keys in _RESIDUE.values() for key in keys}


@pytest.mark.parametrize("zone", ZONES)
def test_every_cell_outside_the_residue_answers_spark(spark: ReparkSession, zone: str) -> None:
    """One zone's cells answer the oracle, and its residue cells still differ.

    pins: zone-horizon-render-1/C-005, C-006, C-007, C-008
    """
    cells = [cell for cell in _CELLS if cell["zone"] == zone]
    answers = answer(spark, cells)
    differing = sorted(
        cell_key(cell)
        for cell, result in zip(cells, answers, strict=True)
        if result != cell["spark"]
    )
    assert differing == sorted(_RESIDUE[zone])


def test_every_residue_cell_is_explained() -> None:
    """A residue cell after 2099 differs in the 2099 control too, or is in a named class.

    pins: zone-horizon-render-1/C-009
    """
    residue = _residue_keys()
    unexplained = []
    for cell in _CELLS:
        key = cell_key(cell)
        if key not in residue or cell["year"] == _CONTROL_YEAR or "frame" in cell:
            continue
        control = cell_key({**cell, "year": _CONTROL_YEAR})
        beyond_nanoseconds = (
            cell["fn"] in _NANOSECOND_RANGE and cell["year"] > _NANOSECOND_LAST_YEAR
        )
        on_a_transition_day = cell["fn"] in _ELAPSED_IN_DAY and cell["probe"].endswith("_at")
        named = (
            cell["fn"] in _ZONE_BLIND | _UPSTREAM_SHIFT or beyond_nanoseconds or on_a_transition_day
        )
        if not named and control not in residue:
            unexplained.append(key)
    assert unexplained == []
    assert not [key for key in residue if key.split("|")[3] in ("csv", "json")]


@pytest.mark.parametrize("zone", _CARD_ZONES)
def test_the_three_expressions_of_the_card_answer_noon(spark: ReparkSession, zone: str) -> None:
    """The cast to string, ``hour`` and ``date_format`` agree with the literal's wall clock.

    pins: zone-horizon-render-1/C-001, C-002
    """
    spark.conf.set("spark.sql.session.timeZone", zone)
    for year in _CARD_YEARS:
        literal = f"TIMESTAMP '{year}-07-01 12:00:00'"
        row = spark.sql(
            f"SELECT CAST({literal} AS STRING), hour({literal}), date_format({literal}, 'HH:mm')"
        ).collect()[0]
        assert tuple(row) == (f"{year}-07-01 12:00:00", 12, "12:00"), (zone, year)


def test_collect_and_the_arrow_path_carry_the_same_instant(spark: ReparkSession) -> None:
    """``collect`` gives the session wall clock and Arrow gives the instant, as before.

    pins: zone-horizon-render-1/C-010
    """
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    frame = spark.sql("SELECT TIMESTAMP '2100-07-01 12:00:00' AS t")
    assert frame.collect()[0][0] == datetime.datetime(2100, 7, 1, 12, 0)
    table = frame.to_arrow()
    assert table.schema.field("t").type == pa.timestamp("us", tz="UTC")
    assert table.column("t").cast(pa.int64()).to_pylist() == [4_118_140_800_000_000]


@pytest.mark.skipif(
    os.environ.get("REPARK_PARITY_LIVE") != "1",
    reason="REPARK_PARITY_LIVE != 1 — live Spark cell skipped (routine CI is JVM-free)",
)
def test_live_oracle_matches_committed_fixture(spark_engine: Any) -> None:
    """Live Spark 4.1.2 re-derives every fixture cell from the committed probes.

    pins: zone-horizon-render-1/C-005
    """
    session = spark_engine.session
    ansi = session.conf.get("spark.sql.ansi.enabled")
    session.conf.set("spark.sql.ansi.enabled", "true")
    try:
        live = record(session, _ORACLE)
    finally:
        session.conf.set("spark.sql.ansi.enabled", ansi)
    assert live["answers"] == _ORACLE["answers"]

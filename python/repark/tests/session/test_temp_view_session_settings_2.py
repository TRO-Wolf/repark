from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import _temp_view_session_settings_2_cells as cells
import pytest
from test_temp_view_session_settings_1 import assert_answer, build, same_as_spark

from repark.spark.session import _reset_active_session_for_tests

pytestmark = pytest.mark.xdist_group("temp_view_session_settings_2")
HERE = Path(__file__)
ORACLE = json.loads(HERE.with_name("temp_view_session_settings_2_spark_oracle.json").read_text())
PLANS = json.loads(HERE.with_name("temp_view_session_settings_2_plans.json").read_text())
STANDING = json.loads(HERE.with_name("temp_view_session_settings_2_standing.json").read_text())
KEYS = sorted(ORACLE["true"])
UNSET_KEYS = sorted(ORACLE["unset"])


def standing(key: str, mode: str) -> tuple[str, dict[str, Any]] | None:
    held = STANDING[mode].get(key)
    if held is None:
        return None
    return STANDING["classes"][held["class"]], held["answer"]


def measure(root: Path, mode: str, what: str) -> Any:
    session = build(root, mode)
    table = cells.Cells(session, str(root), mode)
    found = table.run() if what == "cells" else table.explain()
    session.stop()
    _reset_active_session_for_tests()
    return found


@pytest.fixture(scope="module", params=["true", "false"])
def measured(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[tuple[str, dict[str, Any]]]:
    root = tmp_path_factory.mktemp(f"tvs2-{request.param}")
    yield request.param, measure(root, request.param, "cells")


@pytest.fixture(scope="module")
def unset_measured(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    return measure(tmp_path_factory.mktemp("tvs2-unset"), "unset", "cells")


def assert_standing(found: dict[str, Any], want: dict[str, Any], key: str, reason: str) -> None:
    if "error" in want:
        assert "error" in found, (key, reason, found)
        assert found["error"] == want["error"], (key, reason, found)
        return
    assert "error" not in found, (key, reason, found)
    assert found["rows"] == want["rows"], (key, reason, found)


@pytest.mark.parametrize("key", KEYS)
def test_a_dataframe_door_cell_answers_as_spark(
    measured: tuple[str, dict[str, Any]], key: str
) -> None:
    mode, found = measured
    held = standing(key, mode)
    if held is None:
        assert_answer(found[key], ORACLE[mode][key], key)
        return
    reason, want = held
    assert not same_as_spark(ORACLE[mode][key], found[key]), (key, reason)
    assert_standing(found[key], want, key, reason)


@pytest.mark.parametrize("key", UNSET_KEYS)
def test_an_unset_flag_is_captured_from_the_frame(unset_measured: dict[str, Any], key: str) -> None:
    assert_answer(unset_measured[key], ORACLE["unset"][key], key)


def test_matching_settings_keep_todays_plans(tmp_path: Path) -> None:
    plans = measure(tmp_path, "true", "explain")
    assert plans == PLANS["true"]


NEGATED_NULL_SOURCES = {
    "bare": "SELECT 127 AS id, -NULL AS c",
    "derived": "SELECT id, c FROM (SELECT 127 AS id, -NULL AS c) d",
}
CANNOT_SAFELY_CAST = "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]"


def write_to(session: Any, frame: Any, table: str) -> None:
    frame.writeTo(table).append()


def insert_into(session: Any, frame: Any, table: str) -> None:
    frame.write.insertInto(table)


def sql_insert_from_view(session: Any, frame: Any, table: str) -> None:
    frame.createOrReplaceTempView("negated_null_source")
    session.sql(f"INSERT INTO {table} SELECT * FROM negated_null_source").collect()


NEGATED_NULL_DOORS = {
    "writeTo": write_to,
    "insertInto": insert_into,
    "sqlInsertFromView": sql_insert_from_view,
}
NEGATED_NULL_CELLS = [
    (shape, target, made)
    for shape in sorted(NEGATED_NULL_SOURCES)
    for target in ("DATE", "BOOLEAN")
    for made in ("matching", "other_ansi", "other_zone")
]


def made_frame(session: Any, ansi: str, made: str, sql: str) -> Any:
    if made == "matching":
        return session.sql(sql)
    key, value, back = {
        "other_ansi": ("spark.sql.ansi.enabled", "false" if ansi == "true" else "true", ansi),
        "other_zone": ("spark.sql.session.timeZone", "America/Los_Angeles", "UTC"),
    }[made]
    session.sql(f"SET {key}={value}").collect()
    frame = session.sql(sql)
    frame.collect()
    session.sql(f"SET {key}={back}").collect()
    return frame


def negated_null_answer(session: Any, ansi: str, door: str, cell: tuple[str, str, str]) -> str:
    shape, target, made = cell
    table = f"lc.nn.{door}_{shape}_{target}_{made}".lower()
    session.sql(f"CREATE TABLE {table} (id INT, c {target}) USING iceberg").collect()
    frame = made_frame(session, ansi, made, NEGATED_NULL_SOURCES[shape])
    try:
        NEGATED_NULL_DOORS[door](session, frame, table)
    except Exception as error:
        written = session.sql(f"SELECT * FROM {table}").collect()
        return "refused" if CANNOT_SAFELY_CAST in str(error) and not written else str(error)
    return f"wrote {session.sql(f'SELECT * FROM {table}').collect()}"


@pytest.mark.parametrize("door", sorted(NEGATED_NULL_DOORS))
@pytest.mark.parametrize("ansi", ["true", "false"])
def test_a_negated_null_write_refuses_as_spark(tmp_path: Path, ansi: str, door: str) -> None:
    session = build(tmp_path, ansi)
    try:
        session.sql("CREATE NAMESPACE IF NOT EXISTS lc.nn").collect()
        found = {
            cell: negated_null_answer(session, ansi, door, cell) for cell in NEGATED_NULL_CELLS
        }
    finally:
        session.stop()
        _reset_active_session_for_tests()
    assert found == dict.fromkeys(NEGATED_NULL_CELLS, "refused")

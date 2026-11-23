from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import _temp_view_session_settings_1_cells as cells
import pytest

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

pytestmark = pytest.mark.xdist_group("temp_view_session_settings_1")
HERE = Path(__file__)
ORACLE = json.loads(HERE.with_name("temp_view_session_settings_1_spark_oracle.json").read_text())
PLANS = json.loads(HERE.with_name("temp_view_session_settings_1_plans.json").read_text())
SWEEP = json.loads(HERE.with_name("temp_view_session_settings_1_sweep_standing.json").read_text())
KEYS = sorted(ORACLE["true"])
UNSET_KEYS = sorted(ORACLE["unset"])
RULED = "TEMP-VIEW-SESSION-SETTINGS-1 ruled class: a settings-sensitive conjunct kept above"
AND_GUARD = "AND-GUARD-ANSI-1: the AND evaluates the guarded division on the dropped row"
CAST_TEXT = "direct gap: a string-to-integer cast raises Arrow's text in both modes"
KEPT_CAST = "the view body casts the text of a row the reader's kept-above filter drops in Spark"
MOD_CLASS = "direct gap: 10 % 0 raises DIVIDE_BY_ZERO where Spark raises REMAINDER_BY_ZERO"
SUGGESTION = "direct gap: NUMERIC_VALUE_OUT_OF_RANGE without Spark's WITH_SUGGESTION subclass"
CASE_KEPT = "caseSensitive follows the reader: Spark's twin-column check reads the reader's setting"
HELD_CURTZ = "read/tsql/zone/curtz/"
HELD_ZONE = "HELD-FRAME-CURRENT-TIMEZONE-1: a held frame keeps its creation zone"
STANDING = {
    "filter/or_mixed/on_off": (RULED, "DIVIDE_BY_ZERO"),
    "filter/agg_key_sens/on_off": (RULED, "DIVIDE_BY_ZERO"),
    "filter/and_free/on_off": (RULED + "; " + AND_GUARD, "DIVIDE_BY_ZERO"),
    "filter/and_free/on_on": (AND_GUARD, "DIVIDE_BY_ZERO"),
    "filter/numcast/on_off": (KEPT_CAST, "CAST_INVALID_INPUT"),
    "filter/numcast/off_on": (CAST_TEXT, ""),
    "read/sql/plain/other/case/false>true": (CASE_KEPT, "UNRESOLVED_COLUMN.WITH_SUGGESTION"),
}


def standing(key: str, mode: str = "true") -> tuple[str, str | list[str]] | None:
    if key in SWEEP[mode]:
        held = SWEEP[mode][key]
        return SWEEP["classes"][held["class"]], held["answer"]
    if key in STANDING:
        return STANDING[key]
    if key.endswith("/ansi/mod/true>false"):
        return MOD_CLASS, "DIVIDE_BY_ZERO"
    if key.endswith("/ansi/bool_dec/true>false"):
        return SUGGESTION, "NUMERIC_VALUE_OUT_OF_RANGE"
    if key.startswith(HELD_CURTZ):
        return HELD_ZONE, [key.removeprefix(HELD_CURTZ).split(">")[0]]
    return None


def build(root: Path, mode: str) -> Any:
    _reset_active_session_for_tests()
    builder = (
        ReparkSession.builder.appName("test-temp-view-session-settings-1")
        .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", str(root / "lc"))
        .config("spark.sql.session.timeZone", "UTC")
    )
    if mode != "unset":
        builder = builder.config("spark.sql.ansi.enabled", mode)
    return builder.getOrCreate()


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
    root = tmp_path_factory.mktemp(f"tvs-{request.param}")
    yield request.param, measure(root, request.param, "cells")


@pytest.fixture(scope="module")
def unset_measured(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    return measure(tmp_path_factory.mktemp("tvs-unset"), "unset", "cells")


def same_as_spark(want: dict[str, Any], found: dict[str, Any]) -> bool:
    if "error" in want or "error" in found:
        return "error" in want and "error" in found and want["error"] == found["error"]
    return want.get("rows") == found.get("rows")


def assert_answer(found: dict[str, Any], want: dict[str, Any], key: str) -> None:
    if "error" in want:
        assert "error" in found, (key, found, want)
        if want["error"]:
            assert found["error"] == want["error"], (key, found, want)
        return
    assert "error" not in found, (key, found, want)
    assert found["rows"] == want["rows"], (key, found, want)
    assert found.get("cached") == want.get("cached"), (key, found, want)


@pytest.mark.parametrize("key", KEYS)
def test_a_cell_answers_as_spark(measured: tuple[str, dict[str, Any]], key: str) -> None:
    mode, found = measured
    held = standing(key, mode)
    if held is None:
        assert_answer(found[key], ORACLE[mode][key], key)
        return
    reason, want = held
    assert not same_as_spark(ORACLE[mode][key], found[key]), (key, reason)
    if isinstance(want, list):
        assert found[key].get("rows") == want, (key, reason, found[key])
    else:
        assert "error" in found[key], (key, reason, found[key])
        assert found[key]["error"] == want, (key, reason, found[key])


@pytest.mark.parametrize("key", UNSET_KEYS)
def test_an_unset_flag_is_captured_as_its_default(unset_measured: dict[str, Any], key: str) -> None:
    assert_answer(unset_measured[key], ORACLE["unset"][key], key)


@pytest.mark.parametrize("key", [k for k, (reason, _) in STANDING.items() if RULED in reason])
def test_the_ruled_loud_cells_answer_where_spark_does(key: str) -> None:
    assert ORACLE["true"][key] == {"rows": ["5.0"], "types": ["double"]}, key


def test_matching_settings_keep_todays_plans(tmp_path: Path) -> None:
    plans = measure(tmp_path, "true", "explain")
    assert plans == PLANS["true"]

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import _decimal_common_type_1_cells as cells
import pytest

from repark import ReparkSession
from repark import functions as sf
from repark.spark.session import _reset_active_session_for_tests

ORACLE = json.loads(
    (Path(__file__).with_name("decimal_common_type_1_spark_oracle.json")).read_text()
)
DUPLICATE_KEY_CELLS = ("shape/map_concat_keys_id1", "shape/create_map_keys_id1")
KEYS = sorted(ORACLE["cells"]["false"])


@pytest.fixture(scope="module", params=["false", "true"])
def measured(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[tuple[str, dict[str, Any]]]:
    root = tmp_path_factory.mktemp(f"dct-{request.param}")
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-decimal-common-type-1")
        .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", str(root / "wh"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", request.param)
        .getOrCreate()
    )
    cells.make_tables(session, str(root))
    session.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect()
    found = cells.measure_all(session, sf, "lc")
    found["legacy/conf_after_reset"] = session.conf.get(cells.LEGACY_KEY, "unset")
    session.stop()
    _reset_active_session_for_tests()
    yield request.param, found


def assert_as_spark(found: Any, want: Any, key: str) -> None:
    if isinstance(want, str):
        assert isinstance(found, str), (key, found, want)
        assert found.startswith("ERR:"), (key, found)
        if key in DUPLICATE_KEY_CELLS:
            assert "duplicate map key" in found.lower(), (key, found)
        return
    if isinstance(want, dict) and "statement" in want:
        assert isinstance(found, dict), (key, found)
        assert (found["statement"] == "ok") == (want["statement"] == "ok"), (key, found, want)
        assert cells.same_answer(found["rows"], want["rows"]), (key, found, want)
        return
    assert cells.same_answer(found, want), (key, found, want)


@pytest.mark.parametrize("key", KEYS)
def test_a_cell_answers_as_spark(measured: tuple[str, dict[str, Any]], key: str) -> None:
    mode, found = measured
    assert_as_spark(found[key], ORACLE["cells"][mode][key], key)


def test_reset_clears_the_legacy_flag(measured: tuple[str, dict[str, Any]]) -> None:
    _, found = measured
    assert found["legacy/conf_after_reset"] in ("unset", "false")

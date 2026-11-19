from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import _string_hour_minute_second_1_cells as cells
import pytest

from repark import ReparkSession
from repark import functions as sf
from repark.spark.session import _reset_active_session_for_tests

ORACLE = json.loads(
    Path(__file__).with_name("string_hour_minute_second_1_spark_oracle.json").read_text()
)
KEYS = sorted(ORACLE["cells"])

pytestmark = pytest.mark.xdist_group("test_string_hour_minute_second_1")


def new_session(root: Path) -> Any:
    _reset_active_session_for_tests()
    return (
        ReparkSession.builder.appName("test-string-hour-minute-second-1")
        .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", str(root / "wh"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


@pytest.fixture(scope="module")
def measured(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[tuple[dict[str, Any], dict[str, str]]]:
    root = tmp_path_factory.mktemp("string-hms")
    session = new_session(root)
    cells.make_sources(session, root)
    found = cells.measure_all(session, sf, root, KEYS)
    cells.configure(session, "a1-la")
    plans = {fn: cells.explain(session, fn) for fn in (*cells.HMS, "year")}
    yield found, plans
    session.stop()
    _reset_active_session_for_tests()


def kind(key: str) -> str:
    cfg, _, sid, _, _ = key.split("|")
    if key in cells.N8:
        return "n8"
    if sid == "g":
        return "multi-row"
    return "ntz" if cfg.endswith("ntz") else "projection"


def test_the_oracle_holds_every_cell_of_the_unit() -> None:
    counts = Counter(kind(key) for key in KEYS if key not in cells.N8)
    counts["n8"] = len(cells.N8)
    projection = counts["projection"] + sum(1 for key in cells.N8 if "ntz" not in key)
    ntz = counts["ntz"] + sum(1 for key in cells.N8 if "ntz" in key)
    assert (projection, ntz, counts["multi-row"]) == (3072, 192, 216)
    assert set(cells.N8) <= set(KEYS)


@pytest.mark.parametrize("key", KEYS, ids=[f"{kind(key)}:{key}" for key in KEYS])
def test_a_string_time_field_answers_as_spark(
    measured: tuple[dict[str, Any], dict[str, str]], key: str
) -> None:
    found, _ = measured
    want = ORACLE["cells"][key]
    if key in cells.N8:
        assert want != {"v": cells.N8[key]}, (key, want)
        assert found[key] == {"v": cells.N8[key]}, (key, found[key], want)
        return
    assert found[key] == want, (key, found[key], want)


@pytest.mark.parametrize("fn", cells.HMS)
def test_a_string_time_field_plans_as_the_instant_cast(
    measured: tuple[dict[str, Any], dict[str, str]], fn: str
) -> None:
    _, plans = measured
    assert f"{fn}(to_timestamp(" in plans[fn], plans[fn]
    assert "Date32" not in plans[fn], plans[fn]
    assert "Date32" in plans["year"], plans["year"]

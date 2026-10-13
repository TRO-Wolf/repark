"""Slice 1 fold 1 facade pins: the twelve shapes equal main's oracle.

pins: ice-tsns-coercion-1/C-005
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

import _ice_tsns_coercion_1_doors as doors
import _record_ice_tsns_coercion_1_slice1_fold1 as fold1

EXPECTED_FOLD1_CELLS = 152


def _load_oracle() -> dict[str, Any]:
    """Return the baseline-recorded fold-1 oracle."""
    text: str = (fold1.TESTS / "ice_tsns_coercion_1_slice1_fold1.json").read_text()
    return json.loads(text)


def _normalized(outcome: dict[str, Any], shape: str) -> dict[str, Any]:
    """Return an order-stable outcome for order-unspecific shapes."""
    if "columns" not in outcome:
        return outcome
    if shape in fold1.UNORDERED:
        columns = []
        for column in outcome["columns"]:
            values = sorted(column["values"], key=repr)
            columns.append({**column, "values": values})
        return {"columns": columns}
    if shape == "string_agg":
        columns = []
        for column in outcome["columns"]:
            values = [_multiset(value) for value in column["values"]]
            columns.append({**column, "values": values})
        return {"columns": columns}
    return outcome


def _multiset(value: Any) -> Any:
    """Return the concatenated halves of one delimiter-less aggregation."""
    if not isinstance(value, str) or len(value) % 2:
        return value
    half = len(value) // 2
    return sorted([value[:half], value[half:]])


def test_fold1_shapes_equal_main_oracle(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — 152 cells store main's answer."""
    oracle = _load_oracle()
    count = 0
    with tempfile.TemporaryDirectory() as scratch:
        for zone_index, zone in enumerate(fold1.ZONES):
            for ansi in fold1.ANSIS:
                warehouse = Path(scratch) / f"fold1-{zone_index}-{ansi}"
                warehouse.mkdir()
                spark = doors.open_session(zone, warehouse)
                spark.conf.set("spark.sql.ansi.enabled", ansi)
                try:
                    for source in fold1.SOURCES:
                        for shape, template in fold1.SQL_SHAPES:
                            cell = f"{zone}|{ansi}|sql|{source}|{shape}"
                            outcome = fold1._measure(spark, "sql", source, shape, template)
                            assert _normalized(outcome, shape) == _normalized(
                                oracle[cell], shape
                            ), cell
                            count += 1
                        for shape in fold1.DF_SHAPES:
                            cell = f"{zone}|{ansi}|df|{source}|{shape}"
                            outcome = fold1._measure(spark, "df", source, shape, "")
                            assert _normalized(outcome, shape) == _normalized(
                                oracle[cell], shape
                            ), cell
                            count += 1
                finally:
                    spark.stop()
    assert count == EXPECTED_FOLD1_CELLS

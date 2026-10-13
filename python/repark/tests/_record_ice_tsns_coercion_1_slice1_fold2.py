"""Record the fold-2 oracle: nanosecond sites into consumers on main.

Run with the BASELINE module: twelve ``sequence`` shapes and five Python-door
shapes over sources ``ns`` and ``tzns`` in zones UTC and America/New_York.
Writes ``ice_tsns_coercion_1_slice1_fold2.json`` mapping each cell to its
columns (names, Arrow types, nullability, exact ticks) or its error.

pins: ice-tsns-coercion-1/C-005
"""

from __future__ import annotations

import json
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _ice_tsns_coercion_1_doors as doors
import pyarrow as pa
import pyarrow.compute as pc
from _ice_tsns_coercion_1_spellings import SRC

from repark import functions

TESTS = Path(__file__).resolve().parent
OUT = TESTS / "ice_tsns_coercion_1_slice1_fold2.json"
ZONES = ("UTC", "America/New_York")
SOURCES = ("ns", "tzns")
LONG_LIST = 16


def _select(projection: str, tail: str = "ORDER BY id") -> str:
    """Return one SELECT template over the shared source table."""
    return f"SELECT {projection} FROM {SRC} {tail}"


_SITE = "coalesce({c}, NULL)"
_STEP = f"sequence({_SITE}, {_SITE} + INTERVAL 2 SECOND, INTERVAL 1 SECOND)"
_UNION = (
    "SELECT id, sequence(v, v) AS r FROM "
    f"(SELECT id, {{c}} AS v FROM {SRC} UNION ALL SELECT 9, NULL) u ORDER BY id"
)
SEQUENCE_SQL: tuple[tuple[str, str], ...] = (
    ("coalesce", _select(f"id, sequence({_SITE}, {{c}}) AS v")),
    ("both", _select(f"id, sequence({_SITE}, {_SITE}) AS v")),
    ("step", _select(f"id, {_STEP} AS v")),
    ("greatest", _select("id, sequence(greatest({c}, NULL), {c}) AS v")),
    ("if", _select("id, sequence(if(id > 0, {c}, NULL), {c}) AS v")),
    ("case", _select("id, sequence(CASE WHEN id > 0 THEN {c} ELSE NULL END, {c}) AS v")),
    ("nullif", _select("id, sequence(nullif({c}, NULL), {c}) AS v")),
    ("union", _UNION),
    ("size", _select(f"id, size(sequence({_SITE}, {{c}})) AS v")),
    ("where", _select("id", f"WHERE size(sequence({_SITE}, {{c}})) = 1 ORDER BY id")),
)
SEQUENCE_DF = ("frame", "frame_explode")
DOORS = (
    "map_in_arrow",
    "map_in_arrow_array",
    "map_in_arrow_ltz",
    "map_in_pandas",
    "apply_in_pandas",
)


def echo_batches(batches: Iterator[Any]) -> Iterator[Any]:
    """Yield every batch unchanged."""
    yield from batches


def echo_frame(frame: Any) -> Any:
    """Return the pandas frame unchanged."""
    return frame


def _summarize(ticks: list[Any]) -> Any:
    """Return a long list as its count, ends and common step."""
    if len(ticks) <= LONG_LIST:
        return ticks
    steps = {ticks[index + 1] - ticks[index] for index in range(len(ticks) - 1)}
    return {
        "count": len(ticks),
        "first": ticks[0],
        "last": ticks[-1],
        "step": steps.pop() if len(steps) == 1 else None,
    }


def _encode(array: pa.Array) -> list[Any]:
    """Return ``array`` as JSON-safe values with exact timestamp ticks."""
    if pa.types.is_timestamp(array.type):
        return pc.cast(array, pa.int64()).to_pylist()
    if pa.types.is_list(array.type) and pa.types.is_timestamp(array.type.value_type):
        rows = pc.cast(array, pa.list_(pa.int64())).to_pylist()
        return [None if row is None else _summarize(row) for row in rows]
    return array.to_pylist()


def _capture(frame: Any) -> dict[str, Any]:
    """Return the exact columns of one answered query."""
    table = frame.toArrow()
    columns = []
    for number in range(table.num_columns):
        field = table.schema.field(number)
        columns.append(
            {
                "name": field.name,
                "type": str(table.schema.types[number]),
                "nullable": field.nullable,
                "values": _encode(table.column(number).combine_chunks()),
            }
        )
    return {"columns": columns}


def _site(source: str) -> Any:
    """Return the DataFrame spelling of the untyped-NULL site."""
    return functions.coalesce(functions.col(source), functions.lit(None))


def _sequence_frame(spark: Any, source: str, shape: str) -> Any:
    """Return one DataFrame-door sequence shape."""
    frame = spark.table(SRC)
    if shape == "frame":
        listed = functions.sequence(_site(source), functions.col(source))
        return frame.select("id", listed.alias("v")).orderBy("id")
    second = functions.expr("INTERVAL 1 SECOND")
    listed = functions.sequence(_site(source), functions.col(source) + second, second)
    return frame.select("id", functions.explode(listed).alias("v")).orderBy("id", "v")


def _door_frame(spark: Any, source: str, shape: str) -> Any:
    """Return one Python-door shape over the site frame and its own schema."""
    frame = spark.table(SRC)
    if shape == "map_in_arrow_array":
        listed = functions.array(functions.col(source), functions.lit(None))
        site = frame.select("id", listed.alias("v"))
        return site.mapInArrow(echo_batches, site.schema).orderBy("id")
    site = frame.select("id", _site(source).alias("v"))
    if shape == "map_in_arrow":
        return site.mapInArrow(echo_batches, site.schema).orderBy("id")
    if shape == "map_in_arrow_ltz":
        return site.mapInArrow(echo_batches, "id int, v timestamp").orderBy("id")
    if shape == "map_in_pandas":
        return site.mapInPandas(echo_batches, site.schema).orderBy("id")
    return site.groupBy("id").applyInPandas(echo_frame, site.schema).orderBy("id")


def measure(spark: Any, door: str, source: str, shape: str, template: str) -> dict[str, Any]:
    """Run one cell and return its outcome or its error."""
    try:
        if door == "sql":
            return _capture(spark.sql(template.format(c=source)))
        if door == "df":
            return _capture(_sequence_frame(spark, source, shape))
        return _capture(_door_frame(spark, source, shape))
    except Exception as error:
        return {"error": doors.error_text(error)}


def cells(sources: tuple[str, ...]) -> list[tuple[str, str, str, str]]:
    """Return every ``(door, source, shape, template)`` of one session."""
    listed: list[tuple[str, str, str, str]] = []
    for source in sources:
        listed.extend(("sql", source, shape, template) for shape, template in SEQUENCE_SQL)
        listed.extend(("df", source, shape, "") for shape in SEQUENCE_DF)
        listed.extend(("py", source, shape, "") for shape in DOORS)
    return listed


def measure_all(scratch: Path, sources: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    """Return the outcome of every cell over ``sources`` in every zone."""
    outcomes: dict[str, dict[str, Any]] = {}
    for zone_index, zone in enumerate(ZONES):
        warehouse = scratch / f"fold2-{zone_index}"
        warehouse.mkdir()
        spark = doors.open_session(zone, warehouse)
        try:
            for door, source, shape, template in cells(sources):
                cell = f"{zone}|{door}|{source}|{shape}"
                outcomes[cell] = measure(spark, door, source, shape, template)
        finally:
            spark.stop()
    return outcomes


def main() -> None:
    """Record every fold-2 oracle cell."""
    with tempfile.TemporaryDirectory() as scratch:
        outcomes = measure_all(Path(scratch), SOURCES)
    OUT.write_text(json.dumps(outcomes, indent=1, sort_keys=True) + "\n")
    errors = sum(1 for outcome in outcomes.values() if "error" in outcome)
    print(f"cells={len(outcomes)} errors={errors}")


if __name__ == "__main__":
    main()

"""Slice 1 narrow facade pins: reads and frames equal main, census is main-or-twin.

pins: ice-tsns-coercion-1/C-005
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import _ice_tsns_coercion_1_doors as doors
import pyarrow as pa

READ_CASES = (
    ("coalesce", "SELECT id, coalesce(ns, NULL) AS v FROM ice.ns.src ORDER BY id"),
    ("case", "SELECT id, CASE WHEN id > 0 THEN ns ELSE NULL END AS v FROM ice.ns.src ORDER BY id"),
    ("if", "SELECT id, if(id > 0, ns, NULL) AS v FROM ice.ns.src ORDER BY id"),
)

FRAME_CASES = (
    "SELECT id, coalesce(ns, NULL) AS v FROM ice.ns.src",
    "SELECT id, CASE WHEN id > 0 THEN ns ELSE NULL END AS v FROM ice.ns.src",
)

EXPLAIN_READS = (
    "SELECT coalesce(ns, NULL) FROM ice.ns.src",
    "SELECT CASE WHEN id > 0 THEN ns ELSE NULL END FROM ice.ns.src",
    "SELECT if(id > 0, ns, NULL) FROM ice.ns.src",
    "SELECT array(ns, NULL) FROM ice.ns.src",
    "SELECT coalesce(tzns, NULL) FROM ice.ns.src",
    "SELECT CASE WHEN id > 0 THEN tzns ELSE NULL END FROM ice.ns.src",
    "SELECT coalesce(ns, NULL) AS v FROM ice.ns.src ORDER BY id",
    "SELECT coalesce(ns, NULL) AS v FROM ice.ns.src WHERE id > 0",
    "SELECT id, coalesce(ns, NULL) AS v FROM ice.ns.src",
    "SELECT max(coalesce(ns, NULL)) FROM ice.ns.src",
    "SELECT coalesce(ns, NULL) FROM ice.ns.src UNION ALL SELECT NULL",
    "SELECT DISTINCT coalesce(ns, NULL) FROM ice.ns.src",
)


def _open(zone: str, warehouse: Path) -> Any:
    """Return one session with the shared source table."""
    return doors.open_session(zone, warehouse)


def _arrow_type(spark: Any, sql: str) -> pa.DataType:
    """Return the Arrow type of the last column of one query."""
    table = spark.sql(sql).toArrow()
    return table.schema.types[table.num_columns - 1]


def test_read_shapes_equal_main(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — read types and ticks equal main."""
    warehouse = tmp_path / "reads"
    warehouse.mkdir()
    spark = _open("UTC", warehouse)
    try:
        for shape, sql in READ_CASES:
            got = _arrow_type(spark, sql)
            assert pa.types.is_timestamp(got), shape
            assert got.unit == "us", f"{shape}: {got}"
    finally:
        spark.stop()


def test_frame_and_creating_doors_equal_main(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — frames and CTAS columns equal main."""
    warehouse = tmp_path / "frames"
    warehouse.mkdir()
    spark = _open("UTC", warehouse)
    try:
        for sql in FRAME_CASES:
            frame = spark.sql(sql)
            field = frame.schema["v"]
            assert "TimestampType" in type(field.dataType).__name__, field
            spark.sql("DROP TABLE IF EXISTS ice.ns.ctas_probe")
            spark.sql(f"CREATE TABLE ice.ns.ctas_probe AS {sql}")
            try:
                ctas = spark.table("ice.ns.ctas_probe").schema["v"]
                assert type(ctas.dataType).__name__ == type(field.dataType).__name__
            finally:
                spark.sql("DROP TABLE IF EXISTS ice.ns.ctas_probe")
    finally:
        spark.stop()


def test_census_cells_are_main_or_twin(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — every census cell is main or twin."""
    census = Path(__file__).resolve().parent / "ice_tsns_coercion_1_narrow_census.json"
    assert census.exists(), "narrow census not recorded"
    cells = json.loads(census.read_text(encoding="utf-8"))
    assert len(cells) == 9600
    for cell, outcome in cells.items():
        assert outcome["role"] in ("main", "twin"), cell


def test_registry_sweep_no_site_function_refuses_nanoseconds(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — swept functions answer where main answers."""
    sweep = Path(__file__).resolve().parent / "ice_tsns_coercion_1_narrow_sweep.json"
    assert sweep.exists(), "narrow sweep not recorded"
    rows = json.loads(sweep.read_text(encoding="utf-8"))
    assert rows
    refused = [row for row in rows if row["main_answered"] and not row["head_answered"]]
    assert refused == []


def test_written_cast_to_ns_keeps_digits_and_equals_twin(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — the one named read exception keeps nine digits."""
    warehouse = tmp_path / "written-cast"
    warehouse.mkdir()
    spark = _open("UTC", warehouse)
    try:
        table = spark.sql(
            "SELECT CAST(coalesce(ns, NULL) AS timestamp_ns) AS v FROM ice.ns.src ORDER BY id"
        ).toArrow()
        twin = spark.sql(
            "SELECT __repark_cast_timestamp_ns__(coalesce(ns, NULL)) AS v "
            "FROM ice.ns.src ORDER BY id"
        ).toArrow()
        assert str(table.schema.types[0]) == str(twin.schema.types[0])
        assert table.column(0).to_pylist() == twin.column(0).to_pylist()
        ticks = table.column(0).cast(pa.int64()).to_pylist()
        assert any(tick is not None and tick % 1000 != 0 for tick in ticks)
    finally:
        spark.stop()


def test_explain_of_reads_equals_main(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — read plans carry no store kernel."""
    warehouse = tmp_path / "explain"
    warehouse.mkdir()
    spark = _open("UTC", warehouse)
    try:
        for sql in EXPLAIN_READS:
            for mode in ("", "VERBOSE"):
                shown = f"EXPLAIN {mode} {sql}" if mode else f"EXPLAIN {sql}"
                frame = spark.sql(shown).toPandas().iloc[:, 0].str.cat(sep="\n")
                assert "__repark_cast_timestamp_ns__" not in frame, sql
                assert "__repark_cast_timestamptz_ns__" not in frame, sql
    finally:
        spark.stop()

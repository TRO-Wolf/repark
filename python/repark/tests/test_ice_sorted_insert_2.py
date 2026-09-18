"""ICE-SORTED-INSERT-1 round 3 — RePark-owned rewrites sort and stamp like Spark.

pins: ice-sorted-insert-1/C-006, C-007, C-008, C-009, C-010
"""

from __future__ import annotations

import itertools
import json
import math
import os
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from repark import ReparkSession

_HERE = Path(__file__).resolve().parent
_ORACLE = json.loads((_HERE / "ice_sorted_insert_2_spark_oracle.json").read_text())
_PLAN: dict[str, Any] = {program["name"]: program for program in _ORACLE["plan"]}
_CELLS: dict[str, Any] = _ORACLE["cells"]
LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — the live sort oracle is skipped (CI is JVM-free)"

CATALOG = "icesorted2"
ALLOW_CREATE_V3 = "repark.sql.allowCreateFormatVersion3"
RANGE_VIEWS = (2000, 400, 200)
BINPACK_INPUT_FILES = 6
RDF_FORK_ASK = "BLOCKED-ON-FORK F-RDF-SORT-STAMP-1"
UPDATE_FORK_ASK = "BLOCKED-ON-FORK F-COW-UPDATE-STAMP-1"
REPARK_OWNED = "_merge_not_matched_insert"


def _session(name: str, warehouse: Path) -> ReparkSession:
    """One memory-catalog engine with the oracle's range views registered."""
    engine = ReparkSession.builder.appName(name).config(ALLOW_CREATE_V3, "true").getOrCreate()
    engine.register_memory_catalog(CATALOG, warehouse)
    engine.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.w")
    for rows in RANGE_VIEWS:
        engine.range(rows).createOrReplaceTempView(f"r{rows}")
    return engine


def _adapt(statement: str, table: str, name: str) -> str:
    """One recorded statement as RePark SQL: the oracle's `range(n)` becomes a temp view."""
    rendered = statement.format(t=table, catalog=CATALOG, ns="w", name=name)
    for rows in RANGE_VIEWS:
        rendered = rendered.replace(f"range({rows})", f"r{rows}")
    return rendered


def _files(engine: ReparkSession, table: str) -> list[dict[str, Any]]:
    """file_path / record_count / sort_order_id per live data file."""
    arrow = engine.sql(
        f"SELECT file_path, record_count, sort_order_id FROM {table}.files"
    ).to_arrow()
    return [
        {
            "file_path": path.as_py(),
            "record_count": int(count.as_py()),
            "sort_order_id": stamp.as_py(),
        }
        for path, count, stamp in zip(
            arrow.column("file_path"),
            arrow.column("record_count"),
            arrow.column("sort_order_id"),
            strict=True,
        )
    ]


def _column(path: str, name: str) -> list[Any]:
    """One parquet column as plain Python values."""
    return pq.read_table(path, columns=[name]).column(name).to_pylist()


def _sorted_under(values: list[Any], descending: bool) -> bool:
    """Sortedness under Spark's order: NULLS FIRST ascending, NaN above every value."""

    def rank(value: Any) -> tuple[int, float]:
        if value is None:
            return (0, 0.0)
        if isinstance(value, float) and math.isnan(value):
            return (2, 0.0)
        return (1, -float(value) if descending else float(value))

    keys = [rank(value) for value in values]
    return keys == sorted(keys)


def _float_layout(values: list[Any]) -> dict[str, Any]:
    """NULL/NaN placement of one float file: leading NULLs, then values, then a NaN block."""
    nulls = [index for index, value in enumerate(values) if value is None]
    nans = [
        index
        for index, value in enumerate(values)
        if isinstance(value, float) and math.isnan(value)
    ]
    finite = [value for value in values if value is not None and not math.isnan(value)]
    return {
        "null_count": len(nulls),
        "nan_count": len(nans),
        "nulls_first_block": nulls == list(range(len(nulls))),
        "nan_tail_block": nans == list(range(len(values) - len(nans), len(values))),
        "middle_strictly_ascending": all(a < b for a, b in itertools.pairwise(finite)),
    }


def _expected_stamp(cell: dict[str, Any]) -> int:
    """The one `sort_order_id` Spark stamped on every file of a recorded cell."""
    stamps = {entry["sort_order_id"] for entry in cell["files"]}
    assert len(stamps) == 1, stamps
    return stamps.pop()


def _assert_cell(engine: ReparkSession, table: str, program: dict[str, Any], name: str) -> None:
    """Every live file sorted on the declared key and stamped, with Spark's live row count."""
    cell = _CELLS[name]
    stamp = _expected_stamp(cell)
    key = program["key"]
    descending = program["descending"]
    files = _files(engine, table)
    assert files, (name, table)
    for entry in files:
        keys = _column(entry["file_path"], key)
        assert entry["sort_order_id"] == stamp, (name, entry)
        assert _sorted_under(keys, descending), (name, entry, keys[:8])
        if key == "f":
            assert _float_layout(keys) == cell["files"][0]["layout"], (name, entry)
    live = engine.sql(f"SELECT count(*) AS n FROM {table}").to_arrow().column("n")[0].as_py()
    assert int(live) == sum(entry["record_count"] for entry in cell["files"]), name


def _drive(engine: ReparkSession, program: dict[str, Any], until: str | None = None) -> str:
    """Build one recorded program on RePark, asserting each cell as it is reached."""
    table = f"{CATALOG}.w.{program['name']}"
    engine.sql(f"CREATE TABLE {table} {program['ddl']}").collect()
    engine.sql(f"ALTER TABLE {table} {program['order']}").collect()
    for cell in program["cells"]:
        for statement in cell["sql"]:
            engine.sql(_adapt(statement, table, program["name"])).collect()
        _assert_cell(engine, table, program, cell["cell"])
        if until is not None and cell["cell"].endswith(until):
            break
    return table


@pytest.mark.parametrize("program", ["m3", "m2"])
def test_partitioned_rewrites_sort_and_stamp(tmp_path: Path, program: str) -> None:
    """C-006: the RePark-owned INSERT OVERWRITE and MERGE rewrites sort and stamp."""
    engine = _session(f"sorted2-{program}", tmp_path / "wh")
    try:
        _drive(engine, _PLAN[program], until=REPARK_OWNED)
    finally:
        engine.stop()


@pytest.mark.parametrize("program", ["m3", "m2"])
@pytest.mark.xfail(reason=UPDATE_FORK_ASK, strict=True)
def test_predicate_update_sorts_and_stamps_like_spark(tmp_path: Path, program: str) -> None:
    """C-009: Spark's COW UPDATE rewrite is sorted and stamped; the fork's exec stamps NULL."""
    engine = _session(f"sorted2-upd-{program}", tmp_path / "wh")
    try:
        _drive(engine, _PLAN[program])
    finally:
        engine.stop()


def test_unpartitioned_overwrite_sorts_and_stamps(tmp_path: Path) -> None:
    """C-007: the unpartitioned MERGE/overwrite writer stamps the declared order id."""
    engine = _session("sorted2-up", tmp_path / "wh")
    try:
        _drive(engine, _PLAN["up"])
    finally:
        engine.stop()


def test_reordered_table_stamps_the_current_order(tmp_path: Path) -> None:
    """C-007: after a second `WRITE ORDERED BY`, files carry the CURRENT order id."""
    engine = _session("sorted2-alt", tmp_path / "wh")
    try:
        _drive(engine, _PLAN["alt"])
    finally:
        engine.stop()


def test_owned_float_overwrite_places_nan_like_spark(tmp_path: Path) -> None:
    """C-008: NULLS FIRST, values ascending, NaN a solid tail block, one stamped file."""
    engine = _session("sorted2-fl", tmp_path / "wh")
    try:
        _drive(engine, _PLAN["fl"])
    finally:
        engine.stop()


def test_owned_float_overwrite_canonicalises_negative_nan(tmp_path: Path) -> None:
    """C-008: a negative NaN sorts with the NaN block, as the fork's INSERT path does."""
    engine = _session("sorted2-nan", tmp_path / "wh")
    try:
        table = f"{CATALOG}.w.nan"
        engine.sql(f"CREATE TABLE {table} (id BIGINT, f DOUBLE) USING iceberg").collect()
        engine.sql(f"ALTER TABLE {table} WRITE ORDERED BY (f)").collect()
        engine.sql(
            f"INSERT OVERWRITE {table} SELECT id, CASE WHEN id = 0 THEN -CAST('NaN' AS DOUBLE)"
            " WHEN id = 1 THEN CAST('NaN' AS DOUBLE) WHEN id = 2 THEN CAST(NULL AS DOUBLE)"
            " ELSE CAST(id AS DOUBLE) END AS f FROM r200"
        ).collect()
        files = _files(engine, table)
        assert len(files) == 1, files
        keys = _column(files[0]["file_path"], "f")
        assert _sorted_under(keys, descending=False), keys[:8]
        layout = _float_layout(keys)
        assert layout["nan_count"] == 2, layout
        assert layout["nan_tail_block"], (layout, keys[:8])
        assert layout["nulls_first_block"], (layout, keys[:8])
    finally:
        engine.stop()


def test_v3_lineage_survives_the_default_order_sort(tmp_path: Path) -> None:
    """C-010: `_row_id` travels with its row through the lineage fanout's sort."""
    engine = _session("sorted2-lineage", tmp_path / "wh")
    try:
        program = _PLAN["m3"]
        table = _drive(engine, program, until="v3_partitioned_overwrite")
        before = _lineage(engine, table)
        assert before, table
        assert all(row_id is not None for row_id in before.values()), before
        engine.sql(_adapt(program["cells"][1]["sql"][0], table, program["name"])).collect()
        _assert_cell(engine, table, program, "v3_partitioned_merge_matched_update")
        assert _lineage(engine, table) == before
    finally:
        engine.stop()


def _lineage(engine: ReparkSession, table: str) -> dict[int, int | None]:
    """The table's id → `_row_id` map."""
    arrow = engine.sql(f"SELECT id, _row_id FROM {table}").to_arrow()
    return {
        int(key.as_py()): (None if row_id.as_py() is None else int(row_id.as_py()))
        for key, row_id in zip(arrow.column("id"), arrow.column("_row_id"), strict=True)
    }


@pytest.mark.xfail(reason=RDF_FORK_ASK, strict=True)
def test_binpack_rewrite_sorts_and_stamps_like_spark(tmp_path: Path) -> None:
    """C-009: Spark's binpack re-sorts by the table default and stamps it; the fork does not."""
    engine = _session("sorted2-bp", tmp_path / "wh")
    try:
        program = _PLAN["bp"]
        table = f"{CATALOG}.w.{program['name']}"
        engine.sql(f"CREATE TABLE {table} {program['ddl']}").collect()
        engine.sql(f"ALTER TABLE {table} {program['order']}").collect()
        for chunk in range(BINPACK_INPUT_FILES):
            engine.sql(
                f"INSERT INTO {table} SELECT ((id * 7919) % 2000) + {chunk} AS id,"
                " CAST(id % 2 AS INT) AS p FROM r200"
            ).collect()
        engine.sql(
            f"CALL {CATALOG}.system.rewrite_data_files(table => 'w.{program['name']}')"
        ).collect()
        stamp = _expected_stamp(_CELLS["binpack_after"])
        for entry in _files(engine, table):
            keys = _column(entry["file_path"], program["key"])
            assert entry["sort_order_id"] == stamp, entry
            assert _sorted_under(keys, program["descending"]), (entry, keys[:8])
    finally:
        engine.stop()


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_live_cells_match_fixture(tmp_path: Path) -> None:
    """C-009: live Spark re-derives every recorded cell from the plan in the fixture."""
    import _record_ice_sorted_insert_2_oracle as recorder

    assert recorder.main(["check", str(tmp_path / "spark-wh")]) == 0

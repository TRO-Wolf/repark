"""SIGNED-ZERO-NORMALIZE-1 part B part 1: flat float keys answer like Spark on every door"""

from __future__ import annotations

import contextlib
import json
import struct
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import repark
from repark import ReparkSession, Window, functions
from repark._native import PyReparkSession

pytestmark = pytest.mark.xdist_group("test_signed_zero_keys_1")

CELLS_PATH = Path(__file__).parent / "data" / "signed_zero_keys_1_cells.jsonl"
LINES = [json.loads(line) for line in CELLS_PATH.read_text(encoding="utf-8").splitlines() if line]
SETUP = LINES[0]["setup"]
CELLS = LINES[1:]
ROUTES = ("spark-sql", "f-expr", "typed", "native")
CATALOG = "ice"
NAMESPACE = "szb"
CANONICAL_NAN = 0x7FF8000000000000
TIES = ("OB-4|",)
FIXED_ORDER = ("GB-8|",)
KEYED_OUTPUTS = ("GB-", "DI-", "SO-union|", "SO-intersect|", "SO-except|", "NN-1|", "NN-2|")


def norm(value: Any) -> Any:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, float):
        if value != value:
            return "NaN"
        if value in (float("inf"), float("-inf")):
            return "Infinity" if value > 0 else "-Infinity"
        if value == 0.0 and str(value) == "-0.0":
            return "-0.0"
        return repr(value)
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, (list, tuple)):
        return [norm(item) for item in value]
    if isinstance(value, dict):
        return {key: norm(item) for key, item in value.items()}
    return value


def collect(frame: Any) -> list[list[Any]]:
    table = frame.toArrow() if hasattr(frame, "toArrow") else frame.to_arrow()
    columns = [table.column(index).to_pylist() for index in range(table.num_columns)]
    rows = [list(values) for values in zip(*columns, strict=True)] if columns else []
    return [[norm(value) for value in row] for row in rows]


def keys(rows: list[list[Any]]) -> list[str]:
    return sorted(json.dumps(row, sort_keys=True, default=str) for row in rows)


def unsigned(rows: list[list[Any]]) -> list[list[Any]]:
    return [["0.0" if value == "-0.0" else value for value in row] for row in rows]


def without_nulls(rows: list[list[Any]]) -> list[list[Any]]:
    return [row for row in rows if None not in row]


def same_rows(cell: dict[str, Any], rows: list[list[Any]]) -> bool:
    truth = cell["truth"]
    if cell["cid"].startswith(TIES):
        return keys(rows) == keys(truth) and unsigned(rows) == unsigned(truth)
    if cell["cid"].startswith(FIXED_ORDER):
        return keys(rows) == keys(truth) and without_nulls(rows) == without_nulls(truth)
    return rows == truth if cell["ordered"] else keys(rows) == keys(truth)


def float_bits(value: float) -> int:
    return int.from_bytes(struct.pack(">d", value), "big")


def raw(frame: Any) -> list[list[Any]]:
    table = frame.toArrow() if hasattr(frame, "toArrow") else frame.to_arrow()
    columns = [table.column(index).to_pylist() for index in range(table.num_columns)]
    return [list(values) for values in zip(*columns, strict=True)] if columns else []


def nan_bits(rows: list[list[Any]]) -> list[int]:
    return [
        float_bits(value)
        for row in rows
        for value in row
        if isinstance(value, float) and value != value
    ]


@pytest.fixture
def facade(tmp_path: Path) -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("test_signed_zero_keys_1").getOrCreate()
    session.register_memory_catalog(CATALOG, str(tmp_path / "facade"))
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NAMESPACE}").collect()
    for step in SETUP:
        if step["door"] == "spark-sql":
            session.sql(step["text"]).collect()
    yield session
    session.stop()


@pytest.fixture
def native(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> PyReparkSession:
    root = tmp_path / "native"
    root.mkdir()
    session = PyReparkSession.native(config={"repark.sql.allowCreateFormatVersion3": "true"})
    session.register_memory_catalog(CATALOG, str(root))
    session.create_namespace(CATALOG, NAMESPACE, str(root / NAMESPACE))
    monkeypatch.setattr(repark, "_ANSI_NATIVE", session)
    for step in SETUP:
        if step["door"] == "native":
            collect(repark.sql(step["text"]))
    return session


def run(
    cell: dict[str, Any], facade: ReparkSession | None, gather: Any = collect
) -> list[list[Any]]:
    route = cell["route"]
    if route == "native":
        return gather(repark.sql(cell["text"]))
    assert facade is not None
    if route == "spark-sql":
        return gather(facade.sql(cell["text"]))
    frames = {"t": facade.table(f"{CATALOG}.{NAMESPACE}.t_s")}
    if cell["build"]:
        frames["u"] = facade.table(f"{CATALOG}.{NAMESPACE}.{cell['build']}_s")
    return gather(eval(cell["text"], {"F": functions, "W": Window, **frames}))


def side(cell: dict[str, Any], key: str, facade: ReparkSession | None) -> None:
    for statement in cell[key]:
        if cell["route"] == "native":
            collect(repark.sql(statement))
        elif facade is not None:
            facade.sql(statement).collect()


def mismatch(cell: dict[str, Any], facade: ReparkSession | None) -> str | None:
    try:
        side(cell, "pre", facade)
        rows = run(cell, facade)
    except BaseException as error:
        if isinstance(error, KeyboardInterrupt):
            raise
        return f"{type(error).__name__}: {str(error)[:160]}"
    finally:
        side(cell, "post", facade)
    return None if same_rows(cell, rows) else f"rows {rows} != {cell['truth']}"


@pytest.mark.parametrize("route", ROUTES)
def test_every_float_key_cell_reaches_the_spark_truth(
    route: str, request: pytest.FixtureRequest
) -> None:
    door = request.getfixturevalue("native" if route == "native" else "facade")
    facade = None if route == "native" else door
    cells = [cell for cell in CELLS if cell["route"] == route]
    assert cells
    failures = [
        f"{cell['cid']} {cell['text'][:80]}: {problem}"
        for cell in cells
        if (problem := mismatch(cell, facade)) is not None
    ]
    assert not failures, "\n".join(failures[:25])


@pytest.mark.parametrize("route", ("spark-sql", "typed", "native"))
def test_min_and_max_over_both_zeros_keep_the_declared_sign(
    route: str, request: pytest.FixtureRequest
) -> None:
    door = request.getfixturevalue("native" if route == "native" else "facade")
    tag = "n" if route == "native" else "s"
    table = f"{CATALOG}.{NAMESPACE}.t_{tag}"
    text = f"SELECT min(d), max(d), min(f), max(f) FROM {table} WHERE id <= 4"
    if route == "native":
        rows = collect(repark.sql(text))
    elif route == "spark-sql":
        rows = collect(door.sql(text))
    else:
        frame = door.table(table).filter(functions.col("id") <= 4)
        rows = collect(
            frame.agg(
                functions.min("d"), functions.max("d"), functions.min("f"), functions.max("f")
            )
        )
    assert rows == [["-0.0", "0.0", "-0.0", "0.0"]]


def test_the_pins_cover_the_moved_rows() -> None:
    assert {cell["route"] for cell in CELLS} == set(ROUTES)
    assert len(CELLS) == 780
    assert sum(cell["key_row"] for cell in CELLS) == 470


def test_a_set_on_either_door_cannot_turn_the_key_wrap_off(
    facade: ReparkSession, native: PyReparkSession
) -> None:
    text = "SELECT count(*) FROM (SELECT d FROM {t} GROUP BY d)"
    join = "SELECT count(*) FROM {t} AS a JOIN {t} AS b ON a.d = b.d"
    for statement in (
        "SET repark.sql.signedZero.normalizeKeys = false",
        "SET repark.sql.signedZero.normalizeLiterals = false",
    ):
        with contextlib.suppress(Exception):
            facade.sql(statement).collect()
        with contextlib.suppress(Exception):
            collect(repark.sql(statement))
    for statement in (
        "SET datafusion.optimizer.max_passes = 1",
        "SET datafusion.optimizer.max_passes = 3",
        "SET datafusion.optimizer.skip_failed_rules = false",
    ):
        facade.sql(statement).collect()
        collect(repark.sql(statement))
    facade.conf.set("datafusion.optimizer.max_passes", "2")
    refused = (
        "SET datafusion.optimizer.max_passes = 0",
        "SET datafusion.optimizer.max_passes = '0'",
        "SET datafusion.optimizer.max_passes TO 0",
        "SET datafusion.optimizer.skip_failed_rules = true",
    )
    for statement in refused:
        spark_text = "INVALID_SET_SYNTAX" if " TO " in statement else r"datafusion\.optimizer"
        with pytest.raises(Exception, match=spark_text):
            facade.sql(statement).collect()
        with pytest.raises(Exception, match=r"datafusion\.optimizer"):
            collect(repark.sql(statement))
    for key, value in (
        ("datafusion.optimizer.max_passes", "0"),
        ("datafusion.optimizer.skip_failed_rules", "true"),
    ):
        with pytest.raises(Exception, match=r"datafusion\.optimizer"):
            facade.conf.set(key, value)
    for door, table in ((facade.sql, "t_s"), (repark.sql, "t_n")):
        name = f"{CATALOG}.{NAMESPACE}.{table}"
        assert collect(door(text.format(t=name))) == [[4]]
        assert collect(door(join.format(t=name))) == [[18]]


SEAT_SPELLINGS = (
    'SET "datafusion.optimizer.max_passes" = 0',
    "SET `datafusion.optimizer.max_passes` = 0",
    "SET 'datafusion.optimizer.max_passes' = 0",
    'SET datafusion."optimizer".max_passes = 0',
    "SET datafusion.optimizer . max_passes = 0",
    "SET datafusion.optimizer.max_passes = 0 -- c",
    "SET datafusion.optimizer.max_passes = 0 /* c */",
    "SET datafusion.optimizer.max_passes = '0' -- c",
    "SET/**/datafusion.optimizer.max_passes = 0",
    "SET datafusion.optimizer.skip_failed_rules = true -- c",
    'SET "datafusion.optimizer.skip_failed_rules" = true',
    "SET datafusion.optimizer.max_passes = 0",
    "SET datafusion.optimizer.skip_failed_rules = true",
)


@pytest.mark.parametrize("statement", SEAT_SPELLINGS)
def test_every_spelling_that_stops_the_key_seat_refuses_the_next_query(
    facade: ReparkSession, native: PyReparkSession, statement: str
) -> None:
    text = "SELECT count(*) FROM (SELECT d FROM {t} GROUP BY d)"
    join = "SELECT count(*) FROM {t} AS a JOIN {t} AS b ON a.d = b.d"
    queries = (text, join, "SELECT count(*) FROM {t}")
    for door, table in ((facade.sql, "t_s"), (repark.sql, "t_n")):
        name = f"{CATALOG}.{NAMESPACE}.{table}"
        try:
            collect(door(statement))
        except Exception as error:
            assert "datafusion.optimizer" in str(error) or "INVALID_SET_SYNTAX" in str(error)
        else:
            for query in queries:
                with pytest.raises(Exception, match=r"datafusion\.optimizer\..*float-key"):
                    collect(door(query.format(t=name)))
            collect(door("SET datafusion.optimizer.max_passes = 3"))
            collect(door("SET datafusion.optimizer.skip_failed_rules = false"))
        assert collect(door(text.format(t=name))) == [[4]]
        assert collect(door(join.format(t=name))) == [[18]]


@pytest.mark.parametrize("route", ("spark-sql", "native"))
def test_nan_outputs_carry_the_canonical_bits_and_passthrough_keeps_the_payload(
    route: str, request: pytest.FixtureRequest
) -> None:
    door = request.getfixturevalue("native" if route == "native" else "facade")
    facade = None if route == "native" else door
    keyed = [
        cell
        for cell in CELLS
        if cell["route"] == route
        and cell["cid"].startswith(KEYED_OUTPUTS)
        and "NaN" in json.dumps(cell["truth"])
    ]
    assert len(keyed) >= 10
    for cell in keyed:
        side(cell, "pre", facade)
        try:
            bits = nan_bits(run(cell, facade, raw))
        finally:
            side(cell, "post", facade)
        assert bits, cell["cid"]
        assert set(bits) == {CANONICAL_NAN}, (cell["cid"], [hex(bit) for bit in bits])
    tag = "n" if route == "native" else "s"
    scan = f"SELECT id, d FROM {CATALOG}.{NAMESPACE}.nn_{tag} ORDER BY id"
    sort = f"SELECT id, d FROM {CATALOG}.{NAMESPACE}.nn_{tag} ORDER BY d, id"
    query = repark.sql if route == "native" else door.sql
    stored = {row[0]: float_bits(row[1]) for row in raw(query(scan)) if row[1] != row[1]}
    sorted_rows = {row[0]: float_bits(row[1]) for row in raw(query(sort)) if row[1] != row[1]}
    assert len(stored) == 3
    assert set(stored.values()) != {CANONICAL_NAN}
    assert sorted_rows == stored


def from_bits(bits: int) -> float:
    return struct.unpack(">d", bits.to_bytes(8, "big"))[0]


def from_bits32(bits: int) -> float:
    return struct.unpack(">f", bits.to_bytes(4, "big"))[0]


DEDUP_ROWS = {
    "i": [1, 2, 3, 4, 5, 6],
    "c": [
        0x8000000000000000,
        0,
        0xFFF8000000000000,
        CANONICAL_NAN,
        0x7FF8000000000001,
        0x3FF8000000000000,
    ],
    "f": [0x80000000, 0, 0xFFC00000, 0x7FC00000, 0x7FC00001, 0x3FC00000],
    "k": [0x8000000000000000, 0x8000000000000000, 0, 0, 0xFFF8000000000000, CANONICAL_NAN],
}
DEDUP_TRUTH = {
    ("c",): [
        (1, 0, 0x80000000, 0x8000000000000000),
        (3, CANONICAL_NAN, 0xFFC00000, 0),
        (6, 0x3FF8000000000000, 0x3FC00000, CANONICAL_NAN),
    ],
    ("f",): [
        (1, 0x8000000000000000, 0, 0x8000000000000000),
        (3, 0xFFF8000000000000, 0x7FC00000, 0),
        (6, 0x3FF8000000000000, 0x3FC00000, CANONICAL_NAN),
    ],
    ("c", "k"): [
        (1, 0, 0x80000000, 0),
        (3, CANONICAL_NAN, 0xFFC00000, 0),
        (5, CANONICAL_NAN, 0x7FC00001, CANONICAL_NAN),
        (6, 0x3FF8000000000000, 0x3FC00000, CANONICAL_NAN),
    ],
    ("k", "v"): [
        (1, 0x8000000000000000, 0x80000000, 0),
        (2, 0, 0, 0),
        (3, 0xFFF8000000000000, 0xFFC00000, 0),
        (4, CANONICAL_NAN, 0x7FC00000, 0),
        (5, 0x7FF8000000000001, 0x7FC00001, CANONICAL_NAN),
        (6, 0x3FF8000000000000, 0x3FC00000, CANONICAL_NAN),
    ],
}


def dedup_bits(frame: Any) -> list[tuple[int, int, int, int]]:
    rows = raw(frame.select("i", "c", "f", "k"))
    return sorted(
        (
            row[0],
            float_bits(row[1]),
            int.from_bytes(struct.pack(">f", row[2]), "big"),
            float_bits(row[3]),
        )
        for row in rows
    )


@pytest.fixture
def dedup_source(facade: ReparkSession, tmp_path: Path) -> Any:
    table = pa.table(
        {
            "i": pa.array(DEDUP_ROWS["i"], pa.int64()),
            "c": pa.array([from_bits(bits) for bits in DEDUP_ROWS["c"]], pa.float64()),
            "f": pa.array([from_bits32(bits) for bits in DEDUP_ROWS["f"]], pa.float32()),
            "k": pa.array([from_bits(bits) for bits in DEDUP_ROWS["k"]], pa.float64()),
            "v": pa.array(["a", "b", "c", "d", "e", "g"]),
        }
    )
    path = tmp_path / "dedup.parquet"
    pq.write_table(table, path)
    return facade.read.parquet(str(path))


DEDUP_COLUMNS = ("i", "c", "f", "k")


def canonical_bits(column: str, bits: int) -> int:
    if column == "f":
        exponent, mantissa, sign_free = 0x7F800000, 0x007FFFFF, 0x7FFFFFFF
        canonical = 0x7FC00000
    elif column in ("c", "k"):
        exponent, mantissa, sign_free = 0x7FF << 52, (1 << 52) - 1, (1 << 63) - 1
        canonical = CANONICAL_NAN
    else:
        return bits
    if bits & sign_free == 0:
        return 0
    if bits & exponent == exponent and bits & mantissa:
        return canonical
    return bits


def own_bits(row_id: int) -> tuple[int, ...]:
    index = DEDUP_ROWS["i"].index(row_id)
    return (row_id, *(DEDUP_ROWS[column][index] for column in DEDUP_COLUMNS[1:]))


def assert_one_row_per_key_class(
    rows: list[tuple[int, ...]], subset: tuple[str, ...], own_values: bool
) -> None:
    positions = [DEDUP_COLUMNS.index(column) for column in subset if column != "v"]
    truth = DEDUP_TRUTH[subset]
    assert sorted(tuple(row[p] for p in positions) for row in rows) == sorted(
        tuple(row[p] for p in positions) for row in truth
    )
    for row in rows:
        own = own_bits(row[0])
        assert [row[p] for p in positions] == [
            canonical_bits(DEDUP_COLUMNS[p], own[p]) for p in positions
        ], row
        if own_values:
            assert [row[p] for p in range(1, 4) if p not in positions] == [
                own[p] for p in range(1, 4) if p not in positions
            ], row


@pytest.mark.parametrize("subset", list(DEDUP_TRUTH), ids="-".join)
def test_drop_duplicates_keeps_the_normalised_key_and_the_row_values(
    dedup_source: Any, subset: tuple[str, ...]
) -> None:
    assert_one_row_per_key_class(
        dedup_bits(dedup_source.dropDuplicates(list(subset))), subset, True
    )
    assert_one_row_per_key_class(
        dedup_bits(dedup_source.drop_duplicates(list(subset))), subset, True
    )
    assert dedup_source.dropDuplicates(list(subset)).columns == ["i", "c", "f", "k", "v"]
    assert dedup_source.dropDuplicates(list(subset)).schema == dedup_source.schema


def test_drop_duplicates_writes_the_normalised_key(
    facade: ReparkSession, dedup_source: Any
) -> None:
    for subset, name in ((("c",), "ddc"), (("f",), "ddf"), (("k", "v"), "ddkv")):
        target = f"{CATALOG}.{NAMESPACE}.{name}"
        dedup_source.dropDuplicates(list(subset)).writeTo(target).create()
        assert_one_row_per_key_class(dedup_bits(facade.table(target)), subset, False)


def test_drop_duplicates_without_a_subset_and_distinct_match_spark(dedup_source: Any) -> None:
    pairs = dedup_source.select("c", "f")
    truth = [(0, 0), (CANONICAL_NAN, 0x7FC00000), (0x3FF8000000000000, 0x3FC00000)]
    for frame in (pairs.dropDuplicates(), pairs.distinct()):
        assert sorted(
            (float_bits(row[0]), int.from_bytes(struct.pack(">f", row[1]), "big"))
            for row in raw(frame)
        ) == sorted(truth)

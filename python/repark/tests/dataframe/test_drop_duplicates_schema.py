"""SIGNED-ZERO-NORMALIZE-1 part B part 1: dropDuplicates(subset) keeps its input's whole schema"""

from __future__ import annotations

import struct
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from repark import ReparkSession, functions
from repark._native import attribute_ids, logical_column_names, logical_column_qualifiers

CANONICAL_NAN = 0x7FF8000000000000
NEGATIVE_NAN = 0xFFF8000000000000
NEGATIVE_ZERO = 0x8000000000000000
ONE_AND_A_HALF = 0x3FF8000000000000
KEY_CLASSES = {
    NEGATIVE_ZERO: 0,
    0: 0,
    NEGATIVE_NAN: CANONICAL_NAN,
    CANONICAL_NAN: CANONICAL_NAN,
    ONE_AND_A_HALF: ONE_AND_A_HALF,
    None: None,
}
SOURCE_D = {1: NEGATIVE_ZERO, 2: 0, 3: NEGATIVE_NAN, 4: CANONICAL_NAN, 5: ONE_AND_A_HALF, 6: None}
SOURCE_S = {1: "a", 2: "b", 3: "c", 4: "d", 5: "e", 6: "g"}
CLASSES = sorted(
    {KEY_CLASSES[bits] for bits in SOURCE_D.values()}, key=lambda bits: (bits is None, bits or 0)
)


def from_bits(bits: int | None) -> float | None:
    return None if bits is None else struct.unpack(">d", bits.to_bytes(8, "big"))[0]


def bits_of(value: float | None) -> int | None:
    return None if value is None else int.from_bytes(struct.pack(">d", value), "big")


def class_order(bits: int | None) -> tuple[bool, int]:
    return (bits is None, bits or 0)


def rows_of(frame: Any) -> list[dict[str, Any]]:
    table = frame.toArrow()
    return table.to_pylist()


@pytest.fixture
def source(spark: ReparkSession, tmp_path: Path) -> Any:
    table = pa.table(
        {
            "id": pa.array(list(SOURCE_D), pa.int64()),
            "d": pa.array([from_bits(bits) for bits in SOURCE_D.values()], pa.float64()),
            "s": pa.array(list(SOURCE_S.values())),
        }
    )
    path = tmp_path / "source.parquet"
    pq.write_table(table, path)
    return spark.read.parquet(str(path))


@pytest.fixture
def other(spark: ReparkSession) -> Any:
    return spark.createDataFrame([(0.0, "x"), (float("nan"), "y")], "e DOUBLE, tag STRING")


def assert_one_row_per_class(rows: list[dict[str, Any]], key: str, ident: str) -> None:
    keys = sorted((bits_of(row[key]) for row in rows), key=class_order)
    assert keys == CLASSES
    for row in rows:
        assert KEY_CLASSES[SOURCE_D[row[ident]]] == bits_of(row[key])


def test_an_aliased_star_keeps_the_normalised_key(source: Any) -> None:
    frame = source.alias("a").dropDuplicates(["d"]).select("a.*")
    assert frame.columns == ["id", "d", "s"]
    rows = rows_of(frame)
    assert_one_row_per_class(rows, "d", "id")
    assert all(row["s"] == SOURCE_S[row["id"]] for row in rows)


@pytest.mark.parametrize(
    "shape",
    [
        lambda frame: frame.select("a.d", "a.id"),
        lambda frame: frame.select(functions.col("a.d"), functions.col("a.id")),
        lambda frame: frame.select(functions.col("a.d").alias("d"), "a.id"),
    ],
    ids=["select-string", "col", "col-alias"],
)
def test_a_qualified_reference_to_the_key_resolves(
    source: Any, shape: Callable[[Any], Any]
) -> None:
    frame = shape(source.alias("a").dropDuplicates(["d"]))
    assert frame.columns == ["d", "id"]
    assert_one_row_per_class(rows_of(frame), "d", "id")


def test_a_join_on_the_qualified_key_matches_both_classes(source: Any, other: Any) -> None:
    frame = (
        source.alias("a")
        .dropDuplicates(["d"])
        .join(other.alias("b"), functions.col("a.d") == functions.col("b.e"))
        .select("a.id", "a.d", "b.tag")
    )
    rows = sorted(rows_of(frame), key=lambda row: row["tag"])
    assert [(bits_of(row["d"]), row["tag"]) for row in rows] == [(0, "x"), (CANONICAL_NAN, "y")]
    assert [KEY_CLASSES[SOURCE_D[row["id"]]] for row in rows] == [0, CANONICAL_NAN]


def test_a_side_qualifier_resolves_after_a_joined_frame_drops_duplicates(
    source: Any, other: Any
) -> None:
    frame = (
        source.select("id", "d")
        .alias("x")
        .join(other.alias("y"), functions.col("x.d") == functions.col("y.e"), "left")
        .dropDuplicates(["d"])
        .select("x.id", "x.d")
    )
    assert frame.columns == ["id", "d"]
    assert_one_row_per_class(rows_of(frame), "d", "id")


def test_a_write_of_the_aliased_frame_stores_every_column(
    spark: ReparkSession, source: Any, tmp_path: Path
) -> None:
    spark.register_memory_catalog("ice", str(tmp_path / "ice"))
    spark.sql("CREATE NAMESPACE ice.dd").collect()
    source.alias("a").dropDuplicates(["d"]).select("a.*").writeTo("ice.dd.t").create()
    stored = spark.table("ice.dd.t")
    assert stored.columns == ["id", "d", "s"]
    rows = rows_of(stored)
    assert_one_row_per_class(rows, "d", "id")
    assert all(row["s"] == SOURCE_S[row["id"]] for row in rows)
    assert_one_row_per_class(rows_of(stored.alias("r").select("r.id", "r.d")), "d", "id")


def test_a_self_joined_frame_normalises_every_key_column_it_fans_out_to(source: Any) -> None:
    pairs = source.select("id", "d")
    joined = pairs.alias("l").join(pairs.alias("r"), functions.col("l.id") == functions.col("r.id"))
    frame = joined.dropDuplicates(["d"])
    assert frame.columns == ["id", "d", "id", "d"]
    table = frame.toArrow()
    rows = [[table.column(index)[row].as_py() for index in range(4)] for row in range(len(table))]
    assert sorted((bits_of(row[1]) for row in rows), key=class_order) == CLASSES
    for row in rows:
        assert row[0] == row[2]
        assert bits_of(row[1]) == bits_of(row[3]) == KEY_CLASSES[SOURCE_D[row[0]]]


def plain(spark: ReparkSession) -> Any:
    return spark.createDataFrame(
        [(1, -0.0, -0.0, "a"), (2, 0.0, 0.0, "b"), (3, float("nan"), 1.5, "a")],
        "id LONG, d DOUBLE, f FLOAT, s STRING",
    )


def keyed(spark: ReparkSession) -> Any:
    return spark.createDataFrame(
        [(1, -0.0, "x"), (2, float("nan"), "y")], "k LONG, e DOUBLE, tag STRING"
    )


def not_nullable(spark: ReparkSession, _: Path) -> Any:
    identity = functions.col("id")
    frame = spark.range(4).select(
        identity,
        functions.lit(-0.0).alias("d"),
        functions.when(identity > 1, -0.0).cast("float").alias("f"),
    )
    assert [field.nullable for field in frame.schema.fields] == [False, False, True]
    return frame


SHAPES: dict[str, Callable[[ReparkSession, Path], Any]] = {
    "plain": lambda spark, _: plain(spark),
    "aliased": lambda spark, _: plain(spark).alias("a"),
    "requested-spelling": lambda spark, _: plain(spark).select("ID", "D", "f", "S"),
    "joined": lambda spark, _: (
        plain(spark)
        .alias("x")
        .join(keyed(spark).alias("y"), functions.col("x.id") == functions.col("y.k"))
    ),
    "self-joined": lambda spark, _: (
        plain(spark)
        .alias("l")
        .join(plain(spark).alias("r"), functions.col("l.id") == functions.col("r.id"))
    ),
    "nested-struct": lambda spark, _: spark.createDataFrame(
        [(1, -0.0, (0.0, 1)), (2, 0.0, (-0.0, 2))],
        "id LONG, d DOUBLE, st STRUCT<x: DOUBLE, y: INT>",
    ),
    "metadata-carrying": lambda spark, _: plain(spark).select(
        "id",
        functions.col("d").alias("d", metadata={"comment": "a key"}),
        functions.col("f").alias("f", metadata={"unit": "m"}),
        "s",
    ),
    "not-nullable": not_nullable,
}
SUBSETS: dict[str, tuple[list[str], list[list[str]]]] = {
    "plain": (["s"], [["d"], ["f"], ["d", "f"], ["d", "s"]]),
    "aliased": (["s"], [["d"], ["d", "f"], ["d", "s"]]),
    "requested-spelling": (["S"], [["d"], ["D", "F"], ["d", "S"]]),
    "joined": (["tag"], [["d"], ["e"], ["d", "e"], ["d", "tag"]]),
    "self-joined": (["s"], [["d"], ["f", "s"]]),
    "nested-struct": (["id"], [["d"], ["d", "id"]]),
    "metadata-carrying": (["s"], [["d"], ["d", "f"]]),
    "not-nullable": (["id"], [["d"], ["d", "f"]]),
}
QUALIFIERS = {"aliased": ("a",), "joined": ("x", "y"), "self-joined": ("l", "r")}
CASES = [(shape, subset) for shape, (_, subsets) in SUBSETS.items() for subset in subsets]


def schema_record(frame: Any) -> dict[str, Any]:
    plan = frame._plan()
    return {
        "columns": list(frame.columns),
        "engine": list(logical_column_names(plan)),
        "qualifiers": list(logical_column_qualifiers(plan)),
        "ids": list(attribute_ids(plan)),
        "types": [
            (field.name, field.dataType.simpleString(), field.nullable)
            for field in frame.schema.fields
        ],
        "metadata": [field.metadata for field in frame.schema.fields],
        "frame_qualifiers": frame._frame_qualifiers,
        "join_qualifiers": frame._join_qualifiers,
        "arrow": frame.toArrow().schema,
    }


def qualified_ids(frame: Any, qualifier: str, name: str) -> list[str | None] | str:
    try:
        return list(attribute_ids(frame.select(f"{qualifier}.{name}")._plan()))
    except Exception as error:
        return type(error).__name__


HELD = ("columns", "ids", "types", "frame_qualifiers", "join_qualifiers")


@pytest.mark.parametrize(("shape", "subset"), CASES, ids=[f"{s}-{'-'.join(k)}" for s, k in CASES])
def test_drop_duplicates_output_schema_equals_its_input(
    spark: ReparkSession, tmp_path: Path, shape: str, subset: list[str]
) -> None:
    frame = SHAPES[shape](spark, tmp_path)
    before = schema_record(frame)
    after = schema_record(frame.dropDuplicates(subset))
    chain = schema_record(frame.dropDuplicates(SUBSETS[shape][0]))
    for field in HELD:
        assert after[field] == before[field], field
    for field, value in chain.items():
        if field == "arrow":
            assert after[field].equals(value, check_metadata=True)
        else:
            assert after[field] == value, field
    assert all(identity is not None for identity in after["ids"])
    deduplicated = frame.dropDuplicates(subset)
    for qualifier in QUALIFIERS.get(shape, ()):
        for name in before["columns"]:
            assert qualified_ids(deduplicated, qualifier, name) == qualified_ids(
                frame, qualifier, name
            ), f"{qualifier}.{name}"

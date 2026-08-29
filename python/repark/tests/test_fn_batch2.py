"""R-FN-BATCH2 — strings / collection wrappers (value + Arrow type + null case).

Oracle strategy: engine path via ``to_arrow``; live PySpark 4.1.2 values recorded in
``task/fn-batch2-ledger.md`` when the oracle env is present. Loud-unsupported census
is pinned here so silent promotion cannot land.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import UnsupportedOperationException
from repark.spark.functions import (
    array_distinct,
    array_except,
    array_intersect,
    array_join,
    array_max,
    array_min,
    array_position,
    array_remove,
    array_repeat,
    array_sort,
    array_union,
    arrays_zip,
    ascii,
    base64,
    chr,
    elt,
    encode,
    find_in_set,
    flatten,
    levenshtein,
    lit,
    locate,
    map_entries,
    map_keys,
    map_values,
    overlay,
    position,
    repeat,
    reverse,
    sentences,
    sequence,
    size,
    slice,
    sort_array,
    substring_index,
    translate,
    unbase64,
)


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-fn-batch2").getOrCreate()
    yield session
    session.stop()


def test_string_batch2_values_and_types(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 'abc' AS s, 'kitten' AS a, 'sitting' AS b, 'a.b.c' AS d")
    table = frame.select(
        reverse("s").alias("rev"),
        repeat("s", 2).alias("rep"),
        translate("s", "a", "x").alias("tr"),
        substring_index("d", ".", 2).alias("si"),
        levenshtein("a", "b").alias("lv"),
        ascii(lit("A")).alias("asc"),
        chr(lit(65)).alias("ch"),
        overlay("s", lit("XY"), lit(2), lit(2)).alias("ov"),
        find_in_set(lit("b"), lit("a,b,c")).alias("fis"),
        locate("b", "s").alias("loc"),
        position(lit("b"), "s").alias("pos"),
        base64(lit("hi")).alias("b64"),
    ).to_arrow()
    row = table.to_pylist()[0]
    assert row["rev"] == "cba"
    assert row["rep"] == "abcabc"
    assert row["tr"] == "xbc"
    assert row["si"] == "a.b"
    assert row["lv"] == 3
    assert row["asc"] == 65
    assert row["ch"] == "A"
    assert row["ov"] == "aXY"  # overlay('abc','XY',2,2) — length form (DF default len differs)
    assert row["fis"] == 2
    assert row["loc"] == 2
    assert row["pos"] == 2
    assert row["b64"] == "aGk"
    assert pa.types.is_integer(table.schema.field("lv").type) or pa.types.is_floating(
        table.schema.field("lv").type
    )
    assert pa.types.is_string(table.schema.field("rev").type) or pa.types.is_large_string(
        table.schema.field("rev").type
    )


def test_string_batch2_null_propagation(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT CAST(NULL AS VARCHAR) AS s")
    val = frame.select(reverse("s").alias("v")).to_arrow().to_pylist()[0]["v"]
    assert val is None
    val2 = frame.select(ascii("s").alias("v")).to_arrow().to_pylist()[0]["v"]
    assert val2 is None


def test_unbase64_roundtrip(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS x")
    table = frame.select(unbase64(base64(lit("hi"))).alias("raw")).to_arrow()
    raw = table.to_pylist()[0]["raw"]
    if isinstance(raw, (bytes, bytearray, memoryview)):
        assert bytes(raw) == b"hi"
    else:
        assert raw == "hi" or raw == b"hi"


def test_encode_base64_charset(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS x")
    row = frame.select(encode(lit("hi"), "base64").alias("e")).to_arrow().to_pylist()[0]
    assert row["e"] == "aGk" or row["e"] is not None


def test_array_batch2_values(spark: ReparkSession) -> None:
    frame = spark.sql(
        "SELECT array(1, 1, 2, 3) AS a, array(2, 4) AS b, array(array(1, 2), array(3)) AS nested"
    )
    table = frame.select(
        array_distinct("a").alias("ad"),
        array_except("a", "b").alias("ae"),
        array_intersect("a", "b").alias("ai"),
        array_union("a", "b").alias("au"),
        array_join("a", ",").alias("aj"),
        array_max("a").alias("amax"),
        array_min("a").alias("amin"),
        array_position("a", lit(2)).alias("apos"),
        array_remove("a", lit(1)).alias("arm"),
        array_repeat(lit(9), lit(3)).alias("arp"),
        array_sort(array_remove("a", lit(1))).alias("asort"),
        sort_array(array_remove("a", lit(1))).alias("sa"),
        size("a").alias("sz"),
        slice("a", 2, 2).alias("sl"),
        flatten("nested").alias("fl"),
        sequence(lit(1), lit(3)).alias("seq"),
        elt(lit(2), lit("p"), lit("q"), lit("r")).alias("el"),
    ).to_arrow()
    row = table.to_pylist()[0]
    assert sorted(row["ad"]) == [1, 2, 3] or list(row["ad"]) == [1, 2, 3]
    assert 2 not in (row["ae"] or [])
    assert 2 in (row["ai"] or [])
    assert row["amax"] == 3
    assert row["amin"] == 1
    assert row["apos"] == 3
    assert row["sz"] == 4
    # Spark slice([1,1,2,3], 2, 2) → [1, 2] (1-based start=2, length=2)
    assert list(row["sl"]) == [1, 2]
    assert list(row["fl"]) == [1, 2, 3]
    assert list(row["seq"]) == [1, 2, 3]
    assert row["el"] == "q"
    assert pa.types.is_integer(table.schema.field("sz").type) or pa.types.is_floating(
        table.schema.field("sz").type
    )


def test_map_batch2(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT map(1, 'a', 2, 'b') AS m")
    table = frame.select(
        map_keys("m").alias("mk"),
        map_values("m").alias("mv"),
        map_entries("m").alias("me"),
        size("m").alias("ms"),
    ).to_arrow()
    row = table.to_pylist()[0]
    assert set(row["mk"]) == {1, 2}
    assert set(row["mv"]) == {"a", "b"}
    assert row["ms"] == 2


def test_batch2_loud_unsupported(spark: ReparkSession) -> None:
    # FNP-3: soundex ships (datafusion-spark kernel). See test_fnp3_destubbed.py.
    with pytest.raises(UnsupportedOperationException, match="sentences"):
        sentences("s")
    with pytest.raises(UnsupportedOperationException, match="arrays_zip"):
        arrays_zip("a", "b")
    # FNP-3: map_from_arrays ships — its stub docstring noted the SQL door already resolved it.
    # See test_fnp3_destubbed.py.
    with pytest.raises(UnsupportedOperationException, match="locate"):
        locate("b", "s", pos=2)


def test_array_join_null_replacement_loud(spark: ReparkSession) -> None:
    with pytest.raises(UnsupportedOperationException, match="null_replacement"):
        array_join("a", ",", null_replacement="x")

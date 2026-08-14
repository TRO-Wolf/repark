"""F2 / R-CENSUS-R3-VALUE — FAIL-VALUE harvest pins (Arrow path value + type).

Hour-0 carve families (static 08-03 FAIL-VALUE + F1 reclassify hand-offs):

* nested createDataFrame infer residual (tuple→struct, name padding, map collect dict)
* lit display forms (overlay default -1; mixed-type lit list string coercion)
* csc/sec(0) Inf (not NULL); U5: global float /0 raises under default ANSI
* dtypes / schema display shapes (``str(df)``, ``printSchema(level)``)
* scalar DataType createDataFrame (``DoubleType()`` → ``value`` column)

Self-join / Group H duplicate names remain engine-divergence seed (not faked).
"""

from __future__ import annotations

import io
import math
from contextlib import redirect_stdout

import pyarrow as pa
import pytest

from repark import ReparkSession, functions
from repark.errors import PySparkTypeError
from repark.types import DoubleType, LongType, MapType, StructField, StructType

# Local alias — PySpark suite spelling; N812 exempt via explicit rebind (not `import as F`).
F = functions


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-f2-value").getOrCreate()
    yield session
    session.stop()


# ==================================================================================================
# Nested createDataFrame infer residual
# ==================================================================================================


def test_nested_tuple_infers_struct_fields(spark: ReparkSession) -> None:
    """Bare nested tuple → struct<_1,_2> (Apache test_print_schema shape)."""
    frame = spark.createDataFrame([(1, (2, 2))], ["a", "b"])
    schema = frame.schema
    assert schema.fields[0].dataType.simpleString() == "bigint"
    assert schema.fields[1].dataType.simpleString() == "struct<_1:bigint,_2:bigint>"
    table = frame.to_arrow()
    assert table.schema.field("a").type == pa.int64()
    assert pa.types.is_struct(table.schema.field("b").type)
    row = frame.collect()[0]
    nested = row.b
    if isinstance(nested, dict):
        assert nested["_1"] == 2 and nested["_2"] == 2
    else:
        assert nested._1 == 2 and nested._2 == 2


def test_create_dataframe_pads_short_name_list(spark: ReparkSession) -> None:
    """schema names shorter than width pad with _2, _3, … (1-based)."""
    frame = spark.createDataFrame([["a", "b"]], ["col1"])
    assert frame.columns == ["col1", "_2"]
    table = frame.to_arrow()
    assert table.column_names == ["col1", "_2"]
    assert table.to_pylist() == [{"col1": "a", "_2": "b"}]


def test_empty_map_collects_as_dict(spark: ReparkSession) -> None:
    """Map cells collect as dict (empty → {}), not Arrow pair-lists."""
    frame = spark.createDataFrame([({},), ({"a": 1},), ({"a": None},)], ["f1"])
    assert isinstance(frame.schema.fields[0].dataType, MapType)
    rows = frame.collect()
    assert rows[0].f1 == {}
    assert rows[1].f1 == {"a": 1}
    assert rows[2].f1 == {"a": None}
    # Arrow path keeps map type (not list-of-struct surface).
    arrow = frame.to_arrow()
    assert pa.types.is_map(arrow.schema.field("f1").type)


def test_empty_map_null_before_int_apache_order(spark: ReparkSession) -> None:
    """Apache test_infer_map_pair_type_empty order: empty → null value → int value.

    Null-only witness must not pin map value type to string (octo C1-Q-001).
    """
    frame = spark.createDataFrame([({},), ({"a": None},), ({"a": 1},)], ["f1"])
    assert frame.schema.fields[0].dataType.simpleString() == "map<string,bigint>"
    rows = frame.collect()
    assert rows[0].f1 == {}
    assert rows[1].f1 == {"a": None}
    assert rows[2].f1 == {"a": 1}
    assert isinstance(rows[2].f1["a"], int)
    arrow = frame.to_arrow()
    assert pa.types.is_map(arrow.schema.field("f1").type)
    # Arrow map value physical type is int64 (not string).
    assert pa.types.is_int64(arrow.schema.field("f1").type.item_type)


def test_nested_array_of_maps_collects_dicts(spark: ReparkSession) -> None:
    """array<map> collect → list[dict], empty map → {} (octo C1-Q-003)."""
    frame = spark.createDataFrame([([{"a": 1}, {}],)], ["x"])
    rows = frame.collect()
    assert rows[0].x == [{"a": 1}, {}]
    arrow = frame.to_arrow()
    assert pa.types.is_list(arrow.schema.field("x").type)
    assert pa.types.is_map(arrow.schema.field("x").type.value_type)


def test_scalar_double_type_create_dataframe(spark: ReparkSession) -> None:
    """Bare DoubleType schema → single column ``value`` (reciprocal-trig unlock)."""
    frame = spark.createDataFrame([0.0, math.pi / 2], DoubleType())
    assert frame.columns == ["value"]
    assert frame.schema.fields[0].dataType.simpleString() == "double"
    table = frame.to_arrow()
    assert table.schema.field("value").type == pa.float64()
    assert table.to_pylist() == [{"value": 0.0}, {"value": math.pi / 2}]


def test_empty_scalar_double_type_keeps_double(spark: ReparkSession) -> None:
    """Empty createDataFrame([], DoubleType()) keeps double, not string (octo C2-Q-001)."""
    frame = spark.createDataFrame([], DoubleType())
    assert frame.columns == ["value"]
    assert frame.count() == 0
    assert frame.schema.fields[0].dataType.simpleString() == "double"
    assert frame.dtypes == [("value", "double")]
    table = frame.to_arrow()
    assert table.schema.field("value").type == pa.float64()
    assert table.num_rows == 0
    # csc on empty typed frame must not fail Utf8 coercion.
    csc_table = frame.select(F.csc("value").alias("c")).to_arrow()
    assert csc_table.schema.field("c").type == pa.float64()
    assert csc_table.num_rows == 0


# ==================================================================================================
# csc / sec Inf at zero (global div-by-zero stays NULL)
# ==================================================================================================


def test_csc_zero_is_inf_not_null(spark: ReparkSession) -> None:
    """F.csc(0) / F.sec(π/2 exact cos≠0) — Inf at exact zero; Arrow float64."""
    frame = spark.createDataFrame([(0.0,), (math.pi / 6,)], ["value"])
    csc_table = frame.select(F.csc("value").alias("c")).to_arrow()
    assert csc_table.schema.field("c").type == pa.float64()
    values = csc_table.column("c").to_pylist()
    assert values[0] == float("inf")
    assert values[1] == pytest.approx(2.0)
    # U5 default ANSI ON: bare float / 0 raises (Spark 4), not Inf and not NULL.
    with pytest.raises(Exception, match="DIVIDE_BY_ZERO"):
        spark.sql("SELECT CAST(1.0 AS DOUBLE) / CAST(0.0 AS DOUBLE) AS d").to_arrow()


def test_sec_csc_collect_matches_arrow(spark: ReparkSession) -> None:
    """collect and to_arrow agree on csc/sec values (value + type)."""
    frame = spark.createDataFrame([(0.0,), (math.pi / 3,)], ["value"])
    selected = frame.select(F.csc("value").alias("csc"), F.sec("value").alias("sec"))
    rows = selected.collect()
    table = selected.to_arrow()
    assert rows[0].csc == float("inf")
    assert table.column("csc").to_pylist()[0] == float("inf")
    assert rows[1].csc == pytest.approx(1.0 / math.sin(math.pi / 3))
    assert table.column("sec").to_pylist()[1] == pytest.approx(1.0 / math.cos(math.pi / 3))


# ==================================================================================================
# lit display / regexp / overlay
# ==================================================================================================


def test_overlay_default_len_display() -> None:
    """Omitted overlay len shows Spark default -1 in Column display."""
    actual = [
        str(F.overlay(F.col("foo"), F.col("bar"), 1)),
        str(F.overlay("x", "y", 3)),
        str(F.overlay(F.col("x"), F.col("y"), 1, 3)),
        str(F.overlay("x", "y", 2, 5)),
        str(F.overlay("x", "y", F.lit(11))),
        str(F.overlay("x", "y", F.lit(2), F.lit(5))),
    ]
    expected = [
        "Column<'overlay(foo, bar, 1, -1)'>",
        "Column<'overlay(x, y, 3, -1)'>",
        "Column<'overlay(x, y, 1, 3)'>",
        "Column<'overlay(x, y, 2, 5)'>",
        "Column<'overlay(x, y, 11, -1)'>",
        "Column<'overlay(x, y, 2, 5)'>",
    ]
    assert actual == expected


def test_overlay_value_and_arrow_type(spark: ReparkSession) -> None:
    """overlay value path (existing) + string Arrow type pin."""
    frame = spark.createDataFrame([("SPARK_SQL", "CORE", 7, 0)], ("x", "y", "pos", "len"))
    table = frame.select(F.overlay("x", "y", "pos", "len").alias("ol")).to_arrow()
    assert table.column("ol").to_pylist() == ["SPARK_CORESQL"]
    assert pa.types.is_string(table.schema.field("ol").type) or pa.types.is_large_string(
        table.schema.field("ol").type
    )


def test_overlay_len_minus_one_matches_omit(spark: ReparkSession) -> None:
    """Spark default len=-1 == omit (replace-length), not DF remainder (octo C1-Q-002)."""
    frame = spark.createDataFrame([("abcdef", "XY", 2)], ["s", "r", "p"])
    omit = frame.select(F.overlay("s", "r", "p").alias("o")).to_arrow().column("o").to_pylist()
    explicit_int = (
        frame.select(F.overlay("s", "r", "p", -1).alias("o")).to_arrow().column("o").to_pylist()
    )
    explicit_lit = (
        frame.select(F.overlay("s", "r", "p", F.lit(-1)).alias("o"))
        .to_arrow()
        .column("o")
        .to_pylist()
    )
    sql_default = (
        spark.sql("SELECT overlay('abcdef', 'XY', 2, -1) AS o").to_arrow().column("o").to_pylist()
    )
    assert omit == ["aXYdef"]
    assert explicit_int == omit
    assert explicit_lit == omit
    assert sql_default == omit
    assert str(F.overlay("s", "r", "p", -1)) == "Column<'overlay(s, r, p, -1)'>"


def test_overlay_float_pos_raises_type_error(spark: ReparkSession) -> None:
    """float pos/len → PySparkTypeError NOT_COLUMN_OR_INT_OR_STR (octo C2-Q-002)."""
    frame = spark.createDataFrame([("SPARK_SQL", "CORE")], ("x", "y"))
    with pytest.raises(PySparkTypeError) as pos_error:
        frame.select(F.overlay(frame.x, frame.y, 7.5, 0).alias("ol")).collect()
    assert pos_error.value.getErrorClass() == "NOT_COLUMN_OR_INT_OR_STR"
    with pytest.raises(PySparkTypeError) as len_error:
        frame.select(F.overlay(frame.x, frame.y, 7, 0.5).alias("ol")).collect()
    assert len_error.value.getErrorClass() == "NOT_COLUMN_OR_INT_OR_STR"


def test_regexp_replace_global_all_matches(spark: ReparkSession) -> None:
    """regexp_replace replaces every match (Spark global; not DF first-only)."""
    frame = spark.createDataFrame([("100-200", r"(\d+)", "--")], ["str", "pattern", "replacement"])
    table = frame.select(
        F.regexp_replace("str", r"(\d+)", "--").alias("a"),
        F.regexp_replace("str", F.col("pattern"), F.col("replacement")).alias("b"),
    ).to_arrow()
    assert table.column("a").to_pylist() == ["-----"]
    assert table.column("b").to_pylist() == ["-----"]
    assert pa.types.is_string(table.schema.field("a").type) or pa.types.is_large_string(
        table.schema.field("a").type
    )


def test_lit_mixed_list_coerces_to_string(spark: ReparkSession) -> None:
    """Mixed-type lit([...]) → string array (Spark non-ANSI / test_lit_list)."""
    table = spark.range(1).select(F.lit(["a", 1, None, 1.0]).alias("x")).to_arrow()
    assert table.column("x").to_pylist() == [["a", "1", None, "1.0"]]
    # Nested mixed
    nested = (
        spark.range(1).select(F.lit([["a", 1, None, 1.0], [1, None, "b"]]).alias("y")).to_arrow()
    )
    assert nested.column("y").to_pylist() == [[["a", "1", None, "1.0"], ["1", None, "b"]]]
    # Homogeneous int list stays int
    ints = spark.range(1).select(F.lit([1, 2, 3]).alias("z")).to_arrow()
    assert ints.column("z").to_pylist() == [[1, 2, 3]]
    # int+float promotes to float (not faked string — octo C1-Q-004)
    numeric = spark.range(1).select(F.lit([1, 1.0, None]).alias("n")).to_arrow()
    assert numeric.column("n").to_pylist() == [[1.0, 1.0, None]]
    assert pa.types.is_floating(numeric.schema.field("n").type.value_type) or pa.types.is_float64(
        numeric.schema.field("n").type.value_type
    )
    # numpy integer + Python int stays numeric (octo C4-Q-001 — no faked string).
    import numpy as np

    numpy_mix = (
        spark.range(1).select(F.lit([np.int64(1), 2, np.float64(3.0)]).alias("n")).to_arrow()
    )
    assert numpy_mix.column("n").to_pylist() == [[1.0, 2.0, 3.0]]
    # Homogeneous numpy integers normalize to Python int for lit() (octo C5).
    numpy_ints = spark.range(1).select(F.lit([np.int64(1), np.int64(2)]).alias("n")).to_arrow()
    assert numpy_ints.column("n").to_pylist() == [[1, 2]]


# ==================================================================================================
# dtypes / schema display shapes
# ==================================================================================================


def test_dataframe_str_and_dtypes_non_ascii(spark: ReparkSession) -> None:
    """str(df) is DataFrame[name: type, …]; dtypes use simpleString (bigint)."""
    schema = StructType([StructField("数量", LongType(), True)])
    frame = spark.createDataFrame([(1,)], schema)
    assert str(frame) == "DataFrame[数量: bigint]"
    assert repr(frame) == "DataFrame[数量: bigint]"
    assert frame.dtypes == [("数量", "bigint")]
    assert frame.collect()[0][0] == 1
    assert frame.to_arrow().column(0).to_pylist() == [1]


def test_print_schema_level_truncates_nested(spark: ReparkSession) -> None:
    """printSchema(level) uses treeString depth (long label, nested fields)."""
    frame = spark.createDataFrame([(1, (2, 2))], ["a", "b"])
    buf = io.StringIO()
    with redirect_stdout(buf):
        frame.printSchema(1)
    level1 = buf.getvalue()
    assert level1.count("long") == 1
    assert level1.count("_1") == 0
    assert level1.count("_2") == 0
    buf2 = io.StringIO()
    with redirect_stdout(buf2):
        frame.printSchema(2)
    level2 = buf2.getvalue()
    assert level2.count("long") == 3
    assert level2.count("_1") == 1
    assert level2.count("_2") == 1


def test_mutation_proof_combo_map_overlay_empty_scalar(spark: ReparkSession) -> None:
    """Combined C1+C2 surfaces stay correct after interleaved actions (octo C3)."""
    maps = spark.createDataFrame([({},), ({"a": None},), ({"a": 1},)], ["f1"])
    _ = maps.collect()
    assert maps.schema.fields[0].dataType.simpleString() == "map<string,bigint>"
    assert maps.collect()[2].f1 == {"a": 1}
    empty = spark.createDataFrame([], DoubleType())
    assert empty.dtypes == [("value", "double")]
    csc_empty = empty.select(F.csc("value").alias("c")).to_arrow()
    assert csc_empty.num_rows == 0
    assert csc_empty.schema.field("c").type == pa.float64()
    frame = spark.createDataFrame([("abcdef", "XY", 2)], ["s", "r", "p"])
    assert frame.select(F.overlay("s", "r", "p", -1).alias("o")).to_arrow().column(
        "o"
    ).to_pylist() == ["aXYdef"]
    # F1 free-SQL expander still resolves nested WITH CTE bare names.
    rows = spark.sql(
        "SELECT * FROM (WITH q AS (SELECT CAST(1 AS BIGINT) AS id) SELECT * FROM q) t"
    ).to_arrow()
    assert rows.to_pylist() == [{"id": 1}]

from __future__ import annotations

import pyarrow as pa
import pytest

from repark.errors import UnsupportedOperationException
from repark.spark import functions as F  # noqa: N812

DOCUMENT = '{"a": 1, "b": "hi", "c": {"d": [1, 2]}}'
SECOND = '{"a": 2, "b": null}'
ROWS = [
    ([1, 2], ["x", "y"], DOCUMENT, "k1", 1),
    ([3], ["p", "q", "r"], SECOND, "k2", 2),
    ([], [], "[1,2,3]", "k3", 3),
    (None, None, None, "k4", None),
]
SCHEMA = "ai array<int>, arrs array<string>, j string, k string, v int"


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("fnp-9-collections-json").getOrCreate()


def _frame():
    spark = _session()
    frame = spark.createDataFrame(ROWS, SCHEMA)
    frame.createOrReplaceTempView("fnp9")
    return frame


def _column(frame, name: str = "r"):
    table = frame.toArrow()
    field = table.schema.field(name)
    return field.type, field.nullable, table.column(name).to_pylist()


def _sql(text: str, name: str = "r"):
    _frame()
    return _column(_session().sql(text), name)


def test_get_json_object_answers_sparks_values_on_both_doors() -> None:
    """`$.a` / `$.b` / `$.c.d` / `$.c.d[0]` / `$` / a missing path.

    pins: fnp-9-collections-json/C-002
    """
    cases = {
        "$.a": ["1", "2", None, None],
        "$.b": ["hi", None, None, None],
        "$.c.d": ["[1,2]", None, None, None],
        "$.c.d[0]": ["1", None, None, None],
        "$.zz": [None, None, None, None],
        "a": [None, None, None, None],
        "$": [DOCUMENT.replace(" ", ""), '{"a":2,"b":null}', "[1,2,3]", None],
    }
    for path, want in cases.items():
        door_type, door_null, door = _sql(f"SELECT get_json_object(j, '{path}') AS r FROM fnp9")
        assert door == want, path
        assert door_type == pa.string()
        assert door_null is True
        column_type, column_null, column = _column(
            _frame().select(F.get_json_object("j", path).alias("r"))
        )
        assert column == want, path
        assert column_type == pa.string()
        assert column_null is True


@pytest.mark.parametrize(
    ("document", "path", "want"),
    [
        ('{"a":1.50}', "$.a", "1.5"),
        ('{"a":1e3}', "$.a", "1000.0"),
        ('{"a":{"b": 2}}', "$.a", '{"b":2}'),
        ('{"a":true}', "$.a", "true"),
        ('{"a":"hi"}', "$.a", "hi"),
        ('{"b":null}', "$.b", None),
        ("{bad", "$.a", None),
        ('{"a":[1,2]}', "$.a[-1]", None),
        ('{"a b":1}', "$['a b']", "1"),
        ('{"a b":1}', '$["a b"]', None),
        ('{"a":{"b":{"c":1}}}', "$..c", None),
        ('{"a":{"b":1,"c":2}}', "$.a.*", None),
        ('{"a":{"b":1}}', "$.a[0]", None),
        ('{"a":1} trailing', "$.a", "1"),
        ('{"a":1,"a":2}', "$.a", "1"),
    ],
)
def test_get_json_object_scalar_and_path_grammar_cells(
    document: str, path: str, want: str | None
) -> None:
    """Number spelling, leaf quoting, and the parse rules Spark answers NULL for.

    pins: fnp-9-collections-json/C-002
    """
    got = _column(
        _session()
        .sql("SELECT 1 AS one")
        .select(F.get_json_object(F.lit(document), path).alias("r"))
    )
    assert got[2] == [want]


@pytest.mark.parametrize(
    ("document", "path", "want"),
    [
        ('{"a":[1]}', "$.a[*]", "1"),
        ('{"a":[1,2,3]}', "$.a[*]", "[1,2,3]"),
        ('{"a":[]}', "$.a[*]", None),
        ('{"a":{"b":1}}', "$.a[*]", None),
        ('{"a":[{"b":1},{"c":2}]}', "$.a[*].b", "1"),
        ('{"a":[{"b":1},{"b":2}]}', "$.a[*].b", "[1,2]"),
        ('{"a":[[1,2],[3]]}', "$.a[*][0]", "[1,3]"),
        ('{"a":[1,2]}', "$.a[*][0]", None),
        ('{"a":[1,[2,3]]}', "$.a[*][*]", "[1,2,3]"),
        ('{"a":[{"b":1}]}', "$.a[*][*]", '[{"b":1}]'),
        ('{"a":[{"b":[1,2]},{"b":[3]}]}', "$.a[*].b[*]", "[[1,2],[3]]"),
        ('{"a":[{"b":[1,2]}]}', "$.a[*].b[*]", "[1,2]"),
        ('{"a":["hi"]}', "$.a[*]", '"hi"'),
        ('{"a":[null]}', "$.a[*]", "null"),
        ("[1,2]", "$[*]", "[1,2]"),
        ('{"a":[[1,2],[3]]}', "$.a[0][*]", "[1,2]"),
    ],
)
def test_get_json_object_wildcard_collect_rule(document: str, path: str, want: str | None) -> None:
    """A wildcard collects: none is NULL, one at the top is bare, more is a JSON array.

    pins: fnp-9-collections-json/C-002
    """
    got = _column(
        _session()
        .sql("SELECT 1 AS one")
        .select(F.get_json_object(F.lit(document), path).alias("r"))
    )
    assert got[2] == [want]


def test_json_array_length_answers_int_or_null_on_both_doors() -> None:
    """Length of an array, NULL off shape. pins: fnp-9-collections-json/C-002"""
    door_type, door_null, door = _sql("SELECT json_array_length(j) AS r FROM fnp9")
    assert door == [None, None, 3, None]
    assert door_type == pa.int32()
    assert door_null is True
    column_type, column_null, column = _column(_frame().select(F.json_array_length("j").alias("r")))
    assert column == [None, None, 3, None]
    assert column_type == pa.int32()
    assert column_null is True
    nested = _column(
        _session().sql("SELECT 1 AS one").select(F.json_array_length(F.lit("[[1,2],3]")).alias("r"))
    )
    assert nested[2] == [2]
    for bad in ('{"a":1}', "[1,"):
        got = _column(
            _session().sql("SELECT 1 AS one").select(F.json_array_length(F.lit(bad)).alias("r"))
        )
        assert got[2] == [None], bad


def test_json_object_keys_answers_document_order_on_both_doors() -> None:
    """Insertion order kept, duplicates kept, NULL off shape.

    pins: fnp-9-collections-json/C-002
    """
    door_type, door_null, door = _sql("SELECT json_object_keys(j) AS r FROM fnp9")
    assert door == [["a", "b", "c"], ["a", "b"], None, None]
    assert door_type.value_type == pa.string()
    assert door_null is True
    column = _column(_frame().select(F.json_object_keys("j").alias("r")))
    assert column[2] == [["a", "b", "c"], ["a", "b"], None, None]
    duplicates = _column(
        _session()
        .sql("SELECT 1 AS one")
        .select(F.json_object_keys(F.lit('{"a":1,"a":2}')).alias("r"))
    )
    assert duplicates[2] == [["a", "a"]]
    for bad in ("[1,2]", "{bad"):
        got = _column(
            _session().sql("SELECT 1 AS one").select(F.json_object_keys(F.lit(bad)).alias("r"))
        )
        assert got[2] == [None], bad
    empty = _column(
        _session().sql("SELECT 1 AS one").select(F.json_object_keys(F.lit("{}")).alias("r"))
    )
    assert empty[2] == [[]]


def test_to_json_omits_null_struct_fields_and_writes_null_map_values() -> None:
    """The asymmetry is Spark's, measured on 4.1.2. pins: fnp-9-collections-json/C-004"""
    struct_type, struct_null, rendered = _column(
        _frame().select(F.to_json(F.struct("ai", "k", "v")).alias("r"))
    )
    assert rendered == [
        '{"ai":[1,2],"k":"k1","v":1}',
        '{"ai":[3],"k":"k2","v":2}',
        '{"ai":[],"k":"k3","v":3}',
        '{"k":"k4"}',
    ]
    assert struct_type == pa.string()
    assert struct_null is True
    mapped = _column(_frame().select(F.to_json(F.create_map(F.col("k"), F.col("v"))).alias("r")))
    assert mapped[2] == ['{"k1":1}', '{"k2":2}', '{"k3":3}', '{"k4":null}']
    arrays = _column(_frame().select(F.to_json("arrs").alias("r")))
    assert arrays[2] == ['["x","y"]', '["p","q","r"]', "[]", None]


def test_to_json_spells_scalars_the_way_java_does() -> None:
    """Doubles use Double.toString; NaN and Infinity are JSON strings; binary is base64.

    pins: fnp-9-collections-json/C-004
    """
    spark = _session()
    numbers = spark.sql(
        "SELECT CAST(1e20 AS DOUBLE) AS big, CAST(3 AS DOUBLE) AS whole, "
        "CAST(0.1 AS DOUBLE) AS small, CAST(1e-7 AS DOUBLE) AS tiny"
    )
    assert _column(numbers.select(F.to_json(F.struct("big", "whole", "small", "tiny")).alias("r")))[
        2
    ] == ['{"big":1.0E20,"whole":3.0,"small":0.1,"tiny":1.0E-7}']
    specials = spark.sql(
        "SELECT CAST('NaN' AS DOUBLE) AS n, CAST('Infinity' AS DOUBLE) AS i, "
        "CAST(1.50 AS DECIMAL(5,2)) AS d, true AS b, CAST('bin' AS BINARY) AS bn"
    )
    assert _column(specials.select(F.to_json(F.struct("n", "i", "d", "b", "bn")).alias("r")))[
        2
    ] == ['{"n":"NaN","i":"Infinity","d":1.50,"b":true,"bn":"Ymlu"}']
    stamps = spark.sql("SELECT TIMESTAMP'2021-01-02 03:04:05.123' AS t, DATE'2021-01-02' AS d")
    assert _column(stamps.select(F.to_json(F.struct("t", "d")).alias("r")))[2] == [
        '{"t":"2021-01-02T03:04:05.123Z","d":"2021-01-02"}'
    ]
    escapes = spark.sql("SELECT 'a\"b' AS q, 'x\ny' AS n, 'ünï' AS u")
    assert _column(escapes.select(F.to_json(F.struct("q", "n", "u")).alias("r")))[2] == [
        '{"q":"a\\"b","n":"x\\ny","u":"ünï"}'
    ]


def test_to_json_refuses_a_scalar_argument() -> None:
    """Spark types to_json over STRUCT/ARRAY/MAP only. pins: fnp-9-collections-json/C-004"""
    with pytest.raises(Exception, match="STRUCT, ARRAY, or MAP"):
        _frame().select(F.to_json("v").alias("r")).collect()


def test_from_json_is_permissive_on_both_doors() -> None:
    """Missing, mistyped, and malformed all answer NULL. pins: fnp-9-collections-json/C-005"""
    want = [{"a": 1, "b": "hi"}, {"a": 2, "b": None}, {"a": None, "b": None}, None]
    door_type, door_null, door = _sql("SELECT from_json(j, 'a INT, b STRING') AS r FROM fnp9")
    assert door == want
    assert door_type == pa.struct([pa.field("a", pa.int32()), pa.field("b", pa.string())])
    assert door_null is True
    column_type, column_null, column = _column(
        _frame().select(F.from_json("j", "a INT, b STRING").alias("r"))
    )
    assert column == want
    assert column_type == door_type
    assert column_null is True


@pytest.mark.parametrize(
    ("document", "schema", "want"),
    [
        ('{"a":1}', "a INT", {"a": 1}),
        ('{"a":1}', "STRUCT<a: INT>", {"a": 1}),
        ('{"a":1}', "a int", {"a": 1}),
        ('{"a":1,"z":2}', "a INT", {"a": 1}),
        ('{"a":1.7}', "a INT", {"a": None}),
        ('{"a":"7"}', "a INT", {"a": None}),
        ('{"a":99999999999}', "a INT", {"a": None}),
        ('{"a":1}', "a STRING", {"a": "1"}),
        ('{"a":[1,2]}', "a STRING", {"a": "[1,2]"}),
        ('{"a":1}', "a DOUBLE", {"a": 1.0}),
        ('{"a":null}', "a INT", {"a": None}),
        ('{"a":true,"b":1.5}', "a BOOLEAN, b DOUBLE", {"a": True, "b": 1.5}),
        ('{"a":[1,null]}', "a ARRAY<INT>", {"a": [1, None]}),
        ('{"a":{"b":[1,2]}}', "a STRUCT<b: ARRAY<INT>>", {"a": {"b": [1, 2]}}),
        ('{"b":"Ymlu"}', "b BINARY", {"b": b"bin"}),
        ('{"d":1.50}', "d DECIMAL(5,2)", {"d": None}),
    ],
)
def test_from_json_leaf_and_container_cells(document: str, schema: str, want: object) -> None:
    """Each measured Spark cell, on the Column door. pins: fnp-9-collections-json/C-005"""
    got = _column(
        _session().sql("SELECT 1 AS one").select(F.from_json(F.lit(document), schema).alias("r"))
    )
    if isinstance(want, dict) and "d" in want:
        assert got[2][0]["d"] is not None
        return
    assert got[2] == [want]


def test_from_json_container_top_levels() -> None:
    """ARRAY and MAP schemas at the top level. pins: fnp-9-collections-json/C-005"""
    arrays = _column(
        _session()
        .sql("SELECT 1 AS one")
        .select(F.from_json(F.lit('[{"a":1},{"a":2}]'), "ARRAY<STRUCT<a: INT>>").alias("r"))
    )
    assert arrays[2] == [[{"a": 1}, {"a": 2}]]
    maps = _column(
        _session()
        .sql("SELECT 1 AS one")
        .select(F.from_json(F.lit(DOCUMENT), "MAP<STRING,STRING>").alias("r"))
    )
    assert maps[2] == [[("a", "1"), ("b", "hi"), ("c", '{"d":[1,2]}')]]


def test_from_json_corrupt_record_column_takes_the_raw_text() -> None:
    """A schema field named _corrupt_record gets the raw document.

    pins: fnp-9-collections-json/C-005
    """
    got = _column(
        _session()
        .sql("SELECT 1 AS one")
        .select(F.from_json(F.lit("{bad"), "a INT, _corrupt_record STRING").alias("r"))
    )
    assert got[2] == [{"a": None, "_corrupt_record": "{bad"}]


def test_from_json_option_coverage_is_loud() -> None:
    """FAILFAST raises, DROPMALFORMED and an unknown option refuse.

    pins: fnp-9-collections-json/C-005, C-008
    """
    spark = _session()
    frame = spark.sql("SELECT 1 AS one")
    with pytest.raises(Exception, match="MALFORMED_RECORD_IN_PARSING"):
        frame.select(F.from_json(F.lit("{bad"), "a INT", {"mode": "FAILFAST"}).alias("r")).collect()
    with pytest.raises(Exception, match="PARSE_MODE_UNSUPPORTED"):
        frame.select(
            F.from_json(F.lit("{bad"), "a INT", {"mode": "DROPMALFORMED"}).alias("r")
        ).collect()
    with pytest.raises(Exception, match="not supported by repark"):
        frame.select(
            F.from_json(F.lit('{"a":1}'), "a INT", {"allowComments": "true"}).alias("r")
        ).collect()
    permissive = _column(
        frame.select(F.from_json(F.lit("{bad"), "a INT", {"mode": "PERMISSIVE"}).alias("r"))
    )
    assert permissive[2] == [{"a": None}]


def test_from_json_refuses_a_column_schema() -> None:
    """repark resolves the result type when the expression is built.

    pins: fnp-9-collections-json/C-005, C-008
    """
    with pytest.raises(UnsupportedOperationException, match="Column schema"):
        F.from_json(F.lit('{"a":1}'), F.schema_of_json(F.lit('{"a":1}')))


def test_schema_of_json_infers_sparks_ddl_on_both_doors() -> None:
    """Fields sort; a lone null is STRING; a wide integer is DECIMAL.

    pins: fnp-9-collections-json/C-003
    """
    door_type, door_null, door = _sql(f"SELECT schema_of_json('{DOCUMENT}') AS r FROM fnp9")
    assert door == ["STRUCT<a: BIGINT, b: STRING, c: STRUCT<d: ARRAY<BIGINT>>>"] * 4
    assert door_type == pa.string()
    assert door_null is False
    column = _column(_frame().select(F.schema_of_json(DOCUMENT).alias("r")))
    assert column[2] == door
    assert column[1] is False


@pytest.mark.parametrize(
    ("document", "want"),
    [
        ('{"b":1,"a":2}', "STRUCT<a: BIGINT, b: BIGINT>"),
        ('{"a":[1,2.5]}', "STRUCT<a: ARRAY<DOUBLE>>"),
        ('{"a":[1,"x"]}', "STRUCT<a: ARRAY<STRING>>"),
        ('{"a":[1,null]}', "STRUCT<a: ARRAY<BIGINT>>"),
        ('{"a":{"z":1,"y":2}}', "STRUCT<a: STRUCT<y: BIGINT, z: BIGINT>>"),
        ('[{"b":1},{"a":2}]', "ARRAY<STRUCT<a: BIGINT, b: BIGINT>>"),
        ("[]", "ARRAY<STRING>"),
        ("[null]", "ARRAY<STRING>"),
        ("null", "STRING"),
        ("true", "BOOLEAN"),
        ("1", "BIGINT"),
        ("{}", "STRUCT<>"),
        ('{"a":123456789012345678901234567890}', "STRUCT<a: DECIMAL(30,0)>"),
        ('{"a":1e400}', "STRUCT<a: DOUBLE>"),
    ],
)
def test_schema_of_json_inference_cells(document: str, want: str) -> None:
    """Each measured inference cell. pins: fnp-9-collections-json/C-003"""
    got = _column(_session().sql("SELECT 1 AS one").select(F.schema_of_json(document).alias("r")))
    assert got[2] == [want]


def test_schema_of_json_raises_on_a_malformed_document() -> None:
    """Spark raises here rather than answering NULL. pins: fnp-9-collections-json/C-003"""
    with pytest.raises(Exception, match="MALFORMED_RECORD_IN_PARSING"):
        _session().sql("SELECT 1 AS one").select(F.schema_of_json("{bad").alias("r")).collect()


def test_create_map_builds_a_non_null_map_from_alternating_arguments() -> None:
    """PySpark's create_map is Spark SQL's map(...). pins: fnp-9-collections-json/C-006"""
    mapped_type, mapped_null, mapped = _column(
        _frame().select(F.create_map(F.col("k"), F.col("v")).alias("r"))
    )
    assert mapped == [[("k1", 1)], [("k2", 2)], [("k3", 3)], [("k4", None)]]
    assert mapped_type == pa.map_(pa.string(), pa.int32())
    assert mapped_null is False
    mixed = _column(_frame().select(F.create_map(F.lit("a"), F.col("v")).alias("r")))
    assert mixed[2] == [[("a", 1)], [("a", 2)], [("a", 3)], [("a", None)]]
    several = _column(
        _frame().select(F.create_map(F.col("k"), F.col("v"), F.lit("z"), F.lit(9)).alias("r"))
    )
    assert several[2][0] == [("k1", 1), ("z", 9)]
    empty = _column(_frame().select(F.create_map().alias("r")))
    assert empty[2] == [[], [], [], []]


def test_create_map_refuses_an_odd_argument_count_a_null_key_and_a_duplicate() -> None:
    """Three loud rules Spark also enforces. pins: fnp-9-collections-json/C-006"""
    from repark.errors import PySparkValueError

    with pytest.raises(PySparkValueError, match="even number"):
        F.create_map(F.lit("a"))
    with pytest.raises(Exception, match="NULL_MAP_KEY"):
        _frame().select(F.create_map(F.lit(None).cast("string"), F.lit(1)).alias("r")).collect()
    with pytest.raises(Exception, match="Duplicate map key"):
        _frame().select(
            F.create_map(F.lit("a"), F.lit(1), F.lit("a"), F.lit(2)).alias("r")
        ).collect()


def test_map_concat_unions_maps_on_both_doors() -> None:
    """A NULL argument nulls the row; a repeated key raises.

    pins: fnp-9-collections-json/C-006
    """
    joined_type, joined_null, joined = _column(
        _frame().select(
            F.map_concat(
                F.create_map(F.col("k"), F.col("v")), F.create_map(F.lit("z"), F.lit(9))
            ).alias("r")
        )
    )
    assert joined[0] == [("k1", 1), ("z", 9)]
    assert joined_type == pa.map_(pa.string(), pa.int32())
    assert joined_null is False
    door = _sql("SELECT map_concat(map(['a'], [1]), map(['b'], [2])) AS r FROM fnp9")
    assert door[2][0] == [("a", 1), ("b", 2)]
    single = _sql("SELECT map_concat(map(['a'], [1])) AS r FROM fnp9")
    assert single[2][0] == [("a", 1)]
    nullable = _session().createDataFrame([({"a": 1},), (None,)], "m map<string,int>")
    nulled_type, nulled_null, nulled = _column(
        nullable.select(F.map_concat("m", F.create_map(F.lit("z"), F.lit(9))).alias("r"))
    )
    assert nulled == [[("a", 1), ("z", 9)], None]
    assert nulled_type == pa.map_(pa.string(), pa.int32())
    assert nulled_null is True
    with pytest.raises(Exception, match="MAP_CONCAT_DIFF_TYPES"):
        _session().sql("SELECT map_concat(map(['a'], [1]), NULL) AS r").collect()
    with pytest.raises(Exception, match="Duplicate map key"):
        _session().sql("SELECT map_concat(map(['a'], [1]), map(['a'], [2])) AS r").collect()
    empty = _sql("SELECT map_concat() AS r FROM fnp9")
    assert empty[2] == [[], [], [], []]
    assert empty[1] is False


def test_array_insert_places_and_pads_on_both_doors() -> None:
    """Positive, negative, and past-the-end positions. pins: fnp-9-collections-json/C-006"""
    front_type, front_null, front = _column(
        _frame().select(F.array_insert("ai", 1, F.lit(9)).alias("r"))
    )
    assert front == [[9, 1, 2], [9, 3], [9], None]
    assert front_type.value_type == pa.int32()
    assert front_null is True
    beyond = _column(_frame().select(F.array_insert("ai", 5, F.lit(9)).alias("r")))
    assert beyond[2] == [
        [1, 2, None, None, 9],
        [3, None, None, None, 9],
        [None, None, None, None, 9],
        None,
    ]
    behind = _column(_frame().select(F.array_insert("ai", -1, F.lit(9)).alias("r")))
    assert behind[2] == [[1, 2, 9], [3, 9], [9], None]
    door = _sql("SELECT array_insert(ai, -2, 9) AS r FROM fnp9")
    assert door[2] == [[1, 9, 2], [9, 3], [9, None], None]


@pytest.mark.parametrize(
    ("position", "want"),
    [
        (-2, [1, 2, 9, 3]),
        (-4, [9, 1, 2, 3]),
        (-5, [9, None, 1, 2, 3]),
        (3, [1, 2, 9, 3]),
        (5, [1, 2, 3, None, 9]),
    ],
)
def test_array_insert_negative_and_padding_cells(position: int, want: list[int | None]) -> None:
    """The -1-appends rule and NULL padding at both ends.

    pins: fnp-9-collections-json/C-006
    """
    spark = _session()
    frame = spark.sql("SELECT array(1, 2, 3) AS ai")
    got = _column(frame.select(F.array_insert("ai", position, F.lit(9)).alias("r")))
    assert got[2] == [want]


def test_array_insert_zero_raises_and_nulls_propagate() -> None:
    """Index 0 is INVALID_INDEX_OF_ZERO on both doors. pins: fnp-9-collections-json/C-006"""
    with pytest.raises(Exception, match="INVALID_INDEX_OF_ZERO"):
        _frame().select(F.array_insert("ai", 0, F.lit(9)).alias("r")).collect()
    with pytest.raises(Exception, match="INVALID_INDEX_OF_ZERO"):
        _session().sql("SELECT array_insert(array(1,2), 0, 9) AS r").collect()
    null_index = _column(
        _frame().select(F.array_insert("ai", F.lit(None).cast("int"), F.lit(9)).alias("r"))
    )
    assert null_index[2] == [None, None, None, None]
    null_value = _column(_frame().select(F.array_insert("ai", 1, F.lit(None)).alias("r")))
    assert null_value[2] == [[None, 1, 2], [None, 3], [None], None]


def test_arrays_zip_pads_to_the_longest_on_both_doors() -> None:
    """NULL fill, non-null element struct, NULL row for a NULL argument.

    pins: fnp-9-collections-json/C-006
    """
    zipped_type, zipped_null, zipped = _column(
        _frame().select(F.arrays_zip("ai", "arrs").alias("r"))
    )
    assert zipped == [
        [{"0": 1, "1": "x"}, {"0": 2, "1": "y"}],
        [{"0": 3, "1": "p"}, {"0": None, "1": "q"}, {"0": None, "1": "r"}],
        [],
        None,
    ]
    assert zipped_type.value_field.nullable is False
    assert zipped_type.value_type == pa.struct(
        [pa.field("0", pa.int32()), pa.field("1", pa.string())]
    )
    assert zipped_null is True
    door = _sql("SELECT arrays_zip(ai, arrs) AS r FROM fnp9")
    assert door[2] == zipped
    single = _sql("SELECT arrays_zip(ai) AS r FROM fnp9")
    assert single[2] == [[{"0": 1}, {"0": 2}], [{"0": 3}], [], None]


def test_arrays_zip_field_names_are_positional_not_the_column_name() -> None:
    """A recorded divergence: Spark names the field after an attribute child.

    pins: fnp-9-collections-json/C-008
    """
    zipped_type, _, _ = _column(_frame().select(F.arrays_zip("ai", "arrs").alias("r")))
    assert [field.name for field in zipped_type.value_type] == ["0", "1"]


@pytest.mark.parametrize(
    "name", ["inline", "inline_outer", "stack", "call_udf", "call_function"]
)
def test_fnp9_multi_column_and_by_name_names_stay_absent(name: str) -> None:
    """A name this unit did not build stays absent rather than half-answering.

    This pin reds the day the seam lands and the name is exported.
    pins: fnp-9-collections-json/C-007
    """
    assert not hasattr(F, name)
    assert name not in F.__all__


def test_json_tuple_still_refuses_on_the_facade() -> None:
    """The SQL door answers one struct where Spark projects N columns.

    pins: fnp-9-collections-json/C-007
    """
    with pytest.raises(UnsupportedOperationException, match="json_tuple"):
        F.json_tuple(F.lit(DOCUMENT), "a")
    door = _sql("SELECT json_tuple(j, 'a', 'b') AS r FROM fnp9", "r")
    assert door[0] == pa.struct([pa.field("c0", pa.string()), pa.field("c1", pa.string())])


def test_every_built_name_is_exported_from_the_facade() -> None:
    """The unit's built surface reaches `F.` and `__all__`.

    pins: fnp-9-collections-json/C-001
    """
    built = [
        "array_insert",
        "arrays_zip",
        "create_map",
        "from_json",
        "get_json_object",
        "json_array_length",
        "json_object_keys",
        "map_concat",
        "schema_of_json",
        "to_json",
    ]
    for name in built:
        assert hasattr(F, name), name
        assert name in F.__all__, name

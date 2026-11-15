from __future__ import annotations

import functools
import json
import math
import struct
from collections import Counter
from collections.abc import Callable, Iterator
from decimal import ROUND_HALF_EVEN, ROUND_HALF_UP, Context, Decimal
from fractions import Fraction
from pathlib import Path
from typing import Any

import pytest

import repark
from repark import ReparkSession
from repark import functions as sf
from repark._native import PyReparkSession
from repark.spark.session import _reset_active_session_for_tests

pytestmark = pytest.mark.xdist_group("round_double_exact_1")
ORACLE = json.loads(Path(__file__).with_name("round_double_exact_1_spark_oracle.json").read_text())
BASE_SCALES = list(range(-3, 19))
EXTREME_SCALES = [-400, -309, -308, -20, 19, 20, 22, 23, 30, 308, 309, 400]
SCALES = BASE_SCALES + EXTREME_SCALES
MODES = {"round": ROUND_HALF_UP, "bround": ROUND_HALF_EVEN}
SOURCES = ["DOUBLE", "FLOAT"]
FUNCTIONS = ["round", "bround"]
SPARK_TYPE = {"DOUBLE": "double", "FLOAT": "float"}
EXACT = Context(prec=3000, Emax=999999, Emin=-999999)
F32_MAX = 0x7F7FFFFF


def f32_of_bits(bits: int) -> float:
    return struct.unpack(">f", struct.pack(">I", bits))[0]


def nearest_f32(value: Fraction) -> float:
    if value < 0:
        return -nearest_f32(-value)
    approx = struct.pack(">f", min(float(value), f32_of_bits(F32_MAX)))
    center = struct.unpack(">I", approx)[0]
    best = None
    for candidate in (center - 1, center, center + 1):
        if 0 <= candidate <= F32_MAX:
            key = (abs(Fraction(f32_of_bits(candidate)) - value), candidate & 1)
            if best is None or key < best[0]:
                best = (key, candidate)
    if best is None:
        return 0.0
    top = Fraction(f32_of_bits(F32_MAX))
    if value >= top + (Fraction(2) ** 128 - top) / 2:
        return math.inf
    return f32_of_bits(best[1])


def spark_round(jdk_text: str, scale: int, function: str, source: str) -> float:
    if jdk_text in ("NaN", "Infinity", "-Infinity"):
        return float(jdk_text)
    unit = Decimal(1).scaleb(-scale, EXACT)
    rounded = Decimal(jdk_text).quantize(unit, rounding=MODES[function], context=EXACT)
    if rounded == 0:
        return 0.0
    if source == "FLOAT":
        return nearest_f32(Fraction(rounded))
    return float(rounded)


def bits(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float):
        return "NaN" if math.isnan(value) else struct.pack(">d", value).hex()
    return repr(value)


def values(source: str) -> list[list[Any]]:
    return ORACLE["values"][source]


@functools.cache
def expected(source: str, function: str, scale: int) -> dict[int, str | None]:
    return {row[0]: bits(spark_round(row[2], scale, function, source)) for row in values(source)}


def literal(text: str, source: str) -> str:
    if source == "FLOAT":
        return f"CAST(CAST('{text}' AS DOUBLE) AS FLOAT)"
    return f"CAST('{text}' AS DOUBLE)"


def table_of(source: str) -> str:
    return f"ice.ns.src_{source[0].lower()}"


def build_sources(spark: Any) -> None:
    spark.sql("CREATE NAMESPACE ice.ns").collect()
    for source in SOURCES:
        name = table_of(source)
        spark.sql(f"CREATE TABLE {name} (id INT, v {source}, sc INT) USING iceberg").collect()
        rows = values(source)
        for start in range(0, len(rows), 100):
            chunk = rows[start : start + 100]
            body = ", ".join(f"({row[0]}, {literal(row[1], source)}, 2)" for row in chunk)
            spark.sql(f"INSERT INTO {name} VALUES {body}").collect()


def rows_by_id(frame: Any) -> dict[int, list[str | None]]:
    return {int(row[0]): [bits(cell) for cell in row[1:]] for row in frame.collect()}


def call(function: str) -> Callable[..., Any]:
    return sf.round if function == "round" else sf.bround


def select_frame(spark: Any, door: str, source: str, function: str, scales: list[int]) -> Any:
    name = table_of(source)
    if door == "sql":
        columns = ", ".join(f"{function}(v, {s}) AS c{k}" for k, s in enumerate(scales))
        return spark.sql(f"SELECT id, {columns} FROM {name}")
    frame = spark.table(name)
    if door == "fcol":
        return frame.select(
            "id", *[call(function)(sf.col("v"), s).alias(f"c{k}") for k, s in enumerate(scales)]
        )
    if door == "fexpr":
        return frame.select(
            "id", *[sf.expr(f"{function}(v, {s})").alias(f"c{k}") for k, s in enumerate(scales)]
        )
    return frame.selectExpr("id", *[f"{function}(v, {s}) AS c{k}" for k, s in enumerate(scales)])


def mismatches(
    got: dict[int, list[str | None]], source: str, function: str, scales: list[int]
) -> list[tuple[Any, ...]]:
    out = []
    for k, scale in enumerate(scales):
        for ident, answer in expected(source, function, scale).items():
            have = got.get(ident, ["MISSING"] * len(scales))[k]
            if have != answer:
                out.append((ident, scale, have, answer))
    return out


def measure_select(spark: Any, door: str, source: str, function: str) -> Any:
    frame = select_frame(spark, door, source, function, SCALES)
    types = sorted({field.dataType.simpleString() for field in frame.schema.fields[1:]})
    return types, mismatches(rows_by_id(frame), source, function, SCALES)


LITERAL_CLASSES = ("special", "large", "ordinary", "jdk-not-shortest")


def literal_rows(source: str) -> list[list[Any]]:
    return [
        row
        for row in values(source)
        if row[3] in LITERAL_CLASSES or (row[3].startswith("half/") and row[3].endswith("ulp+0"))
    ]


def measure_literal(spark: Any, door: str, source: str, function: str) -> Any:
    bad = []
    sample = literal_rows(source)
    for start in range(0, len(sample), 10):
        cells = [(row, s) for row in sample[start : start + 10] for s in BASE_SCALES]
        if door == "sql":
            columns = ", ".join(
                f"{function}({literal(row[1], source)}, {s}) AS c{k}"
                for k, (row, s) in enumerate(cells)
            )
            got = spark.sql(f"SELECT {columns}").collect()[0]
        else:
            columns = []
            for k, (row, s) in enumerate(cells):
                value = sf.lit(float(row[1]))
                if source == "FLOAT":
                    value = value.cast("float")
                columns.append(call(function)(value, s).alias(f"c{k}"))
            got = spark.range(1).select(*columns).collect()[0]
        for k, (row, s) in enumerate(cells):
            if bits(got[k]) != expected(source, function, s)[row[0]]:
                bad.append((row[1], s, bits(got[k])))
    return bad


def text_of(value: float) -> str:
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    return repr(value)


def build_expected(spark: Any, source: str, function: str) -> str:
    name = f"ice.ns.expected_{source[0].lower()}_{function}"
    columns = ", ".join(["id INT", f"v {source}"] + [f"e{k} {source}" for k in range(len(SCALES))])
    spark.sql(f"CREATE TABLE {name} ({columns}) USING iceberg").collect()
    rows = values(source)
    for start in range(0, len(rows), 50):
        tuples = []
        for row in rows[start : start + 50]:
            cells = [literal(row[1], source)] + [
                literal(text_of(spark_round(row[2], s, function, source)), source) for s in SCALES
            ]
            tuples.append(f"({row[0]}, {', '.join(cells)})")
        spark.sql(f"INSERT INTO {name} VALUES {', '.join(tuples)}").collect()
    return name


def measure_where(spark: Any, door: str, view: str, function: str) -> Any:
    clause = " OR ".join(f"{function}(v, {s}) <> e{k}" for k, s in enumerate(SCALES))
    predicate = f"NOT isnan(v) AND ({clause})"
    if door == "sql":
        wrong = spark.sql(f"SELECT id FROM {view} WHERE {predicate}").collect()
    elif door == "fexpr":
        wrong = spark.table(view).filter(sf.expr(predicate)).select("id").collect()
    elif door == "filter_str":
        wrong = spark.table(view).filter(predicate).select("id").collect()
    else:
        condition = None
        for k, s in enumerate(SCALES):
            term = call(function)(sf.col("v"), s) != sf.col(f"e{k}")
            condition = term if condition is None else condition | term
        wrong = spark.table(view).filter(~sf.isnan("v") & condition).select("id").collect()
    return sorted(int(row[0]) for row in wrong)


def measure_group(spark: Any, door: str, scale: int, function: str, source: str) -> Any:
    name = table_of(source)
    if door == "sql":
        frame = spark.sql(
            f"SELECT {function}(v, {scale}) AS g, COUNT(*) AS n FROM {name} "
            f"GROUP BY {function}(v, {scale})"
        )
    else:
        key = (
            call(function)(sf.col("v"), scale)
            if door == "fcol"
            else sf.expr(f"{function}(v, {scale})")
        )
        frame = spark.table(name).groupBy(key.alias("g")).agg(sf.count("*").alias("n"))
    got = Counter({bits(row[0]): int(row[1]) for row in frame.collect()})
    want = Counter(expected(source, function, scale).values())
    return sorted((got - want).items()) + sorted((want - got).items())


STORES = [
    ("DOUBLE", "round", 2),
    ("DOUBLE", "bround", 2),
    ("DOUBLE", "round", 18),
    ("DOUBLE", "round", -2),
    ("DECIMAL(10,2)", "round", 2),
    ("DECIMAL(10,2)", "bround", 2),
    ("DECIMAL(38,18)", "round", 18),
]
CAPS = {"DECIMAL(10,2)": "1e7", "DECIMAL(38,18)": "1e19"}


def measure_store(
    spark: Any, door: str, target: str, function: str, scale: int, source: str
) -> Any:
    column = source if target == "DOUBLE" else target
    tag = column.replace("(", "_").replace(",", "_").replace(")", "").lower()
    name = f"ice.ns.st_{door}_{source[0].lower()}_{tag}_{function}_{str(scale).replace('-', 'm')}"
    spark.sql(f"CREATE TABLE {name} (id INT, r {column}) USING iceberg").collect()
    keep = f"abs(v) < {CAPS[target]} AND NOT isnan(v)" if target in CAPS else "true"
    if door == "sql":
        spark.sql(
            f"INSERT INTO {name} SELECT id, {function}(v, {scale}) FROM {table_of(source)} "
            f"WHERE {keep}"
        ).collect()
    else:
        value = (
            call(function)(sf.col("v"), scale)
            if door == "fcol"
            else sf.expr(f"{function}(v, {scale})")
        )
        frame = spark.table(table_of(source)).filter(sf.expr(keep))
        frame.select("id", value.alias("r")).writeTo(name).append()
    stored = {int(row[0]): row[1] for row in spark.sql(f"SELECT id, r FROM {name}").collect()}
    if target in CAPS:
        want = ORACLE["stores"][f"{source}/{target}/{function}/{scale}"]
        got = {str(ident): str(value) for ident, value in stored.items()}
    else:
        want = expected(source, function, scale)
        got = {ident: bits(value) for ident, value in stored.items()}
    return sorted(
        (k, got.get(k), want.get(k)) for k in set(got) | set(want) if got.get(k) != want.get(k)
    )


def measure_null_scale(spark: Any, door: str, function: str, source: str) -> Any:
    if door == "sql":
        frame = spark.sql(
            f"SELECT id, {function}(v, CAST(NULL AS INT)) AS r FROM {table_of(source)}"
        )
    elif door == "fexpr":
        frame = spark.table(table_of(source)).select(
            "id", sf.expr(f"{function}(v, CAST(NULL AS INT))").alias("r")
        )
    else:
        nothing = sf.lit(None).cast("int")
        frame = spark.table(table_of(source)).select(
            "id", call(function)(sf.col("v"), nothing).alias("r")
        )
    return frame.schema.fields[1].dataType.simpleString(), sorted(
        {repr(row[1]) for row in frame.collect()}
    )


def measure_column_scale(spark: Any, door: str, function: str, source: str) -> str:
    try:
        if door == "sql":
            spark.sql(f"SELECT {function}(v, sc) FROM {table_of(source)}").collect()
        elif door == "fexpr":
            spark.table(table_of(source)).select(sf.expr(f"{function}(v, sc)")).collect()
        else:
            frame = spark.table(table_of(source))
            frame.select(call(function)(sf.col("v"), sf.col("sc"))).collect()
    except Exception as error:
        return str(error)
    return "answered"


CARD_SQL = (
    "SELECT ROUND(CAST('1.005' AS DOUBLE), 2) AS a, ROUND(CAST('0.285' AS DOUBLE), 2) AS b, "
    "ROUND(CAST('1.255' AS DOUBLE), 2) AS c, "
    "ROUND(CAST('-22.129547339983247' AS DOUBLE), 18) AS d, "
    "ROUND(CAST('1.255' AS FLOAT), 2) AS e, BROUND(CAST('1.255' AS FLOAT), 2) AS f, "
    "ROUND(ROUND(CAST('1.0049999999999999' AS DOUBLE), 3), 2) AS g, "
    "ROUND(-CAST('1.005' AS DOUBLE), 2) AS h"
)


def measure_shapes(spark: Any) -> dict[str, Any]:
    source = table_of("DOUBLE")
    spark.sql(
        f"CREATE OR REPLACE TEMP VIEW rounded AS SELECT id, ROUND(v, 2) AS r FROM {source}"
    ).collect()
    integer = spark.range(1).select(sf.round(sf.lit(25).cast("int"), -1).alias("r"))
    return {
        "card": [bits(cell) for cell in spark.sql(CARD_SQL).collect()[0]],
        "view": {
            int(row[0]): bits(row[1]) for row in spark.sql("SELECT id, r FROM rounded").collect()
        },
        "subquery": [
            int(row[0])
            for row in spark.sql(
                f"SELECT id FROM (SELECT id, v FROM {source}) "
                "WHERE ROUND(v, 2) = CAST('1.01' AS DOUBLE) ORDER BY id"
            ).collect()
        ],
        "integer": (integer.schema.fields[0].dataType.simpleString(), integer.collect()[0][0]),
    }


def guarded(thunk: Callable[[], Any]) -> Any:
    try:
        return thunk()
    except Exception as error:
        return f"raised: {error}"


SELECT_DOORS = ["sql", "fcol", "fexpr", "select_expr"]
WHERE_DOORS = ["sql", "fcol", "fexpr", "filter_str"]
FRAME_DOORS = ["sql", "fcol", "fexpr"]
GROUP_SCALES = [-2, 2, 18]


def measure_all(spark: Any) -> dict[str, Any]:
    build_sources(spark)
    out: dict[str, Any] = {}
    for source in SOURCES:
        for function in FUNCTIONS:
            for door in SELECT_DOORS:
                out[f"sel/{door}/{source}/{function}"] = guarded(
                    lambda d=door, s=source, f=function: measure_select(spark, d, s, f)
                )
            for door in ["sql", "fcol"]:
                out[f"lit/{door}/{source}/{function}"] = guarded(
                    lambda d=door, s=source, f=function: measure_literal(spark, d, s, f)
                )
            view = guarded(lambda s=source, f=function: build_expected(spark, s, f))
            for door in WHERE_DOORS:
                out[f"where/{door}/{source}/{function}"] = guarded(
                    lambda d=door, v=view, f=function: measure_where(spark, d, v, f)
                )
            for door in FRAME_DOORS:
                for scale in GROUP_SCALES:
                    out[f"group/{door}/{scale}/{source}/{function}"] = guarded(
                        lambda d=door, k=scale, s=source, f=function: measure_group(
                            spark, d, k, f, s
                        )
                    )
                out[f"null/{door}/{source}/{function}"] = guarded(
                    lambda d=door, s=source, f=function: measure_null_scale(spark, d, f, s)
                )
                out[f"colscale/{door}/{source}/{function}"] = measure_column_scale(
                    spark, door, function, source
                )
        for door in FRAME_DOORS:
            for target, function, scale in STORES:
                out[f"store/{door}/{target}/{function}/{scale}/{source}"] = guarded(
                    lambda d=door, t=target, f=function, k=scale, s=source: measure_store(
                        spark, d, t, f, k, s
                    )
                )
    out["shapes"] = guarded(lambda: measure_shapes(spark))
    return out


@pytest.fixture(scope="module", params=["true", "false"])
def measured(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory) -> Any:
    _reset_active_session_for_tests()
    spark = (
        ReparkSession.builder.appName("test-round-double-exact-1")
        .config("spark.sql.ansi.enabled", request.param)
        .getOrCreate()
    )
    try:
        spark.register_memory_catalog("ice", str(tmp_path_factory.mktemp("round-exact") / "wh"))
        return measure_all(spark)
    finally:
        spark.stop()
        _reset_active_session_for_tests()


@pytest.mark.parametrize("door", SELECT_DOORS)
@pytest.mark.parametrize("function", FUNCTIONS)
@pytest.mark.parametrize("source", SOURCES)
def test_select_answers_spark_on_every_value_and_scale(
    measured: Any, door: str, function: str, source: str
) -> None:
    result = measured[f"sel/{door}/{source}/{function}"]
    assert not isinstance(result, str), result
    types, bad = result
    assert types == [SPARK_TYPE[source]]
    assert bad == [], f"{len(bad)} cells differ, first {bad[:5]}"


@pytest.mark.parametrize("door", ["sql", "fcol"])
@pytest.mark.parametrize("function", FUNCTIONS)
@pytest.mark.parametrize("source", SOURCES)
def test_a_folded_literal_answers_as_the_column(
    measured: Any, door: str, function: str, source: str
) -> None:
    bad = measured[f"lit/{door}/{source}/{function}"]
    assert bad == [], f"{len(bad)} literals differ, first {bad[:5]}"


@pytest.mark.parametrize("door", WHERE_DOORS)
@pytest.mark.parametrize("function", FUNCTIONS)
@pytest.mark.parametrize("source", SOURCES)
def test_a_where_predicate_sees_sparks_value(
    measured: Any, door: str, function: str, source: str
) -> None:
    assert measured[f"where/{door}/{source}/{function}"] == []


@pytest.mark.parametrize("door", FRAME_DOORS)
@pytest.mark.parametrize("scale", GROUP_SCALES)
@pytest.mark.parametrize("function", FUNCTIONS)
@pytest.mark.parametrize("source", SOURCES)
def test_a_group_by_key_is_sparks_value(
    measured: Any, door: str, scale: int, function: str, source: str
) -> None:
    assert measured[f"group/{door}/{scale}/{source}/{function}"] == []


@pytest.mark.parametrize("door", FRAME_DOORS)
@pytest.mark.parametrize(("target", "function", "scale"), STORES)
@pytest.mark.parametrize("source", SOURCES)
def test_a_store_holds_sparks_value(
    measured: Any, door: str, target: str, function: str, scale: int, source: str
) -> None:
    bad = measured[f"store/{door}/{target}/{function}/{scale}/{source}"]
    assert bad == [], f"{len(bad)} stored values differ, first {bad[:5]}"


@pytest.mark.parametrize("door", FRAME_DOORS)
@pytest.mark.parametrize("function", FUNCTIONS)
@pytest.mark.parametrize("source", SOURCES)
def test_a_null_scale_answers_null_of_the_input_type(
    measured: Any, door: str, function: str, source: str
) -> None:
    assert measured[f"null/{door}/{source}/{function}"] == (SPARK_TYPE[source], ["None"])


@pytest.mark.parametrize("door", FRAME_DOORS)
@pytest.mark.parametrize("function", FUNCTIONS)
@pytest.mark.parametrize("source", SOURCES)
def test_a_column_scale_is_refused_as_spark_does(
    measured: Any, door: str, function: str, source: str
) -> None:
    assert "NON_FOLDABLE_INPUT" in measured[f"colscale/{door}/{source}/{function}"]


def test_the_card_rows_answer_sparks_values(measured: Any) -> None:
    assert measured["shapes"]["card"] == [
        bits(1.01),
        bits(0.29),
        bits(1.26),
        bits(-22.129547339983247),
        bits(1.25),
        bits(1.25),
        bits(1.01),
        bits(-1.01),
    ]


def test_a_view_and_a_subquery_answer_sparks_values(measured: Any) -> None:
    assert measured["shapes"]["view"] == expected("DOUBLE", "round", 2)
    assert measured["shapes"]["subquery"] == [28, 186, 187, 188]


def test_dataframe_round_of_an_integer_keeps_the_integer_type(measured: Any) -> None:
    assert measured["shapes"]["integer"] == ("int", 30)


@pytest.fixture(params=["true", "false"])
def native_door(request: pytest.FixtureRequest) -> Iterator[None]:
    previous = repark._ANSI_NATIVE
    repark._ANSI_NATIVE = PyReparkSession.native(config={"spark.sql.ansi.enabled": request.param})
    try:
        yield
    finally:
        repark._ANSI_NATIVE = previous


def test_the_native_door_keeps_datafusions_round_as_a_recorded_gap(native_door: None) -> None:
    row = (
        repark.sql(
            "SELECT round(CAST('1.005' AS DOUBLE), 2) AS a, "
            "round(CAST('1.005' AS DOUBLE), 309) AS b"
        )
        .to_arrow()
        .to_pylist()[0]
    )
    assert row["a"] == 1.0
    assert math.isnan(row["b"])
    with pytest.raises(Exception, match="bround"):
        repark.sql("SELECT bround(CAST('2.5' AS DOUBLE), 0) AS c").to_arrow()

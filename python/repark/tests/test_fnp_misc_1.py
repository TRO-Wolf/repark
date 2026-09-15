"""FNP-MISC-1 pins for call_function, call_udf, arrow_udf, arrow_udtf, bucket(Column)."""

from __future__ import annotations

import inspect
import json
import sys
from pathlib import Path
from typing import Any, ClassVar

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from repark import SparkSession
from repark.errors import (
    AnalysisException,
    PySparkException,
    PySparkRuntimeError,
    PySparkTypeError,
    UnsupportedOperationException,
)
from repark.spark import functions as F  # noqa: N812
from repark.spark.functions import col, lit

AGG_ORACLE = "fnp_misc_1_agg_spark_oracle.json"
ARROW2_ORACLE = "fnp_misc_1_arrow2_spark_oracle.json"
ARROW_BUCKET_ORACLE = "fnp_misc_1_arrow_bucket_spark_oracle.json"

FRAME_ROWS: list[tuple[Any, ...]] = [
    (1, "a", 10, 1.5, "x", 3),
    (1, "b", 20, 2.5, "y", 1),
    (1, "b", None, None, None, 2),
    (1, "c", 30, -4.0, "x", None),
    (2, None, 5, 0.0, "z", 7),
    (3, None, None, None, None, None),
]
FRAME_SCHEMA = "g INT, k STRING, v INT, d DOUBLE, s STRING, o LONG"

ORACLE_ARROW: dict[str, Any] = {
    "int": pa.int32(),
    "bigint": pa.int64(),
    "string": pa.string(),
    "double": pa.float64(),
}


@pytest.fixture
def spark() -> Any:
    session = SparkSession.builder.master("local[1]").appName("fnp-misc-1").getOrCreate()
    yield session
    session.stop()


def _frame(spark: Any) -> Any:
    return spark.createDataFrame(FRAME_ROWS, FRAME_SCHEMA)


def _cells(filename: str) -> list[dict[str, Any]]:
    payload = json.loads(Path(__file__).with_name(filename).read_text())
    return list(payload["cells"])


def _cell(cells: list[dict[str, Any]], name: str, expr: str, ansi: bool = True) -> dict[str, Any]:
    for cell in cells:
        if cell["name"] == name and cell["expr"] == expr and cell.get("ansi", True) == ansi:
            return cell
    raise AssertionError(f"oracle cell missing: {name} {expr}")


def _run_select(frame: Any, expr: str) -> Any:
    namespace: dict[str, Any] = {"F": F, "col": col, "lit": lit, "__builtins__": {}}
    return frame.select(eval(expr, namespace))


def _select_column_values(frame: Any, expression: Any, name: str) -> list[Any]:
    """Collect one aliased select column as Python values."""
    return [row[name] for row in frame.select(expression).to_arrow().to_pylist()]


def _assert_value_cell(table: pa.Table, cell: dict[str, Any]) -> None:
    want_names = [column["name"] for column in cell["columns"]]
    assert [field.name for field in table.schema] == want_names
    for field, column in zip(table.schema, cell["columns"], strict=True):
        assert field.type == ORACLE_ARROW[column["type"]], (field.name, field.type)
    rows = table.to_pylist()
    assert [[row[name] for name in want_names] for row in rows] == cell["rows"]


def test_fnp_misc_1_names_present_with_pyspark_calling_convention() -> None:
    """Five names live on F with the PySpark 4.1.2 parameter shapes. pins: fnp-misc-1/C-001"""
    for name in ("call_function", "call_udf", "arrow_udf", "arrow_udtf", "bucket"):
        assert hasattr(F, name), name
        assert name in F.__all__, name
    by_name = inspect.signature(F.call_function)
    assert list(by_name.parameters) == ["funcName", "cols"]
    assert by_name.parameters["cols"].kind == inspect.Parameter.VAR_POSITIONAL
    by_udf = inspect.signature(F.call_udf)
    assert list(by_udf.parameters) == ["udfName", "cols"]
    assert by_udf.parameters["cols"].kind == inspect.Parameter.VAR_POSITIONAL
    arrow = inspect.signature(F.arrow_udf)
    assert list(arrow.parameters) == ["f", "returnType", "functionType"]
    assert all(p.default is None for p in arrow.parameters.values())
    table = inspect.signature(F.arrow_udtf)
    assert list(table.parameters) == ["cls", "returnType"]
    assert table.parameters["cls"].default is None
    assert table.parameters["returnType"].kind == inspect.Parameter.KEYWORD_ONLY
    assert table.parameters["returnType"].default is None
    bucket = inspect.signature(F.bucket)
    assert list(bucket.parameters) == ["numBuckets", "col"]


def test_fnp_misc_1_call_function_abs_answers(spark: Any) -> None:
    """call_function abs value, name and type equal the oracle cell. pins: fnp-misc-1/C-002"""
    cells = _cells(AGG_ORACLE)
    cell = _cell(cells, "call_function", "F.call_function('abs', col('v'))")
    _assert_value_cell(_run_select(_frame(spark), cell["expr"]).to_arrow(), cell)


def test_fnp_misc_1_call_function_name_resolves_case_insensitively(spark: Any) -> None:
    """ABS answers exactly like abs. pins: fnp-misc-1/C-002"""
    cells = _cells(AGG_ORACLE)
    cell = _cell(cells, "call_function", "F.call_function('ABS', col('v'))")
    _assert_value_cell(_run_select(_frame(spark), cell["expr"]).to_arrow(), cell)


def test_fnp_misc_1_call_function_upper_takes_a_column_name(spark: Any) -> None:
    """String-name arguments route like Column arguments. pins: fnp-misc-1/C-002"""
    cells = _cells(AGG_ORACLE)
    cell = _cell(cells, "call_function", "F.call_function('upper', 'k')")
    _assert_value_cell(_run_select(_frame(spark), cell["expr"]).to_arrow(), cell)


def test_fnp_misc_1_call_function_sum_stays_a_grouped_aggregate(spark: Any) -> None:
    """sum answers per group with the bigint type. pins: fnp-misc-1/C-002"""
    cells = _cells(AGG_ORACLE)
    cell = _cell(cells, "call_function", "F.call_function('sum', col('v'))")
    namespace: dict[str, Any] = {"F": F, "col": col, "lit": lit, "__builtins__": {}}
    out = _frame(spark).groupBy("g").agg(eval(cell["expr"], namespace)).orderBy("g")
    _assert_value_cell(out.to_arrow(), cell)


def test_fnp_misc_1_call_function_registered_udf_wins(spark: Any) -> None:
    """A session-registered UDF beats any builtin of the same name. pins: fnp-misc-1/C-002"""
    cells = _cells(AGG_ORACLE)
    cell = _cell(cells, "call_function", "F.call_function('plus_one_py', col('v'))")
    spark.udf.register("plus_one_py", lambda x: None if x is None else x + 1, "int")
    _assert_value_cell(_run_select(_frame(spark), cell["expr"]).to_arrow(), cell)


def test_fnp_misc_1_call_udf_registered_and_builtin_answer(spark: Any) -> None:
    """call_udf answers a registered UDF and a builtin. pins: fnp-misc-1/C-002"""
    cells = _cells(AGG_ORACLE)
    spark.udf.register("plus_one_py", lambda x: None if x is None else x + 1, "int")
    for expr in ("F.call_udf('plus_one_py', col('v'))", "F.call_udf('abs', col('v'))"):
        cell = _cell(cells, "call_udf", expr)
        _assert_value_cell(_run_select(_frame(spark), cell["expr"]).to_arrow(), cell)


def test_fnp_misc_1_by_name_unknown_raises_unresolved_routine(spark: Any) -> None:
    """Unknown names raise the exact UNRESOLVED_ROUTINE message. pins: fnp-misc-1/C-002"""
    cells = _cells(AGG_ORACLE)
    for name, expr in (
        ("call_function", "F.call_function('no_such_fn', col('v'))"),
        ("call_udf", "F.call_udf('nope', col('v'))"),
    ):
        cell = _cell(cells, name, expr)
        assert cell["error_type"] == "AnalysisException"
        with pytest.raises(AnalysisException) as caught:
            _run_select(_frame(spark), expr)
        assert str(caught.value) == cell["message"], str(caught.value)


def test_fnp_misc_1_call_function_dotted_raises_single_part(spark: Any) -> None:
    """Dotted names raise the exact single-part-namespace message. pins: fnp-misc-1/C-002"""
    cells = _cells(AGG_ORACLE)
    cell = _cell(cells, "call_function", "F.call_function('system.builtin.abs', col('v'))")
    assert cell["error_condition"] == "REQUIRES_SINGLE_PART_NAMESPACE"
    with pytest.raises(AnalysisException) as caught:
        _run_select(_frame(spark), cell["expr"])
    assert str(caught.value) == cell["message"], str(caught.value)


def _unresolved_message(name: str) -> str:
    """Build the exact UNRESOLVED_ROUTINE message for one routine name."""
    return (
        f"[UNRESOLVED_ROUTINE] Cannot resolve routine `{name}` on search path "
        "[`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]. SQLSTATE: 42883"
    )


@pytest.mark.parametrize(
    "expr",
    [
        "F.call_function('col', 'v')",
        "F.call_function('lit', 1)",
        "F.call_function('when', lit(True), lit(1))",
    ],
)
def test_fnp_misc_1_by_name_non_routines_raise_unresolved(spark: Any, expr: str) -> None:
    """Facade helpers are not SQL routines. pins: fnp-misc-1/F-4"""
    namespace: dict[str, Any] = {"F": F, "col": col, "lit": lit, "__builtins__": {}}
    with pytest.raises(AnalysisException) as caught:
        eval(expr, namespace)
    assert str(caught.value) == _unresolved_message(expr.split("'")[1])


def test_fnp_misc_1_call_function_declared_refusal_surfaces(spark: Any) -> None:
    """A declared-refusal routine raises its own refusal. pins: fnp-misc-1/F-5"""
    with pytest.raises(UnsupportedOperationException, match="hll_sketch_agg is reachable"):
        F.call_function("hll_sketch_agg", col("v"))


def test_fnp_misc_1_call_function_registered_name_shadows_builtin(spark: Any) -> None:
    """A registered UDF wins over the builtin, any case. pins: fnp-misc-1/L-005"""
    spark.udf.register("abs", lambda x: None if x is None else x + 100, "int")
    frame = _frame(spark)
    for expr in ("F.call_function('abs', col('v'))", "F.call_function('ABS', col('v'))"):
        table = _run_select(frame, expr).to_arrow()
        assert [field.name for field in table.schema] == ["abs(v)"]
        assert [row["abs(v)"] for row in table.to_pylist()] == [110, 120, None, 130, 105, None]


def test_fnp_misc_1_call_function_exact_registry_hit_skips_scan(
    spark: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An exact registry name resolves without scanning. pins: fnp-misc-1/F-3"""
    spark.udf.register("shadow_me", lambda x: x, "int")
    spark.udf.register("PlusOneX", lambda x: x, "int")
    live = dict(spark._udf_registry())
    scans: list[int] = []

    class CountingDict(dict):
        """Registry copy counting full scans."""

        def items(self) -> Any:
            """Count one full scan."""
            scans.append(1)
            return super().items()

    monkeypatch.setattr(type(spark), "_udf_registry", lambda self: CountingDict(live))
    frame = spark.createDataFrame([(1,)], "v INT")
    scans.clear()
    marker = F.call_function("shadow_me", col("v"))
    assert scans == []
    assert [row["shadow_me(v)"] for row in frame.select(marker).to_arrow().to_pylist()] == [1]
    scans.clear()
    fallback = F.call_function("plusonex", col("v"))
    assert scans == [1]
    assert [row["PlusOneX(v)"] for row in frame.select(fallback).to_arrow().to_pylist()] == [1]


def test_fnp_misc_1_call_function_sha2_accepts_lit_bits(spark: Any) -> None:
    """sha2 builds the SQL routine with a literal bit width. pins: fnp-misc-1/L-004"""
    frame = spark.createDataFrame([("abc",), (None,)], "s STRING")
    got = _select_column_values(frame, F.call_function("sha2", col("s"), lit(256)).alias("x"), "x")
    want = _select_column_values(frame, F.sha2(col("s"), 256).alias("x"), "x")
    assert got == want
    assert got[0] == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


def test_fnp_misc_1_call_function_bitwise_not_case(spark: Any) -> None:
    """The CamelCase routine answers through the facade. pins: fnp-misc-1/L-004"""
    frame = spark.createDataFrame([(6,), (None,)], "v INT")
    got = _select_column_values(frame, F.call_function("bitwiseNOT", col("v")).alias("x"), "x")
    want = _select_column_values(frame, F.bitwiseNOT(col("v")).alias("x"), "x")
    assert got == want == [-7, None]


def test_fnp_misc_1_call_function_log_matches_facade(spark: Any) -> None:
    """log with a Python int base matches the facade call. pins: fnp-misc-1/L-004"""
    frame = spark.createDataFrame([(8.0,), (None,)], "d DOUBLE")
    got = _select_column_values(frame, F.call_function("log", 2, col("d")).alias("x"), "x")
    want = _select_column_values(frame, F.log(2, col("d")).alias("x"), "x")
    assert got == want == [3.0, None]


def test_fnp_misc_1_call_function_ntile_matches_facade(spark: Any) -> None:
    """ntile with a Python int matches the facade window call. pins: fnp-misc-1/L-004"""
    from repark.spark.window import Window

    frame = spark.createDataFrame([(1, 10), (1, 20), (2, 30)], "g INT, v INT")
    window = Window.partitionBy("g").orderBy("v")
    got = _select_column_values(frame, F.call_function("ntile", 4).over(window).alias("x"), "x")
    want = _select_column_values(frame, F.ntile(4).over(window).alias("x"), "x")
    assert got == want == [1, 2, 1]


def _wrong_num_args_message(name: str, required: str, got: int) -> str:
    """Build the exact WRONG_NUM_ARGS message for one routine call."""
    return (
        f"[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `{name}` {required} but the actual "
        f"number is {got}. Please, refer to "
        "'https://spark.apache.org/docs/latest/sql-ref-functions.html' for a fix. SQLSTATE: 42605"
    )


def test_fnp_misc_1_call_function_sha2_wrong_arg_count_raises(spark: Any) -> None:
    """sha2 with one argument raises WRONG_NUM_ARGS. pins: fnp-misc-1/L-010"""
    with pytest.raises(AnalysisException) as caught:
        F.call_function("sha2", col("s"))
    assert str(caught.value) == _wrong_num_args_message("sha2", "requires 2 parameters", 1)


def test_fnp_misc_1_call_function_abs_zero_args_raises(spark: Any) -> None:
    """abs with no arguments raises WRONG_NUM_ARGS. pins: fnp-misc-1/L-010"""
    with pytest.raises(AnalysisException) as caught:
        F.call_function("abs")
    assert str(caught.value) == _wrong_num_args_message("abs", "requires 1 parameters", 0)


def test_fnp_misc_1_call_function_substring_range_arity_raises(spark: Any) -> None:
    """A ranged engine arity keeps the engine wording. pins: fnp-misc-1/L-010"""
    with pytest.raises(AnalysisException) as caught:
        F.call_function("substring", col("s"))
    assert str(caught.value) == _wrong_num_args_message("substring", "accepts at least 2 args", 1)


def test_fnp_misc_1_call_function_abs_string_mismatch_is_not_unresolved(spark: Any) -> None:
    """A malformed string under a known routine is its own error. pins: fnp-misc-1/L-010

    Oracle fixtures-batch11.json A11-callfn-abs-x: ``call_function('abs', lit('x'))``
    raises CAST_INVALID_INPUT (a malformed STRING->DOUBLE cast), not
    UNRESOLVED_ROUTINE; A11-sql-abs-1 measures ``abs('-1')`` = 1.0 double.
    """
    frame = spark.createDataFrame([("a",), (None,)], "s STRING")
    with pytest.raises(PySparkException) as caught:
        frame.select(F.call_function("abs", col("s")).alias("x")).collect()
    assert "UNRESOLVED_ROUTINE" not in str(caught.value)
    assert "CAST_INVALID_INPUT" in str(caught.value)
    values = _select_column_values(frame, F.call_function("abs", lit("-1")).alias("v"), "v")
    assert values == [1.0, 1.0]


def test_fnp_misc_1_byname_allowlist_covers_facade() -> None:
    """The facade-only tuple matches the measured engine gap. pins: fnp-misc-1/F-4"""
    from repark.spark.functions import _scalar
    from repark.spark.functions_byname import (
        BYNAME_NAMES,
        BYNAME_NON_ROUTINE_NAMES,
        FACADE_ONLY_ROUTINE_NAMES,
    )

    probe = lit(1)
    derived: list[str] = []
    for name in F.__all__:
        try:
            _scalar(name, probe)
        except ValueError as error:
            if "unsupported function" not in str(error):
                continue
        else:
            continue
        if (
            name not in BYNAME_NON_ROUTINE_NAMES
            and name not in BYNAME_NAMES
            and not isinstance(getattr(F, name), type)
        ):
            derived.append(name)
    assert sorted(derived) == sorted(FACADE_ONLY_ROUTINE_NAMES)


def test_fnp_misc_1_call_function_on_camel_case_aliases_matches_spark(spark: Any) -> None:
    """Camel-case aliases resolve only when Spark's builtin does. pins: fnp-misc-1/F-4"""
    frame = spark.createDataFrame([(12,)], "a INT")
    shifted = frame.select(
        F.call_function("shiftLeft", "a", lit(1)),
        F.call_function("shiftRight", "a", lit(1)),
        F.call_function("shiftRightUnsigned", "a", lit(1)),
    )
    assert shifted.columns == ["shiftleft(a, 1)", "shiftright(a, 1)", "shiftrightunsigned(a, 1)"]
    assert [tuple(row) for row in shifted.collect()] == [(24, 6, 6)]
    for name in ("approxCountDistinct", "toDegrees", "toRadians"):
        with pytest.raises(AnalysisException) as caught:
            frame.select(F.call_function(name, "a")).collect()
        assert str(caught.value).startswith(f"[UNRESOLVED_ROUTINE] Cannot resolve routine `{name}`")


def test_fnp_misc_1_bucket_literal_column_folds_to_the_int_path(spark: Any) -> None:
    """bucket(lit(4)) refuses exactly like bucket(4). pins: fnp-misc-1/C-003"""
    frame = spark.createDataFrame([(1, 10), (2, 20)], "id INT, v INT")
    with pytest.warns(FutureWarning, match="partitioning.bucket"):
        folded = F.bucket(lit(4), col("v"))
    with pytest.warns(FutureWarning, match="partitioning.bucket"):
        direct = F.bucket(4, "v")
    for expression in (folded, direct):
        with pytest.raises(
            AnalysisException, match="PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY"
        ):
            frame.select(expression).collect()


def test_fnp_misc_1_bucket_non_literal_column_raises_not_column_or_int(spark: Any) -> None:
    """A non-foldable Column numBuckets raises NOT_COLUMN_OR_INT. pins: fnp-misc-1/C-003"""
    with (
        pytest.warns(FutureWarning, match="partitioning.bucket"),
        pytest.raises(PySparkTypeError) as caught,
    ):
        F.bucket(col("v"), "v")
    assert caught.value.getCondition() == "NOT_COLUMN_OR_INT"
    assert caught.value.getMessageParameters() == {
        "arg_name": "numBuckets",
        "arg_type": "Column",
    }


def test_fnp_misc_1_bucket_foldable_non_int_column_raises_not_column_or_int() -> None:
    """A foldable non-int literal (string) is not a bucket count. pins: fnp-misc-1/C-003"""
    with (
        pytest.warns(FutureWarning, match="partitioning.bucket"),
        pytest.raises(PySparkTypeError) as caught,
    ):
        F.bucket(lit("four"), "v")
    assert caught.value.getCondition() == "NOT_COLUMN_OR_INT"


@pytest.mark.parametrize("cast_type", ["int", "bigint", "smallint", "tinyint"])
def test_fnp_misc_1_bucket_cast_int_column_folds_to_the_int_path(
    spark: Any, cast_type: str
) -> None:
    """bucket(lit(4).cast(int-type)) folds to bucket(4). pins: fnp-misc-1/L-003"""
    frame = spark.createDataFrame([(1, 10), (2, 20)], "id INT, v INT")
    with pytest.warns(FutureWarning, match="partitioning.bucket"):
        folded = F.bucket(lit(4).cast(cast_type), col("v"))
    with pytest.warns(FutureWarning, match="partitioning.bucket"):
        direct = F.bucket(4, "v")
    assert folded.spark_display_part() == direct.spark_display_part() == 'bucket(4, "v")'
    for expression in (folded, direct):
        with pytest.raises(
            AnalysisException, match="PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY"
        ):
            frame.select(expression).collect()


@pytest.mark.parametrize(
    "count_sql",
    [
        "lit(True).cast('int')",
        "lit(None).cast('int')",
        "lit('4').cast('int')",
        "lit(4.0).cast('int')",
        "lit(4).cast('string')",
        "lit(2) + lit(2)",
    ],
)
def test_fnp_misc_1_bucket_cast_non_int_column_raises_not_column_or_int(count_sql: str) -> None:
    """CAST-non-int and composed foldables are not bucket counts. pins: fnp-misc-1/L-003"""
    namespace: dict[str, Any] = {"lit": lit, "__builtins__": {}}
    with (
        pytest.warns(FutureWarning, match="partitioning.bucket"),
        pytest.raises(PySparkTypeError) as caught,
    ):
        F.bucket(eval(count_sql, namespace), "v")
    assert caught.value.getCondition() == "NOT_COLUMN_OR_INT"
    assert caught.value.getMessageParameters() == {
        "arg_name": "numBuckets",
        "arg_type": "Column",
    }


def _arrow_frame(spark: Any) -> Any:
    rows = [(1, 10, "a"), (2, None, "b"), (1, 30, None)]
    return spark.createDataFrame(rows, "g int, v int, s string")


def test_fnp_misc_1_arrow_udf_scalar_plus_one_with_null(spark: Any) -> None:
    """Scalar arrow UDF answers value, name and type. pins: fnp-misc-1/C-004"""
    cells = _cells(ARROW2_ORACLE)
    cell = _cell(cells, "arrow_udf", "scalar plus one with null")
    plus = F.arrow_udf(lambda a: pc.add(a, 1), "int")
    _assert_value_cell(_arrow_frame(spark).select(plus(col("v")).alias("x")).to_arrow(), cell)


def test_fnp_misc_1_arrow_udf_scalar_default_name(spark: Any) -> None:
    """A lambda arrow UDF projects <lambda>(v) by default. pins: fnp-misc-1/C-004"""
    cells = _cells(ARROW2_ORACLE)
    cell = _cell(cells, "arrow_udf", "scalar default name")
    plus = F.arrow_udf(lambda a: pc.add(a, 1), "int")
    _assert_value_cell(_arrow_frame(spark).select(plus("v")).to_arrow(), cell)


def test_fnp_misc_1_arrow_udf_two_args_with_null(spark: Any) -> None:
    """Two-arg arrow UDF keeps Spark NULL propagation. pins: fnp-misc-1/C-004"""
    cells = _cells(ARROW2_ORACLE)
    cell = _cell(cells, "arrow_udf", "two args")
    join = F.arrow_udf(
        lambda a, b: pc.binary_join_element_wise(
            pc.cast(a, pa.string()), pc.cast(b, pa.string()), "-"
        ),
        "string",
    )
    _assert_value_cell(
        _arrow_frame(spark).select(join(col("v"), col("s")).alias("x")).to_arrow(), cell
    )


def test_fnp_misc_1_arrow_udf_iterator_form_by_type_hints(spark: Any) -> None:
    """Iterator hints select the batch-iterator path. pins: fnp-misc-1/C-004"""
    from collections.abc import Iterator

    cells = _cells(ARROW2_ORACLE)
    cell = _cell(cells, "arrow_udf", "iterator form type hints")

    @F.arrow_udf("long")
    def iter_plus(it: Iterator[pa.Array]) -> Iterator[pa.Array]:
        for batch in it:
            yield pc.add(pc.cast(batch, pa.int64()), 100)

    _assert_value_cell(_arrow_frame(spark).select(iter_plus("v").alias("x")).to_arrow(), cell)


def test_fnp_misc_1_arrow_udf_grouped_agg_by_type_hints(spark: Any) -> None:
    """Scalar-return hints select the grouped-aggregate path. pins: fnp-misc-1/C-004"""
    cells = _cells(ARROW2_ORACLE)
    cell = _cell(cells, "arrow_udf", "grouped agg type hints")

    @F.arrow_udf("double")
    def mean_udf(values: pa.Array) -> float:
        return pc.mean(values).as_py()

    out = _arrow_frame(spark).groupBy("g").agg(mean_udf("v").alias("m")).orderBy("g")
    _assert_value_cell(out.to_arrow(), cell)


def test_fnp_misc_1_arrow_udf_wrong_length_raises_schema_mismatch(spark: Any) -> None:
    """Short results raise the Spark worker text. pins: fnp-misc-1/C-004, fnp-misc-1/L-008"""
    cells = _cells(ARROW2_ORACLE)
    cell = _cell(cells, "arrow_udf", "wrong return length")
    short = F.arrow_udf(lambda a: pa.array([1]), "int")
    with pytest.raises(PySparkRuntimeError) as caught:
        _arrow_frame(spark).select(short(col("v")).alias("x")).to_arrow()
    assert caught.value.getCondition() == "SCHEMA_MISMATCH_FOR_PANDAS_UDF"
    worker_text = cell["tail"][1].rsplit("PySparkRuntimeError: ", 1)[1]
    assert str(caught.value) == worker_text


def test_fnp_misc_1_arrow_udf_wrong_type_casts_to_declared(spark: Any) -> None:
    """A string result casts back to the declared int. pins: fnp-misc-1/C-004"""
    cells = _cells(ARROW2_ORACLE)
    cell = _cell(cells, "arrow_udf", "wrong return type")
    as_string = F.arrow_udf(lambda a: pc.cast(a, pa.string()), "int")
    _assert_value_cell(_arrow_frame(spark).select(as_string(col("v")).alias("x")).to_arrow(), cell)


def test_fnp_misc_1_arrow_udf_missing_return_type_raises(spark: Any) -> None:
    """returnType None raises the exact CANNOT_BE_NONE message. pins: fnp-misc-1/C-004"""
    cells = _cells(ARROW2_ORACLE)
    cell = _cell(cells, "arrow_udf", "no returnType")
    assert cell["error_condition"] == "CANNOT_BE_NONE"
    with pytest.raises(PySparkTypeError) as caught:
        F.arrow_udf(lambda a: a)
    assert caught.value.getCondition() == "CANNOT_BE_NONE"
    assert str(caught.value) == cell["message"], str(caught.value)


def test_fnp_misc_1_arrow_udf_mid_expression_composition_refused(spark: Any) -> None:
    """arrow_udf plus 1 refuses with the disclosed text. pins: fnp-misc-1/C-006, fnp-misc-1/L-008"""
    plus = F.arrow_udf(lambda a: pc.add(a, 1), "int")
    with pytest.raises(UnsupportedOperationException) as caught:
        _arrow_frame(spark).select((plus("v") + 1).alias("x")).to_arrow()
    assert str(caught.value) == (
        "pandas_udf result cannot be used in arithmetic (+) in repark v1 (facade "
        "projection-rewrite bridge only; not a Column expression in the SQL plan). "
        "Materialize via select/withColumn, then apply further expressions on that column. "
        "Mid-expression embedding is an M5-class seed."
    )


def test_fnp_misc_1_arrow_udf_register_then_sql_refused(spark: Any) -> None:
    """spark.udf.register rejects arrow callables in v1. pins: fnp-misc-1/C-006"""
    plus = F.arrow_udf(lambda a: pc.add(a, 1), "int")
    with pytest.raises(UnsupportedOperationException, match="pandas_udf"):
        spark.udf.register("arrow_plus", plus)


def test_fnp_misc_1_arrow_udf_grouped_in_select_refused(spark: Any) -> None:
    """Grouped arrow UDF refusal pins the message. pins: fnp-misc-1/C-006, fnp-misc-1/L-008"""

    @F.arrow_udf("double")
    def mean_udf(values: pa.Array) -> float:
        return pc.mean(values).as_py()

    with pytest.raises(AnalysisException) as caught:
        _arrow_frame(spark).select(mean_udf("v").alias("m")).to_arrow()
    assert str(caught.value) == (
        "GROUPED_AGG pandas_udf cannot be used in select/withColumn without "
        ".over(Window.partitionBy(...)); use groupBy(...).agg(pandas_udf(...)) for "
        "non-window form, or attach an unbounded partition window via .over"
    )


def test_fnp_misc_1_arrow_udf_object_shape(spark: Any) -> None:
    """The decorator answers a reusable bridge callable. pins: fnp-misc-1/C-004, fnp-misc-1/L-008"""

    @F.arrow_udf("long")
    def identity(values: pa.Array) -> pa.Array:
        return values

    assert type(identity).__name__ == "PandasUDFFunction"
    assert identity.__name__ == "identity"
    frame = spark.range(4).select((col("id") * 10).alias("v"))
    table = frame.select(identity("v").alias("v")).to_arrow()
    assert table.schema.field("v").type == pa.int64()
    assert [row["v"] for row in table.to_pylist()] == [0, 10, 20, 30]


def test_fnp_misc_1_arrow_udf_range_frame_cells(spark: Any) -> None:
    """Range-frame cells pin name, type and value. pins: fnp-misc-1/C-004, fnp-misc-1/L-008"""
    cells = _cells(ARROW_BUCKET_ORACLE)
    cell = _cell(cells, "arrow_udf", "arrow_udf(lambda a: pc.add(a, 1), 'long') applied")
    frame = spark.range(4).select((col("id") * 10).alias("v"))
    plus = F.arrow_udf(lambda a: pc.add(a, 1), "long")
    _assert_value_cell(frame.select(plus(col("v")).alias("x")).to_arrow(), cell)


def test_fnp_misc_1_arrow_udf_without_pyarrow_raises_package_not_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pyarrow-less interpreter raises the PACKAGE_NOT_INSTALLED shape. pins: fnp-misc-1/C-004"""
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    with pytest.raises(ImportError, match=r"PACKAGE_NOT_INSTALLED"):
        F.arrow_udf(lambda a: a, "long")


def test_fnp_misc_1_arrow_udf_identity_collapses_nan_to_null(spark: Any) -> None:
    """Identity maps NaN to NULL today (bridge collapses first). pins: fnp-misc-1/L-001"""
    frame = spark.createDataFrame([(1.5,), (float("nan"),), (None,), (0.0,)], "d DOUBLE")
    identity = F.arrow_udf(lambda a: a, "double")
    table = frame.select(identity(col("d")).alias("x")).to_arrow()
    assert table.schema.field("x").type == pa.float64()
    values = [row["x"] for row in table.to_pylist()]
    assert values[0] == 1.5
    assert values[1] is None
    assert values[2] is None
    assert values[3] == 0.0


def test_fnp_misc_1_arrow_grouped_all_null_int_group_arrives_double(spark: Any) -> None:
    """An all-null INT group arrives as double today. pins: fnp-misc-1/L-007"""
    seen: set[tuple[int, str]] = set()

    @F.arrow_udf("double")
    def record(values: pa.Array) -> float:
        """Record each group size and Arrow type, then average."""
        seen.add((len(values), str(values.type)))
        return pc.mean(values).as_py()

    frame = spark.createDataFrame([(1, None), (1, None), (2, 5)], "g INT, v INT")
    table = frame.groupBy("g").agg(record("v").alias("m")).orderBy("g").to_arrow()
    assert [row["m"] for row in table.to_pylist()] == [None, 5.0]
    assert seen == {(2, "double"), (1, "int32")}


def test_fnp_misc_1_arrow_iter_adapter_streams_one_batch() -> None:
    """The iterator adapter holds at most one live input batch. pins: fnp-misc-1/F-2"""
    import weakref

    import pandas as pd

    from repark.spark.functions_arrow_udf import _ArrowIterAdapter

    live: list[Any] = []
    peak = [0]

    def user(batches: Any) -> Any:
        """Record live inputs, then add one to each batch."""
        for array in batches:
            live.append(weakref.ref(array))
            peak[0] = max(peak[0], sum(1 for ref in live if ref() is not None))
            yield pc.add(array, 1)

    adapter = _ArrowIterAdapter(user, pa.int64())
    series = [pd.Series([1, 2], dtype="Int64"), pd.Series([3, 4], dtype="Int64")]
    out = list(adapter(iter(series)))
    assert peak[0] == 1
    assert [row.tolist() for row in out] == [[2, 3], [4, 5]]


class _RowsHandler:
    """Three-row arrow UDTF handler used by the table-function pins."""

    def eval(self, counts: pa.Array) -> Any:
        """Yield one range table with total rows."""
        total = counts[0].as_py() if len(counts) else 0
        yield pa.table({"i": pa.array(list(range(total)), pa.int64())})


def test_fnp_misc_1_arrow_udtf_table_fn_answers_rows(spark: Any) -> None:
    """rows_udtf(lit(3)) yields i 0, 1, 2 as bigint. pins: fnp-misc-1/C-005"""
    cells = _cells(ARROW2_ORACLE)
    cell = _cell(cells, "arrow_udtf", "table fn lit(3)")
    rows_udtf = F.arrow_udtf(_RowsHandler, returnType="i long")
    assert type(rows_udtf).__name__ == "UserDefinedTableFunction"
    _assert_value_cell(rows_udtf(lit(3)).to_arrow(), cell)


def test_fnp_misc_1_arrow_udtf_decorator_form_answers_rows(spark: Any) -> None:
    """The decorator form carries the handler name. pins: fnp-misc-1/C-005, fnp-misc-1/L-008"""

    @F.arrow_udtf(returnType="i long")
    class DecoratedRows:
        """Two-row arrow UDTF handler for the decorator pin."""

        def eval(self, counts: pa.Array) -> Any:
            """Yield one range table with total rows."""
            total = counts[0].as_py() if len(counts) else 0
            yield pa.table({"i": pa.array(list(range(total)), pa.int64())})

    assert type(DecoratedRows).__name__ == "UserDefinedTableFunction"
    table = DecoratedRows(lit(2)).to_arrow()
    assert table.schema.field("i").type == pa.int64()
    assert [row["i"] for row in table.to_pylist()] == [0, 1]


def test_fnp_misc_1_arrow_udtf_lateral_join_via_sql(spark: Any) -> None:
    """A registered arrow UDTF answers SELECT * FROM name(arg). pins: fnp-misc-1/C-005"""
    cells = _cells(ARROW2_ORACLE)
    cell = _cell(cells, "arrow_udtf", "lateral join via sql")
    rows_udtf = F.arrow_udtf(_RowsHandler, returnType="i long")
    spark.udtf.register("rows_udtf", rows_udtf)
    _assert_value_cell(spark.sql("SELECT * FROM rows_udtf(2)").to_arrow(), cell)


def test_fnp_misc_1_arrow_udtf_zero_arg_eval_mismatch_refused(spark: Any) -> None:
    """Calling without the eval argument refuses loud. pins: fnp-misc-1/C-006"""

    @F.arrow_udtf(returnType="x int, y string")
    class Echo:
        """Single-row arrow UDTF handler for the mismatch pin."""

        def eval(self, values: pa.Array) -> Any:
            """Yield one fixed row."""
            yield pa.table({"x": pa.array([1], pa.int32()), "y": pa.array(["p"])})

    with pytest.raises(PySparkException):
        Echo().to_arrow()


def test_fnp_misc_1_arrow_udtf_non_arrow_yield_refused(spark: Any) -> None:
    """Yields outside Table/RecordBatch raise the conversion shape. pins: fnp-misc-1/C-006"""

    @F.arrow_udtf(returnType="i long")
    class TupleYielder:
        """Non-arrow yield handler for the conversion pin."""

        def eval(self, counts: pa.Array) -> Any:
            """Yield a bare tuple instead of an Arrow table."""
            yield (1,)

    with pytest.raises(PySparkException, match="UDTF_ARROW_TYPE_CONVERSION_ERROR"):
        TupleYielder(lit(1)).to_arrow()


def test_fnp_misc_1_arrow_udtf_without_pyarrow_raises_package_not_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pyarrow-less interpreter refuses arrow_udtf too. pins: fnp-misc-1/C-005"""
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    with pytest.raises(ImportError, match=r"PACKAGE_NOT_INSTALLED"):
        F.arrow_udtf(_RowsHandler, returnType="i long")


class _BatchCountingHandler:
    """Range-table handler recording every eval call for the batch pin."""

    calls: ClassVar[list[int]] = []

    def eval(self, counts: pa.Array) -> Any:
        """Yield one range table sized to the input batch."""
        type(self).calls.append(len(counts))
        yield pa.table({"i": pa.array(list(range(len(counts))), pa.int64())})


def test_fnp_misc_1_arrow_udtf_eval_once_per_batch() -> None:
    """Eval runs once per RecordBatch with whole-column Arrays. pins: fnp-misc-1/F-1"""
    from repark.spark.dataframe import _coerce_map_in_arrow_schema
    from repark.spark.udtf import _map_udtf_batches

    _BatchCountingHandler.calls = []
    _, arrow_schema = _coerce_map_in_arrow_schema("i long")
    batch = pa.record_batch([pa.array([7, 8, 9], pa.int64())], names=["n"])
    out = list(
        _map_udtf_batches(
            iter([batch]),
            handler_cls=F.arrow_udtf(_BatchCountingHandler, returnType="i long").func,
            arg_count=1,
            field_names=["i"],
            arrow_schema=arrow_schema,
            surface="batch-pin",
        )
    )
    assert _BatchCountingHandler.calls == [3]
    assert [row["i"] for batch_out in out for row in batch_out.to_pylist()] == [0, 1, 2]


def test_fnp_misc_1_arrow_udtf_swapped_columns_match_by_name(spark: Any) -> None:
    """Same-type reordered yields land by field name. pins: fnp-misc-1/L-002"""

    @F.arrow_udtf(returnType="x int, y int")
    class Swapped:
        """Yield correctly named columns in the wrong order."""

        def eval(self, counts: pa.Array) -> Any:
            """Yield y before x with distinct values."""
            yield pa.table({"y": pa.array([2], pa.int32()), "x": pa.array([1], pa.int32())})

    assert Swapped(lit(1)).to_arrow().to_pylist() == [{"x": 1, "y": 2}]


def test_fnp_misc_1_arrow_udtf_wrong_field_name_refused(spark: Any) -> None:
    """A yield naming no declared field refuses loud. pins: fnp-misc-1/L-002"""

    @F.arrow_udtf(returnType="i long")
    class Misnamed:
        """Yield a column the returnType does not declare."""

        def eval(self, counts: pa.Array) -> Any:
            """Yield j instead of i."""
            yield pa.table({"j": pa.array([1], pa.int64())})

    with pytest.raises(PySparkException, match="missing returnType field"):
        Misnamed(lit(1)).to_arrow()

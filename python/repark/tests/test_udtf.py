"""C6 / U12 — UDTF scalar-arg core (mapInArrow + FROM name(lit_args)).

Validation error classes held (U11); the scalar-arg relation constructor executes; LATERAL and
table-arg stay blocked.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from repark import SparkSession
from repark.errors import (
    PySparkAttributeError,
    PySparkException,
    PySparkTypeError,
    UnsupportedOperationException,
)
from repark.spark.functions import lit, udtf
from repark.spark.udtf import UDTFRegistration, UserDefinedTableFunction


@pytest.fixture
def spark() -> Iterator[SparkSession]:
    session = SparkSession.builder.master("local[1]").appName("test-udtf-u12").getOrCreate()
    yield session
    session.stop()


def test_udtf_decorator_scalar_call_expands_rows(spark: SparkSession) -> None:
    """@udtf constructs UserDefinedTableFunction; lit call yields expanded rows (U12)."""
    _ = spark

    @udtf(returnType="c1: int, c2: int")
    class PlusOne:
        def eval(self, value: int) -> Iterator[tuple[int, int]]:
            yield value, value + 1

    assert isinstance(PlusOne, UserDefinedTableFunction)
    table = PlusOne(lit(1)).to_arrow()
    assert table.column_names == ["c1", "c2"]
    assert table.to_pylist() == [{"c1": 1, "c2": 2}]
    # Arrow types — int32 for DDL int (createDataFrame/mapInArrow path).
    assert str(table.schema.field("c1").type) in {"int32", "int64"}


def test_udtf_multi_row_expand(spark: SparkSession) -> None:
    """eval yielding multiple rows expands the relation multiset."""
    _ = spark

    @udtf(returnType="num: int, squared: int")
    class SquareNumbers:
        def eval(self, start: int, end: int) -> Iterator[tuple[int, int]]:
            for number in range(start, end + 1):
                yield number, number * number

    rows = SquareNumbers(lit(1), lit(3)).collect()
    assert [(row.num, row.squared) for row in rows] == [(1, 1), (2, 4), (3, 9)]


def test_udtf_register_and_sql_from(spark: SparkSession) -> None:
    """spark.udtf.register + SELECT * FROM name(lit_args) rewrite (U12)."""

    @udtf(returnType="word: string")
    class WordSplitter:
        def eval(self, text: str) -> Iterator[tuple[str]]:
            for word in text.split(" "):
                yield (word.strip(),)

    registered = spark.udtf.register("split_words", WordSplitter)
    assert isinstance(registered, UserDefinedTableFunction)
    assert "split_words" in spark._udtf_registry()

    table = spark.sql("SELECT * FROM split_words('hello world')").to_arrow()
    assert table.column_names == ["word"]
    assert sorted(table.column("word").to_pylist()) == ["hello", "world"]


def test_udtf_sql_select_column_subset(spark: SparkSession) -> None:
    """SELECT c1 FROM name(lit_args) projects a subset after the relation constructor."""

    @udtf(returnType="c1: int, c2: int")
    class Pair:
        def eval(self, value: int) -> Iterator[tuple[int, int]]:
            yield value, value + 1

    spark.udtf.register("pair_udtf", Pair)
    table = spark.sql("SELECT c1 FROM pair_udtf(7)").to_arrow()
    assert table.column_names == ["c1"]
    assert table.to_pylist() == [{"c1": 7}]


def test_udtf_lateral_sql_refuses_loud(spark: SparkSession) -> None:
    """LATERAL UDTF stays blocked (U11 seed / U12 bound)."""

    @udtf(returnType="a: int, b: int, c: int")
    class Triple:
        def eval(self, left: int, right: int) -> Iterator[tuple[int, int, int]]:
            yield left, right, left + right

    spark.udtf.register("testUDTF", Triple)
    with pytest.raises(
        UnsupportedOperationException,
        match=r"LATERAL|OuterReferenceColumn|not supported",
    ) as caught:
        spark.sql("SELECT f.* FROM values (0, 1), (1, 2) t(a, b), LATERAL testUDTF(a, b) f")
    assert "__repark_sql_udf" not in str(caught.value)


def test_udtf_non_literal_column_refuses(spark: SparkSession) -> None:
    """Non-foldable Column args refuse (would require LATERAL)."""
    from repark.spark.functions import col

    @udtf(returnType="a: int")
    class Echo:
        def eval(self, value: int) -> Iterator[tuple[int]]:
            yield (value,)

    _ = spark.createDataFrame([(1,), (2,)], "x INT")
    with pytest.raises(
        UnsupportedOperationException,
        match=r"foldable|literal|LATERAL|not supported",
    ):
        Echo(col("x"))


def test_udtf_table_arg_refuses(spark: SparkSession) -> None:
    """DataFrame / table-arg form refuses loud."""

    @udtf(returnType="a: int")
    class Echo:
        def eval(self, value: object) -> Iterator[tuple[object]]:
            yield (value,)

    frame = spark.range(2)
    with pytest.raises(
        UnsupportedOperationException,
        match=r"table-argument|table.arg|not supported|LATERAL",
    ):
        Echo(frame)


def test_udtf_direct_form_and_as_deterministic(spark: SparkSession) -> None:
    """udtf(Handler, returnType=…) and asDeterministic execute (U12)."""
    _ = spark

    class Echo:
        def eval(self, value: object) -> Iterator[tuple[object]]:
            yield (value,)

    built = udtf(Echo, returnType="v: int")
    assert isinstance(built, UserDefinedTableFunction)
    built.asDeterministic()
    assert built.deterministic is True
    assert built(lit(9)).collect()[0].v == 9


def test_udtf_invalid_handler_no_eval() -> None:
    """Handler without eval → Spark INVALID_UDTF_NO_EVAL."""

    class NoEval:
        pass

    with pytest.raises(
        PySparkAttributeError,
        match=r"INVALID_UDTF_NO_EVAL|eval",
    ) as caught:
        udtf(NoEval, returnType="c1: int")
    assert caught.value.getErrorClass() == "INVALID_UDTF_NO_EVAL"
    assert "__repark_sql_udf" not in str(caught.value)


def test_udtf_invalid_handler_not_class() -> None:
    """Non-class handler → Spark INVALID_UDTF_HANDLER_TYPE."""

    def not_a_class() -> None:
        return None

    with pytest.raises(
        PySparkTypeError,
        match=r"INVALID_UDTF_HANDLER_TYPE|class",
    ) as caught:
        udtf(not_a_class, returnType="c1: int")  # type: ignore[arg-type]
    assert caught.value.getErrorClass() == "INVALID_UDTF_HANDLER_TYPE"


def test_udtf_invalid_missing_return_type_and_analyze() -> None:
    """No returnType and no analyze staticmethod → INVALID_UDTF_RETURN_TYPE."""

    class MissingBoth:
        def eval(self, value: object) -> Iterator[tuple[object]]:
            yield (value,)

    with pytest.raises(
        PySparkAttributeError,
        match=r"INVALID_UDTF_RETURN_TYPE|return type",
    ) as caught:
        udtf(MissingBoth)
    assert caught.value.getErrorClass() == "INVALID_UDTF_RETURN_TYPE"


def test_udtf_invalid_both_return_type_and_analyze() -> None:
    """returnType + analyze → INVALID_UDTF_BOTH_RETURN_TYPE_AND_ANALYZE."""

    class Both:
        @staticmethod
        def analyze() -> object:
            return None

        def eval(self, value: object) -> Iterator[tuple[object]]:
            yield (value,)

    with pytest.raises(
        PySparkAttributeError,
        match=r"INVALID_UDTF_BOTH_RETURN_TYPE_AND_ANALYZE|analyze",
    ) as caught:
        udtf(Both, returnType="c1: int")
    assert caught.value.getErrorClass() == "INVALID_UDTF_BOTH_RETURN_TYPE_AND_ANALYZE"


def test_spark_udtf_property_and_register_type(spark: SparkSession) -> None:
    """spark.udtf is UDTFRegistration; wrong-type register → CANNOT_REGISTER_UDTF."""
    assert isinstance(spark.udtf, UDTFRegistration)
    with pytest.raises(
        PySparkTypeError,
        match=r"CANNOT_REGISTER_UDTF|UserDefinedTableFunction",
    ) as caught:
        spark.udtf.register("bad", lambda value: value)  # type: ignore[arg-type]
    assert caught.value.getErrorClass() == "CANNOT_REGISTER_UDTF"
    assert "__repark_sql_udf" not in str(caught.value)


def test_spark_udtf_register_refuses_internal_name_prefix(spark: SparkSession) -> None:
    """Register name must not use __repark_sql_udf materialization prefix."""

    @udtf(returnType="c1: int")
    class OneCol:
        def eval(self, value: int) -> Iterator[tuple[int]]:
            yield (value,)

    with pytest.raises(
        PySparkTypeError,
        match=r"__repark_sql_udf|materialization|reserved",
    ):
        spark.udtf.register("__repark_sql_udf_evil", OneCol)


def test_functions_udtf_export() -> None:
    """repark.functions.udtf is the public decorator export (PySpark functions.udtf)."""
    from repark import functions as functions_module

    assert functions_module.udtf is udtf
    assert "udtf" in functions_module.__all__
    assert "UserDefinedTableFunction" in functions_module.__all__


def test_scalar_udf_paths_refuse_udtf_wrapper(spark: SparkSession) -> None:
    """F.udf / spark.udf.register must not half-wire a table UDTF as scalar (octo U11 C1)."""
    from repark.spark.functions import udf as scalar_udf

    @udtf(returnType="c1: int, c2: int")
    class PlusOne:
        def eval(self, value: int) -> Iterator[tuple[int, int]]:
            yield value, value + 1

    with pytest.raises(PySparkTypeError, match=r"UserDefinedTableFunction|table UDTF|scalar"):
        scalar_udf(PlusOne, "long")
    with pytest.raises(PySparkTypeError, match=r"UserDefinedTableFunction|table UDTF|scalar"):
        spark.udf.register("plus_as_udf", PlusOne)
    assert "plus_as_udf" not in spark._udf_registry()


def test_udtf_python_scalar_args(spark: SparkSession) -> None:
    """Bare Python scalars (not only lit) are accepted as foldable args."""
    _ = spark

    @udtf(returnType="a: int")
    class Echo:
        def eval(self, value: int) -> Iterator[tuple[int]]:
            yield (value,)

    assert Echo(42).collect()[0].a == 42


def test_udtf_empty_eval_yields_empty_frame(spark: SparkSession) -> None:
    """eval that yields nothing → empty relation with declared schema."""
    _ = spark

    @udtf(returnType="a: int")
    class Empty:
        def eval(self, value: int) -> Iterator[tuple[int]]:
            if False:  # pragma: no cover — documents empty iterator shape
                yield (value,)
            return iter(())

    out = Empty(lit(1))
    assert out.collect() == []
    assert out.columns == ["a"]


def test_udtf_sql_name_in_string_does_not_hijack(spark: SparkSession) -> None:
    """Registered UDTF name inside a string/comment must not refuse unrelated SQL (C1-SEC-001)."""

    @udtf(returnType="w: string")
    class Echo:
        def eval(self, text: str) -> Iterator[tuple[str]]:
            yield (text,)

    spark.udtf.register("split_words", Echo)
    spark.range(1).createOrReplaceTempView("t")

    # Name only inside a string literal — engine path, not UDTF rewrite.
    rows = spark.sql("SELECT 'split_words(' AS s FROM t").collect()
    assert rows[0].s == "split_words("

    # Name only inside a line comment — still plain table scan.
    rows = spark.sql("SELECT * FROM t -- split_words(x)\nWHERE id = 0").collect()
    assert len(rows) == 1 and rows[0].id == 0

    # Name only inside a block comment.
    rows = spark.sql("SELECT * FROM t /* split_words( */ WHERE id = 0").collect()
    assert len(rows) == 1


def test_udtf_sql_join_table_factor_refuses(spark: SparkSession) -> None:
    """JOIN … registered_udtf(…) is a table-factor hit → loud U12 refuse (not hijack-by-string)."""

    @udtf(returnType="w: string")
    class Echo:
        def eval(self, text: str) -> Iterator[tuple[str]]:
            yield (text,)

    spark.udtf.register("echo_udtf", Echo)
    spark.range(1).createOrReplaceTempView("t")
    with pytest.raises(
        UnsupportedOperationException,
        match=r"table factor|not a supported U12|JOIN|LATERAL",
    ):
        spark.sql("SELECT * FROM t JOIN echo_udtf('x')")
    with pytest.raises(
        UnsupportedOperationException,
        match=r"table factor|not a supported U12",
    ):
        spark.sql("SELECT * FROM t, echo_udtf('x')")


def test_udtf_register_does_not_hijack_select_list_calls(spark: SparkSession) -> None:
    """UDTF name colliding with SQL fn must not break SELECT-list calls (octo C5-SEC-001)."""

    @udtf(returnType="a: int")
    class One:
        def eval(self, value: int) -> Iterator[tuple[int]]:
            yield (value,)

    spark.range(3).createOrReplaceTempView("t")
    # Register common function names as UDTFs — SELECT-list uses must still plan.
    spark.udtf.register("max", One)
    spark.udtf.register("abs", One)
    assert spark.sql("SELECT max(id) AS m FROM t").collect()[0].m == 2
    rows = spark.sql("SELECT id, abs(id) AS a FROM t ORDER BY id").collect()
    assert [(row.id, row.a) for row in rows] == [(0, 0), (1, 1), (2, 2)]
    # FROM-form still routes to the UDTF.
    assert spark.sql("SELECT * FROM max(9)").collect()[0].a == 9


def test_udtf_sql_unclosed_string_refuses(spark: SparkSession) -> None:
    """Unclosed SQL string in FROM-udtf args refuses (no silent value; C1-SEC-002)."""

    @udtf(returnType="w: string")
    class Echo:
        def eval(self, text: str) -> Iterator[tuple[str]]:
            yield (text,)

    spark.udtf.register("echo_str", Echo)
    with pytest.raises(
        UnsupportedOperationException,
        match=r"unclosed string|scalar SQL literal",
    ):
        spark.sql("SELECT * FROM echo_str('hello)")


def test_udtf_sql_null_true_false_and_case(spark: SparkSession) -> None:
    """SQL NULL/TRUE/FALSE literals + case-insensitive UDTF name (C1-Q-001 pins)."""

    @udtf(returnType="label: string")
    class Show:
        def eval(self, value: object) -> Iterator[tuple[str]]:
            yield (str(value),)

    spark.udtf.register("show_val", Show)
    assert spark.sql("SELECT * FROM show_val(NULL)").collect()[0].label == "None"
    assert spark.sql("SELECT * FROM show_val(TRUE)").collect()[0].label == "True"
    assert spark.sql("SELECT * FROM show_val(FALSE)").collect()[0].label == "False"
    # Registry key lower; SQL upper.
    assert spark.sql("SELECT * FROM SHOW_VAL(1)").collect()[0].label == "1"


def test_udtf_sql_multi_arg_and_escaped_quote(spark: SparkSession) -> None:
    """Multi-arg SQL literals + SQL doubled-quote escape."""

    @udtf(returnType="a: int, b: string")
    class Pair:
        def eval(self, left: int, right: str) -> Iterator[tuple[int, str]]:
            yield left, right

    spark.udtf.register("pair_sql", Pair)
    row = spark.sql("SELECT * FROM pair_sql(3, 'it''s')").collect()[0]
    assert row.a == 3
    assert row.b == "it's"


def test_udtf_eval_arity_mismatch_refuses(spark: SparkSession) -> None:
    """Yield width must match returnType field count (C1-L-003; no silent pad/truncate)."""
    _ = spark

    @udtf(returnType="a: int, b: int")
    class ShortRow:
        def eval(self, value: int) -> Iterator[tuple[int]]:
            yield (value,)

    with pytest.raises(PySparkException, match=r"yielded 1 column|declares 2"):
        ShortRow(1).collect()

    @udtf(returnType="a: int")
    class LongRow:
        def eval(self, value: int) -> Iterator[tuple[int, int]]:
            yield value, value + 1

    with pytest.raises(PySparkException, match=r"yielded 2 column|declares 1"):
        LongRow(1).collect()


def test_udtf_zero_arg_and_register_bad_name(spark: SparkSession) -> None:
    """Zero-arg UDTF executes; register rejects non-simple identifiers."""

    @udtf(returnType="a: int")
    class Const:
        def eval(self) -> Iterator[tuple[int]]:
            yield (7,)

    assert Const().collect()[0].a == 7
    spark.udtf.register("const_udtf", Const)
    assert spark.sql("SELECT * FROM const_udtf()").collect()[0].a == 7

    with pytest.raises(PySparkTypeError, match=r"simple SQL identifier"):
        spark.udtf.register("bad-name", Const)


def test_udtf_sql_scientific_int_and_trailing_comma(spark: SparkSession) -> None:
    """SQL 1e2 accepted as float; trailing comma refuses (octo C2-L-001 / C2-SEC-001)."""

    @udtf(returnType="v: double")
    class EchoNum:
        def eval(self, value: float) -> Iterator[tuple[float]]:
            yield (float(value),)

    spark.udtf.register("echo_num", EchoNum)
    assert spark.sql("SELECT * FROM echo_num(1e2)").collect()[0].v == 100.0
    assert spark.sql("SELECT * FROM echo_num(1E2)").collect()[0].v == 100.0
    with pytest.raises(
        UnsupportedOperationException,
        match=r"trailing comma|scalar SQL literal",
    ):
        spark.sql("SELECT * FROM echo_num(1,)")


def test_udtf_start_failure_still_calls_terminate(spark: SparkSession) -> None:
    """terminate() runs even when start() raises (octo C2-Q-001)."""
    _ = spark
    log: list[str] = []

    class BoomStart:
        def start(self) -> None:
            log.append("start")
            raise RuntimeError("start failed")

        def eval(self, value: int) -> Iterator[tuple[int]]:
            log.append("eval")
            yield (value,)

        def terminate(self) -> None:
            log.append("term")

    handler = UserDefinedTableFunction(BoomStart, returnType="a: int")
    with pytest.raises(PySparkException, match=r"start\(\)|start failed"):
        handler(1).collect()
    assert log == ["start", "term"]


def test_udtf_eval_yield_none_refuses(spark: SparkSession) -> None:
    """Bare yield None refuses (use None cells inside a tuple; octo C2-Q-002)."""
    _ = spark

    @udtf(returnType="a: int")
    class YieldNone:
        def eval(self, value: int) -> Iterator[object]:
            yield None
            yield (value,)

    with pytest.raises(PySparkException, match=r"yielded None"):
        YieldNone(1).collect()

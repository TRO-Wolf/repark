"""grouped-surface-1 pins: apply / applyInArrow / cogroup and the state-API refusals."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom
from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    PySparkNotImplementedError,
    PySparkRuntimeError,
    PySparkTypeError,
    UnsupportedOperationException,
)
from repark.spark.dataframe import DataFrame
from repark.spark.functions import PandasUDFType
from repark.spark.functions_udf import PandasUDFFunction
from repark.spark.types import DoubleType, LongType, StructField, StructType

_ORACLE: dict[str, Any] = json.loads(
    Path(__file__).with_name("facade_grouped_data_oracle.json").read_text(encoding="utf-8")
)["cells"]

_CELL_IDS = {
    "applyInArrow",
    "applyInArrow_bad_schema_result",
    "applyInArrow_ddl_struct",
    "applyInArrow_expr_group",
    "applyInArrow_extra_col",
    "applyInArrow_iter",
    "applyInArrow_key",
    "applyInArrow_missing_col",
    "applyInArrow_not_callable",
    "applyInArrow_returns_not_table",
    "applyInPandasWithState_batch",
    "applyInPandas_exists",
    "apply_pandas_udf",
    "apply_plain_function",
    "apply_scalar_pandas_udf",
    "cogroup_applyInArrow",
    "cogroup_applyInArrow_key",
    "cogroup_applyInPandas",
    "cogroup_applyInPandas_key",
    "cogroup_key_count_mismatch",
    "cogroup_not_grouped",
    "cogroup_one_side_empty",
    "cogroup_type",
    "transformWithStateInPandas_batch",
    "transformWithState_batch",
}


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """A per-test facade session."""
    session = ReparkSession.builder.appName("pytest-grouped-surface-1").getOrCreate()
    yield session
    session.stop()


def _kv_frame(spark: ReparkSession) -> DataFrame:
    """The (id long, v double) frame every grouped oracle cell ran against."""
    return spark.createDataFrame([(1, 1.0), (1, 2.0), (2, 3.0)], "id long, v double")


def _gm(pdf: Any) -> Any:
    """The grouped-map pandas UDF body the oracle cells applied."""
    return pdf.assign(v=pdf.v - pdf.v.mean())


def _assert_frame_cell(frame: DataFrame, cell_id: str) -> None:
    """Compare a frame against one recorded DataFrame oracle cell."""
    cell = _ORACLE[cell_id]["result"]
    assert cell["kind"] == "DataFrame"
    assert frame.columns == cell["columns"]
    assert frame.schema.simpleString() == cell["schema"]
    assert [repr(row) for row in frame.collect()] == cell["rows"]


def test_fixture_covers_the_named_cells() -> None:
    """The copied fixture still holds every cell this unit pins. pins: grouped-surface-1/C-007"""
    assert set(_ORACLE.keys()) == _CELL_IDS


def test_apply_grouped_map_pandas_udf(spark: ReparkSession) -> None:
    """apply runs a GROUPED_MAP pandas UDF and warns. pins: grouped-surface-1/C-001"""
    udf = PandasUDFFunction(_gm, "id long, v double", function_type=PandasUDFType.GROUPED_MAP)
    grouped = _kv_frame(spark).groupBy("id")
    with pytest.warns(UserWarning, match="applyInPandas"):
        result = grouped.apply(udf)
    _assert_frame_cell(result.orderBy("id", "v"), "apply_pandas_udf")
    with pytest.raises(UnsupportedOperationException):
        F.pandas_udf("id long, v double", PandasUDFType.GROUPED_MAP)(_gm)


def test_apply_rejects_non_grouped_map_udfs(spark: ReparkSession) -> None:
    """apply refuses plain functions and SCALAR pandas UDFs. pins: grouped-surface-1/C-001"""
    grouped = _kv_frame(spark).groupBy("id")
    with pytest.raises(PySparkTypeError) as plain:
        grouped.apply(_gm)
    assert plain.value.getCondition() == "INVALID_UDF_EVAL_TYPE"
    assert plain.value.getMessageParameters() == {"eval_type": "SQL_GROUPED_MAP_PANDAS_UDF"}
    assert str(plain.value) == _ORACLE["apply_plain_function"]["error"]["message"]
    scalar = F.pandas_udf(lambda s: s, "double")
    with pytest.raises(PySparkTypeError) as wrong_eval:
        grouped.apply(scalar)
    assert wrong_eval.value.getCondition() == "INVALID_UDF_EVAL_TYPE"
    with pytest.raises(PySparkTypeError):
        grouped.apply(F.col("id"))


def test_apply_in_arrow_table_and_key_forms(spark: ReparkSession) -> None:
    """applyInArrow accepts func(table) and func(key, table). pins: grouped-surface-1/C-002"""
    grouped = _kv_frame(spark).groupBy("id")
    _assert_frame_cell(
        grouped.applyInArrow(lambda t: t, "id long, v double").orderBy("id", "v"),
        "applyInArrow",
    )
    keyed = grouped.applyInArrow(
        lambda k, t: pa.table({"id": [k[0].as_py()], "n": [t.num_rows]}),
        "id long, n long",
    )
    _assert_frame_cell(keyed.orderBy("id"), "applyInArrow_key")
    struct = StructType([StructField("id", LongType()), StructField("v", DoubleType())])
    _assert_frame_cell(
        grouped.applyInArrow(lambda t: t, struct).orderBy("id", "v"),
        "applyInArrow_ddl_struct",
    )


def test_apply_in_arrow_iterator_form(spark: ReparkSession) -> None:
    """Iterator[pa.RecordBatch] hints select Spark's iterator eval type.

    pins: grouped-surface-1/C-002
    """

    def batch_iter(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        yield from batches

    result = _kv_frame(spark).groupBy("id").applyInArrow(batch_iter, "id long, v double")
    _assert_frame_cell(result.orderBy("id", "v"), "applyInArrow")


def test_apply_in_arrow_result_validation_errors(spark: ReparkSession) -> None:
    """Wrong Arrow results raise Spark's inner error class. pins: grouped-surface-1/C-003"""
    grouped = _kv_frame(spark).groupBy("id")
    for cell_id, exc_type, error_class in [
        ("applyInArrow_bad_schema_result", PySparkRuntimeError, "RESULT_COLUMN_TYPES_MISMATCH"),
        ("applyInArrow_missing_col", PySparkRuntimeError, "RESULT_COLUMN_NAMES_MISMATCH"),
        ("applyInArrow_extra_col", PySparkRuntimeError, "RESULT_COLUMN_NAMES_MISMATCH"),
        ("applyInArrow_returns_not_table", PySparkTypeError, "UDF_RETURN_TYPE"),
        ("applyInArrow_iter", PySparkTypeError, "UDF_RETURN_TYPE"),
    ]:
        assert _ORACLE[cell_id]["error"]["raises"] == "PythonException"
        with pytest.raises(exc_type) as raised:
            _run_error_cell(grouped, cell_id)
        assert raised.value.getCondition() == error_class, cell_id


def _run_error_cell(grouped: Any, cell_id: str) -> None:
    """Replay the oracle's failing applyInArrow call and force it."""
    calls = {
        "applyInArrow_bad_schema_result": lambda: grouped.applyInArrow(
            lambda t: t, "id long, v string"
        ),
        "applyInArrow_missing_col": lambda: grouped.applyInArrow(
            lambda t: t.select(["id"]), "id long, v double"
        ),
        "applyInArrow_extra_col": lambda: grouped.applyInArrow(
            lambda t: t.append_column("z", pa.array([1] * t.num_rows)),
            "id long, v double",
        ),
        "applyInArrow_returns_not_table": lambda: grouped.applyInArrow(lambda t: 5, "id long"),
        "applyInArrow_iter": lambda: grouped.applyInArrow(
            lambda t: iter(list(t)), "id long, v double"
        ),
    }
    calls[cell_id]().collect()


def test_apply_in_arrow_not_callable(spark: ReparkSession) -> None:
    """applyInArrow refuses a non-callable with NOT_CALLABLE. pins: grouped-surface-1/C-003"""
    grouped = _kv_frame(spark).groupBy("id")
    with pytest.raises(PySparkTypeError) as raised:
        grouped.applyInArrow(5, "id long")
    assert raised.value.getCondition() == "NOT_CALLABLE"
    assert raised.value.getMessageParameters() == {
        "arg_name": "func",
        "arg_type": "int",
    }


def test_apply_in_arrow_expression_group_key_refusal(spark: ReparkSession) -> None:
    """Expression group keys share the applyInPandas refusal. pins: grouped-surface-1/C-003"""
    assert _ORACLE["applyInArrow_expr_group"]["result"]["rows"] == [
        "Row(k=0, n=1)",
        "Row(k=1, n=2)",
    ]
    grouped = _kv_frame(spark).groupBy(F.col("id") % 2)
    with pytest.raises(AnalysisException):
        grouped.applyInArrow(
            lambda k, t: pa.table({"k": [k[0].as_py()], "n": [t.num_rows]}),
            "k long, n long",
        )


def test_cogroup_type_and_ungrouped_side(spark: ReparkSession) -> None:
    """cogroup returns PandasCogroupedOps and refuses ungrouped sides.

    pins: grouped-surface-1/C-004
    """
    frame = _kv_frame(spark)
    ops = frame.groupBy("id").cogroup(frame.groupBy("id"))
    assert type(ops).__name__ == _ORACLE["cogroup_type"]["result"]["value"]
    assert _ORACLE["cogroup_not_grouped"]["result"]["kind"] == "PandasCogroupedOps"
    with pytest.raises(PySparkTypeError) as raised:
        frame.groupBy("id").cogroup(frame)
    assert raised.value.getCondition() == "NOT_EXPECTED_TYPE"
    assert raised.value.getMessageParameters() == {
        "arg_name": "other",
        "expected_type": "GroupedData",
        "arg_type": "DataFrame",
    }


def test_cogroup_apply_in_pandas(spark: ReparkSession) -> None:
    """Cogrouped applyInPandas merges both sides per key. pins: grouped-surface-1/C-004"""
    frame = _kv_frame(spark)
    both = frame.groupBy("id").cogroup(frame.filter("v > 1").groupBy("id"))
    result = both.applyInPandas(
        lambda left, right: pd.DataFrame(
            {
                "id": [left.id.iloc[0] if len(left) else right.id.iloc[0]],
                "nl": [len(left)],
                "nr": [len(right)],
            }
        ),
        "id long, nl long, nr long",
    )
    _assert_frame_cell(result.orderBy("id"), "cogroup_applyInPandas")
    keyed = (
        frame.groupBy("id")
        .cogroup(frame.groupBy("id"))
        .applyInPandas(
            lambda k, left, right: pd.DataFrame({"id": [k[0]], "n": [len(left) + len(right)]}),
            "id long, n long",
        )
    )
    _assert_frame_cell(keyed.orderBy("id"), "cogroup_applyInPandas_key")
    one_side = frame.groupBy("id").cogroup(frame.filter("id = 1").groupBy("id"))
    empty_result = one_side.applyInPandas(
        lambda left, right: pd.DataFrame({"nl": [len(left)], "nr": [len(right)]}),
        "nl long, nr long",
    )
    _assert_frame_cell(empty_result.orderBy("nl", "nr"), "cogroup_one_side_empty")


def test_cogroup_apply_in_arrow(spark: ReparkSession) -> None:
    """Cogrouped applyInArrow merges both sides per key. pins: grouped-surface-1/C-004"""
    frame = _kv_frame(spark)
    ops = frame.groupBy("id").cogroup(frame.groupBy("id"))
    result = ops.applyInArrow(
        lambda left, right: pa.table({"n": [left.num_rows + right.num_rows]}), "n long"
    )
    _assert_frame_cell(result.orderBy("n"), "cogroup_applyInArrow")
    keyed = ops.applyInArrow(
        lambda k, left, right: pa.table(
            {"id": [k[0].as_py()], "n": [left.num_rows + right.num_rows]}
        ),
        "id long, n long",
    )
    _assert_frame_cell(keyed.orderBy("id"), "cogroup_applyInArrow_key")


def test_cogroup_key_count_mismatch(spark: ReparkSession) -> None:
    """Unequal cogroup key counts raise Spark's require text. pins: grouped-surface-1/C-005"""
    frame = _kv_frame(spark)
    ops = frame.groupBy("id").cogroup(frame.groupBy("id", "v"))
    with pytest.raises(IllegalArgumentException) as raised:
        ops.applyInPandas(lambda left, right: left, "id long, v double")
    assert str(raised.value) == _ORACLE["cogroup_key_count_mismatch"]["error"]["message"]


def test_cogroup_never_collects_a_whole_side(spark: ReparkSession, monkeypatch: Any) -> None:
    """A 200k-row cogroup streams both sides; no whole-side collect/to_arrow.

    pins: grouped-surface-1/C-005
    """
    real_to_arrow = DataFrame.to_arrow
    real_collect = DataFrame.collect

    def guard_to_arrow(self: DataFrame) -> Any:
        if getattr(self, "_map_bridge", None) is None:
            raise AssertionError("whole-side to_arrow during cogroup")
        return real_to_arrow(self)

    def guard_collect(self: DataFrame) -> Any:
        if getattr(self, "_map_bridge", None) is None:
            raise AssertionError("whole-side collect during cogroup")
        return real_collect(self)

    monkeypatch.setattr(DataFrame, "to_arrow", guard_to_arrow)
    monkeypatch.setattr(DataFrame, "collect", guard_collect)
    left = spark.range(0, 200_000).select((F.col("id") % 3).alias("k"))
    right = spark.range(0, 200_000).select((F.col("id") % 3).alias("k"))
    out = (
        left.groupBy("k")
        .cogroup(right.groupBy("k"))
        .applyInArrow(
            lambda k, left_t, right_t: pa.table(
                {"k": [k[0].as_py()], "n": [left_t.num_rows + right_t.num_rows]}
            ),
            "k long, n long",
        )
    )
    rows = {row["k"]: row["n"] for row in out.to_arrow().to_pylist()}
    assert rows == {0: 133_334, 1: 133_334, 2: 133_332}


def test_state_api_refusals(spark: ReparkSession) -> None:
    """The three state APIs raise their declared refusals. pins: grouped-surface-1/C-006"""
    grouped = _kv_frame(spark).groupBy("id")
    with pytest.raises(UnsupportedOperationException) as state:
        grouped.applyInPandasWithState(
            lambda k, pdfs, st: iter([]), "id long", "c long", "Append", "NoTimeout"
        )
    cell = _ORACLE["applyInPandasWithState_batch"]["error"]
    assert type(state.value).__name__ == cell["raises"]
    assert state.value.getCondition() == cell["condition"]
    assert state.value.getMessageParameters() == cell["params"]
    assert str(state.value) == cell["message"]
    for method_name, cell_id, feature in [
        ("transformWithState", "transformWithState_batch", "transformWithState"),
        (
            "transformWithStateInPandas",
            "transformWithStateInPandas_batch",
            "transformWithStateInPandas",
        ),
    ]:
        assert _ORACLE[cell_id]["error"]["raises"] == "Py4JJavaError"
        with pytest.raises(PySparkNotImplementedError) as refused:
            getattr(grouped, method_name)(object(), "id long", "Append", "None")
        assert refused.value.getCondition() == "NOT_IMPLEMENTED"
        assert refused.value.getMessageParameters() == {"feature": feature}


def test_apply_in_pandas_still_answers(spark: ReparkSession) -> None:
    """applyInPandas is unchanged by the new bindings. pins: grouped-surface-1/C-008"""
    result = _kv_frame(spark).groupBy("id").applyInPandas(_gm, "id long, v double")
    _assert_frame_cell(result.orderBy("id", "v"), "applyInPandas_exists")


def test_apply_in_arrow_zero_column_result_drops_group(spark: ReparkSession) -> None:
    """L-001: a 0-column 0-row Arrow result contributes no rows for its group.

    Spark's verify_arrow_result accepts the empty shape and the group emits
    nothing; the later select must not run on it. pins: grouped-surface-1/C-009
    """
    frame = _kv_frame(spark)

    def drop_id1(table: pa.Table) -> pa.Table:
        if table.column("id")[0].as_py() == 1:
            return pa.table({})
        return table

    result = frame.groupBy("id").applyInArrow(drop_id1, "id long, v double")
    assert [tuple(row) for row in result.orderBy("id").collect()] == [(2, 3.0)]
    keyed = frame.groupBy("id").applyInArrow(
        lambda key, table: drop_id1(table), "id long, v double"
    )
    assert [tuple(row) for row in keyed.orderBy("id").collect()] == [(2, 3.0)]

    def drop_id1_iter(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        for batch in batches:
            if batch.column("id")[0].as_py() == 1:
                yield pa.RecordBatch.from_pydict({})
            else:
                yield batch

    iterated = frame.groupBy("id").applyInArrow(drop_id1_iter, "id long, v double")
    assert [tuple(row) for row in iterated.orderBy("id").collect()] == [(2, 3.0)]

    def drop_left1(left: pa.Table, right: pa.Table) -> pa.Table:
        if left.num_rows and left.column("id")[0].as_py() == 1:
            return pa.table({})
        return pa.table({"n": [left.num_rows + right.num_rows]})

    cogrouped = frame.groupBy("id").cogroup(frame.groupBy("id")).applyInArrow(drop_left1, "n long")
    assert [tuple(row) for row in cogrouped.collect()] == [(2,)]

    typed_empty = frame.groupBy("id").applyInArrow(
        lambda table: pa.table(
            {
                "id": pa.array([], type=pa.int64()),
                "v": pa.array([], type=pa.float64()),
            }
        ),
        "id long, v double",
    )
    assert typed_empty.collect() == []
    pandas_empty = frame.groupBy("id").applyInPandas(
        lambda pdf: pd.DataFrame() if pdf["id"].iloc[0] == 1 else pdf,
        "id long, v double",
    )
    assert [tuple(row) for row in pandas_empty.orderBy("id").collect()] == [(2, 3.0)]


def test_group_scan_row_key_is_per_boundary_not_per_row() -> None:
    """R-3: run boundaries come from pyarrow.compute, so the row-key helper runs
    once per contiguous run, never per row. pins: grouped-surface-1/C-009"""
    import repark.spark.dataframe.grouped_udf as grouped_udf

    def counted_scan(batches: list[pa.RecordBatch]) -> tuple[list[tuple[Any, int]], int]:
        calls = {"n": 0}
        real_row_key = grouped_udf._apply_in_pandas_row_key

        def counting_row_key(batch: Any, key_names: list[str], row_index: int) -> tuple[Any, ...]:
            calls["n"] += 1
            return real_row_key(batch, key_names, row_index)

        grouped_udf._apply_in_pandas_row_key = counting_row_key  # type: ignore[assignment]
        try:
            groups = [
                (key, sum(segment.num_rows for segment in segments))
                for key, segments in grouped_udf._iter_apply_in_pandas_keyed_groups(
                    iter(batches), ["k"]
                )
            ]
        finally:
            grouped_udf._apply_in_pandas_row_key = real_row_key  # type: ignore[assignment]
        return groups, calls["n"]

    one_group = pa.table(
        {
            "k": pa.array([7] * 1000, type=pa.int64()),
            "v": pa.array(range(1000), type=pa.int64()),
        }
    ).to_batches(max_chunksize=50)
    groups, calls = counted_scan(one_group)
    assert groups == [((7,), 1000)]
    assert calls <= len(one_group) + 1

    many = pa.table(
        {
            "k": pa.array([index // 2 for index in range(20_000)], type=pa.int64()),
            "v": pa.array(range(20_000), type=pa.int64()),
        }
    ).to_batches(max_chunksize=8192)
    groups, calls = counted_scan(many)
    assert len(groups) == 10_000
    assert all(count == 2 for _key, count in groups)
    assert calls <= 10_000 + len(many)

"""U6 R-APPLYINPANDAS: GroupedData.applyInPandas over the mapInArrow bridge."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pandas as pd
import pyarrow as pa
import pytest

from repark import SparkSession
from repark.errors import AnalysisException, PySparkException, PySparkTypeError
from repark.types import IntegerType, StringType, StructField, StructType


@pytest.fixture
def spark() -> Iterator[SparkSession]:
    session = SparkSession.builder.master("local[1]").appName("test-applyinpandas").getOrCreate()
    yield session
    session.stop()


def _sum_v(pdf: pd.DataFrame) -> pd.DataFrame:
    """Per-group sum of column ``v``; keep key column(s) when present."""
    keys = [name for name in pdf.columns if name != "v"]
    if not keys:
        return pd.DataFrame({"total": [int(pdf["v"].sum())]})
    out = pdf.groupby(keys, dropna=False, as_index=False)["v"].sum()
    return out.rename(columns={"v": "total"})


def _rows(table: pa.Table) -> list[dict[str, Any]]:
    return table.to_pylist()


def _multiset(rows: list[dict[str, Any]]) -> list[tuple[tuple[str, Any], ...]]:
    """Order-insensitive multiset key for Arrow row dicts (nulls + keys sorted)."""

    def cell(value: Any) -> Any:
        if value is None:
            return ("null",)
        if isinstance(value, float) and value != value:  # NaN
            return ("nan",)
        return ("v", value)

    packed = [tuple(sorted((key, cell(val)) for key, val in row.items())) for row in rows]
    return sorted(packed)


def test_applyinpandas_single_key_values(spark: SparkSession) -> None:
    frame = spark.createDataFrame(
        [(1, 10), (2, 20), (1, 30), (2, 5)],
        "k INT, v INT",
    )
    out = frame.groupBy("k").applyInPandas(_sum_v, "k INT, total INT")
    assert _multiset(_rows(out.to_arrow())) == _multiset(
        [{"k": 1, "total": 40}, {"k": 2, "total": 25}]
    )


def test_applyinpandas_multi_key_values(spark: SparkSession) -> None:
    frame = spark.createDataFrame(
        [
            (1, "a", 1),
            (1, "b", 2),
            (1, "a", 3),
            (2, "a", 4),
        ],
        "k INT, g STRING, v INT",
    )
    out = frame.groupBy("k", "g").applyInPandas(_sum_v, "k INT, g STRING, total INT")
    assert _multiset(_rows(out.to_arrow())) == _multiset(
        [
            {"k": 1, "g": "a", "total": 4},
            {"k": 1, "g": "b", "total": 2},
            {"k": 2, "g": "a", "total": 4},
        ]
    )


def test_applyinpandas_null_keys(spark: SparkSession) -> None:
    frame = spark.createDataFrame(
        [(None, 1), (1, 2), (None, 3), (1, 4)],
        "k INT, v INT",
    )

    def sum_null_safe(pdf: pd.DataFrame) -> pd.DataFrame:
        # Avoid pandas groupby float-upcast of null keys; group is already one key.
        key = pdf["k"].iloc[0]
        if pd.isna(key):
            key = None
        return pd.DataFrame({"k": [key], "total": [int(pdf["v"].sum())]})

    out = frame.groupBy("k").applyInPandas(sum_null_safe, "k INT, total INT")
    assert _multiset(_rows(out.to_arrow())) == _multiset(
        [{"k": None, "total": 4}, {"k": 1, "total": 6}]
    )


def test_applyinpandas_empty_input(spark: SparkSession) -> None:
    frame = spark.createDataFrame([], "k INT, v INT")
    calls: list[int] = []

    def tracked(pdf: pd.DataFrame) -> pd.DataFrame:
        calls.append(len(pdf))
        return _sum_v(pdf)

    out = frame.groupBy("k").applyInPandas(tracked, "k INT, total INT")
    assert _rows(out.to_arrow()) == []
    assert calls == []


def test_applyinpandas_global_groupby_one_group(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 10), (2, 20)], "k INT, v INT")

    def sum_all(pdf: pd.DataFrame) -> pd.DataFrame:
        # Global groupBy delivers the whole frame as one group (all columns present).
        return pd.DataFrame({"total": [int(pdf["v"].sum())]})

    out = frame.groupBy().applyInPandas(sum_all, "total INT")
    assert _rows(out.to_arrow()) == [{"total": 30}]


def test_applyinpandas_schema_structtype(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 5), (1, 7)], "k INT, v INT")
    schema = StructType(
        [
            StructField("k", IntegerType()),
            StructField("total", IntegerType()),
        ]
    )
    out = frame.groupBy("k").applyInPandas(_sum_v, schema)
    assert _multiset(_rows(out.to_arrow())) == _multiset([{"k": 1, "total": 12}])


def test_applyinpandas_schema_mismatch_loud(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1)], "k INT, v INT")

    def wrong_name(pdf: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"wrong": [1], "total": [int(pdf["v"].sum())]})

    out = frame.groupBy("k").applyInPandas(wrong_name, "k INT, total INT")
    with pytest.raises(PySparkException, match="schema mismatch"):
        out.collect()


def test_applyinpandas_schema_type_mismatch_loud(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1)], "k INT, v INT")

    def wrong_type(pdf: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"k": ["x"], "total": [1]})

    out = frame.groupBy("k").applyInPandas(wrong_type, "k INT, total INT")
    # Loud either via cast-to-schema conversion error (names the column) or mapInArrow
    # field-type validation — both are schema-mismatch class failures.
    with pytest.raises(
        PySparkException,
        match=r"schema mismatch|declared schema|Conversion failed",
    ):
        out.collect()


def test_applyinpandas_empty_wrong_columns_loud(spark: SparkSession) -> None:
    """Empty group result with wrong column names must not silently become [] (Spark parity).

    octo U6 C1: zero-row ``to_batches()→[]`` previously re-emitted the declared schema and
    swallowed RESULT_COLUMN_NAMES_MISMATCH-class errors.
    """
    frame = spark.createDataFrame([(1, 1), (2, 2)], "k INT, v INT")

    def empty_wrong(pdf: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"wrong": pd.Series(dtype="int32")})

    out = frame.groupBy("k").applyInPandas(empty_wrong, "k INT, total INT")
    with pytest.raises(PySparkException, match=r"schema mismatch.*Unexpected: wrong"):
        out.collect()


def test_applyinpandas_empty_partial_columns_loud(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1)], "k INT, v INT")

    def empty_partial(pdf: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"k": pd.Series(dtype="int32")})

    out = frame.groupBy("k").applyInPandas(empty_partial, "k INT, total INT")
    with pytest.raises(PySparkException, match=r"schema mismatch.*Missing: total"):
        out.collect()


def test_applyinpandas_extra_columns_loud(spark: SparkSession) -> None:
    """Spark rejects unexpected columns; do not silently drop via from_pandas(schema=)."""
    frame = spark.createDataFrame([(1, 1)], "k INT, v INT")

    def with_extra(pdf: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"k": [1], "total": [int(pdf["v"].sum())], "extra": [99]})

    out = frame.groupBy("k").applyInPandas(with_extra, "k INT, total INT")
    with pytest.raises(PySparkException, match=r"schema mismatch.*Unexpected: extra"):
        out.collect()


def test_applyinpandas_empty_zero_column_frame_ok(spark: SparkSession) -> None:
    """Spark accepts ``pd.DataFrame()`` (no columns) as an empty group result."""
    frame = spark.createDataFrame([(1, 1), (2, 2)], "k INT, v INT")

    def empty_none(_pdf: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame()

    out = frame.groupBy("k").applyInPandas(empty_none, "k INT, total INT")
    assert _rows(out.to_arrow()) == []


def test_applyinpandas_user_raise_surfaces(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1)], "k INT, v INT")

    def boom(pdf: pd.DataFrame) -> pd.DataFrame:
        raise RuntimeError("user boom apply")

    out = frame.groupBy("k").applyInPandas(boom, "k INT, total INT")
    with pytest.raises(PySparkException, match="user boom apply") as caught:
        out.collect()
    text = str(caught.value)
    assert "Traceback" in text
    assert "user boom apply" in text
    # Chained cause preserved for debuggers; KeyboardInterrupt must not be wrapped
    # (separate pin) — Exception subclass surfaces as PySparkException + cause.
    assert isinstance(caught.value.__cause__, RuntimeError)


def test_applyinpandas_keyboard_interrupt_not_wrapped(spark: SparkSession) -> None:
    """BaseException (KeyboardInterrupt) must not be swallowed into PySparkException."""
    frame = spark.createDataFrame([(1, 1)], "k INT, v INT")

    def interrupt(_pdf: pd.DataFrame) -> pd.DataFrame:
        raise KeyboardInterrupt("stop-apply")

    out = frame.groupBy("k").applyInPandas(interrupt, "k INT, total INT")
    with pytest.raises(KeyboardInterrupt, match="stop-apply"):
        out.collect()


def test_applyinpandas_none_return_loud(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1)], "k INT, v INT")

    def returns_none(pdf: pd.DataFrame) -> Any:
        return None

    out = frame.groupBy("k").applyInPandas(returns_none, "k INT, total INT")
    with pytest.raises(PySparkException, match=r"must return a pandas\.DataFrame"):
        out.collect()


def test_applyinpandas_non_callable_loud(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1)], "k INT, v INT")
    with pytest.raises(PySparkTypeError, match="callable"):
        frame.groupBy("k").applyInPandas("not-callable", "k INT, total INT")  # type: ignore[arg-type]


def test_applyinpandas_lazy_until_action(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1)], "k INT, v INT")
    calls = 0

    def counted(pdf: pd.DataFrame) -> pd.DataFrame:
        nonlocal calls
        calls += 1
        return _sum_v(pdf)

    out = frame.groupBy("k").applyInPandas(counted, "k INT, total INT")
    assert calls == 0
    _ = out.schema  # schema-only placeholder; must not run func
    assert calls == 0
    _ = out.columns
    assert calls == 0
    out.collect()
    assert calls == 1
    out.collect()
    assert calls == 2  # re-run unless cached


def test_applyinpandas_cache_pins_once(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1), (1, 2)], "k INT, v INT")
    calls = 0

    def counted(pdf: pd.DataFrame) -> pd.DataFrame:
        nonlocal calls
        calls += 1
        return _sum_v(pdf)

    out = frame.groupBy("k").applyInPandas(counted, "k INT, total INT").cache()
    out.collect()
    out.collect()
    assert calls == 1


def test_applyinpandas_expression_group_key_refused(spark: SparkSession) -> None:
    from repark import functions as functions_module

    frame = spark.createDataFrame([(1, 1), (2, 2)], "k INT, v INT")
    with pytest.raises(AnalysisException, match="simple column-name group keys"):
        frame.groupBy(functions_module.col("k") + 1).applyInPandas(_sum_v, "k INT, total INT")


def test_applyinpandas_cube_refused(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1)], "k INT, v INT")
    with pytest.raises(AnalysisException, match="cube/rollup"):
        frame.cube("k").applyInPandas(_sum_v, "k INT, total INT")


def test_applyinpandas_pivot_refused(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, "a", 1)], "k INT, p STRING, v INT")
    with pytest.raises(AnalysisException, match="pivot"):
        frame.groupBy("k").pivot("p").applyInPandas(_sum_v, "k INT, total INT")


def test_applyinpandas_boundary_stitch_multi_batch(spark: SparkSession) -> None:
    """Same group split across two Arrow batches must still call func once (stitch).

    Drives the facade boundary-stitch path by feeding a sorted multi-batch stream
    through the private group iterator (same helper the bridge uses).
    """
    from repark.dataframe import _iter_apply_in_pandas_group_tables

    # Two batches, group k=1 straddles the edge; k=2 only in batch 2.
    batch_a = pa.RecordBatch.from_pydict({"k": [1, 1], "v": [10, 20]})
    batch_b = pa.RecordBatch.from_pydict({"k": [1, 2], "v": [30, 40]})
    groups = list(_iter_apply_in_pandas_group_tables(iter([batch_a, batch_b]), ["k"]))
    assert len(groups) == 2
    assert groups[0].to_pylist() == [
        {"k": 1, "v": 10},
        {"k": 1, "v": 20},
        {"k": 1, "v": 30},
    ]
    assert groups[1].to_pylist() == [{"k": 2, "v": 40}]


def test_applyinpandas_boundary_stitch_null_type_promote(spark: SparkSession) -> None:
    """Stitch when an all-null string key segment infers Arrow null vs string (octo U6 C2)."""
    from repark.dataframe import _iter_apply_in_pandas_group_tables

    # from_pydict infers g:null in batch_a and g:string in batch_b — from_batches alone fails.
    batch_a = pa.RecordBatch.from_pydict({"k": [1, 1], "g": [None, None], "v": [10, 20]})
    batch_b = pa.RecordBatch.from_pydict({"k": [1, 2], "g": [None, "x"], "v": [30, 40]})
    groups = list(_iter_apply_in_pandas_group_tables(iter([batch_a, batch_b]), ["k", "g"]))
    assert len(groups) == 2
    assert groups[0].to_pylist() == [
        {"k": 1, "g": None, "v": 10},
        {"k": 1, "g": None, "v": 20},
        {"k": 1, "g": None, "v": 30},
    ]
    assert groups[1].to_pylist() == [{"k": 2, "g": "x", "v": 40}]


def test_applyinpandas_engine_sort_key_contiguous_stream(spark: SparkSession) -> None:
    """orderBy(keys) + Arrow stream must deliver key-contiguous batches (seam pin)."""
    frame = spark.createDataFrame(
        [(2, 1), (1, 2), (2, 3), (1, 4), (3, 5)],
        "k INT, v INT",
    )
    sorted_frame = frame.orderBy("k")
    reader = pa.RecordBatchReader.from_stream(sorted_frame)
    closed_keys: set[int] = set()
    current: int | None = None
    for batch in reader:
        for value in batch.column("k").to_pylist():
            assert value is not None
            if current is None:
                current = int(value)
            elif int(value) != current:
                closed_keys.add(current)
                # Once a key is left it must not reappear (engine sort contiguity).
                assert int(value) not in closed_keys
                current = int(value)
    assert closed_keys | ({current} if current is not None else set()) == {1, 2, 3}


def test_applyinpandas_e2e_multi_batch_group_calls_once(spark: SparkSession) -> None:
    """Group spanning multiple engine Arrow batches must invoke func once (stitch e2e).

    Engine stream chunk size is 8192 rows; >8192 rows for one key forces a multi-batch
    group through the real orderBy + mapInArrow bridge (not only the unit iterator pin).
    """
    large_group_rows = 20_000  # > 8192 → at least 3 batches for k=1
    rows = [(1, index) for index in range(large_group_rows)] + [(2, 0), (2, 1)]
    frame = spark.createDataFrame(rows, "k INT, v INT")
    # Sanity: sorted stream is multi-batch for this size.
    batch_count = sum(1 for _ in pa.RecordBatchReader.from_stream(frame.orderBy("k")))
    assert batch_count >= 3

    calls: list[int] = []

    def track(pdf: pd.DataFrame) -> pd.DataFrame:
        calls.append(len(pdf))
        return pd.DataFrame({"k": [int(pdf["k"].iloc[0])], "n": [len(pdf)]})

    out_rows = _rows(frame.groupBy("k").applyInPandas(track, "k INT, n INT").to_arrow())
    assert _multiset(out_rows) == _multiset([{"k": 1, "n": large_group_rows}, {"k": 2, "n": 2}])
    assert sorted(calls) == [2, large_group_rows]


def test_applyinpandas_schema_cast_overflow_names_column(spark: SparkSession) -> None:
    """Overflow on a value column must name the conversion failure (not a sibling field)."""
    frame = spark.createDataFrame([(1, 1)], "k INT, v INT")

    def overflow_total(_pdf: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"k": [1], "total": [2**40]})

    out = frame.groupBy("k").applyInPandas(overflow_total, "k INT, total INT")
    with pytest.raises(PySparkException, match=r"declared schema|Conversion failed|not in range"):
        out.collect()


def test_applyinpandas_empty_group_result_schema_ok(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1), (2, 2)], "k INT, v INT")

    def drop_all(pdf: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"k": pd.Series(dtype="int32"), "total": pd.Series(dtype="int32")})

    out = frame.groupBy("k").applyInPandas(drop_all, "k INT, total INT")
    assert _rows(out.to_arrow()) == []


def test_applyinpandas_snake_alias(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1)], "k INT, v INT")
    out = frame.groupBy("k").apply_in_pandas(_sum_v, "k INT, total INT")
    assert _multiset(_rows(out.to_arrow())) == _multiset([{"k": 1, "total": 1}])


def test_applyinpandas_string_schema_types(spark: SparkSession) -> None:
    frame = spark.createDataFrame([("a", 1), ("b", 2), ("a", 3)], "g STRING, v INT")

    def keep(pdf: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "g": [pdf["g"].iloc[0]],
                "n": [len(pdf)],
                "label": [str(pdf["g"].iloc[0])],
            }
        )

    schema = StructType(
        [
            StructField("g", StringType()),
            StructField("n", IntegerType()),
            StructField("label", StringType()),
        ]
    )
    out = frame.groupBy("g").applyInPandas(keep, schema)
    assert _multiset(_rows(out.to_arrow())) == _multiset(
        [
            {"g": "a", "n": 2, "label": "a"},
            {"g": "b", "n": 1, "label": "b"},
        ]
    )


def test_applyinpandas_multi_key_null_and_empty_string(spark: SparkSession) -> None:
    """Null and empty-string group keys are distinct; multi-key null combos group correctly."""
    frame = spark.createDataFrame(
        [
            (1, "", 1),
            (1, "", 2),
            (1, None, 3),
            (None, "", 4),
            (None, None, 5),
            (None, None, 6),
        ],
        "k INT, g STRING, v INT",
    )

    def summarize(pdf: pd.DataFrame) -> pd.DataFrame:
        key = pdf["k"].iloc[0]
        group = pdf["g"].iloc[0]
        if pd.isna(key):
            key = None
        if pd.isna(group):
            group = None
        return pd.DataFrame(
            {
                "k": [key],
                "g": [group],
                "n": [len(pdf)],
                "total": [int(pdf["v"].sum())],
            }
        )

    out = frame.groupBy("k", "g").applyInPandas(summarize, "k INT, g STRING, n INT, total INT")
    assert _multiset(_rows(out.to_arrow())) == _multiset(
        [
            {"k": 1, "g": "", "n": 2, "total": 3},
            {"k": 1, "g": None, "n": 1, "total": 3},
            {"k": None, "g": "", "n": 1, "total": 4},
            {"k": None, "g": None, "n": 2, "total": 11},
        ]
    )


def test_applyinpandas_boundary_stitch_skips_empty_batches(spark: SparkSession) -> None:
    """Empty RecordBatches between segments must not break boundary stitch."""
    from repark.dataframe import _iter_apply_in_pandas_group_tables

    schema = pa.schema([("k", pa.int32()), ("v", pa.int32())])
    batch_a = pa.RecordBatch.from_arrays(
        [pa.array([1], type=pa.int32()), pa.array([10], type=pa.int32())],
        schema=schema,
    )
    empty = pa.RecordBatch.from_arrays(
        [pa.array([], type=pa.int32()), pa.array([], type=pa.int32())],
        schema=schema,
    )
    batch_b = pa.RecordBatch.from_arrays(
        [pa.array([1, 2], type=pa.int32()), pa.array([20, 30], type=pa.int32())],
        schema=schema,
    )
    groups = list(_iter_apply_in_pandas_group_tables(iter([batch_a, empty, batch_b]), ["k"]))
    assert len(groups) == 2
    assert groups[0].to_pylist() == [{"k": 1, "v": 10}, {"k": 1, "v": 20}]
    assert groups[1].to_pylist() == [{"k": 2, "v": 30}]

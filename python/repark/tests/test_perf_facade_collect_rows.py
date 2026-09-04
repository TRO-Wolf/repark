"""PERF-FACADE-COLLECT-1 — the native row fast path equals the Python converter, cell for cell.

Oracle: the pre-existing Python converter, kept callable as
``rows_export.rows_from_arrow_table_python``. Every case compares BOTH value equality and
``repr`` so a same-valued but differently-typed cell (Decimal scale, float for int) is red.
"""

from __future__ import annotations

import datetime
import decimal
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark.dataframe.rows_export import (
    rows_from_arrow_table,
    rows_from_arrow_table_python,
)
from repark.spark.row import Row


@pytest.fixture
def spark(tmp_path: Path) -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-perf-facade-collect").getOrCreate()
    session.createDataFrame([(index,) for index in range(40)], ["id"]).createOrReplaceTempView(
        "nums"
    )
    yield session
    session.stop()


def _cell_signature(value: object) -> tuple[str, str]:
    return (type(value).__name__, repr(value))


def _row_signature(row: Row) -> tuple[tuple[str, ...], tuple[tuple[str, str], ...]]:
    return (tuple(row.__fields__), tuple(_cell_signature(value) for value in row))


def _assert_converters_agree(batch: pa.RecordBatch) -> list[Row]:
    fast = rows_from_arrow_table(batch)
    reference = rows_from_arrow_table_python(batch)
    assert len(fast) == len(reference)
    assert [_row_signature(row) for row in fast] == [_row_signature(row) for row in reference]
    assert fast == reference
    return fast


def _scalar_matrix_batch() -> pa.RecordBatch:
    return pa.record_batch(
        {
            "i8": pa.array([-128, 0, None], type=pa.int8()),
            "i16": pa.array([-32768, 7, None], type=pa.int16()),
            "i32": pa.array([-2147483648, 7, None], type=pa.int32()),
            "i64": pa.array([-9223372036854775808, 7, None], type=pa.int64()),
            "u8": pa.array([255, 0, None], type=pa.uint8()),
            "u16": pa.array([65535, 0, None], type=pa.uint16()),
            "u32": pa.array([4294967295, 0, None], type=pa.uint32()),
            "u64": pa.array([18446744073709551615, 0, None], type=pa.uint64()),
            "f32": pa.array([0.1, float("inf"), None], type=pa.float32()),
            "f64": pa.array([0.1, float("-inf"), None], type=pa.float64()),
            "b": pa.array([True, False, None], type=pa.bool_()),
            "s": pa.array(["héllo wörld", "", None], type=pa.string()),
            "ls": pa.array(["\U0001f600 emoji", "\x00nul", None], type=pa.large_string()),
            "sv": pa.array(
                ["a longer string than sixteen bytes", "x", None], type=pa.string_view()
            ),
            "bin": pa.array([b"\x00\xff", b"", None], type=pa.binary()),
            "lbin": pa.array([b"\x01\x02\x03", b"", None], type=pa.large_binary()),
            "binv": pa.array([b"a" * 40, b"", None], type=pa.binary_view()),
            "nul": pa.array([None, None, None], type=pa.null()),
        }
    )


def test_scalar_type_matrix_matches_the_python_converter() -> None:
    """Every natively converted Arrow type equals the Python converter by value and repr."""
    rows = _assert_converters_agree(_scalar_matrix_batch())
    assert len(rows) == 3
    assert rows[0].f32 == pytest.approx(0.10000000149011612)
    assert rows[0].i64 == -9223372036854775808
    assert rows[2].nul is None


def test_delegated_type_matrix_matches_the_python_converter() -> None:
    """Types the binding declines (decimal, date, time, timestamp, nested) keep exact semantics."""
    batch = pa.record_batch(
        {
            "dec2": pa.array([decimal.Decimal("1.23"), None], type=pa.decimal128(9, 2)),
            "dec6": pa.array([decimal.Decimal("1.230000"), None], type=pa.decimal128(20, 6)),
            "dec0": pa.array([decimal.Decimal("7"), None], type=pa.decimal128(3, 0)),
            "d32": pa.array([datetime.date(2022, 1, 8), None], type=pa.date32()),
            "t64": pa.array([datetime.time(1, 1, 1, 1), None], type=pa.time64("us")),
            "ts": pa.array([datetime.datetime(2020, 1, 1, 2, 3, 4), None], type=pa.timestamp("us")),
            "arr": pa.array([[1, None, 3], None], type=pa.list_(pa.int64())),
            "st": pa.array([{"a": 1, "b": "x"}, None]),
            "i64": pa.array([5, None], type=pa.int64()),
        }
    )
    rows = _assert_converters_agree(batch)
    assert repr(rows[0].dec6) == "Decimal('1.230000')"
    assert rows[0].d32 == datetime.date(2022, 1, 8)
    assert rows[0].arr == [1, None, 3]


def test_map_and_tz_timestamp_still_convert_through_python() -> None:
    """Map cells become dicts and tz-aware timestamps keep the session-wall conversion."""
    batch = pa.record_batch(
        {
            "m": pa.array(
                [[("k", 1), ("é", None)], [], None], type=pa.map_(pa.string(), pa.int32())
            ),
            "tz": pa.array(
                [datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC), None, None],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "s": pa.array(["a", "b", None], type=pa.string()),
        }
    )
    rows = _assert_converters_agree(batch)
    assert rows[0].m == {"k": 1, "é": None}
    assert rows[1].m == {}


INTERVAL_BATCHES: tuple[tuple[str, pa.RecordBatch], ...] = (
    (
        "top_level",
        pa.record_batch(
            {
                "iv": pa.array([(1, 2, 3), None], type=pa.month_day_nano_interval()),
                "i64": pa.array([1, None], type=pa.int64()),
            }
        ),
    ),
    (
        "inside_list",
        pa.record_batch(
            {
                "ivs": pa.array([[(1, 2, 3)], None], type=pa.list_(pa.month_day_nano_interval())),
                "i64": pa.array([1, None], type=pa.int64()),
            }
        ),
    ),
    (
        "inside_struct",
        pa.record_batch(
            {
                "st": pa.array(
                    [{"iv": (1, 2, 3)}, None],
                    type=pa.struct([("iv", pa.month_day_nano_interval())]),
                ),
                "i64": pa.array([1, None], type=pa.int64()),
            }
        ),
    ),
)


@pytest.mark.parametrize(
    ("label", "batch"), INTERVAL_BATCHES, ids=[name for name, _ in INTERVAL_BATCHES]
)
def test_calendar_interval_refusal_survives_the_fast_path(
    label: str, batch: pa.RecordBatch
) -> None:
    """A calendar interval refuses whether it is a column, a list element or a struct field."""
    assert label
    with pytest.raises(Exception, match=r"(?i)interval"):
        rows_from_arrow_table(batch)
    with pytest.raises(Exception, match=r"(?i)interval"):
        rows_from_arrow_table_python(batch)


def test_duplicate_display_names_and_zero_columns_keep_their_shape() -> None:
    """Duplicate names survive positionally and a zero-column batch yields empty-value rows."""
    duplicated = pa.record_batch([pa.array([1, 2]), pa.array([3, 4])], names=["x", "x"])
    rows = _assert_converters_agree(duplicated)
    assert [tuple(row) for row in rows] == [(1, 3), (2, 4)]
    assert rows[0].__fields__ == ["x", "x"]
    empty_columns = pa.record_batch([], names=[]).slice(0, 0)
    assert rows_from_arrow_table(empty_columns) == rows_from_arrow_table_python(empty_columns)


def test_zero_row_batch_returns_no_rows() -> None:
    """An empty batch of typed columns yields no rows on either converter."""
    batch = _scalar_matrix_batch().slice(0, 0)
    assert rows_from_arrow_table(batch) == []
    assert rows_from_arrow_table_python(batch) == []


def test_native_fast_path_is_actually_taken_for_a_scalar_batch() -> None:
    """The binding converts the scalar matrix itself rather than declining to Python."""
    from repark import _native

    batch = _scalar_matrix_batch()
    assert _native.rows_from_record_batch(batch, {}) is not None
    nested = pa.record_batch({"arr": pa.array([[1], None], type=pa.list_(pa.int64()))})
    assert _native.rows_from_record_batch(nested, {}) is None


def test_collect_over_a_wide_session_frame_matches_the_python_converter(
    spark: ReparkSession,
) -> None:
    """End to end: ``collect()`` on a mixed frame equals the Python converter on its Arrow."""
    frame = spark.sql(
        "SELECT id, CAST(id AS DOUBLE) AS d, CAST(id AS STRING) AS s, "
        "CAST(id AS DECIMAL(10,3)) AS dec, id % 2 = 0 AS flag, "
        "CAST(NULL AS STRING) AS n, array(id, id + 1) AS arr "
        "FROM nums"
    )
    collected = frame.collect()
    reference = rows_from_arrow_table_python(frame.to_arrow())
    assert [_row_signature(row) for row in collected] == [_row_signature(row) for row in reference]
    assert collected == reference
    assert collected[0].__fields__ == ["id", "d", "s", "dec", "flag", "n", "arr"]


def test_collect_leaves_the_cyclic_collector_enabled(spark: ReparkSession) -> None:
    """The bulk-materialization GC guard restores the collector it found."""
    import gc

    assert gc.isenabled()
    spark.sql("SELECT id FROM nums LIMIT 8").collect()
    assert gc.isenabled()

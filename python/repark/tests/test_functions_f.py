"""FN-F — try / session / bitwise facade wrappers (value + Arrow type).

Each shipped ``functions`` name is pinned through ``ReparkSession`` on the Arrow
path (``to_arrow()``): value AND type. ``uuid`` pins type + uniqueness, not a
golden value. ``version`` is the repark string (not DataFusion ``version()``).

Deferred this batch (no stubs): charter try_* / to_number / to_binary;
camelCase ``shiftLeft`` / ``shiftRight`` / ``shiftRightUnsigned`` (PySpark
``__all__`` is snake_case; FN-GT1 shipped the snake names);
``assert_true`` — ``raise_error`` is construction-time UOE, not an evaluable Column.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import PySparkTypeError
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-fn-f").getOrCreate()
    yield session
    session.stop()


def _table(frame: object) -> pa.Table:
    return frame.to_arrow()  # type: ignore[attr-defined]


def _is_string(field_type: pa.DataType) -> bool:
    return pa.types.is_string(field_type) or pa.types.is_large_string(field_type)


# ==================================================================================================
# Bitwise
# ==================================================================================================


def test_bitwise_not_and_alias(spark: ReparkSession) -> None:
    assert callable(F.bitwiseNOT)
    frame = spark.createDataFrame([(5,), (-8,), (0,), (None,)], ["x"])
    table = _table(
        frame.select(
            F.bitwise_not("x").alias("n"),
            F.bitwiseNOT("x").alias("a"),
        )
    )
    assert table.column("n").to_pylist() == [-6, 7, -1, None]
    assert table.column("a").to_pylist() == table.column("n").to_pylist()
    assert table.schema.field("n").type == table.schema.field("a").type
    assert pa.types.is_int64(table.schema.field("n").type)


# ==================================================================================================
# Broadcast hint
# ==================================================================================================


def test_broadcast_dataframe_and_column_are_identity(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "s"])
    hinted = F.broadcast(frame)
    base = _table(frame)
    table = _table(hinted)
    assert table.to_pylist() == base.to_pylist()
    assert table.schema == base.schema
    selected = _table(
        frame.select(F.broadcast("id").alias("id"), F.broadcast(F.col("s")).alias("s"))
    )
    assert selected.to_pylist() == base.to_pylist()
    assert pa.types.is_integer(selected.schema.field("id").type)
    assert _is_string(selected.schema.field("s").type)
    with pytest.raises(PySparkTypeError, match="NOT_COLUMN_OR_STR"):
        F.broadcast(123)


# ==================================================================================================
# Session strings
# ==================================================================================================


def test_current_user_and_user_are_stable_repark_string(spark: ReparkSession) -> None:
    assert callable(F.user)
    first = _table(spark.range(1).select(F.current_user().alias("u")))
    second = _table(spark.range(1).select(F.user().alias("u")))
    assert first.column("u").to_pylist() == ["repark"]
    assert second.column("u").to_pylist() == first.column("u").to_pylist()
    assert _is_string(first.schema.field("u").type)
    assert _is_string(second.schema.field("u").type)


def test_current_catalog_database_schema_track_session(
    spark: ReparkSession, tmp_path: Path
) -> None:
    table = _table(
        spark.range(1).select(
            F.current_catalog().alias("c"),
            F.current_database().alias("d"),
            F.current_schema().alias("s"),
        )
    )
    assert table.column("c").to_pylist() == [spark.catalog.currentCatalog()]
    assert table.column("d").to_pylist() == [spark.catalog.currentDatabase()]
    assert table.column("s").to_pylist() == table.column("d").to_pylist()
    assert _is_string(table.schema.field("c").type)
    assert _is_string(table.schema.field("d").type)
    assert _is_string(table.schema.field("s").type)

    spark.register_memory_catalog("fn_f_catalog", tmp_path)
    spark.create_namespace("fn_f_catalog", "other_ns")
    spark.catalog.setCurrentCatalog("fn_f_catalog")
    spark.catalog.setCurrentDatabase("other_ns")
    after = _table(
        spark.range(1).select(
            F.current_catalog().alias("c"),
            F.current_database().alias("d"),
            F.current_schema().alias("s"),
        )
    )
    assert after.column("c").to_pylist() == ["fn_f_catalog"]
    assert after.column("d").to_pylist() == ["other_ns"]
    assert after.column("s").to_pylist() == ["other_ns"]
    assert _is_string(after.schema.field("c").type)


def test_version_is_repark_string_not_datafusion(spark: ReparkSession) -> None:
    table = _table(
        spark.createDataFrame([(1,), (2,)], ["x"]).select(
            F.sum("x").alias("s"),
            F.version().alias("v"),
        )
    )
    assert table.column("v").to_pylist() == [spark.version]
    assert table.column("v").to_pylist()[0].startswith("repark-")
    assert "DataFusion" not in table.column("v").to_pylist()[0]
    assert _is_string(table.schema.field("v").type)
    assert not pa.types.is_decimal(table.schema.field("v").type)
    assert table.column("s").to_pylist() == [3]


# ==================================================================================================
# uuid (non-deterministic)
# ==================================================================================================


def test_uuid_type_and_uniqueness(spark: ReparkSession) -> None:
    table = _table(spark.range(8).select(F.uuid().alias("u")))
    values = table.column("u").to_pylist()
    assert len(values) == 8
    assert None not in values
    assert len(set(values)) == 8
    assert _is_string(table.schema.field("u").type)


# ==================================================================================================
# Deferred names stay absent (no stubs)
# ==================================================================================================


@pytest.mark.parametrize(
    "name",
    [
        "assert_true",
        "shiftLeft",
        "shiftRight",
        "shiftRightUnsigned",
        "try_sum",
        "try_add",
        "to_number",
        "to_binary",
    ],
)
def test_deferred_fn_f_names_are_absent(name: str) -> None:
    assert not hasattr(F, name)

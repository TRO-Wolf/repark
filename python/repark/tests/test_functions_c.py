"""FN-C — aggregate aliases / shims (value + Arrow type).

Each new ``functions`` name is pinned through ``ReparkSession`` on the Arrow path
(``to_arrow()``): value AND type. Alias names resolve and share a behavior case
with their canonical.

Deferred this batch (no stubs): FN-W ships ``lag`` / ``lead`` / ``nth_value`` /
``percent_rank`` / ``cume_dist``; remaining A8 residuals stay absent:
``sum_distinct`` / ``sumDistinct`` (**not a kernel** — DataFusion spells it
``sum(DISTINCT x)``, a modifier on the call, so it needs the DISTINCT path);
charter ENGINE-WORK ``any_value``, ``max_by``, ``min_by``, ``product``,
``grouping_id``, ``percentile``, ``window``, ``window_time``, ``session_window``.

FNP-5 (2026-08-20) shipped ``grouping`` and ``approx_count_distinct`` out of that
list — both were already registered and reachable through ``spark.sql(...)``, and
only the facade's aggregate dispatch lacked an arm.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-fn-c").getOrCreate()
    yield session
    session.stop()


def _table(frame: object) -> pa.Table:
    return frame.to_arrow()  # type: ignore[attr-defined]


# ==================================================================================================
# Aliases: resolve + one behavior case vs canonical
# ==================================================================================================


def test_first_value_alias_of_first(spark: ReparkSession) -> None:
    assert callable(F.first_value)
    frame = spark.createDataFrame([(None,), (7,), (None,)], ["x"])
    table = _table(
        frame.agg(
            F.first("x", ignorenulls=True).alias("f"),
            F.first_value("x", ignorenulls=True).alias("fv"),
        )
    )
    assert table.column("f").to_pylist() == table.column("fv").to_pylist() == [7]
    assert table.schema.field("f").type == table.schema.field("fv").type
    assert pa.types.is_integer(table.schema.field("fv").type)


def test_last_value_alias_of_last(spark: ReparkSession) -> None:
    assert callable(F.last_value)
    frame = spark.createDataFrame([(None,), (7,), (None,)], ["x"])
    table = _table(
        frame.agg(
            F.last("x", ignorenulls=True).alias("l"),
            F.last_value("x", ignorenulls=True).alias("lv"),
        )
    )
    assert table.column("l").to_pylist() == table.column("lv").to_pylist() == [7]
    assert table.schema.field("l").type == table.schema.field("lv").type
    assert pa.types.is_integer(table.schema.field("lv").type)


def test_std_alias_of_stddev(spark: ReparkSession) -> None:
    assert callable(F.std)
    frame = spark.createDataFrame([(1.0,), (2.0,), (3.0,)], ["x"])
    table = _table(frame.agg(F.std("x").alias("s"), F.stddev("x").alias("d")))
    std_value = table.column("s").to_pylist()[0]
    stddev_value = table.column("d").to_pylist()[0]
    assert abs(float(std_value) - float(stddev_value)) < 1e-12
    assert abs(float(std_value) - 1.0) < 1e-9
    assert table.schema.field("s").type == table.schema.field("d").type
    assert pa.types.is_floating(table.schema.field("s").type)


def test_every_alias_of_bool_and(spark: ReparkSession) -> None:
    assert callable(F.every)
    frame = spark.createDataFrame([(True,), (True,), (None,)], ["flag"])
    table = _table(
        frame.agg(
            F.bool_and("flag").alias("a"),
            F.every("flag").alias("e"),
            F.min("flag").alias("m"),
        )
    )
    assert table.column("a").to_pylist() == [True]
    assert table.column("e").to_pylist() == [True]
    assert table.column("m").to_pylist() == [True]
    assert pa.types.is_boolean(table.schema.field("a").type)
    assert table.schema.field("e").type == table.schema.field("a").type


def test_some_alias_of_bool_or(spark: ReparkSession) -> None:
    assert callable(F.some)
    frame = spark.createDataFrame([(False,), (False,), (None,)], ["flag"])
    table = _table(
        frame.agg(
            F.bool_or("flag").alias("o"),
            F.some("flag").alias("s"),
            F.max("flag").alias("m"),
        )
    )
    assert table.column("o").to_pylist() == [False]
    assert table.column("s").to_pylist() == [False]
    assert table.column("m").to_pylist() == [False]
    assert pa.types.is_boolean(table.schema.field("o").type)
    assert table.schema.field("s").type == table.schema.field("o").type


# ==================================================================================================
# SHIMs
# ==================================================================================================


def test_count_if_counts_true_only(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(True,), (False,), (True,), (None,)], ["flag"])
    table = _table(frame.agg(F.count_if("flag").alias("c")))
    assert table.column("c").to_pylist() == [2]
    assert pa.types.is_integer(table.schema.field("c").type)
    assert table.schema.field("c").nullable is False


def test_count_if_accepts_a_predicate_column(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1,), (0,), (3,), (None,)], ["x"])
    table = _table(frame.agg(F.count_if(F.col("x") > 0).alias("c")))
    assert table.column("c").to_pylist() == [2]
    assert pa.types.is_integer(table.schema.field("c").type)


def test_count_if_grouped_and_select_path(spark: ReparkSession) -> None:
    frame = spark.createDataFrame(
        [(1, True), (1, False), (1, True), (2, False)],
        ["g", "flag"],
    )
    grouped = _table(frame.groupBy("g").agg(F.count_if("flag").alias("c")).orderBy("g"))
    assert grouped.column("g").to_pylist() == [1, 2]
    assert grouped.column("c").to_pylist() == [2, 0]
    selected = _table(frame.select(F.count_if("flag").alias("c")))
    assert selected.column("c").to_pylist() == [2]


def test_bool_and_is_false_when_any_false(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(True,), (False,), (True,)], ["flag"])
    table = _table(frame.agg(F.bool_and("flag").alias("a"), F.min("flag").alias("m")))
    assert table.column("a").to_pylist() == [False]
    assert table.column("m").to_pylist() == [False]
    assert pa.types.is_boolean(table.schema.field("a").type)


def test_bool_or_is_true_when_any_true(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(False,), (True,), (False,)], ["flag"])
    table = _table(frame.agg(F.bool_or("flag").alias("o"), F.max("flag").alias("m")))
    assert table.column("o").to_pylist() == [True]
    assert table.column("m").to_pylist() == [True]
    assert pa.types.is_boolean(table.schema.field("o").type)


def test_count_if_empty_group_is_zero(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([], "flag BOOLEAN")
    table = _table(frame.agg(F.count_if("flag").alias("c")))
    assert table.column("c").to_pylist() == [0]
    assert pa.types.is_integer(table.schema.field("c").type)


def test_bool_and_empty_group_is_null(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([], "flag BOOLEAN")
    table = _table(frame.agg(F.bool_and("flag").alias("a"), F.min("flag").alias("m")))
    assert table.column("a").to_pylist() == [None]
    assert table.column("m").to_pylist() == [None]
    assert pa.types.is_boolean(table.schema.field("a").type)


def test_fn_c_deferred_names_are_absent() -> None:
    """No stubs for the A8 residuals and charter ENGINE-WORK still outstanding.

    The list RATCHETS DOWN. FNP-5 (2026-08-20) shipped ``grouping`` and
    ``approx_count_distinct`` — both were already in
    ``all_default_aggregate_functions()`` and reachable through ``spark.sql(...)``; only the
    facade's aggregate dispatch had no arm. Behaviour: ``test_fnp5_aggregates.py``.

    ``sum_distinct`` stays here for a reason worth recording: it is **not a kernel**. DataFusion
    spells it ``sum(DISTINCT x)``, a modifier on the aggregate call, so it needs the facade's
    DISTINCT path rather than a dispatch arm. The camelCase spellings are PySpark's deprecated
    aliases and follow their snake_case originals.
    """
    deferred = (
        "sum_distinct",
        "sumDistinct",
        "approxCountDistinct",
        "any_value",
        "max_by",
        "min_by",
        "product",
        "grouping_id",
        "percentile",
        "window",
        "window_time",
        "session_window",
    )
    present = [name for name in deferred if hasattr(F, name)]
    assert present == []

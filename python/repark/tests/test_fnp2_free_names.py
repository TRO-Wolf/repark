"""FNP-2 — the names that needed no engine work, and the null-ordering corners they closed.

**The asymmetry.** RePark exported ``asc_nulls_first`` and ``desc_nulls_last`` but not
``asc_nulls_last`` or ``desc_nulls_first`` — two corners of a 2x2, and precisely the two that are
not the default. A user reaching for the non-default null placement got ``AttributeError``, which
reads as "repark cannot order nulls that way" rather than "this spelling is missing".

Both spellings existed only as aliases of ``asc`` / ``desc``, so nothing pinned that the *nulls*
half of the name meant anything. These rows pin all four corners by observed row order, not by
which method was called.

Ledger: ``task/fnp-2-free-names-ledger.md``.
"""

from __future__ import annotations

import pytest

from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("fnp2-free-names").getOrCreate()


def _ordered(order_column) -> list:
    """Values of ``v`` after ordering by ``order_column``; NULL sorts as ``None``."""
    spark = _session()
    frame = spark.createDataFrame([(2,), (None,), (1,)], "v int")
    return frame.orderBy(order_column).toArrow().column("v").to_pylist()


@pytest.mark.parametrize(
    ("spelling", "expected"),
    [
        ("asc", [None, 1, 2]),
        ("asc_nulls_first", [None, 1, 2]),
        ("asc_nulls_last", [1, 2, None]),
        ("desc", [2, 1, None]),
        ("desc_nulls_last", [2, 1, None]),
        ("desc_nulls_first", [None, 2, 1]),
    ],
)
def test_all_four_null_ordering_corners(spelling: str, expected: list) -> None:
    """Every spelling is pinned by the ROW ORDER it produces, not by the method it delegates to."""
    assert _ordered(getattr(F, spelling)("v")) == expected, (
        f"F.{spelling} did not place nulls where PySpark places them"
    )


def test_column_method_spellings_match_the_module_functions() -> None:
    """``Column.asc_nulls_last`` and the module function must agree (both doors of the API)."""
    for spelling in ("asc_nulls_first", "asc_nulls_last", "desc_nulls_first", "desc_nulls_last"):
        via_function = _ordered(getattr(F, spelling)("v"))
        via_method = _ordered(getattr(F.col("v"), spelling)())
        assert via_function == via_method, f"F.{spelling}(c) and c.{spelling}() disagree"


def test_alias_spellings_are_the_functions_they_alias() -> None:
    """``column``/``negate``/``session_user`` are bare aliases in PySpark's own ``builtin.py``."""
    assert F.column is F.col
    assert F.negate is F.negative
    assert F.session_user is F.current_user


def test_aliases_evaluate_on_the_arrow_path() -> None:
    """Identity is not enough — the aliases must actually produce values, with the right type."""
    spark = _session()
    frame = spark.createDataFrame([(3,)], "v int")

    out = frame.select(
        F.column("v").alias("c"),
        F.negate("v").alias("n"),
        F.session_user().alias("u"),
    ).toArrow()

    assert out.column("c").to_pylist() == [3]
    assert out.column("n").to_pylist() == [-3]
    assert out.column("u").to_pylist() == ["repark"]
    assert out.schema.field("u").type.equals(out.schema.field("u").type)  # string, not null-typed
    assert str(out.schema.field("u").type) in {"string", "large_string"}


def test_window_order_honours_explicit_null_placement() -> None:
    """``Window.orderBy`` resolved null placement from the DIRECTION, discarding the marker.

    Latent while only the two default spellings existed — ``asc`` always meant nulls-first anyway —
    and reachable the moment ``asc_nulls_last`` exists.
    """
    from repark.spark import Window

    spark = _session()
    frame = spark.createDataFrame([(1, None), (2, 5), (3, 7)], "k int, v int")

    nulls_last = frame.select(
        F.col("k"),
        F.first("v").over(Window.orderBy(F.col("v").asc_nulls_last())).alias("f"),
    ).toArrow()
    nulls_first = frame.select(
        F.col("k"),
        F.first("v").over(Window.orderBy(F.col("v").asc_nulls_first())).alias("f"),
    ).toArrow()

    assert nulls_last.column("f").to_pylist() != nulls_first.column("f").to_pylist(), (
        "the two null placements produced identical windows, so Window.orderBy is still deriving "
        "null placement from the sort direction instead of honouring the marker"
    )
    assert nulls_last.column("f").to_pylist()[0] == 5
    assert nulls_first.column("f").to_pylist()[0] is None


def test_window_specs_differing_only_in_null_placement_do_not_merge() -> None:
    """The plan-collapse structural key omitted null placement, so two windows compared equal.

    Two ``withColumn`` calls whose specs differ only in where NULLs sort are DIFFERENT windows;
    merging them silently reorders one of the two results.
    """
    from repark.spark import Window

    spark = _session()
    frame = spark.createDataFrame([(1, None), (2, 5), (3, 7)], "k int, v int")

    out = (
        frame.withColumn("a", F.first("v").over(Window.orderBy(F.col("v").asc_nulls_first())))
        .withColumn("b", F.first("v").over(Window.orderBy(F.col("v").asc_nulls_last())))
        .toArrow()
    )

    assert out.column("a").to_pylist() != out.column("b").to_pylist(), (
        "adjacent windows differing only in null placement were merged into one spec"
    )


def test_ascending_keyword_supersedes_a_per_column_null_marker() -> None:
    """``orderBy(..., ascending=…)`` re-marks the columns wholesale, direction AND nulls.

    Routing null placement through the column's marker made this path a behaviour change by
    accident: a column carrying ``asc()`` (nulls first) under ``ascending=False`` would have kept
    nulls first while sorting descending. PySpark's ``ascending`` replaces the marker, so the
    override wins on both halves — pinned here because nothing else covers the interaction.
    """
    spark = _session()
    frame = spark.createDataFrame([(2,), (None,), (1,)], "v int")

    out = frame.orderBy(F.col("v").asc(), ascending=False).toArrow().column("v").to_pylist()
    assert out == [2, 1, None], (
        "ascending=False must give descending order with nulls last, regardless of the marker "
        "the column arrived carrying"
    )

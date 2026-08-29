"""Regression pins for critic-found defects, grouped by provenance rather than subsystem.

Each row here failed before its fix and passes after. Ledgers:
``task/fnp-critic-round-1-ledger.md``, ``task/fnp-critic-round-2-ledger.md``.
"""

from __future__ import annotations

import pytest

from repark.errors import PySparkException, UnsupportedOperationException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("fnp-critic-remediation").getOrCreate()


# F-CSP-1 (S0) — nested higher-order functions


def test_nested_higher_order_is_refused_rather_than_silently_wrong() -> None:
    """A loud, explained refusal instead of a silently inverted boolean.

    Unique plan names exposed an upstream limit: DataFusion 54.1 cannot evaluate a nested
    lambda over real columns at all (its own SQL planner fails the same way), so the honest
    answer is a refusal that names the limit.
    """
    frame = _session().createDataFrame([([1, 2], [5, 6]), ([9], [1])], "a array<int>, b array<int>")
    with pytest.raises(UnsupportedOperationException, match="nested inside another"):
        frame.select(F.exists("a", lambda x: F.exists("b", lambda y: y > 4)).alias("e")).toArrow()


def test_a_lambda_body_may_still_capture_an_outer_column() -> None:
    """The refusal must catch NESTING, not capture — capture is ordinary and must keep working."""
    frame = _session().sql("SELECT array(1, 2, 3) AS a, 2 AS threshold")
    got = (
        frame.select(F.exists("a", lambda x: x > F.col("threshold")).alias("e"))
        .toArrow()
        .column("e")
        .to_pylist()
    )
    assert got == [True]


# F-CSP-2 / F-CFS-4 — the ascending= override


@pytest.mark.parametrize(
    ("override", "expected"),
    [
        (None, [1, 2, 3, None]),
        (True, [1, 2, 3, None]),  # truthy is a NO-OP in PySpark; the marker survives
        ([True], [1, 2, 3, None]),
        (False, [3, 2, 1, None]),  # falsy re-marks the column, so nulls follow the new direction
    ],
    ids=["no-override", "True", "[True]", "False"],
)
def test_ascending_override_only_remarks_on_a_falsy_flag(override, expected) -> None:
    """PySpark's ``_sort_cols`` re-marks only in the falsy branch, so ``ascending=True``
    preserves an explicit ``asc_nulls_last``. Wholesale re-marking silently reorders rows —
    and silently wrong row order changes which rows ``limit``/``head`` return.
    """
    frame = _session().createDataFrame([(3,), (None,), (1,), (2,)], "v int")
    ordered = (
        frame.orderBy(F.asc_nulls_last("v"))
        if override is None
        else frame.orderBy(F.asc_nulls_last("v"), ascending=override)
    )
    assert ordered.toArrow().column("v").to_pylist() == expected


# F-CSP-3 / F-CFS-3 — the empty pattern


@pytest.mark.parametrize("text", ["abc", "2026", "", "a1b2"], ids=["abc", "2026", "empty", "a1b2"])
def test_empty_pattern_agrees_between_counting_and_collecting(text: str) -> None:
    """Java's ``Matcher`` on ``Pattern.compile("")`` matches at every position plus the end.

    ``regexp_count`` counted those; ``regexp_extract_all`` returned ``[]`` — a user reading
    ``[]`` as "no matches" would drop rows the count said existed. ``idx=0`` is named
    explicitly (SEM-1): the empty pattern has no capture group, and Spark's two-argument
    default (group 1) raises on such a pattern — this test is about counting and collecting
    agreeing, not about the group default.
    """
    frame = _session().createDataFrame([(text,)], "t string")
    out = frame.select(
        F.regexp_count("t", F.lit("")).alias("n"),
        F.size(F.regexp_extract_all("t", F.lit(""), 0)).alias("sz"),
    ).toArrow()
    assert out.column("n").to_pylist() == out.column("sz").to_pylist()


# F-CSP-4 / F-CFS-2 — the aggregate consumption site


def test_higher_order_works_in_group_by_and_agg() -> None:
    """``PyDataFrame::aggregate`` was the one column-consuming site that never bound lambdas.

    ``groupBy(exists(...))`` is a first-reach idiom and it hard-failed with an internal-sounding
    ``unresolved LambdaVariable x``.
    """
    frame = _session().sql("SELECT array(1, 2, 3) AS a, 1 AS k")

    grouped = frame.groupBy(F.exists("a", lambda x: x > 2).alias("e")).count().toArrow()
    assert grouped.column("e").to_pylist() == [True]

    aggregated = frame.agg(F.max(F.exists("a", lambda x: x > 2)).alias("m")).toArrow()
    assert aggregated.column("m").to_pylist() == [True]


# F-CSP-5 / F-CFS-9 — xxhash64 arity


def test_xxhash64_is_variadic_like_pyspark() -> None:
    """The function exists mainly to hash a composite key; the one-column form is the rare case."""
    frame = _session().createDataFrame([("a", 1)], "s string, i int")
    out = frame.select(
        F.xxhash64("s").alias("one"),
        F.xxhash64("s", "i").alias("two"),
    ).toArrow()
    assert out.column("two").to_pylist() != out.column("one").to_pylist()
    assert str(out.schema.field("two").type) == "int64"


# F-CFS-1 — randstr length ceiling


def test_randstr_refuses_an_enormous_length_instead_of_aborting() -> None:
    """Without a cap this was not an error at all — the process died with SIGABRT, no
    traceback, so a notebook lost every other frame. Every other refusal in that unit is a
    catchable ``exec_err!``; this one took the session with it.
    """
    with pytest.raises(PySparkException, match="between 0 and"):
        _session().range(1).select(F.randstr(4_000_000_000).alias("r")).toArrow()


# F-CFS-5 — approx_count_distinct materialized type


def test_approx_count_distinct_is_a_signed_bigint_through_arithmetic() -> None:
    """It materialized UInt64 while ``schema`` reported bigint, and one subtraction turned the
    count into DECIMAL(21,0) — which is what would land in Parquet/Iceberg and not round-trip.
    """
    frame = _session().createDataFrame([(1,), (2,), (1,)], "v int")
    out = frame.agg(
        F.approx_count_distinct("v").alias("o"),
        (F.approx_count_distinct("v") - F.lit(5)).alias("d"),
    ).toArrow()
    assert str(out.schema.field("o").type) == "int64"
    assert str(out.schema.field("d").type) == "int64"


# F-CSP-10 / F-CFS-11 — stale docstrings


def test_no_working_function_still_documents_itself_as_unsupported() -> None:
    """A function that works while its docstring says otherwise sends users to a workaround.

    Checked structurally rather than by a name list, so a future de-stub that forgets its
    docstring is caught the same way.
    """
    import ast
    import pathlib

    import repark.spark.functions_expr as module

    tree = ast.parse(pathlib.Path(module.__file__).read_text())
    stale = [
        node.name
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and (ast.get_docstring(node) or "").startswith("Unsupported")
        and not any(isinstance(inner, ast.Raise) for inner in ast.walk(node))
    ]
    assert stale == [], f"these work but document themselves as unsupported: {stale}"


@pytest.mark.parametrize(
    ("name", "arity"),
    [
        ("sum", 1),
        ("avg", 1),
        ("min", 1),
        ("max", 1),
        ("stddev", 1),
        ("var_pop", 1),
        ("median", 1),
        ("bit_xor", 1),
        ("approx_count_distinct", 1),
        ("corr", 2),
        ("covar_pop", 2),
        ("regr_count", 2),
        ("regr_slope", 2),
    ],
)
def test_every_dispatched_aggregate_can_be_used_in_a_window(name: str, arity: int) -> None:
    """Casting an aggregate must not hide it from ``over``.

    The signed-bigint fix wrapped the aggregate in a CAST; ``over`` matched
    ``Cast(WindowFunction)`` but not ``Cast(AggregateFunction)``, so the one aggregate the fix
    touched became the only one that could not be windowed. Parametrized across both dispatch
    tables so the next cast-wrapping fix cannot repeat it on a different name.
    """
    from repark.spark import Window

    frame = _session().createDataFrame([(1, 1), (1, 2), (1, 1), (2, 5)], "k int, v int")
    window = Window.partitionBy("k").orderBy("v")
    arguments = ["v"] * arity
    got = frame.select(getattr(F, name)(*arguments).over(window).alias("o")).toArrow()
    assert got.num_rows == 4


def test_approx_count_distinct_stays_a_signed_bigint_in_a_window() -> None:
    """The CAST peeled off by ``over`` is re-applied to the window result, so the windowed form
    has the same type as the grouped form rather than falling back to the engine's unsigned one.
    """
    from repark.spark import Window

    frame = _session().createDataFrame([(1, 1), (1, 2), (1, 1), (2, 5)], "k int, v int")
    window = Window.partitionBy("k").orderBy("v")
    got = frame.select(F.approx_count_distinct("v").over(window).alias("o")).toArrow()
    assert str(got.schema.field("o").type) == "int64"


def test_regr_count_is_a_signed_bigint_through_arithmetic() -> None:
    """The same defect as ``approx_count_distinct``, at the sibling dispatch table.

    ``schema`` reported bigint while the buffer held UInt64; one addition widened the count to
    DECIMAL(21,0), and a uint64 column written to Parquet reads back in Spark as decimal(20,0).
    The cast now comes from the aggregate's own declared return type, so it does not depend on
    anyone remembering a name.
    """
    frame = _session().createDataFrame([(1.0, 2.0), (2.0, 4.1), (3.0, 5.9)], "y double, x double")
    out = frame.agg(
        F.regr_count("y", "x").alias("o"),
        (F.regr_count("y", "x") + F.lit(1)).alias("d"),
    ).toArrow()
    assert str(out.schema.field("o").type) == "int64"
    assert str(out.schema.field("d").type) == "int64"


def test_an_unaliased_higher_order_column_has_the_same_name_on_every_build() -> None:
    """Lambda plan names came from a process-wide counter, so the same query built twice in
    one session produced two different output schemas — anything pinning a column name,
    diffing a schema, or writing with inferred names became non-reproducible.

    ``groupBy`` is the path that shows it: it does not apply the facade's projection name, so
    the plan name reaches the schema. The name is now the lambda's nesting depth, which is
    what the collision was ever about.
    """
    frame = _session().createDataFrame([([1, 2, 3], 1)], "a array<int>, k int")
    first = frame.groupBy(F.exists("a", lambda x: x > 2)).count().columns
    second = frame.groupBy(F.exists("a", lambda x: x > 2)).count().columns
    assert first == second


def test_sibling_lambdas_share_a_name_and_still_evaluate_independently() -> None:
    """Depth-based names mean two lambdas side by side mint the same name.

    That is sound and deliberate: they occupy disjoint scopes, so only an ENCLOSING binding
    can capture. Pinned because a counter-based scheme covered this case only by accident.
    """
    frame = _session().createDataFrame([([1, 2, 3],)], "a array<int>")
    got = (
        frame.select(
            F.exists("a", lambda x: x > 1).alias("wide"),
            F.exists("a", lambda x: x > 5).alias("narrow"),
        )
        .toArrow()
        .to_pylist()
    )
    assert got == [{"wide": True, "narrow": False}]

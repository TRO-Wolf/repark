"""Regression pins for the findings the two Critic passes raised on this branch.

Each row here failed before its fix and passes after. They live in one file because they share a
provenance — an adversarial review — rather than a subsystem, and because a reader asking "what
did the critics actually catch?" should get one answer.

Ledger: ``task/fnp-critic-round-1-ledger.md``.
"""

from __future__ import annotations

import pytest

from repark.errors import PySparkException, UnsupportedOperationException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("fnp-critic-remediation").getOrCreate()


# ---- F-CSP-1 (S0) — nested higher-order functions -------------------------------------------


def test_nested_higher_order_is_refused_rather_than_silently_wrong() -> None:
    """Before: an exactly INVERTED boolean with no error. Now: a loud, explained refusal.

    Two lambdas both minting the plan name ``x`` made the inner body bind to the OUTER variable,
    so ``exists(a, x -> exists(b, y -> y > 4))`` evaluated the inner predicate against ``a``.
    Measured [False, True] where Spark gives [True, False].

    Unique plan names fixed the binding but exposed an upstream limit: DataFusion 54.1 cannot
    evaluate a nested lambda over real columns AT ALL — its own SQL planner fails the same way
    (``Field of physical LambdaVariable with index 0 doesn't match batch field``). So the honest
    answer is a refusal that names the limit, not a wrong number.
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


# ---- F-CSP-2 / F-CFS-4 — the ascending= override ---------------------------------------------


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
    """PySpark's ``_sort_cols`` re-marks only in the falsy branch, so ``ascending=True`` preserves
    an explicit ``asc_nulls_last``. Treating the override as wholesale re-marking silently
    reordered rows — and silently wrong row order changes which rows ``limit``/``head`` return.
    """
    frame = _session().createDataFrame([(3,), (None,), (1,), (2,)], "v int")
    ordered = (
        frame.orderBy(F.asc_nulls_last("v"))
        if override is None
        else frame.orderBy(F.asc_nulls_last("v"), ascending=override)
    )
    assert ordered.toArrow().column("v").to_pylist() == expected


# ---- F-CSP-3 / F-CFS-3 — the empty pattern ---------------------------------------------------


@pytest.mark.parametrize("text", ["abc", "2026", "", "a1b2"], ids=["abc", "2026", "empty", "a1b2"])
def test_empty_pattern_agrees_between_counting_and_collecting(text: str) -> None:
    """Java's ``Matcher`` on ``Pattern.compile("")`` matches at every position plus the end.

    ``regexp_count`` counted those; ``regexp_extract_all`` returned ``[]`` — so two functions in
    one module disagreed on plain ASCII, and a user reading ``[]`` as "no matches" would drop
    every row while the count said otherwise.
    """
    frame = _session().createDataFrame([(text,)], "t string")
    out = frame.select(
        F.regexp_count("t", F.lit("")).alias("n"),
        F.size(F.regexp_extract_all("t", F.lit(""))).alias("sz"),
    ).toArrow()
    assert out.column("n").to_pylist() == out.column("sz").to_pylist()


# ---- F-CSP-4 / F-CFS-2 — the aggregate consumption site --------------------------------------


def test_higher_order_works_in_group_by_and_agg() -> None:
    """``PyDataFrame::aggregate`` was the one column-consuming site that never bound lambdas.

    ``groupBy(exists(...))`` is a first-reach idiom and it hard-failed with an internal-sounding
    ``unresolved LambdaVariable x``. The docstring claiming every site was covered is what stopped
    the gap being looked for.
    """
    frame = _session().sql("SELECT array(1, 2, 3) AS a, 1 AS k")

    grouped = frame.groupBy(F.exists("a", lambda x: x > 2).alias("e")).count().toArrow()
    assert grouped.column("e").to_pylist() == [True]

    aggregated = frame.agg(F.max(F.exists("a", lambda x: x > 2)).alias("m")).toArrow()
    assert aggregated.column("m").to_pylist() == [True]


# ---- F-CSP-5 / F-CFS-9 — xxhash64 arity ------------------------------------------------------


def test_xxhash64_is_variadic_like_pyspark() -> None:
    """The function exists mainly to hash a composite key; the one-column form is the rare case."""
    frame = _session().createDataFrame([("a", 1)], "s string, i int")
    out = frame.select(
        F.xxhash64("s").alias("one"),
        F.xxhash64("s", "i").alias("two"),
    ).toArrow()
    assert out.column("two").to_pylist() != out.column("one").to_pylist()
    assert str(out.schema.field("two").type) == "int64"


# ---- F-CFS-1 — randstr length ceiling --------------------------------------------------------


def test_randstr_refuses_an_enormous_length_instead_of_aborting() -> None:
    """Without a cap this was not an error at all — the process died with SIGABRT, no traceback.

    Every other refusal in that unit is a catchable ``exec_err!``; this one took the session with
    it, so a notebook lost every other frame.
    """
    with pytest.raises(PySparkException, match="between 0 and"):
        _session().range(1).select(F.randstr(4_000_000_000).alias("r")).toArrow()


# ---- F-CFS-5 — approx_count_distinct materialized type ---------------------------------------


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


# ---- F-CSP-10 / F-CFS-11 — stale docstrings --------------------------------------------------


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

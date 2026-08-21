"""FNP-4a — the higher-order seam: a Python lambda reaching the engine.

**What was actually wrong.** DataFusion 54.1 ships a complete higher-order machinery —
``Expr::HigherOrderFunction``, ``Expr::Lambda``, ``Expr::LambdaVariable``, the
``HigherOrderUDFImpl`` trait and three working kernels — and RePark could reach none of it. Two
independent causes: the SQL front end parses ``x -> y`` as PostgreSQL's JSON arrow because
``Dialect::supports_lambda_functions()`` is false under ``Generic`` (that half is FNP-4b), and the
facade's ``call_scalar`` carries a name and a list of argument expressions, while a lambda is not
an argument at all — it is a *body* that must be built against a synthetic parameter.

**How the facade half works.** A ``Column`` is standalone and has no schema, so the facade cannot
evaluate the callable against data. It does not need to: it mints a placeholder ``Column`` per
lambda parameter, calls the user's callable with them, and takes the returned ``Column`` as the
body. The binding assembles ``HigherOrderFunction(func, [values…, Lambda(params, body)])``, and
``DataFrame`` resolves the placeholders against its schema at plan-build time.

``exists`` is the one Spark higher-order function needing no new kernel: DataFusion's
``array_any_match`` is bit-for-bit Spark under the default three-valued logic, so it ships as a
registered alias. The other ten need kernels (FNP-4c).

Ledger: ``task/fnp-4a-lambda-seam-ledger.md``.
"""

from __future__ import annotations

import pytest

from repark.errors import PySparkValueError
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("fnp4a-lambda-seam").getOrCreate()


def test_exists_evaluates_a_python_lambda() -> None:
    spark = _session()
    frame = spark.sql("SELECT array(1, 2, 3) AS a")

    out = frame.select(
        F.exists("a", lambda x: x > 2).alias("hit"),
        F.exists("a", lambda x: x > 9).alias("miss"),
    ).toArrow()

    assert out.column("hit").to_pylist() == [True]
    assert out.column("miss").to_pylist() == [False]
    assert str(out.schema.field("hit").type) == "bool"


def test_exists_is_three_valued_like_spark() -> None:
    """A NULL element neither confirms nor denies, so NULL wins over a false answer.

    This is the behaviour that makes ``exists`` an honest alias of ``array_any_match`` rather than
    a convenient one — Spark's ``followThreeValuedLogicInArrayExists`` default.
    """
    spark = _session()
    frame = spark.sql("SELECT array(1, CAST(NULL AS INT)) AS a")

    # 1 > 5 is false; NULL > 5 is unknown -> the answer is unknown, not false.
    assert frame.select(F.exists("a", lambda x: x > 5).alias("r")).toArrow().column(
        "r"
    ).to_pylist() == [None]
    # 1 > 0 is true -> a true element settles it regardless of the NULL.
    assert frame.select(F.exists("a", lambda x: x > 0).alias("r")).toArrow().column(
        "r"
    ).to_pylist() == [True]


def test_exists_empty_and_null_array_edges() -> None:
    spark = _session()
    empty = spark.sql("SELECT CAST(array() AS ARRAY<INT>) AS a")
    assert empty.select(F.exists("a", lambda x: x > 0).alias("r")).toArrow().column(
        "r"
    ).to_pylist() == [False]

    null_array = spark.sql("SELECT CAST(NULL AS ARRAY<INT>) AS a")
    assert null_array.select(F.exists("a", lambda x: x > 0).alias("r")).toArrow().column(
        "r"
    ).to_pylist() == [None]


def test_the_lambda_body_can_close_over_an_outer_column() -> None:
    """The body is an ordinary expression tree, so a captured column is just another leaf."""
    spark = _session()
    frame = spark.sql("SELECT array(1, 2, 3) AS a, 2 AS threshold")

    out = frame.select(F.exists("a", lambda x: x > F.col("threshold")).alias("r")).toArrow()
    assert out.column("r").to_pylist() == [True]


def test_lambda_parameter_names_are_ours_not_the_users() -> None:
    """PySpark names lambda parameters x/y/z whatever the caller wrote, because they enter the plan.

    Pinned via the projection name so a future change that leaks the user's identifier is caught.
    """
    spark = _session()
    frame = spark.sql("SELECT array(1, 2, 3) AS a")

    columns = frame.select(F.exists("a", lambda anything: anything > 2)).columns
    assert columns == ["exists(a, x -> (x > 2))"], columns


def test_wrong_lambda_arity_is_refused_loudly() -> None:
    spark = _session()
    frame = spark.sql("SELECT array(1, 2, 3) AS a")

    with pytest.raises(PySparkValueError, match="expects 1"):
        frame.select(F.exists("a", lambda x, y: x > y))


def test_a_lambda_returning_a_non_column_is_refused_loudly() -> None:
    spark = _session()
    frame = spark.sql("SELECT array(1, 2, 3) AS a")

    with pytest.raises(PySparkValueError, match="must return a Column"):
        frame.select(F.exists("a", lambda x: True))


def test_lambda_survives_select_filter_with_column_and_order_by() -> None:
    """Resolution is wired at each site that hands a column to DataFusion, not just ``select``.

    An unresolved lambda variable fails when the plan asks it for a type, so a missed site is a
    hard error rather than a silent one — but only for whoever hits that site first.

    The fifth site, ``join_on``, is wired but deliberately unpinned here: it resolves against the
    LEFT frame's schema, so a lambda over a right-side column is not covered by this binding.
    Recorded in the ledger rather than implied to work.
    """
    spark = _session()
    frame = spark.sql("SELECT array(1, 2, 3) AS a, 1 AS k")

    assert frame.filter(F.exists("a", lambda x: x > 2)).count() == 1
    assert frame.withColumn("r", F.exists("a", lambda x: x > 2)).toArrow().column(
        "r"
    ).to_pylist() == [True]
    assert frame.orderBy(F.exists("a", lambda x: x > 2)).count() == 1

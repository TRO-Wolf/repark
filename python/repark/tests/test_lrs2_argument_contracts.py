"""LRS-2 — argument contracts matching PySpark, measured against live PySpark 4.1.2.

Rejected behaviours are pinned too: a test holding the rejected behaviour in place stops it
being "fixed" again.
"""

from __future__ import annotations

import pytest

from repark.errors import AnalysisException, PySparkValueError
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("lrs2-contracts").getOrCreate()


def _frame():
    return _session().createDataFrame([([1, 2, 3], 1)], "a array<int>, k int")


def test_xxhash64_with_no_arguments_refuses_by_its_own_name() -> None:
    """Spark refuses zero-argument ``xxhash64``, through the facade and SQL alike:
    ``[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The 'xxhash64' requires > 0 parameters but the actual
    number is 0``. repark's message must name the user-facing call, not an internal dispatcher.
    """
    with pytest.raises(AnalysisException, match=r"WRONG_NUM_ARGS"):
        _frame().select(F.xxhash64())


def test_xxhash64_still_hashes_and_agrees_with_spark_by_value() -> None:
    """The expected value is Spark's own answer for this input, not repark's read back."""
    got = _frame().select(F.xxhash64("k").alias("h")).toArrow().column("h").to_pylist()
    assert got == [-6698625589789238999]


@pytest.mark.parametrize(
    ("label", "source"),
    [
        ("keyword-only", "lambda *, x: x > 2"),
        ("var-positional", "lambda *x: x"),
        ("var-keyword", "lambda **x: x"),
    ],
)
def test_a_lambda_parameter_kind_spark_rejects_is_refused_with_sparks_message(
    label: str, source: str
) -> None:
    """Spark rejects these parameter kinds with ``[UNSUPPORTED_PARAM_TYPE_FOR_HIGHER_ORDER_FUNCTION]
    Function `<lambda>` should use only POSITIONAL or POSITIONAL OR KEYWORD arguments.``, not a raw
    Python ``TypeError`` about the lambda's internals.
    """
    with pytest.raises(
        PySparkValueError, match=r"UNSUPPORTED_PARAM_TYPE_FOR_HIGHER_ORDER_FUNCTION"
    ):
        _frame().select(F.exists("a", eval(source)))


def test_a_positional_only_lambda_parameter_is_accepted() -> None:
    """Positional-only lambda parameters work in Spark, so they work here:
    ``lambda x, /: x > 2``.
    """
    body = eval("lambda x, /: x > 2")
    got = _frame().select(F.exists("a", body).alias("e")).toArrow().column("e").to_pylist()
    assert got == [True]


def test_a_non_callable_lambda_argument_keeps_raising_a_plain_type_error() -> None:
    """Spark raises a plain ``TypeError: 'nope' is not a callable object`` here, not a facade
    error. A ``PySparkValueError`` guard would create a divergence; do not add one.
    """
    with pytest.raises(TypeError, match="is not a callable object"):
        _frame().select(F.exists("a", "nope"))

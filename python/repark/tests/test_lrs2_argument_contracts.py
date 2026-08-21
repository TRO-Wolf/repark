"""LRS-2 — argument contracts that match PySpark's, measured rather than inferred.

Every expectation here was taken from a live PySpark 4.1.2 (design §7), and the oracle **refuted
two of the three fixes the Critic round suggested**. Those refutations are pinned too: a test that
holds the rejected behaviour in place is what stops it being "fixed" again.

Ledger: ``task/lrs-2-argument-contracts-ledger.md``.
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
    """The review suggested ACCEPTING the zero-argument form and emitting ``lit(42)``.

    Spark raises, through the facade and through SQL alike:
    ``[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The 'xxhash64' requires > 0 parameters but the actual
    number is 0``. Accepting it would have shipped a divergence. What was wrong was only the
    message — ``call_scalar(xxhash64) expects at least 1 args, got 0`` names an internal dispatcher
    the user never called.
    """
    with pytest.raises(AnalysisException, match=r"WRONG_NUM_ARGS"):
        _frame().select(F.xxhash64())


def test_xxhash64_still_hashes_and_agrees_with_spark_by_value() -> None:
    """The refusal must not have narrowed the working form. The expected number is Spark's own
    answer for this input, not repark's read back.
    """
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
    """These passed the arity gate and failed later as a raw Python ``TypeError: <lambda>() takes 0
    positional arguments but 1 was given`` — an error about repark's internals, not the user's code.

    The message is Spark's, verbatim in shape: ``[UNSUPPORTED_PARAM_TYPE_FOR_HIGHER_ORDER_FUNCTION]
    Function `<lambda>` should use only POSITIONAL or POSITIONAL OR KEYWORD arguments.``
    """
    with pytest.raises(
        PySparkValueError, match=r"UNSUPPORTED_PARAM_TYPE_FOR_HIGHER_ORDER_FUNCTION"
    ):
        _frame().select(F.exists("a", eval(source)))


def test_a_positional_only_lambda_parameter_is_accepted() -> None:
    """The review's suggested predicate — reject anything that is not ``POSITIONAL_OR_KEYWORD`` —
    would have broken this. ``lambda x, /: x > 2`` WORKS in Spark, so it works here.
    """
    body = eval("lambda x, /: x > 2")
    got = _frame().select(F.exists("a", body).alias("e")).toArrow().column("e").to_pylist()
    assert got == [True]


def test_a_non_callable_lambda_argument_keeps_raising_a_plain_type_error() -> None:
    """The review suggested guarding this with a ``PySparkValueError``. Spark raises a plain
    ``TypeError: 'nope' is not a callable object`` — which is byte-for-byte what repark already
    raises, so "fixing" it would have CREATED the divergence.

    Pinned so the next reader of that finding does not act on it.
    """
    with pytest.raises(TypeError, match="is not a callable object"):
        _frame().select(F.exists("a", "nope"))

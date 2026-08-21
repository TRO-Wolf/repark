"""SEM-4 — the regexp refusals say Spark's words, and name the function they came from.

Two message defects, no value changes. Both were measured on this tree before the fix.

**The group-index refusal was repark's own wording, in two different shapes.** A negative index
gave ``regexp_extract_all group index must not be negative, got -1`` and an over-large one gave
``regexp_extract_all group index 3 is out of range for a pattern with 2 groups``. Spark folds both
into ONE condition, and a user greps for its name:

    [INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX] The value of parameter(s) `idx` in
    `regexp_extract_all` is invalid: Expects group index between 0 and 2, but got 3.
    SQLSTATE: 22023

*(oracle: live PySpark 4.1.2. ``-1`` and ``3`` produce the same message with the bound filled in;
a zero-group pattern reads "between 0 and 0".)*

**Two of the four regexp kernels named the wrong function in their own planning errors.**
``coerce_regexp_args`` hard-coded ``regexp_count`` / ``regexp_instr``, so
``SELECT regexp_extract_all('a')`` reported ``'regexp_instr' expects 2 or 3 arguments`` and
``SELECT regexp_substr('a')`` reported ``'regexp_count' expects 2 arguments``. That is repark
misreporting itself, not a Spark-parity claim, so these assertions are about internal honesty.

Ledger: ``task/sem-4-regexp-messages-ledger.md``.
"""

from __future__ import annotations

import pytest

from repark.errors import AnalysisException

PAIRS = "([a-z])([0-9])"


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("sem4-regexp-messages").getOrCreate()


def _sql(query: str):
    return _session().sql(query).toArrow().column("r").to_pylist()


@pytest.mark.parametrize(
    ("idx", "bound", "pattern"),
    [
        (3, 2, PAIRS),
        (-1, 2, PAIRS),
        (99, 2, PAIRS),
        (1, 0, "[0-9]*"),
        (-1, 0, "[0-9]*"),
    ],
)
def test_an_invalid_group_index_raises_sparks_condition(idx: int, bound: int, pattern: str) -> None:
    """One condition for both directions, with the bound Spark reports.

    ``-1`` is the case that proves the two old messages collapsed into one: repark had a separate
    "must not be negative" arm, and Spark does not.
    """
    expected = (
        "[INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX] The value of parameter(s) `idx` in "
        f"`regexp_extract_all` is invalid: Expects group index between 0 and {bound}, "
        f"but got {idx}. SQLSTATE: 22023"
    )
    # The class is `PySparkException`, not Spark's `SparkRuntimeException` — a recorded
    # residual (ledger §2). The CONDITION NAME is what this pins, and it is exact.
    with pytest.raises(Exception) as caught:
        _sql(f"SELECT regexp_extract_all('a1b2','{pattern}', {idx}) AS r")
    assert expected in str(caught.value)


def test_a_valid_group_index_is_untouched() -> None:
    """The bound is inclusive at both ends — 0 and the group count both stay legal."""
    assert _sql(f"SELECT regexp_extract_all('a1b2','{PAIRS}', 0) AS r") == [["a1", "b2"]]
    assert _sql(f"SELECT regexp_extract_all('a1b2','{PAIRS}', 2) AS r") == [["1", "2"]]


@pytest.mark.parametrize(
    ("call", "name"),
    [
        ("regexp_extract_all('a')", "regexp_extract_all"),
        ("regexp_substr('a')", "regexp_substr"),
        ("regexp_count('a')", "regexp_count"),
        ("regexp_instr('a')", "regexp_instr"),
    ],
)
def test_an_arity_refusal_names_the_function_it_came_from(call: str, name: str) -> None:
    """``regexp_extract_all`` reported itself as ``regexp_instr``, and ``regexp_substr`` as
    ``regexp_count``, because both borrowed a shared coercion helper that hard-coded a name.
    """
    with pytest.raises(AnalysisException) as caught:
        _sql(f"SELECT {call} AS r")
    assert f"'{name}' expects" in str(caught.value)


def test_a_bad_idx_type_names_the_function_it_came_from() -> None:
    """Same helper, the other hard-coded name."""
    with pytest.raises(AnalysisException) as caught:
        _sql(f"SELECT regexp_extract_all('a1b2','{PAIRS}', array(1)) AS r")
    assert "'regexp_extract_all' idx must be an integer" in str(caught.value)

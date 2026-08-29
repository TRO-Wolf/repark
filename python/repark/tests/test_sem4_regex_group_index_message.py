"""SEM-4 — the regexp refusals say Spark's words, and name the function they came from.

Group-index refusals must fold into Spark's ONE condition, whose name a user greps:

    [INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX] The value of parameter(s) `idx` in
    `regexp_extract_all` is invalid: Expects group index between 0 and 2, but got 3.
    SQLSTATE: 22023

*(oracle: live PySpark 4.1.2. ``-1`` and ``3`` produce the same message with the bound filled in;
a zero-group pattern reads "between 0 and 0".)*

Arity refusals from the shared coercion helper must name the function the user called — repark
misreporting itself, not a Spark-parity claim, so those assertions are about internal honesty.

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
    """One condition for both directions, with the bound Spark reports."""
    expected = (
        "[INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX] The value of parameter(s) `idx` in "
        f"`regexp_extract_all` is invalid: Expects group index between 0 and {bound}, "
        f"but got {idx}. SQLSTATE: 22023"
    )
    # The class is `PySparkException`, not Spark's `SparkRuntimeException` — a recorded residual
    # (ledger §2). The CONDITION NAME is what this pins, and it is exact.
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
    """The shared coercion helper must use the caller's function name, not a hard-coded one."""
    with pytest.raises(AnalysisException) as caught:
        _sql(f"SELECT {call} AS r")
    assert f"'{name}' expects" in str(caught.value)


def test_a_bad_idx_type_names_the_function_it_came_from() -> None:
    """Same helper, the other hard-coded name."""
    with pytest.raises(AnalysisException) as caught:
        _sql(f"SELECT regexp_extract_all('a1b2','{PAIRS}', array(1)) AS r")
    assert "'regexp_extract_all' idx must be an integer" in str(caught.value)

"""SEM-6 — ``regexp_substr`` returns NULL for a zero-width match, as Spark does.

Spark's rule: take the FIRST match; if it is empty, the result is NULL — Spark does not look for
a later non-empty one (``regexp_substr('a1b2', '[0-9]*')`` is NULL even though ``'1'`` matches at
position 1). This is a different arm from "no match at all", which was already NULL. Every
expected value below is Spark 4.1.2's own answer.
"""

from __future__ import annotations

import pytest

ASTRAL = "\U0001f389ab"


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("sem6-substr-null").getOrCreate()


def _sql(expression: str):
    return _session().sql(f"SELECT {expression} AS r").toArrow().column("r").to_pylist()[0]


@pytest.mark.parametrize(
    ("label", "expression"),
    [
        ("empty pattern", "regexp_substr('ab', '')"),
        ("empty match at position 0", "regexp_substr('ab', 'b*')"),
        ("empty match before a non-empty one", "regexp_substr('a1b2', '[0-9]*')"),
        ("empty string subject", "regexp_substr('', '')"),
        ("optional group that did not participate", "regexp_substr('ac', '(b)?')"),
        ("zero-width anchor", "regexp_substr('ab', '$')"),
        ("empty pattern on astral text", f"regexp_substr('{ASTRAL}', '')"),
    ],
)
def test_a_zero_width_match_is_null(label: str, expression: str) -> None:
    """Spark returns NULL for every one."""
    assert _sql(expression) is None


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("regexp_substr('ab', 'a*')", "a"),
        ("regexp_substr('a1b2', '[0-9]+')", "1"),
        ("regexp_substr('a1b2', '([a-z])([0-9])')", "a1"),
    ],
)
def test_a_non_empty_match_is_unchanged(expression: str, expected: str) -> None:
    """The rule is "empty MATCH → NULL", not "empty pattern → NULL", so the boundary is pinned
    from both sides: ``a*`` (first match non-empty) must keep returning ``'a'``.
    """
    assert _sql(expression) == expected


@pytest.mark.parametrize(
    ("label", "expression"),
    [
        ("no match at all", "regexp_substr('ab', 'x')"),
        ("NULL subject", "regexp_substr(CAST(NULL AS STRING), 'a')"),
        ("NULL pattern", "regexp_substr('ab', CAST(NULL AS STRING))"),
    ],
)
def test_the_paths_that_were_already_null_stay_null(label: str, expression: str) -> None:
    """NULL-in NULL-out and the genuine no-match are a different arm from the zero-width one;
    they must not be collapsed into it.
    """
    assert _sql(expression) is None

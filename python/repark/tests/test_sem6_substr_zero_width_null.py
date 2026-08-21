"""SEM-6 — ``regexp_substr`` returns NULL for a zero-width match, as Spark does.

**This unit changes what a working query returns.** ``regexp_substr('ab', '')`` returned ``''`` and
now returns NULL. Registry row ``RE-3`` is retired by this commit, and the pin that codified the
old answer (``test_lrs6_regexp_divergences.py``) leaves with it.

Spark's rule, measured across the whole zero-width space rather than inferred from one sample:
**take the FIRST match; if it is empty, the result is NULL.** Spark does not go looking for a
later non-empty one — ``regexp_substr('a1b2', '[0-9]*')`` is NULL even though ``'1'`` matches at
position 1. That distinction is what separates this from "return NULL when there is no match",
which repark already did correctly.

Every expected value below is Spark 4.1.2's own answer.

Ledger: ``task/sem-6-substr-zero-width-null-ledger.md``.
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
    """All seven returned ``''`` before this unit. Spark returns NULL for every one."""
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
    """The controls that keep the change narrow, and they are measured, not assumed.

    ``a*`` is the one that matters: its first match IS non-empty, so it must keep returning ``'a'``.
    A fix written as "empty pattern → NULL" rather than "empty MATCH → NULL" would still pass the
    first block above and break nothing here — which is why the boundary is pinned from both sides.
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
    """These three already matched Spark before this unit. NULL-in NULL-out and the genuine
    no-match are a different arm from the zero-width one, and must not be collapsed into it.
    """
    assert _sql(expression) is None

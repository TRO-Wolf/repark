"""SEM-3 — ``regexp_extract_all`` accepts a string ``idx``, as every other door already does.

``F.regexp_extract_all(s, pattern, "1")`` raised ``AnalysisException: No field named "1"`` — the
string was read as a column name. Spark accepts it, repark's own SQL door accepts it, and repark's
own sibling ``F.regexp_instr(s, pattern, "0")`` accepts it, so this repository disagreed with
itself on plain input.

A regression from the FNP-6a critic remediation: ``task/fnp-6a-regexp-ledger.md`` records the
wrapper as having carried ``lit_indices={1, 2}``. Position 1 (``regexp``) genuinely had to go —
Spark reads a bare string there as a COLUMN NAME (oracle below) — but F-FNP6A-1 dropped the whole
set instead of narrowing it to ``{2}``, taking the correct half with the incorrect half.

Every expected value is Spark's own answer, taken from a live PySpark 4.1.2, not read back out of
repark:

* ``regexp_extract_all('a1b2', '([a-z])([0-9])', '2')`` → ``['1', '2']``
* ``regexp_extract_all('a1b2', '([a-z])([0-9])', '0')`` → ``['a1', 'b2']``
* ``regexp_extract_all(s, p)`` with ``p`` a COLUMN holding the pattern → ``['a', 'b']``

Ledger: ``task/sem-3-string-idx-ledger.md``.
"""

from __future__ import annotations

import pytest

from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("sem3-string-idx").getOrCreate()


def _frame():
    return _session().createDataFrame([("a1b2", "([a-z])([0-9])")], "s string, p string")


def _extract(regexp: object, idx: object | None = None) -> list[str]:
    arguments = ("s", regexp) if idx is None else ("s", regexp, idx)
    return (
        _frame()
        .select(F.regexp_extract_all(*arguments).alias("r"))
        .toArrow()
        .column("r")
        .to_pylist()[0]
    )


PAIRS = "([a-z])([0-9])"


@pytest.mark.parametrize(
    ("idx", "expected"),
    [("0", ["a1", "b2"]), ("1", ["a", "b"]), ("2", ["1", "2"])],
)
def test_a_string_idx_is_a_literal_group_index(idx: str, expected: list[str]) -> None:
    """Spark's answers. Before this unit each of these raised ``No field named "<idx>"``."""
    assert _extract(F.lit(PAIRS), idx) == expected


@pytest.mark.parametrize(
    ("idx", "expected"),
    [(0, ["a1", "b2"]), (1, ["a", "b"]), (2, ["1", "2"])],
)
def test_an_int_idx_still_means_the_same_thing(idx: int, expected: list[str]) -> None:
    """The integer form already worked and must keep working — the fix narrows, it does not move."""
    assert _extract(F.lit(PAIRS), idx) == expected


def test_a_column_idx_is_still_a_column_not_a_literal() -> None:
    """``lit_indices`` must stay EMPTY for a Column ``idx``, or a genuine column reference would be
    stringified into a literal. This is the half of the fix that is easy to get wrong.
    """
    assert _extract(F.lit(PAIRS), F.lit(2)) == ["1", "2"]


def test_a_bare_string_regexp_is_still_a_column_name() -> None:
    """The half F-FNP6A-1 got RIGHT, pinned so re-narrowing cannot quietly re-break it.

    ``p`` is a column holding the pattern; Spark resolves the bare string as a column reference,
    not as a pattern literal. Measured on the live oracle at ``['a', 'b']``.
    """
    assert _extract("p") == ["a", "b"]

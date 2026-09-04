"""The cursor dbt drives, over results with more than one row.

pins: dbt-1-adapter/C-002
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

CATALOG = "cur"
NAMESPACES = ("alpha", "beta", "gamma")

ROWS_SQL = "select * from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(n, s)"


@pytest.fixture
def session(tmp_path: Path) -> Iterator[Any]:
    """A memory catalog holding three namespaces and one three-row table."""
    from repark import ReparkSession

    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    built = ReparkSession.builder.appName("dbt-1-cursor").getOrCreate()
    built.register_memory_catalog(CATALOG, warehouse)
    for namespace in NAMESPACES:
        built.sql(f"CREATE NAMESPACE {CATALOG}.{namespace} LOCATION '{warehouse / namespace}'")
    built.sql(f"create or replace table {CATALOG}.alpha.three using iceberg as {ROWS_SQL}")
    try:
        yield built
    finally:
        built.stop()


def _cursor(session: Any) -> Any:
    """A fresh cursor over the shared session, as the connection handle hands one out."""
    from dbt.adapters.repark.session import ReparkHandle

    return ReparkHandle(session).cursor()


def test_show_namespaces_fetchall_returns_every_row(session: Any) -> None:
    """``show namespaces`` is a real multi-row result and every row comes back."""
    cursor = _cursor(session)
    cursor.execute(f"show namespaces in {CATALOG}")
    assert cursor.row_count == 3
    assert sorted(cursor.fetchall()) == [("alpha",), ("beta",), ("gamma",)]


def test_select_fetchall_returns_every_row_and_then_nothing(session: Any) -> None:
    """``fetchall`` drains the result: the second call returns no rows."""
    cursor = _cursor(session)
    cursor.execute(f"select n, s from {CATALOG}.alpha.three order by n")
    assert cursor.fetchall() == [(1, "a"), (2, "b"), (3, "c")]
    assert cursor.fetchall() == []


def test_fetchmany_pages_through_the_result(session: Any) -> None:
    """``fetchmany`` is what dbt calls under ``--limit``; it must page, not restart."""
    cursor = _cursor(session)
    cursor.execute(f"select n, s from {CATALOG}.alpha.three order by n")
    assert cursor.fetchmany(2) == [(1, "a"), (2, "b")]
    assert cursor.fetchmany(2) == [(3, "c")]
    assert cursor.fetchmany(2) == []


def test_fetchone_walks_the_result_then_answers_none(session: Any) -> None:
    """``fetchone`` advances one row per call and answers None at the end."""
    cursor = _cursor(session)
    cursor.execute(f"select n, s from {CATALOG}.alpha.three order by n")
    assert [cursor.fetchone() for _ in range(3)] == [(1, "a"), (2, "b"), (3, "c")]
    assert cursor.fetchone() is None


def test_description_carries_every_column(session: Any) -> None:
    """dbt reads column names from ``description``; a short one silently drops columns."""
    cursor = _cursor(session)
    cursor.execute(f"select n, s from {CATALOG}.alpha.three order by n")
    described = cursor.description
    assert described is not None
    assert [column[0] for column in described] == ["n", "s"]
    assert [column[6] for column in described] == [True, True]
    assert [column[1] for column in described] == ["int64", "string"]


def test_a_statement_with_no_columns_has_no_description(session: Any) -> None:
    """DDL answers a zero-column Arrow table; dbt must see None, not an empty row."""
    cursor = _cursor(session)
    cursor.execute(f"drop table if exists {CATALOG}.alpha.absent")
    assert cursor.description is None
    assert cursor.row_count is None
    assert cursor.fetchall() == []


def test_execute_strips_a_trailing_semicolon(session: Any) -> None:
    """dbt's own macros end some statements with a semicolon."""
    cursor = _cursor(session)
    cursor.execute(f"select n from {CATALOG}.alpha.three order by n;")
    assert cursor.fetchall() == [(1,), (2,), (3,)]


def test_bindings_are_refused_rather_than_interpolated(session: Any) -> None:
    """String-formatting a binding into SQL is an injection path; the cursor refuses instead."""
    from dbt_common.exceptions import DbtRuntimeError

    cursor = _cursor(session)
    with pytest.raises(DbtRuntimeError) as caught:
        cursor.execute("select ? as n", ["1"])
    assert "does not interpolate bindings" in str(caught.value)


def test_close_drops_the_held_result(session: Any) -> None:
    """A closed cursor holds no rows, so a reused handle cannot answer a stale result."""
    cursor = _cursor(session)
    cursor.execute(f"select n from {CATALOG}.alpha.three order by n")
    cursor.close()
    assert cursor.description is None
    assert cursor.fetchall() == []


def test_each_cursor_is_independent(session: Any) -> None:
    """dbt takes one cursor per statement; draining one must not drain another."""
    first = _cursor(session)
    second = _cursor(session)
    first.execute(f"select n from {CATALOG}.alpha.three order by n")
    second.execute(f"select n from {CATALOG}.alpha.three order by n")
    assert first.fetchall() == [(1,), (2,), (3,)]
    assert second.fetchall() == [(1,), (2,), (3,)]

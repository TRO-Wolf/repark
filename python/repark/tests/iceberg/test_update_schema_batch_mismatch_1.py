from __future__ import annotations

import collections
import itertools
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

REPEATS = 30
MISMATCH = "Mismatch between schema and batches"
WRITER_NOT_NULL = "Column 'r' is declared as non-nullable but contains null values"
KINDS = {
    "v2-cow": ("2", "copy-on-write"),
    "v2-mor": ("2", "merge-on-read"),
    "v3": ("3", "copy-on-write"),
}
FLAT_COLUMNS = "id BIGINT, i INT, d DOUBLE, tf FLOAT, e DOUBLE, s STRING, flag STRING"
FLAT_SELECT = "id, i, d, tf, e, s, flag"
FLAT_LAYOUTS = {
    "mixed-nulls": (
        "(4, NULL, CAST('-0.0' AS DOUBLE), CAST('-0.0' AS FLOAT), NULL, 'a ', 'f4')",
        "(6, -1, CAST('0.0' AS DOUBLE), CAST('0.0' AS FLOAT), NULL, 'A', 'f6')",
    ),
    "no-nulls": (
        "(4, 7, CAST('2.5' AS DOUBLE), CAST('2.5' AS FLOAT), CAST('1.0' AS DOUBLE), 'a ', 'f4')",
        "(6, -1, CAST('3.5' AS DOUBLE), CAST('3.5' AS FLOAT), CAST('2.0' AS DOUBLE), 'A', 'f6')",
    ),
}
FLAT_ROWS = {
    "mixed-nulls": [
        (4, None, "-0.0", "-0.0", None, "a ", "f4"),
        (6, -1, "0.0", "0.0", None, "A", "f6"),
    ],
    "no-nulls": [
        (4, 7, "2.5", "2.5", "1.0", "a ", "f4"),
        (6, -1, "3.5", "3.5", "2.0", "A", "f6"),
    ],
}
PREDICATES = {
    "d-null-safe-zero": ("d <=> 0.0", ("mixed-nulls",), (4, 6)),
    "id-in-list": ("id IN (4, 6)", ("mixed-nulls", "no-nulls"), (4, 6)),
    "i-null-or-le": ("i IS NULL OR i <= 7", ("mixed-nulls", "no-nulls"), (4, 6)),
    "flag-in-list": ("flag IN ('f4', 'f6')", ("mixed-nulls", "no-nulls"), (4, 6)),
    "id-one-file": ("id = 4", ("mixed-nulls", "no-nulls"), (4,)),
    "id-in-subquery": ("id IN (SELECT id FROM ice.ns.src)", ("mixed-nulls",), (4, 6)),
}
FLAT_CELLS = [
    (name, layout, kind)
    for name, (_, layouts, _) in PREDICATES.items()
    for layout in layouts
    for kind in KINDS
]
NESTED_COLUMNS = (
    "id BIGINT, i INT, st STRUCT<a: INT, b: STRING>, arr ARRAY<INT>, "
    "m MAP<STRING, INT>, flag STRING"
)
NESTED_SELECT = "id, i, st, arr, m, flag"
NESTED_LAYOUTS = {
    "top-null-struct": (
        "(4, 1, CAST(NULL AS STRUCT<a: INT, b: STRING>), array(1, 2), map('k', 1), 'f4')",
        (4, 1, None, [1, 2], [("k", 1)]),
    ),
    "child-null-struct": (
        "(4, 1, named_struct('a', CAST(NULL AS INT), 'b', 'x'), array(1, 2), map('k', 1), 'f4')",
        (4, 1, {"a": None, "b": "x"}, [1, 2], [("k", 1)]),
    ),
    "child-null-list": (
        "(4, 1, named_struct('a', 1, 'b', 'x'), array(1, CAST(NULL AS INT)), map('k', 1), 'f4')",
        (4, 1, {"a": 1, "b": "x"}, [1, None], [("k", 1)]),
    ),
    "child-null-map": (
        "(4, 1, named_struct('a', 1, 'b', 'x'), array(1, 2), map('k', CAST(NULL AS INT)), 'f4')",
        (4, 1, {"a": 1, "b": "x"}, [1, 2], [("k", None)]),
    ),
}
NESTED_OTHER_ROW = (
    "(6, 2, named_struct('a', 5, 'b', 'y'), array(1, 2), map('k', 1), 'f6')",
    (6, 2, {"a": 5, "b": "y"}, [1, 2], [("k", 1)]),
)
NESTED_CELLS = [(layout, kind) for layout in NESTED_LAYOUTS for kind in ("v2-cow", "v3")]
REQUIRED_COLUMNS = "id BIGINT, i INT, r INT NOT NULL, flag STRING"
REQUIRED_ROWS = ("(4, NULL, 1, 'f4')", "(6, -1, 2, 'f6')")
REQUIRED_TRUTH = [(4, None, 1, "f4"), (6, -1, 2, "f6")]
COUNTER = itertools.count()


@pytest.fixture
def spark(tmp_path: Path) -> Iterator[Any]:
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-update-schema-batch-mismatch-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog("ice", str(tmp_path / "wh"))
    session.sql("CREATE NAMESPACE ice.ns").collect()
    session.sql("CREATE TABLE ice.ns.src (id BIGINT) USING iceberg").collect()
    session.sql("INSERT INTO ice.ns.src VALUES (4), (6)").collect()
    yield session
    session.stop()
    _reset_active_session_for_tests()


def props(kind: str) -> str:
    version, mode = KINDS[kind]
    return (
        f"'format-version' = '{version}', 'write.delete.mode' = '{mode}', "
        f"'write.update.mode' = '{mode}', 'write.merge.mode' = '{mode}'"
    )


def norm(value: Any) -> Any:
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, dict):
        return {key: norm(item) for key, item in value.items()}
    if isinstance(value, list):
        return [norm(item) for item in value]
    if isinstance(value, tuple):
        return tuple(norm(item) for item in value)
    return value


def rows(session: Any, table: str, select: str) -> list[tuple[Any, ...]]:
    arrow = session.sql(f"SELECT {select} FROM {table} ORDER BY id").to_arrow()
    columns = [arrow.column(index).to_pylist() for index in range(arrow.num_columns)]
    return [norm(tuple(row)) for row in zip(*columns, strict=True)]


def two_file_table(session: Any, kind: str, columns: str, inserts: tuple[str, str]) -> str:
    table = f"ice.ns.t{next(COUNTER)}"
    session.sql(
        f"CREATE TABLE {table} ({columns}) USING iceberg TBLPROPERTIES ({props(kind)})"
    ).collect()
    for values in inserts:
        session.sql(f"INSERT INTO {table} VALUES {values}").collect()
    return table


def outcome(session: Any, statement: str) -> str:
    try:
        session.sql(statement).collect()
    except Exception as error:
        text = str(error)
        if MISMATCH in text:
            return "refused-mismatch"
        if WRITER_NOT_NULL in text:
            return "refused-writer-not-null"
        return f"refused-other: {text[:200]}"
    return "answered"


def tally(results: list[str]) -> dict[str, int]:
    return dict(collections.Counter(results))


@pytest.mark.parametrize(("predicate", "layout", "kind"), FLAT_CELLS)
def test_two_file_update_answers_in_every_batch_order(
    spark: Any, predicate: str, layout: str, kind: str
) -> None:
    where, _, updated = PREDICATES[predicate]
    truth = [(*row[:-1], "upd") if row[0] in updated else row for row in norm(FLAT_ROWS[layout])]
    results = []
    for _ in range(REPEATS):
        table = two_file_table(spark, kind, FLAT_COLUMNS, FLAT_LAYOUTS[layout])
        result = outcome(spark, f"UPDATE {table} SET flag = 'upd' WHERE {where}")
        if result == "answered" and rows(spark, table, FLAT_SELECT) != truth:
            result = f"wrong: {rows(spark, table, FLAT_SELECT)}"
        results.append(result)
    assert tally(results) == {"answered": REPEATS}


@pytest.mark.parametrize(("layout", "kind"), NESTED_CELLS)
def test_nested_null_carried_through_a_two_file_update(spark: Any, layout: str, kind: str) -> None:
    first_sql, first_row = NESTED_LAYOUTS[layout]
    other_sql, other_row = NESTED_OTHER_ROW
    truth = norm([(*first_row, "upd"), (*other_row, "upd")])
    results = []
    for _ in range(REPEATS):
        table = two_file_table(spark, kind, NESTED_COLUMNS, (first_sql, other_sql))
        result = outcome(spark, f"UPDATE {table} SET flag = 'upd' WHERE id = 4 OR id = 6")
        if result == "answered" and rows(spark, table, NESTED_SELECT) != truth:
            result = f"wrong: {rows(spark, table, NESTED_SELECT)}"
        results.append(result)
    assert tally(results) == {"answered": REPEATS}


@pytest.mark.parametrize("kind", list(KINDS))
@pytest.mark.parametrize(
    "assignment", ["r = nullif(r, 1)", "r = i", "r = if(id = 4, CAST(NULL AS INT), r)"]
)
def test_identity_route_null_into_required_column_refuses_with_the_writer_text(
    spark: Any, assignment: str, kind: str
) -> None:
    results = []
    for _ in range(REPEATS):
        table = two_file_table(spark, kind, REQUIRED_COLUMNS, REQUIRED_ROWS)
        result = outcome(spark, f"UPDATE {table} SET {assignment} WHERE id = 4 OR id = 6")
        if rows(spark, table, "id, i, r, flag") != REQUIRED_TRUTH:
            result = f"stored: {rows(spark, table, 'id, i, r, flag')}"
        results.append(result)
    assert tally(results) == {"refused-writer-not-null": REPEATS}

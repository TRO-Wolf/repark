"""CREATE OR REPLACE TABLE … AS twice leg body for the real-AWS acceptance harness."""

from __future__ import annotations

from typing import NamedTuple

from _acceptance import (
    ICEBERG_TABLE_PROPERTIES,
    _sql,
    fq_table,
    snapshot_log_oldest_first,
)

from repark import ReparkSession

REPLACE_SEED_ROWS: tuple[tuple[int, str], ...] = ((1, "s1"), (2, "s2"), (3, "s3"))
REPLACE_FIRST_ROWS: tuple[tuple[int, str], ...] = (
    (1, "r1"),
    (2, "r2"),
    (3, "r3"),
    (4, "r4"),
)
REPLACE_SECOND_ROWS: tuple[tuple[int, str], ...] = (
    (1, "q1"),
    (2, "q2"),
    (3, "q3"),
    (4, "q4"),
    (5, "q5"),
)


class StepObservation(NamedTuple):
    """Rows, Arrow field types, and the ordered ``.snapshots`` log after one statement."""

    rows: list[dict[str, object]]
    field_types: dict[str, str]
    snapshot_ids: list[int]
    snapshot_operations: dict[int, str]


class ReplaceTwiceOutcome(NamedTuple):
    """Observations after the seed CTAS and each of the two replaces."""

    table: str
    created: StepObservation
    first_replace: StepObservation
    second_replace: StepObservation


def _values_select(rows: tuple[tuple[int, str], ...]) -> str:
    value_sql = ", ".join(f"({identifier}, '{name}')" for identifier, name in rows)
    return f"SELECT * FROM (VALUES {value_sql}) AS t(id, name)"


def create_or_replace_seed_sql(table: str) -> str:
    """The plain-CTAS seed statement (no IF NOT EXISTS: a name collision fails loud)."""
    return (
        f"CREATE TABLE {table} USING iceberg "
        f"TBLPROPERTIES ({ICEBERG_TABLE_PROPERTIES}) AS {_values_select(REPLACE_SEED_ROWS)}"
    )


def create_or_replace_sql(table: str, rows: tuple[tuple[int, str], ...]) -> str:
    """One ``CREATE OR REPLACE TABLE … AS`` statement over ``rows``."""
    return (
        f"CREATE OR REPLACE TABLE {table} USING iceberg "
        f"TBLPROPERTIES ({ICEBERG_TABLE_PROPERTIES}) AS {_values_select(rows)}"
    )


def _expected_rows(rows: tuple[tuple[int, str], ...]) -> list[dict[str, object]]:
    return [{"id": identifier, "name": name} for identifier, name in rows]


def _observe(spark: ReparkSession, table: str) -> StepObservation:
    arrow = spark.sql(f"SELECT * FROM {table} ORDER BY id").to_arrow()
    log = snapshot_log_oldest_first(spark, table)
    return StepObservation(
        rows=arrow.to_pylist(),
        field_types={field.name: str(field.type) for field in arrow.schema},
        snapshot_ids=[snapshot_id for snapshot_id, _operation in log],
        snapshot_operations=dict(log),
    )


def run_create_or_replace_twice(
    spark: ReparkSession, catalog: str, namespace: str, table: str
) -> ReplaceTwiceOutcome:
    """Seed 3 rows by CTAS, then run CREATE OR REPLACE twice (4 rows, then 5)."""
    target = fq_table(catalog, namespace, table)
    _sql(spark, create_or_replace_seed_sql(target))
    created = _observe(spark, target)
    _sql(spark, create_or_replace_sql(target, REPLACE_FIRST_ROWS))
    first_replace = _observe(spark, target)
    _sql(spark, create_or_replace_sql(target, REPLACE_SECOND_ROWS))
    second_replace = _observe(spark, target)
    return ReplaceTwiceOutcome(
        table=target,
        created=created,
        first_replace=first_replace,
        second_replace=second_replace,
    )


def assert_replace_twice_outcome(
    outcome: ReplaceTwiceOutcome, *, exact_counts: bool = True
) -> None:
    """The measured shape: last-SELECT rows, measured types, history retained, all ``append``."""
    created = outcome.created
    first_replace = outcome.first_replace
    second_replace = outcome.second_replace
    assert created.rows == _expected_rows(REPLACE_SEED_ROWS)
    assert first_replace.rows == _expected_rows(REPLACE_FIRST_ROWS)
    assert second_replace.rows == _expected_rows(REPLACE_SECOND_ROWS)
    for step in (created, first_replace, second_replace):
        assert step.field_types == {"id": "int32", "name": "string"}
    if exact_counts:
        assert len(created.snapshot_ids) == 1
        assert len(first_replace.snapshot_ids) == 2
        assert len(second_replace.snapshot_ids) == 3
    else:
        assert len(created.snapshot_ids) >= 1
        assert len(first_replace.snapshot_ids) > len(created.snapshot_ids)
        assert len(second_replace.snapshot_ids) > len(first_replace.snapshot_ids)
    assert set(created.snapshot_ids) <= set(first_replace.snapshot_ids)
    assert set(first_replace.snapshot_ids) <= set(second_replace.snapshot_ids)
    current_ids = {
        created.snapshot_ids[-1],
        first_replace.snapshot_ids[-1],
        second_replace.snapshot_ids[-1],
    }
    assert len(current_ids) == 3
    assert created.snapshot_operations[created.snapshot_ids[-1]] == "append"
    assert first_replace.snapshot_operations[first_replace.snapshot_ids[-1]] == "append"
    assert second_replace.snapshot_operations[second_replace.snapshot_ids[-1]] == "append"

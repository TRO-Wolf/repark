"""CREATE OR REPLACE TABLE … AS twice leg body for the real-AWS acceptance harness."""

from __future__ import annotations

from typing import NamedTuple

from _acceptance import ICEBERG_TABLE_PROPERTIES, _sql, fq_table, snapshot_ids_oldest_first

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


class ReplaceTwiceOutcome(NamedTuple):
    """Ordered rows, ``.snapshots`` count and current snapshot id after each of the three writes."""

    table: str
    created_rows: list[dict[str, object]]
    created_snapshot_count: int
    created_current_snapshot_id: int
    first_replace_rows: list[dict[str, object]]
    first_replace_snapshot_count: int
    first_replace_current_snapshot_id: int
    second_replace_rows: list[dict[str, object]]
    second_replace_snapshot_count: int
    second_replace_current_snapshot_id: int


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


def _observe(spark: ReparkSession, table: str) -> tuple[list[dict[str, object]], int, int]:
    rows = spark.sql(f"SELECT * FROM {table} ORDER BY id").to_arrow().to_pylist()
    snapshot_ids = snapshot_ids_oldest_first(spark, table)
    return rows, len(snapshot_ids), snapshot_ids[-1]


def run_create_or_replace_twice(
    spark: ReparkSession, catalog: str, namespace: str, table: str
) -> ReplaceTwiceOutcome:
    """Seed 3 rows by CTAS, then run CREATE OR REPLACE twice (4 rows, then 5)."""
    target = fq_table(catalog, namespace, table)
    _sql(spark, create_or_replace_seed_sql(target))
    created_rows, created_count, created_current = _observe(spark, target)
    _sql(spark, create_or_replace_sql(target, REPLACE_FIRST_ROWS))
    first_rows, first_count, first_current = _observe(spark, target)
    _sql(spark, create_or_replace_sql(target, REPLACE_SECOND_ROWS))
    second_rows, second_count, second_current = _observe(spark, target)
    return ReplaceTwiceOutcome(
        table=target,
        created_rows=created_rows,
        created_snapshot_count=created_count,
        created_current_snapshot_id=created_current,
        first_replace_rows=first_rows,
        first_replace_snapshot_count=first_count,
        first_replace_current_snapshot_id=first_current,
        second_replace_rows=second_rows,
        second_replace_snapshot_count=second_count,
        second_replace_current_snapshot_id=second_current,
    )


def assert_replace_twice_outcome(
    outcome: ReplaceTwiceOutcome, *, exact_counts: bool = True
) -> None:
    """The measured shape: rows equal the last SELECT and history grows one snapshot per replace."""
    assert outcome.created_rows == _expected_rows(REPLACE_SEED_ROWS)
    assert outcome.first_replace_rows == _expected_rows(REPLACE_FIRST_ROWS)
    assert outcome.second_replace_rows == _expected_rows(REPLACE_SECOND_ROWS)
    if exact_counts:
        assert outcome.created_snapshot_count == 1
        assert outcome.first_replace_snapshot_count == 2
        assert outcome.second_replace_snapshot_count == 3
    else:
        assert outcome.created_snapshot_count >= 1
        assert outcome.first_replace_snapshot_count > outcome.created_snapshot_count
        assert outcome.second_replace_snapshot_count > outcome.first_replace_snapshot_count
    snapshot_ids = {
        outcome.created_current_snapshot_id,
        outcome.first_replace_current_snapshot_id,
        outcome.second_replace_current_snapshot_id,
    }
    assert len(snapshot_ids) == 3

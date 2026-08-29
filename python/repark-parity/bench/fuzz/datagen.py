"""Seeded in-memory table generation for the SQL fuzzer.

Type pool: int32 / int64 / float64 / decimal(12,2) / utf8 / date / timestamp /
bool, NULL density ≥ 10%. No NaN / Inf floats (``EXCLUSIONS`` in map.md — NaN
ordering is a known divergence class).
"""

from __future__ import annotations

import math
import random
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Final

from pydantic import BaseModel, ConfigDict, Field

# Table names used by the generator (≤3 tables for joins).
TABLE_NAMES: Final[tuple[str, ...]] = ("t0", "t1", "t2")

# Column catalog per table. Every column is nullable; generator injects nulls.
# Types: "int32" | "int64" | "float64" | "decimal" | "utf8" | "date" | "timestamp" | "bool"
ColumnType = str

# Every table carries a non-null unique ``row_id`` (int64) used as a total-order
# tiebreaker for ORDER BY / LIMIT so engines cannot disagree on tied rows.
TABLE_SCHEMAS: Final[dict[str, list[tuple[str, ColumnType]]]] = {
    "t0": [
        ("row_id", "int64"),
        ("id", "int64"),
        ("a", "int32"),
        ("b", "int64"),
        ("f", "float64"),
        ("d", "decimal"),
        ("s", "utf8"),
        ("dt", "date"),
        ("ts", "timestamp"),
        ("flag", "bool"),
    ],
    "t1": [
        ("row_id", "int64"),
        ("id", "int64"),
        ("a", "int32"),
        ("f", "float64"),
        ("s", "utf8"),
        ("flag", "bool"),
        ("d", "decimal"),
    ],
    "t2": [
        ("row_id", "int64"),
        ("id", "int64"),
        ("b", "int64"),
        ("d", "decimal"),
        ("dt", "date"),
        ("s", "utf8"),
    ],
}

# Default rows per table — small enough that 200 queries stay well under 60s.
DEFAULT_ROWS_PER_TABLE: Final[int] = 16

# Per-cell null draw probability. ``row_id`` is never null (ORDER BY tiebreaker),
# so the *effective* overall density is a bit lower than this draw rate; 0.18 keeps
# measured density (including row_id) ≥ 0.10 on every v1 table (charter floor).
NULL_DENSITY: Final[float] = 0.18

STRING_POOL: Final[tuple[str, ...]] = (
    "alpha",
    "beta",
    "gamma",
    "delta",
    "x",
    "y",
    "zz",
    "hello",
    "world",
    "",
)


class FuzzTable(BaseModel):
    """One generated table: name, column types, and row tuples."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    columns: tuple[tuple[str, ColumnType], ...]
    rows: tuple[tuple[Any, ...], ...]

    @property
    def column_names(self) -> list[str]:
        return [name for name, _ in self.columns]

    @property
    def column_types(self) -> dict[str, ColumnType]:
        return dict(self.columns)


class FuzzDatabase(BaseModel):
    """Full multi-table fixture produced from a seed."""

    model_config = ConfigDict(extra="forbid")

    seed: int
    tables: dict[str, FuzzTable] = Field(default_factory=dict)

    def table(self, name: str) -> FuzzTable:
        return self.tables[name]


def generate_database(
    seed: int,
    *,
    rows_per_table: int = DEFAULT_ROWS_PER_TABLE,
    null_density: float = NULL_DENSITY,
) -> FuzzDatabase:
    """Build deterministic tables from ``seed`` (no wall-clock entropy)."""
    if rows_per_table < 1:
        msg = f"rows_per_table must be >= 1; got {rows_per_table}"
        raise ValueError(msg)
    if not 0.0 <= null_density < 1.0:
        msg = f"null_density must be in [0, 1); got {null_density}"
        raise ValueError(msg)

    rng = random.Random(seed)
    database = FuzzDatabase(seed=seed)
    for table_name in TABLE_NAMES:
        schema = TABLE_SCHEMAS[table_name]
        rows = _generate_rows(rng, schema, rows_per_table, null_density)
        rows = _enforce_null_floor(rng, schema, rows, floor=0.10)
        database.tables[table_name] = FuzzTable(
            name=table_name,
            columns=tuple(schema),
            rows=tuple(rows),
        )
    return database


def _enforce_null_floor(
    rng: random.Random,
    schema: list[tuple[str, ColumnType]],
    rows: list[tuple[Any, ...]],
    *,
    floor: float,
) -> list[tuple[Any, ...]]:
    """Flip additional non-row_id cells to NULL until overall density ≥ floor."""
    if not rows or not schema:
        return rows
    total = len(rows) * len(schema)
    target = math.ceil(floor * total)
    mutable = [list(row) for row in rows]
    nulls = sum(1 for row in mutable for cell in row if cell is None)
    if nulls >= target:
        return [tuple(row) for row in mutable]

    candidates: list[tuple[int, int]] = []
    for row_index, row in enumerate(mutable):
        for col_index, (col_name, _) in enumerate(schema):
            if col_name == "row_id":
                continue
            if row[col_index] is not None:
                candidates.append((row_index, col_index))
    rng.shuffle(candidates)
    for row_index, col_index in candidates:
        if nulls >= target:
            break
        mutable[row_index][col_index] = None
        nulls += 1
    return [tuple(row) for row in mutable]


def _generate_rows(
    rng: random.Random,
    schema: list[tuple[str, ColumnType]],
    row_count: int,
    null_density: float,
) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for row_index in range(row_count):
        cells: list[Any] = []
        for col_index, (col_name, col_type) in enumerate(schema):
            # Unique non-null row identity — never null (ORDER BY / LIMIT tiebreaker).
            if col_name == "row_id":
                cells.append(int(row_index))
                continue
            # Join key ``id`` stays dense and mostly non-null so joins are meaningful;
            # still inject nulls at the charter density floor on a fraction of rows.
            if col_name == "id":
                if rng.random() < null_density * 0.5:
                    cells.append(None)
                else:
                    # Overlapping id space across tables for INNER/LEFT joins.
                    cells.append(int(rng.randint(0, max(3, row_count // 2))))
                continue
            if rng.random() < null_density:
                cells.append(None)
            else:
                cells.append(_sample_value(rng, col_type, row_index=row_index, col_index=col_index))
        rows.append(tuple(cells))
    return rows


def _sample_value(
    rng: random.Random,
    col_type: ColumnType,
    *,
    row_index: int,
    col_index: int,
) -> Any:
    """Draw a finite, shared-dialect value. Never NaN/Inf."""
    del row_index, col_index  # reserved for future structured patterns
    if col_type == "int32":
        return int(rng.randint(-50, 50))
    if col_type == "int64":
        return int(rng.randint(-10_000, 10_000))
    if col_type == "float64":
        # Finite only; no NaN/Inf (NaN ordering exclusion — map.md EXCLUSIONS).
        # Prefer values with limited fractional bits so AVG remains stable under 1e-6.
        return float(rng.randint(-100, 100)) + float(rng.choice((0.0, 0.25, 0.5, 0.75)))
    if col_type == "decimal":
        cents = rng.randint(-10_000, 10_000)
        return Decimal(cents) / Decimal(100)
    if col_type == "utf8":
        return rng.choice(STRING_POOL)
    if col_type == "date":
        base = date(2020, 1, 1)
        return base + timedelta(days=rng.randint(0, 800))
    if col_type == "timestamp":
        base = datetime(2020, 1, 1, 0, 0, 0)
        return base + timedelta(hours=rng.randint(0, 20_000), minutes=rng.choice((0, 15, 30, 45)))
    if col_type == "bool":
        return bool(rng.choice((True, False)))
    msg = f"unknown column type: {col_type!r}"
    raise ValueError(msg)


def null_density_of(table: FuzzTable) -> float:
    """Fraction of NULL cells — used by unit pins to enforce the ≥10% floor."""
    if not table.rows or not table.columns:
        return 0.0
    total = len(table.rows) * len(table.columns)
    nulls = sum(1 for row in table.rows for cell in row if cell is None)
    return nulls / float(total)


def null_density_nullable_columns(table: FuzzTable) -> float:
    """NULL fraction excluding the non-null ``row_id`` identity column."""
    if not table.rows:
        return 0.0
    nullable_indices = [i for i, (name, _) in enumerate(table.columns) if name != "row_id"]
    if not nullable_indices:
        return 0.0
    total = len(table.rows) * len(nullable_indices)
    nulls = sum(1 for row in table.rows for i in nullable_indices if row[i] is None)
    return nulls / float(total)

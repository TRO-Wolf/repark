"""Greedy query + data minimizer for differential divergences.

On a WRONG-RESULT (or dual-ERROR with different messages is *not* minimized —
only value divergences), shrink by:

1. Drop LIMIT
2. Drop ORDER BY items
3. Drop WHERE
4. Drop join arms (rightmost first)
5. Drop SELECT columns (rightmost first, keep ≥1)
6. Drop GROUP BY keys (and matching select projections)
7. Drop trailing rows from every table (binary-ish greedy)

Each candidate is re-executed on both engines; a shrink is kept only when the
divergence **persists**. Returns the smallest QuerySpec + FuzzDatabase that
still diverges, plus the compare message.
"""

from __future__ import annotations

import copy
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .compare import CompareResult, compare_result_sets
from .datagen import FuzzDatabase, FuzzTable
from .generator import GeneratedQuery, QuerySpec


@dataclass
class MinimizedRepro:
    """Minimal divergence witness."""

    seed: int
    query_index: int
    sql: str
    spec: QuerySpec
    database: FuzzDatabase
    compare_message: str
    repark_rows: list[tuple[Any, ...]]
    duckdb_rows: list[tuple[Any, ...]]
    steps: int


ExecuteFn = Callable[
    [str, FuzzDatabase, bool],
    tuple[list[tuple[Any, ...]] | None, list[tuple[Any, ...]] | None, str | None, str | None],
]
"""(sql, db, order_sensitive) → (repark|None, duck|None, repark_err|None, duck_err|None)."""


def minimize_divergence(
    *,
    seed: int,
    query: GeneratedQuery,
    database: FuzzDatabase,
    execute: ExecuteFn,
    max_steps: int = 200,
) -> MinimizedRepro | None:
    """Shrink a diverging query. Returns None if the divergence cannot be reconfirmed."""
    spec = copy.deepcopy(query.spec)
    db = _clone_database(database)

    def still_diverges(
        candidate_spec: QuerySpec,
        candidate_db: FuzzDatabase,
    ) -> tuple[bool, CompareResult | None, list[tuple[Any, ...]], list[tuple[Any, ...]]]:
        sql = candidate_spec.render()
        repark_rows, duck_rows, repark_err, duck_err = execute(
            sql,
            candidate_db,
            candidate_spec.has_order_by,
        )
        if repark_err is not None or duck_err is not None:
            # Dual error or single-sided error: only keep if both succeeded before
            # and now one side errors differently — treat as non-value divergence;
            # do not accept shrinks that turn WRONG-RESULT into ERROR.
            return False, None, [], []
        if repark_rows is None or duck_rows is None:
            return False, None, [], []
        result = compare_result_sets(
            repark_rows,
            duck_rows,
            order_sensitive=candidate_spec.has_order_by,
        )
        return (
            not result.equal,
            result,
            repark_rows,
            duck_rows,
        )

    ok, base_result, base_repark, base_duck = still_diverges(spec, db)
    if not ok or base_result is None:
        return None

    steps = 0
    repark_rows = base_repark
    duck_rows = base_duck
    compare_message = base_result.message

    # 1. Drop LIMIT
    if spec.limit is not None and steps < max_steps:
        trial = copy.deepcopy(spec)
        trial.limit = None
        steps += 1
        diverges, result, rp, dk = still_diverges(trial, db)
        if diverges and result is not None:
            spec = trial
            repark_rows, duck_rows, compare_message = rp, dk, result.message

    # 2. Drop ORDER BY items **leftmost-first** so generator-appended tiebreakers
    # (``row_id`` / ``ord_tie``) stay longer than user keys (C1-L-002). Dropping
    # rightmost-first removed total-order keys while LIMIT remained and could
    # invent non-deterministic false divergences. If ORDER BY becomes empty while
    # LIMIT remains, also clear LIMIT (LIMIT without ORDER BY is non-deterministic).
    while spec.order_by and steps < max_steps:
        trial = copy.deepcopy(spec)
        trial.order_by = list(trial.order_by[1:])
        if not trial.order_by and trial.limit is not None:
            trial.limit = None
        steps += 1
        diverges, result, rp, dk = still_diverges(trial, db)
        if diverges and result is not None:
            spec = trial
            repark_rows, duck_rows, compare_message = rp, dk, result.message
        else:
            break

    # 3. Drop WHERE
    if spec.where_sql is not None and steps < max_steps:
        trial = copy.deepcopy(spec)
        trial.where_sql = None
        steps += 1
        diverges, result, rp, dk = still_diverges(trial, db)
        if diverges and result is not None:
            spec = trial
            repark_rows, duck_rows, compare_message = rp, dk, result.message

    # 4. Drop join arms rightmost-first
    while spec.joins and steps < max_steps:
        trial = copy.deepcopy(spec)
        trial.joins = list(trial.joins[:-1])
        # Drop select exprs that reference the dropped table.
        dropped = spec.joins[-1].right_table
        kept_exprs: list[str] = []
        kept_aliases: list[str] = []
        removed_aliases: list[str] = []
        for expr, alias in zip(trial.select_exprs, trial.select_aliases, strict=True):
            if f"{dropped}." in expr:
                removed_aliases.append(alias)
                continue
            kept_exprs.append(expr)
            kept_aliases.append(alias)
        if not kept_exprs:
            break
        trial.select_exprs = kept_exprs
        trial.select_aliases = kept_aliases
        # Scrub ORDER BY aliases that vanished with the dropped projections.
        removed_set = set(removed_aliases)
        trial.order_by = [
            item
            for item in trial.order_by
            if item.expr not in removed_set and f"{dropped}." not in item.expr
        ]
        if not trial.order_by and trial.limit is not None:
            trial.limit = None
        # WHERE may reference the dropped table — clear it (C6-L-001).
        if trial.where_sql is not None and f"{dropped}." in trial.where_sql:
            trial.where_sql = None
        steps += 1
        diverges, result, rp, dk = still_diverges(trial, db)
        if diverges and result is not None:
            spec = trial
            repark_rows, duck_rows, compare_message = rp, dk, result.message
        else:
            break

    # 5. Drop SELECT columns rightmost-first (keep ≥1). Scrub ORDER BY items that
    # referenced the dropped alias so the candidate stays valid SQL (C2-L-001).
    while len(spec.select_exprs) > 1 and steps < max_steps:
        trial = copy.deepcopy(spec)
        dropped_alias = trial.select_aliases[-1]
        trial.select_exprs = list(trial.select_exprs[:-1])
        trial.select_aliases = list(trial.select_aliases[:-1])
        trial.order_by = [item for item in trial.order_by if item.expr != dropped_alias]
        if not trial.order_by and trial.limit is not None:
            trial.limit = None
        # If group_by references a dropped alias projection, leave group_by as-is
        # (group keys are fully-qualified table.col, not aliases).
        steps += 1
        diverges, result, rp, dk = still_diverges(trial, db)
        if diverges and result is not None:
            spec = trial
            repark_rows, duck_rows, compare_message = rp, dk, result.message
        else:
            break

    # 6. Drop GROUP BY keys rightmost-first (and matching select projections + ORDER BY).
    while spec.group_by and steps < max_steps:
        trial = copy.deepcopy(spec)
        dropped_key = trial.group_by[-1]
        trial.group_by = list(trial.group_by[:-1])
        # Remove matching select projection if present.
        new_exprs: list[str] = []
        new_aliases: list[str] = []
        removed_aliases: list[str] = []
        for expr, alias in zip(trial.select_exprs, trial.select_aliases, strict=True):
            if expr == dropped_key:
                removed_aliases.append(alias)
                continue
            new_exprs.append(expr)
            new_aliases.append(alias)
        if not new_exprs:
            break
        trial.select_exprs = new_exprs
        trial.select_aliases = new_aliases
        removed = set(removed_aliases)
        removed.add(dropped_key)
        trial.order_by = [item for item in trial.order_by if item.expr not in removed]
        if not trial.order_by and trial.limit is not None:
            trial.limit = None
        steps += 1
        diverges, result, rp, dk = still_diverges(trial, db)
        if diverges and result is not None:
            spec = trial
            repark_rows, duck_rows, compare_message = rp, dk, result.message
        else:
            break

    # 7. Drop trailing rows from each table greedily
    for table_name in list(db.tables.keys()):
        table = db.tables[table_name]
        while len(table.rows) > 1 and steps < max_steps:
            trial_db = _clone_database(db)
            old = trial_db.tables[table_name]
            trial_db.tables[table_name] = FuzzTable(
                name=old.name,
                columns=old.columns,
                rows=old.rows[:-1],
            )
            steps += 1
            diverges, result, rp, dk = still_diverges(spec, trial_db)
            if diverges and result is not None:
                db = trial_db
                table = db.tables[table_name]
                repark_rows, duck_rows, compare_message = rp, dk, result.message
            else:
                break

    return MinimizedRepro(
        seed=seed,
        query_index=query.index,
        sql=spec.render(),
        spec=spec,
        database=db,
        compare_message=compare_message,
        repark_rows=repark_rows,
        duckdb_rows=duck_rows,
        steps=steps,
    )


def _clone_database(database: FuzzDatabase) -> FuzzDatabase:
    tables = {
        name: FuzzTable(name=table.name, columns=table.columns, rows=table.rows)
        for name, table in database.tables.items()
    }
    return FuzzDatabase(seed=database.seed, tables=tables)

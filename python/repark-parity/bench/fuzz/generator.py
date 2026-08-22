"""Seeded SQL query generator — shared-dialect subset only (v1).

In scope: SELECT projections/arithmetic/CASE/casts, WHERE nested boolean,
GROUP BY + common aggregates, ORDER BY NULLS FIRST/LAST, LIMIT, INNER/LEFT
joins ≤3 tables, scalar subqueries.

Out of scope (v2 seeds): window frames, set ops, laterals.

Exclusions (generator-side — full rationale in map.md EXCLUSIONS):

- No float64 columns in SUM/AVG (float aggregation order / non-associativity).
- No CAST(float → int) (DuckDB rounds half-up; Spark truncates toward zero).
- No CAST(decimal|timestamp|bool|float → VARCHAR) (engine-specific string formats).
- No NaN/Inf literals or expressions that produce them.
- No division by bare integer columns without a non-zero guard (avoid engine-specific
  div-by-zero error shapes polluting the differential signal).
- LIMIT always pairs with ORDER BY; ORDER BY always ends with ``row_id`` tiebreaker
  so tied keys cannot produce engine-specific LIMIT survivors.
"""

from __future__ import annotations

import random
from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from .datagen import TABLE_NAMES, TABLE_SCHEMAS, ColumnType, FuzzDatabase

DEFAULT_SEED: Final[int] = 42
SMOKE_QUERY_COUNT: Final[int] = 200

JoinKind = Literal["INNER", "LEFT"]
AggName = Literal["COUNT", "SUM", "AVG", "MIN", "MAX"]


class JoinClause(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: JoinKind
    right_table: str
    left_key: str
    right_key: str


class OrderItem(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    expr: str
    direction: Literal["ASC", "DESC"]
    nulls: Literal["FIRST", "LAST"] | None


class QuerySpec(BaseModel):
    """Structured query AST — used for rendering and greedy minimization."""

    model_config = ConfigDict(extra="forbid")

    index: int
    kind: Literal["project", "aggregate", "join"]
    from_table: str
    select_exprs: list[str] = Field(default_factory=list)
    select_aliases: list[str] = Field(default_factory=list)
    joins: list[JoinClause] = Field(default_factory=list)
    where_sql: str | None = None
    group_by: list[str] = Field(default_factory=list)
    order_by: list[OrderItem] = Field(default_factory=list)
    limit: int | None = None

    @property
    def has_order_by(self) -> bool:
        return bool(self.order_by)

    def render(self) -> str:
        """Render to a single SQL string shared by RePark and DuckDB."""
        select_parts: list[str] = []
        for expr, alias in zip(self.select_exprs, self.select_aliases, strict=True):
            select_parts.append(f"{expr} AS {alias}")
        select_sql = ", ".join(select_parts) if select_parts else "1 AS one"
        sql = f"SELECT {select_sql} FROM {self.from_table}"
        for join in self.joins:
            sql += (
                f" {join.kind} JOIN {join.right_table}"
                f" ON {self.from_table}.{join.left_key} = {join.right_table}.{join.right_key}"
            )
        if self.where_sql:
            sql += f" WHERE {self.where_sql}"
        if self.group_by:
            sql += " GROUP BY " + ", ".join(self.group_by)
        if self.order_by:
            order_parts: list[str] = []
            for item in self.order_by:
                part = f"{item.expr} {item.direction}"
                if item.nulls is not None:
                    part += f" NULLS {item.nulls}"
                order_parts.append(part)
            sql += " ORDER BY " + ", ".join(order_parts)
        if self.limit is not None:
            sql += f" LIMIT {int(self.limit)}"
        return sql


class GeneratedQuery(BaseModel):
    """One generated query with its index, SQL text, and AST."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    index: int
    sql: str
    spec: QuerySpec

    @property
    def has_order_by(self) -> bool:
        return self.spec.has_order_by


def generate_queries(
    seed: int,
    count: int,
    *,
    database: FuzzDatabase | None = None,
) -> list[GeneratedQuery]:
    """Produce ``count`` queries deterministically from ``seed``.

    The RNG stream is: one ``Random(seed)`` for the whole batch; each query
    consumes from that stream in order. Same seed + count → byte-identical SQL.
    """
    if count < 0:
        msg = f"count must be >= 0; got {count}"
        raise ValueError(msg)
    rng = random.Random(seed)
    # database is optional metadata for column typing; schemas are static.
    del database
    queries: list[GeneratedQuery] = []
    for index in range(count):
        spec = _generate_one(rng, index)
        queries.append(GeneratedQuery(index=index, sql=spec.render(), spec=spec))
    return queries


def _generate_one(rng: random.Random, index: int) -> QuerySpec:
    kind_roll = rng.random()
    if kind_roll < 0.40:
        return _gen_project(rng, index)
    if kind_roll < 0.70:
        return _gen_aggregate(rng, index)
    return _gen_join(rng, index)


# ---------------------------------------------------------------------------
# Column helpers
# ---------------------------------------------------------------------------


def _cols(table: str) -> list[tuple[str, ColumnType]]:
    return list(TABLE_SCHEMAS[table])


def _cols_of_types(table: str, allowed: set[str]) -> list[str]:
    return [name for name, col_type in _cols(table) if col_type in allowed]


def _qual(table: str, col: str) -> str:
    return f"{table}.{col}"


def _pick_table(rng: random.Random) -> str:
    return rng.choice(list(TABLE_NAMES))


# ---------------------------------------------------------------------------
# Expression builders (shared-dialect only)
# ---------------------------------------------------------------------------


def _numeric_expr(rng: random.Random, table: str, *, allow_float: bool = True) -> str:
    """Build a numeric projection expression (no float SUM/AVG — that is separate)."""
    types: set[str] = {"int32", "int64", "decimal"}
    if allow_float:
        types.add("float64")
    candidates = _cols_of_types(table, types)
    if not candidates:
        return "1"
    col = rng.choice(candidates)
    base = _qual(table, col)
    roll = rng.random()
    if roll < 0.25:
        return base
    if roll < 0.45:
        lit = rng.randint(1, 5)
        op = rng.choice(["+", "-", "*"])
        return f"({base} {op} {lit})"
    if roll < 0.60:
        # Safe division: divisor is a non-zero literal so engines agree.
        lit = rng.choice([2, 3, 4, 5])
        return f"(CAST({base} AS DOUBLE) / {lit}.0)"
    if roll < 0.80:
        return _case_numeric(rng, table, base)
    return _cast_expr(rng, table, col)


def _case_numeric(rng: random.Random, table: str, base: str) -> str:
    lit = rng.randint(0, 10)
    then_v = rng.randint(-5, 5)
    else_v = rng.randint(-5, 5)
    return (
        f"(CASE WHEN {base} IS NULL THEN {then_v} "
        f"WHEN {base} > {lit} THEN {else_v} ELSE {base} END)"
    )


def _cast_expr(rng: random.Random, table: str, col: str) -> str:
    """Safe casts only — never float→int; never format-sensitive →VARCHAR.

    VARCHAR casts of decimal/timestamp/bool/float differ in spelling across
    engines (padding, ``T`` vs space, true/false case) — excluded (map.md).
    """
    type_by_name = dict(TABLE_SCHEMAS[table])
    col_type = type_by_name[col]
    base = _qual(table, col)
    if col_type in {"int32", "int64"}:
        # INT→VARCHAR is stable ("12"); DECIMAL/DOUBLE are numeric compares.
        target = rng.choice(["BIGINT", "DOUBLE", "DECIMAL(12,2)", "VARCHAR"])
        return f"CAST({base} AS {target})"
    if col_type == "decimal":
        target = rng.choice(["DOUBLE", "DECIMAL(12,2)"])
        return f"CAST({base} AS {target})"
    if col_type == "float64":
        # DOUBLE only — NOT INT (rounding) and NOT VARCHAR (format).
        return f"CAST({base} AS DOUBLE)"
    if col_type == "date":
        # Date→VARCHAR is ISO-stable across both engines in practice.
        return f"CAST({base} AS VARCHAR)"
    if col_type in {"timestamp", "bool", "utf8"}:
        # timestamp/bool VARCHAR formats diverge; utf8 cast is a no-op.
        return base
    return base


def _predicate(rng: random.Random, table: str, *, depth: int = 0) -> str:
    """Nested boolean / null-propagating predicate."""
    if depth >= 2 or rng.random() < 0.55:
        return _atomic_predicate(rng, table)
    left = _predicate(rng, table, depth=depth + 1)
    right = _predicate(rng, table, depth=depth + 1)
    op = rng.choice(["AND", "OR"])
    if rng.random() < 0.15:
        return f"(NOT ({left}))"
    return f"({left} {op} {right})"


def _atomic_predicate(rng: random.Random, table: str) -> str:
    cols = _cols(table)
    name, col_type = rng.choice(cols)
    qual = _qual(table, name)
    roll = rng.random()
    if roll < 0.20:
        return f"{qual} IS NULL" if rng.random() < 0.5 else f"{qual} IS NOT NULL"
    if col_type in {"int32", "int64", "float64", "decimal"}:
        lit = rng.randint(-20, 20)
        op = rng.choice(["=", "<>", "<", ">", "<=", ">="])
        return f"{qual} {op} {lit}"
    if col_type == "bool":
        return f"{qual} = {rng.choice(['true', 'false'])}"
    if col_type == "utf8":
        sample = rng.choice(["alpha", "beta", "x", "y", "hello", "%a%"])
        if "%" in sample:
            return f"{qual} LIKE '{sample}'"
        return f"{qual} = '{sample}'"
    if col_type == "date":
        return f"{qual} > DATE '2020-06-01'"
    if col_type == "timestamp":
        return f"{qual} > TIMESTAMP '2020-06-01 00:00:00'"
    return f"{qual} IS NOT NULL"


def _scalar_subquery_predicate(rng: random.Random, outer: str) -> str:
    """``outer.col OP (SELECT agg FROM other)`` — uncorrelated scalar subquery."""
    other = rng.choice([name for name in TABLE_NAMES if name != outer] or [outer])
    # SUM/AVG only on integral / decimal columns (float-agg exclusion).
    summable = _cols_of_types(other, {"int32", "int64", "decimal"})
    if not summable:
        return f"{outer}.id IS NOT NULL"
    col = rng.choice(summable)
    agg = rng.choice(["MIN", "MAX", "SUM", "COUNT"])
    if agg == "COUNT":
        sub = f"(SELECT COUNT({other}.{col}) FROM {other})"
        outer_col = rng.choice(_cols_of_types(outer, {"int32", "int64"}) or ["id"])
        return f"{outer}.{outer_col} < {sub}"
    if agg == "SUM":
        sub = f"(SELECT SUM({other}.{col}) FROM {other})"
    else:
        sub = f"(SELECT {agg}({other}.{col}) FROM {other})"
    outer_num = _cols_of_types(outer, {"int32", "int64", "decimal"})
    if not outer_num:
        return f"{outer}.id IS NOT NULL"
    outer_col = rng.choice(outer_num)
    op = rng.choice([">", "<", ">=", "<="])
    return f"{outer}.{outer_col} {op} {sub}"


# ---------------------------------------------------------------------------
# Query shapes
# ---------------------------------------------------------------------------


def _gen_project(rng: random.Random, index: int) -> QuerySpec:
    table = _pick_table(rng)
    n_proj = rng.randint(1, 4)
    exprs: list[str] = []
    aliases: list[str] = []
    for proj_i in range(n_proj):
        roll = rng.random()
        if roll < 0.55:
            exprs.append(_numeric_expr(rng, table))
        elif roll < 0.75:
            cols = [name for name, _ in _cols(table)]
            col = rng.choice(cols)
            exprs.append(_qual(table, col))
        else:
            cols = [name for name, _ in _cols(table)]
            col = rng.choice(cols)
            exprs.append(_cast_expr(rng, table, col))
        aliases.append(f"c{proj_i}")

    where_sql: str | None = None
    if rng.random() < 0.55:
        if rng.random() < 0.25:
            where_sql = _scalar_subquery_predicate(rng, table)
        else:
            where_sql = _predicate(rng, table)

    order_by: list[OrderItem] = []
    if rng.random() < 0.45:
        order_by = _make_order(rng, table, aliases, exprs)

    limit: int | None = None
    if rng.random() < 0.40:
        limit = rng.randint(1, 8)
        # LIMIT without ORDER BY is non-deterministic across engines — force order.
        if not order_by:
            order_by = _make_order(rng, table, aliases, exprs)

    return QuerySpec(
        index=index,
        kind="project",
        from_table=table,
        select_exprs=exprs,
        select_aliases=aliases,
        where_sql=where_sql,
        order_by=order_by,
        limit=limit,
    )


def _gen_aggregate(rng: random.Random, index: int) -> QuerySpec:
    table = _pick_table(rng)
    # Group keys: prefer low-cardinality-ish columns.
    group_candidates = _cols_of_types(table, {"int32", "int64", "bool", "utf8", "date"})
    n_group = rng.randint(0, min(2, len(group_candidates)))
    group_by: list[str] = []
    exprs: list[str] = []
    aliases: list[str] = []
    if n_group > 0:
        chosen = rng.sample(group_candidates, n_group)
        for group_i, col in enumerate(chosen):
            qual = _qual(table, col)
            group_by.append(qual)
            exprs.append(qual)
            aliases.append(f"g{group_i}")

    n_agg = rng.randint(1, 3)
    for agg_i in range(n_agg):
        agg_expr, alias = _agg_expr(rng, table, agg_i)
        exprs.append(agg_expr)
        aliases.append(alias)

    where_sql: str | None = None
    if rng.random() < 0.35:
        where_sql = _predicate(rng, table)

    order_by: list[OrderItem] = []
    if rng.random() < 0.40 and aliases:
        # ORDER BY alias is widely accepted; NULLS always explicit.
        alias = rng.choice(aliases)
        order_by.append(
            OrderItem(
                expr=alias,
                direction=rng.choice(["ASC", "DESC"]),
                nulls=rng.choice(["FIRST", "LAST"]),
            )
        )
        # Append remaining aliases as tiebreakers for a total order on the group set.
        for extra in aliases:
            if all(item.expr != extra for item in order_by):
                order_by.append(OrderItem(expr=extra, direction="ASC", nulls="LAST"))

    limit: int | None = None
    if rng.random() < 0.35:
        limit = rng.randint(1, 10)
        if not order_by and aliases:
            for alias in aliases:
                order_by.append(OrderItem(expr=alias, direction="ASC", nulls="LAST"))

    # Total-order tiebreaker after GROUP BY: aliases alone can still tie when two
    # groups share identical projected values. MIN(row_id) is unique per base row
    # and stable per group (C1-L-001). Required whenever ORDER BY or LIMIT is set.
    if order_by or limit is not None:
        tie_alias = "ord_tie"
        if tie_alias not in aliases:
            exprs.append(f"MIN({_qual(table, 'row_id')})")
            aliases.append(tie_alias)
        if all(item.expr != tie_alias for item in order_by):
            order_by.append(OrderItem(expr=tie_alias, direction="ASC", nulls="LAST"))
        if limit is not None and not order_by:
            order_by.append(OrderItem(expr=tie_alias, direction="ASC", nulls="LAST"))

    return QuerySpec(
        index=index,
        kind="aggregate",
        from_table=table,
        select_exprs=exprs,
        select_aliases=aliases,
        where_sql=where_sql,
        group_by=group_by,
        order_by=order_by,
        limit=limit,
    )


def _agg_expr(rng: random.Random, table: str, agg_i: int) -> tuple[str, str]:
    """Return (sql, alias). SUM/AVG never touch float64 (exclusion)."""
    agg: AggName = rng.choice(["COUNT", "SUM", "AVG", "MIN", "MAX"])
    if agg == "COUNT":
        if rng.random() < 0.4:
            return "COUNT(*)", f"agg{agg_i}"
        cols = [name for name, _ in _cols(table)]
        col = rng.choice(cols)
        return f"COUNT({_qual(table, col)})", f"agg{agg_i}"
    if agg in {"SUM", "AVG"}:
        summable = _cols_of_types(table, {"int32", "int64", "decimal"})
        if not summable:
            return "COUNT(*)", f"agg{agg_i}"
        col = rng.choice(summable)
        return f"{agg}({_qual(table, col)})", f"agg{agg_i}"
    # MIN / MAX — floats OK for MIN/MAX (not for SUM/AVG).
    ordered_types = {"int32", "int64", "decimal", "float64", "utf8", "date", "timestamp"}
    ordered = _cols_of_types(table, ordered_types)
    if not ordered:
        return "COUNT(*)", f"agg{agg_i}"
    col = rng.choice(ordered)
    return f"{agg}({_qual(table, col)})", f"agg{agg_i}"


def _gen_join(rng: random.Random, index: int) -> QuerySpec:
    left = _pick_table(rng)
    remaining = [name for name in TABLE_NAMES if name != left]
    n_joins = rng.randint(1, min(2, len(remaining)))
    rights = rng.sample(remaining, n_joins)
    joins: list[JoinClause] = []
    for right in rights:
        kind: JoinKind = rng.choice(["INNER", "LEFT"])
        joins.append(
            JoinClause(
                kind=kind,
                right_table=right,
                left_key="id",
                right_key="id",
            )
        )

    # Projections from left + first right.
    exprs: list[str] = []
    aliases: list[str] = []
    left_cols = [name for name, _ in _cols(left)]
    for proj_i in range(rng.randint(1, 3)):
        col = rng.choice(left_cols)
        exprs.append(_qual(left, col))
        aliases.append(f"c{proj_i}")
    if rights:
        right = rights[0]
        right_cols = [name for name, _ in _cols(right) if name != "id"]
        if right_cols and rng.random() < 0.8:
            col = rng.choice(right_cols)
            exprs.append(_qual(right, col))
            aliases.append("r0")

    where_sql: str | None = None
    if rng.random() < 0.40:
        where_sql = _predicate(rng, left)

    right_tables = [join.right_table for join in joins]
    order_by: list[OrderItem] = []
    if rng.random() < 0.35:
        order_by = _make_order(rng, left, aliases, exprs, extra_tables=right_tables)

    limit: int | None = None
    if rng.random() < 0.40:
        limit = rng.randint(1, 8)
        if not order_by:
            order_by = _make_order(rng, left, aliases, exprs, extra_tables=right_tables)

    return QuerySpec(
        index=index,
        kind="join",
        from_table=left,
        select_exprs=exprs,
        select_aliases=aliases,
        joins=joins,
        where_sql=where_sql,
        order_by=order_by,
        limit=limit,
    )


def _make_order(
    rng: random.Random,
    table: str,
    aliases: list[str],
    exprs: list[str],
    *,
    extra_tables: list[str] | None = None,
) -> list[OrderItem]:
    """ORDER BY alias or base column, with **explicit** NULLS FIRST/LAST.

    Always terminates with ``table.row_id ASC`` (and any join-partner row_ids)
    so the order is a total order and LIMIT cannot pick engine-specific tied
    survivors. NULLS is never omitted — Spark and DuckDB disagree on the
    default NULLS placement for ASC/DESC.
    """
    del exprs
    items: list[OrderItem] = []
    n = rng.randint(1, min(2, max(1, len(aliases))))
    # Prefer aliases (stable across engines); fall back to a typed column.
    for order_i in range(n):
        if aliases and rng.random() < 0.7:
            expr = aliases[order_i % len(aliases)]
        else:
            # Avoid ORDER BY float columns when possible (NaN-free data, but still).
            non_float = [
                name
                for name in _cols_of_types(
                    table, {"int32", "int64", "decimal", "utf8", "date", "bool"}
                )
                if name != "row_id"
            ]
            if non_float:
                expr = _qual(table, rng.choice(non_float))
            else:
                expr = aliases[0] if aliases else "1"
        items.append(
            OrderItem(
                expr=expr,
                direction=rng.choice(["ASC", "DESC"]),
                # Always explicit — default NULLS placement diverges (map.md).
                nulls=rng.choice(["FIRST", "LAST"]),
            )
        )
    # Total-order tiebreakers (never null, unique per base-table row; for joins
    # include every participating table's row_id so the join multiset is ordered).
    items.append(OrderItem(expr=_qual(table, "row_id"), direction="ASC", nulls="LAST"))
    for extra in extra_tables or []:
        items.append(OrderItem(expr=_qual(extra, "row_id"), direction="ASC", nulls="LAST"))
    return items

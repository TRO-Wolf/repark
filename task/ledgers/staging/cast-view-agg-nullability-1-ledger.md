# Unit ledger — CAST-VIEW-AGG-NULLABILITY-1 · `max` over a view of a cast arithmetic column raises an internal schema error

**Date:** 2026-10-08 · **Branch:** `fix/cast-view-agg-nullability-1` · **Base:** `40fc916f` (`main`)
**Model:** Muse Spark (muse-spark-1.3-contributor) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's product PR merges.

**Card:** [../../roadmap/mid-term/cast-view-agg-nullability-1-card-2026-10-08.md](../../roadmap/mid-term/cast-view-agg-nullability-1-card-2026-10-08.md).

**Why now.** `SELECT max(ts) FROM t` over a view of
`CAST(946684800 + (id * 1577) % 3000000000 AS TIMESTAMP)` from `range(10)` raises
`Physical input schema should be the same as the one converted from logical input schema`
(`(physical) true vs (logical) false` at `ts`), while `count(*)`, a plain select, and a
parquet roundtrip all answer. Spark 4.1.2 answers the query.

**Not in this unit:** `Cargo.toml` (no dependency change), `.github/`, `STATUS.md`,
`wrap_as_ntz` (R-1), `ntz_store.rs` (R-2).

## 0. Step 0 — shrink, Spark answers, site

Measured 2026-10-08 on base `40fc916f`. RePark ran at UTC; Spark 4.1.2
(`/tmp/sparkenv`, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`) ran at its session default
`America/New_York`, so wall clocks differ by zone while the instants agree (max `ts` is
epoch 946698993 on both).

### 0.1 Reduced forms

Round A (view `t` as shown, then the aggregate; `OK` = answers):

| Form | View definition | Aggregate | RePark | Spark value (type) |
|---|---|---|---|---|
| full_view_max | `CAST(946684800 + (id * 1577) % 3000000000 AS TIMESTAMP) AS ts FROM range(10)` | `max(ts)` | RAISES | 1999-12-31 22:56:33 EST (`TimestampType`, nullable) |
| full_noview_max | — | `max(CAST(…)) FROM range(10)` | OK 2000-01-01 03:56:33 | same instant (`TimestampType`, nullable) |
| no_cast_view_max | `946684800 + (id * 1577) % 3000000000 AS x` | `max(x)` | OK 946698993 | 946698993 (`LongType`, nullable) |
| no_mod_view_max | `CAST(946684800 + id * 1577 AS TIMESTAMP) AS ts` | `max(ts)` | OK 2000-01-01 03:56:33 | 1999-12-31 22:56:33 EST (`TimestampType`, nullable) |
| no_plus_view_max | `CAST((id * 1577) % 3000000000 AS TIMESTAMP) AS ts` | `max(ts)` | RAISES | 1969-12-31 22:56:33 EST (`TimestampType`, nullable) |
| no_mult_view_max | `CAST(946684800 + id % 3000000000 AS TIMESTAMP) AS ts` | `max(ts)` | RAISES | 1999-12-31 19:00:09 EST (`TimestampType`, nullable) |
| cast_only_view_max | `CAST(id AS TIMESTAMP) AS ts` | `max(ts)` | OK 1970-01-01 00:00:09 | 1969-12-31 19:00:09 EST (`TimestampType`, nullable) |
| cast_lit_view_max | `CAST(946684800 AS TIMESTAMP) AS ts` | `max(ts)` | OK 2000-01-01 00:00:00 | — (not run; literal shape) |
| arith_only_view_max | `(id * 2) % 7 AS x` | `max(x)` | OK 6 | 6 (`LongType`, nullable) |
| full_view_min | full view | `min(ts)` | RAISES | 1999-12-31 19:00:00 EST (`TimestampType`, nullable) |
| full_view_count | full view | `count(*)` | OK 10 | 10 (`LongType`, not null) |
| full_view_select | full view | `SELECT ts FROM t` | OK 10 rows | 10 rows (`TimestampType`, nullable) |
| cast_only_view_count | `CAST(id AS TIMESTAMP) AS ts` | `count(*)` | OK 10 | 10 (`LongType`, not null) |
| arith_only_view_count | `(id * 2) % 7 AS x` | `count(*)` | OK 10 | 10 (`LongType`, not null) |
| full_view_count_ts | full view | `count(ts)` | RAISES | 10 (`LongType`, not null) |

Round B (narrower):

| Form | View definition | Aggregate | RePark | Spark value (type) |
|---|---|---|---|---|
| mod_cast_ts | `CAST(id % 3 AS TIMESTAMP) AS ts FROM range(10)` | `max(ts)` | RAISES | 1969-12-31 19:00:02 EST (`TimestampType`, nullable) |
| litmod_cast_ts | `CAST(5 % 3 AS TIMESTAMP) AS ts FROM range(10)` | `max(ts)` | OK 1970-01-01 00:00:02 | 1969-12-31 19:00:02 EST (`TimestampType`, nullable) |
| mod_cast_bigint | `CAST(id % 3 AS BIGINT) AS x` | `max(x)` | OK 2 | 2 (`LongType`, nullable) |
| mod_cast_string | `CAST(id % 3 AS STRING) AS s` | `max(s)` | OK '2' | — (not run; non-timestamp target) |
| mod_cast_double | `CAST(id % 3 AS DOUBLE) AS x` | `max(x)` | OK 2.0 | — (not run; non-timestamp target) |
| mod_nocast | `id % 3 AS x` | `max(x)` | OK 2 | 2 (`LongType`, nullable) |
| div_cast_bigint | `CAST(id / 2 AS BIGINT) AS x` | `max(x)` | OK 4 | — (not run; division shape) |
| mod_cast_ts_noview | — | `max(CAST(id % 3 AS TIMESTAMP)) FROM range(10)` | OK 1970-01-01 00:00:02 | — (covered by full_noview_max) |
| values_mod_cast_ts | `CAST(x % 3 AS TIMESTAMP) AS ts FROM (VALUES (1),(2),(3)) v(x)` | `max(ts)` | OK 1970-01-01 00:00:02 | 1969-12-31 19:00:02 EST (`TimestampType`, nullable) |
| plus_cast_ts | `CAST(id + 3 AS TIMESTAMP) AS ts` | `max(ts)` | OK 1970-01-01 00:00:12 | 1969-12-31 19:00:12 EST (`TimestampType`, nullable) |
| mul_cast_ts | `CAST(id * 1577 AS TIMESTAMP) AS ts` | `max(ts)` | OK 1970-01-01 03:56:33 | — (not run; covered by no_mod_view_max) |
| parquet_roundtrip | full view written to parquet and read back | `max(ts)` | OK 2000-01-01 03:56:33 | — (RePark control from the card) |

Needed to raise: `%`, the `TIMESTAMP` cast, the view, an aggregate that reads the
column (`max`/`min`/`count(col)` raise; `count(*)` does not), and a non-nullable
`%` input (`range().id` raises; nullable `VALUES` does not). Not needed: `+`, `*`.
Minimal raising form: `mod_cast_ts`.

### 0.2 Plans for the minimal form

`EXPLAIN VERBOSE SELECT max(ts) FROM t` over the `mod_cast_ts` view:

- `initial_logical_plan` (analyzed): the `%` divisor carries the ANSI guard,
  `range().id % __repark_ansi_nonzero_divisor__(CAST(Int32(3) AS Int64))`, so the
  modulo and the `ts` cast are nullable on both sides (`df.schema` and the executed
  batch schema both say nullable).
- `spark_ltz_timestamp_cast` wraps the cast chain once more (4 casts total).
- `simplify_expressions` folds `__repark_ansi_nonzero_divisor__(3)` to `Int64(3)`.
  The optimized logical projection is non-nullable `ts`; the physical `ProjectionExec`
  built from the same plan reports nullable `ts`; the `AggregateExec` input check
  (`datafusion-54.1.0/src/physical_planner.rs`, `schema_satisfied_by`) raises.

### 0.3 Site

RePark code: `wrap_as_ltz` in
[`crates/repark-functions/src/instant_ts.rs`](../../../crates/repark-functions/src/instant_ts.rs)
(lines 681–685), reached through the `spark_ltz_timestamp_cast` analyzer rule for
`CAST(<integer> AS TIMESTAMP)`:

```rust
fn wrap_as_ltz(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let nullable = expr.nullable(schema).unwrap_or(true);
    let field = Arc::new(Field::new("ts", ltz_timestamp_type(), nullable));
    Transformed::yes(Expr::Cast(Cast::new_from_field(Box::new(expr), field)))
}
```

It builds the cast with `Cast::new_from_field` carrying a **named** (`"ts"`) field whose
nullability is sampled at analyzer time. DataFusion's logical side ignores that field
(`cast_output_field` in `datafusion-expr-54.1.0/src/expr_schema.rs` derives nullability
from the cast child), while the physical side honors it (`CastExpr::nullable` in
`datafusion-physical-expr-54.1.0/src/expressions/cast.rs` answers
child-nullable OR target-nullable whenever the target field is not the default
empty-named field). At analyzer time the guarded `%` is nullable, so the baked field
says nullable; after `simplify_expressions` folds the guard, logical says
non-nullable and physical still says nullable. The `"ts"` name dates to the TZ-4
commit (`c7e65890`) with no recorded consumer; no RePark code reads a cast field
name. Every control in §0.1 follows: no fold, no mismatch.

Owner: **repark**. DataFusion functions involved (`cast_output_field`,
`CastExpr::nullable`, `simplify_expressions`, the aggregate schema check) behave as
documented; the stale contract is baked on the RePark side.

## Clauses

| ID | Clause | Verdict | Evidence / proof obligation |
|---|---|---|---|
| C-001 | Step 0 shrink recorded: each reduced form and whether it raises; `%` + `TIMESTAMP` cast + view + column aggregate + non-nullable input needed, `+`/`*` not | OPEN | §0.1 tables; pins land with the fix |
| C-002 | Spark 4.1.2 values and types recorded for the full repro and the reduced forms | OPEN | §0.1 Spark column; full repro max is epoch 946698993 |
| C-003 | Site is RePark's `wrap_as_ltz`, with the logical/physical functions named | OPEN | §0.2–§0.3; `EXPLAIN VERBOSE` stage trace |
| C-004 | Fix at the site; full repro answers Spark's value and type; controls pinned; one pin red on the old behaviour for the named reason; gates green | OPEN | product commit + test file + gate runs below |

## Residues

- R-1 (2026-10-08): `wrap_as_ntz` (`instant_ts.rs`, the `TIMESTAMP_NTZ` conf path) shares
  the named-field pattern; untested here and out of the card's one shape. Candidate for a
  follow-up card.
- R-2 (2026-10-08): `repark-iceberg/src/write/ntz_store.rs` builds a cast with
  `Cast::new_from_field`; a DML store path, outside this shape, not assessed.

## Gates

No gate run yet; Step 0 only.

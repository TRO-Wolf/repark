# Unit ledger — CAST-VIEW-AGG-NULLABILITY-1 · `max` over a view of a cast arithmetic column raises an internal schema error

**Date:** 2026-10-08 · **Branch:** `fix/cast-view-agg-nullability-1` · **Base:** `40fc916f` (`main`)
**Model:** Muse Spark (muse-spark-1.3-contributor) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Fold 1 (2026-10-09):** S1 diagnosis — HALT (C-006); the S1 class reproduces
with no RePark code in the loop (Case 3); the upstream draft lives in §Fold 1.

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

## 1. Fix

`wrap_as_ltz` builds its cast with `Cast::new` (the default target field) instead of
`Cast::new_from_field` with the named `"ts"` field that sampled analyzer-time
nullability; `wrap_ns_literal` drops its now-unused schema parameter. No other
product line changes. No code comment added; the reason lives in
`crates/repark-functions/src/map.md`.

What moves: only the physical nullability of `CAST(<integer> AS TIMESTAMP)` shapes
whose child nullability changes after analysis — exactly the shapes that raised the
aggregate input check. The logical side is untouched (`to_field` never read the
baked field), so every analyzed schema is byte-identical before and after.

Neighbour comparison (all §0.1 forms, pre-fix vs post-fix): the 6 raising forms now
answer with Spark's instants and types (full max/min/count, no-plus, no-mult,
minimal); the 20 answering forms are byte-identical, including every Step 0
control. Zero moved answers.

Pins: `python/repark/tests/test_cast_view_agg_nullability_1.py` (10 tests). Pre-fix
run: the 3 raising-form tests fail with the card's `Physical input schema should be
the same` error, the 7 controls pass. Post-fix run: 10 pass.

## Clauses

| ID | Clause | Verdict | Evidence / proof obligation |
|---|---|---|---|
| C-001 | Step 0 shrink recorded: each reduced form and whether it raises; `%` + `TIMESTAMP` cast + view + column aggregate + non-nullable input needed, `+`/`*` not | **PROVEN** | §0.1 tables; `test_cast_view_agg_nullability_1.py` |
| C-002 | Spark 4.1.2 values and types recorded for the full repro and the reduced forms | **PROVEN** | §0.1 Spark column; full repro max is epoch 946698993 |
| C-003 | Site is RePark's `wrap_as_ltz`, with the logical/physical functions named | **PROVEN** | §0.2–§0.3; `EXPLAIN VERBOSE` stage trace |
| C-004 | Fix at the site; full repro answers Spark's value and type; controls pinned; one pin red on the old behaviour for the named reason; gates green | **PROVEN** | §1; `test_cast_view_agg_nullability_1.py`; gate runs below |
| C-005 | Sweep: neighbours compared, parity-doc row checked, card closed, attestation filed | **PROVEN** | §2; attestation above |
| C-006 | Fold-1 proposition: the S1 agrees inside `instant_ts.rs` | **REJECTED** | §Fold 1: three-class proof; the boundary is invisible at analyzer time; the pure-DataFusion UNION-plus-BIGINT repro; Case 3 |

## 2. Sweep

- Neighbours: §1 comparison stands (6 fixed, 20 byte-identical, 0 moved).
- Boundary probes post-fix, all matching Spark 4.1.2's instants: empty `range(0)`
  max is NULL; NULL modulo input gives max epoch 1 / `count(ts)` 2 / `count(*)` 3;
  negative divisor gives max epoch 2 / min epoch 0; `id % 0` still raises
  `[DIVIDE_BY_ZERO]` under ANSI (error path unchanged).
- Parity-doc row: none names this defect (`docs/spark-sql-iceberg-parity.md`
  nullability hits are the `fillna` and explode rows); no registry change.
- Facade subset `pytest python/repark/tests -k "cast or view or agg"` (serial, no
  xdist plugin in the fresh venv): 1470 passed, 48 skipped, 14 xfailed, 0 failed.
- Card closed in this commit (Status fixed, §Resolution).

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: cast-view-agg-nullability-1
  complete: true
  reattested: []
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Clauses walked against behavior — the 26-form shrink matrix re-run post-fix (§1), the Spark values asserted in the pins, the EXPLAIN VERBOSE stage trace naming the site (§0.2–§0.3), the fix with its pre/post pin runs and the gate list.
      artifacts: [task/ledgers/staging/cast-view-agg-nullability-1-ledger.md, python/repark/tests/test_cast_view_agg_nullability_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Boundary shapes exercised post-fix and matched to Spark 4.1.2 — empty input (NULL max), NULL modulo input (skipped by max, counted by count(*)), negative divisor, literal-only modulo, nullable VALUES input, non-timestamp cast targets, division, plus/star rewrites, count(*) beside count(col).
      artifacts: [task/ledgers/staging/cast-view-agg-nullability-1-ledger.md]
    - id: AT-3
      status: ATTACKED
      evidence: The ANSI zero-divisor error path is unchanged — id % 0 still raises DIVIDE_BY_ZERO through the same view-plus-aggregate shape; the removed internal error was the defect, not a handled mode.
      artifacts: [task/ledgers/staging/cast-view-agg-nullability-1-ledger.md]
    - id: AT-4
      status: N/A
      justification: A stateless analyzer rewrite over an immutable plan; no shared state, no ordering assumption, no concurrency surface.
    - id: AT-5
      status: N/A
      justification: No privileged action, no secret, no deserialized input, no path handling; the change narrows an Arrow field constructor call.
    - id: AT-6
      status: ATTACKED
      evidence: The 20 previously-answering neighbour forms are byte-identical post-fix; the logical side is untouched so every analyzed schema is unchanged; the parquet roundtrip control pins the materialized path.
      artifacts: [task/ledgers/staging/cast-view-agg-nullability-1-ledger.md, python/repark/tests/test_cast_view_agg_nullability_1.py]
    - id: AT-7
      status: N/A
      justification: No added pass or allocation — the fix removes one nullability computation and one field construction per wrapped cast.
    - id: AT-8
      status: ATTACKED
      evidence: The DataFusion contract is read at its source — Cast::new builds the default field, cast_output_field derives logical nullability from the child, CastExpr::nullable honors a named target field — and the fix uses the default-field path both sides derive from; nothing about upstream is presumed.
      artifacts: [task/ledgers/staging/cast-view-agg-nullability-1-ledger.md]
    - id: AT-9
      status: N/A
      justification: No new log, metric, or error text; the failure the fix removes diagnosed itself with the field-level mismatch message.
    - id: AT-10
      status: ATTACKED
      evidence: The pre-fix run is the revert mutation — the 3 raising-form pins fail with the card's named error while the 7 controls pass, and all 10 pass post-fix; the diff removes a branch and adds none, so every changed line is covered by a pin that reds without it.
      artifacts: [python/repark/tests/test_cast_view_agg_nullability_1.py]
```

## Residues

- R-1 (2026-10-08): `wrap_as_ntz` (`instant_ts.rs`, the `TIMESTAMP_NTZ` conf path) shares
  the named-field pattern; untested here and out of the card's one shape. Candidate for a
  follow-up card.
- R-2 (2026-10-08): `repark-iceberg/src/write/ntz_store.rs` builds a cast with
  `Cast::new_from_field`; a DML store path, outside this shape, not assessed.

## Fold 1 — S1 diagnosis (HALT, 2026-10-09)

Fold brief: `fix/cast-view-agg-nullability-1` fold 1 (guided, Muse). The Opus
verdict (`/tmp/oc-worker/direct/wo/cast-view-agg-nullability-1/verify/verdict.json`,
PR #1014) measures 103,668 cells on base `40fc916f` vs head `804ed70f`: 0
values, types, nullable flags or field metadata moved on any jointly answering
cell; 5,182 cells that raised on base now answer; 12 cells that answered on
base now raise (S1).

### The S1

`CAST(<expr the optimizer folds to a bare Iceberg column> AS TIMESTAMP)` under
a SQL temp view or a UNION, then an aggregate. The 8 folding wrappers
(verifier `repro_s1.py`): `coalesce(b, 7)`, `nvl`, `ifnull`,
`CASE WHEN true THEN b END`, `CASE WHEN 1 = 1 THEN b ELSE 0 END`,
`if(true, b, 0)`, `nvl2(b, b, 0)`, `coalesce(b, b)` over a NOT NULL Iceberg
column. Head raises the aggregate input check on the metadata axis:
`(physical) {"PARQUET:field_id": "1"} vs (logical) {}`. Non-folding neighbours
(`b + 0`, `b * 1`, `abs(b)`, `greatest(b)`, `coalesce(n, 7)` over a nullable
column, bare `b`) answer on every shape.

### Mechanism (measured on head `804ed70f`, no code change)

Three facts combine:

1. DataFusion's cast propagates the child field on both sides. Logical
   `cast_output_field` (`datafusion-expr-54.1.0/src/expr_schema.rs`) clones the
   child field and swaps the type, keeping nullability and metadata. Physical
   `CastExpr::resolved_target_field`
   (`datafusion-physical-expr-54.1.0/src/expressions/cast.rs`) does the same
   whenever the target field is the default (empty name, nullable, empty
   metadata, which is exactly what `Cast::new` builds); a non-default target
   is used verbatim. So a default-target cast is fully child-derived on both
   sides, and both sides agree whenever they read the same plan state.
2. Two logical boundaries freeze the analyzed (pre-fold) schema while the
   physical side reads the optimized (post-fold) plan. The SQL temp view is a
   `ReplanningTempView` (`crates/repark-spark/src/view_ddl/temp_view.rs`):
   its scan carries the fixed creation schema (coalesce child, no metadata),
   while `scan` re-plans and executes the optimized inner plan (bare `b`,
   `PARQUET:field_id`). The UNION node keeps its analyzed schema across
   optimizer rebuilds when the width is unchanged
   (`LogicalPlan::with_new_exprs`,
   `datafusion-expr-54.1.0/src/logical_plan/plan.rs`); branch projections
   recompute post-fold, the union does not.
3. The DataFrame temp view expands (`ViewTable` inlines the inner plan;
   confirmed by `EXPLAIN`: the physical plan shows the inner casts over the
   Iceberg scan), so both sides read the folded child and agree. The physical
   planner elides redundant same-type default-target casts
   (`cast_with_target_field`), so the surviving physical LTZ cast is the
   innermost one, which is the cast `wrap_as_ltz` builds. `wrap_as_ltz` does
   control the physical side of S1, and still cannot fix it (§Why no fix).

### Why no fix inside `instant_ts.rs` exists

Three shape classes need three different physical fields from the same
analyzed child (`coalesce(b, 7)`, non-nullable, no metadata; the fold target
is bare `b`, non-nullable, `PARQUET:field_id`):

- Direct cast, recomputed boundary (`ice_b_agg`-style, answers on head): the
  logical side carries the child metadata, so the physical side must carry
  it: default (child-derived) target. A verbatim empty-metadata target raises
  the reverse mismatch. This is the surviving M2 mutant.
- Fold plus frozen boundary (S1: SQL view, UNION): the logical side is frozen
  pre-fold with no metadata, so the physical side must drop it: verbatim
  empty-metadata target. The default target raises S1.
- Fold plus recomputed boundary (the card's original defect): the logical side
  is recomputed post-fold, so the physical nullability must track the child:
  default target. A baked nullable target raises the original error.

The physical API couples the two axes: the default target is dynamic on both,
a verbatim target is static on both. No single construction satisfies the
frozen family and the recomputed family at once. And `wrap_as_ltz` cannot
choose per shape: it is an analyzer rule over the view body, analyzed
standalone at `CREATE` time; the enclosing view reference, union and aggregate
are separate statements that do not exist yet. The boundary is invisible at
the site. Any rule keyed on the analyzed child (bare column or not, nullable
or not) fails one family; the DF-view S1 shape kills every verbatim variant
and the SQL-view/UNION S1 shape kills the default one. Rebuilding the inner
cast instead of the outer one moves the static point but keeps the coupling.

### Case 3: the class reproduces with no RePark code in the loop

Plain `CAST(coalesce(b, 7) AS BIGINT)` (a stock DataFusion cast;
`wrap_as_ltz` only fires for timestamp targets), no views, under a UNION plus
aggregate, raises the identical error on head:

```sql
CREATE TABLE ic.ns.src (b BIGINT NOT NULL, n BIGINT) USING iceberg;
INSERT INTO ic.ns.src VALUES (1, 1), (2, NULL), (3, 3);
SELECT max(x) AS m, count(x) AS c FROM
  (SELECT CAST(coalesce(b, 7) AS BIGINT) AS x FROM ic.ns.src
   UNION ALL
   SELECT CAST(coalesce(b, 7) AS BIGINT) AS x FROM ic.ns.src) q;
-- Internal error: Physical input schema should be the same as the one
-- converted from logical input schema. Differences:
-- field metadata at index 0 [x]: (physical) {"PARQUET:field_id": "1"} vs (logical) {}
```

Controls on the same build: the same query without the UNION answers
(`m = 3, c = 3`); the UNION over direct `CAST(b AS BIGINT)` answers
(`m = 3, c = 6`); the DF-view shape without the UNION answers. The defect is
DataFusion-internal: a default-target cast whose child folds onto a
metadata-carrying column, under any frozen logical boundary, desynchronizes
the aggregate input check. `wrap_as_ltz`'s `Cast::new` joined stock behavior;
the old named field masked this axis at the cost of the nullability axis. A
RePark-side patch at `wrap_as_ltz` would be a shape-specific patch over an
upstream defect, which the brief's Case 3 forbids.

### Upstream issue draft

```text
Title: Default-target CAST under UNION + aggregate trips the physical/logical
input check when the child folds onto a metadata-carrying column

DataFusion 54.1.0. A default-target CAST(f(...)) whose child folds to a bare
column carrying field metadata (e.g. PARQUET:field_id) raises "Physical input
schema should be the same as the one converted from logical input schema ...
field metadata at index 0 ... (physical) {"PARQUET:field_id": "1"} vs
(logical) {}" when an aggregate sits above a UNION. Repro: a table with a
NOT NULL BIGINT column carrying field metadata, then

SELECT max(x), count(x) FROM
  (SELECT CAST(coalesce(b, 7) AS BIGINT) AS x FROM t
   UNION ALL
   SELECT CAST(coalesce(b, 7) AS BIGINT) AS x FROM t) q;

Cause: cast_output_field (logical) and CastExpr::resolved_target_field
(physical) both propagate the child field for a default target, so both sides
agree when read from the same plan state; but LogicalPlan::with_new_exprs
preserves the UNION's analyzed (pre-fold, metadata-free) schema while the
physical plan is built from the folded branches. Proposed principle: a cast's
output is a new value and carries no field metadata of its child, on either
side. Recomputing the union schema post-fold would fix the UNION shape only;
dropping child metadata from the cast output on both sides fixes every frozen
boundary uniformly. Note the output-visibility hazard that motivates the
principled fix: today a SELECT CAST(pk AS <type>) result (and a CTAS file)
carries the source column's PARQUET:field_id, so a new value masquerades under
the source column's identity.
```

The draft is unrun against stock DataFusion; the UNION-plus-BIGINT repro above
ran on RePark head, whose cast path there is stock. DataFusion per
`Cargo.lock`: `datafusion` / `datafusion-expr` / `datafusion-physical-expr`
54.1.0.

Filed 2026-10-10 as [apache/datafusion#26178](https://github.com/apache/datafusion/issues/26178),
after the repro was run on stock DataFusion 54.1.0 with no RePark code in the
loop and answered the wrong value there. PR #1014 stays parked on that issue.

### Recommendation and carry-over

Recommended: file the issue upstream and hold PR #1014 until the S1 class is
resolved; the 12 S1 cells answered on base, so head is worse than main on
those statements. Fallback: upstream union-recompute plus a RePark-side
replanning-view scan-output conformance (both outside this fold's scope; the
view half strips metadata outside the cast, against this fold's ruling, and
each frozen boundary needs its own). Not recommended: ship head accepting the
12 regressions.

Carry-over for the authorized-fix fold, in order: red pins for every
`repro_s1.py` form (8 wrappers x SQL-view and UNION shapes) with base's
answers; the M2-killer pin (direct `CAST` of an Iceberg column plus DF view
plus aggregate, which reds on any verbatim empty-metadata target); the §1
"what moves" correction (the physical field's metadata moves too: 1,770
Iceberg-source cells); the dated residue row for the 132 newly answering
cells that carry the pre-existing wrong double-cast values (verdict S3:
`CAST(CAST(id % 3 AS INT) AS TIMESTAMP)` answers epoch 0 per row and
`CAST(id / 2 AS TIMESTAMP)` answers 0,0,1,1,2,2 s on base and head; Spark
4.1.2 answers 0,1,2,0,1,2 s and 0,0.5,...,2.5 s); R-1 and R-2 unchanged
(R-1's NTZ path shares the shape, so the same impossibility covers it).

Out of scope observed: a CTAS over direct `CAST(b AS TIMESTAMP)` writes the
new `ts` column carrying the source's `PARQUET:field_id: 1` (verifier
`probe1.py`, `ice_b` CTAS row; same on base and head). That is the output
face of the propagation this halt describes.

No guard relaxed; no code or test file changed in this fold.

## Gates

- `cargo fmt --check`: exit 0.
- `make rust-clippy`: exit 0.
- `make rust-panic-ban`: exit 0.
- `cargo test --locked -p repark-functions --lib`: 902 passed, 0 failed, 1 ignored.
- `make develop`: exit 0 (rebuilt with the fix; rebuilt again after the extras sync).
- Unit test file (10 tests): 3 fail pre-fix with the card's error, 7 pass; 10 pass post-fix.
- `pytest python/repark/tests -k "cast or view or agg"` (serial; no xdist plugin in the
  fresh venv): 1470 passed, 48 skipped, 14 xfailed, 0 failed.
- `uvx ruff@0.15.22 check .` / `format --check .`: clean (3 UP017 autofixes + 1 format on
  the new test file).
- `python3 scripts/sync_map_md.py --check`: 374 maps clean.
- `bash scripts/check_map_md.sh --base origin/main`: exit 0.
- `make check-ledger-grammar`: clean.

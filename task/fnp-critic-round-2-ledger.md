# Unit ledger — Critic round 2 · re-attesting round 1's own remediation

**Date:** 2026-08-20 · **Branch:** `feat/spark-function-parity` · **Base:** `5d69153` ·
**Charter:** [fnp-0-charter-ledger.md](fnp-0-charter-ledger.md) ·
**Round 1:** [fnp-critic-round-1-ledger.md](fnp-critic-round-1-ledger.md) ·
**Mode:** two independent Critic passes, **hard context break** — fresh agents, Opus tier per the
standing owner grant, with lenses chosen to attack round 1 rather than repeat it: one dedicated to
*defeating* the eight remediations with inputs their pins do not cover, one spending its effort
where round 1 did not look (feature interactions, and whether the new pins would catch a
deliberately introduced bug).

Both passes returned **NOT_CONVERGED**. 17 findings, **4 at or above the S1 floor** — filed
independently, so the four are three distinct defects.

## What the second break bought

Round 1 was reviewed by agents who had not written the code. Round 2 was reviewed by agents who had
not written the code **or the fixes**, and were told the fixes were the target. That is what found
the class the first pass could not have: every S1 here is a defect the round-1 remediation
**introduced or left half-done**, and `make preflight` was green over all of them.

Both passes also independently REFUTED two round-1 dispositions (F-CFS-3 empty regex, F-CFS-5
approx_count_distinct), each holding on the axis the pin tested and failing on one it did not.

## Blocking findings and their remediation

### R2-1 — `over()` could not see an aggregate through a CAST *(F-R3-2 + FNP-R3-2, S1, filed twice)*

The round-1 fix for F-CFS-5 wrapped the aggregate in `Expr::Cast`. `PyColumn::over` matched
`Expr::WindowFunction`, `Expr::Cast(WindowFunction)` and `Expr::AggregateFunction` — but not
`Expr::Cast(AggregateFunction)`, so it fell to the catch-all and refused:

```
F.approx_count_distinct("v").over(w)
  -> ValueError: over() applies only to a window or aggregate function column
```

A regression on valid PySpark, and the message named the wrong cause. Both agents derived it
independently, one from execution and one from the diff alone.

**Fix** (`crates/repark-python/src/column/mod.rs`): peel an optional CAST once, match the inner
expression, re-apply the cast to the window result. The windowed form now carries the same type as
the grouped form — `approx_count_distinct` over a window is `int64`, not the engine's unsigned.

**Pins:** `test_every_dispatched_aggregate_can_be_used_in_a_window` (13 names across both dispatch
tables — the critic's own suggestion, so the next cast-wrapping fix cannot repeat this on a
different name) and `test_approx_count_distinct_stays_a_signed_bigint_in_a_window`.

### R2-2 — the unsigned-count fix was keyed on a name, so its sibling stayed broken *(FNP-R3-1, S1)*

`regr_count` materialized `UInt64` while `DataFrame.schema` reported bigint — the exact defect
round 1 had just fixed for `approx_count_distinct`, at the arm next to it:

```
agg(regr_count(y,x)).schema      -> struct<regr_count(y, x):bigint>
agg(regr_count(y,x)+1).schema    -> struct<v:decimal(21,0)>
written parquet                  -> rc: uint64      (Spark reads it back as decimal(20,0))
```

Round 1 treated a symptom as a name rather than as a class.

**Fix**: `cast_unsigned_count_to_signed` in `column/function_dispatch.rs`, applied at both the unary
and binary aggregate sites. The cast is taken from the **UDAF's own declared return type**, probed
with Int64 arguments — Spark has no unsigned integer type, so an unsigned result is a fidelity
defect however it arises, and an aggregate added to either table is covered the day it is added.
The cast also moved after the IGNORE NULLS builder chain, which only accepts a bare aggregate as
its receiver.

**Pin:** `test_regr_count_is_a_signed_bigint_through_arithmetic`.

**What the fix does not reach, found by the fix itself.** FNP-5's two-door test went red on the
first run after this change: the facade now returns `int64` while `SELECT regr_count(...)` still
returns `UInt64`, so the doors disagree on type. Round 1's `approx_count_distinct` fix had opened
the same divergence silently — nothing checked it. Correcting the **door** means moving the cast
into the shared analyzer layer, where the rewrite has to be idempotent across re-analysis and must
not rename an `Aggregate` node's output field that a parent `Projection` refers to by name. That is
an engine-semantics unit, not a line in a remediation commit.

So the facade — the surface this campaign is about, and the one Spark users see — is correct, and
the divergence is pinned as a **ratchet** (`DOOR_RETURNS_UNSIGNED` in `test_fnp5_aggregates.py`):
fixing the door turns the test red and the row leaves. Registered in STATUS.md, along with a
separate gap the measurement turned up — the SQL door does not know the name
`approx_count_distinct` at all, only DataFusion's `approx_distinct`.

### R2-3 — the lambda counter leaked a non-deterministic name into the output schema *(F-R3-1, S1)*

Round 1 gave lambda parameters unique plan names using a process-wide `itertools.count()`. The plan
name reaches the output schema on any higher-order column the facade does not name, so the same
query built twice in one session produced two different schemas:

```
df.groupBy(F.exists("a", lambda x: x > 2)).count().columns[0]
  build 1: array_any_match(....a,(x_0) -> x_0 > Int64(2))
  build 2: array_any_match(....a,(x_1) -> x_1 > Int64(2))
```

The round-1 pin aliased the column and so could not see it.

**Fix**: the plan name now carries the lambda's **nesting depth** (a `ContextVar`, so concurrent
builds on different threads cannot interleave), which is what the collision was ever about. Two
builds of the same expression mint the same names; an inner lambda still cannot collide with an
enclosing one. Sibling lambdas share a depth and so share a name — sound, because they occupy
disjoint scopes and only an enclosing binding can capture.

**Pins:** `test_an_unaliased_higher_order_column_has_the_same_name_on_every_build` and
`test_sibling_lambdas_share_a_name_and_still_evaluate_independently`.

**Not fixed, and deliberately:** the name is still the engine's spelling over an internal relation
(`array_any_match(datafusion.public.__repark_cdf_<uuid>.a, ...)` rather than PySpark's
`exists(a, lambdafunction(...))`). Measured: `groupBy(F.col("k") + 1)` names its key
`datafusion.public.__repark_cdf_<uuid>.k + Int64(1)` too, so this is how the group-by path names
**every** unaliased expression key — not something the lambda work introduced. Naming group keys
from the facade's projection name is a change to a sensitive shared path and belongs in its own
unit. Forwarded.

## Corrected because a false claim is worse than the finding's severity

Three sub-floor findings are errors in the branch's own record. Left alone they would ship a
statement that is not true, so they were fixed here rather than forwarded.

- **F-R3-7 (S3)** — STATUS.md still forwarded the `approx_count_distinct` unsigned row to FNP-Z
  after round 1 closed it. Re-pointed: the row is now recorded **closed** (and the fix generalized
  by R2-2), with FNP-6a's astral-text residual and FNP-6b's `randstr` cap registered in its place.
- **F-R3-5 / FNP-R3-5 (S2/S3, filed twice)** — the `_sort_specs` comment quoted PySpark's
  `_sort_cols` without noting where it departs from it. The three departures are now stated (strict
  length instead of a truncating `zip`; tuples accepted where PySpark's `isinstance` rejects them),
  and the round-1 remediation's regression of the bad-type branch from `PySparkTypeError` to
  `PySparkValueError` is undone — PySpark raises `NOT_BOOL_OR_LIST`, which is a type error.
- **F-R3-8 (S3)** — the C-012 ratchet table's size was written in prose as "seventeen", recorded in
  the round-1 notes as 19, and is actually **20**. The prose count is gone; the size is now an
  assertion inside the test, where a drifting count goes red instead of misleading a reader.

## Forwarded — the S2/S3 remainder

Recorded, not fixed: below the S1 floor, and each is its own small unit rather than a line in a
remediation commit. They are the backlog the next branch works from.

| ID | S | What |
|---|---|---|
| F-R3-3 / FNP-R3-4 | S2 | `refuse_nested_higher_order` walks lambda bodies only, so a higher-order call in a **value argument** escapes the guard and surfaces the internal `unresolved LambdaVariable` error the guard exists to replace |
| FNP-R3-3 | S2 | a higher-order column as a **Window ordering key** fails at physical planning with a raw `SanityCheckPlan` dump; the same column orders a DataFrame correctly |
| F-R3-6 | S2 | `cube` / `rollup` over a higher-order column fail with a raw sqlparser error — a second SQL-text route past `bound`, and the F-CSP-4 disclosure names only joins |
| F-R3-4 | S2 | the empty-pattern collector agrees with `regexp_count` on BMP text only; on astral text the two disagree and both diverge from Java's `Matcher` |
| F-R3-9 | S3 | the `randstr` cap is per-row, so a legal length x a large batch still reaches an arrow-rs offset panic (caught at the boundary); the cap itself is an unregistered divergence |
| F-R3-10 | S3 | `xxhash64()` with zero arguments is rejected with the dispatcher's internal arity message; PySpark accepts it |
| FNP-R3-6 | S3 | `_lambda_arity` rejects only `*args`/`**kwargs`, so keyword-only and positional-only parameters pass the gate and fail as a raw Python `TypeError` |
| FNP-R3-7 | S3 | `SCALAR_NAMES` is hand-maintained, so the kernels this branch added are outside the C-012 guard's domain |
| — | S2 | (from R2-3) group-by names every unaliased expression key with the engine's spelling over an internal relation, not the facade's projection name |

## Gate

Every S1 fixed with a regression pin that fails without it. `make ci`, `cargo test`, and the facade
suite green. Disposition: **PASSED at the S1 floor**, with the table above carried forward.

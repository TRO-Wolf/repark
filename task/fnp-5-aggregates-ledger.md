# Unit ledger — FNP-5 · aggregates the engine already had

**Unit:** FNP-5 · **Date:** 2026-08-20 · **Executor:** Claude (Opus 5) ·
**Branch:** `feat/spark-function-parity` · **Base:** `d3337b1` (FNP-4a) ·
**Charter:** [fnp-0-charter-ledger.md](fnp-0-charter-ledger.md) clauses **C-009**, **C-012** ·
**Design:** [../docs/design/spark-function-parity.md §7](../docs/design/spark-function-parity.md).
**SEPMO:** STANDARD. Floor S1.

**Writable:** `crates/repark-python/src/column/function_dispatch.rs`,
`python/repark/src/repark/spark/{functions.py,functions_expr.py}`, the facade tests, this ledger,
`task/map.md`, the touched `map.md` files.

## The class, again

Same shape as FNP-3, one layer up: every kernel here is in
`datafusion::functions_aggregate::all_default_aggregate_functions()`, so `register_all` put it on
every session and `spark.sql(...)` resolved it — the facade's aggregate dispatch simply had no
arm. Thirteen names for roughly the cost of one.

## Shipped — 13 names

The nine `regr_*` (`avgx`, `avgy`, `count`, `intercept`, `r2`, `slope`, `sxx`, `sxy`, `syy`) ·
`grouping` · `approx_count_distinct` · `listagg` · `string_agg`. `__all__` 340 → 353.

Rather than add nine more copies of the twelve-line `corr` / `covar_pop` wrapper, those three
collapse to one `_binary_aggregate(name, col1, col2)` that all twelve two-column aggregates now
share. Nine copies would have made FNP-8's repatriation job strictly worse, and the attributes
they thread — `agg_name`, the quoted structural `sql_expr` free-SQL global-agg needs,
`partition_transform` — have to stay in lockstep across every copy.

## Census corrections

The design listed 16 wire-only names. Probing the live session instead of trusting the list:

| Name | Design said | Measured |
|---|---|---|
| `sum_distinct` | wire-only | **Not a registered function.** DataFusion spells it `sum(DISTINCT x)` — a modifier on the aggregate call, not a kernel. Needs the facade's DISTINCT path, not an arm. |
| `listagg_distinct`, `string_agg_distinct` | wire-only | Same — DISTINCT modifiers, not kernels. |
| `approx_count_distinct` | wire-only | Registered as `approx_distinct`; ships here under Spark's spelling, with the divergence below. |
| `listagg` | wire-only | Registered as `string_agg`; ships as the Spark spelling of the same kernel. |

So 13 shipped, and the three DISTINCT variants move to whichever unit builds the DISTINCT
aggregate path. `any_value` / `max_by` / `min_by` were never in this unit — the design has them in
FNP-12 as new kernels, and the probe confirms they are absent from the registry.

## Divergences recorded, not papered over

- **`approx_count_distinct(col, rsd)`** — Spark's estimator is HyperLogLog++, DataFusion's is
  HyperLogLog. The `rsd` (relative standard deviation) argument therefore has nothing to tune and
  is **accepted and ignored**, the same treatment `percentile_approx`'s accuracy argument already
  gets. The pin asserts the signature contract and the count on a tiny input; it deliberately does
  **not** claim the estimate matches Spark's.
- **Return type**: `approx_count_distinct` returns `uint64` where Spark returns a signed bigint.
  Recorded here; a registry row belongs to FNP-Z.

## Oracles

The nine regression aggregates are pinned against an **exact** fit — `y = 2x + 1` over `x = 1..4`
— so every statistic has a closed-form answer (slope 2, intercept 1, r² 1, sxx 5, syy 20, sxy 10,
avgx 2.5, avgy 6, count 4) that does not depend on RePark agreeing with itself. Each is also
cross-checked against the SQL door for value and type.

## Findings

| ID | Sev | Finding | Disposition |
|---|---|---|---|
| F-FNP5-1 | S1 | Thirteen registered aggregates were unreachable from the facade | `REMEDIATED` — pinned against closed-form values and cross-checked per door |
| F-FNP5-2 | S3 | The design's "16 wire-only" over-counted: three are DISTINCT modifiers, not kernels | `REMEDIATED` — re-scoped above; the design table is corrected |
| F-FNP5-3 | S2 | `approx_count_distinct` returns `uint64`, Spark returns signed bigint | `ACCEPTED_FLAGGED` — registry row handed to FNP-Z |
| F-FNP5-4 | S3 | Adding nine `regr_*` would have meant nine copies of the `corr` wrapper | `REMEDIATED` — one `_binary_aggregate` helper, twelve callers |

## Gates

| Gate | Result |
|---|---|
| `cargo test --workspace --no-fail-fast` | **45 binaries, 1,990 passed, 0 failed**, cargo exit 0 |
| `make ci` | exit **0**. Three reds on the way, all mechanical: `rust-fmt-check`, and two `RUF003` ambiguous-glyph findings on `×` / `²` in comments. |
| facade pytest (full) | first run **1 failed, 3,488 passed, 70 skipped** — see below; then green |

## A third fence, and the same lesson

`test_functions_c.py::test_fn_c_deferred_names_are_absent` asserted `grouping` and
`approx_count_distinct` were **absent**, and the module docstring repeated the list. FN-C recorded
its deferral in two places, exactly as FN-D/GT2 did for `datediff` in three.

The list is now marked **RATCHETS DOWN** and both sites are updated together. `sum_distinct`
stays on it with the reason attached — *not a kernel; DataFusion spells it `sum(DISTINCT x)`* —
because a name sitting on a deferred list with no reason is indistinguishable from a name nobody
has looked at.

This is the third unit in a row where the blocker was a prior unit's scope fence recorded as a
passing assertion. The pattern is worth stating plainly: **a fence and a decision look identical
from the outside**, and only the ledger tells them apart. Every deferral this campaign writes
carries its mechanism for that reason.

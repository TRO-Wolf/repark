# Unit ledger — FNP-1 · the two-door asymmetry

**Unit:** FNP-1 · **Date:** 2026-08-20 · **Executor:** Claude (Opus 5) ·
**Branch:** `feat/spark-function-parity` · **Base:** `5d69153` (`main`, post-#188) ·
**Charter:** [fnp-0-charter-ledger.md](../../staging/fnp-0-charter-ledger.md) clause **C-012** ·
**Design:** [../docs/design/spark-function-parity.md §2.4](../../../../docs/design/spark-function-parity.md) ·
**Slate:** [../briefs/spark-function-parity.md](../../../../briefs/spark-function-parity.md).
**SEPMO:** STANDARD (fails LIGHT criterion 5 — a silent-wrong-results path — and criterion 1,
public facade behaviour). Floor S1.

**Writable:** `crates/repark-functions/src/{instant_ts,expr_fn,aggregate}.rs`,
`crates/repark-python/src/column/{function_dispatch.rs,mod.rs,door_parity_tests.rs}`, this ledger,
`task/map.md`. Registry / STATUS / lockfiles / `.github` closed.

## The defect

The facade builds a standalone `Expr` with no `SessionContext` to resolve names against, so
`function_dispatch.rs` embeds a UDF instance by hand. The SQL door resolves the same spelling out
of the session registry, where `register_all` installed `datafusion-spark` and then overwrote
names with the repark shims. Nothing forced the two to agree.

`expr_fn.rs`'s own module doc already states the policy — *"byte-for-byte the same function the
SQL path resolves"* — and FN-GT1/GT2 applied it by hand to sixteen names. Two live holes remained:

| Name | Facade resolved | Door resolved | Consequence |
|---|---|---|---|
| `to_timestamp` | DF-core `to_timestamp` → `Timestamp(ns, None)` | `instant_ts::SparkToTimestamp` | The facade loses the TZ-4 PR-1 LTZ wire type and PR-2 session-zone localization. `F.to_timestamp(x)` and `spark.sql("SELECT to_timestamp(x)")` return different types. |
| `avg` | DF-core `Avg` | `aggregate::SparkAvgWithRetract` | The facade loses the Spark i64-count and null-on-empty arms, and accepts a `Duration` argument Spark does not. The kernel behind FLOAT-AGG-2. |

## What was built

The clause is now **mechanical rather than hand-checked**. `column/door_parity_tests.rs` compares
the concrete UDF the facade embeds against the one `register_all` installs, using `ScalarUDF` /
`AggregateUDF` `PartialEq` (which delegates to the inner impl — exactly the "is it the same
function?" question). Three tests:

1. `every_scalar_spelling_resolves_the_same_kernel_on_both_doors` — the live set plus the six
   GT1/GT2-closed names as **positive controls**, so a future change that re-opens one goes red
   instead of staying silent.
2. `facade_avg_is_the_repark_retracting_kernel_not_datafusion_core`.
3. `expected_divergences_are_all_still_real` — the sanctioned-out table **ratchets DOWN only**; a
   row that has quietly been fixed fails the build, so the table cannot rot into a lie.

Fixes: `instant_ts::to_timestamp_udf` and a new `aggregate::avg_udaf` become `pub` (matching the
existing `timestamp_cast::to_date_udf` idiom); `expr_fn::to_timestamp` is the facade-embed builder;
both dispatch arms now name the repark kernel.

## Findings

| ID | Sev | Finding | Disposition |
|---|---|---|---|
| F-FNP1-1 | S1 | `to_timestamp` resolves DF-core on the facade — measured fail-first by `every_scalar_spelling_resolves_the_same_kernel_on_both_doors` | `REMEDIATED` — regression proof: that test, red before the dispatch edit and green after |
| F-FNP1-2 | **S2** (downgraded from S1 — see below) | `avg` resolves DF-core on the facade (`Avg` vs `SparkAvgWithRetract`) | `REMEDIATED` — regression proof: `facade_avg_is_the_repark_retracting_kernel_not_datafusion_core`, red before the dispatch edit |
| F-FNP1-4 | S3 | **The `avg` divergence is behaviourally latent.** No input was found on which the two kernels disagree. Probed: all-null column, empty frame, and a sliding `rowsBetween(-1, 0)` window (the retract path `SparkAvgWithRetract` exists for) — facade and door agree on type and value in every case, and the facade `avg` pin passes both before and after the fix. | `ACCEPTED_FLAGGED` — the fix stands on the policy ("both doors resolve the same UDF"), not on a demonstrated wrong answer. Two implementations of the same semantics drifting apart is a latent hazard, but this ledger does **not** claim a user-visible `avg` bug was fixed, and F-FNP1-2 is downgraded to S2 accordingly. |
| F-FNP1-3 | S3 | The census listed `cardinality` among the 17 latent divergences, but both doors already agree — the ratchet test caught it on its first run | `REMEDIATED` — row removed. The census evidence file keeps the original claim; this ledger is the correction. The live set is **18**, not 19. |

## Fail-first evidence

```
every_scalar_spelling_resolves_the_same_kernel_on_both_doors
  → ["to_timestamp"]
facade_avg_is_the_repark_retracting_kernel_not_datafusion_core
  → left:  AggregateUDF { inner: Avg { … aliases: ["mean"] } }
    right: AggregateUDF { inner: SparkAvgWithRetract { … } }
expected_divergences_are_all_still_real
  → ["cardinality"]
```

## Still open in this unit

The seventeen latent names are listed in `EXPECTED_DIVERGENCES` with a per-name reason. They are
**disclosed, not closed** — each needs its own semantic adjudication (is the DataFusion-core
behaviour actually wrong for Spark, or only differently-implemented?) before it is fixed or
registered. The table is the enumeration that adjudication works through, and it can only shrink.

## Gates

| Gate | Result |
|---|---|
| `cargo test --workspace --no-fail-fast` | **45 binaries, 1,987 passed, 0 failed**, cargo exit 0. The pre-change baseline over the same 45 binaries was 1,984; the delta is exactly this unit's three new guard tests. |
| `make ci` | exit **0**. Two reds on the way: `rust-fmt-check` on the long tuple literals in the divergence table, and `py-lint` N812 on the `functions as F` import — the suite's idiom carries an explicit `noqa` with the reason, and this file now matches it. |
| `make py-test-facade` | **3,437 passed, 70 skipped, 0 failed** (167s, wheel rebuilt via maturin). The `to_timestamp` type change broke nothing — see the coverage miss below. |

## Vigilance note — a truncated gate is not a gate

The first verification run of this unit was piped through `grep … | head -30`. `head` closed the
pipe and became the exit status, so the command reported **exit 0 and "1,783 passed, 0 failed"**
while actually having truncated the output at 30 of 45 result lines. The number was a partial sum
and the exit code was not cargo's.

It was caught by comparing the binary count against the earlier baseline run (45 result lines,
not 30) rather than by anything in the output itself — a truncated green looks exactly like a
real one. AGENTS.md already requires checking REAL exit codes and never a pipe's; the failure
mode here is narrower and worth naming: **a `head` or `-m` on a verification pipeline silently
converts an incomplete run into a passing report.** Verification output goes to a file and is
summed from the file; filters are for reading, never for deciding.

Re-run untruncated: 45 binaries, 1,987 passed, 0 failed. No claim in this ledger rests on the
truncated run.

## Regression proof (R5) — measured both ways

`python/repark/tests/test_two_door_kernel_parity.py` is the facade-layer evidence. Proven by
reverting the two dispatch lines, rebuilding the wheel, and re-running:

```
PRE-FIX   2 failed, 4 passed
  assert TimestampType(timestamp[ns]) == TimestampType(timestamp[us, tz=UTC])
POST-FIX  6 passed
```

Fresh execution through the public entry point (binding manifest `s0_fresh_execution`), novel
input, absent from any committed test at the time:

```
FACADE  to_timestamp type : timestamp[us, tz=UTC]
SQLDOOR to_timestamp type : timestamp[us, tz=UTC]
VALUES  both              : 2026-03-01 12:34:56+00:00
```

## Coverage miss (ref-08 `coverage_misses`)

**The entire facade suite passed unchanged across this fix** — 3,437 tests, before and after a
change that moves a public return type from `timestamp[ns]` to `timestamp[us, tz=UTC]`. Nothing
pinned it. That is how the divergence survived: the entry-point matrix had no row comparing the
two doors on the same name, so a name could resolve one kernel on the facade and another on the
SQL door indefinitely without a single test going red.

The attestation category that sat clean over the failure is **cross-entry-point equivalence**.
Both new test files close it — the Rust guard on UDF identity, the facade file on Arrow type and
value — and the facade file is the one that would have caught it.

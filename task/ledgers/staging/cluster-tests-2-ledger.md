# Unit ledger — CLUSTER-TESTS-2 · the two other cluster-feature tests that fail on main

**Date:** 2026-10-10 · **Branch:** `fix/cluster-tests-2` · **Base:** `53851112` (`ci/cluster-feature-job`)
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Card:** [cluster-feature-ci-1-card-2026-10-09.md](../../roadmap/mid-term/cluster-feature-ci-1-card-2026-10-09.md).

**Retires:** this ledger moves to `../completed/` when the unit's PR merges.

**Why now.** After CLUSTER-CODEC-TEST-1 the cluster-feature run is green except the two
cancel tests (`cluster_two_executors.rs::cancel_mid_flight…`,
`multi_stage.rs::session_spill_dir…`), which fail at submit because their `range()` seed
plans a node the cluster codec cannot carry. The owner ruled (2026-10-10) that the
cluster-feature CI job must be green on day one, so these two go green first, in their
own small PR.

**Not in this unit:** product code, `.github/`, `Cargo.toml`, `STATUS.md`, codec support
for any new plan node (a (b) reading would HALT to its own brief), the CI job itself
(the card stays open for it).

## PROPOSITION LEDGER — CLUSTER-TESTS-2 — 2026-10-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Both failures share one cause: `SELECT id FROM range(100000000)` plans `StreamingTableExec` (RePark's `SparkRangeFunc` since RANGE-TVF-ID-1, `eaa5e67a`, 2026-09-18), which datafusion-proto 54.1.0 cannot serialise and the Repark/Ballista extension codec refuses, so the job goes Queued → Failed at submit and the cancel path is never reached. | The reproduce output (both tests Failed with `Unsupported plan node, name: [StreamingTableExec]`), `range_table.rs::scan` returning `StreamingTableExec`, the proto conversion list carrying only generator-backed `LazyMemoryExec`. | **PROVEN** | Reproduced 2026-10-10 on `53851112`: both tests fail exactly as the brief says. `git log -S` trail: at M1-B merge (2026-09-10, DF 54.1.0 already pinned) `range()` was the DataFusion built-in (`RangeFunc` → `LazyMemoryExec` with a recognised generator → proto `GenerateSeriesNode`), and the M1-B ledger records the cancel test green. `eaa5e67a` overrode `range` with `SparkRangeFunc` (Spark's `id` column, 1–4 args) whose `scan` returns `StreamingTableExec`; proto has no conversion for that node and the extension codec refuses it. The `Plan: LazyMemTableExec { .. }` tail is DataFusion's own hand-written `Debug` for `StreamingTableExec` (`streaming.rs:177`), not a second node. Red since 2026-09-18, unseen because no CI runs the feature; noted 2026-10-04 in `tests/map.md`. |
| C-002 | The honest fix is test-side ((a)): no contract promises `range()`-sourced plans on the cluster, so the seed — not the product — is what changed. | The M1-B C-005 and M1-C C-004 wordings (a long query / a cancel, `range` as seed), the crate `map.md` surface, the dependency and backend-implementor search. | **PROVEN** | M1-B C-005 promises "a long cluster query cancelled mid-flight"; M1-C C-004 promises spill-dir cleanup "after cancel of `range(100000000)`" as the seed of a cleanup pin, not a `range` surface pin. The crate `map.md` promises no `range` surface. No crate outside `repark-distributed` depends on it, and `ExecutionBackend` has only `SingleNodeBackend`, so no user-facing door reaches `ReparkClusterExecutor` today. `tests/map.md` (2026-10-04) already reads the red as the seed's node. Codec support for `StreamingTableExec` would be product work under its own brief; this unit writes none. |
| C-003 | Replacing the seed with `SELECT value FROM generate_series(0, 99999999)` — the DataFusion built-in, `LazyMemoryExec` with a proto-recognised generator, the same 100M-row long-running shape that was green 2026-09-10 — makes both tests pass with their cancel, task-count and shuffle-file assertions unchanged, and the full cluster-feature command passes 5 consecutive runs. | The two updated tests plus 5 consecutive full-command runs and the wall time. | **OPEN** | Open question: does `generate_series` resolve on a `ReparkSession` (only `range` is overridden), cross the codec, and stay Running long enough to cancel? Measured in step 3. |

## Coverage attestation

Deferred: C-003 is OPEN; the attestation is filed when it closes.

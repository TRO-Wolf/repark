# Spill-coverage matrix — NEVEROOM-1 step 2 (2026-09-11)

Full-tier D-1 × D-3 run: 9 operators × 3 input multiples (2×, 4×, 8× the
memory limit) × 3 repetitions, each cell in its own subprocess under an
address-space cap. Machine CSV:
[spill-coverage-matrix-2026-09-11.csv](spill-coverage-matrix-2026-09-11.csv).

## Host and build profile

- Host: Johns-Linux-Mint, x86_64, 64 cores, 125 GiB RAM, idle at launch
  (`pgrep -x cargo`, `pgrep -x rustc`, `pgrep -x maturin`, `pgrep -x java`
  all empty; a `dv-nightly` JVM lane completed before timing began).
- Build: release module in `.venv` — `repark._native.__debug_assertions__
  == False`, repark 1.1.1, DataFusion 54.1.0.
- Pool: `datafusion.runtime.memory_limit = 1024M` (1 GB), one FairSpillPool.
- `datafusion.execution.target_partitions = 4` (the H3 baseline's own
  parameter).
- `datafusion.execution.batch_size = 8192` — pinned for the full tier
  because the `generate_series` default of 65536 rows makes one scan batch
  ~560 MB at the 8× payload width, an un-accounted allocation that aborts
  the worker before the measured operator ever runs.
- Address-space cap (S2-8): `RLIMIT_AS = VmSize_at_apply + 3 × limit`,
  applied after session construction and verified by read-back. Measured
  VmSize at apply ≈ 8.6 GB → cap ≈ 11.8 GB.
- Input (D-1): `range(1_000_000)` + `repeat('x', n) AS payload` generated
  in-engine, payload width sized so Arrow bytes hit the multiple; no files.
  Measured input bytes per side: 2 147 000 000 (2×) / 4 294 000 000 (4×) /
  8 589 000 000 (8×).

## Exact command

```
cd python/repark-parity/tests/spill
.venv/bin/python matrix_run.py \
  --scratch /tmp/noom2-scratch/matrix \
  --results /tmp/noom2-scratch/matrix/results.jsonl \
  --csv docs/perf/spill-coverage-matrix-2026-09-11.csv \
  --reps 3
```

`matrix_run.py` runs `python/repark-parity/tests/spill/matrix_worker.py`
per cell; each worker writes a result JSON plus an `EXPLAIN` plan sidecar.
`spill_bytes` in the CSV is the max across reps; `seconds` the median wall.

## Matrix

Outcomes per cell are rep 1 / rep 2 / rep 3; a cell whose three repetitions
disagree is `UNSTABLE`. `KILLED` means the worker died without a loud
refusal (SIGABRT on an un-accounted allocation in every case observed here).

| operator | 2× | 4× | 8× | notes |
|---|---|---|---|---|
| sort | refused | refused | refused | `SortExec`+`SortPreservingMergeExec` in plan; pool refusal names `ExternalSorter`/`SortPreservingMergeExec` |
| hash_aggregate | **UNSTABLE** (refused/spilled/refused) | refused | refused | `AggregateExec` in plan; rep 2 spilled 7 350 412 902 B in 170 spill files; the refuse-vs-spill boundary is racy at 2×. Upstream accounting epic: https://github.com/apache/datafusion/issues/22758 |
| hash_join | refused | **KILLED** | refused | `HashJoinExec` in plan; refusals name `HashJoinInput`; 4× aborts on an un-accounted ~35 MB allocation while the build side saturates the pool. No upstream spill path: https://github.com/apache/datafusion/issues/24768 |
| sort_merge_join | refused | refused | refused | `SortMergeJoinExec` in plan (forced by `datafusion.optimizer.prefer_hash_join=false`); refusals name `ExternalSorter`/`SortExec` |
| window_sliding | completed | completed | completed | `BoundedWindowAggExec` in plan; streams the full input, takes no pool reservation |
| window_unbounded | **UNSTABLE** (refused/refused/KILLED) | refused | refused | `WindowAggExec`+`SortExec` in plan; the un-accounted per-partition input cache (~531 MB) sometimes aborts before the sort's refusal. Upstream accounting epic: https://github.com/apache/datafusion/issues/22758 |
| dynamic_flatten | completed | completed | completed | `UnnestExec` in plan (nested `named_struct`+`array` input built via `dynamicFlatten()`); takes no pool reservation. Upstream accounting epic: https://github.com/apache/datafusion/issues/22758 |
| nested_loop_join | refused | refused | refused | `NestedLoopJoinExec` in plan with `RepartitionExec` under the build side (the H3 panic shape); refusal names `NestedLoopJoinLoad`. Upstream defect: https://github.com/apache/datafusion/issues/24661 |
| collect | refused | refused | refused | facade boundary; refusal is `MemoryError` (carries no message) |

## H3-SPILL fixed outcomes — reproduced

Both H3-SPILL-RESIDUE-1 fixed outcomes reproduce at all three multiples:

- `nested_loop_join`: **refused** 3/3 per cell — a typed
  `PySparkException` naming `NestedLoopJoinLoad[0]` ("Resources exhausted:
  Failed to allocate additional … fair(pool_size: 1024.0 MB)") with the
  REPARK remediation text appended. The upstream panic still fires beneath
  the containment (worker stderr shows `panicked at
  datafusion-physical-plan-54.1.0/src/repartition/mod.rs:1277:22: partition
  not used yet`) and is reported as the pool refusal — the FIXED outcome,
  not the historical `internal_error`.
- `collect`: **refused** 3/3 per cell as `MemoryError` — the typed
  CPython refusal from H3-SPILL-COLLECT-1, not the facade internal error.

## Cannot-spill / un-accounted cells (D-4 notes)

- `hash_join` — no upstream hash-join spill path; the build side fails the
  query on pool exhaustion. Epic:
  https://github.com/apache/datafusion/issues/24768 (consolidates #17267).
- `nested_loop_join` — the upstream spill fallback re-executes the build
  child and panics; repark contains it as the refusal above. Defect:
  https://github.com/apache/datafusion/issues/24661.
- `window_sliding`, `window_unbounded` — `BoundedWindowAggExec` /
  `WindowAggExec` take no pool reservation; the window cells are bounded
  only by the address-space cap. Upstream accounting epic:
  https://github.com/apache/datafusion/issues/22758.
- `dynamic_flatten` — `UnnestExec` takes no pool reservation; same epic:
  https://github.com/apache/datafusion/issues/22758.
- `collect` — the facade boundary is repark-side (no pool reservation);
  upstream accounting epic https://github.com/apache/datafusion/issues/22758.

## Findings this matrix adds

- **No cell reports `spilled` stably at this tier.** The only observed
  spill is `hash_aggregate-2x` rep 2 (7 350 412 902 B across 170 spill
  files, 42.2 s); the same cell refused in reps 1 and 3. At
  `target_partitions=4` the four concurrent operator reservations exhaust
  the fair pool before a spill cycle completes often enough to be the
  usual outcome — `refused` is the dominant terminal state at 1 GB.
- **`hash_join-4x` is consistently KILLED** (3/3, SIGABRT): with a 4.3 GB
  build side the pool saturates, then an un-accounted ~35 MB allocation in
  the join path hits the address-space cap and aborts. At 2× the pool
  refusal wins the race; at 8× the reservation's first big request is
  refused before the un-accounted allocations pile up. This cell is the
  honest upstream boundary — the fix is a spilling hash join, not a
  harness change.
- **`window_unbounded-2x` is UNSTABLE** (refused ×2, KILLED ×1): whether
  the `SortExec`'s spill reservation refuses first or the un-accounted
  `WindowAggExec` partition cache's ~531 MB allocation aborts first is a
  timing race.
- **`sort` refuses rather than spills** at all three multiples: the four
  concurrent `ExternalSorter`/`SortPreservingMergeExec` reservations exceed
  the fair share, so the first big reservation request is refused (the CI
  tier's 64 MB / 1-partition cell spills; the parameters differ).
- **`window_sliding` and `dynamic_flatten` complete at all three
  multiples**: both stream under the cap with no pool reservation and no
  accumulation — the bounded-memory end of the matrix.

## Never-OOM at v1.3

Never-OOM at v1.3 is the matrix as measured: 24 of 27 cells are stable at
2×/4×/8× of 1 GB. Each stable cell spills, completes, or refuses loudly —
no silent OOM. The three blocked cells stay as measured (S2-18; no
operator change, S2-6): `hash_join` 4× `KILLED`
(https://github.com/apache/datafusion/issues/24768), `hash_aggregate` 2×
`UNSTABLE` (https://github.com/apache/datafusion/issues/22758),
`window_unbounded` 2× `UNSTABLE`
(https://github.com/apache/datafusion/issues/22758). A DataFusion bump
that closes #24768 or #22758 re-runs `matrix_run.py --reps 3` as its pin.

Ledger: [../../task/ledgers/staging/neveroom-1-ledger.md](../../task/ledgers/completed/neveroom-1-ledger.md).

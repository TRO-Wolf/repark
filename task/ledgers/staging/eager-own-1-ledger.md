# Unit ledger — EAGER-OWN-1 · eager results own their materialization

**Date:** 2026-09-13 · **Branch:** `fix/eager-own-1` · **Base:** `8936346a`
**Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** The owner's
[eager materialization retention review](../../roadmap/mid-term/eager-materialization-retention-review-2026-09-13.md)
confirms in source that a bare `temp_df.eager()` (result not assigned) registers a
`__repark_cache_*` MemTable that nothing owns: the frame registry is a `WeakSet`,
`unpersist()` on the source does not know the dropped child, and only
`catalog.clearCache()` sweeps orphans by prefix. Each repeat adds a full result set
to resident memory. The review measured nothing, so step 0 measures first (D-7):
a deterministic 1,000,000-row TA `withColumns` fixture, ten bare `eager()` calls,
per-iteration wall / VmRSS / VmHWM / registration count, in a subprocess on the
release native. No product code changes in step 0.

**Not in this step:** the shared-ownership handle (D-2/D-3), the eager-on-eager
reuse fast path (D-4), the `test_cache_persist.py` orphan-assertion flip, and the
session cache budget (D-6 — separate card EAGER-BUDGET-1). `STATUS.md`,
`briefs/next-sequence.md`, `.github/`, `Cargo.toml`, `Cargo.lock`,
`pyproject.toml`, `uv.lock` are untouched.

## PROPOSITION LEDGER — EAGER-OWN-1 — 2026-09-13

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The base-tree defect is measured, not asserted: ten bare `temp_df.eager()` calls (results not assigned) on the deterministic 1e6-row, 25-column TA `withColumns` fixture leave ten `__repark_cache_*` registrations after the loop and after `gc.collect()`, with per-iteration wall / VmRSS / VmHWM recorded on the release native. | `python/repark-parity/tests/eager_own/` bench pin under `REPARK_EAGER_OWN_BENCH=1`; committed `docs/perf/eager-own-1-2026-09-13/base.json`. | **PROVEN** | Base `8936346a`, release native (`__debug_assertions__` False), Python 3.12.3, `nproc` 64, `free -g` total 125 GiB, under `systemd-run --user --scope -p MemoryMax=24G -p MemorySwapMax=0`. Per-iteration (s / VmRSS / VmRSS-gc / VmHWM / regs): 0: 0.827 / 546 MB / 546 MB / 1,012 MB / 1 · 1: 0.814 / 1,253 / 1,253 / 1,253 / 2 · 2: 0.804 / 1,196 / 1,196 / 1,492 / 3 · 3: 0.844 / 1,630 / 1,630 / 1,630 / 4 · 4: 0.805 / 1,803 / 1,803 / 1,858 / 5 · 5: 0.809 / 1,877 / 1,877 / 2,262 / 6 · 6: 0.815 / 2,052 / 2,052 / 2,354 / 7 · 7: 0.856 / 2,667 / 2,667 / 2,667 / 8 · 8: 0.810 / 2,457 / 2,457 / 2,945 / 9 · 9: 0.824 / 2,967 / 2,967 / 3,041 / 10. Post-loop regs 10, post-gc regs 10, post-clearCache regs 0; RSS stays 2,967 MB after clearCache (allocator retention, as the review predicted). Wall is FLAT (0.80–0.86 s) at this size on this box — the retention and RSS growth are measured; the reported per-iteration slowdown is not reproduced at 10 iterations under no paging pressure and is honestly recorded as such. JSON: [docs/perf/eager-own-1-2026-09-13/base.json](../../../docs/perf/eager-own-1-2026-09-13/base.json). pins: eager-own-1/C-001 |
| C-002 | Repeated eager calls on the original lazy plan, discarding results: abandoned eager registrations do not accumulate after their owners are released (review validation row 1). | Step-1 handle (D-2): pin asserts the registration count returns to 0 after `gc.collect()` without `clearCache()`. | **OPEN** | Awaits step 1. |
| C-003 | Reuse one eager frame across actions: no new materialization; values and Arrow types remain unchanged (review validation row 2). | Step-1 pin: two actions on one eager frame produce one registration and identical values/types. | **OPEN** | Awaits step 1. |
| C-004 | Eager called on an eager frame: backing reuse follows the D-4 identity contract — a new wrapper sharing the handle and shape, no collection, no new registration; a frame whose view was explicitly dropped materializes afresh. | Step-1 pin on both branches. | **OPEN** | Awaits step 1. |
| C-005 | Parent released while a `.lazy()` copy, a derived frame, a join or a union survives: remaining users still produce correct values and Arrow types (review validation row 4; D-2 handle union across join/union parents). | Step-1 pins per survivor shape. | **OPEN** | Awaits step 1. |
| C-006 | Explicit `unpersist()`, `clearCache()`, and session stop each stay documented-correct and idempotent under the handle; the finalizer never touches a stopped session (review validation row 5; D-2). | Step-1 pins; `weakref.finalize` with `atexit=False` and the stopped-session skip. | **OPEN** | Awaits step 1. |
| C-007 | A collection or post-registration failure leaves no unintended cache registration (review validation row 6). | Step-1 pin driving a materialize failure (`repark.cache.max_bytes` refusal) and asserting no `__repark_cache_*` residue. | **OPEN** | Awaits step 1. |
| C-008 | Several simultaneous eager results remain independently valid; cleanup matches the ownership contract (review validation row 7). | Step-1 pin: two live eager frames hold distinct registrations; releasing one leaves the other answering. | **OPEN** | Awaits step 1. |
| C-009 | Original source changes between eager calls are observed: fresh lazy-plan evaluation still sees the new source (D-5 — no plan-equivalence caching) and existing snapshots keep their semantics (review validation row 8). | Step-1 pin mutating the source between two `eager()` calls. | **OPEN** | Awaits step 1. |
| C-010 | Exported Arrow objects (`toArrow`, `toPandas`, `to_polars`, `collect`) stay valid after the registration is gone — exports share the MemTable buffers by refcount and need no handle (D-2). | Step-1 pin: export, drop the registration, then read the export. | **OPEN** | Awaits step 1. |
| C-011 | The `test_cache_persist.py` orphan assertion is flipped: after the handle, a GC'd cached frame's `__repark_cache_*` view no longer survives as an orphan — the pin asserts the inverse of today's "table SURVIVES the frame" row, and `clearCache()` remains the explicit sweep. | Step-1 edit of `test_clear_cache_drops_orphan_cache_views`. | **OPEN** | Awaits step 1. |
| C-012 | The step-0 harness re-run after the fix: registration count 0 after the loop + `gc.collect()`, RSS plateaus, per-iteration time flat — the before/after tables pasted beside C-001's. | `REPARK_EAGER_OWN_BENCH=1` re-run on the step-1 tree; `after.json` beside `base.json`. | **OPEN** | Awaits step 1. |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: eager-own-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Step 0 covers the card's measurement clause end to end — the worker builds the card's fixture (1e6 rows, one ordered series, the ~19-column TA withColumns chain), the pin runs it in a subprocess, and the base-tree assertion (ten bare eager calls leave ten registrations) is the defect the unit exists to fix. C-002..C-012 carry the review's full validation table forward as OPEN clauses, one per row, so step 1 cannot ship a partial contract.
      artifacts: [python/repark-parity/tests/eager_own/test_eager_own.py, python/repark-parity/tests/eager_own/eager_own_worker.py]
    - id: AT-2
      status: ATTACKED
      evidence: The harness runs the defect path itself — bare eager() with the result unassigned, gc.collect() between the loop and the count, and clearCache() as the named relief — rather than a proxy. The small pin (2,000 rows, 3 iterations) exercises the identical worker path in the default suite so CI validates the harness, not just the opt-in bench.
      artifacts: [python/repark-parity/tests/eager_own/test_eager_own.py]
    - id: AT-3
      status: N/A
      justification: Step 0 adds test-only Python under python/repark-parity/tests — no Rust, no product code, no shared or mutable state across the worker boundary (one subprocess per run, JSON out).
    - id: AT-4
      status: N/A
      justification: No concurrency: the worker is a single subprocess; the pin waits on it with a timeout.
    - id: AT-5
      status: N/A
      justification: No authn/authz, deserialization of hostile input, path, credential, or network surface; the worker reads /proc/self/status and writes one JSON file to an argv path under the test's tmp dir.
    - id: AT-6
      status: ATTACKED
      evidence: The registration count is read off catalog.listTables() — the same session-catalog surface the review names — and cross-checked against session.list_temp_view_names(), so the count cannot drift from what clearCache() would sweep.
      artifacts: [python/repark-parity/tests/eager_own/eager_own_worker.py]
    - id: AT-7
      status: ATTACKED
      evidence: This is the measurement clause: per-iteration wall time, VmRSS after the call and after gc.collect(), VmHWM peak, and registration counts are recorded per iteration plus post-loop, post-gc, and post-clearCache rows, on the release native (__debug_assertions__ is False), under a 24G systemd-run scope so a runaway cannot kill the box. The committed base.json carries the machine header.
      artifacts: [docs/perf/eager-own-1-2026-09-13/base.json, python/repark-parity/tests/eager_own/eager_own_worker.py]
    - id: AT-8
      status: ATTACKED
      evidence: The measured path was read in source before the harness was written: eager.py::_eager_materialize, core.py::_materialize_cache_if_needed / _register_cache_frame / unpersist, catalog.py::clear_cache, _temp_views.scratch_view_name, and the catalog.list_tables hidden-name roster (the __repark_cache_ prefix is NOT hidden, so listTables counts it).
      artifacts: [task/ledgers/staging/eager-own-1-ledger.md]
    - id: AT-9
      status: N/A
      justification: No new user-visible behavior in step 0; the pin documents the base defect and step 1 flips the same assertion shape to zero.
    - id: AT-10
      status: ATTACKED
      evidence: The small pin is the live mutation check for step 1: it asserts registrations == iterations on the base tree, so any handle implementation that leaks or double-drops changes a counted assertion rather than passing silently.
      artifacts: [python/repark-parity/tests/eager_own/test_eager_own.py]
  complete: true
```

## Evidence

**C-001 measure (base `8936346a`, release native).**
`.venv/bin/python -c 'import repark._native as n; print(n.__debug_assertions__)'`
printed `False`. Box idle first (`pgrep -x cargo|rustc|maturin` all empty), then:

```
systemd-run --user --scope -p MemoryMax=24G -p MemorySwapMax=0 \
  env REPARK_EAGER_OWN_BENCH=1 \
  REPARK_EAGER_OWN_BENCH_JSON=/tmp/e-own/docs/perf/eager-own-1-2026-09-13/base.json \
  .venv/bin/python -m pytest python/repark-parity/tests/eager_own -q
→ 2 passed in 9.93 s
```

Machine: `nproc` = 64, `free -g` total = 125 GiB, Python 3.12.3. Per-iteration
table (iteration · wall s · VmRSS after call · VmRSS after gc · VmHWM ·
`__repark_cache_*` in `catalog.listTables()`):

| iter | s | VmRSS (MB) | VmRSS-gc (MB) | VmHWM (MB) | regs |
|---|---|---|---|---|---|
| 0 | 0.827 | 546 | 546 | 1,012 | 1 |
| 1 | 0.814 | 1,253 | 1,253 | 1,253 | 2 |
| 2 | 0.804 | 1,196 | 1,196 | 1,492 | 3 |
| 3 | 0.844 | 1,630 | 1,630 | 1,630 | 4 |
| 4 | 0.805 | 1,803 | 1,803 | 1,858 | 5 |
| 5 | 0.809 | 1,877 | 1,877 | 2,262 | 6 |
| 6 | 0.815 | 2,052 | 2,052 | 2,354 | 7 |
| 7 | 0.856 | 2,667 | 2,667 | 2,667 | 8 |
| 8 | 0.810 | 2,457 | 2,457 | 2,945 | 9 |
| 9 | 0.824 | 2,967 | 2,967 | 3,041 | 10 |

Post-loop: 10 registrations, VmRSS 2,967 MB. Post-`gc.collect()`: 10
registrations, VmRSS 2,967 MB — the orphans survive collection exactly as the
review's confirmed path says. Post-`clearCache()`: 0 registrations; VmRSS still
2,967 MB — the allocator retains the freed arenas, matching the review's "a
high resident reading alone does not prove live-object retention" caveat; the
registration count is the concrete ownership signal. Per-iteration wall is
flat (0.80–0.86 s) at this size on this box — the retained-result accumulation
and RSS growth are measured; the reported per-iteration slowdown is NOT
reproduced at ten iterations on a 125 GiB host and the claim is recorded
honestly rather than inflated. Raw run:
[docs/perf/eager-own-1-2026-09-13/base.json](../../../docs/perf/eager-own-1-2026-09-13/base.json).

The small pin (`test_bare_eager_registrations_accumulate_small`, 2,000 rows ×
3 iterations) is green on the base tree in the default suite and is the pin
step 1 flips to a post-gc count of 0.


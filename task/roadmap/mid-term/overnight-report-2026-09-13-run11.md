# Overnight report — run 11 of 2026-09-13 (EAGER-OWN-1)

**Session:** one Opus orchestrator (overnight-11), 2026-09-13 08:27 → 12:03 local.
**Grants:** G-1 (squash-merge on green), G-2 (bounded decisions), G-3 (stop when the unit merges or by 20:00),
G-4 **Devin** actor, **Grok** critic-logic and Python perf reviewer (Grok actor only as fallback; no Muse, no GLM), G-5 no.
STATUS.md, tags and the release pipeline untouched. **Procedure:**
[overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md).
**Card:** [eager-own-1-card-2026-09-13.md](eager-own-1-card-2026-09-13.md).

## 1. What landed

| Unit | PR | Merged | Rounds |
|---|---|---|---|
| EAGER-OWN-1: a refcounted handle owns every `__repark_cache_*` view; bare `eager()` no longer leaks; eager-on-eager reuses the backing | #565 | `abed32c9` | Devin 4 (step 0, step 1, step 2, review fix; one session, $0), Grok critic-logic 1 ($1.00), Grok Python perf reviewer 1 ($1.06) |

Ledger: [eager-own-1-ledger.md](../../ledgers/completed/eager-own-1-ledger.md). The owner's review is archived at
[docs/history/eager-own-1/](../../../docs/history/eager-own-1/map.md). Nothing is parked. Devin needed no Grok fallback.

## 2. Step 0 — before and after

Fixture: 1,000,000 rows, one ordered series (`ts`, `open`, `high`, `low`, `close`, `volume`, numpy seed 20260913), a
`withColumns` chain of 19 TA and plain columns (ema ×3, sma ×3, rsi ×2, adx, linearreg, linearreg_slope, trange, atr,
mom, ema over true range, a ratio, a round, a null fill, a string condition): 25 columns. Ten bare `temp_df.eager()`
calls, results not assigned. Subprocess worker on the release native (`__debug_assertions__` False), 64 cores,
125 GiB, under `systemd-run --scope MemoryMax=24G`. Harness: `python/repark-parity/tests/eager_own/`
(`REPARK_EAGER_OWN_BENCH=1`); raw JSON in `docs/perf/eager-own-1-2026-09-13/{base,after}.json`.

| iter | before s | after s | before RSS MB | after RSS MB | before regs | after regs |
|---|---|---|---|---|---|---|
| 0 | 0.827 | 0.802 | 546 | 540 | 1 | 0 |
| 1 | 0.814 | 0.818 | 1,253 | 1,041 | 2 | 0 |
| 2 | 0.804 | 0.916 | 1,196 | 1,096 | 3 | 0 |
| 3 | 0.844 | 0.833 | 1,630 | 1,109 | 4 | 0 |
| 4 | 0.805 | 0.869 | 1,803 | 917 | 5 | 0 |
| 5 | 0.809 | 0.820 | 1,877 | 858 | 6 | 0 |
| 6 | 0.815 | 0.797 | 2,052 | 959 | 7 | 0 |
| 7 | 0.856 | 0.864 | 2,667 | 1,210 | 8 | 0 |
| 8 | 0.810 | 0.838 | 2,457 | 1,059 | 9 | 0 |
| 9 | 0.824 | 0.829 | 2,967 | 1,199 | 10 | 0 |

| Reading | Before (`8936346a`) | After |
|---|---|---|
| registrations after the loop / after `gc.collect()` | 10 / 10 | 0 / 0 |
| RSS after the loop | 2,967 MB | 1,199 MB |
| peak RSS (VmHWM) | 3,041 MB | 1,257 MB |
| RSS after `clearCache()` | 2,967 MB (registrations 0; the allocator keeps the freed arenas) | 1,199 MB |
| per-iteration wall | 0.80–0.86 s, flat | 0.80–0.92 s, flat |

The leak is measured and fixed: each abandoned result held about 270 MB until `clearCache()`. **The reported
slowdown did not reproduce** on this 125 GiB box: wall time stayed flat with ~3 GB retained. The owner's slowdown is
most likely memory pressure (paging, allocator growth) on a smaller machine or a longer loop. EAGER-BUDGET-1 step 0
measures that under a memory cap.

## 3. What the handle looks like

`python/repark/src/repark/spark/dataframe/cache_handle.py` (new):

- `CacheViewHandle` — slots `_session`, `_alive_token`, `_view_name`, `_released`, `_finalizer`. `__init__` arms
  `weakref.finalize(handle, _drop_cache_view_registration, session, alive_token, view_name)` with `atexit = False`
  and adds the handle to a per-session `WeakSet` (`alive_token["cache_view_handles"]`). `release()` detaches the
  finalizer, drops the registration if the session is alive, and is idempotent.
- `_drop_cache_view_registration` — a module-level callback that holds neither handle nor frame. It returns at once
  on a stopped session and logs a drop failure at debug level (explicit drops still raise).
- Frames carry `_handles: tuple` (the shared empty tuple when none) and the registering frame also has
  `_cache_view_owned_handle`. `DataFrame._spawn(inner, *others)` passes `self._handles` and unions another frame's
  handles only when it has some, so a frame without handles pays one attribute store per spawn.
- `unpersist()` on the owner drops the registration. On an eager-on-eager wrapper it releases only that wrapper's
  hold (R11-D-1). `clearCache()` releases every live handle first, then runs the frame loop and prefix sweep as before.
- `eager()` on a frame with a live cache view and a shape returns a wrapper that shares the view, handle and shape:
  no collection, no registration (D-4).
- Measured on the base tree: a frame built over a cache view keeps answering after the registration is dropped,
  because the native plan holds the table provider. The handle governs the catalog name, and the data lives as long as
  some plan holds it.
- `core.py` 4117 → 4094 lines. `_warn_storage_level_cosmetic_once` moved verbatim to `cache_handle.py` to stay under
  the ceiling (R11-D-5). Both size baselines moved down.

## 4. The critic's and the reviewer's verdicts

**Grok critic-logic** (read-only, fresh clone at `e1587cbb`, 30 turns): **FINDINGS — 0 P1, 2 P2, 3 P3.** It found
no wrong result, no premature drop and no leak on any D-2 holder kind, and no call into a stopped session. It checked
`createOrReplaceTempView`, aliases, SQL by the cache name, `mapInArrow`, checkpoint, writers, `explain`, a new session
after `stop()`, and D-5 with a rewritten CSV. Eleven mutations were run. The stopped-session skip is pinned: removing
the `alive` check fails the spy pin.
- L-001 (P2): 14 D-2 holder kinds had no failing pin (withColumn/s, limit, offset, orderBy, sort, groupBy.agg,
  distinct, drop, sample, intersect, subtract, crossJoin, mapInArrow). **Fixed**: a parametrized pin, every arm red
  under the propagation-strip mutation.
- L-002 (P2): `atexit = False` unpinned. **Fixed** (red under `atexit = True`).
- L-003 (P3) `_collapse_base` can mask stripping: closed by L-001's `_handles` assertion. L-004 (P3) `derived.eager()`
  keeps the parent's handle too, which is bounded and documented. L-005 (P3) the `_released` guard: pinned.

**Grok Python perf reviewer (S2-21)** (read-only, branch and base trees with the same release native, 38 turns): **PASS — no P1, no P2, one P3.** The dependent `withColumn` chain is inside noise at every depth (build median 50: 40.6 vs 41.0 ms, −0.94 %; 200: 4.09 vs 4.06 s, +0.70 %; 500: 192.8 vs 195.2 s, −1.21 %; count −1.02 % / +0.04 % / −0.37 %). A `_spawn` with no handle costs +650 ns (+2.3 %) and allocates nothing; `filter` +0.97 %. Eager-on-eager takes 66 µs against 4.66 ms on base. C-012 re-measured (0 registrations, RSS plateau). P-001 (P3, ledger): `union_handles` is O(k²) and always copies, so a two-handle `_spawn(inner, other)` costs +1.85 µs (+7.1 %). Typical k is 1–2. Side fact: the `withColumn` chain build is superlinear on both trees (41 ms → 4.1 s → 193 s at depth 50 / 200 / 500), already known and not this unit.

## 5. Decisions under §6

- R11-D-1 The registering frame owns the handle. `unpersist()` on the owner and `clearCache()` drop the view;
  `unpersist()` on a D-4 wrapper releases only its hold. Needed because D-4 reuse would otherwise let one wrapper's
  `unpersist()` drop a view its sibling scans. The base-tree measurement (plans keep answering after a drop) makes this
  safe.
- R11-D-2 Handle propagation is an immutable tuple with no allocation per spawn when empty; every two-frame spawn site
  is audited in the ledger.
- R11-D-3 Finalizer contract: module-level callback, `atexit = False`, stopped-session skip, GC-path errors logged
  at debug.
- R11-D-4 `clearCache()` releases handles before its existing loop and sweep.
- R11-D-5 Moved `_warn_storage_level_cosmetic_once` verbatim to `cache_handle.py` so `core.py` stays under its
  4117 ceiling without condensing unrelated code. `test_dfcore_1_exports.py` was at the 1000-line default ceiling, so
  its expected tables split into `_dfcore_1_expected.py` (the gate's sanctioned out).
- R11-D-6 Step 0's slowdown claim stays unreproduced rather than forced. D-7 only allows claims backed by both numbers.
- R11-D-7 The critic and reviewer ran while Devin did step 2 (docs and docstring trims only). The review fix round
  added pins and no product code.
- R11-D-8 Branch rebased onto `f0951855` (#564, ABS-EXPR-1). Two map conflicts, both sides kept. The release native
  was rebuilt once and copied into the critic and reviewer clones (identical `.so` sha on base and branch).

## 6. Owner questions — recommendations

- **Q-E1 (cache()/persist() views also die with their last holder?)** — **Recommend yes (D-3 as built).** A repark
  cache is keyed by object identity, not by plan as Spark's CacheManager is. Once every frame that scans the view is
  gone, no later frame can reach it, so keeping it only holds memory until `clearCache()`. Explicit `unpersist()` and
  `clearCache()` behave as before. One mechanism covers both, and splitting it would bring back the orphan path for
  `cache()`.
- **Q-E2 (EAGER-BUDGET-1 policy)** — **Recommend refuse (default), no eviction.** With the handle in place, an un-held
  snapshot no longer exists: it dies with its last holder. Every view the budget could see is held by a live frame, so
  "evict oldest un-held" has nothing to evict, and evicting a held view would break the review's "never silently evict a
  live snapshot" rule. A clear refusal naming the budget, the retained bytes and `unpersist()` is the honest option.

## 7. Card opened — EAGER-BUDGET-1 (seeded)

### Card EAGER-BUDGET-1 — a session cache budget and retained-bytes accounting (from EAGER-OWN-1, 2026-09-13)

**Why.** EAGER-OWN-1 made cache views die with their last holder. Live results can still exceed a machine:
`repark.cache.max_bytes` is checked per result and only after the full collection
(`temp_views.rs::register_collected_memtable`). There is no session total and no way to read retained bytes. Measured
2026-09-13 (release native, 1e6 × 25-column TA fixture): one result holds **~270 MB** of RSS (546 → 2,967 MB over
ten retained results); the collection peak for one result is **~1.26 GB VmHWM** after the fix; RSS does not return
after `clearCache()` (2,967 MB with zero registrations: allocator retention), so RSS cannot serve as the accounting
signal. The reported per-call slowdown did **not** reproduce on a 125 GiB host at ten iterations (wall flat
0.80–0.86 s with ~3 GB retained).

**Decisions (proposed; Q-E2 rules the first).** D-1 `repark.cache.max_total_bytes` (session, default unset = no
budget). A materialization that would exceed it refuses with a named error carrying the budget, the retained bytes and
the fix (`unpersist()`), and never evicts. D-2 retained bytes are the sum of distinct Arrow buffers across live
cache views (dedupe shared buffers by address), exposed read-only (`spark.catalog` or a `repark` conf readback). D-3
admission is incremental: the budget is checked during collection, batch by batch, so a refusal happens before the
full peak. D-4 per-result `max_bytes` keeps its meaning. D-5 no spill (a spill-backed cache is its own decision).

**Steps.** 0 (Devin, measure): reproduce the slowdown under memory pressure. Run the EAGER-OWN-1 harness on the
**base of EAGER-OWN-1** under `MemoryMax` 2 / 4 / 8 GB with 30 iterations, recording per-iteration wall, RSS,
major faults and swap. Run the same on the fixed tree. Also measure how retained bytes compare with the MemTable's
reported size. 1 (Devin): D-2 accounting and its readback. 2 (Devin): D-1 + D-3 incremental admission in
`temp_views.rs`, one pin per door. **Home:** `crates/repark-core/src/session/temp_views.rs`, its `repark-python`
binding, `python/repark/src/repark/spark/dataframe/{eager,cache_handle}.py`, `spark/catalog.py`, pins. **Gates:** as
EAGER-OWN-1 plus the Rust Gates; S2-21 Rust and Python reviewers.

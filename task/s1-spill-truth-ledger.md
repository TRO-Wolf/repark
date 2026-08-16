# Unit ledger — S-1 spill truth and reach

**Unit:** S-1 · conductor-15 Track T1 · **Date:** 2026-08-15 ·
**Lane:** `/tmp/grok-s1` · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-s1` · **Branch:** `grok/s1-spill-truth` ·
**Base (FROZEN):** `8cbde88`.

**Charter:** conductor-15 T1 + addendum A2–A6 / A12. Source:
`planning/hardening/SPILL-RECON-2026-08.md` §1 / §3 / §4. Registry / STATUS /
PROJECT / ARCHITECTURE / `Cargo.lock` / `[patch]` CLOSED.

## GO / deferred

| Name | Class | Disposition |
|---|---|---|
| R1 FairSpillPool SET | A3 fallback (swap, not resize) | **SHIPPED** in this commit |
| R2 temp_directory | refuse-loud + build-time wire | follow commit on this branch |
| R3 RAM-relative default | `clamp(0.6 × detected, MIN, 8 GiB)` | follow commit on this branch |
| `max_temp_directory_size` | residual | unless it fits with no ceiling drama |
| in-place FairSpillPool resize | DF 54.1 has no seam | documented; swap is the path |

## Files (R1)

- `crates/repark-core/src/session/spill.rs` — extract + SET intercept + build apply
- `crates/repark-core/src/session.rs` — `mod spill`; `sql_with` intercept; pool resolve
- `crates/repark-core/src/session/map.md`, `src/map.md`, `map.md`
- `python/repark/src/repark/spark/session/builder_conf.py` — one-truth prose now accurate
- `python/repark/tests/test_t2_sort_memory.py` — `fair(` required, `greedy(` forbidden
- `python/repark/tests/test_t2_spill_reach.py` — recon §3 battery
- `python/repark/tests/map.md`, `task/map.md`, this ledger

## Mutation-proof pins

| If this is dropped… | this test reds |
|---|---|
| SET lands in GreedyMemoryPool | `runtime_set_memory_limit_oom_is_fair_not_greedy` + t2 `fair(`/`greedy(` |
| builder pseudo-key does nothing | `builder_datafusion_memory_limit_installs_fair_spill_pool` |
| dual knobs silently pick one | `builder_dual_memory_knobs_refuse` |
| SET `0` stays bounded | `runtime_set_memory_limit_zero_is_unbounded` |
| sort/agg no longer spill | `test_*_spills_under_small_fair_pool` |
| hash join / array_agg start spilling | pinned AS `Resources exhausted` |

## Notes

A3: `FairSpillPool.pool_size` is outside the mutex in datafusion-execution 54.1.0.
There is no in-place resize. R1 swaps a new FairSpillPool via
`RuntimeEnvBuilder::from_runtime_env` + `SessionStateBuilder` (same swap DF
does, but fair). In-flight reservations stay on the old pool.

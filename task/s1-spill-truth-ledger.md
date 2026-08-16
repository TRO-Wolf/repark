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
| R2 temp_directory | refuse-loud + build-time wire | **SHIPPED** in the R2 commit |
| R3 RAM-relative default | `clamp(0.6 × detected, MIN, 8 GiB)` | **SHIPPED** in the R3 commit |
| `max_temp_directory_size` | residual | unless it fits with no ceiling drama |
| in-place FairSpillPool resize | DF 54.1 has no seam | documented; swap is the path |

## Files (R3)

- `session/spill.rs` — `default_memory_limit_bytes` / cgroup+MemTotal parse / clamp
- `session.rs` + `session/tests.rs` — default pin flipped to Finite / floor / cap / helper
- `builder_conf.py` / `_funcs.py` / `dataframe/core.py` — 8 GiB default prose → RAM-relative
- maps + this ledger

## Files (R2)

- `session/spill.rs` — `TEMP_DIRECTORY_KEY` in `REPARK_OWNED_*`; build-time
  `with_temp_directory`; runtime SET refuse names `TMPDIR`
- `session.rs` — apply `with_temp_directory` before `RuntimeEnv` build
- `test_t2_sort_memory.py` — runtime refuse + no store + builder `datafusion-*` dir
- maps + this ledger

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

## Follow-up (same branch)

Facade probe after R3: builder `temp_directory` was re-SET by `_apply_builder_datafusion_conf`
and refused. Skip that key (Rust already applied it). Spill-reach recipes retuned:
grouping sets over `md5` (not `id % 8`); SMJ on `md5` (range is pre-sorted); hash_join /
array_agg use a 16 MiB pool + payload. 27/27 green in 21s.

## Notes

A3: `FairSpillPool.pool_size` is outside the mutex in datafusion-execution 54.1.0.
There is no in-place resize. R1 swaps a new FairSpillPool via
`RuntimeEnvBuilder::from_runtime_env` + `SessionStateBuilder` (same swap DF
does, but fair). In-flight reservations stay on the old pool.

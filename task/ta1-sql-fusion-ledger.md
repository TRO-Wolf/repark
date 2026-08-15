# Unit ledger — TA-1 SQL same-OVER WindowAggExec fusion

**Unit:** TA-1 · conductor-13 Track T2 · **Date:** 2026-08-15 ·
**Lane:** `/tmp/grok-ta` · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-ta` · **Branch:** `grok/ta1-sql-fusion` ·
**Base (FROZEN):** `cd0db4f459e62994b45f8aadd1d5b58f040d90a5` (#114 on origin/main).

**Charter:** `planning/grok/BRIEF-ta-wave-13.md` TA-1 + conductor-13 Addendum
A1–A12 (A12: TA-1 touches neither `crates/repark-ta` nor existing
`test_ta.py`). **SEPMO:** acc (Actor → Critic-1 quality → Critic-2 security).
Floor S1. Risk tier: mechanical/standard (test-only).

CLOSED: `crates/repark-ta/**`, `python/repark/src/repark/spark/ta.py`,
`python/repark/tests/test_ta.py`, lockfiles, STATUS, registry, merge/mod.rs,
functions package, `position_delete.rs`. No engine edits.

## Charge

Perf-note idea 11: DataFrame-door fusion is already pinned (N2 in
`python/repark/tests/test_ta.py` — **read-only**). SQL-door fusion was
ASSUMED. Pin via EXPLAIN / physical-plan string inspection on both doors:

1. A SELECT with many `ta_*(…) OVER w` items sharing PARTITION BY / ORDER BY
   must plan **one** `WindowAggExec`.
2. Negative: an intervening filter between windows MAY stack — pin TRUTH.

N2 mechanic (reference only): `plan.count("WindowAggExec") == 1`.

## Probe (2026-08-15, both doors, deleted after pinning)

Measured with `create_physical_plan` indent-display. EXPLAIN `physical_plan`
Utf8 column agreed on every count.

| Shape | Spark door | ANSI door (`TaExtension`) |
|---|---|---|
| named `OVER w` ×4 (`ema`/`sma`/`rsi`/`mom`) | **1** | **1** |
| inline same `OVER (PARTITION BY sym ORDER BY ts)` ×4 | **1** | **1** |
| source `WHERE close > 0` *below* both windows | **1** | **1** |
| window → `WHERE close > 0` → window, **both outputs live** | **2** | **2** |
| window → `WHERE ema5 IS NOT NULL` → window, both live | **2** | **2** |
| window → `WHERE close > 0` → window, `ema5` **not** selected | **1** | **1** |

The last row is dead-code elimination of the unused first window, **not**
fusion. The stacked pins keep `ema5` in the outer SELECT so the count cannot
collapse that way.

Predicate pushdown of `close > 0` moves `FilterExec` below the first window
but does **not** re-fuse the two logical `WindowAggr` nodes. Same stacking as
N2 `test_stage_b_filter_blocks_merge` (DataFrame door).

Fusion HOLDS on both doors. No fusion-failure FINDING.

## What landed

| Artifact | Path | Role |
|---|---|---|
| Spark-door pins | `crates/repark-spark/tests/ta_window.rs` | named + inline + intervening filter |
| ANSI-door pins | `crates/repark-sql/tests/ta_toll.rs` | same SQL shapes, native + `TaExtension` |
| Maps | `crates/repark-spark/tests/map.md`, `crates/repark-sql/tests/map.md`, `task/map.md` | lockstep |
| This ledger | `task/ta1-sql-fusion-ledger.md` | unit record |

No `crates/repark-ta` edit. No `test_ta.py` edit.

## Mutation-proof pins

| Behavior | Spark door | ANSI door |
|---|---|---|
| named `OVER w` ×4 → 1 `WindowAggExec` | `sql_same_named_over_window_fuses_to_one_window_agg_exec` | same name in `ta_toll` |
| inline same-spec ×4 → 1 | `sql_same_inline_over_spec_fuses_to_one_window_agg_exec` | same |
| intervening filter (input-col **and** window-output), both outputs live → 2 | `sql_intervening_filter_between_windows_stacks_window_agg_exec` | same |

Each pin counts `WindowAggExec` on **both** `create_physical_plan` indent-display
and the `EXPLAIN` `physical_plan` row, and requires the TA function names to
remain in the plan text (guards DCE).

## ACC

- Risk tier: mechanical/standard. Test-only. Floor S1.
- Cycle 1 Critic-1 Q-001 S1: byte-slicing plan text at 2000 for the assertion
  message can panic on a multibyte UTF-8 boundary. REMEDIATED — print the
  whole plan string.
- Cycle 1 Critic-1 Q-002 S2: first probe dropped `ema5` from the outer
  SELECT; DF eliminated the first window and reported 1. REMEDIATED — stacked
  SQL keeps both outputs live; tokens `ta_ema`/`ta_sma` required.
- Critic-2: CLEAN (static SQL, no user input, no AWS, EXPLAIN does not
  execute, no engine surface).
- Label: `ACC-CONVERGED`.

## FINDINGS

None. Same-OVER fusion holds (1) on both SQL doors. Intervening filter between
live windows stacks (2) on both doors. Pin records that truth.

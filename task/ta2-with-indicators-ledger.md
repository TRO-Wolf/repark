# Unit ledger — TA-2 `ta.with_indicators` serving helper

**Unit:** TA-2 · conductor-13 Track T2 · **Date:** 2026-08-15 ·
**Lane:** `/tmp/grok-ta` · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-ta` · **Branch:** `grok/ta2-with-indicators` ·
**Base (FROZEN):** `cd0db4f459e62994b45f8aadd1d5b58f040d90a5` (independent of TA-1 #116).

**Charter:** `BRIEF-ta-wave-13.md` TA-2 + conductor-13 Addendum A12.
**SEPMO:** acc + C4. Floor S1. Sequential hat-switch Actor → Critic-1 → Critic-2 → C4.

A12 / file fence: `ta.py` (additive helper + `__all__`), NEW `test_ta_*.py`,
map.md rows (this lane only), this ledger + `task/map.md`. CLOSED:
`crates/repark-ta/**`, existing `test_ta.py`, `ta_window.rs`, `ta_toll.rs`,
lockfiles, STATUS, registry, functions package, merge, position_delete.

## Intent

`ta.with_indicators(df, *, partition, order, columns, null_lookback=False,
last_row=False)` so ETL cannot forget `partitionBy`. Missing partition is the
silent cross-symbol RSI footgun (one global series across symbols that share
timestamps). Implement with existing plan pieces only (`over_columns` +
`row_number`/`max`). No engine edits.

## ACC

- Risk tier: standard. Floor S1. acc + C4. Sequential hat-switch.
- Cycle 1 Critic-1 Q-001 S1: leak fixture used interleaved *different*
  timestamps; charter requires **same timestamps**, two symbols.
  REMEDIATED — `_two_symbol_bars` now shares `ts` 0..N-1.
- Cycle 1 Critic-1 Q-002 S1: `getattr(column, "_lookback", None)` is stolen
  by `Column.__getattr__` (returns a field-access Column). A non-TA value
  plus `null_lookback=True` would not refuse. REMEDIATED — `isinstance`
  on `_LookbackAwareColumn` / `_NullLookbackColumn`. Pin in the refuse test.
- Cycle 1 Critic-1 Q-003 S2: last_row `max` over partition-only window hits
  DataFusion `ORDER BY column cannot be empty`. REMEDIATED — same ORDER BY
  as the TA window + `rowsBetween(unboundedPreceding, unboundedFollowing)`
  so it is a partition max, not a running max.
- Cycle 1 Critic-1 Q-004 S2: plan tokens were short `ema`/`rsi` (DCE-weak).
  REMEDIATED — assert live `ta_ema` / `ta_sma` / `ta_rsi` / `ta_mom`.
- Critic-2: CLEAN after Q-001/Q-002 (no SQL concat on partition/order;
  `str` is not character-iterated; caller `columns` dict is copied).
- C4: 10/10 named claims pinned (export, required kwargs, empty/bad refuse,
  Arrow value+type, list vs str, same-ts leak, last_row count+values+plan=2,
  fused plan=1 + tokens, null_lookback thread, last_row+null_lookback).
  Non-TA `null_lookback` refuse is in the empty/bad test.
- **Label: `ACC-CONVERGED`.**

## Gates (real exit codes)

| Gate | Exit |
|---|---|
| `make verify` | **0** |
| `make py-test-facade` (via preflight) | **0** (3129 passed, 71 skipped; +10 vs TA-1's 3119) |
| `make audit` (via preflight) | **0** |
| `make workflows-lint` (via preflight) | **0** |
| `make preflight` | **0** |

## Files

- `python/repark/src/repark/spark/ta.py` — helper + `_LookbackAwareColumn`
  so default factories carry lookback without applying the rewrite
- `python/repark/tests/test_ta_with_indicators.py` — NEW pins (A12)
- `python/repark/src/repark/spark/map.md`, `python/repark/tests/map.md`,
  `task/map.md` — this lane's rows only

## Mutation-proof pins

| Behavior | Test |
|---|---|
| Public export | `test_with_indicators_is_exported` |
| `partition`/`order` keyword-only, no default | `test_with_indicators_partition_and_order_are_required_keyword_only` |
| Empty / bad partition or order refuses | `test_with_indicators_refuses_empty_or_bad_partition` |
| Non-TA column + `null_lookback` refuses | `test_with_indicators_refuses_empty_or_bad_partition` |
| Arrow value+type ≡ hand-built `over_columns` | `test_with_indicators_matches_hand_built_over_columns_arrow_value_and_type` |
| list vs str partition/order | `test_with_indicators_list_partition_and_order_match_str_form` |
| Cross-symbol RSI leak vs helper (same ts) | `test_cross_symbol_rsi_without_partition_leaks_helper_does_not` |
| `last_row` count + last-bar values + plan=2 | `test_last_row_row_count_and_values` |
| One `WindowAggExec` (N2 + `ta_*` tokens) | `test_with_indicators_plan_is_one_window_agg_exec` |
| `null_lookback` via `_NullLookbackColumn` | `test_null_lookback_threads_through_existing_helper` |
| `last_row` + `null_lookback` | `test_last_row_with_null_lookback_keeps_last_bar_values` |

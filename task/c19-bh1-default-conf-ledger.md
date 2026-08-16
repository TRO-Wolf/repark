# Unit ledger — BH-1 default-conf TA bench primaries

**Unit:** BH-1 · conductor-19 · **Date:** 2026-08-16 ·
**Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-c19` · **Branch:** `grok/c19-bh1-default-conf` ·
**Base:** origin/main @ 3a9bc63 (A1 fallback; fetch failed host-side).

**Charter:** `BRIEF-conductor-19.md` + Addendum 2026-08-16 Q&A round 1 (A2, A3).
Measure-only. No engine / lockfile / pyproject edits.

## Intent

The P-2 scripts hardcoded `target_partitions=1` on every caller. Unset already
plans `Hash([symbol], num_cpus)`, so that default manufactured a fake "tp
lever". PRIMARY recorded results now run at default conf (knob omitted).
`tp=1` stays only as labeled isolation.

## What shipped

| Artifact | Path |
|---|---|
| Emit/session contract | `python/repark-parity/bench/ta/target_partition_contract.py` |
| Callers | six `bench_*.py` under `bench/ta/` |
| Session docstring | `python/repark-parity/bench/ta/harness.py` |
| Bench map | `python/repark-parity/bench/ta/map.md` |
| Contract pins | `python/repark-parity/tests/test_ta_bench_conf.py` |
| This ledger | `task/c19-bh1-default-conf-ledger.md` + `task/map.md` row |

## Cell contract (A2 / A3)

- Default/primary: omit `repark.target.partitions`; emit `target_partitions=default`.
- Isolation: `target_partitions=1` + `isolation=single_core` (no spaces in kv).
- `bench_many_symbols`: isolation + default `partition_by_symbol` + cliff at
  default conf. No third explicit-cores cell.
- `bench_batch_size`: stays isolation (SortExec lever; not a primary).

## Honest cuts

- Numbers are not re-transcribed here (measurement-only code change).
- Skipped `last_row` collect lines still omit the tp field (A7 ignores them).

## Gates (real exit codes)

| Gate | Exit |
|---|---|
| `make verify` | **0** |
| `make py-test` | **0** (160 passed, includes `test_ta_bench_conf.py`) |
| `make preflight` | **0** (facade 3271 passed, 70 skipped) |

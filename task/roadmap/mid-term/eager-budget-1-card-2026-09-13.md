# Card EAGER-BUDGET-1 — a session cache budget and retained-bytes accounting

**Date:** 2026-09-13 · **Opened by:** run 11 ([overnight-report-2026-09-13-run11.md](overnight-report-2026-09-13-run11.md) §7) ·
**Worked by:** run 12b · **Follows:** [eager-own-1-card-2026-09-13.md](eager-own-1-card-2026-09-13.md) (Q-E2 ruled there).

## Why

EAGER-OWN-1 made cache views die with their last holder. Live results can still exceed a machine:
`repark.cache.max_bytes` is checked per result and only after the full collection
(`crates/repark-core/src/session/temp_views.rs::register_collected_memtable`). There is no session total and no way to
read retained bytes. Measured 2026-09-13 (release native, 1e6 × 25-column TA fixture): one result holds ~270 MB of RSS;
the collection peak for one result is ~1.26 GB VmHWM after EAGER-OWN-1; RSS does not return after `clearCache()`
(allocator retention), so RSS cannot serve as the accounting signal. The reported per-call slowdown did not reproduce on
a 125 GiB host at ten iterations.

## Owner rulings

- **Q-E2 (2026-09-13): REFUSE, never evict.** A materialization that would take retained cache bytes past the session
  budget refuses; no live view is ever evicted.

## Decisions

| id | decision |
|---|---|
| D-1 | `repark.cache.max_total_bytes`: a session conf (runtime `spark.conf.set` or builder `.config`, read at materialize time like `repark.cache.max_bytes`); unset or `0` = no budget; negative, non-integer or over-u64 values refuse with `[INVALID_CONF_VALUE.REQUIREMENT]` exactly as `repark.cache.max_bytes` does. An overrun refuses with a named error whose message carries the budget, the retained bytes at admission, the bytes admitted so far for the refused result, and the fix (`unpersist()` a cached/eager frame or `spark.catalog.clearCache()`, or raise the conf). Never evicts. |
| D-2 | Retained bytes = the sum of **distinct** Arrow buffers across the session's live `__repark_cache_*` MemTable registrations (dedupe by buffer data pointer, so a buffer shared by two views or by sliced arrays counts once). Computed natively on demand (no Python-side bookkeeping), exposed read-only as `spark.conf.get("repark.cache.retained_bytes")` (a decimal string); setting or unsetting that key refuses. Checkpoint (`__repark_ckpt_`) and user temp views are not counted. |
| D-3 | Admission is incremental: the collection streams, and each batch's distinct-buffer bytes are added to a running total checked against `budget - retained` before the next batch is pulled, so a refusal happens before the full collection peak. On refusal the stream is dropped and nothing is registered. |
| D-4 | Per-result `repark.cache.max_bytes` keeps its meaning (the same message family), now also checked incrementally on the same running total. |
| D-5 | No spill. A spill-backed cache is its own decision. |
| D-6 | A collection failure or a refusal leaves no registration and no Python handle (pinned). |

## Steps

| step | worker | what |
|---|---|---|
| 0 | Devin | Measure: the EAGER-OWN-1 harness (`python/repark-parity/tests/eager_own/eager_own_worker.py`) at 30 iterations under `systemd-run --scope -p MemoryMax=2G/4G/8G -p MemorySwapMax=0`, on the base of EAGER-OWN-1 (`8936346a`) and on `main`, release natives: per-iteration wall, RSS, major faults, and the cgroup outcome (OOM kill or not). Also compare the MemTable's `get_array_memory_size` sum with a distinct-buffer sum for one retained result. No product code. |
| 1 | Devin | D-2: native distinct-buffer accounting over live cache views, the binding, the conf readback; pins. |
| 2 | Devin | D-1 + D-3 + D-4 + D-6: incremental admission in `temp_views.rs`; the conf; pins for the refusal message, refusal-before-peak (VmHWM under the budget in a capped subprocess), no registration after a refusal or a collection failure, and `max_bytes` unchanged. |

**Home:** `crates/repark-core/src/session/temp_views.rs` (and a sibling module when the size gate needs it), its
`crates/repark-python/src/session.rs` binding, `python/repark/src/repark/spark/dataframe/{eager,cache_handle,core}.py`,
`spark/catalog.py`, `spark/session/session_configuration.py` (the read-only conf key only), the harness under
`python/repark-parity/tests/eager_own/`, pins under `python/repark/tests/`, `docs/perf/eager-budget-1-2026-09-13/`.
**Gates:** the eager_own pins, `make verify`, `make preflight`, the parity suite; S2-21 Rust and Python reviewers plus a
Grok critic-logic round before the PR.

## Pointers

- Up: [map.md](map.md) · Ledger: [../../ledgers/staging/eager-budget-1-ledger.md](../../ledgers/staging/eager-budget-1-ledger.md)

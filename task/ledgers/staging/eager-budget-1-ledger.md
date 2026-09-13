# Unit ledger — EAGER-BUDGET-1 · a session cache budget and retained-bytes accounting — steps 0–1

**Date:** 2026-09-13 · **Branch:** `fix/eager-budget-1` · **Base:** `acc9b550` (`main`,
run 11 report merge; EAGER-OWN-1 `#565` delivered at `abed32c9`)
**Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card
[EAGER-BUDGET-1](../../roadmap/mid-term/eager-budget-1-card-2026-09-13.md) (run 11 §7;
Q-E2 ruled REFUSE, never evict): `repark.cache.max_bytes` is checked per result and only
after the full collection
(`crates/repark-core/src/session/temp_views.rs::register_collected_memtable`); there is no
session total and no way to read retained bytes. Step 0 is measurement only — the
EAGER-OWN-1 harness at 30 iterations under `systemd-run --user --scope -p MemoryMax=
2G/4G/8G -p MemorySwapMax=0` on the EAGER-OWN-1 base `8936346a` and on `main`, release
natives: per-iteration wall / VmRSS / VmHWM / `ru_majflt`, the cgroup outcome, and a
Python-side `Table.nbytes` vs distinct-`buffer.address` probe for one retained result.
No product code changes in this step.

**Not in this step:** every D-1…D-6 behaviour (steps 1 and 2), `STATUS.md`,
`briefs/next-sequence.md`, `.github/`, `Cargo.toml`, `Cargo.lock`, `pyproject.toml`,
`uv.lock`.

## PROPOSITION LEDGER — EAGER-BUDGET-1 steps 0–1 — 2026-09-13

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The step-0 measurement is committed: the EAGER-OWN-1 worker at `--rows 1000000 --iterations 30` under `systemd-run --user --scope -p MemoryMax={2G,4G,8G} -p MemorySwapMax=0`, on base `8936346a` (bare `eager()`, leaked results are the retention) and on `main` (bare `eager()` released; results appended to a list = retained), release natives with `__debug_assertions__` False on both trees; per-iteration wall / VmRSS / VmHWM / `ru_majflt`, the cgroup outcome per cell, and the `Table.nbytes` vs distinct-`buffer.address` probe for one retained result. | Committed JSON per cell under `docs/perf/eager-budget-1-2026-09-13/`, the per-cap iteration-band tables and the probe numbers in Evidence, the machine header and both shas. | PROVEN | Nine cells run 2026-09-13 (box idle before each, `OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8`): base-bare and main-retained OOM-killed at iter 5 under 2G and ~16 under 4G (journal `oom-kill`, rc 137), both survive 30 at 8G (~6.7 GB VmHWM); main-bare survives all caps (VmHWM ~1.37 GB). Probe: `nbytes` 199,008,178 vs distinct-buffer 188,196,432 (385 buffers, ratio 0.9457), identical both trees. No per-call slowdown as a function of live views; `ru_majflt` 0 everywhere. Full tables in Evidence; files in [docs/perf/eager-budget-1-2026-09-13/](../../../docs/perf/eager-budget-1-2026-09-13/map.md). |
| C-002 | D-2 distinct-buffer accounting: retained bytes = the sum of distinct Arrow buffers across the session's live `__repark_cache_*` MemTable registrations, deduped by buffer data pointer — a buffer shared by two views or by sliced arrays counts once; zero with no live view; the count drops back when a view is released; `__repark_ckpt_*` and user temp views are not counted. | Step-1 pins: shared buffers across two views count once; zero with no view; drops on release. | PROVEN | `ReparkSession::retained_cache_bytes` + `distinct_buffer_bytes` in `crates/repark-core/src/session/cache_budget.rs`; dedupe key is `Buffer::data_ptr()` (allocation base) — `as_ptr()` double-counts an arrow-58 slice because `PrimitiveArray::slice` pushes the offset into the `Buffer` (measured: 16→32 before the fix). Rust: 6/6 `cargo test -p repark-core cache_budget`. Python pin red→green in `test_eager_budget_1.py`; measured 160 vs 34 logical bytes (2-row) and 16,000 vs 4,890 over 600 buffers (200-row). Evidence below. |
| C-003 | The read-only `repark.cache.retained_bytes` conf readback: `spark.conf.get` returns the native sum as a decimal string; `spark.conf.set` and `unset` on that key refuse. | Step-1 pins: get returns a decimal string; set/unset refuse. | PROVEN | `get` recomputes natively per call (decimal string), `getAll` carries the key, `isModifiable` is `False`, `set`/`unset` refuse with `IllegalArgumentException` `[INVALID_CONF_VALUE.REQUIREMENT]` "read-only", stopped session raises `RuntimeError` via `_ensure_alive`. Binding is a free `_native.retained_cache_bytes` pyfunction (session.rs at its exact CAP-1 baseline; one `#[pymethods]` block per type — the `catalog_census` out). Pin red→green below. |
| C-004 | D-1 conf parse: `repark.cache.max_total_bytes` unset or `0` = no budget; negative, non-integer or over-u64 values refuse with `[INVALID_CONF_VALUE.REQUIREMENT]` exactly as `repark.cache.max_bytes` does; settable at runtime (`spark.conf.set`) and at build (`.config`), read at materialize time. | Step-2 pins per value class, mirroring the `max_bytes` parse pin. | OPEN | Step 2. |
| C-005 | The D-1 refusal message carries the budget, the retained bytes at admission, the bytes admitted so far for the refused result, and the fix (`unpersist()` a cached/eager frame or `spark.catalog.clearCache()`, or raise the conf); the error is a named error, and nothing is evicted. | Step-2 pin on the message fields and the error class. | OPEN | Step 2. |
| C-006 | D-3 refusal before the full collection peak: admission is incremental (each batch's distinct-buffer bytes join a running total checked against `budget - retained` before the next pull), so a capped subprocess refuses with VmHWM under the uncapped peak. | Step-2 capped-subprocess VmHWM pin. | OPEN | Step 2. |
| C-007 | D-6: a refusal or a collection failure leaves no `__repark_cache_*` registration and no Python handle, on both the `cache()` + action path and `eager()`. | Step-2 pins on both entry paths. | OPEN | Step 2. |
| C-008 | D-4: `repark.cache.max_bytes` keeps its per-result meaning and message family, now also checked incrementally on the same running total. | Step-2 pins: existing `max_bytes` pins green plus the incremental check. | OPEN | Step 2. |
| C-009 | D-5: no spill — a refused or admitted materialization writes no disk file. | Step-2 pin: no file appears under any spill/tmp dir during the loop. | OPEN | Step 2. |
| C-010 | Existing cache pins stay green: `python/repark/tests/test_cache_persist.py`, `python/repark/tests/test_eager_own_1.py`, and the `python/repark-parity/tests/eager_own/` harness pins all pass on the final tree. | The named suites green in Gates. | OPEN | Steps 1–2. |

## Evidence

### C-001 measurement

Cells run one at a time, box idle first
(`while pgrep -x cargo >/dev/null || pgrep -x rustc >/dev/null || pgrep -x maturin
>/dev/null; do sleep 20; done` before every cell),
`OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8`.

Machine: `nproc` 64, `free -g` total 125 GiB, Python 3.12.3. Shas: base `8936346a`
(the sibling base tree's own release native), main `acc9b550`. Both natives
`__debug_assertions__` False.

Outcome matrix (30-iteration target; `regs` = `__repark_cache_*` count at end):

| cap | cell | outcome | last iter | VmHWM | regs |
|---|---|---|---|---|---|
| 2G | base-bare | OOM kill (rc 137) | 5 | 2,025 MB | 6 |
| 2G | main-bare | completed | 29 | 1,384 MB | 0 |
| 2G | main-retained | OOM kill (rc 137) | 5 | 2,051 MB | 6 |
| 4G | base-bare | OOM kill (rc 137) | 16 | 4,139 MB | 17 |
| 4G | main-bare | completed | 29 | 1,361 MB | 0 |
| 4G | main-retained | OOM kill (rc 137) | 15 | 4,054 MB | 16 |
| 8G | base-bare | completed | 29 | 6,695 MB | 30 leaked, 0 after clearCache |
| 8G | main-bare | completed | 29 | 1,369 MB | 0 |
| 8G | main-retained | completed | 29 | 6,767 MB | 30 held, 0 after gc |

Iteration bands (median wall s per third; killed cells read from `.partial`):

| cap | cell | first | mid | last | max | majflt Σ |
|---|---|---|---|---|---|---|
| 2G | base-bare | 0.810 | 0.785 | 1.758 | 2.394 | 0 |
| 2G | main-bare | 2.025 | 2.344 | 2.362 | 3.249 | 0 |
| 2G | main-retained | 2.713 | 2.429 | 2.781 | 2.813 | 0 |
| 4G | base-bare | 2.620 | 2.675 | 2.605 | 3.064 | 0 |
| 4G | main-bare | 2.631 | 2.734 | 2.600 | 3.015 | 0 |
| 4G | main-retained | 2.718 | 2.288 | 2.154 | 2.997 | 0 |
| 8G | base-bare | 2.289 | 2.452 | 2.263 | 4.508 | 0 |
| 8G | main-bare | 2.307 | 2.232 | 2.762 | 4.069 | 0 |
| 8G | main-retained | 2.406 | 2.219 | 2.916 | 4.026 | 0 |

Retained-buffer probe (one retained 1e6 × 25 result, identical on both trees):
`Table.nbytes` 199,008,178; distinct-`buffer.address` sum 188,196,432 over 385
buffers; ratio 0.9457 — `nbytes`/`get_array_memory_size` double-counts ~5.4 %
because derived columns share buffers inside one result, so D-2's distinct dedupe
is the honest session total.

Slowdown verdict: the reported per-call slowdown does **not** reproduce as a
function of live cache views under any cap on either tree. Completed cells are
flat within noise (the ~1.2× last-third rise appears identically on the
no-retention 8G bare cell — box noise). The one real slowdown is 2G base-bare's
final two iterations before the kill (0.80 → 2.39 s) — cgroup reclaim stall at
the cap edge, not a view-count effect: `main-retained` dies at the same iteration
with wall flat. `ru_majflt` is 0 in every cell (swap off; deaths are
anonymous-memory OOM kills, not thrashing). Absolute per-iteration level varies
0.8–2.9 s across cells with box load; only within-cell shape is comparable.

### C-002 / C-003 pins — step 1 (2026-09-13)

Red first, on this branch before any product code
(`python/repark/tests/test_eager_budget_1.py`, debug native built by `make
develop`):

```
.venv/bin/python -m pytest python/repark/tests/test_eager_budget_1.py -q
FAILED test_retained_bytes_track_cache_lifecycle
FAILED test_eager_on_eager_reuse_leaves_retained_unchanged
FAILED test_retained_returns_to_prior_value_when_last_holder_dies
FAILED test_clear_cache_returns_retained_to_zero
FAILED test_retained_bytes_conf_is_read_only
5 failed in 0.17s
```

All five red identically: `Exception: Configuration property
repark.cache.retained_bytes is not set.` (`builder_conf.py:267`).

Green after implementation:

```
cargo test -p repark-core cache_budget
test session::tests::cache_budget::distinct_buffer_bytes_ignores_pointers_already_in_the_set ... ok
test session::tests::cache_budget::no_cache_view_returns_zero ... ok
test session::tests::cache_budget::dropped_cache_view_no_longer_counts ... ok
test session::tests::cache_budget::non_cache_temp_views_are_ignored ... ok
test session::tests::cache_budget::two_cache_views_sharing_one_buffer_count_it_once ... ok
test session::tests::cache_budget::sliced_array_counts_its_parent_buffer_once ... ok
test result: ok. 6 passed; 0 failed
.venv/bin/python -m pytest python/repark/tests/test_eager_budget_1.py -q
5 passed, 1 warning in 0.33s
```

Dedupe-key measurement (the one divergence from the card's literal text): the
card names `Buffer::as_ptr()`; under arrow-58 `Array::slice` pushes the offset
into the `Buffer` itself (`PrimitiveArray::slice` → `Buffer::slice` → advanced
`ptr`), so `as_ptr()` counted a slice's parent twice — measured 16 → 32 retained
when registering a sliced batch beside its parent. `Buffer::data_ptr()` is the
allocation base without offset and satisfies the card's actual semantic ("a
buffer shared … or by sliced arrays counts once"); the `data_ptr()`-as-key
slice pin is green.

Capacity-vs-logical tolerance (written into the C-002 pin per the brief):
`retained` counts `Buffer::capacity()` (the allocation the session retains),
while a pyarrow distinct-`buffer.address` sum counts logical `buffer.size`.
Measured on this step's debug native: two-row fixture retained 160 vs logical
34 across 3 buffers; 200-row fixture retained 16,000 vs logical 4,890 across
600 buffers — small collected buffers carry the ~64-byte MutableBuffer floor,
so the pin asserts `retained >= logical` and `retained <= 2 * logical +
64 * buffer_count` rather than byte equality. The step-0 probe's 0.9457 ratio
was `nbytes`-vs-logical inside one table, a different comparison; no
Rust-vs-pyarrow identity existed to inherit 0-tolerance from.

Stopped session: `conf.get("repark.cache.retained_bytes")` after `stop()`
raises `RuntimeError`, the same `_ensure_alive` door every other conf read uses.

Orchestrator audit of the step-1 diff (2026-09-13, accepted; recorded here per
the audit):

- **R12b-D-1:** the readback key is `repark.cache.retained_bytes`, computed
  read-only — the orchestrator's choice under D-2.
- **R12b-D-3:** the conf intercepts live in `builder_conf.RuntimeConfig` plus
  `session_configuration.py`, which is the real conf seam; accepted as Home.
- **C-002 note:** the figure is buffer `capacity()` keyed by `data_ptr()` — the
  allocation, not the logical length — the honest number for a memory budget.
  The small-frame ratio is allocation overhead, measured retained 16,000 vs
  logical 4,890 at 200 rows (and 160 vs 34 on the two-row fixture); it
  approaches 1 on large results — the step-0 probe measured a 0.9457
  logical-sum ratio on a 1e6-row result.
- **Out of scope, recorded:** the intermittent debug-native crash while
  planning a 2000-branch UNION (`spark.sql` before any step-1 code runs); does
  not reproduce on release natives.

## Gates run

- `.venv/bin/python -m pytest python/repark-parity/tests/eager_own -q` — 1 passed,
  1 skipped (the bench pin stays opt-in).
- `make check-docs-links` — clean (827 files, 5,263 links).
- `make check-ledger-grammar` — clean (126 live ledgers).
- `make verify` — clean (clippy, panic-ban, crate-dag, lib-rs, rust/py file-size,
  python-conventions, docstring-presence, manifest, ledger lifecycle + grammar,
  docs-compaction, docs-links, owner-ruling, parity-live dual-wire, rust tests).

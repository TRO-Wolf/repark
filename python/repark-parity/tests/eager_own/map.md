# map — python/repark-parity/tests/eager_own

## Purpose

EAGER-OWN-1 step 0 (2026-09-13): the measured reproduction of the reported
bare-`eager()` retention defect, as a subprocess-isolated pin in the style of
`../spill/`. A bare `temp_df.eager()` (result not assigned) evaluates the lazy
plan, registers a `__repark_cache_*` MemTable, and returns a child that is
dropped at once; nothing owns the registration (the frame registry is a
`WeakSet`), so each repeat adds a full result set to resident memory and only
`catalog.clearCache()` sweeps the orphans by prefix. The owner measured the
slowdown on a 1,000,000-row, 25-column TA `withColumns` chain; this harness is
the deterministic fixture for the before/after pair the card demands (D-7).

The worker builds one ordered series (`ts`, `open`, `high`, `low`, `close`,
`volume`; numpy `default_rng(20260913)`, `ts` epoch-seconds from
2026-01-01 at 60 s steps via a polars frame through the `createDataFrame`
Arrow path), then the card's 19-column `withColumns` chain — three `ema`, three
`sma`, two `rsi`, `adx`, `linearreg` + `linearreg_slope`, `trange`, `atr`,
`mom` (one fused `ta.over_columns` over `Window.orderBy("ts")`), then a second
`withColumns` carrying `ema` over the derived `tr`, a close/open ratio, a
`round`, a `coalesce` null replacement, and a `when`/`otherwise` string
condition. Ten bare `eager()` calls follow (results not assigned). Per
iteration the worker records wall time, `VmRSS` after the call and after
`gc.collect()`, `VmHWM`, `ru_majflt` before/after the call
(`resource.getrusage(RUSAGE_SELF)`), and the `__repark_cache_*` name count in
`catalog.listTables()`; it also records the count post-loop, post-gc, and
post-`clearCache()`, cross-checked against `session.list_temp_view_names()`.
Each iteration record is flushed as one JSON line to `<json-out>.partial`
(fsync) so a cgroup-killed cell still yields the iterations it reached.
The JSON goes to the `--json-out` argv path.

EAGER-BUDGET-1 step 0 (2026-09-13) added two argv flags; existing argv and
JSON shape are unchanged:

- `--retain` appends each `eager()` result to a list held through the loop —
  the honest retained-pressure case on `main` (on base `8936346a` bare
  `eager()` already leaks, so base cells run without the flag).
- `--retained-probe` materializes one retained eager result before the loop,
  calls `.toArrow()` on it, and records `table_nbytes` beside the sum of
  `buffer.size` over DISTINCT `buffer.address` across every column chunk's
  `buffers()` (the flat 25-column fixture has no nested arrays), plus the
  ratio — the Python-side rehearsal of D-2's distinct-buffer accounting (the
  Rust-side MemTable figure is step 1's).

The bench pin (`test_bare_eager_registrations_released_million_rows`) is
opt-in through `REPARK_EAGER_OWN_BENCH=1` so CI never runs the million-row
loop; `REPARK_EAGER_OWN_BENCH_JSON` redirects the JSON output path (used to
write `docs/perf/eager-own-1-2026-09-13/base.json` and `after.json`). The small
pin (2,000 rows, 3 iterations) always runs so the default suite exercises the
worker. Step 0 asserted `registrations == iterations` — the defect pinned;
step 1 (the shared-ownership handle, D-2) flipped both pins to the
`_assert_owned_shape` contract: `registrations_after_call == 0` per iteration
(the returned child is freed at statement end and the finalizer drops the view
synchronously), and post-loop / post-gc / post-clearCache all zero.

Measured facts this harness is built on (base `8936346a`, release native,
`__debug_assertions__` False):

- `_eager_materialize` (`python/repark/src/repark/spark/dataframe/eager.py`)
  has no already-eager branch: every bare call makes a fresh `_identity_child`,
  sets `_persist_requested`, materializes under a fresh
  `_CACHE_VIEW_PREFIX` name, then counts for `_eager_shape`.
- `catalog.listTables()` is NOT blind to cache views: `_is_hidden_list_tables_name`
  hides `__repark_cdf_` / `__repark_mia_` / `__repark_tt_` / `$` names only, so
  the temp-view rows carry `__repark_cache_*` names the pin can count — the
  same names `clearCache()` sweeps.
- The returned child is unreferenced, so CPython frees it at statement end by
  refcount; `registrations_after_call` already shows the orphan — `gc.collect()`
  is recorded anyway because step 1's `weakref.finalize` drop is the flip the
  pin measures.
- One iteration retains roughly one full 25-column 1e6-row result (~200 MB of
  Float64 plus the string column); ten iterations ≈ 2 GB resident growth.

## Contents

- `conftest.py` — the `pytest.importorskip("repark")` guard, same shape as
  `../spill/conftest.py`.
- `eager_own_worker.py` — the subprocess entry (`sys.executable` path, argv
  `--rows` / `--iterations` / `--json-out`): fixture build, TA chain, the
  bare-eager loop, `/proc/self/status` reads, JSON payload with the
  per-iteration table and the post-loop / post-gc / post-clearCache snapshots.
- `test_eager_own.py` — the pins: `_run_worker` subprocess wrapper, the
  `_assert_owned_shape` post-fix assertion block (all-zero registrations), the
  always-on small pin, and the `REPARK_EAGER_OWN_BENCH=1`-gated million-row pin.
  Step 2 trimmed the module docstring to one line — the `pins:` citation lives here.
- `map.md` — this file.

pins: eager-own-1/C-001, C-012
pins: eager-budget-1/C-001

## Pointers

- Up: [../map.md](../map.md)
- Sibling harness style: [../spill/map.md](../spill/map.md)
- Ledger: [../../../../task/ledgers/staging/eager-own-1-ledger.md](../../../../task/ledgers/completed/eager-own-1-ledger.md)
- Committed measurement: [../../../../docs/perf/eager-own-1-2026-09-13/map.md](../../../../docs/perf/eager-own-1-2026-09-13/map.md)

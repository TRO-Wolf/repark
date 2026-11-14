# task/wo/nan-minmax-1/evidence/delta/ — the delta round

NAN-MINMAX-1 part 1's one delta, 2026-11-14, model `claude-opus-5-5`, after both verifiers failed #1333 on its
records (class (b); no class (a) S1/S2). Builds, all release, the Spark door: main `631d283c` (module `3f603240…`),
#1333's head `c9c3b8d4` (`6eafceab…`) and the delta's fix commit `1a8f2749` (`516c9cba…`). Every run printed the
module it loaded.

**F1 red, then green** (verifier 1: a float row group with no footer min or max left the file's bounds exact, and the
file pruner dropped the file).

- **Rust** (`crates/repark-core/src/spark_nullable/float_bounds_tests.rs`), on `c9c3b8d4`'s product:
  `a_row_group_without_float_bounds_turns_the_file_bounds_inexact` failed (`Exact(Float64(1))` where `Inexact` was
  asserted) and `read_parquet_keeps_a_nan_row_group_under_a_range_and_a_nan_filter` failed (`d > 5.0` counted 0,
  not 2). Its control, `an_all_null_row_group_keeps_the_file_bounds_exact`, passed. On the fix: 7 of 7 pass.
- **Facade** (`test_a_row_group_without_footer_bounds_keeps_its_nan_rows_on_both_doors`, `R_one`, `R_many`,
  `R_nullnan`): 3 failed on `c9c3b8d4` and on main; for example `R_nullnan` answered `{'gt5': [], 'eqnan': [[0]]}`
  where Spark answers `{'gt5': [[2]], 'eqnan': [[1]]}`. On the fix the pin file gives 43 passed, 5 xfailed.
- **The two new strict xfails** each failed in 10 of 10 runs on the fix under `--runxfail`: `max(d)` answered
  1008.199999 in every one of the test's five runs, and `d >= 1.0` answered ids 2 and 4. On main the `d >= 1.0`
  xfail passes (main read the NaN as `1.0`), which is item (ii).

**Pruning against main** (`prune_probe.py <data roots> <out>`). Verifier 1's five 100-file sets (`P_*`: 20 rows a
file in row groups of 10) and its three files with an unbounded row group (`R_*`, the facade cells' shapes), each
under its selective filter, with the `EXPLAIN ANALYZE` pruning metrics:

| set | filter | file ranges, main / head | row groups, main / head | count, main / head | Spark |
|---|---|---|---|---|---|
| `P_spread` | `d >= 1990.0` | 100 → 1 / 100 → 1 | 2 → 1 / 2 → 1 | 10 / 10 | |
| `P_const` | `d >= 99.0` | 100 → 99 / 100 → 99 | 198 → 2 / 198 → 2 | 20 / 20 | |
| `P_mix` | `d >= 1975.0` | 100 → 51 / 100 → 51 | 102 → 3 / 102 → 3 | 25 / 25 | |
| `P_const_nan` | `d > 98.5` | 100 → 99 / 100 → 99 | 198 → 2 / 198 → 2 | 20 / 20 | 20 |
| `P_spread_nan` | `d > 1985.5` | 100 → 1 / 100 → 1 | 2 → 2 / 2 → 2 | 15 / 15 | 15 |
| `R_one` | `d > 5.0` | 1 → 0 / **1 → 1** | 0 → 0 / **2 → 1** | 0 / **2** | 2 |
| `R_many` | `d > 5.0` | 2 → 1 / **2 → 2** | 2 → 1 / **4 → 2** | 2 / **4** | 4 |
| `R_nullnan` | `d > 5.0` | 1 → 0 / **1 → 1** | 0 → 0 / **2 → 1** | 0 / **1** | 1 |

- Pruning moves only on the three `R_*` sets, which hit F1: the file holding the unbounded row group is opened, and
  row-group pruning keeps that row group, as Spark's reader does. `= NaN` there answers 2, 2 and 1, Spark's answers.
- The only other answer that moves is `isnan(d)` over `P_const_nan`'s constant files: 1 on main, 100 on the head,
  part 1's own fix (verifier 2 measured Spark's 100). The Spark column is verifier 1's measurement.

- `prune_probe.py` — the probe (type annotations added after the run; the logic is the one run).
- `prune-base.json`, `prune-head3.json` — its output on main and on the fix, with the module's sha256.

Up: [../map.md](../map.md).

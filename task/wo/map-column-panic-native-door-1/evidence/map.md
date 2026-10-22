# task/wo/map-column-panic-native-door-1/evidence/ — measurements of MAP-COLUMN-PANIC-NATIVE-DOOR-1

Measured 2026-10-21 on main `b7a20167` (the baseline) and the head of `fix/map-column-panic-native-door-1`; the wheels are
`maturin develop --profile ci`, every run under `taskset -c 0-25` with `CARGO_BUILD_JOBS=12`, every Python probe under a 40 GB
`systemd-run` scope. The box was shared with another builder (load 5 to 25 while these ran).

- [red-on-main-rust.txt](red-on-main-rust.txt) — the four `native_session_` tests of `repark-python` with the pins added and the install absent:
  2 failed (`SELECT map('a', id) AS m FROM range(1)` panics, reported as a failure by `catch_unwind`; the three-row two-pair map raises
  `map requires key and value lists to have the same length`), 2 passed (the constant-map control and `native_session_is_not_spark_doored`).
- [green-head-rust.txt](green-head-rust.txt) — the same command on the head: 4 passed.
- [red-on-main-facade.txt](red-on-main-facade.txt) — `test_map_column_panic_native_door_1.py` against the main wheel with
  `REPARK_PANIC_FATAL=0` (the armed suite aborts at the first panic): 17 failed, 5 passed (`column_key`, `int_key`, `null_value`,
  `map_keys` and the literal control already answered). On the head all 22 pass.
- [oracle-table.md](oracle-table.md) — the oracle replay on the native door: 47 statements, main against head against the Spark door, with the
  reason for each statement that differs from Spark; the raw answers are [replay-main-native.json](replay-main-native.json),
  [replay-head-native.json](replay-head-native.json), [replay-main-spark.json](replay-main-spark.json) and
  [replay-head-spark.json](replay-head-spark.json).
- [controls-head-native.json](controls-head-native.json), [controls-head-spark.json](controls-head-spark.json) — the ten map-free
  statements that reproduce each native-door difference (string cast spelling, `date_format`, a naive instant, a double where Spark has a
  decimal, the Spark-only functions, map equality, `map()`).
- [cost.json](cost.json) — the native door's `SELECT 1` and a 1,000-row `map(id, id + 1)` select, main wheel against head wheel,
  alternating processes: three rounds of 10 runs and two of 30; `SELECT 1` costs 1.023x to 1.042x (median 1.035x, under the 1.05x cap),
  the map select 0.76x to 0.89x.
- [probe-children.txt](probe-children.txt) — the four children of the repointed PANIC-FATAL pin run by hand: `PanicException` and exit 1
  through the method fence, SIGABRT through the Arrow C stream fence, and the unchanged `repark internal error` text with the flag removed.
- [bisect-memory-pin.txt](bisect-memory-pin.txt) — the CI runner's measure of `test_replace_dict_depth40_memory_linear` on a throwaway draft branch, six builds: commit 1 alone and commit 1 plus one unrelated registered function pass (replace 7.52 MB and 7.57 MB against bounds near 9.6 MB); every build with the probe fails (9.03 MB against 8.39 MB with the probe inside `fence.rs`, 9.99 MB against 8.55 MB with it in its own module `panic_probe.rs`). Moving the probe did not bring the pin back under its bound. Main's own value is not printed by any passing run.
- [ansi-red-on-main.txt](ansi-red-on-main.txt), [ansi-green-head.txt](ansi-green-head.txt) — the three pins of `crates/repark-sql/tests/ansi_door_map_column.rs` on the ANSI door: 2 failed (the one-row `list array` panic, the three-row same-length error) and 1 passed on main's code, 3 passed with the rule pushed in `AnsiDialect`.
- [ansi-oracle-table.md](ansi-oracle-table.md) — the same 47 statements on the Rust-embedded ANSI door, main against head, beside the Python native door and the Spark door; the raw answers are [replay-main-ansi.json](replay-main-ansi.json) and [replay-head-ansi.json](replay-head-ansi.json). The harness (a throwaway Rust test writing Arrow IPC files that were read back with the Python replay's normaliser) is not committed.
- [facade-counts.txt](facade-counts.txt) — the full facade suite on the head, armed: 20312 passed, 485 skipped, 151 xfailed, 0 failed.

The Rust gates on the head: `make verify` ran `make ci` and then `cargo nextest run --locked --workspace` (7772 tests run, 7772 passed, 4 slow,
97 skipped, 191.8 s); `make rust-panic-ban` is green; `cargo test -p repark-core` passes (1497, 37 and 8 tests, 1 ignored);
`cargo test -p repark-python --lib` passes 172 tests, 3 of them the new native-door pins and 3 the new probe tests.

The `conftest.py` mutation of the repointed pin is stored with its own unit: [../../panic-fatal-suite-1/evidence/mutation-flag-zero-red-native-door-1.txt](../../panic-fatal-suite-1/evidence/mutation-flag-zero-red-native-door-1.txt).

Up: [../map.md](../map.md).

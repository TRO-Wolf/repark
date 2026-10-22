# task/wo/map-column-panic-native-door-1/ — MAP-COLUMN-PANIC-NATIVE-DOOR-1

MAP-COLUMN-PANIC-NATIVE-DOOR-1 (2026-10-21, orchestrator ruling under the owner's overnight grant): the native `repark.sql()` door
answers `map(<key>, <column>)` instead of panicking `list array` (one source row) or raising `map requires key and value lists to
have the same length` (several rows). PR #1131 fixed the Spark doors with the analyzer rule `SparkMapRewrite` and left the native
door open as the card's first follow-up; the ruling was that the same rule goes on the native door, because the rule is a panic fix and no
door may panic. The unit built on branch `fix/map-column-panic-native-door-1` from main `b7a20167` (builder Claude Sonnet 5.5,
orchestrator review). It retires when the pull request merges; the card's status paragraph and the v1.5.6 release-notes draft record it
in the same change.

The work ran in six commits. First, the rule: `NativeDoorTypes::configure_analyzer_rules` in `repark-python` (the extension behind
`PyReparkSession::native`, which is `repark.sql()`) pushes `SparkMapRewrite` after the unit-widen call, behind `type_coercion`, as on the
Spark door. The order named `repark-core`'s `dialect.rs` and `df_guards.rs` as the place; they are the generic seams, and neither can
name the rule because `repark-core` sits below `repark-functions` in the crate DAG, so the extension is the one place and the Rust pins
moved with it, beside `native_session_is_not_spark_doored`. The rule needs no `LambdaRebind`: a `map` call holds no lambda and the
native door registers no higher-order function. Two of the four Rust pins are red on main (the one-row panic, wrapped in
`catch_unwind` so it reports as a failure and not an abort; the three-row same-length error) and green on the head; a facade pin file
runs 16 of the Spark recording's cells on `repark.sql()` through the Arrow path, 17 of its 22 tests red on the main wheel. Second, the
PANIC-FATAL pin: it had used this door's panic as its probe, so `_native.__panic_probe__('method')` (underscore-named test surface in its own
module `panic_probe.rs`, registered ungated like `__panic_fatal__`) raises a Rust panic on purpose inside a method fence. Armed, the
child ends in `PanicException` (exit 1); unarmed, it keeps the `repark internal error` text. The `conftest.py` mutation (flag `0`) is red on
two of the pin's three tests. Third, these records. Fourth, the ANSI door. Fifth, the memory-pin bisect in the evidence. Sixth, the probe's stream leg removed; the method leg alone suffices for the pin, and the Arrow C stream fence stays pinned in Rust by the child-process tests in `fence.rs` that call `fence_stream_poll` under `Probe.op` with the flag on and off, which gives up only the abort across the real `extern "C"` stream callback. The memory pin is open and awaits the owner's ruling: the runner's `test_replace_dict_depth40_memory_linear` value varies between builds of identical sources (the sixth and seventh heads differ only in a test docstring; one passed with 24 checks and the other read 10,006,528 B against the 8,388,608 B bound), a re-run on the same wheel repeats that wheel's number, it does not reproduce on the builder's box, and main's runner value is not printed by any passing run.

What the measurements say: on the native door the 47 statements of the oracle replay answer without a panic on the head (5 panics and 14
same-length errors on main); the answers that main already gave are unchanged; 28 statements equal the Spark door and the 19 that do not
differ for native-door reasons, each reproduced by a map-free control; the Spark door's answers are identical on main and the head. A
repeated or NULL key now raises Spark's `[DUPLICATED_MAP_KEY]` / `[NULL_MAP_KEY]` text on the native door too. The cost of the added rule is
1.02x to 1.04x on the native `SELECT 1` (0.43 ms to 0.44 ms) and 0.76x to 0.89x on a 1,000-row `map(id, id + 1)` select. The ZONE-ORACLE-1
corpus is not re-recorded here: its 20 REFUSED and 10 ERROR counts are on the Spark door, which this change does not touch. The numbers, the
logs and the raw answers are in [evidence/](evidence/map.md). The ANSI door was the one finding of the first three commits and is commit 4,
under the orchestrator's ruling that no door may panic: `repark-sql`'s `AnsiDialect`, the Rust-embedded ANSI door with no Python entry, builds
its analyzer list through the same `analyzer_rules_with_timestamp_unit_widen` and lacked the rule; `AnsiDialect::configure_analyzer_rules` now
pushes it, the same one push. The red-first pins are `crates/repark-sql/tests/ansi_door_map_column.rs` (the one-row `list array` panic and the
three-row same-length error red on main, green on the head; the constant-map control green on both). The same 47-statement replay on that
door: on main 5 panics and 14 same-length errors, on the head none; the 14 answers main gave are identical on the head; 33 answer on the
head, the same 33 as the Python native door, 43 of 47 equal that door's answers and 4 more in rows only (the Python export coerces `Utf8View`
and list nullability); the repeated and NULL key refusals carry Spark's text here too.

- [evidence/](evidence/map.md) — the red-on-main and green-on-head logs, the oracle replay table and its raw answers, the map-free controls,
  the cost medians, the two probe children, the memory-pin bisect and the ANSI door's replay.

Up: [../map.md](../map.md).

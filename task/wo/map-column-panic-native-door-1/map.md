# task/wo/map-column-panic-native-door-1/ — MAP-COLUMN-PANIC-NATIVE-DOOR-1

MAP-COLUMN-PANIC-NATIVE-DOOR-1 (2026-10-21, orchestrator ruling under the owner's overnight grant): the native `repark.sql()` door
answers `map(<key>, <column>)` instead of panicking `list array` (one source row) or raising `map requires key and value lists to
have the same length` (several rows). PR #1131 fixed the Spark doors with the analyzer rule `SparkMapRewrite` and left the native
door open as the card's first follow-up; the ruling was that the same rule goes on the native door, because the rule is a panic fix and no
door may panic. The unit built on branch `fix/map-column-panic-native-door-1` from main `b7a20167` (builder Claude Sonnet 5.5,
orchestrator review). It retires when the pull request merges; the card's status paragraph and the v1.5.6 release-notes draft record it
in the same change.

The work ran in three commits. First, the rule: `NativeDoorTypes::configure_analyzer_rules` in `repark-python` (the extension behind
`PyReparkSession::native`, which is `repark.sql()`) pushes `SparkMapRewrite` after the unit-widen call, behind `type_coercion`, as on the
Spark door. The order named `repark-core`'s `dialect.rs` and `df_guards.rs` as the place; they are the generic seams, and neither can
name the rule because `repark-core` sits below `repark-functions` in the crate DAG, so the extension is the one place and the Rust pins
moved with it, beside `native_session_is_not_spark_doored`. The rule needs no `LambdaRebind`: a `map` call holds no lambda and the
native door registers no higher-order function. Two of the four Rust pins are red on main (the one-row panic, wrapped in
`catch_unwind` so it reports as a failure and not an abort; the three-row same-length error) and green on the head; a facade pin file
runs 16 of the Spark recording's cells on `repark.sql()` through the Arrow path, 17 of its 22 tests red on the main wheel. Second, the
PANIC-FATAL pin: it had used this door's panic as its probe, so `_native.__panic_probe__(shape)` (underscore-named test surface, registered
ungated like `__panic_fatal__`) now raises a Rust panic on purpose inside a method fence and inside the Arrow C stream poll fence, and
the pin keeps both legs: armed, the method child ends in `PanicException` (exit 1) and the stream child aborts (SIGABRT); unarmed, both keep
the `repark internal error` text. The `conftest.py` mutation (flag `0`) is red on three tests. The probe lives in its own module `panic_probe.rs`, with `fence.rs` unchanged: the first placement inside `fence.rs` moved the CI runner's memory pin (`test_replace_dict_depth40_memory_linear`) over its bound, and the bisect that found it is in [evidence/](evidence/map.md). Third, these records.

What the measurements say: on the native door the 47 statements of the oracle replay answer without a panic on the head (5 panics and 14
same-length errors on main); the answers that main already gave are unchanged; 28 statements equal the Spark door and the 19 that do not
differ for native-door reasons, each reproduced by a map-free control; the Spark door's answers are identical on main and the head. A
repeated or NULL key now raises Spark's `[DUPLICATED_MAP_KEY]` / `[NULL_MAP_KEY]` text on the native door too. The cost of the added rule is
1.02x to 1.04x on the native `SELECT 1` (0.43 ms to 0.44 ms) and 0.76x to 0.89x on a 1,000-row `map(id, id + 1)` select. The ZONE-ORACLE-1
corpus is not re-recorded here: its 20 REFUSED and 10 ERROR counts are on the Spark door, which this change does not touch. The numbers, the
logs and the raw answers are in [evidence/](evidence/map.md). One finding is left open and untouched: `repark-sql`'s `AnsiDialect`, the Rust-embedded ANSI
door with no Python entry, builds its analyzer list through the same `analyzer_rules_with_timestamp_unit_widen` and lacks the rule; a one-off
probe test (not committed) showed `SELECT map('a', id) AS m FROM range(1)` panicking `list array` and the three-row two-pair map raising the
same-length error there. It wants the same one push in `AnsiDialect::configure_analyzer_rules`, which this unit's brief did not cover.

- [evidence/](evidence/map.md) — the red-on-main and green-on-head logs, the oracle replay table and its raw answers, the map-free controls,
  the cost medians and the four probe children.

Up: [../map.md](../map.md).

# map — crates/repark-spark/tests/

## Purpose

Integration tests of the assembled Spark door: a real `repark_core::ReparkSession` built with
`SparkExtension` + `SparkDialect`, exercised end-to-end (the deferred-test landing zone for
rows that needed the door installed, per `task/port/deferred-tests.md`).

## Contents

- [session_extension.rs](session_extension.rs) — deferred test #1
  (`temp_view_then_sql_runs_the_spark_function_shim`): temp view + `session.sql` reaches the
  Spark date shim (`year`, `weekofyear`) through the installed extension + dialect.

## I want to...

| ...do this | go to |
|---|---|
| See the door-installed session end-to-end pin | [session_extension.rs](session_extension.rs) |
| See which deferred rows land where | [../../../task/port/deferred-tests.md](../../../task/port/deferred-tests.md) |
| Read the extension under test | [../src/extension.rs](../src/extension.rs) |

## Pointers

- Up: [../map.md](../map.md)
- Unit-level batteries live beside their modules under [../src/](../src/map.md); this
  directory is only for whole-session assemblies.
- `session_extension.rs` is deferred test #1 un-deferred (row closed in
  `task/port/deferred-tests.md`, phase-2 PR-2).

## Debug

- `cargo test -p repark-spark --test session_extension` runs just this file. Never
  `--all-features` (AGENTS.md PyO3 note).
- Week-53 assertion is ISO-week semantics (2021-01-01 → ISO week 53 of 2020) — a failure there
  is the date shim regressing, not the fixture.

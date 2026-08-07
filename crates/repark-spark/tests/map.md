# map — crates/repark-spark/tests/

## Purpose

Integration tests of the assembled Spark door: a real `repark_core::ReparkSession` built with
`SparkExtension` + `SparkDialect`, exercised end-to-end (the deferred-test landing zone for
rows that needed the door installed, per `task/port/deferred-tests.md`).

## Contents

- [session_extension.rs](session_extension.rs) — deferred test #1
  (`temp_view_then_sql_runs_the_spark_function_shim`): temp view + `session.sql` reaches the
  Spark date shim (`year`, `weekofyear`) through the installed extension + dialect.
- [ddl_sessions.rs](ddl_sessions.rs) — deferred rows #2, #4, #5, #6, #7 (phase-2 PR-3a): CTAS
  end-to-end, namespace-`location` on a strict catalog (ADV-1 / N5), the BUG-001 dual-key
  property pin, the `spark.catalog` metadata surface, and the config-driven memory catalog —
  all on memory/local catalogs (AWS-free).

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

- `cargo test -p repark-spark --test session_extension` (or `--test ddl_sessions`) runs one
  file. Never `--all-features` (AGENTS.md PyO3 note).
- `ddl_sessions.rs` failures usually mean a PR-3a handler regressed (ctas / namespace_ddl /
  catalog_ops), not the session seams — reproduce via the equivalent `session.sql` statement.
- Week-53 assertion is ISO-week semantics (2021-01-01 → ISO week 53 of 2020) — a failure there
  is the date shim regressing, not the fixture.

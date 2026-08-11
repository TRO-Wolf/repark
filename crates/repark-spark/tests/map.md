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
- [dml_sessions.rs](dml_sessions.rs) — deferred row #3 (phase-2 PR-3b):
  `session_sql_bare_dml_applies_eagerly` — the F-BR-2 bare-`INSERT` eager-apply trap through
  `session.sql` (memory catalog, AWS-free).
- [session_timezone.rs](session_timezone.rs) — **H-1a split B (2026-08-10):** the
  session-timezone extraction class, pinned on real sessions at two non-UTC zones plus a
  half-hour offset. Eight extractor-family pins (`year`; `month`/`dayofmonth`/`dayofyear`;
  `hour`/`minute`/`second`; the `dayofweek`/`weekday`/ISO-week/`quarter` family; `date_trunc`;
  `date_format`; the DST spring-forward and fall-back boundaries; pre-1970 instants), the
  **native DataFrame API** cell built from `repark_functions::expr_fn` (a standalone `Expr` with
  no session — the shape the Python facade's `F.year(col)` takes, and the cell a
  registration-time zone would miss), and the negatives that make the claim falsifiable: `DATE`
  and `TIME` arguments must not move under ANY zone, the underlying instants must not move at
  all, and the extractor-layer fallback must equal `repark_core::DEFAULT_SESSION_TIME_ZONE`.
  Value AND Arrow type on every row. The `DATE` negative caught a real over-reach during the fix
  (a non-idempotent coercion arm).
  **Reworked 2026-08-10 after an adversarial panel measured three wrong-answer families against
  live Spark 4.1.2**, so the file now also pins: `date_trunc` across the DST **fall-back**
  (`date_trunc_preserves_the_source_offset_across_a_fall_back` — the truncated local time is
  ambiguous and the source offset must be preserved); `date_trunc` of a `DATE`/string **composed**
  into every extractor (`date_trunc_of_a_date_or_string_lands_on_the_session_zone_timeline` — the
  single-hop `DATE` negative provably cannot see this); the two reachable DST **gap** zones
  (`dst_gap_zones_resolve_like_spark` — Lord Howe's 30-minute step, Santiago's midnight
  transition); the date-valued shims this crate's engine owns
  (`date_valued_shims_take_the_date_in_the_session_zone` — `trunc`/`add_months`); and two DECLARED
  divergences pinned as such, `a_zoneless_timestamp_input_is_read_as_utc_and_diverges_from_spark`
  (registry TZ-7) and `timestamp_to_date_paths_outside_this_crate_still_read_the_stored_zone`
  (registry TZ-8). Every expectation in the file is a live-Spark measurement, not a derivation.
  The `DATE` negative's CLAIM was also narrowed to match its coverage: `date_trunc(fmt, DATE)` is a
  session-zone localization in Spark, so it moves and now says so.
- [ta_window.rs](ta_window.rs) — deferred rows #8-#14 (phase-2 PR-4): the seven
  `sql_route_*` cases, ported from v1 `repark-session/tests/ta_window.rs`. Proves the TA window
  UDFs the composed `repark_ta::TaExtension` registers are `f64::to_bits`-identical to the
  `repark_ta` kernels on the crate's own 5000-row OHLC goldens (`../../repark-ta/tests/goldens/*.bin`
  — read, never re-recorded), across single/scalar-param/multi-series/parked-four families,
  `PARTITION BY` scoping, a 12k multi-batch partition, and the non-literal-period refuse.

## I want to...

| ...do this | go to |
|---|---|
| See the door-installed session end-to-end pin | [session_extension.rs](session_extension.rs) |
| See which deferred rows land where | [../../../task/port/deferred-tests.md](../../../task/port/deferred-tests.md) |
| Read the extension under test | [../src/extension.rs](../src/extension.rs) |
| See the TA SQL route pinned bit-exact | [ta_window.rs](ta_window.rs) |
| See the session-timezone class pinned at the Spark door + DataFrame API | [session_timezone.rs](session_timezone.rs) |
| See the same class at the ANSI door | [../../repark-sql/tests/session_timezone_ansi_door.rs](../../repark-sql/tests/session_timezone_ansi_door.rs) |
| See the same class at the facade | [../../../python/repark/tests/test_session_timezone_parity.py](../../../python/repark/tests/test_session_timezone_parity.py) |

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
- `ta_window.rs` "missing fixture …" means the goldens moved: the path is
  `$CARGO_MANIFEST_DIR/../repark-ta/tests/goldens`, i.e. repark-ta's sibling position in the
  workspace. A *bit* mismatch is an engine/UDF regression, never a goldens edit — see
  [../../repark-ta/map.md#debug](../../repark-ta/map.md).
- Week-53 assertion is ISO-week semantics (2021-01-01 → ISO week 53 of 2020) — a failure there
  is the date shim regressing, not the fixture.

- **Neutral-fixture scrub (2026-08-10, hardening-prep):** an owner-approved, forward-only,
  comment-and-fixture-only pass moved this directory's doc text and example literals to
  neutral placeholders — the upstream job the acceptance shape mirrors is named generically
  ("the source publish job"), and example table/view/entity names are placeholders carrying no
  domain vocabulary. Outcome-neutral: every renamed fixture moved together with the assertions
  that read it. Sites here: `ddl_sessions.rs` — two doc comments, and the temp-view fixture in
  `catalog_surface_table_exists_and_temp_views`, now `staging_view`.

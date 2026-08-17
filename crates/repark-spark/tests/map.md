# map — crates/repark-spark/tests/

## Purpose

Integration tests of the assembled Spark door: a real `repark_core::ReparkSession` built with
`SparkExtension` + `SparkDialect`, exercised end-to-end (the deferred-test landing zone for
rows that needed the door installed, per `task/port/deferred-tests.md`).

## Contents

- [session_extension.rs](session_extension.rs) — deferred test #1
  (`temp_view_then_sql_runs_the_spark_function_shim`): temp view + `session.sql` reaches the
  Spark date shim (`year`, `weekofyear`) through the installed extension + dialect.
- [declared_sorted_tighten.rs](declared_sorted_tighten.rs) — **SE-1 PR-D1:** Spark-door
  execution-layer pin that `tightenNulls` elides `SortExec` on the serving-shape window
  (`ORDER BY ts` = NULLS FIRST over nullable keys) via `create_physical_plan` (not EXPLAIN);
  hint mode keeps the sort; Iceberg CTAS of a tightened frame refuses; CTAS of a
  derived expression over a tightened source refuses (SQM F1); INSERT into an
  existing table stays allowed.
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
  divergences pinned as such, `a_zoneless_timestamp_input_localizes_in_the_session_zone`
  (registry TZ-7) and `timestamp_to_date_paths_read_the_session_zone` (TZ-8 CAST/`to_date`
  plus `datediff` riding CAST, FIXED) plus
  `last_day_and_date_add_over_a_timestamp_still_refuse` (TZ-8 named residual) plus
  `native_dataframe_api_cast_to_date_reads_the_session_zone` (`Expr::Cast` cell).
  Every expectation in the file is a live-Spark measurement, not a derivation.
  The `DATE` negative's CLAIM was also narrowed to match its coverage: `date_trunc(fmt, DATE)` is a
  session-zone localization in Spark, so it moves and now says so.
  **TZ-4 PR-1 (2026-08-13):** `date_trunc` return type pins flipped to `timestamp[us, tz=UTC]`.
- [timestamp_cast_seconds.rs](timestamp_cast_seconds.rs) — **TZ-5 (2026-08-12):** the
  `CAST(TIMESTAMP AS <numeric>)` epoch-seconds class at the **Spark door** and the **native
  DataFrame API**, on real sessions, value AND Arrow type. Nine pins: whole instants either side
  of 1970; the **floor edge both signs** (Spark uses `Math.floorDiv`, so `-0.5 s → -1` and
  `-1.25 s → -2` where truncation toward zero says `0` and `-1` — the only inputs that separate
  the real fix from the plausible one); zone-independence across three zones (a cast reads the
  instant, never a wall clock); a real timestamp COLUMN with its null mask; narrower integer
  targets (`INT`/`SMALLINT`, which repark refused outright before the fix); float and decimal
  targets, which keep the fraction; and two fences — the REVERSE direction
  (`CAST(<integer> AS TIMESTAMP)`) still reads seconds and round-trips; **TZ-4 PR-1** flipped
  that reverse CAST's Arrow type to `timestamp[us, tz=UTC]`. `CAST(ts AS DATE)` is TZ-8
  (session-zone Date32; type pin here stays Date32). **B-TZ-4 (V-3 A5 overflow):** `CAST(ts AS STRING)` is now `Utf8`
  (was `Utf8View`). Ledger: `../../../task/tz5-cast-seconds-ledger.md`,
  `../../../task/v3-btz4-ledger.md`.
- [session_timestamp_type.rs](session_timestamp_type.rs) — **Q10:** Spark-door +
  native DataFrame pins for `spark.sql.timestampType`. Default LTZ type/value,
  NTZ opt-in literals/casts (naive µs, no localization), invalid-value refusal
  naming both tokens, DDL `TIMESTAMP` → Iceberg `timestamp` under NTZ /
  `timestamptz` under LTZ. `to_timestamp` stays LTZ.
- [ta_window.rs](ta_window.rs) — deferred rows #8-#14 (phase-2 PR-4): the seven
  `sql_route_*` cases, ported from v1 `repark-session/tests/ta_window.rs`. Proves the TA window
  UDFs the composed `repark_ta::TaExtension` registers are `f64::to_bits`-identical to the
  `repark_ta` kernels on the crate's own 5000-row OHLC goldens (`../../repark-ta/tests/goldens/*.bin`
  — read, never re-recorded), across single/scalar-param/multi-series/parked-four families,
  `PARTITION BY` scoping, a 12k multi-batch partition, and the non-literal-period refuse.
  **TA-1 (2026-08-15):** three plan-shape pins — named `OVER w` and inline same-spec
  `ta_*(…) OVER (PARTITION BY … ORDER BY …)` each plan one `WindowAggExec` (EXPLAIN +
  `create_physical_plan`); an intervening filter between two live windows stacks two
  (`sql_intervening_filter_between_windows_stacks_window_agg_exec`). Ledger:
  [../../../task/ta1-sql-fusion-ledger.md](../../../task/ta1-sql-fusion-ledger.md).

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
| See the timestamp-cast epoch-seconds class at the Spark door + DataFrame API | [timestamp_cast_seconds.rs](timestamp_cast_seconds.rs) |
| See that class at the ANSI door | [../../repark-sql/tests/timestamp_cast_ansi_door.rs](../../repark-sql/tests/timestamp_cast_ansi_door.rs) |
| See that class at the facade | [../../../python/repark/tests/test_timestamp_cast_parity.py](../../../python/repark/tests/test_timestamp_cast_parity.py) |

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
- `sql_*_window_agg_exec` RED: DataFusion's same-OVER fusion / intervening-filter stacking
  moved. The pin records measured truth (`task/ta1-sql-fusion-ledger.md`); do not "fix"
  the engine in this lane, and do not drop `ema5` from the stacked SELECT (DCE then
  collapses the count to 1).
- Week-53 assertion is ISO-week semantics (2021-01-01 → ISO week 53 of 2020) — a failure there
  is the date shim regressing, not the fixture.

- **Neutral-fixture scrub (2026-08-10, hardening-prep):** an owner-approved, forward-only,
  comment-and-fixture-only pass moved this directory's doc text and example literals to
  neutral placeholders — the upstream job the acceptance shape mirrors is named generically
  ("the source publish job"), and example table/view/entity names are placeholders carrying no
  domain vocabulary. Outcome-neutral: every renamed fixture moved together with the assertions
  that read it. Sites here: `ddl_sessions.rs` — two doc comments, and the temp-view fixture in
  `catalog_surface_table_exists_and_temp_views`, now `staging_view`.

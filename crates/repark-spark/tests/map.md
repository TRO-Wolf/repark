# map — crates/repark-spark/tests/

## Purpose

Integration tests of the assembled Spark door: a real `repark_core::ReparkSession` built with
`SparkExtension` + `SparkDialect`, exercised end-to-end.

## Contents

- [session_extension.rs](session_extension.rs) — temp view + `session.sql` reaches the
  Spark date shim (`year`, `weekofyear`) through the installed extension + dialect.
- [declared_sorted_tighten.rs](declared_sorted_tighten.rs) — Spark-door execution pins that
  `tightenNulls` elides `SortExec` for nullable `ORDER BY ts` keys, while hint mode keeps it.
  Tightened-source CTAS, derived expressions, EXISTS subqueries, cache remints, lazy `into_view`,
  and bare/two-part catalog names refuse without publishing the sink. All-nullable projection
  CREATE and INSERT remain allowed.
- [ddl_sessions.rs](ddl_sessions.rs) — CTAS
  end-to-end, namespace-`location` on a strict catalog (ADV-1 / N5), the BUG-001 dual-key
  property pin, the `spark.catalog` metadata surface, and the config-driven memory catalog —
  all on memory/local catalogs (AWS-free).
- [dml_sessions.rs](dml_sessions.rs) — `session_sql_bare_dml_applies_eagerly` — the F-BR-2 bare-
  `INSERT` eager-apply trap through
  `session.sql` (memory catalog, AWS-free).
- [session_timezone.rs](session_timezone.rs) — live-Spark value and Arrow-type pins for
  extractors, `date_trunc`, `date_format`, DST boundaries, pre-1970 instants, and the native
  DataFrame API across non-UTC and half-hour zones. DATE/TIME invariants, source instants,
  default-zone fallback, composed DATE/string inputs, date-valued shims, and TZ-7/TZ-8 registry
  divergences are covered.
- [timestamp_cast_seconds.rs](timestamp_cast_seconds.rs) — the
  `CAST(TIMESTAMP AS <numeric>)` epoch-seconds class at the **Spark door** and the **native
  DataFrame API**, on real sessions, value AND Arrow type. Nine pins: whole instants either side
  of 1970; the **floor edge both signs** (Spark uses `Math.floorDiv`, so `-0.5 s → -1` and
  `-1.25 s → -2` where truncation toward zero says `0` and `-1` — the only inputs that separate
  the real fix from the plausible one); zone-independence across three zones (a cast reads the
  instant, never a wall clock); a real timestamp COLUMN with its null mask; narrower integer
  targets (`INT`/`SMALLINT`); float and decimal
  targets, which keep the fraction; reverse `CAST(<integer> AS TIMESTAMP)` reads seconds and
  round-trips; its Arrow type is `timestamp[us, tz=UTC]`. `CAST(ts AS DATE)` is TZ-8
  (session-zone Date32), and `CAST(ts AS STRING)` is `Utf8`. Ledger: `../../../task/tz5-cast-seconds-ledger.md`,
  `../../../task/v3-btz4-ledger.md`.
- [session_timestamp_type.rs](session_timestamp_type.rs) — **Q10:** Spark-door +
  native DataFrame pins for `spark.sql.timestampType`. Default LTZ type/value,
  NTZ opt-in literals/casts (naive µs, no localization), invalid-value refusal
  naming both tokens, DDL `TIMESTAMP` → Iceberg `timestamp` under NTZ /
  `timestamptz` under LTZ. `to_timestamp` stays LTZ.
- [ta_window.rs](ta_window.rs) — seven
  `sql_route_*` cases prove the TA window
  UDFs the composed `repark_ta::TaExtension` registers are `f64::to_bits`-identical to the
  `repark_ta` kernels on the crate's own 5000-row OHLC goldens (`../../repark-ta/tests/goldens/*.bin`
  — read, never re-recorded), across single/scalar-param/multi-series/parked-four families,
  `PARTITION BY` scoping, a 12k multi-batch partition, and the non-literal-period refuse.
  Three plan-shape pins cover named `OVER w` and inline same-spec
  `ta_*(…) OVER (PARTITION BY … ORDER BY …)` each plan one `WindowAggExec` (EXPLAIN +
  `create_physical_plan`); an intervening filter between two live windows stacks two
  (`sql_intervening_filter_between_windows_stacks_window_agg_exec`). Ledger:
  [../../../task/ta1-sql-fusion-ledger.md](../../../task/ledgers/archive/2026-08/2026-08-15-ta1-sql-fusion-ledger.md).

## I want to...

| ...do this | go to |
|---|---|
| See the door-installed session end-to-end pin | [session_extension.rs](session_extension.rs) |
| See test ownership notes | [map.md](map.md) |
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
- `session_extension.rs` covers the session extension seam.

## Debug

- `cargo test -p repark-spark --test session_extension` (or `--test ddl_sessions`) runs one
  file. Never `--all-features` (AGENTS.md PyO3 note).
- `ddl_sessions.rs` failures usually mean a DDL handler regressed (ctas / namespace_ddl /
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

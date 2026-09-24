# map — crates/repark-spark/tests/

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001). Wrapped-line fragments rewritten as complete sentences (D-002).

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
  CREATE and INSERT remain allowed. EAGER-BUDGET-1 step 2 (2026-09-13): the
  cache-materialize call site passes `None` for the new `max_total_bytes` parameter; the
  pinned refusal is unchanged. pins: eager-budget-1/C-005
  **ICE-VIEWS-1 / V-001 (2026-09-22):** CREATE VIEW over a tightened source
  refuses before the catalog write — including the facade's
  `/* repark:bare-name */` marked spelling, which is a registered-catalog
  write, not a session view — while an untightened CREATE VIEW persists and
  reads back; a session `CREATE TEMPORARY VIEW` over the tightened source
  serves the source rows byte-for-byte (IPI-40 PR6a1,
  `session_scoped_temp_view_serves_and_select_into_stays_allowed`) and SELECT
  INTO stays allowed; `ALTER VIEW … AS` keeps its own refusal ahead of the tighten
  error.
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
  divergences are covered. **FNP-11B step 4 (2026-09-15):**
  `time_arguments_never_move_with_the_session_zone` asserts the zone-independent
  `[UNSUPPORTED_TIME_TYPE]` refusal (hour/minute/second over TIME) instead of
  the old answering values. pins: fnp-11b/C-005
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
- [decimal_float_coercion.rs](decimal_float_coercion.rs) — WO-2 (xo-muse8 UNIT1
  fix-b): a decimal literal against a DOUBLE/FLOAT column widens the literal to
  DOUBLE (`d = CAST(0.0 AS DOUBLE)`, Spark's analyzed shape), never the column to
  decimal — so NaN rows answer instead of `Overflowing on NaN`. Forty-three
  live-Spark-4.1.2-measured row pins over a NaN/-0.0/null-bearing DOUBLE, FLOAT
  and DECIMAL(6,2) fixture: 26 match Spark (six comparison operators, IN/NOT IN,
  BETWEEN/NOT BETWEEN, negatives, reversed sides, FLOAT with the 0.1f32
  double-widening discriminator, decimal-vs-decimal/decimal-vs-integral
  near-miss controls) and 17 pin the separate pre-existing signed-zero kernel
  divergence (the float eq kernel keeps `-0.0` distinct from `0.0`, so zero-bound
  `=`/`<>`/`<`/`>=`/IN/BETWEEN cases differ from Spark by exactly the `-0.0`
  row), each with Spark's answer in the assertion message. One logical-plan
  shape pin proves no decimal cast remains.
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

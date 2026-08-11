# map — repark-functions

## Purpose

The Spark-compatible function registry **and the Spark expression-semantics layer** (crate-DAG
tier 3, a capability leaf with no internal deps — the doors consume it, never the reverse): wire
`datafusion-spark` into a session, hand-implement the Spark functions it lacks (date / string /
collection shims), and carry the analyzer rule that rewrites raw DataFusion operator semantics
(integer `/`, div-by-zero, `[]` subscript) to Spark's — the AR-WG-SQL fidelity layer.

## Contents

- **R-FN-BATCH3:** expr_fn next_day/hour/minute/second; **octo X1-C3:** hour/minute/second are
  repark `DatePartUdf` shims (accept Time32/64 + Timestamp; overwrite datafusion-spark Timestamp-only).


- `aggregate.rs` / R-RETRACT-SHIM: Float64 `avg` with `retract_batch` (overwrites SparkAvg; sliding windows).
  Q1 unit test: `percentile_approx_sql_aliases_resolve` pins Spark SQL aliases.

- `Cargo.toml` — package; depends on `datafusion` + `datafusion-spark` + `arrow` + `chrono`.
  DataFusion-native: speaks `datafusion::error::Result`, so **no** `repark-core` dep.
  **r24 G10 / PERF-10:** crate-level `criterion` 0.5.1 dev-dep + `[[bench]] ratio_string_datetime`
  (never `[workspace.dependencies]`). See [benches/map.md](benches/map.md).
- `benches/` — PERF-10 ratio micro-benches (`date_format`/`to_char`, `substring`/`upper`).
- `src/lib.rs` — `register_all(ctx)` (datafusion-spark's full set, then the shims — later
  registration wins) + **Q1** `approx_percentile_cont` re-registered with aliases
  `percentile_approx` / `approx_percentile` via `AggregateUDF::with_aliases` +
  `spark_date_shim_functions()` + `analyzer_rules()` (installed by the session on every
  context via the Spark door's `SessionExtension` in `repark-spark`) + the shared
  `shim_udf_boilerplate!` macro. Error conversion from `DataFusionError` happens one layer
  up in `repark-core` (this crate stays DataFusion-native).
- `src/cardinality.rs` — **r24 SB1 / SEC-01:** plan-time `array_repeat`/`repeat`/`sequence` ceilings
  (`repark.sql.maxArrayElements` default 10_000_000) + `ReparkSqlConfig` extension
  (`allowLocalFilesystemDDL` for SEC-02); analyzer rule `ArrayCardinalityCeiling`.
- `src/analyzer.rs` — `SparkExprSemantics`: int `/` → double, div/mod-by-zero → NULL (Spark
  **non-ANSI**), 0-based `[]` array subscript, and the planner-embedded-`substr` swap. Idempotent
  by construction (see the module docs). NB: the rule runs after `TypeCoercion`, so ONE analyze is
  not always a whole-plan fixpoint — a division under a set op (`UNION`) reaches `Float64` only on
  the SECOND analyze; single-analyze *schema* consumers must analyze to the fixpoint (Group L-write,
  `repark_sql::execute_ctas`; the `analyze_eagerly` docstring carries the rule).
- `src/string.rs` — `substring`/`substr` with Spark's character-based position semantics;
  **D2** `concat` (`SparkConcat`) overwrites datafusion-spark: coerce all args → `Utf8`
  (Spark stringify), always emit `Utf8` (never `Utf8View`), any-NULL → NULL — fixes
  TPC-DS Q5/Q80/Q84; pins include `register_all` overwrite + multi-row null mask.
- `src/collection.rs` — Spark `element_at` (arrays + maps) + the embedded `__repark_array_get__`
  subscript UDF + **`spark_get_item_udf` / `__repark_get_item__`** (polymorphic array 0-based or
  map-by-key for facade `Column.__getitem__` non-int/non-str keys — octo C2-L-001).
- `src/datetime.rs` — the Spark calendar date shim: extractors `year`/`month`/`dayofmonth`/`day`/
  `dayofyear`/`quarter`/`weekofyear`/`yearofweek`/`dayofweek`/`weekday`, **hour/minute/second**
  (Time+Timestamp; X1-octo C3), `make_date`, and the WG2 calendar-math shims `add_months`
  (end-of-month clamp), `trunc(date,fmt)`, `date_trunc(fmt,ts)`, `date_format` (Java pattern →
  string), each with unit tests + a named `*_udf()` constructor.
  **r20 A1 SAF-001:** out-of-chrono Date32 → NULL in `add_months`/`trunc` (extreme i32 pins);
  **SAF-002:** per-site downcast evidence + defensive `cast` before `as_primitive`/`as_string`
  on datetime invoke paths (LargeUtf8 format pin).
  **r24 A3 PERF-02:** `date_format` compiles Java pattern once per invocation (scalar
  pattern cache) via `compile_java_pattern` + `format_compiled_java_pattern`.
  **r24 A3 PERF-03:** `substring`/`spark_substring` uses `char_indices` byte offsets +
  sized `StringBuilder` (no per-cell `Vec<char>`).
  **octo C1-Q-004:** `perf_measure_*` 1M-row benches gated on `REPARK_PERF_MEASURE=1`.
  **octo C2-Q-001:** compile pattern apostrophe/unterminated pins.
- `src/expr_fn.rs` — logical-`Expr` builders for the date functions including Group I `weekday`
  (embed the UDF instance so a `PyColumn` gets a self-contained expression); `date_add`/`last_day`
  come from `datafusion-spark`.

## I want to...

| ...do this | go to |
|---|---|
| Add a Spark date function | `src/datetime.rs` (`functions()` list) + a golden unit test |
| Add a non-date Spark function | new or existing `src/<group>.rs` (`string`/`collection`); register it from `lib.rs::register_all` |
| Check what datafusion-spark already provides | its installed source (`hour`/`last_day`/`date_add`/…); don't duplicate |
| Fix a Spark-semantics mismatch in a *function* | the matching shim module; lean on `datafusion-spark` where it is correct |
| Tune plan-time array expansion ceiling / local DDL conf | `src/cardinality.rs` (`ReparkSqlSettings`, `MAX_ARRAY_ELEMENTS_KEY`)
| Fix a Spark-semantics mismatch in an *operator* (`/`, `%`, `[]`, ORDER BY defaults) | `src/analyzer.rs` (plan-level, type-aware) — ORDER BY defaults live in `repark-spark::spark_ast` (AST-level) |

## Component contract

- **Owns:** the Spark-compatible function registry (`register_all`) — `datafusion-spark` wiring +
  hand-rolled shims (date / string / collection) + the Spark expression-semantics analyzer rules (int
  `/` → double, div / mod-by-zero → NULL, 0-based `[]`); the plan-time array-cardinality ceiling +
  the `repark.sql.*` config extension.
- **Does not own:** session construction (repark-core installs these via a `SessionExtension`); SQL
  routing (the doors); TA / ML kernels.
- **Public inputs:** a DataFusion `SessionContext` (`register_all(ctx)`); `analyzer_rules()`; `Expr`
  builder calls.
- **Public outputs:** registered scalar / aggregate UDFs + aliases; analyzer rules; logical-`Expr`
  builders (consumed by the Spark door + the Python bindings).
- **State & lifecycle:** stateless registration; idempotent analyzer rules.
- **Allowed internal deps:** **none internal** — speaks `datafusion::error::Result` (no `repark-core`
  dep). Third-party: datafusion + datafusion-spark + arrow + chrono.
- **Failure model:** `datafusion::error::Result`; a missing function gets a Rust shim or a LOUD
  unsupported error — never Python compute.
- **Extension points:** add a Spark date / non-date function (a shim module + register from
  `lib.rs`); fix an operator-semantics mismatch in `analyzer.rs`.
- **Test strategy:** `cargo test -p repark-functions` — golden unit tests per function (ISO-8601 basis,
  matching Spark); env-gated micro-benches.
- **Known limitations:** session-timezone semantics for tz-timestamp extractors are a follow-up; the
  analyzer rule runs after `TypeCoercion`, so single-analyze *schema* consumers must reach the fixpoint.

## Dependencies worth knowing

- `arrow` carries an explicitly DECLARED `chrono-tz` feature. Without it
  `arrow::array::timezone::Tz` accepts only fixed offsets, so every IANA zone id would fail at
  query time on the session-timezone extraction path. It resolves transitively through
  `datafusion-functions` anyway; declaring it means a DataFusion feature change cannot silently
  break `spark.sql.session.timeZone = "America/New_York"`.

## Pointers

- Up: [../map.md](../map.md)
- Related: the Spark door's `SessionExtension` (`crates/repark-spark/src/extension.rs`)
  calls `register_all` + `analyzer_rules` at session build; `repark-core` owns the
  `SessionExtension` trait and error conversion layer.
- Goldens are ISO-8601 (Python `date.isocalendar`) — the same basis Spark uses.

## Debug

| Symptom | First check |
|---|---|
| `dayofweek` off by one | Spark is 1=Sunday..7=Saturday — shim adds 1 to arrow `DayOfWeekSunday0`; `weekday` is 0=Monday |
| `weekofyear`/`yearofweek` look wrong near Jan 1 | they are ISO-8601 (arrow `WeekISO`/`YearISO`); e.g. 2021-01-01 → week 53 of 2020 |
| `make_date(2024, 2, 29)` errors on coercion | signature is `Int64` (DataFusion literals are Int64); Int32 columns widen in |
| A date function is missing | hand-roll it in `datetime.rs` (`date_format` / `add_months` already ship) |
| What arg types do extractors accept? | `coerce_types` accepts date / timestamp (any unit+zone) / string (parsed to date); anything else is a planning error. A bare `Int` is rejected (Spark has no such overload) |
| `year(tz_timestamp)` value looks off | the field is extracted in the stored zone, not a session timezone — Spark session-tz semantics are a follow-up (planning works; only the tz interpretation differs) |

First checks: `cargo test -p repark-functions`. Escalate to: [../map.md#debug](../map.md).

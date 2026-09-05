# map — repark-functions

## Purpose

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

The Spark-compatible function registry **and the Spark expression-semantics layer** (crate-DAG
tier 3, a capability leaf with no internal deps — the doors consume it, never the reverse): wire
`datafusion-spark` into a session, hand-implement the Spark functions it lacks (date / string /
collection shims), and carry the analyzer rule that rewrites raw DataFusion operator semantics
(integer `/`, div-by-zero, `[]` subscript) to Spark's — the AR-WG-SQL fidelity layer.

## Contents

- **R-FN-BATCH3:** expr_fn next_day/hour/minute/second; **octo X1-C3:** hour/minute/second are
  repark `DatePartUdf` shims (accept Time32/64 + Timestamp; overwrite datafusion-spark Timestamp-only).


- `aggregate.rs` / R-RETRACT-SHIM + **DEC-5 / Z-3 U1:** `SparkAvgWithRetract` keeps
  Float64 retract (X2 sliding windows) and now also keeps Spark-typed decimal `avg`
  (`(p,s)→(min(38,p+4), min(38,s+4))`) with decimal `retract_batch` (small copy of
  DF `DecimalAvgAccumulator` / `DecimalAverager`). Signature is DF `Avg`'s: Decimal
  stays decimal; Integer/Float still coerce to Float64. Pins:
  `group_avg_decimal128_stays_decimal_14_6_i128`, `sliding_avg_decimal128_retracts`.
  **W-2 U2 ride-along (Z-3 S3):** revert-red pins
  `group_avg_decimal32_stays_decimal_9_6_i32`,
  `group_avg_decimal64_stays_decimal_14_6_i64`,
  `group_avg_decimal256_stays_decimal_14_6_i256`.
  FN-FIX-1 unit test: `percentile_approx_sql_aliases_resolve` pins Spark SQL aliases
  on the discrete UDAF. pins: fn-fix-1-registry-rows/C-002
  **PERF-AGG-AVG-1 (2026-09-05):** the UDAF now also serves grouped aggregation
  through `src/avg_groups.rs` (`groups_accumulator_supported` /
  `create_groups_accumulator` over Float64 and Decimal32/64/128/256, `try_avg`
  overflow on the 2×-MAX shape → per-group NULL, the sum-wrap shape filed as
  BACKLOG `AVG-DEC-SUMWRAP-1`); the `Accumulator` retract arms are untouched for
  window frames. The 300 ms isolated cost on `avg(l_quantity) GROUP BY l_partkey` (200 k
  groups) was one boxed accumulator per group; the groups path keeps one sum and one
  count vector. pins: perf-agg-avg-1/C-001, C-002

- `Cargo.toml` — package; depends on `datafusion` + `datafusion-spark` + `arrow` + `chrono`.
  DataFusion-native: speaks `datafusion::error::Result`, so **no** `repark-core` dep.
  **r24 G10 / PERF-10:** crate-level `criterion` 0.8 dev-dep + `[[bench]] ratio_string_datetime`
  (never `[workspace.dependencies]`). See [benches/map.md](benches/map.md).
- `benches/` — PERF-10 ratio micro-benches (`date_format`/`to_char`, `substring`/`upper`).
- `src/spark_length.rs` — **GT1-FIX G5 / A3 / R3-1:** Spark `bit_length` /
  `octet_length` (stringify non-binary; BINARY pass-through including
  Dictionary(_, Binary); refuse ARRAY/STRUCT/MAP; decimal scale-padded
  stringify). Wired from `string::functions()` + `expr_fn`. Ledger:
  `task/fn-gt1-ledger.md`.
- `src/spark_log.rs` — **SEM-1 (2026-08-31):** Spark-door `log` (natural / `log(base, expr)`,
  null-guard both arities). Overwrites DataFusion base-10 from `register_all`.
  pins: sem-1-spark-answer-parity/C-004
- `src/spark_log1p.rs` — **LOG1P-1 (2026-09-02):** `log1p` / `expm1` (`unary` +
  `ln_1p` / `exp_m1`; `log1p` `nullif` on `x <= -1`) on both SQL doors.
  pins: log1p-1-precise-kernels/C-002
- `src/spark_regexp.rs` — **GT1-FIX A1/A2 / R3 / R4-1:** Spark `regexp_count` /
  `regexp_instr` (NULL-in NULL-out INT; idx ignore-value; UTF-16 start;
  Java find-loop for empty-after-non-empty; positional mid-surrogate probe
  not `is_match("")`; Dictionary(_, Utf8) coerce). Overwrite from
  `string::functions()` + `expr_fn`.
- `src/spark_split_part.rs` — **GT1-FIX F-6c / R3-1:** Spark `split_part`
  STRING `partNum` implicit-cast; partNum 0 fail-loud; Dictionary(_, Utf8).
- `src/higher_order/` — FNP-4c kernels on the FNP-4a shared table. See
  [src/higher_order/map.md](src/higher_order/map.md).
- `src/lib.rs` — `register_all(ctx)` (datafusion-spark's full set, then the shims — later
  registration wins) + **FN-FIX-1** `percentile_approx` / `approx_percentile` discrete UDAF
  (`percentile_approx_udaf` with alias); `approx_percentile_cont` stays the t-digest name for ML +
  `spark_date_shim_functions()` + `analyzer_rules()` (`SparkDecimalPrecision` first, then
  `SparkDecimalRewrite` (U4b `/` + DEC-6), then `SparkIntegerOverflow` (F-Y10-1), then
  `SparkExprSemantics` + cardinality + instant_ts; installed by the session on every
  context via the Spark door's
  `SessionExtension` in `repark-spark`) + DEC-8 `register_spark_decimal_planner` from
  `register_all` + the shared `shim_udf_boilerplate!` macro. Error conversion from
  `DataFusionError` happens one layer up in `repark-core` (this crate stays DataFusion-native).
- `src/decimal_precision.rs` — **V-2 / DEC U3+U4a:** Spark `DecimalPrecision` rule (integer-literal
  min-precision on `+ − *`; add/sub/mul 38-clamp via CAST-after). `/` formula and DEC-8
  plan-refuse live in `decimal_spark.rs`. Ledger: `task/v2-dec-u3u4-ledger.md`.
- `src/decimal_spark.rs` — **R-2 / U4b + DEC-8 + DEC-6:** Spark `/` UDF (`resultDecimalType`
  + 38-clamp), `SparkDecimalRewrite` analyzer slot (A5, before `SparkExprSemantics`; UDF
  owns `/0`), `SparkDecimalExprPlanner` for `(38,20)*(38,20)`, checked `+`/`−` UDF reading
  the landed ANSI knob. Ledger: `task/r2-dec-close-ledger.md`.
- `src/integer_spark.rs` — **F-Y10-1:** checked integer `+` / `-` / `*` UDFs, `ExprPlanner`,
  and analyzer rule. `ansi=true` raises `ARITHMETIC_OVERFLOW`; `ansi=false` wraps.
  Planner rewrite requires a typed Int32/Int64 operand (column or CAST); pure-literal
  arithmetic stays Int64. Planned UDF calls alias to the original BinaryExpr name.
  `install_integer_overflow` is the ANSI-door hook.
  pins: f-y10-1-int-overflow/C-001, C-002, C-003
- `src/cardinality.rs` — **r24 SB1 / SEC-01:** plan-time `array_repeat`/`repeat`/`sequence` ceilings
  (`repark.sql.maxArrayElements` default 10_000_000) + `ReparkSqlConfig` extension
  (`allowLocalFilesystemDDL` for SEC-02); analyzer rule `ArrayCardinalityCeiling`.
  Known limitations (2026-08-29): reachable `i64::MIN` negative strides can panic in debug or
  return `Some(1)` in release, and `i128::MIN / -1` or `% -1` can panic; these are pre-existing
  separate-fix items.
- `src/timestamp_cast.rs` — **TZ-5** plus **B-TZ-4:** the embedded UDFs the analyzer's
  `Expr::Cast` arm puts under timestamp casts. `__repark_epoch_seconds_floor__` (→ `Int64`,
  exact `div_euclid` **floor** — Spark uses `Math.floorDiv`, so `-0.5 s` is `-1`, not `0`)
  serves integer targets; `__repark_epoch_seconds_real__` (→ `Float64`) serves `DOUBLE`/`FLOAT`/
  `DECIMAL`, which keep the fraction. Two UDFs, not one: a decimal intermediate loses the floor
  edge to arrow's truncating decimal→int cast, and an f64 one cannot floor a sub-microsecond
  present-day instant. Per-`TimeUnit` divisor; current timestamp outputs are `Timestamp(µs, UTC)`.
  **B-TZ-4 (2026-08-13):**
  `__repark_timestamp_to_string__` (→ `Utf8`) emits Spark's session-zone space-separated
  wall (NTZ = stored wall; trailing-zero fraction trimmed; year −1 is `-0001`, year 10000 is
  `+10000`). Embedded, never registered. **TZ-8 (2026-08-14):**
  `__repark_timestamp_to_date__` (embedded CAST → Date32) + registered `to_date`
  overwrite; both share `datetime::invoke_local_dates`. **DATE-FN-1 (2026-09-04):**
  registered `date` and `unix_timestamp`. Ledgers:
  `task/tz5-cast-seconds-ledger.md`, `task/v3-btz4-ledger.md`, `task/r4-tz8-ledger.md`.
  pins: date-fn-1-spark-date-spelling/C-002
- `src/timestamp_type.rs` — **Q10:** `spark.sql.timestampType` ConfigExtension
  (default `TIMESTAMP_LTZ`) + parse/from-map/from-options. Sibling of
  `SparkAnsiConfig`. Invalid value names both legal tokens.
- `src/ansi.rs` — **U5:** `spark.sql.ansi.enabled` ConfigExtension (default TRUE) +
  `__repark_ansi_nonzero_divisor__`. Sibling of `ReparkSqlConfig`, not mixed into
  `repark.sql.*`.
- `src/analyzer.rs` — `SparkExprSemantics`: int `/` → double, div/mod-by-zero follows
  `spark.sql.ansi.enabled` (raise when TRUE, `nullif` NULL when false), 0-based `[]` array
  subscript, the planner-embedded-`substr` swap, and **TZ-5**
  `CAST(TIMESTAMP AS <numeric>)` → epoch SECONDS (scaling pushed UNDER the user's cast via
  [`timestamp_cast`], so the outer cast still applies the width; the reverse direction
  `CAST(<integer> AS TIMESTAMP)` remains unchanged). **B-TZ-4:**
  `CAST(TIMESTAMP AS STRING)` → `__repark_timestamp_to_string__` (`Utf8`, session-zone wall).
  **TZ-8:** `CAST(TIMESTAMP AS DATE)` → `__repark_timestamp_to_date__` (session-zone Date32).
  Rewrites match source types and injected output shapes, so each rewrite is idempotent. Set-operation
  schemas still require repeated analysis to reach the fixpoint; `analyze_eagerly` carries that rule.
- `src/string.rs` — `substring`/`substr` with Spark's character-based position semantics;
  **D2** `concat` (`SparkConcat`) overwrites datafusion-spark: coerce all args → `Utf8`
  (Spark stringify), always emit `Utf8` (never `Utf8View`), any-NULL → NULL — fixes
  TPC-DS Q5/Q80/Q84; pins include `register_all` overwrite + multi-row null mask.
  Also registers `regexp_count` / `regexp_instr` / `split_part`.
- `src/collection/str_to_map.rs` — regex `str_to_map` UDF overwriting the DataFusion
  literal-split kernel; it depends on workspace `regex` and lives in the canonical module tree
  beside `shuffle.rs`, `map_from_entries.rs`, and `url/java_uri.rs`.
- `src/try_invert/` — FNP-7a/7b scalar `try_*` kernels. `try_element_at` aliases `element_at`.
  `try_avg` is a distinct UDAF (not an `avg` alias). INTERVAL `try_avg` refuses `[FNP-11]`.
  pins: fnp-7-try-inversions/C-001, C-013, C-018
- `src/collection.rs` — Spark `element_at` (arrays + maps) + the embedded `__repark_array_get__`
  subscript UDF + **`spark_get_item_udf` / `__repark_get_item__`** (polymorphic array 0-based or
  map-by-key for facade `Column.__getitem__` non-int/non-str keys).
  Registers the `str_to_map` overwrite.
- `src/aggregate.rs` — also carries `approx_count_distinct_udaf`, which
  registers DataFusion's `approx_distinct` under Spark's spelling as well. It lives in
  `functions()` rather than `register_all` because the crate root is at its ceiling, and because
  `functions()` is already the list `register_all` installs.
- `src/instant_ts.rs` — `now` / `current_timestamp` / `to_timestamp` emit `timestamp[us, tz=UTC]`;
  zoneless values localize in the session zone, while zone-suffixed values remain instants.
  **Q10:** NTZ opt-in arm of the CAST/literal rewrite.
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
- `src/expr_fn.rs` — logical-`Expr` builders for date functions, including `weekday`, that embed
  UDF instances for facade columns; `date_add`/`last_day` and the regexp/split builders share the
  same kernels registered by the SQL door.

## I want to...

| ...do this | go to |
|---|---|
| Add a Spark date function | `src/datetime.rs` (`functions()` list) + a golden unit test |
| Add a non-date Spark function | new or existing `src/<group>.rs` (`string`/`collection`); register it from `lib.rs::register_all` |
| Check what datafusion-spark already provides | its installed source (`hour`/`last_day`/`date_add`/…); don't duplicate |
| Fix a Spark-semantics mismatch in a *function* | the matching shim module; lean on `datafusion-spark` where it is correct |
| Tune plan-time array expansion ceiling / local DDL conf | `src/cardinality.rs` (`ReparkSqlSettings`, `MAX_ARRAY_ELEMENTS_KEY`)
| Fix a Spark-semantics mismatch in an *operator* (`/`, `%`, `[]`, ORDER BY defaults) | `src/analyzer.rs` (plan-level, type-aware) — ORDER BY defaults live in `repark-spark::spark_ast` (AST-level) |
| Fix Spark decimal result `(p,s)` / integer-literal min-precision | `src/decimal_precision.rs` (`SparkDecimalPrecision` for `+ − *`) + `src/decimal_spark.rs` (`/` UDF, DEC-8 `ExprPlanner`, DEC-6 overflow) |
| Fix a Spark-semantics mismatch in a *CAST* | `src/analyzer.rs` `rewrite_timestamp_to_numeric_cast` (TZ-5) / `rewrite_timestamp_to_string_cast` (B-TZ-4) / `rewrite_timestamp_to_date_cast` (TZ-8) for the SHAPE + `src/timestamp_cast.rs` for the kernel; cast **failure** semantics (overflow, malformed strings) are a different surface and are not owned here |

## Component contract

- **Owns:** the Spark-compatible function registry (`register_all`) — `datafusion-spark` wiring +
  hand-rolled shims (date / string / collection) + the Spark expression-semantics analyzer rules (int
  `/` → double, div / mod-by-zero gated by `spark.sql.ansi.enabled`, 0-based `[]`); the plan-time
  array-cardinality ceiling + the `repark.sql.*` config extension + the Spark ANSI carrier.
- **Does not own:** session construction (repark-core installs these via a `SessionExtension`); SQL
  routing (the doors); TA / ML kernels.
- **Public inputs:** a DataFusion `SessionContext` (`register_all(ctx)`); `analyzer_rules()`; `Expr`
  builder calls.
- **Public outputs:** registered scalar / aggregate UDFs + aliases; analyzer rules; logical-`Expr`
  builders (consumed by the Spark door + the Python bindings).
- **State & lifecycle:** stateless registration; idempotent analyzer rules.
- **Allowed internal deps:** **none internal** — speaks `datafusion::error::Result` (no `repark-core`
  dep). Third-party: datafusion + datafusion-spark + arrow + chrono + regex.
- **Failure model:** `datafusion::error::Result`; a missing function gets a Rust shim or a LOUD
  unsupported error — never Python compute.
- **Extension points:** add a Spark date / non-date function (a shim module + register from
  `lib.rs`); fix an operator-semantics mismatch in `analyzer.rs`.
- **Test strategy:** `cargo test -p repark-functions` — golden unit tests per function (ISO-8601 basis,
  matching Spark); env-gated micro-benches.
- **Known limitations:** the analyzer runs after `TypeCoercion`, so single-analyze schema consumers
  must reach the fixpoint; cardinality edge-case risks are listed under `src/cardinality.rs`.

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
| `year(tz_timestamp)` value looks off | check the installed session-timezone carrier and the LTZ/NTZ input type |
| `CAST(ts AS BIGINT)` looks 10⁹ too big | the analyzer rule is not installed — the rewrite ships with the Spark door's `SessionExtension`; a bare session keeps DataFusion's raw tick (pinned in `crates/repark-sql/tests/timestamp_cast_ansi_door.rs`) |
| `CAST(ts AS STRING)` is ISO-`T` / `string_view` | the B-TZ-4 rewrite is not firing — check `rewrite_timestamp_to_string_cast` and `__repark_timestamp_to_string__`; a bare session still rewrites (the UDF is in `SparkExprSemantics`) but renders NTZ/UTC |
| `CAST(ts AS DATE)` / `to_date(ts)` is a day late west of UTC | the TZ-8 rewrite / `to_date` overwrite is not firing — check `rewrite_timestamp_to_date_cast` and `__repark_timestamp_to_date__`; `datediff` rides CAST; `last_day`/`date_add` over TIMESTAMP still refuse |
| `CAST(ts AS BIGINT)` is off by one before 1970 | Spark FLOORS (`Math.floorDiv`): `-0.5 s` is `-1`. If it reads `0`, something reintroduced truncating division — check `seconds_floor_from_ticks` uses `div_euclid` |
| A `CAST` grew a second `__repark_epoch_seconds_*` wrapper | the rewrite must match on the SOURCE type (a timestamp), never the target; `the_timestamp_cast_rewrite_is_idempotent` is the pin |

First checks: `cargo test -p repark-functions`. Escalate to: [../map.md#debug](../map.md).

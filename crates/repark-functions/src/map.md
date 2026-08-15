# map — repark-functions/src

## Purpose

Source for `repark-functions` — Spark function registry, the function shims (date / string /
collection), and the Spark expression-semantics analyzer rule. See [../map.md](../map.md).

## Contents

- `timestamp_type.rs` — **Q10:** Spark-door `spark.sql.timestampType` carrier
  (`SparkTimestampTypeConfig`, `PREFIX = repark.timestamp`, default
  **TIMESTAMP_LTZ**). Parsed from the builder map in `SparkExtension::configure`.
  Missing carrier also defaults LTZ (today's type-resolution). Invalid value
  fail-louds naming `TIMESTAMP_LTZ` and `TIMESTAMP_NTZ`. Ledger:
  `task/q10-timestamptype-ledger.md`.
- `ansi.rs` — **U5 / Q10=A:** Spark-door `spark.sql.ansi.enabled` carrier
  (`SparkAnsiConfig`, `PREFIX = repark.ansi`, default **TRUE**) + the embedded
  `__repark_ansi_nonzero_divisor__` raise kernel. Parsed from the builder map in
  `SparkExtension::configure`. Missing carrier also defaults TRUE. `notabool`
  fail-louds with Spark's `should be boolean, but was` needle
  (`DataFusionError::Configuration`; IllegalArgument class is a named residue —
  `engine_err` never emits `Error::Config`). Ledger: `task/s1-ansi-knob-u5-ledger.md`.
- `session_time_zone.rs` (+ `session_time_zone/`) — **H-1a split B:** the CARRIER that brings the
  resolved session timezone to the extractors. A `ConfigExtension` with a two-segment `PREFIX`
  (`repark.session`), a `set` that always refuses naming `spark.sql.session.timeZone`, and empty
  `entries()` — so it is a channel, never a second spelling of the knob. Filled by
  `repark-spark`'s `SparkExtension::configure` (the only crate depending on both the engine that
  owns the key and this leaf); read by `datetime.rs` at invoke. The empty `entries()` also erases
  the zone from `ScalarFunctionExpr` equality (DataFusion 54.1 compares sorted config entries), so
  two identical extractor expressions built under different session zones compare EQUAL — safe only
  while no plan cache or cross-session expression reuse exists, and stated in the module doc beside
  the rationale rather than left to be rediscovered.
- `datetime.rs` — **session-zone semantics (H-1a split B + its 2026-08-10 rework).** The coercion
  path is **type-driven** as of TZ-4 PR-2 (`coerce_date_arg` / `coerce_to_timestamp_micros` /
  `coerce_to_date32`: `Timestamp(_, Some(_))` is an LTZ instant; `Timestamp(_, None)` is NTZ
  and stays naive; `Date32`/`Time`/string keep zone-free types; every arm a fixed point,
  because DataFusion re-analyzes at physical planning). Zoneless LTZ inputs are localized
  by `instant_ts.rs`. At invoke,
  `LocalSource` says whether the widened micros are an instant or a zone-free wall clock —
  `invoke_local_micros` for `date_trunc`/`date_format`, `invoke_local_dates` for `trunc`/
  `add_months`. `micros_from_local_datetime` models `java.time.ZonedDateTime.ofLocal` arm for arm
  (single offset; **ambiguous → prefer the SOURCE instant's offset**, which is what
  `ZonedDateTime.truncatedTo` does and what keeps a fall-back hour's two instants distinct; gap →
  `offset_before_gap`, a bounded 26-hour lookback that replaced a 15-minute forward search whose
  "all IANA gaps are one hour" justification was false).
- `cardinality.rs` — r24 SB1 SEC-01 ceilings + SEC-02 conf extension (const
  CAST / arithmetic / abs / coalesce / greatest / least / nullif / CASE /
  arrow_cast / utf8→int / float math / log*/exp/sqrt / bitwise / trivial scalar-subquery fold; depth-bounded)


- **R-FN-BATCH4** aggregate expansion.

- **Q1 R-ML-QUANTILE:** `register_all` re-registers `approx_percentile_cont` with Spark SQL
  aliases `percentile_approx` / `approx_percentile` (`AggregateUDF::with_aliases`); unit pin
  `percentile_approx_sql_aliases_resolve` in `aggregate.rs`.

- **R-FN-BATCH3:** expr_fn next_day/hour/minute/second; **X1-octo C3:** hour/minute/second are
  repark DatePartUdf (Time+Timestamp), overwriting datafusion-spark Timestamp-only.


- `aggregate.rs` / R-RETRACT-SHIM + **DEC-5 / Z-3 U1:** Float64 `avg` retract (X2) plus
  Spark-typed decimal `avg` with decimal `retract_batch` (no Numeric→Float64 coerce).
  **W-2 U2 ride-along:** Decimal32/64/256 accumulator arms have revert-red pins
  (`group_avg_decimal32_stays_decimal_9_6_i32` and siblings).

- `decimal_precision.rs` — **V-2 / DEC U3+U4a:** `SparkDecimalPrecision` analyzer rule.
  U3: integer-literal `fromLiteral` (`DECIMAL(digits,0)`) on `+ − *` only (typed INT
  columns untouched). U4a: CAST-after add/sub/mul clamp (`allowPrecisionLoss=true`).
  `/` formula, DEC-8, and DEC-6 live in `decimal_spark.rs`. Inserted **first** in
  `analyzer_rules()` (before `SparkDecimalRewrite` then `SparkExprSemantics`).
  Ledger: `task/v2-dec-u3u4-ledger.md`.
- `decimal_spark.rs` — **R-2:** `SparkDecimalRewrite` (A5 slot: clean `decimal / decimal`
  before `SparkExprSemantics`; UDF owns `/0`) + `SparkDecimalExprPlanner` (DEC-8
  compute-with-clamp) + checked `+`/`−` (DEC-6, reads `SparkAnsiConfig`). Registered
  from `lib.rs` (`analyzer_rules` + `register_all`). Ledger: `task/r2-dec-close-ledger.md`.
- `lib.rs` — `register_all(ctx)` (datafusion-spark's full set, then the date + string + collection
  + **r20 G2** `random` (Spark XORShift `rand`/`randn`/`random`) shims — later registration wins a
  name clash) + Q1 percentile aliases + `spark_date_shim_functions()` +
  `analyzer_rules()` (`SparkDecimalPrecision` → `SparkDecimalRewrite` → Spark semantics +
  cardinality + instant_ts; the session installs them via the Spark door's `SessionExtension`;
  error conversion one layer up is `repark-core`) + `register_spark_decimal_planner` +
  `analyze_eagerly(state, plan)` — the ONE blessed way to run the analyzer before a plan's
  schema or expressions cross a boundary (`ctx.sql` plans are PRE-analysis; an un-analyzed
  schema over analyzed buffers bit-reinterprets at the Arrow export — consumed by
  `repark-spark::spark_ast` and `repark-python::column::sql`) + the
  `shim_udf_boilerplate!` macro every shim `ScalarUDFImpl` shares.
- `random.rs` — **r20 G2:** Spark `XORShiftRandom` + MurmurHash3 `hashSeed`; `rand`/`randn`
  ScalarUDFs (seed + partitionIndex=0; sequential within batch). Pins: first `rand(0)` value,
  sampleBy seed-0 count band.
- `analyzer.rs` — `SparkExprSemantics` (AR-WG-SQL, audit findings #1/#5/#16; Group L 2026-07-23: BUG-004 disproved — the `get_type` bail is unreachable, correlated `int/int` already promotes to double; the real bug — the single-analyze CTAS write-schema path — is FIXED in Group L-write, `repark_sql::execute_ctas` re-analyzes to the fixpoint): integer `/` →
  always-double division; division/modulo-by-zero → NULL (Spark **non-ANSI**, the recorded
  decision); `[]` array subscript → 0-based with invalid-index → NULL (rewrites the planner's
  `array_element` onto the embedded `__repark_array_get__` UDF); swaps planner-embedded built-in
  `substr` nodes onto the Spark shim (the `SUBSTRING` special form bypasses the registry);
  **F2 octo C1:** `overlay(..., -1)` literal 4th arg dropped to 3-arg (Spark replace-length;
  pin `overlay_len_minus_one_matches_three_arg`);
  **TZ-5 (2026-08-12):** `CAST(TIMESTAMP AS <numeric>)` → epoch SECONDS
  (`rewrite_timestamp_to_numeric_cast` + `epoch_seconds_for_target`), pushing the scaling UNDER
  the user's cast onto `timestamp_cast.rs`'s two embedded UDFs so the outer cast still applies the
  requested width — the rewrite owns the *scale*, never the cast-FAILURE surface. Matched on the
  SOURCE type, which is what makes it idempotent (its own output casts an `Int64`/`Float64`); the
  reverse direction `CAST(<integer> AS TIMESTAMP)` was probed and is already correct, so it is
  pinned as a fence rather than rewritten.
  **B-TZ-4 (2026-08-13):** `CAST(TIMESTAMP AS STRING)` →
  `__repark_timestamp_to_string__` via `rewrite_timestamp_to_string_cast` (Utf8, not Utf8View).
  **TZ-8 (2026-08-14):** `CAST(TIMESTAMP AS DATE)` →
  `__repark_timestamp_to_date__` via `rewrite_timestamp_to_date_cast` (session-zone Date32
  for LTZ; stored wall for NTZ). `datediff` rides CAST. `last_day`/`date_add`
  over TIMESTAMP stay residual. Runs
  after the built-in analyzer rules (sees type-coerced plans, emits exactly-typed expressions,
  recomputes every node schema); every rewrite is **idempotent** — the passthrough analyzes
  eagerly and physical planning analyzes again. NB (Group L-write): running after `TypeCoercion`
  means ONE analyze is not always a whole-plan fixpoint — a division under a `UNION` reaches
  `Float64` only on the SECOND analyze, so single-analyze *schema* consumers (CTAS) must re-analyze
  to the fixpoint. **Group L (2026-07-23, BUG-004):** the `get_type`
  bail is **defensive-only** — correlated / outer refs arrive as typed `OuterReferenceColumn`, so a
  correlated `int / int` already resolves and IS promoted to double (not truncated); `int / int` is
  cast to `Float64` **only when both operands are integer** so Spark's decimal `/` (result = decimal
  iff ≥1 operand decimal AND none float; precision a documented gap) is preserved. Parity/regression
  matrix pinned (correlated by analyzed-plan + executable, decimal-mix TYPE class, wide/narrow int),
  mutation-proven both ways. Known limitation (filed): a parent op type-coerced over a pre-rewrite
  int `/` (e.g. `(a/b) > 1`) is not re-coerced → loud `Float64 > Int64` at execution.
- **r24 A3 PERF-02:** `datetime.rs` `date_format` compiles the Java pattern once per
  invocation (`compile_java_pattern` + per-row `format_compiled_java_pattern`; scalar pattern
  cache). Zero behavior change; `date_format_matches_spark_on_the_dim_dates_patterns` is the net.
- **r24 A3 PERF-03:** `string.rs` `spark_substring` uses `char_indices` byte offsets +
  sized `StringBuilder` (no per-cell `Vec<char>`); edge/null/multibyte pins hold.
- **octo C1-Q-004:** `perf_measure_date_format_compile_once` /
  `perf_measure_substring_char_indices` gated on `REPARK_PERF_MEASURE=1` (not default suite tax).
- **octo C2-Q-001:** `compile_java_pattern` apostrophe/punct edges + unterminated-quote Err pin.
- `string.rs` — `SparkSubstring` (`substring`, alias `substr`; audit #6): Spark's
  `UTF8String.substringSQL` character-based semantics — pos 0 acts as 1, negative pos counts
  from the end, the window clips (never errors), negative len → `''`, NULL args → NULL.
  **D2 `SparkConcat` (`concat`)** overwrites `datafusion-spark`'s: coerce args to `Utf8`
  (incl. non-string Spark stringify), any-NULL → NULL, always emit `Utf8` (never `Utf8View`)
  — closes the plan-promises-Utf8 / kernel-returns-Utf8View panic that blocked TPC-DS
  Q5/Q80/Q84 SQL `concat` on the Arrow path. Unit pins:
  `concat_register_all_overwrites_datafusion_spark` (name overwrite);
  `concat_array_any_null_propagates_per_row` (Apply null-mask path).
- `instant_ts.rs` — **TZ-4 PR-1+PR-2:** overwrite `now` / `current_timestamp` / `to_timestamp`
  with Arrow `Timestamp(µs, UTC)`. PR-2 localizes zoneless LTZ inputs (`TIMESTAMP '…'`,
  zoneless `to_timestamp`, `CAST(str|date|ntz AS TIMESTAMP)`) in the session zone; a
  zone-suffixed string is not localized. Analyzer rule `spark_ltz_timestamp_cast` still wraps
  integer `CAST AS TIMESTAMP`. **Q10:** when `spark.sql.timestampType=TIMESTAMP_NTZ` the
  same rule resolves bare `TIMESTAMP` literals / casts to naive µs (no localization);
  `to_timestamp` / `now` stay LTZ. Pins: `instant_ts::tests::*`.
- `timestamp_cast.rs` — **TZ-5 (2026-08-12)** plus **B-TZ-4 (2026-08-13):** the embedded UDFs
  `analyzer.rs` puts under timestamp casts. `__repark_epoch_seconds_floor__` (→ `Int64`) serves
  integer targets with exact `div_euclid` **floor** — Spark uses `Math.floorDiv`, so `-0.5 s` is
  `-1` and `-1.25 s` is `-2`; truncation toward zero agrees on every positive instant and every
  whole negative second, so a negative FRACTIONAL second is the only input that catches it.
  `__repark_epoch_seconds_real__` (→ `Float64`) serves `DOUBLE`/`FLOAT`/`DECIMAL`, which keep the
  fraction (Spark computes its own decimal cast through a double). Two UDFs and not one is a
  correctness requirement: a decimal intermediate loses the floor edge to arrow's truncating
  decimal→int cast, and an f64 one cannot floor a sub-microsecond present-day instant (f64 resolves
  ~2e-7 s there). Per-`TimeUnit` divisor (`createDataFrame` gives `timestamp[us]`, `to_timestamp`
  gives `timestamp[us, tz=UTC]` after TZ-4 PR-1); nullability propagated via
  `return_field_from_args`. **B-TZ-4:** `__repark_timestamp_to_string__` (→ `Utf8`,
  `Volatility::Volatile`) renders Spark's space-separated session-zone wall for LTZ and the
  stored wall for NTZ; trailing-zero fractions are stripped (recorded: `.123400` → `.1234`).
  Embedded, never registered. Pins: `epoch_seconds_floor_is_floor_not_truncation` and siblings,
  plus `spark_timestamp_string_trims_trailing_fraction_zeros` / year-shape / LTZ-vs-NTZ here;
  facade corpus `test_timestamp_cast_parity.py`. **TZ-8:** `__repark_timestamp_to_date__`
  (embedded CAST) + registered `to_date` overwrite share `datetime::invoke_local_dates`.
  Pin `ltz_date_is_session_zone_and_ntz_is_stored_wall`. Ledgers:
  `task/tz5-cast-seconds-ledger.md` §4, `task/v3-btz4-ledger.md`, `task/r4-tz8-ledger.md`.
- `collection.rs` — `SparkElementAt` (`element_at`; audit #15 — previously an alias of
  `map_extract`, broken on every array): arrays are 1-based / negative-from-end / OOB → NULL
  with index 0 → error (Spark `INVALID_INDEX_OF_ZERO`); maps return the plain value-or-NULL
  (`map_extract` unwrapped through `array_element`). Also `SparkArrayGet`
  (`__repark_array_get__`) — the embedded (never registered) `[]` subscript UDF the analyzer
  swaps in. **E1 octo C2:** `SparkGetItem` / `spark_get_item_udf` (`__repark_get_item__`) —
  polymorphic array 0-based or map-by-key for facade `Column.__getitem__` Column/other keys
  (never fail-open to parent container).
- `datetime.rs` — Spark calendar date shim. `DatePartUdf` (generic `date_part`-backed extractor with
  a Spark indexing offset) covers `year`/`month`/`dayofmonth`/`day`/`dayofyear`/`quarter`/`weekofyear`/
  `yearofweek`/`dayofweek`/`weekday`; `MakeDate` builds `Date32` from three `Int64` columns. WG2 added
  the calendar-math shims: `AddMonths` (Spark end-of-month-preserving `add_months`), `TruncDate`
  (`trunc(date, fmt)` → `Date32`; invalid fmt → NULL, so `'Q'` is NULL not `QUARTER`), `DateTrunc`
  (`date_trunc(fmt, ts)` → µs `Timestamp`, format-first arg order, overrides DataFusion's native
  `date_trunc`), `DateFormat` (Java-pattern → `Utf8`; `format_java_pattern` handles quoted literals +
  `y M L d D q Q E H m s` — unsupported letters raise). Each exposed via a named `*_udf()` constructor.
  Inputs are coerced by `coerce_date_arg` / `coerce_to_date32` / `coerce_to_timestamp_micros`
  (`user_defined` signature): date / timestamp (any unit+zone) / string — matching Spark. Tests run
  the UDFs through a real `SessionContext` against ISO-8601 / Spark goldens.
  **H-1a split B (2026-08-10) — the SESSION TIMEZONE.** Every calendar field of a `TIMESTAMP` now
  resolves in `spark.sql.session.timeZone`, as Spark's does. The coercion path carries the whole
  decision: a `Timestamp` of any unit/zone (including **none**) is an INSTANT and is coerced to a
  `UTC`-annotated timestamp, which is what marks it for session-zone resolution; `Date32`/`Time*`/
  string arguments keep zone-free types and therefore never move. `DatePartUdf` re-annotates the
  instant into the session zone (metadata only) before `date_part`; `DateTrunc` and `DateFormat`
  go through `invoke_local_micros`, which is the ONE place that decides what "local" means for
  them (and, for `date_trunc`, puts the truncated local time back on the timeline with
  `java.time`'s DST rules — earliest offset when ambiguous, pushed forward across a gap). The zone
  is read at **invoke** time from `ScalarFunctionArgs::config_options` (`session_time_zone.rs`),
  not baked in at registration, because `expr_fn` embeds a UDF into a standalone `Expr` with no
  session — the DataFrame-API entry point would otherwise be missed. `coerce_types` MUST stay
  idempotent (DataFusion re-analyzes at physical planning); pinned by
  `coercion_is_idempotent_so_a_second_analysis_cannot_promote_a_date`. TZ-4 PR-1: `date_trunc`'s
  output is `Timestamp(µs, UTC)`.
  **r20 A1:** SAF-001 out-of-chrono Date32 → NULL in `add_months`/`trunc` (pins
  `extreme_date32_add_months_and_trunc_null_without_panic`,
  `chrono_boundary_date32_add_months_computes`, `extreme_date32_year_extractor_no_panic`);
  SAF-002 downcast evidence + defensive `cast` before `as_primitive`/`as_string` (pin
  `trunc_accepts_large_utf8_format_without_panic`).
- `expr_fn.rs` — logical-`Expr` builders (`year`, `month`, `quarter`, `weekofyear`, `dayofweek`,
  `weekday` (0=Monday; Group I facade wire-up), `dayofmonth`, `dayofyear`, `last_day`,
  `add_months`, `date_add`, `date_format`, `trunc`, `date_trunc`, **TZ-8 `to_date`**) that embed
  the UDF instance directly, so `repark-python`'s `PyColumn` gets a self-contained date-function
  expression without a `SessionContext`. Extractors + calendar-math come from `datetime`;
  `to_date` from `timestamp_cast`; `date_add` (days cast to `Int32`) + `last_day` from
  `datafusion-spark`.

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| `year`/`hour`/`date_trunc` ignore `spark.sql.session.timeZone` | The carrier is not on the session. Only `SparkExtension::configure` installs it; a bare DataFusion context falls back to `UTC`. See `session_time_zone/map.md`. |
| A `DATE` shifted by a day under a non-UTC session | A coercion arm stopped being idempotent, so a second analysis pass promoted the date to an instant. That exact bug shipped in a draft of this fix and is pinned by `coercion_is_idempotent_so_a_second_analysis_cannot_promote_a_date` + `crates/repark-spark/tests/session_timezone.rs::date_arguments_never_move_with_the_session_zone`. |
| EVERY IANA zone id fails at query time but `+05:30` works | The `chrono-tz` feature on this crate's `arrow` dependency is gone. It is DECLARED in `Cargo.toml` for exactly this reason — re-declare it there rather than relying on `datafusion`'s feature graph. |
| `date_trunc` returns the right instant with the wrong-looking wall clock | Expected if the viewer ignores the UTC annotation: ticks are Spark's instant. After TZ-4 PR-1 the type is `timestamp[us, tz=UTC]`. |

First checks: `cargo test -p repark-functions`. Escalate to: [../map.md#debug](../map.md).

## DF 54.1 note (2026-08-01)
as_any trait methods removed (DF54 trait upcasting); Cast uses field-aware API where touched.

<!-- 2026-08-02: r16 combine rider — doc-markdown backtick fix in datetime.rs test doc (workspace clippy runs what unit-scoped octo gates missed) -->

<!-- 2026-08-04 (r24 combine rider): doc backticks (`arrow_cast`, `char_indices`) +
  #[allow(clippy::cast_precision_loss)] on the PERF-02/PERF-03 ns/row measurement tests
  (report-only arithmetic; repo convention is the narrowest allow). -->

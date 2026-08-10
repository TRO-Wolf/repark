# map — repark-functions/src

## Purpose

Source for `repark-functions` — Spark function registry, the function shims (date / string /
collection), and the Spark expression-semantics analyzer rule. See [../map.md](../map.md).

## Contents

- `cardinality.rs` — r24 SB1 SEC-01 ceilings + SEC-02 conf extension (const
  CAST / arithmetic / abs / coalesce / greatest / least / nullif / CASE /
  arrow_cast / utf8→int / float math / log*/exp/sqrt / bitwise / trivial scalar-subquery fold; depth-bounded)


- **R-FN-BATCH4** aggregate expansion.

- **Q1 R-ML-QUANTILE:** `register_all` re-registers `approx_percentile_cont` with Spark SQL
  aliases `percentile_approx` / `approx_percentile` (`AggregateUDF::with_aliases`); unit pin
  `percentile_approx_sql_aliases_resolve` in `aggregate.rs`.

- **R-FN-BATCH3:** expr_fn next_day/hour/minute/second; **X1-octo C3:** hour/minute/second are
  repark DatePartUdf (Time+Timestamp), overwriting datafusion-spark Timestamp-only.


- `aggregate.rs` / R-RETRACT-SHIM: Float64 `avg` with `retract_batch` (overwrites SparkAvg; sliding windows).

- `lib.rs` — `register_all(ctx)` (datafusion-spark's full set, then the date + string + collection
  + **r20 G2** `random` (Spark XORShift `rand`/`randn`/`random`) shims — later registration wins a
  name clash) + Q1 percentile aliases + `spark_date_shim_functions()` +
  `analyzer_rules()` (the Spark semantics rules the session installs on every context via the
  Spark door's `SessionExtension` in `repark-spark`; error conversion one layer up is
  `repark-core`) +
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
  pin `overlay_len_minus_one_matches_three_arg`). Runs
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
  **r20 A1:** SAF-001 out-of-chrono Date32 → NULL in `add_months`/`trunc` (pins
  `extreme_date32_add_months_and_trunc_null_without_panic`,
  `chrono_boundary_date32_add_months_computes`, `extreme_date32_year_extractor_no_panic`);
  SAF-002 downcast evidence + defensive `cast` before `as_primitive`/`as_string` (pin
  `trunc_accepts_large_utf8_format_without_panic`).
- `expr_fn.rs` — logical-`Expr` builders (`year`, `month`, `quarter`, `weekofyear`, `dayofweek`,
  `weekday` (0=Monday; Group I facade wire-up), `dayofmonth`, `dayofyear`, `last_day`,
  `add_months`, `date_add`, `date_format`, `trunc`, `date_trunc`) that embed the UDF instance
  directly, so `repark-python`'s `PyColumn` gets a self-contained date-function expression without
  a `SessionContext`. Extractors + calendar-math come from `datetime`; `date_add` (days cast to
  `Int32`) + `last_day` from `datafusion-spark`.

## Pointers

- Up: [../map.md](../map.md)

## Debug

First checks: `cargo test -p repark-functions`. Escalate to: [../map.md#debug](../map.md).

## DF 54.1 note (2026-08-01)
as_any trait methods removed (DF54 trait upcasting); Cast uses field-aware API where touched.

<!-- 2026-08-02: r16 combine rider — doc-markdown backtick fix in datetime.rs test doc (workspace clippy runs what unit-scoped octo gates missed) -->

<!-- 2026-08-04 (r24 combine rider): doc backticks (`arrow_cast`, `char_indices`) +
  #[allow(clippy::cast_precision_loss)] on the PERF-02/PERF-03 ns/row measurement tests
  (report-only arithmetic; repo convention is the narrowest allow). -->

# map — repark-functions/src

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

Source for `repark-functions` — Spark function registry, the function shims (date / string /
collection), and the Spark expression-semantics analyzer rule. See [../map.md](../map.md).
Source documentation may retain model provenance; code-quality grade tags stay outside code.

Child modules use Rust's default layout: `str_to_map`, `shuffle`, and `map_from_entries` live under
[`collection/`](collection/map.md), `java_uri` lives under [`url/`](url/map.md), Spark
higher-order kernels live under [`higher_order/`](higher_order/map.md), and FNP-7 `try_*`
scalars live under [`try_invert/`](try_invert/map.md).

## Contents

- `declared_refuse.rs` — FNP-15/16 parse-altitude refusals for Spark function names this
  engine will not build. Spark door and `F.expr` / `filter_sql` call `refuse_in_statement` /
  `refuse_in_sql`. FNP-15 names are unreachable; FNP-16 sketches (32) are armed as
  deferred-by-cost; CSV/XML/XPath (11), VARIANT (8), and geospatial (5) likewise.
  `armed_names()` is 62.
  pins: fnp-15-16/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-013
- `spark_length.rs` — **GT1-FIX G5 / A3 / R3-1:** Spark `bit_length` /
  `octet_length`. Stringifies non-binary; BINARY pass-through (including
  Dictionary(_, Binary)); refuses ARRAY/STRUCT/MAP; decimal scale-padded
  stringify. Ledger: `task/fn-gt1-ledger.md`.
- `spark_log.rs` — **SEM-1 (2026-08-31):** Spark-door `log`. Owner ruling 2026-08-31
  fixes RE-1 and LOG-1 to Spark. One-arg is the natural log;
  two-arg is `log(base, expr)`. Both arities return NULL on Spark's domain edges (zero,
  negative, null, base <= 0). Overwrites DataFusion's base-10 `LogFunc` from `register_all`.
  Native ANSI `repark.sql()` does not load this kernel.
- `spark_isnan.rs` — **FN-FIX-1 (2026-09-03):** Spark `isnan`; NULL → false, non-nullable bool.
  pins: fn-fix-1-registry-rows/C-002
- `spark_initcap.rs` — **FN-FIX-2 (2026-09-04):** Spark `initcap`; a word starts only after
  SPACE (U+0020). `'a-b'` → `'A-b'`. pins: fn-fix-2-string-rows/C-001, C-002, C-004
- `spark_chr.rs` — **FN-FIX-2 (2026-09-04):** Spark `chr` / `char`; `n % 256`, `''` when
  `n < 0`. pins: fn-fix-2-string-rows/C-002
- `spark_elt.rs` — **FN-FIX-2 (2026-09-04):** Spark `elt`; ANSI out-of-range raises
  `INVALID_ARRAY_INDEX`; NULL `n` is NULL. pins: fn-fix-2-string-rows/C-002
- `spark_result_types.rs` (+ `spark_result_types/tests.rs`) — **TYPES-1 (2026-09-05):**
  `SparkIntegerLiteral` narrows in-range `Int64` literals to `Int32` (first in
  `analyzer_rules()`, after DataFusion's own `TypeCoercion`; `LIMIT` fetch/skip stay `Int64`
  for the physical planner), `SignedAggregate` casts `regr_count`/`approx_distinct` to
  `Int64`, `SignedWindow` casts the rank family to `Int32`.
  pins: types-1/C-001, C-003, C-005, C-007
- `count_if.rs` — **TYPES-1 (2026-09-05):** SQL-door `count_if` aggregate UDF answering
  `Int64`. pins: types-1/C-003
- `spark_from_unixtime.rs` — **TYPES-1 (2026-09-05):** SQL-door `from_unixtime`
  overwriting scalar UDF answering session-zone STRING, reusing the `date_format` pattern
  compiler; 1- and 2-arg shapes; always nullable (Spark marks `FromUnixTime` nullable
  even for non-null input — live-measured on 4.1.2). pins: types-1/C-006
- `spark_year_pad.rs` — **TYPES-1 round 5 (2026-09-05):** the Java-pattern year arm
  extracted from `datetime.rs` (`datetime.rs` 1709→1700): negative years pad the digits
  and re-attach the sign (`-0499`), `yy` is `abs(year) % 100` (`-499` → `99`), 5+-digit
  positives keep `+` — all live-measured on 4.1.2. pins: types-1/C-006
- `java_regex.rs` — **FN-FIX-2 (2026-09-04):** Java nested character-class union. `[[:alpha:]]`
  is `{':','a','l','p','h'}`, not POSIX alpha. pins: fn-fix-2-string-rows/C-002
- `spark_regexp_match.rs` — **FN-FIX-2 (2026-09-04):** `regexp_like` / `rlike` /
  `regexp_replace` compile through `compile_spark_regex`. pins: fn-fix-2-string-rows/C-002
- `percentile_approx.rs` — **FN-FIX-1 (2026-09-03):** discrete `percentile_approx` /
  `approx_percentile` in the column's type; array-of-percentages arm;
  `select_nth_unstable` per requested percentile. Accuracy knob ignored
  (`FN-APPROXPCT-ACC-1`). Group held in memory (`PERF-APPROXPCT-1`).
  Flatten offset-buffer rewrite lives in `collection/flatten.rs`.
  pins: fn-fix-1-registry-rows/C-002
  **PERF-APPROXPCT-1 (2026-09-05):** the buffer is a Greenwald-Khanna
  `QuantileSummaries` (new `quantile_summaries.rs`), a line-faithful port of Spark 4.1's
  insert/compress/merge/query (same thresholds, delta rule, backwards merge test,
  Long-division target error, edge rules); state is the serialized summary plus the
  percentages list. Accuracy is honored (default 10000): literals extract at accumulator
  build so a bad knob fails before any row moves, non-literals fall back to first-row-wins
  in `update_batch`. A NULL array element reads as 0.0 (Spark's `toDoubleArray` unboxes
  null to 0.0 — measured on live 4.1.2, not guessed); an empty array answers NULL. Answers
  are inserted doubles cast back, so the `as` int casts never meet an out-of-range value.
  pins: perf-approxpct-1/C-001
  **Round 2 (2026-09-06):** `merge_batch` stages partial summaries and folds once in
  `evaluate`/`state`, sorted by serialized bytes — the final task sees one partial per
  call in arrival order (16 calls at 1e6), so a within-call sort fixed nothing and one
  session answered 5 distinct values. The fold is order-free given the partial set; the
  sketch arithmetic is untouched. Empty partials skip (a merge no-op).
  pins: perf-approxpct-1/C-004
  **Round 3 (2026-09-06):** `return_type` plan-refuses a non-integral accuracy with
  Spark's `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` wording (INTEGRAL / SQLSTATE
  42K09 / BOOLEAN·DOUBLE·STRING·DECIMAL). Structured sqlExpr/inputSql params are
  not in the DataFusion `Plan` error (`FN-APPROXPCT-ACC-TYPE-1`).
  pins: perf-approxpct-1/C-002
- `quantile_summaries.rs` — the sketch behind `percentile_approx.rs` (see that row). Merge
  takes the other side by reference and stages a flushed clone, so the Digest discipline
  (both sides compressed before merge) holds with no panic path. The head sort uses
  `total_cmp` (stable, like Spark's TimSort) while the merge loops use IEEE `<`/`<=`
  exactly as the Scala source does; f64↔decimal crosses exactly on the shortest repr and
  quantizes HALF_UP to scale where the repr runs long. `threshold_value_serializes` pins
  the 10000/50000 constants Spark's `QuantileSummaries` object carries. The edge test pins
  the clamp sides on n=200 at exactly eps, and the eager test pins mid-insert compression
  with no query call; all four boundary mutations die (ledger §5).
  pins: perf-approxpct-1/C-001 perf-approxpct-1/C-006
  **Round 2 (2026-09-06):** `accuracy_ten_and_hundred_pin_single_scan_answers` pins the
  acc10/acc100 single-scan medians (90.0/99.0) and `state_size_follows_one_over_eps` pins
  the 1e6-row serialized sizes (952656/4776/72 B at acc 10000/100/2); the merge-threshold
  ×4.0 mutant dies on exactly these two and survives the older seventeen.
  pins: perf-approxpct-1/C-006
- `spark_log1p.rs` — **LOG1P-1 (2026-09-02):** Spark-named `log1p` / `expm1` kernels
  (`f64::ln_1p` / `f64::exp_m1` via Arrow `unary`; `log1p` then `nullif` on `x <= -1`).
  Numeric coerce to Float64; NULL in → NULL out. Registered from `register_all`
  (Spark door) and `spark_log1p::register` (ANSI door + native `PyReparkSession::native`).
  pins: log1p-1-precise-kernels/C-002
  **Critic remediation (2026-09-01):** `null_slots_null_out_even_when_the_buffer_holds_a_live_value`
  builds a null slot whose underlying buffer value is 5.0 (both arities) and asserts NULL —
  SQL-door null pins cannot see the null arms because a built null slot reads back 0.0 and the
  domain guard masks it.
  pins: sem-1-spark-answer-parity/C-001, C-004, C-007, C-009
- `spark_regexp.rs` — **GT1-FIX A1/A2 / R3 / R4-1:** Spark `regexp_count` /
  `regexp_instr` (Java find-loop; positional mid-surrogate probe, not
  `is_match("")`; Dictionary(_, Utf8) coerce). **FN-FIX-2:** `compile_spark_regex`
  translates Java nested classes before `bind_ascii_perl_classes`.
  pins: fn-fix-2-string-rows/C-002
  **FN-REGEXP-EXTRACT-1 (2026-09-04):** Spark `regexp_extract` (first match's
  group, `''` on no match, any-arg-nullable; `validate_group_index` names the
  caller; round 2: validation runs only inside the match arm — non-matching
  input answers `''` for any idx; the bad-group Rust test uses matching input
  `'a-b'`). pins: fn-regexp-extract-1/C-001
  **SEM-4 (2026-08-21):**
  `validate_group_index` carries Spark's `REGEX_GROUP_INDEX` condition (one
  message for negative and over-large alike); `extract_rows` passes the index
  raw because the bound needs the compiled regex; `coerce_regexp_args` takes
  the caller's name. Ledger: `task/sem-4-regexp-messages-ledger.md`. **SEM-1
  (2026-08-21):** the two-argument default is capture group **1**, Spark's,
  not the whole match — one knob for both doors, closing registry row `RE-1`.
  Ledger: `task/sem-1-extract-all-group-default-ledger.md`. **SEM-6 (2026-08-21):**
  `invoke_substr` returns NULL for a ZERO-WIDTH match, not `''` — Spark takes
  the first match and nulls it when empty, closing registry row `RE-3`.
  Ledger: `task/sem-6-substr-zero-width-null-ledger.md`.
- `spark_split_part.rs` — **GT1-FIX F-6c / R3-1:** STRING `partNum` +
  Dictionary(_, Utf8); partNum 0 fail-loud.
- `higher_order/` — FNP-4c Spark higher-order kernels (`transform`, `filter`, `forall`,
  `aggregate`/`reduce`, `zip_with`, `transform_keys`, `transform_values`, `map_filter`,
  `map_zip_with`) plus the FNP-4a `exists` alias of `array_any_match`. Registry both doors
  read. pins: fnp-4c-higher-order-kernels/C-001, C-002, C-003, C-004, C-005, C-006, C-007,
  C-008, C-009, C-010, C-011, C-013, C-014
- `try_invert/` — FNP-7a/7b scalar `try_*` kernels (NULL instead of raise). `try_element_at`
  aliases `element_at`. `try_sum` reuses datafusion-spark; `try_avg` is its own UDAF
  (decimal overflow NULL; INTERVAL input is the FNP-11 loud refuse).
  pins: fnp-7-try-inversions/C-001, C-002, C-004, C-005, C-006, C-007, C-008, C-009, C-010,
  C-011, C-012, C-014, C-015, C-018, C-019
- `lib.rs` — crate-root stays at **175** under `check_lib_rs` with `pub mod timestamp_type`.
- `timestamp_type.rs` — **Q10:** Spark-door `spark.sql.timestampType` carrier
  (`SparkTimestampTypeConfig`, `PREFIX = repark.timestamp`, default
  **TIMESTAMP_LTZ**). Parsed from the builder map in `SparkExtension::configure`.
  Missing carrier also defaults LTZ. Invalid value
  fail-louds naming `TIMESTAMP_LTZ` and `TIMESTAMP_NTZ`. Ledger:
  `task/q10-timestamptype-ledger.md`.
- `ansi.rs` — **U5 / Q10=A:** Spark-door `spark.sql.ansi.enabled` carrier
  (`SparkAnsiConfig`, `PREFIX = repark.ansi`, default **TRUE**) + the embedded
  `__repark_ansi_nonzero_divisor__` raise kernel. Parsed from the builder map in
  `SparkExtension::configure`. Missing carrier also defaults TRUE. `notabool`
  fail-louds with Spark's `should be boolean, but was` needle
  (`DataFusionError::Configuration`; IllegalArgument class is a named residue —
  `engine_err` never emits `Error::Config`). Ledger: `task/s1-ansi-knob-u5-ledger.md`.
- `session_time_zone.rs` (+ `session_time_zone/`) — the carrier that brings the
  resolved session timezone to the extractors. A `ConfigExtension` with a two-segment `PREFIX`
  (`repark.session`), a `set` that always refuses naming `spark.sql.session.timeZone`, and empty
  `entries()` — so it is a channel, never a second spelling of the knob. Filled by
  `repark-spark`'s `SparkExtension::configure` (the only crate depending on both the engine that
  owns the key and this leaf); read by `datetime.rs` at invoke. The empty `entries()` also erases
  the zone from `ScalarFunctionExpr` equality (DataFusion 54.1 compares sorted config entries), so
  two identical extractor expressions built under different session zones compare EQUAL — safe only
  while no plan cache or cross-session expression reuse exists, and stated in the module doc beside
  the rationale rather than left to be rediscovered.
- `datetime.rs` — session-zone semantics are type-driven (`coerce_date_arg` /
  `coerce_to_timestamp_micros` /
  `coerce_to_date32`: `Timestamp(_, Some(_))` is an LTZ instant; `Timestamp(_, None)` is NTZ
  and stays naive; `Date32`/`Time`/string keep zone-free types; every arm is a fixed point because
  DataFusion re-analyzes at physical planning. Zoneless LTZ inputs are localized by `instant_ts.rs`.
  `LocalSource` distinguishes instants from wall clocks at invoke; `date_trunc`/`date_format` use
  `invoke_local_micros`, while `trunc`/`add_months` use `invoke_local_dates`. Overlaps prefer the
  source instant's offset; gaps use the bounded `offset_before_gap` resolver.
- `cardinality.rs` — r24 SB1 SEC-01 ceilings + SEC-02 conf extension (const
  CAST / arithmetic / abs / coalesce / greatest / least / nullif / CASE /
  arrow_cast / utf8→int / float math / log*/exp/sqrt / bitwise / trivial scalar-subquery fold; depth-bounded).
  **V3-2:** `allowCreateFormatVersion3` (default false) + `resolve_create_format_version`
  (`Model: Grok 4.6 xHigh`). **V3-9 (2026-09-02):** the refusal dropped its parenthetical
  "v3 tables cannot yet do merge-on-read row-level writes" — false since the `V3-MOR-1` lift;
  it names the conf and the v2 default only. pins: v3-9-mor-predicate-dml-dv/C-006


- **R-FN-BATCH4** aggregate expansion.

- **FN-FIX-1:** `percentile_approx` / `approx_percentile` are the discrete UDAF
  (`src/percentile_approx.rs`); `approx_percentile_cont` stays t-digest for ML.
  Unit pin `percentile_approx_sql_aliases_resolve` in `aggregate.rs`.

- **R-FN-BATCH3:** expr_fn next_day/hour/minute/second; **X1-octo C3:** hour/minute/second are
  repark DatePartUdf (Time+Timestamp), overwriting datafusion-spark Timestamp-only.


- `aggregate.rs` / R-RETRACT-SHIM + **DEC-5 / Z-3 U1:** Float64 `avg` retract (X2) plus
  Spark-typed decimal `avg` with decimal `retract_batch` (no Numeric→Float64 coerce).
  **W-2 U2 ride-along:** Decimal32/64/256 accumulator arms have revert-red pins
  (`group_avg_decimal32_stays_decimal_9_6_i32` and siblings).
  **TYPES-1 (2026-09-05):** registers `count_if` plus the `SignedAggregate` wrappers
  (`regr_count`, `approx_distinct` → `Int64`). pins: types-1/C-003
- `aggregate.rs` also serves `groups_accumulator_supported` /
  `create_groups_accumulator` (**PERF-AGG-AVG-1**), delegating to `avg_groups.rs`; the
  `Accumulator` arms and `state_fields` are untouched, so window frames keep the retract
  path, and the decimal `DecimalAverager` is shared as `pub(crate)`. Round 2 removed
  the dead `is_distinct` early return in `create_groups_accumulator` (unreachable:
  the only caller is guarded by `groups_accumulator_supported`).
  pins: perf-agg-avg-1/C-001, C-002
- `avg_groups.rs` — **PERF-AGG-AVG-1 (2026-09-05):** the
  `GroupsAccumulator` for the Spark `avg` / `try_avg` UDAF, shaped on DataFusion 54.1's
  own `AvgGroupsAccumulator` (`datafusion-functions-aggregate/src/average.rs`):
  per-group `Vec<u64>` counts plus `Vec<Native>` sums, `EmitTo::First` partial emission,
  `convert_to_state`, and a local null-state tracker (the
  `datafusion-functions-aggregate-common` `NullState` cannot be used without a new
  dependency, so its All/Some fast-path semantics are re-implemented here). Three
  deliberate deviations from the reference, each forced by a house contract: the average
  closure returns `Option` so 2×-MAX `try_avg` overflow yields per-group NULL instead
  of failing the query (the sum-wrap shape is BACKLOG `AVG-DEC-SUMWRAP-1`); the
  float state stays `[sum, count:Int64]` and the decimal state
  `[count:UInt64, sum]` because `state_fields` belongs to the untouched retract path;
  length mismatches are loud `exec_err!` instead of the reference's `assert_eq!`, and
  indexing is bounds-checked instead of `get_unchecked` (`unsafe` is forbidden in this
  crate). Unit tests drive `update_batch` / `merge_batch` / `evaluate` / `state` /
  `size` directly with `EmitTo::First`, the way DataFusion's own groups-accumulator
  tests do; the decimal merge test merges three groups with distinct partials
  across two states and asserts every group, so a decimal-arm index scramble reds
  exactly it.
  pins: perf-agg-avg-1/C-001
- `groups_null_state.rs` — **PERF-AGG-AVG-1 (2026-09-05):** the groups accumulator's
  per-group validity tracker, split out of `avg_groups.rs` so neither file passes the
  1000-line ceiling. It re-implements the `datafusion-functions-aggregate-common`
  `NullState` All/Some fast-path semantics (no new dependency allowed): batches with no
  nulls and no filter skip tracking entirely, otherwise one validity bit per group with
  `EmitTo::First` splitting the mask. The unit test pins the split.
  pins: perf-agg-avg-1/C-001

- `decimal_precision.rs` — **V-2 / DEC U3+U4a:** `SparkDecimalPrecision` analyzer rule.
  U3: integer-literal `fromLiteral` (`DECIMAL(digits,0)`) on `+ − *` only (typed INT
  columns untouched). U4a: CAST-after add/sub/mul clamp (`allowPrecisionLoss=true`).
  `/` formula, DEC-8, and DEC-6 live in `decimal_spark.rs`. Inserted **second** in
  `analyzer_rules()` (after `SparkIntegerLiteral`, before `SparkDecimalRewrite` then
  `SparkExprSemantics`). **TYPES-1 (2026-09-05):** the default-cast check accepts the
  narrowed `(20,0)`-over-`Int32` shape and keeps `(10,0)`-over-`Int32` user casts declared.
  Ledger: `task/v2-dec-u3u4-ledger.md`.
  pins: types-1/C-002
- `decimal_spark.rs` — **R-2:** `SparkDecimalRewrite` (A5 slot: clean `decimal / decimal`
  before `SparkExprSemantics`; UDF owns `/0`) + `SparkDecimalExprPlanner` (DEC-8
  compute-with-clamp) + checked `+`/`−` (DEC-6, reads `SparkAnsiConfig`). Registered
  from `lib.rs` (`analyzer_rules` + `register_all`). Ledger: `task/r2-dec-close-ledger.md`.
  **CUTOVER-SCHEMA-1 (2026-09-04):** the rule also carries Spark's decimal-cast
  nullability — `CAST(x AS DECIMAL)` with a non-null child wraps the child in the
  `__repark_decimal_cast_nullable__` identity UDF, whose `return_field_from_args`
  declares nullable. The UDF survives the optimizer (physical and analyzed schemas
  agree), the wrap is idempotent (a wrapped child reads nullable, so re-analysis
  skips it), and inner decimal arithmetic still rewrites on the way down. The branch
  sits after the U4a CAST-after stop, so `CAST(arith AS DECIMAL)` keeps its wrap and
  DEC-9 stays BACKLOG; INT/STRING targets are untouched. A `coalesce(x, NULL)` wrap
  was measured and rejected: it stays non-null when `x` is non-null.
  Round 3 (2026-09-05): the rule is nullable-iff-overflow-exposed
  (`decimal_cast::decimal_cast_can_overflow`: small ints under their digit bound and
  same-or-wider decimal targets stay non-null); the dead `Boolean` arm is gone —
  every `BOOLEAN → DECIMAL` cast refuses downstream, so the arm's answer was
  unobservable (registry `CAST-BOOL-DEC-1`).
  pins: cutover-schema-1/C-003
  **NULLABILITY-2 (2026-09-05):** the cast hook is generalized
  (`nullable_spark_cast`): string→{integral, float, boolean, date, timestamp},
  float→integral, timestamp→{int8, int16, int32}, and decimal→integral casts wrap
  the non-null child; decimal targets keep the overflow-exposure check. String
  literals that parse as the date/timestamp target are exempt — DataFusion plans
  `DATE 'x'` and the explicit spelling identically with no planner hook, so the
  exemption keeps typed literals Spark-equal and concedes explicit valid-literal
  casts (registry `CAST-NULL-1` residue). The `(38,·)` add/sub UDFs propagate
  operand nullability (division stays always-nullable). Round 4 (2026-09-06):
  `nonnull_spark_cast` is Date32 → Timestamp only; DataFusion already marks CAST
  of a non-null struct/list/map child non-null, so those wrap arms were deleted.
  pins: nullability-2/C-001, C-002
- `spark_nullability.rs` — **NULLABILITY-2 (2026-09-05):** the `SparkNullability`
  analyzer rule, slotted after `SparkDecimalRewrite`: date→timestamp casts and
  null-safe equal wrap their output in the non-null identity UDF
  (`__repark_spark_nonnull__`); decimal `+`/`-`/`*` (native and `(38,·)`-UDF
  forms) wrap their output in the nullable marker iff ANSI is off. Output wraps
  run transform-down with marker-stop (never descend into a marker, stop after a
  wrap) so re-analysis is a no-op — the idempotency `Column.sql` relies on.
  Round 2 (2026-09-06): `named_struct`/`struct`/`map`/`make_array` constructors
  mark non-null — Spark's top-level propagation, measured both ANSI modes.
  Round 4 (2026-09-06): the Struct/List/Map arms of `nonnull_spark_cast` are
  deleted. DataFusion's Cast nullability already follows the child for those
  types, so the wrap was tautological; `nonnull_spark_cast` remains Date32 →
  Timestamp only. The facade column-CAST pin still holds Spark-equal non-null
  struct CAST.
  pins: nullability-2/C-001, C-002, C-004
- `bool_decimal.rs` — **NULLABILITY-2 (2026-09-05):** the `BoolDecimalCast` analyzer
  rule, installed on BOTH doors via `install_shared_analyzer_rules` (the session The function carries no doc line by the comment rule; this row is its description: the analyzer rules both doors install (integer overflow, boolean-to-decimal casts).
  installer calls it in place of the integer-only one — same line count, so the
  session map needs no ratchet): `CAST(bool AS DECIMAL(p,s))` becomes a
  precision-carrying UDF (true → 1, false → 0 at scale; per-row nulls). The UDF
  reads ANSI once at plan time for the overflow edge (`(2,2)`: raise vs NULL);
  its return field marks overflow-exposed targets nullable. Registry
  `CAST-BOOL-DEC-1`.
  pins: nullability-2/C-003
- Integer `+ − *` overflow (**F-Y10-1 C-001**, measured 2026-08-30): same-width
  Int32/Int64 `BinaryExpr` wrapped via Arrow `arrow-arith`; `CAST(INT) + 1`
  widened to Int64 because DataFusion types a bare integer literal as Int64.
  pins: f-y10-1-int-overflow/C-001
  **TYPES-1 (2026-09-05)** retired the literal-width split below: pure-literal `1 + 1`
  is `Int32`, `2147483647 + 1` raises under ANSI and wraps when ANSI is off.
  pins: types-1/C-002
- `integer_spark.rs` — **F-Y10-1:** checked integer `+` / `-` / `*` UDFs that
  read `SparkAnsiConfig` (DEC U5 shape). `ansi=true` raises Spark's
  `ARITHMETIC_OVERFLOW`; `ansi=false` wraps at the source Arrow type. An
  `ExprPlanner` keeps `CAST(INT) + 1` as Int32 so TypeCoercion cannot widen it.
  **TYPES-1 (2026-09-05):** the planner leaves pure-literal pairs to the analyzer, which
  now sees narrowed literals — `1 + 1` is `Int32`, `2147483647 + 1` raises/wraps.
  SMALLINT/Int16 still Arrow-wraps (residue 2026-08-30; not this partition).
  Lambda-variable operands never arm (FNP-4c interaction; pin
  `lambda_variable_operands_do_not_arm`).
  Planner `Planned` results alias to the original BinaryExpr name so
  unaliased SQL does not leak `__repark_spark_int_*`. Post-remediation corpus
  pins i64 sub/mul raise, i32 MIN×−1 wrap, i64 CAST+lit wrap. `install_integer_overflow`
  is the ANSI-door hook. Ledger:
  `task/ledgers/staging/f-y10-1-int-overflow-ledger.md`.
  pins: f-y10-1-int-overflow/C-001, C-002, C-003, C-004, C-005
  (clippy implicit_clone: projection name uses `clone` on the field name)
- `lib.rs` — `register_all(ctx)` (datafusion-spark's full set, then the date + string + collection
  + **r20 G2** `random` (Spark XORShift `rand`/`randn`/`random`) shims + **SEM-1** `spark_log`
  (Spark-door natural `log`, dual-arity null-guard) + **LOG1P-1** `spark_log1p`
  (`log1p` / `expm1`) — later registration wins a
  name clash) + Q1 percentile aliases + `spark_date_shim_functions()` +
  `analyzer_rules()` (`SparkIntegerLiteral` → `SparkDecimalPrecision` →
  `SparkDecimalRewrite` → `SparkIntegerOverflow` → Spark semantics +
  cardinality + instant_ts + a closing `TypeCoercion` — the narrowing runs after
  DataFusion's own coercion and re-opens mixes, so the closing pass shuts them before the
  next rule (pins: types-1/C-007); the session installs them via the
  Spark door's `SessionExtension`;
  error conversion one layer up is `repark-core`) + `register_spark_decimal_planner` +
  `register_spark_integer_planner` +
  `analyze_eagerly(state, plan)` — the ONE blessed way to run the analyzer before a plan's
  schema or expressions cross a boundary (`ctx.sql` plans are PRE-analysis; an un-analyzed
  schema over analyzed buffers bit-reinterprets at the Arrow export — consumed by
  `repark-spark::spark_ast` and `repark-python::column::sql`) + the crate-root
  re-export of `shim_udf_boilerplate!`.
- `shim_macros.rs` — the `shim_udf_boilerplate!` (`name` / `signature`) macro every shim
  `ScalarUDFImpl` shares, re-exported at the crate root so call sites keep saying
  `crate::shim_udf_boilerplate!`. File-backed rather than root-inline because
  `scripts/check_lib_rs.py` counts every `lib.rs` line and this root sits at its 175 ceiling —
  the ceiling did not rise.
- `url.rs` — Spark `parse_url` / `try_parse_url` use `java.net.URI`-shaped splitting (sibling
  `java_uri.rs`).
  `datafusion-spark` 54.1 extracts with `url::Url`, a WHATWG-URL **normalizer**;
  Spark uses `java.net.URI`, a **splitter**. Eleven measured divergences closed:
  explicit `AUTHORITY` port kept, scheme/host case kept, dot segments unresolved,
  IDN host → NULL (registry-based authority) not punycode, empty-userinfo
  punctuation kept (`USERINFO` is `''`), opaque-URL `PATH` NULL, `%2e` kept
  **verbatim** — never decoded, and so never resolved as a dot segment either.
  Spark reads the **`Raw`** getters for `PATH` / `QUERY` / `REF` / `FILE` /
  `AUTHORITY` / `USERINFO`; only `HOST` (`getHost`) and `PROTOCOL` (`getScheme`)
  are non-`Raw`, and neither can hold an escape — so nothing this module serves
  is percent-decoded (MEASURED-JAVAP over `ParseUrlEvaluator$`). Also: an
  unparsable URL raises `INVALID_URL` on `parse_url` (upstream NULLed schemeless
  text) and NULLs on `try_parse_url`; the `QUERY` key is a Java regex
  (`(&|^)<key>=([^&]*)`, group 2) whose **compile failure raises under both**
  UDFs (`TryParseUrl`'s replacement is `ParseUrl(params, failOnError = false)`,
  not `TryEval`, and `getPattern` has no `catch`); and a 3-arg call with a
  non-`QUERY` part short-circuits to NULL before the URL is parsed at all.
  Registered from `lib.rs` after the `datafusion-spark` defaults so both doors
  resolve it. **Residual:** that key is a
  `java.util.regex` pattern on Spark and a `regex`-crate pattern here, and the
  `regex` crate is a finite automaton — `a(?=1)` lookahead, `(?<=&)b` lookbehind,
  `(a)\1` backreference, `(?>a)` atomic group and `\Qa\E` quoting all compile on
  Java and **raise** here (both UDFs). Everything else measured agrees, including
  `\p{Alpha}`, `a++`, `[a-z&&[^b]]` and `(?<n>a)`. Pinned by
  `url::tests::parse_url_query_key_regex_dialect_residual`; full agree/diverge
  table in `task/fn-gt2-ledger.md` "X8 RESIDUAL".
- `java_uri.rs` — the RFC-2396 splitter behind `url.rs`: scheme / authority
  (server vs registry) / userinfo / host / port / path / query / fragment, and
  Java's character classes and `scanEscape` rules. Every accessor is a `raw_*`
  getter handing back the recorded span verbatim; there is **no decoder in the
  module at all**, because a decoder could only ever reintroduce the divergence.
  No normalization anywhere — that is the whole point.
- `random.rs` — **r20 G2:** Spark `XORShiftRandom` + MurmurHash3 `hashSeed`; `rand`/`randn`
  ScalarUDFs (seed + partitionIndex=0; sequential within batch). Pins: first `rand(0)` value,
  sampleBy seed-0 count band.
- `analyzer/` — file-backed submodules of `analyzer.rs`. See [analyzer/map.md](analyzer/map.md).
  **G6-3 / G6-5 (2026-08-15):** `analyzer/cast_legality.rs` holds Spark's CAST/TRY_CAST
  type-legality deny matrix (`{Date32,Date64} ↔ {Int8,Int16,Int32,Int64}`) and its refusal
  (`[DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION]`, naming `UNIX_DATE` / `DATE_FROM_UNIX_DATE`).
  Called at the head of `rewrite_timestamp_casts` and from the `Expr::TryCast` arm. Not ANSI-gated
  (legality is a check on the type PAIR, not the eval mode). It is deliberately NOT
  the store-assignment matrix (`repark-iceberg`'s `write/store_assign.rs`) — the two answer
  different questions and each is laxer than the other somewhere.
- `analyzer.rs` — `SparkExprSemantics`: integer `/` → always-double division; division/modulo-by-zero
  follows `spark.sql.ansi.enabled` (raise when TRUE, NULL otherwise); `[]` array subscript →
  0-based with invalid-index → NULL (rewrites the planner's
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
  after the built-in analyzer rules. Rewrites match source types to injected output shapes, so each
  rewrite is idempotent. Set-operation schemas may require repeated analysis to reach a fixpoint;
  single-analyze schema consumers must perform that analysis. Integer division rewrites only when
  both operands are integers; decimal division remains decimal when no operand is float. A parent
  operator may retain an incompatible type after a pre-rewrite integer division.
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
- `instant_ts.rs` — overwrite `now` / `current_timestamp` / `to_timestamp` with Arrow
  `Timestamp(µs, UTC)`. Zoneless LTZ inputs (`TIMESTAMP '…'`,
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
  gives `timestamp[us, tz=UTC]`); nullability propagates via
  `return_field_from_args`. **B-TZ-4:** `__repark_timestamp_to_string__` (→ `Utf8`,
  `Volatility::Volatile`) renders Spark's space-separated session-zone wall for LTZ and the
  stored wall for NTZ; trailing-zero fractions are stripped (recorded: `.123400` → `.1234`).
  Embedded, never registered. Pins: `epoch_seconds_floor_is_floor_not_truncation` and siblings,
  plus `spark_timestamp_string_trims_trailing_fraction_zeros` / year-shape / LTZ-vs-NTZ here;
  facade corpus `test_timestamp_cast_parity.py`. **TZ-8:** `__repark_timestamp_to_date__`
  (embedded CAST) + registered `to_date` overwrite share `datetime::invoke_local_dates`.
  Pin `ltz_date_is_session_zone_and_ntz_is_stored_wall`. Ledgers:
  `task/tz5-cast-seconds-ledger.md` §4, `task/v3-btz4-ledger.md`, `task/r4-tz8-ledger.md`.
  **DATE-FN-1 (2026-09-04):** registered `date` (Spark `CAST AS DATE`) and `unix_timestamp` (the `date` / `unix_timestamp` registrations in `expr_fn.rs` / `timestamp_cast.rs` carry no doc comments; this row is their contract)
  (alias `to_unix_timestamp`). Zero-arg `unix_timestamp()` is a scalar epoch so a
  three-row input yields three identical BIGINT values.
  pins: date-fn-1-spark-date-spelling/C-002, C-003
  **TYPES-1 (2026-09-05):** `parse_session_zone` is `pub(crate)` for
  `spark_from_unixtime.rs`. pins: types-1/C-006
- `collection.rs` — `SparkElementAt` (`element_at`; public `element_at_udf()` for the facade embed):
  arrays are 1-based / negative-from-end / OOB → NULL
  with index 0 → error (Spark `INVALID_INDEX_OF_ZERO`); maps return the plain value-or-NULL
  (`map_extract` unwrapped through `array_element`).
  `str_to_map.rs` (`#[path]`) regex `str_to_map` overwrites DF's literal split
  (`bind_ascii_perl_classes` binds `\s`/`\d`/`\w` to the POSIX
  ASCII classes — Java's Perl classes are ASCII-only, the `regex` crate's are
  Unicode, so NBSP used to split where Spark does not).
  Two siblings are registered from `collection::functions()` — `shuffle.rs` (`ReparkShuffle`:
  the upstream kernel's
  NULL-slot placeholder read panics `arrow-data` when the child values buffer is
  empty, i.e. `CAST(NULL AS ARRAY<INT>)`; guarded input is returned as-is, and the
  Spark 4.0 `shuffle(array, seed)` overload passes through) and
  `map_from_entries.rs` (`ReparkMapFromEntries`: duplicate keys raise
  `DUPLICATED_MAP_KEY` per `spark.sql.mapKeyDedupPolicy=EXCEPTION`, instead of the
  upstream kernel's silent last-write-wins).
  Also `SparkArrayGet`
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
  `y M L d D q Q E H m s` — unsupported letters raise). TYPES-1 round 4 (2026-09-05): `yyyy`
  renders Java's leading `+` past 4 digits (pins: types-1/C-006). Each exposed via a named
  `*_udf()` constructor.
  Inputs are coerced by `coerce_date_arg` / `coerce_to_date32` / `coerce_to_timestamp_micros`
  (`user_defined` signature): date / timestamp (any unit+zone) / string — matching Spark. Tests run
  the UDFs through a real `SessionContext` against ISO-8601 / Spark goldens.
  Session-zone semantics resolve LTZ instants in `spark.sql.session.timeZone`; `Timestamp(_, None)`
  is NTZ wall time and remains wall time. LTZ conversion preserves the preferred source offset on
  DST overlaps and uses the current gap resolver. Rewrites match source types and output shapes,
  which makes each rewrite idempotent; repeated analysis remains a separate set-operation schema
  fixpoint requirement. `date_trunc` outputs `Timestamp(µs, UTC)`.
  **r20 A1:** SAF-001 out-of-chrono Date32 → NULL in `add_months`/`trunc` (pins
  `extreme_date32_add_months_and_trunc_null_without_panic`,
  `chrono_boundary_date32_add_months_computes`, `extreme_date32_year_extractor_no_panic`);
  SAF-002 downcast evidence + defensive `cast` before `as_primitive`/`as_string` (pin
  `trunc_accepts_large_utf8_format_without_panic`).
  **TYPES-1 (2026-09-05):** the `date_format` pattern compiler and wall-clock helpers are
  `pub(crate)` for `spark_from_unixtime.rs`. pins: types-1/C-006
- `format_version.rs` — **V3-10:** `resolve_alter_format_version` is the one `ALTER … format-version`
  resolver for every door: it parses the request, refuses a downgrade and anything above
  `MAX_SUPPORTED_FORMAT_VERSION` naming the key and both versions, returns `None` for the
  same-version no-op, and refuses `'3'` without the session opt-in naming the shared
  `cardinality::ALLOW_CREATE_FORMAT_VERSION_3_KEY` const. It composes that last message itself
  rather than borrowing `resolve_create_format_version`'s, because the CREATE text ends in a
  create-only clause; the conf key is the half the pins assert and it stays a single const, so
  the two doors cannot drift on the thing that matters. The request is parsed as a SIGNED
  integer and is NOT trimmed, which is what makes `'-1'` a downgrade and `' 3 '` unparsable the
  way Spark classifies them.
  The `#[allow(clippy::missing_errors_doc)]` on it (and on the three `write/format_version.rs`
  entry points) stands in for the `# Errors` doc comment the no-code-comments ruling forbids;
  the error domain is the row above.
  pins: v3-10-upgrade-v2-to-v3/C-002, C-003
- `expr_fn.rs` — logical-`Expr` builders for date, string, collection, URL, bitmap, and higher-order
  functions. Builders embed the same shims registered by the SQL door, including `unix_date`,
  `bit_length`, regexp/split functions, `shuffle`, `map_from_entries`, and `str_to_map`, so facade
  columns remain self-contained without a `SessionContext`.
  **TYPES-1 (2026-09-05):** `from_unixtime` builder over the new UDF. pins: types-1/C-006

Facade builders embed the same kernels registered by the SQL door, including `to_timestamp`, `avg`,
the additional `datafusion-spark` functions, and map builders; keep both dispatch surfaces aligned.
Higher-order SQL and facade resolution read one shared table; aliases require matching arity and
semantics, while unsupported lambda forms remain explicit refusals.
Regex collection and counting preserve Java empty-match stepping; astral mid-surrogate starts are
counting-only, and no-match results remain function-specific. Random functions share deterministic
streams, preserve pool order, reject non-constant bounds, and enforce per-row and total-size caps.
Validation functions preserve binary-vs-UTF8 representation behavior; `assert_true` passes only
`true` and fails on NULL. The module's registration helper keeps these surfaces aligned.

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| `year`/`hour`/`date_trunc` ignore `spark.sql.session.timeZone` | The carrier is not on the session. Only `SparkExtension::configure` installs it; a bare DataFusion context falls back to `UTC`. See `session_time_zone/map.md`. |
| A `DATE` shifted by a day under a non-UTC session | Check coercion idempotence; `coercion_is_idempotent_so_a_second_analysis_cannot_promote_a_date` and `crates/repark-spark/tests/session_timezone.rs::date_arguments_never_move_with_the_session_zone` pin the contract. |
| EVERY IANA zone id fails at query time but `+05:30` works | The `chrono-tz` feature on this crate's `arrow` dependency is gone. It is DECLARED in `Cargo.toml` for exactly this reason — re-declare it there rather than relying on `datafusion`'s feature graph. |
| `date_trunc` returns the right instant with the wrong-looking wall clock | Expected if the viewer ignores the UTC annotation: ticks are Spark's instant, typed as `timestamp[us, tz=UTC]`. |

First checks: `cargo test -p repark-functions`. Escalate to: [../map.md#debug](../map.md).

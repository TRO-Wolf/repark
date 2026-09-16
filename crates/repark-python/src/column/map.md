# map — repark-python/src/column

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

`PyColumn` is an immutable DataFusion expression wrapper. Its constructors and operators provide
the Python facade's Column surface while DataFrame methods bind expressions to input schemas.

## Modules

- [`display.rs`](display.rs) — **FACADE-2 step 2 (2026-09-12):** `PyColumnParts` is a
  static-method namespace. Each Group-2 op is one native call that builds the `Expr` and
  returns a 4-tuple `(PyColumn, spark_display, sql_expr, join_sql_expr)` so the `Expr` is
  moved, not cloned, back to Python. Child fragments (including Python `str`/`repr` and
  `_idents` quoting) are inputs; Rust only concatenates. Byte identity with the pre-move
  Python f-strings is the contract (card D-1).
  **Step-2b remediation (2026-09-12, review F1–F5):** operand parts extract as borrowed
  `&str` tuples, not owned `String`s — PyO3 borrows the UTF-8 cache instead of copying the
  growing SQL fragments across the boundary per op. `alias` returns a 2-tuple
  `(PyColumn, spark_display)`: its `sql_expr` is the child's unchanged string, so Python
  passes the existing `sql_expr_part()` through rather than round-tripping a copy through
  Rust. The generator arm of `cast` (`apply_engine=false`) takes `Bound<PyColumn>` and
  returns the same handle via `unbind` — step-1 reused `self._inner`, so no `Expr` clone.
  `case_when` moves `.expr` out of its arm pairs like `PyColumn::case_when`, and every
  `wrap_*` builder pre-sizes its `String` instead of `format!`.
  pins: facade-2/C-008, C-009, C-010, C-011, C-012, C-013
  **COLUMN-PARITY-1 critic round (2026-09-14):** `update_fields` (struct expr + op-tag /
  path literals + `with` values → `update_fields(st, WithField(..))` /
  `dropfield()` display), `repark_isnan` (`isnan(child)` display), and `in_list`
  (`(left IN (..))` display) follow the same one-call 4-tuple shape, plus the
  string-only `field_join_sql` helper for the join-ON bracket fragment.
  pins: column-parity-1/C-008
  **FACADE-2 step 3 (2026-09-13):** `call_scalar` renders the generic `name(args)` call —
  the shared helper every `F.<fn>(...)` builder routes through — as a 4-tuple
  `(PyColumn, spark_display, sql_expr, join_sql_expr)` with an optional display
  override for the bespoke-name callers. Argument part lists arrive as
  `Bound<PyList>` items extracted in one pass to `PyBackedStr` handles — owned
  zero-copy refs to the Python `str` data (the step-2b lesson: the growing fragments
  never copy across the boundary). Operand `Expr`s move out of the extracted
  `PyColumn`s into `call_scalar_expr`'s owned `Vec<Expr>` — no second clone on top
  of the operational one (S2-21 P2-1). The step-3 `#[pymethods]` wrappers for the
  Group-1 typed constructors live in this impl block — `#[pymethods]` cannot be split
  across files — while their `Expr` construction stays in `display/construct.rs`.
  pins: facade-2/C-014, C-016, C-017, C-019, C-020
  **DECIMAL-CACHE-1 remediation (2026-09-15):** `unary_neg` emits `Expr::Negative`
  of the child instead of `lit(0_i32) - child`, so facade `-decimal(p,s)` keeps the
  child type like Spark's `UnaryMinus` (the old encoding drifted through the
  pre-coercion seat to `(11,2)` / `(38,9)`); display and SQL fragments unchanged.
  pins: decimal-cache-1/C-007
  **FNP-WIN-1 (2026-09-15):** `time_window` is a `#[pymethods]` wrapper in this same
  impl block for the same reason — a thin call into the `repark-functions`
  time-window UDFs; grouping/expansion live in the analyzer rule. Step 4 adds
  the `session_window` wrapper the same way (marker call aliased
  `session_window`).
  pins: fnp-win-1/C-001, C-002, C-004, C-008
- [`display/construct.rs`](display/construct.rs) — **FACADE-2 step 3 (2026-09-13):** the
  Group-1 typed constructors that replace `_native.PyColumn.sql` call sites:
  `lit_timestamp`, `lit_date`, `lit_time`, `lit_array_cast`, `pi`, `uuid` — a `display`
  submodule because `mod.rs` is at its exact line baseline. Each builds the `Expr` the
  analyzed SQL text produced and renders the display/SQL fragments in Rust.
  `lit_timestamp` mirrors the analyzer's fold decision: text that parses inside the
  nanosecond `i64` range folds to `Literal(TimestampMicrosecond(µs, UTC))`; out-of-range
  or unparsable text (measured: Arrow's cast yields a null element, never an error)
  keeps the `to_timestamp(__repark_decimal_cast_nullable__(Utf8))` call form the SQL
  path stored. `lit_date` / `lit_time` store `Cast(Utf8, Date32)` /
  `Cast(Utf8, Time64(Nanosecond))`; `lit_array_cast` stores `Cast(array-call,
  List<item: <element>, nullable>)` — `VARCHAR` maps to `Utf8View` internally and the
  Arrow exporter coerces views to `string` at the door.
  pins: facade-2/C-014, C-015, C-016
- [`mod.rs`](mod.rs) owns `PyColumn`, constructors, operators, aggregates, and window attachment.
  **PERF-APPROXPCT-1 (2026-09-05):** `approx_percentile_cont` / `approx_percentile_list`
  take `accuracy: Option<i64>` (None omits the third literal, so default-accuracy display
  names keep the two-arg Spark shape). The list call construction collapses to a `let` and
  the percentage validation folds into a named closure; both hold the file at its exact
  baseline (ratchet 1053→1052 with the accuracy import). pins: perf-approxpct-1/C-002
- [`function_dispatch.rs`](function_dispatch.rs) owns scalar and aggregate function dispatch.
  Its default arm hands the name to [`function_dispatch/`](function_dispatch/map.md) before
  refusing.
  **FNP-BITMAP-FACADE-1 (2026-09-15):** `unary_aggregate_udaf` gains the
  `bitmap_construct_agg` / `bitmap_or_agg` / `bitmap_and_agg` arms mapping to
  `repark_functions::bitmap_agg::{bitmap_construct_agg_udaf, bitmap_or_agg_udaf,
  bitmap_and_agg_udaf}` (the module flipped `pub` in `repark-functions` for the cross-crate
  path, the same shape as the `repark_functions::aggregate` arms). The file sat at the exact
  1000-line ceiling, so the four `datafusion::logical_expr::binary_expr` arms condense onto
  the existing import (8 → 4 lines each, the `IsNotDistinctFrom` call staying wrapped for
  rustfmt) before the three arms land — 1000 → 991 lines, no baseline raised.
  pins: fnp-bitmap-facade-1/C-001
  **DEGREES-RUST-1 (2026-09-15, owner Q-15a-1):** `call_scalar_expr` gains the `degrees` /
  `radians` arms onto the engine's scalar UDFs (`datafusion::functions::expr_fn`) — the
  single-multiply `f64::to_degrees` / `f64::to_radians` form the facade measured bit-equal
  to Spark, so reuse and not a new kernel. **Run 16a round 3:** the arms rebind to the
  Spark-exact `repark_functions::expr_fn::degrees` / `radians` pair, which carries the
  refusals and the ANSI switch the engine UDFs lack.
  pins: fnp-bitmap-facade-1/C-011, C-012, C-013, C-014
  **FNP-11B step 2 (2026-09-15):** the `to_date` arm takes 1 or 2 args
  (`expr_fn::to_date` widens to `Vec<Expr>`); `unix_timestamp` takes 0 to 2.
  pins: fnp-11b/C-002, C-003
- [`function_dispatch/dispatch_json.rs`](function_dispatch/dispatch_json.rs) —
  **FNP-9/10 (2026-09-05):** arms for
  `get_json_object`, `json_array_length`, `json_object_keys`, `schema_of_json`, `to_json`,
  `from_json`, `array_insert`, `arrays_zip` and `map_concat`, plus `create_map`, which lowers
  the facade's alternating key/value arguments to DataFusion's `map(keys, values)` — the same
  shape the Spark door already builds for `map(...)`, so the facade still makes one engine
  call. The arms live here and not in `function_dispatch.rs` because that file was at 992 of
  its 1000-line ceiling; the cohesive `column/dispatch/` split the campaign charter names is
  FNP-Z's, and the slate forbids doing it piecemeal inside a feature unit.
  pins: fnp-9-collections-json/C-006, C-007
  LOG1P-1: `log1p` / `expm1` arms embed `repark_functions::expr_fn` kernels.
  pins: log1p-1-precise-kernels/C-002
  **FNP-11A:** the fourteen temporal arms live in `dispatch_json.rs` beside the JSON
  arms (the parent file is at its 1000-line ceiling, so no arm lands there;
  `datediff` moved here when the door grew its arity route).
  pins: fnp-11a/C-002, C-003
  **FNP-GEN-1 step 2 (2026-09-16):** the `posexplode` / `posexplode_outer` / `inline` /
  `inline_outer` arms embed the registered placeholder UDFs and the
  `__repark_gen_alias` arm embeds the marker the facade's `_GeneratorColumn.alias`
  uses to carry multi-name output aliases into `generator::GeneratorRewrite`.
  pins: fnp-gen-1/C-002, C-003
  **DATE-FN-1:** `unix_timestamp` / `to_unix_timestamp` (0 or 1 arg). PySpark has no `F.date`.
  pins: date-fn-1-spark-date-spelling/C-002
  **FN-FIX-1:** `isnan` / `sha2` / array kernels. pins: fn-fix-1-registry-rows/C-002
  **FN-FIX-2:** `initcap` / `chr` / `elt` / `rlike` / `regexp_like` / `regexp_replace`
  embed the Spark kernels; `elt` left EXPECTED_DIVERGENCES. pins: fn-fix-2-string-rows/C-002
  **FN-REGEXP-EXTRACT-1:** the `regexp_extract` arm embeds `expr_fn::regexp_extract`.
  pins: fn-regexp-extract-1/C-001
  **TYPES-1 (2026-09-05):** the `from_unixtime` arm takes the optional format arg.
  pins: types-1/C-006
  **ABS-EXPR-1 (2026-09-13):** `abs` / `cbrt` / `nullif` arms — plain `expr_fn` calls —
  replace the facade `when(...)` rewrites that embedded the child 3×/2× per level
  (exponential native memory; run 9 OBS-R9-6). `expr_fn::abs` is `checked_abs` on the
  signed ints: integer-min raises `Int*NArray overflow on abs(…)` (Spark raises
  `ARITHMETIC_OVERFLOW`; the class differs, the raise is pinned). `cbrt` wraps its
  argument as `arg * lit(1.0f64)` — Spark's `cbrt` is a UnaryMathExpression (always
  DoubleType, computed in f64); the multiply coerces f32/int/decimal to f64 while
  bool/string still refuse (no numeric coercion).
  pins: abs-expr-1/C-001, C-002
- [`expr_build.rs`](expr_build.rs) owns type parsing, alias handling, and expression inspection.
  **FN-FIX-1:** `window_from_aggregate` copies `IGNORE NULLS`. pins: fn-fix-1-registry-rows/C-002
  **WIN-SLIDE-1 (2026-09-04):** `single_wrapped_aggregate` / `replace_wrapped_aggregate` let
  `Column.over` push a window spec INTO the one aggregate inside a scalar wrapper. `F.collect_list`
  and `F.collect_set` build Spark's empty-group semantics as
  `coalesce(array_agg(x) IGNORE NULLS, make_array())`, so `over()` used to refuse them outright;
  the group-by spelling is untouched, and two aggregates in one expression still refuse (there is
  no single window to push). pins: win-slide-1/C-002
  **PERF-APPROXPCT-1 (2026-09-05):** `percentile_approx_scalar_expr` (new) and
  `percentile_approx_list_expr` take `Option<i64>` accuracy and build the two- or three-arg
  UDAF call. pins: perf-approxpct-1/C-002
  **FNP-8 (2026-09-06):** `sql_context` builds the `PyColumn.sql` throwaway
  context with `repark_spark::dialect_for_executing_parse`, so column-free `F.expr` with
  `x -> y` parses as a lambda. `mod.rs` stays at its exact baseline — the call site
  is a one-line swap. pins: fnp-8/C-004
  **FNP-4B (2026-09-15):** `plan_expr_column` analyzes eagerly, then falls back on an
  unresolved-column failure (`Diagnostic`-wrapped included) to `parse_unresolved_expr`,
  which discovers referenced names through typed errors on a normalization-off context and
  returns the unresolved tree for the consumer frame to bind (exact-case `Column`
  contract). `PyColumn.sql` canonicalizes fragments first (struct-at-EOF included).
  pins: fnp-4b/C-003, C-019
  **FNP-4B critic (2026-09-15):** `parse_unresolved_expr` reuses the eager `SessionContext`
  instead of building a second one after analysis fails. `sql_context` installs
  `FoldSparkNumericCasts`, `SparkProjectionDisplay`, and `__repark_spark_as__`.
  **FNP-4B round 8 (2026-09-15):** `sql_context` serves a process-wide cached
  `SessionContext` (fixed Databricks dialect, rules and `register_all` baked in once)
  and `PyColumn.sql` blocks on the shared engine runtime: 10k `F.expr` 2.33s → 0.72s.
  **Round 5 (2026-09-15):** `sql_context` also installs
  `__repark_suffix_literal__`. pins: fnp-4b/C-021
  **UNRESOLVED-ROUTINE-1 (2026-09-16):** `plan_expr_column` builds the
  `SELECT (…) AS _repark_expr` wrapper inside and maps failures with the
  original fragment (`unknown_routine_to_py_err`, so `F.expr` positions at the
  fragment); `parse_canonical_predicate` stays verbatim and `filter_sql` maps
  with the predicate. `mod.rs` ceiling ratcheted 1014 → 1013 (wrapper move).
  pins: unresolved-routine-1/C-003
  **FNP-8 repair (2026-09-07):** the throwaway context builds its standard analyzer vector with
  the same pre-coercion HOF preparation as a normal Spark session. pins: fnp-8/C-003, C-004
  **FNP-8-REVIEW (2026-09-07):** the nested-HOF refusal names the Column door as the
  refusing side and the SQL door as serving nested lambdas (F5 reword).
  pins: fnp-8-review/C-005
- [`window.rs`](window.rs) owns Spark frame conversion and unordered-window policy.
  **WIN-SLIDE-1 (2026-09-04):** a `RANGE` offset is emitted as `ScalarValue::Utf8`, not `Int64`.
  DataFusion's window-frame coercion casts a `Utf8` bound to the ORDER BY key's type (that is the
  shape its own SQL planner produces) and passes any other scalar through untouched — and a bound
  whose type does not match the key degrades silently to UNBOUNDED PRECEDING, so
  `rangeBetween(-2, 0)` over an `IntegerType` or `DoubleType` key answered the cumulative column.
  `ROWS` / `GROUPS` bounds stay `UInt64`, which is already the coercion target.
  Registry: `WIN-RANGE-DF-1`. pins: win-slide-1/C-003
- [`door_parity_tests.rs`](door_parity_tests.rs) pins standalone facade UDF behavior against SQL.
  **DOOR-CONVERGE-1 (2026-09-15):** `EXPECTED_DIVERGENCES` ratchets 22 → 14 — `abs`,
  `hypot`, `bin`, `rint`, `base64`, `unbase64`, `size`, `cardinality`,
  `array_contains`/`array_has`, `ascii`, `length`, `character_length` leave the table
  and join `SCALAR_NAMES`; the facade arms for them live in
  [`function_dispatch/dispatch_spark.rs`](function_dispatch/dispatch_spark.rs) and call
  the same `repark_functions::expr_fn` kernels `register_all` installs on the door.
  Round 2 (2026-09-16): `unbase64` raises the Java MIME-decoder errors on malformed
  endings; `array_contains` coerces element and needle to the tightest common type,
  refuses incompatible pairs with `DATATYPE_MISMATCH.ARRAY_FUNCTION_DIFF_TYPES`, and
  answers `array_contains(array(), 1)` = `False`. Round 3 (2026-09-15): ruling R-10
  reverted the `array`/`make_array` `containsNull` declaration and the registry-wide
  promise retag to DOOR-CONVERGE-2 (fixtures-batch7) — literal-haystack
  `array_contains` nullability is the residual that rides on them.
  **DOOR-CONVERGE-2 (2026-09-15):** `reverse`, `sequence` and `split` join
  `SCALAR_NAMES` (the C-006 ratchet); `concat` stays out — variadic, no fixed arity.
  `generate_series` joins `EXPECTED_DIVERGENCES` instead (table 14 → 15): the facade
  keeps it as a `sequence` alias on `SparkSequence` while the SQL door keeps DataFusion's
  native spelling for its table-function callers — C-003's reroute exposed the split,
  overwriting the door would break `FROM generate_series` users.
  pins: door-converge-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008,
  C-009, C-010, C-011, C-012, C-013, C-014; pins: door-converge-2/C-006
  **ABS-EXPR-1 (2026-09-13):** `EXPECTED_DIVERGENCES` gains `abs` — the facade's core
  `checked_abs` raises on integer-min (Spark ANSI-on answer); the door's `SparkAbs`
  wraps because repark never sets `execution.enable_ansi_mode` — measured on typed
  int8/16/32/64 columns (each minimum returns unchanged) and value-pinned by
  `test_abs_door_parity_integer_min`. Table 21 → 22.
  pins: abs-expr-1/C-002
  **TYPES-1 (2026-09-05):** `from_unixtime` left EXPECTED_DIVERGENCES (ratchet 22 → 21).
  pins: types-1/C-006

## Contracts

- `literal` distinguishes Python `bool` from `int` and accepts only supported scalar types.
  A Python `int` that fits in Int32 is an Int32 literal (Spark `IntegerType`), so
  `col(int32) + 1` stays Int32. pins: f-y10-1-int-overflow/C-002
- `sql` analyzes standalone expressions before handoff; parse errors map to `ParseException` and
  unresolved names map to `AnalysisException`. This path bypasses the Spark SQL router, so
  FNP-15/16 declared-function names refuse through `refuse_sql_fragment` (collation +
  declared-absent). pins: fnp-15-16/C-001
- Higher-order lambda variables are resolved against the consuming DataFrame schema.
- Nested higher-order functions refuse loudly rather than producing an invalid plan.
- `concat` embeds the door-converged `repark_functions::string::concat_udf` (DOOR-CONVERGE-2):
  string, binary and array arms with any-NULL → NULL, one kernel on both doors.
  pins: door-converge-2/C-001
- `reverse` routes through `function_dispatch/dispatch_spark.rs` (DOOR-CONVERGE-2): the
  facade shares the SQL door's array-aware kernel instead of the string-only lowering.
  pins: door-converge-2/C-002
- `sequence` routes through `function_dispatch/dispatch_spark.rs` (DOOR-CONVERGE-2): the
  facade shares the SQL door's kernel with the literal-expansion ceiling kept.
  pins: door-converge-2/C-003
- `split` routes through `function_dispatch/dispatch_spark.rs` (DOOR-CONVERGE-2): the Rust
  arm is ready, but Python `F.split` raises before reaching it (run 16a owns that half).
  pins: door-converge-2/C-004
- `to_timestamp_ltz` / `to_timestamp_ntz` dispatch beside `to_timestamp`, and
  `try_to_timestamp` joins the `try_to_date` arm (FNP-11B step 3): thin arms over
  `expr_fn` onto the new `timestamp_ltz_ntz` kernels.
  pins: fnp-11b/C-001, C-002
- `make_time` / `to_time` / `time_diff` / `time_trunc` share one refusal arm over
  `expr_fn::time_family_refusal`, with `current_time` (0 or 1 args) and `typeof`
  beside `hour` (FNP-11B step 4): thin arms onto the new `time_family` kernels.
  pins: fnp-11b/C-001, C-002, C-005
- `to_number` / `to_binary` / `to_char` / `to_varchar` share one arm over
  `expr_fn::to_char_family` (FNP-11B step 5): arity is enforced by the kernels'
  coercions, keeping the dispatch file under its ceiling. pins: fnp-11b/C-001,
  C-002, C-005
- Window frames use Spark-relative offsets. Count-like unsigned results are cast to signed types.
- Unknown scalar, aggregate, cast, or window names fail with typed Python exceptions.

## Change locations

FNP-7 try_* scalar and aggregate names dispatch here (`try_divide` … `try_to_time`,
`try_sum`, `try_avg`); FNP-11B step 3 adds `try_to_timestamp` to the same arm.
pins: fnp-7-try-inversions/C-013; fnp-11b/C-001
SEM-1 `log` embeds `SparkLog` (1- or 2-arg); `ln` stays DataFusion `ln`.
pins: sem-1-spark-answer-parity/C-005, C-006

Add a Column method in `mod.rs`, a scalar or aggregate dispatch arm in `function_dispatch.rs`, a
builder rule in `expr_build.rs`, or a frame rule in `window.rs`. Add the matching parity test.

## Verification

Run `cargo fmt --check`, `cargo test -p repark-python`, the exact-equivalence scanner, and map
sync after changes.

## Pointers

- Up: [src map](../map.md)
- Crate: [repark-python map](../../map.md)
- **FNP-4B remediation (2026-09-15):** `expr_build.rs` / `mod.rs` added code comments removed (ruling 2026-08-26); `mod.rs` ceiling ratcheted 1040 → 1038.

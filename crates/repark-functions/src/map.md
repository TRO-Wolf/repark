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
  deferred-by-cost; CSV/XML/XPath (11), VARIANT (8), geospatial (5), and the XML pair
  `from_xml` / `schema_of_xml` (FNP-GEN-1 D-6, dated 2026-09-15, message names
  `FNP-16-csv-xml-xpath`) likewise. `armed_names()` is 64.
  pins: fnp-15-16/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-013, fnp-gen-1/C-002, C-003
- `generator.rs` — **FNP-GEN-1 step 2 (2026-09-16):** placeholder `ScalarUDF`s for
  `posexplode` / `posexplode_outer` / `inline` / `inline_outer` plus the
  `__repark_gen_alias` marker that carries multi-name aliases out of the facade, and
  `GeneratorRewrite`, the analyzer rule that rewrites a `Projection` holding exactly one
  generator call into inner-projection → one multi-column `Unnest` →
  final-projection, splicing the generated columns at the call's select-list position.
  `posexplode` gets `pos` from the internal `__repark_gen_ordinality` UDF (positions per
  list element) unnested in parallel with the values; `inline` reads struct fields
  through `__repark_gen_field`, which unions the parent struct's validity into the
  child so a NULL element emits NULL fields rather than Arrow defaults; the outer
  spellings normalize NULL/empty inputs to a one-NULL-element list so `preserve_nulls`
  emits Spark's all-NULL row. Intended-non-nullable outputs wrap in the
  `__repark_spark_nonnull__` schema marker (value-preserving). A single `AS name` or
  `__repark_gen_alias` name list must match the output arity or the rewrite raises
  `[COLUMN_ALIASES_MISMATCH]`; two generators, a `stack` call, an explode-path
  `__repark_arr_*` temp alias, or a sibling column carrying a reserved
  `__repark_gen_*` name all refuse `Only one generator allowed per select list`;
  a generator argument that is or references an aggregate output refuses
  `[MISSING_GROUP_BY]`; nested calls and non-container inputs refuse
  `[UNSUPPORTED_GENERATOR]`-class. Registered from `lib.rs::register_all`; the rule
  registers at the tail of `analyzer_rules()` in `registration.rs`, after the closing
  `TypeCoercion`. The `#[cfg(test)]` suite lives in [`generator/tests.rs`](generator/tests.rs)
  (moved out in remediation round 1 so this file keeps the size ceiling).
  **Step 2 (run 18a):** `json_tuple` joins the rule — its site carries every argument
  (the document plus the field expressions), non-string arguments refuse
  `[DATATYPE_MISMATCH.NON_STRING_TYPE]` and fewer than two arguments refuse
  `[WRONG_NUM_ARGS]`, both with Spark's message shape. The expansion computes one
  `STRUCT<c0..cN>` per row through the internal `__repark_json_tuple` kernel (one
  parse per row, no unnest) and reads the fields back through `__repark_gen_field`.
  A lone alias on a single output is ignored and any other count mismatch raises
  `[UDTF_ALIAS_NUMBER_MISMATCH]`, both per the s34 oracle; the other four names keep
  `[COLUMN_ALIASES_MISMATCH]`.
  pins: fnp-gen-1/C-002, C-003, C-005
- `spark_length.rs` — **GT1-FIX G5 / A3 / R3-1:** Spark `bit_length` /
  `octet_length`. Stringifies non-binary; BINARY pass-through (including
  Dictionary(_, Binary)); refuses ARRAY/STRUCT/MAP; decimal scale-padded
  stringify. Ledger: `task/fn-gt1-ledger.md`.
  **JAVA-DOUBLE-STR-1 (2026-09-15):** `FLOAT`/`DOUBLE` inputs keep their type
  through coercion and count the shared Java-formatter bytes (registry BL-7).
  pins: java-double-str-1/C-006
  **Round 2 (2026-09-15):** lengths come from the stack formatter's byte count,
  with no `StringArray` built. pins: java-double-str-1/C-016
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
- `spark_degrees.rs` — **FNP-BITMAP-FACADE-1 run 16a round 3 (DEGREES-RUST-1, owner
  Q-15a-1):** Spark-exact `degrees` / `radians` ScalarUDFs shared by both doors.
  `Signature::user_defined` + pass-through `coerce_types`; numerics and STRING pass,
  every other type refuses at planning with `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`
  naming the DOUBLE requirement, the resolved argument and the Spark type name
  (the shared `bitmap_agg::spark_type_name`). The kernel keeps the bit-exact
  single multiply, casts STRING itself like `abs` does (`CAST_INVALID_INPUT` under
  ANSI read from `args.config_options`, NULL under ANSI-off), and always answers
  nullable Float64. **Run 16a round 4:** one trailing Java float suffix (`d`/`D`/`f`
  /`F`) is accepted when the remainder is a decimal floating literal that already
  parses — `'1d'`, `'1.5F'`, `'1e2d'` answer; suffixes after `Infinity`/lowercase
  names/hex (`'Infinityd'`, `'infd'`, `'1dd'`, `'0x10'`) keep today's refuse/NULL. The shared
  SQL `CAST` kernel is untouched. pins: fnp-bitmap-facade-1/C-012, C-013, C-014,
  C-017, C-018
- `spark_math.rs` — **DOOR-CONVERGE-1 (2026-09-15):** Spark `abs` / `hypot` / `bin` /
  `rint` kernels shared by both doors. `abs` keeps the input width, refuses BOOLEAN, and
  reads the ANSI carrier (`repark.ansi` extension via
  `ansi::spark_ansi_enabled_from_options`) to raise Spark-shaped `[ARITHMETIC_OVERFLOW]`
  on signed minima. `hypot` is rescaled `f64::hypot` (infinity over NaN). `bin` / `rint`
  use `Signature::user_defined` + `coerce_types` so a BOOLEAN input refuses with
  `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` rather than an internal signature error.
  **Round 5 (2026-09-16):** `abs` accepts STRING per Spark's implicit STRING→DOUBLE
  cast — `coerce_types` passes the utf8 type through and the kernel safe-casts
  itself so a malformed value raises `[CAST_INVALID_INPUT]` under ANSI (NULL under
  ANSI-off) instead of a bare Arrow cast error (fixtures-batch11.json
  A11-sql-abs-1/-x, A11-api-abs-x, A11-callfn-abs-x).
  **SQL-LITERAL-TYPING-1 remediation round 1 (2026-09-16):** `Factorial`
  shadows upstream `factorial` the same way (`user_defined` + integer widths
  down to Int32, kernel delegated to `spark_factorial`) so `factorial(5)`
  survives plan construction. Unit tests pin the literal, the explicit
  `CAST(5 AS BIGINT)`, and the text refusal.
  pins: sql-literal-typing-1/L-002
  pins: door-converge-1/C-002, C-003, C-008
- `spark_base64.rs` — **DOOR-CONVERGE-1 (2026-09-15):** Spark `base64` / `unbase64` — a
  hand-rolled `java.util.Base64` MIME codec (no `base64` crate dep): RFC 4648 padding,
  CRLF chunking every 76 output characters, lenient decode that skips non-alphabet bytes
  and accepts unpadded input. String and binary input; NULL propagates.
  **Round 2 (2026-09-16):** malformed endings raise the Java decoder's texts
  (`incorrect ending byte at N`, `wrong 4-byte ending unit`, `Last unit does not have
  enough valid bits`); encode writes one `StringBuilder`, decode uses a `static`
  table into a single batch buffer with the input null bitmap.
  pins: door-converge-1/C-001, C-010
- `spark_result_types.rs` (+ `spark_result_types/tests.rs`) — **TYPES-1 (2026-09-05):**
  `SparkIntegerLiteral` narrows in-range `Int64` literals to `Int32` (first in
  `analyzer_rules()`, after DataFusion's own `TypeCoercion`; `LIMIT` fetch/skip stay `Int64`
  for the physical planner), `SignedAggregate` casts `regr_count`/`approx_distinct` to
  `Int64`, `SignedWindow` casts the rank family to `Int32`.
  pins: types-1/C-001, C-003, C-005, C-007
  **DOOR-CONVERGE-1 (2026-09-15):** `SignedAggregate` additionally declares
  `is_nullable() = false` with `default_value = 0` — `approx_count_distinct` /
  `regr_count` answer non-null `bigint`, `0` on empty input. pins: door-converge-1/C-006
  **FNP-8 (2026-09-07):** exposes the existing single-node provisional-integer narrowing inside
  the crate so HOF preparation can reuse it without changing global literal or overflow rules.
  **SQL-LITERAL-TYPING-1 round 3 (2026-09-16):** the `Negative` fold is gone
  from the shared helper, so a parenthesized `-(2147483648)` stays bigint in
  lambda bodies and constructors exactly as on the top-level door
  (LIT2-SQL-03); only the lexer-level negative token narrows.
  pins: sql-literal-typing-1/V-001
- `lambda_rebind.rs` — **FNP-8 (2026-09-06):** `LambdaRebind`, in `analyzer_rules()`
  twice — right after `SparkIntegerLiteral` and last. Two passes over lambda bindings: it
  packs a multi-parameter lambda body that leaves a parameter unreferenced into the
  facade's own `named_struct` + `get_field("__hof_body")` shape (the physical planner
  remaps by referenced position, so an `(x, i)` body mentioning only `i` would read the
  element slot), then re-resolves every binding from the current value types (the
  narrowing rules run after SQL planning bound them). The early seat matters: the
  closing `TypeCoercion` bakes casts from the bindings it sees (`CAST(x AS BIGINT)`,
  `CAST(init AS BIGINT)`), so it must see rebound ones; the late seat repairs whatever
  the later narrowers re-stale. Unary and fully-referenced bodies pass through
  untouched, so the Column door's plans are byte-identical.
  pins: fnp-8/C-004
  **JAVA-DOUBLE-STR-1 round 2 (2026-09-15):** the shared pre-coercion insertion point
  seats one more rule one slot later — `SparkFloatStringify`, which must see `LIKE`
  and `CASE` before `TypeCoercion` errors on (or mis-unifies) float/string mixes.
  pins: java-double-str-1/C-013
  **DECIMAL-CACHE-1 (2026-09-15):** the same insertion point seats `SparkDecimalPrecision`
  one slot later still (immediately pre-coercion). DataFusion's default `TypeCoercion`
  pre-wraps bare integer literals (`Int32 -> (10,0)`), which the late seat can no longer
  tell apart from explicit user casts (#386 keeps `(10,0)`-over-`Int32` user casts declared);
  the early seat sees bare literals and min-precisions them, so facade `price * 5` reaches
  `(38,8)`. The appended seat stays (sessions without this preparation, and shapes the
  early seat skips); the already-correct-`CAST` stop makes the second run a no-op.
  pins: decimal-cache-1/C-002
  **FNP-8 repair (2026-09-07):** `HigherOrderPreparation` runs only before the first default
  type-coercion pass. It narrows direct constructor literals for indexed `transform`, narrows a
  direct `aggregate`/`reduce` initial literal and its lambda-body literals, and derives direct
  constructor element nullability from expression fields. Direct projection lineage refines
  constructor nullability only; a nullable-element source keeps its inherited Int64 Column result.
  A raw Int64 constructor subquery with a bare Int64 initializer defers early aggregate numeric
  preparation so the existing late literal and binding passes narrow the source and fold together.
  Explicit casts remain present. The same module owns
  `analyzer_rules_with_higher_order_preparation`, which places this rule before the first default
  `type_coercion` rule and refuses a vector without that insertion point.
  pins: fnp-8/C-003, C-004, C-005, C-006
  **FNP-8-REVIEW (2026-09-07):** preparation narrows provisional integer literals in
  every HOF body plus direct array/map constructor literals before the first bind, all
  leading value args — except a column tracing to a still-provisional constructor,
  where the late rules narrow value and body together (F1). Aggregate keeps its
  deferral; `binding-zip_empty`/`binding-zip_null` converged to Spark Int32.
  Direct array constructors are wrapped bottom-up so nested non-null elements bind
  non-null (F4). The provisional skip also covers value-side columns fed by bare
  literals (VALUES rows, double-nested subqueries), traced iteratively.
  pins: fnp-8-review/C-001, C-004
  **FNP-8-REVIEW round 2 (2026-09-07):** both lineage walks resolve through every plan
  node a literal reaches. `source_feeds_bare_integer` maps Join (semi/anti/mark aware,
  both sides plus cross), Aggregate group keys, Window input columns, Unnest
  dependencies, scalar-subquery plans, the recursive-CTE static term, and every
  passthrough node; `source_element_nullable` chases multi-hop column lineage for the
  element wrap, with Union demanding every branch non-null. Nodes no spelling reaches
  (lateral view refuses loud) keep a defensive arm or a documented terminal.
  The Join arm wraps only sides the join cannot pad (padded sides stay conservative,
  since the wrapper asserts a non-null field and padded NULLs would crash Arrow);
  the Values arm resolves every row and bails on any non-constructor cell.
  pins: fnp-8-review/C-009, C-010
- `decimal_precision.rs` (`decimal_precision/negate_null_tests.rs`) — **DECIMAL-CACHE-1
  round 2 (2026-09-15):** `SparkNegateNullDecimal`, first in `analyzer_rules()`.
  DataFusion's scalar `Negative` kernel rejects a constant-folded null decimal
  (`Decimal128(None,p,s)` → `Internal error`) where Spark `UnaryMinus` propagates null;
  the rule folds `Negative` over a null decimal literal — directly, through
  `Cast`/`TryCast` to a decimal target (the facade/SQL `-(lit/cast-null)` shape), and
  through nested negatives — to a null literal of that type before const-folding runs.
  Valued decimals, null/valued non-decimals, and non-decimal cast targets pass through
  untouched (their kernels already agree with Spark). Recompute runs only when a node
  rewrote. The rule lives in this file (not a new module) so `lib.rs` stays at its exact
  175-line ceiling with the one seat line; tests ride the canonical child module.
  pins: decimal-cache-1/C-012
- `json.rs` (+ [`json/`](json/map.md)) — **FNP-10 (2026-09-05):** the Spark JSON family —
  `get_json_object`, `json_array_length`, `json_object_keys`, `schema_of_json`, `to_json`,
  `from_json`. Registered from `register_all`; no new dependency (see `json/map.md`). Each
  kernel carries its own `#[cfg(test)] mod tests` running the measured cells through a
  `SessionContext`. Each
  kernel carries its own `#[cfg(test)] mod tests` running the measured cells through a
  `SessionContext`.
  **Remediation (run 18a):** `tuple.rs` holds scalar field names once per batch;
  detail lives in `json/map.md`.
  pins: fnp-9-collections-json/C-002, C-003, C-004, C-005, fnp-gen-1/PERF-003
- `count_if.rs` — **TYPES-1 (2026-09-05):** SQL-door `count_if` aggregate UDF answering
  `Int64`. pins: types-1/C-003
- `bitmap_agg.rs` (+ [`bitmap_agg/`](bitmap_agg/map.md)) — **FNP-6D (2026-09-15,
  round 2):** Spark `bitmap_construct_agg` / `bitmap_or_agg` / `bitmap_and_agg`.
  State is `[u8; 4096]` (or `n_groups * 4096` on the groups path). Empty
  `construct`/`or` answer all-zero; empty `and` answers all-ones. Short/long
  BINARY is zero-padded or truncated to 4096. STRING construct args coerce to
  BIGINT. Out-of-range positions raise `[INVALID_BITMAP_POSITION]` SQLSTATE
  22003. Sliding frames use the session WIN-SLIDE-1 rescan (no local
  `create_sliding_accumulator` refuse). GroupsAccumulator in `bitmap_agg/groups.rs`.
  pins: fnp-6d/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009,
  C-010, C-011, C-012, C-013, C-014, C-015, C-016, C-017. **FNP-6D-FOLLOWUP-1
  (2026-09-15):** user-defined signatures with planning-time Spark refusals
  (`DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`, BINARY/BIGINT wanted); strict
  ANSI-on STRING-to-BIGINT parsing (`CAST_INVALID_INPUT`, SQLSTATE 22018) with
  in-place `Int64` walks and direct `&[u8]` visits (no per-batch Vec, no payload
  copies); numerics truncate as Spark, non-finite and out-of-range numerics raise
  `CAST_OVERFLOW` (SQLSTATE 22003) detected per row from the source array;
  `FixedSizeBinary` folds as BINARY.
  pins: fnp-6d-followup-1/C-001, C-002, C-003, C-004
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
- `spark_regex_engine.rs` — **JAVA-REGEX-FEATURES-1 (2026-09-16):** the shared
  compiler. Per-pattern engine selection in Rust: the `regex` crate serves every pattern
  it can express; `fancy-regex` 0.11 serves lookaround, backreferences, possessive
  quantifiers and atomic groups only (a quantified group alone never routes fancy;
  matching still delegates to the DFA where fancy has no hard node, so routing alone
  changes no value).
  Dangling `\1`…`\7` read as Java octal; `${…}` drops out of replacements before engine
  expansion (Spark's SQL substitution empties it); lookbehind answers semantically
  (see `spark_regex_lookbehind.rs`): no textual rewrite. A numbered backref to a named
  group numericizes (`\1` to a named group 1 reads as `\k<...>` does); when a numeric
  backref meets any named group, fancy-regex would refuse, so names strip to numbers.
  Invalid patterns
  raise Spark's `INVALID_PARAMETER_VALUE.PATTERN` naming the caller (`split` keeps its
  long-standing text). Overrun is disjunctive: backtrack budget 100M, a lookbehind
  search budget per call, or a
  catastrophic-class pattern (unbounded-quantified group over alternation or nested
  loops) on a haystack over 10000 bytes (Java's recursive matcher overflows there —
  measured `StackOverflowError` at 40000). Match collection walks the same
  mid-surrogate step as counting, so zero-width counts agree on supplementary text;
  the replace template borrows when no `$` is present.
  pins: java-regex-features-1/C-001 … C-005,
  C-007 (plain patterns stay on the DFA path)
- `spark_regex_lookbehind.rs` — **JAVA-REGEX-FEATURES-1 (2026-09-16):** the
  semantic lookbehind the engine compiles instead of normalizing (file-size split
  from `spark_regex_engine.rs`): each `(?<=X)`/`(?<!X)` becomes a `()` marker in a
  skeleton pattern, and a match verifies X against the text ending at the marker by
  scanning start positions down from it (bounded by X's static max length, else to
  zero; Java group numbering kept, interior refs refuse loud).
  pins: java-regex-features-1/C-001
  **Step 6:** split invalids name the translated pattern with the engine detail
  (the `Q15-13` text leg). pins: java-regex-features-1/C-008
  **Step 7:** scanners are char-aware with `\Q…\E` skipping (no single-byte emits,
  no quoted group miscounts). pins: java-regex-features-1/C-001
- `spark_regexp_match.rs` — **FN-FIX-2 (2026-09-04):** `regexp_like` / `rlike` /
  `regexp_replace` compile through `compile_spark_regex`. pins: fn-fix-2-string-rows/C-002
  **JAVA-REGEX-FEATURES-1 (2026-09-16):** the compiler moved to `spark_regex_engine`
  (per-function names for the invalid-pattern message; `is_match` / `replace_all` fallible
  for the overrun tripwire). pins: java-regex-features-1/C-001, C-002, C-004, C-005, C-006
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
  **DOOR-CONVERGE-2 round 3 (2026-09-15):** the shared compiler gains `translate_java_pattern`
  (`\Q…\E` quoting; lookaround/backreference/possessive refuse naming the feature) and the
  bounded `collect_matches_up_to`; unit tests live in `spark_regexp/tests.rs` (file-size split,
  move-only). pins: door-converge-2/C-008, C-009
  **JAVA-REGEX-FEATURES-1 (2026-09-16):** the compiler (scan, translation, all match walks)
  moves to `spark_regex_engine.rs`; this file keeps the UDF shells and row walks over the
  unified `SparkRegex`. `regexp_substr` nullability follows the arguments like its siblings
  (was hardcoded true). pins: java-regex-features-1/C-001, C-002, C-003, C-005, C-006
- `spark_split_part.rs` — **GT1-FIX F-6c / R3-1:** STRING `partNum` +
  Dictionary(_, Utf8); partNum 0 fail-loud.
- `spark_time_window.rs` — **FNP-WIN-1 (2026-09-15):** the `window` scalar UDF
  (`struct<start:timestamp,end:timestamp>` over the input timestamp type, UTC-epoch
  buckets, NULL time to NULL struct) plus the internal
  `__repark_window_starts__` list UDF that feeds the expansion rule; Spark
  interval-string durations with the oracle `CANNOT_PARSE_INTERVAL` text;
  month/year durations refuse loud (no fixed microsecond length).
  Audit round 16a splits the refusal: `window` keeps the Legacy
  `[_LEGACY_ERROR_TEMP_3231]` text while the session gap carries its own
  gapDuration text; non-positive window durations refuse
  `CANNOT_PARSE_INTERVAL`, and `Date32` coerces to `Timestamp(ns)`.
  Remediation 16a: `startTime` parses signed (negative/zero allowed);
  `slide > window` and `abs(start) >= slide` refuse with the oracle
  `PARAMETER_CONSTRAINT_VIOLATION` text; kernels batch (one downcast per
  batch, typed timestamp builders, streamed sliding starts) and a NULL time
  expands to an empty sliding list.
  pins: fnp-win-1/C-002, C-005
- `spark_window_time.rs` — **FNP-WIN-1 step 3 (2026-09-15):** the `window_time`
  scalar UDF (window `end` minus one microsecond, end field's timestamp type,
  NULL struct to NULL); non-struct arguments refuse loud. Remediation 16a
  reuses the batched timestamp kernels; behavior unchanged.
  pins: fnp-win-1/C-003
- `spark_session_window.rs` — **FNP-WIN-1 step 4 (2026-09-15):** the
  `session_window` marker UDF (GROUP-BY-only; per-row evaluation refuses loud)
  plus the `__repark_session_gap__` / `__repark_ts_micros__` /
  `__repark_session_assemble__` internal UDFs the sessionize rule threads
  through its lag / flag / running-sum / min-max plan. Remediation 16a
  replaces `__repark_session_gap__` with `__repark_session_end__`, which
  answers each row's calendar session end (month/year gaps via the local
  civil month-end helper over `datetime::spark_add_months`, NULL /
  non-positive gaps to NULL for the drop filter) with one parse per distinct
  gap string per batch; the assemble UDF takes an Int64-micros or a
  TIMESTAMP end; `Date32` coerces to `Timestamp(ns)`.
  pins: fnp-win-1/C-004, C-008. Round 2 (2026-09-15) shares the two-spec
  `[_LEGACY_ERROR_TEMP_1039]` refusal text (`MULTIPLE_SESSION_EXPRESSIONS`)
  with the analyzer and SQL staging; the Python door's duplicate-name error
  is registry row WIN-4. pins: fnp-win-1/C-004
- `registration.rs` — **FNP-WIN-1 step 4 (2026-09-15):** the `analyzer_rules()`
  home moved out of `lib.rs` so the crate root stays under its `check_lib_rs`
  ceiling; `SparkSessionWindow` registers beside the window rules.
  **FNP-GEN-1 step 2 (2026-09-16):** `generator::GeneratorRewrite` joins the tail
  of the list, after the closing `TypeCoercion`, so the rule sees post-coercion
  projections on both doors.
  **Step 3 (run 18a):** `csv::fold::CsvFold` registers before `TypeCoercion`, so a
  literal options map still carries its declared types when the rule reads the
  parse mode; the `csv` module registers its UDFs in `lib.rs` beside the other
  families.
  **Step 4a (run 18a):** `csv::schema_of_csv` joins the `csv::functions()`
  registry with the Spark `CSVInferSchema` ladder; folding lands in step 4b.
  **Step 4b (run 18a):** `CsvFold` folds the literal call and unaliases it inside
  `from_csv`; `from_csv` answers a `Null` placeholder until the fold lands, and
  `expr_fn` gains the `schema_of_csv` builder the Python dispatch arm uses.
  **Gate pass (run 18a):** the round's `csv` code is clippy-clean under
  `-D warnings`; `expr_fn::from_csv` / `schema_of_csv` carry `#[must_use]`.
  **Remediation (run 18a):** the `csv` root owns the session-zone stamp helpers
  (`CsvStampParsers`, the default stamp ladder, `infers_as_timestamp`); detail
  lives in `csv/map.md`.
  pins: fnp-win-1/C-004, C-008, fnp-gen-1/C-002, C-003, C-004, C-006, L-002, L-003,
  L-004, L-005, R-18a-14, PERF-001, PERF-002, PERF-004, PERF-005, PERF-006
- `lib.rs` — crate-root stays at **182** under `check_lib_rs` (D-8 one-time
  FNP-WIN-1 grant; step 4 moved the `analyzer_rules()` home to
  `registration.rs`).
- `higher_order/` — FNP-4c Spark higher-order kernels (`transform`, `filter`, `forall`,
  `aggregate`/`reduce`, `zip_with`, `transform_keys`, `transform_values`, `map_filter`,
  `map_zip_with`) plus native `exists`. Registry both doors
  read. pins: fnp-4c-higher-order-kernels/C-001, C-002, C-003, C-004, C-005, C-006, C-007,
  C-008, C-009, C-010, C-011, C-013, C-014
  **FNP-8 (2026-09-06):** `exists` is a native kernel (the FNP-4a `array_any_match`
  alias is gone); `aggregate` refuses init/merge width mismatches instead of coercing.
- `try_invert/` — FNP-7a/7b scalar `try_*` kernels (NULL instead of raise). `try_element_at`
  aliases `element_at`. `try_sum` reuses datafusion-spark; `try_avg` is its own UDAF
  (decimal overflow NULL; INTERVAL input is the FNP-11 loud refuse).
  pins: fnp-7-try-inversions/C-001, C-002, C-004, C-005, C-006, C-007, C-008, C-009, C-010,
  C-011, C-012, C-014, C-015, C-018, C-019
- `temporal_ctor/` — FNP-11A scalar temporal kernels (constructors, intervals,
  month arithmetic, zone conversion, add/diff, dateadd/datediff arity routes;
  scalar args and column casts hoist out of the row loops). Details in
  [`temporal_ctor/map.md`](temporal_ctor/map.md).
  pins: fnp-11a/C-002, C-003, C-004, C-005, C-011, C-013
- `lib.rs` — crate-root stays at **175** under `check_lib_rs` with `pub mod timestamp_type`.
  **FNP-BITMAP-FACADE-1 (2026-09-15):** `mod bitmap_agg;` flips to `pub mod bitmap_agg;`
  (same line count) so `repark-python`'s `unary_aggregate_udaf` can reach the three
  `bitmap_agg` UDAF constructors the way it reaches `aggregate`'s.
  pins: fnp-bitmap-facade-1/C-001
- `lib.rs` — crate-root stays at **162** under `check_lib_rs` with `pub mod timestamp_type`
  (D-8 one-time FNP-WIN-1 grant; step 4 moved the `analyzer_rules()` home to
  `registration.rs`).
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
  **SET-ANSI-RUNTIME-1 (2026-09-15):** `parse_runtime_spark_sql_ansi_enabled` (the
  runtime gate: case-insensitive `true`/`false` only — `1`/`yes`/padded values refuse
  with Spark's `INVALID_CONF_VALUE.TYPE_MISMATCH`; the lenient builder parser is frozen).
  The function carries `#[allow(clippy::missing_errors_doc)]` (R-17c-3: no Rust `///`).
  Its carrier `set` refusal now points at the runtime `SET` spelling.
  pins: set-ansi-runtime-1/C-001
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
  **SQL-SET-DOOR-1 (2026-09-14):** `current_timezone_udf` — the SQL-door `current_timezone()`
  Spark answers in `SELECT current_timezone()` — reads this same carrier and returns a
  non-nullable Utf8 (a `ReturnFieldArgs` field so DataFusion plans the not-null `string`
  Spark promises). Its `name()` is `&'static str` — a literal bound, what pedantic wants.
  It registers through `instant_ts::functions()` — the session-zone temporal
  family — rather than a `lib.rs` line, because the crate root sits on its exact line ceiling.
  Pins: `session_time_zone::tests::current_timezone_*`.
  **SET-ANSI-RUNTIME-1 (2026-09-15):** `SessionTimeZoneConfig::set_zone` swaps the live
  carrier value after the session's runtime gate already accepted it (no re-validation).
  Its carrier `set` refusal now points at the runtime `SET TIME ZONE` spelling.
  **R-17c-4:** the carrier holds `display` (raw echo) + canonical `zone` (extractor reader).
  **R-17c-6:** the in-crate canonicaliser mirrors the core gate (exact-case prefixes,
  zero-seconds to `±HH:MM`) and asserts the same shared table. pins: set-ansi-runtime-1/C-002
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
  **DECIMAL-CACHE-1 (2026-09-15):** measurement-only so far — the test-only
  `measure_cache_seam_logical_vs_physical_decimal_field` records unanalyzed, analyzed and
  physical fields per probe (facade plans pin the unanalyzed `(38,10)` schema; facade
  `* Int32(5)` analyzes to `(38,6)` where the oracle demands `(38,8)`).
  pins: decimal-cache-1/C-001
  **DECIMAL-CACHE-1 seam fix (2026-09-15):** the rule is additionally seated pre-coercion
  (see `lambda_rebind.rs`); product logic unchanged. Tests add a prod-like assembly helper
  (`Analyzer::new().rules` + preparation + `analyzer_rules()`, ANSI on) with six pins:
  facade `*`/`+`/col shapes, SQL `* 5`, explicit-cast preservation, explicit `(1,0)` cast —
  each asserting logical == physical (name, type, nullability, metadata) and the value.
  pins: decimal-cache-1/C-002
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
  rule, installed on BOTH doors via `install_shared_analyzer_rules` (defined here since FNP-11B step 3 and re-exported from the crate root, so `repark_functions::install_shared_analyzer_rules` and run 16b's `session.rs` call are unchanged; the session The function carries no doc line by the comment rule; this row is its description: the analyzer rules both doors install (integer overflow, boolean-to-decimal casts; the TIME guard left for `analyzer_rules()` in remediation round 1).
  installer calls it in place of the integer-only one — same line count, so the
  session map needs no ratchet): `CAST(bool AS DECIMAL(p,s))` becomes a
  precision-carrying UDF (true → 1, false → 0 at scale; per-row nulls). The UDF
  reads ANSI once at plan time for the overflow edge (`(2,2)`: raise vs NULL);
  its return field marks overflow-exposed targets nullable. Registry
  `CAST-BOOL-DEC-1`.
  pins: nullability-2/C-003
- `int_to_binary.rs` — **BL-11 (2026-09-16):** the `IntToBinaryCast` analyzer rule,
  slotted after `SparkExprSemantics` so `SparkIntegerLiteral` has already narrowed bare
  literals: a `CAST(<integral> AS BINARY)` becomes the `__repark_int_to_binary__` UDF
  (big-endian bytes of natural width, NULL propagates, nullability follows the input)
  only when ANSI is off; the same rule refuses every other `→ BINARY` cast with Spark's
  `DATATYPE_MISMATCH` (`CAST_WITH_CONF_SUGGESTION` plus the conf remedy for integrals
  under ANSI on, `CAST_WITHOUT_SUGGESTION` otherwise, `TRY_CAST` always without). The
  rule sees both doors because the SQL door analyzes eagerly at build and the native
  `DataFrame` path analyzes the same rule set at collect. Registry `BL-11`.
  pins: bl-11-numeric-binary/C-001, C-002, C-003
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
  name clash) + Q1 percentile aliases + `datetime::functions()` date shims (the
  single-use helper is inlined; the root stays under its ceiling) +
  `analyzer_rules()` (`SparkIntegerLiteral` → `LambdaRebind` → `SparkDecimalPrecision` →
  `SparkDecimalRewrite` → `SparkIntegerOverflow` → Spark semantics +
  cardinality + instant_ts + a closing `TypeCoercion` — the narrowing runs after
  DataFusion's own coercion and re-opens mixes, so the closing pass shuts them before the
  next rule (pins: types-1/C-007) + `LambdaRebind` twice — after the integer narrowing
  and final (pins: fnp-8/C-004); the
  session installs them via the Spark door's `SessionExtension`;
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
  **DOOR-CONVERGE-2 (2026-09-15):** planner-embedded `array_concat` nodes (the `||`
  operator over equal lists, plus direct calls) rewrite onto the door-converged `concat`
  UDF when every argument is list-shaped or NULL — the nested planner bakes its own UDF
  in, so only the analyzer sees the name. pins: door-converge-2/C-001;
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
- `java_double.rs` + `java_double/` — **JAVA-DOUBLE-FD-1 port (2026-09-15):**
  `dtoa.rs` (independent JDK 17 `FloatingDecimal` port) + `bigint.rs` (ported
  `FDBigInteger`), `tests_corpus.rs` (byte-equality over
  `REPARK_JDK17_TOSTRING_CORPUS`, green on all 39,427 rows) with full
  `tables_doubles.rs` (82 non-shortest + 100 random) / `tables_floats.rs`
  (579 non-shortest + 100 random). `format_float.rs` (C-003:
  `__repark_format_float__` exact-decimal HALF_UP for single-verb `%f`/`%F`);
  the rule also folds Java-suffixed string literals to DOUBLE/FLOAT with
  one-level projection propagation (C-006).
  **JAVA-DOUBLE-STR-1 (2026-09-15):** the single home of
  Java float spellings — `java_double_text` / `java_float_text` (moved from
  `json/reader.rs`; `reader.rs`, `decode.rs` and `to_json.rs` import from here,
  so no second formatter exists) plus the Spark-door `CAST(<FLOAT|DOUBLE> AS
  <string>)` rewrite. `SparkFloatToStringCast` (registered in
  `analyzer_rules()` right after `SparkExprSemantics`) turns the cast into the
  embedded `__repark_float_to_string__` UDF, so SQL, `selectExpr`, `F.expr` and
  `col.cast` all answer Java text typed `Utf8`; the native ANSI door never
  installs the rule and keeps Arrow text. Shortest-`e` rendering with Java
  thresholds (plain decimal for `1e-3 <= |x| < 1e7`, `d.dddEn` outside, one
  digit after the point); `Double.MIN_VALUE` (±bit pattern 1) spells
  `4.9E-324` and `Float.MIN_VALUE` spells `1.4E-45` where Rust shortest prints
  shorter (oracle BL7-16 / JD-cast-13, J10-float-min). Round 2 formats into
  caller stack buffers (`with_java_double_text` / `with_java_float_text`, len
  helpers for the length kernels) so no per-value `String` is allocated; the
  rule is now `SparkFloatStringify` with `TRY_CAST`, `LIKE`, `CASE`,
  `format_string` `%s`-only and single-verb `%f` arms, `array_join`, suffixed-literal
  folding and one-level projection propagation, non-literal STRING to FLOAT/DOUBLE
  casts routed to the column parse kernel, plus bad-literal folding to
  `CAST_INVALID_INPUT` (ANSI on) or `NULL`. It rides pre-coercion (one slot in
  the shared insertion) and post-coercion (in `analyzer_rules()`); both seats
  are idempotent. The JDK-longhand remainder is JAVA-DOUBLE-FD-1.
  **FNP-4B round 8 (2026-09-15):** the text helpers are `pub` (module `pub`) so the
  Spark door names suffix-literal fields from value text.
  **JAVA-DOUBLE-FD-1 fix round 1 (2026-09-15, R-17c-2):** `rewrite_float_cast`
  exempts `CAST(__repark_suffix_literal__(…) AS FLOAT|DOUBLE)` from the
  non-literal parse-kernel route so the door's `FoldSparkNumericCasts` still
  folds it to a non-null literal; `SUFFIX_LITERAL_NAME` is the `pub` single
  source here that repark-spark re-exports.
  pins: java-double-str-1/C-003, C-004, C-009, C-010, C-011, C-012, C-013, C-014, C-015, C-016
- `string.rs` — `SparkSubstring` (`substring`, alias `substr`; audit #6): Spark's
  `UTF8String.substringSQL` character-based semantics — pos 0 acts as 1, negative pos counts
  from the end, the window clips (never errors), negative len → `''`, NULL args → NULL.
  **D2 `SparkConcat` (`concat`)** overwrites `datafusion-spark`'s: coerce args to `Utf8`
  (incl. non-string Spark stringify), any-NULL → NULL, always emit `Utf8` (never `Utf8View`)
  — closes the plan-promises-Utf8 / kernel-returns-Utf8View panic that blocked TPC-DS
  Q5/Q80/Q84 SQL `concat` on the Arrow path. Unit pins:
  `concat_register_all_overwrites_datafusion_spark` (name overwrite);
  `concat_array_any_null_propagates_per_row` (Apply null-mask path).
  **JAVA-DOUBLE-STR-1 (2026-09-15):** `FLOAT`/`DOUBLE` args keep their type
  through coercion and stringify inside the kernel with the shared Java
  formatter (registry BL-7); every other non-string arg still coerces to `Utf8`.
  pins: java-double-str-1/C-005
  **Round 2 (2026-09-15):** the owned `__repark_array_join__` shim (built only by
  the float-stringify rule for float-element lists) joins through the same
  stack formatter; string and other leaves mirror the upstream kernel.
  pins: java-double-str-1/C-010
  **DOOR-CONVERGE-2 (2026-09-15):** one UDF, three arms by argument family — all-array args
  widen through `collection/concat_array.rs` (common element type, OR `containsNull`, NULL
  array → NULL row, `DATA_DIFF_TYPES` on a non-array sibling); any-`Binary` args stay
  `Binary`; everything else keeps the `Utf8` path. The facade `PyColumn::concat` embeds the
  same UDF, so both doors resolve one kernel. The array coerce validates but never casts
  (provisional widths resolve post-narrowing). pins: door-converge-2/C-001
  **DOOR-CONVERGE-2 round 3 (2026-09-15):** all-`Binary` stays `Binary`, any other mix
  with `Binary` answers `STRING` (binary read as UTF-8 text); unit tests live in
  `string/tests.rs` (file-size split, move-only). pins: door-converge-2/C-008
- `spark_split.rs` — **DOOR-CONVERGE-2 (2026-09-15):** door-converged `split`
  (Java-regex pattern through the shared `compile_spark_regex` + `collect_matches`
  stepping, `limit` > 0 caps with the remainder last, `limit` ≤ 0 keeps trailing
  empties, empty pattern splits per character, NULL in → NULL out, numeric first
  argument casts to string; overlapping matches resume one char past the last start
  (`find_at`, cures `'.'`-pattern Q12-50/51). The facade arm lives in `dispatch_spark.rs`,
  but the Python `F.split` still raises `UnsupportedOperationException` before reaching
  it — P2 hand-off to run 16a. pins: door-converge-2/C-004
  **DOOR-CONVERGE-2 round 3 (2026-09-15):** scalar patterns compile once, pattern columns
  resolve through an LRU(64) `PatternCache`, plain literals take the `str` path, and
  `limit` > 0 stops the match walk after `limit - 1` (equivalence-pinned). pins:
  door-converge-2/C-009
- `spark_sequence.rs` — **DOOR-CONVERGE-2 (2026-09-15):** door-converged `sequence`
  (int widths kept, descending default step, dates with 1-day default and month steps,
  timestamps with interval steps, NULL bound/step → NULL with `containsNull=false`, zero
  or wrong-sign step raises Spark's `Illegal sequence boundaries` text, decimal bounds
  refuse `SEQUENCE_WRONG_INPUT_TYPES`). `coerce_types` validates but never casts: the
  built-in `type_coercion` runs before `spark_integer_literal` narrowing, so a widening
  coerce would shield provisional `Int64` literals behind `CAST`s and defeat the narrow —
  widths resolve in the return type after narrowing, and the kernel casts internally.
  Month stepping reuses `datetime::spark_add_months` (now `pub(crate)`). The facade arm
  moved to `dispatch_spark.rs` with the `refuse_facade_literal_expansion` ceiling kept;
  the plan-time `ArrayCardinalityCeiling` still fires on the `sequence` name.
  pins: door-converge-2/C-003
  **DOOR-CONVERGE-2 round 3 (2026-09-15):** month steps compute element `i` from the start
  (`start + months × i`); the runtime cap reuses the literal refusal text for column stops;
  closed-form counts reserve up front at native width with a scalar fast path. The int/date/
  timestamp row kernels live in `spark_sequence/rows.rs` (file-size split, move-only).
  pins: door-converge-2/C-007, C-009
- `spark_reverse.rs` — **DOOR-CONVERGE-2 (2026-09-15):** door-converged `reverse`
  (overwrites the string-only DataFusion kernel): arrays reverse element order with the
  element type, `containsNull` and nullability kept; strings reverse by character; untyped
  `NULL` answers a NULL `STRING`; any other type refuses
  `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`. The facade arm moved to `dispatch_spark.rs`,
  so both doors resolve this kernel. pins: door-converge-2/C-002
- `instant_ts.rs` — overwrite `now` / `current_timestamp` / `to_timestamp` with Arrow
  `Timestamp(µs, UTC)`. Zoneless LTZ inputs (`TIMESTAMP '…'`,
  zoneless `to_timestamp`, `CAST(str|date|ntz AS TIMESTAMP)`) in the session zone; a
  zone-suffixed string is not localized. Analyzer rule `spark_ltz_timestamp_cast` still wraps
  integer `CAST AS TIMESTAMP`. **Q10:** when `spark.sql.timestampType=TIMESTAMP_NTZ` the
  same rule resolves bare `TIMESTAMP` literals / casts to naive µs (no localization);
  `to_timestamp` / `now` stay LTZ. **SQL-SET-DOOR-1 (2026-09-14):** its `functions()` vector
  also carries `session_time_zone::current_timezone_udf` — the `lib.rs`-free registration
  slot for the session-zone family's SQL-door `current_timezone()` (crate root is at its
  line ceiling). Pins: `instant_ts::tests::*`.
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
- `java_datetime.rs` — **FNP-11B step 2 (2026-09-15):** the one shared Java-datetime-pattern
  parser (card D-1) behind `to_date` / `to_timestamp` / `unix_timestamp` with a format.
  Tokenizes `yyyy`/`yy`/`M`/`MM`/`MMM`/`MMMM`/`d`/`H`/`h`/`m`/`s`/`S`/quoted literals;
  an unquoted `Y` run refuses
  `[INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_PATTERN_RECOGNITION]`; other failures
  render Spark's `[CANNOT_PARSE_TIMESTAMP]` text under ANSI and NULL otherwise. Constant
  patterns compile once per batch (`FormatPlan::Shared`, per-row fallback); each input
  column is cast once per batch. Batch entry points `to_date_with_format` /
  `stamps_with_format_column` / `unix_seconds_with_format`; non-string inputs keep the
  1-arg path with the format ignored. `to_timestamp` 1-arg malformed strings translate to
  `[CAST_INVALID_INPUT]` under ANSI and NULL otherwise. SQL-door double-quoted pattern
  literals stay a run-16c parser seam (cells 129/130).
  pins: fnp-11b/C-002, C-003, C-004; `java_datetime::tests::*`.
  **TYPES-1 (2026-09-05):** `parse_session_zone` is `pub(crate)` for
  `spark_from_unixtime.rs`. pins: types-1/C-006
- `timestamp_ltz_ntz.rs` — **FNP-11B step 3 (2026-09-15):** `to_timestamp_ltz` /
  `to_timestamp_ntz` / `try_to_timestamp` on the step-2 parser (card D-1, no new
  parser). `to_timestamp_ltz` forwards both arities to the `to_timestamp` kernel;
  `try_to_timestamp` forwards with the ANSI extension cloned off so data errors
  answer NULL under both ANSI settings while pattern refusals still raise.
  `to_timestamp_ntz` emits naive walls: the format arm reuses the
  `plan_format_column` / `parse_wall_or_null` primitives through a module-local
  walls helper (the `stamps_with_format_column` loop stays untouched behind its
  pins, and `java_datetime.rs` sits 3 lines under its ceiling), while the 1-arg
  arm strips a zone/offset suffix and round-trips the wall through the
  `to_timestamp` kernel before un-localizing, so offset strings keep their
  written wall and malformed strings retarget `[CAST_INVALID_INPUT]` at the
  `TIMESTAMP_NTZ` name. pins: fnp-11b/C-002, C-003, C-004;
  `timestamp_ltz_ntz::tests::*`.
- `time_family.rs` — **FNP-11B step 4 (2026-09-15):** the TIME family behind one
  refusal kernel (`TimeRefusal`, unconditional `[UNSUPPORTED_TIME_TYPE]`) serving
  `make_time` / `to_time` / `time_diff` / `time_trunc` on both doors;
  `CurrentTime` answering session-zone `time64[ns]` with Spark's
  `[DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE]` past precision 6; `SparkTypeof`
  spelling Arrow types the way Spark does (`time(6)`, `timestamp_ntz`);
  `DatePartWithoutTime` wrapping the `datetime.rs` hour/minute/second kernels so
  a TIME input refuses while timestamps answer (the file's own
  `hour_minute_second_accept_time_and_timestamp` pin is rewritten to the
  refusal, net −1 line with the ceiling row ratcheted 1700 to 1699);
  `TimeCastGuard` analyzer rule refusing any
  `CAST`/`TryCast` to TIME whose inner is not a string literal, which is also
  what poisons plans reading the TIME frame. `TIME'…'` literals stay literal
  casts and answer; `CAST('str' AS TIME)` answers where Spark raises (residual).
  Registration folds into `instant_ts::functions()`; the guard wires into
  `analyzer_rules()` since remediation round 1 (both doors refuse the same
  CAST-to-TIME text at build). pins: fnp-11b/C-002, C-003, C-004, C-005;
  `time_family::tests::*`.
  **FNP-11B remediation round 1 (2026-09-16):** `interval_avg` keeps exact-width
  (`i128`) sums with the overflow flag recomputed per add/subtract/merge and
  merges every state row (L-001, L-003); `to_char` resolves its input arm once
  with one cached mask; `make` precasts DATE/TIME once for the 2/3-arg arm;
  `to_number`/`to_binary` cache one format; `ntz_single` strips through one
  builder. PERF-001 stays per-row: the batch forward is unsound against the
  inner batch-atomic error (see the ledger). pins: fnp-11b/C-006, C-007.
  **FNP-11B step 6 (2026-09-15):** `SparkTypeof` spells `array<…>` / `map<…>` /
  `struct<…>` recursively through the same table (no second table; the
  `repark-spark` renderer stays uncalled across the crate edge) and wrong arity
  raises Spark's `[WRONG_NUM_ARGS.WITHOUT_SUGGESTION]` verbatim.
  pins: fnp-11b/C-005.
- `interval_avg.rs` — **FNP-11B step 4 (2026-09-15, BL-13):** the
  `IntervalAvgAccumulator` behind `avg` / `try_avg` over interval and duration
  inputs (Spark `Long`-micros accumulation with checked adds, round-half-away
  division, `MonthDayNano` out). Overflow answers NULL on `try_avg` and raises
  Spark's `[INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION]` on `avg`; all-NULL
  answers NULL; mixed month/day-time inputs follow the overflow arm. `aggregate.rs`
  only gains the signature/return-type/accumulator/state-fields arms; the
  `[FNP-11]` refusals are gone. pins: fnp-11b/C-006;
  `interval_avg::tests::*`, `aggregate::tests::*`.
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
  **ARRAY-NULL-1 (2026-09-14):** `collection/array_append.rs` registers
  `spark_array_append_udf`/`spark_array_prepend_udf` last in `functions()` —
  DataFusion's kernels drop the input array's null buffer, so the shims graft it
  back and serve both doors under Spark's `(array, element)` order. Element
  coercion is Spark's recursive `findTightestCommonType`, validated (never
  plan-cast) by a user-defined signature and converted at invoke time in
  `collection/array_append/coerce.rs` — see [collection/map.md](collection/map.md).
  pins: array-null-1/C-003, C-004
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
- `expr_fn.rs` — logical-`Expr` builders for date, string, collection, URL, bitmap, temporal, and higher-order
  functions. Builders embed the same shims registered by the SQL door, including `unix_date`,
  `bit_length`, regexp/split functions, `shuffle`, `map_from_entries`, and `str_to_map`, so facade
  columns remain self-contained without a `SessionContext`.
  **TYPES-1 (2026-09-05):** `from_unixtime` builder over the new UDF. pins: types-1/C-006
  **FNP-11B step 5 (2026-09-15):** `to_char_family` builder dispatching the
  four formatting names onto the `try_invert::strict` kernels by name.
  pins: fnp-11b/C-001, C-002
  **SQL-LITERAL-TYPING-1 remediation round 1 (2026-09-16):** the `factorial`
  builder embeds the `Factorial` shadow UDF (not upstream's), keeping the
  facade kernel identical to the SQL-door registration; the door-parity test
  holds it. pins: sql-literal-typing-1/L-002

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
- **DOOR-CONVERGE-1 rebase (2026-09-15):** `collection.rs` keeps main's `array_append` / `array_prepend` shims (ARRAY-NULL-1) beside this unit's `array_contains` / `size` modules; the round-2 `make_array` shim stays removed (R-10).
- **FNP-11A R3 (2026-09-15):** `expr_fn::datediff` is the Spark `datediff` spelling. The function door routes two arguments to `date_diff` and three to `timestampdiff` through `temporal_ctor::date_alias` (pins: fnp-11a/C-019).
- **FNP-6D-FOLLOWUP-1 rebase (2026-09-15, run 16a):** after #612 moved the Java text helpers into `java_double.rs`, `bitmap_agg.rs` imports `java_double_text` / `java_float_text` from `crate::java_double`; `json.rs` keeps `mod reader;` private as on main.
- **FNP-WIN-1 squash onto 4bd43fc8 (2026-09-15, run 16a):** `lib.rs` drops its `use std::sync::Arc;` — `analyzer_rules()` and its `Arc<dyn AnalyzerRule>` list now live in `registration.rs`, so the crate root no longer names `Arc` (clippy `-D warnings`).
- **FNP-GEN-1 verification-critic fix-up (2026-09-16, run 17a):** `generator.rs::ordinality` packed
  positions densely from 0 but **cloned the input's offset buffer**. Arrow's `ListArray::slice`
  keeps the whole values buffer and slices only the offsets, so a sliced list whose first offset is
  not 0 gave `offsets.last() > values.len()` and `ListArray::new` **panicked**. The unit's own
  frames and every oracle cell are unsliced with first offset 0, so no pin reached it; a slice
  arrives in ordinary execution after a limit, a take or a concat. It now builds a fresh
  `OffsetBuffer` from the measured lengths, and the `as_list_array` panic-downcast at the call site
  became an `exec_err`. Regression pin: `ordinality_packs_positions_for_a_sliced_list`.
  pins: fnp-gen-1/C-002

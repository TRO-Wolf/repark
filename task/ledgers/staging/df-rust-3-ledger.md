# Charter ledger — DF-RUST-3 · `DataFrame.freqItems` / `DataFrameStatFunctions.freqItems` / `DataFrame.transpose`

**Date:** 2026-09-15 (round 1), 2026-09-16 (round 2 remediation) · **Branch:**
`feat/df-rust-3` · **Base:** `0355ef5e` (round 1), `a2bffafa` (round 2) · **Model:**
swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `DF-FREQITEMS-1` and `DF-TRANSPOSE-1`, appended beside DF-FOREACH-1 /
DF-OBSERVE-1 in
[../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).

**Why now.** The 1.5 PySpark-parity campaign: `DataFrame.freqItems` and
`DataFrame.transpose` are absent and `DataFrameStatFunctions.freqItems` is a disclosed
"not supported yet" refusal (EX-DF-19). The run-16b oracle recorded PySpark 4.1.2 classic
answers for both families
([python/repark/tests/facade_df_rust3_oracle.json](../../../python/repark/tests/facade_df_rust3_oracle.json)
— `freq_*` / `transpose_*` cells, plus the `*_tuple` duplicates from the dfsubq probe).
Both are real Rust kernels: a DataFusion UDAF implementing Spark's `FreqItemCounter`
(bytecode-verified `javap` of
`org.apache.spark.sql.execution.stat.FrequentItems`, spark-sql_2.13-4.1.2.jar) and an eager
transpose kernel implementing `ResolveTranspose` (bytecode-verified `javap` of
spark-sql_2.13-4.1.2.jar `ResolveTranspose`/`Transpose`).

**Not in this unit:** no SQL doors — Spark has no SQL spelling for `freqItems`
(`collect_frequent_items` is not a registered SQL function) and `|> TRANSPOSE` answers
`PARSE_SYNTAX_ERROR` in 4.1.2 (cell `transpose_sql`); the function registry
(crates/repark-functions, function_dispatch.rs) and the SQL parser (crates/repark-spark)
belong to other runs today. The `metadata_*` oracle cells belong to a later unit.

## PROPOSITION LEDGER — DF-RUST-3 — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `DataFrame.freqItems(cols, support=None)` and `DataFrameStatFunctions.freqItems(cols, support=None)` share one body and answer the oracle's `freq_default`, `freq_support_05`, `freq_stat`, `freq_tuple`, `freq_support_one`, `freq_empty_df`, `freq_many_distinct`, `freq_struct_col`, `freq_array_col`, `freq_bool_date`, `freq_decimal_ts`, `freq_dup_col` / `freq_dup_col_tuple` cells: one row, one `array<T>` column per requested name (input element type preserved), named `<name>_freqItems`, nullable false, duplicates kept, empty frame → one row of empty arrays, `freqItems([])` → the frame's row count of empty rows with no columns. Result arrays compare sorted (Spark hash-map order). | `python/repark/tests/test_df_rust3_freqitems_transpose.py` freq pins driven from the fixture. | **PROVEN** | `statistics.freqItems` is the single body bound on both doors; `crates/repark-core/src/freq_items.rs` implements `FreqItemCounter` per R-1 (capacity `floor(1/support)`, add/merge exact, NULL a key, no eval threshold); all named cells green (54 passed in the unit file). |
| C-002 | Argument shapes: `cols` a str raises `PySparkTypeError` `NOT_LIST_OR_TUPLE` (`freq_string_arg` byte-exact); a `Column` element raises `PySparkTypeError` `NOT_ITERABLE` `Column is not iterable.` (`freq_col_obj`); `support=None` → 0.01; support outside `[1e-4, 1]` raises `IllegalArgumentException` `requirement failed: Support must be in [1e-4, 1], but got <java-double>.` (`freq_support_tiny` `1.0E-5`, `freq_support_gt_one` `1.5`); an unknown column raises `AnalysisException` `UNRESOLVED_COLUMN.WITH_SUGGESTION` with the sorted proposal (`freq_missing_col`). Per R-4 int support is accepted as float; bool/str/other raise `PySparkTypeError` `NOT_FLOAT`. | The argument pins in the same module. | **PROVEN** | Validation lives in `statistics.freqItems` before any plan; the `1.0E-5` rendering goes through `repark_functions::java_double`-style facade formatting; every named error cell asserted byte-exact against the fixture. |
| C-003 | `DataFrame.transpose(indexColumn=None)` answers the oracle's `transpose_default`, `transpose_index`, `transpose_index_col_obj`, `transpose_int_index`, `transpose_bool_float_index`, `transpose_index_names_order`, `transpose_dup_index`/`_tuple`, `transpose_null_index`, `transpose_nulls_values`, `transpose_key_col_clash`/`_tuple`, `transpose_mixed_types`, `transpose_map_value`, `transpose_empty`, `transpose_single_col` cells: `key` string non-null first column; one column per non-null index row sorted ascending on the raw index (duplicates keep a column each, duplicate names allowed); value cells cast to the tightest common type; empty input → only `key` with one row per value column; no value columns → `key` plus index columns and zero rows. | The transpose pins in the same module. | **PROVEN** | `crates/repark-core/src/transpose.rs` implements `ResolveTranspose` per R-7/R-8 (null-index filter, `spark.sql.transposeMaxValues` default 500, stable ascending sort on the raw index, `findTightestCommonType` fold). `transpose_dup_index` accepts either legal equal-key dict (Spark's order unspecified); positional truth pinned by `transpose_dup_index_tuple` `('a', 1, 2)`. |
| C-004 | `transpose` errors: non-atomic index → `AnalysisException` `TRANSPOSE_INVALID_INDEX_COLUMN` `42804` with `reason` `Index column must be of atomic type, but found: ArrayType(IntegerType,true)` (`transpose_array_index`, Catalyst type-name rendering); no tightest common type → `TRANSPOSE_NO_LEAST_COMMON_TYPE` `42K09` naming the first failing pair in frame order (`transpose_incompatible` `"INT"`/`"STRING"`, `transpose_index_other` `"STRING"`/`"INT"`, `transpose_long_decimal` `"BIGINT"`/`"DECIMAL(5,2)"`); >500 index rows → `TRANSPOSE_EXCEED_ROW_LIMIT` `54006` (`transpose_too_many`, params `config`/`maxValues`); unknown index name → `UNRESOLVED_COLUMN.WITH_SUGGESTION` `42703` (`transpose_missing_index`). | The error pins in the same module. | **PROVEN** | Rust embeds `[CONDITION] … SQLSTATE:`; `surface_a.transpose` catches and attaches `_spark_error_class` / `_spark_message_parameters` / `_spark_sql_state` so `getErrorClass`/`getMessageParameters`/`getSqlState` answer the fixture exactly. |
| C-005 | The unit's rows land: registry `DF-FREQITEMS-1` / `DF-TRANSPOSE-1` appended (the F-4 support-type divergence note and the hash-order note recorded); the old `freqItems` refusal and its pins flipped; `docs/examples/dataframe/` example with `COVERS` for the three public names; inventory and EX-0 count bumped by exactly the added names; `EXPECTED_DATAFRAME_DIR` gains `freqItems`/`transpose`, `EXPECTED_NEW_PACKAGE_SUBMODULES` and `EXPECTED_NEW_CORE_SUBMODULES` gain the new module(s); every touched `map.md` in lockstep. | The registry diff, the maps, the gates. | **PROVEN** | EX-DF-19 flipped to `FIXED 2026-09-15 (DF-RUST-3)`; registry rows DF-FREQITEMS-1/DF-TRANSPOSE-1 appended; `docs/examples/dataframe/freq_items_transpose.py` covers all three names (EXAMPLE-OK); inventory +2, backlog −1 (baseline 111), EX-0 raw walk 1059 → 1061; `EXPECTED_DATAFRAME_DIR` gained both names; `core.py` held at 4015, `writer_readwriter.py` at 1101. |
| C-006 | No regression: `test_dfcore_1_exports.py`, the freqItems refusal pins' replacements, `make verify`, `cargo test -p repark-core`, the facade files that touch statistics/writer paths, and the parity suite stay green on the rebuilt release native. | The gates table. | **PROVEN** | See the gates table: unit file 54 passed, exports 10 passed, facade suite 8314 passed, `cargo test --locked --workspace` green, parity suite 757 passed, `make ci` clean. |
| C-007 | `freqItems` key equality is IEEE `==` for `Float32`/`Float64` only (R-9): `+0.0` and `-0.0` are one key with the first-inserted spelling surviving; every `NaN` row equals nothing — the insert always happens and the lookup always misses — so two NaNs at capacity 1 answer `[]` and at the default capacity answer `[nan, nan]`; `Float32` behaves identically. Nested floats keep bit-exact equality inside `array`/`struct` keys (`[nan]` == `[nan]`, `[0.0]` != `[-0.0]`, `{a: nan}` == `{a: nan}`), matching Spark's boxed-container `equals`; NULL still dedupes. | `python/repark/tests/test_df_rust3_freqitems_transpose.py` float-key pins over fixture cells `freq_signed_zero_cap1`, `freq_signed_zero_default`, `freq_signed_zero_cap2_with_one`, `freq_signed_zero_float_cap1`, `freq_signed_zero_neg_first_default`, `freq_nan_keys_cap1`, `freq_nan_keys_default`, `freq_two_pos_zero_cap1`, `freq_array_pm_zero_cap1`, `freq_array_nan_cap1`, `freq_struct_nan_cap1`; Rust unit tests in `freq_items.rs`. | **PROVEN** | `FreqKey(ScalarValue)` wraps the accumulator key: `PartialEq` uses IEEE `==` on `Float32`/`Float64` (`self == other`, so NaN != NaN and `0.0 == -0.0`), `Hash` canonicalises `+0.0`/`-0.0` to `+0.0` bits; every other type delegates to `ScalarValue` semantics so nested containers keep bitwise equality. Red-first on `a2bffafa`: `test_freq_signed_zero_collapses` `[]` vs `[0.0]`; `test_freq_signed_zero_first_key_kept` `[-0.0, 0.0]` vs `[-0.0]`; `test_freq_signed_zero_float` `[]` vs `[0.0]`; `test_freq_nan_keys` `[nan]` vs `[]`. All green after. |
| C-008 | `transpose` renders a binary index column name by lossy UTF-8 decode (R-10): each invalid byte becomes one U+FFFD replacement character, so `bytes([0xFF, 0xFE])` answers the two-codepoint name `U+FFFD U+FFFD` (cells `transpose_binary_invalid_utf8`, `transpose_binary_invalid_utf8_repr`); a NULL binary index row is dropped before naming and contributes no column (cell `transpose_binary_null_index`); a name is never `''` unless the index value really is empty. | The transpose pins in the same module over the three fixture cells. | **PROVEN** | `build_output` in `transpose.rs` now decodes `Binary`/`LargeBinary`/`FixedSizeBinary`/`BinaryView` index values via `String::from_utf8_lossy` instead of `value()` (which returned `""` on invalid UTF-8); null index rows were already filtered upstream — pinned by `transpose_binary_null_index` (`['key', 'b']` only). Red-first on `a2bffafa`: `test_transpose_binary_invalid_utf8` answered `''`. All green after. |
| C-009 | `_freq_items` resolves requested names with one lookup structure built once (R-11): `available` set + `folded` display-name map + display→engine overlay; duplicate requested names keep duplicate columns in request order; a case-insensitive hit keeps the requested spelling in the output name (`freqItems(["I"])` → `I_freqItems`, measured on Spark 4.1.2); the `UNRESOLVED_COLUMN` suggestion list stays the sorted `available` proposal (`freq_missing_col` byte-exact); display-overlay renames still resolve to the right engine field. | `test_freq_name_lookup` (duplicates/order), `test_freq_case_insensitive_requested_spelling`, `test_freq_display_overlay`, `test_freq_missing_col` in the same module. | **PROVEN** | One pass builds `available`, `folded`, `by_display`; resolution is O(names + columns). Red-first on `a2bffafa`: `test_freq_case_insensitive_requested_spelling` answered `i_freqItems` instead of `I_freqItems`. All green after. |
| C-010 | Map keys in `freqItems` do not dedupe in Spark 4.1.2 (`mutable.Map[Any, Long]` keys are `MapData` objects whose equality is identity): two identical `{a: 1}` rows answer `[{'a': 1}, {'a': 1}]` at the default capacity and `[]` at capacity 1 — `FreqKey` must answer false for any comparison involving a `ScalarValue::Map`, while a map nested inside a `struct`/`array` key remains part of the parent's content and dedupes bit-exactly. | `python/repark/tests/test_df_rust3_freqitems_transpose.py` map-key pins over fixture cells `freq_map_dup_default`, `freq_map_dup_cap1`, `freq_map_distinct_default`, `freq_map_of_map_default`, plus regression guards `freq_array_dup_default`, `freq_array_dup_cap1`, `freq_struct_dup_cap1`, `freq_struct_with_map_default`, `freq_array_of_map_default`; Rust unit tests in `freq_items.rs`. | **PROVEN** | R-13 ruled implement it: `(ScalarValue::Map(_), _) | (_, ScalarValue::Map(_)) => false` — insert always happens, lookup always misses, same shape as NaN. Nested-map measurement on live Spark 4.1.2 (2026-09-16): `struct`/`array` keys containing maps still dedupe by content (`struct_with_map_default` → `[Row(x='x', m={'a': 1})]`, `array_of_map_default` → `[[{'a': 1}]]`), matching the top-level-only arm with no extra work. Red-first on `d27b86c3`: `test_freq_map_keys_never_equal` `[{'a': 1}]` vs `[{'a': 1}, {'a': 1}]`; the two regression-guard tests already passed. All green after. |

VERDICT: 10 clauses, 10 PROVEN, 0 OPEN, 0 REJECTED (round 3 close-out 2026-09-16).

## Per-name decision table

| name | decision | one line of reason |
|---|---|---|
| `DataFrame.freqItems` | implemented | New name; binds `statistics.freq_items` (same body as `stat.freqItems`, R-5). |
| `DataFrameStatFunctions.freqItems` | implemented | The disclosed refusal retires; the shared body calls the Rust UDAF. |
| `DataFrame.transpose` | implemented | New name; binds `transpose.transpose`, the eager Rust kernel (R-8). |

## Rulings (orchestrator, run 16b/17b card)

- R-1 (card F-1, binding): the freqItems kernel is a DataFusion UDAF in repark-core —
  one accumulator per column, mergeable state — implementing Spark's `FreqItemCounter`
  exactly: capacity `size = floor(1/support)`; `add(key, count)`: if the key is present add
  `count`; else if the map holds fewer than `size` keys insert `key -> count`; else let
  `m` = the minimum count and `r = count - m`: `r >= 0` inserts `key -> count`, drops every
  entry `<= m`, then subtracts `m` from each remaining entry; `r < 0` subtracts `count`
  from every entry and drops entries that reach `<= 0`. Row updates call `add(v, 1)`;
  merge calls `add(k, v)` per entry. NULL is a key like any other value. Bytecode-verified
  against `FreqItemCounter.update/merge` (spark-sql_2.13-4.1.2.jar).
- R-2 (card F-2): result is ONE row, columns `<name>_freqItems` of `array<T>` (T = input
  type, incl. struct/array/map/decimal/timestamp/binary/bool/date), nullable false,
  duplicates kept; empty frame → one row of empty arrays; `freqItems([])` → n empty rows,
  no columns.
- R-3 (card F-3): argument checks in the thin Python wrapper before any plan — str `cols`
  → `NOT_LIST_OR_TUPLE`; `Column` element → `NOT_ITERABLE`; `support=None` → 0.01; support
  outside `[1e-4, 1]` → `IllegalArgumentException` with the verbatim message (Scala Double
  rendering); unknown column → `UNRESOLVED_COLUMN.WITH_SUGGESTION`, sorted proposal.
- R-4 (card F-4): int/float `support` is accepted as float (Spark Connect's behaviour;
  classic Py4JError is a bridge artefact); bool/str/other → `PySparkTypeError` `NOT_FLOAT`
  `[NOT_FLOAT] Argument `support` should be a float, got <type>.` — divergence note in the
  registry row.
- R-5 (card F-5): `df.stat.freqItems` and `df.freqItems` share one body; the "not
  supported yet" refusal and its pins flip.
- R-6 (card F-6): no SQL door for freqItems — nothing registered.
- R-7 (card T-1..T-4): the transpose kernel is Rust in repark-core taking the frame and an
  optional index column name; index resolves eagerly like Spark (default = first column;
  one index only; atomic types only; `UNRESOLVED_COLUMN.WITH_SUGGESTION` on a miss); value
  columns are the rest in frame order and must share `findTightestCommonType` semantics
  (empty → string; pairwise rules bytecode-verified: identical → same; NullType → other;
  string+string → tightestCommonString; integral+decimal → decimal iff `isWiderThan`;
  numeric+numeric non-decimal → `numericPrecedence.lastIndexWhere(t => t == n1 || t == n2)`
  with Float promoted to Double; datetime+datetime → `findWiderDateTimeType`; interval and
  geography/geometry pairs → merged/ANY; else `findTypeForComplex` for array/struct/map,
  else no common type); output `key` string non-null + one column per non-null index row
  sorted ascending on the raw index, named by Spark's index→string cast, duplicate names
  allowed, value fields nullable.
- R-8 (card T-5..T-7): row limit = session conf `spark.sql.transposeMaxValues` if exposed,
  else 500 (`TRANSPOSE_EXCEED_ROW_LIMIT` `54006`); no SQL door (PARSE_SYNTAX_ERROR in
  Spark too — nothing pinned, no grammar added); facade signature
  `transpose(self, indexColumn: ColumnOrName | None = None)`.
- R-9 (round 2, L-101, binding): the accumulator's key equality for `Float32`/`Float64`
  is IEEE `==` — `+0.0`/`-0.0` one key, every `NaN` equal to nothing (insert always
  happens, lookup always misses, exactly like Spark's `mutable.Map` insert-miss). No
  other type's equality changes; nested float keys were to be measured and any divergence
  halted on. Implemented as `FreqKey` in `freq_items.rs`; the nested measurement matched
  Spark for `array`/`struct` (bit-exact) but found a separate `Map`-key divergence,
  recorded as open clause C-010.
- R-10 (round 2, L-103, binding): binary index values decode to column names lossily —
  one U+FFFD per invalid byte (`UTF8String.fromBytes` semantics); never `''` unless the
  value is empty; null index rows are dropped before naming (pinned, not assumed).
  Implemented via `String::from_utf8_lossy` in `transpose.rs`.
- R-11 (round 2, Y-1, binding): `_freq_items` builds one lookup structure once —
  O(names + columns) — preserving duplicates, request order, the sorted suggestion list,
  and (measured during the round) the requested spelling of a case-insensitive hit.
- R-12 (round 2, record only): the review's perf findings are residue — no code change.
  See "Round 2 residue" below.
- R-13 (round 3, binding — answers the Q1 hand-back question): implement the map-key arm.
  Orchestrator re-measured on live PySpark 4.1.2 (`mapkey_probe_2026-09-16.json`):
  `freq_map_dup_default` → `[{'a': 1}, {'a': 1}]`, `freq_map_dup_cap1` → `[]`,
  `freq_map_distinct_default` → `[{'a': 1}, {'b': 2}]`, `freq_array_dup_default` →
  `[[1, 2]]` (deduped), `freq_struct_dup_cap1` → `[Row(x=1, y='x')]` (deduped). MapData
  alone has identity equality; array/struct keys compare by content. One arm in
  `FreqKey`: `ScalarValue::Map` never equal, everything else untouched. The nested-map
  boundary (`struct`/`array` holding maps) was measured again in-round:
  `struct_with_map_default` and `array_of_map_default` dedupe by content in Spark,
  exactly what the top-level-only arm gives — no divergence to record.

## Round 2 residue (recorded, no code change)

- **P-101** (Rust perf, P2): the accumulator boxes a `ScalarValue` per row and allocates
  an owned key per Utf8/Binary row. Spark's own counter boxes into a `mutable.Map[Any,
  Long]` per row — repark is in Spark's class; reviewer's isolated floor measured ~66e6
  rows/s for Int32 and ~28e6 rows/s for String. Recorded; no change.
- **P-102** (Rust perf, P2): the min-scan is O(capacity) on a miss at capacity and
  `merge` can reach O(P·C²) when P disjoint full maps merge at support 1e-4 (C=10000).
  Spark's `CollectFrequentItems.add` performs the same linear `minBy` scan — repark is
  in Spark's class; at the default support 0.01 (C=100) the merge term is negligible.
  Recorded; no change.
- **Y-2** (Python perf, P3): one extra PyO3 crossing for the schema fetch before the
  kernel call. Recorded; no change.
- **Y-3** (Python perf, P3): `statistics._java_double` duplicates the Rust Java-double
  renderer in `repark-functions`. Recorded in the Rust-first roll-call as known
  duplication; moving it would touch a file another run owns tonight. No change.

## Rust-first roll-call

- `freq_items` — DataFusion `AggregateUDFImpl` + `Accumulator` in `repark-core`
  (`freq_items.rs`); the PyO3 binding is a `#[pyfunction]` shim. Python does argument-shape
  checks and name plumbing only — nothing computes in Python.
- `transpose` — eager kernel in `repark-core` (`transpose.rs`): plan filter/sort/limit,
  collect once, build the result `RecordBatch` over a `MemTable`, return the frame and the
  raw index scalars; the index→string display names are formatted in `repark-python` via
  `repark_functions::java_double` (the Java-double text lives in repark-functions and the
  DAG has no repark-core → repark-functions edge). Python binds the name and resolves the
  index argument only. Known duplication (Y-3, recorded 2026-09-16): the facade's
  `statistics._java_double` re-renders the Java-double string for the `support` error
  message in Python; it stays until the run that owns that file can route it through the
  binding.
- Error conditions: Rust errors embed `[CONDITION] … SQLSTATE:` in the message; the facade
  catches and attaches `_spark_error_class` / `_spark_message_parameters` /
  `_spark_sql_state` (the surface_a / `_integral` pattern).

## Settled decisions (recorded, not guessed)

- **Index→string names** come from Spark's `Cast(index, StringType)` semantics per type:
  ints plain digits, doubles/floats Java `Double.toString`/`Float.toString` (`0.5`, `1.5`),
  decimal its toString, boolean `true`/`false`, date `yyyy-MM-dd`, timestamp
  `yyyy-MM-dd HH:mm:ss[.ffffff]`, binary UTF-8 — the probed types (string, int, double)
  are pinned; the rest follow the same renderer.
- **Transpose sort is on the raw index value**, ascending (Spark's `SortOrder(index,
  Ascending)` before the cast), with a total order in Rust (NaN sorts lowest, Spark's
  default); equal index values keep input order (stable sort) — `transpose_dup_index_tuple`
  `('a', 1, 2)` pins first-occurrence order.
- **The `freqItems([])` shape** delegates to the zero-column select (5 `Row()`s, matching
  `freq_empty_cols`).
- **Aggregate output engine names** are unique `__repark_freqitems_<i>` aliases; the facade
  overlays the real `<name>_freqItems` display names (duplicates included) — the existing
  `_display_names`/`_engine_names` mechanism.
- **Transpose output engine names** are `__repark_transpose_<i>` for i >= 0 (field 0
  included); display names are `["key", *index_strings]`.

## Gates

| gate | result |
|---|---|
| Red first — unit test file on the base tree (round 1) | 44 failed on the base tree (missing names / refused calls), all green after the implementation |
| Red first — new pins on `a2bffafa` (round 2) | 6 failed: `test_freq_signed_zero_collapses` `[]` vs `[0.0]`, `test_freq_signed_zero_first_key_kept` `[-0.0, 0.0]` vs `[-0.0]`, `test_freq_signed_zero_float` `[]` vs `[0.0]`, `test_freq_nan_keys` `[nan]` vs `[]`, `test_freq_case_insensitive_requested_spelling` `i_freqItems` vs `I_freqItems`, `test_transpose_binary_invalid_utf8` `''` vs `` — all green after the fixes |
| Red first — new pins on `d27b86c3` (round 3) | 1 failed: `test_freq_map_keys_never_equal` `[{'a': 1}]` vs `[{'a': 1}, {'a': 1}]`; the regression-guard tests `test_freq_nested_map_keys_dedupe` / `test_freq_container_keys_dedupe` already passed — all green after the `FreqKey` map arm |
| `cargo test -p repark-core freq` / `transpose` | green — `freq` filter 14 passed incl. the signed-zero/NaN/map-key tests; `transpose` filter 10 passed |
| `cargo test --locked --workspace` | green — repark-core 504 passed incl. the new `freq_items`/`transpose` unit tests (round 2: 514; round 3: 516 incl. the two map-key tests) |
| Release native rebuild + unit file | `maturin develop --release` clean; unit file 63 passed (round 2), 66 passed (round 3 incl. exports file) |
| `test_dfcore_1_exports.py` | 10 passed (green alongside the unit file, 66 total round 3) |
| `make verify` | green — every structural gate clean (fmt, clippy both lists, crate-DAG, lib-rs, file-size, lib-py, conventions, docstrings, example-coverage 1057/945/111, manifest, ledger lifecycle + grammar, docs, owner-ruling, parity-live, matrix-liveness, ruff, taplo, typos); re-run green on 2026-09-16 after the round-3 diff |
| `make py-test-facade` | 8314 passed, 372 skipped, 2 xfailed (round 1); 8428/367/2 (round 2); 8431 passed, 367 skipped, 2 xfailed (round 3) |
| parity suite `python/repark-parity/tests` | 757 passed, 2 skipped, 12 xfailed (EX-0 raw-walk pin moved 1059 → 1061); same counts re-confirmed 2026-09-16 rounds 2 and 3 |
| comment fence | clean — no code comments added; rationale lives in this ledger and the touched `map.md` rows |

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  - id: AT-1
    status: ATTACKED
    evidence: Red-first on the base tree (44 failures) and again on the round-1 head `a2bffafa` (6 failures covering signed-zero collapse, NaN non-equality, case-insensitive display spelling and the invalid-UTF-8 index name) proved every pin binds a real surface; the FreqItemCounter/CollectFrequentItems add/merge and ResolveTranspose tightest-common-type/row-limit semantics were read from spark-sql_2.13-4.1.2.jar bytecode, and each oracle cell drives a fixture-keyed assertion rather than a guessed answer.
    artifacts: [python/repark/tests/test_df_rust3_freqitems_transpose.py, python/repark/tests/facade_df_rust3_oracle.json]
  - id: AT-2
    status: ATTACKED
    evidence: Every freq_* and transpose_* fixture cell asserted — argument shapes, nulls, empty frames, complex element types, duplicate names/columns, support bounds, index resolution, sort order, float-key equality (signed zero, NaN, Float32, nested array/struct), binary index lossy decode, and every conditioned error with class, params and sqlstate.
    artifacts: [python/repark/tests/test_df_rust3_freqitems_transpose.py, python/repark/tests/facade_df_rust3_oracle.json]
  - id: AT-3
    status: ATTACKED
    evidence: No unsafe outside repark-python's PyO3 module, no env reads at query time, everything through Session (the transpose row limit is read from spark.sql.transposeMaxValues); the panic-ban, crate-DAG and both file-size gates hold — repark-core stays edge-free of repark-functions (the Java-double renderer is called from repark-python, which already depends on both).
    artifacts: [crates/repark-core/src/freq_items.rs, crates/repark-core/src/transpose.rs, crates/repark-python/src/dataframe_stats.rs]
  - id: AT-4
    status: N/A
    justification: No auth, IAM, network or secret surface; both kernels are in-process computations over Arrow batches.
  - id: AT-5
    status: N/A
    justification: No persistence, catalog or commit-path change; both features build MemTable/logical results in memory.
  - id: AT-6
    status: ATTACKED
    evidence: Bad-shape inputs measured against Spark: str cols, Column element, non-float support, out-of-range support (Java-double rendering 1.0E-5), unknown names with sorted suggestions, non-atomic index, no least common type, >500 index rows — each raises Spark's own class and message shape.
    artifacts: [python/repark/tests/test_df_rust3_freqitems_transpose.py, crates/repark-core/src/transpose.rs]
  - id: AT-7
    status: N/A
    justification: Facade-method unit, no system-level, migration or fleet-wide change.
  - id: AT-8
    status: ATTACKED
    evidence: Exact baselines held — core.py 4015, writer_readwriter.py 1101, dataframe.rs 1017 (the new bindings moved to the dataframe_stats.rs pyfunction module after PyO3 rejected a second #[pymethods] block); statistics.py and surface_a.py stay under the default ceiling.
    artifacts: [crates/repark-python/src/dataframe_stats.rs, python/repark/src/repark/spark/dataframe/statistics.py, python/repark/src/repark/spark/dataframe/surface_a.py]
  - id: AT-9
    status: ATTACKED
    evidence: Every measured divergence is recorded rather than hidden: R-4 int-support acceptance (Connect semantics, not Py4JError), the unspecified hash-map/equal-key orders, and the full boxed-value key-equality rule (floats IEEE `==`, maps identity, everything else content) plus U+FFFD index rendering are written into the DF-FREQITEMS-1/DF-TRANSPOSE-1 registry rows; the dict cells accept either legal equal-key variant while tuple cells pin positional truth; the round-3 map-key boundary was measured on both sides — top-level maps never equal (pinned) and maps nested in struct/array dedupe by content (regression-guarded).
    artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/test_df_rust3_freqitems_transpose.py]
  - id: AT-10
    status: ATTACKED
    evidence: The four old refusal pins flipped to positive assertions in the same commit (test_df_batch2.py, test_g1_stat_and_expander.py, test_dfcore_5_approx_quantile.py, test_examples_dataframe_d.py); the full facade suite and parity suite run the neighbours (round-2 counts in the gates table).
    artifacts: [python/repark/tests/test_df_batch2.py, python/repark/tests/test_examples_dataframe_d.py, python/repark/tests/test_dfcore_1_exports.py]
complete: true
```

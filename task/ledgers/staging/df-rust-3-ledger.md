# Charter ledger — DF-RUST-3 · `DataFrame.freqItems` / `DataFrameStatFunctions.freqItems` / `DataFrame.transpose`

**Date:** 2026-09-15 · **Branch:** `feat/df-rust-3` · **Base:** `0355ef5e` · **Model:**
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

VERDICT: 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED (delivered 2026-09-15).

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

## Rust-first roll-call

- `freq_items` — DataFusion `AggregateUDFImpl` + `Accumulator` in `repark-core`
  (`freq_items.rs`); the PyO3 binding is a `#[pyfunction]` shim. Python does argument-shape
  checks and name plumbing only — nothing computes in Python.
- `transpose` — eager kernel in `repark-core` (`transpose.rs`): plan filter/sort/limit,
  collect once, build the result `RecordBatch` over a `MemTable`, return the frame and the
  raw index scalars; the index→string display names are formatted in `repark-python` via
  `repark_functions::java_double` (the Java-double text lives in repark-functions and the
  DAG has no repark-core → repark-functions edge). Python binds the name and resolves the
  index argument only.
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
| Red first — unit test file on the base tree | 44 failed on the base tree (missing names / refused calls), all green after the implementation |
| `cargo test --locked --workspace` | green — repark-core 504 passed incl. the new `freq_items`/`transpose` unit tests |
| Release native rebuild + unit file | `maturin develop --release` clean; unit file 54 passed |
| `test_dfcore_1_exports.py` | 10 passed |
| `make verify` | green — `make ci` ran every structural gate clean (fmt, clippy both lists, crate-DAG, lib-rs, file-size, lib-py, conventions, docstrings, example-coverage 1054/942/111, manifest, ledger lifecycle + grammar, docs, owner-ruling, parity-live, matrix-liveness, ruff, taplo, typos) |
| `make py-test-facade` | 8314 passed, 372 skipped, 2 xfailed |
| parity suite `python/repark-parity/tests` | 757 passed, 2 skipped, 12 xfailed (EX-0 raw-walk pin moved 1059 → 1061) |
| comment fence | clean — no code comments added; rationale lives in this ledger and the touched `map.md` rows |

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  - id: AT-1
    status: ATTACKED
    evidence: Red-first on the base tree (44 failures) proved every pin binds a real surface; the FreqItemCounter add/merge and ResolveTranspose tightest-common-type/row-limit semantics were read from spark-sql_2.13-4.1.2.jar bytecode and the Spark 4.1.2 source, and each oracle cell drives a fixture-keyed assertion rather than a guessed answer.
    artifacts: [python/repark/tests/test_df_rust3_freqitems_transpose.py, python/repark/tests/facade_df_rust3_oracle.json]
  - id: AT-2
    status: ATTACKED
    evidence: All 17 freq_* cells and all 19 transpose_* cells asserted — argument shapes, nulls, empty frames, complex element types, duplicate names/columns, support bounds, index resolution, sort order, and every conditioned error with class, params and sqlstate.
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
    evidence: Every measured divergence is recorded rather than hidden: R-4 int-support acceptance (Connect semantics, not Py4JError) and the unspecified hash-map/equal-key orders are written into the DF-FREQITEMS-1/DF-TRANSPOSE-1 registry rows; the dict cells accept either legal equal-key variant while tuple cells pin positional truth.
    artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/test_df_rust3_freqitems_transpose.py]
  - id: AT-10
    status: ATTACKED
    evidence: The four old refusal pins flipped to positive assertions in the same commit (test_df_batch2.py, test_g1_stat_and_expander.py, test_dfcore_5_approx_quantile.py, test_examples_dataframe_d.py); the full facade suite (8314 passed) and parity suite (757 passed) run the neighbours.
    artifacts: [python/repark/tests/test_df_batch2.py, python/repark/tests/test_examples_dataframe_d.py, python/repark/tests/test_dfcore_1_exports.py]
complete: true
```

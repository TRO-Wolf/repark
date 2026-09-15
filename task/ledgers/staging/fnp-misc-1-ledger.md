# Charter ledger — FNP-MISC-1 · call_function, call_udf, arrow_udf, arrow_udtf, bucket(Column)

**Unit:** FNP-MISC-1 · **Date:** 2026-09-15 · **Branch:** `feat/fnp-misc-1` · **Base:** `origin/main`
`4d6b1ab0` · **Model:** muse-spark-1.3-contributor · **Policy:** [AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** **Python only — no Rust, never cargo, maturin or `make develop`.**

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** Five Spark 4.1.2 facade names are absent or refuse while the 1.5 Spark-parity
campaign closes the function surface: `call_function` / `call_udf` are absent, `arrow_udf` /
`arrow_udtf` are absent, and `bucket` with a Column `numBuckets` refuses. The orchestrator
recorded the live PySpark 4.1.2 oracle on 2026-09-14 (three JSON cells files, copied to
`python/repark/tests/fnp_misc_1_*_spark_oracle.json`); this unit implements the five names
against those cells or raises Spark's own error condition with a dated registry row.

**Shape rule (owner, 2026-09-14):** a name is done when it is present on
`repark.spark.functions` and answers PySpark 4.1.2 for its argument shapes, NULLs, result
types and error classes, with pins — or, where the engine cannot carry it, raises Spark's own
error condition with a dated registry row. Prefer implementing. Never a silent stub.

**Not in this unit:** any edit to `dataframe/**`, `column.py`, `session/**`, `catalog.py`,
`types.py` (another orchestrator owns them); any dependency file; `STATUS.md`;
`briefs/next-sequence.md`; any JVM; `REPARK_PARITY_LIVE=1`.

**Decisions (orchestrator rulings under G-2).** D-1 Name resolution happens when
`call_function` / `call_udf` is called (a facade `Column` is built without a session —
registry row FNP9-BYNAME-1), not at analysis as in Spark. The error class and message match;
only the raise point differs. Record that residual in FNP9-BYNAME-1, flipped to **FIXED
2026-09-15 (FNP-MISC-1)** with the residual noted, and update the pin
`test_fnp_9_collections_json.py::test_fnp9_multi_column_and_by_name_names_stay_absent` so it no
longer asserts absence for these two names. D-2 Files: new module
`python/repark/src/repark/spark/functions_arrow_udf.py` (listed in the directory `map.md`) for
`arrow_udf` / `arrow_udtf`; `call_function` / `call_udf` in a new `functions_byname.py`;
`bucket` in `functions.py`. Export through the canonical functions module the way
`functions_declared.install_into` and siblings do. The forbidden-file list above is owned by
another orchestrator: a target needing an edit there (for example `arrow_udf(...) + 1`
composition, which the pandas bridge refuses mid-expression, or `spark.udf.register` of an
arrow UDF then `spark.sql`) is not made: RePark's current behaviour is pinned and one registry
row per gap is filed in §7 (**BACKLOG 2026-09-15**, repark / Apache Spark / Pin / Rationale
bullets, the oracle cell cited). D-3 `pyarrow` is an optional dependency; `arrow_udf` /
`arrow_udtf` import it lazily and raise Spark's `[PACKAGE_NOT_INSTALLED]` shape when it is
absent. D-4 Pins: `python/repark/tests/test_fnp_misc_1.py`, red first on the base tree, red
output pasted into the ledger. Column names, types and rows compare exactly; the oracle's
`nullable` flags are informative, not pinned, where the pandas bridge already differs (any
difference noted in the clause evidence).

**Rust-first (owner, 2026-09-14) — why each name here stays in Python.** The standing instruction lands every new
function in Rust unless the engine cannot carry it; the reasons, one line each: `arrow_udf` / `arrow_udtf` — user
Python code over Arrow batches, and the engine has no Python/Arrow UDF runtime, so execution rides the existing pandas
UDF and Python UDTF bridges (the zero-copy bridge is `PERF-ARROW-UDF-1`); `call_function` / `call_udf` — API plumbing,
a by-name lookup that builds the engine's own routine call (or a session-registered Python UDF), and the kernels it
reaches are Rust; `bucket(Column, col)` — API plumbing, a literal fold into the existing partition-transform marker.

## PROPOSITION LEDGER — FNP-MISC-1 — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The five names are present on `repark.spark.functions` (and `__all__`) with PySpark 4.1.2 calling conventions. | `test_fnp_misc_1.py::test_fnp_misc_1_names_present_with_pyspark_calling_convention`, red on base. | **PROVEN** | Step 3: all five present; parameter names, kinds and defaults pinned (annotations are repark-side spellings, not PySpark's `ColumnOrName` strings). Split inventory gains the arrow tail segment. pins: fnp-misc-1/C-001 |
| C-002 | `call_function` / `call_udf` resolve Spark-equal: registered UDF wins, else the engine scalar built by name, else a facade-only aggregate-path fallback, else `AnalysisException [UNRESOLVED_ROUTINE]`; dotted names `[REQUIRES_SINGLE_PART_NAMESPACE]`. | Value cells (`abs`, `ABS`, `upper`, `sum` grouped, `plus_one_py` registered) plus the two error cells, red on base. | **PROVEN** | Step 2: 8 pins green (`test_fnp_misc_1_call_function_*`, `test_fnp_misc_1_call_udf_*`, `test_fnp_misc_1_by_name_unknown_*`). New module `functions_byname.py`; `functions.py` unchanged at 1962 lines. Round 2 (F-4/L-004): builtins resolve as SQL routines through the engine, so facade helpers that are not routines (`col`, `lit`, `when`, `udf`, ...) raise `[UNRESOLVED_ROUTINE]`; `sha2`/`ntile`/`log` accept Columns; `FACADE_ONLY_ROUTINE_NAMES` curates the engine gap. Round 3 (L-010): engine arity failures map to `[WRONG_NUM_ARGS.WITHOUT_SUGGESTION]` at call time. Residual per D-1: resolution at call time, not analysis. pins: fnp-misc-1/C-002 |
| C-003 | `bucket` with a Column `numBuckets`: a literal Column folds to the int path (same `[PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY]` refusal outside `partitionedBy` as the int form); a non-literal Column raises `PySparkTypeError [NOT_COLUMN_OR_INT]`; every call emits the 4.1.2 `FutureWarning`. | Fold + refusal + warning pins, red on base (refuses "not supported yet", no warning). | **PROVEN** | Step 2: 3 pins green (`test_fnp_misc_1_bucket_*`). A foldable Column folds through its SQL literal text — bare digits plus, since round 2 (L-003), CAST(int-literal AS INT/SMALLINT/TINYINT/BIGINT); `lit(2)+lit(2)`-style composed foldables and CAST(True/NULL/'4'/4.0/string-target) refuse `NOT_COLUMN_OR_INT` (measured residual, same class as the non-literal refusal). 10 more pins round 2 (`test_fnp_misc_1_bucket_cast_int_*`, `test_fnp_misc_1_bucket_cast_non_int_*`); `functions.py` still 1962 lines. pins: fnp-misc-1/C-003 |
| C-004 | `arrow_udf` scalar / iterator / grouped-aggregate cells answer Spark-equal over the pandas bridge, plus the error cells (`[CANNOT_BE_NONE]`, `[SCHEMA_MISMATCH_FOR_PANDAS_UDF]`, wrong-type cast, `[PACKAGE_NOT_INSTALLED]`). | Value cells plus error cells, red on base. | **PROVEN** | Step 3: 13 pins green in `test_fnp_misc_1.py` (scalar, default name, two-arg, iterator, grouped, wrong length/type, missing returnType, range-frame cells, object shape, pyarrow-less). New module `functions_arrow_udf.py`; eval type inferred from hints with explicit `functionType` override. Residuals in §7. pins: fnp-misc-1/C-004 |
| C-005 | `arrow_udtf` answers the oracle cell: `rows_udtf(lit(3))` yields `i` = 0, 1, 2 as bigint over the Python UDTF path. | UDTF value cell, red on base. | **PROVEN** | Step 3: 5 pins green (direct + decorator value cells, lateral join via SQL, zero-arg mismatch, non-arrow yield, pyarrow-less). Round 2 (F-1/L-002): each `eval` runs once per input RecordBatch with whole-column `pa.Array` args, and yielded Tables project onto the declared `returnType` names (missing/extra → loud), over the additive `_map_arrow_udtf_batches` branch in `udtf.py`; the plain-Python path is unchanged. Residuals in §7. pins: fnp-misc-1/C-005 |
| C-006 | Every D-2 gap (composition, register-then-sql, grouped-in-select message, zero-arg Echo message, non-arrow yield) is pinned as RePark's current behaviour with one §7 registry row per gap. | Gap pins green plus §7 rows citing the oracle cell each. | **PROVEN** | Step 3: 4 gap pins green plus the non-arrow-yield conversion pin; §7 carries FNP-MISC-1-COMP-1, FNP-MISC-1-REGSQL-1, FNP-MISC-1-GRPSEL-1, FNP-MISC-1-UDTFARGS-1 with oracle cells cited (round 2, L-006: the four rows plus PERF-ARROW-UDF-1, FNP-MISC-1-NAN-1, FNP-MISC-1-ALLNULL-1 each name their pin); lateral join via SQL answers (no row needed). pins: fnp-misc-1/C-006 |
| C-007 | No regression: the UDF/UDTF suites stay green and ruff check + format-check pass. | Step-4 gate table (trio + `test_*udf*` + `test_*udtf*` + ruff). | **PROVEN** | Step 4 gates, all green: trio (`test_fnp_misc_1`, `test_fnp_9_collections_json`, `test_fnp15_16_declared_refuse`) 475 passed; UDF/UDTF suites (`test_pandas_udf_oracle`, `test_pandas_udf`, `test_udf_oracle`, `test_udf`, `test_udtf`) 183 passed, 2 skipped (pre-existing skips); `ruff check python/` clean; `ruff format --check python/` clean. `check_example_coverage` green at backlog 112 — the four new names are invisible to its static enumeration (installer tables not in `FUNCTION_EXPORT_BINDINGS`); registering bindings plus examples is a follow-up, noted for handoff. pins: fnp-misc-1/C-007 |

## Red first (step 1, base tree `4d6b1ab0`)

Red run: `.venv/bin/python -m pytest python/repark/tests/test_fnp_misc_1.py -q` on the base
tree, before any product change: **32 failed, 0 passed.** Every pin fails; the representative
failure per clause is quoted below.

- C-001/C-002/C-004/C-005/C-006: `assert hasattr(F, name), name` reads
`AssertionError: call_function` (`test_fnp_misc_1_names_present_with_pyspark_calling_convention`);
  every other pin errors on the same absent attribute.
- C-003 fold: `F.bucket(lit(4), col("v"))` raises `UnsupportedOperationException`
  (`bucket(Column, col) as a partition transform is not supported yet`) inside the
  `pytest.warns(FutureWarning)` block, and no warning is emitted.
- C-003 refusal: `F.bucket(col("v"), "v")` raises the same `UnsupportedOperationException`,
  not `PySparkTypeError [NOT_COLUMN_OR_INT]`.


## §7 Divergence registry (BACKLOG 2026-09-15 unless noted)

### FNP-MISC-1-COMP-1 — `arrow_udf(...)` mid-expression composition refuses

- **repark** — `UnsupportedOperationException`: `pandas_udf result cannot be used in
  arithmetic (+) in repark v1 (facade projection-rewrite bridge only; not a Column
  expression in the SQL plan). Materialize via select/withColumn, then apply further
  expressions on that column. Mid-expression embedding is an M5-class seed.` (measured
  2026-09-15 on this tree).
- **Apache Spark** — `(plus("v") + 1)` answers `[[12], [None], [32]]` as `x int`.
  *(oracle: `python/repark/tests/fnp_misc_1_arrow2_spark_oracle.json` cell `arrow_udf` /
  `composed plus(v) + 1`, live PySpark 4.1.2, 2026-09-14.)*
- **Pin** — `python/repark/tests/test_fnp_misc_1.py::test_fnp_misc_1_arrow_udf_mid_expression_composition_refused`
- **Rationale** — BACKLOG, filed 2026-09-15. Closing it needs bridge markers to become
  Column expressions (`dataframe/**`, `column.py`), which another orchestrator owns (D-2).
  The refusal is the pandas bridge's own, inherited by sharing its machinery.

### FNP-MISC-1-REGSQL-1 — `spark.udf.register` of an arrow UDF, then `spark.sql`

- **repark** — `UnsupportedOperationException`: `spark.udf.register does not accept
  pandas_udf callables in repark v1; register a classic scalar Python function (or use
  F.udf). pandas_udf stays on the DataFrame path (M6).` (measured 2026-09-15; the arrow
  callable is a `PandasUDFFunction`, so the existing name-based gate catches it).
- **Apache Spark** — `spark.udf.register("arrow_plus", plus)` then
  `SELECT arrow_plus(v)` answers `[[2], [None]]` as `x int`.
  *(oracle: `python/repark/tests/fnp_misc_1_arrow2_spark_oracle.json` cell `arrow_udf` /
  `register and sql`, live PySpark 4.1.2, 2026-09-14.)*
- **Pin** — `python/repark/tests/test_fnp_misc_1.py::test_fnp_misc_1_arrow_udf_register_then_sql_refused`
- **Rationale** — BACKLOG, filed 2026-09-15. The SQL registry is classic-scalar-only
  (`session/**`), owned by another orchestrator (D-2).

### FNP-MISC-1-GRPSEL-1 — grouped arrow UDF in `select` raises a different AnalysisException

- **repark** — `AnalysisException`: `GROUPED_AGG pandas_udf cannot be used in
  select/withColumn without .over(Window.partitionBy(...)); use groupBy(...).agg(pandas_udf(...))
  for non-window form, or attach an unbounded partition window via .over`. (measured
  2026-09-15).
- **Apache Spark** — `AnalysisException [MISSING_AGGREGATION] The non-aggregating
  expression "v" is based on columns which are not participating in the GROUP BY clause.`
  *(oracle: `python/repark/tests/fnp_misc_1_arrow_bucket_spark_oracle.json` cell
  `arrow_udf` / `arrow_udf grouped agg (pa.Array -> scalar)`, live PySpark 4.1.2,
  2026-09-14.)*
- **Pin** — `python/repark/tests/test_fnp_misc_1.py::test_fnp_misc_1_arrow_udf_grouped_in_select_refused`
- **Rationale** — BACKLOG, filed 2026-09-15. Both sides refuse loud with `AnalysisException`;
  only the message differs, and matching it needs a `dataframe/**` edit owned by another
  orchestrator (D-2).

### FNP-MISC-1-UDTFARGS-1 — zero-arg call on an arrow UDTF whose `eval` takes arguments

- **repark** — `PySparkException`: `UDTF UserDefinedTableFunction('Echo') eval() raised
  TypeError: Echo.eval() missing 1 required positional argument: 'values'` plus traceback.
  (measured 2026-09-15).
- **Apache Spark** — `PySparkRuntimeError
  [UDTF_EVAL_METHOD_ARGUMENTS_DO_NOT_MATCH_SIGNATURE] Failed to evaluate the user-defined
  table function 'Echo' because the function arguments did not match the expected signature
  of the 'eval' method (missing a required argument: 'a').`
  *(oracle: `python/repark/tests/fnp_misc_1_arrow2_spark_oracle.json` cell `arrow_udtf` /
  `decorator, no args`, live PySpark 4.1.2, 2026-09-14.)*
- **Pin** — `python/repark/tests/test_fnp_misc_1.py::test_fnp_misc_1_arrow_udtf_zero_arg_eval_mismatch_refused`
- **Rationale** — BACKLOG, filed 2026-09-15. Matching the condition needs a
  `udtf.py`-adjacent signature check at call time; the bridge text it emits is shared
  UDTF-path behavior. Both sides refuse loud.

### No row (implemented, residual noted)

- Wrong-length arrow results raise `PySparkRuntimeError
  [SCHEMA_MISMATCH_FOR_PANDAS_UDF] Result vector from arrow_udf was not the required
  length: expected N, got M.` — message- and condition-equal to Spark; Spark wraps it in a
  JVM `PythonException`, which has no repark counterpart (pinned message + condition).
- Non-Arrow UDTF yields raise `[UDTF_ARROW_TYPE_CONVERSION_ERROR] PyArrow UDTF must return
  an iterator of pyarrow.Table or pyarrow.RecordBatch objects.` — message-equal to Spark's
  worker text, surfaced as repark's bridge `PySparkException`.
- The `arrow_udf` object is a `PandasUDFFunction` (bridge callable with `__name__`), where
  Spark answers a plain `function`; pinned as `test_fnp_misc_1_arrow_udf_object_shape`.
- Group key `g` reports nullable True where the oracle says False — pre-existing groupBy key
  behavior, identical for a direct `F.sum` grouping; not introduced here (D-4 note).
- The two-arg pin casts both inputs to `pa.string()`: the oracle lambda casts only `a`
  because Spark hands both sides as `string`, while repark STRING columns arrive as
  `large_string` and pyarrow 25 has no `(string, large_string)` join kernel. Spark's
  answers are still the pinned values.
- `partitioning.bucket` has no repark counterpart (out of scope; the card covers `F.bucket`
  only). SQL-door `SELECT bucket(4, v)` refuses loud (`AnalysisException: Invalid function
  'bucket'`) where Spark raises `UNRESOLVED_ROUTINE` — adjacent surface, not a clause.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: fnp-misc-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against the card. The deliverable is 32 pins against
        the orchestrator-recorded live PySpark 4.1.2 cells plus two new facade modules and
        the FNP9-BYNAME-1 flip, and every pin ran red on the base tree before the product
        change existed.
      artifacts: [python/repark/tests/test_fnp_misc_1.py, python/repark/src/repark/spark/functions_byname.py, python/repark/src/repark/spark/functions_arrow_udf.py]
    - id: AT-2
      status: ATTACKED
      evidence: NULLs in every value cell, case-variant routine names, unknown and dotted
        names, non-literal / foldable-non-int / composed-foldable bucket counts, short and
        wrong-typed arrow results, missing returnType, a pyarrow-less interpreter, zero-arg
        UDTF calls, and non-Arrow yields are all pinned.
      artifacts: [python/repark/tests/test_fnp_misc_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: No nested defs in the new product modules, one-line docstrings throughout,
        full type hints, no bare except, ruff check and format clean on the whole tree.
      artifacts: [python/repark/src/repark/spark/functions_byname.py, python/repark/src/repark/spark/functions_arrow_udf.py]
    - id: AT-4
      status: N/A
      justification: No shared or mutable cross-call state — adapters instantiate per
        decoration and hold only the user callable plus the declared type; the bridges own
        all batching. No threads, no locks, no async.
    - id: AT-5
      status: N/A
      justification: No authn/authz, credential, network, or deserialization surface. The
        test eval runs committed fixture strings only.
    - id: AT-6
      status: ATTACKED
      evidence: Every value cell compares column names, Arrow types, and rows exactly
        against the live-Spark fixture; error cells compare type plus the exact Spark
        message or condition. The two-arg kernel gap failed loud mid-build and forced the
        documented harness adaptation rather than a silent value change.
      artifacts: [python/repark/tests/test_fnp_misc_1.py, python/repark/tests/fnp_misc_1_agg_spark_oracle.json, python/repark/tests/fnp_misc_1_arrow2_spark_oracle.json, python/repark/tests/fnp_misc_1_arrow_bucket_spark_oracle.json]
    - id: AT-7
      status: N/A
      justification: Not a perf unit. The adapters run inside the existing per-batch bridge
        callbacks and add no new materialization; no timing claim is made.
    - id: AT-8
      status: ATTACKED
      evidence: The session registry entry shape, the pandas bridge slot contract, the
        grouped-agg call convention, the UDTF scalar-arg expansion, and the lit display
        forms were all read from the tree or measured before use, not assumed.
      artifacts: [python/repark/src/repark/spark/functions_byname.py, python/repark/src/repark/spark/functions_arrow_udf.py]
    - id: AT-9
      status: ATTACKED
      evidence: Every loud path names Spark's own condition and message (UNRESOLVED_ROUTINE,
        REQUIRES_SINGLE_PART_NAMESPACE, NOT_COLUMN_OR_INT, CANNOT_BE_NONE,
        SCHEMA_MISMATCH_FOR_PANDAS_UDF, PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY,
        UDTF_ARROW_TYPE_CONVERSION_ERROR) and the pins assert the text.
      artifacts: [python/repark/tests/test_fnp_misc_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: No separate mutation round was run; the bite proof is red-first (all 32 pins
        failed on the base tree) plus green-after, and one live mid-build failure (the
        two-arg Arrow kernel mismatch) that reded its pin until addressed.
      artifacts: [python/repark/tests/test_fnp_misc_1.py, task/ledgers/staging/fnp-misc-1-ledger.md]
  complete: true
```

VERDICT: 7 clauses, 7 PROVEN, 0 OPEN, 0 REJECTED.

## Round 2 (2026-09-15) — perf + critic-logic remediations

Each finding: disposition, the pin that goes red without the fix, evidence.

### F-1 (P1) — arrow UDTF batch-native path

Fixed. `udtf.py` gains an Arrow-native map branch (`_map_arrow_udtf_batches`, additive
only — the plain path diff is zero removed lines): the user `eval` runs once per input
RecordBatch with whole-column Arrays, and yielded Tables concatenate to the sink with no
`to_pylist` / per-row tuples. The wrapper keeps a raising `eval` guard so direct per-row
calls fail loud instead of going silently slow.
Pin: `test_fnp_misc_1_arrow_udtf_eval_once_per_batch` (3-row batch, eval called once with
a length-3 Array; red before: three length-1 calls).
Perf (100,000 input rows through `_map_udtf_batches`, best of 5, same harness):
before 2.971 s, after 0.0059 s. Plain-Python UDTF on the same harness: 0.022 s.

### L-002 (P1) — arrow UDTF projects yields by declared field name

Fixed with F-1. `_project_arrow_table` selects the declared `returnType` field names
(missing → loud, extra → loud) and casts to the declared Arrow schema.
Pins: `test_fnp_misc_1_arrow_udtf_swapped_columns_match_by_name` (red before: positional
swap answered `x=2, y=1`);
`test_fnp_misc_1_arrow_udtf_wrong_field_name_refused` (red before: `{"j": [1]}` answered
`i=1`).

### F-2 (P2) — iterator adapter streams one batch at a time

Fixed. `_ArrowIterAdapter.__call__` returns a generator over a `_ArrowBatchFeeder` that
converts each Series batch on pull and records its row count; outputs validate against the
recorded length in order. The adapter holds at most one input batch of its own (the
bridge still buffers its Series batches upstream, unchanged).
Pin: `test_fnp_misc_1_arrow_iter_adapter_streams_one_batch` (weakref peak live inputs
over two batches must stay 1; red before: 2).

### F-3 (P2) — by-name resolution fast path

Fixed inside the L-004 rewrite. The name lowercases once; an empty registry short-circuits;
exact then folded direct gets precede the case-insensitive scan; builtins resolve without
touching the facade module dict (engine match probe, then `__dict__` only on fallback).
Pin: `test_fnp_misc_1_call_function_exact_registry_hit_skips_scan` (zero `items()` scans
on an exact hit, one on a case-variant fallback; red before: scans on both).
Timings (best of 5): 1,000 `call_function('abs')` calls at 0.0077 s before / 0.0079 s
after with an empty registry (noise; the engine probe costs what the old getattr cost),
0.0248 s before / 0.0208 s after with 256 registry entries (~16%). Against a direct
`F.abs` call the by-name overhead floor (session check plus engine probe) measures ~1.67x
at 20,000 calls. Builtin names still scan a non-empty registry by semantic necessity
(shadowing must be checked); the pin proves direct hits skip it.

### F-4 (P2) + L-004 (P2) — builtins resolve as SQL routines through the engine

Fixed. `_resolve_routine` builds the engine scalar by name (`_scalar`, the same builder
the facade wrappers use, with the lowercased name so displays stay `abs(v)`-shaped);
only the engine's `unsupported function` build error falls through to the facade tuple,
and anything else is `[UNRESOLVED_ROUTINE]` at call time — no per-call SQL, no
`session/**` edits, so no HALT. `col`, `lit`, `when`, `udf`, `broadcast`, `expr` and the
other `BYNAME_NON_ROUTINE_NAMES` never resolve even though the facade defines them.
Pins: `test_fnp_misc_1_by_name_non_routines_raise_unresolved` (exact messages; red
before: all three resolved),
`test_fnp_misc_1_call_function_sha2_accepts_lit_bits` (literal width answers the SQL
routine; red before: `TypeError: unhashable type`),
`test_fnp_misc_1_call_function_bitwise_not_case` (red before: `UNRESOLVED_ROUTINE`),
`test_fnp_misc_1_call_function_log_matches_facade`,
`test_fnp_misc_1_call_function_ntile_matches_facade`,
`test_fnp_misc_1_byname_allowlist_covers_facade` (the 238-name tuple equals the live
measured engine gap, so facade/engine drift reds it).
Residuals: `ntile(lit(4))` still refuses loud (`IllegalArgumentException` from the facade
shape — the ruling keeps window names on the facade arm); the tuple curates helpers out
by name, so a future facade helper needs a denylist entry to stay unresolved.

### F-5 (P2) — declared-refusal callables resolve

Fixed. The fallback accepts any callable that is not a type object, so
`_DeferredFamilyRefusal` instances (and any callable class instance) surface their own
refusal instead of `[UNRESOLVED_ROUTINE]`; the four UDF/UDTF classes stay unresolved.
Pin: `test_fnp_misc_1_call_function_declared_refusal_surfaces` (red before:
`UNRESOLVED_ROUTINE`).

### L-005 (P2) — registered name shadows the builtin

Pinned as a guard. `test_fnp_misc_1_call_function_registered_name_shadows_builtin`
registers `abs` (+100) and asserts both `call_function('abs')` and `('ABS')` answer the
shadow. Green before and after (round-1 code already checked the registry first); reds
by construction on any facade-first reorder because the shadowed values differ.

### L-001 (P1) — NaN collapses to NULL before the adapter runs

Pinned as today's behavior plus a registry row. The probe showed the user function
receiving `double [1.5, None, None, 0.0]` — the pandas nullable-`Float64` conversion in
`dataframe/udf_bridge.py` collapses NaN and null before `_series_to_arrow` runs, so no
fix inside `functions_arrow_udf.py` can recover it (measured 2026-09-15).
Pin: `test_fnp_misc_1_arrow_udf_identity_collapses_nan_to_null` (type plus exact
null positions; a Spark-correct NaN assertion reds against it by scratch measurement).
Row: `FNP-MISC-1-NAN-1` in `docs/spark-sql-iceberg-parity.md` §7, pointing at
PERF-ARROW-UDF-1 for the fix.

### L-007 (P2) — all-null INT group arrives as `double`

Pinned as today's behavior plus a registry row. The probe showed group types
`{(2, "double"), (1, "int32")}` — the applyInPandas group Series for the empty group is
float64, and the adapter cannot know the declared input type to cast back (only the
bridge schema knows it).
Pin: `test_fnp_misc_1_arrow_grouped_all_null_int_group_arrives_double`.
Row: `FNP-MISC-1-ALLNULL-1` in `docs/spark-sql-iceberg-parity.md` §7.

### L-009 (P3) — `PACKAGE_NOT_INSTALLED` surfaces as `ImportError`

Noted, not fixed. Repark has no `PySparkImportError` taxonomy and adding one is outside
this unit; the message is Spark-equal (`[PACKAGE_NOT_INSTALLED] PyArrow >= 15.0.0 must
be installed; however, it was not found.`) and `PySparkImportError` is an `ImportError`
subclass in PySpark, so `except ImportError` parity holds.

### D-5 (ruling, accepted residual R-1)

`arrow_udf` scalar/iterator forms ride the pandas bridge (Arrow→pandas→Arrow in the
bridge plus pandas→Arrow→pandas in the adapter: two extra copies per batch), measuring
1.42x / 1.45x the equivalent `pandas_udf` on 1,000,000 rows (best of 5: 0.01058 s vs
0.00747 s, S2-21). Accepted; the zero-copy fix is follow-up card PERF-ARROW-UDF-1.
Row: `PERF-ARROW-UDF-1` in `docs/spark-sql-iceberg-parity.md` §7.

### L-003 (P2) — `bucket(Column)` folds only a bare-digit `sql_expr`

Fixed. The fold unwraps one `CAST(<signed int literal> AS INT|SMALLINT|TINYINT|BIGINT)`
after the bare-digit attempt, inline in `bucket` (3 lines, net-zero: the docstring
condenses to the one-line sibling shape and its contract moves to `spark/map.md`, so
`functions.py` stays at its 1962-line baseline).
Pins: `test_fnp_misc_1_bucket_cast_int_column_folds_to_the_int_path` (int/bigint/smallint/
tinyint fold to the `bucket(4, "v")` fragment and refuse like the int form; red before:
`NOT_COLUMN_OR_INT` on all four);
`test_fnp_misc_1_bucket_cast_non_int_column_raises_not_column_or_int` (CAST of
TRUE/NULL/'4'/4.0, CAST to STRING, and `lit(2)+lit(2)` refuse with Column params —
guard pins, green before and after by construction).
Red-first: the 4 fold cells fail with the unwrap removed; refusal guards stay green.

### L-006 (P2) — four D-2 BACKLOG rows never landed in the parity registry SSOT

Fixed. `docs/spark-sql-iceberg-parity.md` §7 carries `FNP-MISC-1-COMP-1`,
`FNP-MISC-1-REGSQL-1`, `FNP-MISC-1-GRPSEL-1`, `FNP-MISC-1-UDTFARGS-1` (plus round-2
`PERF-ARROW-UDF-1`, `FNP-MISC-1-NAN-1`, `FNP-MISC-1-ALLNULL-1`), each naming its pin;
this ledger's §7 rows now cite the same `python/repark/tests/...` oracle paths with the
live version and date, and the C-006 clause row names the full prefixed IDs. The
round-2 mechanism changes are folded into the C-002/C-003/C-005 clause rows.

### L-008 (P2) — several pins would stay green on a typed or message regression

Fixed. `range_frame_cells` routes through `_assert_value_cell` (name plus `bigint` type,
not values only); `object_shape` and the decorator UDTF pin assert the `pa.int64()` field
type; `wrong_length` drops the fixture-vs-fixture tail assert and compares
`str(caught.value)` to the Spark worker text (`tail[1]` past the `PySparkRuntimeError: `
prefix); `grouped_in_select` and `mid_expression_composition` assert RePark's full
disclosed message. The quotes surfaced two truncations, fixed in both homes: the
§7 GRPSEL-1 row now carries the full `...agg(pandas_udf(...)) for non-window form, or
attach an unbounded partition window via .over` text, and COMP-1 gains its final
`Mid-expression embedding is an M5-class seed.` sentence.
Bite proof: int64→int32 plus one message word reddens the three touched asserts;
the `_assert_value_cell` routing bites through the shared helper.

### F-6 (P3) — multi-line module docstrings

Fixed. `functions_byname.py` already opens with one line; `functions_arrow_udf.py`
condenses 11 lines to `"""Arrow user-defined functions and table functions (FNP-MISC-1)."""`.
The `functionType`-override and lazy-pyarrow `[PACKAGE_NOT_INSTALLED]` facts move to the
`spark/map.md` module entry, which already carries the rest. (`udtf.py`'s multi-line
module docstring is pre-existing, untouched.)

## Round 3 (2026-09-15) — verification-remediation L-010

Round-2 verification (critic-logic cycle 2, 59/59 green): L-002/L-003/L-004/L-005/L-008 and
S2-21 P1-1/P2-1/P2-2 REMEDIATED; L-001/L-007 ACCEPTED-AS-ROW (`FNP-MISC-1-NAN-1`,
`FNP-MISC-1-ALLNULL-1`); L-006 REMEDIATED (§7 rows cite oracle cells and pins); L-009
ACCEPTED-AS-ROW (bare `ImportError`, Spark-equal message); R-1 ACCEPTED-AS-ROW under D-5.
One new P2 below; no new silent-wrong P1.

### L-010 (P2) — engine-known `call_function` arity errors leak `ValueError`

REMEDIATED. `_resolve_routine` maps the engine `call_scalar(<name>) expects <spec>, got
<M>` failure at call time to `AnalysisException [WRONG_NUM_ARGS.WITHOUT_SUGGESTION]`
(`_arity_error`, additive in the `ValueError` arm — unknown-function and other
`ValueError` paths re-raise untouched). Exact-count specs use Spark's words (`The
`sha2` requires 2 parameters but the actual number is 1. Please, refer to
'https://spark.apache.org/docs/latest/sql-ref-functions.html' for a fix. SQLSTATE:
42605`, matching the live agg-oracle `max_by`/`min_by` shape minus the SQL-position
suffix, which a `call_function` call site has not got). Ranged specs (`at least N`,
`N or M`, `at most N`, `0 or 1 seed arg`) keep the condition, the name and the actual
number while reusing the engine wording (`The `substring` accepts at least 2 args but
the actual number is 1. ...`), disclosed here because the engine never reports a single
accepted count for them.
Pins: `test_fnp_misc_1_call_function_sha2_wrong_arg_count_raises`,
`test_fnp_misc_1_call_function_abs_zero_args_raises`,
`test_fnp_misc_1_call_function_substring_range_arity_raises` (exact full-message
equality), plus `test_fnp_misc_1_call_function_abs_string_mismatch_is_not_unresolved`
(guard: a type mismatch on a known routine still builds, then collect raises its own
planning error — never `UNRESOLVED_ROUTINE`).
Red run without the fix (`git stash` of `functions_byname.py` only):
`ValueError: call_scalar(sha2) expects 2 args, got 1`,
`ValueError: call_scalar(abs) expects 1 args, got 0`,
`ValueError: call_scalar(substring) expects at least 2 args, got 1` —
3 failed, 1 passed (guard green before and after by construction), 59 deselected.

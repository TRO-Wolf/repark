# Charter ledger — FNP-GEN-1 · generators and semi-structured parsers answer PySpark 4.1.2

**Date:** 2026-09-15 · **Branch:** `feat/fnp-gen-1` · **Base:** `bee2cde3` · **Model:**
muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `EX-FN-2`, `EX-FN-6`, `EX-FN-8`, `EX-FN-16`, `EX-FN-22`, `FNP9-GENERATORS-1`
and the FNP-Z `json_tuple` residue flip at unit close, not in step 1.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The 1.5 Spark-parity campaign chartered nine generator and semi-structured
names (`inline`, `inline_outer`, `posexplode`, `posexplode_outer`, `json_tuple`,
`from_csv`, `schema_of_csv`, `from_xml`, `schema_of_xml`). The live PySpark 4.1.2
oracle recording of 2026-09-14 already holds their answers; this unit builds the Rust
kernels and facade wrappers against those cells. Run 16a step 1 (this round) lays the
ledger, the fixture subset and the red-first pins only: no product code, no cargo.

**Not in this round:** any engine or facade change; registry flips; `LATERAL VIEW`
spellings (run 16c's parser work, D-2/D-7); re-recording the oracle.

## Decisions

| Id | Ruling | Applied |
|---|---|---|
| D-1 | Orchestrator: kernels in `crates/repark-functions/src/` (new modules in its `map.md`), registered in `register_all`; generators follow the existing `explode` / `stack` plumbing (`StackRewrite` in `crates/repark-core`, `functions_stack.py`). Facade wrappers in `functions_json.py` / `functions_collections.py` / new `functions_generators.py`. | Steps 2–5. |
| D-2 | Orchestrator: `LATERAL VIEW` spellings are run 15c's parser work; pin SELECT-list SQL cells only. | C-003; blocked list below. |
| D-3 | Orchestrator: never edit `dataframe/**`, `column.py`, `session/**`, `catalog.py`, `types.py`, the SQL parser/dialect, or any dependency manifest; HALT at the seam instead. | Whole unit. |
| D-4 | Orchestrator: flip `EX-FN-2`, `EX-FN-6`, `EX-FN-8`, `EX-FN-16`, `EX-FN-22`, `FNP9-GENERATORS-1` and the FNP-Z `json_tuple` residue to FIXED (or DECLARED) with the new pins; one section-7 row per residual divergence. | Step 6. |
| D-5 | Orchestrator: pins in `python/repark/tests/test_fnp_gen_1.py` on both doors over the oracle cells, red first with the red summary in the ledger; Rust unit tests beside each kernel. | This round + steps 2–5. |
| D-6 | Owner ruling Q-15B-1, 2026-09-15: XML stays a dated DECLARED refusal in 1.5 (only the ORC reader gained a dependency). `from_xml` / `schema_of_xml` raise Spark's own unsupported-operation error with a dated registry row joining `FNP-16-csv-xml-xpath`; pins assert the refusal and the fixture keeps Spark's answers for a later release. One-line reason: an XML parser needs a new dependency, which this unit may not add. | C-002, C-003, step 5. |
| D-7 | Orchestrator: `LATERAL VIEW` cells are run 16c's work; list them as `blocked on 16c`, do not pin them. | C-003. |
| D-8 | Orchestrator, run 17a (brief's D-13): the frozen `posexplode` / `posexplode_outer` parameter `column` renames to `col` to match PySpark 4.1.2; the api-freeze record regenerates in the same commit. | C-001. |
| D-9 | Orchestrator, run 17a (brief's D-16): the generators are a Rust analyzer rule rewriting a `Projection` that contains a registered `posexplode` / `inline` scalar, wired like `StackRewrite` and registered through `crates/repark-functions/src/registration.rs`; the facade stays thin and `dataframe/**` and the `_select_with_generator` allow-list are untouched. | C-002, C-003, step 2. |
| D-10 | Orchestrator, run 17a (brief's D-17): `struct(1,'a')` names fields `c0`/`c1` where Spark's oracle records `col1`/`col2` — a real divergence owned by another run's struct-field-naming slice. Recorded as a residual; the literal-array `inline` cell is a non-strict xfail on it and every other pin builds frames so it cannot bite. | Residuals; step 2. |

## Step-1 inputs

- Fixture `python/repark/tests/fnp_gen_1_spark_oracle.json`: 62 cells whose `name`
is one of the nine card names plus their 9 `signatures`, copied verbatim from
`/tmp/oc-worker/pa-gen/o245_spark_oracle.json` (live PySpark 4.1.2, recorder
`/tmp/oc-worker/pa-gen/o245.py`); `spark_version` 4.1.2 kept. Nothing re-recorded.
- No cell projects the `ts` column (checked: zero exprs mention it), so the
driver-local America/New_York timestamp note needs no localization in this unit.
- Blocked on 16c (unpinned, D-7): the three ansi-True `LATERAL VIEW` cells —
`SELECT id, x, y FROM FRAME LATERAL VIEW inline(arr_s) v AS x, y`,
`SELECT id, p, v FROM FRAME LATERAL VIEW OUTER posexplode(arr_i) t2 AS p, v`,
`SELECT id, a, b FROM FRAME LATERAL VIEW json_tuple(js, 'a', 'b') j AS a, b`
(plus their three ansi-False twins).
- Step-2 obligation found while pinning: RePark spells the posexplode parameter
`column`, Spark spells it `col`; the signature pin reds until the rename lands.
- Spark behavior the pins lock in: single-field `json_tuple(...).alias('x')`
answers column `c0` (Spark ignores the alias); the FAILFAST and uninferable-literal
`schema_of_csv` Spark errors record as unclassified `Py4JJavaError` with a truncated
message, so those pins assert the raise shape and step 4 records the exact class.

## Proposition ledger

| Clause | Claim | Verdict | Evidence |
|---|---|---|---|
| C-001 | All nine names sit on `repark.spark.functions` with PySpark 4.1.2's parameter names and defaults. | PROVEN | Step 2 (`f195d086` + working tree): `test_nine_names_present_on_the_facade` and every signature pin pass — `inline`/`inline_outer` land through `functions_generators.py`, and `posexplode`/`posexplode_outer` carry Spark's `col` parameter (D-8; `docs/design/v1-0-api-freeze.json` regenerated, `test_api_freeze` green). pins: fnp-gen-1/C-001 |
| C-002 | The Python door answers the oracle cells for `inline`, `inline_outer`, `posexplode`, `posexplode_outer` (array, map, multi-alias), `json_tuple`, `from_csv`, `schema_of_csv`, and refuses `from_xml` / `schema_of_xml` per D-6. | OPEN | Step 2: every generator cell green on the Python door (array, map, multi-alias, outer); `from_xml` / `schema_of_xml` raise `UnsupportedOperationException` naming `FNP-16-csv-xml-xpath`. Still red for steps 3–4: `json_tuple` (2 cells), `from_csv` (2 cells), `schema_of_csv` (2 cells). pins: fnp-gen-1/C-002 |
| C-003 | The SQL door answers the SELECT-list cells for the same names and refuses the XML pair per D-6; `LATERAL VIEW` cells stay unpinned per D-7. | OPEN | Step 2: every generator SELECT-list cell green on `spark.sql` through the `GeneratorRewrite` analyzer rule (D-9); `from_xml` / `schema_of_xml` refuse at parse altitude with the dated `FNP-16-csv-xml-xpath` message. Still red for steps 3–4: `json_tuple` answers one `struct<c0>` column, `from_csv` / `schema_of_csv` raise `Invalid function`. `LATERAL VIEW` cells remain `blocked on 16c`. pins: fnp-gen-1/C-003 |
| C-004 | Error cells: `from_csv` FAILFAST raises the parse error, `schema_of_csv` on a non-foldable column raises `DATATYPE_MISMATCH.NON_FOLDABLE_INPUT`, the uninferable literal raises. | OPEN | Red on base: 3 failed — FAILFAST raises the stub refusal (`"not supported yet"` still in the message); the non-foldable cell raises `Invalid function` with no `DATATYPE_MISMATCH` match; the uninferable-literal cell raises `Invalid function`. Step 4's cells; unchanged by step 2. pins: fnp-gen-1/C-004 |
| C-005 | NULL / empty / outer rows: `inline_outer` and `posexplode_outer` keep a NULL row for NULL and empty arrays; NULL `js` answers all-NULL fields; NULL `csvrow` answers NULL struct and `',,'` an all-NULL struct. | OPEN | Step 2: `inline_outer` / `posexplode_outer` NULL-and-empty rows green on both doors (the rewrite normalizes NULL/empty inputs to a one-NULL-element list and unnests with `preserve_nulls`). Still red for steps 3–4: the `js` and `csvrow` halves ride on `json_tuple` / `from_csv`. pins: fnp-gen-1/C-005 |
| C-006 | No regression: the touched suites stay green at unit close. | OPEN | Step 2 gates: `cargo test -p repark-functions --lib` 644 passed; `cargo test -p repark-sql --lib` 342 passed; clippy `-D warnings` clean on the workspace; `make verify` green end to end; `test_explode_rewrite` / `test_fnp_9_collections_json` / `test_fnp15_16_declared_refuse` / `test_session_surface_1` / `test_examples_functions_a` / `test_examples_functions_b` / `test_functions_c` / `test_functions_split_identity` all green. pins: fnp-gen-1/C-006 |
| C-007 | Registry rows flip with the new pins and every touched `map.md` stays in lockstep. | OPEN | Step 2: `FNP-16-csv-xml-xpath` and `EX-FN-22` record the `from_xml` / `schema_of_xml` armed dated refusal (D-6); example inventory/backlog regenerated (`F.inline`, `F.inline_outer` covered by new examples; `F.posexplode` / `F.posexplode_outer` examples added). `EX-FN-2` / `EX-FN-8` / `FNP9-GENERATORS-1` flips land in step 6. pins: fnp-gen-1/C-007 |

VERDICT: 7 clauses, 1 PROVEN, 6 OPEN, 0 REJECTED.

## Red summary (base `bee2cde3`, `PYTHONPATH=python/repark/src .venv/bin/python -m pytest python/repark/tests/test_fnp_gen_1.py -q`)

37 tests: 32 failed, 5 passed. The 5 passes are signature pins on existing stubs
whose parameter names and defaults already equal Spark's (`json_tuple`, `from_csv`,
`schema_of_csv`, `from_xml`, `schema_of_xml`). Per-name red causes: `inline` /
`inline_outer` — `AttributeError` on the facade, `Invalid function 'inline'` on SQL;
`posexplode` / `posexplode_outer` — `UnsupportedOperationException` stub on the
facade, `Invalid function 'posexplode'` on SQL, plus the `column`/`col` parameter
divergence; `json_tuple` — stub refusal on the facade, single-`struct<c0>` shape on
SQL; `from_csv` / `schema_of_csv` — stub refusals on the facade, `Invalid function`
on SQL, FAILFAST carrying the stub text; `from_xml` / `schema_of_xml` — stub text
without the `FNP-16-csv-xml-xpath` marker on the facade, `Invalid function` on SQL.

## Step 2 green summary (generator slice, working tree on `f195d086`)

`PYTHONPATH=python/repark/src .venv/bin/python -m pytest python/repark/tests/test_fnp_gen_1.py -q`
on the built release native: 24 passed, 12 failed, 1 xfailed. The 12 reds are exactly
the deferred step-3/4 names — `json_tuple` (4), `from_csv` (4), `schema_of_csv` (4);
the xfail is the literal-array `inline` cell blocked on struct-field naming (D-10).
Every `posexplode` / `posexplode_outer` / `inline` / `inline_outer` cell is green on
both doors, and all four `from_xml` / `schema_of_xml` refusal pins pass.

Architecture landed per D-9: `crates/repark-functions/src/generator.rs` registers
placeholder `ScalarUDF`s for the four names plus the `__repark_gen_alias` marker, and
`GeneratorRewrite` (registered at the tail of `repark_functions::analyzer_rules()` in
`registration.rs`) rewrites a `Projection` carrying exactly one generator call into
inner-projection → list `Unnest` → struct `Unnest` → final-projection shape. `posexplode`
builds `arrays_zip(range(0, cardinality), arg)`; `inline` unnests the struct array then
the struct. Outer spellings normalize NULL/empty inputs to a one-NULL-element list so
`Unnest`'s null preservation emits Spark's all-NULL row. Non-nullable outputs
(`posexplode`'s `pos`) wrap in `coalesce(expr, zero)` so the optimizer's recomputed
schema keeps Spark's nullability. Nested generator calls, two generators in one
projection, and non-container inputs refuse with `[UNSUPPORTED_GENERATOR]`-class errors.
Nine Rust unit tests sit beside the kernel; all pass.

Facade per D-9: `python/repark/src/repark/spark/functions_generators.py` holds the four
thin wrappers plus `_GeneratorColumn`, whose `alias(*names)` packs multi-name aliases
into the `__repark_gen_alias` marker call that the rewrite peels. `dataframe/**` and the
`_select_with_generator` allow-list are untouched; the FNP-9 stay-absent pin flipped to
a presence pin. `dispatch_json.rs` gained the five dispatch arms.

D-8 landed: `posexplode` / `posexplode_outer` take `col`; the api-freeze record was
regenerated and `test_api_freeze` is green.

D-6 landed for step 2's refusal pins: `from_xml` / `schema_of_xml` are armed in both
`declared_refuse.rs` copies (parse-altitude, both SQL doors) and their facade stubs
name `FNP-16-csv-xml-xpath`; the roster grew 62 to 64 in the Rust counts while the
Python family tuples stay unchanged because the two names keep real signature-bearing
stubs in `functions_expr.py`.

### Step-2 residuals

- D-10 (brief D-17): `struct(...)` field naming diverges (`c0`/`c1` vs Spark's
  `col1`/`col2`); owned by the struct-field-naming slice. Only the literal-array
  `inline` cell is affected — pinned `xfail(strict=False)`; every frame pin uses
  named structs.
- Row order: the pins compare rows order-insensitively. `spark.sql` SELECT-list cells
  embed the frame as a `(SELECT ... UNION ALL ... ORDER BY id)` subquery and the
  optimizer drops the subquery sort, so generated row order is not pinned. Spark's
  fixture order is recorded for reference; order-sensitive comparison returns when a
  frame preserves it.
- `spark.tvf.posexplode` / `posexplode_outer` delegate to the live `F.` wrappers; their
  session-surface pins flipped from refusal to answer assertions.
- `check_example_coverage.py` crossed the 1000-line default (1002) adding the
  `functions_generators.py` installer source and `GENERATOR_NAMES` binding; recorded as
  a `check_lib_py` exception row with its debt reason and split seam, and
  `test_explode_rewrite.py` / `functions_expr.py` baselines ratcheted down to measured.

## Remediation round 1

Three reviewer reports landed on step 2 (`rg-logic`, `rg-rustperf`, `rg-pyperf`,
run 17a on `85455aeb`). Red-first pins went into `test_fnp_gen_1.py` before any
fix: 6 reds confirmed on the remediation base (mid-list position,
SQL `AS`-alias arity, Python `explode`+`posexplode`, Python `stack`+`inline`,
SQL `stack`+`inline`, generator-over-aggregate); the NULL-struct pins already
passed on the SQL-derived frame and were kept as the L-004 regression guard.
After the fixes: `34 passed, 12 failed, 1 xfailed` — the 12 reds are exactly the
deferred step-3/4 names.

### P1 dispositions — all closed

- **L-004** (`generator.rs`, data corruption): `coalesce(expr, zero)` removed.
  `inline` now reads fields through the internal `__repark_gen_field` UDF, which
  unions the parent struct's validity bitmap into the extracted child, so a NULL
  struct element emits NULL fields rather than Arrow defaults; `inline_output`
  folds `item.is_nullable()` into output nullability so the
  `__repark_spark_nonnull__` schema marker never masks a real NULL. Schema kept
  Spark-non-null where the element cannot be null; values are never rewritten.
  Pins: `test_python_door_inline_keeps_null_struct_element`,
  `test_sql_door_inline_keeps_null_struct_element`; Rust pin uses a
  non-nullable-field `StructArray` with a NULL parent slot.
- **L-001** (`generator.rs`, select position): `build_expansion` splices the
  output specs at the generator's own index in `projection.expr`
  (`select(id, posexplode(arr_i), num)` → `id, pos, col, num` on both doors).
  Pin: `test_generator_keeps_select_list_position`.
- **L-003** (`generator.rs`, alias arity): the `Expr::Alias` arm of
  `peel_generator` now carries `Some([alias.name])` into the shared arity check,
  so SQL `posexplode(arr_i) AS foo` raises `[COLUMN_ALIASES_MISMATCH]` just like
  the Python `.alias("foo")` marker path. Pins: both single-alias tests.
- **L-002 / PYPERF-004** (door split on mixes): `_GeneratorColumn` cannot carry
  `_generator` — that flag routes a pure generator select into
  `_select_with_generator`, whose allow-list refuses these names — so it carries
  `_repark_generator` instead, which the `functions_stack.py` one-generator gate
  reads (stack mix refuses on the Python door). In the Rust rule, sibling
  projections containing a `stack` call or an explode-path `__repark_arr_*`
  temp alias refuse `Only one generator allowed per select list`, which is what
  makes the Python `explode`+`posexplode` mix refuse at the guarded-unnest
  mid-projection. `_GeneratorColumn` also clears `_projection_name` so
  `_collapse_identity_projection_alias` keeps the call bare — the identity
  `Expr::Alias` it used to add reached the rewrite as a one-name alias.
  Pins: all four mix tests on both doors.
- **PYPERF-001** (aggregate check Python-only): the `_is_aggregate` branch was
  dropped from `_generator_call`; `GeneratorRewrite` now refuses a generator
  whose argument is an `AggregateFunction` or references an `Aggregate` node's
  output (`collect_list` is extracted to an `Aggregate` node, leaving a column
  ref) with `[MISSING_GROUP_BY]` on both doors. Pin:
  `test_generator_over_aggregate_argument_refuses`.

### P2 dispositions — all closed

- **PERF-001**: `range(0, cardinality)` replaced by the internal
  `__repark_gen_ordinality` UDF that writes `0..n-1` Int32 straight from the
  list offsets — no Int64, no step logic, no post-unnest cast.
- **PERF-002**: `arrays_zip` removed from the path; the original list (or the
  `map_keys`/`map_values` zero-copy views) unnests in the same
  `unnest_columns_with_options` call as the ordinality list — one `take` on the
  existing buffers, same cost class as `explode`. The intermediate
  `List<Struct>` is gone and with it the second `Unnest` node.
- **PYPERF-002**: `alias` passes names through `_native.PyColumn.literal` — no
  `lit` Column rebuild, no per-call import.
- **PYPERF-003**: `_generator_call`/`alias` no longer copy `_is_foldable` onto
  the generator Column; generator-plus-aggregate selects still refuse
  `[MISSING_GROUP_BY]` through the aggregate classifier plus the Rust check.
- **L-005**: the facade `from_xml` / `schema_of_xml` stubs now raise
  `UnsupportedOperationException` with the verbatim `{name} is reachable without
  a JVM …` string `declared_refuse` produces on both SQL doors (same class, same
  text, `FNP-16-csv-xml-xpath` marker intact).

### P3 dispositions

- **L-006**: closed — a sibling column carrying a reserved `__repark_gen_*`
  output name refuses with a named error before the rewrite.
- **PERF-003**: moot — the struct-flatten `Unnest` no longer exists; fields read
  via `__repark_gen_field` in the final projection.
- **PERF-004**: resolved by the L-004 mechanism change — `coalesce` is gone;
  `__repark_spark_nonnull__` is an identity UDF in the same cost class.
- **PERF-005**: partial — `build_expansion` skips the site by index (no second
  peel); `arg`/`cond` clones remain across the CASE/ordinality trees, CSE-bound
  as the report noted. Recorded; not worth a plan-shape change this round.
- **PERF-006**: unchanged — output names still pack as Utf8 literals on the
  marker UDF; plan-time only. Recorded.
- **PYPERF-005**: unchanged — single-name `alias(metadata=…)` validates then
  drops the metadata dict; no oracle cell uses it. Recorded.
- **PYPERF-006**: unchanged — the dispatch arm builds a fresh placeholder UDF
  per call. Recorded.
- **PYPERF-007**: closed — the inner `UnsupportedOperationException` import in
  `from_xml` was removed.

### Residuals

- `df.select(F.explode(a), F.posexplode(b))` on a `createDataFrame` frame
  refuses with a schema error (`No field named "posexplode(b)"`) rather than
  the one-generator text: the guarded-unnest mid-projection qualifies the
  argument (`datafusion.public.__repark_cdf_*.b`), so the outer SELECT fails
  name resolution before the analyzer reaches the sibling `__repark_arr_*`
  guard. SQL-sourced frames refuse with the Spark text. Both doors refuse
  without answering; closing the message gap needs `_select_with_generator`
  (`dataframe/**`, out of this unit's fence) to see the family.

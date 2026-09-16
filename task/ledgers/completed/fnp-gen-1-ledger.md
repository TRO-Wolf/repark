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
| C-002 | The Python door answers the oracle cells for `inline`, `inline_outer`, `posexplode`, `posexplode_outer` (array, map, multi-alias), `json_tuple`, `from_csv`, `schema_of_csv`, and refuses `from_xml` / `schema_of_xml` per D-6. | PROVEN | Step 2: every generator cell green on the Python door (array, map, multi-alias, outer); `from_xml` / `schema_of_xml` raise `UnsupportedOperationException` naming `FNP-16-csv-xml-xpath`. Run 18a: all three green on the Python door (census 46 passed, s34 37 passed). pins: fnp-gen-1/C-002 |
| C-003 | The SQL door answers the SELECT-list cells for the same names and refuses the XML pair per D-6; `LATERAL VIEW` cells stay unpinned per D-7. | PROVEN | Step 2: every generator SELECT-list cell green on `spark.sql` through the `GeneratorRewrite` analyzer rule (D-9); `from_xml` / `schema_of_xml` refuse at parse altitude with the dated `FNP-16-csv-xml-xpath` message. Run 18a: `json_tuple` projects `c0`/`c1`, `from_csv` answers structs, `schema_of_csv` answers DDL on `spark.sql`; the SQL `AS (x, y)` and call-result `.c` cells stay `xfail(strict=True)` naming the 18c seam. `LATERAL VIEW` cells remain `blocked on 16c`. pins: fnp-gen-1/C-003 |
| C-004 | Error cells: `from_csv` FAILFAST raises the parse error, `schema_of_csv` on a non-foldable column raises `DATATYPE_MISMATCH.NON_FOLDABLE_INPUT`, the uninferable literal raises. | PROVEN | Red on base: 3 failed — FAILFAST raises the stub refusal (`"not supported yet"` still in the message); the non-foldable cell raises `Invalid function` with no `DATATYPE_MISMATCH` match; the uninferable-literal cell raises `Invalid function`. Run 18a: FAILFAST, `NON_FOLDABLE_INPUT`, `UNEXPECTED_NULL`, `UNEXPECTED_INPUT_TYPE`, `INTERNAL_ERROR`, `NON_MAP_FUNCTION`, `NON_STRING_TYPE`, `WRONG_NUM_ARGS` all match on both doors. pins: fnp-gen-1/C-004 |
| C-005 | NULL / empty / outer rows: `inline_outer` and `posexplode_outer` keep a NULL row for NULL and empty arrays; NULL `js` answers all-NULL fields; NULL `csvrow` answers NULL struct and `',,'` an all-NULL struct. | PROVEN | Step 2: `inline_outer` / `posexplode_outer` NULL-and-empty rows green on both doors (the rewrite normalizes NULL/empty inputs to a one-NULL-element list and unnests with `preserve_nulls`). Run 18a: the `js` and `csvrow` halves green on both doors (all-NULL fields, NULL structs, `',,'` all-NULL). pins: fnp-gen-1/C-005 |
| C-006 | No regression: the touched suites stay green at unit close. | PROVEN | Step 2 gates: `cargo test -p repark-functions --lib` 644 passed; `cargo test -p repark-sql --lib` 342 passed; clippy `-D warnings` clean on the workspace; `make verify` green end to end; `test_explode_rewrite` / `test_fnp_9_collections_json` / `test_fnp15_16_declared_refuse` / `test_session_surface_1` / `test_examples_functions_a` / `test_examples_functions_b` / `test_functions_c` / `test_functions_split_identity` all green. Run 18a gates: `make verify` green end to end; lib 751 passed; s34 37 passed with 3 xfailed (18c); census 46 passed; examples / session-surface / fnp9 / e1 green; workspace clippy `-D warnings` green. pins: fnp-gen-1/C-006 |
| C-007 | Registry rows flip with the new pins and every touched `map.md` stays in lockstep. | PROVEN | Step 2: `FNP-16-csv-xml-xpath` and `EX-FN-22` record the `from_xml` / `schema_of_xml` armed dated refusal (D-6); example inventory/backlog regenerated (`F.inline`, `F.inline_outer` covered by new examples; `F.posexplode` / `F.posexplode_outer` examples added). `EX-FN-2` / `EX-FN-8` / `FNP9-GENERATORS-1` flips land in step 6. Run 18a step 5: EX-FN-6/8/16 rows flip to answer rows with answer pins; the SES-TVF-1 and FNP-16 passages name each departure; the map hook enforced lockstep on every commit. pins: fnp-gen-1/C-007 |

VERDICT: 7 clauses, 7 PROVEN, 0 OPEN, 0 REJECTED.

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

## Orchestrator fix-up before the PR gate (2026-09-16, run 17a)

Twelve `json_tuple` / `from_csv` / `schema_of_csv` pins — this unit's steps 3–4, written red-first
in step 1 — were still plain failures after step 2. A merged PR cannot carry failing tests, so each
is now `xfail(strict=True)` with a reason naming the step. Strict is the point: when the step-3
kernels land the pins XPASS and fail, which forces that round to retire them instead of quietly
leaving them marked. The unit ships steps 1–2 (`posexplode`, `posexplode_outer`, `inline`,
`inline_outer` answering on both doors, and `from_xml` / `schema_of_xml` as dated declared
refusals); the three parser names stay OPEN in the clause table. pins: fnp-gen-1/C-004

## Verification critic and its two residuals (2026-09-16, run 17a)

A Grok verification critic on the remediated head found **every one of the five P1s CLOSED** and no
new P1. It filed two P2 residuals; the orchestrator raised one of them and fixed it:

- **V-002, raised to P1 and fixed.** `ordinality` cloned the input's offset buffer while packing
  positions densely from 0. Arrow's `ListArray::slice` slices offsets but keeps the whole values
  buffer, so a sliced list with a non-zero first offset made `ListArray::new` **panic**. A panic is
  not a P2 in a repository with a panic-ban gate, and a sliced `ListArray` is ordinary in execution
  (after a limit, a take, a concat) even though no oracle cell or unit frame produces one. Fixed
  with a fresh `OffsetBuffer` built from the measured lengths, plus an `exec_err` in place of the
  `as_list_array` panic downcast, pinned by `ordinality_packs_positions_for_a_sliced_list`.
- **V-001, accepted as a residual.** `peel_generator` tells a user `AS` from a NamePreserver-restored
  display alias with a `starts_with(funcname + "(")` prefix test, so a user alias whose text happens
  to look like the call is dropped on the SQL door while the Python door still raises
  `COLUMN_ALIASES_MISMATCH`. It needs a non-textual marker for restored aliases to close properly,
  which is a change to the name-restoration path rather than to this unit. Recorded here and in the
  registry with the seam named. pins: fnp-gen-1/C-003

## Step 3-4 (run 18a, 2026-09-16) — R-18a-1..R-18a-6 applied, red-first pins

Branch `feat/fnp-gen-1-s3` off `origin/main 33c87cbf`. Actor: muse-spark-1.3-contributor.

### Rulings applied this round

| Id | Ruling | Applied |
|---|---|---|
| R-18a-1 | (brief mechanics) Step order, red-first pins, commit-per-slice, handback.json. | Whole round. |
| R-18a-2 | Every argument check (foldability, literal schema, parse mode, options map shape, alias count, field types) decided in Rust so both doors raise the same condition (Q-17a-2); the facade binds names and packs `options` into a `map()` literal Column, never inspects a value. | Steps 2-4 kernels; facade holds names/shapes only. |
| R-18a-3 | `schema_of_csv('')` raises condition `INTERNAL_ERROR`, SQLSTATE `XX000`, on both doors; registry records it as matching a Spark defect; the pin asserts the condition. | Step 4 kernel + pins S2/S8 + registry row. |
| R-18a-4 | `json_tuple` joins `GeneratorRewrite` in `generator.rs`; field extraction is a Rust kernel parsing each row once for all N fields; reuse the crate's JSON machinery and DDL parser; no new dependencies. | Step 2 kernel (`json/tuple.rs`) + rewrite arm. |
| R-18a-5 | `LATERAL VIEW json_tuple` cells stay unpinned, blocked on run 18c's parser (D-7). Extended: the SQL `AS (x, y)` UDTF-alias cells and the call-result `.c` cell hit DataFusion SQL seams (`select.rs` multiple-alias refusal; `expr` dot access on non-string exprs) owned by run 18c, so they pin `xfail(strict=True)` with registry rows and retire with 18c. | Step 1 pins J7/J8/C18 + registry. |
| R-18a-6 | `from_csv` tokenizer follows Spark's univocity defaults; reuse the reader-path CSV tokenizer if one exists, else a small kernel tokenizer; cast each input column once per batch. | Step 3 kernel (no shared reader-path tokenizer found; small kernel tokenizer). |

### Step 1 red summary

Copied `/tmp/oc-worker/sa-gen3/fnp_gen_1_s34_spark_oracle.json` verbatim to
`python/repark/tests/fnp_gen_1_s34_spark_oracle.json` (`cmp` identical, 80 cells = 40
ansi pairs; ansi-True/False expectations identical on rows, columns, conditions and
root causes), listed in `python/repark/tests/map.md`, pinned by
`python/repark/tests/test_fnp_gen_1_s34.py` (40 tests, one per ansi pair, recorder
lambdas from `o_gen34.py` beside each behavior).

`PYTHONPATH=python/repark/src .venv/bin/python -m pytest python/repark/tests/test_fnp_gen_1_s34.py -q`
on the unfixed tree: **36 failed, 1 passed, 3 xfailed**. The pass is
`test_python_door_json_tuple_zero_fields_refuses` (the E1 `CANNOT_BE_EMPTY` facade
shape predates the kernel; the pin locks it via `getCondition()`). The xfails are the
three 18c-blocked pins (J7/J8/C18). Per-name red causes: `json_tuple` Python —
stub `UnsupportedOperationException`; SQL — upstream struct shape; `from_csv` /
`schema_of_csv` Python — stub refusals, SQL — `Invalid function`; error pins carry
the stub/`Invalid function` text instead of the oracle condition.
pins: fnp-gen-1/C-002, C-003, C-004, C-005

### Step 2 green summary (run 18a): `json_tuple` kernel + rewrite arm

`crates/repark-functions/src/json/tuple.rs` holds the internal `__repark_json_tuple`
kernel (one `parse_json` per row for all N fields through the shared reader; R-18a-4,
R-18a-6 style batch casts, no new dependency); `generator.rs` gains the `JsonTuple`
site (all arguments carried), `[DATATYPE_MISMATCH.NON_STRING_TYPE]` for non-string
arguments, `[WRONG_NUM_ARGS]` under two arguments, the lone-alias ignore and
`[UDTF_ALIAS_NUMBER_MISMATCH]` mismatch (both per the s34 oracle; the older four
names keep `[COLUMN_ALIASES_MISMATCH]`), and the unnest-free expansion computing one
struct per row read back through `__repark_gen_field`. The expansion lives in
`generator/json_tuple.rs` so `generator.rs` keeps the 1000-line ceiling (924).
`test_fnp_gen_1.py` retires its four `json_tuple` strict marks; the FNP-Z residue pin
`test_json_tuple_still_refuses_on_the_facade` becomes
`test_json_tuple_answers_on_both_doors` (the unit charter supersedes the refusal).
`GENERATOR_NAMES` keeps the four step-2 names: `json_tuple` holds its presplit
`__all__` row through a static import, so `test_functions_split_identity.py` is
untouched. A NULL-typed literal argument refuses `NON_STRING_TYPE` while a
string-typed NULL value answers all-NULL fields, per the fixture cells.
`cargo test -p repark-functions --lib`: 738 passed. s34 `json_tuple` pins: 9 passed,
2 xfailed (18c). Neighbor suites (`test_fnp_gen_1`, `test_explode_rewrite`,
`test_fnp_9_collections_json`) green.
pins: fnp-gen-1/C-002, C-003

### Step 3 green summary (run 18a): `from_csv` kernel + options rule

`crates/repark-functions/src/csv.rs` (options, univocity-default tokenizer, Java
date-pattern reader) plus `csv/from_csv.rs` (the struct kernel: PERMISSIVE
null-pads and parses typed fields, FAILFAST throws the decoded row, the corrupt
column captures whole records, NULL documents answer NULL structs, output
nullability follows the input) plus `csv/fold.rs` (`CsvFold` before
`TypeCoercion`, refusing `DROPMALFORMED` on readable literal options maps with
`[PARSE_MODE_UNSUPPORTED]`). All other argument checks are type- or
literal-based in `return_field_from_args`, so both doors raise the same
condition (R-18a-2). The DDL parser's `Execution`-flavored errors convert to
`Plan` at the CSV boundary so a bad DDL raises `AnalysisException`, not
`PySparkException`. The facade binds the schema as a literal and packs options
into an inline `create_map` (`functions_collections` imports this module, so the
facade cannot import `create_map` back); the output name keeps Spark's
first-argument form. `test_fnp_gen_1.py` retires its four `from_csv` marks; the
foldable-schema s34 pin stays red until step 4 registers `schema_of_csv`.
`cargo test -p repark-functions --lib`: 744 passed. s34 `from_csv` pins: 16
passed, 1 xfailed (18c), 1 red (step-4 C10).
pins: fnp-gen-1/C-002, C-003, C-004

### Step 4 green summary (run 18a): `schema_of_csv` kernel + literal fold

`crates/repark-functions/src/csv/schema_of_csv.rs` holds the scalar kernel (one
DDL string per row over the Spark `CSVInferSchema` ladder
INT→BIGINT→DOUBLE→TIMESTAMP→BOOLEAN→DATE→STRING, rendered
`STRUCT<_c0: TYPE, …>`; the empty document raises the `INTERNAL_ERROR` defect,
R-18a-3) plus seven `#[cfg(test)]` pins. `CsvFold` folds a literal call at
analysis time — through the `__repark_spark_nonnull__` shim `SparkNullability`
wraps around a `map` options call and the name-preserving outer alias it leaves
behind — reading options in pre-coercion (`map(k, v, …)`) or post-coercion
(`map(make_array, make_array)`) shape, and unaliases the fold inside `from_csv`;
a still-non-literal schema raises `[INVALID_SCHEMA.NON_STRING_LITERAL]` in the
rule, so the kernel answers a `Null` placeholder meanwhile and the rebuilt
`Projection`/`Aggregate`/`Window` carries the struct type. A non-foldable input
raises `[DATATYPE_MISMATCH.NON_FOLDABLE_INPUT]`, NULL
`[DATATYPE_MISMATCH.UNEXPECTED_NULL]`, a non-string literal
`[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]`. The two Python value pins take
`limit(1)`: the oracle captured a frameless one-row select while the pins select
over the six-row frame (step-4 pin correction, same oracle rows).
`cargo test -p repark-functions --lib`: 751 passed. s34: 37 passed, 3 xfailed
(18c).
pins: fnp-gen-1/C-004

### Step 5 green summary (run 18a): registry flips + census pins

`test_fnp_gen_1.py` retires its four `schema_of_csv` strict marks (46 passed, 1
xfailed). The EX-FN-6/8/16 registry rows flip from refusal to answer rows with
their example pins (`test_from_csv_answers`, `test_json_tuple_answers`,
`test_schema_of_csv_answers`), and the tvf `json_tuple` pins flip
(`test_tvf_json_tuple_answers_today`; the field-columns pin asserts the
`c0`/`c1` frame); the SES-TVF-1 and FNP-16 passages name each departure. The
`from_csv` facade checks its schema argument with the conditioned
`NOT_COLUMN_OR_STR` bar (`test_from_csv_bad_schema_raises_not_column_or_str`
green). The round's `csv` code is clippy-clean under `-D warnings` (token arms
parse straight into their target width; behavior unchanged).
pins: fnp-gen-1/C-002, C-003, C-004, C-007

## Coverage attestation (run 18a)

```yaml
COVERAGE_ATTESTATION:
  pr_unit: fnp-gen-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Unchecked on purpose and named here — a folded schema_of_csv under a non-Projection parent (Aggregate/Window rebuild is coded, no pin drives it); inference timestamp/date shapes beyond the pinned cells plus option plumbing; the header/inferSchema keys, accepted but unapplied to inference.
      artifacts: [crates/repark-functions/src/csv/fold.rs, crates/repark-functions/src/csv/schema_of_csv.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Every checkable claim carries a pin — s34 37 passed with 3 xfailed (18c), census 46 passed, 7 kernel unit tests, the flipped example/tvf answer pins, the e1 bar pin.
      artifacts: [python/repark/tests/test_fnp_gen_1_s34.py, python/repark/tests/test_fnp_gen_1.py, python/repark/tests/test_examples_functions_a.py, python/repark/tests/test_session_surface_1.py, python/repark/tests/test_e1_errorclass.py]
    - id: AT-3
      status: ATTACKED
      evidence: Every error condition pins per door — FAILFAST, NON_FOLDABLE_INPUT, UNEXPECTED_NULL, UNEXPECTED_INPUT_TYPE, INTERNAL_ERROR, NON_MAP_FUNCTION, NON_STRING_TYPE, WRONG_NUM_ARGS, NOT_COLUMN_OR_STR; the only raise-surface change is the three flipped refusals.
      artifacts: [python/repark/tests/test_fnp_gen_1_s34.py, python/repark/tests/test_e1_errorclass.py]
    - id: AT-4
      status: N/A
      justification: No delegated Critic ran in run 18a (single-session Actor); the orchestrator audit of steps 1-3 is clean with no outstanding findings to re-attack.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, .github, push, or dependency change; all edits inside the workspace; every commit carries the TRO-Wolf identity plus the Authored-By trailer; zero added comment lines.
      artifacts: [task/ledgers/staging/fnp-gen-1-ledger.md]
    - id: AT-6
      status: ATTACKED
      evidence: No public signature change — schema_of_csv keeps its (csv, options) facade shape, from_csv gains one internal bar; the same queries answer the same rows as the oracle.
      artifacts: [python/repark/src/repark/spark/functions_expr.py]
    - id: AT-7
      status: ATTACKED
      evidence: The s34 oracle fixture is used verbatim; the only pin-side change is limit(1) on two selects, keeping the same oracle rows; no hand-computed expectations.
      artifacts: [python/repark/tests/fnp_gen_1_s34_spark_oracle.json, python/repark/tests/test_fnp_gen_1_s34.py]
    - id: AT-8
      status: ATTACKED
      evidence: check_lib_py and the CAP-1 mirror agree on 2178; the new csv files sit under the 1000-line default ceiling; no unrecorded shrink or growth.
      artifacts: [scripts/check_lib_py.py, python/repark-parity/tests/test_cap_1_source_file_line_cap.py]
    - id: AT-9
      status: ATTACKED
      evidence: The live-PySpark-4.1.2 s34 cells are the oracle for every value and error; the kernel unit tests assert engine-internal contracts (row-wise invoke, ladder rendering) where the oracle is silent.
      artifacts: [crates/repark-functions/src/csv/schema_of_csv.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Pins cited in the tests map and the csv maps; the clause rows above carry the red runs (step 1) and the green runs (steps 2-5 plus the gate pass); verdict 7/7 PROVEN.
      artifacts: [task/ledgers/staging/fnp-gen-1-ledger.md, python/repark/tests/map.md, crates/repark-functions/src/csv/map.md]
```

## Remediation (run 18a, 2026-09-16) — R-18a-13..R-18a-15, critic L-001..L-005 + PERF-001..009

Branch `feat/fnp-gen-1-s3` rebased onto `origin/main 02abfd0e` by the orchestrator
(diff-of-diffs empty). Actor: muse-spark-1.3-contributor.

### Remediation step 1 red summary

Copied `/tmp/oc-worker/sa-gen3/fnp_gen_1_s34_critic_spark_oracle.json` verbatim to
`python/repark/tests/fnp_gen_1_s34_critic_spark_oracle.json` (`cmp` identical, 34
cells; ansi-True/False expectations identical; timestamp cells doubled across the
UTC and America/New_York session zones), listed in `python/repark/tests/map.md`,
pinned by `python/repark/tests/test_fnp_gen_1_s34_critic.py` (16 tests, one per
behavior, each asserting both ansi cells; zones via runtime `conf.set`).

`.venv/bin/python -m pytest python/repark/tests/test_fnp_gen_1_s34_critic.py -q`
on the unfixed tree: **12 failed, 3 passed, 1 xfailed**. Green at red:
`test_python_door_from_csv_extra_token_without_corrupt_column` (PERMISSIVE already
drops the extra token), `test_python_door_from_csv_empty_frame` (no panic through the engine on any
empty variant probed; the kernel-level guard lands in step 3 with a Rust unit test
that invokes the kernel directly on a 0-length input) and
`test_python_door_json_tuple_trailing_content` (the shared JSON
reader is already lenient about trailing bytes; the pin locks it). The xfail is the
exact-expr SQL timestamp pin, blocked on run 18c's dot-access seam (R-18a-5
extended). Per-finding red causes: L-001 — extra token dropped with NULL corrupt
column, middle corrupt column misaligns tokens (`the corrupt record column is a
string field` at execution); L-002 — default TIMESTAMP parses date-only so every
stamp field is NULL; L-004 — naive stamps stored as UTC micros (invisible while
L-002 NULLs them); R-18a-13 — non-STRING corrupt column raises at execution, not
analysis; L-005 — extra scale digits and `1e2` NULL instead of rounding, and the
engine has no struct-to-string cast so the oracle's struct rendering pins
Python-side; L-003 — kernel indexes row 0 of a broadcast scalar (Rust-level; the
Python empty-frame pin passes through the engine short-circuit); R-18a-14 —
ladder infers `STRUCT<_c0: STRING, _c1: STRING, _c2: TIMESTAMP, _c3: DOUBLE,
_c4: DOUBLE, _c5: STRING, _c6: STRING, _c7: STRING>` (no fractional/`Z` stamps, no
`DECIMAL(24,0)`, `1.5f` not DOUBLE).
pins: fnp-gen-1/L-001, L-002, L-003, L-004, L-005, R-18a-13, R-18a-14

### Remediation dispositions (R-18a-13..R-18a-15)

| Id | Sev | Disposition |
|---|---|---|
| L-001 | P1 | Closed in `1fd3ca3f`. Tokens map onto the data fields only; malformed is token-count mismatch or a conversion failure; the whole record lands in the corrupt column at any position. Pins: `test_python_door_from_csv_extra_token_marks_corrupt`, `..._corrupt_column_middle_wellformed`, `..._middle_malformed` plus Rust `extra_token_with_trailing_corrupt_column_marks_malformed`, `middle_corrupt_column_maps_data_fields_only`. |
| L-002 + L-004 | P1/P2 | Closed in `5337d55e`. Default stamps try offset, naive and date-only shapes; `timestampFormat`/`dateFormat` compile once per invoke through the shared Java machinery with no `dateFormat` fallback for `TIMESTAMP`; naive stamps localize in the session zone, `TIMESTAMP_NTZ` stays wall-clock, builders carry the field zone. Pins: `..._default_timestamps_utc`, `..._new_york`, `..._timestamp_format_option`, `..._timestamp_ntz_stays_wall_clock`, `test_sql_door_from_csv_default_timestamp_value`. |
| L-003 / PERF-001 | P1 | Closed in `5337d55e`. Schema and options come off the `ColumnarValue::Scalar`; a 0-length schema array answers an empty `Null` frame without indexing row 0; only documents go through `values_to_arrays`. Pins: Rust `empty_documents_answer_an_empty_struct` (direct 0-length invoke) plus Python `test_python_door_from_csv_empty_frame`. |
| R-18a-13 | P1 | Closed in `1fd3ca3f`. Non-STRING corrupt column refuses `[INVALID_CORRUPT_RECORD_TYPE]` (SQLSTATE 42804) in `return_field_from_args`, both doors. Pins: `test_python_door_from_csv_non_string_corrupt_column_refuses` (malformed and well-formed rows, proving analysis time), `test_sql_door_from_csv_non_string_corrupt_column_refuses`, Rust `non_string_corrupt_column_refuses_at_analysis`. |
| L-005 | P2 | Closed in `37977bf0`. `DECIMAL` tokens run through `from_json`'s `decimal_units` (HALF_UP, scientific notation, precision overflow to NULL); `parse_decimal` deleted. Pins: `test_python_door_from_csv_decimal_rounds_half_up`, Rust `decimal_tokens_round_half_up_take_exponents_and_null_on_overflow`. |
| R-18a-14 | P2 | Closed in `37977bf0`. Inference gains fractional/`Z`/`HH:mm` stamps (date-only stays `DATE`), `DECIMAL(n,0)` past `BIGINT` capped at precision 38, `1.5f`-style suffix to `DOUBLE`, sharing the parser's default stamp shapes. Pins: `test_python_door_schema_of_csv_inference_ladder`, Rust `schema_of_csv_infers_fractional_zulu_short_stamps_big_integers_and_float_suffix`. |
| PERF-002 | P2 | Closed in `5337d55e` with L-003 (no broadcast of schema/options arrays); every critic and s34 pin passes over the new path. |
| PERF-003 | P2 | Closed in `a2547e53`. Scalar `json_tuple` field names hold one shared value per batch; array names keep the column path with the mixed-length check. No behavior change: all `json_tuple` pins green. |
| PERF-004 | P2 | Closed in `a2547e53`. The tokenizer borrows unquoted fields (`Cow`), allocating only for quoted/escaped ones. No behavior change: full pin suites green plus a unicode probe (`héllo,"wörld",3.5`). |
| PERF-005 | P2 | Closed in `a2547e53`. The PERMISSIVE path builds no payload; the FAILFAST payload rebuilds on scratch builders for the single malformed row with byte-identical text (`[1,null]` probed). |
| PERF-006 | P2 | Closed in `5337d55e` with L-002 (`CsvStampParsers` compiled once per invoke). |
| PERF-007 | P3 | Noted, not changed (R-18a-15): the decimal half is gone with `parse_decimal` (L-005); the boolean `to_ascii_lowercase` alloc is unmeasured on every pinned path. |
| PERF-008 | P3 | Noted, not changed (R-18a-15): the `GeneratorField` rebuild on a null-free parent is N wrapper invocations, not a payload copy; restructuring the rewrite is outside this unit. |
| PERF-009 | P3 | Noted, not changed (R-18a-15): per-row re-inference only fires when `CsvFold` misses; the foldable contract holds on every pinned path. |
| PYPERF-001 | P3 | Noted, not changed (R-18a-15): a variadic generator helper would not change kernel arity or FFI count; facade thinness holds. |
| PYPERF-002 | P3 | Noted, not changed (R-18a-15): pre-wrap plus identity-wrap costs Python objects only with the same FFI count as `from_json`, and the E1 bar needs the pre-check. |
| PYPERF-003 | P3 | Noted, not changed (R-18a-15): an empty options dict as a 3-arg empty map is equivalent to omitted options; no cell distinguishes them. |

Residuals: the four exact-expr SQL timestamp cells pin `xfail(strict=True)` on run 18c's dot-access seam (R-18a-5 extended); the SQL-door timestamp VALUE pins green beside them. Struct-to-string rendering stays Python-side (the engine has no struct-to-string cast). `F.col("s.t1")` nested access is unpinned surface; the pins read whole structs.
pins: fnp-gen-1/L-001, L-002, L-003, L-004, L-005, R-18a-13, R-18a-14, PERF-001, PERF-002, PERF-003, PERF-004, PERF-005, PERF-006

### Remediation gates (run 18a)

- `cargo test -p repark-functions --lib` → `test result: ok. 757 passed; 0 failed; 1 ignored`.
- Release native rebuilt (`maturin develop --release`) → `Installed repark-1.4.2`.
- Named pin suites (`test_fnp_gen_1.py`, `test_fnp_gen_1_s34.py`, `test_fnp_gen_1_s34_critic.py`, `test_explode_rewrite.py`, `test_fnp_9_collections_json.py`, `test_functions_c.py`, `test_functions_split_identity.py`) → `293 passed, 5 xfailed`.
- Whole parity suite (`make py-test`) → `757 passed, 2 skipped, 12 xfailed`.
- `make rust-clippy` → clean (`Finished dev profile`, no warnings).
- `make verify` → exit 0 (first attempt red only on one 101-char line in the new pin file, fixed plus `ruff format`; re-run green end to end).

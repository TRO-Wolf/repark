# Brief for the Opus 5.5 orchestrator — the shipped struct-comparison wrong write, its record, and unit `STRUCT-COMPARE-BY-POSITION-1` — 2026-11-09

**From:** Fable 5.1 (`claude-fable-5-1`), consult. Routing, records and design only; Fable builds nothing.
**To:** the Opus 5.5 orchestrator (`claude-opus-5-5`, nothing else).
**Companion documents (read in this order):** `Step 4.1-4.2 evaluation 2026-11-09.md` (the measured table, §1; the mechanism, §2; the parked branch, §3; the cards, §6; the insertion inventory, §7; the decisions, §8); `Unit C review-cap memo 2026-11-09.md` (the post-coercion seat and the insertion-inventory rule, both adopted); `Repark Master Plan 2026-11-08.md` (R2a :114, R4 :117, R7 :120, Step 3.2 :243, Step 4.2 :265).
**Repo state assumed:** main after #1279, #1280, #1282; PR #1283 (unit C, `fix/struct-setop-reorder-refusal-1`) in delta 2 and awaiting one Opus re-check; PR #1267 (`fix/ansi-subscript-and-ambiguous-field-1`, head `8e929978`) PAUSED and OPEN; PR #1284 (status docs) open, CI green, merge word not given; v1.5.6 shipped on `ae60c359`.

This brief does three things. It tells you what to record today. It tells you how to write and run the unit that fixes the shipped wrong write. It tells you what waits on the owner and what does not. Nothing in it is a ruling; where a decision is the owner's, it is marked **OWED** with the default the owner can adopt by pasting "defaults".

---

## 0. The one-paragraph version

RePark 1.5.6, and every release on DataFusion 54.1 before it, pairs the fields of two structs **by name** when a comparison operator, an `IN` list, or a join key compares a struct against a struct whose fields carry the same names in a different order. Spark pairs them **by position**. The result is a wrong boolean, and through `WHERE`, that boolean selects the wrong rows for `DELETE`, `UPDATE` and `MERGE` on Iceberg tables. The trigger is narrow (same names, different order, same field count) but the consequence is a wrong write, so it is S1 with measured DML reach, the same class unit C was dispatched for on set operations. It was planned as a refusal (Step 3.2 under R2a Option B), listed as "carded" in PR #1283's "Outside the clause", and never built. The positional pairing rule that fixes it exists on the parked branch `8e929978` and survived four verifier rounds on its comparison arms. The work is: record the defect today at clerk grade, then carve the comparison slice out of `8e929978` onto a branch from main as unit `STRUCT-COMPARE-BY-POSITION-1`, measured first, built once, critiqued once, one delta, then merged into v1.5.7.

---

## 1. The defect, measured

**Correction, 2026-11-09 (after the clerk's 4,393-cell table):** the measured table under `task/wo/struct-compare-by-position-1/evidence/` governs; the rows below are Fable's single-shape probes and two of them were misstated. Cell 39: RePark did not delete "every row"; Fable's probe ran the MERGE on a table that the preceding UPDATE and DELETE had reduced to one row, so the by-name match deleted the only row left. Measured properly, v1.5.6 deletes row 2 (by name) where Spark deletes row 1 (by position): still a wrong write, one row. Cells 37 and 38: the Spark column is **derived from Spark's SELECT with the same WHERE**, because Spark's own DELETE and UPDATE with a struct literal on an Iceberg table fail with Spark's internal error (the oracle defect the parked table already recorded); the evaluation said "would delete row 2" and this brief dropped the "would". Cell 22 depends on the data: with one row per side (`{1,2}` vs `{2,1}`) Spark finds no match; the clerk's two-row shape finds two positional matches. Cell 16 as Fable spelled it is an `IN (subquery)` in the SELECT list, which RePark refuses at physical planning; the clerk's `WHERE ... IN (subquery)` shape answers on both engines. Both shapes belong in the table.

All cells from the evaluation §1, measured 2026-11-09 on Spark 4.1.2 (the box oracle, ANSI on, case-insensitive), RePark head `b8a703c3` (Spark door, scratch-clone build) and the published `repark==1.5.6` wheel in a fresh venv. Table `t(id INT, s STRUCT<a: INT, b: INT>)`, rows `(1, {a:3,b:4})`, `(2, {a:4,b:3})`. Predicate `s = named_struct('b', 4, 'a', 3)`.

| # | Statement | Spark 4.1.2 | RePark head | 1.5.6 wheel | Class |
|---|---|---|---|---|---|
| 10 | `SELECT named_struct('a',1,'b',2) = named_struct('b',2,'a',1)` | `false` | `true` | `true` | wrong answer |
| 13 | `<` on the reordered pair | `true` | `false` | not measured | wrong answer |
| 14 | `IN (named_struct('b',2,'a',1))` | `false` | `true` | `true` | wrong answer |
| 22 | `JOIN ... ON l.s = r.s` with reordered declarations | no match | one match | not measured | wrong answer |
| 30 | `Column.isin(F.expr("named_struct('b',2,'a',1)"))` | `false` | `true` | not measured | wrong answer, DataFrame door |
| 31 | `col == F.expr(...)`, `filter("s = ...")`, `selectExpr("s = ...")` | `false` | `true` on all three | not measured | wrong answer, every string door |
| 36 | `SELECT id FROM t WHERE s = named_struct('b', 4, 'a', 3)` | row 2 | **row 1** | **row 1** | DML predicate |
| 37 | `UPDATE t SET id = 99 WHERE` same predicate | row 2 | **updated row 1** | not measured | **wrong write** |
| 38 | `DELETE FROM t WHERE` same predicate | row 2 | **deleted row 1** | **deleted row 1** | **wrong write, shipped** |
| 39 | `MERGE INTO t USING (SELECT named_struct('b',3,'a',4) AS k) u ON t.s = u.k WHEN MATCHED THEN DELETE` | matches `{4,3}` only | **deleted every row** | not measured | **wrong write** |

Cells that are safe today and change class after the fix (Spark answers, RePark refuses): 11 (`=` on different names, Spark `true`), 15 (`IN` list on different names, Spark `true`), 25 (`CAST` of a differently named struct, Spark positional). Cells Spark refuses and RePark answers (not in this unit, D4 below): 17–20 (`CASE`, `IF`, `coalesce`, `array` over a reordered pair, Spark `DATATYPE_MISMATCH.DATA_DIFF_TYPES`). Cell 24 (explicit `CAST(... AS STRUCT<a,b>)` pairs by name, Spark by position) is its own card (D5). Cell 16 (`IN (subquery)` reordered): Spark answers `false` positionally; RePark head refuses at physical planning, which is safe.

**What is pre-existing and what is not.** The pairing comes from pinned upstream code (§2), so it has been in every RePark release on DataFusion 54.1; 1.5.6 did not introduce it. Only 1.5.6 and head were measured. Say "pre-existing, measured on 1.5.6 and head" in every record; do not claim 1.5.5 or earlier as measured.

**What 1.5.6 did do.** The empty-DML guard (#1277) is the only product change in 1.5.6, and it is unrelated. Do not yank 1.5.6; it fixes a wipe and the prior wheels carry this same pairing.

**Why it reached a release unnoticed.** Step 3.2 was a refusal unit under R2a Option B with no severity reading and no release gate attached; it sat behind unit C in the queue and PR #1283's "Outside the clause" named it as "comparison operators on structs (Step 3.2)" with no S-rating. The lesson line (§3.4) is that a class rated S1 with DML reach is a unit or a release blocker, never only a card or a queue entry.

---

## 2. Why it happens, and why the fix is a rename, not a cast

Evaluation §2 has the full trace. The parts you need to write the brief:

- **Type level.** `datafusion-expr-common-54.1.0/src/type_coercion/binary.rs` `struct_coercion` (:1265): equal field count; if every left name exists on the right, `coerce_struct_by_name` (:1340) emits the **left** field order; otherwise `coerce_struct_by_position` (:1370). `comparison_coercion` reaches it at :939 and serves `=`, `<`, `IN` lists (`get_coerce_type_for_list`), join keys (`coerce_join` :192 → `coerce_binary_op` :292) and `CASE x WHEN`. The analyzer then inserts `Cast(right AS left_type)`.
- **Cast level.** `ColumnarValue::cast_to` → `cast_array_by_name` (`columnar_value.rs:306`) → DataFusion `nested_struct::cast_column` (by name) or arrow-cast `cast_struct_to_struct` (`arrow-cast/src/cast/mod.rs:2223`, by name when every target name exists, positional otherwise). The physical `CastExpr` refuses when `validate_data_type_compatibility` fails, which is why cells 11, 15 and 25 refuse rather than answer.
- **Both layers are pinned upstream crates.** Neither is RePark code. Flipping either is not an option, and the Parquet read path needs by-name pairing (Spark itself reads a `struct<b,a>` file back under `struct<a,b>` by name, cell 35).
- **The fix.** If the right operand already carries the left operand's field names before `type_coercion` runs, the type level finds equal types, inserts no cast, and the arrow fast path is positional. **Renaming the right operand's struct fields to the left operand's names before coercion is sufficient and touches no cast.** That is what `StructOperandsByPosition` on `8e929978` does: it wraps the operand in a one-argument ScalarUDF `__repark_struct_by_position__` whose return type is the renamed type and whose body re-labels the array without copying (`renamed_array`).
- **Evidence destruction to design around.** `SimplifyExpressions` folds `CAST(named_struct(...))` into a literal with the target names. A view body is stored after `apply_type_coercion`. A PREPARE body runs the bare optimizer. The rule's seat and arms must account for each (the insertion inventory, §6).

---

## 3. Records to make today (clerk grade, before any lane; none of these waits on the owner)

These are docs PRs. Haiku 5.5 clerk per the 2026-10-24 clerk-tier ruling, or you directly if the clerk would need a design decision. One PR is fine. No product code.

### 3.1 Ledger row

`task/ledgers/staging/ice-v2-label-1-ledger.md`, next id after C-115. Text to carry, in the ledger's row grammar: dated 2026-11-09; struct-against-struct comparison (`=`, `!=`, `<`, `<=`, `>`, `>=`, `<=>`), `IN` list and join key pair fields **by name** where Spark pairs by position when both operands hold the same field names in a different order; measured on the Spark SQL door, the DataFrame typed door (`==`, `isin`) and the DataFrame string doors (`filter(str)`, `F.expr`, `selectExpr`); DML reach measured: `UPDATE` and `DELETE` act on the wrong row and a `MERGE ... ON` with a reordered key deleted every row (evaluation §1 cells 36–39); severity **S1**, impact **wrong write**, compatibility **pre-existing on 1.5.6 and head; earlier wheels not measured**; the Step 3.2 refusal was never built; closes on unit `STRUCT-COMPARE-BY-POSITION-1`, its table-driven pins, and the narrow re-audit. Status OPEN. Per R2b the row carries the impact and compatibility columns in its text.

### 3.2 Card

`task/roadmap/mid-term/struct-compare-by-position-1-card-2026-11-09.md`, in the shape of `ctas-query-failure-cleanup-1-card-2026-11-05.md` (Date, Filed by, Status, Why, Proposed fix, Close). Status: "filed 2026-11-09, awaiting the owner's D1/D2 rulings; S1 with DML reach; blocks the v2 label". Proposed fix: the comparison arms of `StructOperandsByPosition` from `8e929978`, carved onto a branch from main, seated before `type_coercion` in both analyzer lists. Close: the table-driven pins per door over the measured table, both profiles, and the DML-reach probe.

Two sibling cards, each three lines, so the classes outside the unit are rulings with a domain and not loose ends (lesson 2026-11-02): `STRUCT-USER-CAST-POSITIONAL-1` (cell 24: `CAST(named_struct('b',3,'a',4) AS STRUCT<a: INT, b: INT>)` pairs by name, Spark positional, `{4,3}` vs `{3,4}`; S2, a wrong value on an explicit cast; Spark door) and `STRUCT-TYPE-UNION-ACCEPTS-REORDER-1` (cells 17–20: `CASE`/`IF`/`coalesce`/`array` over a reordered pair answer where Spark raises `DATATYPE_MISMATCH.DATA_DIFF_TYPES`; S3, accepts what Spark refuses; no wrong value measured).

Add the three cards to `task/roadmap/mid-term/map.md` (lockstep gate).

### 3.3 Release notes

`task/roadmap/mid-term/v1-5-6-release-notes-2026-11-08.md`, under `## Known issues` (line 168). One paragraph, no file paths, no account or table names: comparisons between a struct column and a struct literal or column whose fields are declared in a different order pair fields by name where Spark pairs by position; a `WHERE` built from such a comparison selects the wrong rows for `SELECT`, `UPDATE`, `DELETE` and `MERGE`; the defect is pre-existing and was measured on 1.5.6; until the fix ships, write struct literals in the column's declared field order (or compare field by field); fix unit `STRUCT-COMPARE-BY-POSITION-1`, target v1.5.7. Ruling B of 2026-11-08 ("no outward notice until the fix ships; the release notes then name the defect") applies to outward channels; the repo's own notes are the record and may carry the known-issue line now. If you read ruling B as covering the release-notes file too, hold the paragraph in the card and put it in the notes with the fix. **OWED-B** (default: the known-issue line goes in now).

### 3.4 Lesson

`task/lessons.md`, dated 2026-11-09: a class rated S1 with measured DML reach is a unit or a release blocker, never only a card or a "carded" line in a PR body; the struct-comparison pairing sat as Step 3.2 behind unit C with no severity reading while 1.5.6 shipped. Why: the queue carried the item, the release gate did not. One line, in the file's DO/Why form.

### 3.5 Handoff

`task/handoff-frontier-2026-11-07.md` (or its successor if you have moved to a dated 2026-11-09 handoff): one dated line under "the queue" placing `STRUCT-COMPARE-BY-POSITION-1` after unit C's re-check, pointing at the card and the evaluation. The evaluation and this brief are copied into `task/wo/struct-compare-by-position-1/evidence/` when the unit directory is created (§4.1), with every `<home>` path scrubbed to the `<...>` placeholders unit C's `provenance.md` uses.

### 3.6 PR #1284

Its merge word is the owner's. It is a three-line status doc, CI green, mergeStateStatus CLEAN; nothing here conflicts with it. Ask for the word in the same message as the rulings in §8.

---

## 4. The unit: `STRUCT-COMPARE-BY-POSITION-1`

### 4.1 Shape

- **Grade:** B (guided) for the build lane; the sketch is this brief plus the work order you write from it. Opus 5.5 builder. Verifiers per R7: this unit changes answers, not refusals, so it is design-heavy and Opus 5.5 and Muse MAX run **in parallel on the same head and the same table**, findings merged by class before anything returns to the builder.
- **Cap:** one build, one critique, one delta fix, one delta re-check, then park (lesson 2026-11-02). No exception is pre-granted.
- **Branch:** from main, `fix/struct-compare-by-position-1`. Not from #1267. See D1.
- **Directory:** `task/wo/struct-compare-by-position-1/` with `map.md`, `order.md`, `evidence/` (table, raw legs, provenance, this brief, the evaluation).
- **Owning layer (master plan §1.2):** a `repark-spark` analyzer rule. Spark door only. The native door (`repark.sql()`, `AnsiDialect`) gets no rule and a divergence record, because it is stock DataFusion by design (2026-11-08 memo). Not `repark-core`, where unit C's `StructSetopNames` lives, because unit C refuses on both doors and this unit implements Spark semantics on one.
- **Release:** v1.5.7, with unit C and SIGNED-ZERO-NORMALIZE-1 (Q5 of 2026-11-08 set v1.5.7 = C + signed zero; adding this unit amends Q5 and is **OWED-Q5'**, default yes).

### 4.2 Clause (what the unit claims; the table is the clause)

On the Spark door, for every door that reaches the Spark session's analyzer, a struct-against-struct operand pair under `=`, `!=`, `<`, `<=`, `>`, `>=`, `<=>`, `NOT <=>`, an `IN` list, an `IN (subquery)`, or a join key:

1. with equal field counts and pairwise field types that Spark pairs (the parked `pairing`/`widens` rule: same Spark type, numeric widening, decimal room, temporal-to-temporal, NULL), **answers what Spark answers**, pairing fields by position whatever the names are, including inside arrays, maps and nested structs;
2. with a field-count mismatch or a field-type clash, **refuses with Spark's error class** (`DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES` for binary operators, `DATATYPE_MISMATCH.DATA_DIFF_TYPES` for `IN` lists, `DATATYPE_MISMATCH.IN_SUBQUERY_DATA_TYPE_MISMATCH` for subqueries), persisted state unchanged;
3. with duplicate field names on either operand, follows R4: a combination is in the supported subset only when the table proves correct values, full schema and persisted state; everything else refuses fail-closed before coercion;
4. through DML: the same predicate in `DELETE ... WHERE`, `UPDATE ... WHERE`, `MERGE ... ON`, `WHEN MATCHED AND`, `WHEN NOT MATCHED AND` selects Spark's rows and writes nothing else;
**Correction to items 1, 2 and 4, 2026-11-09 (from the step-0 table and the owner's ruling the same day):**

- Item 1 is too wide. Spark **refuses** a pair whose field names are reordered and whose paired field types differ, even where the types would widen (`DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES`). The parked `widens` arm may answer only for pairs whose names are in the same order; a reordered pair answers only when every paired type is the same Spark type. The measured table is the clause.
- Item 4, struct-literal `DELETE`: Spark 4.1.2 on Iceberg cannot run it (`Cannot create expression literal from GenericInternalRow`), so those 72 cells have no direct oracle. Owner ruling, "default": each is held to the **derived** oracle, the fixture minus the rows Spark's `SELECT` twin selects. They do not fail closed, and the 14 same-order `DELETE`s keep answering. Record the Spark error as a declared divergence.

5. with no change to: set operations (unit C's refusal stands until `STRUCT-SETOP-BY-POSITION-1`), `unionByName` (nested fields stay by name, cell 29), the write-door pairing rule from ICE-STRUCT-PAIRING-1 (`crates/repark-spark/tests/struct_pairing_doors.rs` must stay green unchanged), file reads (by name), explicit `CAST` (carded), `CASE`/`IF`/`coalesce`/`array` (carded), the native door.

Equality with Spark is by rows and schema for answers and by **error class** for refusals, as the parked driver already does (`struct_domain_driver.py` `CLASS` regex), never by error text, because Spark renders field names in its texts and RePark's names differ until `STRUCT-NAMING-1`.

### 4.3 Independence from naming

Without `STRUCT-NAMING-1`, main names unnamed `struct(...)` arguments `c0, c1, ...` where Spark names them `col1, col2, ...` or the attribute name. This unit does not depend on that: positional pairing makes the names irrelevant to the comparison's **value**. Two places where names still show: Spark's error texts (handled by class matching) and the schema of a `SELECT struct(...) = ...` result (a boolean, no names). The one shape where naming changes a value is a by-name seat, and no by-name seat is in this clause. State this in the order so the builder does not pull the naming pass in.

### 4.4 What to carry from `8e929978`, by file, not by cherry-pick

The branch's commits interleave three halves (ANSI subscript, naming, pairing) and touch the same files (`extension.rs`, the table). Copy files at `8e929978` through `gh api .../contents/<path>?ref=8e929978` into the scratch clone and edit; do not cherry-pick. Authorship stays with the new commits under the standing trailer.

| From `8e929978` | Take | Strip or change |
|---|---|---|
| `crates/repark-spark/src/normalize/struct_position.rs` (865 lines) | `StructOperandsByPosition` + `insert_struct_position_rule_before_coercion` (:33-61); `Seat`, `Pairing`, `pairing`, `widens`, `is_string` (:63-152); `align_node` minus the `Union` arm, `has_struct_candidate`, `is_comparison`, `spark_operator`, `align_expr`, `binary_op_diff_types` (:154-304); `DuplicateFieldOperands` and its helpers (:306-424); `refuse_in_subquery`, `subquery_pairs`, `same_name`, `holds_struct_pair`, `clashes`, `aligned_operand`, `renamed_element`, `renamed_type`, `by_position`, `planned_by_position`, `planned_operand`, `positional_comparison` (:425-632); `StructByPosition` UDF and `renamed_array` (:717-865) | Delete `align_union`, `ordinal`, `ORDINALS`, `renamed_input` and the `INCOMPATIBLE_COLUMN_TYPE` text (:634-715, :26-31), unless you keep `renamed_input` for the `IN (subquery)` rename (S-1 below). Rename the file's `name()` to `struct_operands_by_position` as is. Keep it under the 1000-line ceiling (`make check-rust-file-size`). |
| `crates/repark-spark/tests/struct_domain_table.rs` | the driver shape (`include_str!` of the cells, fixtures W/VI/VL, `Reason::Card`, the panic catch, the `WRITES` read-back) | re-point at the new cells file; restrict to the comparison seats; add the door axis (§5) |
| `crates/repark-spark/tests/struct_operands_by_position.rs` | the comparison pins | drop the `SUBSCRIPT_NAMES` and naming blocks (they are `STRUCT-NAMING-1`) |
| `python/repark/tests/sql/data/struct_domain_grid.py` (458 lines) | `FIXTURES`, `OPS`, `DF_OPS`, `LEFT_ARMS`, `RIGHT_TYPES`, `OTHER_PAIRS`, `COMBOS`, `COMPARE_SEATS`, `NESTED`, `NULL_PREDICATES`, the `_cmp`/`_cell` builders | drop `VALUE_SEATS` (write-door pairing, already shipped by ICE-STRUCT-PAIRING-1 and pinned in `struct_pairing_doors.rs`), `insert_union_all`; add the `join_on`, `df_join` seats and the `door` axis |
| `python/repark/tests/sql/data/struct_domain_driver.py` | `Grid`, `value_of`, `rows_of`, `error_of`, the fresh-table-per-write rule | none |
| `python/repark/tests/sql/data/struct_domain_cells.jsonl` (1031 cells) | **nothing as-is**; re-measure on Spark with the new generator | the Spark leg must be re-run because the seat set and the door axis change; reuse is by key only |

Do **not** carry: `keyword_lower/struct_names.rs`, the `router.rs:122` call, `functions_collections.py` / `display.rs` (`F.struct` naming), the ANSI subscript files, the `skip_empty_dml`/empty-DML guard commit `8e929978` itself (superseded by #1277), `map_ordering.rs` changes beyond what `struct_position.rs` imports (`refuse_in_expr`, `render`; check they exist on main, and if `render` moved, re-point).

Helpers the file imports that exist on main today: `repark_functions::cast_map::spark_sql_name` (`cast_map.rs:70`), `repark_functions::case_sensitive::spark_case_sensitive_from_options` (`case_sensitive.rs:106`), `normalize/map_ordering.rs` `render` (:131, `pub(crate)`) and `refuse_in_expr` (:62, **private** on main; `8e929978` widened it, so the carve needs the one-line `pub(super)` on that function and nothing else from the branch's `map_ordering.rs`).

### 4.5 What is new in this unit

1. **Two call sites, one function, one test.** `normalize` is a private module in `repark-spark` (`lib.rs:33`, selective `pub use` at :134). Re-export `insert_struct_position_rule_before_coercion` and `DuplicateFieldOperands` from `repark_spark`. Call the insert from `SparkExtension::configure_analyzer_rules` (`crates/repark-spark/src/extension.rs:328-337`, after `insert_map_subscript_before_coercion`, as `8e929978` did at its :144) **and** from `build_expr_context` (`crates/repark-python/src/column/expr_build.rs:283-295`, after the map-subscript insert). Register `DuplicateFieldOperands` as an ExprPlanner in `SparkExtension::register` (as `8e929978` did) **and** on the expression context (it has no `register` hook; add it after `register_all`). Add a test beside `expr_context_seats_struct_setop_names_before_coercion` (`expr_build.rs:816-838`) asserting `struct_operands_by_position` sits before `type_coercion` in the expression context, and one in `extension/tests.rs` for the session. This is the "one insert function, two call sites, one test" rule from the evaluation §4; it is the F9 gap of #1267 closed for this rule.
2. **Cast-transparent operand read.** A view body is stored after `apply_type_coercion`, so at read the operand arrives as `Cast(inner AS left_type)` (or under an `Alias`). `align_expr` must type the operand **beneath** one `Alias` and one `Cast`/`TryCast`, rename beneath, and drop the cast when the renamed type equals the target (unit C's delta 2 reads beneath the cast the same way). Measured first in step 0 (§5, the `view` and `cte` seats on both the Spark door and a DataFrame `createOrReplaceTempView`).
3. **F10 closed.** `DuplicateFieldOperands::plan_binary_op` covers binary operators only. Duplicate-named struct operands under `IN`/`NOT IN`/`isin` reach DataFusion's duplicate-name debug assertion (caught panic on dev/CI wheels, an answer on the release wheel). Generalise to `InList` and `InSubquery` through the analyzer rule's existing `positional_comparison` path (R4: only table-proven combinations answer; the rest refuse with Spark's class). The table crosses duplicate names with every operator and every DML seat, and the Rust table test runs under **both** profiles with a pin failing any panic on either.
4. **Join keys.** `align_node` uses `map_expressions`, which for `LogicalPlan::Join` visits the `on` pairs as `Eq` binary expressions, so cell 22 should fall under the rule once seated. Prove it: add `join_on` (SQL `JOIN ... ON l.s = r.s`) and `df_join` (`a.join(b, a.s == b.s)` and `a.join(b, 'k')` where `k` is a struct) seats to the table.
5. **`IN (subquery)`.** Spark answers a reordered subquery positionally (cell 16, `false`). The parked `refuse_in_subquery` refuses when names mismatch. **S-1 (sketch decision, default):** when the subquery's output type differs from the left operand only in names, rename the subquery's output through a `Projection` of `by_position(...)` over the subquery plan (the parked `renamed_input` does exactly this for one union input; keep it for this use); when types clash, refuse with Spark's `IN_SUBQUERY_DATA_TYPE_MISMATCH` class. Fallback if the measurement shows the rename moves a cell wrongly: refuse the rename case too and card it as over-refusal (safe).
6. **Door axis in the table (§5).**

### 4.6 Not in the unit, by name

Set operations (unit C, then `STRUCT-SETOP-BY-POSITION-1`); `struct(...)` naming (`STRUCT-NAMING-1`); `CASE`/`IF`/`coalesce`/`array`/`array_contains` acceptance (card, D4); explicit `CAST` (card, D5); `INSERT ... BY NAME` nested reorder (clerk row, D6); `GROUP BY`/`DISTINCT` over structs with differing names (not a comparison operator; measure one row in step 0 and card if it differs); the native door; the metadata-only DELETE door and the Iceberg conflict filter (separate seats, table rows, §6); retiring `EXPR_CONTEXT` (4.1); Trino.

---

## 5. Step 0: the domain table (measured before the build opens)

Per the 2026-11-02 lesson and the 2026-10-22 write-path rule, the table comes first and the clause is the table. A Haiku clerk or you; no frontier build tokens. Spark 4.1.2 on the box is the oracle. RePark legs: a release-profile wheel built from the branch point on main (provenance recorded: SHA, lock hash, profile) and the PyPI 1.5.6 wheel for the DataFrame string doors (cells 30–31 were measured on head only). Scratch clones only, never the live worktree. Paths in committed files scrubbed to `<pyspark-4.1.2-oracle>`, `<warehouse-spark>`, `<warehouse-repark>`, `<venv-...>`, `<main-wheel>` as unit C's `provenance.md` does.

**Axes.**

| Axis | Values |
|---|---|
| operator | `=`, `<=>`, `!=`, `<`, `IN` list, `NOT IN` list, `IN (subquery)` |
| shape (left vs right) | same order; reordered same names; different names; field-count mismatch; nested struct reordered; array of struct reordered; map value struct reordered; typed NULL on one side; both literals; column vs literal; column vs column; duplicate names on one side; duplicate names on both sides |
| type pair | the parked `RIGHT_TYPES`/`OTHER_PAIRS` (int/int, int/bigint, int/double, int/decimal(12,2), int/string, int/boolean, float/double, date/timestamp, dec5/dec7) |
| seat | `select`, `where`, `cte`, `scalar_subquery`, `view` (SQL temp view, read after create), `df_temp_view` (`createOrReplaceTempView` then `spark.sql`), `join_on`, `delete`, `update_where`, `merge_on`, `merge_matched_and`, `merge_not_matched_and` |
| door | `sql` (`spark.sql`), `df_typed` (`==`, `<`, `isin`, `join(on=)`), `df_string` (`filter(str)`, `where(str)`, `F.expr`, `selectExpr`) |
| case flag | `spark.sql.caseSensitive` false and true |
| profile (RePark only) | release wheel; dev build for the panic pin |

Not every cross is meaningful; the generator prunes the df doors to the seats a DataFrame can express (`select`, `where`, `join_on`, `df_temp_view`) and the DML seats to `sql`. Target size is in the low thousands, as the parked table was 1031 over fewer axes. Each cell records: key, seat, door, shape, types, statement (both spellings where the df door differs), Spark result (rows+schema, `ok` with read-back, or error class), RePark result per leg, and the label (`EQUAL`, `WRONG-ANSWER`, `WRONG-WRITE`, `REFUSED-WHERE-SPARK-ANSWERED`, `ACCEPTED-WHERE-SPARK-REFUSED`, `BOTH-REFUSED`, `PANIC`).

**Controls (hand-picked, as controls only):** the eight evaluation cells 10, 13, 14, 16, 22, 30, 31, 36–39; the identical-struct same-order rows for every seat (must be `EQUAL` before and after); unit C's reordered `UNION ALL` row (must stay refused after this unit); `unionByName` nested reorder (`{4,3}`, by name, unchanged); one `struct_pairing_doors.rs` write cell (unchanged).

**Rows from the evaluation §9 that this table must include:** duplicate-name operands (R4), ANSI off, case-sensitive sessions, map-keyed structs beyond one level, one aggregate and one window over a struct (`collect_list(s)`, `PARTITION BY s`) as out-of-clause probes, the DataFrame string doors on the 1.5.6 wheel. The distributed and server doors and Trino stay unmeasured and are named as such in the order.

**Before the lane opens, the table has no "unmeasured" cell inside the clause.** A cell the oracle cannot run (Spark setup error) is `ORACLE-MISSING` and ruled fail-closed.

---

## 6. Insertion inventory (the 2026-11-09 rule; copy into the order and answer each row "seen at the seat: yes / no, because")

Evidence is the field-name list on each struct operand.

| Where evidence is created, moved or destroyed | Before the seat? | Handling in this unit |
|---|---|---|
| Session analyzer list: `context_with_df_54_1_rule_guards` (`crates/repark-core/src/session/df_guards.rs:69-100`) → `SparkExtension::configure_analyzer_rules` (`extension.rs:328`) | the seat itself | insert before `type_coercion`; pin the index |
| Expression context list: `build_expr_context` (`expr_build.rs:283`), used by `PyColumn::plan_sql_text` (`column/mod.rs:41-43`) for unbound `F.expr`/`selectExpr`, analyzed eagerly in `plan_expr_column` (`expr_build.rs:131-147`) | a second seat | insert there too; register the planner there too; pin both |
| Bound `filter(str)`/`where(str)` through `parse_canonical_predicate` (`expr_build.rs:46-70`) | uses the frame's own session | covered by the session seat; table rows prove it |
| `SqlToRel` names unnamed `struct(...)` args `c{i}` (`datafusion-sql-54.1.0/src/expr/mod.rs:833`); `struct` UDF return type `c{pos}` | created before | out of clause (`STRUCT-NAMING-1`); positional pairing is indifferent to it |
| `type_coercion` inserts `Cast(right AS left_type)` in `coerce_binary_op`, the `InList` arm, `coerce_join` | after the seat | none needed when the seat renames first |
| `create_view` stores the body after `apply_type_coercion` (`datafusion-54.1.0/src/execution/context/mod.rs:931-969`); inlined at scan build | before, for a view body | cast-transparent read (§4.5.2); `view` and `df_temp_view` seats in the table |
| PREPARE arm runs the bare optimizer (`context/mod.rs:734-763`); `SimplifyExpressions` folds literal casts | before, literal evidence destroyed | PREPARE is a native-door statement; out of clause here; one table row proves the Spark door does not reach it, else a belt arm as unit C carded |
| Recursive CTE term cast by the SQL planner | before | not a comparison seat; unit C / `STRUCT-SETOP-BY-POSITION-1` |
| `DuplicateFieldOperands` ExprPlanner runs at planning, before any analyzer | before | keep; generalise (§4.5.3); register on both contexts |
| Metadata-only DELETE door (`crates/repark-spark/src/router/delete_update.rs` `try_metadata_delete_door`) evaluates the predicate outside the analyzer | separate seat | table row per DML shape: a struct predicate must fall through to the passthrough plan or refuse; measured, not assumed |
| `crates/repark-iceberg/src/write/conflict_filter.rs:74` widens the text predicate to an Iceberg `Predicate` | separate | struct terms become `AlwaysTrue` (wider, safe); one table row |
| `crates/repark-spark/src/merge/nested_assign.rs:426` renders assignment SQL | separate | not a comparison |
| Optimizer `PushDownFilter` into `TableScan.filters` | after the analyzer | the renamed operand travels as a UDF call; one row proves a pushed struct predicate still answers |
| Native door `PreExecute` | separate session | out of clause; divergence record |

---

## 7. Pins, gates, review, halts, hand-back

**Pins (table-driven, one per door, red before and green after):**

- Rust: `crates/repark-spark/tests/struct_compare_by_position.rs`, `include_str!` of the cells, runs every `sql` cell on a `ReparkSession` with `SparkExtension` + `SparkDialect`, both case flags, asserting rows+schema or error class and, for DML seats, the read-back; a second pass under `--release` in CI with the no-panic assertion (the F10 class).
- Python facade: `python/repark/tests/sql/test_struct_compare_by_position_1.py` driving the `df_typed` and `df_string` cells through the facade, plus the `sql` controls; full facade suite as the gate (builder-gate rule 2026-11-04).
- Seat pins: the two analyzer-order tests (§4.5.1).
- Unchanged pins: `struct_pairing_doors.rs`, `test_struct_setop_reorder_refusal_1.py`, the `unionByName` cell.
- Native-door pin: the same statement on `repark.sql()` still answers by name (divergence recorded, not fixed).

**Gates:** `make verify`, `make preflight` (full facade), `make check-rust-file-size`, `make check-map-md`, `make check-model-ban`, `make rust-panic-ban`, CI green including the wheel smoke. Cost: the standing coercion-rulings bar (2026-10-12) of ≤1.05x median on the perf set for a plan-time rule; `has_struct_candidate` walks every expression of every node, so measure it on the C-012 set and record the number in the hand-back whether or not it bites.

**Review (R7):** Opus 5.5 and Muse MAX in parallel on the same head and the same table; each verifier's brief carries the standing claim "the same expression in DML" (2026-11-05 rule), the door list, the insertion inventory, and the instruction that a finding outside the table records the table as wrong and cards it rather than opening a round. Findings merged by class. One delta fix, one delta re-check. A FAIL after that parks the unit.

**Halts (end the round early, hand back):** a cell inside the clause whose Spark answer the rule cannot produce without reading beneath more than one cast; a write-door pin in `struct_pairing_doors.rs` changing; a panic on the release profile; the cost bar exceeded; any need to touch `repark-core` or the native session; any need to edit `nested_struct`/arrow behaviour; a required change to the `unionByName` path.

**Hand-back:** the table with labels before and after; the number of cells moved per label; the cost number; the insertion inventory with each row's answer; "doors I did not touch, and why"; the cards filed; the ledger row text; the PR number.

---

## 8. Decisions owed to the owner (one message, defaults marked; nothing in §4–§7 launches before the words come back)

| Id | Question | Default |
|---|---|---|
| **D1** | Carve the comparison slice from `8e929978` onto a new branch from main, or reorganise #1267 whole per the 2026-11-07 proposal? | **Carve.** #1267 closes with a comment pointing at the three successor units; its ANSI-subscript half (proven every round) re-opens as its own PR from main when a lane is free |
| **D2** | Amend R2a so this unit runs **before** the label as a fix unit, replacing the never-built Step 3.2 refusal? | **Yes.** A refusal would over-refuse cells 11 and 15 (Spark answers `true`) and must derive Spark's names anyway; the rule exists |
| **D3** | `EXCEPT`/`INTERSECT` reorder stays a refusal permanently (Spark refuses too)? | **Yes** (affects `STRUCT-SETOP-BY-POSITION-1`, not this unit) |
| **D4** | Cells 17–20 (`CASE`/`IF`/`coalesce`/`array` accept a reordered pair Spark refuses): fold into this unit or card? | **Card** (`STRUCT-TYPE-UNION-ACCEPTS-REORDER-1`, S3) |
| **D5** | Cell 24 (explicit `CAST` pairs by name): card or fold? | **Card** (`STRUCT-USER-CAST-POSITIONAL-1`, S2) |
| **D6** | Cell 34 (`INSERT ... BY NAME` nested reorder): clerk row? | **Yes**, in step 0 |
| **D7** | Rate cells 36–39 S1 with DML reach, recorded in the staging ledger beside unit C's clause? | **Yes** (§3.1) |
| **OWED-B** | The known-issue paragraph goes into the repo's v1.5.6 notes now, or with the fix? | **Now** (§3.3) |
| **OWED-Q5'** | v1.5.7 = unit C + signed zero + this unit? | **Yes** |
| **#1284** | Merge word | owner's |

Lane order after the words: unit C's Opus re-check (already launched) → this unit (step 0 table runs as clerk work alongside C's re-check, since clerk work is not a lane) → SIGNED-ZERO-NORMALIZE-1 as ruled → `STRUCT-SETOP-BY-POSITION-1` → `STRUCT-NAMING-1` → `EXPR-DOOR-UNIFY-1`. One implementation lane at a time (2026-11-02).

---

## 9. Standing constraints the order inherits (restate them in the order; do not assume the builder has them)

- Anthropic models write **no comments in code**; markdown prose is allowed; a `map.md` row carries any citation.
- Opus means `claude-opus-5-5`, verified from the transcript, never from the brief.
- Scratch clones only; the live worktree is never touched; after any fan-out confirm live `git status`, stash list and remotes.
- Commit trailer `Authored-By: Claude (claude-opus-5-5) <noreply@anthropic.com>`; never a co-author trailer, never a session link; TRO-Wolf identity.
- No `<home>` paths, account ids, buckets, regions, ARNs or the owner's catalog or table names in any committed text; use the `<...>` placeholders.
- `map.md` lockstep for every `.rs`/`.py`/`.toml` touched; file-size ceiling 1000 lines (`DEFAULT_CEILING`).
- Merges only on the owner's word. Defaults are not rulings.
- No Sail or PySail dependency; reading is allowed, depending is not.
- Usage conservation: Haiku clerk for the table and the records, Opus 5.5 for the build and one verifier, Muse MAX for the other; no Opus sub-agents without the owner naming Opus.

---

## 10. What Fable did not measure, so the order says so

Trino as a native reference; the distributed and server doors; aggregates and windows over structs beyond the two probe rows; the 1.5.5 and earlier wheels; the cost of the analyzer pass. Each is named in the order as unmeasured and ruled fail-closed or out of clause, never inferred.

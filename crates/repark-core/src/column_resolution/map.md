# map — repark-core/src/column_resolution

ICE-MIXED-CASE-1 (2026-09-17, Q-20b-2): the fold's unit battery lives here,
split from `../column_resolution.rs` under the file-size gate. Statement cells
(false/true spellings), fragment scoping, the `[AMBIGUOUS_REFERENCE]` shape,
backticked exact under `true`, and the DataFrame filter alias binding.
Round 21b: the helpers pass `case_insensitive` to `plan_statement_with_column_repair`
explicitly (the module owns no carrier); `dataframe_filter_binds_projection_alias`
plans through `sql_with_column_repair` so the flag reaches the fold.
Run 21b red-first pins: `v01_*`, `v02_*`, `v04_*`, `l08_*` on `measured_ctx` (MemTables
shaped like the probe's tables), and the 42704 / one-option-per-twin sentence.
Step 5: `l08_bare_twin_options_carry_the_full_relation_name` registers `sc.ns.tw` in a
memory catalog (the measured Spark shape) and pins the bare-name form for the session default.
Step 6: `r02_lowercase_only_plans_skip_the_audit` pins the early-exit predicate.
Step 2 adds `v01_order_by_a_select_alias_still_orders_by_the_alias` (the positional guard
keeps ORDER BY on the alias).
Round 2 (2026-09-18): the measured fixture's table tuple is the `MeasuredTable` alias
(clippy `type_complexity`).
Round 2 N-02: `l08_every_reference_to_a_case_twin_is_ambiguous` is deleted — it stayed green
with the step-5 audit reverted (the old walk refuses every such reference too; proof in the
ledger) and the Python `test_case_twin_reference_is_ambiguous_exact_or_not` holds the same four
cells. `l08_qualified_reference_is_not_ambiguous_because_a_bare_spelling_appears_elsewhere` is
the pin that goes red under that revert (the old walk over-refused); `l08_correlated_reference_to_a_case_twin_is_ambiguous`
pins the outer-reference audit (red on the round-1 head and under the revert).
Round 2 Q-21b-12: `n03_star_over_a_case_twin_answers_both_columns_declared` pins the declared
star answer on a twin MemTable (Spark refuses 42711; a star refusal here would also refuse the
DataFrame `filter` / `table` lowerings, which Spark answers — ledger N-03).
Run 22b (2026-09-18, the debug-wheel segfault): `s22b_*` plan a 1,000-branch `UNION ALL`
(plain and wrong-case fold) and a 5,000-branch one (both case modes) through
`plan_statement_with_column_repair` on a thread with a 2 MiB stack — the tokio worker default.
Red on the round-1 head: the unguarded derived `SetExpr::clone` overflows (ledger Run 22b).
The 5,000-branch pin plans in the default mode only (about 48 s in debug: DataFusion's own
per-level `span()` walk is quadratic). `s22b_stack_estimate_counts_every_union_level_inside_a_subquery`
pins the estimate. `fold.rs`'s `Level::collect` and `CaseFold::fold_usings` walk a set
operation's branches with an explicit stack (left branch first, the old recursion's order).
pins: ice-mixed-case-1/C-007, C-009, C-013, C-014, C-015, C-016, C-017, C-020, C-021

IPI-51 PR6 slice 1 (2026-09-21): a `FieldNotFound` that survives the fold is stamped
`UNRESOLVED_COLUMN.WITH_SUGGESTION` / `42703` by `stamp_unresolved_column`, while empty
valid fields fall through so frameless nullary names keep `WITHOUT_SUGGESTION`
(the fall-through is pinned by `unresolved_stamp_skips_empty_valid_fields` on `SELECT nope`,
red under deletion of the early return). The DataFrame SQL-lowering facade pins
`test_unpivot_quotes_hostile_names_and_labels`, `test_select_hostile_count_name_does_not_retarget_from`
and `test_select_batch4_af_sql_expr_and_case_preserved` require the stamped condition / `42703`,
not raw FieldNotFound text (retargeted 2026-09-21).
pins: ice-error-conditions-1/C-011

## Files

- `fold.rs` — round 21b step 3: the scope-aware statement fold (`Known` field sources,
  per-query `Level` with per-SELECT relation scopes and projection / alias-reference slots,
  `CaseFold` visitor, JOIN USING folded per query level in `pre_visit_query` (step 4, V-04),
  run 22b: set-operation branches walked with an explicit stack, no recursion,
  `fold_statement`). Split from `../column_resolution.rs`
  under the file-size gate. **WO CASESENS-1 S1b (2026-09-27):** a `Level` carries
  its CTEs' syntactic output names (computed by `scope_fields::query_outputs`),
  a CTE-shadowed `FROM` name and a derived-table alias get scope fields, so an
  outer reference binds a case-differing output; `fold_statement` first runs
  `scope_fields::InjectAliases`, which folds a column-alias list `x("ID", …)`
  into the body as `AS` items where the body's own clauses cannot
  reference them (DataFusion's alias-list path resolves `col(field.name())`
  through its normalizer and cannot see stored case). pins:
  ice-mixed-case-1/C-013, C-014, casesens-1/C-003
- `stack.rs` — run 22b: the nesting-depth stack estimate and the grown-stack future that
  `plan_statement_with_column_repair` polls the repair through. The per-level 32 KiB is
  about 1.9× the measured debug cost of the derived `SetExpr::clone` (17,216 B per `UNION`
  level); at 8 KiB the 1,000-branch pins crash (mutation proof, ledger Run 22b).
  pins: ice-mixed-case-1/C-022
  **IPI-40 PR6 (2026-09-24):** `GrownStack` and `on_grown_stack_with(red_zone, segment, future)`
  are public through `column_resolution` (`on_grown_stack` passes one value for both) so
  repark-spark's re-planning temp-view scan grows the stack the same way; removing that wrapper
  overflows the 100-level temp-view chain pins. pins: ice-views-1/C-018
- `tests.rs` — the battery below.
- `inner_scopes.rs` — **WO CASESENS-1 slice 1 (2026-09-27):** the inner-scope
  spelling pass. `respell_inner_scopes` collects the written projections of the
  original statement's derived tables and CTE bodies in visit order (the leftmost
  `SELECT` of a set-operation body; expression subqueries never collected) and
  applies them to the folded statement paired by index, refusing to guess on a
  length mismatch. An unaliased plain or compound reference whose written last
  segment differs from DataFusion's name for it gains `AS "<written>"`, an
  unquoted non-lowercase alias is double-quoted, a twice-written spelling is left
  alone, and an upper-case column-alias list is quoted. Wired into
  `finish_with_display`, which re-plans once on change and falls back to the
  pre-respell plan when the respelled statement fails (ledger D1: `r5_subq_both`
  keeps main's answer). Unit battery inline. **Fold round (2026-09-27):** the
  battery pins the twice-written spelling in another case, the unequal scope
  walks, the quoted value, the upper-case expression alias and the scalar
  subquery, each red under its guard's removal (ledger M14–M18).
  pins: casesens-1/C-001, C-002
- `scope_fields.rs` — **WO CASESENS-1 S1b (2026-09-27):** the syntactic scope
  outputs the repair fold needs before a plan exists. `query_outputs` reads a
  query's projection the way DataFusion names it (a plain or compound reference
  keeps its written segment, an `AS` alias and a column-alias list normalize as
  the planner's identifier normalizer does, a set operation takes the left
  branch, a plain `*` or `t.*` expands the statement's own relations), returning
  `None` where a shape is not computable so the scope stays opaque rather than
  guessed. `InjectAliases` rewrites `WITH c(cols) AS` and `(query) AS x(cols)`
  by moving the alias columns into the body as `AS` items, bail-out on every
  clause that can reference projection aliases (`ORDER BY`, `GROUP BY`,
  `HAVING`, `QUALIFY`, `SORT/CLUSTER/DISTRIBUTE BY`, lateral views, window
  clauses) so a bound name is never renamed.
  pins: casesens-1/C-003, casesens-1/R-CS1-1
- `fold_text.rs` — **WO CASESENS-1 slice 1 (2026-09-27):** `fold_query_text`
  runs the repair fold loop over a query text (plan, absorb `FieldNotFound`
  valid fields, `fold_statement`, re-plan, one pass per distinct miss) and
  returns the folded text, unchanged when the first plan succeeds; the MERGE
  door folds a parenthesized derived source with it before the fragment rewrite.
  pins: casesens-1/C-004
- `display.rs` — **U11-EDGE-1 (2026-09-26):** the query-spelling display rewrite. Under the
  default `caseSensitive=false`, `display_rewrite` compares the planned output names with the
  projection as written (the left branch of a set operation) and re-plans once with quoted
  aliases for every plain or compound reference whose written spelling differs (`SELECT ID` →
  `ID`, `SELECT t.ID` → `ID`, `SELECT s.A` → `A`, an unquoted alias keeps its case); `*` keeps
  the stored names; a spelling written twice is left to the planner. `GROUP BY` / `HAVING` /
  `ORDER BY` references to a re-quoted alias follow it. `strict_case_check` refuses a bare
  wrong-case reference under `caseSensitive=true` on a single-table query with Spark's
  `UNRESOLVED_COLUMN.WITH_SUGGESTION` text (`` `ID` `` and the stored names as suggestions).
  `count(*)` keeps DataFusion's `count(*)` name (Spark says `count(1)`; out of the unit).
  Measured: `target/probe-u11-edge-1/spark_case.json`, `spark_r2.json`.
  pins: u11-edge-1/C-001, C-003, C-005, C-006, C-007
  Rust pins in `tests.rs`: `display_rewrite_keeps_the_written_spelling`,
  `wrong_case_select_filters_orders_and_reads_rows`, `quoted_wrong_case_and_qualified_resolve`,
  `group_by_wrong_case_groups`, `sensitive_session_refuses_folded_names_and_keeps_backticks`;
  `dataframe_filter_binds_projection_alias` binds the now case-kept alias `Id` by its schema
  name (DataFusion's `col()` folds to `id`).
  **Round 2 (2026-09-26, V-002):** `keep_ref_qualifiers` gives a plain or compound reference
  that the repair or the display rewrite aliased (`t.id AS "ID"`, `t.Data AS data`) its
  relation back on the top projection (through `Sort` / `Limit` / `DISTINCT`), so a later
  DataFrame `F.col("t.ID")` still finds `t`; an explicit `AS` alias stays unqualified as in
  Spark. Pin `respelled_plain_references_keep_their_relation`. pins: u11-edge-1/C-018

## Purpose

Unit tests for the Spark-door case-insensitive column fold. The implementation
stays in `../column_resolution.rs`; this directory holds only the battery.
U11-EDGE-1 round 6 (2026-09-26, V-001): `tests.rs` gains
`attribute_copies_bind_case_twins_exactly_and_scratch_relations_render_unqualified` — a
`_repark_h1_sel_*` view over the `tw` case twins refuses a written `id` with unqualified
candidates, and its attribute copies answer a cast, a `CASE WHEN` and an `IN` exactly.
pins: u11-edge-1/C-027
Round 7 (2026-09-26, V-001): `a_view_body_is_not_audited_against_the_outer_statement` — a
view over the `tw` case twins projecting `ID AS id` answers `SELECT id, Data FROM vj`, while a
CTE (plain and aliased) and a derived table over the twins still refuse. Red on d3993f87
(`target/probe-u11-edge-1/red-r7-rust.txt`). pins: u11-edge-1/C-028
Round 7 (V-002, V-003): `attribute_copies_never_collide_and_suggestions_hide_scratch_names` — a
frame holding `id`, `__repark_attr_6964` and `__repark_attr_6964_` gets its `id` copy as
`__repark_attr_6964__`, answers `copy + 1` and the user field's `7`, and a missing name's
suggestion list is `[`id`]`. Red with the copy collision and with the filter off
(`red-r7-rust.txt`, mutation M19). pins: u11-edge-1/C-029, C-030

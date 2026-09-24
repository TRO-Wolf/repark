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
  under the file-size gate. pins: ice-mixed-case-1/C-013, C-014
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

## Purpose

Unit tests for the Spark-door case-insensitive column fold. The implementation
stays in `../column_resolution.rs`; this directory holds only the battery.

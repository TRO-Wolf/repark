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
pins: ice-mixed-case-1/C-001…C-010, C-013…C-016

## Files

- `fold.rs` — round 21b step 3: the scope-aware statement fold (`Known` field sources,
  per-query `Level` with per-SELECT relation scopes and projection / alias-reference slots,
  `CaseFold` visitor, JOIN USING folded per query level in `pre_visit_query` (step 4, V-04),
  `fold_statement`). Split from `../column_resolution.rs`
  under the file-size gate. pins: ice-mixed-case-1/C-013, C-014
- `tests.rs` — the battery below.

## Purpose

Unit tests for the Spark-door case-insensitive column fold. The implementation
stays in `../column_resolution.rs`; this directory holds only the battery.

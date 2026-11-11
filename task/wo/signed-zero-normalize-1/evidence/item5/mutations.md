# Item 5: the mutation record (head `0a72d7c45`)

Each mutation is applied alone, the named pins run, and `git checkout` restores the tree. The Rust pins ran in debug.
The facade pin is `python/repark/tests/sql/test_signed_zero_normalize_1.py`, on a ci-profile build of the mutated tree.

| Mutation | Pins that turn red | Measured failure |
|---|---|---|
| Seat 1 off (`install_literal_seat` adds no rule) | core: `lists_ranges_cases_and_computed_operands_follow_ieee_zero`, `the_bound_form_is_a_fixpoint_of_both_seats`, the analyzer rule-list pin `default_hooks_are_noops_and_build_succeeds_without_extension`. The DML pins and `decimal_float_coercion.rs` stay green. | Literal against literal, `(SELECT CAST('-0.0' AS DOUBLE) = 0.0)`, answers `[]`; Spark answers ids 1–8. Seat 1b cannot reach it, because the simplifier folds it first. |
| Seat 1b off (no rule after `simplify_expressions`) | core: `every_zero_shape_compares_with_ieee_equality_on_both_float_types`, `lists_ranges_cases_and_computed_operands_follow_ieee_zero`, `seat_1b_rewrites_a_zero_made_by_constant_folding`, `seat_1b_hands_the_next_pass_a_simplified_plan`, and the optimizer rule-list pin. repark-sql: `a_float_zero_delete_…` and `a_float_zero_update_…`. Facade: all five routes. | D2 wrong rows (`D2-3` `d < abs(-0.0)` answers `[[4]]`, the truth is `[]`; `D2-2`), plus D5 and A3 cells. Spark-door `DELETE WHERE d = CAST('-0.0' AS DOUBLE)` misses row 2. |
| F1 off (no `is_float_zero` check, and `Decimal::parse` accepts a zero) | repark-iceberg: `a_zero_float_literal_leaves_the_filter_unscoped` (P4), and the §5 `a_float_zero_predicate_never_takes_the_metadata_path_over_zero_only_files`. | `signed_zero_dml.rs` (D1) stays green. Per F1, D1's wrong rows were row-level misses that seat 1 fixes, so the order's "and D1" does not hold. |
| `<=>` arms off (`IsDistinctFrom` and `IsNotDistinctFrom` leave `is_float_comparison`) | repark-spark: `decimal_comparison_forms_all_widen_the_literal`. Facade: the `spark-sql` and `filter-str` routes. | The E6 refusal comes back: `D4-t7` `d <=> 0.0` raises `Cannot cast to Decimal128(30, 15). Overflowing on inf`; D5 and D6 `<=>` cells refuse the same way. |
| `CASE` arm off (no `widen_case`) | repark-spark: `decimal_comparison_forms_all_widen_the_literal`, its simple-`CASE` cases. | The facade was not run for this mutation. |
| Seat 1b's re-simplify off (item 4's cost change) | core: `seat_1b_hands_the_next_pass_a_simplified_plan`. | The optimizer takes 3 passes instead of 2. |

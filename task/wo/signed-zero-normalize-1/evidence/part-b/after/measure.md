# Part 1 measurements: the mutation record and the 50-repeat check

Head `66fb7f13` (product code identical to `e710af4f`), base frozen at `276dc503`. Measured 2026-11-12 by
`claude-opus-5-5`. The cost gates (order §8) are in [../cost/](../cost/map.md).

## 1. The mutation record (order §6)

Each mutation was applied alone to the clone's tree, built, and run against the pins named in the table. Then
`git checkout` restored the touched files, and `git status -- crates` was confirmed empty after each one.

- **The Rust pins:** `cargo test --locked -p repark-core --lib signed_zero` in debug, 27 tests. They are the P5r, P6
  and UDF tests in `signed_zero/key_tests.rs`, and the shared helper's `freq_items.rs` tests that match the filter.
- **The facade:** the literal-skip mutation also ran against a ci-profile build of the mutated tree. The files were
  part A's `sql/test_signed_zero_normalize_1.py` (P1–P4, P6), this part's `sql/test_signed_zero_keys_1.py` (P5 and
  the names and door-policy pins) and `types/test_decimal_float_coercion_1.py`.
- **The raw records:** [mutations.jsonl](mutations.jsonl).

| Mutation | The patch | The order expects red | Turns red (of 27 Rust tests) |
|---|---|---|---|
| Seat 2 off | `signed_zero.rs`: the key rule's `on: policy.keys` becomes `on: false` | the key rows (P5, P5r) | 10: `every_key_site_treats_both_zeros_and_every_nan_as_one_value_on_both_float_types`, `each_float_key_carries_one_wrapper_and_a_second_optimize_is_the_identity`, `prepare_then_execute_wraps_each_key_once`, `a_view_and_a_hand_built_repartition_wrap_their_keys_once`, `a_recursive_union_dedups_both_zeros`, `group_and_distinct_outputs_store_the_positive_zero`, `output_names_and_qualifiers_survive_the_wrap`, `no_provider_in_use_takes_a_wrapped_filter_as_exact`, `a_set_statement_cannot_reach_the_door_policy`, `the_keys_switch_off_restores_the_bit_pattern_keys_on_that_session_only` |
| Idempotence off | `keys.rs`: `Scope::normalized` always answers `false`, so an already-wrapped key or a column defined by a wrapper is wrapped again | the one-wrapper pins | 5: `each_float_key_carries_one_wrapper_and_a_second_optimize_is_the_identity`, `prepare_then_execute_wraps_each_key_once`, `a_view_and_a_hand_built_repartition_wrap_their_keys_once` (it shows `__repark_normalize_zero__(__repark_normalize_zero__(g.d))`), `a_recursive_union_dedups_both_zeros`, `no_provider_in_use_takes_a_wrapped_filter_as_exact` |
| The UDF's NaN canonicalisation off | `repark-common` `float_canon.rs`: `canonical_f64` and `canonical_f32` return a NaN as given | NN-* | 6: `every_key_site_treats_both_zeros_and_every_nan_as_one_value_on_both_float_types` (its NaN-payload rows), `the_kernel_maps_both_zeros_and_every_nan_and_keeps_nulls_and_other_bits`, `a_view_and_a_hand_built_repartition_wrap_their_keys_once`, `no_provider_in_use_takes_a_wrapped_filter_as_exact`, `a_set_statement_cannot_reach_the_door_policy`, `the_keys_switch_off_restores_the_bit_pattern_keys_on_that_session_only` |
| The UDF's zero map off | `float_canon.rs`: `value + 0.0` becomes `value` in both helpers | every zero key row | 13: the 10 key and UDF tests (`every_key_site_…`, `prepare_then_execute_…`, `a_view_…`, `a_recursive_union_…`, `group_and_distinct_outputs_…`, `no_provider_…`, `a_set_statement_…`, `the_keys_switch_off_…`, `the_kernel_maps_…`, `the_udf_maps_a_scalar_and_keeps_its_field`), plus the shared helper's `freq_items.rs` users `float32_signed_zero_keys_collapse`, `signed_zero_keys_collapse_at_capacity` and `signed_zero_keys_collapse_to_first_inserted` |
| The `keys` switch off on one door | not mutated separately | that door's key rows only | held by `the_keys_switch_off_restores_the_bit_pattern_keys_on_that_session_only` (one session off, its twin on) and by the facade's `test_a_set_on_either_door_cannot_turn_the_key_wrap_off` |
| The literal skip off (seat 2 wraps a literal) | `keys.rs` `wrap_key`: a float literal is wrapped in `__repark_normalize_zero__` instead of canonicalised in place | part A's P1 or a names pin | **none**: 27 of 27 Rust tests pass, and the 18 facade tests pass on the ci build. **A surviving mutant**, see below |
| Null-aware anti-join wrapping off | `keys.rs`: a join's float pair is wrapped only when `!join.null_aware` | SQ-notin-ieee (default, Q1) | 1: `every_key_site_treats_both_zeros_and_every_nan_as_one_value_on_both_float_types`, at `SELECT id FROM t WHERE d NOT IN (SELECT d FROM u WHERE id IN (1, 3)) ORDER BY id` (the SQ-notin-ieee shape) |

**The surviving mutant (literal skip).** No pin that ran turns red:
- `key_tests.rs` has no float literal in a key position;
- no failure appears in the three facade files either, so none of their cells reaches seat 2 with a literal key after
  the optimizer.

The mutation changes the plan only, not the answer: the UDF over a literal gives the canonical literal that the
in-place path writes. So the order's expectation, "part A's P1 or a names pin", does not hold on this head. Closing
it needs an EXPLAIN pin with a literal in a key position that survives the optimizer: for example a `PARTITION BY`
or join pair with a literal side, checked for zero wrappers on the literal. That is a pin to add in this unit's delta
round. Not added here: this lane's brief is measurement only.

## 2. UPDATE-SCHEMA-BATCH-MISMATCH-1, 50 repeats per kind (order §7)

**The probe:** [schema_batch_repeat.py](schema_batch_repeat.py), on the base package (`276dc503`) and the head package
(`66fb7f13`), each a release build in its own directory.
- The raw results are [schema-batch-repeat-276dc503.json](schema-batch-repeat-276dc503.json) and
  [schema-batch-repeat-head-66fb7f13.json](schema-batch-repeat-head-66fb7f13.json).
- Each repeat builds a fresh two-file table per kind: one file holds the `-0.0` row (id 4), the other the `0.0` row
  (id 6). The truth is both rows updated.

| Statement | Kind | Base `276dc503` | Head `66fb7f13` |
|---|---|---|---|
| literal: `WHERE d <=> 0.0` | v2-cow | 24 answered, **26 refused** (mismatch) | 24 answered, **26 refused** |
| literal | v2-mor | 50 answered | 50 answered |
| literal | v3 | 25 answered, **25 refused** | 28 answered, **22 refused** |
| key: `WHERE d IN (SELECT d FROM src)`, `src` = one `0.0` | v2-cow | 50 wrong (row 4 not updated) | 25 answered, **25 refused** |
| key | v2-mor | 50 wrong | 50 answered |
| key | v3 | 50 wrong | 26 answered, **24 refused** |

**The like-for-like rate does not rise.** The literal statement touches both files on both builds. It refuses
51/150 on the base and 48/150 on the head, so by that reading there is no §7 halt.

**The key statement is new exposure, not a higher rate:**
- On the base it never touches the `-0.0` file. It answers wrong 150 times (an S1 wrong write) and never refuses.
- On the head the key matches both zeros (the fix), so it touches both files. It then refuses at the card's existing
  rate, 49/150, and never answers wrong.
- **This needs a ruling.** The order's §7 halt asks whether the card reproduces more often after the change, and I do
  not settle that here. Per touched file pair, the rate equals the literal statement's. Counted per statement, the
  key form goes from 0 refusals to 49.

Up: [map.md](map.md).

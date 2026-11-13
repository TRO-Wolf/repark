# Part 1 measurements: the mutation record, the literal-skip pin and the 50-repeat check

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
| The literal skip off (seat 2 wraps a literal) | `keys.rs` `wrap_key`: a float literal is wrapped in `__repark_normalize_zero__` instead of canonicalised in place | part A's P1 or a names pin | On `66fb7f13`: **none** (27 of 27 Rust, 18 of 18 facade on the ci build). After the §1a pin: **1 of 28**, `a_float_literal_key_is_canonicalised_in_place_and_never_wrapped` |
| Null-aware anti-join wrapping off | `keys.rs`: a join's float pair is wrapped only when `!join.null_aware` | SQ-notin-ieee (default, Q1) | 1: `every_key_site_treats_both_zeros_and_every_nan_as_one_value_on_both_float_types`, at `SELECT id FROM t WHERE d NOT IN (SELECT d FROM u WHERE id IN (1, 3)) ORDER BY id` (the SQ-notin-ieee shape) |

### 1a. The literal-skip pin (delta, 2026-11-13)

On `66fb7f13` the literal-skip mutant survived every pin. The question was whether any reachable plan keeps a float
literal in a seat-2 key position after the optimizer. **It does**, so the mutant is not equivalent, and a plan pin
now holds it.

**Method.**
- **Core session:** a throwaway probe test (not committed) ran 63 SQL statements and 6 hand-built plans on the
  `key_tests.rs` session. It printed each optimized plan on the head, then again with the mutation applied. A plan
  that differs is one where seat 2 met a literal in a key position.
- **The facade:** the Spark door (`spark.sql`), the DataFrame API with `F.lit` and `F.expr`, and the native door
  (`repark.sql`). It ran `EXPLAIN` on 34 shapes over an Iceberg table on the head's ci build. A float literal that
  survives in a key position there is a position the mutant would wrap. The facade was not rebuilt with the mutation;
  the pin is the Rust one.

**Reached: the optimized plan keeps a float literal in a key position** (core: the plan differs under the mutation).

| Shape | Door | What the head's optimized plan holds | Under the mutation |
|---|---|---|---|
| `count(*) OVER (PARTITION BY -0.0)` | core, native | `PARTITION BY [Float64(0)]` | `PARTITION BY [__repark_normalize_zero__(Float64(0))]` |
| `PARTITION BY -0.0D` | Spark | `PARTITION BY [Float64(0)]` | not run |
| `Window.partitionBy(F.lit(-0.0))` | DataFrame | `PARTITION BY [Float64(0)]` | not run |
| `PARTITION BY d, CAST(0.0 AS FLOAT)` | core, native, Spark | `PARTITION BY [__repark_normalize_zero__(t.d), Float32(0)]` | the `Float32(0)` is wrapped |
| `PARTITION BY coalesce(NULL, -0.0)` | core, Spark (`-0.0D`) | folded to `PARTITION BY [Float64(0)]` | wrapped |
| `rank() OVER (ORDER BY CAST('NaN' AS DOUBLE))` | core, native | `ORDER BY [Float64(NaN) …]` | wrapped |
| `Window.orderBy(F.lit(float('nan')))` | DataFrame | `ORDER BY [Float64(NaN) …]` | not run |
| `count(*) OVER (PARTITION BY d ORDER BY -0.0)` | core | `ORDER BY [Float64(0) …]` | wrapped |
| `ORDER BY -0.0, id` | core, native, Spark (`-0.0D`) | `Sort: Float64(0) …, t.id …` | wrapped |
| `ORDER BY CAST('NaN' AS DOUBLE), id` | core, Spark | `Sort: Float64(NaN) …` | wrapped |
| `GROUP BY ROLLUP(d, 0.0)` | core, native | `ROLLUP (__repark_normalize_zero__(t.d) AS d, Float64(0))` | the `Float64(0)` is wrapped |
| `GROUPING SETS ((d), (CAST(-0.0 AS DOUBLE)))` | core, Spark | `(Float64(0) AS Float64(-0))` | wrapped |
| `count(DISTINCT 0.0)` | core, native | the distinct rewrite's `groupBy=[[Float64(0) AS alias1]]` | wrapped |
| `count(DISTINCT 0.0D)`, `countDistinct(F.lit(0.0))` | Spark, DataFrame | the distinct argument `count(DISTINCT Float64(0))` | not run |
| `count(DISTINCT CAST('NaN' AS DOUBLE))` | core | the distinct argument `Float64(NaN)` | wrapped |
| `array_agg(d ORDER BY -0.0)` | core | the aggregate's `ORDER BY [Float64(0) …]` | wrapped |
| hand-built `Repartition` `Hash(lit(0.0), id)`, `Hash(lit(-0.0))`, `DistributeBy(lit(NaN))`; `Sort(lit(0.0))` | core | the literal, canonical | wrapped |

The mutation changes no answer: the UDF over a literal gives the canonical literal the in-place path writes, and a
constant key does not split rows. So no facade cell is added.

**Not reached** (the plan is the same under the mutation):

| Shapes | Why seat 2 never meets the literal as a key |
|---|---|
| `GROUP BY d, -0.0`; `GROUP BY CAST(-0.0 AS FLOAT)`; `GROUP BY 1` over a literal; `GROUP BY k` with `k` a literal alias; `GROUP BY coalesce(NULL, -0.0)`; `DISTINCT ON (0.0)`; hand-built `Aggregate` on `lit(1.5)`; on the facade `groupBy(F.lit(-0.0))`, `groupBy(F.expr('CAST(-0.0 AS FLOAT)'))` and Spark `GROUP BY d, -0.0D` | the optimizer drops a constant plain `GROUP BY` key (`groupBy=[[]]`); only `ROLLUP` and `GROUPING SETS` keep it |
| `SELECT DISTINCT -0.0`; `DISTINCT d, CAST('NaN' AS FLOAT)`; a group over a subquery's literal column; `ORDER BY k` over a literal alias; a join on a subquery's literal column; every set operation with a literal arm (`UNION`, `INTERSECT`, `EXCEPT`, two-column forms, `FLOAT` casts); `IN (SELECT -0.0)`, `NOT IN (SELECT NaN)` | the literal stays in a `Projection` below, and the key is a column (wrapped as a column, on the head too) |
| `CASE WHEN … THEN -0.0 ELSE 0.0 END`, `coalesce(d, -0.0)`, `0.0 + u.d`, `CASE WHEN u.id > 0 THEN 0.0 END` as a group, partition or join key | the key is not a literal; seat 2 wraps the whole expression on both builds |
| comparisons with a literal side: `d = -0.0`, `-0.0 = u.e` and `CAST('NaN' AS DOUBLE) = u.e` in a join `ON`, `FULL JOIN … ON t.d = -0.0`, `d IN (e, 0.0, …)` short and long, `BETWEEN e AND 0.0`, simple `CASE d WHEN 0.0 …`, `FILTER (WHERE d IN (…))`, `EXISTS` with `u.e = 0.0`, `-0.0 IN (SELECT d …)`, `NaN NOT IN (SELECT d …)` | the simplifier turns `IN`, `BETWEEN` and simple `CASE` into binary comparisons; part A's seat 1 turns a zero literal into a range; seat 2's comparison walk skips a literal side before `wrap_key` |
| Spark `PARTITION BY -0.0`, `rollup('d', F.lit(0.0))` | the literal is `DECIMAL(1,1)`, not a float |
| `IS NOT DISTINCT FROM -0.0` in a join `ON`; a recursive CTE over a bare literal; `(d, -0.0) IN (SELECT d, e …)`; a hand-built join on `lit(0.0)`; Spark `ROLLUP(d, 0.0D)` (`fold_spark_numeric_casts`); Spark `double('NaN')`; the `F.expr` window `count(*) OVER (PARTITION BY -0.0D)` (schema error) | the statement does not plan on the head |
| DataFrame `orderBy(F.lit(-0.0), 'id')`, `orderBy(F.expr('-0.0D'), 'id')`, `repartition(4, F.lit(-0.0))` | the facade's `explain` shows no `Sort` or `Repartition` node for these |

**The pin:** `key_tests.rs::a_float_literal_key_is_canonicalised_in_place_and_never_wrapped`. It covers eight of the
reached core shapes: `PARTITION BY -0.0`, `PARTITION BY d, CAST(0.0 AS FLOAT)`, window `ORDER BY` NaN, `ORDER BY -0.0`,
`ROLLUP(d, 0.0)`, `GROUPING SETS` with `CAST(-0.0 AS DOUBLE)`, `count(DISTINCT 0.0)` and `array_agg(d ORDER BY -0.0)`.
For each one it asserts:
- the key node holds the canonical literal (for example `PARTITION BY [Float64(0)]`);
- the plan has exactly the expected number of wrappers (0, or 1 where a column key sits beside the literal);
- no wrapper takes a literal argument;
- a second optimize is the identity.

**Runs:**
- **Green on the head:** `cargo test --locked -p repark-core --lib signed_zero` gives 28 passed.
- **Red under the mutation:** the same command gives 27 passed and 1 failed (the pin). Its first failing case is
  `PARTITION BY -0.0`, which now shows `PARTITION BY [__repark_normalize_zero__(Float64(0))]`.
- `git checkout` restored `keys.rs`, and `git status -- crates` showed only the pin's test file.

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
- **RULED 2026-11-13** (orchestrator, as the order's author): not a halt. The ruling is in the order,
  [order-part-b.md](../../../order-part-b.md), under "Rulings after the measurements". The owner may overrule.

## 3. A note for the verifiers: P5 and OB-4's row order

- **What the pin asserts.** P5 (`test_signed_zero_keys_1.py::test_every_float_key_cell_reaches_the_spark_truth`)
  marks the OB-4 cells `ordered`. So it compares the rows as a list: `ORDER BY d` (and `ORDER BY f`) over ids 1–4,
  with no tie-breaker, must return `0.0, -0.0, 0.0, -0.0` in that exact order, signs included, on the Spark-SQL and
  native routes.
- **What the row says.** The registry row SIGNED-ZERO-KEYS-1-TIES (`docs/spark-sql-iceberg-parity.md`) declares that
  order unspecified. The two zeros are ties, and "nothing guarantees" the measured order.
- **Why the pin asserts it anyway.** `facade_cells.py` writes the cell file and copies each cell's `ordered` flag from
  the head read leg. OB-4 carries `ordered: true`, and its truth is Spark's recorded answer. On the one-file table the measured order equals Spark's input
  order on every door, so the exact-order assertion is green. So the pin asserts more than the row promises.
  - A change that keeps the zeros equal but reorders the ties (for example a different sort, or a multi-file layout)
    would turn it red, and the row says that is allowed.
  - Kept as is: the brief leaves the judgement to the verifiers. The narrower form would compare OB-4 as a multiset,
    or check only that the column is all zeros.

Up: [map.md](map.md).

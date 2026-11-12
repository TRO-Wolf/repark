# LISTVIEW-INNER-JOIN-KEY-1, build item 1: the cause, the completed table, the proposed fix

**Builder:** Claude Opus 5.5 (`claude-opus-5-5`), 2026-11-11. **Tree:** this branch's base, the order commit on main
`f341171a` (no product code differs from main). **Build:** release profile, `debug_assertions` false.
**Oracle:** Spark 4.1.2 with the Iceberg 1.11.0 runtime, one JVM under the shared lock. No product code is written in
this item, and no timing is taken.

Everything below is measured unless it says READ (read from source) or INFERRED.

## 1. The cause in one paragraph

The DataFrame key-name join builder `join_on_named_keys` (`crates/repark-core/src/session/df_guards/case_bind.rs:618`)
hands each key pair to `LogicalPlanBuilder::join` (`:652-659`), which puts it into `Join.on` without a type check
(READ, `datafusion-expr-54.1.0/src/logical_plan/builder.rs:1041-1166`). DataFusion 54.1's optimizer rule
`eliminate_cross_join` (registered for every query, `datafusion-optimizer-54.1.0/src/optimizer.rs:294`) flattens every
**Inner** join: `flatten_join_inputs` copies `join.on` into its key set and only `join.filter` into the filters it
re-adds (`eliminate_cross_join.rs:236-247`). `find_inner_join` then keeps a key pair only when
`can_hash(left type)` holds (`:322-327`) and has no else branch, so a pair that fails `can_hash` is dropped with no
filter. `can_hash` (`datafusion-expr-54.1.0/src/utils.rs:863-909`) is false for `ListView`, `LargeListView`, `Union`,
`RunEndEncoded`, a `Dictionary` over any of those, and any `List`, `LargeList`, `FixedSizeList`, struct or map that holds
one. With no key left the node becomes `Cross Join:`, `optimize_projections` prunes the key from both scans, and the
physical plan is `CrossJoinExec`. With a second, hashable key (`["k", "j"]`) the join keeps `j` alone. The rule runs
only for Inner joins, which is why every outer, semi and anti cell is correct. **Owning layer: RePark's key-name
builders; the fix sits there, before planning. No DataFusion change is needed.**

## 2. The plans before and after `eliminate_cross_join` (main)

From `EXPLAIN VERBOSE` over each frame (`raw/main/plan.jsonl`, 45 records; the full per-stage list is kept for the
`LV_I64` and `L_I64` `df_str` cells). `writeTo` is captured as `EXPLAIN VERBOSE INSERT INTO <iceberg table> SELECT lid,
rid FROM <the frame's view>`, the same frame the `writeTo` cell appends. In every record the stage before the first
`eliminate_cross_join` is `logical_plan after eliminate_filter`, and every earlier stage (all analyzer rules,
`type_coercion`, RePark's `struct_operands_by_position` and `join_condition_refusals`) shows the same join line. That
was checked over the full stage lists of all 45 records in scratch; the committed records keep the stage before and
after (`ecj_before`, `ecj_after`, `physical_join`) and the key stages' text, and the full list for two cells.

| Kinds | Form | Before `eliminate_cross_join` | After | Physical |
|---|---|---|---|---|
| the 7 non-hashable kinds | `df_str`, `df_list`, aggregate, `writeTo` | `Inner Join: _repark_jl….k = _repark_jr….k` | `Cross Join:` | `CrossJoinExec` |
| the 7 non-hashable kinds | `df_two` (`["k", "j"]`) | `Inner Join: ….k = ….k, ….j = ….j` | `Inner Join: ….j = ….j` | `HashJoinExec … on=[(j@1, j@1)]` |
| `L_I64`, `NEST_L` (controls) | all five forms | `Inner Join: ….k = ….k` | unchanged | `HashJoinExec … on=[(k@1, k@1)]` |

The 7 non-hashable kinds are `ListView` and `LargeListView` of `Int64`, `Utf8` and a struct, and a struct holding a
`ListView` (`NEST_LV`). The `df_two` row is the order's "joins on `j` alone", now read from the plan.

## 3. The experiment (scratch only, never committed)

A scratch patch on this clone, built release into the clone's own environment, switched by an environment variable
read per process, then reverted with `git checkout` and the main module restored (`git diff -- crates` is empty at
the commit). Three modes, each run over the `failing`, `variant`, `routes`, `mixed`, `lateral` and `plan` sections
(`raw/exp-*`):

- **`exp-norule`:** `unnest_safe_optimizer_rules` (`df_guards.rs:110`) skips the rule named `eliminate_cross_join`.
- **`exp-filter`:** for an Inner join, `join_on_named_keys` moves each pair whose left type fails `can_hash` out of
  `on` into the join filter (`l.k = r.k`, qualified), and keeps the hashable pairs in `on` (candidate shape A).
- **`exp-cast`:** for every join type, `join_on_named_keys` replaces each non-hashable key with
  `CAST(key AS <hashable kind>)` and calls `join_with_expr_keys`; hashable keys stay plain columns (candidate shape B).
  The hashable kind maps `ListView` to `List`, `LargeListView` to `LargeList`, `RunEndEncoded` and a non-hashable
  `Dictionary` to their value type, and rewrites `List`, `LargeList`, `FixedSizeList`, struct and map children the same
  way.

**Result: the key holds in every mode.**

| Row | main | `exp-norule` | `exp-filter` | `exp-cast` |
|---|---|---|---|---|
| `df_str`, `df_list` inner, 7 kinds (14) | CROSS-PRODUCT | EQUAL | EQUAL | EQUAL |
| `df_two` inner, 7 kinds | WRONG-ANSWER | EQUAL | EQUAL | EQUAL |
| aggregate, 7 kinds | WRONG-ANSWER | EQUAL | EQUAL | EQUAL |
| `writeTo().append()`, 7 kinds | WRONG-WRITE | EQUAL | EQUAL | EQUAL |
| type variants `df_str`, `df_list` inner, 7 kinds (14) | CROSS-PRODUCT | EQUAL | EQUAL | EQUAL |
| type variants `writeTo`, 7 kinds | WRONG-WRITE | EQUAL | EQUAL | EQUAL |
| pandas `ListView`, `LargeListView`, `RunEndEncoded` (3) | CROSS-PRODUCT | EQUAL | EQUAL | EQUAL |
| mixed `ListView` × `List` key name, inner and left (24) | REFUSED | REFUSED | REFUSED | **EQUAL** |
| mixed `ListView` × `List` `ON` form, inner (12) | REFUSED | REFUSED | REFUSED | REFUSED |
| `lateralJoin` key name, 7 kinds inner + list form (8) | cross product | **9 rows, the key holds** | cross product | cross product |
| every control (List kinds, `ON`, left, semi, SQL comma join) | EQUAL | EQUAL | EQUAL | EQUAL |

The plans agree (`raw/exp-*/plan.jsonl`). `exp-norule` plans `HashJoinExec … on=[(k@1, k@1)]` on the `ListView`
key itself, so DataFusion's hash join handles the type; only the rule drops it. `exp-filter` plans
`NestedLoopJoinExec … filter=k@0 = k@1`, an O(n·m) join. `exp-cast` plans
`HashJoinExec … on=[(CAST(….k AS List(Int64)), …)]`. Under `exp-cast` the `L_I64` and `NEST_L` plans are identical to
main's at every stage once the random alias suffixes are masked, for the four non-write forms (the `exp-*` `writeTo`
plan records failed on an invalid scratch table name and are not evidence; the `exp-*` `writeTo` cells themselves ran). The lateral row moves only under `exp-norule`,
because the lateral builder is a second seat that the other two patches did not touch (§5).

## 4. The other candidates, ruled out

| Candidate | Evidence |
|---|---|
| `join_using` | The key-name path never calls it: `case_bind.rs:652` calls `join`, which builds `JoinConstraint::On` (READ). `join_using` would plan a non-hashable key as `cross_join(…).filter(…)` (READ, `builder.rs:1213-1229`), and the initial plan instead shows `Inner Join: k = k` (§2). `join_using` is the cause of a different row, SQL `USING` (§6). |
| The key projection after the join (`case_bind.rs:671-688`) | It runs the same code for every join type, and the left, semi and anti cells on the same frames are EQUAL. The plan is already `Cross Join:` at `eliminate_cross_join`, before the projection is optimized. All three experiments fix the result without touching it. |
| The facade schema reporting `ListView` as `string` | The facade reports `string` for `ListView`, `LargeListView` and `RunEndEncoded` columns (`raw/main/routes.jsonl`, `schema_report`), from the Python fallback `return StringType()` (`python/repark/src/repark/spark/types.py:1307`, READ). The join does not read it: the Python entry passes key names only (`core.py:2218-2246`, READ), and the Rust builder binds by name against DataFusion's own schema (`case_bind.rs:637-644`). The same frames join correctly on left, semi and anti, and under all three experiments with the report unchanged. Binding 4: carded, not the cause. |
| RePark's own analyzer rules (struct pairing, join refusals, coercion) | Every analyzer stage shows `Inner Join: k = k` unchanged (§2). `NEST_LV`, a struct key, fails the same way as `LV_I64`, and the plain struct twin `NEST_L` passes. |
| Hashing or comparing a `ListView` | Left, right, full, semi and anti key-name joins run `HashJoinExec` on the `ListView` key and are EQUAL (step-0 table), and `exp-norule` plans the inner join as `HashJoinExec` on it and is EQUAL. |
| The WHERE route into the same rule | SQL `FROM l, r WHERE l.k = r.k` (a Filter over a cross join, which `eliminate_cross_join` also rewrites) is EQUAL for all 9 kinds: the rule keeps the predicate it could not use as a key (READ, `remove_join_expressions`). The DataFrame twin `l.crossJoin(r).where(l.k == r.k)` refuses for every kind, `List` included (`No field named l.k`): an alias-resolution gap, not this defect, carded (§9). |

## 5. The named seats

| Seat | Builds `Join.on` from names | Measured |
|---|---|---|
| DataFrame `join(other, "k" / ["k", …], how)` → `join_on_named_keys` (`case_bind.rs:618-688`, called from `crates/repark-python/src/dataframe_names.rs:153`) | yes | the defect (§2) |
| DataFrame `lateralJoin(other, "k" / ["k"], how)` → `lateral_join` (`crates/repark-python/src/subquery.rs:167-187`, `Join::try_new` with name pairs) | yes | **the same defect:** 8 cross products (the 7 kinds' inner `lateral_str` and `LV_I64` `lateral_list`), fixed by `exp-norule` only |
| SQL `JOIN … USING` / `NATURAL JOIN`, and the native door's `USING` → DataFusion `join_using` | via DataFusion | refused, a separate DataFusion cause (§6) |
| `Join::try_new` in `attr_id.rs:717` and `subquery.rs:161` (core) | no: they rebuild an existing join's `on` | not a seat |

Spark refuses key names in `lateralJoin` outright: PySpark 4.1.2 takes only a `Column` there (`'str' object has no
attribute '_jc'`). RePark accepts them for every kind, a pre-existing over-acceptance already carded by the family as
`LATERAL-USING-ACCEPT-1` (`task/wo/struct-compare-seat-family-1/evidence/pin_cells.py:18`).

## 6. SQL `USING` and `NATURAL`: a second DataFusion cause

SQL `JOIN … USING (k)`, `LEFT JOIN … USING (k)`, `SELECT *` over `USING`, an aliased `USING` and `NATURAL JOIN` all
refuse on a `ListView` key and answer on a `List` key (`raw/main/using.jsonl`, 10 cells). The native door, which is
stock DataFusion, refuses the same 7 kinds with DataFusion's own text, `Schema error: No field named lv_r.k. Valid fields
are lv_l.lid, lv_l.k, lv_l.id, lv_l.j.` (`raw/main/native.jsonl`); the Spark door maps it to
`UNRESOLVED_COLUMN.WITH_SUGGESTION`.

The site (READ, `datafusion-expr-54.1.0/src/logical_plan/builder.rs:1188-1222`, `join_using`): when the left key fails
`can_hash`, the second branch evaluates `can_hash(field_from_column(self.plan.schema(), r)?)`, looking the **right**
key up in the **left** schema, and the `?` returns that error. The branch after it would also be wrong if reached: it
plans `cross_join(right).filter(l = r)` and ignores the join type, so a `LEFT JOIN … USING` would become an inner join.
The cause sits in DataFusion, outside the key-name builder. **Binding 3: it stays refused (it fails closed) and is
carded, with an upstream report.**

## 7. The completed table (`table.md`, `raw/main`, `raw/spark`)

251 cells on main and 251 on Spark 4.1.2 in seven sections, plus 45 plan records on main. The `failing` section re-runs
the step-0 failing forms on this tree and adds two WHERE forms; it reproduces the step-0 table's failing cells exactly
(14 cross products, 7 `df_two`, 7 aggregate, 7 writes, 7 SQL `USING`).

| Class on main | failing | variant | lateral | native | routes | mixed | using | total |
|---|---|---|---|---|---|---|---|---|
| CROSS-PRODUCT | 14 | 14 | | | 3 | | | **31** |
| WRONG-ANSWER | 14 | | | | | | | **14** |
| WRONG-WRITE | 7 | 7 | | | | | | **14** |
| REFUSED-WHERE-SPARK-ANSWERED | 16 | | | 7 | | 36 | 5 | **64** |
| ACCEPTED-WHERE-SPARK-REFUSED | | | 17 | | | | | **17** |
| BOTH-REFUSED | | 5 | | | | | | **5** |
| STRUCK-NO-ROUTE | | 15 | | | 4 | | | **19** |
| EQUAL | 48 | 14 | | 19 | 1 | | 5 | **87** |
| total | 99 | 55 | 17 | 26 | 8 | 36 | 10 | **251** |

- **failing (99):** the 7 kinds and 2 controls × `df_str`, `df_list`, `df_two`, aggregate, `writeTo`, left, left semi,
  SQL `ON`, SQL `USING`, SQL comma join with WHERE, and `crossJoin().where`. The 16 refusals are the 7 SQL `USING`
  cells (§6) and the 9 `crossJoin().where` cells (every kind, §4).
- **variant (55):** 11 type variants × inner `df_str`, inner `df_list`, `writeTo`, SQL `ON` and left `df_str`.
- **lateral (17):** the 13 base kinds' inner `lateral_str`, plus `lateral_list` and left for `LV_I64` and `L_I64`.
  Spark refuses all 17. 8 of them are also cross products on main (§5).
- **native (26):** the order's `arrow_cast` `ON` and `USING` controls, one each per base kind, as one self-contained
  query each (common table expressions, `make_array`). `ON` is EQUAL for all 13 kinds. `USING` is EQUAL for the 6
  hashable kinds and refused for the 7 others (§6).
- **routes (8):** pandas and Polars build routes (§8).
- **mixed (36):** the family's 24 mixed `ListView` × `List` key-name cells (§8), and 12 `ON`-form twins.
- **using (10):** §6.

## 8. The type-variant inventory, the routes and the mixed cells

**Produced, and failing like `ListView`** (SQL `arrow_cast`; each: inner `df_str` and `df_list` cross products, the
`writeTo` persisting the cross product, SQL `ON` and left EQUAL):

| Kind | Arrow type reached | Spark twin |
|---|---|---|
| `REE_I64`, `REE_UTF8` | `run_end_encoded<run_ends: int32, values: int64 / string>` | `bigint`, `string` |
| `L_LV`, `LL_LV`, `FSL_LV` | `list` / `large_list` / `fixed_size_list[2]` of `list_view<int64>` | `array<array<bigint>>` |
| `STRUCT_REE` | `struct<a: run_end_encoded<…, int64>>` | `struct<a:bigint>` |
| `L_REE` | `list<run_end_encoded<…, int64>>` | `array<bigint>` |
| `NEST_LV` (step-0) | `struct<a: list_view<int64>>` | `struct<a:array<bigint>>` |

**Produced, refused on both sides:** `MAP_LV` (`map<string, list_view<int64>>`): Spark and RePark both refuse a map
key with `DATATYPE_MISMATCH.INVALID_ORDERING_TYPE` (5 BOTH-REFUSED).

**Struck in writing (no route produces the column):**

| Type | Route tried | What happened |
|---|---|---|
| `Union` | SQL `arrow_cast(…, 'Union(Sparse, 0: ("a": Int64))')` | `Unsupported CAST from Int64 to Union(…)`: arrow-cast cannot cast to a union |
| `Union` | pandas `ArrowExtensionArray` over a sparse union | `createDataFrame` reaches `int64`: the union never reaches a column; that cell is EQUAL |
| `Dictionary(Int32, ListView(Int64))` | SQL `arrow_cast` | `Unsupported output type for dictionary packing: ListView(Int64)` |
| `Dictionary(Int32, ListView(Int64))` | pandas | pyarrow `dictionary_encode` has no kernel for `list_view` |
| `Dictionary(Int32, RunEndEncoded)` | SQL `arrow_cast` | `Unsupported output type for dictionary packing: RunEndEncoded(…)` |
| containers of a union or of a non-hashable dictionary | — | struck with their element types |

**Build routes** (one inner key-name cell each; `raw/main/routes.jsonl`):

| Route | Reached | Result |
|---|---|---|
| pandas `pd.ArrowDtype` `list_view`, `large_list_view`, `run_end_encoded` | the same Arrow type; the facade reports `string` | CROSS-PRODUCT (3) |
| pandas sparse union | `int64` | EQUAL |
| pandas dictionary of `list_view` | — | struck (pyarrow) |
| Polars `pl.from_arrow` over `list_view` / `large_list_view` | — | struck: Polars rejects the type (`The datatype "+vl" is still not supported`); Polars has no union or run-end type either |
| a raw `pa.Table` to `createDataFrame` | — | struck: `createDataFrame expects a list of rows, a pandas DataFrame, or a polars DataFrame` |

**The 24 mixed cells** (the family's `K-KIND` probe, `ListView(Struct)` against `List`, `LargeList` and
`FixedSizeList(2)` of the same struct, field order same and reversed, inner and left): all 24 refuse on main where
Spark answers, as on 1.5.6. The cause is DataFusion's comparison coercion, whose `list_coercion` has no view arm
(READ, `datafusion-expr-common-54.1.0/src/type_coercion/binary.rs:1719-1753`): `Cannot infer common argument type for
comparison operation List(Struct(…)) = ListView(Struct(…))`. The `ON` form refuses the same way (12 twins). Under
`exp-cast` all 24 key-name cells answer and equal Spark, because both keys reach coercion as lists. The `ON` twins stay
refused under every mode: that path is not the key-name builder.

## 9. The proposed fix

**The two candidate shapes (the order's):**
- **A.** Non-hashable keys go into the join filter, not `on` (as `join_using` intends).
- **B.** The keys are cast to their hashable kind for the join.

**I pick B.** Both make the key hold on every measured failing row (§3). B is better on every other axis measured:

| | A (filter) | B (cast) |
|---|---|---|
| join plan | `NestedLoopJoinExec`, O(n·m); RePark's NLJ build-side rule carries a `RefuseFallback` policy (READ, `crates/repark-core/src/nlj_build_reset.rs:22`) | `HashJoinExec` on the cast key |
| join types | Inner only (a filter on an outer join changes its meaning, and outer joins work today) | every type, one rule |
| the 24 mixed cells | still refused (the filter's `=` hits the same coercion gap) | EQUAL, at no extra code (question 8: pinned as fixed) |
| hashable keys | unchanged | unchanged (the new branch only runs when a key fails `can_hash`; plans identical, §3) |

**Shape, file and line:**
1. A helper in repark-core next to the builder (a new `df_guards/` file, say `join_keys.rs`) that maps a key column to
   `CAST(key AS hashable(T))` when `can_hash(T)` is false, with `hashable` as in §3, and returns the column unchanged
   otherwise. A `Union` has no hashable kind and no route produces one (§8); the helper refuses it with a plan error
   naming the type rather than letting the rule drop it (question 2).
2. `join_on_named_keys`, `case_bind.rs:652-659`: when every key passes `can_hash`, the existing
   `LogicalPlanBuilder::join` call stays byte for byte. Otherwise the keys go through the helper and
   `LogicalPlanBuilder::join_with_expr_keys`, for every join type. The cast lives only in `Join.on`: the projection at
   `:671-688` keeps the original left key, so the output column keeps its type.
3. `lateral_join`, `crates/repark-python/src/subquery.rs:167-177`: the same helper over the name pairs (question 3).
4. Pins as the order says: a facade pin over the DataFrame cells of this table, a Rust pin at `join_on_named_keys` (the
   plan keeps every key, and a hashable-key plan is unchanged), and the write cells.

**Predicted effect on every row** (from `exp-cast`, which is this shape on `join_on_named_keys`; the lateral rows are
predicted from `exp-norule`, which removed the same drop for that seat):

| Rows | Now | After |
|---|---|---|
| 31 cross products (failing 14, variant 14, routes 3) | CROSS-PRODUCT | EQUAL |
| 14 wrong answers (`df_two`, aggregate) | WRONG-ANSWER | EQUAL |
| 14 wrong writes | WRONG-WRITE | EQUAL |
| 24 mixed key-name cells | REFUSED | EQUAL |
| 8 lateral cross products | cross product | the key holds; still ACCEPTED-WHERE-SPARK-REFUSED (`LATERAL-USING-ACCEPT-1`) |
| 7 SQL `USING`, 5 `using`-section, 7 native `USING` | REFUSED | unchanged (§6, carded) |
| 12 mixed `ON` twins | REFUSED | unchanged (`STRUCT-SETOP-LISTVIEW-COERCION-1`) |
| 9 `crossJoin().where` | REFUSED | unchanged (carded) |
| 5 map keys | BOTH-REFUSED | unchanged |
| 19 struck | — | — |
| 87 EQUAL, every plain `List`, `LargeList`, `FixedSizeList` cell among them | EQUAL | unchanged; plans identical |

**Risk to the struct units' pins (#1283, #1290, #1292, #1295):** low. Their pins
(`test_struct_setop_reorder_refusal_1.py`, `test_struct_setop_type_family_1.py`,
`test_struct_compare_by_position_1.py`, `test_struct_compare_seat_family_1.py`) join on struct, `List(struct)` and map
keys, which pass `can_hash`, so the new branch never runs for them and the existing call is unchanged. The only family
pins on a `ListView` key are the 54 `K-KIND` `view` cells in `struct_compare_seat_family_cells.jsonl`. The 28 SQL
cells do not reach the builder. Of the 26 DataFrame cells, 2 (view against view, left) answer today; the matching
`LV_STRUCT` and `LLV_STRUCT` left cells stay EQUAL under `exp-cast`, and item 3 re-runs the two family cells. The 24 mixed cells carry `known: STRUCT-SETOP-LISTVIEW-COERCION-1`, so the test accepts a refusal and
asserts Spark's rows once they answer, and `exp-cast` measures Spark's rows on all 24. Under B the struct pairing rule
(`struct_operands_by_position`) sees `List(struct)` keys on both sides, the shape the family already pins for the `List`
kinds. Item 3 re-runs all four suites unchanged.

## 10. Questions for a ruling

1. **Shape B (cast), on every join type, with hashable keys untouched.** Lean: yes.
2. **A `Union` key.** No route produces one (§8). Refuse with a plan error naming the type, or move an Inner pair into
   the join filter? Lean: refuse; it fails closed and adds no nested-loop path.
3. **The lateral seat in this unit.** Lean: yes, the same helper at `subquery.rs:167`, so a lateral key-name join
   never drops a key. The over-acceptance itself stays carded as `LATERAL-USING-ACCEPT-1`: refusing key names there
   would move the plain `List` lateral cells, which binding 2 forbids.
4. **SQL `USING` and `NATURAL` on a non-hashable key** (§6, a DataFusion cause). Lean: card it (for example
   `LISTVIEW-SQL-USING-1`), file the upstream report for both `join_using` defects, and pin the refusal as it stands.
5. **The facade's `string` schema for `ListView`, `LargeListView` and `RunEndEncoded`** (`types.py:1307`, not the
   cause). Lean: card it (for example `FACADE-LISTVIEW-SCHEMA-1`).
6. **The `crossJoin().where(l.k == r.k)` refusal on every kind** (`No field named l.k`). Lean: card it; out of clause.
7. **The new failing type variants** (`RunEndEncoded`, lists of a `ListView`, a struct or list holding a run-end
   value): 14 cross products and 7 wrong writes beyond the order's rows. Lean: in the unit's pins; the same fix covers
   them.
8. **The cost bar.** Lean: the ≤ 1.05x gate runs on a plain `List` key-name join, the unchanged path; the cast path's
   time against the `List`-key join of the same data is reported beside it.

## 11. What contradicts the order or the step-0 notes

- **The native `USING` control is not a passing control.** The order adds one `arrow_cast` `ON` and `USING` cell per
  kind as controls. `ON` passes for all 13 kinds; `USING` refuses for the 7 non-hashable kinds in stock DataFusion
  (§6). The SQL `USING` refusal is DataFusion's `join_using`, not RePark's door.
- **Two of the order's type variants are struck:** `Union` and a `Dictionary` over a non-hashable value cannot be
  produced by any facade, pandas or Polars route (§8). `RunEndEncoded` and the containers of a `ListView` are produced
  and fail.
- **The Polars route cannot produce a `ListView`;** only pandas can (§8).
- **Step-0 notes, judgement 1:** the pandas `ArrowDtype` route does not mistype the data. The column reaches the
  engine as `list_view<int64>` and reaches the defect; only the facade's schema report says `string`.
- **The mixed cells** are refused by DataFusion's comparison coercion (no view arm), not by the key-name builder; they
  move only under shape B.
- **`lateralJoin` with key names** has the same defect at a second seat, and Spark refuses those calls entirely.

## 12. Deviations

- The Spark legs ran under the JVM lock, the build lock, `nice` and `taskset`, but without `ulimit -v`: a 64 GiB
  virtual-memory cap is not set for a JVM leg.
- The `semi` cells of the first main run selected `rid` and were re-measured (`left_semi`, `lid` only). The first main
  run's `writeTo` cells hit a stale scratch warehouse and were re-measured on a fresh one. The raw files hold the
  re-measured cells only.

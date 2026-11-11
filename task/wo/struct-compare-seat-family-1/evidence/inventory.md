# Inventories for STRUCT-COMPARE-SEAT-FAMILY-1

Written in build item 1, before any arm is built, as the order requires. The seat is unchanged from #1292: the
analyzer rule `struct_operands_by_position`, inserted just before `type_coercion` by
`insert_struct_position_rule_before_coercion` (session and expression context), with the planner
`DuplicateFieldOperands` and the DataFrame binders' `duplicate_field_operands_by_position`. The new arms live inside
`align_node`. The DataFusion source is the pinned 54.1.0 from the cargo registry.

Cells are cited by id: `Q…` and `M…` from `step0/table.md` (the clerk's table), `X…` and `Y…` from its completeness
additions.

## 1. Insertion inventory, re-answered for the new arms

These are #1292's fifteen rows (`../../struct-compare-by-position-1/evidence/inventory.md`), answered for the four
new arms: key-list joins, `BETWEEN` / `NOT BETWEEN`, simple `CASE` and `nullif`.

| # | Where evidence is created, moved or destroyed | Seen at the seat by the new arms | Because |
|---|---|---|---|
| 1 | Session analyzer list | **yes** | The arms are new match arms in the same rule. `LogicalPlan::Join` reaches `align_node` like any node (`transform_up_with_subqueries`). |
| 2 | Expression context (`build_expr_context`, unbound `F.expr` / `selectExpr`) | **yes** for `BETWEEN`, `CASE` and `nullif`; **n/a** for joins | The same function installs the rule there. An expression context holds no join. |
| 3 | Bound `filter(str)` / `where(str)` | **yes** | Planned against the frame's session, so row 1 analyzes it. These are the `str_filter` cells of `F-*`, and `X0061`–`X0111` for `NOT BETWEEN`. |
| 4 | `SqlToRel` names unnamed `struct(…)` arguments `c{i}` | **yes, and irrelevant** | Positional pairing ignores names. Out of clause (`STRUCT-NAMING-1`). |
| 5 | `type_coercion` casts by name: `coerce_join` (`type_coercion.rs:192-218`), the `Between` arm (`:625-647`), `coerce_case_expression` (`:1026`, via `get_coerce_type_for_case_when`, `type_coercion/other.rs:53`), `coerce_arguments_for_signature` for `nullif` (`:700`) | **no** | They run after the seat. The seat renames first, so `comparison_coercion` finds equal names in equal order and casts nothing by name. |
| 6 | `create_view` stores the body after `apply_type_coercion` | **yes** | Spark-door temporary views store the unanalyzed plan, as #1292 records. A body that arrives coerced carries `Cast(right AS left)` on the right key (`coerce_join`) or operand. The cast-transparent read handles it: one `Alias`, then one `Cast` / `TryCast`. A coerced key-list join is still marked `USING`, and `get_as_join_column` (`expr.rs:2035-2044`) sees through that one cast. After the arm renames the key it is no longer a column, so the arm sets `join_constraint` to `On` (order design 1.3). |
| 7 | `PREPARE … AS` / `EXECUTE` | **yes, reached, for queries** | `EXECUTE` plans through the session analyzer. The prepared cells pair as the direct ones do: `Q2066`–`Q2088` and the item 1 cells `X0140`–`X0148` (the `K-KIND` `EXECUTE` `X0144`/`X0145` is WRONG-ANSWER by name, like its direct twin). **Writes are not reached.** The Spark door refuses `PREPARE` of `DELETE` and `INSERT … SELECT` ("a prepared write would run later, at EXECUTE"), and `PREPARE … AS MERGE` does not parse (`Y0001`–`Y0006`). That claim rests on three spellings, not one. |
| 8 | Recursive CTE term cast by the SQL planner | **no** | Not a comparison seat. Unit C owns it. |
| 9 | `DuplicateFieldOperands` at SQL planning | **BinaryExpr only** | `ExprPlanner::plan_binary_op` sees only binary operators. `BETWEEN`, simple `CASE`, `nullif` and join keys with duplicate field names reach the seat unplanned. The new arms pair them positionally inside the seat, as the `IN` arms do (F10 of #1292), so duplicate names never reach `struct_coercion`, whose debug assertion is `binary.rs:1316`. |
| 10 | Metadata-only `DELETE` door | **separate seat** | It is measured by the `D-PRED` `delete` cells (`M0001`, …), as in #1292. |
| 11 | `conflict_filter.rs` widens the predicate to an Iceberg `Predicate` | **no** | A struct term becomes `AlwaysTrue`, which is wider and safe. |
| 12 | `nested_assign.rs` renders assignment SQL | **no** | Not a comparison. |
| 13 | `PushDownFilter` into `TableScan.filters` | **yes, already rewritten** | The renamed operand is a call to the immutable `__repark_struct_by_position__` UDF, which no pushdown translates. |
| 14 | Native door (`repark.sql()`, `AnsiDialect`) | **no** | A separate session with no Spark extension, by design. It keeps its recorded divergence (order design 7). |
| 15 | DataFrame binders `bound_column` / `bound_projection` | **yes, for `nullif`; no need for the others** | `F.expr` text types an unknown column as a `Utf8` placeholder, so the analyzer never sees the pair before the frame types the expression. Typing differs by form (`expr_schema.rs`). `Between` and `Case` type as `Boolean` and as the first non-null `THEN` without coercing their operands (`:146-156`, `:190`). A `ScalarFunction` types through `fields_with_udf` (`:576-599`, `:722`), which for `nullif`'s `Signature::comparable` (`nullif.rs:82`) calls `comparison_coercion` (`type_coercion/functions.rs:816-820`). So a duplicate-named `nullif` pair reaches `struct_coercion` at the binder. `duplicate_field_operands_by_position` must pair `nullif`'s arguments there, as it pairs a binary comparison. The binder seat stays the same seat with one more expression form. |

## 2. Consumers of the pairing (Q4)

Every consumer of comparison coercion in DataFusion 54.1.0, read from source. Each entry is a seat of the step-0
table or is named out of clause with its card.

### 2.1 Callers of comparison coercion in `type_coercion.rs`

| Caller | Source | Disposition |
|---|---|---|
| `BinaryExpr` (comparison operators) | `:616-622` → `coerce_binary_op` `:292` → `BinaryTypeCoercer`, which calls `comparison_coercion` (`binary.rs:924`) for comparison operators | #1292's seat. Controls: `C-ON`, and every `ON` cell. |
| `InList` | `:660-690`, `get_coerce_type_for_list` (`other.rs:41-45`, `fold_coerce` with `comparison_coercion`) | #1292's seat. |
| `InSubquery` | `:483-510` (`comparison_coercion` `:495`) | #1292's seat. |
| `SetComparison` (`= ANY (subquery)`) | `:511-531` | **Out of clause.** Spark has no such syntax; the door accepts it (memo §4). Card: the door accepts non-Spark syntax, the same class as `PREPARE` (memo §4, F-5 of #1292). |
| `Between` | `:625-647`: three `comparison_coercion` calls, `expr`/`low`, `expr`/`high`, then `low`/`high` | **This unit**: `F-BETWEEN`, `F-NOT_BETWEEN`, `X0061`–`X0111`, `P-PREPARE`. |
| `Case` | `:694` → `coerce_case_expression` `:1026` → `get_coerce_type_for_case_when` (`other.rs:53-57`, `fold_coerce` with `comparison_coercion`) for the `WHEN` operands; `get_coerce_type_for_case_expression` (type union, not comparison) for `THEN`/`ELSE` | **This unit** for the `WHEN` operands: `F-CASE1`, `F-CASE2`. The `THEN`/`ELSE` union stays D4's (order design 3). |
| `coerce_join` | `:192-218`, `coerce_binary_op(lhs, left_schema, Eq, rhs, right_schema)` per `Join.on` pair | **This unit**: `J-USING`, `J-NATURAL`, `J-DF`, `K-KIND`, `D-INSERT`, `D-MERGE`. The set-operation joins are unit C's (2.3). |
| `coerce_union` | `:223-250`, `coerce_union_schema_with_schema` (type union by position) | Not a comparison consumer. `UNION` by position is `STRUCT-SETOP-BY-POSITION-1`; unit C refuses today. |

### 2.2 Functions whose signature reaches comparison coercion

| Function | Path | Disposition |
|---|---|---|
| `nullif` | `Signature::comparable(2, …)` (`nullif.rs:82`) → `TypeSignature::Comparable` (`type_coercion/functions.rs:816-820`) | **This unit**: `F-NULLIF_LR`, `F-NULLIF_RL`. Row 15 adds its binder pairing. |
| Any other `Signature::comparable` / `TypeSignature::Comparable` | grep of every `datafusion-*-54.1.0` crate, aggregate and window included | **None.** `nullif` is the only one. |
| `greatest`, `least` | not `Comparable`; their own coercion (type union) | Out of clause, D4 card (Q7). |
| RePark's `map(…)` constructor | `repark-functions/src/collection/create_map.rs:63` calls `comparison_coercion` on key and value types | Out of clause, D4 card (Q7; memo §4). |
| RePark's `map_concat` | `collection/map_concat.rs:80` | Out of clause, D4 card (Q7). |
| RePark's `array_insert` | `collection/array_insert.rs:59-62` | Both engines refuse (memo §4). Nothing to do. |
| `array_position`, `arrays_overlap`, `array_union`, `array_intersect`, `array_except` | `datafusion-functions-nested`, array-element coercion (not `comparison_coercion`) | Out of clause, D4 card (memo §4). |
| Physical `InList` (`datafusion-physical-expr` `expressions/in_list.rs`) | physical planning, after the analyzer | Not a logical consumer. The operands it sees are already renamed by the `InList` arm. |

### 2.3 Plan nodes that store operand pairs outside an expression

| Node | Producer | Disposition |
|---|---|---|
| `Join.on`, `NullEqualsNothing` | SQL `USING` / `NATURAL` (`join_using`, `builder.rs:1171-1240`, `JoinConstraint::Using`); DataFrame `join(other, "s")` / `["s"]` (`case_bind.rs:618-689`, `LogicalPlanBuilder::join`, `JoinConstraint::On`); `lateralJoin` with key names (`subquery.rs:167-187`) | **This unit** for `USING`, `NATURAL` and DataFrame key names. Lateral key names are struck (`LATERAL-USING-ACCEPT-1`, completeness check). |
| `Join.on`, `NullEqualsNull` | `INTERSECT` / `EXCEPT` (`intersect_or_except`, `builder.rs:1416-1421`) | Unit C (`struct_setop.rs:125-141`). The new arm must skip them (order design 1.1). Controls: `S-SETOP`, `X0146`, `X0147`. |
| `Join.on` built by the optimizer (`DecorrelatePredicateSubquery`, `ExtractEquijoinPredicate`) | after the analyzer, from expressions the seat already rewrote | Not a separate consumer. |
| `Union` inputs | SQL `UNION`, DataFrame `union` | `STRUCT-SETOP-BY-POSITION-1` (out of clause, order design 7). |
| `RecursiveQuery` static and recursive terms | recursive CTE | Unit C (#1292 row 8). |

**Finding for the hand-back.** Every consumer is already a table seat or named out of clause above. None is new to
the memo's list.

**Two results the memo did not predict.** Both come from the completeness additions and are reported, not built:
- `NATURAL` semi and anti (`X0001`–`X0060`): Spark refuses ("Unsupported natural join type"), and head answers
  whatever the key type.
- A `MAP` key column (`X0112`–`X0139`, and the clerk's `map` shape): Spark refuses with `INVALID_ORDERING_TYPE`,
  whatever the value type, and head answers.

`step0/notes.md` has both sections.

### 2.4 Builder-side facts the arms must respect (source)

- **`join_using` in 54.1 cannot store a swapped pair in practice.**
  - Its second branch calls `field_from_column(self.plan.schema(), r)?` with the right-normalized column against the
    left schema (`builder.rs:1203-1212`). That fails, as `UNRESOLVED_COLUMN.WITH_SUGGESTION`, whenever the left key
    type is not hashable: the measured `K-KIND` `view`-left SQL cells (`Q2034`, `Q2042`, `Q2050`, `Q2058`).
  - The arm still types each key against its own side and leaves a pair alone when neither assignment resolves
    (order design 1.2).
- **`can_hash`** (`utils.rs:863-910`) is false for `ListView`, `LargeListView`, `Union` and `RunEndEncoded`. A struct
  is hashable when its fields are, and a map when its entries are. So struct and map keys reach `Join.on`, and a
  `ListView` left key is refused by the builder before the analyzer.
- **`map_expressions` on a `Join`** hands each `on` key to the closure alone (`tree_node.rs:565-586`). The arm
  therefore works on the pair list directly, not through `align_expr`.
- **`using_columns`** (`plan.rs:489-522`) is the only reader that fails on a non-column key. Its readers are
  `expand_wildcard` / `exclude_using_columns` (`utils.rs:413-451`), the builder's column normalization
  (`builder.rs:1010`), the SQL planner's `select.rs:820-831`, and the unparser. Every one runs while the plan is
  being built, before the analyzer.

## 3. Type-variant inventory for join keys (2026-11-09 rule)

### 3.1 Representations that reach `Join.on`, from the `K-KIND` rows

128 cells: left and right kinds from `plain` `List`, `LargeList`, `FixedSizeList(2)` and `ListView` of
`STRUCT<a BIGINT, b BIGINT>`; reordered and same-order; inner and left joins; SQL `USING` and DataFrame `join(…, "s")`.
These are head outcomes. The Spark leg is plain arrays.

| Left kind | Right kind | Reordered (`rev`) | Same order | Where it stops |
|---|---|---|---|---|
| `List`, `LargeList` | `List`, `LargeList` | WRONG-ANSWER (by name), e.g. `Q1938`, `Q1946`, `Q1970`, `Q1978` | EQUAL | reaches `coerce_join`, which pairs by name |
| `FixedSizeList(2)` | `FixedSizeList(2)` | WRONG-ANSWER (`Q2018`) | EQUAL | the same |
| `List` / `LargeList` / `FSL(2)` mixed with `FSL(2)` | — | EQUAL (the element counts differ, so nothing matches on either engine) | `List` × `FSL(2)` same order refuses with an Arrow schema error (`Q1958`); the others are EQUAL | `Q1958` fails on a list field-name mismatch (`element` against `item`), the error family of `FACADE-INGEST-ARROW-NORMALIZE-1` F-5; there is no reorder, so it is out of clause |
| any non-view | `ListView` | REFUSED: "Cannot infer common argument type" (`Q1962`, `Q1994`, `Q2026`) | the same | `comparison_coercion` has no view against non-view arm (`STRUCT-SETOP-LISTVIEW-COERCION-1`) |
| `ListView` | any, SQL | REFUSED: `UNRESOLVED_COLUMN` (`Q2034`, `Q2058`) | the same | `join_using`'s second branch (2.4) |
| `ListView` | non-view, DataFrame | REFUSED: "Cannot infer common argument type" (`Q2035`) | the same | as above |
| `ListView` | `ListView`, DataFrame | WRONG-ANSWER (`Q2059`) | EQUAL (`Q2065`) | reaches `coerce_join` |

The memo's other key shapes are carried in the table: `STRUCT<p STRUCT<…>>` (`nest`), `ARRAY<STRUCT>` (`arr`), maps
(`map`, `mapkey_*`), duplicate names (`dup`), case-differing names (`case`), widened (`wide_*`), different arity
(`arity`) and unrelated names (`unrel`).

### 3.2 What `coerce_join` unifies

It is `comparison_coercion` per pair, the same table as #1292's inventory (`binary.rs`):
- `struct_coercion` (`:1265`): by name when every left name is on the right, else by position. Debug assertion
  against duplicate names at `:1316`.
- `list_coercion`, `dictionary_coercion`, `ree_coercion`, `map_coercion` and `null_coercion`.

The arm renames the right key to the left key's names through every kind above, keeping the right key's kind and
encoding (`renamed_type`), so `coerce_join` finds equal names in equal order.

### 3.3 The refusing fallthrough for keys

- **The same `pairing` walk** as the `=` arm (`Seat::Comparison`).
  - A struct-bearing pair that is not struct with struct, map with map or list with list is a clash.
  - So is a reordered and widened pair, or a different arity.
  - A clash refuses with `BINARY_OP_DIFF_TYPES` on `(s = s)`.
- **A key whose type is a map** refuses before pairing, if the ruling asked in the hand-back adopts it, with Spark's
  `INVALID_ORDERING_TYPE` on `(s = s)`. This is the `ON` controls' class (`X0125`, `X0139`).
- **Identical types** are never renamed and never refused.

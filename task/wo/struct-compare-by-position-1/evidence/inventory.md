# Inventories for STRUCT-COMPARE-BY-POSITION-1

Written before item 2 of the build, as the order requires. Evidence is the field-name list on each struct operand. The
seat is the analyzer rule `struct_operands_by_position`, inserted immediately before `type_coercion` by one function,
`insert_struct_position_rule_before_coercion`, from two call sites.

## Insertion inventory (brief §6)

| # | Where evidence is created, moved or destroyed | Seen at the seat | Because |
|---|---|---|---|
| 1 | Session analyzer list: `context_with_df_54_1_rule_guards` → `SparkExtension::configure_analyzer_rules` | **yes** | It is the seat. The rule sits after `spark_map_subscript` and before `type_coercion`; `analyzer_configuration_seats_hof_preparation_and_float_stringify_before_type_coercion` pins the index. |
| 2 | Expression context list: `build_expr_context`, used by `PyColumn::plan_sql_text` for unbound `F.expr` / `selectExpr`, analyzed eagerly in `plan_expr_column` | **yes** | It is the second seat, installed by the same function. `expr_context_seats_struct_operands_by_position_before_coercion` pins the index and the planner registration. |
| 3 | Bound `filter(str)` / `where(str)` through `parse_canonical_predicate` | **yes** | The predicate is planned against the frame's own session, so the session seat (row 1) analyzes it. The step-0 `filter_str` and `where_str` seats measure it. |
| 4 | `SqlToRel` names unnamed `struct(...)` arguments `c{i}`; the `struct` UDF's return type | **yes, and irrelevant** | The names arrive at the seat, but positional pairing ignores names. Out of clause (`STRUCT-NAMING-1`). |
| 5 | `type_coercion` inserts `Cast(right AS left_type)` in `coerce_binary_op`, the `InList` arm and `coerce_join` | **no** | It runs after the seat. The seat renames the right operand first, so `comparison_coercion` finds equal names in equal order and casts nothing by name. |
| 6 | `create_view` stores the body after `apply_type_coercion` | **yes** | On the Spark door a temporary view stores the unanalyzed plan (`view_ddl/temp_view.rs`, `ViewTable::new(frame.logical_plan().clone(), None)`), so the seat sees the original operands when the view is read. A body that does arrive coerced reaches the cast-transparent read: one `Alias`, then one `Cast` / `TryCast`, renamed beneath, the cast dropped when the renamed type equals its target. The `view` and `df_temp_view` seats measure it. |
| 7 | PREPARE runs the bare optimizer; `SimplifyExpressions` folds literal casts | **no** | The Spark door does not reach PREPARE: the step-0 probe `probe_prepare` is `PARSE_SYNTAX_ERROR` on Spark and a parse refusal on RePark. No belt arm is needed. |
| 8 | Recursive CTE term cast by the SQL planner | **no** | It is not a comparison seat. Unit C owns it; item 4's F-4 pin drives its `RecursiveQuery` arm. |
| 9 | `DuplicateFieldOperands` ExprPlanner at SQL planning, before any analyzer | **yes** | It is registered on the session (`SparkExtension::register`) and on the expression context (`build_expr_context`). It wraps duplicate-named comparison operands in a planned marker; the seat's `positional_comparison` consumes the marker. `IN` lists and `IN (subquery)` with duplicate names take the positional path inside the seat (F10). |
| 10 | Metadata-only DELETE door (`router/delete_update.rs` `try_metadata_delete_door`) | **separate seat** | The step-0 probe `probe_delete_metadata` and every `delete` cell measure it on the after-table. |
| 11 | `repark-iceberg/src/write/conflict_filter.rs` widens the text predicate to an Iceberg `Predicate` | **no** | A struct term becomes `AlwaysTrue`, which is wider and safe; it never selects rows. |
| 12 | `repark-spark/src/merge/nested_assign.rs` renders assignment SQL | **no** | It is not a comparison. |
| 13 | Optimizer `PushDownFilter` into `TableScan.filters` | **yes, already rewritten** | It runs after the analyzer. The renamed operand travels as a call to the immutable `__repark_struct_by_position__` UDF, which no Iceberg pushdown translates, so the predicate stays a residual filter. The `where` and DML seats measure it. |
| 14 | Native door `PreExecute` (`repark.sql()`, `AnsiDialect`) | **no** | It is a separate session with no Spark extension, by design. The native-door pin records that it still pairs by name. |
| 15 | DataFrame binders `bound_column` / `bound_projection` (`repark-python/src/dataframe_names.rs`): the last step before DataFusion types an expression for `select`, `withColumn`, `filter`, `sort`, `aggregate` and a join condition | **yes, third seat (r4)** | `F.expr` text planned by `parse_unresolved_expr` types an unknown column as a `Utf8` placeholder, so the planner (row 9) sees no struct pair, and the analyzer never runs before the frame types the comparison. A duplicate-named struct pair then reached `struct_coercion`, whose debug assertion panics in the ci profile, pre-existing on main. `duplicate_field_operands_by_position` pairs it by position at the binder, or refuses with `BINARY_OP_DIFF_TYPES`. `after/debug-profile.md` holds the cells. |

## Type-variant inventory (the 2026-11-09 rule)

### (1) Representations that reach the seat, per source

From `variant-seat-c/` (P10) and `variant-ext/` (P31 to P38):

| Source | Representations measured |
|---|---|
| pandas Arrow dtypes through the facade | `List`, `LargeList`, `FixedSizeList(1)`, `FixedSizeList(2)`, `ListView`, `LargeListView`; `RunEndEncoded` of a list; `Dictionary` refused at facade ingest (`FACADE-INGEST-ARROW-NORMALIZE-1`) |
| polars | `LargeList`; `FixedSizeList` for `polars_array` |
| Iceberg scan | `List` whatever the writer used |
| SQL literals and `array(...)` | `Struct`, `List` |
| native `arrow_cast` | every list kind; `Dictionary` of a list refused by `arrow_cast` itself |
| depth | top level, inside a struct, inside a map value (standard and renamed entry fields) |

### (2) The pairs `comparison_coercion` unifies

`datafusion-expr-common-54.1.0/src/type_coercion/binary.rs`, as the Unit C park memo §1.1 lists them:

- `equals_datatype` first: identical types, which is the only way two `ListView` or two `LargeListView` operands unify.
- `dictionary_coercion`: dictionary against dictionary on the values; dictionary against a plain type on the value.
- `ree_coercion`: the same for `RunEndEncoded`.
- `list_coercion` (:1691): `FixedSizeList` against `FixedSizeList` (same size keeps it, a different size gives `List`);
  `LargeList` against any of `List`, `LargeList`, `FixedSizeList`; `List` against `List` or `FixedSizeList`.
  No arm for a view against a non-view list, which DataFusion refuses (out of clause, `STRUCT-SETOP-LISTVIEW-COERCION-1`).
- `struct_coercion` (:1265): equal field count; by name when every left name exists on the right
  (`coerce_struct_by_name`, left order), otherwise by position.
- `map_coercion`: `struct_coercion` over the entries.
- `null_coercion`: `NULL` against anything.

The seat makes every such pair positional before coercion: it renames the right operand's struct fields to the left
operand's names through every kind above, keeping the right operand's own kind and encoding, so `comparison_coercion`
then finds equal names in equal order.

### (3) The refusing fallthrough

- `pairing` peels `Dictionary` and `RunEndEncoded` on both sides (`struct_walk::peel_encoding`), pairs struct with
  struct, map with map, and any list kind with any list kind (`struct_walk::list_element`). Any other pair where either
  side holds a struct (`struct_walk::holds_struct`), such as a struct against a list or a `Union` holding a struct, is a
  clash and refuses with Spark's class for the seat.
- A struct-bearing pair whose rename returns nothing refuses; it never passes through to by-name coercion.
- Identical types never reach either path (F-1): equal types need no rename and are never refused.

## Seats beyond the order's list (built in item 2, named for review)

The order lists these comparison seats: `=` and the other comparison operators, an `IN` list, `IN (subquery)` and
join keys. The rule also has two arms that sit outside that list:

| arm | what it does | cells |
|---|---|---|
| `element_at(map, key)` with a struct key reordered against the map's key type | refuses with Spark's `DATATYPE_MISMATCH.MAP_FUNCTION_DIFF_TYPES`. 1.5.6 answered here, where Spark refuses, and `KNOWN` may not hold an acceptance (orchestrator ruling, r3 ruling 1). | step-0 04381 (ACCEPTED-WHERE-SPARK-REFUSED on 1.5.6, BOTH-REFUSED on head) |
| a map-value read (`get_field` over a map) as a comparison operand | the rename goes beneath the field access, onto the map column, so a projected join key stays a plain expression | ext P33, the map-value depth rows (P33-00270, P33-00276 join keys) |

Neither arm reads beneath more than one cast.

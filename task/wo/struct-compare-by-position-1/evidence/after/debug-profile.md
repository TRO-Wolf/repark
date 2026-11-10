# The ci-profile red on PR #1292 (r4)

CI builds the facade wheel with `--profile ci`. That profile inherits `dev`, so DataFusion's debug assertions are on.
`datafusion-expr-common-54.1.0/src/type_coercion/binary.rs:1326` asserts that a struct handed to `struct_coercion` has
no duplicate field names, and panics when it does.

**The path.** `F.expr` text is planned by `parse_unresolved_expr`, which types an unknown column as a `Utf8` placeholder.
1. `DuplicateFieldOperands` sees no struct pair, and the analyzer does not run before the frame's `select` types the
   comparison.
2. On a pair such as `struct<a:int,b:int>` = `struct<a:bigint,a:bigint>`, Arrow's `equals_datatype` ignores field names
   but not field types, so the call reaches `struct_coercion` and the assertion fires.

**The capture fix.** The facade pin's per-cell capture caught `Exception`, but `PanicException` is a `BaseException`.
So one panic took down the module-scoped fixture, and CI reported 2050 setup errors. The capture now records `PANIC`
for that one cell.

**The panicking cells.** All seven are on the string door, seat `expr_select`, shape `dup_one`. Spark's answers are
from `../../step0/raw/spark.jsonl`. "Before" is head before r4; "after" is head with r4.

| Cell | Op | Spark | Main ci | Main release | Before, ci | Before, release | After, ci | After, release |
|---|---|---|---|---|---|---|---|---|
| 01546 | `=` | true | PANIC | false | PANIC | true | true | true |
| 01565 | `<=>` | true | PANIC | false | PANIC | true | true | true |
| 01584 | `!=` | false | PANIC | true | PANIC | false | false | false |
| 01603 | `<` | false | PANIC | false | PANIC | false | false | false |
| 01622 | `<=` | true | PANIC | false | PANIC | true | true | true |
| 01641 | `>` | false | PANIC | true | PANIC | false | false | false |
| 01660 | `>=` | true | PANIC | true | PANIC | true | true | true |

**Main.** Main `b64a5326` in the ci profile also panics on 21 more cells: `filter_str`, `where_str` and `selectExpr`
for the same operators. The earlier items of this unit had already fixed those.

**The seat.** `duplicate_field_operands_by_position` (`crates/repark-spark/src/normalize/struct_position.rs`) runs in
the repark-python binders `bound_column` and `bound_projection`, against the frame schema.
- On a comparison whose operand types resolve to a struct pair with duplicate names on either side, it wraps both
  operands in the planned marker the SQL planner already uses.
- On a type clash, it refuses with `BINARY_OP_DIFF_TYPES`.
- It reads beneath no cast.

**Pins.**
- Four Rust unit tests.
- The facade pin's capture fix.
- Fifteen no-panic DataFrame shapes beyond the table: a typed door with a `bigint` literal, `withColumn`, `sort`,
  `agg` and a join condition. All fifteen panicked on head in the ci profile before r4.

**Revert check.** With the binder seat removed and a ci build, the module fails 22 tests: the seven cells and the
fifteen shapes. With the seat restored, the module passes in ci and in release.

**Release outcomes are unchanged.** The three tables' RePark legs on the r4 release build equal r6 cell for cell:
4393 of 4393 on step 0, 218 of 218 on seat C and 246 of 246 on the extension.

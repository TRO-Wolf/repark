# ASOF-JOIN-1 — insertion inventory

The owner's rule of 2026-11-09: every site where an as-of join would enter RePark, with file:line, read on main
`a4d8857f` (the product of `9a48932f`). The seats are (a) and (b) of [sketch.md](sketch.md) §5. "Touch" says which
seat edits the site; "read" means the seat depends on it but leaves it unchanged.

**The finding in one line:** RePark has no as-of surface anywhere. `df._joinAsOf` falls through `__getattr__` to
`ATTRIBUTE_NOT_SUPPORTED`; no Rust function, logical node, rule or operator names an as-of join; DataFusion 54.1.0 has
none; the SQL door rejects both `ASOF` spellings. There is no `pyspark.pandas` door either.

## 1. The facade (`python/repark/src/repark/spark/`)

| Site | file:line | What is there today | Seat |
|---|---|---|---|
| Attribute fallthrough | `dataframe/core.py:1571` (`__getattr__`) | `df._joinAsOf` is not a member, so it lands here and raises `PySparkAttributeError [ATTRIBUTE_NOT_SUPPORTED] Attribute _joinAsOf is not supported.` (measured: every direct cell on main) | read: the new member shadows it |
| The `join` method | `dataframe/core.py:2151` (`join`), `:2255` (`_join_on_condition_h1`) | The join type table (`inner`…`left_anti`), the SQL-text condition join through `_native.prepare_join_condition` / `requalify_join_sides`, the display/engine name split. A Column `on` for the as-of join reuses this condition preparer. | read (a, b) |
| The binding block | `dataframe/core.py:817`–`:828` | Module members bound onto `DataFrame` by assignment: `streaming_batch.DECLARED_MEMBERS` (`:817`), `surface_b.*` (`:821`–`:823`), `subquery.scalar/exists/lateralJoin/asTable` (`:825`–`:828`) | touch (a, b): one binding line |
| `core.py`'s size row | `scripts/check_lib_py.py:62`–`:63` | `core.py` is pinned at an **exact baseline of 3461 lines**; a growth needs the owner, a shrink must ratchet the row. The binding must be line-neutral: fold `:825`–`:828` into one tuple-unpack line (`scalar, exists, lateralJoin, asTable, _joinAsOf = …`) and ratchet the row down, or place the new name on an existing tuple line. | touch (a, b) |
| The precedent module | `dataframe/subquery.py:1`–`:10`, `:90` (`lateralJoin`) | A PySpark 4.0 method family kept out of `core.py` because of that baseline, a thin adapter over `repark_core::session::df_guards::subquery`. A new `dataframe/asof_join.py` follows it. | new file (a, b) |
| `pandas_api` | `dataframe/streaming_batch.py:116` | Refuses with `[NOT_IMPLEMENTED] pandas_api is not implemented.`; bound at `core.py:817`. **There is no `pyspark.pandas` door on the facade:** no `repark.spark.pandas` module, and `repark.spark.sql` (the alias package, `sql/__init__.py:1`–`:8`) re-exports no pandas name. `merge_asof` has nowhere to enter without a pandas-on-Spark layer. | none (Q3) |
| `min_by` / `max_by` | `functions.py` (no definition; the facade's `F` has neither) | Absent from `F`; `F.expr("min_by(…)")` and SQL `min_by`/`max_by` raise `UNRESOLVED_ROUTINE` (domain table §6). Spark's `RewriteAsOfJoin` is built on `min_by`. | (a) needs it, or a window |
| `monotonically_increasing_id` | `functions_expr.py` (the R-FN-BATCH4 refusals, e.g. `:1515`) | Refused: `functions.monotonically_increasing_id is not supported yet (single-node semantics disclosed; R-FN-BATCH4)`. The workaround therefore needs a unique left key the user already has, or a `row_number` over a global order. | none |
| `first` / `last` with `ignorenulls` | `functions.py:653`, `:676`; `window.py:281` (`Window`) | Present; workaround W2 (union plus window fill) runs on them today. | read (a) |
| `unionByName` | `dataframe/core.py:2596` | Present; W2 uses it. | read (a) |
| `select` with a list | `dataframe/core.py:1167` (`select(*cols)`) | `df.select([cols])` raises `PySparkTypeError expected a column name (str) or Column, got list` (PySpark accepts a list; domain table §6). Not as-of work; carded separately. | none |

### The surface tests a new member touches

| Test | file:line | Why |
|---|---|---|
| The `dir(DataFrame)` snapshot | `python/repark/tests/dataframe/_dfcore_1_expected.py:383` (`EXPECTED_DATAFRAME_DIR`), asserted by `test_dfcore_1_exports.py:328` | Every new attribute name, private ones included, joins the snapshot. |
| The DataFrame surface oracle | `python/repark/tests/dataframe/facade_dataframe_surface_oracle.json` (158 cells, PySpark 4.1.2, recorded 2026-09-14), read by `test_df_surface_a_1.py:29`, `test_df_surface_b_1.py:29`, `test_df_plan_introspect_1.py:13` | The precedent for recording PySpark's answer per call shape; `_joinAsOf` cells would join it or a sibling oracle file. |
| The example-drift gate | `scripts/check_example_coverage.py:228` | Names starting with `_` are skipped. A **public** `joinAsOf` would need an example script (Q2). |
| The API freeze | `docs/design/v1-0-api-freeze.json`, `python/repark-parity/tests/test_api_freeze.py:43`, `:79` (891 frozen names) | Additive names are allowed at a minor; a public name regenerates the register (`scripts/build_api_freeze.py --write`). |

## 2. The Rust plan builder

| Site | file:line | What is there today | Seat |
|---|---|---|---|
| The key join binding | `crates/repark-python/src/dataframe/mod.rs:603` (`join_on_names`) | `#[pymethods]` entry for a names join: `join_type_from_str`, then `dataframe_names::join_on_keys`. The file is 816 lines (default ceiling 1000). | touch (a, b): one `#[pymethods]` entry, or a free function in a new file |
| The condition join binding | `crates/repark-python/src/dataframe/mod.rs:624` (`join_on_condition`) | Binds a `PyColumn` condition and calls DataFusion's `DataFrame::join_on`. | read |
| Key resolution | `crates/repark-python/src/dataframe_names.rs:144` (`join_on_keys`), `:282` (`requalify_join_sides`) | Name-to-field resolution per side and the requalified output (897 lines). The as-of seat reuses `requalify_join_sides` so both sides' columns survive with Spark's names. | read |
| Exact-join sides | `crates/repark-core/src/session/df_guards/join_exact.rs:23` (`join_exact_sides`) | The SQL-free condition join used by `_join_exact_or_sql`. | read |
| Session wiring | `crates/repark-core/src/session/df_guards.rs:187`–`:195` | `SessionStateBuilder` with `StackQueryPlanner`, RePark's optimizer and analyzer rules, and the physical rules `NljBuildSideReset`, `ParallelWindowRule`, `ParallelProjectionRule`. | touch (a): a rule here; read (b) |
| Rule precedent | `crates/repark-core/src/session/df_guards/subquery.rs:220`, `:336`, `:407` | `OptimizerRule`s that rewrite subqueries (`ReparkScalarSubqueryGuard`, `ReparkProjectionExists`, `ReparkLateralProjectionHoist`), registered at `df_guards.rs:237`–`:243`. | precedent (a) |
| Extension node precedent | `crates/repark-core/src/stack.rs:115` (`UnpivotNode`, a `UserDefinedLogicalNodeCore`), `crates/repark-core/src/extension.rs:278` (`ReaderParse`) | RePark's own logical nodes. | precedent (b) |
| Extension planner | `crates/repark-core/src/stack/planner.rs:17` (`StackQueryPlanner`), `:36`–`:55` (`StackExtensionPlanner::plan_extension`, one downcast to `UnpivotNode`) | The single place a custom node becomes a custom operator. | touch (b): one more downcast branch |
| Custom operator precedent | `crates/repark-core/src/stack/exec.rs:29` (`UnpivotExec`), `:89` (`impl ExecutionPlan`) | RePark's own `ExecutionPlan`, 607 lines. | precedent (b) |
| Parallel windows | `crates/repark-core/src/parallel_window/rule.rs:16` (`ParallelWindowRule`) | Splits window work by partition key; seat (a)'s window runs through it. | read (a) |
| Correlated non-equality scalar subquery | the SQL door (no single site; the failing rule is DataFusion's `scalar_subquery_to_join`, guarded at `df_guards.rs:237`) | `SELECT l.lid, (SELECT max(r.t) FROM r WHERE r.k1 = l.k1 AND r.t <= l.t) FROM l` fails on main with `Optimizer rule 'scalar_subquery_to_join' failed caused by Schema error: Schema contains qualified field name …` (Spark answers). This is the shape Spark's rewrite produces, so RePark cannot copy Spark's plan as is. Carded separately (`card.md`). | none |

## 3. DataFusion 54.1.0 and sqlparser 0.62.0 (read only, `~/.cargo`)

| Site | file:line | Finding |
|---|---|---|
| Join operators | `datafusion-physical-plan-54.1.0/src/joins/` | `hash_join`, `sort_merge_join`, `nested_loop_join.rs`, `cross_join.rs`, `symmetric_hash_join.rs`, `piecewise_merge_join/`: **no as-of or nearest-match operator.** |
| Piecewise merge join | `datafusion-physical-plan-54.1.0/src/joins/piecewise_merge_join/exec.rs:67`–`:99`; flag `datafusion-common-54.1.0/src/config.rs:1236` (`enable_piecewise_merge_join`, default `false`); chosen at `datafusion-54.1.0/src/physical_planner.rs:1611`–`:1629` | "Currently experimental" (`mod.rs:18`). A range join for **one** inequality and **no** equi-keys that emits **every** match; not a nearest pick, and it refuses a residual equi-condition (`physical_planner.rs:1610`, the TODO). It could only feed a rewrite, and only without keys. |
| SQL parse | `sqlparser-0.62.0/src/parser/mod.rs:15769`; `src/ast/query.rs:2839` (`JoinOperator::AsOf { match_condition, constraint }`) | sqlparser parses Snowflake's `ASOF JOIN … MATCH_CONDITION (…)` in every dialect, and so treats `ASOF` as a keyword, not an alias. |
| SQL plan | `datafusion-sql-54.1.0/src/relation/join.rs:101` | `other => not_impl_err!("Unsupported JOIN operator {other:?}")`: `JoinOperator::AsOf` is not planned. |
| Windows | `first_value` / `last_value` with `IGNORE NULLS`, `row_number`, bounded frames | Present; they carry seat (a). |

## 4. Spark's side, for the seat to mirror

| Site | Jar | Finding |
|---|---|---|
| `classic.Dataset.joinAsOf` (two overloads) | `spark-sql_2.13-4.1.2.jar` | The `usingColumns` overload builds `this(c) === other(c)` per name and `&&`s them, so **both** key columns stay in the output; the Column overload resolves through `resolveSelfJoinCondition` (self joins work), then `createAsOfJoinPlan$1` builds `AsOfJoin.apply` with `AsOfJoinDirection(direction)`. |
| `AsOfJoin$.apply`, `makeAsOfCond`, `makeOrderingExpr` | `spark-catalyst_2.13-4.1.2.jar` | The condition and the distance; [sketch.md](sketch.md) §1. |
| `AsOfJoin` | same | `require(joinType in (Inner, LeftOuter), "Unsupported as-of join type …")`; `resolved` needs the tolerance assertion `tolerance >= Literal.default(type)` foldable and true. |
| `RewriteAsOfJoin` | same | A correlated `ScalarSubquery` of `min_by(struct(right.*), distance)` over `Filter(cond, right)`, then `Filter(IsNotNull)` for inner, then `GetStructField` per right column. |
| `SqlBaseLexer`, `SqlBaseParser$JoinTypeContext` | `spark-sql-api_2.13-4.1.2.jar` | No `ASOF` token; join types `INNER CROSS LEFT OUTER SEMI RIGHT FULL ANTI`. |

Excerpts: [evidence/jar/javap-asof.txt](evidence/jar/javap-asof.txt).

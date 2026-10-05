# ATTR-VIEW-SEMANTICS-1 — design sketch: SQL aliases mint (b), a foreign column refuses as Spark does (c, with Q5 folded in)

**Status:** DONE, 2026-10-05. Claude Opus 5.5 (`claude-opus-5-5`, high), the sketch round of [order.md](order.md). The round halted once at H5 and resumed on the orchestrator's rulings the same day.

**Scope after the rulings:**
- Item (a), fresh ids on every read of a SQL temp view, is delivered on the ATTR-ID-1 stack as a regression fix. It is a separate Muse lane, `fix/attr-view-a-stack`.
- This design's slices are (b), then (c) with Q5 folded in. Both branch from the stack after (a) merges into it, and they land on main as one follow-up PR after the stack-to-main merge.

No product code changed. Every measurement below was taken on throwaway builds of the stack, and the throwaways were reverted. The last one is kept as a diff: [design-evidence/throwaway-a-b1-c.diff](design-evidence/throwaway-a-b1-c.diff).

## 0. Rulings this design builds on

| id | ruling (2026-10-05) |
|---|---|
| R-7 (in the order) | The #951 pin `test_sql_view_v9_unchanged` is a holding pin. Slice (c) replaces it in the same commit with `test_sql_view_foreign_column_missing_attributes`, which asserts the oracle text. |
| Q2 = B1 | The SQL alias mint uses a written-alias marker. It is a plan fact, never an id, and never appears in the stamped schema. It must tell user-written aliases apart from the case-display aliases. Measure B1 first; any row that moves away from Spark is H1. |
| Q3 = general (c) | The release note names every refused shape, DataFrame-door shapes included, each backed by a live probe row. The per-operation table is pinned cell by cell. |
| Q4 | `filter`/`sort` pull-up values are wrong on main `9f41347f` and on the stack alike (P07, P08, P13, P14, P16, P20, P21; [design-evidence/edge_main.json](design-evidence/edge_main.json)). It gets its own card, ranked above (c), in the same release right after (c). It is a dependency note here only (§5). |
| Q5 | `drop` of an absent foreign column is a no-op. Folded into (c): a table row, a pin, a release-note line. |
| Q02 | Option A: (a) lands on the stack. See §2.2(a). |

## 1. How this was measured

- **Oracle:** live Spark 4.1.2, session zone America/New_York, run through `systemd-run --user --scope --slice=repark.slice env JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 /tmp/sparkenv/bin/python`. Runs were on 2026-10-05: 16:14–17:16 UTC (mint, ops, edge, case probes) and 19:55–21:00 UTC (pin, join, reserved-name probes). Ids come from `queryExecution().analyzed().output()`.
- **Stack:** `wip/feat/attr-id-1-2026-10-01` at `b9db3536`, debug `make develop` in `/tmp/xavsstack`. Ids come from `repark._native.attribute_ids`.
- **Builds measured:**

  | build | contents |
  |---|---|
  | base | the stack as is |
  | a-base | the stack with (a) only: the `copy_attribute_ids` line dropped in `view_ddl/execute.rs`, the baseline the orchestrator asked for |
  | b1 | (a) + B1 |
  | head | (a) + B1 + general (c), the full throwaway |

- **Raw outputs** are in [design-evidence/](design-evidence/map.md), one JSON per probe and build. The probe scripts stay in `/tmp/oc-worker/direct/wo/attr-view-semantics-1/design/` (outside the repo, so ruff does not rewrite them). Every row id cited below is a key in those JSONs.

## 2.1 The mint points, measured

Source `d = createDataFrame([(1, 10), (2, 20)], ["id", "v"])`, registered as `tv`. An id is shown as `d.id`/`d.v` when it equals the source's id, and `new` otherwise. `b1` is (a)+B1.

| # | shape | Spark 4.1.2 | base | b1 | b1 vs Spark |
|---|---|---|---|---|---|
| M04 | `SELECT * FROM tv` | `d.id, d.v` | same | same | EQUAL |
| M05 | `SELECT v FROM tv` | `d.v` | `d.v` | `d.v` | EQUAL |
| M06 | `SELECT v AS v FROM tv` | `new` | `d.v` | `new` | EQUAL (toward) |
| M07 | `SELECT v AS w FROM tv` | `new` | `d.v` | `new` | EQUAL (toward) |
| M08 | `SELECT t.v FROM tv t` | `d.v` | `d.v` | `d.v` | EQUAL |
| M09 | `SELECT t.v AS v FROM tv t` | `new` | `d.v` | `new` | EQUAL (toward) |
| M10 / M14 | `v + 0 AS v`, `CAST(v AS BIGINT) AS v` | `new` | `new` | `new` | EQUAL |
| M11 | `SELECT id, v AS v FROM tv` | `d.id, new` | `d.id, d.v` | `d.id, new` | EQUAL (toward) |
| M12 | `SELECT * FROM (SELECT v AS v FROM tv) q` | `new` | `d.v` | `new` | EQUAL (toward) |
| M13 | `SELECT v FROM tv WHERE v > 0` | `d.v` | `d.v` | `d.v` | EQUAL |
| C | `SELECT V FROM tv` / `SELECT tv.V FROM tv` | `d.v` | `d.v` | `d.v` | EQUAL |
| C | `SELECT ID, V FROM tv` | `d.id, d.v` | same | same | EQUAL |
| C | `SELECT v AS V` / `SELECT V AS V` / `` SELECT `v` AS v `` | `new` | `d.v` | `new` | EQUAL (toward) |
| C | `SELECT * FROM (SELECT V FROM tv) q` | `d.v` | `d.v` | `d.v` | EQUAL |
| C | `WITH q AS (SELECT v FROM tv) SELECT * FROM q` | `d.v` | `d.v` | `d.v` | EQUAL |
| C | `WITH q AS (SELECT v AS v FROM tv) SELECT * FROM q` | `new` | `d.v` | `new` | EQUAL (toward) |
| C | `SELECT v FROM tv UNION ALL SELECT v FROM tv` | `d.v` | `d.v` | `d.v` | EQUAL |
| C | `SELECT v AS v FROM tv UNION ALL SELECT v FROM tv` | `new` | `d.v` | `new` | EQUAL (toward) |
| C | `SELECT id AS id, sum(v) AS v FROM tv GROUP BY id` | `new, new` | `d.id, new` | `new, new` | EQUAL (toward) |
| C | `SELECT DISTINCT v`, `GROUP BY id, v`, `ORDER BY id`, `a.v` from a self-join | carried | carried | carried | EQUAL |
| M20–M28 | `CREATE [OR REPLACE] TEMP VIEW … AS SELECT …` (with/without column list, view over view); every `table(sv)` / `SELECT * FROM sv` read | `new` on **every read** (`#9,#10` then `#11,#12` then `#15,#16`) | `d.id, d.v` | `new` per read | EQUAL (toward), item (a) |
| M02 / M03 / M40 / M41 | DataFrame view (`createOrReplaceTempView`), read twice | `d.id, d.v` | same | same | EQUAL |
| M43 | DataFrame view of `sql("SELECT v AS v FROM tv")` | that frame's own id | `d.v` | that frame's id | EQUAL (toward) |
| M30 / M31 | `d.select(d.v.alias("v"))` / `.alias("w")` | `new` | `new` | `new` | EQUAL |
| M32 / M33 | `withColumnRenamed("v","w")` / `("v","v")` | `d.id, new` | same | same | EQUAL |
| M34 / M36 / M37 | `d.select("v")`, `d.select(d.v)`, `d.alias("t").select("t.v")` | `d.v` | same | same | EQUAL |
| M35 | `d.selectExpr("v AS v")` | `new` | `d.v` | `new` | EQUAL (toward) |

C = `case_*.json`; M = `mint_*.json`. **B1 result (H1 check required by Q2): all 45 id rows equal Spark, 22 moved toward, 0 moved away, 23 were already equal.** That counts the 29 mint-table rows and the 16 spelling/scope rows ([mint_b1.json](design-evidence/mint_b1.json), [case_b1.json](design-evidence/case_b1.json)).

Every row the ruling named moved toward Spark: M06, M07, M09, M11, M12, the CTE row and the UNION row. The rows it named as carries stayed carried: `SELECT V`, `SELECT tv.V` and `SELECT ID, V`.

**Door differences in Spark:**
- **View registration.** A SQL-defined temp view mints on every read; a DataFrame view carries. The stack has one path for each (`execute_create_temp_view` and `ReparkSession::create_or_replace_temp_view_from`), so H2 did not fire. Item (a) owns this row.
- **Aliases.** They do not differ: an explicit alias mints in both doors, and a bare or case-variant reference carries in both.

## 2.2 Where each mint happens

### (a) SQL temp-view registration: delivered on the stack (lane `fix/attr-view-a-stack`)

The change is one line: `execute_create_temp_view` (`crates/repark-spark/src/view_ddl/execute.rs:157`, stack) stops calling `frame_names::copy_attribute_ids(plan, &view.definition_plan)` and registers `ctx.read_table(provider)?` as it comes. Every read then builds a facade frame that `stamp` mints fresh, because the provider is born clean (`strip_schema_ids`, `view_ddl/temp_view.rs:169`).

Evidence:
- **Id rows:** M20–M28, all toward Spark.
- **Q02:** `a = table(sv); b = table(sv); a.join(b, a.id == b.id).select(a.v, b.v)` answers `[(10, 10), (20, 20)]` as Spark does, where the base refuses `_LEGACY_ERROR_TEMP_1182`. (Q02 is `edge_head.json`, judged on head.)
- **Q03:** `table(sv).select(table(sv).v)` now refuses `MISSING_ATTRIBUTES` as Spark does, but only once (c) is in.
- **(a) on its own,** measured on a-base:
  - **Facade suite:** identical to base. 15,270 passed and 15 errors, all of them deep-stack fixture timeouts on the debug build: `test_deep_subquery_expression_1.py` (6) and `test_deep_filter_chain_crash_1.py` (9).
  - **Replay:** 0 of 43,893 judged cells moved.
  - **The #951 pins** (V-3a, V-3b, DESCRIBE, cycle walk, the no-internal-error sweep) stayed green. `test_sql_view_v9_unchanged` stayed green too: V9 still answers by name until (c) lands.

### (b) The SQL alias: B1, the written-alias marker

**Why the plan alone cannot do it** (measured on a plan-level throwaway, [mint_planlevel.json](design-evidence/mint_planlevel.json), [case_planlevel.json](design-evidence/case_planlevel.json)):
- DataFusion 54.1 drops `col AS samename` while planning (`datafusion-sql-54.1.0/src/select.rs:834`).
- RePark's display passes add `AS "<written>"` that the user never wrote: `display_rewrite` at `crates/repark-core/src/column_resolution/display.rs:13`, and `respell_inner_scopes` at `inner_scopes.rs:10`.
- So a plan-only mint missed M06/M09/M11/M12 and moved `SELECT V`, `SELECT tv.V` and `SELECT ID, V` away from Spark.

**B1, as built and measured on the throwaway:**

1. **Mark (user door only):** in `PyReparkSession::sql` (`crates/repark-python/src/session.rs:169`), before planning. Never in `sql_built` (`:189`), which carries facade-built SQL such as `table()` and the join and select builders.
   - Parse the text with `SparkSqlDialect`. Act only on a single `Statement::Query`, so no view, CTAS or DML body text is ever changed.
   - In every query scope, wrap each written `SelectItem::ExprWithAlias` whose `expr` is an `Identifier` or `CompoundIdentifier` as `repark_written_alias(<expr>) AS <alias>`. That covers the body, CTE bodies, derived tables and both set-operation legs.
   - The wrap is spliced into the original text at the expression's span, so nothing else in the text changes.
   - The display passes run afterwards on the marked statement. Their aliases wrap bare references, never a function call, so the two never collide. Measured: `SELECT V` / `SELECT ID, V` still carry.
2. **Plan, with a fallback:** plan the marked text. If that planning fails, plan the original text instead.
   - That makes the marker unable to add an error. At worst a written alias carries, which is today's behaviour and the same class.
   - Measured need: `SELECT id AS Id, ID FROM sc.ns.t` under case folding (casesens `p1/tw_Id_ID`). Marked, DataFusion sees an unqualified `id` beside a qualified `t.id` and raises a schema ambiguity before RePark's case repair runs. With the fallback, all 70 `test_casesens_1.py` cells pass.
3. **Consume:** after planning, rewrite every `Projection` expression `Alias(repark_written_alias(x), name)` to `alias_with_fresh_id(x, name)` (`attr_id.rs:118`), and recompute parent schemas.
   - The marker is then gone from the stamped plan; `stamp` sees an alias that carries its own id and keeps it.
   - A marker left anywhere else is a no-op function whose `simplify` returns its argument (for example a `GROUP BY` that names the alias), so the optimized and physical plans are identical to the unmarked ones.
4. **The function:** `repark_written_alias` is a scalar UDF.
   - **Definition:** signature `any(1)`, Immutable; its return type and nullability are the argument's, with no metadata.
   - **Registration:** registered on the session the first time a statement is marked.
   - **Visibility:** it is not listed by `spark.catalog.listFunctions()` (measured: 436 names, none of them the marker).
   - **What must change:** after the first marked statement, user SQL calling it by name answered (`[Row(z=10)]`), where Spark raises `UNRESOLVED_ROUTINE` ([reserved_spark.json](design-evidence/reserved_spark.json)). So the mark step must refuse a statement whose written text already calls `repark_written_alias`, case-insensitively, with the exact Spark text: ``[UNRESOLVED_ROUTINE] Cannot resolve routine `repark_written_alias` on search path [`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]. SQLSTATE: 42883``. The stack already gives that text before the first marked statement.

**How a SQL-planner alias is told apart from a DataFrame-door one:**
- A DataFrame-door `.alias()` or rename already carries its own minted id in `Alias.metadata` (`own_id`, `attr_id.rs:282`; M30–M33 are EQUAL on base).
- The facade's internal rebinding aliases carry none and must carry. They never pass through the user SQL door, so they are never marked.
- `selectExpr` (`python/repark/src/repark/spark/filter_quote.py:591`) and `SQLTransformer` run user-written SQL through the user door, and Spark mints there too (M35).

**R-5:** ids are still minted only by `attr_id.rs` (`alias_with_fresh_id`). The marker is a plan fact that the consume step removes, never an id. Nothing is decided by name, and Python is not involved.

## 2.3 (c) The refusal: general, per operation

**Spark's rule, measured cell by cell** ([ops_spark.json](design-evidence/ops_spark.json), [edge_spark.json](design-evidence/edge_spark.json), [pin_spark.json](design-evidence/pin_spark.json), [join_spark.json](design-evidence/join_spark.json)).

A column is foreign when its attribute id is not in the frame's output.

| operation | Spark refuses when the foreign id is… | Spark answers when… | operator in the message |
|---|---|---|---|
| `select`, `withColumn`, `select` after a join | not in the input | the id is a key of a by-name join below (hidden output; J1, J2) | `!Project` |
| `groupBy(col)`, `agg(f(col))`, `groupBy(...).agg(f(col))` | not in the input | — | `!Aggregate` |
| `filter` / `where` | not reachable by pull-up | reachable through Project, Filter, Limit, Distinct (P02, P07, P13, P16, P20, P21, J6) | `!Filter` |
| `orderBy` / `sort` | not reachable by pull-up | same chain (P04, P08, P14) | `!Sort` |
| join condition | not in either side | — | `!Join` (existing preparer) |
| `drop(col)` | never | absent → no-op (P17, P18, P19; V4, V9 `drop`) | — |

Pull-up is **blocked** by `df.alias()` (P09, P10), any view (P11, P12, P15; V9, V12, V15 `filter`/`orderBy`), Union (P22), and an Aggregate for a non-key column (P05, P06).

**The two conditions:**
- `RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION` when a column of the same display name, under the session's case rule, is in the input.
- `RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT` otherwise.

The message first line is `Resolved attribute(s) "<name>" missing from "<input>, …" in operator !<Op>`, followed by Spark's ` Attribute(s) with the same name appear in the operation: "<name>". Please check if the right attribute(s) are used. SQLSTATE: XX000` for APPEAR. RePark prints no `#N` ids, which is the house rule (`self_join.rs:720`). Pins assert the condition, the parameters, and the message up to `!<Op>`.

**Where the stack falls back to names today:**
- `_bind_stable_id_column` (`python/repark/src/repark/spark/column_fields.py:655`, ending `return column` at `:683`);
- the token rewrite `_replace_local_attr_token` (`python/repark/src/repark/spark/dataframe/join_attr_tokens.py:101`, `:117`);
- for `agg`, `_rebind_simple_name_aggregate`, which re-binds `max(d.v)` by name before any check. Measured: a hook placed after it never fired.

**The design, as measured:**

- **Rust, a new sibling of `attr_id.rs`.** `crates/repark-core/src/session/df_guards/attr_reach.rs`, read-only over ids:
  ```rust
  pub enum Located { Output, Hidden, PullUp, Absent }
  pub fn locate(plan: &LogicalPlan, id: &AttrId) -> Located
  ```
  - `Output`: the root schema holds the id.
  - `Absent`: no node of the plan holds it.
  - Otherwise, walk down from the root through `Projection`, `Filter`, `Sort`, `Limit`, `Distinct::All`, `Repartition`, and `SubqueryAlias` nodes whose alias is facade-internal (`__repark…` / `_repark…`):
    - the first node whose schema holds the id is `PullUp`;
    - it is `Hidden` instead when that node is a `Join` and the id is a column of its `on` pairs;
    - a user `SubqueryAlias`, `Union`, `Aggregate`, `TableScan`, `Values` or `EmptyRelation` reached first is `Absent`;
    - any other node (Window, condition Join, Unnest, Extension, …) is `PullUp`, so no new refusal can fire where Spark might answer (R-4 by construction).
  - **Measured:** the stack plans by-name joins with equi keys in `Join.on` (J1/J2 → `Hidden`, which Spark answers) and condition joins with the predicate in `Join.filter`. Under a condition join the key first surfaces as a renamed projection column, so J5 is `PullUp`, which Spark refuses.
- **Rust refusal.** `crates/repark-python/src/dataframe_names.rs`, a new `#[pyfunction] refuse_unreachable_attribute(frame, attr_id, door, name)`:
  - it runs `locate` and applies the door table;
  - project and aggregate doors refuse on `PullUp`/`Absent`; filter and sort doors refuse on `Absent`; `Output`/`Hidden` never refuse;
  - it raises through `frame_lineage::missing_attributes_error`, which gains an `operator: &str` argument. The existing callers pass `"!Join"`.
  - **R-5:** the identity question and the door policy are both answered in Rust. Python only names the door.
- **Python routing:** `_refuse_unemitted_ids(frame, column, door="project")` (`python/repark/src/repark/spark/dataframe/unemitted_ids.py`) is the existing semi/anti hook that select, withColumn and filter already pass through.
  - **What it checks:** a new helper `_refuse_absent_ids` hands every referenced id to the Rust function: the column's own `_attr_id` plus each `__REPARK_ATTR_` token in the precomputed `_join_sql_expr`.
  - **Do not call `column.join_sql_part()` there.** On a 2,000-deep frame it walks the frame-node chain recursively and raised `RecursionError` (measured: `test_grown_stack_gate_1.py::test_deep_linear_regression_fits`).
  - **Door call sites:**

    | door | where it is called | edit |
    |---|---|---|
    | filter | `core.py` `filter` | passes `door="filter"`, net 0 lines |
    | groupBy keys | `core.py` `group_by` → `_column_of` | passes `door="aggregate"`, net 0 lines |
    | sort | `column_fields.py` `_bind_sort_key` | calls the hook with `door="sort"` |
    | agg | `joins_columns.py` `GroupedData.agg` | calls it on the **written** columns (`_agg_funnel_columns(exprs)`), before `_rebind_simple_name_aggregate` |

- **`drop`:** no change needed. `_drop_targets` (`python/repark/src/repark/spark/subset_resolve.py:156`) already skips a Column whose id is not held. With (a)+(b), V4, V9 and P19 `drop(d.v)` move to Spark's no-op (measured).

## 2.4 Blast radius of (b) + general (c), with (a) present (head vs a-base)

| surface | toward Spark | away | same class | not moved |
|---|---|---|---|---|
| facade suite (3 shards, `-n 24`, `--timeout=240`) | 12 tests (below) | 0 | 0 | 15,259 passed; the same 15 deep-stack errors as a-base |
| replay (`replay_work.py`, judged against `s4/spark.json` without `nondet.json`) | 16 cells | 0 | 0 | 43,877 of 43,893 |
| self-join pins (`test_attr_id_1_sj*.py`) | 1 (`sj4_v1_swapped_alias_parent_select`, strict XPASS) | 0 | 0 | all other SJ pins green |
| #951 pins | 1 (`test_sql_view_v9_unchanged`, R-7) | 0 | 0 | V-3a, V-3b, DESCRIBE, cycle, sweep green |
| probes: mint/case ids (45 rows) | 22 | 0 | 0 | 23 already EQUAL |
| probes: ops (140 cells) | 78 | 0 | 2 (V3/DFX `where(d.v == 10)`: Q4 card) | 60 EQUAL |
| probes: edge (36 cells) | 13 | 0 | 11 (Q4 pull-up values P02/P07/P08/P13/P14/P16/P20/P21/P23/P24, and Q05) | 12 EQUAL |
| probes: views (order matrix, 18 rows) | V3, V4, V9, V12, V15 (and V-3a/V-3b vs `d5c97862`) | 0 | V6b, V10 (out of scope) | the rest |
| probes: pin shapes (12) / joins (14) | 11 (the facade pins held the name answer on a-base) / J5 | 0 | 0 / J2 left/right/full values, J6 (pre-existing) | ISIN one frame / J1, J2 inner, J3, J4, J7, J8 |

**The 12 facade tests that move,** every one re-measured on the live oracle ([pin_spark.json](design-evidence/pin_spark.json)). Spark refuses each with `MISSING_ATTRIBUTES` and head matches the condition and names. So each pin held the old name-fallback answer, and slice (c) rewrites it:

| test (stack) | Spark (probe row) |
|---|---|
| `test_attr_view_semantics_1.py::test_sql_view_v9_unchanged` | APPEAR naming `v` (R-M20); R-7 replaces it |
| `test_attr_id_1_s4.py::test_s4_fillna_dup_name_binds_parent_source[insensitive, sensitive]` | APPEAR naming `b` (S4-fillna cs=false/true) |
| `test_attr_id_1_s4.py::test_s4_replace_dup_name_binds_parent_source[insensitive, sensitive]` | APPEAR naming `b` (S4-replace) |
| `test_attr_id_1_s4.py::test_s4_expression_output_parent_ref_uses_engine_name[insensitive, sensitive]` | APPEAR naming `b` (S4-expr-output) |
| `test_attr_id_1_s4.py::test_s4_renamed_output_parent_ref_refuses` | MISSING_FROM_INPUT naming `v` (S4-renamed-V2); the pin asserted the DataFusion `No field named` text |
| `test_attr_id_1_s4.py::test_s4_case_only_rename_parent_ref_binds` | APPEAR naming `v` (S4-case-only-rename) |
| `test_attr_id_1_s4.py::test_s4_replaced_output_parent_ref_reads_new_value` | APPEAR naming `v` (S4-replaced-output); card MISSING-REF-RESOLVE-1 D-3 already planned this flip |
| `test_attr_id_1_sj4.py::test_sj4_v1_swapped_alias_parent_select` | APPEAR naming `v` (SJ4-V1); strict xfail under card MISSING-REF-RESOLVE-1, now passes |
| `test_column_parity_1.py::test_isin_list` | APPEAR naming `i` (ISIN two frames). The pin builds its frame twice (`_base_df(spark).select(_base_df(spark).i…)`), which Spark refuses. The single-frame form it means answers `[True, False, None]` on both (ISIN one frame), so the fix is the test's construction, not the oracle row. |

**The 16 replay cells:** `r3.{F,T}_cp_{al1,al1id,al_al,wcr}_sel_parent` (8) and `r4p4.{F,T}_al {q,u,x,z} sel parent` (8). All moved to Spark's `MISSING_ATTRIBUTES` condition. The list is in [replay-moves-head-vs-abase.json](design-evidence/replay-moves-head-vs-abase.json).

**H1–H5 on the measured head:**
- **H1:** no row moved away.
- **H2:** not triggered (§2.1).
- **H3:** no name-based fallback was added. The existing name binding survives only on the filter/sort `PullUp` arm, which the Q4 card replaces.
- **H4:** no replay cell moved away.
- **H5:** the only #951 pin that changes is the one R-7 names.

**Dependency note (Q4 and MISSING-REF-RESOLVE-1).** The stack card `task/roadmap/mid-term/missing-ref-resolve-1-card-2026-10-03.md` already proposes:
- D-1: `select` refuses, which is this (c);
- D-2: filter and orderBy resolve through the child's input, which is the Q4 card;
- D-3: the S4 and SJ4 flips listed above.

Slice (c) delivers D-1 and the D-3 select flips. D-2 stays with the Q4 card. Ordering: the Q4 card is "above (c), right after it", so (c) must land first and leave the `PullUp` arm answering as today. `test_sj4_v1_replaced_parent_filter` stays a strict xfail, re-pointed at the Q4 card.

**Open items (not designed here):**
- **Q05**, `a.join(b, "id").select(a.v)` over two SQL-view reads, still raises `AMBIGUOUS_REFERENCE` where Spark answers. It is the V6b class, checked against the SJ slices.
- **J2 left/right/full:** `df1.join(df2, "id", how).select(df2.id)` answers `df1`'s key values on base and head alike, where Spark returns `df2`'s (`[2, 3, None]` for left). That is a pre-existing value gap to card.
- **Q02** is closed by (a).

## 2.5 Slices, in landing order

Both slices branch from `wip/feat/attr-id-1-2026-10-01` after (a) has merged into it. Each round is one commit, one DIFF-PROBE, and one scoped Opus verifier (R-0). Ceilings are the defaults (1000) unless noted; line counts are at `b9db3536`.

### Slice B — SQL aliases mint (item b)

**Files:**

| file | now | change |
|---|---|---|
| `crates/repark-python/src/written_alias.rs` | new | the `repark_written_alias` UDF and `register(ctx)`; `mark_written_aliases(sql: &str) -> Result<Option<String>>` (span splice, `Statement::Query` only, the reserved-name refusal); ≈170 lines measured |
| `crates/repark-python/src/session.rs` | 802 | `PyReparkSession::sql`: mark → plan → fall back to the original text on a planning error → `consume_written_aliases`; ≈ +25 |
| `crates/repark-python/src/lib.rs` | 184 | `mod written_alias;` (+1) |
| `crates/repark-core/src/session/df_guards/attr_reach.rs` | new | `pub const WRITTEN_ALIAS`, `pub fn consume_written_aliases(plan: LogicalPlan) -> Result<LogicalPlan>`, which mints only through `attr_id::alias_with_fresh_id` |
| `crates/repark-core/src/session/df_guards.rs` | 237 | `pub(crate) mod attr_reach;` (+1) |
| `crates/repark-core/src/session/df_guards/case_bind.rs` | 724 | re-export `WRITTEN_ALIAS`, `consume_written_aliases` through `frame_names` (+1–2) |
| `crates/repark-core/src/session/tests/attr_reach.rs` | new | Rust pins below; `tests/mod.rs` (34) +1 |
| `python/repark/tests/test_attr_view_semantics_1_alias.py` | new | Python pins below |
| `python/repark/tests/attr_view_semantics_1_spark_oracle.json` | new | the Spark rows the pins read, copied from `design-evidence/*_spark.json` with their dates |
| map.md rows | — | `df_guards/map.md`, `session/tests/map.md`, `repark-python/src/map.md`, `python/repark/tests/map.md` (lockstep) |

`attr_id.rs` (973/1000) is **not edited**.

**Pins**, each with the mutation that turns it red and its Spark answer:

| pin | asserts (Spark 4.1.2, 2026-10-05) | red under |
|---|---|---|
| `test_written_alias_mints[M06,M07,M09,M11,M12,cte,union,v_as_V,V_as_V,backtick,group_alias,selectExpr]` | the aliased position's id is new against `d`; the others carry (§2.1) | B-M1: `mark_written_aliases` returns `None` |
| `test_reference_carries[M05,M08,V,tv_V,ID_V,sub_V,cte_bare,union_bare,distinct,group_bare,where,star]` | the ids equal `d`'s | B-M2: the mark also wraps bare `Identifier` items |
| `test_written_alias_columns_and_rows_unchanged[...same 12 + 12]` | columns and rows equal the oracle | B-M3: consume names the output after the inner column instead of the written alias |
| `test_view_of_sql_alias_frame_keeps_its_id` | M43: `table(qv)` ids equal `sqlf`'s, not `d`'s | B-M1 |
| `test_case_twin_alias_falls_back` | `SELECT id AS Id, ID FROM sc.ns.t` (casesens `p1/tw_Id_ID`) answers the oracle row | B-M4: the fallback to the original text removed |
| `test_reserved_marker_name_unresolved[before, after]` | `SELECT repark_written_alias(v) AS z FROM tv` raises `UNRESOLVED_ROUTINE` with Spark's exact text, before and after a marked statement | B-M5: the reserved-name refusal removed |
| `test_marker_not_listed` | `spark.catalog.listFunctions()` has no `repark_written_alias` | B-M6: the marker registered as a catalog function |
| Rust `attr_reach::consume_removes_every_projection_marker` | after consume, no `Projection` expression is `Alias(repark_written_alias(..))` and the alias carries a native id | B-M7: consume skipped |
| Rust `attr_reach::optimized_plan_has_no_marker` | the optimized plan of `SELECT v AS w, count(*) FROM tv GROUP BY w` contains no `repark_written_alias` (a marker consume leaves in a group key; to verify first, and HB3 if it survives) | B-M8: `simplify` returns `Original` |

Card proposal names: the card's `test_sql_alias_mints` is **renamed** to the parametrized `test_written_alias_mints`. `test_sql_alias_foreign_column_missing_attributes` moves to slice C, because (b) does not refuse.

**Gates:**
- the build and Rust checks, run inside the build slot (`flock …opus-cargo.lock systemd-run --user --slice=repark.slice …`):
  - `cargo test -p repark-core --lib`
  - `cargo test -p repark-python --lib`
  - `cargo clippy --all-targets -- -D warnings -A clippy::disallowed_methods`
  - `cargo fmt --check`
  - `make rust-panic-ban`
- the structure checks:
  - `bash scripts/check_lib_rs.sh`
  - `python3 scripts/check_rust_file_size.py`
  - `python3 scripts/check_lib_py.py`
  - `bash scripts/check_map_md.sh --base <stack head>`
- ruff 0.15.22
- `make develop`, then locally: `test_attr_view_semantics_1*.py`, `test_casesens_1.py`, `test_casesens_2*.py`, `test_attr_id_1_*.py`, `test_grown_stack_gate_1.py`
- the replay like-for-like against the a-base recorded here: 0 away
- the full facade on CI

**Halt rules:**

| rule | trigger |
|---|---|
| HB1 | any `test_reference_carries` row mints (the H1 class) |
| HB2 | a marked statement plans but answers different rows or columns than the unmarked one |
| HB3 | the marker is visible: in `listFunctions`, in an optimized or physical plan, in a view's stored SQL or `DESCRIBE`/`SHOW CREATE` text, or callable by user SQL |
| HB4 | any replay cell or facade test moves away from a-base |
| HB5 | `attr_id.rs` needs an edit beyond what the re-export requires (R-5) |

### Slice C — a foreign column refuses as Spark does, per operation (item c, Q5 folded in)

**Files:**

| file | now | change |
|---|---|---|
| `crates/repark-core/src/session/df_guards/attr_reach.rs` | from B | `Located`, `locate(plan, id)`; ≈ +60 |
| `crates/repark-core/src/session/df_guards/case_bind.rs` | 724 | re-export `Located`, `locate` |
| `crates/repark-python/src/dataframe_names.rs` | 801 | `#[pyfunction] refuse_unreachable_attribute(frame, attr_id, door, name)` and its registration; ≈ +30 |
| `crates/repark-python/src/frame_lineage.rs` | 354 | `missing_attributes_error(.., operator: &str)`; existing callers pass `"!Join"` |
| `crates/repark-python/src/tests.rs` | 890 | `missing_attribute_refusals_follow_spark_subclasses` gains the `!Project` / `!Filter` / `!Sort` / `!Aggregate` operators |
| `python/repark/src/repark/spark/dataframe/unemitted_ids.py` | 97 | `_refuse_unemitted_ids(.., door="project")` + `_refuse_absent_ids`; ≈ +30 |
| `python/repark/src/repark/spark/dataframe/core.py` | **3464 = its ceiling** | `filter` and `group_by` pass the door; **net 0 lines** |
| `python/repark/src/repark/spark/column_fields.py` | 955 | `_bind_sort_key` calls the hook with `door="sort"` (+1) |
| `python/repark/src/repark/spark/dataframe/joins_columns.py` | 955 | `GroupedData.agg` checks the written columns with `door="aggregate"` before the name rebind (+2) |
| `crates/repark-core/src/session/tests/attr_reach.rs` | from B | `locate` pins |
| `python/repark/tests/test_attr_view_semantics_1_doors.py` | new | the door table, cell by cell |
| rewritten pins | — | the 12 tests in §2.4: `test_attr_view_semantics_1.py`, `test_attr_id_1_s4.py`, `test_attr_id_1_sj4.py` (drop one strict xfail; re-point `test_sj4_v1_replaced_parent_filter`'s xfail reason at the Q4 card), `test_column_parity_1.py` (one frame) |
| map.md rows | — | lockstep, as in B |

**The door table, pinned cell by cell:** `test_foreign_column_door[<cell>]`, reading the oracle JSON.

- **Sources:**

  | source | definition |
  |---|---|
  | V3 | `sql("SELECT id, v + 1 AS v FROM tv")` |
  | V4 | `sql("SELECT id, v AS v FROM tv")` |
  | V9 | `table(sv)` |
  | V12 | `tv` re-registered from another frame |
  | V15 | a view over `groupBy("id").agg(sum("v").alias("v"))` |
  | DFX | `d.select(d.id, (d.v + 1).alias("v"))` |
  | WCR | `d.withColumnRenamed("v", "v")` |
  | NEG | `d.select(d.id, (-d.v).alias("v"))` |
  | SEL | `d.select("id")` |

- **Mutation key:**

  | mutation | change |
  |---|---|
  | C-M1 | the project-door check removed |
  | C-M2 | the aggregate-door check removed, or moved after `_rebind_simple_name_aggregate` |
  | C-M3 | the filter door refuses on `PullUp` |
  | C-M4 | the sort door refuses on `PullUp` |
  | C-M5 | `locate` treats a user `SubqueryAlias`, `Union` or `Aggregate` as pass-through |
  | C-M6 | `Hidden` removed |
  | C-M7 | `_drop_targets` binds an absent id by name |
  | C-M8 | the APPEAR/MISSING_FROM_INPUT choice inverted |
  | C-M9 | the operator hard-coded to `!Join` |

| cell group | cells | Spark answer | red under |
|---|---|---|---|
| select refuses | `{V3,V4,V9,V12,V15,DFX,WCR}×select(d.v)`, `×select(d.v + 1)`; `{V9,V12}×select(d.id)`; R-M30, R-M31, R-M32, R-M35, R-M43b, P01, J5 | APPEAR or MISSING_FROM_INPUT as recorded, `!Project` | C-M1; C-M8 on the MISSING_FROM_INPUT cells |
| select answers | `{V3,V4,V15,DFX,WCR}×select(d.id)`, R-V15b, R-V3d, R-M43, R-M42, ISIN one frame | the rows | C-M1 inverted (refuses all) |
| withColumn refuses | `{V3,V4,V9,V12,V15,DFX,WCR}×withColumn('z', d.v)`, `×withColumn('z', d.v + 1)`, P03 | APPEAR / MISSING_FROM_INPUT, `!Project` | C-M1, C-M9 |
| groupBy/agg refuse | `{V3,V4,V9,V12,V15,DFX,WCR}×{groupBy(d.v).count(), groupBy('id').agg(max(d.v)), agg(max(d.v))}` | APPEAR, `!Aggregate` | C-M2, C-M9 |
| filter answers (pull-up) | V4, WCR, J3 `filter`; V3/DFX `filter(d.v > 15)` (rows equal today) | the rows | C-M3 |
| filter answers, value pending (Q4) | P02, P07, P13, P16, P20, P21, P23, P24, J6, V3/DFX `where(d.v == 10)` | the Spark rows, `xfail(strict=True, reason="card <Q4>")` | (xfail) |
| filter refuses | `{V9,V12,V15}×{filter, where, filter().select('v')}`, P05, P09, P11, P22 | APPEAR, `!Filter` | C-M5 |
| sort answers (pull-up) | `{V3,V4,DFX,WCR}×{orderBy(d.v.desc()), sort(d.v)}`, P04, J4 | the rows | C-M4 |
| sort answers, order pending (Q4) | P08, P14 | the Spark order, strict xfail (Q4 card) | (xfail) |
| sort refuses | `{V9,V12,V15}×{orderBy, sort}`, P06, P10, P12, P15 | APPEAR, `!Sort` | C-M5 |
| join keys stay selectable | J1 inner/left/right/full, J2 inner, J7, J8 | the rows | C-M6 |
| join after a minted frame | `{V3,V4,V9,V12,V15,DFX,WCR}×join(other, q.id == other.id).select(d.v)` refuse; V9 `join(other, d.id == other.id)` refuses `!Join` | as recorded | C-M1 |
| drop ignores an absent column (Q5) | V4, V9, P19 `drop(d.v)` (moved), P17, P18 (already) | the rows with `v` kept | C-M7 |
| the R-7 pin | `test_sql_view_foreign_column_missing_attributes`: V9 | APPEAR naming `v`, message prefix up to `!Project` | C-M1 |
| no recursion on deep frames | `test_grown_stack_gate_1.py::test_deep_linear_regression_fits` stays green | the fit | the hook calling `column.join_sql_part()` |

Card proposal names:

| card name | here |
|---|---|
| `test_expr_alias_foreign_column_missing_attributes` | kept as the V3 select cells |
| `test_replaced_view_foreign_column_missing_attributes` | kept as the V12 select cells |
| `test_agg_view_foreign_column_missing_attributes` | kept as the V15 select cells |
| `test_sql_view_foreign_column_missing_attributes` | the R-7 pin (V9 select) |
| `test_sql_alias_foreign_column_missing_attributes` | the V4 select cells |

All five become ids of the parametrized `test_foreign_column_door`.

**Gates:** as in slice B, plus the whole of `test_attr_id_1_*.py`, `test_g1_stat_and_expander.py`, `test_column_parity_1.py`, `test_fnp_alias_1.py`, `test_g4b_semi_join.py`; then the replay like-for-like against B's head, with 0 away and the 16 cells of §2.4 EQUAL.

**Halt rules:**

| rule | trigger |
|---|---|
| HC1 | any cell Spark answers raises (H1) |
| HC2 | a facade test outside the 12 of §2.4 goes red, or a replay cell moves away (H4) |
| HC3 | the door policy, or an id comparison, is needed in Python (R-5, H3) |
| HC4 | `core.py` would grow (its ceiling) |
| HC5 | a self-join pin other than the strict XPASS changes (R-4) |
| HC6 | a #951 pin other than the R-7 one goes red (H5) |

## 2.6 Release note (the stack's minor, R-2)

Every refused shape below raises `AnalysisException` with Spark 4.1.2's condition. Before, RePark answered by name, or (where marked †) raised a DataFusion `Schema error`. Each line cites its live probe row (2026-10-05).

**SQL aliases and views make new attributes. A column of the source frame no longer resolves against them:**
- `spark.sql("SELECT v + 0 AS v FROM tv").select(d["v"])` → `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION` (V3; R-M10).
- `spark.sql("SELECT v AS v FROM tv").select(d["v"])` → `…APPEAR_IN_OPERATION`, even though the alias keeps the name (V4; R-M06). The same goes for `SELECT t.v AS v`, CTE and UNION-leg aliases. `SELECT v AS w …` → `…RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT` (R-M07).
- `d.selectExpr("v AS v").select(d["v"])` → `…APPEAR_IN_OPERATION` (R-M35).
- `spark.table("sv").select(d["v"])`, with `sv` from `CREATE TEMP VIEW sv AS SELECT * FROM tv` → `…APPEAR_IN_OPERATION`: every read of a SQL-defined temp view makes new attributes (V9; R-M20). With a column list → `…MISSING_FROM_INPUT` (R-M23); a view over a view → `…APPEAR_IN_OPERATION` (R-M26).
- `spark.table("sv").select(spark.table("sv")["v"])` → `…APPEAR_IN_OPERATION`: two reads, two attribute sets (Q03).
- After `tv` is re-registered from another frame, `spark.table("tv").select(d["v"])` → `…APPEAR_IN_OPERATION` (V12; R-V12).
- A view over `d.groupBy("id").agg(sum("v").alias("v"))`, then `.select(d["v"])` → `…APPEAR_IN_OPERATION` (V15; R-V15).
- A DataFrame view of `spark.sql("SELECT v AS v FROM tv")`, then `.select(d["v"])` → `…APPEAR_IN_OPERATION` (R-M43b).

**DataFrame-door shapes that make new attributes refuse the same way:**
- `d.select(d["v"].alias("v")).select(d["v"])` → `…APPEAR_IN_OPERATION` (R-M30). With `.alias("w")` → `…MISSING_FROM_INPUT` † (R-M31).
- `d.withColumnRenamed("v", "w").select(d["v"])` → `…MISSING_FROM_INPUT` † (R-M32). To the same or a case-only name, `("v", "v")` / `("v", "V")` → `…APPEAR_IN_OPERATION` (WCR select; S4-case-only-rename).
- `d.withColumn("v", d["v"] + 1).select(d["v"])` → `…APPEAR_IN_OPERATION` (S4-replaced-output).
- `d.select((d["v"] + 1).alias("v"), d["id"]).select(d["v"])` → `…APPEAR_IN_OPERATION` (DFX select; S4-expr-output).
- `d.select(d["v"].alias("id"), d["id"].alias("v")).select(d["v"])` → `…APPEAR_IN_OPERATION` (SJ4-V1).
- After a join, `joined.fillna(0).select(left["b"])` and `joined.replace(2, 200).select(left["b"])` → `…APPEAR_IN_OPERATION` (S4-fillna, S4-replace).
- `d.select("id").select(d["v"])` and `d.select("id").withColumn("z", d["v"])` → `…MISSING_FROM_INPUT` † (P01, P03).
- A column taken from a second `createDataFrame` of the same rows is a different attribute: `spark.createDataFrame(rows).select(spark.createDataFrame(rows)["i"])` → `…APPEAR_IN_OPERATION` (ISIN two frames).
- `x.join(y, x["id"] == y["id"]).select(x["v1"]).select(y["id"])` → `…MISSING_FROM_INPUT` † (J5).

**The same rule applies across operations on any of the frames above:**
- `withColumn("z", d["v"])`, and any expression over it, refuses with operator `!Project`; `select` after a join of such a frame, too.
- `groupBy(d["v"])`, `agg(max(d["v"]))` and `groupBy("id").agg(max(d["v"]))` refuse with operator `!Aggregate`.
- `filter(d["v"] > 15)` and `orderBy(d["v"])` refuse with `!Filter` / `!Sort` when the column cannot be reached through the frame. That is across a temp view (V9, V12, V15, P11, P12, P15), `df.alias()` (P09, P10), a union (P22), or an aggregate (P05, P06).
- `spark.table("sv").join(other, d["id"] == other["id"])` refuses with operator `!Join` (V9 join).

**Unchanged:**
- `filter` and `orderBy` with such a column still answer when the column sits below a plain projection, as in Spark.
- The key of a by-name join stays selectable from either side: `df1.join(df2, "id", how).select(df2["id"])` (J1, J2). For left, right and full joins the right side's values are a known gap, carded separately.

**Q5:** `spark.table("sv").drop(d["v"])` and `spark.sql("SELECT id, v AS v FROM tv").drop(d["v"])` now keep `v`, ignoring the absent column as Spark does (P19; V4, V9 drop). They used to drop `v` by name.

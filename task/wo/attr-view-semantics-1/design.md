# ATTR-VIEW-SEMANTICS-1 — design sketch for items (a)–(c): HALTED at H5, with the measurement done so far

**Status: HALT (order §4, H5), 2026-10-05.** Written by Claude Opus 5.5 (`claude-opus-5-5`, high) for the orchestrator. Order: `task/wo/attr-view-semantics-1/order.md` (PR #952, not yet on `origin/main` when this was written). Card: [attr-view-semantics-1-card-2026-10-04.md](../../roadmap/mid-term/attr-view-semantics-1-card-2026-10-04.md).

This sketch stops before §2.4 (blast radius on the proposed mint points) and §2.5 (slices) because H5 fires by construction (§6). It records everything measured up to the halt: the mint table (§2.1), where each mint lives (§2.2), what the refusal needs (§2.3), and draft release-note lines (§2.6). It also records three design findings the owner should see with the halt.

No product code changed. One throwaway local patch was built and measured, then reverted: plan-level (a)+(b), saved as `/tmp/oc-worker/direct/wo/attr-view-semantics-1/design/throwaway-ab-planlevel.diff`. It is the variant §2.2 shows is not viable for (b).

## 0. How this was measured

- **Oracle:** live Spark 4.1.2, session zone America/New_York, run on 2026-10-05 between 16:14 and 17:16 UTC through `systemd-run --user --scope --slice=repark.slice env JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 /tmp/sparkenv/bin/python`. Ids come from `frame._jdf.queryExecution().analyzed().output()`.
- **Stack:** `wip/feat/attr-id-1-2026-10-01` at `b9db3536` (includes the #951 fix), a `make develop` debug build in `/tmp/xavsstack`. Ids come from `repark._native.attribute_ids(frame._inner)`.
- **Probes, and one JSON per engine,** all in `/tmp/oc-worker/direct/wo/attr-view-semantics-1/design/`:

  | probe | engine outputs | measures |
  |---|---|---|
  | `mint_probe.py` | `mint_spark.json`, `mint_stack.json`, `mint_patched_ab.json` | §2.1 ids, and the foreign-column outcomes per shape |
  | `ops_probe.py` | `ops_spark.json`, `ops_stack.json` | operator by source |
  | `edge_probe.py` | `edge_spark.json`, `edge_stack.json` | pull-up edges and per-read view ids |
  | `case_probe.py` | `case_spark.json`, `case_stack.json`, `case_patched_ab.json` | spelling and scope shapes |

- **Source frame:** `d = createDataFrame([(1, 10), (2, 20)], ["id", "v"])`, registered as `tv`. An id is shown as `d.id` or `d.v` when it equals the source's id, and as `new` otherwise.
- **Baselines on the unpatched stack, recorded for the resumed round:**
  - Facade suite, three shards at `-n 24 --timeout=240` (`facade_shard.sh`, `facade-base/`): 15,270 passed, 484 skipped, 150 xfailed, 15 errors. All 15 errors are module-fixture timeouts in `test_deep_subquery_expression_1.py` (6) and `test_deep_filter_chain_crash_1.py` (9) on the debug build.
  - Replay (`replay-base/stack.merged.json`): 43,989 cells. Of the 43,893 judged against `/tmp/oc-worker/direct/wo/attr-id-1/s4/spark.json` (with `nondet.json` excluded), 32,772 are EQUAL and 11,121 are not.

## 2.1 The mint points, measured

| # | shape | Spark 4.1.2 | stack `b9db3536` | stack vs Spark |
|---|---|---|---|---|
| M04 | `SELECT * FROM tv` | `d.id, d.v` | `d.id, d.v` | EQUAL |
| M05 | `SELECT v FROM tv` | `d.v` | `d.v` | EQUAL |
| M06 | `SELECT v AS v FROM tv` | `new` | `d.v` | DIFFERS |
| M07 | `SELECT v AS w FROM tv` | `new` | `d.v` | DIFFERS |
| M08 | `SELECT t.v FROM tv t` | `d.v` | `d.v` | EQUAL |
| M09 | `SELECT t.v AS v FROM tv t` | `new` | `d.v` | DIFFERS |
| M11 | `SELECT id, v AS v FROM tv` | `d.id, new` | `d.id, d.v` | DIFFERS |
| M12 | `SELECT * FROM (SELECT v AS v FROM tv) q` | `new` | `d.v` | DIFFERS |
| M10 / M14 | `v + 0 AS v`, `CAST(v AS BIGINT) AS v` | `new` | `new` | EQUAL |
| — | `SELECT V FROM tv`, `SELECT tv.V FROM tv`, `SELECT ID, V FROM tv` | carried (`d.v`; `d.id, d.v`) | carried | EQUAL |
| — | `SELECT v AS V`, `SELECT V AS V` | `new` | `d.v` | DIFFERS |
| — | `WITH q AS (SELECT v AS v FROM tv) SELECT * FROM q` | `new` | `d.v` | DIFFERS |
| — | `SELECT v AS v FROM tv UNION ALL SELECT v FROM tv` | `new` | `d.v` | DIFFERS |
| — | `SELECT id AS id, sum(v) AS v FROM tv GROUP BY id` | `new, new` | `d.id, new` | DIFFERS |
| M20 | `CREATE TEMP VIEW sv AS SELECT * FROM tv`; `table(sv)` | `new, new` (`#9, #10`) | `d.id, d.v` | DIFFERS |
| M21 | `table(sv)` read again | `new, new` (`#11, #12`, fresh again) | `d.id, d.v` | DIFFERS |
| M22 | `SELECT * FROM sv` | `new, new` (`#15, #16`) | `d.id, d.v` | DIFFERS |
| M23 | `CREATE TEMP VIEW sc (a, b) AS SELECT * FROM tv` | `new, new` | `d.id, d.v` | DIFFERS |
| M24 | `CREATE TEMP VIEW sv1 AS SELECT v FROM tv` | `new` | `d.v` | DIFFERS |
| M25 / M27 | `CREATE OR REPLACE TEMP VIEW sv …`, read, read again | `new, new` (`#22, #23`, then `#32, #33`) | `d.id, d.v` | DIFFERS |
| M26 / M28 | view over a view (`svv AS SELECT * FROM sv`); `SELECT * FROM svv` | `new, new` (fresh against `sv` too) | `d.id, d.v` | DIFFERS |
| M02 / M03 | `table(tv)` (a DataFrame view), read twice | `d.id, d.v` both times | `d.id, d.v` both times | EQUAL |
| M40 / M41 | `createOrReplaceTempView(dv)`; `table(dv)`, `SELECT * FROM dv` | `d.id, d.v` | `d.id, d.v` | EQUAL |
| M43 | `createOrReplaceTempView` of `sql("SELECT v AS v FROM tv")` | that frame's own id | `d.v` (the frame never minted) | DIFFERS, and follows from M06 |
| M30 | `d.select(d.v.alias("v"))` | `new` | `new` | EQUAL |
| M31 | `d.select(d.v.alias("w"))` | `new` | `new` | EQUAL |
| M32 / M33 | `d.withColumnRenamed("v", "w")` / `("v", "v")` | `d.id, new` | `d.id, new` | EQUAL |
| M34 / M36 / M37 | `d.select("v")`, `d.select(d.v)`, `d.alias("t").select("t.v")` | `d.v` | `d.v` | EQUAL |
| M35 | `d.selectExpr("v AS v")` | `new` | `d.v` | DIFFERS |

**Where the DataFrame door and the SQL door differ in Spark (called out per §2.1):**

1. **View registration.** A SQL-defined temp view (`CREATE [OR REPLACE] TEMP VIEW … AS SELECT …`, with or without a column list, over a view or not) mints fresh ids **on every read** (M20, M21, M22, M25, M27). A DataFrame view (`createOrReplaceTempView`) carries the registered frame's ids on every read (M02, M40, M43).

   This is a real door difference. H2 does **not** fire, because the stack already has two registration paths:
   - `execute_create_temp_view` in `crates/repark-spark/src/view_ddl/execute.rs`, the SQL door, which registers a `ReplanningTempView` provider;
   - `ReparkSession::create_or_replace_temp_view_from` in `crates/repark-core/src/session/temp_views.rs:38`, the DataFrame door, which registers `frame.into_view()`.

   The per-read mint is load-bearing. Two reads of a SQL view joined, `a = table(sv); b = table(sv); a.join(b, a.id == b.id).select(a.v, b.v)` (Q02) and `a.join(b, "id").select(a.v)` (Q05), **answer** in Spark. The stack refuses them today (`_LEGACY_ERROR_TEMP_1182`, `AMBIGUOUS_REFERENCE`) because both reads share ids. And `table(sv).select(table(sv).v)` (Q03) raises `MISSING_ATTRIBUTES` in Spark, while the stack answers.
2. **Aliases do not differ between doors in Spark.** An explicit alias mints in both doors: `.alias()` (M30, M31), `withColumnRenamed` even to the same name (M32, M33), SQL `AS` (M06, M07, M09) and `selectExpr("v AS v")` (M35). A bare reference carries in both (M05, M08, M34, M36, M37), and so does a case-variant bare reference (`SELECT V`). The stack's DataFrame door already mints on alias and rename. Only the SQL text doors keep the source id: `spark.sql`, `selectExpr`, and `SQLTransformer`, which runs user SQL through the same native door.

## 2.2 Where each mint happens in RePark

**(a) The SQL temp-view registration.**
- **Today:** `execute_create_temp_view` (`crates/repark-spark/src/view_ddl/execute.rs`, the `copy_attribute_ids` call at line 157) carries the definition plan's ids above a scan of the born-clean provider (`strip_schema_ids` at `crates/repark-spark/src/view_ddl/temp_view.rs:169`).
- **Proposed:** replace nothing. Drop the carry, and register `ctx.read_table(provider)?` as it comes. The signature is unchanged: `pub(crate) async fn execute_create_temp_view(ctx: &SessionContext, catalogs: &CatalogRegistry, statement: CreateTempViewStatement, temp_views: Option<&dyn TempViewSession>) -> Result<DataFrame>`.
- **Why that mints per read, as Spark does:**
  - every read builds a facade frame, and `DataFrame.__init__` stamps it (`python/repark/src/repark/spark/dataframe/core.py:235`);
  - `stamp` (`crates/repark-core/src/session/df_guards/attr_id.rs:171`) finds a clean `TableScan` under a non-Projection root and mints a pass-through id for every field.
- **Measured on the throwaway (2026-10-05):** M20–M28 all move to `new` on every read, which is toward Spark, and no id row moved away.
- **`copy_attribute_ids` itself stays.** `python/repark/src/repark/spark/dataframe/cache_handle.py:103` and `:121` still call it.
- **What depends on the resumed round:** the facade suite, the replay and the SJ pins were not run on this patch.

**(b) The SQL alias.** Measured: this mint **cannot be made on the plan.** Two facts:

1. **DataFusion 54.1 drops the alias when it names its own column.** In `datafusion-sql-54.1.0/src/select.rs:834` (cargo registry; DataFusion is not patched in `Cargo.toml`), `SelectItem::ExprWithAlias` keeps `Expr::Column` bare when `column.name == alias`. So `SELECT v AS v`, `` SELECT `v` AS v ``, `SELECT tv.v AS v`, `SELECT (v) AS v` and `SELECT t.v AS v` all plan exactly as `SELECT v` does (measured: all `d.v` under a plan-level mint).
2. **RePark's Spark door adds aliases the user never wrote.** Two passes give an unaliased reference whose written spelling differs from DataFusion's name an `AS "<written>"`:
   - at the root scope, `display_rewrite` (`crates/repark-core/src/column_resolution/display.rs:13`, U11-EDGE-1);
   - in inner scopes, `respell_inner_scopes` (`crates/repark-core/src/column_resolution/inner_scopes.rs:10`, CASESENS-1). So `SELECT V FROM tv` reaches the plan as `v AS V`, the same as a written `SELECT v AS V`.

**The throwaway proves both.** It added `mint_written_aliases(plan)` (mint every `Alias(Column)` without its own id) on the user door, `PyReparkSession::sql` at `crates/repark-python/src/session.rs:169`, and ran on 2026-10-05:
- **Toward Spark:** M07, M35, `SELECT v AS V`, `SELECT V AS V`.
- **Away from Spark:** `SELECT V FROM tv`, `SELECT tv.V FROM tv` and `SELECT ID, V FROM tv`. Spark carries there, and the patch minted.
- **Still missed:** M06, M09, M11, M12, the CTE and the union rows.

So the plan-level variant would trip H1 once (c) refuses, and it is not proposed.

**The seam is right; the information is not on the plan.** `PyReparkSession::sql` is the door for user-written SQL text: `spark.sql`, `selectExpr` (`python/repark/src/repark/spark/filter_quote.py:591`) and `SQLTransformer`. Spark mints in all three. Facade-built SQL goes through `sql_built` (`session.rs:189`): `table()` at `python/repark/src/repark/spark/catalog_surface.py:348`, joins, the H1 select path. Facade-built SQL must keep carrying.

What is missing is which select items the user actually wrote with `AS` over a bare reference. That exists only in the written AST, before DataFusion plans it. There are two ways to carry it. This is a design fork, and it touches R-5 ("no new carrier"), so it is a question (Q2), not a choice made here:
- **B1, a written-alias marker.**
  - **The rewrite:** on the user door only, and for `Statement::Query` only (never view DDL, so no view body text changes), rewrite each written `ExprWithAlias { expr: Identifier | CompoundIdentifier, alias }` into `alias(repark_written_alias(expr))`. It applies in every scope: CTE bodies, derived tables, set-operation legs.
  - **The function:** `repark_written_alias` is a no-op scalar UDF whose `simplify` returns its argument, so the optimized and physical plans are unchanged.
  - **Why it mints:** `stamp_projection` (`attr_id.rs:253`) mints because the aliased expression is no longer a bare column.
  - **It covers every measured row:** M06, M09, M11, M12, the CTE and the union.
  - **The R-5 question:** it is a marker that carries a fact from the AST to the plan. Whether that counts as a new carrier is the owner's call.
- **B2, a root-position mask.** Read the written AST of the outermost `SELECT`, mark the output positions written as `<bare ref> AS <name>`, and after planning mint exactly those positions. `display.rs` already reads the written root projection by position (`set_written`, `projection_written`), so B2 can reuse it. This adds a new `attr_id.rs` function, `mint_positions(plan, &[usize]) -> Result<LogicalPlan>`, which is `project_ids` with fresh ids. There is no marker. It **misses** every nested scope that Spark mints (M12, the CTE row, the union row), so those stay as named residue.

**The DataFrame door is not touched by (b).** `.alias()` and `withColumnRenamed` already mint on the stack (M30–M33 EQUAL). The order asks how a SQL-planner `Projection` of `Alias(Column)` is told apart from a DataFrame-door one. The DataFrame door's aliases carry their own minted id in `Alias.metadata` (`own_id`, `attr_id.rs:282`). Its internal rebinding aliases (`attribute_column(engine).alias(shown)`) carry nothing and must carry. Under B1 or B2, only the user SQL door ever mints a written alias, so the two never meet.

## 2.3 (c) The refusal

**Spark's rule, measured 2026-10-05 (`ops_*.json`, `edge_*.json`):**

| door | Spark refuses (`MISSING_ATTRIBUTES`) when the column's id is… | measured |
|---|---|---|
| `select`, `withColumn`, `groupBy(col)`, `agg(f(col))`, `select` after a join | not in the operator's input | V3, V4, V9, V12 and V15 × each door; DFX = `d.select(d.id, (d.v + 1).alias("v"))`; WCR = `d.withColumnRenamed("v", "v")`; P01, P03 |
| `filter` / `where` / `orderBy` / `sort` | not reachable by Spark's missing-reference pull-up | answers through Project, Filter, Limit and Distinct (P02, P04, P07, P13, P14, P16, P20, P21); refuses across `df.alias()` (P09, P10), any view (P11, P12, P15, V9, V12, V15), Union (P22) and an Aggregate for a non-key column (P05, P06) |
| `drop(col)` | — never refuses: an absent column is a no-op | P17, P18, P19, V4, V9 |

**Where the stack falls back to names today:**
- the bare-column bind, `_bind_stable_id_column` in `python/repark/src/repark/spark/column_fields.py:655`. Its last line, `return column` at line 683, hands an unbound column to name resolution;
- the expression-token bind, `_replace_local_attr_token` in `python/repark/src/repark/spark/dataframe/join_attr_tokens.py:101`. When the id is absent, line 117 returns the token unchanged and the caller falls back.

The only id-based `MISSING_ATTRIBUTES` refusals that exist are two:
- semi/anti unemitted ids, `_raise_if_id_not_emitted` in `python/repark/src/repark/spark/dataframe/unemitted_ids.py:47`;
- the join-condition preparer, `missing_message` and `missing_condition` in `crates/repark-core/src/session/df_guards/self_join.rs:711` and `:720`, raised through `missing_attributes_error` in `crates/repark-python/src/frame_lineage.rs:96`. That function hard-codes `operator = "!Join"` at line 105.

**`resolve` does not distinguish "absent id".** `resolve` (`attr_id.rs:890`) is name-driven: written name, then matching positions, then distinct ids. It answers `Missing` only for a name with no hit, and it never takes an id. So the smallest change is a sibling in `attr_id.rs`, next to `resolve` and not inside it:

```rust
pub enum Located { Output(Vec<usize>), PullUp, Absent }
pub fn locate(plan: &LogicalPlan, id: &AttrId) -> Located
```

How `locate` answers:
- **`Output`:** the id is in the root schema.
- **`PullUp`:** the id is reachable through Spark's measured pull-up chain (Projection, Filter, Sort, Limit, Distinct). An unmeasured node such as Window, Join, Repartition or Unnest also answers `PullUp`, so no new refusal can fire where Spark might answer (R-4 by construction).
- **`Absent`:** everything else, including a `SubqueryAlias`, a view scan, a Union or an Aggregate for a non-key column on the path.

**How the refusal is raised:** a native error builder that takes the operator (`!Project`, `!Filter`, `!Sort`, `!Aggregate`), generalising `missing_attributes_error`. Python only routes on `Located` and never compares ids itself (R-5).
- `select`, `withColumn`, `groupBy` and `agg` refuse unless the column is `Output`.
- `filter` and `sort` refuse only on `Absent`.
- `drop` turns non-`Output` into a no-op.

No name-based fallback is added. The current name fallback stays only on the `PullUp` arm of `filter` and `sort`, which is H3-adjacent. There it already answers (same class), but with a wrong value whenever the alias computes (P07, P08, P13, P14, P16, P20, P21, V3 `where(d.v == 10)`). A true pull-up is a separate card (Q4).

**Spark's exact text (live, 2026-10-05; `#N` ids as printed):**

| shape | condition | message (first line) |
|---|---|---|
| V3 `sql("SELECT v + 0 AS v FROM tv").select(d.v)` | `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION` | `Resolved attribute(s) "v" missing from "v" in operator !Project [v#1L]. Attribute(s) with the same name appear in the operation: "v".` |
| V4 `sql("SELECT v AS v FROM tv").select(d.v)` | same | same text |
| V9 `table(sv).select(d.v)` | same | `Resolved attribute(s) "v" missing from "id", "v" in operator !Project [v#1L]. Attribute(s) with the same name appear in the operation: "v".` |
| V12 `tv` replaced by `e`, `table(tv).select(d.v)` | same | same as V9 |
| V15 view over an aggregate, `table(ta).select(d.v)` | same | same as V9 |
| no same-name output, e.g. `sql("SELECT v AS w FROM tv").select(d.v)` | `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT` | `Resolved attribute(s) "v" missing from "w" in operator !Project [v#1L].  SQLSTATE: XX000` |

For `RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION`, the second line is `Please check if the right attribute(s) are used. SQLSTATE: XX000;`, followed by the analyzed plan.

The stack cannot print `#1L`. The existing RePark refusals print the operator without ids (`!Join`, `!Project.`). Pins should assert the condition, `missingAttributes`, `input`, `operation`, and the message prefix up to `in operator !<Op>`.

**Breadth.** Spark refuses the same `.select(d.v)` after any DataFrame-door alias or rename, too:
- `d.select(d.v.alias("v")).select(d.v)`;
- `d.withColumnRenamed("v", "v").select(d.v)`;
- `d.select("id").select(d.v)` — the stack raises a DataFusion `Schema error` there, condition `None`.

The stack answers or misclassifies these today. A generic (c) moves them all toward Spark, but they are not among the five shapes R-2 names (Q3).

## 2.4 Blast radius

Not run on the proposed mint points; the halt comes first. What was run:
- the baselines above;
- the plan-level throwaway on the id rows only (§2.2(b)). It shows three away-from-Spark id rows, which is why that variant is not proposed.

## 2.5 Slices

Not written; the order of (a), (b) and (c) and their pins depend on the H5 ruling and on Q2.

The card's proposed pins stand as proposals. `test_sql_view_v9_unchanged` is the exception (§6).

## 2.6 Release-note text (draft for R-2; breadth per Q3)

- V3: `spark.sql("SELECT v + 0 AS v FROM tv").select(d["v"])` now raises `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION`, as Spark 4.1.2 does: the alias is a new attribute.
- V4: `spark.sql("SELECT v AS v FROM tv").select(d["v"])` raises the same: in Spark, a SQL alias mints a new attribute even when it keeps the name.
- V9: `spark.table("sv").select(d["v"])`, with `sv` created by `CREATE TEMP VIEW sv AS SELECT * FROM tv`, raises the same: every read of a SQL-defined temp view yields new attributes.
- V12: after `tv` is re-registered from another frame, `spark.table("tv").select(d["v"])` with a column of the old frame raises the same.
- V15: `spark.table("ta").select(d["v"])`, with `ta` a temp view over `d.groupBy("id").agg(sum("v").alias("v"))`, raises the same: the aggregate's `v` is not `d`'s `v`.

## 6. Halt

**H5 fires, by construction.** The #951 pin `python/repark/tests/test_attr_view_semantics_1.py::test_sql_view_v9_unchanged` (stack, line 65) asserts that V9, `spark.table("sv").select(source["v"])`, answers `[(10,), (20,)]`. The order's own aim (§0, R-2) and the card's (c) make V9 raise `MISSING_ATTRIBUTES`. Any (c) that meets the order turns that pin red.
- (a) alone does not: after (a), `sv` mints, and V9 still answers through the name fallback (measured on the throwaway: R-M20 stayed OK).
- The other #951 pins (V-3a, V-3b, the no-internal-error sweep, DESCRIBE, the cycle walk, and the Rust `temp_view_attr_ids`) are not implicated by this analysis. They were not re-run on a patch.

Options, not chosen:
- **O1.** Rule that `test_sql_view_v9_unchanged` was a holding pin for item (d). Slice (c) then replaces it with the refusal pin, `test_sql_view_foreign_column_missing_attributes`, in the same commit, and the H5 wording for this unit excludes that one pin.
- **O2.** Keep the pin and take V9 out of (c), for example by refusing only on `Output`-less Project doors over non-view sources. V9 then still differs from Spark, and R-2's release note loses a line.

## 7. Questions for the owner (each with the measured fact behind it)

- **Q1 (H5):** O1 or O2 above.
- **Q2 ((b) carrier):** B1 (the written-alias marker UDF: complete, but a marker from the AST to the plan) or B2 (the root-position mask: no marker, but it misses nested-scope aliases Spark mints: M12, the CTE row and the union row). A plan-only mint is ruled out by measurement (§2.2).
- **Q3 ((c) breadth):** a generic id-absent refusal also refuses DataFrame-door shapes that Spark refuses and RePark 1.5.2 answers or misclassifies:
  - `d.select(d.v.alias("v")).select(d.v)`;
  - `d.withColumnRenamed("v", "v").select(d.v)`;
  - `d.select("id").withColumn("z", d.v)`;
  - `groupBy(d.v)` and `agg(max(d.v))` over any minted frame.

  Should the release note name them, or (c) be scoped to the five SQL and view shapes?
- **Q4 (pull-up):** `filter` and `sort` with a foreign column below a computed alias answer with the wrong value on the stack today. For example, `d.select(d.id, (-d.v).alias("v")).filter(d.v > 15)`: Spark `[(2, -20)]`, stack `[]` (P07, P13, V3 `where`). This is not a refusal. Is it a separate pull-up card, or in this unit?
- **Q5 (drop):** `table(sv).drop(d.v)` drops `v` by name on the stack, where Spark treats an absent column as a no-op (P19; also V4 and V9 `drop`). Fold it into (c)'s door table, or card it?

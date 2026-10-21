# Sketch EXPLAIN-DML-WRITE-1: one wrapper refusal, three call sites

**Date:** 2026-10-21. **Author:** Claude (claude-opus-5-5), the Opus 5.5 lane, step 0 of
[the order](order.md). **Status:** sketch, documents only. Nothing was run for it: every path and
line below was read on main `c91f69c5` (#1124 merged), not on the order's `085d77e8`.
**Card:** [EXPLAIN-DML-WRITE-1](../../roadmap/mid-term/explain-dml-write-1-card-2026-10-16.md).
**Rulings:** 136, 138, 139, 70, 76 (a). **Retires:** when the unit's build merges or parks; it
is then frozen with the unit's records.

## 1. Summary

- On every door, the twelve wrapper statements of the card's table reach exactly one place where
  the door holds DataFusion's parsed `DFStatement` and has not yet called
  `SessionContext::execute_logical_plan`, which is where DataFusion stores a `PREPARE` and where an
  `EXPLAIN ANALYZE` frame is built to run its write. One function in
  `crates/repark-iceberg/src/write/statement_writes.rs`, beside 106's walk, called once at that
  place on each door, refuses all of them. There is no second seam.
- The three call sites are one line each, in the three product files the order names:
  `repark-core/src/dialect.rs`, `repark-sql/src/router.rs`, `repark-spark/src/spark_ast.rs`.
  `normalize.rs` is not touched. No ceiling rises. No crate edge is added.
- The new refusal runs before 106's G3-E8 walk on `repark.sql()` and before the strict pregate on
  the repark-spark doors, so every wrapped form answers with the wrapper's text, as the order's
  step 4 asks for the `IN (SELECT …)` `PREPARE` forms. 106's pins for wrapped subquery forms keep
  refusing and change only the text they assert (§7).
- 106's walk returns `INSERT` and `MERGE` too, so `EXPLAIN ANALYZE` and `PREPARE` of `INSERT` and
  `MERGE` reach the same seam. That is §6's last halt condition: the decision is the owner's
  (Q1, §9). The sketch names it and does not decide it; its lean is `DELETE` and `UPDATE` only.

## 2. The doors on `c91f69c5`

### 2.1 `repark.sql()` (`DataFusionDialect`, behind the local-filesystem guard)

`repark.sql` (`python/repark/src/repark/__init__.py:73`, native session at :97, call at :98) →
`PyReparkSession.sql` (`crates/repark-python/src/session.rs:171`; the native session is built at
:150 with `with_local_fs_guard` at :161) → `plan_session_sql_inner` (:93) →
`ReparkSession::sql` (`crates/repark-core/src/session.rs:418`) → `sql_with` (:426) →
`sql_with_write_options_inner` (`session/write_options.rs:99`, the dialect call at :124) →
`LocalFsGuardedDialect::execute_with_write_options` (`local_fs_policy.rs:55`, its
`refuse_native_statement` at :61; the default `DataFusionDialect` is chosen at :104) → the trait's
default `execute_with_write_options` (`dialect.rs:93`, empty options, then :105) →
`DataFusionDialect::execute` (`dialect.rs:132`):

1. `belt.plan(query)` (:139 → `pre_execute.rs:35`, `create_logical_plan`): plans, executes nothing.
2. `refuse_dml_subquery_where(cx.ctx, query)` (:140 → :146): parses with the session's
   `sql_parser.dialect` (:147-151); peels `DFStatement::Explain` layers, answering `Ok` at the
   first plain one (:152-158); on a `DFStatement::Statement` (:159) runs
   `refuse_dml_subquery_predicate(&write, false)` (`predicate_dml/subquery_where.rs:38`) on every
   statement `executed_writes` returns (:162-166).
3. `belt.guard` (:141), then `belt.execute` (:142 → `pre_execute.rs:52`,
   `execute_logical_plan`): a `PREPARE` is stored here; an `EXPLAIN ANALYZE` frame runs its write
   when it is read.

### 2.2 The ANSI door (`repark_sql::AnsiDialect`)

`AnsiDialect::execute` (`crates/repark-sql/src/dialect.rs:48`) → `router::execute`
(`router.rs:24`): text guards (:25), pre-parse recognisers (:28-39), text rewrites (:40-51) →
`execute_time_travelled` (:55 → :83): time-travel and lineage rewrites (:89-93), then
`sql_to_statement` with `PARSER_DIALECT`, `Generic` (:96-103).
- A leading `EXPLAIN [ANALYZE]` is a `DFStatement::Explain`; it takes the `let … else` at
  :109-111 straight to `delegate` (:305) → `delegate_plan` (:317): plan (:324), the `Analyze`
  peel for the DML target (:328-331), the encryption and fill steps (:332-340), the guards
  (:342-343) → `execute_logical_plan` (:312-314).
- `PREPARE` and `EXECUTE` are `DFStatement::Statement`; after the G15 and declared-function
  refusals (:125-126) they fall to the `_` arm (:182-190) → `delegate`.
- The G3-E8 twin, `refuse_dml_subquery_predicate(statement, true)`, runs only on the bare
  `UPDATE` (:199) and `DELETE` (:267) arms.

### 2.3 The repark-spark doors (the Spark SQL session and the router API)

- **Session door:** `ReparkSession.sql` (`python/repark/src/repark/spark/session/session_core.py:149`)
  → `sql_run.run_sql` (:174; `sql_run.py:17`, whose SET, CACHE, UDTF and registered-UDF rewrites
  take none of the twelve) → `PyReparkSession.sql` (`session.rs:171`; the facade session installs
  `SparkDialect` at :127) → `ReparkSession::sql` → `SparkDialect::execute` /
  `execute_with_write_options` (`crates/repark-spark/src/dialect.rs:13`, :39) →
  `router::execute_in_session` (`router.rs:88`).
- **Router API:** `repark_spark::execute` (`router.rs:35`), `execute_static_overwrite` (:44),
  `execute_with_read_only` (:60), `execute_with_statement_options` (:77), re-exported at
  `lib.rs:67` → `execute_in_session` (:88).
- **Shared from there:** `execute_calibrated` (:130 → :133) → `nested_ns::refuse_nested_supply`
  (:143 → `router/nested_ns.rs:78`, its walk at :132; it refuses only for a table with a nested
  naive timestamp leaf, so the card's `INT` seeds pass) → `execute_time_travelled` (:189 → :208)
  → `execute_inner` (:252 → :284) → `parse_single_normalized` (:316 → `normalize.rs:187`,
  Databricks dialect) → the match at :331, where `Statement::Explain`, `Statement::Prepare` and
  `Statement::Execute` take the `_` arm (:410) → `spark_ast::execute_passthrough`
  (`spark_ast.rs:29`) → `execute_passthrough_by` (:37) → `execute_passthrough_inner` (:190) →
  `sql_to_statement` (:204) → the match at :209:
  - `DfStatement::Explain` (:238-242) only lowers `timestamp_ns` casts; plan (:248), guards
    (:265, :267), not an eager command (:269), `execute_logical_plan` (:293).
  - `DfStatement::Statement` (:210): the before-pregate (:211); `try_execute_identity_dml`
    (:212) declines a `PREPARE`; the strict after-pregate (:215 → :154 →
    `refuse_dml_subquery_predicate_in_statement`, `normalize.rs:368`) skips the allow-list
    (:370-374, a `PREPARE` is not an allow-listed `DELETE`), then refuses a subquery predicate on
    each statement the walk returns (:375-377 → :381); then plan and `execute_logical_plan`
    (:293) store the `PREPARE`.
- The bare `DELETE` and `UPDATE` take `delete_update::execute_delete` / `execute_update`
  (`router.rs:385`, :388) and end in the same passthrough (`router/delete_update.rs:39`, :90).
  A statement `parse_single_normalized` cannot read goes through
  `execute_unparsable_fallthrough` (`router.rs:943`) to the same passthrough (:964).

## 3. The twelve statements, per door, on `c91f69c5` (by reading)

`t` and `s` are the card's seeds. "Writes" means the frame writes when it is read. The card
measured main `56057f65`, before #1090 (106) merged; the cells 106 changed are marked.

| # | Statement | `repark.sql()` | ANSI door | Both repark-spark doors |
|---|---|---|---|---|
| 1 | `EXPLAIN DELETE … WHERE id = 2` | :155 answers `Ok`; plan, no write on Iceberg | :109-111 → `delegate`; plan, no write | :238 → :293; plan, no write |
| 2 | `EXPLAIN UPDATE … WHERE id = 2` | as 1 | as 1 | as 1 |
| 3 | `EXPLAIN DELETE … WHERE id IN (SELECT id FROM s)` | as 1 | as 1 | as 1 |
| 4 | `EXPLAIN UPDATE … WHERE id IN (SELECT …)` | as 1 | as 1 | as 1 |
| 5 | `EXPLAIN ANALYZE DELETE … WHERE id = 2` | peeled (:157), walk returns the `DELETE`, no subquery → `Ok` → :142; writes 1 row | :109-111 → `delegate`; writes 1 row | :238 → :293; writes 1 row |
| 6 | `EXPLAIN ANALYZE UPDATE … WHERE id = 2` | as 5; writes 1 row | as 5 | as 5 |
| 7 | `EXPLAIN ANALYZE DELETE … WHERE id IN (SELECT …)` | **since 106:** G3-E8 refusal at :163, rows held | writes every row (DataFusion's DML planner) | writes every row; never reaches the pregate (106 §13.2) |
| 8 | `EXPLAIN ANALYZE UPDATE … WHERE id IN (SELECT …)` | **since 106:** G3-E8 refusal | writes every row | writes every row |
| 9 | `PREPARE p AS DELETE … WHERE id = 2; EXECUTE p` | walk unwraps `Prepare` (`statement_writes.rs:17`), no subquery → stored at :142; `EXECUTE p` writes 1 row | `_` arm → stored; `EXECUTE p` writes 1 row | strict pregate passes → stored at :293; `EXECUTE p` writes 1 row |
| 10 | `PREPARE p AS UPDATE … WHERE id = 2; EXECUTE p` | as 9 | as 9 | as 9 |
| 11 | `PREPARE p AS DELETE … WHERE id IN (SELECT …); EXECUTE p` | **since 106:** G3-E8 refusal at `PREPARE`; `EXECUTE p`: `Prepared statement 'p' does not exist` | stored; `EXECUTE p` writes every row | **since 106:** G3-E8 refusal at `PREPARE` (`normalize.rs:375`) |
| 12 | `PREPARE p AS UPDATE … WHERE id IN (SELECT …); EXECUTE p` | as 11 | as 11 | as 11 |

The control row (`EXPLAIN SELECT * FROM t WHERE id = 2`) takes the row-1 path on every door.

## 4. The one seam

**Home:** `crates/repark-iceberg/src/write/statement_writes.rs` (35 lines), beside
`executed_writes`. The crate already reaches `datafusion::sql::parser::Statement`.

**Signature:**

```rust
#[allow(clippy::missing_errors_doc)]
pub fn refuse_wrapped_write(statement: &DFStatement) -> datafusion::error::Result<()>
```

with `use datafusion::sql::parser::Statement as DFStatement;`. The `allow` is the one
`predicate_dml/subquery_where.rs:37` carries: the comment ban leaves no `# Errors` doc line, and
clippy pedantic asks for one on a public function that returns `Result`. The error is
`DataFusionError::Plan`, the default the order names; the facade maps it to `AnalysisException`
as it maps the G3-E8 refusal. Spark 4.1.2 raises `ParseException` for both wrappers; the class
differs, the order sets it, and the ledger records it.

**What it answers:**

| Parsed statement | Answer |
|---|---|
| `DFStatement::Explain` with `analyze` false, at any depth | `Ok`: a plain `EXPLAIN` runs nothing (ruling 136; the walk's rule) |
| `DFStatement::Explain` with `analyze` true | look through it; if the `DFStatement::Statement` under it walks to a refused write, the `EXPLAIN ANALYZE` refusal |
| `DFStatement::Statement(Statement::Prepare { .. })` | if `executed_writes` of it returns a refused write, the `PREPARE` refusal |
| `DFStatement::Statement(Statement::Explain { analyze: true, .. })` | as a DataFusion `EXPLAIN ANALYZE` (DataFusion's parser never yields it at the top; a sqlparser parse can) |
| every other `DFStatement::Statement`: a bare `DELETE`/`UPDATE`/`INSERT`/`MERGE`, a query, a CTE carrier, `EXECUTE` | `Ok`: bare statements keep their door's routing and 106's refusal |
| `CopyTo`, `CreateExternalTable`, `Reset` | `Ok` (`EXPLAIN ANALYZE COPY` keeps the PA-LOCAL-FS-POLICY-1 refusal) |

"A refused write" is a `Statement::Delete` or `Statement::Update` in `executed_writes(..)` under
Q1's lean, and any statement it returns under the other answer. The text follows the outermost
wrapper: `PREPARE p AS EXPLAIN ANALYZE DELETE …` gets the `PREPARE` text, and
`EXPLAIN ANALYZE PREPARE p AS DELETE …` gets the `EXPLAIN ANALYZE` text. Shape, about 45 lines with
the two texts and the `refused_write` test, no comment:

```rust
pub fn refuse_wrapped_write(statement: &DFStatement) -> Result<()> {
    let mut analyzed = false;
    let mut current = statement;
    while let DFStatement::Explain(explain) = current {
        if !explain.analyze {
            return Ok(());
        }
        analyzed = true;
        current = explain.statement.as_ref();
    }
    let DFStatement::Statement(inner) = current else {
        return Ok(());
    };
    let refusal = match inner.as_ref() {
        _ if analyzed => explain_analyze_refusal(),
        Statement::Explain { analyze: true, .. } => explain_analyze_refusal(),
        Statement::Prepare { name, .. } => prepare_refusal(name),
        _ => return Ok(()),
    };
    if executed_writes(inner).iter().any(refused_write) {
        return Err(DataFusionError::Plan(refusal));
    }
    Ok(())
}
```

`EXECUTE` needs no arm: a refused `PREPARE` is never stored, so `EXECUTE p` meets DataFusion's
`Prepared statement 'p' does not exist`.

## 5. The three call sites

Each is one call, on the parsed statement the door already holds, before anything executes.

| Door | File and place | Line count |
|---|---|---|
| `repark.sql()` | `crates/repark-core/src/dialect.rs`, in `refuse_dml_subquery_where`, after the parse (:149-151) and before the `EXPLAIN` peel (:152): `repark_iceberg::write::statement_writes::refuse_wrapped_write(&parsed)?;` | 171 → 172 |
| ANSI | `crates/repark-sql/src/router.rs`, in `execute_time_travelled`, after the parse (:96-103) and before the `RESET` arm (:104): `repark_iceberg::write::statement_writes::refuse_wrapped_write(&statement)?;` | 365 → 366 or 367 after `cargo fmt` |
| both repark-spark doors | `crates/repark-spark/src/spark_ast.rs`, in `execute_passthrough_inner`, after `sql_to_statement` (:204) and before the match (:209): `repark_iceberg::write::statement_writes::refuse_wrapped_write(&statement)?;` | 830 → 831 or 832 |

- **Order on each door.** On `repark.sql()` the call runs after `belt.plan` (a statement that does
  not plan keeps its plan error) and before 106's walk. On the ANSI door it runs after the text
  guards and the rewrites, before any routing. On the repark-spark doors it runs after the
  nested-NTZ gate (`router.rs:143`), so a wrapped write into a nested naive leaf keeps the
  nested-NTZ gate's refusal text, and before the strict pregate. #1124's `PREPARE`-with-parameter rule
  in `nested_ns.rs` is untouched.
- **Why `spark_ast.rs`, not `router.rs`.** `execute_passthrough_inner` is the only place on the
  repark-spark doors that holds a `DFStatement`; every one of the twelve reaches it (§2.3).
  `router.rs` (968 lines) is not one of the order's files.
- **Why inside `refuse_dml_subquery_where`.** It is the only place in `dialect.rs` that holds the
  parse; a second parse in `execute` would be a second call.

## 6. The two refusal texts

Both name the statement, say what still works, and name no data-deleting action (ruling 76 (a)):
no `DELETE`, `TRUNCATE`, `DROP`, purge or expire, not even the verb the user wrote.

**`PREPARE` (ruling 138's constraints; the exact words are this sketch's):**

```text
PREPARE `{name}` is refused: its statement writes to a table, and a prepared write would run later, at EXECUTE. PREPARE of a query still works.
```

**`EXPLAIN ANALYZE` (proposed; the owner rules it):**

```text
EXPLAIN ANALYZE is refused: its statement writes to a table, and EXPLAIN ANALYZE runs the statement it explains. A plain EXPLAIN of the same statement still shows its plan without running it, and EXPLAIN ANALYZE of a query still works.
```

`{name}` is the prepared name as written (`Ident`'s display). Both are `const` texts plus that one
substitution, so a pin can assert them whole.

## 7. Existing pins that flip, and why

Every flip below is a row of the card's table under Q2's reading, on a door the unit closes.

**Refusal to refusal: only the asserted text changes (the seam runs before 106's walk).**
- `crates/repark-core/src/dialect/tests.rs`:
  `the_default_dialect_refuses_an_explain_analyze_of_a_subquery_dml` (3 cells, G3-E8 → the
  `EXPLAIN ANALYZE` text); `a_prepared_subquery_dml_refuses_and_leaves_nothing_to_execute`
  (4 cells → the `PREPARE` text); `only_explain_analyze_reaches_the_statement_it_wraps` (its
  first loop of 3 → the wrapper texts, because the call sits inside the function it drives; its
  second loop, the three plain `EXPLAIN` forms, still answers `Ok`).
- `crates/repark-spark/src/tests/wrapped_subquery_dml.rs`:
  `a_prepared_subquery_delete_refuses_on_the_router_door`,
  `a_prepared_subquery_update_refuses_on_the_router_door`,
  `a_prepared_explain_analyze_subquery_delete_refuses_on_the_router_door` (G3-E8 → `PREPARE`).
- `python/repark/tests/writes/test_repark_sql_update_subquery_where_1.py`:
  `test_an_explain_analyze_of_a_subquery_predicate_refuses` (P-E3, P-E4) and
  `test_a_prepared_subquery_predicate_refuses` (P-P1 to P-P4).
- `python/repark/tests/writes/test_repark_sql_update_subquery_where_1_spark_door.py`:
  `test_a_prepared_subquery_predicate_refuses` (P-S1 to P-S3).

**Answer to refusal (ruling 138: a `PREPARE` whose body is a write).**
- `wrapped_subquery_dml.rs::a_plain_predicate_prepare_still_runs_on_the_router_door` (the order
  names it).
- `test_repark_sql_update_subquery_where_1_spark_door.py::test_a_plain_predicate_prepare_still_runs`
  (2 cells; the order names it).

**Answer to refusal (ruling 136: `EXPLAIN ANALYZE` of `DELETE` or `UPDATE`).**
- `crates/repark-spark/src/tests/nested_ns_routes.rs`:
  `a_wrapped_statement_that_does_not_supply_the_column_still_runs` (its
  `EXPLAIN ANALYZE UPDATE ice.sales.t SET k = 3 WHERE id = 1` cell; the snapshot arithmetic of
  the loop moves with it) and `a_wrapped_write_to_a_table_with_no_such_leaf_is_not_gated` (its
  `EXPLAIN ANALYZE UPDATE ice.sales.p …` cell, the same).
- `python/repark/tests/writes/test_write_body_carriers_1.py::test_wrappers_keep_running_their_write`:
  the `update` and `delete` bodies under `EXPLAIN ANALYZE`, format versions 2 and 3 (4 cells).
- `python/repark/tests/timestamps/test_ice_tsns_type_planner_1.py` (slow, in the full facade
  suite): the census route `explain_analyze_update` stores nothing any more. Of its 150 census
  cells, `ice_tsns_type_planner_1_main.json` lists 105 as errored on main, so about 45 cells
  main answered become lost, and the harness fails them unless each has a dated row (Q4).
  Step 1 counts them exactly.

**Unchanged, checked by reading.** `a_statement_that_wraps_a_write_is_decided_as_the_write_it_runs`
(`nested_ns_routes.rs`), `ntz_nested_refuse_routes.rs::a_statement_that_wraps_a_write_of_an_instant_is_decided_as_that_write`,
and `test_ice_tsns_merge_wall_1.py::test_a_statement_that_wraps_a_nested_write_is_refused_and_writes_nothing`:
the nested-NTZ gate refuses them first, before the passthrough. Plain `EXPLAIN` pins, bare-`IN`
pins, the `EXPLAIN ANALYZE COPY` pins (PA-LOCAL-FS-POLICY-1), `EXPLAIN ANALYZE SELECT` pins
(`attr_id.rs`, `nlj_tight_pool.rs`, the spill and perf files, `test_df_explain_1.py`),
`PREPARE p_sink AS CREATE VIEW` (`temp_view_doors.rs`): the walk returns no write for any of them.

**Flip only if Q1 is answered (a).** `nested_ns_routes.rs` (the `INSERT` cells and the prepared
`INSERT` of the two tests above), `ntz_nested_refuse_routes.rs::a_prepared_write_of_an_instant_refuses_in_a_utc_session_too`
(the `walled` control), `ntz_nested_refuse_casts.rs::a_prepared_write_with_a_parameter_that_supplies_the_column_refuses_in_every_zone`
(the `keyed`, `fixed`, `walled` controls), `enc_1.rs::encrypted_table_explain_analyze_insert_refuses`
(the text: the seam would run before the encryption refusal at `spark_ast.rs:277`),
`test_write_body_carriers_1.py::test_wrappers_keep_running_their_write` (the `insert` and
`overwrite` cells under both wrappers, and the `merge` cell's exception class), and the planner
census routes `explain_analyze_insert` and `prepare_execute_insert` (about 65 answered cells each).

## 8. The pins this unit adds (step 1 writes them red first)

Every seed is created through the Iceberg catalog, and one `assert_iceberg` guard requires an
`IcebergTableScan` in `EXPLAIN SELECT * FROM <table>` before the cell; rows and the snapshot
count are read before and after (and the warehouse files in Python).

- **`repark-iceberg`, `write::statement_writes::tests`** (new `statement_writes/tests.rs` and
  its `map.md`; the module filter `cargo test -p repark-iceberg statement_writes` reaches it):
  `an_explain_analyze_of_a_delete_or_update_refuses_with_its_text` (plain, `IN`, `VERBOSE`,
  nested `EXPLAIN ANALYZE`, around `PREPARE`);
  `a_prepare_of_a_delete_or_update_refuses_with_its_text` (plain, `IN`, parameterised, around
  `EXPLAIN ANALYZE`); `a_query_a_plain_explain_and_a_bare_statement_answer_nothing` (`SELECT`,
  `EXPLAIN SELECT`, `EXPLAIN ANALYZE SELECT`, `PREPARE p AS SELECT`, `EXPLAIN DELETE`,
  `EXPLAIN ANALYZE EXPLAIN DELETE`, `PREPARE p AS EXPLAIN DELETE`, `EXECUTE p`, bare `DELETE` and
  `UPDATE`, `EXPLAIN ANALYZE COPY`); `neither_refusal_names_a_data_deleting_action`; and, by Q1,
  `an_insert_or_merge_under_a_wrapper_answers_nothing` or its refusing twin.
- **`repark.sql()`, Rust** (`crates/repark-core/src/dialect/tests.rs`):
  `an_explain_analyze_of_dml_refuses_and_writes_nothing` (`DELETE`, `UPDATE`; plain and `IN`);
  `a_prepared_write_refuses_and_leaves_nothing_to_execute` (plain and `IN`, and
  `PREPARE p AS EXPLAIN ANALYZE DELETE …`; `EXECUTE p` then finds no statement);
  `a_prepared_query_and_a_plain_explain_still_answer`.
- **ANSI door** (new `crates/repark-sql/tests/ansi_explain_dml_write.rs`, seeded as
  `ansi_subquery_where_identity.rs` is, plus the guard, VF-1; its `map.md` row):
  `the_ansi_door_refuses_an_explain_analyze_of_dml`,
  `the_ansi_door_refuses_a_prepared_write_and_leaves_nothing_to_execute`,
  `the_ansi_door_still_answers_a_prepared_query_and_a_plain_explain` (with `EXPLAIN SELECT` and
  `EXPLAIN ANALYZE SELECT`). `cargo test -p repark-sql --test ansi_explain_dml_write`.
- **Router API** (`crates/repark-spark/src/tests/wrapped_subquery_dml.rs`):
  `an_explain_analyze_of_dml_refuses_on_the_router_door`,
  `a_prepared_write_refuses_on_the_router_door` (the three G3-E8 `PREPARE` pins and the plain
  one, rewritten to the `PREPARE` text), `a_prepared_query_still_runs_on_the_router_door`,
  `an_explain_analyze_of_a_select_answers_on_the_router_door`; the bare-`IN` and plain-`EXPLAIN`
  pins stay.
- **Python, `repark.sql()` and the Spark SQL session** (new
  `python/repark/tests/writes/test_explain_dml_write_1.py`, both doors as one parameter; its
  `writes/map.md` row): `test_an_explain_analyze_of_dml_refuses_and_writes_nothing`,
  `test_a_prepared_write_refuses_and_leaves_nothing_to_execute`,
  `test_a_prepared_query_still_executes`,
  `test_a_plain_explain_of_dml_answers_its_plan_and_writes_nothing`,
  `test_explain_and_explain_analyze_of_a_select_answer`, `test_df_explain_cost_still_answers`
  (Spark session), `test_the_bare_in_forms_keep_the_matching_rows_on_the_spark_door`. The
  flipped 106 cells of §7 are edited where they are, not moved: a move between files is a
  relocation unit of its own (docs/testing.md).

**Mutants for step 5:** each call site removed alone; the `Explain` look-through removed; the
`Prepare` arm removed; the plain-`EXPLAIN` early `Ok` turned into a refusal (the controls kill
it); `refused_write` widened or narrowed against Q1's answer.

## 9. Questions

- **Q1 (halt condition 6, the order §1 "not ruled").** Do `EXPLAIN ANALYZE` and `PREPARE` of
  `INSERT` and `MERGE` refuse too? *Premise:* the seam reads `executed_writes`, which returns
  `INSERT`, `UPDATE`, `MERGE` and `DELETE`, so both verbs reach it. Ruling 138's words ("a
  write") read literally include `INSERT`; the order keeps `INSERT` and `MERGE` for the owner.
  Today `EXPLAIN ANALYZE INSERT` and `PREPARE … AS INSERT` then `EXECUTE` write on every door,
  and six tests plus about 130 planner census cells assert that (§7); #1124 built a
  `PREPARE`-with-parameter refusal for prepared `INSERT`s. `MERGE` already refuses under both
  wrappers on every door: DataFusion 54.1's planner has no `MERGE` arm (`Unsupported SQL
  statement`), and `test_wrappers_keep_running_their_write` pins that, rows held. Spark 4.1.2
  refuses both wrappers for every statement. *Lean:* (b) this unit refuses `DELETE` and `UPDATE`
  only, the card's table and the verbs that change or lose existing rows; `INSERT` and `MERGE`
  under the wrappers stay as main answers them, and the ledger files them as a follow-up for a
  separate ruling. (a) widens the same function by one match arm and flips the pins listed.
- **Q2 (ruling 70's reading of "the card's table").** *Premise:* the card writes each row as a
  wrapper and a verb with `…` for the rest. *Lean:* a row covers every spelling of that wrapper
  and verb: any predicate or none, `VERBOSE` or `FORMAT`, a parameterised `PREPARE`, a CTE-carried
  statement, and `EXPLAIN ANALYZE` around `PREPARE` (the last refused on `repark.sql()` before 106 with
  `Unsupported logical plan: Statement(Prepare)`, 106 ledger §12.7). Step 6's census reports every lost answer against its row.
- **Q3 (stale docstrings).** *Premise:* the flipped Python pins sit in files whose module and
  test docstrings say `PREPARE` refuses "with the G3-E8 text" or that wrappers "answer as main
  answers them" (`test_repark_sql_update_subquery_where_1.py`, its `_spark_door` twin,
  `test_write_body_carriers_1.py`); ruling 138 makes those sentences false, and the comment rule
  forbids editing a comment line. *Lean:* delete only the sentences that become false, add no
  docstring text, and put the new statement in the `writes/map.md` rows and the ledger.
- **Q4 (the planner census).** *Premise:* `test_ice_tsns_type_planner_1.py` fails a cell main
  answered and the head does not, unless `ice_tsns_type_planner_1_dated.json` holds a dated row
  for it with its ruling; that file is a test fixture the order does not list. *Lean:* add one
  dated row per lost `explain_analyze_update` cell, ruling 136, class "ruled refusal", with the
  exact cells counted in step 1; no change to the harness or to the census route.

## 10. Halt conditions of the order's §6, checked by reading

| Condition | Result |
|---|---|
| A second seam | None. All twelve statements reach one parsed-`DFStatement` point per door before `execute_logical_plan` (§2, §3). |
| A statement main answers that the head refuses outside the card's table | None by reading under Q2's reading and Q1's lean; every answer-to-refusal flip in §7 is a table row. Q1 (a) would add `INSERT` answers outside the table. Step 6 measures. |
| A product file §2 does not list, or a ceiling that rises | None. Four product files; the largest growth is `statement_writes.rs` (35 → about 80); `normalize.rs` untouched. Q3 and Q4 touch test files and one test fixture outside §2's test list, named here. |
| A seed that is not an `IcebergTableScan` | Not measured at step 0. The ANSI seed is CTAS through `AnsiDialect` (VF-1); step 1 adds the guard and runs it before any other pin. |
| A refusal text that names a data-deleting action | None. §6's texts name `PREPARE`, `EXECUTE`, `EXPLAIN`, `EXPLAIN ANALYZE`, a query, a table and a write. |
| `EXPLAIN ANALYZE` of `INSERT` or `MERGE` reaching the seam with a decision to make | **Met.** Both reach it (Q1). |

## 11. Observed, out of scope

- The ANSI door has no walk for a DML carried inside a query (`WITH c AS (…) DELETE … WHERE id
  IN (SELECT id FROM c)`): it falls to `delegate`, where DataFusion answers `This feature is not
  implemented: Query …` and writes nothing (106 §13.4 measured that answer on the repark-spark
  doors). No wrapper is involved, so the seam does not change it.
- A plain `EXPLAIN` of DML read through `repark.sql()` on an in-memory table writes (ruling 136,
  not this card).

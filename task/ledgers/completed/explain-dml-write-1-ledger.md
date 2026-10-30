# Unit ledger — EXPLAIN-DML-WRITE-1 · `EXPLAIN ANALYZE` and `PREPARE` of a write refuse on every door

**Date:** 2026-10-21 · **Branch:** `feat/explain-dml-write-1` · **Base:** `origin/main` (`c91f69c5`)
· **Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. `risk_tier: standard`. **Order:**
[order.md](../../wo/explain-dml-write-1/order.md). **Sketch (step 0):**
[sketch.md](../../wo/explain-dml-write-1/sketch.md) (`9bcd0da5`). **Card:**
[EXPLAIN-DML-WRITE-1](../../roadmap/mid-term/explain-dml-write-1-card-2026-10-16.md).

**Retires:** this ledger moves to `../completed/` in the unit's last commit after the verifier's
PASS and the owner's merge.

**Why.** On every RePark door `EXPLAIN ANALYZE` of a write performs the write, and `PREPARE` of
one stores a plan that `EXECUTE` runs; with a subquery in a `DELETE` or `UPDATE` `WHERE` either
form changed every row (card, 101 measured cells). Spark 4.1.2 has neither form and refuses both
at parse.

**Rulings this unit runs under.** 136, 138 and 139 (card); the owner's step-0 rulings of
2026-10-21:
- **Ruling 140:** the `EXPLAIN ANALYZE` text, adopted verbatim as the sketch proposed it; the
  `PREPARE` text adopted as the sketch wrote it under ruling 138. Both are **ruled 2026-10-21
  (owner ruling 140)**:
  - `EXPLAIN ANALYZE is refused: its statement writes to a table, and EXPLAIN ANALYZE runs the statement it explains. A plain EXPLAIN of the same statement still shows its plan without running it, and EXPLAIN ANALYZE of a query still works.`
  - ``PREPARE `{name}` is refused: its statement writes to a table, and a prepared write would run later, at EXECUTE. PREPARE of a query still works.``
- **Ruling 141 (sketch Q1, halt 6):** (a). `INSERT` and `MERGE` refuse under both wrappers too, in
  this unit, on every door, through the same seam: one rule, a wrapper never performs a write.
- **Ruling 142 (Q2):** a row of the card's table covers every spelling of that wrapper and verb
  (any or no predicate, `VERBOSE`, `FORMAT`, a parameterised `PREPARE`, a CTE-carried statement,
  `EXPLAIN ANALYZE` around `PREPARE`).
- **Ruling 143 (Q3):** delete only false docstring sentences; add no docstring text; never reword.
- **Ruling 144 (Q4):** one dated row per lost planner census cell in
  `ice_tsns_type_planner_1_dated.json`, class "ruled refusal", ruling 136 for `EXPLAIN ANALYZE`
  cells and 138 for `PREPARE` cells; the order's §2 file list gains that fixture.

**Not in this unit:** a plain `EXPLAIN` of DML on an in-memory table (ruling 136); `EXPLAIN` or
`EXPLAIN ANALYZE` of a query; the bare subquery-predicate `DELETE`/`UPDATE` (106's, unchanged);
routing `repark.sql()` through the ANSI door; `Cargo.toml`, `Cargo.lock`, `.github`.

## PROPOSITION LEDGER — EXPLAIN-DML-WRITE-1 — 2026-10-21

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | On `repark.sql()`, `EXPLAIN ANALYZE` of `DELETE`, `UPDATE`, `INSERT` and `MERGE`, each with a plain and an `IN (SELECT …)` predicate, refuses with the ruling-140 text; the Iceberg table's rows, snapshot count and warehouse files hold. | `test_explain_dml_write_1.py` `test_an_explain_analyze_of_a_write_refuses_and_writes_nothing[repark_sql-*]`; `an_explain_analyze_of_a_write_refuses_and_writes_nothing` in `crates/repark-core/src/dialect/tests.rs`. | **PROVEN** (2026-10-21; a `MERGE` under either wrapper keeps DataFusion's plan refusal on this door, which plans first, and writes nothing, §3) | Red on main (§1), green on the head (§4). |
| C-002 | On `repark.sql()`, `PREPARE p AS` each of the same eight writes refuses with the ruling-138 text; `EXECUTE p` then finds no prepared statement; rows, snapshots and files hold. | `test_a_prepared_write_refuses_and_leaves_nothing_to_execute[repark_sql-*]`; `a_prepared_write_refuses_and_leaves_nothing_to_execute` (Rust). | **PROVEN** (2026-10-21; the same `MERGE` note as C-001) | Red on main (§1), green on the head (§4). |
| C-003 | On the ANSI door (`AnsiDialect`), both wrappers refuse each of the eight writes with their texts, `EXECUTE p` finds nothing, and the rows and snapshots hold; every seed passes an `IcebergTableScan` guard (VF-1). | `crates/repark-sql/tests/ansi_explain_dml_write.rs`. | **PROVEN** | Red on main (§1.2), green on the head (§4); the guard passes on every seed (§6: 106 of 106). |
| C-004 | On both repark-spark doors (the Spark SQL session and the router API), both wrappers refuse each of the eight writes with their texts, `EXECUTE p` finds nothing, rows and snapshots hold. | `test_explain_dml_write_1.py[spark_session-*]`; `wrapped_subquery_dml.rs` `an_explain_analyze_of_a_write_refuses_on_the_router_door`, `a_prepared_write_refuses_on_the_router_door`. | **PROVEN** | Red on main (§1), green on the head (§4). |
| C-005 | Ruling 142's spellings refuse on every door: `EXPLAIN ANALYZE VERBOSE`, a parameterised `PREPARE`, `PREPARE` around `EXPLAIN ANALYZE`, `EXPLAIN ANALYZE` around `PREPARE`, a CTE-carried write; the text follows the outermost wrapper. | `test_every_spelling_of_a_wrapped_write_refuses`; the Rust door pins; the unit tests of `statement_writes`. | **PROVEN** | §1, §4; unit tests §2; the census §6. |
| C-006 | Controls answer on every door, before and after: `PREPARE q AS SELECT …` then `EXECUTE q`; a plain `EXPLAIN` of `DELETE`, `UPDATE` and `INSERT` answers its plan and writes nothing; `EXPLAIN SELECT` and `EXPLAIN ANALYZE SELECT` answer; `df.explain("cost")` answers; the bare `IN` forms keep the matching rows on the Spark SQL door. | The control pins of the four door files. | **PROVEN** | Green on main (§1) and on the head (§4); unchanged in the census (§6). |
| C-007 | One seam: `repark_iceberg::write::statement_writes::refuse_wrapped_write(&DFStatement)` beside `executed_writes`, with its unit tests; one call per door in `repark-core/src/dialect.rs`, `repark-sql/src/router.rs` and `repark-spark/src/spark_ast.rs`; no other product file, no ceiling raised, no crate edge, no comment line. | Steps 2 and 3; `make check-rust-file-size`, `make check-crate-dag`, the comment gate. | **PROVEN** | §2, §3, §8. |
| C-008 | Every existing pin that rulings 136, 138 and 141 flip is re-asserted in place with the wrapper's text, listed here with its red line; docstrings touched only as ruling 143 allows. | §1.3. | **PROVEN** | §1.3 lists each pin, its ruling and its red line; green on the head (§4). |
| C-009 | Each call site removed alone reds that door's pins; the seam's `Explain` look-through and `Prepare` arm, each removed alone, red named pins. | Step 5. | **PROVEN** | §5: three call-site reverts and five mutants, none survived. |
| C-010 | Never worse: the 106 census shape re-run on main and head on each door; every lost answer is a row of the card's table under rulings 141 and 142. | Step 6. | **PROVEN** | §6: 424 cells, 0 changed outside the table, 0 tables not held. |
| C-011 | The planner census (`test_ice_tsns_type_planner_1.py`) passes on the head with one dated row per cell the head loses on the `explain_analyze_insert`, `explain_analyze_update` and `prepare_execute_insert` routes, and no other change to the harness or the routes. | Ruling 144; step 1 and step 4 counts. | **PROVEN** | §1.4, §4.1: 350 dated rows; the harness passes 60 of 60 on the head. |
| C-012 | The records say what holds: the card, the 1.5.6 known-issue lines, the registry rows, the maps of every touched directory, this ledger. | Step 7. | **PROVEN** | §7. |
| C-013 | The gates are green on the final commit. | Order §5. | **PROVEN** | §8. |

VERDICT: 13 clauses, 0 PROVEN, 13 OPEN, 0 REJECTED (step 1). **2026-10-21, step 7:** 13 clauses, 13 PROVEN, 0 OPEN, 0 REJECTED.

## 1. Red first (step 1, tests only)

The product code of the tree is main's `c91f69c5` byte for byte (`git diff origin/main --
crates/` touches only `#[cfg(test)]` files). The release module was built from it with
`maturin develop --profile ci`: sha256
`60286bb37aaae7728dfe3c1c332d691f19d2e546b24b2583f8a35762f478d522`. Every seed is created through
the Iceberg catalog and checked for an `IcebergTableScan` before the cell.

### 1.1 Python, main's module

`pytest` on the four files (`test_explain_dml_write_1.py`, the two 106 files,
`test_write_body_carriers_1.py`): **50 failed, 61 passed**. Every control passed. The red
lines, one per kind:

| Pins | Red on main |
|---|---|
| `test_an_explain_analyze_of_a_write_refuses_and_writes_nothing` (both doors; `delete`, `update`, `insert` × plain and `in`) | `Failed: DID NOT RAISE AnalysisException` (the write ran) or, for the `IN` `DELETE`/`UPDATE` forms on `repark.sql()`, `AssertionError: Error during planning: DELETE with a subquery in its WHERE clause is refused on `ice.ns.t`…` (106's G3-E8 text, not the wrapper's) |
| the same, `merge_plain`, `merge_in` (both doors) | `repark.errors.UnsupportedOperationException: This feature is not implemented: Unsupported SQL statement: MERGE INTO ice.ns.t AS target USING …` |
| `test_a_prepared_write_refuses_and_leaves_nothing_to_execute` (both doors, eight writes) | `Failed: DID NOT RAISE AnalysisException` (stored); the `IN` `DELETE`/`UPDATE` forms the G3-E8 text; `MERGE` the `UnsupportedOperationException` above |
| `test_every_spelling_of_a_wrapped_write_refuses` (both doors, 3 spellings) | `DID NOT RAISE` or the G3-E8 text |
| 106: `test_an_explain_analyze_of_a_subquery_predicate_refuses` (P-E3, P-E4), `test_a_prepared_subquery_predicate_refuses` (P-P1 to P-P4) | `test_repark_sql_update_subquery_where_1.py:226: AssertionError: Error during planning: DELETE with a subquery in its WHERE clause is refused on `ice.ns.t`…` |
| 106 Spark door: `test_a_prepared_subquery_predicate_refuses` (P-S1 to P-S3) | `…_spark_door.py:107: AssertionError: Error during planning: DELETE with a subquery in its WHERE clause is refused on `ice.ns.t`…` |
| 106 Spark door: `test_a_plain_predicate_prepare_refuses` (2) | `…_spark_door.py:127: Failed: DID NOT RAISE AnalysisException` |
| `test_write_body_carriers_1.py::test_wrappers_refuse_their_write` | `test_write_body_carriers_1.py:303: Failed: DID NOT RAISE AnalysisException` |

### 1.2 Rust, main's product code

`cargo test -p repark-core --lib dialect`: 5 failed, 9 passed. `cargo test -p repark-sql --test
ansi_explain_dml_write`: 2 failed, 1 passed. `cargo test -p repark-spark --lib --
wrapped_subquery_dml nested_ns_routes ntz_nested_refuse_routes enc_1`: 10 failed, 64 passed.
`cargo test -p repark-spark --test ntz_nested_refuse_casts`: 1 failed, 9 passed. The controls
(`a_prepared_query_and_a_plain_explain_still_answer`,
`the_ansi_door_still_answers_a_prepared_query_and_a_plain_explain`,
`a_prepared_query_still_runs_on_the_router_door`,
`explain_and_explain_analyze_of_a_select_answer_on_the_router_door`, the bare-`IN` and
plain-`EXPLAIN` router pins) pass.

| Pin | Where | Red on main |
|---|---|---|
| `only_explain_analyze_reaches_the_statement_it_wraps` | `crates/repark-core/src/dialect/tests.rs:465:9` | `EXPLAIN ANALYZE DELETE FROM t WHERE id IN (SELECT id FROM s): Error during planning: DELETE with a subquery in its WHERE clause is refused …` |
| `a_prepared_write_refuses_and_leaves_nothing_to_execute` | `crates/repark-core/src/dialect/tests.rs:233:49` | `PREPARE p AS DELETE FROM ice.ns.t WHERE id = 2: []` |
| `the_default_dialect_refuses_an_explain_analyze_of_a_subquery_dml` | `crates/repark-core/src/dialect/tests.rs:234:5` | `EXPLAIN ANALYZE DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.s): Error during planning: DELETE with a subquery in its WHERE …` |
| `a_prepared_subquery_dml_refuses_and_leaves_nothing_to_execute` | `crates/repark-core/src/dialect/tests.rs:234:5` | `PREPARE p AS DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.s): Error during planning: DELETE with a subquery in its WHERE clause …` |
| `an_explain_analyze_of_a_write_refuses_and_writes_nothing` | `crates/repark-core/src/dialect/tests.rs:233:49` | `EXPLAIN ANALYZE DELETE FROM ice.ns.t WHERE id = 2: [RecordBatch { schema: Schema { fields: [Field { name: "plan_type", data_type: Utf8 }, …` |
| `the_ansi_door_refuses_a_prepared_write_and_leaves_nothing_to_execute` | `crates/repark-sql/tests/ansi_explain_dml_write.rs:111:53` | `PREPARE p AS DELETE FROM ice.ns.t WHERE id = 2: []` |
| `the_ansi_door_refuses_an_explain_analyze_of_a_write` | `crates/repark-sql/tests/ansi_explain_dml_write.rs:111:53` | `EXPLAIN ANALYZE DELETE FROM ice.ns.t WHERE id = 2: [RecordBatch { schema: Schema { fields: [Field { name: "plan_type", data_type: Utf8 }, …` |
| `encrypted_table_explain_analyze_insert_refuses` | `crates/repark-spark/src/tests/enc_1.rs:810:5` | `External error: Table sales.enc carries property 'encryption.key-id': RePark has no table encryption and refuses to write plaintext into a …` |
| `a_wrapped_statement_that_does_not_supply_the_column_meets_the_wrapper_refusal` | `crates/repark-spark/src/tests/nested_ns_routes.rs:444:55` | `EXPLAIN ANALYZE INSERT INTO ice.sales.t (id, k) VALUES (7, 0): ` (it answered) |
| `a_wrapped_write_to_a_table_with_no_such_leaf_is_not_gated` | `crates/repark-spark/src/tests/nested_ns_routes.rs:444:55` | `EXPLAIN ANALYZE INSERT INTO ice.sales.p SELECT 5, named_struct('v', TIMESTAMP '2026-01-02 03:04:05.123456+00:00', 'n', 1), 0:` |
| `a_prepared_write_of_an_instant_refuses_in_a_utc_session_too` | `crates/repark-spark/src/tests/ntz_nested_refuse_routes.rs:228:57` | `refused: ` (the `PREPARE walled` answered `Ok(())`) |
| `a_plain_predicate_prepare_refuses_on_the_router_door` | `crates/repark-spark/src/tests/wrapped_subquery_dml.rs:104:10` | `PREPARE p AS DELETE FROM ice.sales.t WHERE id = 2: []` |
| `a_prepared_subquery_delete_refuses_on_the_router_door` | `crates/repark-spark/src/tests/wrapped_subquery_dml.rs:105:5` | `PREPARE p AS DELETE FROM ice.sales.t WHERE id IN (SELECT id FROM ice.sales.s): Error during planning: DELETE with a subquery in its WHERE …` |
| `a_prepared_explain_analyze_subquery_delete_refuses_on_the_router_door` | `crates/repark-spark/src/tests/wrapped_subquery_dml.rs:105:5` | `PREPARE p AS EXPLAIN ANALYZE DELETE FROM ice.sales.t WHERE id IN (SELECT id FROM ice.sales.s): Error during planning: DELETE with a …` |
| `a_prepared_subquery_update_refuses_on_the_router_door` | `crates/repark-spark/src/tests/wrapped_subquery_dml.rs:105:5` | `PREPARE p AS UPDATE ice.sales.t SET v = 5 WHERE id IN (SELECT id FROM ice.sales.s): Error during planning: UPDATE with a subquery in its …` |
| `a_prepared_write_refuses_on_the_router_door` | `crates/repark-spark/src/tests/wrapped_subquery_dml.rs:104:10` | `PREPARE p AS DELETE FROM ice.sales.t WHERE id = 2: []` |
| `an_explain_analyze_of_a_write_refuses_on_the_router_door` | `crates/repark-spark/src/tests/wrapped_subquery_dml.rs:104:10` | `EXPLAIN ANALYZE DELETE FROM ice.sales.t WHERE id = 2: [RecordBatch { schema: Schema { fields: [Field { name: "plan_type", data_type: Utf8 …` |
| `a_prepared_write_with_a_parameter_that_supplies_the_column_refuses_in_every_zone` | `crates/repark-spark/tests/ntz_nested_refuse_casts.rs:454:5` | `assertion left == right failed`: the `wrong` list holds the three prepared controls, which answered, and their stored walls |

### 1.3 Existing pins this unit flips, by ruling

- **Refusal to refusal, text only (the seam runs before 106's walk):** repark-core
  `the_default_dialect_refuses_an_explain_analyze_of_a_subquery_dml`,
  `a_prepared_subquery_dml_refuses_and_leaves_nothing_to_execute`,
  `only_explain_analyze_reaches_the_statement_it_wraps`; router
  `a_prepared_subquery_delete_refuses_on_the_router_door`,
  `a_prepared_subquery_update_refuses_on_the_router_door`,
  `a_prepared_explain_analyze_subquery_delete_refuses_on_the_router_door`; Python P-E3, P-E4,
  P-P1 to P-P4, P-S1 to P-S3; `enc_1.rs::encrypted_table_explain_analyze_insert_refuses` (the ENC-1
  text gives way to the ruling-140 text; ruling 141).
- **Answer to refusal, ruling 138:** `a_plain_predicate_prepare_still_runs_on_the_router_door`,
  now `a_plain_predicate_prepare_refuses_on_the_router_door`; the Spark door's
  `test_a_plain_predicate_prepare_still_runs`, now `test_a_plain_predicate_prepare_refuses`.
- **Answer to refusal, ruling 136:** the `EXPLAIN ANALYZE UPDATE` and `DELETE` cells of
  `test_write_body_carriers_1.py::test_wrappers_keep_running_their_write`, now
  `test_wrappers_refuse_their_write`.
- **Answer to refusal, ruling 141 (the six `INSERT` tests):**
  1. `nested_ns_routes.rs::a_wrapped_statement_that_does_not_supply_the_column_still_runs`, now
     `…_meets_the_wrapper_refusal` (its `EXPLAIN ANALYZE UPDATE` cell is ruling 136);
  2. `nested_ns_routes.rs::a_wrapped_write_to_a_table_with_no_such_leaf_is_not_gated` (the
     nested gate still lets each through; the wrapper refusal follows, never `NOT_YET`);
  3. `ntz_nested_refuse_routes.rs::a_prepared_write_of_an_instant_refuses_in_a_utc_session_too`
     (the `walled` control);
  4. `ntz_nested_refuse_casts.rs::a_prepared_write_with_a_parameter_that_supplies_the_column_refuses_in_every_zone`
     (the `keyed`, `fixed` and `walled` controls);
  5. `enc_1.rs::encrypted_table_explain_analyze_insert_refuses` (text);
  6. `test_write_body_carriers_1.py::test_wrappers_refuse_their_write` (the `insert` and
     `overwrite` cells under both wrappers, and the `merge` cell, which refused with
     `UnsupportedOperationException` and now with the ruling-140 text).
  Their red lines are in §1.1 and §1.2.
- **Docstrings (ruling 143), every one touched:**
  - `test_repark_sql_update_subquery_where_1.py`, module docstring: the paragraph "Owner ruling
    131 (b): the refusal also fires inside ``EXPLAIN ANALYZE``, which executes the statement it
    wraps." deleted (the G3-E8 refusal no longer fires there; the wrapper refusal does).
  - the same file, `test_a_prepared_subquery_predicate_refuses`: its one-sentence docstring
    ("PREPARE refuses with the G3-E8 text, …") deleted, the docstring with it.
  - `test_repark_sql_update_subquery_where_1_spark_door.py`, module docstring: the sentences
    "``PREPARE`` of a subquery-predicate ``DELETE`` or ``UPDATE``, alone or around ``EXPLAIN
    ANALYZE``, refuses with the G3-E8 text, so ``EXECUTE`` finds nothing to run; on main it
    emptied the table or rewrote every row." and "The bare ``id IN (SELECT ...)`` forms still
    change exactly the matching rows on RePark's own executor, a ``PREPARE`` with a plain
    predicate still runs, and a plain ``EXPLAIN`` answers its plan." deleted.
  - the same file: the docstrings of `test_a_prepared_subquery_predicate_refuses` and of the
    flipped plain-`PREPARE` test deleted; `assert_g3e8` and `G3E8_PARTS`, unused after the flip,
    deleted with the helper's docstring.
  - `test_write_body_carriers_1.py`, module docstring: the sentence "The 14 wrapper cells and the
    5 ANSI cells pin main's measured answers, and the neighbour group pins the CTAS shapes that
    keep answering." deleted; the flipped test's docstring ("EXPLAIN ANALYZE and PREPARE/EXECUTE
    answer as main answers them.") deleted.
  No docstring gained text and no line was reworded; no presence gate covers a test file.

### 1.4 The planner census, main's side (ruling 144)

`test_ice_tsns_type_planner_1.py` runs the three wrapper routes through `doors.measure` for 450
census cells (150 per route, 30 groups). A scratch replica of `_census_run` restricted to those
routes, run on main's module above: `explain_analyze_insert` 150 answered; `explain_analyze_update`
120 answered, 30 errored; `prepare_execute_insert` 150 answered. `ice_tsns_type_planner_1_main.json`
(the harness's record of an older main) lists 175 of the 450 as answered (65, 45 and 65). The head
refuses every statement on these routes, so the harness fails each cell the head makes red or
lost; the exact set is measured on the head's module in step 4 and the dated rows land there,
with the count in §4.

## 2. The shared refusal (step 2)

`crates/repark-iceberg/src/write/statement_writes.rs` (35 → 78 lines) gains
`pub fn refuse_wrapped_write(statement: &DFStatement) -> Result<()>` beside `executed_writes`,
which is unchanged. `#[allow(clippy::missing_errors_doc)]` as `predicate_dml/subquery_where.rs`
carries it: the comment ban leaves no `# Errors` line. The two texts are `const`s with the
prepared name substituted; the error is `DataFusionError::Plan`. No comment line. Its unit tests,
`write::statement_writes::tests` (new `statement_writes/tests.rs` and `map.md`):
`cargo test -p repark-iceberg --lib statement_writes`: **4 passed**, 0 failed.

## 3. The three call sites (step 3)

One line each, on the parsed `DFStatement` the door already holds, before `execute_logical_plan`:

| Door | File | Place | Lines |
|---|---|---|---|
| `repark.sql()` | `crates/repark-core/src/dialect.rs` | `refuse_dml_subquery_where`, after the parse, before 106's `EXPLAIN` peel and walk | 171 → 172 |
| ANSI | `crates/repark-sql/src/router.rs` | `execute_time_travelled`, after `sql_to_statement`, before the `RESET` arm | 365 → 366 |
| router API and Spark SQL session | `crates/repark-spark/src/spark_ast.rs` | `execute_passthrough_inner`, after `sql_to_statement`, before the `DfStatement` match | 830 → 831 |

`normalize.rs` is untouched; `make check-rust-file-size` reports no ceiling change.

**Found at green, by measurement (not a second seam):** on `repark.sql()` the call runs after
`belt.plan`, as 106 placed its walk (106 AT-3: a statement that does not plan keeps its plan
error). DataFusion 54.1 does not plan `MERGE`, so `EXPLAIN ANALYZE MERGE …` and `PREPARE p AS
MERGE …` keep `This feature is not implemented: Unsupported SQL statement: MERGE INTO …` there,
as on main, and write nothing. The ANSI and repark-spark doors call the seam before planning and
give `MERGE` the ruled texts. The `repark.sql()` `MERGE` cells pin the plan refusal and the held
table; the question is in the hand-back (Q-B1).

## 4. Pins green (step 4)

The head release module (`maturin develop --profile ci` on `b8f862ec`'s code; sha256
`11e5bd48809075f1e4a33ff3b6907347c75c96fd90ffe10d85be79231765062e`):

- **Python:** the four files of §1.1: **111 passed**, 0 failed.
- **Rust:** `cargo test -p repark-core --lib dialect` 14 passed; `cargo test -p repark-sql --test
  ansi_explain_dml_write` 3 passed; `cargo test -p repark-spark --lib -- wrapped_subquery_dml
  nested_ns_routes ntz_nested_refuse_routes enc_1` 74 passed; `cargo test -p repark-spark --test
  ntz_nested_refuse_casts` 10 passed; `cargo test -p repark-iceberg --lib statement_writes` 4
  passed.
- **The `repark.sql()` `MERGE` cells (§3):** `an_explain_analyze_of_a_write_refuses_and_writes_nothing`
  and `a_prepared_write_refuses_and_leaves_nothing_to_execute` in `dialect/tests.rs`, and the
  `repark_sql-merge_*` cells of `test_explain_dml_write_1.py`, assert the plan refusal
  (`Unsupported SQL statement: MERGE INTO …`) and the held table on that door; the other doors
  assert the ruled texts.

### 4.1 The planner census (ruling 144)

The scratch replica of §1.4 on the head's module: all 450 cells on the three routes refuse (300
with the `EXPLAIN ANALYZE` text, 150 with the `PREPARE` text). 350 of them are red or lost for
the harness (175 lost, the cells `ice_tsns_type_planner_1_main.json` records as answered; 175 more
red, where main's recorded twin answered): `explain_analyze_insert` 125, `explain_analyze_update`
100, `prepare_execute_insert` 125. The other 100 errored on the recorded main with a recorded
twin that errored too, and stay green. One dated row per red or lost cell lands in
`ice_tsns_type_planner_1_dated.json`: date 2026-10-21, class `ruled refusal`, ruling 136 for the
200 `EXPLAIN ANALYZE` cells and 138 for the 150 `PREPARE` cells, owner text naming this unit; 290
rows before, 640 after. No harness or route change. `pytest
python/repark/tests/timestamps/test_ice_tsns_type_planner_1.py -n 8` on the head's module: **60
passed** in 249 s.

## 5. Revert check and mutants (step 5)

Each change applied alone to the head `57031982`, the named suites run, the file restored
(`git status` clean after). No variant survived.

| Variant | Suite | Result | Killed by |
|---|---|---|---|
| R1: the `dialect.rs` call removed | `cargo test -p repark-core --lib dialect` | 5 failed, 9 passed | `an_explain_analyze_of_a_write_refuses_and_writes_nothing`, `a_prepared_write_refuses_and_leaves_nothing_to_execute`, `a_prepared_subquery_dml_refuses_and_leaves_nothing_to_execute`, `the_default_dialect_refuses_an_explain_analyze_of_a_subquery_dml`, `only_explain_analyze_reaches_the_statement_it_wraps` |
| R2: the ANSI `router.rs` call removed | `cargo test -p repark-sql --test ansi_explain_dml_write` | 2 failed, 1 passed | `the_ansi_door_refuses_an_explain_analyze_of_a_write`, `the_ansi_door_refuses_a_prepared_write_and_leaves_nothing_to_execute` |
| R3: the `spark_ast.rs` call removed | `cargo test -p repark-spark --lib wrapped_subquery_dml` | 6 failed, 4 passed | `an_explain_analyze_of_a_write_refuses_on_the_router_door`, `a_prepared_write_refuses_on_the_router_door`, `a_plain_predicate_prepare_refuses_on_the_router_door`, the three `a_prepared_…_refuses_on_the_router_door` pins |
| M1: the `Explain` look-through removed (an `EXPLAIN ANALYZE` answers `Ok`) | `statement_writes` unit tests; ANSI pins | 2 failed; 1 failed | `an_explain_analyze_of_a_write_refuses_with_its_text`, `neither_refusal_names_a_data_deleting_action`; `the_ansi_door_refuses_an_explain_analyze_of_a_write` |
| M2: the `Prepare` arm removed | the same | 1 failed; 1 failed | `a_prepare_of_a_write_refuses_with_its_text`; `the_ansi_door_refuses_a_prepared_write_and_leaves_nothing_to_execute` |
| M3: a plain `EXPLAIN` refused | the same | 1 failed; 3 failed | `a_query_a_plain_explain_and_a_bare_statement_answer_nothing`; all three ANSI pins (the seed's `EXPLAIN SELECT` guard and the controls) |
| M4: only `DELETE` and `UPDATE` refused (ruling 141 undone) | the same | 2 failed; 2 failed | `an_explain_analyze_of_a_write_refuses_with_its_text`, `a_prepare_of_a_write_refuses_with_its_text`; both ANSI refusal pins (their `INSERT` cells) |
| M5: the `analyzed` arm removed | the same | 2 failed; 1 failed | `an_explain_analyze_of_a_write_refuses_with_its_text`, `neither_refusal_names_a_data_deleting_action`; `the_ansi_door_refuses_an_explain_analyze_of_a_write` |

The Python doors' revert is main's module itself, whose product code is main's byte for byte:
the 50 pins of §1.1 are red on it and green on the head's module (§4).

## 6. Never worse (step 6)

106's §13.6 census shape, widened to the four verbs and four doors (evidence:
[census](../../wo/explain-dml-write-1/evidence/map.md)). 106 cells per door; every seed is an
`IcebergTableScan` on main and head (424 of 424). Main is `c91f69c5`'s code (module `60286bb3…`
for the Python doors; the three call sites restored to `origin/main` for the Rust doors); head is
`57031982` (module `11e5bd48…`).

| Door | Unchanged | Wrote on main, ruled refusal on head | Refused on main, ruled refusal on head | Outside the class, or the table not held |
|---|---|---|---|---|
| `repark.sql()` | 52 | 15 | 39 | 0 |
| ANSI door | 48 | 53 | 5 | 0 |
| Spark SQL session | 48 | 33 | 25 | 0 |
| router API | 48 | 33 | 25 | 0 |

- **Every changed cell** is a write under `EXPLAIN ANALYZE` or `PREPARE`: a row of the card's
  table under rulings 141 and 142 (any predicate or none, `VERBOSE`, parameterised, `PREPARE`
  around `EXPLAIN ANALYZE`, `EXPLAIN ANALYZE` around `PREPARE`, CTE-carried, `INSERT`, `MERGE`).
  On the head each refuses with the ruled text of its outermost wrapper and the table holds
  (rows as seeded, one snapshot); after a refused `PREPARE`, `EXECUTE p` answers `Prepared
  statement 'p' does not exist`. No answer is lost outside the table.
- **"Wrote on main"** on the ANSI door includes every subquery form (no refusal there before) and
  `PREPARE`/`EXECUTE`, which emptied the table or rewrote every row (106's critical class); on
  the repark-spark doors the `EXPLAIN ANALYZE` forms of every predicate and the plain-predicate
  `PREPARE` forms; on `repark.sql()` the plain-predicate and `INSERT` forms (106 refused the
  subquery ones with the G3-E8 text, now the wrapper's text).
- **Unchanged, among them:** every bare statement (the Spark SQL session and the router keep the
  matching rows for the `IN`, `NOT IN`, `EXISTS`, `NOT EXISTS` and correlated `IN` forms; the ANSI
  door its identity forms; `repark.sql()` its G3-E8 refusals), every plain `EXPLAIN` (plan, no
  write), `PREPARE` of a query and its `EXECUTE`, `EXPLAIN [ANALYZE] SELECT`, the bare `INSERT …
  SELECT … IN`, the bare `MERGE` (RePark's executor on three doors, DataFusion's plan refusal on
  `repark.sql()`), nested `EXPLAIN EXPLAIN ANALYZE` and the CTAS.
- **`repark.sql()`'s `MERGE`:** `EXPLAIN ANALYZE MERGE` and `PREPARE … AS MERGE` are unchanged
  there (the plan refusal, §3), so they count as unchanged; the other doors moved them from that
  refusal to the ruled text.

The card's measured table gains the `INSERT` and `MERGE` rows (ruling 141), from these cells:

| Statement | `repark.sql()` main → head | ANSI door main → head | Spark SQL session main → head | router API main → head |
|---|---|---|---|---|
| `EXPLAIN ANALYZE INSERT INTO t SELECT … WHERE id IN (SELECT …)` | 2 rows inserted, 1 → 2 → refused, 1 → 1 | the same | the same | the same |
| `EXPLAIN ANALYZE INSERT INTO t VALUES (5, 50)` | 1 row inserted → refused | the same | the same | the same |
| `PREPARE p AS INSERT … ; EXECUTE p` | 2 rows inserted at `EXECUTE` → refused at `PREPARE`, nothing to execute | the same | the same | the same |
| `EXPLAIN ANALYZE MERGE INTO t USING (… IN (SELECT …)) …` | DataFusion's plan refusal → the same | plan refusal → ruled refusal | plan refusal → ruled refusal | plan refusal → ruled refusal |
| `PREPARE p AS MERGE …; EXECUTE p` | plan refusal → the same | plan refusal → ruled refusal | plan refusal → ruled refusal | plan refusal → ruled refusal |
| `EXPLAIN INSERT …` (control) | plan, no write → the same | the same | the same | the same |
| bare `INSERT … SELECT … IN (SELECT …)` (control) | 2 rows → the same | the same | the same | the same |

## 7. Records (step 7)

- **Card** [EXPLAIN-DML-WRITE-1](../../roadmap/mid-term/explain-dml-write-1-card-2026-10-16.md):
  a dated status line (built, PR #1137, verifier pending) and a section for rulings 140 to 144
  with what holds, the two texts, and the `INSERT` and `MERGE` rows of the measured table
  (ruling 141), from §6.
- **1.5.6 notes** (`task/roadmap/mid-term/v1-5-6-release-notes-draft.md`): the EXPLAIN-DML-WRITE-1
  known-issue line gains a dated "built, verifier pending" note; the 106 line gains the measured
  VF-2 wording from 106 ledger §13.6 and the wrapper text.
- **Registry** (`docs/spark-sql-iceberg-parity.md`): the REPARK-SQL-UPDATE-SUBQUERY-WHERE-1 row,
  the one that cites this card, gains the dated wrapper-refusal line with its pins.
- **Maps:** every touched directory's `map.md` in the commit that touched it (the test and
  product directories, `task/wo/explain-dml-write-1/` and its `evidence/`, `task/ledgers/staging/`,
  `task/roadmap/mid-term/`, `docs/`, `python/repark/tests/timestamps/`).

## 8. Gates

On the head before the records commit (`7ab3b8b4`; the records commit adds documents only):

| Command | Result |
|---|---|
| `cargo test --no-fail-fast -p repark-iceberg` | exit 0, 1026 passed, 62 s |
| `cargo test --no-fail-fast -p repark-core` | exit 0, 1541 passed, 2 ignored, 129 s |
| `cargo test --no-fail-fast -p repark-sql` | exit 0, 579 passed, 4 ignored, 218 s |
| `cargo test --no-fail-fast -p repark-spark` | exit 0, 2935 passed, 5 ignored, 458 s |
| `pytest python/repark/tests/writes -k "subquery_where or explain_dml"` (head module) | 101 passed |
| `test_write_body_carriers_1`, `test_df_explain_1`, `test_pa_local_fs_policy_1`, `test_ice_tsns_merge_wall_1`, `test_ice_ntz_nested_refuse_1`, `test_ice_tsns_coercion_1`, `test_dataframe_x3_census` (head module) | 497 passed, 1 skipped (live Spark) |
| `test_ice_tsns_type_planner_1.py` (head module) | 60 passed, 249 s |
| `make ci` (step 4 head `57031982`) | exit 0, 512 s; it carries `rust-clippy`, `rust-panic-ban`, `check-rust-file-size`, `check-crate-dag`, the docs and ledger gates, `check-model-ban` |
| the comment gate of the lane brief, `git diff origin/main...HEAD` | prints nothing |

`make verify` and `make preflight` run the whole workspace suite and the whole facade suite; under
AGENTS.md "Delegated-agent standing rules" (owner, 2026-09-18) the lane runs the scoped gate above
and CI runs the rest. `make ci` on the final commit is in the hand-back.

```
COVERAGE_ATTESTATION:
  pr_unit: explain-dml-write-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause is pinned on every door it names, red on main and green on the head;
        the four verbs under both wrappers, plain and subquery predicates, Iceberg seeds behind a guard.
      artifacts: [python/repark/tests/writes/test_explain_dml_write_1.py, crates/repark-sql/tests/ansi_explain_dml_write.rs, crates/repark-spark/src/tests/wrapped_subquery_dml.rs, crates/repark-core/src/dialect/tests.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Boundary spellings - VERBOSE, nested EXPLAIN in both orders, EXPLAIN ANALYZE
        around PREPARE, PREPARE around EXPLAIN [ANALYZE], parameterised PREPARE, a WHERE-less
        DELETE, a CTE-carried DELETE, COPY under EXPLAIN ANALYZE, PREPARE of a view.
      artifacts: [crates/repark-iceberg/src/write/statement_writes/tests.rs, task/wo/explain-dml-write-1/evidence/census-cells.txt]
      fix_round_1: A trailing semicolon, a leading comment and lower case are pinned on every Rust
        door (C-014); on repark.sql() a wrapped write that does not plan, the CTE-carried one
        included, keeps DataFusion's error, ruled as built (C-016).
    - id: AT-3
      status: ATTACKED
      evidence: A refused statement writes nothing - rows, snapshot count and warehouse files read
        before and after; a refused PREPARE is never stored, so EXECUTE finds nothing.
      artifacts: [python/repark/tests/writes/test_explain_dml_write_1.py]
    - id: AT-4
      status: ATTACKED
      evidence: The refusal is a pure function of the parsed statement; it holds no state and runs
        before execute_logical_plan on every door.
      artifacts: [crates/repark-iceberg/src/write/statement_writes.rs]
    - id: AT-5
      status: ATTACKED
      evidence: Memory catalogs under temporary directories only; no network, no credentials.
        The texts name no data-deleting action (ruling 76 (a)), pinned.
      artifacts: [crates/repark-iceberg/src/write/statement_writes/tests.rs]
    - id: AT-6
      status: ATTACKED
      evidence: The change removes a write path (wrappers that wrote, and with a subquery
        emptied or rewrote the table); the census lists every changed answer on four doors.
      artifacts: [task/wo/explain-dml-write-1/evidence/map.md]
    - id: AT-7
      status: N/A
      justification: The refusal walks the parsed statement once more per SQL call, bounded by
        the parse already done; no wall-clock claim is made.
    - id: AT-8
      status: ATTACKED
      evidence: No crate edge, no Cargo.toml, Cargo.lock or .github change; the error is the
        DataFusionError::Plan the G3-E8 refusal returns; the facade maps it to AnalysisException.
      artifacts: [crates/repark-iceberg/src/write/statement_writes.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The texts say what is refused, why, and what still works; the card, the 1.5.6
        notes, the registry row and the maps record the rule.
      artifacts: [task/roadmap/mid-term/explain-dml-write-1-card-2026-10-16.md]
    - id: AT-10
      status: ATTACKED
      evidence: Three call-site reverts and five mutants each red named pins (section 5).
      artifacts: [crates/repark-iceberg/src/write/statement_writes/tests.rs, crates/repark-sql/tests/ansi_explain_dml_write.rs]
  complete: true
```

## Hand-back

`Model: claude-opus-5-5`. `risk_tier: standard`. Verifier: pending. Owner merges.

## 9. Fix round 1 (2026-10-21, after the verifier's FAIL at `eccd920a`)

The verifier (fresh Opus 5.5, hostile) found the product right on every door and failed the unit
on one S1 and three S2 findings. The orchestrator ruled each item under the owner's grant of
2026-10-21 20:35 UTC. One commit per item.

### 9.1 S1-1: the ruling-142 spellings on every Rust door

The verifier's mutant `if !<sql>.trim_end().ends_with(';') { refuse_wrapped_write(..)?; }` at the
three call sites survived every door pin, and with it `EXPLAIN ANALYZE DELETE … WHERE id = 2;`
deleted row 2 on the ANSI door, the router API and the native door. Each door's own test file
gains one pin over six spellings: a trailing `;`, a leading `--` comment line and lower case, each
under `EXPLAIN ANALYZE` and under `PREPARE`, over `DELETE`, `UPDATE` and `INSERT`; each asserts the
ruled text, that `EXECUTE p` finds nothing, and the unchanged table:
`every_spelling_of_a_wrapped_write_refuses_and_writes_nothing` (`repark-core` `dialect/tests.rs`),
`the_ansi_door_refuses_every_spelling_of_a_wrapped_write` (`ansi_explain_dml_write.rs`),
`every_spelling_of_a_wrapped_write_refuses_on_the_router_door` (`wrapped_subquery_dml.rs`). Green
on the head: 15, 4 and 11 passed.

The kill, with the mutant applied at all three sites at once, then restored (evidence
[mutant-trailing-semicolon.txt](../../wo/explain-dml-write-1/evidence/mutant-trailing-semicolon.txt)):

| Door | Suite | Red under the mutant |
|---|---|---|
| `repark.sql()` (native) | `cargo test -p repark-core --lib dialect` | 1 failed, 14 passed: `every_spelling_of_a_wrapped_write_refuses_and_writes_nothing` panicked at `dialect/tests.rs:233:49`: `EXPLAIN ANALYZE DELETE FROM ice.ns.t WHERE id = 2;: [RecordBatch { … plan_type … plan …` (the statement answered its plan) |
| ANSI | `cargo test -p repark-sql --test ansi_explain_dml_write` | 1 failed, 3 passed: `the_ansi_door_refuses_every_spelling_of_a_wrapped_write` panicked at `ansi_explain_dml_write.rs:111:53`: `EXPLAIN ANALYZE DELETE FROM ice.ns.t WHERE id = 2;: [RecordBatch { …` |
| router API | `cargo test -p repark-spark --lib wrapped_subquery_dml` | 1 failed, 10 passed: `every_spelling_of_a_wrapped_write_refuses_on_the_router_door` panicked at `wrapped_subquery_dml.rs:104:10`: `EXPLAIN ANALYZE DELETE FROM ice.sales.t WHERE id = 2;: [RecordBatch { …` |

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-014 | Fix round 1, S1-1 (2026-10-21): on `repark.sql()`, the ANSI door and the router API, a wrapped write spelled with a trailing `;`, a leading `--` comment or in lower case refuses with the ruled text and holds the table; with the refusal skipped on a trailing `;` at all three call sites, each door's pins go red. | The three `every_spelling…` pins; the mutant run. | **PROVEN** | §9.1; the evidence file. |

### 9.2 S2-3: the prepared name as written

Ruled (orchestrator, under the owner's grant, 2026-10-21): `{name}` renders the identifier as the
user wrote it, quoting included, the `Ident` display the sketch §6 named. `statement_writes.rs`'s
`prepare_refusal` formats `{name}` (the `Ident`) instead of `name.value`. An unquoted name renders
the same bytes as before, so no existing pin changes. `a_prepare_of_a_write_refuses_with_its_text`
gains `PREPARE "My P" AS DELETE …`, whose text carries ``PREPARE `"My P"` is refused``, and
`PREPARE MyP …` (``PREPARE `MyP` ``, the case as written). With `name.value` restored the pin is
red: `tests.rs:64:5`: `Error during planning: PREPARE `My P` is refused: …`.
`cargo test -p repark-iceberg --lib statement_writes`: 4 passed.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-015 | Fix round 1, S2-3 (2026-10-21): the `PREPARE` refusal names the prepared statement as written, quoting included; an unquoted name renders as before. | `a_prepare_of_a_write_refuses_with_its_text`; the revert to `name.value`. | **PROVEN** | §9.2. |

### 9.3 S2-1: correction to §4.1 (2026-10-21)

| Date | Where | As written | Correct |
|---|---|---|---|
| 2026-10-21 | §4.1, the dated-row split | "ruling 136 for the 200 `EXPLAIN ANALYZE` cells and 138 for the 150 `PREPARE` cells" | 225 rows cite ruling 136 (`explain_analyze_insert` 125 + `explain_analyze_update` 100) and 125 cite ruling 138 (`prepare_execute_insert` 125); the total of 350 and the per-route counts stand. The rows in `ice_tsns_type_planner_1_dated.json` were right; only the sentence was wrong (verifier finding S2-1). |

### 9.4 S2-2 and Q-B1: the plan-first class on `repark.sql()`

Ruled (orchestrator, under the owner's grant, 2026-10-21): **accepted as built.** On
`repark.sql()` the call stays after `belt.plan` (106's AT-3 order holds), so every wrapped write
that fails DataFusion planning keeps DataFusion's own error and writes nothing: `MERGE`, a
CTE-carried `DELETE`, `UPDATE` or `INSERT` and a DML-in-CTE query, a missing table or column,
`EXPLAIN ANALYZE … FORMAT x`, `DESCRIBE ANALYZE`. The verifier measured 16 such cells of 91 on
that door, each the same error on main. The ANSI door and both repark-spark doors give each the
ruled text. This unit's own census shows the CTE case on `repark.sql()`
(`census-repark-sql-head.txt`, `explain_analyze_cte_delete` and `prepare_cte_delete`: `Query …
not implemented yet`, the table held). The card, the registry line and the 1.5.6 notes now name
the whole class; the card carries the follow-up "moving the native-door call before planning
would move 16 refusal texts and no write; unscheduled".

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-016 | Fix round 1, S2-2 (2026-10-21; C-005 stays as written): on `repark.sql()` a CTE-carried wrapped write, and every other wrapped write DataFusion cannot plan, refuses with DataFusion's plan error, not the ruled text, because that door plans before the refusal runs (106's AT-3 order, ruled to hold); nothing is written; the other three doors give the ruled text. | The `repark.sql()` census cells `explain_analyze_cte_delete`, `prepare_cte_delete`, `explain_analyze_merge`, `prepare_merge`; the `MERGE` cells of `dialect/tests.rs` and `test_explain_dml_write_1.py`. | **PROVEN** | §3, §6, §9.4; the verifier's 16 cells. |

Moved to completed on 2026-10-30 by the staging-ledger lifecycle round (governance step 5): the unit merged on main as `18626942` (2026-10-21, PR #1138); clauses as written.

# Unit ledger — REPARK-SQL-UPDATE-SUBQUERY-WHERE-1 · `repark.sql()` refuses a subquery in `UPDATE` or `DELETE` `WHERE` (ruling 106, the stopgap)

## Round 2 (2026-10-16): build the stopgap B2

**Date:** 2026-10-16 · **Branch:** `fix/repark-sql-update-subquery-where-1` · **Base:** `origin/main`
(`0f074aff`) · **Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **Rubric:** STANDARD.
`risk_tier: standard`. **Registry:** `REPARK-SQL-UPDATE-SUBQUERY-WHERE-1` (dated row 2026-10-15,
stopgap 2026-10-16). **Card:**
[REPARK-SQL-UPDATE-SUBQUERY-WHERE-1](../../roadmap/mid-term/repark-sql-update-subquery-where-1-card-2026-10-15.md).
**Sketch (round 1):**
[sketch.md](../../wo/repark-sql-update-subquery-where-1/sketch.md) (`6cb6c310`).

**Retires:** this ledger moves to `../completed/` in the unit's last commit; routing stays on the
card after it.

**Why.** Through `repark.sql()` (the `DataFusionDialect` door), an `UPDATE` or `DELETE` whose
`WHERE` holds a subquery wrote every row or emptied the table and reported success; Spark 4.1.2
writes only the matching rows. Round 1 measured it and recommended the stopgap B2: one home for
the ANSI door's G3-E8 refusal, which `repark.sql()` calls with no identity exemption.

**Rulings this round runs under** (round 2 brief, provisional on the sketch's leans; the owner
sees them and may overrule): Q1 yes, 106 builds the stopgap B2 and leaves routing on the card;
Q4 yes, the stopgap covers `DELETE`, and the card, the registry row and the 1.5.6 note name it
with a dated line; Q5, `repark.sql()` raises the G3-E8 text verbatim, one text for the family;
Q6 yes, `repark.sql()` refuses every subquery-predicate `UPDATE` and `DELETE`, the five identity
forms included, until routing lands; Q7 no, the Spark SQL door's copy
(`crates/repark-spark/src/normalize.rs` 311-436) stays this round, and a later tidy unit is one
line in the card. Q2 (routing's mechanism) and Q3 (how N1 to N7 close) are with the owner and do
not block this round.

**Not in this unit:** routing `repark.sql()` through the ANSI door; the Spark SQL door's copy;
`local_fs_policy.rs` and the native door's builder line (U-5, #1075); `Cargo.toml`, `Cargo.lock`,
`.github`; tier-2 live runs.

## PROPOSITION LEDGER — REPARK-SQL-UPDATE-SUBQUERY-WHERE-1 — 2026-10-16

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | On `repark.sql()` every `UPDATE` whose `WHERE` holds a subquery raises the G3-E8 text verbatim, the rows read back as seeded and no file appears under the warehouse: eight predicate forms on an Iceberg table, the card's unqualified spelling, an in-memory table. | `test_repark_sql_update_subquery_where_1.py` P-U1 to P-U10. | **PROVEN** (2026-10-17: re-proven on Iceberg tables behind the guard, §12.3 and §12.5; rounds 1 to 3 ran on in-memory tables, finding V106-F4) | Red on main (§1), green on the head module (§3). |
| C-002 | The same holds for `DELETE`: the eight forms, the card's spelling and an in-memory table refuse with the G3-E8 text and write nothing. | P-D1 to P-D10. | **PROVEN** (2026-10-17: re-proven on Iceberg tables behind the guard, §12.3 and §12.5; P-D10 keeps its labelled in-memory table) | Red on main (§1: P-D1 `DID NOT RAISE`), green on the head module (§3). |
| C-003 | The stopgap loses exactly the answers in its class, and only two of them were right: the two shapes main answered with Spark's rows by coincidence refuse, and every other statement main's `repark.sql()` answers keeps its answer. | P-C1, P-C2; the 138-statement census of §5a against Spark 4.1.2. | **PROVEN** | §5a: 20 lost answers, 18 wrong on main, 2 coincidence shapes, no loss outside the class. |
| C-004 | Statements outside the refused class answer on `repark.sql()` as on main: a plain predicate and a literal `IN` list for both verbs, and an `INSERT … SELECT` that reads an `IN` subquery. | The five control cells of the test file. | **PROVEN** | Green on main and on the head (§1, §3). |
| C-005 | `DataFusionDialect` (every session built without a dialect) refuses the class in Rust, the rows held. | `the_default_dialect_refuses_a_subquery_in_update_or_delete_where` in `crates/repark-core/src/dialect/tests.rs`. | **PROVEN** | Red on main (§1), green on the head (§3). |
| C-006 | The ANSI door's behaviour is byte-identical: its five identity forms answer Spark's rows, its G3-E8 unit and end-to-end pins keep their names and assertions, and its whole suite is green before and after. | `ansi_subquery_where_identity.rs`; `guards/tests.rs` and `guards/ansi_door.rs`; `cargo test -p repark-sql` on main and head. | **PROVEN** | §2: identity pin green on main and head; the five unit call sites gain the `true` argument only; §5b: 555 green on main, 556 on head (one added), none missing or worse. |
| C-007 | The refusal lives once for the ANSI door and `repark.sql()`, in `repark_iceberg::write::predicate_dml::subquery_where`; the ANSI router passes the identity exemption at both call sites, `DataFusionDialect` none; no crate edge; the Spark SQL door's copy is unchanged; moved code carries no comment. | The module and its four unit tests; `router.rs`; `dialect.rs`; `make check-crate-dag`; the comment gate. | **PROVEN** | §2, §3, §7. |
| C-008 | The pins catch the defect and the mechanism: with `6cb6c310`'s `dialect.rs` the 22 pins are red; with the ANSI router passing no exemption the identity pin is red; six mutants each die on a named test. | §4 revert checks and §6 mutants. | **PROVEN** | §4, §6. |
| C-009 | Nothing that is green on main is red on the head: the four crates' suites and the facade suite. | `cargo test --no-fail-fast -p repark-iceberg -p repark-core -p repark-spark -p repark-sql` on main's tree and the head; `pytest python/repark/tests -n 8` on the head module. | **PROVEN** | §5b, §5c. |
| C-010 | The card, the registry row and the 1.5.6 note name `DELETE` and the refusal with dated lines; the maps of every touched directory and this ledger land with the unit. | The three records; the map rows. | **PROVEN** | §8. |
| C-011 | The mechanical gates are green and the release module is built from a clean tree at the final commit with its provenance. | §7. | **PROVEN** | §7, §9. |
| C-012 | Owner ruling 131 (b): on `repark.sql()`, `EXPLAIN` and `EXPLAIN ANALYZE` of a subquery-predicate `DELETE` and `UPDATE` refuse with the G3-E8 text, and rows and warehouse files hold after the result is read. | P-E1 to P-E4 in `test_repark_sql_update_subquery_where_1.py`. | **REJECTED** (2026-10-17, owner ruling 135 (a) as the orchestrator read it: a plain `EXPLAIN` answers its plan again, C-020; the `EXPLAIN ANALYZE` half holds as C-020) | §11.2: 4 red on `ff731179`, green on the head and on the merged module. |
| C-013 | `DataFusionDialect` unwraps `EXPLAIN` and `EXPLAIN ANALYZE`, with or without `VERBOSE` or `FORMAT`, at any depth, and refuses the inner statement with no exemption; the change is `dialect.rs` alone, the ANSI door and the shared function are unchanged, and a plain `EXPLAIN` of a DML without a subquery predicate behaves as on main. | `the_default_dialect_refuses_an_explained_subquery_dml`, `the_unwrap_reaches_a_nested_explain`; the census of §11.5. | **REJECTED** (2026-10-17, superseded: only `EXPLAIN ANALYZE` reaches the inner statement, and the refusal reads the moved walk, C-021) | §11.2, §11.5. |
| C-014 | The round-3 pins catch the rule: with `ff731179`'s `dialect.rs` the four pins are red, and four mutants of the unwrap each die on a named test; nothing that answered on `ff731179` stops answering except `EXPLAIN` of a subquery-predicate DML and the merged local-file cells; the four crates and the facade suite are green. | §11.4, §11.5. | **PROVEN** | §11.4, §11.5. |
| C-015 | Owner ruling 126 (a): `origin/main` (#1075) merges without conflict and no rebase, and on the release module built from the clean merge the Q6 cell under the guard, U-1's Q6 cell, U-5's local-file cell and this unit's 31 pins pass. | §11.3. | **PROVEN** | §11.3. |
| C-016 | The records name the round: this ledger's section 11, the card's `EXPLAIN` line, the new card EXPLAIN-DML-WRITE-1 with its map row, and its 1.5.6 known-issue line; the gates are green on the final commit. | §11.6, §11.7. | **PROVEN** | §11.6, §11.7. |
| C-017 | Owner ruling 135 (a): on `repark.sql()`, `PREPARE` of a subquery-predicate `DELETE` or `UPDATE` refuses with the G3-E8 text, alone, parameterised, or around `EXPLAIN ANALYZE`, so `EXECUTE` finds no prepared statement; the Iceberg table's rows, snapshots and warehouse files hold. | P-P1 to P-P4 in `test_repark_sql_update_subquery_where_1.py`; `a_prepared_subquery_dml_refuses_and_leaves_nothing_to_execute` in `crates/repark-core/src/dialect/tests.rs`. | **PROVEN** | Red on the merge `5a51a758` (§12.3), green on the head (§12.5). |
| C-018 | Every unit pin, Python and `repark-core`, seeds its tables through the Iceberg catalog, and one helper per language fails unless the scan is an `IcebergTableScan`; each pin checks the rows and the snapshot count before and after; the helper fails on the round-3 CTAS seed. | `assert_iceberg` in both test files; mutant M5. | **PROVEN** | §12.3 (the guard on the CTAS seed), §12.6 (M5 in both languages). |
| C-019 | The refusal parses with the session's SQL dialect: under `datafusion.sql_parser.dialect = 'MsSQL'` a bracketed subquery-predicate `DELETE` refuses with the G3-E8 text and writes nothing. | P-M1; `a_non_default_parser_dialect_still_refuses`. | **PROVEN** | Green on the merge and the head; red under mutant M4 (§12.6). |
| C-020 | As the orchestrator read ruling 135: a plain `EXPLAIN` of a subquery-predicate `DELETE` or `UPDATE` answers its plan on `repark.sql()` with an Iceberg table's rows and snapshots unchanged, as on main; `EXPLAIN ANALYZE` of either verb refuses with the G3-E8 text. | P-E1 to P-E4; `a_plain_explain_of_a_subquery_dml_answers_its_plan`, `the_default_dialect_refuses_an_explain_analyze_of_a_subquery_dml`, `only_explain_analyze_reaches_the_statement_it_wraps`. | **PROVEN** | §12.3, §12.5. |
| C-021 | The walk lives once: `executed_writes` moved from `repark-spark`'s `router/nested_ns.rs` to `repark_iceberg::write::statement_writes`, gained `DELETE`, and the Spark SQL router calls it with its suite unchanged; `DataFusionDialect` runs the refusal with no exemption on each statement the walk answers and peels DataFusion's own `EXPLAIN` layers under the walk's rule; `explained_statement` is gone; the change is `dialect.rs` and the moved walk, with no crate edge. | The refactor and fix commits. | **PROVEN** | §12.4, §12.5, §12.8. |
| C-022 | The pins catch the rule: with `996f4d5c`'s `dialect.rs` the `PREPARE` pins are red, and five mutants each die on a named test. | §12.6. | **PROVEN** | §12.6. |
| C-023 | Never worse: on Iceberg seeds, against the merge's module, `repark.sql()` loses no answer outside the subquery-predicate class, bare or under a wrapper; the four crates' suites and the facade suite are green; ruling 113's Q6 cells and U-5's local-file cells pass on the head module. | §12.7. | **PROVEN** | §12.7. |
| C-024 | The records say what now holds: this section, the corrected "every" lines (V106-F6), card EXPLAIN-DML-WRITE-1 as ruling 136 rules, the 1.5.6 lines and the map rows; the gates are green. | §12.8, §12.9. | **PROVEN** | §12.8, §12.9. |
| C-025 | Owner ruling 139: on both repark-spark doors (the `SparkDialect` session door and the router API), `PREPARE` of a subquery-predicate `DELETE` or `UPDATE`, alone or around `EXPLAIN ANALYZE`, refuses with the G3-E8 text and `EXECUTE` finds no prepared statement; the Iceberg table's rows and snapshots hold. | `crates/repark-spark/src/tests/wrapped_subquery_dml.rs` (three pins); P-S1 to P-S3 in `test_repark_sql_update_subquery_where_1_spark_door.py`. | **PROVEN** | Red on the merge `e5dc4e4c` (§13.3), green on the head (§13.4). |
| C-026 | On both repark-spark doors the bare `id IN (SELECT …)` `DELETE` and `UPDATE` change exactly the matching rows, a plain-predicate `PREPARE`/`EXECUTE` writes its row, and a plain `EXPLAIN` of the subquery `DELETE` answers its plan and writes nothing, before and after. | The controls in both files. | **PROVEN** | §13.3, §13.4. |
| C-027 | The fix is `normalize.rs` alone: `refuse_dml_subquery_predicate_in_statement` keeps its allow-list exemption for the top-level statement and refuses each `DELETE` and `UPDATE` the round-4 walk returns, with no exemption; the existing CTE pin moves only from DataFusion's NotImplemented to the G3-E8 refusal for its two subquery forms; with the merge's `normalize.rs` the new pins are red, and four mutants each die on a named test. | §13.4, §13.5. | **PROVEN** | §13.4, §13.5. |
| C-028 | Never worse on the repark-spark doors: on Iceberg seeds against the merge's module every bare subquery-predicate shape that executes keeps its rows, and nothing outside the wrapped subquery class changes; the four crates' suites and the facade suite are green; the records say what holds. | §13.6, §13.7, §13.8. | **PROVEN** | §13.6 to §13.8. |

VERDICT: 28 clauses, 26 PROVEN, 0 OPEN, 2 REJECTED (C-012 and C-013, superseded 2026-10-17 by ruling 135).

## 1. Red first (test commit `ef74e07d`, module of `6cb6c310`)

The module is the round-1 build from the clean tree at `0f074aff` (`_native.abi3.so` sha256
`976605d7…c3cc9`); `6cb6c310` adds only documents, so it is `6cb6c310`'s module.

```
.venv/bin/python -m pytest python/repark/tests/writes/test_repark_sql_update_subquery_where_1.py -q
22 failed, 5 passed
```

Two assertions:

- P-D1 (`DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.s)`): `Failed: DID NOT RAISE
  AnalysisException` — main deletes every row.
- P-U4 (`… WHERE v = (SELECT s.w FROM ice.ns.s WHERE s.id = t.id)`): `assert g3e8_text('UPDATE',
  'ice.ns.t') in 'Invalid (non-executable) plan after Analyzer\ncaused by\nError during planning:
  Correlated scalar subquery must be aggregated to return at most one row'`.

The five controls pass. Rust, same tree:
`cargo test -p repark-core --lib dialect::tests` → the new pin FAILED, `UPDATE ice.ns.t SET v = 5
WHERE id IN (SELECT id FROM ice.ns.s): ()` (the statement ran); `cargo test -p repark-sql --test
ansi_subquery_where_identity` → 1 passed.

## 2. One home for the refusal (refactor commit `b30b7be8`)

- **Moved** from `crates/repark-sql/src/guards.rs` (lines 447-569 on main) to
  `crates/repark-iceberg/src/write/predicate_dml/subquery_where.rs`, without comments: the verb
  type, `refuse_dml_subquery_predicate`, `delete_target`, `update_target` and the message, its
  `format!` block copied byte for byte. The shared function takes `identity_forms_execute`;
  `true` keeps the old allow-list exemption, `false` refuses every form.
- **Reused, not moved:** the subquery probe. `predicate_dml.rs` already had
  `expression_contains_subquery`, which stops at any `Query` node as the guard's did, so the
  guard's copy is deleted. `object_name_of` stays in `guards.rs` for its two other guards.
- **Callers:** `router.rs` at both former call sites passes `true`; the five `guards/tests.rs`
  call sites pass `true`, their names and assertions unchanged.
- **Sizes:** `predicate_dml.rs` 989 → 990 lines (default ceiling 1000); `guards.rs` 800 → 687.
- **Byte identity:** the shared tests hold the full `UPDATE` and `DELETE` texts and the `<table>`
  fallback; the 43 guard tests (G3-E8 unit and end-to-end pins among them) and the whole
  `repark-sql` suite (556) are green after the move.

## 3. The fix (commit `80a8cfe0`)

`DataFusionDialect::execute` runs the belt's steps itself: `PreExecute::plan`, then
`refuse_dml_subquery_where` (parse with the session's `sql_parser.dialect`, the parse
`create_logical_plan` uses, and call the shared function with `false`), then `PreExecute::guard`,
then `PreExecute::execute`. A statement that does not plan keeps its plan error. On a module
built at `80a8cfe0`: the test file 27 passed; `cargo test -p repark-core --lib -- dialect::tests
pre_execute` 9 passed. The core map's rule "new pre-execute refusals land in `guard`" is not
broken in spirit: this refusal reads the parsed statement and only this door lacks an identity
path, and the ANSI router calls the same function.

## 4. Revert checks

- **`dialect.rs` from `6cb6c310`**, module rebuilt: `22 failed, 5 passed`; P-U1 `Failed: DID NOT
  RAISE AnalysisException`. The file was restored and the tree is clean.
- **The ANSI router without the exemption** (`refuse_dml_subquery_predicate(statement, false)`
  at both sites): `ansi_subquery_where_identity` FAILED, `UPDATE ice.ns.t SET v = 5 WHERE id IN
  (SELECT id FROM ice.ns.s) plans: Error during planning: UPDATE with a subquery in its WHERE
  clause is refused on \`ice.ns.t\`: …`. Restored.

## 5. Never worse

### 5a. Every answer the head takes away, with Spark's answer

The round-1 census (136 statements) and the two coincidence shapes, on `repark.sql()`, main's
module against the head's release module: 115 unchanged, 3 changed without losing an answer
(U4 and D4 refuse with the G3-E8 text instead of DataFusion's planner text; an `EXPLAIN ANALYZE`
timing), and 20 answers lost. All 20 are in the refused class.

| Statement | Main's answer | Spark 4.1.2 |
|---|---|---|
| `UPDATE ice.ns.t SET v = 5 WHERE id IN (SELECT id FROM ice.ns.s)` | 5 5 5 5 | 10 5 5 40 |
| `… WHERE id = (SELECT max(id) FROM ice.ns.s)` | 5 5 5 5 | 10 20 5 40 |
| `… WHERE EXISTS (SELECT 1 FROM ice.ns.s WHERE s.id = t.id)` | 5 5 5 5 | 10 5 5 40 |
| `… WHERE id NOT IN (SELECT id FROM ice.ns.s)` | 5 5 5 5 | 5 20 30 5 |
| `… WHERE EXISTS (SELECT 1 FROM ice.ns.s WHERE s.id = 100)` | 5 5 5 5 | 10 20 30 40 |
| `… WHERE id IN (SELECT id FROM ice.ns.s) AND v > 25` | 10 20 5 5 | 10 20 5 40 |
| `UPDATE t SET v = 5 WHERE id IN (SELECT id FROM s)` (card spelling) | 5 5 5 5 | 10 5 5 40 |
| `UPDATE t SET v = 5 WHERE id = (SELECT max(id) FROM s)` | 5 5 5 5 | 10 20 5 40 |
| `DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.s)` | no row left | 1 4 |
| `… WHERE id = (SELECT max(id) FROM ice.ns.s)` | no row left | 1 2 4 |
| `… WHERE EXISTS (SELECT 1 FROM ice.ns.s WHERE s.id = t.id)` | no row left | 1 4 |
| `… WHERE id NOT IN (SELECT id FROM ice.ns.s)` | no row left | 2 3 |
| `… WHERE EXISTS (SELECT 1 FROM ice.ns.s WHERE s.id = 100)` | no row left | 1 2 3 4 |
| `… WHERE id IN (SELECT id FROM ice.ns.s) AND v > 25` | 1 2 | 1 2 4 |
| `DELETE FROM t WHERE id IN (SELECT id FROM s)` (card spelling) | no row left | 1 4 |
| `UPDATE ice.ns.t SET v = 'z' WHERE id = ANY (SELECT id FROM ice.ns.s2)` | every row `z` | `ParseException` |
| `DELETE FROM mem3 WHERE a IN (SELECT 1)` (non-Iceberg table) | no row left | refuses: `UNSUPPORTED_FEATURE.TABLE_OPERATION` |
| `UPDATE mem3 SET a = 5 WHERE a IN (SELECT 1)` (non-Iceberg table) | 5 5 5 | refuses: `UNSUPPORTED_FEATURE.TABLE_OPERATION` |
| `DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.t)` | no row left | no row left (coincidence) |
| `UPDATE ice.ns.t SET v = 5 WHERE EXISTS (SELECT 1 FROM ice.ns.s)` | 5 5 5 5 | 5 5 5 5 (coincidence) |

`UPDATE` rows show `v` for ids 1 to 4; `DELETE` rows show the ids that remain. Spark: PySpark
4.1.2 with `iceberg-spark-runtime-4.1_2.13` 1.11.0, `local[1]`, a private Hadoop catalog per run;
the two non-Iceberg cells ran on a `spark_catalog` parquet table (and a temporary view, which
refuses too). Only the last two rows were right on main; they are P-C1 and P-C2.

### 5b. The four crates, main's tree against the head

`cargo test --no-fail-fast -p <crate>`, each inside the build lock; main's tree is `origin/main`
(`0f074aff`) checked out in the same clone, then the branch again.

| Crate | Main | Head | Missing on head | Green on main, red on head |
|---|---|---|---|---|
| `repark-iceberg` | 1013 passed | 1017 passed | 0 | 0 |
| `repark-core` | 1504 passed, 2 ignored | 1505 passed, 2 ignored | 0 | 0 |
| `repark-spark` | 2844 passed, 5 ignored | 2844 passed, 5 ignored | 0 | 0 |
| `repark-sql` | 555 passed, 4 ignored | 556 passed, 4 ignored | 0 | 0 |

The head adds six tests: the four shared-refusal tests, the core pin and the ANSI identity pin.
No flake was seen.

### 5c. The facade suite on the head release module

`pytest python/repark/tests -n 8` over all 533 test files, each run once: 20028 passed, 488
skipped, 151 xfailed, 0 failed, 0 errors. A single run does not finish inside the lane's
ten-minute command limit on the shared box, so the files ran in 23 consecutive slices, each with
`-n 8`; every file sat in exactly one slice. Earlier attempts with larger slices were cut at the
limit before they finished; they showed no failure, and their files ran again in the 23 slices.

## 6. Mutants

| Mutant | Change | Killed by |
|---|---|---|
| M1 | `DataFusionDialect` skips the call | `the_default_dialect_refuses_a_subquery_in_update_or_delete_where` (the `UPDATE … IN` ran); the 22 Python pins (revert check, §4) |
| M2 | the exemption applied on `repark.sql()` (`false` → `true`) | the core pin; Python P-U1, P-D1, P-D3, P-D5, P-D6, P-C1 (6 failed, 21 passed) |
| M3 | the exemption dropped on the ANSI router (`true` → `false`, both sites) | `the_ansi_door_answers_the_identity_subquery_forms_with_sparks_rows` |
| M4 | `DELETE` not checked | `subquery_where` tests: `the_refusal_text_is_the_g3_e8_text_for_both_verbs_and_the_fallback_target`, `a_form_outside_the_allow_list_refuses_with_or_without_the_exemption`, `the_identity_exemption_passes_the_allow_listed_forms_and_only_with_it` |
| M5 | a correlated `EXISTS` not detected (an `EXISTS` selection passes) | `a_form_outside_the_allow_list_refuses_with_or_without_the_exemption` (`UPDATE … EXISTS (… s.id = t.id): ()`), `the_identity_exemption_passes_the_allow_listed_forms_and_only_with_it` |
| M6 | one word of the text changes (`EVERY` → `every`) | `the_refusal_text_is_the_g3_e8_text_for_both_verbs_and_the_fallback_target` |

Each mutant was applied, run and restored; the tree was clean after each.

## 7. Gates

Run on the head (`80a8cfe0` product code, records staged), each with its exit code. The two
gates that read commits, `check_map_md --base origin/main` and the comment gate, run on the final
commit; the hand-back records them.

| Gate | Command | Exit |
|---|---|---|
| clippy | `make rust-clippy` (workspace, all targets, `-D warnings`) | 0 |
| panic ban | `make rust-panic-ban` | 0 |
| format | `cargo fmt --check` | 0 |
| Rust file size | `make check-rust-file-size` (1215 files; `predicate_dml.rs` 990 of 1000) | 0 |
| crate DAG | `make check-crate-dag` (25 internal edges, no new edge) | 0 |
| crate roots | `make check-lib-rs` | 0 |
| Python size | `make check-lib-py` | 0 |
| docstrings | `make check-docstring-presence` | 0 |
| Python conventions | `make check-python-conventions` | 0 |
| manifest | `make check-manifest` | 0 |
| ruff | `make py-lint`, `make py-format-check` | 0, 0 |
| docs links | `python3 scripts/check_docs_links.py` | 0 |
| ledger grammar | `python3 scripts/check_ledger_grammar.py` | 0 |
| spelling | `uvx typos@1.47.2` (`make spell-check`) | 0 |
| map sync | `python3 scripts/sync_map_md.py --check` | 0 |

## 8. Records

- The card gains a dated round-2 section: `DELETE` is in, the stopgap has landed, routing stays the
  card, and the Spark SQL door's copy is a later tidy unit.
- The registry row's heading and body name `DELETE` and the refusal, with dated lines and the pins.
- The 1.5.6 draft gains a dated behaviour line, and its known-issue bullet now names the refusal.
- Map rows: `python/repark/tests/`, `crates/repark-sql/tests/`, `crates/repark-core/src/` and
  `src/dialect/`, `crates/repark-iceberg/src/write/`, `write/predicate_dml/` and its `tests/`,
  `crates/repark-sql/src/` and `src/guards/`, `task/ledgers/staging/`, the card's row in
  `task/roadmap/mid-term/`.

## 9. Module provenance

The release module the facade suite and the census ran on: built at `80a8cfe0` from a clean tree
inside the build lock, `uvx maturin@1.14.1 develop --release`, features `extension-module` and
`allocator-mimalloc` (from `python/repark/pyproject.toml`), `CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16`
so the final crate fits one lock window; `_native.abi3.so` sha256
`12a7b5329147b1dc7300cbe2a6e1ed42ac2bbd18597daf5f9714d39d03cacdc1`. The records commit adds only
documents, so the same artifact is the final commit's module; the hand-back records its rebuild
at the final commit.

## 9b. Observed, out of scope: `EXPLAIN` still reaches the defect

Measured 2026-10-16 after the fix, with the same seeds; not changed by this unit.

| Statement | `repark.sql()`, head module | Native replica, main (Rust) | ANSI door, main | Spark SQL facade, head |
|---|---|---|---|---|
| `EXPLAIN DELETE FROM ice.ns.t WHERE id = 2`, result read | id 2 deleted | id 2 deleted | rows kept | rows kept |
| `EXPLAIN UPDATE ice.ns.t SET v = 5 WHERE id = 2`, result read | id 2 written | id 2 written | rows kept | rows kept |
| `EXPLAIN DELETE … WHERE id IN (SELECT id FROM ice.ns.s)`, result read | no row left | no row left | rows kept | rows kept |
| `EXPLAIN ANALYZE DELETE … WHERE id IN (SELECT id FROM ice.ns.s)` | no row left | — | no row left | — |
| `EXPLAIN ANALYZE UPDATE ice.ns.t SET v = 5 WHERE id IN (SELECT …)` | every row 5 | — | every row 5 | — |

On the `DataFusionDialect` path a plain `EXPLAIN` of a `DELETE` or an `UPDATE` writes when its
result is read, with any predicate; main is the same, because an `EXPLAIN` statement takes the
same plan, guard, execute belt before and after this unit. The refusal reads only a bare
`UPDATE` or `DELETE`, so an `EXPLAIN`- or `EXPLAIN ANALYZE`-wrapped one passes it on both doors.
Creating the frame without reading it writes nothing. The round-2 hand-back asks the owner where
this goes.

## 11. Owner rulings 131 and 126, 2026-10-16 (round 3)

### 11.1 The rulings, verbatim

```
Q131 → (b), and the (a) card opens alongside it. Not (c).
106 grows by one rule before its verifier runs: the repark.sql()
subquery-predicate refusal fires inside EXPLAIN and inside EXPLAIN
ANALYZE, both verbs. The router already does this for nested writes
(executed_writes / explained_statement in nested_ns.rs), so follow that
shape. Pins: both wrappers × DELETE and UPDATE, rows kept, red on the
current head.
New critical card, next Iceberg write unit after 106: a plain EXPLAIN of
DML never writes on any door, and EXPLAIN ANALYZE of DML refuses on every
door. Pins on all three doors, so routing repark.sql() later cannot bring
the write back. Named in the 1.5.6 known issues. 106 does not hold for
it: a ready refusal does not wait behind a broader fix.
```

```
Q126 → (a). U-5 #1075 merges now: PASS at d38ac00b, CI green. Then the
acceptance run and the P2 report, and the report lands before the EXPLAIN
card lane opens. 106 merges main in and re-runs the Q6 cell and the
local-file cell. Ruling 113's condition (the guard wraps the handed
dialect) lives in U-5's build, so merge order no longer matters; the unit
still open absorbs the merge.
```

### 11.2 The four cells

Seeds as §2.1 of the sketch; `ice.ns.t` holds (1, 10), (2, 20), (3, 30), (4, 40). On
`ff731179`'s module (sha256 `12a7b532…cacdc1`) each statement answered and its result, once
read, wrote; on the head each refuses with the G3-E8 text and the rows and warehouse files hold.

| Pin | Statement | `ff731179`, result read | Head |
|---|---|---|---|
| P-E1 | `EXPLAIN DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.s)` | no row left | G3-E8 refusal, rows kept |
| P-E2 | `EXPLAIN UPDATE ice.ns.t SET v = 5 WHERE id IN (SELECT id FROM ice.ns.s)` | every row 5 | G3-E8 refusal, rows kept |
| P-E3 | `EXPLAIN ANALYZE DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.s)` | no row left | G3-E8 refusal, rows kept |
| P-E4 | `EXPLAIN ANALYZE UPDATE ice.ns.t SET v = 5 WHERE id IN (SELECT id FROM ice.ns.s)` | every row 5 | G3-E8 refusal, rows kept |

Red first (test commit `af8c59dc`): `4 failed, 27 passed`; P-E1 and P-E3 `Failed: DID NOT RAISE
AnalysisException`. The two `repark-core` pins failed with `EXPLAIN DELETE FROM ice.ns.t WHERE id
IN (SELECT id FROM ice.ns.s): ()` and `a nested EXPLAIN still refuses: ()`.

The fix (commit `7a6f917f`, `dialect.rs` only): `refuse_dml_subquery_where` parses the statement
and `explained_statement` recurses through DataFusion's `DFStatement::Explain` to the inner
statement, which the shared refusal reads with no exemption. It follows the Spark router's
`executed_writes`, but fires on plain `EXPLAIN` too, where the router skips it, because reading a
plain `EXPLAIN` of a DML writes on this door. The parser takes `EXPLAIN EXPLAIN …`; the planner
refuses it ("Nested EXPLAINs are not supported") before the refusal runs, so the second level is
reached only by `the_unwrap_reaches_a_nested_explain`. `VERBOSE` and `FORMAT TREE` forms wrote on
`ff731179` too and are covered by the same unwrap.

A plain `EXPLAIN` of a DML without a subquery predicate still writes on `repark.sql()` when its
result is read, as on main: `EXPLAIN DELETE FROM ice.ns.t WHERE id = 2` deletes id 2 and `EXPLAIN
UPDATE ice.ns.t SET v = 5 WHERE id = 2` writes id 2, on the merged head as on `ff731179`. That is
card EXPLAIN-DML-WRITE-1's subject, unchanged here.

### 11.3 The merge and the cells

`git fetch origin`; `origin/main` was `14ad3c6d` (#1075, PA-LOCAL-FS-POLICY-1). `git merge
origin/main`, no rebase, merged without a conflict as `9bbfd11f`. The native builder line is now
`.with_extension(Arc::new(crate::session_runtime::NativeDoorTypes)).with_local_fs_guard()`;
`build()` wraps the held `DataFusionDialect`, which carries this unit's refusal, in
`LocalFsGuardedDialect`, whose `execute` calls the inner dialect.

On the release module built from the clean tree at `9bbfd11f` (sha256
`4e6061d53f1743b625baf42b5f94235993baa26c1006ba032562254d3b00216b`):

| Cell | Result |
|---|---|
| `test_pa_local_fs_policy_1.py::test_repark_sql_keeps_the_q6_wall_type_under_the_guard` | 1 passed |
| `test_ice_tsns_type_planner_1.py::test_repark_sql_plans_timestamp_as_the_wall` | 1 passed |
| U-5's local-file cell, `test_pa_local_fs_policy_1.py` | 26 passed |
| `test_repark_sql_update_subquery_where_1.py` (22 pins, 4 new pins, 5 controls) | 31 passed |

### 11.4 Revert check and mutants

- **`dialect.rs` from `ff731179`**, module rebuilt: `4 failed, 27 passed` (P-E1 to P-E4, `DID NOT
  RAISE AnalysisException`); both new `repark-core` pins fail. Restored.

| Mutant | Change | Killed by |
|---|---|---|
| MA | the unwrap skipped for plain `EXPLAIN` | `the_default_dialect_refuses_an_explained_subquery_dml` (`EXPLAIN DELETE … : ()`), `the_unwrap_reaches_a_nested_explain` |
| MB | the unwrap skipped for `EXPLAIN ANALYZE` | `the_default_dialect_refuses_an_explained_subquery_dml` (`EXPLAIN ANALYZE DELETE … : ()`), `the_unwrap_reaches_a_nested_explain` |
| MC | only one level unwrapped | `the_unwrap_reaches_a_nested_explain` (the door cannot reach a second level: the planner refuses a nested `EXPLAIN` first) |
| MD | the exemption passed as `true` inside the unwrap | `the_default_dialect_refuses_an_explained_subquery_dml` (`EXPLAIN DELETE … : ()`) |

### 11.5 Never worse since `ff731179`

The census of §5a with the coincidence shapes and the round-2 `EXPLAIN` cells, 141 statements on
`repark.sql()`, `ff731179`'s module against the merged head's: 132 unchanged, 9 answers lost.

| Statement | `ff731179` | Head | Spark 4.1.2 | Cause |
|---|---|---|---|---|
| `EXPLAIN DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.s)` | plan; reading it left no row | G3-E8 refusal, rows kept | plan, rows kept | ruling 131 (b) |
| `EXPLAIN ANALYZE DELETE … WHERE id IN (SELECT …)` | no row left | G3-E8 refusal, rows kept | `ParseException` | ruling 131 (b) |
| `EXPLAIN ANALYZE UPDATE … SET v = 5 WHERE id IN (SELECT …)` | every row 5 | G3-E8 refusal, rows kept | `ParseException` | ruling 131 (b) |
| `COPY (SELECT 1 AS a) TO '<outside>/…' STORED AS CSV` (4 cells: plain, with the builder opt-in, after either `SET` spelling) | file written | U-5's refusal, nothing written | `ParseException` | #1075 |
| `EXPLAIN ANALYZE COPY … TO '<outside>/…'` | file written | U-5's refusal | `ParseException` | #1075 |
| `CREATE EXTERNAL TABLE ext … LOCATION '<outside>/ext/'` | table made | U-5's refusal | `AnalysisException` (`STORED AS` CSV invalid) | #1075 |

The six local-file cells carry U-5's text ("repark.sql() does not allow local-file DDL"); they
come with the merge, under PA-02, and U-5's ledger holds them. Plain `EXPLAIN DELETE … WHERE id =
2` and `EXPLAIN UPDATE … WHERE id = 2` still write as on `ff731179`.

`cargo test --no-fail-fast -p <crate>` on the merged head: `repark-iceberg` 1017 passed;
`repark-core` 1533 passed, 2 ignored; `repark-spark` 2871 passed, 5 ignored; `repark-sql` 576
passed, 4 ignored. Against `ff731179`: no test missing, none green there and red here; the head
adds the two `repark-core` pins and U-5's tests. `pytest python/repark/tests -n 8` on the merged
release module, all 534 files once in 25 slices: 20058 passed, 488 skipped, 151 xfailed, 0 failed.

### 11.6 Gates on the merged head

`make rust-clippy` 0, `make rust-panic-ban` 0, `cargo fmt --check` 0, `make check-rust-file-size`
0, `make check-crate-dag` 0 (25 internal edges, no new edge), `make check-lib-rs` 0, `make
check-lib-py` 0, `make check-docstring-presence` 0, `make check-python-conventions` 0, `make
check-manifest` 0, `make py-lint` 0, `make py-format-check` 0; the documentation gates and the
comment gate run on the final commit (hand-back).

### 11.7 Records

The card gains its dated `EXPLAIN` line; the new critical card
[EXPLAIN-DML-WRITE-1](../../roadmap/mid-term/explain-dml-write-1-card-2026-10-16.md) carries
the owner's goal, the cells per door, the pins it owes and its place, with a map row and a 1.5.6
known-issue line.

## 12. Owner rulings 135 and 136, 2026-10-17 (round 4)

The final verifier failed `996f4d5c` (findings V106-F1 to F7): `PREPARE` and then `EXECUTE`
carried a subquery-predicate `DELETE` or `UPDATE` past the refusal and wrote every row on an
Iceberg table (F1, F2); no test killed a refusal that parses with `GenericDialect` (F3); the pins
ran on DataFusion in-memory tables, not Iceberg ones (F4); the card's plain-`EXPLAIN` write held
for in-memory tables only (F5); records said "every" form refuses (F6); the Spark SQL facade's
`EXPLAIN ANALYZE UPDATE` cell was unmeasured (F7).

### 12.1 The rulings, verbatim

```
Q135 → (a) one fix round on 106, with the shape changed:
  - The subquery refusal moves onto the executed_writes walk in
    nested_ns.rs. Main already unwraps PREPARE there. One check then
    covers PREPARE/EXECUTE, EXPLAIN ANALYZE and every wrapper. No
    PREPARE-specific arm.
  - Tests create their tables through the Iceberg catalog, and one
    guard asserts the provider is Iceberg, so this cannot regress to
    in-memory tables silently.
  - One pin on a non-default dialect.
  Then the final verifier. FAIL → park, no third round.
```

```
Q136 → correct the card. Plain EXPLAIN of DML on an Iceberg table does
not write; the write was on in-memory tables only, and the CRITICAL on
the Iceberg path is withdrawn. The card keeps two items: EXPLAIN
ANALYZE of DML executes on every door, and Spark refuses it, so RePark
refuses it too; and the PREPARE/EXECUTE path on the ANSI door. The
measurement lane is Muse xhigh, as item 3 of the overnight plan said.
No new model permission is needed. The table gains PREPARE/EXECUTE
rows on all three doors and labels any in-memory-table row as such.
You write the options from the table.
```

### 12.2 The orchestrator's two readings (the orchestrator's, not the owner's)

These are the orchestrator's readings of ruling 135, as the round-4 brief states them; the owner
sees both and may overrule them.

- **Orchestrator's reading 1, one walk for both doors, in a crate both reach.** `executed_writes`
  lives in `crates/repark-spark/src/router/nested_ns.rs` (113-140). `repark.sql()` runs through
  `DataFusionDialect` in `repark-core`, and `repark-core` cannot depend on `repark-spark`. So the
  walk moves to `repark-iceberg`, beside `write::nested_ns_gate`, which `nested_ns.rs` already
  imports. `nested_ns.rs` calls the moved walk, and its behaviour does not change. `dialect.rs`
  runs the subquery refusal on each statement the walk returns. The round-3 `explained_statement`
  is deleted and leaves nothing behind. Moved code sheds its comments. No new crate edge.
- **Orchestrator's reading 2, plain `EXPLAIN` follows the walk.** The walk returns no write for a
  plain `EXPLAIN`, so a plain `EXPLAIN` of a subquery-predicate DML answers its plan again.
  Ruling 136 establishes that this does not write on an Iceberg table, and main answers it that
  way; refusing it would take away an answer main gets right. The round-3 plain-`EXPLAIN` pins
  and their `repark-core` twins become: the plan is answered, and the Iceberg table's rows and
  snapshots are unchanged. The `EXPLAIN ANALYZE` pins keep refusing.

The brief named the plain-`EXPLAIN` pins "P-E1 and P-E3"; in the file the plain ones are P-E1
(`DELETE`) and P-E2 (`UPDATE`), and P-E3 and P-E4 are the `EXPLAIN ANALYZE` ones. The reading was
applied by what each pin runs.

### 12.3 The merge and red first

`git fetch origin`; `origin/main` was `56057f65`. `git merge origin/main`, no rebase, merged
without a conflict (the release-notes draft merged on its own) as `5a51a758`, with the default
subject. Main brought no Rust change (the dbt adapter's redaction, docs). The merge's release
module: sha256 `9f0102e5ea1d3bdb096ffad3de33dd17928f5ffabbe1d74c684763a146528087`.

The test commit `d44dcffd` seeds every pin through the Iceberg catalog: in Python
`native.create_namespace`, then `CREATE TABLE` with columns, then `INSERT`; in `repark-core` the
catalog handle's `create_table` after `ReparkSession::create_namespace`, then
`refresh_catalog_provider` and `INSERT`, the path the crate's `session/tests/io_stats.rs` uses.
One helper per language, `assert_iceberg`, fails unless `EXPLAIN SELECT * FROM <table>` holds an
`IcebergTableScan`; every pin's seed calls it, and every pin compares the rows and the snapshot
count (and in Python the warehouse files) before and after. P-U10 and P-D10 keep `mem3`, a table
labelled in-memory by the same plan check, because C-002 names an in-memory table; their Iceberg
seed goes through the guard.

On the merge's module, before any fix:

- **Python:** `6 failed, 30 passed`. The four `PREPARE` pins: `Failed: DID NOT RAISE
  AnalysisException` on `PREPARE p AS DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM
  ice.ns.s)` (on that module `EXECUTE p` then answered `count=4` and emptied the table, snapshots
  1 → 2). P-E1 and P-E2 failed on the round-3 refusal (`AnalysisException`, the G3-E8 text on
  `ice.ns.t`).
- **`repark-core`:** `3 failed, 6 passed`. `PREPARE p AS DELETE FROM ice.ns.t WHERE id IN
  (SELECT id FROM ice.ns.s): []` (the statement answered); `EXPLAIN DELETE FROM ice.ns.t WHERE id
  IN (SELECT id FROM ice.ns.s): "Error during planning: DELETE with a subquery…"`; `PREPARE p AS
  EXPLAIN ANALYZE DELETE FROM t WHERE id IN (SELECT id FROM s): ()`.
- **The dialect pin** (P-M1 and its twin) is green on the merge: the round-3 refusal already
  parses with the session's dialect. Its red is mutant M4 (§12.6).
- **The guard on the old seed:** with the round-3 seed (`CREATE SCHEMA … WITH (location = …)`,
  then CTAS), `assert_iceberg("ice.ns.t")` fails: `AssertionError: ice.ns.t is not an Iceberg
  table: TableScan: ice.ns.t projection=[id, v] / DataSourceExec: partitions=1,
  partition_sizes=[1]`.

### 12.4 The walk moves (refactor `cb5111c9`)

`executed_writes` moved from `crates/repark-spark/src/router/nested_ns.rs` (lines 113 to 139 on
the merge) to `crates/repark-iceberg/src/write/statement_writes.rs`, public and `#[must_use]`,
with no comment; `nested_ns.rs` imports it beside `nested_ns_gate`. The moved walk also answers
`Statement::Delete`. The router's match ignores it (`_ => {}`), and `visit_statements` visits the
statement itself first, so the `BY NAME` index 0 is still the top `INSERT`. `cargo test
--no-fail-fast -p repark-spark`: 2871 passed, 5 ignored before (`d44dcffd`) and after
(`cb5111c9`), the same 2874 test outcomes; no pin moved. `make rust-clippy` 0 on the refactor.

### 12.5 The fix (`3e5cc1b0`)

`dialect.rs` only. `refuse_dml_subquery_where` parses with the session's `sql_parser.dialect`,
peels DataFusion's own `EXPLAIN` layers (`DFStatement::Explain`) under the walk's rule, an
`EXPLAIN ANALYZE` reaching the statement it wraps and a plain `EXPLAIN` answering no write, then
runs `refuse_dml_subquery_predicate` with no exemption on every statement
`statement_writes::executed_writes` answers. `explained_statement` is deleted. On the head's
release module (sha256 `24c97666800c45f6cdefede74cdc4a7d5b60ee778d36efb5695a1988cba8715d`) the
file passes 36 of 36; the `repark-core` dialect tests pass 9 of 9.

| Pin | Statement | Merge `5a51a758` | Head |
|---|---|---|---|
| P-P1 | `PREPARE p AS DELETE … WHERE id IN (SELECT …)`; `EXECUTE p` | no row left, 1 → 2 | G3-E8 refusal at `PREPARE`; `EXECUTE`: `Prepared statement 'p' does not exist`; rows and snapshots held |
| P-P2 | `PREPARE p AS UPDATE … SET v = 5 WHERE id IN (SELECT …)`; `EXECUTE p` | every row 5, 1 → 2 | the same, rows held |
| P-P3 | `PREPARE p AS EXPLAIN ANALYZE DELETE …`; `EXECUTE p` | no row left, 1 → 2 | the same, rows held |
| P-P4 | `PREPARE p(INT) AS DELETE … WHERE id IN (SELECT id FROM ice.ns.s WHERE w > $1)`; `EXECUTE p(0)` | no row left, 1 → 2 | the same, rows held |
| P-M1 | MsSQL: `DELETE FROM [ice].[ns].[t] WHERE [id] IN (SELECT [id] FROM [ice].[ns].[s])` | G3-E8 refusal on `[ice].[ns].[t]` | the same |
| P-E1, P-E2 | plain `EXPLAIN DELETE` and `EXPLAIN UPDATE … IN (SELECT …)`, read | G3-E8 refusal | the plan; rows and snapshots unchanged |
| P-E3, P-E4 | `EXPLAIN ANALYZE DELETE` and `UPDATE … IN (SELECT …)` | G3-E8 refusal | G3-E8 refusal |

### 12.6 Revert check and mutants

- **Revert check:** `dialect.rs` from `996f4d5c` on the head: the `repark-core` dialect tests
  `3 failed, 6 passed`, the `PREPARE` pin red with `PREPARE p AS DELETE … : []`. For Python, the
  merge's module carries that same `dialect.rs` byte for byte (`git diff 996f4d5c 5a51a758 --
  crates/repark-core/src/dialect.rs` is empty), and its four `PREPARE` pins are red (§12.3); no
  separate Python module was built for the revert. Restored.

| Mutant | Change | Killed by |
|---|---|---|
| M1 | the walk skips `Prepare` (`Statement::Prepare { .. } => Vec::new()`) | `a_prepared_subquery_dml_refuses_and_leaves_nothing_to_execute` (`PREPARE p AS DELETE … : []`), `only_explain_analyze_reaches_the_statement_it_wraps` |
| M2 | the walk skips `EXPLAIN ANALYZE` (no write under any `Explain`) | the same two (`PREPARE p AS EXPLAIN ANALYZE DELETE … : []`) |
| M3 | the walk drops `Delete` | `the_default_dialect_refuses_a_subquery_in_update_or_delete_where`, `the_default_dialect_refuses_an_explain_analyze_of_a_subquery_dml`, `a_prepared_subquery_dml_refuses_and_leaves_nothing_to_execute`, `a_non_default_parser_dialect_still_refuses`, `only_explain_analyze_reaches_the_statement_it_wraps` |
| M4 | the refusal parses with `GenericDialect` | `a_non_default_parser_dialect_still_refuses` (the bracketed `DELETE` ran: `[RecordBatch … count …]`) |
| M5 | one pin's seed back to CTAS | the guard: Python, P-U1 to P-D8 seeded by CTAS, 16 failed with `AssertionError: ice.ns.t is not an Iceberg table: … DataSourceExec …`; `repark-core`, a CTAS table in the seed, 5 failed with `ice.ctas.t is not an Iceberg table` |

No mutant survived.

### 12.7 Never worse

The census of §5a and §11.5 (145 statements) on Iceberg seeds (`create_namespace`, then `CREATE
TABLE` with columns and `INSERT`; `mem3` stays in-memory and labelled), and 41 round-4 cells:
`PREPARE` and `EXECUTE`, plain `EXPLAIN`, `EXPLAIN ANALYZE` and the MsSQL and MySQL dialects, each
class with controls outside it (a plain predicate, a literal `IN` list, `INSERT … SELECT` reading
a subquery, `PREPARE` of a `SELECT`). 186 cells, the merge's module against the head's; `ice.ns.t`
is an Iceberg table in all 108 cells that create it.

| Change | Cells | Merge | Head | Spark 4.1.2 |
|---|---|---|---|---|
| lost, in the class | `PREPARE` of a subquery-predicate `DELETE`, `UPDATE`, parameterised `DELETE`, `EXISTS` `UPDATE`, `EXPLAIN ANALYZE DELETE` and `EXPLAIN ANALYZE UPDATE`, then `EXECUTE` (6) | the table emptied or every row rewritten, 1 → 2 | G3-E8 refusal at `PREPARE`, rows kept | `ParseException`, rows kept |
| gained, in the class | plain `EXPLAIN` of a subquery-predicate `DELETE` (twice), `UPDATE` and `VERBOSE DELETE` (4) | G3-E8 refusal | the plan, rows kept, 1 → 1 | the plan, rows kept (`VERBOSE`: `ParseException`) |
| other | `EXPLAIN ANALYZE PREPARE p AS DELETE … IN (SELECT …)` | refused (`Unsupported logical plan: Statement(Prepare)`) | G3-E8 refusal | `ParseException` |
| other | `EXPLAIN ANALYZE SELECT … IN (SELECT …)`, `EXPLAIN ANALYZE INSERT … SELECT …` | answered | answered; only the timing metrics differ | — |

173 cells are unchanged, the controls under every wrapper among them. No answer is lost outside the
subquery-predicate class, bare or under a wrapper.

`cargo test --no-fail-fast -p <crate>` on the head: `repark-iceberg` 1017 passed;
`repark-core` 1536 passed, 2 ignored; `repark-spark` 2871 passed, 5 ignored; `repark-sql` 576
passed, 4 ignored. Against the merge's Rust tree (the round-3 runs; `git diff 9bbfd11f 5a51a758 --
crates/` changes one map), nothing is green there and red here, and nothing is missing but the two
rewritten unit pins. `pytest python/repark/tests -n 8` on the head's module, all 534 files once in
25 slices: 20063 passed, 488 skipped, 151 xfailed, 0 failed (round 3: 20058; this file adds 5).
On the head's module ruling 113's Q6 cells
(`test_pa_local_fs_policy_1.py::test_repark_sql_keeps_the_q6_wall_type_under_the_guard`,
`test_ice_tsns_type_planner_1.py::test_repark_sql_plans_timestamp_as_the_wall`) pass 2 of 2 and
U-5's `test_pa_local_fs_policy_1.py` 26 of 26.

**What reading 2 costs on an in-memory table:** reading a plain `EXPLAIN DELETE FROM m WHERE a IN
(SELECT a FROM ms)` through `repark.sql()` empties an in-memory `m` on the head, as on main;
round 3 refused it. Building the frame without reading it writes nothing, and the `EXPLAIN
ANALYZE`, `PREPARE` and bare forms refuse. Card EXPLAIN-DML-WRITE-1 labels the row.

### 12.8 Gates

On the code at `3e5cc1b0`: `make rust-clippy` 0, `make rust-panic-ban` 0, `cargo fmt --check`
0, `make check-rust-file-size` 0, `make check-crate-dag` 0 (25 internal edges, no new edge),
`make check-lib-rs` 0, `make check-lib-py` 0, `make check-docstring-presence` 0, `make
check-python-conventions` 0, `make check-manifest` 0, `make py-lint` 0, `make py-format-check` 0.
The documentation gates and the comment gate run on the final commit (hand-back).

### 12.9 Records

The "every" lines of finding V106-F6 say what now holds, with a dated correction: the card's
round-2 line, the registry row, the 1.5.6 behaviour and known-issue lines, and
`crates/repark-core/src/map.md`. The card gains its ruling-135 line. Card
[EXPLAIN-DML-WRITE-1](../../roadmap/mid-term/explain-dml-write-1-card-2026-10-16.md) carries
ruling 136 verbatim, a dated section above the 2026-10-16 text (kept as written) with the two
items, the table with `PREPARE` rows on all three doors and its in-memory rows labelled, the
options written from it, and the pins it now owes; its map row and the 1.5.6 known-issue line
follow. Clauses C-012 and C-013 are REJECTED as superseded; C-017 to C-024 carry the round.

## 13. Owner rulings 138 and 139, 2026-10-17 (round 4, continued)

The owner widened round 4 while it ran. This part of the round carries ruling 139's first bullet,
the repark-spark placement; ruling 138's `PREPARE`-of-a-write refusal and the ANSI door belong to
card EXPLAIN-DML-WRITE-1.

### 13.1 The rulings, verbatim

```
Q138 → (a). PREPARE whose body is a write refuses on every door;
PREPARE of a query keeps working. Spark refuses it, so RePark
refuses it. It is not a lane of its own: it lands inside the
EXPLAIN-DML-WRITE-1 unit from Q139, since both edit the same walks.
The refusal text names the statement and says PREPARE of a query
still works; it names no data-deleting action.
```

```
Q139 → (a), CRITICAL for the wrapper-plus-subquery form, with the
scope corrected after I looked at main:
  - repark-spark's two doors share one strict pregate: the
    statement path calls one function that today matches only a
    bare DELETE or UPDATE. 106's fix round makes that one function
    the walk-level refusal (this is the Q135 shape, placed on the
    function every repark-spark caller passes), so the Spark door
    closes with 106, not later. 106's verifier pins both
    repark-spark doors. If the Spark door turns out not to reach
    that function, the fix-round sketch says so, and the Spark
    door joins the ANSI door in the unit below.
  - The ANSI door is a different crate with its own twin guard,
    called on the bare DELETE and UPDATE paths only, and the
    door-to-door ban means it cannot share repark-spark's walk.
    That twin is the EXPLAIN-DML-WRITE-1 unit: the walk-level
    refusal on the ANSI door, plus Q138's PREPARE-of-DML refusal
    on every door.
  - The unit takes 106's Opus slot the day 106 merges or parks.
    If 106 merges, its P2 report is filed first, then the lane
    opens. If 106 parks, the unit branches from main and carries
    the repark-spark placement itself. No fourth lane, and nothing
    branched from 106's round-4 head; (c) is refused.
  - Step-0 sketch before step 1: it names the exact call sites per
    door on main. Then one Opus xhigh round, then the verifier,
    which pins each wrapper on each door in the IN-subquery form,
    on tables created through the Iceberg catalog with the
    provider guard, the same test shape as 106. P2 report on merge.
  - Docs PR today, docs-only, no verifier: the measured table on
    the card and the 1.5.6 known-issue line naming both wrappers
    and both doors.
```

### 13.2 The sketch, written before any code

The tree is the merge of main `238a5b02` (#1094) into round 4's head, `e5dc4e4c`; main changed no
code, so every line below is main's `repark-spark` too. All paths are under
`crates/repark-spark/src/` unless named.

**repark-spark's two doors.**

1. **The Spark SQL session door:** `SparkDialect::execute` and `execute_with_write_options`
   (`dialect.rs:12-61`), the `SqlDialect` the Python facade's `ReparkSession` installs
   (`crates/repark-python/src/session.rs:127`). Both call `router::execute_in_session`.
2. **The router API:** `repark_spark::execute`, `execute_with_read_only`,
   `execute_static_overwrite` and `execute_with_statement_options` (`router.rs:35-86`,
   re-exported at `lib.rs:67`), the entry this crate's own tests drive. All four call
   `execute_in_session` (`router.rs:88`).

**The shared path into the function.** `execute_in_session` (`router.rs:88`) →
`execute_calibrated` (`router.rs:133`) → `execute_time_travelled` (`router.rs:208`) →
`execute_inner` (`router.rs:284`, called at `router.rs:252`). There, `parse_single_normalized`
(`router.rs:316`) parses the statement. A bare `DELETE` takes `delete_update::execute_delete`
(`router.rs:385`, `router/delete_update.rs:11`), a bare `UPDATE` takes
`delete_update::execute_update` (`router.rs:388`, `router/delete_update.rs:60`), and every
statement no dedicated arm takes falls to `spark_ast::execute_passthrough` (`router.rs:410`). Each
of the bare arms ends in the same call (`delete_update.rs:39`, `delete_update.rs:90`). The
passthrough runs `execute_passthrough_by` (`spark_ast.rs:37`) → `execute_passthrough_inner`
(`spark_ast.rs:190`). For a `DfStatement::Statement` (`spark_ast.rs:210`) it runs
`apply_pregate_rewrites_before_identity_dml` (`spark_ast.rs:211`), then `try_execute_identity_dml`
(`spark_ast.rs:212`), which runs an allow-listed shape on RePark's own executor and returns, then
`apply_pregate_rewrites_after_identity_dml` (`spark_ast.rs:215`) in `Strict` mode, whose first step
is `refuse_dml_subquery_predicate_in_statement` (`spark_ast.rs:154` → `normalize.rs:368`). That
function returns early for any statement `try_allowed_delete_in` or `try_allowed_update_in`
accepts (`normalize.rs:370-374`), refuses a bare `Statement::Delete` or `Statement::Update` whose
`WHERE` holds a subquery (`normalize.rs:375-387`), and answers `Ok` for every other statement
(`normalize.rs:388`). Its only caller is `spark_ast.rs:154`.

**Each form, per door.** The two doors share every step from `execute_in_session` on, so each
row holds for both.

| Form | Reaches `refuse_dml_subquery_predicate_in_statement`? | In what shape |
|---|---|---|
| The bare statement, `DELETE`/`UPDATE … WHERE id IN (SELECT id FROM s)` | No, in practice. `delete_update.rs:26` and `:79` skip their own G3-E8 check for an allow-listed shape, and `try_execute_identity_dml` (`spark_ast.rs:212`) then runs it and returns before the function. A subquery shape that is not allow-listed is refused at `delete_update.rs:27` or `:80`, before the passthrough. The function sees a bare statement only when the identity path declines an allow-listed shape (a target outside the catalog registry), and its allow-list exemption then lets it pass. | `Statement::Delete` / `Statement::Update` |
| `PREPARE p AS <DML>`, then `EXECUTE p` | Yes, for `PREPARE`: the `_` arm (`router.rs:410`) sends it to the passthrough; `try_execute_identity_dml` declines it (not a `Delete` or `Update`). `EXECUTE p` reaches it too, with no body: the prepared plan is DataFusion's, stored at `PREPARE`. | `Statement::Prepare { statement: Delete/Update }`, which the catch-all answers `Ok`; then `Statement::Execute` |
| `PREPARE p AS EXPLAIN ANALYZE <DML>` | Yes, as for `PREPARE`. | `Statement::Prepare { statement: Statement::Explain { analyze: true, statement: Delete } }` |
| DataFusion's own `EXPLAIN ANALYZE <DML>` | **No.** `state.sql_to_statement` (`spark_ast.rs:204`) parses a leading `EXPLAIN` as `DfStatement::Explain`, whose arm (`spark_ast.rs:238-242`) only lowers `timestamp_ns` casts; the plan then runs with `Analyze` over the `Dml`. | never reaches it |
| Plain `EXPLAIN <DML>` | **No**, the same `DfStatement::Explain` arm. It answers its plan and writes nothing on an Iceberg table (round 4's facade cells; card EXPLAIN-DML-WRITE-1). | never reaches it |

**The orchestrator's reading, checked.** Confirmed: `execute_passthrough_inner` runs
`try_execute_identity_dml` before the strict pregate; the function matches only a bare `DELETE` or
`UPDATE` after its allow-list exemption; `id IN (SELECT …)` is allow-listed; a `PREPARE` reaches
it as `Statement::Prepare` and passes the catch-all; DataFusion's `EXPLAIN [ANALYZE]` takes the
`DfStatement::Explain` arm and never reaches it. One addition: the bare `DELETE` and `UPDATE` do
not depend on the function at all. `router/delete_update.rs` runs its own G3-E8 check first, and
the identity path runs the allow-listed shapes before the function, so the function's allow-list
exemption, applied to a statement found under a wrapper, is the only thing that would let a walked
`id IN (SELECT …)` through.

**What this round does with each form.**
- `PREPARE p AS <DML>` and `PREPARE p AS EXPLAIN ANALYZE <DML>` reach the function, so the fix
  makes it refuse each `DELETE` and `UPDATE` the round-4 walk
  (`repark_iceberg::write::statement_writes::executed_writes`) returns. The allow-list exemption
  holds only for the top-level bare statement, and no walked statement gets it.
- DataFusion's own `EXPLAIN ANALYZE <DML>` does not reach the function on either door. By ruling
  139 it joins card EXPLAIN-DML-WRITE-1, and this round does not route it to the function.
- A plain `EXPLAIN` writes nothing on an Iceberg table, and the walk returns no write for it.
- A `PREPARE` with a plain predicate still prepares and executes: the walk returns its `DELETE`,
  which has no subquery to refuse.

### 13.3 Red first (test commit `a8b9b6c6`)

Both doors use round 4's test shape: Iceberg tables created through the catalog (`ice.sales.t` and
`ice.sales.s` in the router harness's `sales` namespace; `ice.ns.t` and `ice.ns.s` after `CREATE
NAMESPACE` on the facade), each seed checked for an `IcebergTableScan`, and the rows and the
snapshot count read before and after. The refusal pins assert the G3-E8 text's fixed parts and the
verb, as `dml.rs`'s `assert_g3e8_message` does. On the merge `e5dc4e4c`, before any fix:

| Pin | Door | Red assertion on the merge |
|---|---|---|
| `a_prepared_subquery_delete_refuses_on_the_router_door` | router API | `PREPARE p AS DELETE FROM ice.sales.t WHERE id IN (SELECT id FROM ice.sales.s): []` (the `PREPARE` answered) |
| `a_prepared_subquery_update_refuses_on_the_router_door` | router API | `PREPARE p AS UPDATE ice.sales.t SET v = 5 WHERE id IN (SELECT id FROM ice.sales.s): []` |
| `a_prepared_explain_analyze_subquery_delete_refuses_on_the_router_door` | router API | `PREPARE p AS EXPLAIN ANALYZE DELETE FROM ice.sales.t WHERE id IN (SELECT id FROM ice.sales.s): []` |
| P-S1 `PREPARE p AS DELETE … IN (SELECT …)` | Spark SQL session | `Failed: DID NOT RAISE AnalysisException` |
| P-S2 `PREPARE p AS UPDATE … IN (SELECT …)` | Spark SQL session | `Failed: DID NOT RAISE AnalysisException` |
| P-S3 `PREPARE p AS EXPLAIN ANALYZE DELETE … IN (SELECT …)` | Spark SQL session | `Failed: DID NOT RAISE AnalysisException` |

Router door 3 failed, 3 passed; Spark SQL session door 3 failed, 5 passed. The controls are green
before and after: the bare `IN` `DELETE` leaves ids 1 and 4, the bare `UPDATE` sets ids 2 and 3,
snapshots 1 → 2; a plain-predicate `PREPARE`/`EXECUTE` writes id 2, 1 → 2; a plain `EXPLAIN` of the
subquery `DELETE` answers its plan, 1 → 1. On the merge each `PREPARE` cell, read through
`EXECUTE p`, emptied the table or rewrote every row (snapshots 1 → 2).

### 13.4 The fix (`34e04151`)

`crates/repark-spark/src/normalize.rs` alone in product code:
`refuse_dml_subquery_predicate_in_statement` keeps its allow-list exemption for the top-level
statement (a bare allow-listed `DELETE` or `UPDATE` still answers `Ok`), then runs the G3-E8
refusal through the new `refuse_written_dml_subquery_predicate`, with no exemption, on each
statement `repark_iceberg::write::statement_writes::executed_writes` returns. The text is the
G3-E8 text, and on the facade it names the target as `ice.ns.t`. `cargo test --no-fail-fast -p repark-spark`: 2877 passed, 5 ignored; the unit's
Python files: 44 passed.

**One existing pin moved, within its own text.** The walk also reaches a DML inside a query, so
`WITH c AS (SELECT id FROM ice.sales.keys) DELETE FROM ice.sales.tgt WHERE id IN (SELECT id FROM
c)` and its `UPDATE` twin now refuse with the G3-E8 text, where DataFusion answered `This feature
is not implemented: Query …`. Both refuse and write nothing either way.
`g3e8_cte_prefixed_dml_is_loud_today_and_writes_nothing` asked for exactly this before it was
relaxed ("route it through the G3-E8 valve before relaxing this pin"); it now expects
`defect G3-E8` for its two subquery forms and `not implemented yet` for the subquery-free form,
line-neutral in `dml.rs` (its exact size baseline of 1139 lines holds), and the stale
"un-valved" doc line is gone. Spark 4.1.2 answers both CTE forms with the matching rows; RePark
answers neither, on the merge or the head.

### 13.5 Revert check and mutants

- **Revert check:** `normalize.rs` from `e5dc4e4c` on the head: the three router-door pins and the
  CTE pin red (`PREPARE p AS … : []`; `got This feature is not implemented: Query`). For Python, the
  merge's module carries that `normalize.rs` byte for byte, and P-S1 to P-S3 are red on it (§13.3).
  Restored.

| Mutant | Change | Killed by |
|---|---|---|
| N1 | the walk call removed, only the bare statement checked | the three router-door `PREPARE` pins, `g3e8_cte_prefixed_dml_is_loud_today_and_writes_nothing` |
| N2 | the allow-list exemption applied to each walked statement | the same four |
| N3 | `Prepare` not unwrapped by the walk | the three router-door `PREPARE` pins |
| N4 | `Delete` dropped from the walk | `a_prepared_subquery_delete_refuses_on_the_router_door`, `a_prepared_explain_analyze_subquery_delete_refuses_on_the_router_door`, the CTE pin |

No mutant survived. N4 leaves the bare `DELETE` family green because `router/delete_update.rs`
refuses a bare subquery `DELETE` before the passthrough, which the sketch shows.

### 13.6 Never worse

A census on the Spark SQL session door, Iceberg seeds, the merge's module (`24c97666…`, the same
code as `e5dc4e4c`) against the head's (`df9f2196…`): 86 cells. Eleven predicates (`IN`, `NOT
IN`, `EXISTS` uncorrelated and correlated, `NOT EXISTS` correlated, correlated `IN`, `=
(SELECT …)`, `IN … AND v > 25`, `= ANY`, and two plain ones), each as a bare `DELETE` and
`UPDATE`, a `PREPARE`d `DELETE` and `UPDATE`, a plain `EXPLAIN DELETE` and an `EXPLAIN ANALYZE
DELETE`; plus `PREPARE` around `EXPLAIN [ANALYZE]`, parameterised `PREPARE`, `PREPARE` of a
`SELECT` and of an `INSERT`, CTE-prefixed DML, `INSERT … SELECT … IN`, a MERGE over a subquery
source, `SELECT`/`EXPLAIN [ANALYZE] SELECT` with a subquery, and CTAS. Every seed is an
`IcebergTableScan`.

| Change | Cells | Merge | Head | Spark 4.1.2 |
|---|---|---|---|---|
| unchanged | 63, every bare shape among them: `IN`, `NOT IN`, `EXISTS`, `NOT EXISTS`, correlated `IN` keep their rows | — | — | — |
| lost, in the wrapped class | 21 `PREPARE` forms of a subquery-predicate `DELETE` or `UPDATE` (alone, parameterised, around `EXPLAIN ANALYZE`), then `EXECUTE` | the table emptied, every row rewritten, or the wrong rows (`IN … AND v > 25` deleted ids 3 and 4) | G3-E8 refusal at `PREPARE`, rows kept, 1 → 1 | `ParseException` |
| other, in the wrapped class | `WITH c AS (…) DELETE`/`UPDATE … WHERE id IN (SELECT id FROM c)` | `NotImplemented`, rows kept | G3-E8 refusal, rows kept | the matching rows |

No answer is lost outside the wrapped subquery class. `EXPLAIN ANALYZE` of every DML still writes
on this door (unchanged; card EXPLAIN-DML-WRITE-1).

`cargo test --no-fail-fast -p <crate>` on the head: `repark-iceberg` 1017 passed; `repark-core`
1536 passed, 2 ignored; `repark-spark` 2877 passed, 5 ignored; `repark-sql` 576 passed, 4 ignored.
Against round 4's runs (the merge's Rust tree), nothing is green there and red here and nothing is
missing; `repark-spark` adds the six router-door pins. `pytest python/repark/tests -n 8` on the
head's module, all 535 files once in 26 slices: 20071 passed, 488 skipped, 151 xfailed, 0 failed
(round 4: 20063; the Spark-door file adds 8).

### 13.7 Gates

The gates run on the final commit; their exit codes are in the hand-back.

### 13.8 Records

- The merge took main's card as its base (#1094: rulings 136, 138 and 139 and the measured table);
  round 4's card section of §12.9 (its table and options) gave way to main's text, and round 4's
  card edits survive as one dated line, "2026-10-17, 106 round 4", which now says what 106 closes
  on `repark.sql()` and on both repark-spark doors, and which form the sketch sends to the unit.
- The 1.5.6 bullet for this unit says what holds after the round; main's EXPLAIN-DML-WRITE-1
  bullet is kept as main wrote it.
- The 106 card, the registry row and the map rows gain a ruling-139 line.

## 10. Coverage attestation

```
COVERAGE_ATTESTATION:
  pr_unit: repark-sql-update-subquery-where-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause is walked against its pins. Every form the brief names refuses on
        repark.sql() with the G3-E8 text and holds rows and warehouse files, for both verbs; the
        card's own spelling and an in-memory table are covered; the ANSI door keeps answering
        its identity forms.
      artifacts: [python/repark/tests/writes/test_repark_sql_update_subquery_where_1.py, crates/repark-sql/tests/ansi_subquery_where_identity.rs]
      round_4: Every pin now runs on Iceberg tables behind one guard (an IcebergTableScan), with
        rows and snapshot counts read before and after; PREPARE forms and a non-default parser
        dialect join the pins (section 12).
    - id: AT-2
      status: ATTACKED
      evidence: Boundary shapes are exercised - a subquery that matches no row, a NOT IN, a
        subquery beside a literal condition, a correlated scalar subquery, = ANY, two-part and
        unqualified names, a target that is not a plain table (the <table> fallback), and
        statements with no WHERE or no subquery, which pass.
      artifacts: [crates/repark-iceberg/src/write/predicate_dml/tests/subquery_where.rs]
    - id: AT-3
      status: ATTACKED
      evidence: A refused statement writes nothing - no new file under the warehouse, the rows
        as seeded. A statement that does not plan keeps its plan error, because the refusal runs
        after planning; a parse the refusal cannot read falls through to the unchanged belt.
      artifacts: [python/repark/tests/writes/test_repark_sql_update_subquery_where_1.py, crates/repark-core/src/dialect.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The refusal is a pure function of the parsed statement and a flag; it holds no
        state. The native door's order is plan, refusal, guard, execute, with no step that can
        write before the refusal.
      artifacts: [crates/repark-iceberg/src/write/predicate_dml/subquery_where.rs]
    - id: AT-5
      status: ATTACKED
      evidence: Memory catalogs under temporary directories only; no network, no credentials, no
        AWS. The refusal text names only the target the user wrote.
      artifacts: [task/ledgers/staging/repark-sql-update-subquery-where-1-ledger.md]
    - id: AT-6
      status: ATTACKED
      evidence: The change removes a silent data-loss path (every row written, the table
        emptied). The census of section 5a lists every answer the head takes away with Spark's
        answer; only the two coincidence shapes were right, as the sketch predicted.
      artifacts: [task/wo/repark-sql-update-subquery-where-1/sketch.md]
    - id: AT-7
      status: N/A
      justification: No system-breaking resource path. The native door parses each statement
        once more after planning, a cost bounded by the parse planning already does; it was not
        timed this round, and no wall-clock claim is made.
    - id: AT-8
      status: ATTACKED
      evidence: No crate edge, no Cargo.toml, Cargo.lock or .github change; the shared function
        lives in a crate both callers already reach. The ANSI door's text and order of valves are
        unchanged, so its error contract holds; repark.sql() raises AnalysisException, the class
        the other doors' G3-E8 refusal maps to.
      artifacts: [crates/repark-sql/src/router.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The refusal names the verb, the defect id G3-E8, the target and a rewrite; the
        maps of every touched directory, the registry row and the card record where it lives.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Both revert checks are red and six mutants each die on a named test (section 6);
        every branch the diff adds has an input that changes the output - the flag's two values,
        the parse fallthrough.
      artifacts: [crates/repark-core/src/dialect/tests.rs, crates/repark-iceberg/src/write/predicate_dml/tests/subquery_where.rs]
      round_4: The revert check is red and five mutants of the walk, the parse and the seed each
        die on a named test (section 12.6).
  complete: true
```

## Hand-back

`Model: claude-opus-5-5`. `risk_tier: standard`. Round 2 builds the stopgap. Routing, Q2 and Q3
stay with the owner. Round 4 (2026-10-17, rulings 135 and 136) moves the refusal onto the shared
walk; its hand-back goes to the final verifier.

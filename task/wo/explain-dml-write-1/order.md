# EXPLAIN-DML-WRITE-1 — `EXPLAIN ANALYZE` of DML and `PREPARE` of a write refuse on every door     grade: B   engine band: Claude Opus 5.5 build, one scoped Opus 5.5 verifier   release: 1.5.6

**Status:** order, **awaits the owner's adopt of the 2026-10-20 plan (P3)**. Not dispatched.
Written by Claude (claude-opus-5-5), clerk, on 2026-10-20, from card
[EXPLAIN-DML-WRITE-1](../../roadmap/mid-term/explain-dml-write-1-card-2026-10-16.md) (rulings
136, 138 and 139 and its measured table) and from how #1090 (REPARK-SQL-UPDATE-SUBQUERY-WHERE-1,
"106") built the walk-level refusal: ledger
[repark-sql-update-subquery-where-1](../../ledgers/completed/repark-sql-update-subquery-where-1-ledger.md)
§12 and §13, P2 report
[p2-report-repark-sql-update-subquery-where-1-2026-10-17](../../../docs/cutover/p2-report-repark-sql-update-subquery-where-1-2026-10-17.md).
Every path and line below was read on main `085d77e8` (main has since taken only the docs merges #1044 and #1067); nothing was run. The 2026-10-20 plan puts
this unit third on the one Opus 5.5 lane, after PARITY-LIVE-RERECORD-1 and U-8.

## 0. Why, and what is out of scope

On every RePark door, `EXPLAIN ANALYZE` of a `DELETE` or `UPDATE` performs the write, and
`PREPARE` of one stores a plan that `EXECUTE` runs; with a subquery in the `WHERE` either form
changes every row of the table (card, measured table, main `56057f65`, 101 cells). Spark 4.1.2
refuses both at parse, so RePark refuses both (rulings 136 and 138); 106 closed only the
subquery-predicate class on `repark.sql()` and the `PREPARE` forms on the repark-spark doors.

Out of scope, by name: a plain `EXPLAIN` of DML on an in-memory table (ruling 136, not this
card); `EXPLAIN` or `EXPLAIN ANALYZE` of a query, including the facade's `df.explain("cost")`,
which sends `EXPLAIN ANALYZE` over a `SELECT`
(`python/repark/src/repark/spark/dataframe/explain.py`) and must keep answering; the bare
subquery-predicate `DELETE`/`UPDATE` (106's, unchanged on every door); `INSERT` and `MERGE`
under the wrappers unless the step-0 sketch shows they reach the same seam (then they are named
in the sketch and ruled, not added in the round); routing `repark.sql()` through the ANSI door;
any change to the walk's answer for a statement it already returns.

## 1. Rulings already made

- **Ruling 136 (owner, 2026-10-17):** plain `EXPLAIN` of DML on an Iceberg table does not write;
  `EXPLAIN ANALYZE` of DML executes on every door and Spark refuses it, so RePark refuses it.
- **Ruling 138 (owner, 2026-10-17):** `PREPARE` whose body is a write refuses on every door;
  `PREPARE` of a query keeps working. The refusal text names the statement and says `PREPARE` of a
  query still works; it names no data-deleting action. It lands inside this unit.
- **Ruling 139 (owner, 2026-10-17):** critical for the wrapper-plus-subquery form. The ANSI door
  is a different crate with its own twin call and cannot share repark-spark's walk call site (the
  door-to-door ban); its walk-level refusal is this unit. Step-0 sketch before step 1, naming the
  exact call sites per door on main; one Opus round; then the verifier, which pins each wrapper
  on each door in the `IN`-subquery form on tables created through the Iceberg catalog with the
  provider guard, the same test shape as 106; P2 report on merge.
- **106's sketch §13.2 (ledger, 2026-10-17):** DataFusion's own `EXPLAIN ANALYZE <DML>` on both
  repark-spark doors takes the `DfStatement::Explain` arm of `execute_passthrough_inner` and never
  reaches the strict pregate; it joins this unit.
- **VF-1 (106 P2 report, 2026-10-17):** the ANSI-door seed in
  `crates/repark-sql/tests/ansi_subquery_where_identity.rs` calls no provider guard; this unit's
  ANSI-door pins reuse that seed and add the guard.
- **Halts 55, 70 and 73, standing guidance 4 and 8** (carried by the 2026-10-20 plan §7): a FAIL
  parks and the owner hears first; a statement main answers that the head refuses outside the
  named table halts; a second seam halts; the P2 report lands before the next lane opens.
- **Not ruled, so not decided here:** the exact text of the `EXPLAIN ANALYZE` refusal (the
  sketch proposes it under ruling 138's constraints and the owner rules it); whether
  `EXPLAIN ANALYZE` of `INSERT` or `MERGE` refuses too (the card names `DELETE` and `UPDATE`).

## 2. Files

Read on main `085d77e8`. The sketch confirms or corrects each before step 1.

**The doors and where each wrapper enters (product, edited):**

| Door | Entry | Where `EXPLAIN [ANALYZE]` goes | Where `PREPARE` goes |
|---|---|---|---|
| `repark.sql()` | `crates/repark-core/src/dialect.rs` `DataFusionDialect::execute` → `refuse_dml_subquery_where` (171 lines) | peeled in `refuse_dml_subquery_where`'s `DFStatement::Explain` loop | walked by `executed_writes`; today refused only with a subquery predicate |
| ANSI | `crates/repark-sql/src/router.rs` `execute_time_travelled` (365 lines) | a leading `EXPLAIN` parses as a non-`Statement` `DFStatement` and goes straight to `delegate` | `Statement::Prepare` falls to the `_` arm and `delegate`; `refuse_dml_subquery_predicate(statement, true)` runs only on the bare `DELETE` and `UPDATE` arms (`execute_update_routed`, `execute_identity_or_delegate`) |
| Spark SQL session and router API | `crates/repark-spark/src/spark_ast.rs` `execute_passthrough_inner` (830 lines) | the `DfStatement::Explain` arm only lowers `timestamp_ns` casts | `apply_pregate_rewrites_after_identity_dml` → `refuse_dml_subquery_predicate_in_statement` in `crates/repark-spark/src/normalize.rs` (986 lines) |

The card names the ANSI twin as `guards::refuse_dml_subquery_predicate`; on main the function is
`repark_iceberg::write::predicate_dml::subquery_where::refuse_dml_subquery_predicate`, called
from `router.rs` (106 moved it to one shared home).

**The shared home (proposed):** `crates/repark-iceberg/src/write/statement_writes.rs` (35 lines),
where 106 put the walk `executed_writes`. One public refusal beside it that, given a parsed
statement, refuses `EXPLAIN ANALYZE` over a write and `PREPARE` over a write, and answers nothing
for a query. Each door calls it once at the place the table above names. That is the one seam;
three call sites are not three seams.

**Ceilings** (`scripts/check_rust_file_size.py`, default 1000): `normalize.rs` is at 986 and takes
no new function; `spark_ast.rs` at 830, `router.rs` at 365 and `dialect.rs` at 171 take one call
each.

**Test modules (new or edited), under the directories #1113 created:**
- `crates/repark-sql/tests/` — a new module for the ANSI door, seeded as
  `ansi_subquery_where_identity.rs` is, plus the provider guard (VF-1); its `map.md` row.
- `crates/repark-spark/src/tests/wrapped_subquery_dml.rs` (187 lines) — the router-API door;
  `a_plain_predicate_prepare_still_runs_on_the_router_door` flips by ruling 138.
- `crates/repark-core/src/dialect/tests.rs` (377 lines) — `repark.sql()` at the Rust level.
- `python/repark/tests/writes/test_repark_sql_update_subquery_where_1.py` (`repark.sql()`) and
  `python/repark/tests/writes/test_repark_sql_update_subquery_where_1_spark_door.py` (Spark SQL
  session); `test_a_plain_predicate_prepare_still_runs` flips by ruling 138. Or one new
  `python/repark/tests/writes/test_explain_dml_write_1.py`, if the sketch prefers; either way the
  `writes/map.md` row.

**Docs (edited):** the card's status; the 1.5.6 known-issue lines for EXPLAIN-DML-WRITE-1 and
106 in `task/roadmap/mid-term/v1-5-6-release-notes-draft.md` (take 106 ledger §13.6's measured
wording, VF-2); the registry rows that cite this card in `docs/spark-sql-iceberg-parity.md`; the
`map.md` of every directory touched; a unit ledger under `task/ledgers/staging/`.

**Python facade seam:** none found by reading. `ReparkSession.sql()` and `repark.sql()` hand the
text to the native doors; the facade's only `EXPLAIN ANALYZE` is `df.explain("cost")` over a
query, a control.

## 3. Skeletons

Grade B: none. The sketch names the shared function's signature and its error type (the
`DataFusionError::Plan` the G3-E8 refusal already returns is the default).

## 4. Steps

0. **Sketch** (`task/wo/explain-dml-write-1/sketch.md`, docs only, one commit): per door, the
   call path from the entry to where each of the twelve wrapper statements of the card's table
   goes on main, with file and line; the one seam and its signature; the two refusal texts; the
   pins below by name; which existing pins flip under rulings 136 and 138 and why. Any form that
   needs a second shared function, or a change outside the four product files of §2, is a halt
   here. The sketch goes to the orchestrator before step 1.
1. **Red first** (one commit, tests only): every pin of step 4 written and run on main; the
   red lines pasted into the ledger. Controls green on main.
2. **The shared refusal** (one commit): the function in `statement_writes.rs` and its unit tests
   in `repark-iceberg`.
3. **The three call sites** (one commit, or one per door): `repark.sql()`, ANSI, repark-spark.
4. **Pins green** (with step 3 or its own commit). On every door, Iceberg tables created through
   the catalog, an `IcebergTableScan` asserted on each seed, rows and snapshot count read before
   and after:
   - `EXPLAIN ANALYZE` of `DELETE` and `UPDATE`, plain predicate and `IN (SELECT …)`: refused,
     rows and snapshots held, nothing to execute;
   - `PREPARE p AS DELETE …` and `PREPARE p AS UPDATE …`, plain and `IN (SELECT …)`, and
     `PREPARE p AS EXPLAIN ANALYZE DELETE …`: refused with ruling 138's text; `EXECUTE p` then
     finds no prepared statement; rows held;
   - controls: `PREPARE p AS SELECT …` then `EXECUTE p` answers; plain `EXPLAIN` of `DELETE` and
     `UPDATE` answers its plan and writes nothing; `EXPLAIN SELECT` and `EXPLAIN ANALYZE SELECT`
     answer; `df.explain("cost")` answers; the bare `IN` forms keep the matching rows on the
     Spark SQL door.
5. **Revert check and mutants:** each call site removed alone reds that door's pins; the shared
   function's `Explain` and `Prepare` arms each removed alone red pins. Recorded in the ledger.
6. **Never worse:** the 106 census shape (ledger §13.6) re-run on main and head on each door;
   every lost answer is in the card's table, or it is a halt.
7. **Records** (one commit): card, known-issue lines, registry rows, maps, ledger.

## 5. Gates and their expected output

| Command | Green means |
|---|---|
| `make verify` | exit 0 |
| `make preflight` | exit 0 |
| `cargo test -p repark-iceberg statement_writes`, `cargo test -p repark-sql --test <the new module>`, `cargo test -p repark-spark wrapped_subquery_dml`, `cargo test -p repark-core dialect` | every test passes |
| `make rust-panic-ban`, `make rust-clippy` | exit 0 |
| `python3 scripts/check_rust_file_size.py` | `files clean`, no ceiling raised |
| `pytest python/repark/tests/writes -k "subquery_where or explain_dml"` on the release module | 0 failed |
| `make check-docs-links check-docs-compaction check-ledger-grammar`, `python3 scripts/sync_map_md.py --check` | exit 0 |
| the comment gate the lane brief names (106 ran `comment_ban.py` against `origin/main`) | `hits=0` |

CI green on the branch before the verifier. Then one fresh, scoped Opus 5.5 verifier: the pins of
step 4 on each door, the mutants, the never-worse census. A FAIL parks the unit; the owner hears
first (rulings 55, 73).

## 6. Halt rules

Stop, commit what is green, and hand back with the evidence on any of:
- **a second seam:** a form that the one shared function plus one call per door cannot refuse;
- **any statement main answers that the head refuses outside the card's table** (ruling 70), on
  any door, `EXPLAIN ANALYZE SELECT` and `df.explain("cost")` included;
- a needed change in a product file §2 does not list, or a ceiling that would have to rise;
- a seed that is not an `IcebergTableScan`;
- a refusal text that would name a data-deleting action;
- `EXPLAIN ANALYZE` of `INSERT` or `MERGE` reaching the seam with a decision to make (§1, not ruled).

A decision the order does not make is a halt with the question, its premise and a lean.

## 7. Hand-back

One JSON object: `status` (`CONCLUDED`, `HALT` or `BLOCKED`); `head`; `commits` (full sha and
subject, in order); `sketch` (the path and the seam named); `pins` (`red_on_main`,
`green_on_head`, per door); `controls`; `mutants` (each and the pin that killed it);
`never_worse` (cells, lost answers, each lost answer's card row); `gates` (command and exit
code); `halts`; `questions` (id, question, premise, lean); `out_of_scope_observed`.

Standing rules from [TEMPLATE.md](../TEMPLATE.md): no code comments from Anthropic models and
relocated code sheds its comments; `map.md` lockstep; the scratch clone, never the live worktree;
never push, never open a pull request; the TRO-Wolf identity and exactly one trailer,
`Authored-By: Claude (claude-opus-5-5) <noreply@anthropic.com>`.

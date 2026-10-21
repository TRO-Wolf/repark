# P2 report for #1137, EXPLAIN-DML-WRITE-1 (2026-10-21)

Sources: the builder's step-0, phase-B and fix-round hand-backs, the verifier's round-1 and
round-2 hand-backs, the unit ledger
([explain-dml-write-1-ledger](../../task/ledgers/staging/explain-dml-write-1-ledger.md)), the card
([EXPLAIN-DML-WRITE-1](../../task/roadmap/mid-term/explain-dml-write-1-card-2026-10-16.md)) and
the unit's work order and evidence under `task/wo/explain-dml-write-1/`. Where a source does not
carry a number, this report says "not recorded".

## Verifier verdicts, by time

The hand-backs do not carry the clock time of either verdict (not recorded). The times below are
the pushed commits of the PR and the merge, in UTC, which bound the verdicts.

- 2026-10-21 19:51:45Z: the builder's last phase-B commit, `eccd920a`, is pushed. Verifier round 1
  (head `eccd920a`, time of verdict not recorded): **FAIL**. The product behaved correctly on every
  door; the findings were pins and records.
  - **S1-1:** the trailing-semicolon mutant survived every door pin and is not equivalent. With
    the refusal skipped on a trailing `;`, `EXPLAIN ANALYZE DELETE … WHERE id = 2;` deleted row 2
    on the ANSI door, the router API and the native door, and `PREPARE p AS DELETE …;` then
    `EXECUTE p` deleted it. The unmutated head refused both on all four doors.
  - **S2-1:** the ledger said the 350 dated planner rows split 200 for ruling 136 and 150 for
    ruling 138. The fixture holds 225 citing 136 and 125 citing 138.
  - **S2-2:** the ledger's C-005 and the registry and release-notes lines said a wrapped write
    refuses with the ruled text "on every door". On `repark.sql()` the class that DataFusion cannot
    plan keeps DataFusion's error, wider than the `MERGE` the card named.
  - **S2-3:** the `PREPARE` text formatted `name.value`, so `PREPARE "My P" AS DELETE …` showed
    the name without its quotes.
- 2026-10-21 20:35Z: the owner's grant under which the orchestrator ruled each fix item (unit
  ledger §9).
- 2026-10-21 22:02:56Z to 22:05:58Z: fix round 1, four commits, one per finding, to `4b72871b`:
  - `c0326f02`, S1-1, the ruling-142 spellings on every Rust door.
  - `4da64ed1`, S2-3, the prepared name as written.
  - `4a1661de`, S2-1, the dated-row split corrected (ledger §9.3).
  - `4b72871b`, S2-2 and Q-B1, the plan-first class on `repark.sql()` recorded.
- Verifier round 2 (head `4b72871b`, time of verdict not recorded): **PASS**, no S1 and no S2. All
  four round-1 findings closed and re-measured; three S3 nits, listed under Follow-ups.
- 2026-10-21 23:16:32Z: `81b5e7cb`, a merge of main into the branch that adds five lines to
  `docs/spark-sql-iceberg-parity.md`. Whether the verifier's PASS covered it is not recorded.
- 2026-10-21 23:32:10Z (19:32:10 EDT): merged to main as `b7a20167` (squash). The PR check rollup
  at merge lists 26 checks: 24 `SUCCESS`, 2 `SKIPPED` (the tag-only release wheels and the abi3
  wheel), none failing.

Owner rulings of 2026-10-21 (unit ledger, "Rulings this unit runs under"):

- **140:** the `EXPLAIN ANALYZE` text is adopted as the step-0 sketch proposed it, and the
  `PREPARE` text as written under ruling 138.
- **141:** `INSERT` and `MERGE` refuse under both wrappers too, on every door, through the same
  seam: a wrapper never performs a write.
- **142:** a row of the card's table covers every spelling of that wrapper and verb (any or no
  predicate, `VERBOSE`, `FORMAT`, a parameterised `PREPARE`, a CTE-carried statement,
  `EXPLAIN ANALYZE` around `PREPARE`).
- **143:** false docstring sentences in the flipped pins are deleted, never reworded, and no
  docstring text is added.
- **144:** the planner census gains one dated row per lost cell in
  `ice_tsns_type_planner_1_dated.json`, class "ruled refusal", harness and routes unchanged.

Orchestrator ruling on Q-B1, under the owner's grant: accepted as built. On `repark.sql()` the
call stays after `belt.plan`, so every wrapped write that DataFusion cannot plan keeps DataFusion's
own error and writes nothing. The same ruling set the prepared name to the identifier as written,
quoting included.

## Acceptance run

No live AWS acceptance run (the Glue and S3 Tables job of the template) is recorded for this
unit, and none is listed in either verifier hand-back. The gates below are the ones the
hand-backs record. The builder ran no `make verify` or `make preflight` locally (AGENTS.md
delegated-agent rule, whole-workspace suites left to CI); the verifier ran `make verify` in
round 1.

Builder, fix round 1 (head `4b72871b`, fix-round hand-back):

| Gate | Exit | Result |
|---|---|---|
| `cargo fmt --all -- --check` | 0 | clean |
| `make ci` | 0 | wall 556 s |
| `cargo test --no-fail-fast -p repark-iceberg` | 0 | 1026 passed |
| `cargo test --no-fail-fast -p repark-core` | 0 | 1542 passed, 2 ignored |
| `cargo test --no-fail-fast -p repark-sql` | 0 | 580 passed, 4 ignored |
| `cargo test --no-fail-fast -p repark-spark` | 0 | 2936 passed, 5 ignored |
| pytest, the unit's four writes files, rebuilt head module | 0 | 111 passed |
| pytest `test_ice_tsns_type_planner_1.py` | 0 | 60 passed (the planner census, 60 of 60) |
| comment gate grep on `origin/main...HEAD` | 0 | prints nothing |
| `python3 scripts/sync_map_md.py` | 0 | 427 maps clean |
| `python3 scripts/check_model_ban.py` | 0 | clean |
| trailers | | 4 of 4 commits carry exactly the `Authored-By` line |

Builder, phase B (head `eccd920a`, phase-B hand-back): `make ci` exit 0 in 552 s; the same four
per-crate counts at 1026, 1541 (2 ignored), 579 (4 ignored) and 2935 (5 ignored); 101 passed on
`tests/writes -k 'subquery_where or explain_dml'`; 60 passed on the planner census
(`-n 8`, 249 s); 497 passed and 1 skipped on seven neighbouring wrapper files.
`make check-comment-density` exited 2 because the target does not exist on main.

Verifier, round 1 (head `eccd920a`):

| Gate | Result |
|---|---|
| `cargo test -p repark-iceberg --lib statement_writes` | 4 of 4 passed |
| `cargo test -p repark-core --lib dialect` | 14 of 14 passed |
| `cargo test -p repark-sql --test ansi_explain_dml_write` | 3 of 3 passed |
| `cargo test -p repark-spark --lib wrapped_subquery_dml` | 10 of 10 passed |
| facade suite on head (`-n 16`) | 20287 passed, 3 failed, 485 skipped, 151 xfailed, 8 min 23 s |
| live leg, `REPARK_PARITY_LIVE=1`, `tests/parity` and `tests/writes` co-collected, `-n 8` | 5914 passed, 13 skipped, 3 xfailed, 0 failed |
| `make verify` | exit 0; `make ci` green, workspace nextest 7763 of 7763 passed, 97 skipped |

The 3 facade failures are environmental: `test_oracle_applyinpandas_null_keys`,
`test_oracle_pandas_udf_multi_arg` and `test_oracle_pandas_udf_with_column` raised
`ModuleNotFoundError: pandas` in PySpark's worker interpreter, and pass when re-run alone on head
and on main.

Verifier, round 2 (head `4b72871b`): `make ci` exit 0; the per-crate counts match the builder's
(1026, 1542 with 2 ignored, 580 with 4 ignored, 2936 with 5 ignored); the four writes files 111
passed; the dialect, ANSI and router pin groups 15, 4 and 11 passed on the clean head; comment
gate prints nothing; `sync_map_md.py --check` 427 maps clean; ledger grammar 346 live ledgers
clean. The round-2 hand-back records no wall for these runs, and it did not re-run the facade
suite, the live leg or `make verify` (round-1 results above stand for `eccd920a`; their re-run on
`4b72871b` is not recorded).

Cost, verifier round 1 (10-run medians, main against head, ratios at most 1.05x): `repark.sql()`
`SELECT` 1.024x, `INSERT` 0.996x, `EXPLAIN SELECT` 0.966x; Spark SQL session 1.028x, 0.984x,
1.005x; ANSI 1.016x, 0.941x, 1.014x; router API 0.988x, 1.027x, 1.028x.

## What changed in the write path

- **The seam.** `refuse_wrapped_write(&DFStatement) -> datafusion::error::Result<()>` in
  `crates/repark-iceberg/src/write/statement_writes.rs`, beside `executed_writes`. It looks through
  DataFusion `EXPLAIN` layers (a plain one answers `Ok`) and refuses when an `EXPLAIN ANALYZE`, or
  a `PREPARE` at the top, wraps a statement whose `executed_writes` holds a write (`DELETE`,
  `UPDATE`, `INSERT`, `MERGE`). The text follows the outermost wrapper. The error is
  `DataFusionError::Plan`; a refused `PREPARE` is never stored, so `EXECUTE` finds nothing. The
  file went from 35 to 78 lines in step 2; the verifier recorded 77 on head against a ceiling of
  1000. Its unit tests are `statement_writes/tests.rs`, 4 passed.
- **The three call sites,** one line each, on the parsed statement the door already holds, before
  `execute_logical_plan`:

| Door | File and function | Place |
|---|---|---|
| `repark.sql()` | `crates/repark-core/src/dialect.rs`, `refuse_dml_subquery_where` | after the parse and after `belt.plan`, before 106's `EXPLAIN` peel and walk |
| ANSI door | `crates/repark-sql/src/router.rs`, `execute_time_travelled` | after the parse, before the `RESET` arm |
| both repark-spark doors | `crates/repark-spark/src/spark_ast.rs`, `execute_passthrough_inner` | after `sql_to_statement`, before the match, after the nested-NTZ gate and before the strict pregate |

  Lines on head: `dialect.rs` 172, `router.rs` 366, `spark_ast.rs` 831 (verifier, round 1); no
  ceiling rose, no crate edge, `normalize.rs` untouched.
- **The two ruled texts, verbatim** (ruling 140; the `PREPARE` name is the identifier as written,
  quoting included, since fix round 1):

  ```
  EXPLAIN ANALYZE is refused: its statement writes to a table, and EXPLAIN ANALYZE runs the statement it explains. A plain EXPLAIN of the same statement still shows its plan without running it, and EXPLAIN ANALYZE of a query still works.
  ```

  ```
  PREPARE `{name}` is refused: its statement writes to a table, and a prepared write would run later, at EXECUTE. PREPARE of a query still works.
  ```

  Neither names a data-deleting action. After the `Error during planning: ` prefix of
  `DataFusionError::Plan`, the verifier found the bytes equal to the ruled text in 204 refusals on
  all four doors.
- **Never worse (the 106 census shape, 106 cells per door, Iceberg seeds, 424 of 424 guarded).**
  Builder and verifier counts match on every row. Source: the phase-B hand-back, the verifier's
  round-1 hand-back and ledger §6.

| Door | Unchanged | Wrote on main, ruled refusal on head | Refused on main, ruled refusal on head |
|---|---|---|---|
| `repark.sql()` | 52 | 15 | 39 |
| ANSI door | 48 | 53 | 5 |
| Spark SQL session | 48 | 33 | 25 |
| router API | 48 | 33 | 25 |

  Outside the class, or a table not held: 0 on every door. Every changed cell is a write under
  `EXPLAIN ANALYZE` or `PREPARE`, a row of the card's table under rulings 141 and 142. The
  verifier's own 91-cell probe agreed: no wrapped statement changed any table on head, and no
  control cell differed between main and head. Live Spark 4.1.2 raised `ParseException` on both
  wrappers.
- **The planner census (ruling 144).** 350 dated rows in
  `python/repark/tests/timestamps/ice_tsns_type_planner_1_dated.json`, class "ruled refusal", dated
  2026-10-21; 290 rows before, 640 after. **225 cite ruling 136:** `explain_analyze_insert` 125 and
  `explain_analyze_update` 100. **125 cite ruling 138:** `prepare_execute_insert` 125. The
  per-route counts and the total stood from step 4; only the 200/150 sentence in ledger §4.1 was
  wrong, corrected in §9.3. The verifier measured all 450 wrapper-route cells: main answered 420
  (the 30 errors are `explain_analyze_update` cells), head refuses all 450, and the harness passes
  60 of 60.
- **Mutants.** Source: ledger §5 and §9.1, the phase-B hand-back, both verifier hand-backs.

| Mutant | Killed by |
|---|---|
| M1 builder: `Explain` look-through removed | `an_explain_analyze_of_a_write_refuses_with_its_text`, `neither_refusal_names_a_data_deleting_action`; `the_ansi_door_refuses_an_explain_analyze_of_a_write` |
| M2 builder: `Prepare` arm removed | `a_prepare_of_a_write_refuses_with_its_text`; `the_ansi_door_refuses_a_prepared_write_and_leaves_nothing_to_execute` |
| M3 builder: plain `EXPLAIN` refused | `a_query_a_plain_explain_and_a_bare_statement_answer_nothing`; all three ANSI pins |
| M4 builder: `DELETE` and `UPDATE` only | `an_explain_analyze_of_a_write_refuses_with_its_text`, `a_prepare_of_a_write_refuses_with_its_text`; both ANSI refusal pins |
| M5 builder: `analyzed` arm removed | `an_explain_analyze_of_a_write_refuses_with_its_text`, `neither_refusal_names_a_data_deleting_action`; `the_ansi_door_refuses_an_explain_analyze_of_a_write` |
| R1 builder: `dialect.rs` call removed | 5 repark-core dialect pins (5 failed, 9 passed) |
| R2 builder: `router.rs` call removed | both ANSI refusal pins (2 failed, 1 passed) |
| R3 builder: `spark_ast.rs` call removed | 6 `wrapped_subquery_dml` pins (6 failed, 4 passed) |
| V1 verifier: look-through stops at the first `EXPLAIN` | `an_explain_analyze_of_a_write_refuses_with_its_text` |
| V2 verifier: `PREPARE` refused by name, not body | `a_query_a_plain_explain_and_a_bare_statement_answer_nothing` |
| V3 verifier: verb set drops `INSERT` | `an_explain_analyze_of_a_write_…`, `a_prepare_of_a_write_…` |
| V4 verifier: verb set drops `MERGE` | the same two |
| V5 verifier: refusal skipped on a trailing `;` (all three sites) | **survived** all door pins in round 1 (S1-1); killed after fix round 1, see below |
| V6 verifier: `analyzed` flag read from `verbose` | 3 unit pins |
| V7 verifier: Spark call after the strict pregate | 4 `wrapped_subquery_dml` `PREPARE` pins |

  The trailing-semicolon mutant after fix round 1 (verifier round 2; each door's pin is
  `every_spelling_of_a_wrapped_write_…`, over six spellings: a trailing `;`, a leading `--`
  comment and lower case, each under `EXPLAIN ANALYZE` and under `PREPARE`):

| Mutant applied at | `repark-core --lib dialect` | `repark-sql --test ansi_explain_dml_write` | `repark-spark --lib wrapped_subquery_dml` |
|---|---|---|---|
| clean head | 15 passed | 4 passed | 11 passed |
| all three sites | 1 failed: `every_spelling_of_a_wrapped_write_refuses_and_writes_nothing` | 1 failed: `the_ansi_door_refuses_every_spelling_of_a_wrapped_write` | 1 failed: `every_spelling_of_a_wrapped_write_refuses_on_the_router_door` |
| `dialect.rs` only | 1 failed | 4 passed | 11 passed |
| `router.rs` only | 15 passed | 1 failed | 11 passed |
| `spark_ast.rs` only | 15 passed | 4 passed | 1 failed |

  The red lines are the statement `EXPLAIN ANALYZE DELETE … WHERE id = 2;` answering its plan, at
  `dialect/tests.rs:233:49`, `ansi_explain_dml_write.rs:111:53` and `wrapped_subquery_dml.rs:104:10`.
  The Python doors were given no extra spelling pins (fix-round hand-back); the verifier confirmed
  by probe that `repark.sql()` and the Spark SQL session refuse the three spellings under both
  wrappers. For the prepared name, with `name.value` restored the pin
  `a_prepare_of_a_write_refuses_with_its_text` goes red at `tests.rs:64:5`; an unquoted name
  renders the same bytes as before.

## Follow-ups

- **The native-door plan-first class (S2-2 and Q-B1, ruled as built, unscheduled).** On
  `repark.sql()` the seam runs after `belt.plan`, so a `MERGE`, a CTE-carried `DELETE`, `UPDATE` or
  `INSERT` and a DML-in-CTE query, a wrapped write on a missing table or column,
  `EXPLAIN ANALYZE … FORMAT x` and `DESCRIBE ANALYZE` keep DataFusion's own error text; nothing is
  written, and main gave the same errors. The other three doors give each the ruled text. Moving
  the native-door call before planning would move 16 refusal texts and no write (the verifier's
  91-cell probe: 16 cells change, all refusal to refusal); it would also end 106's AT-3 order (a
  statement that does not plan keeps its plan error) for wrapped writes. Unscheduled.
- **S3-1, round 2.** The new spelling pins put one verb on each spelling (`;` on `DELETE`, `--`
  on `UPDATE`, lower case on `INSERT`), not the full spelling-by-verb grid, while ledger §9.1
  reads like a full grid. Each spelling is covered under both wrappers and the mutant dies on every
  door. No product risk.
- **S3-3, round 2.** The 106 (VF-2) line in the 1.5.6 release-notes draft names the plan-first
  class in general words ("a wrapped write that DataFusion cannot plan"); the EXPLAIN-DML-WRITE-1
  line carries the full list.
- **S3-2, round 2.** Under the `Ident` display a backtick-quoted prepared name renders with doubled
  backticks (``PREPARE ``bq`` is refused``) on every door. The verifier read it as consistent with
  "as written, quoting included".
- **Round-1 S3 items with no recorded action.** S3-2: `DESCRIBE ANALYZE <write>` gets the
  `EXPLAIN ANALYZE` text on the ANSI and native doors (sqlparser yields an analyzed `Explain`; main
  refused it too). S3-3: the ledger's clause rows were rewritten in place in step 7. S3-4: the
  `df.explain("cost")` seed (`test_df_explain_cost_still_answers`) has no `assert_iceberg` guard.
  Whether the guard was added is not recorded; the fix-round hand-back lists no such change.
- **Observed out of scope (phase-B and verifier hand-backs).** A plain `EXPLAIN` of DML read
  through `repark.sql()` on an in-memory table still writes (ruling 136, not this card). The ANSI
  door has no walk for DML carried inside a query; bare it answers as before, under a wrapper it now
  refuses. On a bare Rust `ReparkSession` with no local-filesystem guard (not a product door),
  `EXPLAIN ANALYZE COPY (SELECT 1) TO 'probe_out'` wrote a directory; the Python `repark.sql()`
  door refuses it through its guard. Spark raises `ParseException` for both wrappers, where
  RePark's refusal is `DataFusionError::Plan`, an `AnalysisException` on the facade.
- **Records still open.** The unit ledger's header says it moves to `../completed/` in the unit's
  last commit after the verifier's PASS and the owner's merge; on `b7a20167` it is still in
  `task/ledgers/staging/`. The card's own text still reads "verifier pending, owner merges", and
  the 1.5.6 release-notes lines for the unit still read "built, verifier pending" and "This line
  becomes a behaviour change when the unit merges". The 1.5.6 line for the unit is the clerk's to
  turn into a behaviour-change line at release.
- **Not run locally by the builder:** `make verify` and `make preflight` (CI ran the same surface,
  green). `make check-comment-density` is not a target on main.

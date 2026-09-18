# Charter ledger — ICE-MIXED-CASE-1 · Spark-door case-insensitive column resolution

**Date:** 2026-09-17 · **Branch:** `fix/ice-mixed-case-1` · **Base:** `origin/main`
`32c0e1a3` (rebased onto `71482620` in round 21b) · **Model:** claude-opus-5 (round 21b; rounds 1–5 muse-spark-1.3-contributor) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** A Spark-created Iceberg table `(userId BIGINT, eventName STRING,
`Mixed Case` INT)` adopted into RePark fails loud on the SQL door for every
unquoted reference (`SELECT userId` → `AnalysisException: Schema error: No
field named userid`), while Spark 4.1.2 under `spark.sql.caseSensitive=false`
resolves every spelling case-insensitively and raises `[AMBIGUOUS_REFERENCE]`
on case-only collisions. Registry row ID-1 claims unquoted mixed-case
references agree with Spark; for stored mixed-case columns that is false
(rating row V2-27, claim C-11, measured 2026-09-16).

**Not in this unit:** the ANSI door (`repark.sql()` native) is unchanged; the
DataFrame door's existing Python matching stays unless the Rust rule makes it
redundant and the facade suite stays green; table/namespace/catalog name case
is untouched; nested-field case is untouched; the fork is untouched (no
table-format semantics change, so no fork PR).

## Rulings recorded at open

- Q-19b-1 (orchestrator, 2026-09-16): the Spark SQL door resolves column
  references (SELECT list, WHERE, GROUP BY / ORDER BY, JOIN ON, UPDATE SET
  targets and expressions, DELETE WHERE, MERGE ON / SET / INSERT column lists
  and values, INSERT INTO t (cols)) against the input schema
  case-insensitively when `spark.sql.caseSensitive` is false (default), and
  exactly when it is true, matching Spark on both. Ambiguous matches raise
  Spark's error class and message shape as measured. The ANSI door is
  unchanged. The fix is Rust, not Python.
- Q-17a-2 (owner, 2026-09-16): a decision that raises, casts, coerces or
  branches on a value is a kernel too. Resolution lands in Rust.
- R-19b-1 (this lane, 2026-09-17): DataFusion lowercases unquoted identifiers
  at parse (`enable_ident_normalization`, default true) and resolves exactly,
  so the original case is unrecoverable downstream. The Spark door therefore
  plans with normalization off (SparkExtension configure) and a Spark-door
  analyzer rule rewrites `Column` references case-insensitively when the flag
  is false. The ANSI door keeps normalization on and gains no rule.
- R-19b-2 (this lane, 2026-09-17): the rule also audits already-resolved
  columns: when two input fields differ only by case, any reference to either
  raises `[AMBIGUOUS_REFERENCE]`, matching Spark, instead of silently taking
  the exact match.
- Q-20b-2 (orchestrator, 2026-09-17, round-5 REDESIGN): normalization stays ON;
  the Spark door folds the parsed statement once against the valid fields
  (alias / ORDER BY / HAVING / GROUP BY / JOIN-USING / UPDATE / INSERT / MERGE
  coverage) plus a plan ambiguity audit; `caseSensitive=true` exact-case is the
  backticked spelling, unquoted exact-case refusal is DECLARED; the DataFrame
  door is unchanged; the retry loop is deleted.
- R-20b-1 (this lane, 2026-09-17): the fold emits backticked stored-case
  spellings, not double-quoted ones. Double-quoted spans are string literals
  on the Spark door (registry ID-2), and `t."userId"` does not parse in the
  session dialect — the MERGE `ON` fragment proved it (`No field named t` on
  `t.userid = s.USERID`, backticked `ON` green). Statement planning takes the
  AST directly, so the quoting only mattered on the fragment re-parse path.
- R-20b-2 (this lane, 2026-09-17): the four write-side twin-check sites
  (`merge/mod.rs`, `merge/insert.rs`, `merge/not_matched_by_source.rs`,
  `predicate_dml.rs`) delegate to one `resolve_write_column` helper in
  `name_resolution.rs`; both exact-baseline files ratchet down
  (`mod.rs` 1782→1780, `predicate_dml.rs` 1141→1139). `column_resolution.rs`
  tests move to `column_resolution/tests.rs` under the file-size gate
  (996 + 312).

- Q-21b-1 (orchestrator, 2026-09-17, run 21b): the measurement wins — every
  case-twin refusal keeps the class `[AMBIGUOUS_REFERENCE]` and carries
  `SQLSTATE: 42704` (measured on PySpark 4.1.2 + Iceberg 1.11.0), not the
  unmeasured `42702` Q-20b-2 chose. Pins, ledger and registry say 42704.
- Q-21b-2 (orchestrator, 2026-09-17, run 21b): the option list has one entry per
  matching field (two twins, two entries), each in the REQUESTED spelling,
  qualified the way Spark qualifies it — `` `t`.`X` `` for a qualified reference,
  the relation's name (`` `cat`.`ns`.`tbl`.`X` `` when the FROM names it that way)
  for a bare one. A case twin is refused even for an exact-case reference.
- R-21b-1 (this lane, 2026-09-17, round 21b step 0): origin/main landed
  `repark_functions::case_sensitive::SparkCaseSensitiveConfig` (ICE-RTAS-BYNAME-1)
  as the `spark.sql.caseSensitive` carrier, runtime setter included. The unit's
  `ColumnResolutionConfig` duplicated it; after the rebase `SET
  spark.sql.caseSensitive` would have written only main's. The unit's carrier is
  deleted; `plan_statement_with_column_repair` / `sql_with_column_repair` take
  `case_insensitive` as an argument and the Spark door passes
  `!spark_case_sensitive_from_options(..)`.
- Q-21b-11 (orchestrator, 2026-09-18, run 21b round 2, G-2): N-01 is not a defect.
  Spark resolves the inner SELECT-list `EVENTNAME` to the outer `mc.eventName`
  (correlated) and answers `[[1], [2]]` in all three spellings (`probe_mc2.py`).
  The ruling said to pin those answers, then measure the scalar-subquery cell.
  Measured outcome in §10 step 2: RePark refuses all four at physical planning,
  the same as on an all-lowercase schema, so the lane pinned the loud refusal as
  DECLARED (registry ID-1b) instead of an answer. That choice is item 1 of §7.
- N-02 ruling (orchestrator, 2026-09-18): a pin that stays green with its fix
  reverted proves nothing. Add one that goes red under the revert, or prove that
  no such shape exists and delete the hollow pin. §10 step 3 does both: the
  proof, the deletion, and the pin that goes red under the revert.
- Q-21b-12 (orchestrator, 2026-09-18, G-2): if it is small and confined to the
  fold/audit module, the Spark SQL door refuses a star that would output ASCII
  case twins with `[COLUMN_ALREADY_EXISTS]` / 42711. Otherwise, pin RePark's
  answer with a DECLARED registry row. The refusal was built and measured and was
  not confined to the module (§10 step 4), so the declared answer is pinned
  (registry ID-1a).

## PROPOSITION LEDGER — ICE-MIXED-CASE-1 — 2026-09-17

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | SELECT list and WHERE on the adopted table answer Spark (false): unquoted mixed/lower case, quoted exact and quoted wrong-case. | `test_ice_mixed_case_1.py` MC-SEL-01…04, MC-WHERE-01…02 cells vs oracle (false). | PROVEN | Round-5 redesign (Q-20b-2): offline 42 passed 2026-09-17, release native (§6); live 78-cell re-run pending. |
| C-002 | GROUP BY / ORDER BY / self-JOIN ON answer Spark (false), including ordering by a differently-cased key. | Same pin file, MC-GRP-01, MC-ORD-01, MC-JOIN-01. | PROVEN | Round-5 redesign (Q-20b-2): offline 42 passed 2026-09-17, release native (§6); live 78-cell re-run pending. |
| C-003 | UPDATE (SET target and expression, WHERE) and DELETE WHERE answer Spark (false), verified by reading the table back. | Same pin file, MC-UPD-01…02, MC-DEL-01. | PROVEN | Round-5 redesign (Q-20b-2): offline 42 passed 2026-09-17, release native (§6); live 78-cell re-run pending. |
| C-004 | MERGE answers Spark (false): unquoted source aliases with backticked ON, differently-cased ON/SET/INSERT cols and values, `UPDATE SET *` / `INSERT *` with differently-cased source columns. | Same pin file, MC-MRG-01…04. | PROVEN | Round-5 redesign (Q-20b-2): offline 42 passed 2026-09-17 (§6); `test_merge_insert_scope.py` 4 passed incl. NOT MATCHED source-scope pin; Rust `fragment_rewrite_scopes_bare_references_to_one_side` green; live re-run pending. |
| C-005 | INSERT INTO t (cols) with differently-cased column list answers Spark (false). | Same pin file, MC-INS-01. | PROVEN | Round-5 redesign (Q-20b-2): offline 42 passed 2026-09-17, release native (§6); live 78-cell re-run pending. |
| C-006 | Quoted wrong-case (`` `USERID` ``) resolves under false and refuses under true; backticked exact-case resolves under true and answers the recorded `true` oracle rows; unquoted exact-case refuses loud under true (DECLARED split, registry ID-1). | Same pin file: `test_sql_door_exact_spelling_succeeds_case_sensitive` (MC-SEL-04) + `test_sql_door_backticked_exact_case_succeeds_case_sensitive` (`_TRUE_BACKTICK_SQL`) vs oracle (true); `test_sql_door_unquoted_exact_case_refuses_case_sensitive` pins the declared refusal. | PROVEN | Offline 42 passed 2026-09-17, release native, round-5 redesign (§6); live re-run pending (§6). |
| C-007 | A frame carrying `a` and `A` refuses any reference to either with Spark's `[AMBIGUOUS_REFERENCE]` class and message shape under false. | Same pin file, MC-AMB-01…02; Rust unit tests for the rule. | PROVEN | Round-5 redesign: pin cells green offline 2026-09-17 (§6); `repark-core --lib` 591 passed incl. 16 column_resolution tests (incl. the alias-shadow pin). |
| C-008 | A temp view of a DataFrame with camelCase columns answers Spark (false) for the SELECT/WHERE cells. | Same pin file, MC-VIEW-01…02. | PROVEN | Round-5 redesign (Q-20b-2): offline 42 passed 2026-09-17, release native (§6); live 78-cell re-run pending. |
| C-009 | The DataFrame door (`select`, `filter`) answers the same cells as recorded (unchanged behavior). | Same pin file, MC-DF-01…04. | PROVEN | Round-5 redesign (Q-20b-2): offline 42 passed 2026-09-17, release native (§6); live 78-cell re-run pending. |
| C-010 | The live tier re-runs Spark and asserts repark == pinned golden == live Spark for every cell. | Same pin file, live tier under `REPARK_PARITY_LIVE=1`. | PROVEN | 81 passed (42 offline + 39 live) 2026-09-17 via jb-jvm.sh lock, PySpark 4.1.2 from /tmp/sparkenv mixed with the lane native (§6). |
| C-011 | ID-1 is rewritten to the new truth (dated 2026-09-17, ICE-MIXED-CASE-1 round 5); `cross_door_identifier_case_folding_agrees_unquoted_and_diverges_quoted` pins the new per-door truth; any still-divergent shape is its own row with its pin. | Registry diff + `cross_door.rs` diff. | PROVEN | `docs/spark-sql-iceberg-parity.md` ID-1 rewritten (false FIXED, `true` unquoted-exact DECLARED refusal, ANSI INTENDED split); `cross_door.rs` ROW 8 asserts ANSI-refuses / Spark-resolves; `cargo test -p repark-sql` pending re-run (§6). Round 21b: ID-1 restated (42704, V-01/V-02/V-04 coverage, the F-DML-FIELD-ID-1 xfail) and row ID-1a added (case twins: FIXED references, DECLARED twin Iceberg tables). |
| C-012 | Full gates green: new pin file, live tier, `cargo test -p repark-spark --lib`, `cargo test -p repark-sql`, `make verify`, whole facade suite, whole parity suite. | Gate outputs pasted below. | OPEN | Pins 42 offline + 81 live; core 591 / spark 1043 / iceberg 435 / sql all binaries; `make ci` green; facade r5b 9368 passed; parity r5 757 passed (§6). Round 21b: re-opened. The code changed; this round's gates are in §9 step 9, and the whole facade and parity suites are the orchestrator's run. |
| C-013 | V-01: a SELECT-list column aliased to its own case-variant spelling folds (`SELECT userId AS USERID`, `userId + 1 AS USERID … ORDER BY USERID`, `COUNT(userId) AS USERID … HAVING USERID > 0`, `USERID AS USERID`); alias references elsewhere stay unfolded. | `test_measured_query_cells_answer_spark[V01_*]` vs `measured_21b` cells; Rust `v01_select_alias_of_the_same_name_folds_the_aliased_column`. | PROVEN | Step 2 `81e7ae3e`; the four V01 cells and both Rust `v01_*` pins green on the release native (§9, step 9 gates). |
| C-014 | V-02: every relation's stored fields feed the fold, scope by scope; an outer spelling is never rewritten into an inner scope. | `test_measured_query_cells_answer_spark[V02_*]`; Rust `v02_every_relation_folds_not_only_the_first_miss`, `v02_outer_spelling_is_not_rewritten_into_an_inner_scope`. | PROVEN | Step 3 `b1771781`; V02 cells and both Rust `v02_*` pins green (§9, step 9 gates). |
| C-015 | V-04: JOIN USING folds in every statement that carries a query (INSERT … SELECT … JOIN … USING). | `test_measured_query_cells_answer_spark[V04_join_using_select]`, `test_measured_join_using_insert_answers_spark`; Rust `v04_join_using_folds_inside_insert`. | PROVEN (fold) | Step 4 `4d61a077`; `V04_join_using_select`, `test_join_using_insert_folds_and_writes_the_left_columns` and Rust `v04_join_using_folds_inside_insert` (full rows on MemTables) green. The measured INSERT cell's `y` column is a strict xfail on fork ask F-DML-FIELD-ID-1, pre-existing on `origin/main` (§7). |
| C-016 | L-08: any reference (bare, qualified, exact or case-variant) to a name with an ASCII case twin in the node's input refuses `[AMBIGUOUS_REFERENCE]` / `42704`, one option per matching field in the requested spelling (Q-21b-1, Q-21b-2); a Spark-written twin Iceberg table refuses at adoption (DECLARED). | `test_case_twin_reference_is_ambiguous_exact_or_not`, `test_measured_case_twin_table_refuses_at_adoption[L08_*]`, `test_sql_door_ambiguous_reference_matches_spark_shape`; Rust `l08_*`, `case_only_collision_raises_the_spark_sentence`, `join_collision_on_bare_reference_raises`; `name_resolution.rs` `write_side_case_twins_are_ambiguous`. | PROVEN | Step 5 `5b131bce`; `test_case_twin_reference_is_ambiguous_exact_or_not` (4), `test_sql_door_ambiguous_reference_matches_spark_shape` (2), adoption refusal (4), Rust `l08_*` + collision pins green; registry ID-1a (FIXED references, DECLARED twin Iceberg tables). Round 2: the hollow `l08_every_reference_to_a_case_twin_is_ambiguous` is deleted (C-021 carries the teeth), and correlated references are covered. |
| C-019 | Q-21b-11: the three N01 IN-subquery spellings and the scalar-subquery cell never answer silently. Spark answers the IN cells `[[1],[2]]` through the correlated outer column; RePark refuses them loudly with a typed `UnsupportedOperationException` (DECLARED, ID-1b). The scalar cell refuses on both engines. | `test_correlated_in_subquery_select_list_refuses_where_spark_answers[N01_*]` (3), `test_correlated_scalar_subquery_select_list_refuses_like_spark`, against `measured_21b_r2`. | PROVEN (DECLARED) | §10 step 2, `4752784b`. The lowercase control refuses the same way with no fold, so the refusal predates this unit. |
| C-020 | Q-21b-12: `SELECT *` over a twin frame answers `['id','ID']` where Spark refuses 42711 (DECLARED, ID-1a). The unquoted twin view DDL refuses. | `test_star_over_a_case_twin_frame_answers_both_columns_declared`; Rust `n03_star_over_a_case_twin_answers_both_columns_declared`. | PROVEN (DECLARED) | §10 step 4, `b6279dd8`. |
| C-021 | N-02: the step-5 audit hunk has a pin that goes red when the hunk is reverted, and correlated references to a twin refuse 42704. | Rust `l08_qualified_reference_is_not_ambiguous_because_a_bare_spelling_appears_elsewhere`, `l08_correlated_reference_to_a_case_twin_is_ambiguous`; Python `test_case_twin_reference_is_ambiguous_exact_or_not` (correlated cell). | PROVEN | §10 step 3, `7d0239a4`. Red under the revert, and red on the round-1 head for the correlated cells. |
| C-017 | R-02 / V-03: when no referenced schema holds an upper-case ASCII field the audit and the AST clone are skipped; the audit that runs indexes each node schema once. Before/after cost measured on a default-profile release native. | Rust `r02_lowercase_only_plans_skip_the_audit`; timing in §9 step 6. | PROVEN | `53514646`; default-profile timing 381.5/393.2 µs → 377.1/365.4 µs per `sql()` (§9). |
| C-018 | R-03: MERGE fragment scopes come from loaded schemas, not two `SELECT * … LIMIT 0` plans. | `merge_fragments.rs` diff; MC-MRG-01…04 stay green. | PROVEN | Step 7: target from Iceberg metadata, named source from its provider schema; a subquery source keeps one LIMIT 0 plan (no metadata exists). MC-MRG-01…04 + `test_merge_insert_scope.py` green on the release native (§9). |

## 1. Red-first record (base `32c0e1a3`, release native in `.venv`, 2026-09-17)

Round-1 record: the pin file went in before the fix and ran against the
unfixed release native. Wrong-case references refused loud (`Schema error:
No field named ...`), which is the shape the fix removes. Offline state at
round-1 end: 35 passed / 4 failed; the 4 failures were the MERGE cells,
blocked on Rust E0063 (`case_insensitive` missing in the `MergeSpec`
initializer) — the stale native could not run the new lowering. Round 2
closed E0063 (Spark-door `true` placeholder overwritten from the carrier at
execution; ANSI-door `false`) and re-ran the pins (see §6).

## 2. Work log

### 2026-09-17 — open

- Branch `fix/ice-mixed-case-1` from main verified at `32c0e1a3`. Ledger opened.
- Read AGENTS.md read-first path, docs/testing.md, `crates/repark-spark/map.md`,
  `crates/repark-spark/src/map.md`, registry §3 ID-1/ID-2/ID-3, the defect probes
  (`p_read_edges.py` mixed-case block, `common.py`), and the fixture/oracle
  conventions (`test_ice_spark_table_1.py`, `_oracle_pins.py`,
  `test_v3_live_oracle.py`, `cross_door.rs`, `ice_spark_table_1` fixture).

### 2026-09-17 — round 2

- Closed E0063: `MergeSpec` initializer gains `case_insensitive` in
  `repark-spark/src/merge.rs` (`true` placeholder, overwritten from the
  carrier at execution) and `repark-sql/src/merge.rs` (`false`).
- Removed the three added comment lines the audit flagged (no replacements in
  code; the ROW 8 / design §2 Q10 fact lives in §5 above).
- `truth.json` beside the torture fixture is the writer-record, not a second
  answer file; no dedupe needed.
- Identity DELETE/UPDATE execute before the repair loop, so their specs now
  take the flag at the execution site: carrier value on the Spark door
  (`spark_ast.rs`), `false` on the ANSI door (`router.rs`).
- Size gate: resolution logic consolidated into shared helpers instead of
  growing three exact-baseline files. `name_resolution.rs` (no baseline)
  gains `resolve_arrow_field` (exact-or-first-insensitive, ambiguity in the
  stored schema resolves to missing, as before), `dedup_key`, and the
  `resolve_scoped` index method; `merge/mod.rs`, `insert.rs`,
  `not_matched_by_source.rs`, and `predicate_dml.rs` delegate to them.
  `repark-spark/src/merge.rs` splits MERGE fragment preprocessing into
  `merge_fragments.rs` (1000-line default ceiling holds at 999).
  `merge/tests/` shared constructors (`spec`, `update`, `delete`) move to
  `tests/helpers.rs`; `streaming_scan.rs` delegates to it;
  `predicate_dml/tests` collapses `identity_spec` into an `IN_SELECTION`
  const (precedent: `NOT_IN_SELECTION`). Baselines ratcheted down:
  `merge/mod.rs` 1792→1783, `merge/tests/merge.rs` 1065→1032,
  `merge/tests/streaming_scan.rs` 3028→3020, `predicate_dml.rs` 1142→1141,
  `predicate_dml/tests` 1442→1440, `cross_door.rs` 1258→1254.

## 3. Probe reproduction (release native, unfixed tree)

`/tmp/ib-scratch/probes/p_mixed_case.py` (run with `/tmp/ib-scratch/run-probe.sh`)
adopts a Spark-written mixed-case table and issues wrong-case references on
both doors. Unfixed result, both doors: `SELECT userId` folds to `userid`
and refuses (`No field named userid`); backticked `` `USERID` `` refuses;
`caseSensitive=true` refuses. Spark 4.1.2 resolves all three under the
default flag. Defect probes `p_read_edges.py` / `common.py` in the same
directory.

## 4. Spark oracle recording (PySpark 4.1.2, single JVM)

`python/repark/tests/_record_ice_mixed_case_1.py` recorded
`python/repark/tests/ice_mixed_case_1_spark_oracle.json` verbatim from one
PySpark 4.1.2 JVM: every cell under `spark.sql.caseSensitive` false and
true (rows + columns on success; error class + message + SQL on refusal,
including the `[AMBIGUOUS_REFERENCE]` shape). The adopted table itself is
committed as `python/repark-parity/fixtures/torture/data/ice_mixed_case_1/`
(table root + `map.md` + `truth.json`), written 2026-09-17 by PySpark 4.1.2
+ iceberg-spark-runtime-4.1_2.13:1.11.0; that directory's `truth.json` is the
fixture writer-record (seed rows, schema, location — the `ice_spark_table_1`
contract), not a second answer file. The single answer source is the oracle
JSON the pin file reads.

## 5. Implementation

`crates/repark-core/src/column_resolution.rs` (new): the
`ColumnResolutionConfig` carrier (`SPARK_SQL_CASE_SENSITIVE_KEY`), the
statement repair loop `plan_statement_with_column_repair` (plan → on
`FieldNotFound` rewrite the one missing reference against the valid fields,
case-insensitively, qualifier-aware, 64-attempt cap; more than one
case-insensitive match raises the Spark `AMBIGUOUS_REFERENCE` shape; an
ambiguity audit also covers already-resolved plans), and
`rewrite_fragment_case` for DML fragments against known scopes.
`crates/repark-spark/src/extension.rs` installs the carrier from the builder
conf and turns ident normalization off on the Spark door only.
`crates/repark-spark/src/merge.rs` threads `MergeSpec.case_insensitive` and
overwrites it from the carrier at execution; `crates/repark-sql/src/merge.rs`
passes `false`.
`crates/repark-iceberg/src/write/merge/` (`mod.rs`, `insert.rs`,
`not_matched_by_source.rs`), `name_resolution.rs` (`resolve_exact`), and
`predicate_dml.rs` / `plain.rs` thread the flag through MERGE ON/SET/INSERT
and identity DELETE/UPDATE validation — name resolution only, no executor or
commit-path change.
`crates/repark-spark/src/spark_ast.rs` (round 2) stamps the carrier value
onto identity-DML specs, which execute before the repair loop;
`crates/repark-sql/src/router.rs` (round 2) stamps `false` (ANSI exact).
`crates/repark-python/src/session_runtime.rs` + `builder_conf.py` +
`session_configuration.py` + `sql_set_statements.py` plumb
`spark.sql.caseSensitive` from builder conf and runtime `conf.set` / `SET`
into the live carrier. Registry ID-1 rewritten (Spark-door FIXED, ANSI-door
INTENDED split per G11 Option A); `cross_door.rs` ROW 8 (design §2 Q10
identifier-case-folding pin) now asserts ANSI-refuses / Spark-resolves;
guides `sql-doors.md` / `dataframe-guide.md` updated.

## 6. Gates

- `test_ice_mixed_case_1.py` offline: 39 passed (release native).
- `test_ice_mixed_case_1.py` live tier (`REPARK_PARITY_LIVE=1`, Java 17,
  `ib-jvm.sh` lock): 78 passed — repark == recorded golden == live Spark
  4.1.2 on every cell and flag. Live-tier repairs in this unit: the live
  error recipe now stringifies with `traceback.format_exception_only` like
  the record script (the `str(exc)` wrapper prefix broke all 21 error-cell
  comparisons); the live tier replays the record's `CREATE TEMP VIEW` DDL
  verbatim (`_LIVE_SETUP_SQL`) instead of DataFrame registration; the live
  table is re-seeded before every cell (record-definition order vs pytest
  alphabetical order left mutated state behind for later read cells).
- `cargo test -p repark-core --lib`: 584 passed, 1 ignored.
- `cargo test -p repark-iceberg --lib`: 434 passed.
- `cargo test -p repark-spark --lib`: 1043 passed, 4 ignored.
- `cargo test -p repark-sql`: 18 binaries green, 0 failures.
- `scripts/check_rust_file_size.py`: clean (606 files; merge.rs at the 1000
  default ceiling).
- `check_lib_py.sh`, `check_python_conventions.py`,
  `check_docstring_presence.py`: exit 0.
- Round-2 correctness finds (each a failing gate test, fixed same round):
  identity DELETE/UPDATE specs take the flag at the execution site (they run
  before the repair loop); identity DELETE selections are canonicalized
  against the target schema door-side (the raw `WHERE` reached execution
  unrepaired); the statement audit now only flags user-written references
  (star expansion and cross-node leakage false-fired) and only
  case-variant collisions (same-name scope duplicates defer to DataFusion
  scoping); the fragment visitor handles Databricks `CompoundFieldAccess`
  for quoted qualifiers and strips quote characters from scope aliases.
  Known residue: the audit does not flag a qualified exact-match reference
  when the same scope also holds a case-variant (e.g. `SELECT t.ID` with
  `t.id` + `t.ID` first-wins silently); no pin names that shape.
- Round-2 continued (2026-09-17, still uncommitted): orchestrator comment-ban
  fix (one line in `merge/mod.rs`, baseline ratcheted 1783 to 1782); 9 clippy
  `all-targets` errors in `column_resolution.rs` fixed (derived Default, match
  guards, let-chains, `clone_from`, `Function` variant, tests module last,
  sync helper) plus one needless-borrow in `spark_ast.rs`; 7 ruff errors in
  the unit pin/record scripts fixed (`strict=True`, import sort, E501 wraps).
  `make py-test-facade` (debug native): 2 failures, both mine, both untriaged
  to a fix: `test_not_matched_bare_column_resolves_to_source` (fragment audit
  raises AMBIGUOUS_REFERENCE for bare `score` in WHEN NOT MATCHED, where Spark
  resolves source-only) and
  `test_logical_names_equal_analyzed_names_for_dataframe_transforms[filter]`
  (`F.col("Id")` filter plans `nums.id`; origin-map path untouched by this
  unit, needs base comparison). Full-suite pass count not captured (tail-only
  log). Parity suite (`make py-test`) not run. All C-001…C-012 still OPEN.
- Round 3 (2026-09-17): WIP `2c948a80` committed first; the WHERE-cell ids
  renamed to the `MC-WHERE-*` spelling in the record script, oracle JSON, pin
  file and ledger (typos gate read the old trigram as a word). Q-20b-1
  (ADOPTED): `rewrite_fragment_case`
  gains `unqualified_scope` — NOT MATCHED [BY TARGET] fragments resolve bare
  refs against the source alias only, NOT MATCHED BY SOURCE against the target
  alias only, MATCHED/ON against both; qualified refs still validate against
  both scopes. New Rust pin `fragment_rewrite_scopes_bare_references_to_one_side`.
  `test_logical_names_equal_analyzed_names_for_dataframe_transforms[filter]`
  is SUSPECTED PRE-EXISTING, not this unit's: `F.col("Id"/"id"/"ID")` filters
  fail identically under `SET spark.sql.caseSensitive = true` (the unit's path
  bypassed) and the traceback (`core.py count` → `eager.py _count_rows` →
  native) never enters unit code; the facade origin-map/engine-name files are
  untouched by this branch. The pinned answer (names agree) remains Spark's
  answer, so no HALT. The orchestrator checks it on the pristine tree.
- Facade r3 (2026-09-17, release native, full log
  `/tmp/oc-worker/ib-build/facade-r3.log`): 2 failed, 9313 passed, 406
  skipped, 26 xfailed in 2139 s. Failure 1 is the suspected-pre-existing
  filter pin above (`test_merge_insert_scope.py` fully green, 4 passed —
  the Q-20b-1 fix holds). Failure 2 is this unit's and is fixed in-tree:
  `test_moved_symbol_bodies_match_the_integrated_baseline` pinned the old
  `_SQLCONF_DEFAULTS` body hash; the `spark.sql.caseSensitive` default row
  moved it `c37ad587…` to `c79dabbc…`, and the EXPECTED table carries the new
  hash (re-run: 1 passed).
- Parity r3 (2026-09-17, full log `/tmp/oc-worker/ib-build/parity-r3.log`):
  1 failed, 756 passed, 2 skipped, 12 xfailed in 461 s. The failure is this
  unit's and is fixed in-tree: the CAP-1 mirror `_RUST_BASELINES` still
  carried the six pre-ratchet ceilings; it now matches `check_rust_file_size.py`
  (1782/1032/3020/1141/1440/1254) and the mirror file re-runs 23 passed.
  The mirror table is referenced by name only elsewhere (wiring + packet
  lists, both green in the full run).
- Round 5 / Q-20b-2 redesign (2026-09-17, release native rebuilt after every
  Rust edit): normalization stays ON (the `enable_ident_normalization = false`
  line is out of `extension.rs`); the fold is plan → single fold → replan plus
  the plan ambiguity audit, no retry loop. Red-first on the redesign: the
  carried-over pins failed 8 (MC-MRG-02/03/04 under false — the fold emitted
  double-quoted `t."userId"`, which the session dialect refuses to parse after
  a dot; MC-SEL-01/MC-UPD-01/MC-MRG-01 + the SET-statement test under true —
  the declared unquoted-exact refusal). Fixes in the same round: fold and
  fragment rewrites emit backticks (R-20b-1; double-quoted spans are string
  literals on this door per ID-2); true-flag pins rewritten to the declared
  contract (backticked success vs oracle `true` rows, unquoted refusal pinned).
  Offline after the fix: 42 passed, 39 skipped (live tier) on the release
  native. `test_perf_facade_logical_names.py` + `test_merge_insert_scope.py`:
  27 passed (the round-4 filter regression is gone with normalization ON).
  Also fixed in round 5: the fold's SELECT-alias guard compared
  case-insensitively, so `SELECT `ID` AS id` skipped the fold and `cross_door`
  ROW 8 reddened; the guard is now exact-case (pinned by
  `select_alias_does_not_shadow_wrong_case_column`), ROW 8 green.
  Final round-5 numbers on the release native: pins offline 42 passed; live
  tier 81 passed (42 + 39) via the jb-jvm.sh lock, PySpark 4.1.2 from
  /tmp/sparkenv mixed with the lane native over PYTHONPATH (both 3.12.3);
  `cargo test -p repark-core --lib` 591 passed, 1 ignored (16 fold tests);
  `cargo test -p repark-iceberg` 435 passed; `cargo test -p repark-spark
  --lib` 1043 passed, 4 ignored; `cargo test -p repark-sql` all 18 binaries
  green (incl. `cross_door` 23 passed); the pinned clippy gate green after 9
  pedantic fixes in `column_resolution.rs`; `cargo fmt --check` clean;
  `check_rust_file_size.py` clean (611 files; `merge/mod.rs` 1782→1780,
  `predicate_dml.rs` 1141→1139, fold tests split 984 + tests file);
  `check_lib_py.py` clean; ruff check + format clean; `make ci` EXIT 0;
  facade r5b 9368 passed, 406 skipped, 26 xfailed
  (`/tmp/oc-worker/ib-build/facade-r5b.log`); parity r5 757 passed, 2 skipped,
  12 xfailed (`/tmp/oc-worker/ib-build/parity-r5.log`). One `make test` red:
  `listing_cost_list_tables_cheaper_than_provider_rebuild` (a wall-clock ≤2×
  comparison on an unrelated surface) failed once under the parallel
  facade+parity load and passes isolated and in the quiet-box full lib re-run
  (435 passed) — the same flake→pass shape as round 3.

## 9. Round 21b (claude-opus-5, 2026-09-17)

### Step 0 — rebase onto `origin/main` `71482620`

Two conflicts on the round-2 commit (`session_runtime.rs`, `builder_conf.py`), both
from main's own `spark.sql.caseSensitive` carrier; resolved to main's side and the
unit's duplicate carrier deleted (R-21b-1). No conflict markers in the tree.
Release native rebuilt (codegen-units 16). `test_ice_mixed_case_1.py` 42 passed,
39 skipped; `cargo test -p repark-core --lib` 590 passed, 1 ignored (the three
carrier-parse tests left with the carrier).

### Step 1 — red-first on the unfixed tree

Measured cells: `python/repark/tests/ice_mixed_case_1_spark_oracle.json`
`measured_21b.cells` is `/tmp/oc-worker/kb-oracle/mc-truth.json` verbatim
(checked equal). Spark's twin-table `v3.metadata.json` is committed beside the
fixture (`twin_v3.metadata.json`).

Probe of RePark on the fixture shapes before any fix: `SELECT userId AS USERID`,
`userId + 1 AS USERID … ORDER BY USERID`, `COUNT(userId) AS USERID … HAVING`
already answer on the Spark door (the Databricks-dialect parse keeps `userId` and
`USERID` distinct, so the exact-match alias guard does not fire); `USERID AS
USERID` fails (`No field named userid`). V-02's recorded statement answers
(the outer miss happens to be reported first); V-04's SELECT answers, its INSERT
fails. RePark cannot create or adopt a case-twin Iceberg table at all: `CREATE
TABLE … (`id` INT, `ID` INT)` and `register_table` on Spark's twin metadata both
refuse with `DataInvalid => Cannot build lower case index: id and ID collide`
(the fork's schema index).

Rust `cargo test -p repark-core --lib column_resolution` — 10 passed, 8 FAILED:

```
case_only_collision_raises_the_spark_sentence: … could be: [`t`.`id`]. SQLSTATE: 42702
join_collision_on_bare_reference_raises: … could be: [`amb_l`.`a`, `amb_r`.`a`]. SQLSTATE: 42702
l08_bare_twin_options_carry_the_full_relation_name: … could be: [`tw_full`.`ID`]. SQLSTATE: 42702
l08_every_reference_to_a_case_twin_is_ambiguous: SELECT t.ID FROM tw AS t: … could be: [`t`.`ID`]. SQLSTATE: 42702
v01_select_alias_of_the_same_name_folds_the_aliased_column: FieldNotFound userid (valid mc.userId, mc.eventName)
v02_every_relation_folds_not_only_the_first_miss: FieldNotFound eventname (valid mc.userId, …)
v02_outer_spelling_is_not_rewritten_into_an_inner_scope: FieldNotFound "USERID" (valid mc.userId, mc.eventName) — the inner miss's fold rewrote the outer `userid` to `USERID`
v04_join_using_folds_inside_insert: FieldNotFound userid (valid ja.userId, ja.x)
```

Python `test_ice_mixed_case_1.py` — 50 passed, 8 FAILED, 39 skipped:

```
test_sql_door_ambiguous_reference_matches_spark_shape[MC-AMB-01]: … [`amb_l`.`a`, `amb_r`.`a`]. SQLSTATE: 42702
test_sql_door_ambiguous_reference_matches_spark_shape[MC-AMB-02]: … [`amb_l`.`A`, `amb_r`.`A`]. SQLSTATE: 42702
test_measured_query_cells_answer_spark[V01_alias_upper_of_upper]: Schema error: No field named userid. Valid fields are mc21.ns.mc."userId", mc21.ns.mc."eventName".
test_measured_join_using_insert_answers_spark: Schema error: No field named userid. Valid fields are mc21.ns.ja."userId", mc21.ns.ja.x.
test_case_twin_reference_is_ambiguous_exact_or_not[SELECT t.ID …]: … could be: [`t`.`ID`]. SQLSTATE: 42702
test_case_twin_reference_is_ambiguous_exact_or_not[SELECT ID …]: … could be: [`twv`.`ID`]. SQLSTATE: 42702
test_case_twin_reference_is_ambiguous_exact_or_not[SELECT id …]: … could be: [`twv`.`id`]. SQLSTATE: 42702
test_case_twin_reference_is_ambiguous_exact_or_not[SELECT t.id …]: … could be: [`t`.`id`]. SQLSTATE: 42702
```

Green on the unfixed tree and kept as regression pins: the other three V01
cells, both V02 cells, V04 SELECT, and the four
`test_measured_case_twin_table_refuses_at_adoption[L08_*]` cells (RePark's
declared answer: refusal at adoption). Name comparison on the measured cells
is case-insensitive, the module's existing convention for the declared
output-spelling divergence.

### Steps 2–5 — the fixes

- **Step 2, V-01** (`81e7ae3e`): the SELECT-alias guard is positional. Each query
  level records its projection expressions (always foldable) and its
  alias-reference slots — GROUP BY, HAVING, QUALIFY, SORT BY, ORDER BY. Only an
  ident inside an alias-reference slot that names a SELECT alias is left for
  DataFusion to bind to the alias. That match is ASCII case-insensitive, like
  Spark's alias resolution. Before this, the guard compared exact spellings and
  applied everywhere, so an ident spelled like its own alias never folded. A
  case-insensitive guard was unsafe then because it also covered the projection
  (cross_door ROW 8), and a projection slot can no longer be shielded. WHERE
  and JOIN ON are not alias-reference positions: they always fold, because
  Spark does not resolve SELECT aliases there. New pin
  `v01_order_by_a_select_alias_still_orders_by_the_alias` keeps ORDER BY on
  the alias.
- **Step 3, V-02** (`b1771781`): the fold moved to `column_resolution/fold.rs`,
  and a miss now drives a loop:
  - The first `FieldNotFound` resolves every relation the statement names
    through the session catalog, once (`catalog_fields`). Each miss's
    `valid_fields` is absorbed by qualifier, which covers CTEs and derived
    tables.
  - The fold resolves each ident against its own SELECT's relations first,
    then outward for correlated references. A relation whose fields are
    unknown stops the outward search.
  - Loop: replan, and stop when a miss repeats or a fold changes nothing. There
    is no attempt cap.

  The fold's collision sentence also moved to one option per field
  (Q-21b-1/2), because the new fold raises it.
- **Step 4, V-04** (`4d61a077`): JOIN USING columns now fold in
  `pre_visit_query`. That runs for every query the visitor reaches: INSERT …
  SELECT, CTAS, subqueries and CTE bodies, each against its own SELECT's
  relations. When every matching relation stores the same spelling
  (`ja.userId`, `jb.userId`), the ident folds to that spelling. DataFusion
  still raises its own ambiguity where one exists.
- **Step 5, L-08**: the audit indexes each node's input fields once. It walks
  every input schema with `DFSchema::iter`, with no `merge` and no `columns()`
  clone, and keeps only lowercase keys that hold two or more distinct
  spellings. A node whose inputs hold no upper-case ASCII field skips the index
  entirely.

  The audit fires on the resolved column whenever the input holds a twin of
  it. How the user wrote the reference only picks the requested spelling and
  the reference form: a qualified write (`t.ID`) keeps the qualifier and
  filters the twins to that relation, and a bare write (`id`, exact case
  included) matches every twin. An expression the user never wrote (a star
  expansion) is not audited. Options are one per matching field, qualified by
  the field's relation parts (`` `t` `` for an alias, `` `cat`.`ns`.`tbl` ``
  for a full name), in the requested spelling, with `SQLSTATE: 42704`. The
  write-side `ambiguous_write_message` (`name_resolution.rs`) follows
  Q-21b-1/2 too.

  `SELECT * FROM tw` (Spark `[COLUMN_ALREADY_EXISTS]` / `42711`): RePark's
  answer on the measured Iceberg shape is the adoption refusal pinned by
  `test_measured_case_twin_table_refuses_at_adoption[L08_star_twin]`. The fork
  cannot parse a twin schema, and a fork change is out of this unit's scope
  (fork rule), so this is a DECLARED registry row, not silence. A star over a
  twin frame is not audited: it is not a written reference, and Spark's answer
  on a frame was not measured.

### Step 6 — R-02 / V-03 early exit, measured

Code in `53514646`. Cost of one trivial Spark-door statement,
`spark.sql("SELECT id FROM nums")` over a `spark.range(8)` temp view (lowercase
schema, first-time plan success). Timed in Python with `time.perf_counter_ns`
around `session.sql(...)`: 300 warm-up calls, then 7 rounds of 1000 calls,
reporting the median of the round medians, plus 500 calls of `.collect()`.
Both wheels were built on the **DEFAULT release profile** (`lto = "thin"`,
`codegen-units = 1`; `CARGO_PROFILE_RELEASE_CODEGEN_UNITS` unset) with
`maturin build --release` and installed in fresh venvs on the same Python
3.12.3. Script: `/tmp/kb-mixed-scratch/timing.py`.

| Build | `sql()` median | `sql().collect()` median |
|---|---|---|
| before — `28b6ff00` (the reviewed Q-20b-2 design + red pins, no early exit), run 1 | 381.5 µs | 959.7 µs |
| after — `53514646` (step 6), run 1 | 377.1 µs | 961.5 µs |
| before, run 2 | 393.2 µs | 1006.5 µs |
| after, run 2 | 365.4 µs | 920.3 µs |

The box was shared (load average 65–80 on 64 cores from other lanes), so the
run-to-run spread (±3%) is about the size of the effect. Read the numbers as:
the audit plus the AST clone were ~4–7% of a trivial statement's
`sql()`, and the early exit removes that on lowercase-only schemas. There is no
regression. `collect()` is dominated by execution.

### Step 7 — R-03

`merge_fragments.rs` no longer plans `SELECT * FROM <target> LIMIT 0`. Target
field names are read from `catalog.load_table(&spec.target)` →
`metadata().current_schema()`, using the handle `merge.rs` now resolves before
the rewrite. A named source (`USING updates s`) reads
`ctx.table_provider(..).schema()`. A subquery source still plans one `SELECT *
FROM (…) AS s LIMIT 0`: it is an expression with no metadata to read, and
reaching its schema otherwise would mean planning it anyway. What remains:
`repark_iceberg::write::merge::execute_merge` loads the target table once more
itself. Passing the loaded `Table` in would change that function's signature on
both doors (Spark and ANSI `repark-sql/src/merge.rs`), which is a
repark-iceberg API change, not a fork change; left for a follow-up and recorded
here. Replacing a full `statement_to_plan` (catalog resolve + SqlToRel + table
load) with one metadata load is strictly less work. Pins after the change, on
the release native: `test_ice_mixed_case_1.py` + `test_merge_insert_scope.py`
62 passed, 39 skipped, 1 xfailed.

### Step 8 — registry and rulings

`docs/spark-sql-iceberg-parity.md`:
- ID-1: restated for round 21b — the V-01 / V-02 / V-04 coverage, `42704`, the
  `measured_21b` oracle, the pins, and the F-DML-FIELD-ID-1 strict xfail.
- New row ID-1a (ASCII case-twin columns): FIXED for references with the
  measured sentence; DECLARED for twin Iceberg tables, which the fork refuses
  to load.

Every `42702` in the tree is gone (registry, `column_resolution.rs`,
`name_resolution.rs`, pins). Q-21b-1 and Q-21b-2 are in the rulings section.
The `caseSensitive=true` unquoted-exact DECLARED refusal stays in ID-1
unchanged.

## 10. Run 21b round 2 (claude-opus-5, 2026-09-18)

Head `fc3c20f6`, not rebased. Release native built with
`CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16` (no timing in this round).

### Step 1 — clippy (`cb60f7b1`)

`clippy::similar_names` in `audit_column`: the written-form hits are now
`relation_hit` / `bare_hit`. `clippy::type_complexity` in the measured fixture:
`type MeasuredTable<'a> = (&'a str, Vec<Field>, Vec<ArrayRef>)`. No `#[allow]`.
`make rust-clippy` is green (4 m 28 s).

### Step 2 — Q-21b-11 (`4752784b`)

The Spark cells were copied from `/tmp/oc-worker/kb-oracle/probe_mc2.log` into
the oracle's new `measured_21b_r2` block without re-deriving them. The probe
stopped at the N03 view before it wrote `mc-truth-2.json`, so the log is the
record.

RePark (release native, `mc21` memory catalog, the probe's `mc` / `other`):

| Statement | RePark | Spark |
|---|---|---|
| `SELECT USERID FROM mc WHERE EVENTNAME IN (SELECT EVENTNAME FROM other)` | `UnsupportedOperationException: … Physical plan does not support logical expression InSubquery(…)` | `[[1],[2]]`, `USERID` |
| same, `userId` / `eventName` | same refusal | `[[1],[2]]`, `userId` |
| same, `userid` / `eventname` | same refusal | `[[1],[2]]`, `userid` |
| `SELECT userId, (SELECT max(EVENTNAME) FROM other) AS m FROM mc` (and `max(eventName)`) | `… does not support logical expression ScalarSubquery(<subquery>)` | `CORRELATED_REFERENCE`, `0A000` |
| all-lowercase control `lc.ns.mc(userid, eventname)`: `… WHERE eventname IN (SELECT eventname FROM other)` | same `InSubquery` refusal (no fold runs) | — |
| control: correlated `EXISTS (… WHERE name = eventname)` / uncorrelated `IN (SELECT name …)` | `[[1]]` / `[[1]]` | — |

The critic's plan was right: the fold produces `outer_ref(mc.eventName)`, which
is Spark's resolution. The plan never executes, though, because the engine does
not decorrelate an outer reference in an IN-subquery projection. That happens on
any schema, so it is not a case defect and not a silent answer. Pinning
`[[1],[2]]` would have needed a planner decorrelation rewrite outside the
fold/audit module, so the refusal is pinned DECLARED (ID-1b) and flagged in §7.
The scalar cell refuses on both engines. The fold creates no answer Spark lacks,
so nothing changes there. The ledger records the choice: "pin RePark's answer
and file a dated registry note".

### Step 3 — N-02 (`7d0239a4`)

Proof that no refusal shape exists which the pre-step-5 walk misses. Write
`hit_q` for "a qualified written reference whose qualifier equals the resolved
relation's table" and `hit_b` for "a bare written reference of the same name,
case-insensitive".
- New walk refuses when either holds:
  - `hit_q` and more than one distinct field matches after the qualifier filter;
  - no `hit_q`, `hit_b`, and more than one distinct spelling in the node's input.
- Old walk refuses when either holds:
  - `hit_q` and `honored > 1`, where `honored` uses the same qualifier filter
    over the same input fields;
  - otherwise, `hit_b` and more than one spelling over the same input fields.

Every refusal of the new walk therefore satisfies one of the old walk's
conditions. The only field-set difference is `DFSchema::merge`, which drops
`(qualifier, field)` pairs that repeat exactly across inputs; that cannot turn
two differently-spelled fields into one. So the old walk refuses everything the
new walk refuses.

The old walk differs in two ways only:
1. It prints options from the resolved relation, not from the FROM text.
   `l08_bare_twin_options_carry_the_full_relation_name` already pins this.
2. It falls through to the bare branch even when `hit_q` holds, so a qualified
   reference that names one field refuses as soon as any bare spelling of the
   name is written elsewhere in the statement.

Measured with the old walk applied temporarily over 16 shapes: subquery alias,
CTE, join, unaliased derived table, 1-/2-/3-part qualifiers, WHERE, EXISTS inner,
a projection alias over a twin, and the over-refusal. Every twin reference
refused under both walks. Two shapes differed from Spark, and neither was a miss
the old walk alone makes:
- `SELECT i.USERID FROM ids i JOIN ja j ON i.USERID = j.userId WHERE j.x IN (SELECT userid FROM jb)`:
  the old walk says `[AMBIGUOUS_REFERENCE] Reference `userid` is ambiguous, could
  be: [`i`.`userid`, `j`.`userid`]`, and the new walk answers. This is difference 2.
- `SELECT 1 FROM tw t WHERE EXISTS (SELECT 1 FROM other o WHERE o.name = CAST(t.ID AS STRING))`:
  both walks answered silently. The correlated `t.ID` is an `OuterReferenceColumn`,
  not an `Expr::Column`, so neither walk looked at it.

What changed:
- `l08_every_reference_to_a_case_twin_is_ambiguous` is deleted. It was hollow, by
  the proof above, and the Python
  `test_case_twin_reference_is_ambiguous_exact_or_not` pins the same four cells
  with the same sentences.
- New pin with teeth:
  `l08_qualified_reference_is_not_ambiguous_because_a_bare_spelling_appears_elsewhere`
  (difference 2). It answers `USERID` / `[]`, and `[[2]]` with `IN (SELECT userid FROM ja)`.
- Fix: `audit_plan_for_ambiguity` now audits each subquery expression's
  `outer_ref_columns` (EXISTS / IN / set-comparison / scalar) against the twins
  of the node that holds the subquery, which is the outer scope.
- New pin `l08_correlated_reference_to_a_case_twin_is_ambiguous` covers three
  shapes: qualified in EXISTS, bare in EXISTS, and IN. The Python twin pin gains
  the EXISTS cell.

Red on the round-1 head, before the outer-reference fix:
```
test column_resolution::tests::l08_correlated_reference_to_a_case_twin_is_ambiguous ... FAILED
thread '…l08_correlated_reference_to_a_case_twin_is_ambiguous' panicked at crates/repark-core/src/column_resolution/tests.rs:58:10:
called `Result::unwrap_err()` on an `Ok` value: Projection(Projection { expr: [Literal(Int64(1), None)], input: Filter(Filter { predicate: Exists(…
test result: FAILED. 0 passed; 1 failed
```
Red with `audit_plan_for_ambiguity` / `audit_column` reverted to the pre-step-5
walk (merged `DFSchema`, honored-vs-bare, `reference_parts`), all other code at
the fixed head:
```
test column_resolution::tests::l08_correlated_reference_to_a_case_twin_is_ambiguous ... FAILED
test column_resolution::tests::l08_bare_twin_options_carry_the_full_relation_name ... FAILED
test column_resolution::tests::l08_qualified_reference_is_not_ambiguous_because_a_bare_spelling_appears_elsewhere ... FAILED
thread '…l08_qualified_reference_is_not_ambiguous_because_a_bare_spelling_appears_elsewhere' panicked at crates/repark-core/src/column_resolution/tests.rs:356:62:
test result: FAILED. 18 passed; 3 failed
```
Tree restored, then 21 `column_resolution` tests green.

### Step 4 — Q-21b-12 (`b6279dd8`)

The star refusal was built first, red-first:
- Wildcard recorded in `WrittenRefs`.
- At each `Projection`, two passthrough columns of the same relation whose names
  are ASCII case twins raise `[COLUMN_ALREADY_EXISTS] The column `id` already
  exists. … SQLSTATE: 42711`.
- Red on the unfixed tree:
  `n03_star_over_a_case_twin_refuses_column_already_exists ... FAILED`
  (`unwrap_err()` on `Ok`).
- Green after the fix: `SELECT * FROM twv`, `t.*` and `CREATE TEMP VIEW … AS SELECT * FROM twv`
  refuse 42711, and a cross-relation join star (`a` / `A`) still answers.

It then failed `test_filter_predicate_rewrite.py`, five cells:
- `test_unambiguous_column_still_filters_on_a_case_colliding_frame[filter]`
  and its siblings, and `test_ambiguous_reference_error_uses_the_spark_message_shape`.
- Cause: the DataFrame door's `filter` on a `createDataFrame([...], ["id", "ID", "other"])`
  frame lowers to a `SELECT * …` through the same resolution path.
- `spark.table("twv")` refused the same way.
- Spark answers those DataFrame calls.

Confining the refusal to user SQL would mean threading a door flag in from the
callers, which is outside the fold/audit module. So the refusal was reverted,
per the ruling's alternative. Pinned instead: RePark's answer (`['id','ID']`,
`[[1, 0]]`) beside both recorded Spark 42711 cells (`L08_star_twin`,
`N03_star_twin_view_create`). The unquoted twin view DDL refuses with the
engine's `Projections require unique expression names`. Registry ID-1a carries
the DECLARED bullet.

## 7. Open questions (HALT writes here; empty means none)

No HALT. Round 2 (2026-09-18) hands the orchestrator three items:

1. **Q-21b-11 divergence from the brief's expected pin.** The brief ruled that the
   three N01 IN cells be pinned as answering `[[1],[2]]`. They refuse on RePark
   at physical planning. It is an engine decorrelation gap that predates this
   unit and is independent of case (§10 step 2), so the lane pinned the loud
   refusal as DECLARED (ID-1b) rather than invent a planner rewrite. The decision
   needed is whether to open a planner unit that decorrelates an outer reference
   in an IN-subquery projection (`x IN (SELECT outer.y FROM s)`).
2. **Spark cells this round relied on without measuring** (for the next probe):
   - (a) `SELECT i.USERID FROM ids i JOIN ja j ON i.USERID = j.userId WHERE j.userId IN (SELECT userid FROM ja)`
     is expected to answer `[[2]]`; C-021's teeth pin assumes it.
   - (b) `SELECT 1 FROM tw t WHERE EXISTS (SELECT 1 FROM other o WHERE o.name = CAST(t.ID AS STRING))`
     on the twin table is expected to refuse `42704`.
   - (c) `createOrReplaceTempView` of a twin frame from the DataFrame door, and
     `spark.table(...)` on it. Is it 42711 at view creation, as the SQL DDL is?
3. The earlier INS-JOIN-FIELD-ID item below is unchanged.

One out-of-scope defect found in round 21b, handed to the orchestrator:

- **INS-JOIN-FIELD-ID (P1, silent wrong answer, pre-existing on `origin/main`
  `71482620`, not a case defect) — a new shape of the open fork ask
  F-DML-FIELD-ID-1** (ICE-RTAS-BYNAME-1 ledger: the fork's id-first
  `batch_column_index` misroutes a DML batch whose field ids come from a
  differently-ordered source table; here `b.y` carries field id 2 and the
  target expects `y` at id 3). `INSERT INTO t SELECT a.k, x, y FROM a JOIN b
  ON a.k = b.k` with `a`, `b`, `t` all Iceberg tables and all-lowercase names
  writes `y = NULL`. The SELECT alone answers `[[1, 10, 100]]`. Reproduced on
  this branch's native, on the reviewed PR-head wheel (`28b6ff00`) and on the
  `/tmp/kc-misc` native built at `71482620` (only test files dirty). The
  following all write the right value: `y + 0 AS y`, frame temp views in place
  of `a`/`b`, and `vb.y` from a frame view. The lead is that the right-side
  Iceberg column arrives carrying its source `PARQUET:field_id` (`b.y` is field
  id 2, the same id as `a.x`) and is lost between the join output and the
  write; stripping the metadata avoids it. Registry ID-1's Pin bullet names it. Out of this unit's surface (the
  write path, not name resolution), so no fix here.
  `test_measured_join_using_insert_answers_spark` asserts the measured V-04
  INSERT cell under `xfail(strict=True)` naming this defect, and turns red when
  the defect is fixed.
  `test_join_using_insert_folds_and_writes_the_left_columns` and the Rust
  `v04_join_using_folds_inside_insert` (MemTables, full rows) keep the V-04
  fold green. Probe: `/tmp/kb-mixed-scratch/probe5.py`, `probe6.py`.

## 8. Coverage attestation

Withdrawn in round 21b step 1 and not restored in step 8: C-012 (full gates,
including the whole facade and parity suites the orchestrator runs) is OPEN
after this round's code change, so not every clause is PROVEN.
Round 2 (2026-09-18): C-019, C-020 and C-021 are PROVEN, and C-016 is restated.
C-012 stays OPEN, because the whole facade and parity suites are the
orchestrator's run, so the attestation stays withheld. The round's own gates
are in `/tmp/oc-worker/kb-mixed/handback-2.md`.

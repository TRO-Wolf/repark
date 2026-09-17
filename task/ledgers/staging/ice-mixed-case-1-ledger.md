# Charter ledger — ICE-MIXED-CASE-1 · Spark-door case-insensitive column resolution

**Date:** 2026-09-17 · **Branch:** `fix/ice-mixed-case-1` · **Base:** `origin/main`
`32c0e1a3` · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
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

## PROPOSITION LEDGER — ICE-MIXED-CASE-1 — 2026-09-17

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | SELECT list and WHERE on the adopted table answer Spark (false): unquoted mixed/lower case, quoted exact and quoted wrong-case. | `test_ice_mixed_case_1.py` MC-SEL-01…04, MC-WHERE-01…02 cells vs oracle (false). | OPEN | |
| C-002 | GROUP BY / ORDER BY / self-JOIN ON answer Spark (false), including ordering by a differently-cased key. | Same pin file, MC-GRP-01, MC-ORD-01, MC-JOIN-01. | OPEN | |
| C-003 | UPDATE (SET target and expression, WHERE) and DELETE WHERE answer Spark (false), verified by reading the table back. | Same pin file, MC-UPD-01…02, MC-DEL-01. | OPEN | |
| C-004 | MERGE answers Spark (false): unquoted source aliases with backticked ON, differently-cased ON/SET/INSERT cols and values, `UPDATE SET *` / `INSERT *` with differently-cased source columns. | Same pin file, MC-MRG-01…04. | OPEN | |
| C-005 | INSERT INTO t (cols) with differently-cased column list answers Spark (false). | Same pin file, MC-INS-01. | OPEN | |
| C-006 | Quoted wrong-case (`` `USERID` ``) resolves under false and refuses under true; exact-case unquoted resolves under true and wrong-case refuses under true. | Same pin file, MC-CS-TRUE-01…04 (SQL door, flag true) vs oracle (true). | OPEN | |
| C-007 | A frame carrying `a` and `A` refuses any reference to either with Spark's `[AMBIGUOUS_REFERENCE]` class and message shape under false. | Same pin file, MC-AMB-01…02; Rust unit tests for the rule. | OPEN | |
| C-008 | A temp view of a DataFrame with camelCase columns answers Spark (false) for the SELECT/WHERE cells. | Same pin file, MC-VIEW-01…02. | OPEN | |
| C-009 | The DataFrame door (`select`, `filter`) answers the same cells as recorded (unchanged behavior). | Same pin file, MC-DF-01…04. | OPEN | |
| C-010 | The live tier re-runs Spark and asserts repark == pinned golden == live Spark for every cell. | Same pin file, live tier under `REPARK_PARITY_LIVE=1`. | OPEN | |
| C-011 | ID-1 is rewritten to the new truth (dated 2026-09-16, ICE-MIXED-CASE-1); `cross_door_identifier_case_folding_agrees_unquoted_and_diverges_quoted` pins the new per-door truth; any still-divergent shape is its own row with its pin. | Registry diff + `cross_door.rs` diff. | OPEN | |
| C-012 | Full gates green: new pin file, live tier, `cargo test -p repark-spark --lib`, `cargo test -p repark-sql`, `make verify`, whole facade suite, whole parity suite. | Gate outputs pasted below. | OPEN | |

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

## 7. Open questions (HALT writes here; empty means none)

None.

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
| C-001 | On `repark.sql()` every `UPDATE` whose `WHERE` holds a subquery raises the G3-E8 text verbatim, the rows read back as seeded and no file appears under the warehouse: eight predicate forms on an Iceberg table, the card's unqualified spelling, an in-memory table. | `test_repark_sql_update_subquery_where_1.py` P-U1 to P-U10. | **PROVEN** | Red on main (§1), green on the head module (§3). |
| C-002 | The same holds for `DELETE`: the eight forms, the card's spelling and an in-memory table refuse with the G3-E8 text and write nothing. | P-D1 to P-D10. | **PROVEN** | Red on main (§1: P-D1 `DID NOT RAISE`), green on the head module (§3). |
| C-003 | The stopgap loses exactly the answers in its class, and only two of them were right: the two shapes main answered with Spark's rows by coincidence refuse, and every other statement main's `repark.sql()` answers keeps its answer. | P-C1, P-C2; the 138-statement census of §5a against Spark 4.1.2. | **PROVEN** | §5a: 20 lost answers, 18 wrong on main, 2 coincidence shapes, no loss outside the class. |
| C-004 | Statements outside the refused class answer on `repark.sql()` as on main: a plain predicate and a literal `IN` list for both verbs, and an `INSERT … SELECT` that reads an `IN` subquery. | The five control cells of the test file. | **PROVEN** | Green on main and on the head (§1, §3). |
| C-005 | `DataFusionDialect` (every session built without a dialect) refuses the class in Rust, the rows held. | `the_default_dialect_refuses_a_subquery_in_update_or_delete_where` in `crates/repark-core/src/dialect/tests.rs`. | **PROVEN** | Red on main (§1), green on the head (§3). |
| C-006 | The ANSI door's behaviour is byte-identical: its five identity forms answer Spark's rows, its G3-E8 unit and end-to-end pins keep their names and assertions, and its whole suite is green before and after. | `ansi_subquery_where_identity.rs`; `guards/tests.rs` and `guards/ansi_door.rs`; `cargo test -p repark-sql` on main and head. | **PROVEN** | §2: identity pin green on main and head; the five unit call sites gain the `true` argument only; §5b: 555 green on main, 556 on head (one added), none missing or worse. |
| C-007 | The refusal lives once for the ANSI door and `repark.sql()`, in `repark_iceberg::write::predicate_dml::subquery_where`; the ANSI router passes the identity exemption at both call sites, `DataFusionDialect` none; no crate edge; the Spark SQL door's copy is unchanged; moved code carries no comment. | The module and its four unit tests; `router.rs`; `dialect.rs`; `make check-crate-dag`; the comment gate. | **PROVEN** | §2, §3, §7. |
| C-008 | The pins catch the defect and the mechanism: with `6cb6c310`'s `dialect.rs` the 22 pins are red; with the ANSI router passing no exemption the identity pin is red; six mutants each die on a named test. | §4 revert checks and §6 mutants. | **PROVEN** | §4, §6. |
| C-009 | Nothing that is green on main is red on the head: the four crates' suites and the facade suite. | `cargo test --no-fail-fast -p repark-iceberg -p repark-core -p repark-spark -p repark-sql` on main's tree and the head; `pytest python/repark/tests -n 8` on the head module. | **PROVEN** | §5b, §5c. |
| C-010 | The card, the registry row and the 1.5.6 note name `DELETE` and the refusal with dated lines; the maps of every touched directory and this ledger land with the unit. | The three records; the map rows. | **PROVEN** | §8. |
| C-011 | The mechanical gates are green and the release module is built from a clean tree at the final commit with its provenance. | §7. | **PROVEN** | §7, §9. |

VERDICT: 11 clauses, 11 PROVEN, 0 OPEN, 0 REJECTED.

## 1. Red first (test commit `ef74e07d`, module of `6cb6c310`)

The module is the round-1 build from the clean tree at `0f074aff` (`_native.abi3.so` sha256
`976605d7…c3cc9`); `6cb6c310` adds only documents, so it is `6cb6c310`'s module.

```
.venv/bin/python -m pytest python/repark/tests/test_repark_sql_update_subquery_where_1.py -q
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
      artifacts: [python/repark/tests/test_repark_sql_update_subquery_where_1.py, crates/repark-sql/tests/ansi_subquery_where_identity.rs]
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
      artifacts: [python/repark/tests/test_repark_sql_update_subquery_where_1.py, crates/repark-core/src/dialect.rs]
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
  complete: true
```

## Hand-back

`Model: claude-opus-5-5`. `risk_tier: standard`. Round 2 builds the stopgap. Routing, Q2 and Q3
stay with the owner.

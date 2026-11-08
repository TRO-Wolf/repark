# Unit ledger — EMPTY-DML-SHARED-GUARD-1 · a DELETE or UPDATE whose input folds to no rows writes nothing

**Date:** 2026-11-08 · **Base:** `bd9c5437` (the first parent of the squash; the branch was cut at `fdc61f26`) · **Head:** `a188225c4` (the PR #1277 head both verifiers read) · **Squash:** `03f925c5` on main (PR #1277)
**Model:** builder `claude-opus-5-5` (commits `958eccfa8`, code and tests, and `086ae31bb`, the work order, map rows and corpus row) · verifiers `claude-opus-5-5` and Muse MAX (`muse-spark-1.3-contributor`), one round each · records `claude-haiku-5-5`
**Policy:** [../../../AGENTS.md](../../../AGENTS.md) · **Path:** STANDARD · **Risk tier:** high (a write path on every core session, as the Opus 5.5 verdict grades it).

**Status:** completed 2026-11-08 at `a188225c4` (squashed to main as `03f925c5`, PR #1277) after the two verifiers. Both verdicts were PASS in their one round, with zero blocking findings. Clauses C-001 to C-005, C-007 to C-010 and C-017 are PROVEN, with the scope stated in each row. C-006 and C-011 to C-016 are OPEN, each with its closing question or its card. The records of this ledger (the five-card set, the registry rows, the map rows and the removal of the module docstring that Opus 5.5 F4 names) land in the records PR on branch `docs/empty-dml-shared-guard-1-records`, which merges only on the owner's word.

**Why.** On main, a DELETE or UPDATE whose WHERE folds to an empty relation reached the Iceberg provider with no predicate and, for UPDATE, no assignments. DataFusion 54.1 rebuilds the DML's filters from the optimized input, and an empty input leaves nothing to rebuild. So `WHERE false`, `WHERE NULL`, `1 = 0`, `prim_col = NULL`, `id IS NULL` on a required column and the struct and array spellings deleted every row on the native door, and on the facade over v2-cow, v2-mor and v3. The Step 0.3 table measured it at `fdc61f26` and on PyPI 1.5.5, identically. The guard plans such a DELETE or UPDATE as a zero-count no-op, a one-row `Values` node in the DML's own output schema, before `DefaultPhysicalPlanner` runs, so the provider is never called.

**Rulings.** The owner's reply of 2026-11-08 to the Step 0.1 table: **"Defaults."** (R1). The owner's ruling of 2026-11-08: **"A-D defaults"**, which adopted A (v1.5.6 is the guard alone) and D (the guard writes and commits nothing; Spark's empty-snapshot commits, INTERNAL_ERROR and FILTER_NOT_BOOLEAN cells are recorded as declared divergences). The owner's reply of 2026-11-08 to the recommendations on the two verdicts: **"All recommendations"**, which adopted F2 (ruling D also covers Spark's errors from a no-op DML's SET expression) and F4 (remove the unrequired module docstring).

**Sources.** The clauses come from the work order [order.md](../../wo/empty-dml-shared-guard-1/order.md) §6 (the coverage claim), §7 (the pins), with §3 (the seat), §4 (the route table) and §5 (the controls). The measured Spark cells are [spark.jsonl](../../wo/empty-dml-shared-guard-1/evidence/raw/spark.jsonl) and their table, [empty_dml_shared_guard_1_spark_oracle.json](../../../python/repark/tests/writes/empty_dml_shared_guard_1_spark_oracle.json); the Step 0.3 table is [empty-dml-table.md](../../wo/empty-dml-shared-guard-1/evidence/empty-dml-table.md).

## PROPOSITION LEDGER — EMPTY-DML-SHARED-GUARD-1 — 2026-11-08

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | (§6) A zero-row optimized DELETE or UPDATE input never becomes an unrestricted provider write, on every route in order §4 item 1: the native DataFusion door, the ANSI door, the Spark door and the facade. | The core recording-provider test, the three Rust door tests and the wheel table test: the provider is never called and the answer is `count` 0. | PROVEN | Standing claim 1 of both verdicts. Head: 271 predicate cells with 0 breaches (the Opus 5.5 verdict's probe); the Muse MAX verdict's facade probes found no wrong write. Base: 53 breaches, each a full wipe of a table whose SELECT returned no rows. |
| C-002 | (§3) Every session either door builds carries the guard: the native SQL session, the facade session, the Python native session and the `F.expr` expression context all plan through `StackQueryPlanner`. | The Python-door session test, the native-door test, the expression-context test and the core nested-plan test. | PROVEN | Layer claim of both verdicts. `StackQueryPlanner::create_physical_plan` in `crates/repark-core/src/stack/planner.rs` is the only `QueryPlanner` implementation and is installed on every core session by `crates/repark-core/src/session/df_guards.rs`. DataFusion 54.1 optimizes before it calls the planner, so the seat sees the plan that executes. |
| C-003 | (§7.1, §7.2) The Step 0.3 table is a table-driven test on both doors over v2-cow, v2-mor and v3, in debug and release, and each cell asserts rows, snapshot and data files unchanged where no write is meant, and on v3 the `_row_id` and `_last_updated_sequence_number` columns. The guard's own before-and-after state check ships with it. Red on base, green on head. | The three repark-sql door tests, the wheel table test and the core rewrite test. | PROVEN | Red on base (Opus 5.5 verdict): wheel 29 of 56 failed, core 5 of 8, repark-sql 3 of 3, repark-python 3 of 3, and the failures are exactly the empty cells and the uuid table. Green on head: repark-sql, repark-python and core in debug; the release wheel 56 of 56 and the release core 8 of 8. Scope: neither verifier ran the release profile of the Rust door suite. |
| C-004 | (§7.3) The deletes corpus family gets its special-values row: the Step 0.3 predicates on DELETE and UPDATE, one line per cell, on the native and the facade doors, with the measured Spark answer per cell. | none (records): the 150-line file and its rule paragraph. | PROVEN | `docs/corpus/deletes-special-values-cells-2026-11-08.jsonl` holds 150 lines. The section "Special values (2026-11-08, EMPTY-DML-SHARED-GUARD-1)" in `docs/corpus/deletes-rule-2026-11-03.md` names its fields and the two declared divergences. |
| C-005 | (§7.4) The DML-reach rule (2026-11-05): the same predicates measured in DELETE and UPDATE on both doors. An empty INSERT source is a control and inserts nothing. | The wheel table test (both verbs, both doors) and the core test `truncate_and_an_empty_insert_source_are_left_to_the_provider`. | PROVEN | Standing claim 1 of the Opus 5.5 verdict: 444 RePark cells and 333 Spark cells over three predicate sets, with 271 predicate cells on the head matching their own SELECT. Standing claim 2: `INSERT INTO t SELECT * FROM t WHERE false` keeps the rows, adds one snapshot and matches Spark's rows. |
| C-006 | (§7.5) Every assertion, controls included, traces to a measured Spark cell in the table (the oracle-control rule). | none (records) | OPEN | Closing question: the uuid cells have no Spark oracle (`ORACLE-MISSING`, since Spark cannot declare a uuid column here). The uuid test compares with the measured cells `c-one-eq-zero` and `c-selective` as a proxy that the test discloses. The owner either accepts the proxy as the reference for those two cells, or the uuid cells are measured another way. The Opus 5.5 verdict notes the proxy as an observation, not a finding. |
| C-007 | (§4 item 4) A refused wrapper of an empty DML stays a refusal: `EXPLAIN ANALYZE` and `PREPARE` of a DELETE or UPDATE answer the existing refusal on every door, with no write. | The core wrapper test and the wheel wrapper test. | PROVEN | Both verdicts. The refusal is statement-level (`refuse_wrapped_write` in `crates/repark-iceberg/src/write/statement_writes.rs`), which runs before planning, so the guard cannot reach it. The four `ansi_explain_dml_write` tests are green and unchanged. Spark 4.1.2 answers these spellings with `PARSE_SYNTAX_ERROR` (the Opus 5.5 verdict's Spark cells). |
| C-008 | (§5, the controls) The controls keep what Spark writes: `c-true`, `c-no-where` and `c-selective`, and the identity UPDATE `c-identity-update`. | The wheel control test and the core selective test. | PROVEN | Standing claim 5 of both verdicts. Green on base and on head in all three suites; `-k "control or refused"` gives 22 passed on base and all 56 on head (the Opus 5.5 verdict). |
| C-009 | Ruling D, first declared divergence (the empty-snapshot commits). RePark commits nothing where Spark commits an empty snapshot: a no-op UPDATE on v2-cow, v2-mor and v3 (`WHERE false`, `arr[1] = 1 AND 1 = 0`, the struct spelling, `1 = 0`, `prim_col = NULL`, `id IS NULL`), and `DELETE … WHERE NULL` on v2-cow and v3. | The wheel table test, with `snapshot_changed` False on every kind except the metadata-delete residue of C-013. | PROVEN | The recorded Spark cells in [spark.jsonl](../../wo/empty-dml-shared-guard-1/evidence/raw/spark.jsonl): on each no-op UPDATE the snapshot count goes from 1 to 2 with no data file added, and the same holds for `DELETE … WHERE NULL` on v2-cow and v3. Registry rows EMPTY-DML-SHARED-GUARD-1-D1 in [spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md). |
| C-010 | Ruling D, second declared divergence (Spark's refusals). RePark answers count 0 and commits nothing where Spark refuses `UPDATE … WHERE NULL` with `[INTERNAL_ERROR]` on v2-cow and v3, and `DELETE` and `UPDATE … WHERE NULL` with `[DATATYPE_MISMATCH.FILTER_NOT_BOOLEAN]` on v2-mor. | The wheel table test, the `a-null` cells of both verbs on every kind. | PROVEN | The recorded Spark refusals in [spark.jsonl](../../wo/empty-dml-shared-guard-1/evidence/raw/spark.jsonl) (cells `a-null`); the raw error payloads name the classes above. Registry rows EMPTY-DML-SHARED-GUARD-1-D2. |
| C-011 | Ruling D extended by F2 (owner, "All recommendations", item 8): Spark's error from the SET expression of a no-op DML is a declared divergence. The Opus 5.5 verdict measured `UPDATE … SET prim_col = 1 / 0 WHERE false` on v2-mor: Spark refuses with `CAST_OVERFLOW_IN_TABLE_INSERT`, and RePark answers count 0 and commits nothing. | none (records): no test on main runs a SET expression that errors, since the wheel table's assignment is fixed at `flag = 'z'`. | OPEN | Closing question: a wheel cell with `prim_col = 1 / 0` on v2-mor and its recorded Spark refusal, then the registry row lands with that pin, since the registry admits no row without one (§6). Card [EMPTY-DML-SET-ERROR-DIVERGENCE-1](../../roadmap/mid-term/empty-dml-set-error-divergence-1-card-2026-11-08.md). |
| C-012 | Plain `EXPLAIN` of an empty DML (Opus 5.5 F1): the text now renders `Values` and `DataSourceExec` in place of the `Dml` node, on both doors. The pin asserts only that the output is non-empty. | `an_empty_dml_nested_in_a_wider_plan_is_guarded_too`, which asserts `!plan.is_empty()`. | OPEN | Closing question: pin the new text, or record it as the intended shape of an empty DML's plan. Card [EXPLAIN-EMPTY-DML-SHAPE-1](../../roadmap/mid-term/explain-empty-dml-shape-1-card-2026-11-08.md). |
| C-013 | Residue: `DELETE … WHERE id IS NULL` on the required `id` commits one empty snapshot through the metadata-delete door, on the facade and on the Rust Spark and ANSI doors. Rows and files are unchanged. The door runs before physical planning (order §4 item 2), so the guard does not reach it. Spark commits no snapshot for that DELETE. | The wheel table test (`METADATA_DELETE_RESIDUE`) and the repark-sql Spark and ANSI door tests, whose `RESIDUES` table pins the snapshot flip. | OPEN | Closing question: the owner rules whether the metadata-delete door stops committing the empty snapshot, or the residue is a declared divergence. Not a wrong write. Card [META-DELETE-EMPTY-COMMIT-1](../../roadmap/mid-term/meta-delete-empty-commit-1-card-2026-11-08.md). |
| C-014 | Residue: on `repark.sql()`, a full DELETE on v2 merge-on-read (`WHERE true` and no WHERE) writes a position-delete file where Spark leaves `data/` unchanged. Rows and snapshot equal Spark's. | The repark-sql native door test, whose `RESIDUES` table pins the file flip for `c-true` and `c-no-where` on v2-mor. | OPEN | Non-empty control input, outside the coverage claim. Closing question: the owner rules the delete-file layout of a full merge-on-read delete on the native door, or the residue is carded for a unit. Card [NATIVE-MOR-FULL-DELETE-FILE-1](../../roadmap/mid-term/native-mor-full-delete-file-1-card-2026-11-08.md). |
| C-015 | v3 lineage (Opus 5.5 F5): the table test reads `_row_id` and `_last_updated_sequence_number` on the Spark door only. On the native and ANSI doors lineage is implied by the unchanged snapshot and files and is not asserted. | none (records): the lineage read in `crates/repark-sql/tests/empty_dml_shared_guard.rs` is conditional on the Spark door. | OPEN | Closing question: read lineage through the Spark-dialect session on the same catalog for the other doors, or record why not. Card [V3-LINEAGE-NATIVE-ASSERT-1](../../roadmap/mid-term/v3-lineage-native-assert-1-card-2026-11-08.md). |
| C-016 | Affected-row shape (Muse MAX F1): the facade's guarded empty answers are `[{count: 0}]`, and the provider-path writes answer `[{}]`. The row count is 1 on every route. The shape depends on the route and predates the unit. | none (records): the wheel table test checks `count` only where the column exists. | OPEN | Closing question: record the route-dependent shape as the facade's contract, or align the guarded answer with the provider-path one with a pin. Card [FACADE-AFFECTED-ROW-SHAPE-1](../../roadmap/mid-term/facade-affected-row-shape-1-card-2026-11-08.md). |
| C-017 | Hygiene (repo rules; Opus 5.5 F4): no code comment in any added Rust, Python or shell line, and the module docstring the unit added to the wheel test is removed. | none (records) | PROVEN | Both verdicts: no added comment in the unit's Rust, Python or shell lines. The one docstring, Opus 5.5 F4, is removed by the records PR. After the removal the wheel test passes `ruff check` and `ruff format --check` at the pinned 0.15.22. |

## Evidence

**Verifier verdicts.** The Opus 5.5 verdict (`claude-opus-5-5`), one round, on PR #1277 at head `a188225c4`: PASS, zero blocking findings, five S3 findings (F1 to F5). Its Standing claims 1 to 8 are HELD, with the residues in claim 8 correctly pinned. The Muse MAX verdict (`muse-spark-1.3-contributor`), one round, on the same PR: PASS, zero blocking findings, one S3 finding (F1). Its standing claims 1 to 8 are HELD. Neither verifier ran the release-profile Rust door suite. The Opus 5.5 verdict measured its base reds with the release wheel of `fdc61f26`; the Muse MAX verdict's base evidence is its neutralized-guard run.

**Counts.** Opus 5.5, debug: the core `stack::empty_dml` tests 8 passed; repark-sql `empty_dml_shared_guard` 3 passed; repark-python `--lib empty_dml` 3 passed; repark-sql `ansi_explain_dml_write` 4 passed. Release: the wheel `test_empty_dml_shared_guard_1.py` 56 passed (`maturin develop --release`) and the core 8 passed. Muse MAX: repark-sql `empty_dml_shared_guard` 3 passed (debug), the head release wheel 56 passed, and the core 8 passed in debug and in release; it did not run repark-python `--lib` or the ANSI EXPLAIN tests. CI on `a188225c4`: 24 SUCCESS and 2 SKIPPED (the tag-only wheel jobs), per the Opus 5.5 verdict.

**Bite.** Muse MAX neutralized the guard call with a one-line change: `an_empty_delete_or_update_never_reaches_the_provider_and_answers_a_zero_count` and `an_empty_dml_nested_in_a_wider_plan_is_guarded_too` failed. The tree was restored and the core suite returned to 8 of 8 green.

**Seat and layers.** Both verdicts confirm the seat by reading DataFusion's `SessionState::create_physical_plan`, which optimizes before it calls the planner. Neither found a second `QueryPlanner` installation. `empty_dml.rs` has no `static`, no global and no Python-side or text-level logic, so the rewrite is plan-local and backend-agnostic, which keeps the distributed option open.

**Commits.** `958eccfa8` (the code and the tests, ten files including the four pin files), `086ae31bb` (the work order, the map rows and the corpus special-values row), and the merge `a188225c4` of main at `bd9c5437`. Squashed to main as `03f925c5` (PR #1277).

## Scope notes

- Out of the clause and unchanged: `EXECUTE IMMEDIATE '<empty DML>'` refuses on the facade on base and on head, where Spark answers. The Opus 5.5 verdict records it as pre-existing (its standing claim 3). It is not carded here.
- The native door cannot create v2-mor, v3 or uuid tables (`CREATE` refuses `TBLPROPERTIES` and the UUID type), so those kinds are covered through the Rust DataFusion-dialect door, not the exact Python native builder. A Trino oracle for the native door's empty-predicate cells was not measured.
- Ballista execution of a DML was not probed, since the distributed codec encodes only Iceberg scans and no DML is re-planned on an executor.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: empty-dml-shared-guard-1
  complete: true
  verdicts:
    opus_5_5: PASS, one round, claude-opus-5-5, PR #1277 at head a188225c4, zero blocking findings, five S3 findings
    muse_max: PASS, one round, PR #1277, zero blocking findings, one S3 finding
  reattested: []
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Both verdicts walked the order's §3, §4, §5, §6 and §7 clauses one by one against the tests on main, and checked each standing claim by measurement, not by the builder's text.
      artifacts: [python/repark/tests/writes/test_empty_dml_shared_guard_1.py, crates/repark-sql/tests/empty_dml_shared_guard.rs, crates/repark-core/src/stack/empty_dml/tests.rs, task/wo/empty-dml-shared-guard-1/order.md]
    - id: AT-2
      status: ATTACKED
      evidence: Empty, null, struct, array, uuid and one-row predicates on three table kinds, partial folds such as `id = 1 AND false` and `id IN (1) AND NULL`, extra empty folds the table does not list, and the controls. 444 RePark cells and 333 Spark cells in the Opus 5.5 probe.
      artifacts: [python/repark/tests/writes/test_empty_dml_shared_guard_1.py, crates/repark-sql/tests/empty_dml_shared_guard.rs, task/wo/empty-dml-shared-guard-1/evidence/empty-dml-table.md]
    - id: AT-3
      status: ATTACKED
      evidence: TRUNCATE, INSERT with an empty source, INSERT OVERWRITE with an empty source, MERGE with an empty source and with a false condition, EXPLAIN ANALYZE and PREPARE of empty writes, and a subquery DML refusal. Each keeps its answer and writes nothing extra.
      artifacts: [crates/repark-core/src/stack/empty_dml/tests.rs, python/repark/tests/writes/test_empty_dml_shared_guard_1.py]
    - id: AT-4
      status: N/A
      justification: The rewrite is a pure function over the logical plan with no static, global or interior mutability (crates/repark-core/src/stack/empty_dml.rs, both verdicts). The unit adds no writer, lock or ordering assumption, and a guarded statement never reaches the commit path.
    - id: AT-5
      status: N/A
      justification: The unit adds no authentication, path, secret, deserialization or input-parsing surface. The guard rewrites a plan that the existing parser has already built.
    - id: AT-6
      status: ATTACKED
      evidence: On every guarded cell the rows, the snapshot, the data files and, on v3, the lineage are unchanged, except the metadata-delete residue (C-013) and the native merge-on-read residue (C-014). The result schema and row count equal base's on every answered cell (Opus 5.5 standing claim 4). 313 of the 444 RePark cells equal base in every fact, and the other 131 are zero-count no-ops with rows, snapshot, files and lineage unchanged.
      artifacts: [python/repark/tests/writes/test_empty_dml_shared_guard_1.py, crates/repark-sql/tests/empty_dml_shared_guard.rs, task/wo/empty-dml-shared-guard-1/evidence/empty-dml-table.md]
    - id: AT-7
      status: N/A
      justification: The guard is one transform pass over each DML node's input (crates/repark-core/src/stack/empty_dml.rs). It adds no per-row loop, no cache and no unbounded growth, and routine performance is not an AT-7 surface under the critic reference.
    - id: AT-8
      status: ATTACKED
      evidence: Each door keeps its result schema and its count column. The wrapper refusal texts are unchanged. The seat is DataFusion's own planner hook. Spark's measured divergences are declared (C-009, C-010) or carded (C-011), and the residues are carded (C-012 to C-016), not left as silent differences.
      artifacts: [crates/repark-core/src/stack/planner.rs, crates/repark-core/src/stack/empty_dml.rs, docs/corpus/deletes-rule-2026-11-03.md]
    - id: AT-9
      status: N/A
      justification: The guard emits no log, metric or alarm and fails no statement. A refused wrapper keeps its existing text, so the diagnostic surface is neither added nor removed.
    - id: AT-10
      status: ATTACKED
      evidence: The neutralized-guard run reddened both execution pins and the restored tree passed. The table was red on base and green on head. The residual pin gaps are carded (C-012 for the EXPLAIN text, C-015 for the lineage on two doors, C-016 for the affected-row shape).
      artifacts: [crates/repark-core/src/stack/empty_dml/tests.rs, crates/repark-sql/tests/empty_dml_shared_guard.rs, python/repark/tests/writes/test_empty_dml_shared_guard_1.py]
```

## Findings

Verbatim from the verdicts, with the clause and the disposition added. The Opus 5.5 records are the Opus 5.5 verdict's F1 to F5, and the Muse MAX record is the Muse MAX verdict's F1.

```yaml
FINDING:
  id: OPUS-F1
  severity: S3
  category: AT-8
  clause: C-012
  claim: Plain EXPLAIN of an empty DML now renders the rewritten plan, where base rendered the Dml node. The pin asserts only that the output is non-empty.
  verbatim: |
    **F1 — plain EXPLAIN of an empty DML now renders the rewritten plan. S3 (advisory). Inside the unit's change,
    outside the clause.**
    - Impact: `EXPLAIN DELETE FROM t WHERE false` on base showed `logical_plan: Dml: op=[Delete] table=[ice.ns.t] /
      EmptyRelation` and `physical_plan: IcebergDeleteExec: table=ns.t, mode=CopyOnWrite`. On head it shows
      `logical_plan: Values: (UInt64(0))` and `physical_plan: DataSourceExec: partitions=1`, on both doors (DataFusion's
      `handle_explain` stringifies `e.plan` after the seat). This is honest about what executes, but the target table and
      the operation are gone from the output. The core pin `an_empty_dml_nested_in_a_wider_plan_is_guarded_too` only
      asserts `!plan.is_empty()`. No write either way.
    - Compatibility: EXPLAIN text is DataFusion's, a disclosed divergence; it was never Spark-equal.
    - Repro: `repark.sql("EXPLAIN DELETE FROM ice.ns.t WHERE false").to_arrow().to_pylist()` (`probes/head.jsonl`,
      `w-explain-delete-false`).
    - Spark cell (my oracle run): `== Physical Plan == DeleteFromTable ice.ns.t205_v2_cow, [FALSE], …`.
    - Disposition: record the new text as intended in the map row, or pin it. Not blocking.
  disposition: OPEN (card EXPLAIN-EMPTY-DML-SHAPE-1; closing question in C-012)
```

```yaml
FINDING:
  id: OPUS-F2
  severity: S3
  category: AT-8
  clause: C-011
  claim: UPDATE with a SET expression that errors, under a WHERE that folds to empty, is a zero-count no-op on v2-mor, where Spark refuses with CAST_OVERFLOW_IN_TABLE_INSERT. No test on main pins it.
  verbatim: |
    **F2 — `UPDATE … SET prim_col = 1 / 0 WHERE false` on v2-mor: Spark refuses, head answers a no-op. S3. Outside the
    clause; an undeclared divergence cell.**
    - Impact: head answers `count 0` and writes nothing. Base rewrote every file and committed. Spark 4.1.2 v2-mor refuses
      with `ArithmeticException [CAST_OVERFLOW_IN_TABLE_INSERT] … prim_col`. On v2-cow and v3 Spark answers and commits an
      empty snapshot, which ruling D covers. Ruling D names only the `INTERNAL_ERROR` and `FILTER_NOT_BOOLEAN` refusals,
      so this third Spark refusal kind on an empty-predicate UPDATE is accepted without a declared row. Not a wrong write.
    - Compatibility: Spark refuses and RePark accepts the no-op; the head moves from wrong write to no write.
    - Repro: facade, v2-mor table, `UPDATE ice.ns.t SET prim_col = 1 / 0 WHERE false` (`probes/head-extra.jsonl`,
      `r-update-div0-false`).
    - Spark cell: `probes/spark-extra.jsonl` `r-update-div0-false` v2-mor: REFUSED `ArithmeticException`.
    - Disposition: add it to the clerk's declared-divergence rows next to ruling D's cells, or rule it.
  disposition: OPEN (card EMPTY-DML-SET-ERROR-DIVERGENCE-1; the registry row lands with its pin)
```

```yaml
FINDING:
  id: OPUS-F3
  severity: S3
  category: AT-1
  clause: C-013, C-014
  claim: The residue cards, the ledger and the registry divergence rows were not on the reviewed head, and the residues are pinned in tests that point at no card.
  verbatim: |
    **F3 — residue cards, ledger and registry divergence rows are not on the reviewed head. S3 (records). Outside the
    clause.**
    - Impact: `git grep` for META-DELETE-EMPTY-COMMIT or NATIVE-MOR-FULL-DELETE-FILE on `a188225c4` finds nothing, and no
      `task/ledgers/**` ledger names the unit. The PR body says "The ledger, registry divergence rows and cards follow in a
      records PR after verification", while order §9 says the clerk writes "the `map.md` rows, the ledger and the registry
      text on the same branch". The declared divergences are stated in prose in the corpus rule paragraph. The residues are
      pinned in tests that point at no card.
    - Compatibility: none; records only.
    - Disposition: land the cards, ledger and registry rows on this branch before merge, or the owner accepts the
      deferral. Not a code defect.
  disposition: REMEDIATED (the cards for C-013 and C-014 and this ledger land in the records PR; the registry rows for ruling D land there too, and the F2 row waits for its pin)
```

```yaml
FINDING:
  id: OPUS-F4
  severity: S3
  category: AT-1
  clause: C-017
  claim: The new wheel test carries a module docstring, a comment in the repo's terms, that no gate requires.
  verbatim: |
    **F4 — module docstring in the new Python test. S3 (rules hygiene). Inside the repo rules.**
    - `python/repark/tests/writes/test_empty_dml_shared_guard_1.py:1` adds
      `"""EMPTY-DML-SHARED-GUARD-1: a DELETE or UPDATE whose input folds to no rows writes nothing."""`. Ruff `D` is off for
      `python/repark/tests/**` (`pyproject.toml:42`), and the presence gate skips `tests` paths, so no gate demands it. The
      owner's comment-ban standard allows "exactly one one-line docstring where the docstring-presence gate demands it,
      nothing more", and the orchestrator's added-line grep flags this line. 60 of the 70 sibling `test_*.py` files carry a
      module docstring, so it matches house convention.
    - Disposition: drop the line or accept it as convention. The owner's ruling decides.
  disposition: REMEDIATED (the docstring and its blank line are removed in the records PR; nothing else in the file changes)
```

```yaml
FINDING:
  id: OPUS-F5
  severity: S3
  category: AT-10
  clause: C-015
  claim: The Rust table test asserts v3 lineage only on the Spark door, and the native and ANSI doors rely on the unchanged snapshot and files.
  verbatim: |
    **F5 — the Rust table test asserts v3 lineage only on the Spark door. S3 (test coverage). Inside the clause's pins.**
    - `crates/repark-sql/tests/empty_dml_shared_guard.rs:527` reads lineage only when `door == Door::Spark`. Order §7 asks
      every v3 cell to assert `_row_id` and `_last_updated_sequence_number`. On the ANSI and DataFusion-dialect doors the
      test asserts snapshot and data files unchanged, which implies lineage unchanged for empty cells, but it does not assert
      lineage itself. My wheel probe measured facade v3 lineage `none` on every guarded cell. The native Python door cannot
      create v3.
    - Disposition: read lineage through the Spark-dialect session on the same catalog, or record why not.
  disposition: OPEN (card V3-LINEAGE-NATIVE-ASSERT-1; closing question in C-015)
```

```yaml
FINDING:
  id: MUSE-F1
  severity: S3
  category: AT-8
  clause: C-016
  claim: The facade's affected-row column shape is route-dependent: a guarded empty answers [{count: 0}] and a provider-path write answers [{}]. The row count is 1 on every route.
  verbatim: |
    - F1 (inside the clause, S3 advisory): facade affected-row column shape is
      route-dependent (`[{'count': 0}]` guarded vs `[{}]` provider-path); row
      count preserved everywhere. Repro: any two facade cells above. No action
      required; not blocking.
  disposition: OPEN (card FACADE-AFFECTED-ROW-SHAPE-1; closing question in C-016)
```

No S0, S1 or S2 finding was raised by either verdict, and no finding is blocking.

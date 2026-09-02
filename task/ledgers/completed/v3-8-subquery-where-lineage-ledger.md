# Charter ledger — V3-8 · subquery-`WHERE` DML keeps v3 row lineage

**Date:** 2026-09-02 · **Branch:** `feat/v3-8-subquery-where-lineage` · **Base:** `origin/main`
`cee8126` · **Model:** claude-opus-5 (medium) · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) · **Path:** STANDARD.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** V3-7 lifted MERGE. The last `V3-COW-1` refusal was subquery-`WHERE` DML, whose
copy-on-write rewrite in `predicate_dml.rs` reassigned every survivor. Spark 4.1.2 +
Iceberg 1.11.0 keeps stored `_row_id` on those shapes exactly as on a plain `WHERE`.

**Not in this unit:** merge-on-read subquery-`WHERE` DML on v3 (a pre-existing V2-only
delete-file gate, not a lineage refusal); subquery spellings outside the allow-listed hole
(`G3-E8`); fork repin; `.github/`.

## PROPOSITION LEDGER — V3-8 — 2026-09-02

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Measure the live oracle (PySpark 4.1.2 + Iceberg 1.11.0, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `local[1]`, `coalesce(1)` single-file seed) on `DELETE … IN` / `NOT IN` / `EXISTS` / `NOT EXISTS` and `UPDATE … IN` / `EXISTS`, on COW and MoR v3, recording rows, `_row_id`, `_last_updated_sequence_number`, next-row-id, first-row-id, added-rows and data-file / manifest / delete-file counts beside every counter. | Oracle transcript table. | **PROVEN** | Twelve cells below. Spark reassigned no stored id on any cell. |
| C-002 | On format v3 the subquery-`WHERE` COW rewrite carries stored `_row_id` and writes NULL `_last_updated_sequence_number` for rows UPDATE changed, matching the oracle's rows, lineage triples and next-row-id. Lift the `V3-COW-1` refusal for those shapes; `row_lineage_guard.rs` loses its last caller and is deleted. Merge-on-read on v3 and shapes outside the allow-listed hole stay refused for their own pre-existing reasons. Mutation-proof: drop the carried lineage → every lifted pin reds. | Spark-door, ANSI-door and facade pins with absolute values; mutation N red of M. | **PROVEN** | Engine table below. Ten Spark-door cells (created + adopted), ANSI and facade twins, plus outside-the-hole controls on all three doors. Mutation: 27 red (12 of them the newly lifted pins). Citation: `crates/repark-spark/src/tests/v3_subquery_dml.rs`. |
| C-003 | Re-record the `V3-COW-1` byte tripwire naming this unit; registry `V3-COW-1` is FIXED and names the two residual non-lineage refusals; north star §3 COW and MoR DML rows say exactly what is proven; STATUS v3 workstream truth-up and nothing else; a `REPARK_PARITY_LIVE`-gated cell for one subquery DELETE and one UPDATE; maps in lockstep; this ledger `move`d to `completed/` last. | `make check-map-sync`, `check-ledger-grammar`, `check-ledgers`. | **PROVEN** | Tripwire drops the deleted guard and re-records the three surviving files; registry heading is FIXED; north-star COW row is ✅. Citation: `python/repark-parity/tests/test_v3r_1_rulings.py`. |

VERDICT: 3 clauses, 3 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: v3-8-subquery-where-lineage
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Five subquery-WHERE COW shapes Spark-equal on rows, lineage triples, next-row-id and data files.
      artifacts: [crates/repark-spark/src/tests/v3_subquery_dml.rs, crates/repark-sql/src/v3/cow.rs, python/repark/tests/test_v3_cow_dml.py]
    - id: AT-2
      status: ATTACKED
      evidence: Created and adopted v3; IN, NOT IN, EXISTS, NOT EXISTS, UPDATE IN; v2 control unchanged by the existing suites.
      artifacts: [crates/repark-spark/src/tests/v3_subquery_dml.rs, crates/repark-iceberg/src/write/predicate_dml/tests/update.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Outside-the-hole UPDATE still refuses G3-E8 without V3-COW-1 and leaves the table unmoved, on all three doors.
      artifacts: [crates/repark-spark/src/tests/v3_cow.rs, crates/repark-sql/src/v3/cow.rs, python/repark/tests/test_v3_cow_dml.py]
    - id: AT-4
      status: N/A
      justification: No new shared mutable engine state.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, or secret handling. Commit path unchanged; only the rewrite write schema widens.
      artifacts: [crates/repark-iceberg/src/write/predicate_dml.rs, crates/repark-iceberg/src/write/predicate_dml/lineage.rs]
    - id: AT-6
      status: N/A
      justification: No Catalog trait change.
    - id: AT-7
      status: N/A
      justification: No new recursion or unbounded allocation.
    - id: AT-8
      status: N/A
      justification: No dependency pin change.
    - id: AT-9
      status: ATTACKED
      evidence: V3-COW-1 FIXED; the two residual refusals are named and are not lineage.
      artifacts: [docs/spark-sql-iceberg-parity.md, task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md]
    - id: AT-10
      status: ATTACKED
      evidence: Three clauses pinned; maps lockstep; mutation 27 red (lineage drop) and 61 red (column swap) then restored; tripwire re-recorded.
      artifacts: [crates/repark-spark/src/tests/v3_lineage.rs]
  complete: true
```

## Oracle transcript (C-001)

Live oracle: PySpark 4.1.2 + Iceberg 1.11.0, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`,
`local[1]`, `coalesce(1)` single-file seed `(id,name,_row_id,seq) =
(1,a,0,1),(2,b,1,1),(3,c,2,1)`, next-row-id 3, 1 data file, 1 manifest, 0 delete files.
Source table holds one row, `id = 2`. Interpreter `<pyspark-4.1.2-oracle>`. Transcript: this table.

| Cell | After (id,name,_row_id,seq) | next | first | added | data files | manifests | delete files |
|---|---|---|---|---|---|---|---|
| COW DELETE … IN | (1,a,0,1),(3,c,2,1) | 5 | 3 | 2 | 1 | 2 | 0 |
| COW DELETE … NOT IN | (2,b,1,1) | 4 | 3 | 1 | 1 | 2 | 0 |
| COW DELETE … EXISTS | (1,a,0,1),(3,c,2,1) | 5 | 3 | 2 | 1 | 2 | 0 |
| COW DELETE … NOT EXISTS | (2,b,1,1) | 4 | 3 | 1 | 1 | 2 | 0 |
| COW UPDATE … IN | (1,a,0,1),(2,m,1,2),(3,c,2,1) | 6 | 3 | 3 | 1 | 2 | 0 |
| COW UPDATE … EXISTS | (1,a,0,1),(2,m,1,2),(3,c,2,1) | 6 | 3 | 3 | 1 | 2 | 0 |
| MoR DELETE … IN | (1,a,0,1),(3,c,2,1) | 3 | 3 | 0 | 1 | 2 | 1 |
| MoR DELETE … NOT IN | (2,b,1,1) | 3 | 3 | 0 | 1 | 2 | 1 |
| MoR DELETE … EXISTS | (1,a,0,1),(3,c,2,1) | 3 | 3 | 0 | 1 | 2 | 1 |
| MoR DELETE … NOT EXISTS | (2,b,1,1) | 3 | 3 | 0 | 1 | 2 | 1 |
| MoR UPDATE … IN | (1,a,0,1),(2,m,1,2),(3,c,2,1) | 4 | 3 | 1 | 2 | 3 | 1 |
| MoR UPDATE … EXISTS | (1,a,0,1),(2,m,1,2),(3,c,2,1) | 4 | 3 | 1 | 2 | 3 | 1 |

Data-file and delete-file counts are the snapshot summary's `total-data-files` /
`total-delete-files`; the `.files` metadata table counts both together.

## Engine after the lift (C-002)

Same seed, created **and** adopted v3, `write.delete.mode` / `write.update.mode` =
`copy-on-write`.

| Cell | Engine after | next / first / added | data files | Verdict |
|---|---|---|---|---|
| COW DELETE … IN | (1,a,0,1),(3,c,2,1) | 5 / 3 / 2 | 1 | Spark-equal |
| COW DELETE … NOT IN | (2,b,1,1) | 4 / 3 / 1 | 1 | Spark-equal |
| COW DELETE … EXISTS | (1,a,0,1),(3,c,2,1) | 5 / 3 / 2 | 1 | Spark-equal |
| COW DELETE … NOT EXISTS | (2,b,1,1) | 4 / 3 / 1 | 1 | Spark-equal |
| COW UPDATE … IN | (1,a,0,1),(2,m,1,2),(3,c,2,1) | 6 / 3 / 3 | 2 | lineage + next-row-id equal; `F-v3-8-update-files` |
| COW UPDATE … EXISTS | refused | — | — | outside the allow-listed hole (`G3-E8`, pre-existing) |
| MoR, every shape | refused | — | — | predicate DML's V2-only delete-file gate (pre-existing) |

**F-v3-8-update-files.** Layout artefact: the COW UPDATE rewrite is `survivors UNION ALL
new values`, so the engine writes 2 data files where Spark writes 1. Lineage, next-row-id,
first-row-id and added-rows all match.

**Residual refusals are not lineage.** `refuse_dml_subquery_predicate` (`G3-E8`) refuses
subquery spellings outside the allow-listed hole; `resolve_write_mode` refuses merge-on-read
DML on a non-V2 table. Neither mentions `V3-COW-1`, and both predate this unit. The ANSI
door routes allow-listed shapes through the same `execute_predicate_dml`, so it is pinned on
the lift; its own subquery guard is the `G3-E8` one and is left alone.

**Mutation.** `attach_present_lineage` temporarily returned the user batch without the
lineage pair: **27 red** — 20 in `repark-spark` (all 10 new `v3_subquery_dml.rs` cells plus
the V3-7 MERGE and V3-5 rewrite survivor pins), 4 in `repark-sql` (including
`adopted_v3_cow_subquery_where_dml_keeps_row_lineage`), 3 in the facade (including
`test_facade_adopted_v3_cow_subquery_where_dml_keeps_row_lineage`). All 12 newly lifted pins
are among them. Restored. Second mutation — `lineage_columns.rs::conform_batch` swaps
`_row_id` ↔ `_last_updated_sequence_number` on the read side: **61 red** across the two Rust
doors, 6 of them in `v3_subquery_dml.rs`, so the two columns are distinguished rather than
aliased. Restored.

**Facade interpreter.** `.venv/bin/pytest` shebang points at the live worktree. Facade gates
in this unit run as `.venv/bin/python -m pytest …` after `make develop` (and `make py-test` /
`make preflight`). Never `.venv/bin/pytest`. The live cell also needs the `record` extra:
`uv sync --locked --extra record --extra numpy --extra pandas --extra polars --extra ml-ext
--no-install-package repark`, then `make develop`, then
`REPARK_PARITY_LIVE=1 JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1` — green.

```yaml
PROPORTIONALITY_RUBRIC:
  id: RUBRIC-v3-8-subquery-where-lineage
  pr_unit: v3-8-subquery-where-lineage
  criteria:
    blast_radius: FAIL (predicate DML write path + the last v3 guard lift)
    reversibility: PASS (one revert commit; no migration)
    size: FAIL (writer, pins, registry, maps)
    novelty: PASS (reuses V3-7's lineage helpers; no new dependency)
    sensitivity: FAIL (write/commit path)
    clarity: PASS (charter frozen 2026-09-02; three clauses)
  path: STANDARD
  recorded_by: Actor
```

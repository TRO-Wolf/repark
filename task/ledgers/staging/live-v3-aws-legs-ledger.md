# Charter ledger — LIVE-v3 · the Glue and S3 Tables format-v3 acceptance legs

**Date:** 2026-09-02 · **Branch:** `feat/live-v3-aws-legs` · **Base:** `origin/main`
`ca9c007` · **Model:** claude-opus-5 (medium) · **Registry:**
[../../../docs/spark-sql-iceberg-parity.md](../../../../docs/spark-sql-iceberg-parity.md) row
`S3T-V3-1` · **North star:**
[../../roadmap/epic-term/v1-0-iceberg-v3-northstar.md](../../../roadmap/epic-term/v1-0-iceberg-v3-northstar.md)
§3 "Live: Glue + S3 Tables v3 legs" · **Path:** STANDARD (`risk_tier: standard`; one Actor cycle).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** Every v3 row in the north-star matrix is green locally and none of them has been
re-proven live. MW-10 measured the S3 Tables *permission* on format v2; the v3 legs did not exist.
No credentials exist on the build box, so the deliverable is the leg code plus a local pin of the
exact numbers, so the first live run is a measurement and not a debugging session.

**Not in this unit:** `.github/` (the workflow is read-only here — the two legs need no new
variable and no new IAM action); any AWS or `gh` call; the fork; repairing the `_row_id` nondeterminism the unit
measured (§7, F-LIVEV3-1 / registry `V3-ROWID-3`) — that is unit **V3-11**; the Rust refusal text
in `crates/repark-sql` and `crates/repark-functions` (V3-9's).

## PROPOSITION LEDGER — LIVE-v3 — 2026-09-02

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A new non-`test_` module `_acceptance_v3.py` carries one catalog-agnostic `run_v3_acceptance(spark, catalog, namespace, table_name)` that, with `repark.sql.allowCreateFormatVersion3` on, CTASes a merge-on-read v3 table partitioned by identity `part` from an in-memory frame, appends to the `min-input-files` floor, deletes one row (a deletion vector), MERGEs matched-UPDATE + NOT MATCHED INSERT, reads `_row_id` / `_last_updated_sequence_number`, runs `rewrite_data_files` and `expire_snapshots`, optionally adopts the same metadata location on a second session, and returns every observed count; `assert_v3_acceptance_outcome` asserts the local engine's exact numbers. The harness keeps its never-teardown shape: no `DROP`, and the single `DELETE FROM` is row-scoped. | Helper + asserter exist; the DROP/DELETE guard is tightened rather than weakened; §6 numbers. | **PROVEN** | §6. `run_v3_acceptance` / `assert_v3_acceptance_outcome` / `assert_v3_lineage` / `assert_v3_row_ids_are_stable` / `assert_deletion_vectors` in `python/repark/tests/_acceptance_v3.py`, plus the single-scan `v3_rows_and_lineage` reader and the `v3_data_files_per_partition` count that arms `V3_FILES_PER_PARTITION` — a sibling module, because CAP-1 caps a source file at 1,000 lines and `_acceptance.py` was already at 834; `v3_row_delete_sql` is the one `DELETE FROM` in it and is AST-pinned with its `WHERE`. Citation: `python/repark/tests/map.md`. |
| C-002 | A local test runs the same body against the local catalog the other facade tests use and asserts the same outcome, yielding the expected numbers the live legs will assert; it is mutation-proof. | The new test green; mutation `N` red of `M`. | **PROVEN** | `test_v3_acceptance_local.py::test_v3_acceptance_leg_body_against_the_local_catalog` green, five consecutive runs. Mutation §7: **19 red of 19**. Citation: `python/repark/tests/map.md`. |
| C-003 | `test_aws_acceptance.py` gains `test_v3_dv_dml_maintenance_against_glue` and `test_v3_dv_dml_maintenance_against_s3tables` as twins of the MW-4 / MW-10 legs — same module gate, scratch namespace, never-teardown `testing_v3_dv_<uuid4>` naming, `TABLE_BUCKET_ARN` skip — and the S3 Tables leg encodes the v3 decision table: supported → the full leg; refused at CREATE → the refusal is classified and recorded and the leg passes; anything else → raised. A storage-delete denial still fails loud first. | Both legs exist and are AST-pinned; the classifier's edges are pinned AWS-free. | **PROVEN** | `test_acceptance_v3_helpers.py::test_v3_legs_are_twins_of_the_mor_legs` pins helper + asserter calls, the `uuid4` table name, `V3_ALLOW_CREATE_KEY` on `.config`, Glue-only location guard and `adopt_with`, S3-Tables-only `exact_commit_counts=False`, and the `pytest.fail(format_denial_failure(...))` denial path. The same file guards `_acceptance_v3.py` for DROP / a second `DELETE FROM` and pins `register_table` behind `adopt_with`. Decision-table edges: `test_v3_acceptance_local.py::test_v3_create_refusal_classification_is_the_s3_tables_decision_table`. Citation: `python/repark/tests/map.md`. |
| C-004 | Documents say exactly what is true: registry `S3T-V3-1` records wired-and-unmeasured with the pending question; the north-star row stays ❌ and names what the first run answers; `docs/tier2-aws.md` §6 carries one row per leg and states that nothing widens; STATUS's v3 workstream names LIVE-v3 as wired-and-unmeasured and `docs/design/format-v3-track.md` §7's "not exercised against a table with expirable snapshots" carries its dated correction; every touched `map.md` in lockstep; this ledger `move`d to `completed/` last. | A tree pin over the five documents; `make check-map-sync`, `check-ledger-grammar`, `check-ledgers`, `check-docs-compaction`. | **PROVEN** | `python/repark-parity/tests/test_live_v3_docs.py` (seven tests, whitespace-normalized; it refuses a green claim, holds STATUS under its 25,000-byte ceiling, and reads the `V3-ROWID-3` row's two measured answers). Maps: `python/repark/tests/map.md` (four entries), `python/repark-parity/tests/map.md`, `docs/map.md`, `task/roadmap/epic-term/map.md`, `task/ledgers/staging/map.md`. Citation: `python/repark-parity/tests/map.md`. |
| C-005 | The `_row_id` the merge-on-read MERGE insert assigns is measured on the live PySpark oracle over the identical statement sequence, both readings are filed as a registry row naming the follow-up unit, and STATUS carries one line of state pointing at it. | Oracle transcript, N runs each side; registry row; STATUS line; the tree pin. | **PROVEN** | §7. repark 11 six times / 10 four times over ten runs; Spark `11` in **10 of 10** (two JVMs × five fresh warehouses), survivor triples and the post-DELETE Puffin DV identical on both sides. Registry §7 `V3-ROWID-3` (BACKLOG, follow-up **V3-11**); STATUS "Known correctness issues" one line. Citation: `python/repark-parity/tests/map.md`. |

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: live-v3-aws-legs
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The leg body is asserted on nine independent readings of one sequence — DV content and file format, DV counts at three points, the rewrite triple, the row set, the lineage triples, snapshot counts before and after expire, and the adopted read.
      artifacts: [python/repark/tests/_acceptance_v3.py, python/repark/tests/test_v3_acceptance_local.py]
    - id: AT-2
      status: ATTACKED
      evidence: Both catalog shapes are encoded — Glue with the location guard and the adopt step, S3 Tables with neither, relaxed service-commit counts, and the refusal branch; the classifier's four edges are pinned AWS-free.
      artifacts: [python/repark/tests/test_aws_acceptance.py, python/repark/tests/test_v3_acceptance_local.py]
    - id: AT-3
      status: ATTACKED
      evidence: Row lineage is asserted as an invariant, not only as a table — survivor _row_id must be unchanged across MERGE and across rewrite, and the MERGE insert must take an id no survivor holds.
      artifacts: [python/repark/tests/_acceptance_v3.py]
    - id: AT-4
      status: N/A
      justification: No new shared mutable state. The second session is a distinct engine handle built by newSession and used only for the adopt read.
    - id: AT-5
      status: ATTACKED
      evidence: Never-teardown is strengthened, not relaxed — DROP TABLE / DROP NAMESPACE stay banned and the one DELETE FROM is AST-pinned inside a single-key builder. The refusal record masks account ids. The legs need no new IAM action and no new workflow variable; .github/ is untouched.
      artifacts: [python/repark/tests/test_acceptance_v3_helpers.py, docs/tier2-aws.md]
    - id: AT-6
      status: ATTACKED
      evidence: register_table is attempted only where the catalog supports it; on S3 Tables it is skipped by citation to the dated gap S3T-1 / fork R126 rather than tried and swallowed.
      artifacts: [python/repark/tests/test_aws_acceptance.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: N/A
      justification: No recursion and no unbounded allocation; the leg writes eleven single-row appends and reads bounded metadata tables.
    - id: AT-8
      status: N/A
      justification: No dependency, lock or toolchain change.
    - id: AT-9
      status: ATTACKED
      evidence: Registry S3T-V3-1 states the pending measurement and the decision table, and the tree pin refuses a green claim before a run. The _row_id nondeterminism was measured on both engines and filed as registry V3-ROWID-3 with STATUS state and follow-up unit V3-11, rather than pinned to a flapping value.
      artifacts: [docs/spark-sql-iceberg-parity.md, STATUS.md, python/repark-parity/tests/test_live_v3_docs.py]
    - id: AT-10
      status: ATTACKED
      evidence: Five clauses pinned; five map.md files in lockstep; mutation 19 red of 19, restored and re-run green.
      artifacts: [python/repark/tests/test_v3_acceptance_local.py, python/repark/tests/test_acceptance_v3_helpers.py]
  complete: true
```

## 6. The statement sequence and the measured local answers (C-001)

Catalog `ice` (memory catalog, namespace `sales` with a `LOCATION`), session config
`repark.sql.allowCreateFormatVersion3 = true`. Table properties: `format-version 3`,
merge-on-read delete / update / merge, `write.target-file-size-bytes` 268435456.

| # | Statement | Measured |
|---|---|---|
| 1 | `CREATE TABLE … USING iceberg PARTITIONED BY (part) TBLPROPERTIES (…) AS SELECT * FROM v` (one row) | table created; 1 data file |
| 2 | `INSERT INTO … VALUES (n, 'nn', n % 2)` × 9 (ids 2–10, one row each) | `data_files` grouped by `partition.part` = `[(0, 5), (1, 5)]` — exactly AT Spark's `min-input-files` floor of 5, which is what makes step 6 run at all; asserted, not assumed |
| 3 | `DELETE FROM … WHERE id = 3` | 1 delete file: `content = 1`, `file_format = 'PUFFIN'` |
| 4 | `MERGE INTO … WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *` (update id 2, insert id 11) | 2 delete files, both `(1, 'PUFFIN')`; rows `1,2(m2),4..10,11` |
| 5 | `SELECT id, _row_id, _last_updated_sequence_number FROM …` | survivors `(1,0,1) (2,1,12) (4,3,4) (5,4,5) (6,5,6) (7,6,7) (8,7,8) (9,8,9) (10,9,10)`; insert `(11, 10 or 11, 12)` |
| 6 | `CALL ice.system.rewrite_data_files(table => 'sales.…')` | rewritten 12, added 2, `removed_delete_files_count` 2; 0 delete files left; rows and lineage unchanged |
| 7 | `CALL ice.system.expire_snapshots(table => …, older_than => now+24h, retain_last => 1)` | 14 snapshots → 1 |
| 8 | `CALL ice.system.register_table(table => 'sales.…_adopted', metadata_file => <metadata_log_entries tail>)` on a second session | the adopted table reads the same ten rows |

The brief expected the deletion vector at `content = 2`; the measured answer is `content = 1`
(Iceberg `PositionDeletes`) with `file_format = 'PUFFIN'`. The asserter pins both fields.

`exact_commit_counts=False` (S3 Tables only) relaxes rows 5's sequence numbers and row 7's
snapshot counts, because that service commits on its own (MW-10, `docs/tier2-aws.md` §2). Row
sets, `_row_id` values and every file count stay exact there.

## 7. Mutation (C-002, C-003) and the finding

Each mutation applied alone, the suite re-run, the file restored.

| # | Mutation | Result |
|---|---|---|
| 0 | `V3_FILES_PER_PARTITION` 5 → 4 | red |
| 1–7 | each expected count in `_acceptance_v3` moved by one (DVs after DELETE / after MERGE, rewritten, added, removed, snapshots before, snapshots after) | 7 red |
| 8 | survivor lineage `(2, 1, 12)` → `(2, 1, 2)` (MERGE does not advance the sequence) | red |
| 9–10 | `DELETION_VECTOR_FILE_FORMAT` → `PARQUET`; `DELETION_VECTOR_CONTENT` → 2 | 2 red |
| 11 | `V3_MERGE_UPDATED_ID` 2 → 4 | red |
| 12 | delete predicate `id = {row_id}` → `id = -{row_id}` (no DV written) | red |
| 13 | drop `adopt_with` from the local test | red |
| 14 | drop `adopt_with` from the Glue leg | red |
| 15 | Glue leg's `exact_commit_counts` relaxed on S3 Tables removed | red |
| 16 | `table_name` a fixed literal instead of prefix + `uuid4` | red |
| 17 | Glue leg's `assert_glue_scratch_namespace_location` deleted | red |
| 18 | `v3_row_delete_sql` loses its `WHERE` | red |

**19 red of 19.**

**Finding F-LIVEV3-1 (S2, AT-9) — measured on both engines, filed as `V3-ROWID-3`.** The
`_row_id` a merge-on-read `MERGE … WHEN NOT MATCHED THEN INSERT` assigns to the new row is **not
deterministic in repark**: over ten identical local runs of this exact sequence it was `11` six
times and `10` four times, with `_last_updated_sequence_number` `12` every time and every survivor
triple stable. Two earlier seed shapes (a two-row CTAS spanning both partitions, and two-row
appends) also shuffled survivor `_row_id` between runs, which is why the seed is one CTAS row plus
nine single-row appends.

The same sequence was then measured on the live oracle — PySpark 4.1.2 +
`iceberg-spark-runtime-4.1_2.13:1.11.0`, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `local[2]`,
Hadoop catalog, driven through the same `_acceptance_v3` SQL builders:

| Engine | Runs | Inserted row `(id, _row_id, seq)` | Survivor triples | DV after DELETE |
|---|---|---|---|---|
| repark | 10 (10 processes) | `(11, 11, 12)` ×6, `(11, 10, 12)` ×4 | identical in all 10 | 1 × `(1, 'PUFFIN')` |
| Spark 4.1.2 / Iceberg 1.11.0 | 10 (2 JVMs × 5 warehouses) | `(11, 11, 12)` ×10 | identical, and equal to repark's | 1 × `(1, 'PUFFIN')` |

Spark is deterministic at `next-row-id` = 11, so repark's `11` matches Spark and its `10` does
not. Both values are free — no survivor holds 10 or 11 — so no read returns a duplicated id; the
defect is the instability. Disposition: **RECORDED** as registry §7 BACKLOG row `V3-ROWID-3`, with
one line of state in STATUS, and the fix assigned to follow-up unit **V3-11**. This unit's asserter
pins the invariant that survives the instability (a fresh id at or above the seed count that no
survivor holds) plus every survivor triple exactly; a value pin would flap four times in ten. When
V3-11 lands, that pin tightens to the Spark value and the row retires.

## 8. The first live run

The orchestrator, not this unit, runs it — on merged `main`, after the environment's required
reviewer approves:

```bash
gh workflow run aws-acceptance.yml --ref main
```

| Leg | Expected | If it differs |
|---|---|---|
| `test_v3_dv_dml_maintenance_against_glue` | §6's numbers exactly | a count mismatch is a real Glue-vs-local difference: record it in `S3T-V3-1`, do not relax the asserter |
| `test_v3_dv_dml_maintenance_against_s3tables` | either §6's numbers with relaxed commit counts, or a passing run carrying an `S3T-V3-1 refused-at-create` warning | an unclassified error reds: extend `is_format_version_3_refusal` only after reading the actual message |
| both | no `AccessDenied` | a denial fails loud with action, resource and masked account — a stop, never a widened policy |

After the run: update `S3T-V3-1` with the run id and the measured answer, move the north-star row
off ❌ only if Glue reproduced §6, and record the run's state in `STATUS.md`.

## 9. Performance (reviewer numbers) and one declined change

| Id | Change | Measured | Disposition |
|---|---|---|---|
| P1 | the post-MERGE and post-rewrite pairs each scanned the table twice at the same snapshot (`v3_ordered_rows` then `v3_lineage_rows`); replaced by one `v3_rows_and_lineage` reader selecting `id, name, part, _row_id, _last_updated_sequence_number … ORDER BY id` and splitting the Arrow columns | 45 → 22 object opens per pair; 178.8 → 92.4 ms and 168.6 → 70.3 ms | **TAKEN**; every pinned value unchanged (the local pin is green on the same constants) |
| P3 | pair the nine single-row appends into five commits | ~46 % less wall time | **DECLINED**. Pairing puts two partitions in one commit, which is exactly the shape that made survivor `_row_id` order-dependent in the two earlier seeds (§7). The per-id lineage pin is the point of this leg; a 46 % faster leg that can only assert a multiset is worth less than a deterministic one |
| — | make the local pin adopt through a bare `spark.newSession`, as the Glue leg does | n/a | **NOT DONE, measured why.** The Glue leg can, because its catalog comes from `.config(...)` and `newSession` replays the builder config. The local catalog is registered at RUNTIME by `register_memory_catalog`, which `newSession` does not carry: a bare `spark.newSession()` read of the table raises `AnalysisException: table 'ice.sales.f' not found`, and re-registering on the new handle yields a fresh empty catalog (`NamespaceNotFound`). The local `_second_session` helper therefore registers the catalog AND creates the namespace; the live legs pass `spark.newSession` unchanged |

## Pointers
- Up: [map.md](map.md)

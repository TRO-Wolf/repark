# Charter ledger — V3-12 · merge a legacy parquet position delete into the deletion vector

**Date:** 2026-09-02 · **Branch:** `feat/v3-12-legacy-delete-merge` · **Base:** `origin/main`
`3eb6b71` · **Model:** claude-opus-5 (medium) · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) · **Registry:**
[../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)
`V3-UPGRADE-DV-1` · **Path:** STANDARD (`risk_tier: standard`; one Actor cycle).
**Proven pattern:**
[../archive/2026-09/2026-09-02-v3-10-upgrade-v2-to-v3-ledger.md](../archive/2026-09/2026-09-02-v3-10-upgrade-v2-to-v3-ledger.md).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** V3-10 landed the in-place v2 → v3 upgrade and filed the one thing it could not do:
an upgraded table still carrying a v2 parquet position delete refused the next merge-on-read
write at the fork's commit door, where Spark merges those positions into the new deletion vector
and drops the superseded file in the same commit. That refusal was `V3-UPGRADE-DV-1`, dated
2026-09-02, with unit V3-12 named as its TRIGGER.

**Not in this unit:** any fork change (the pin stays `ff4764d3`); widening the predicate-DML hole
so a plain-`WHERE` merge-on-read statement commits through RePark's own path; equality deletes,
which have no write surface on either engine.

## PROPOSITION LEDGER — V3-12 — 2026-09-02

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The refusal reproduces before the change, on the doors it actually reaches, and the two refusal messages are DIFFERENT sites: the commit door (`validate_fresh_dvs_only`) for statements that reach RePark's own row-delta path, and the fork's own `IcebergDeleteExec` for the plain-`WHERE` spellings that do not. | Run the V3-10 pin at the base; read the two fork sites. | **PROVEN** | At the base, `v3_upgrade.rs::merge_on_read_delete_over_a_legacy_parquet_position_delete_refuses_loudly` is green on "Cannot commit deletion vector … live position delete file … still applies". `DELETE … WHERE id = 3` instead raises "is still covered by a Parquet position-delete file … not yet ported" from `crates/integrations/datafusion/src/physical_plan/delete.rs::write_deletion_vectors`, BEFORE any IO — a second, fork-owned refusal the registry had not recorded. Citation: `crates/repark-iceberg/src/write/merge/map.md`. |
| C-002 | Spark's rule is measured, not assumed: which legacy deletes it merges, which it removes, and which it leaves alone — across the base cell, two deletes on one data file, a partitioned pair where only one file is touched, a delete COVERING two data files, UPDATE, MERGE matched-DELETE, and copy-on-write. | A live PySpark 4.1.2 + Iceberg 1.11.0 transcript at a matched layout (§6, §12). | **PROVEN** | Twelve cells, §6 + §12. Spark merges the positions of EVERY applicable live position delete that names a touched data file, and removes ONLY the file-scoped ones; scope comes from equal `file_path` bounds because `referenced_data_file` is NULL on every Spark-written one. A delete covering two data files is written by `write.delete.granularity = 'partition'` on BOTH engines, Spark COMMITS over it, and Spark never removes it — not even once every file it covers carries a DV. Citation: `docs/spark-sql-iceberg-parity.md`. |
| C-003 | On the DV path a touched data file's live file-scoped parquet position deletes are read back through their reserved columns, unioned into the new DV, and removed in the SAME `RowDelta` — matching Spark on every measured cell, on three doors — with no fork change; and the shapes NOT measured keep the loud refusal. | The new module and its wiring; cells on the Spark SQL, ANSI and facade doors; two refusal pins; `grep` that `Cargo.toml` / `Cargo.lock` are byte-identical to `main`. | **PROVEN** | `write/merge/dv_close/legacy_deletes.rs` + `dv_close.rs::plan_deletion_vectors` (§7). Nine Spark-door cells in `crates/repark-spark/src/tests/v3_legacy_delete.rs`, one ANSI cell in `crates/repark-sql/src/v3/create.rs`, one facade + live cell in `python/repark/tests/test_v3_legacy_delete_merge.py`. Refusals kept: the plain-`WHERE` arm (`V3-UPGRADE-DV-PLAIN-1`) and a delete covering two data files (`V3-UPGRADE-DV-PART-1`). `git diff origin/main -- Cargo.toml Cargo.lock` is empty. Citation: `crates/repark-iceberg/src/write/merge/map.md`. |
| C-004 | The pins are load-bearing and the added cost is measured, not predicted: each mutation of the new logic reds a named subset, the incidental controls stay green, and the union is paid once per commit rather than per statement row. | Four mutations, each restored; the control pins; a 200k-row / 100k-position timing table. | **PROVEN** | §8 mutation table (M1 7/10, M2 7/10, M3 1/10, M4 6/10). Controls green: a table that stays v2 keeps two live parquet deletes and rows `[1, 4]`; a v3 table with no legacy delete is untouched (`merge_on_read_delete_after_an_engine_upgrade_writes_a_deletion_vector`); the RDF-1 bounds pins are green. §9 cost table: a 1-row and a 50-row statement pay the SAME noise-level delta over a 100k-position legacy delete. No tripwired file was touched, so no byte hash is re-recorded. Citation: `crates/repark-spark/src/tests/map.md`. |
| C-005 | The record says what landed: `V3-UPGRADE-DV-1` → FIXED with both readings equal, the two narrower refusals filed as their own dated DECLARED rows with TRIGGERs, the north-star "Upgrade" and "MOR DML" rows trued, the STATUS Next line discharging V3-12, and every touched `map.md` in lockstep. | `make check-map-sync check-ledger-grammar check-ledgers check-docs-compaction`; read the four documents. | **PROVEN** | Registry: `V3-UPGRADE-DV-1` FIXED, `V3-UPGRADE-DV-PLAIN-1` and `V3-UPGRADE-DV-PART-1` DECLARED and dated 2026-09-02. North star §3 "Upgrade" and "Write: MOR DML" both carry the V3-12 clause. STATUS's v3 bullet and Next line updated. `docs/design/format-v3-track.md` gains a V3-12 line. Maps: `write/merge/`, `repark-spark/src/tests/`, `repark-sql/src/v3/`, `python/repark/tests/`. No fork change was needed, so the handoff is untouched. Citation: `docs/design/format-v3-track.md`. |

| C-006 | The v3 DV close reads the snapshot the statement SCANNED, not the current one, so a `to_branch` merge-on-read write sees the branch's own deletion vectors and legacy deletes; one snapshot id serves the scan, the legacy collection, the close and `validate_from_snapshot`. | Two branch cells whose branch head differs from `main`, red before the change; `grep` that the close no longer receives `None`. | **PROVEN** | `a_second_merge_on_read_delete_on_a_diverged_branch_merges_the_branch_only_dv` was RED at the round's start — "Cannot commit deletion vector … the current snapshot already carries a live deletion vector for that data file" — because the close ran against `main`, which held no DV, and wrote a second one. `a_legacy_parquet_delete_that_exists_only_on_a_branch_merges_on_that_branch` was already green (the legacy collection resolved the branch from the first commit) and holds that half. Registry `V3-DV-BRANCH-1` FIXED. §15. Citation: `crates/repark-iceberg/src/write/merge/map.md`. |

| C-007 | The perf round's claims are MEASURED, not assumed: the concurrency change, the projection and the allocation hygiene are each stated at what the fixture actually shows, and anything the fixture cannot reach is named as unmeasured rather than claimed. | Before/after at 8 and 48 delete manifests, three runs a side; a fair projected-vs-full decode A/B at 100k positions with and without a `row` column, two runs a side; two correctness unit tests; the touched suites. | **PROVEN** | §16. `buffer_unordered(8)` on both manifest walks is worth **nothing** on a local warehouse (~28 ms per delete manifest either way) — it is kept because it matches the fork's own `DV_IO_CONCURRENCY` idiom and is not a regression, NOT because this unit measured a win. The `ProjectionMask` is worth nothing when the delete file has no `row` column (the shape both engines write) and **32%** of the decode when it has one (74.9 → 50.3 ms at 100k positions). Correctness, not speed, is the projection's real yield: indexing off the file schema instead of the projected batch silently misreads a delete file with a `row` column, and `a_leading_row_column_is_projected_away_without_shifting_the_reserved_columns` is red on exactly that. `positions_are_filtered_to_the_referenced_data_file` reaches the per-row `file_path` filter AT-3 had listed as unreached. `repark-iceberg --lib` 380 passed, `repark-spark --lib` 774 passed, `repark-sql --lib` 339 passed. Citation: `crates/repark-iceberg/src/write/merge/map.md`. |

VERDICT: 7 clauses, 7 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: v3-12-legacy-delete-merge
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The merge is pinned on all three doors at Spark's measured values, plus a live cell that runs the same five statements on both engines and compares one shape dict (delete files before and after, plus lineage).
      artifacts: [crates/repark-spark/src/tests/v3_legacy_delete.rs, crates/repark-sql/src/v3/create.rs, python/repark/tests/test_v3_legacy_delete_merge.py]
    - id: AT-2
      status: ATTACKED
      evidence: One legacy delete and two on one data file; a touched file and an untouched sibling; MERGE-DELETE, UPDATE, subquery DELETE, copy-on-write and the append after; the plain-WHERE spelling and a delete covering two data files, both of which must still refuse.
      artifacts: [crates/repark-spark/src/tests/v3_legacy_delete.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Pinned - a delete whose bounds cover more than one data file is not collected, so the fork's commit door refuses on three doors instead of the engine silently superseding it, and the refusal leaves every row and delete file as the upgrade left them. The per-row `file_path` filter is reached by `positions_are_filtered_to_the_referenced_data_file`, whose fixture names two data files in one delete file. NOT pin-reached and stated as defensive rather than attested - the reject direction of `applies()` (see M4) and the non-Utf8 / non-Int64 / negative-position arms (no door this engine serves writes such a delete file). Those are correctness guards for delete files this engine did not write; they are cheap and total, and neither is claimed as covered.
      artifacts: [crates/repark-iceberg/src/write/merge/dv_close/legacy_deletes.rs, crates/repark-spark/src/tests/v3_legacy_delete.rs]
    - id: AT-4
      status: N/A
      justification: The collection is a sequential read over the scanned snapshot's manifests inside the commit's own future; it holds no lock, spawns nothing, and its result is consumed by the same task that built it.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM or secret handling. No .github change. No dependency change - Cargo.toml and Cargo.lock are byte-identical to main and the fork pin stays ff4764d3.
      artifacts: [Cargo.toml, Cargo.lock]
    - id: AT-6
      status: ATTACKED
      evidence: prepare_row_delta_deletes takes the snapshot id commit_target::snapshot_id_for_commit already resolved for the target scan and validate_from_snapshot, and passes it to BOTH the legacy-delete collection and the fork container close - which had always received None. A diverged-branch cell that was red on exactly that (a fresh DV over the branch's live one) is the pin; main-branch behaviour is unchanged because the resolved id IS the current snapshot there.
      artifacts: [crates/repark-iceberg/src/write/merge/dv_close.rs, crates/repark-iceberg/src/write/merge/snapshot_commit.rs, crates/repark-spark/src/tests/v3_legacy_delete.rs]
    - id: AT-7
      status: ATTACKED
      evidence: The delete manifests are read once per commit and only for data files the statement touched; each superseded delete file is read once and folded into the position map before the container close, so the fork writes ONE union rather than a second pass. Measured at 200k rows with a 100k-position legacy delete - a 50-row statement pays the same delta a 1-row statement pays. V3-12 perf round: every manifest walk and candidate read runs at buffer_unordered(8) like the fork's manifest_stream, positions decode through a ProjectionMask over the two reserved leaves, referenced_data_file_location borrows via Cow instead of allocating per examined entry, DataFile is cloned only for confirmed candidates, the position Vec is sized from record_count, and the position map moves into new_positions with append instead of copying. Before/after in ledger section 16.
      artifacts: [crates/repark-iceberg/src/write/merge/dv_close/legacy_deletes.rs, crates/repark-iceberg/src/write/merge/dv_close.rs]
    - id: AT-8
      status: N/A
      justification: No dependency or fork-pin change; this unit is engine-side only.
    - id: AT-9
      status: ATTACKED
      evidence: V3-UPGRADE-DV-1 FIXED with both readings equal and its pins named; the two shapes that are NOT fixed are filed as their own dated DECLARED rows with TRIGGERs rather than folded into the FIXED row; north star, STATUS and the v3 track say the same thing.
      artifacts: [docs/spark-sql-iceberg-parity.md, STATUS.md, task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md, docs/design/format-v3-track.md]
    - id: AT-10
      status: ATTACKED
      evidence: Five clauses pinned; four maps in lockstep; four mutations each red then restored; the Spark transcript and the cost table are measured, not predicted.
      artifacts: [crates/repark-iceberg/src/write/merge/map.md, crates/repark-spark/src/tests/map.md, python/repark/tests/map.md, crates/repark-sql/src/v3/map.md]
  complete: true
```

## 6. Oracle transcript (C-002)

Live oracle: PySpark 4.1.2 + Iceberg 1.11.0, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, Hadoop
catalog, ANSI on, UTC. `local[1]` where the cell needs ONE data file (at `local[2]` a four-row
`INSERT … VALUES` splits into two files and no merge is exercised at all).

| Cell | Setup | v3 statement | Spark |
|---|---|---|---|
| A2 | 1 data file, ids 1..4; v2 MoR DELETE id 2 → parquet delete, `record_count` 1, `referenced_data_file` NULL | MERGE-DELETE id 3 | ONE Puffin, `record_count` 2, referencing the data file; parquet delete GONE; `next-row-id` 4; lineage `(1,0,1),(4,3,1)` |
| A2+ | as A2, then `INSERT (9,'z')` | — | `(1,0,1),(4,3,1),(9,4,4)`, `next-row-id` 5 |
| A | 2 data files, legacy delete on file 1, DV on file 2 | DELETE id 3 | NO merge, NO removal — the legacy delete does not apply to the touched file; both stay live |
| D | 1 data file, ids 1..6, two v2 MoR DELETE statements — Spark's own v2 arm merges the first parquet delete into the second and REMOVES it, so only one is ever live per data file | DELETE id 4 | Puffin `record_count` 3, parquet GONE, `removed-position-deletes` 2 |
| C | partitioned, 2 data files in ONE partition; v2 DELETE id IN (1,3) → TWO delete files, one per data file, each file-scoped by equal `file_path` bounds | DELETE id 2 (file 1 only) | Puffin `record_count` 2 for file 1, file 1's parquet delete removed, file 2's STAYS LIVE |
| E | 1 data file, ids 1..5, legacy delete id 2 | UPDATE id 3 | `overwrite` snapshot; Puffin `record_count` 2, parquet GONE, new 1-row data file; `next-row-id` 6; lineage `(1,1,1),(3,0,3),(4,4,1),(5,5,1)` |
| F | as E | MERGE matched-DELETE id 3 | Puffin `record_count` 2, parquet GONE, `next-row-id` 5, lineage `(1,0,1),(4,3,1),(5,4,1)` |
| G | as E, `write.delete.mode` = copy-on-write | DELETE id 3 | data file rewritten, the legacy parquet delete STAYS LIVE untouched, `next-row-id` 3 |

Summary counts on the merging commit (A2): `added-dvs 1`, `added-position-deletes 2`,
`added-delete-files 1`, `removed-delete-files 1`, `removed-position-delete-files 1`,
`removed-position-deletes 1`.

**Ruling from the cells (corrected 2026-09-02, §12).** Spark merges the positions of EVERY
applicable live position delete that names a data file this commit gives a DV, and REMOVES only
the file-scoped ones. `referenced_data_file` is NULL on every Spark-written one, so file scope is
equal `file_path` lower/upper bounds — Java `ContentFileUtil.isFileScoped`, the fork's
`referenced_data_file_location`. A delete covering more than one data file IS producible (from
`write.delete.granularity = 'partition'`, on both engines) and Spark commits over it while
leaving it live; §12 measures that cell and §13 says why this engine still refuses it. Equality
deletes have no write surface on either engine (V3-10 `F-v3-10-eqdel-upgrade`) and the fork's
commit door already ignores them.

The first draft of this ledger claimed the covering-two-files shape was unmeasurable and cited
Java's `validatePreviousDeletes` as consistent with refusing. A review measured it; both claims
were wrong and are struck.

## 7. The route (C-003)

| Step | Site |
|---|---|
| Collect | `write/merge/dv_close/legacy_deletes.rs::collect_superseded_legacy_deletes` reads the scanned snapshot's manifest list once: DELETE manifests first, for live non-Puffin position deletes whose `referenced_data_file_location` names a touched file; only if one survives does it read the DATA manifests for the touched files' sequence numbers, so a table with no legacy delete walks no data manifest at all (RP-7's two no-data-manifest pins hold that, and an eager walk reds both). Applicability is `delete_seq >= data_seq`, unknown erring toward "applies" — the same test the fork's commit door runs |
| Read | positions come off the parquet by reserved field id (`RESERVED_FIELD_ID_DELETE_FILE_PATH` / `_POS`, name fallback) and are filtered to the referenced data file, so a delete file naming other data files contributes only its own rows |
| Union | `dv_close.rs::plan_deletion_vectors` folds them into `new_positions` BEFORE `close_touched_dv_containers_with_partitions`, so the fork writes one union and the DV's `record_count` counts it once |
| Remove | the superseded `DataFile`s are appended to `close.removed`, so `apply_close`'s `remove_deletes_many` carries them in the same `RowDelta` and `validate_fresh_dvs_only` sees the supersede instead of refusing |

No fork change: the fork's guard already lets a DV through when the same commit removes the
delete it would supersede. `is_deletion_vector` and `referenced_data_file_location` are ported
engine-side because the fork's `delete_file_index` module is `pub(crate)`.

`prepare_row_delta_deletes` gained the commit `branch` so the merge set is read off the same
snapshot the commit door checks.

## 8. Mutation table (C-004)

Rust pin set M = 10 (nine `v3_legacy_delete.rs` cells + the ANSI cell).

| Mutation | Red |
|---|---|
| M1 collect nothing (no merge at all) | 7 of 10 |
| M2 merge the positions but never remove the superseded files | 7 of 10 |
| M3 treat every position delete as file-scoped (drop the equal-bounds test) | 1 of 10 — `a_partition_scoped_legacy_delete_still_refuses_loudly` |
| M4 invert the sequence-number applicability test (`>=` → `<=`) | 6 of 12 — this proves the ACCEPT direction of `applies()` is load-bearing (an applicable delete stops being merged, which is M1's failure mode). It does NOT reach the REJECT direction: a file-scoped delete whose sequence is BELOW its own data file's requires the same data-file path to be re-added at a higher sequence after the delete, which no door this engine serves can produce. Stated as a known gap rather than pinned. |
| M5 walk the data manifests eagerly, before any candidate is found | 2 of 2 RP-7 no-data-manifest pins (`closing_a_covered_v3_delete_reads_no_data_manifest`, `a_supplied_partition_map_closes_a_fresh_partitioned_delete_with_no_data_manifest`) |

Each mutation was restored and the set re-run green.

## 9. Added cost (C-004)

Facade door, 200,000-row v2 MoR table, a legacy parquet position delete carrying 100,000
positions, then upgraded to v3 and deleted from again.

| Statement rows | No legacy delete | With a 100,000-position legacy delete |
|---|---|---|
| 1 | 0.383 s | 0.311 s |
| 50 | 0.430 s | 0.442 s |

The union is O(positions) ONCE per commit, not per statement row: the 50-row statement pays the
same noise-level delta the 1-row statement pays, and the DV lands at `record_count` 100,001 /
100,050. The delete manifests are read once and each superseded delete file is read once.

## 10. Residuals filed

| Registry row | Shape | TRIGGER |
|---|---|---|
| `V3-UPGRADE-DV-PLAIN-1` | `DELETE`/`UPDATE … WHERE <non-subquery>` over a legacy delete refuses in the FORK's own `IcebergDeleteExec`, before any IO — only the `IN (SELECT …)` / `EXISTS` hole reaches RePark's row-delta path | a fork `write_deletion_vectors` that merges previous deletes, or the engine widening its predicate-DML hole |
| `V3-UPGRADE-DV-PART-1` | a position delete whose `file_path` bounds are not equal covers two data files; not merged, so the fork's commit door refuses | a measured Spark cell for a delete file covering two data files, from any door that can write one |

## 11. Gates

| Gate | Exit |
|---|---|
| `make preflight` | 0 |
| `make verify` | 0 |
| `make py-test` | 0 |
| `make check-map-sync check-ledger-grammar check-ledgers check-docs-compaction` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 |
| `cargo test -p repark-iceberg -p repark-spark -p repark-sql --locked` | 0 |
| live cells (`REPARK_PARITY_LIVE=1`, `test_v3_legacy_delete_merge.py`) | 0 |

## 12. The covering-two-files cell, measured (C-002 correction)

Live PySpark 4.1.2 + Iceberg 1.11.0, at the layout of
`a_partition_scoped_legacy_delete_still_refuses_loudly`: a v2 partitioned merge-on-read table at
`write.delete.granularity = 'partition'`, two data files in `part = 7`, ids 1..4.

| Step | Statement | Spark |
|---|---|---|
| P1 | v2 `DELETE … WHERE id IN (1, 3)` | ONE parquet delete, `record_count` 2, `referenced_data_file` NULL, `file_path` bounds **absent** from the manifest (so `referenced_data_file_location` is `None`); rows `(2,b,7),(4,d,7)` |
| P2 | upgrade to v3, then `MERGE … WHEN MATCHED THEN DELETE` id 2 (touches data file 1 only) | **COMMITS.** `.delete_files` = [PARQUET `rc` 2 **still live**, PUFFIN `rc` 2 referencing data file 1]; summary `added-dvs 1`, `added-position-deletes 2`, `added-delete-files 1`, `total-delete-files 2`, `total-position-deletes 4`, **no** `removed-delete-files`; rows `[(4,'d',7)]`, `next-row-id` 4, lineage `[(4,1,2)]` |
| P3 | `INSERT (9,'z',7)` | rows `[(4,1,2),(9,4,5)]`, `next-row-id` 5; parquet delete still live |
| P4 | `MERGE … DELETE` id 4 (the OTHER data file) | **COMMITS.** A second PUFFIN `rc` 2 for data file 2; the PARQUET delete is **STILL LIVE**. Spark never removes it, even once every data file it covers carries a DV. `total-delete-files` 3, `total-position-deletes` 6, rows `[(9,'z',7)]` |

The DV `record_count` of 2 in P2 is the union of the legacy position (id 1) and the statement's
own (id 2) inside data file 1 — Spark merged a NON-file-scoped delete's positions.

## 13. Why the engine still refuses the covering-two-files shape (C-003 residual)

| Route | Verdict |
|---|---|
| Merge the positions and leave the delete file live, as Spark does | The fork's `validate_fresh_dvs_only` admits an added DV over a live position delete ONLY when the same commit removes that delete. It never inspects the DV's contents, so merging changes nothing and the commit is refused. |
| Merge the positions and ALSO remove the delete file | Measurably wrong. In the P2 cell the removed delete is the only thing deleting id 3 from data file 2, which this commit did not touch: the row resurrects and the table reads `[(3,'c',7),(4,'d',7)]` where Spark reads `[(4,'d',7)]`. Silent data resurrection. |
| Rewrite the delete file without the merged file's rows | Diverges from the measured oracle on `.delete_files` content and on every `total-position-deletes` count, and invents a file Spark does not write. |

So the merge is engine-side and cheap, but the COMMIT is not expressible at the pinned fork
`ff4764d3`. Collecting the positions would build a correct DV the commit door rejects anyway —
untested, unreachable code — so `collect_superseded_legacy_deletes` deliberately skips the shape
and the refusal stays loud on three doors. Filed as `V3-UPGRADE-DV-PART-1` with the fork ask as
its TRIGGER.

## 14. Python live-helper review (addendum)

Measured by the Python perf reviewer, then fixed here.

| # | Finding | Fix |
|---|---|---|
| P1 | `_spark_legacy_merge_shape` called `getOrCreate()` unguarded. Under the real collection order `test_parity_live.py` sorts first and holds a session-scoped `local[2]` session, so the helper BORROWED it — master and jars silently dropped, the four-row `INSERT … VALUES` split into two data files, the legacy delete and the new DV landed on different files, no merge was exercised, and the pin reded: the ordered two-module run gave **1 failed, 14 passed** with `[('PARQUET', 1, False), ('PUFFIN', 1, True)]` where `[('PUFFIN', 2, True)]` was expected. The following `session.stop()` then killed the shared fixture for every later live test. | `_live_session` records `SparkSession.getActiveSession()`, builds only when none is alive, and stops only what it built. The seed is `createDataFrame(...).coalesce(1).writeTo(...).append()`, which fixes ONE data file whatever master the session runs on, so the cell holds under the borrowed session rather than needing its own. Re-run: the ordered two-module command is **110 passed**. |
| P2 | The helper used catalog name `local`, which is `_live_parity.LIFECYCLE_SPARK_CATALOG`, and repointed and `rmtree`d its warehouse. | Module-private catalog `v312legacy`, its own `mkdtemp` warehouse, and the table is dropped before the warehouse goes. |
| P3 | `os.environ.pop("PYSPARK_SUBMIT_ARGS", None)` permanently disarmed `_live_parity`'s Iceberg arming for the rest of the process. | Dropped; the jar path goes through `spark.jars` on the session this helper builds, and a borrowed session is already armed. |
| P3 | The repark-only assertions ran before `pytest.skip`, so four seconds of real work was reported as skipped. | Split: `test_v3_legacy_parquet_position_delete_merges_into_the_dv` always runs, `…_merge_matches_spark` carries the live comparison and skips first. |

## 15. The branch snapshot the close was never given (C-006)

`close_touched_dv_containers_with_partitions` takes a `snapshot_id`; RePark had always passed
`None`, which the fork resolves to the CURRENT snapshot. The legacy-delete collection this unit
added resolved the branch instead, so the two disagreed on a `to_branch` write — and the fork's
commit door, which validates against the target branch, agreed with neither.

| Reading | Before | After |
|---|---|---|
| snapshot the target scan reads | `snapshot_id_for_commit(table, branch)` | unchanged |
| snapshot the legacy collection reads | `snapshot_for_ref(branch)` | the scan's `snapshot_id` |
| snapshot the DV container close reads | **current (`main`)** | the scan's `snapshot_id` |
| snapshot `validate_from_snapshot` pins | the scan's `snapshot_id` | unchanged |

Red before the change: a v3 MoR table, `CREATE BRANCH b`, `MERGE … DELETE` id 2 on `t.branch_b`,
then id 3 on the same branch — the second close saw no DV on `main`, wrote a fresh one, and the
commit door refused "the current snapshot already carries a live deletion vector for that data
file". Green after: ONE branch DV of `record_count` 2, branch reads `[1, 4]`, `main` unmoved at
`[1, 2, 3, 4]`. The `branch: Option<&str>` parameter this unit first added to
`prepare_row_delta_deletes` is gone — one resolved snapshot id now serves every reader.

## 16. The perf round, measured (C-007)

Debug build, local warehouse, same fixture both sides, the code the only variable.

**Manifest walk — `buffer_unordered(8)` on both walks.** A v3 MoR table at
`commit.manifest-merge.enabled = false`, one delete manifest per commit, then one more
merge-on-read DELETE timed. The statement finds ZERO legacy candidates, so this isolates the
walk. Three runs a side, median.

| Delete manifests | Before | After |
|---|---|---|
| 8 | 326 ms | 335 ms |
| 48 | 1.454 s | 1.476 s |
| slope | ~28.2 ms/manifest | ~28.5 ms/manifest |

**No measurable win.** On a local warehouse the walk is manifest DECODE, not IO wait, so there is
nothing for concurrency to overlap. The change is kept because it is the fork's own
`DV_IO_CONCURRENCY` shape and is not a regression; this unit does NOT claim an object-store win it
did not measure. The duplicate pass — both the delete-manifest walk and the data-manifest walk
for sequence numbers — is the real cost, and it is fork ask **F-22**, not something the engine can
remove alone. An earlier draft of this table reported 570 ms / 1.085 s "before"; those were
first-run warm-up artifacts and are struck.

**Decode — `ProjectionMask` over the two reserved leaves.** 100,000 positions, one referenced data
file, decoded through the same function with the mask on and off, two runs a side.

| Delete file | Full decode | Projected |
|---|---|---|
| 2 columns, 1.0 MB (what Spark and this engine write) | 50.4 / 50.1 ms | 49.9 / 50.3 ms |
| 3 columns with a `row` payload, 8.0 MB | 72.7 / 74.9 ms | 50.0 / 50.3 ms |

The `row` column costs **32%** of the decode and the projection removes all of it; with no `row`
column there is nothing to skip and the mask is free. The projection's larger yield is
correctness: the reserved column INDICES shift when projection drops a column, so they are read
off each projected batch, never off the file schema.
`a_leading_row_column_is_projected_away_without_shifting_the_reserved_columns` is red if that
regresses.

**Allocation hygiene** (`Cow` on `referenced_data_file_location` so the common bounds leg borrows,
`DataFile` cloned only for confirmed candidates, `Vec::with_capacity(record_count)`, `mem::take`
+ `append` into `new_positions`) is below this fixture's resolution and is claimed as hygiene
only, not as a measured gain.

Both measurements stay in the tree as `#[ignore]`d cells — `measure_legacy_walk_cost` and
`measure_projected_decode` — so RP-8 re-measures them against the F-21/F-22 fork. Neither is a
wall-clock CI pin.

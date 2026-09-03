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
| C-002 | Spark's rule is measured, not assumed: which legacy deletes it merges, which it removes, and which it leaves alone — across the base cell, two deletes on one data file, a partitioned pair where only one file is touched, UPDATE, MERGE matched-DELETE, and copy-on-write. | A live PySpark 4.1.2 + Iceberg 1.11.0 transcript at a matched layout (§6). | **PROVEN** | Eight cells, §6. Spark merges and removes exactly the FILE-SCOPED position deletes of the data files the commit gives a DV; scope comes from equal `file_path` bounds because `referenced_data_file` is NULL on every Spark-written one. It never writes a delete file covering two data files from this path, so that shape is UNMEASURED. Citation: `docs/spark-sql-iceberg-parity.md`. |
| C-003 | On the DV path a touched data file's live file-scoped parquet position deletes are read back through their reserved columns, unioned into the new DV, and removed in the SAME `RowDelta` — matching Spark on every measured cell, on three doors — with no fork change; and the shapes NOT measured keep the loud refusal. | The new module and its wiring; cells on the Spark SQL, ANSI and facade doors; two refusal pins; `grep` that `Cargo.toml` / `Cargo.lock` are byte-identical to `main`. | **PROVEN** | `write/merge/dv_close/legacy_deletes.rs` + `dv_close.rs::plan_deletion_vectors` (§7). Nine Spark-door cells in `crates/repark-spark/src/tests/v3_legacy_delete.rs`, one ANSI cell in `crates/repark-sql/src/v3/create.rs`, one facade + live cell in `python/repark/tests/test_v3_legacy_delete_merge.py`. Refusals kept: the plain-`WHERE` arm (`V3-UPGRADE-DV-PLAIN-1`) and a delete covering two data files (`V3-UPGRADE-DV-PART-1`). `git diff origin/main -- Cargo.toml Cargo.lock` is empty. Citation: `crates/repark-iceberg/src/write/merge/map.md`. |
| C-004 | The pins are load-bearing and the added cost is measured, not predicted: each mutation of the new logic reds a named subset, the incidental controls stay green, and the union is paid once per commit rather than per statement row. | Four mutations, each restored; the control pins; a 200k-row / 100k-position timing table. | **PROVEN** | §8 mutation table (M1 7/10, M2 7/10, M3 1/10, M4 6/10). Controls green: a table that stays v2 keeps two live parquet deletes and rows `[1, 4]`; a v3 table with no legacy delete is untouched (`merge_on_read_delete_after_an_engine_upgrade_writes_a_deletion_vector`); the RDF-1 bounds pins are green. §9 cost table: a 1-row and a 50-row statement pay the SAME noise-level delta over a 100k-position legacy delete. No tripwired file was touched, so no byte hash is re-recorded. Citation: `crates/repark-spark/src/tests/map.md`. |
| C-005 | The record says what landed: `V3-UPGRADE-DV-1` → FIXED with both readings equal, the two narrower refusals filed as their own dated DECLARED rows with TRIGGERs, the north-star "Upgrade" and "MOR DML" rows trued, the STATUS Next line discharging V3-12, and every touched `map.md` in lockstep. | `make check-map-sync check-ledger-grammar check-ledgers check-docs-compaction`; read the four documents. | **PROVEN** | Registry: `V3-UPGRADE-DV-1` FIXED, `V3-UPGRADE-DV-PLAIN-1` and `V3-UPGRADE-DV-PART-1` DECLARED and dated 2026-09-02. North star §3 "Upgrade" and "Write: MOR DML" both carry the V3-12 clause. STATUS's v3 bullet and Next line updated. `docs/design/format-v3-track.md` gains a V3-12 line. Maps: `write/merge/`, `repark-spark/src/tests/`, `repark-sql/src/v3/`, `python/repark/tests/`. No fork change was needed, so the handoff is untouched. Citation: `docs/design/format-v3-track.md`. |

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

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
      evidence: Every read of a legacy delete file maps its error through a named message carrying the delete file path; a negative position and a non-Utf8 or non-Int64 reserved column are refused rather than coerced; a delete whose bounds cover more than one data file is not collected, so the fork's commit door still refuses instead of the engine silently superseding it.
      artifacts: [crates/repark-iceberg/src/write/merge/dv_close/legacy_deletes.rs]
    - id: AT-4
      status: N/A
      justification: The collection is a sequential read over the scanned snapshot's manifests inside the commit's own future; it holds no lock, spawns nothing, and its result is consumed by the same task that built it.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM or secret handling. No .github change. No dependency change - Cargo.toml and Cargo.lock are byte-identical to main and the fork pin stays ff4764d3.
      artifacts: [Cargo.toml, Cargo.lock]
    - id: AT-6
      status: ATTACKED
      evidence: prepare_row_delta_deletes gained a branch argument so the merge set is read off the same snapshot the fork's commit door checks; every caller passes the commit's own branch, and the two in-module test callers pass None.
      artifacts: [crates/repark-iceberg/src/write/merge/dv_close.rs, crates/repark-iceberg/src/write/merge/snapshot_commit.rs]
    - id: AT-7
      status: ATTACKED
      evidence: The delete manifests are read once per commit and only for data files the statement touched; each superseded delete file is read once and folded into the position map before the container close, so the fork writes ONE union rather than a second pass. Measured at 200k rows with a 100k-position legacy delete - a 50-row statement pays the same delta a 1-row statement pays.
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

**Ruling from the cells.** Spark merges and removes exactly the FILE-SCOPED legacy position
deletes of the data files this commit gives a DV. `referenced_data_file` is NULL on every
Spark-written one, so file scope is equal `file_path` lower/upper bounds — Java
`ContentFileUtil.isFileScoped`, the fork's `referenced_data_file_location`. A delete covering
more than one data file is not producible from Spark SQL and stays UNMEASURED. Equality deletes
have no write surface on either engine (V3-10 `F-v3-10-eqdel-upgrade`) and the fork's commit
door already ignores them.

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
| M4 invert the sequence-number applicability test | 6 of 10 |
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

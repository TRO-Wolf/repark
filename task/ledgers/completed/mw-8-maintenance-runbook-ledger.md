# MW-8 — the maintenance runbook

**Date:** 2026-08-24 · **Branch:** `feat/mw6-wave` · **Base:** `c93464b` (MW-7's departure
commit on this branch) · **Charter:** owner, 2026-08-23 (slate:
[../../../briefs/next-sequence.md](../../../briefs/next-sequence.md) "MW-8"; defaults: §6 of
[mw-7-scale-measurement-ledger.md](mw-7-scale-measurement-ledger.md))
· **Fork pin:** `5e7b2e4` (RP-1)

**Retires:** moved to `completed/` in this departure commit.

Docs plus one executable test. Nothing under `crates/` changed, and no engine behaviour moved.

## 1. What the unit delivers, and what it deliberately does not

Table management in production is a procedure SEQUENCE, not six procedures. The six were each
documented on their own; the order to run them in, the cadence, and the honest limit were not.

- **The guide section** — [../../../docs/guide/iceberg-guide.md](../../../docs/guide/iceberg-guide.md)
  "The maintenance sequence", the last subsection of "Maintenance". Seven numbered steps an
  Airflow DAG mirrors one-to-one, the executed `CALL` list, then the operating rules: why the
  order is load-bearing, the cadence and its ceiling, the delete-file trigger, the step nobody
  may skip, the two cheap steps, the day of latency on the orphan net, the budget, what the
  runbook cannot do, the S3 Tables retry, and the five edits a migrating Spark DAG needs.
- **The pin** — `python/repark/tests/test_mw8_runbook.py`, nine clauses, 4.5 s at gate scale.

**This ledger states no measurement of its own.** Every number in the guide section is MW-7's,
single-homed in that ledger, and the guide links it rather than copying it into a second home.
The table below is the map from a guide statement to the home of its number, and C-009 holds
the citations mechanically so the map cannot rot silently.

## 2. Where each documented default comes from

| The guide says | Home |
|---|---|
| Run the cycle every 10 merges; treat 20 as the ceiling; the 2× crossing is 19.6 merges | MW-7 §6.1 |
| Trigger on the delete-file count (≈157 files at the crossing) where the platform reports it | MW-7 §6.1, with `MOR-2` for one delete file per partition per commit |
| The order is load-bearing: 400 delete files → 8 before data compaction, or the expensive step reads 50× | MW-7 §6.2 |
| `expire_snapshots` is never skipped — 14,782 MB for a 342 MB table (43×) | MW-7 §6.3 |
| `rewrite_manifests` 0.4 s, orphan dry run 0.1 s; the manifest list 25,665 → 3,659 B | MW-7 §6.4 |
| Budget ≈2.5 min per 1e7-row merge-on-read table at 50 merges of debt | MW-7 §6.5 |
| The orphan step lags a day and a zero-row dry run proves nothing on a young warehouse | MW-7 §6.4 and §6.7, `ORPHAN-1` for the floor |
| The runbook cannot reclaim delete-laden data files; a pass leaves ≈2× the control | MW-7 §6.8, registry `RDF-1`, fork ask F-16 |
| On S3 Tables, retry a commit conflict — the service commits its own compaction alongside yours | MW-1 §2 (fork `ENGINE_CONTRACT` §8), already stated in the guide's "Maintenance on Glue and S3 Tables" |
| The five edits a migrating Spark DAG needs | `ORPHAN-1`, `ORPHAN-2`, `MANIFEST-1`, `MANIFEST-2`, `MANIFEST-3` |

## 3. PROPOSITION LEDGER — MW-8 — 2026-08-24

Every clause is pinned by `python/repark/tests/test_mw8_runbook.py`. The fixture is one
documented cycle at gate scale: 6,000 rows, 2 partitions, six MERGEs of 600 ids each, a 64 KiB
target file size, then the sequence with a census after every step.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The sequence the guide documents is the sequence the engine runs: five procedures in the charter's order, `remove_orphan_files` last and in its dry-run default, with the armed call as a seventh step. | Read the order from the MW-7 driver's `maintenance_sequence` rather than restating it, and assert the recorded procedure list and the orphan SQL text. | PROVEN | `test_the_runbook_runs_the_documented_procedures_in_order`. Provocation P1: `remove_orphan_files` moved to the front of the driver's sequence reds it. |
| C-002 | Step 2 folds the accumulated position-delete files to one per partition, and the count it starts from is `partitions × merges` — one delete file per `(spec, partition)` per commit (registry `MOR-2`). | Assert the pre-step census equals the arithmetic, the post-step census equals `partitions`, and the procedure's own two counts agree with both. | PROVEN | `test_position_delete_compaction_folds_the_deletes_to_one_per_partition` (12 → 2). Provocation P4: step 2 replaced by a no-op procedure reds it. |
| C-003 | Step 3 reduces the data-file count the merge workload fanned out, and fails no file. | Compare the census after step 2 with the census after step 3, and assert `rewritten_data_files_count > 0` and `failed_data_files_count == 0`. | PROVEN | `test_data_compaction_reduces_the_data_file_count` (50 → 6). |
| C-004 | The complete sequence cannot reclaim a delete-laden data file. Both CTAS files sit inside Java's bin-pack band, the MERGEs delete every row in them, and after all seven steps they are still live, `removed_delete_files_count` is 0, and the delete files covering them survive with their records. | Assert the band precondition per seeded file, then assert each seeded path is in the live set after the sequence, `removed_delete_files_count == 0`, and `delete_records == merges × rows_per_merge`. | PROVEN | `test_delete_laden_seed_files_survive_the_whole_runbook`. Registry `RDF-1`; mechanism and Spark oracle are MW-7 C-011. Provocations P2a/P2b: a target size that puts the seeded files OUT of the band makes them candidates, and both the precondition and the still-live assertion red. |
| C-005 | Step 4 reduces the manifest count and the manifest-list bytes a reader opens first, and returns Spark's two columns. | Compare the census after step 3 with the census after step 4, compare the manifest-list size against the pre-maintenance census, and assert the result column set. | PROVEN | `test_manifest_compaction_drops_the_manifest_count` (11 → 3 manifests). Rests on MW-6's wiring; this clause holds only that the documented cycle observes the drop. |
| C-006 | Step 5 prunes the snapshots to `retain_last`, and deletes the files the first two steps replaced — including exactly the `partitions × merges` delete files step 2 folded. | Assert the snapshot count before and after, and assert the six-column result's data, position-delete, manifest and manifest-list counts. | PROVEN | `test_expire_snapshots_prunes_the_snapshots_and_deletes_what_they_held` (12 → 1 snapshots; 48 data + 12 delete + 39 manifests + 11 manifest lists). Provocation P5: `retain_last => 5` in the driver reds it. |
| C-007 | Step 6 lists in Spark's one column and answers zero rows on a young warehouse, and step 7 — the armed form — refuses an `older_than` inside the 24-hour floor and deletes nothing outside it. | Assert the dry run's column list and row count, assert the census does not move across it, assert the armed call raises naming the floor, and assert the armed call outside the floor returns zero rows in the same column. | PROVEN | `test_the_orphan_step_is_a_lagging_net_and_the_armed_form_keeps_the_floor`. The floor on the DRY-RUN form is MW-3's `test_remove_orphan_files_floor_matches_spark`; the armed form is the cell this adds. Provocation P6: moving the probe's cutoff outside the floor reds it (`DID NOT RAISE`). |
| C-008 | The runbook never changes the row set. `COUNT(*)` is 6,000 `int64` on the Arrow path before the workload and after all seven steps. | Assert value and Arrow type at both ends. | PROVEN | `test_the_runbook_never_changes_the_row_set`. This is the correctness control under every other clause: maintenance moves which files hold and mask rows, never which rows are live. |
| C-009 | The guide's runbook section links the home of every number and every divergence it states — the MW-7 ledger and the six registry rows. | Read the section out of the guide and require every citation token to be present in it. | PROVEN | `test_the_guide_section_links_every_source_it_cites`. Provocation P3: one `MOR-2` link replaced by bare prose reds it. |

VERDICT: PASS (OPEN=0, REJECTED=0). LOGIC_SCORE = 9/9.

```yaml
KILLED_ASSUMPTIONS:
  - "The RDF-1 residue needs a hand-built pathological fixture": REMOVED (measured. The ordinary documented cycle produces it: two CTAS files of 81,254 B and 81,281 B, both inside the [49,152, 117,965] band for a 64 KiB target, carry 3,600 dead rows through all seven steps. The 1e7 x 50 shape reproduces at 6,000 rows because the merge windows are contiguous ids and the CTAS files are id-clustered)
  - "A gate-scale runbook cycle needs the driver's full leg runner": REMOVED (run_leg does the CTAS itself, so the seeded data-file paths cannot be captured, and C-004 rests on those paths. The fixture calls the driver's seed/create/merge/census/step helpers directly and keeps its own session, which also lets step 7 run on the same table)
  - "The armed orphan call is covered by MW-3's floor pin": REMOVED (MW-3 pins the floor on the DRY-RUN form. `dry_run => false` is the one call in the runbook that destroys data, and nothing pinned that the floor still holds there)
CLARIFYING_QUESTIONS:
  - "The guide states MW-7's numbers with a link to MW-7's ledger rather than re-deriving them here. A second home for a measurement is how one of them goes stale, so this ledger's clause table is about the RUNBOOK and the guide's citations, not about the numbers."
  - "No COVERAGE_ATTESTATION is filed here, per the charter — the Critic files it. `make check-ledger-grammar` reds on exactly that finding until they do, because every clause above is PROVEN."
```

**F-MW7-2 reproduces at gate scale, and it is MW-7's disclosure, not a new one.** Step 2 rewrote
12 delete files into 2 and grew the delete bytes 27,594 → 37,492 (+35.9 %), the same direction
as the 1e7 run's +31 %. The file-count win is what a scan pays; the byte total moving the other
way is recorded against
[mw-7-scale-measurement-ledger.md](mw-7-scale-measurement-ledger.md)
F-MW7-2 and is not re-homed here. The guide does not size storage from a file count, so it
carries no claim this contradicts.

## 4. Provocation proofs (pin liveness)

Each mutation was applied, the named pin was watched go RED, and the mutation was reverted.
Nothing below is committed; `git diff` after the sweep shows the guide only.

| # | Mutation | Pin that reds | Output (verbatim) |
|---|---|---|---|
| P1 | `remove_orphan_files` prepended to the driver's `maintenance_sequence` | `test_the_runbook_runs_the_documented_procedures_in_order` (C-001) | `At index 0 diff: 'remove_orphan_files' != 'rewrite_position_delete_files'` |
| P2a | `RUNBOOK_TARGET_FILE_SIZE` dropped to 8 KiB, putting the seeded files OUT of the bin-pack band | `test_delete_laden_seed_files_survive_the_whole_runbook` (C-004) | `AssertionError: seeded file 81281 B is outside the bin-pack band for 8192` — `assert 81281 <= (1.8 * 8192)` |
| P2b | the same, with the band precondition removed so the run continues | same | `AssertionError: a 100 %-dead seeded file is still live: it was never a rewrite candidate` — the out-of-band files WERE rewritten and only `compacted-*` files remain live |
| P3 | the `MOR-2` link in the runbook section replaced by bare prose | `test_the_guide_section_links_every_source_it_cites` (C-009) | `AssertionError: the runbook section cites no home for: #mor-2` |
| P4 | step 2's SQL replaced by a `rewrite_manifests` call, so nothing folds the deletes | `test_position_delete_compaction_folds_the_deletes_to_one_per_partition` (C-002) | `AssertionError: rewrite_position_delete_files: 12 -> 12, expected one delete file per partition` — `assert 12 == 2` |
| P5 | the driver's `expire_snapshots` step changed to `retain_last => 5` | `test_expire_snapshots_prunes_the_snapshots_and_deletes_what_they_held` (C-006) | `assert 5 == 1` on `census_after.snapshots` |
| P6 | the floor probe's cutoff moved to 25 hours, outside the floor | `test_the_orphan_step_is_a_lagging_net_and_the_armed_form_keeps_the_floor` (C-007) | `Failed: DID NOT RAISE PySparkException` |

P2a and P2b are the red-green pair that identifies the mechanism rather than the outcome, the
same shape MW-7's C-011 used. In band, the 100 %-dead files are kept; out of band, the very same
files are rewritten. Size is the only thing that changed, which is the candidate filter and
nothing else.

C-003, C-005 and C-008 are held by the same battery and were not separately mutated: each asserts
a shape the recorded cycle reports rather than a branch anything chooses.

## 5. Lockstep

- Guide: [../../../docs/guide/iceberg-guide.md](../../../docs/guide/iceberg-guide.md) "The
  maintenance sequence", rowed in [../../../docs/guide/map.md](../../../docs/guide/map.md). The
  snippet in it was executed verbatim against the built module before it landed (the guide
  directory's truth rule); every step answered Spark's column list on a one-row table.
- Pin: `python/repark/tests/test_mw8_runbook.py`, rowed in
  [../../../python/repark/tests/map.md](../../../python/repark/tests/map.md).
- Driver reuse: the census, the generator, the CTAS, the MERGE SQL and the sequence itself come
  from [../../../python/repark-parity/bench/mw7/map.md](../../../python/repark-parity/bench/mw7/map.md).
  This unit adds no second implementation of any of them, and nothing under `bench/` changed.
- STATUS: a dated MW-8 line on the MW scorecard; the remainder becomes V3-2.
- Slate: MW-8 leaves [../../../briefs/next-sequence.md](../../../briefs/next-sequence.md); V3-2
  is next.
- Registry: no new row. `RDF-1`, `MOR-2`, `ORPHAN-1`, `ORPHAN-2`, `MANIFEST-1`, `MANIFEST-2` and
  `MANIFEST-3` are cited by the guide and unchanged by this unit.
- Scratch: the warehouses this unit's probes wrote were deleted after the numbers were read.
  Nothing generated by this unit is committed.

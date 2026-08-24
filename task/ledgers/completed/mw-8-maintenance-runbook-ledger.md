# MW-8 — the maintenance runbook

**Date:** 2026-08-24 · **Branch:** `feat/mw6-wave` · **Base:** `b96db91` (MW-7's departure
commit on this branch) · **Charter:** owner, 2026-08-23 (slate:
[../../../briefs/next-sequence.md](../../../briefs/next-sequence.md) "MW-8"; defaults: §6 of
[mw-7-scale-measurement-ledger.md](mw-7-scale-measurement-ledger.md))
· **Fork pin:** `5e7b2e4` (RP-1)

**Retires:** moved to `completed/` in this departure commit.

Docs plus one executable test. Nothing under `crates/` changed, and no engine behaviour moved.

## 1. What the unit delivers, and what it deliberately does not

Table management in production is one scheduled CYCLE, not six separate procedures. The six were
each documented on their own; the order, the cadence, the cutoff to pass, and the honest limit
were not.

- **The guide section** — [../../../docs/guide/iceberg-guide.md](../../../docs/guide/iceberg-guide.md)
  "The maintenance runbook", the last subsection of "Maintenance". Seven numbered steps an
  Airflow DAG mirrors, the executed `CALL` block, then the operating rules: the expire cutoff and
  what it costs in time travel, why the order is load-bearing, the cadence and its ceiling, the
  delete-file trigger, the step nobody may skip, the two cheap steps, the day of latency on the
  orphan net, the budget, what the cycle cannot reclaim, how to retry a step, and the six edits a
  migrating Spark DAG needs.
- **The pin** — `python/repark/tests/test_mw8_runbook.py`, ten clauses, 4.4 s at gate scale.

**MW-7's numbers stay MW-7's.** Every cadence, budget and ratio figure in the guide is cited to
that ledger rather than copied into a second home. §2 is the map from a guide statement to the
home of its number, and C-009 holds those links mechanically. The measurements in §3 are this
unit's own — they are about the runbook's own `CALL` statements, so this ledger is their home and
the guide cites here.

## 2. Where each documented default comes from

| The guide says | Home |
|---|---|
| Pass `older_than` to `expire_snapshots`; the fallback is `history.expire.max-snapshot-age-ms`, default 5 days | fork `crates/iceberg/src/transaction/expire_snapshots.rs:56-57`; the engine applies a cutoff only when the argument is present (`crates/repark-spark/src/call.rs:572,579-581`); the consequence is measured in §3 |
| The cutoff is the time-travel window; `retain_last` is a floor, not a cap | fork `expire_snapshots.rs:45,48` (`kept < minSnapshotsToKeep \|\| ancestor.timestamp_ms >= expireOlderThan`); measured in §3 |
| Run the cycle every 10 merges; treat 20 as the ceiling; the 2× crossing is 19.6 merges | MW-7 §6.1 |
| Trigger on the delete-file count (≈157 files at the crossing) where the platform reports it | MW-7 §6.1, with `MOR-2` for one delete file per partition per commit |
| The order is load-bearing: 400 delete files → 8 before data compaction, or the expensive step reads 50× | MW-7 §6.2 |
| `expire_snapshots` is never skipped — 14,782 MB for a 342 MB table (43×), reclaimed with a cutoff one day in the FUTURE and `retain_last => 1` | MW-7 §6.3 for the ratio, §4.4 for the cycle that produced it, and `bench/mw7/measure.py` `maintenance_sequence` for the call itself |
| `rewrite_manifests` 0.4 s, orphan dry run 0.1 s; the manifest list 25,665 → 3,659 B | MW-7 §6.4 |
| Budget ≈2.5 min per 1e7-row merge-on-read table at 50 merges of debt | MW-7 §6.5 |
| The orphan step lags a day and a zero-row dry run proves nothing on a young warehouse | MW-7 §6.4 and §6.7, `ORPHAN-1` for the floor |
| After a cycle a merge-on-read table still reads at 2.02× (point) / 2.45× (partition) and holds 1.90× the live bytes | MW-7 §4.3 (the two probe ratios) and §4.4 (the live bytes) |
| The cycle cannot reclaim delete-laden data files | MW-7 §6.8, registry `RDF-1`, fork ask F-16 |
| Step 4 raises on an idle cycle where Spark answers `0, 0` | registry `MANIFEST-1`; the loop it creates is measured in §3 |
| On S3 Tables, retry a step that fails on a commit conflict | MW-1 §2 (fork `ENGINE_CONTRACT` §8), already homed in the guide's "Maintenance on Glue and S3 Tables" |
| The six edits a migrating Spark DAG needs | `ORPHAN-1`, `ORPHAN-2`, `MANIFEST-1`, `MANIFEST-2`, `MANIFEST-3`, plus the expire cutoff above |

## 3. MEASUREMENTS: the expire cutoff and the idle cycle (2026-08-24)

One host, one run each. **The claim is the RESULT SHAPE, not the wall clock** — six zeros against
real deletions, and whether time travel survives. Fixture throughout: a 6,000-row partitioned v2
merge-on-read table, 2 partitions, six MERGEs of 600 ids, `write.target-file-size-bytes` 64 KiB,
built with the MW-7 driver's generator. `COUNT(*)` held 6,000 at every point in every run below.

### 3.1 The cycle as first drafted, with no `older_than` on step 5

Three cycles, six fresh MERGEs before each:

| cycle | `expire_snapshots` result | warehouse before → after | snapshots |
|---|---|---:|---:|
| 1 | six zeros | 544,222 → 994,218 B | 12 |
| 2 | six zeros | 1,443,454 → 2,047,135 B | 23 |
| 3 | six zeros | 2,570,745 → 3,265,712 B | 34 |

**544,222 → 3,265,712 B is 6.00× across three cycles, and step 5 reclaimed nothing at any
point.** A cutoff of `now − 3 days` answers the same six zeros (545,020 → 3,275,771 B over the
same three cycles), so "compute a cutoff" is not on its own the fix — the cutoff has to be one
the snapshots are actually older than.

### 3.2 The cycle as now printed, one cycle, three windows

Executed by extracting the guide's own `MAINTENANCE_CYCLE` block and running it. Steps 2 to 4
answered identically in all three runs: `rewrite_position_delete_files` `12, 2`;
`rewrite_data_files` 48 rewritten / 4 added, `removed_delete_files_count` 0;
`rewrite_manifests` `9, 1`. Only step 5 moves.

| `EXPIRE_CUTOFF` | step 5 result (data / pos-delete / eq-delete / manifests / lists / stats) | snapshots after | warehouse before → after | time travel to the CTAS snapshot |
|---|---|---:|---:|---|
| `now − 7 days` (as printed) | 0 / 0 / 0 / 0 / 0 / 0 | 12 | 544,217 → 994,293 B | works |
| `now` | 0 / 0 / 0 / 0 / 4 / 0 | 8 | 544,278 → 981,426 B | **gone** |
| `now + 1 day` (the MW-7 driver's idiom) | 48 / 12 / 0 / 39 / 11 / 0 | 1 | 544,707 → 491,764 B | **gone** |

The failure is verbatim `AnalysisException: Error during planning: unknown Iceberg snapshot id
<id>: not found in table metadata`.

**Two things this table settles.** The seven-day window reclaiming nothing is CORRECT, not a
defect: the operator asked for seven days of history and the table is minutes old. And the block
computes `now` once at the top, before steps 2 to 4 commit, so even a zero window cannot expire
the snapshots the same cycle just wrote — a cycle reclaims what earlier cycles left.

### 3.3 The idle cycle: step 4 raises where Spark answers two zeros

The same cycle run a second time with no MERGEs in between:

| step | run 1 | run 2 |
|---|---|---|
| 2 `rewrite_position_delete_files` | `12, 2` (+ bytes) | four zeros |
| 3 `rewrite_data_files` | 48 rewritten / 4 added | five zeros |
| 4 `rewrite_manifests` | `9, 1` | **raises** |
| 5 `expire_snapshots` | six zeros | six zeros |
| 6 `remove_orphan_files` | zero rows | zero rows |

Run 2's step 4, verbatim:

```text
UnsupportedOperationException: This feature is not implemented: CALL rewrite_manifests found
nothing to do on the data manifests of `ns.orders`, and it will not report zeros while 2 delete
manifest(s) stay uncompacted. Apache Spark rewrites delete manifests in a second leg of this
procedure; the owned fork's action carries every delete manifest forward unchanged, so this
engine cannot. Compact the delete FILES first with `CALL rewrite_position_delete_files`, which
reduces how many delete manifests later commits produce
```

**The refusal's own remedy cannot be taken.** The 2 delete manifests are the 2 delete files step 2
folded to — one per partition, at the fold floor — and under `RDF-1` those files are permanent, so
running step 2 again answers four zeros and the operator loops. The refusal is right about the
divergence and wrong about the way out on this shape. `MANIFEST-1` is unchanged; what is new is
that the runbook reaches it on an ordinary retry, which the guide now says.

## 4. PROPOSITION LEDGER — MW-8 — 2026-08-24

Every clause is pinned by `python/repark/tests/test_mw8_runbook.py`. The fixture is one
documented cycle at gate scale: 6,000 rows, 2 partitions, six MERGEs of 600 ids each, a 64 KiB
target file size, then the cycle with a census after every step.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The cycle the guide documents is the cycle the engine runs: five procedures in the charter's order, `remove_orphan_files` last and in its dry-run default, with the armed call as a seventh step. | Read the order from the MW-7 driver's `maintenance_sequence` rather than restating it, and assert the recorded procedure list and the orphan SQL text. | PROVEN | `test_the_runbook_runs_the_documented_procedures_in_order`. Provocation P1: `remove_orphan_files` moved to the front of the driver's sequence reds it. |
| C-002 | Step 2 folds the accumulated position-delete files to one per partition, and the count it starts from is `partitions × merges` — one delete file per `(spec, partition)` per commit (registry `MOR-2`). | Assert the pre-step census equals the arithmetic, the post-step census equals `partitions`, and the procedure's own two counts agree with both. | PROVEN | `test_position_delete_compaction_folds_the_deletes_to_one_per_partition` (12 → 2). Provocation P4: step 2 replaced by a no-op procedure reds it. |
| C-003 | Step 3 reduces the data-file count the merge workload fanned out, and fails no file. | Compare the census after step 2 with the census after step 3, and assert `rewritten_data_files_count > 0` and `failed_data_files_count == 0`. | PROVEN | `test_data_compaction_reduces_the_data_file_count` (50 → 6). |
| C-004 | The complete cycle cannot reclaim a delete-laden data file. Both CTAS files sit inside Java's bin-pack band, the MERGEs delete every row in them, and after all seven steps they are still live, `removed_delete_files_count` is 0, and the delete files covering them survive with their records. | Assert the band precondition per seeded file, then assert each seeded path is in the live set after the cycle, `removed_delete_files_count == 0`, and `delete_records == merges × rows_per_merge`. | PROVEN | `test_delete_laden_seed_files_survive_the_whole_runbook`. Registry `RDF-1`; mechanism and Spark oracle are MW-7 C-011. Provocations P2a/P2b: a target size that puts the seeded files OUT of the band makes them candidates, and both the precondition and the still-live assertion red. |
| C-005 | Step 4 reduces the manifest count and the manifest-list bytes a reader opens first, and returns Spark's two columns. | Compare the census after step 3 with the census after step 4, compare the manifest-list size against the pre-maintenance census, and assert the result column set. | PROVEN | `test_manifest_compaction_drops_the_manifest_count` (11 → 3 manifests). Rests on MW-6's wiring; this clause holds only that the documented cycle observes the drop. |
| C-006 | Step 5 prunes the snapshots to `retain_last`, and deletes the files the first two steps replaced — including exactly the `partitions × merges` delete files step 2 folded. | Assert the snapshot count before and after, and assert the six-column result's data, position-delete, manifest and manifest-list counts. | PROVEN | `test_expire_snapshots_prunes_the_snapshots_and_deletes_what_they_held` (12 → 1 snapshots; 48 data + 12 delete + 39 manifests + 11 manifest lists). The fixture uses the driver's future cutoff, which is the maximum-reclamation end of §3.2. Provocation P5: `retain_last => 5` in the driver reds it. |
| C-007 | Step 6 lists in Spark's one column and answers zero rows on a young warehouse, and step 7 — the armed form — refuses an `older_than` inside the 24-hour floor and deletes nothing outside it. | Assert the dry run's column list and row count, assert the census does not move across it, assert the armed call raises naming the floor, and assert the armed call outside the floor returns zero rows in the same column. | PROVEN | `test_the_orphan_step_is_a_lagging_net_and_the_armed_form_keeps_the_floor`. The floor on the DRY-RUN form is MW-3's `test_remove_orphan_files_floor_matches_spark`; the armed form is the cell this adds. Provocation P6: moving the probe's cutoff outside the floor reds it (`DID NOT RAISE`). |
| C-008 | The cycle never changes the row set. `COUNT(*)` is 6,000 `int64` on the Arrow path before the workload and after all seven steps. | Assert value and Arrow type at both ends. | PROVEN | `test_the_runbook_never_changes_the_row_set`. This is the correctness control under every other clause: maintenance moves which files hold and mask rows, never which rows are live. |
| C-009 | Every source the runbook section relies on is LINKED from it — the MW-7 ledger, this ledger, and the six registry rows. | Read the section out of the guide and require every citation token to be present in it. | PROVEN | `test_the_guide_section_links_every_source_it_names`. **Deliberately narrow**, and measured so: provocation P10 replaced one of two identical citations with a fabricated number and this clause stayed GREEN. It holds that each home is linked, not that each number is cited. C-010 is the drift detector with teeth. |
| C-010 | The `CALL` statements the guide PRINTS are the statements this unit measured: same procedures, same order, same argument names, and the same value wherever the guide prints a literal. | Parse the guide's python block, read `MAINTENANCE_CYCLE` out of the AST, and compare procedure, order and arguments against `measure.maintenance_sequence`'s. | PROVEN | `test_the_printed_cycle_matches_the_sequence_the_engine_runs`. This is the clause that would have caught F-MW8-1: provocation P8 restores the pre-remediation expire call and it reds naming the missing `older_than`. Provocation P9 edits the printed `retain_last` value and it reds. |

VERDICT: PASS (OPEN=0, REJECTED=0). LOGIC_SCORE = 10/10.

```yaml
KILLED_ASSUMPTIONS:
  - "The RDF-1 residue needs a hand-built pathological fixture": REMOVED (measured. The ordinary documented cycle produces it: two CTAS files of 81,254 B and 81,281 B, both inside the [49,152, 117,965] band for a 64 KiB target, carry 3,600 dead rows through all seven steps)
  - "A gate-scale runbook cycle needs the driver's full leg runner": REMOVED (run_leg does the CTAS itself, so the seeded data-file paths cannot be captured, and C-004 rests on those paths. The fixture calls the driver's seed/create/merge/census/step helpers directly and keeps its own session, which also lets step 7 run on the same table)
  - "The armed orphan call is covered by MW-3's floor pin": REMOVED (MW-3 pins the floor on the DRY-RUN form. dry_run => false is the one call in the cycle that destroys data, and nothing pinned that the floor still holds there)
  - "expire_snapshots with no older_than expires what the cycle just replaced": REMOVED (Critic, 2026-08-24, F-MW8-1. The engine applies a cutoff only when the argument is present, so the fork falls back to now − history.expire.max-snapshot-age-ms, 5 days. Three documented cycles reclaimed NOTHING and grew the warehouse 6.00x — the exact pathology the section's own 43x rule warns about, produced by the runbook)
  - "Any computed cutoff fixes it, so print the orphan step's cutoff on step 5 too": REMOVED (measured while remediating. A cutoff of now − 3 days answers the same six zeros on a table minutes old. The cutoff is a TIME-TRAVEL WINDOW: reclamation is bounded by it, and a window shorter than the garbage is what reclaims the garbage. The guide now says that, and says what it costs)
  - "A prose-citation check is enough to hold a docs unit honest": REMOVED (Critic, 2026-08-24, F-MW8-3. C-009 read tokens out of the section and stayed green through a fabricated number AND through an edit to the printed SQL. Nothing read the block an operator actually copies. C-010 now parses it and compares it to the measured sequence)
CLARIFYING_QUESTIONS:
  - "The guide states MW-7's numbers with a link to MW-7's ledger rather than re-deriving them here. The expire-cutoff and idle-cycle measurements in section 3 are about the runbook's OWN statements, so they are homed here and the guide cites here."
  - "No COVERAGE_ATTESTATION is filed here, per the charter — the Critic files it. `make check-ledger-grammar` reds on exactly that finding until they do, because every clause above is PROVEN."
```

## 5. Critic findings, and what each one changed

```yaml
FINDING:
  id: F-MW8-1
  severity: S1
  category: AT-1
  clause: C-001, C-010
  disposition: REMEDIATED
  title: the runbook's own step 5 carried no older_than, so the documented cycle reclaimed nothing
  mechanism: >
    The engine applies a cutoff only when the argument is present
    (crates/repark-spark/src/call.rs:572,579-581), so the fork falls back to
    now − history.expire.max-snapshot-age-ms, default 5 days
    (crates/iceberg/src/transaction/expire_snapshots.rs:56-57). That is a time-travel default,
    not a maintenance one: the cycle keeps 5 days of every file it replaced, and on a table
    younger than 5 days it reclaims nothing at all.
  evidence: >
    Section 3.1: three documented cycles, six zeros every time, warehouse 544,222 → 3,265,712 B
    (6.00x). A computed cutoff of now − 3 days answers the same six zeros, which is why the
    remediation is a WINDOW the operator picks rather than a copy of the orphan step's cutoff.
  remediation: >
    The printed block computes EXPIRE_CUTOFF from a named time-travel window and passes it. Two
    paragraphs follow the block: why the fallback is not a maintenance default, and what the
    cutoff costs — measured, including the CTAS snapshot becoming unreadable. The 43x rule now
    names the call that produced it (a cutoff one day in the FUTURE with retain_last => 1) and
    cites MW-7 4.4 beside 6.3. C-010 pins the printed statement against the measured one.
FINDING:
  id: F-MW8-2
  severity: S2
  category: AT-3
  clause: C-005
  disposition: REMEDIATED
  title: the cycle run twice with no merges between raises on step 4, and the refusal's remedy loops
  evidence: >
    Section 3.3. Run 2 step 4 raises UnsupportedOperationException (MANIFEST-1) because the data
    leg is idle while 2 delete manifests remain. Those 2 are the delete files step 2 folded to,
    one per partition, and RDF-1 makes them permanent — so the refusal's "compact the delete
    FILES first" answers four zeros and the operator loops. Spark answers 0, 0 there.
  remediation: >
    The guide gains a "Retrying a step" subsection naming the exception, the idle-cycle trigger
    and the loop, with the two remedies (catch it, or guard step 4 on steps 2-3 having rewritten
    something). The porting list gains MANIFEST-1's refusal half. No engine change: the refusal
    is the recorded divergence, and this is the runbook meeting it.
FINDING:
  id: F-MW8-3
  severity: S2
  category: AT-10
  clause: C-009, C-010
  disposition: REMEDIATED
  title: no clause read the guide's MAINTENANCE_CYCLE, so the printed SQL could drift silently
  evidence: >
    The Critic inserted fabricated uncited numbers and C-009 stayed green, then edited the
    printed retain_last and all nine clauses stayed green. Reproduced here as P10 (still green,
    and now stated as C-009's measured limit) and P9 (reds under C-010).
  remediation: >
    C-010 parses the block's AST, reads MAINTENANCE_CYCLE, and compares procedure, order,
    argument names and literal argument values against measure.maintenance_sequence's. C-009's
    proposition is narrowed to what it actually checks. P8 shows C-010 reds on the F-MW8-1 defect.
FINDING:
  id: F-MW8-4
  severity: S2
  category: AT-1
  clause: C-009
  disposition: REMEDIATED
  title: two guide claims cited a home that does not hold them
  evidence: >
    "about 2x ... 1.9x live bytes" cited MW-7 6.8, which carries no numbers; the figures live in
    4.3 (point 2.02x, partition 2.45x) and 4.4 (live bytes 1.90x). "a fifth of that at the
    recommended cadence" converted 6.5's DEBT claim into a TIME claim nothing measured.
  remediation: >
    Both probe ratios are now given, cited to 4.3 and 4.4, in the guide, STATUS and the slate.
    The "fifth of that" clause is deleted rather than re-homed: no run measured it.
FINDING:
  id: F-MW8-5
  severity: S3
  category: AT-8
  clause: C-009
  disposition: REMEDIATED
  title: the S3 Tables subsection named an isolation level its home does not, and restated the section above it
  evidence: >
    MW-1 2 records validate_data_files_exist and routine CommitFailed requirement mismatches from
    service-side compaction. It names no isolation level. The subsection also repeated the
    paragraph 100 lines above it.
  remediation: >
    "serializable" is gone and the restatement is a pointer. The subsection is now "Retrying a
    step", which carries both retry cases — the S3 Tables conflict and F-MW8-2's idle-cycle
    refusal — since both answer the same operator question.
```

## 6. Provocation proofs (pin liveness)

Each mutation was applied, the named pin was watched go RED, and the mutation was reverted.
Nothing below is committed.

| # | Mutation | Pin that reds | Output (verbatim) |
|---|---|---|---|
| P1 | `remove_orphan_files` prepended to the driver's `maintenance_sequence` | `test_the_runbook_runs_the_documented_procedures_in_order` (C-001) | `At index 0 diff: 'remove_orphan_files' != 'rewrite_position_delete_files'` |
| P2a | `RUNBOOK_TARGET_FILE_SIZE` dropped to 8 KiB, putting the seeded files OUT of the bin-pack band | `test_delete_laden_seed_files_survive_the_whole_runbook` (C-004) | `AssertionError: seeded file 81281 B is outside the bin-pack band for 8192` — `assert 81281 <= (1.8 * 8192)` |
| P2b | the same, with the band precondition removed so the run continues | same | `AssertionError: a 100 %-dead seeded file is still live: it was never a rewrite candidate` — the out-of-band files WERE rewritten and only `compacted-*` files remain live |
| P3 | the `MOR-2` link in the runbook section replaced by bare prose | `test_the_guide_section_links_every_source_it_names` (C-009) | `AssertionError: the runbook section links no home for: #mor-2` |
| P4 | step 2's SQL replaced by a `rewrite_manifests` call, so nothing folds the deletes | `test_position_delete_compaction_folds_the_deletes_to_one_per_partition` (C-002) | `AssertionError: rewrite_position_delete_files: 12 -> 12, expected one delete file per partition` — `assert 12 == 2` |
| P5 | the driver's `expire_snapshots` step changed to `retain_last => 5` | `test_expire_snapshots_prunes_the_snapshots_and_deletes_what_they_held` (C-006) | `assert 5 == 1` on `census_after.snapshots` |
| P6 | the floor probe's cutoff moved to 25 hours, outside the floor | `test_the_orphan_step_is_a_lagging_net_and_the_armed_form_keeps_the_floor` (C-007) | `Failed: DID NOT RAISE PySparkException` |
| P8 | the PRINTED expire call reverted to its pre-remediation form (no `older_than`) | `test_the_printed_cycle_matches_the_sequence_the_engine_runs` (C-010) | `AssertionError: the guide's expire_snapshots call has drifted from the sequence this unit measured:` / `printed: [('table', "'{}'"), ('retain_last', '1')]` / `measured: [('table', "'ns.orders'"), ('older_than', '1787654960475'), ('retain_last', '1')]` |
| P9 | the PRINTED `retain_last` edited to 5 (the Critic's own mutation) | same | `AssertionError: the guide prints expire_snapshots(retain_last => 5) where this unit measured 1` — `assert '5' == '1'` |
| P10 | one of the two MW-8 ledger citations replaced by a fabricated `9,999 kB` figure | none — C-009 stays GREEN | `2 passed` |

**P8 is the load-bearing one:** it restores exactly the defect the Critic filed as F-MW8-1 and
the suite now reds on it. **P10 is recorded as a negative result**, not a gap left quiet: C-009
checks that each home is linked, and a number invented beside a link that still appears elsewhere
in the section passes it. That is why C-009's proposition is worded narrowly and C-010 exists.

P2a and P2b are the red-green pair that identifies the RDF-1 mechanism rather than the outcome,
the same shape MW-7's C-011 used. In band, the 100 %-dead files are kept; out of band, the very
same files are rewritten. Size is the only thing that changed.

C-003, C-005 and C-008 are held by the same battery and were not separately mutated: each asserts
a shape the recorded cycle reports rather than a branch anything chooses.

## 7. Lockstep

- Guide: [../../../docs/guide/iceberg-guide.md](../../../docs/guide/iceberg-guide.md) "The
  maintenance runbook", rowed in [../../../docs/guide/map.md](../../../docs/guide/map.md).
  **Verified by executing the printed block itself** — extracted from the page, run against the
  built module on the §3 fixture (6,000 rows, 2 partitions, six MERGEs), at three cutoffs. Steps
  2 to 4 did real work in every run (`12, 2`; 48 rewritten / 4 added; `9, 1`) and step 5's
  results are the §3.2 table. The earlier one-row check is withdrawn: it exercised no step.
- Pin: `python/repark/tests/test_mw8_runbook.py`, rowed in
  [../../../python/repark/tests/map.md](../../../python/repark/tests/map.md).
- Driver reuse: the census, the generator, the CTAS, the MERGE SQL and the sequence itself come
  from [../../../python/repark-parity/bench/mw7/map.md](../../../python/repark-parity/bench/mw7/map.md).
  This unit adds no second implementation of any of them, and nothing under `bench/` changed.
- STATUS: a dated MW-8 line on the MW scorecard; the remainder becomes V3-2.
- Slate: MW-8 leaves [../../../briefs/next-sequence.md](../../../briefs/next-sequence.md); V3-2
  is next.
- Registry: no new row. `RDF-1`, `MOR-2`, `ORPHAN-1`, `ORPHAN-2`, `MANIFEST-1`, `MANIFEST-2` and
  `MANIFEST-3` are cited by the guide and unchanged by this unit. F-MW8-2 is the runbook meeting
  `MANIFEST-1`, not a new divergence.
- Scratch: the warehouses this unit's probes wrote were deleted after the numbers were read.
  Nothing generated by this unit is committed.

## Coverage attestation (Critic, reissued at `c56bd70`)

```yaml
COVERAGE_ATTESTATION:
  pr_unit: MW-8
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        Re-walked every number in the rewritten section against its home. The new MW-8 §3
        citations, the MW-7 §6.1-§6.5 citations and the new §4.3/§4.4 citations all resolve and
        all match. Re-checked the two claims F-MW8-4 disputed: 2.02x / 2.45x / 1.90x now cite
        the sections that state them, and "a fifth of that" is deleted. Checked the three
        remediation sub-claims the coordinator named — retain_last as a floor, `now` computed
        once, and the withdrawn one-row verification. Found four residual prose defects (F-C8-1,
        F-C8-2, F-C8-4, F-C8-5, F-C8-6), all S3.
      artifacts: ["link/anchor resolver over the section: 20/20 files, 18/18 fragments OK", "fork transaction/expire_snapshots.rs:43-49 — 'the floor is ref.min_snapshots_to_keep'", docs/guide/iceberg-guide.md:623, :653-662, task/ledgers/completed/mw-8-maintenance-runbook-ledger.md §3, §7]
    - id: AT-2
      status: ATTACKED
      evidence: >
        Drove the expire cutoff across its whole boundary range through the printed block:
        now-7d (as printed), now-3d, now (zero window) and now+1d. The zero window is the
        boundary that matters and it behaves as the record says — time travel to the CTAS
        snapshot fails with the exact message quoted in the ledger. Re-confirmed the orphan
        cutoff still clears the 24-hour floor at three days, and the armed/floor probes still
        hold.
      artifacts: [scratchpad/mw8-critic/verify_cutoffs.py, scratchpad/mw8-critic/sixcols.py, "AnalysisException: Error during planning: unknown Iceberg snapshot id …: not found in table metadata"]
    - id: AT-3
      status: ATTACKED
      evidence: >
        Re-checked the idempotency defect I filed as F-MW8-2 against what the guide now says. The
        "Retrying a step" subsection names the exception, the trigger, the loop the refusal's own
        remedy causes, and two remedies; the porting list carries the refusal half with an
        instruction for the DAG task. Every element matches the refusal I measured in the first
        pass and the registry row it comes from.
      artifacts: [docs/guide/iceberg-guide.md:723-739, :755-758, "docs/spark-sql-iceberg-parity.md MANIFEST-1 — the refusal clause"]
    - id: AT-4
      status: ATTACKED
      evidence: >
        Re-read the reworded S3 Tables subsection against MW-1's record. "serializable" is gone,
        no mechanism is restated, and what remains is a pointer to the section that owns the
        fact — which is what F-MW8-5 asked for. Also re-verified the ordering claim survives the
        rewrite: the printed order still matches the driver's, now held mechanically by C-010
        rather than by prose.
      artifacts: [docs/guide/iceberg-guide.md:728-729, task/ledgers/archive/2026-08/2026-08-21-mw-1-lift-fence-ledger.md §2]
    - id: AT-5
      status: N/A
      justification: >
        Unchanged from the first pass and untouched by the remediation. Docs plus one
        local-catalog test: no authn/z decision, no secret, no deserialization, no untrusted
        input, no path traversal. The one destructive call the section documents (`dry_run =>
        false`) is attacked under AT-2 as a safety boundary.
    - id: AT-6
      status: ATTACKED
      evidence: >
        This is where the S1 lived and where I spent the most measurement. COUNT(*) held 6,000
        through all four cutoff runs. The integrity question the remediation raises is the new
        one — the cutoff now destroys history on purpose — so I measured what each window costs:
        seven days keeps time travel to the CTAS snapshot, zero and +1 day destroy it. The guide
        states both and names the window as the price. Verified the block cannot expire the
        snapshots the same cycle wrote, which is the property that makes a scheduled cycle safe.
      artifacts: ["now-7d: snapshots 12 -> 12, time travel works (COUNT(*)=6000)", "now+1d: 48 / 12 / 0 / 39 / 11 / 0, snapshots 12 -> 1, time travel gone", "zero window: all 5 snapshots the cycle itself wrote survived"]
    - id: AT-7
      status: ATTACKED
      evidence: >
        Re-measured the unbounded-growth defect. With the cutoff now printed, the operator can
        choose whether the cycle reclaims: at seven days on a minutes-old table it still grows
        (547,839 -> 1,003,006 B in one cycle), and the guide now says so in the same paragraph
        rather than promising 43x. At +1 day the same cycle shrinks the warehouse
        (547,007 -> 495,205 B). The growth is now a documented consequence of a stated choice
        instead of a silent defect, which is what closes the AT-7 exposure.
      artifacts: [scratchpad/mw8-critic/verify_cutoffs.py, docs/guide/iceberg-guide.md:644-651]
    - id: AT-8
      status: ATTACKED
      evidence: >
        Re-read all six porting bullets against their registry rows. Five cite a row and match
        it, including the MANIFEST-1 bullet that now carries the refusal. The sixth — the new
        expire-cutoff bullet — cites nothing and is not a divergence: the fork mirrors Java's
        5-day `history.expire.max-snapshot-age-ms` default, and no registry row exists because
        nothing diverges. Filed as F-C8-7, S3.
      artifacts: [docs/guide/iceberg-guide.md:743-761, "fork transaction/expire_snapshots.rs:56-57, :59 'divergences from Java (timing only, same outcomes)'"]
    - id: AT-9
      status: ATTACKED
      evidence: >
        The six zeros that gave an operator no signal are now explained before they appear: the
        section tells the reader that a young table answers zeros and why that is correct. The
        step 4 refusal, whose own remedy loops, is documented with the way out. Both operability
        gaps I filed are closed in prose. One residual: the step 5 one-liner still describes
        expire as content-based, which contradicts the paragraphs below it (F-C8-6, S3).
      artifacts: [docs/guide/iceberg-guide.md:611 vs :653-662, docs/guide/iceberg-guide.md:731-739]
    - id: AT-10
      status: ATTACKED
      evidence: >
        Re-ran the two provocations that carry the S1 regression proof and one that carries
        C-009's. P8 and P9 reproduce the ledger's recorded output verbatim; P3 reds but with a
        message the ledger's "verbatim" row no longer matches (F-C8-4). Confirmed C-009's
        narrowed proposition matches what the check does, and that P10's negative result is
        recorded rather than hidden. Read C-010's implementation against its clause: the code
        compares literal values, the clause says so, and the test's own docstring denies it
        (F-C8-3). Baseline 10 passed before and after; clone restored and clean.
      artifacts: ["P8: printed [('table', \"'{}'\"), ('retain_last', '1')] vs measured [… ('older_than', '1787655963591') …]", "P9: the guide prints expire_snapshots(retain_last => 5) where this unit measured 1", "P3: the runbook section links no home for: #mor-2", python/repark/tests/test_mw8_runbook.py:100, :480, :507-509]
  reattested: [AT-1, AT-2, AT-3, AT-4, AT-6, AT-7, AT-8, AT-9, AT-10]
  complete: true
```

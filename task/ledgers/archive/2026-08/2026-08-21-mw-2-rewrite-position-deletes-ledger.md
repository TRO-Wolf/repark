# MW-2 — the procedure that reclaims what merge-on-read leaves behind

**Date:** 2026-08-21 · **Branch:** `feat/mw-2-rewrite-position-deletes` · **Base:** `90431bf`
(`main`, post-#196) · **Charter:** [mw-0-charter-ledger.md](../../completed/mw-0-charter-ledger.md) · **Design:**
[../docs/design/iceberg-maintenance-wave.md](../../../../docs/history/iceberg-maintenance-wave/design.md)

MW-0 measured the campaign's thesis: ten merge-on-read MERGEs grow delete files one per merge and
never reclaim them, and scan cost tracks that growth 2.1× on a table whose contents never change.
This unit ships the procedure that reverses it, and closes the last Spark column any procedure
here was missing.

## 1. `rewrite_position_delete_files`

Wired to the fork's `RewritePositionDeleteFiles`, which mirrors Java's
`RewritePositionDeleteFiles$Result` one accessor at a time. The charter predicted this unit would
choose nothing about its result schema (C-205) and that held: four columns, Spark's names, Spark's
order, two ints then two bigints, all non-nullable.

The schema was confirmed by **executing** the procedure on a live Spark 4.0.1 + Iceberg 1.10.0
oracle rather than read from the jar's `OUTPUT_TYPE` constant. MW-0 could only read the constant
for the two procedures the 4.1.2 oracle refused to run; MW-1 stood up the 4.0.1 oracle, and every
value in this unit is measured on it.

The `options` map and the `where` filter refuse loudly, matching the austerity `rewrite_data_files`
already had. The fork does expose `RewritePositionDeleteFiles::filter`, so this is a deferral with
a known landing place, not a capability claim.

## 2. `rewrite_data_files` returns Spark's fifth column

`removed_delete_files_count` was the last omitted column on this surface. It is now present,
non-nullable, and reads `0`.

That zero is measured, not assumed. Spark reported `0` on every fixture tried — including a
partitioned table with six data files per partition and twelve live position deletes, run both
with `options => map('remove-dangling-deletes','true')` and with default options. The column
counts what Java's RemoveDanglingDeletes sub-action removed; that sub-action runs only under the
`remove-dangling-deletes` option, whose default is false
(`RewriteDataFiles.REMOVE_DANGLING_DELETES_DEFAULT`, javap-verified against the shipping jar), and
this procedure refuses the options map. The non-default path is unreachable, so the count of
removals is genuinely zero.

This is the distinction the module's never-fabricate rule turns on. A fabricated count is a number
standing in for one the engine could not obtain. This is a real count of a real quantity that
happens to be zero, and it agrees with Spark's default-path answer.

**No procedure on this surface omits a Spark column any more.**

## 3. Two divergences, measured and registered

Both are file layout. Neither changes a row, and both were measured across their boundary rather
than asserted at one point.

### MOR-1 — compaction runs below Spark's floor

| live delete files | Spark | repark |
|---:|---|---|
| 1 | `0, 0, 0, 0` | `0, 0, 0, 0` |
| 2 | `0, 0, 0, 0` | `2, 1, …` |
| 4 | `0, 0, 0, 0` | `4, 1, …` |
| 8 | `8, 1, …` | `8, 1, …` |

Spark's planner extends `SizeBasedFileRewritePlanner`, whose `MIN_INPUT_FILES_DEFAULT` is 5. The
fork's `RewritePositionDeleteFiles` drops only single-file groups (`entries.len() < 2`).

**The fix belongs in the fork, and this unit deliberately did not attempt it.** The fork's
`RewriteDataFiles`, in the same crate, already implements the full gate — `min_input_files = 5`,
`enoughInputFiles || enoughContent || tooMuchContent`. So this is one action out of step with its
neighbour, and closing it means giving the position-delete planner the gate the data-file planner
already has.

The alternative was to gate admission in the CALL router: walk the manifests, group live
position-delete files by `(spec, partition)`, and skip the fork call when no group reaches five.
It was rejected. Spark's rule is not a file count — a group under the count still rewrites when
its bytes exceed the target — so a count-only gate in the router would trade over-compaction for
under-compaction and put planner admission in two places at once. Registered as a row with a pin
that reds on purpose when the fork gains the gate.

### MOR-2 — the merge-on-read writer is partition-granularity

One MERGE touching six distinct data files writes **one** position-delete file here. Spark writes
six, because `TableProperties.DELETE_GRANULARITY_DEFAULT` is `file` — confirmed on the oracle by
leaving the property unset and watching eight deletes across eight data files produce eight delete
files. This engine reads no granularity property at all.

Pre-existing write-path behavior, surfaced by MW-2 rather than introduced by it. It is registered
here rather than left for MW-5 because it is what makes the parity pin honest: the parity claim is
parity with Spark **on a partition-granularity table**, and MOR-2 is the measurement showing that
is the only kind of table this engine writes.

## 3a. The guard: a deletion vector refuses rather than reporting zeros

Found by asking what the MOR-1 and MOR-2 measurements imply for a table this engine did not
write, which is exactly MW-4's case.

A format-v3 table carries **Puffin deletion vectors** instead of Parquet position deletes. A
vector is file-scoped — one per data file, never bin-packed — so the fork's collector skips it by
design (`data_file.file_format() != DataFileFormat::Parquet`) and returns it in no count. Wired
naively, this procedure answers four zeros on such a table. That is indistinguishable from
"nothing to compact", so an operator runs the reclaim procedure forever on a table that never
reclaims, and the campaign's one invariant — no refusal becomes silent — is broken by a surface
this very unit added.

It now refuses, naming the count. Refusing on **any** live vector, including a mixed table that
also holds compactable Parquet position deletes, is deliberate: the procedure's contract to its
caller is the table's position deletes, and silently doing part of that job is the failure being
prevented.

This engine writes neither half of the problem — it creates tables at format v2
(`'format-version' = '3'` refuses at CREATE, verified) and refuses merge-on-read writes on a v3
table (`resolve_merge_mode`). The exposure is entirely tables written elsewhere, which is why it
was invisible until the drop-in case was thought through.

**Spark does the silent thing too, so this is stricter than Spark rather than a bug fix.**
Measured after the guard was written, which is the right order only by luck: a live Spark 4.0.1 +
Iceberg 1.10.0 session created a v3 table, three merge-on-read `DELETE`s produced three `PUFFIN`
delete files, and `rewrite_position_delete_files` returned `0, 0, 0, 0` and left all three in
place. Spark's own answer on a v3 table is the silent no-op this engine now refuses to give.

That reframes the change. It is not restoring parity — it is a deliberate divergence, taken on the
same reasoning as the orphan-files dry-run default the owner ruled in OD-2: on a maintenance
surface a silent zero is indistinguishable from "already clean", and the operator never learns the
reclaim never happened. It is also a drop-in break, because a job that called this on a v3 table
under Spark got a useless success and now gets an error. Queued as `B-MOR-3` and flagged to the
owner as reversible in one line.

**What is and is not pinned.** The classification rule is pinned directly as a table, and a
mutation dropping its data-file exclusion reds it. The no-false-positive half is pinned end to
end: the guard reads zero on a v2 table this engine wrote, and on a table with no snapshot at all.
The **vector-present path has no fixture**, because this engine cannot produce a deletion vector;
pinning it needs a v3 table written by another engine. That is a cross-engine fixture and belongs
with the v3 porting work, not here — recorded rather than papered over.

## 4. The pins, and what each one is for

| Pin | Asserts | Red before |
|---|---|---|
| `call_rewrite_position_delete_files_compacts_like_spark` | 8 delete files → `rewritten = 8`, `added = 1`, delete files 8 → 1, row set unchanged | Procedure refused |
| `call_rewrite_position_delete_files_is_a_zero_result_when_there_is_nothing_to_do` | No deletes, and exactly one delete file, both give four zeros and touch nothing | Procedure refused |
| `call_rewrite_position_delete_files_refuses_options_and_where` | Deferred arguments refuse by name | Refused under the wrong message |
| `call_rewrite_data_files_returns_sparks_five_columns` | Spark's five, all non-nullable, fifth reads 0 | 4 columns |
| `call_mor1_compacts_below_sparks_min_input_files_floor` | The divergence held at 4, the largest count where the two disagree | (green — pins existing behavior) |
| `call_mor2_merge_writes_one_position_delete_per_partition` | One delete file where Spark's default writes six | (green — pins existing behavior) |
| `call_deletion_vector_rule_matches_the_forks_skip_clause` | The refuse rule, including that a DATA file is never caught | (new; mutation-checked) |
| `call_rewrite_position_delete_files_guard_passes_a_v2_table` | The guard does not refuse a table it can compact | (new; the pin that separates a fix from a wrecking ball) |
| `call_deletion_vector_guard_handles_a_table_with_no_snapshot` | The early return, so a fresh table is not refused | (new) |

The four procedure pins were run against the unfixed `call.rs` and all four went red for the
stated reason. The two divergence pins are green by construction, which is what a BACKLOG row's
pin is for.

The byte columns are asserted as an ordering — `rewritten > added > 0` — not as values. They are
real parquet sizes and this engine's writer does not produce byte-identical files to Spark's;
pinning Spark's `11429`/`1454` would be pinning Spark's parquet encoder.

## 5. What MW-1 handed forward, and where it went

The MW-1 ledger left MW-2 two things and both are discharged here.

**The `rewrite_data_files` omitted column** is closed rather than registered, following the
owner's 1:1 ruling and MW-1's precedent — a divergence that no longer exists does not get a row.

**"Compaction keeps position deletes where Spark's orphans them."** MW-1 found this while pinning
the expire split and named MW-2 as the unit where the fork's dangling-delete surface comes into
scope. It came into scope and the answer was narrower than expected: the surface exists
(`RemoveDanglingDeleteFiles`), but Spark does not reach it from `rewrite_data_files` either unless
the caller opts in, and it reports `0` even then on every fixture measured. So there is nothing to
match, and MW-1's observation resolves as a property of the option rather than a gap. The
procedure that actually reclaims delete files is the one this unit wired.

## 6. What MW-5 now inherits

Zero of the three registry rows the charter queued — MW-1 closed the expire funnel, MW-2 closed
the `rewrite_data_files` column. In their place MW-5 inherits **MOR-1 and MOR-2**, already landed
as rows with pins, and two decisions for the owner. First, whether the fork's
position-delete planner gets the size-based gate its data-file neighbour already has — fork work,
so a question rather than a queued unit. Second, whether the deletion-vector refusal stays
stricter than Spark (`B-MOR-3`).

MW-2 also put **format v3 on the roadmap** as track A12, promoted out of "watch, do not schedule"
by the owner on 2026-08-21. The measurement that moved it: the fork already ships deletion-vector
read and write, Puffin, row-lineage spec fields and the v3 types, so v3 is mostly engine-side
wiring rather than a fork campaign. Its first unit builds the cross-engine fixture this unit could
not, which is what promotes `B-MOR-3` from a queued candidate to a row.

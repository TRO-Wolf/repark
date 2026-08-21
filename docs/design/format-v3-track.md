# Format-v3 — the scope audit, and what it changed

**Settled 2026-08-21 · V3-0 · base `a893c3b` (`main`, post-#198) ·
roadmap item [A12](../../task/roadmap-intake-2026-08-21.md#a12-format-v3-and-deletion-vectors--owner-scheduled-2026-08-21) ·
ledger [`task/v3-0-charter-ledger.md`](../../task/v3-0-charter-ledger.md)**

A12 was written during roadmap intake, from source reading rather than execution. It described a
read path that was "not gated — unverified" and a write side fenced off behind four refusals, and
it proposed a six-unit track starting from a cross-engine fixture. This document is what happened
when the fixture was actually built and the surfaces were actually run.

Two things changed. The engine turned out to be **further along on v3 than A12 assumed**: it reads
Spark-written deletion vectors correctly and appends to a v3 table with spec-correct row lineage,
both verified against Spark rather than inferred. And one surface turned out to be **wrong in a
way nothing was watching**, which is why this document ships with a guard attached rather than as
documentation alone.

## 1. The fixture

Everything below is measured against a real format-v3 table, not a synthetic one. A live PySpark
4.0.1 session with the shipping Iceberg 4.0/Scala-2.13 1.10.0 runtime, a Hadoop catalog on local
disk, `format-version = 3` and all three write modes set to merge-on-read.

The pinned 4.1.2 oracle is unusable here for the same reason recorded under MOR-1: Spark's
`DataSourceV2Relation.create` signature changed between 4.0 and 4.1, so the shipping jar cannot
execute Iceberg maintenance procedures at all.

Two shapes were used:

| Fixture | Contents | Spark's ground truth |
|---|---|---|
| single-file | 1000 rows, one `DELETE` on `id % 7 = 0` | 1 Parquet data file (1000 rows), 1 Puffin vector (143 positions), 857 live rows, `sum(id) = 428429` |
| six-file | six 100-row inserts, one `DELETE` on `id % 11 = 0` | 6 Parquet data files, 6 Puffin vectors, 545 live rows, `sum(id) = 1392090` |

Addressing them needed a decision A12 correctly flagged as its first question, and §4 answers it.

## 2. What already works, verified against Spark

**Reading a v3 table with deletion vectors is correct today.** The engine returned 857 rows and
`sum(id) = 428429` on the single-file fixture — Spark's numbers exactly, with the Puffin vector
applied. Predicate pushdown (`WHERE id < 100` → 85 rows) and the `files`, `snapshots` and
`history` metadata tables all answered correctly on the same table.

**Appending to a v3 table is correct today, including row lineage.** `INSERT` committed, and the
new snapshot carried `first-row-id = 1000` and `added-rows = 1` with the table's `next-row-id`
advancing 1000 → 1001. Spark then read the engine's commit back and agreed: 858 rows,
`sum(id) = 528429`, and the inserted row served as `_row_id = 1000`,
`_last_updated_sequence_number = 3`. That is a full cross-engine round trip on a format the
project had not claimed to support.

**The four refusals A12 listed all fire, with accurate messages.** Merge-on-read `DELETE`,
`UPDATE` and `MERGE` each refuse and name the format version; `rewrite_position_delete_files`
refuses with MW-2's deletion-vector guard, which counted the live vectors correctly — one on the
single-file fixture, six on the six-file one. MW-2 wrote that guard without a v3 table to try it
on. It works.

## 3. What is wrong: `rewrite_data_files` reassigns row lineage

`rewrite_data_files` has no format-version check. On the six-file fixture it compacted seven data
files into one, applied all six deletion vectors, and produced **the correct 546 rows**. Spark read
the result back and agreed on every row.

It also reassigned every row's lineage. The row `id = 5099` arrived as
`_row_id = 599, _last_updated_sequence_number = 6` and came out as `_row_id = 691, seq = 9`. Spark
performing the same compaction on the same fixture left it at `599 / 6` on both sides of the call.

That preservation is the point of the field. V3 makes row lineage mandatory so that moving a row
between files is distinguishable from changing it, which is what lets a downstream consumer read
incrementally. A compaction that regenerates `_row_id` reports that all 546 rows changed when none
did, and it does so while returning a result that reads as a clean success.

The fix is not available in the CALL router. The fork's `maintenance/rewrite_data_files.rs`
contains no row-lineage handling at all, while the spec layer beneath it — `manifest/entry.rs`,
`table_metadata_builder.rs`, `snapshot.rs` — carries `first_row_id` throughout. One action is out
of step with the crate around it, the same shape as MOR-1.

**Reachability, stated plainly.** This engine cannot create a v3 table: `CREATE TABLE` and CTAS
both refuse `format-version`. So nothing RePark wrote is exposed. What is exposed is a v3 table
that was already in the catalog when RePark was pointed at it — an existing Glue database holding
tables written by Spark or Athena, which is precisely the drop-in case the product is for. MW-1
lifted the maintenance fence on Glue and S3 Tables, so `CALL rewrite_data_files` reaches such a
table today.

**V3-0 therefore ships a guard**, not just a finding: the procedure refuses format-v3 outright and
names row lineage. That is stricter than Spark, which does the rewrite correctly, and it is the
same trade MW-2 took for deletion vectors and OD-2 took for the orphan dry-run default. An
unattended maintenance procedure gets a loud stop rather than a plausible wrong answer. Registry
row `V3-LINEAGE-1`.

### 3a. A second divergence the guard makes unreachable

The same compaction left all six Puffin vectors live in the manifest after the data files they
were scoped to had been rewritten away. Spark removed them and reported
`removed_delete_files_count = 6`. Nothing in this engine could have reclaimed them —
`rewrite_position_delete_files` refuses deletion vectors, and `expire_snapshots` does not remove
live manifest entries. With V3-LINEAGE-1 in place no v3 rewrite runs, so no vectors are stranded.
Queued as `V3-DANGLE-1`, owned by whichever unit lifts the guard.

### 3b. What this corrects in MW-2

MW-2 closed `removed_delete_files_count` as an honest constant `0`, reasoning that Java's
`REMOVE_DANGLING_DELETES_DEFAULT` is false and this procedure refuses the options map, so the only
path that removes delete files is unreachable. Measured on v2, that is exactly right: Spark
compacted six data files and reported `removed_delete_files_count = 0`, leaving all six position
deletes in place.

On v3 it is wrong. Spark reported `6` with no option set, because a deletion vector is scoped to a
single data file and dies when that file is rewritten. Removal is an ordinary consequence of
compaction there, not an opt-in sub-action. The constant is correct for every version this
procedure still runs on and becomes wrong the moment v3 is admitted; the note now lives next to
the constant in `call.rs`.

## 4. The addressing question, answered

A12 named this as V3-1's first question: this engine has no Hadoop-catalog surface, so reaching a
foreign table needs either a catalog shim or an adopt-existing-table path. Measured, the answer is
adoption, and it is small.

`Catalog::register_table(ident, metadata_location)` is on the fork's trait and is **fully
implemented for the memory catalog and for Glue**. S3 Tables refuses it with a clean
`FeatureUnsupported`, which is an honest answer rather than a gap to close. Registration validated
the metadata before claiming the pointer, and the adopted table read correctly.

Spark reaches the same fork-level call through a procedure, so wiring it is parity work rather
than invention. Read out of the 1.10.0 jar's own bytecode rather than from documentation:

| | |
|---|---|
| Name | `system.register_table` |
| Parameters | `table` STRING **required**, `metadata_file` STRING **required** |
| Result | 3 columns, **all nullable** BIGINT: `current_snapshot_id`, `total_records_count`, `total_data_files_count` |
| Values | read from the current snapshot's summary (`total-records`, `total-data-files`); all three are null when the table has no current snapshot |
| Refusals | empty `metadata_file`; a non-Iceberg catalog |

It calls `Catalog.registerTable(identifier, metadataFile)` directly, which is the same surface the
fork exposes.

**One caveat, isolated by experiment.** The Spark fixture's metadata pointer is
`v3.metadata.json`, the Hadoop-catalog convention. Adoption succeeded and reads worked, but every
write failed with `Invalid metadata file name format: v3.metadata.json`, because the fork's
`MetadataLocation` parser requires `<version>-<uuid>.metadata.json` to compute the next pointer.
Copying the identical file to a name of that shape made `INSERT` and `expire_snapshots` both
succeed, which is how the cause was isolated from anything to do with v3. Catalogs that write
version-uuid pointers, Glue among them, are unaffected. Queued as `V3-ADOPT-1`; the error names
the symptom rather than the cause, and whichever unit lands the adopt path owns that text.

## 5. The unit slate, as revised

A12's six units still hold in outline. Three change.

| Unit | Scope | Change from A12 |
|---|---|---|
| **V3-0** | This audit, and the `rewrite_data_files` row-lineage guard | New — A12 had no charter unit |
| **V3-1** | Wire `CALL system.register_table`; land the cross-engine v3 fixture and promote `B-MOR-3` to a row | Was "read a v3 table and build the fixture, blocked on an addressing decision". The decision is made (§4) and the read half is already verified (§2), so what is left is the surface and the pins |
| **V3-2** | Create v3 tables behind an explicit opt-in | Unchanged, still wants MW closed first |
| **V3-3** | Merge-on-read writes on v3 via the fork's `DVFileWriter` | Unchanged, still the big one |
| **V3-4** | Row lineage as a read surface and a write obligation | Grows a read half: `_row_id` and `_last_updated_sequence_number` are not plannable columns today (`V3-ROWID-1`), where Spark serves both |
| **V3-5** | v3 maintenance: lift V3-LINEAGE-1 once the fork carries lineage through a rewrite; `remove_dangling_delete_files` for `V3-DANGLE-1` | Grows a second obligation. A12 already warned not to scope this as "make the MW-2 refusal go away"; it now also owns `removed_delete_files_count` (§3b) |
| **V3-6** | v3 types, reconciled with H6 VARIANT and the ANSI nanosecond work | Unchanged |

**Sequencing against MW is unchanged and still binding.** V3-1 can run any time. V3-2 and later
want MW-4's live acceptance behind them, because adding a second format version underneath the
campaign's only real-catalog evidence would mean proving two things at once.

## 6. Fork work this track needs

Both items are fork-side and neither is in the CALL router.

1. **Row lineage through `RewriteDataFiles`** — carry `first_row_id` and the row-level sequence
   number across a data-file rewrite instead of letting the new file take fresh values. Gates
   V3-LINEAGE-1's removal. The spec layer already models the fields.
2. **`MetadataLocation` and foreign pointer names** — either accept the Hadoop `vN.metadata.json`
   convention or fail with an error naming the convention rather than the filename. Gates the
   error-quality half of `V3-ADOPT-1`; the functional half is avoidable by using a catalog that
   writes version-uuid pointers.

Neither blocks V3-1.

## 7. What was measured and is not claimed

- **Nothing was measured on Glue or S3 Tables.** Every number here is local filesystem. §4's
  claim that Glue implements `register_table` is read from the fork's source at the pin, not run.
- **`expire_snapshots` on v3 returned all zeros** on a fixture with nothing to expire, which is
  the same answer it gives on v2 and is not evidence either way. It was not exercised against a
  table with expirable snapshots.
- **No equality-delete or partitioned v3 fixture was built.** Both fixtures are unpartitioned and
  carry only position-style deletes.
- **The row-lineage guard is pinned on an upgraded table, not a Spark-written one.** The pin
  builds v3 through the fork's own `upgrade_table_version` so it runs in CI with no oracle. The
  Spark-written half is measured in the ledger and cannot be a pin until V3-1 lands a fixture CI
  can read.

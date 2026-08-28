# Format-v3 — the scope audit, and what it changed

**Settled 2026-08-21 · V3-0 · base `a893c3b` (`main`, post-#198) ·
roadmap item [A12](../../task/roadmap/mid-term/roadmap-intake-2026-08-21.md#a12-format-v3-and-deletion-vectors--owner-scheduled-2026-08-21) ·
ledger [`task/v3-0-charter-ledger.md`](../../task/ledgers/staging/v3-0-charter-ledger.md)**

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

**Reachability, stated plainly.** By default this engine cannot create a v3 table: `CREATE TABLE`
and CTAS refuse `format-version = 3` unless the session sets
`repark.sql.allowCreateFormatVersion3` (V3-2). So nothing RePark wrote is exposed without that
opt-in. What is still exposed without it is a v3 table
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

> **Errata 2026-08-24 (MW-7).** That v2 sentence is true of THIS fixture and is not general.
> The six-file fixture deletes `id % 11 = 0` — roughly 9 % of each file — so no file is
> delete-laden enough to matter. Re-measured on the same Spark 4.0.1 + Iceberg 1.10.0 oracle
> with delete-heavy v2 shapes (tiling, and 30 % deleted), the same sequence ends with **zero**
> delete files and zero delete records, still reporting `removed_delete_files_count = 0` and
> still with `remove-dangling-deletes` off. Java's `BinPackRewriteFilePlanner` has a live
> `tooHighDeleteRatio` clause at `DELETE_RATIO_THRESHOLD_DEFAULT = 0.3`: past that ratio the
> file is rewritten and its delete files die in the rewrite commit, with the count staying 0
> because nothing was *removed as dangling*. So "delete files survive compaction on v2" is a
> property of a low delete ratio, not of v2. The fork defers that clause, which is why this
> engine retains them without bound — registry row
> [RDF-1](../spark-sql-iceberg-parity.md), fork ask F-16.

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
version-uuid pointers, Glue among them, are unaffected. **Admitted as registry row
`V3-ADOPT-1` by V3-1 (2026-08-21):** the CALL write now names the Hadoop convention and the
version-uuid shape; the fork still cannot compute the next pointer from `vN.metadata.json`.

## 5. The delivery sequence, revised 2026-08-28

The owner approved this sequence after RP-2 measured the first v3 write paths. The sequence
keeps useful guarded work, fixes the fork invariant before lifting the guard, and consumes the
repair through a fresh immutable repin.

### Step 1 — land the narrowed RP-2 increment

Salvage `feat/rp-2-fork-repin` at fork rev `ce92a7bf`. Its honest product contract is narrower
than its first ledger narrative:

- a first merge-on-read DELETE on a DV-free v3 table commits a Puffin DV;
- any table with a live DV refuses DELETE before a write, including a second engine DELETE and
  the Spark-written shared-Puffin fixture;
- COW DELETE may lift when the committed lineage pins stay Spark-equal;
- COW UPDATE and MERGE remain guarded;
- `rewrite_data_files` remains guarded because the measured rewrite reassigned row lineage;
- the F-3 dangling-delete option and true count may land independently.

The unit must add a committed second-DELETE refusal pin and remove every claim that re-delete
already merges and supersedes. The guard is a temporary safety boundary, not the final V3-3
capability. PR #254 targets a different fork batch and cannot amend this narrowed unit; it is
superseded when this ruling lands.

### Step 2 — repair shared-Puffin DV closure in the fork

Fork item F-17 owns the table-format invariant. The measured fixture has two DV blobs in one
Puffin file: the `part=0` blob deletes id 2 and the `part=1` blob deletes id 5. An engine DELETE
of id 1 touched only `part=0`; the result was `{3,4,5,6}` instead of `{3,4,6}`. The untouched
sibling blob vanished when path-keyed removal superseded the shared Puffin.

The fork repair must carry every live sibling blob when one blob is superseded. It must cover
different partitions, DELETE and UPDATE, and Java reading the fork-written result. The sabotage
case removes sibling carry and must make the regression red. The detailed request and the engine
pin are in the [fork handoff](../../task/roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md#f-17-north-star-blocker-added-2026-08-28--shared-puffin-dv-sibling-closure).

### Step 3 — charter RP-3 against one post-fix fork SHA

RP-3 takes the complete landed fork batch after F-17. It includes F-7 U3, F-16, F-9, F-15,
F-14, and F-17 when they are present at the selected SHA. A later fork landing does not widen
the unit. The repin re-runs every standing duty in `AGENTS.md` and measures these cells:

| Input state | Operation | Required result |
|---|---|---|
| DV-free | first MOR DELETE | Puffin DV committed; Spark reads identical rows |
| engine-written DV | second MOR DELETE | positions merged; old DV superseded; one live DV |
| Spark-written DV | MOR DELETE on the same data file | same result as engine-written input |
| shared Puffin | touch one of several blobs | untouched sibling deletes stay effective |
| multiple files and partitions | one DELETE touches several files | one correct DV per data file; spec and partition correct |
| equality delete plus DV | DELETE touches the table | neither delete class is lost |
| DV-free COW | sequential DELETE operations | survivor rows and lineage stay Spark-equal |
| unsafe state | guarded operation | loud pre-write refusal; bytes and rows unchanged |

Every reachable cell runs through both SQL doors and the facade. Values and Arrow types are
asserted through `collect` or `to_arrow`. RP-3 also re-measures `rewrite_data_files` lineage at
the selected SHA. A red result becomes a fork or engine-owned finding before V3-5 charters; it
is never treated as closed because fork row R166 is green.

### Step 4 — deliver V3-3 and the guarded upgrade

V3-3 completes MOR DELETE, UPDATE, and MERGE across partitioned and spec-evolved tables. It
merges existing DVs, preserves Puffin siblings, pins concurrency and pre-write failures, and
round-trips each supported action through Spark. The opt-in v2-to-v3 upgrade lands only after
V3-3 proves the engine can safely mutate an upgraded table.

### Step 5 — run the remaining product units on their real dependencies

- **V3-4:** serve `_row_id` and `_last_updated_sequence_number`; preserve lineage across COW
  DELETE, UPDATE, and MERGE.
- **V3-5:** make maintenance production-grade: lineage-preserving compaction, DV removal and
  sibling closure, v3 position-delete conversion, true result counts, and the existing
  maintenance suite.
- **V3-6:** finish binary variant, nanosecond timestamps, unknown, and column defaults. V3-6 may
  run in parallel with V3-3 or V3-4 after its fork type support is pinned; it does not wait for
  V3-5 merely because its unit number is higher.

### Step 6 — close the v1.0 gate

Run full v3 statement coverage, the merged-code-only Glue and S3 Tables acceptance legs where
the service permits them, the v3 `10^7 x 50` scale workload, the nightly v3 oracle, and the v1.0
API review. The tag waits until every north-star matrix row is green or has a dated, pinned
DECLARED disposition.

FNP, TA performance, dbt, and the general correctness backlog may run while the fork lane is
blocked. They do not replace or delay a ready v3 unit.

## 6. Fork work this track needs

1. **Shared-Puffin DV sibling closure (F-17)** blocks lifting the broad live-DV guard and is the
   immediate fork dependency for full V3-3.
2. **Row lineage through `RewriteDataFiles`** remains an executed question. RP-2 measured a full
   reassignment at `ce92a7bf`; RP-3 re-runs it before assigning the fix to the fork or engine.
3. **`MetadataLocation` Hadoop pointer math (F-14)** must write the next `vN.metadata.json`
   pointer, or keep the dated engine refusal.
4. **V3 schema and IO support (F-15)** gates each V3-6 type independently.

The FNP and TA performance campaigns consume none of these fork surfaces.

## 7. What was measured and is not claimed

- **Nothing was measured on Glue or S3 Tables.** Every number here is local filesystem. §4's
  claim that Glue implements `register_table` is read from the fork's source at the pin, not run.
- **`expire_snapshots` on v3 returned all zeros** on a fixture with nothing to expire, which is
  the same answer it gives on v2 and is not evidence either way. It was not exercised against a
  table with expirable snapshots.
- **No equality-delete or partitioned v3 fixture was built.** Both fixtures are unpartitioned and
  carry only position-style deletes.
- **V1 is unpinned and unpinnable.** The guard admits it (correctly — v1 has no row lineage), but
  no test can build a v1 fixture: the catalog creates v2 and the fork refuses a downgrade.
- **The row-lineage guard is pinned on an upgraded table, not a Spark-written one.** The pin
  builds v3 through the fork's own `upgrade_table_version` so it runs in CI with no oracle.
  **V3-1 (2026-08-21) landed the Spark-written fixture** (`crates/repark-spark/src/tests/fixtures/v3-spark-mor/`)
  and uses it for `register_table` + `B-MOR-3`; the V3-0 lineage-refusal pin itself is still the
  upgraded-table one.

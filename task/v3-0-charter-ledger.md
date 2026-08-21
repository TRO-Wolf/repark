# V3-0 — the format-v3 scope audit, and the defect it found

**Date:** 2026-08-21 · **Branch:** `feat/v3-0-lineage-guard` · **Base:** `a893c3b` (`main`,
post-#198) · **Design:** [../docs/design/format-v3-track.md](../docs/design/format-v3-track.md) ·
**Roadmap item:** [roadmap-intake-2026-08-21.md](roadmap-intake-2026-08-21.md) A12

A12 promoted format-v3 out of "watch, do not schedule" on the strength of MW-2's finding, and
sketched six units from source reading. This unit is the scope audit that sketch never got: the
fixture built, the surfaces run, the claims checked one at a time.

It was intended as a charter — measurements and a slate, no product change, the shape MW-0 took.
It does not close that way. §3 found a silent divergence on a shipped, fence-lifted surface, and
this ledger records why the guard shipped with the audit instead of waiting for a unit of its own.

## 1. The fixture, and what it cost to get one

The engine cannot create a format-v3 table — `CREATE TABLE` and CTAS both refuse
`format-version` — so every measurement needed a table written elsewhere.

A live PySpark 4.0.1 session with the Iceberg 4.0/Scala-2.13 1.10.0 runtime, a Hadoop catalog on
local disk, `format-version = 3` and all three write modes merge-on-read. Spark's own ground
truth, captured before this engine was pointed at anything:

| Fixture | Spark reports |
|---|---|
| single-file | 1 Parquet data file (1000 rows), 1 `PUFFIN` delete file (143 positions), **857 live rows**, `sum(id) = 428429` |
| six-file | 6 Parquet data files, 6 `PUFFIN` delete files, **545 live rows**, `sum(id) = 1392090` |

The pinned 4.1.2 oracle cannot execute Iceberg maintenance procedures at all — the
`DataSourceV2Relation.create` break already recorded under MOR-1 — so 4.0.1 is the oracle
throughout.

Addressing the fixture is A12's stated first question and §4 answers it. For the audit itself the
fork's `Catalog::register_table` was called directly, which reaches the same code Spark's
procedure reaches and skips the question of what surface should expose it.

## 2. What A12 got wrong, in the optimistic direction

A12 listed "reading a foreign v3 table" as **not gated, unverified**, and everything else as
fenced. Two of those rows are better than stated.

**Reading is correct, not merely ungated.** The engine returned **857 rows, `sum(id) = 428429`** on
the single-file fixture: Spark's numbers exactly, deletion vector applied, with no engine change.
`WHERE id < 100` pushed down to 85 rows. The `files`, `snapshots` and `history` metadata tables all
answered — `files` showing both the Parquet data file and the Puffin vector.

**Appending is correct, including row lineage.** This is the one nobody had claimed. `INSERT`
committed to the v3 table; the fork advanced `next-row-id` 1000 → 1001 and stamped the new
snapshot `first-row-id = 1000`, `added-rows = 1`. Spark then read the engine's commit back:

```
SPARK_READBACK rows=858 sum=528429
  LINEAGE row_id=1000 seq=3 id=100000     <- the row RePark inserted
  LINEAGE row_id=999  seq=1 id=999
```

A full cross-engine round trip on a format the project does not claim to support, with the
mandatory lineage field assigned correctly.

**The four refusals fire, with accurate messages.** MOR `DELETE`, `UPDATE` and `MERGE` each refuse
and name the format version. `rewrite_position_delete_files` refuses through MW-2's
deletion-vector guard and counts the vectors correctly: `found 1 live Puffin deletion vector(s)`
on the single-file fixture, `found 6` on the six-file one. MW-2 built that guard with no v3 table
to try it against. It is right.

That last measurement is the repark half of `B-MOR-3`. It is **not** a pin — it needs an oracle,
so CI cannot run it — and the registry entry has been truthed up to say so rather than promoted.

## 3. What A12 missed: `rewrite_data_files` reassigns row lineage

`rewrite_data_files` carries no format-version check, and A12's table did not list it.

On the six-file fixture it compacted seven data files into one, applied all six deletion vectors,
and produced **the correct 546 rows** (`sum(id) = 1492090`, the extra row and value being the
probe's own insert). Spark read the result and agreed on every row. Nothing about the data is
wrong, which is exactly what makes this quiet.

Lineage did not survive. Measured on both engines over the same fixture shape:

| Engine | `id = 5099` before | `id = 5099` after |
|---|---|---|
| Spark 4.0.1 | `_row_id = 599`, `seq = 6` | `_row_id = 599`, `seq = 6` |
| RePark | `_row_id = 599`, `seq = 6` | **`_row_id = 691`, `seq = 9`** |

Preserving lineage across a rewrite is the reason the field is mandatory in v3: it makes moving a
row between files distinguishable from changing it. A compaction that regenerates `_row_id` tells
every incremental consumer that all 546 rows changed when none did, and returns a result that
reads as a clean success while doing it.

**It is fork work.** `maintenance/rewrite_data_files.rs` has no row-lineage handling at all, while
the spec layer under it (`manifest/entry.rs`, `table_metadata_builder.rs`, `snapshot.rs`) carries
`first_row_id` throughout. One action out of step with its own crate — the same shape as MOR-1,
and not something the CALL router can correct.

### Why the guard shipped here instead of waiting

The reachable case is not hypothetical. The engine cannot create a v3 table, so nothing RePark
wrote is exposed. What is exposed is a v3 table **already in the catalog** — an existing Glue
database holding tables written by Spark or Athena, which is the drop-in case this product is
for. MW-1 lifted the maintenance fence on Glue and S3 Tables, so `CALL rewrite_data_files`
reaches such a table today.

Shipping an audit that documents a live silent divergence and leaves it open is the failure this
project names in its own guard code: a procedure that succeeds quietly where it could not do the
job is the thing the guard exists to prevent. So V3-0 refuses format-v3 in
`rewrite_data_files` and names row lineage in the message. Stricter than Spark, which does the
rewrite correctly — the same trade MW-2 took for deletion vectors and OD-2 for the orphan dry-run
default.

Registry row `V3-LINEAGE-1`.

### 3a. A second divergence, made unreachable by the first

The same compaction left all six Puffin vectors live in the manifest after their data files were
rewritten away; Spark removed them and reported `removed_delete_files_count = 6`. Nothing in this
engine could reclaim them — `rewrite_position_delete_files` refuses deletion vectors and
`expire_snapshots` does not touch live manifest entries. With V3-LINEAGE-1 in place no v3 rewrite
runs, so nothing is stranded. Queued as `V3-DANGLE-1` rather than admitted as a row, because an
unreachable divergence cannot carry a live pin.

### 3b. A correction to MW-2

MW-2 closed `removed_delete_files_count` as an honest constant `0`: Java's
`REMOVE_DANGLING_DELETES_DEFAULT` is false, this procedure refuses the options map, so the only
removing path is unreachable. Re-measured here:

| Table | Spark's `removed_delete_files_count` | Delete files after |
|---|---|---|
| v2, 6 data files, 6 position deletes | **0** | all 6 remain |
| v3, 6 data files, 6 deletion vectors | **6** | none remain |

So MW-2's reasoning is exactly right where it was measured and wrong one format version up: a
deletion vector is scoped to one data file and dies with it, making removal an ordinary
consequence of compaction rather than an opt-in. The constant is correct for every version the
procedure still runs on, and the note now sits beside it in `call.rs` so whoever admits v3 sees
it.

## 4. The addressing question, answered

A12: "this engine has no Hadoop-catalog surface, so the fixture needs either a catalog shim or an
adopt-existing-table path — that decision is V3-1's first question."

Adoption, and it is small. `Catalog::register_table(ident, metadata_location)` is on the fork's
trait, implemented for the memory catalog and for **Glue**, and refused cleanly by S3 Tables with
`FeatureUnsupported` — an honest answer, not a gap. Spark reaches the same call through a
procedure, so exposing it is parity rather than invention. Read out of the 1.10.0 jar's bytecode:

| | |
|---|---|
| Name | `system.register_table` |
| Parameters | `table` STRING required, `metadata_file` STRING required |
| Result | 3 columns, **all nullable** BIGINT: `current_snapshot_id`, `total_records_count`, `total_data_files_count` |
| Values | from the current snapshot summary (`total-records`, `total-data-files`); all null when there is no current snapshot |
| Refusals | empty `metadata_file`; a non-Iceberg catalog |

**The caveat, isolated by experiment.** The fixture's pointer is `v3.metadata.json`, the Hadoop
convention. Adoption and reads worked; every write failed with
`Invalid metadata file name format: v3.metadata.json`, because the fork's `MetadataLocation`
parser needs `<version>-<uuid>.metadata.json` to compute the next pointer. Copying the identical
file to a name of that shape made `INSERT` and `expire_snapshots` both succeed — which is how the
cause was separated from anything to do with v3. Catalogs writing version-uuid pointers, Glue
included, are unaffected. Queued as `V3-ADOPT-1`.

## 5. The pins, and why they do not use the Spark fixture

Three pins in `crates/repark-spark/src/tests/call_v3.rs`, none of them needing an oracle. The
fixture is built by upgrading an engine-created table through the fork's own
`Transaction::upgrade_table_version`, so CI can run them.

| Pin | Asserts |
|---|---|
| `v3_fixture_really_is_format_v3` | the table is v2 before the upgrade and v3 after — without this the refusal pin would pass unchanged if the upgrade silently no-opped |
| `call_rewrite_data_files_refuses_a_v3_table_rather_than_reassigning_row_lineage` | the refusal fires, and its message names both row lineage and the format version |
| `call_rewrite_data_files_still_compacts_a_v2_table` | the incidental control — six files still compact to one on v2, still five columns |

The control is the one that matters most. The guard is a format-version comparison, and its
plausible failure is firing one version early and quietly disabling compaction on every table the
engine actually creates.

Watched red before the guard existed: the refusal pin failed, the other two passed. That ordering
is what proves the fixture is sound and the guard is what changed the outcome.

**What is not pinned.** Everything in §2 and §3 measured against the Spark-written fixture. It
needs a live oracle, so it stays a measurement in this ledger until V3-1 lands a fixture CI can
read. That is the same reason `B-MOR-3` is still queued rather than promoted.

## 6. Scope

| | |
|---|---|
| Product change | one guard in `call.rs`, plus the `removed_delete_files_count` note beside it |
| Registry | one row (`V3-LINEAGE-1`), three queued candidates (`V3-DANGLE-1`, `V3-ROWID-1`, `V3-ADOPT-1`), one truth-up (`B-MOR-3`) |
| Docs | the track design, this ledger, the A12 truth-up, the guide's procedure note |
| Forbidden surface | none. No AWS credential or environment, no `Cargo.toml [patch]` change, no `.github/` change, no lockfile edit, no IAM |

## 7. Open, and not blocking

- **V3-LINEAGE-1 is reversible in one line** if the owner would rather match Spark and accept the
  lineage loss. The measurement is here; the decision is the owner's, and it is the same shape as
  OD-2's dry-run default.
- **Nothing was measured on Glue or S3 Tables.** Every number is local filesystem. The claim that
  Glue implements `register_table` is read at the fork pin, not run.
- **MW is still open.** MW-4 waits on OD-3, which the owner executes. V3-1 can run alongside it;
  V3-2 and later should not.

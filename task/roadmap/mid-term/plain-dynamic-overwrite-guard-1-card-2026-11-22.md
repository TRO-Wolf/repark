# Card PLAIN-DYNAMIC-OVERWRITE-GUARD-1: a dynamic partitioned path overwrite deletes the partitions it does not write

**Date:** 2026-11-22. **Filed and built by:** the builder (`claude-opus-5-5`), branch
`fix/plain-dynamic-overwrite-guard-1` off main `550789fd`. **Source:** the Spark audit's item P01-01
(witnesses `setting.residual.sources.partitionOverwriteMode.1.1` and `.0.1`,
`review.settings.overwrite.dynamic`, `.static`, `.append`; eleven paired `astra.*` receipts).
**Severity:** S1, silent data loss in released 1.5.6.

**Status:** **REFUSED on main, fix for v1.5.7** (owner ruling 2026-11-22, D8 and D3 adopted): a loud
refusal closes the data loss now; implementing dynamic partition overwrite for path writes is later work
and the positive answer stays open. It ships in v1.5.7 only on a separate publish approval.

## The defect

With `spark.sql.sources.partitionOverwriteMode=dynamic`, as a session conf or as the writer's
`partitionOverwriteMode` option, a partitioned `mode("overwrite")` write to a plain path replaces the
whole directory. Spark replaces only the partitions present in the written rows and keeps every other
one; an empty input keeps them all. On 1.5.6 an empty input empties the directory.

## Measured (Spark 4.1.2 and a release package of `550789fd`)

200 cells per engine: Parquet, JSON, CSV and text, both spellings (`.parquet(p)` and friends, and
`format(x).save(p)`), into a cold seed directory written by the same engine (rows 10, 20, 30 under
`a=1/b=x`, `a=1/b=y`, `a=2/b=x`) or into an absent target. Each cell records every file's name,
size and SHA-256 under the scratch root before and after, and the rows read back from the files.

| Case (all four formats, both spellings) | Spark 4.1.2 | `550789fd` |
|---|---|---|
| session DYNAMIC, composite input `(1,x,40)`, `(3,z,50)`, ANSI on and off | keeps `a=1/b=y`, `a=2/b=x` | **only 40 and 50 remain** |
| session DYNAMIC, empty input, ANSI on and off | every file byte-identical | **no rows remain** |
| session DYNAMIC, one row; one partition key | replaces one partition | **every other partition deleted** |
| session STATIC, then `conf.set` or SQL `SET` DYNAMIC after the writer is built | dynamic | **every other partition deleted** |
| writer option DYNAMIC over a STATIC session (plain, mixed-case key and value, last of two spellings) | dynamic | Parquet/JSON/CSV: `AnalysisException` "option … is not supported yet", files intact; **text: every other partition deleted** |
| writer option STATIC over a DYNAMIC session | static | Parquet/JSON/CSV: the same `AnalysisException`; text: static, equal |
| writer option `bogus`, `""`, `" dynamic"` | fails (`Py4JJavaError`), files intact, every mode | Parquet: `AnalysisException`; text: proceeds as static |
| static overwrite, composite and empty; DYNAMIC unpartitioned overwrite | as written | equal |
| DYNAMIC append (one row, empty), ignore, error | as written | equal (an empty Parquet append adds a file; rows equal) |
| DYNAMIC overwrite of an absent target, one row and empty | creates it | equal rows (an empty Parquet write leaves a root part file) |

70 of 200 cells lose rows on `550789fd`. The raw logs, the probe and the full table are kept with the
hand-back, outside the repository.

## Fix

**The seat:** `repark_core::partition_overwrite_mode::refuse_dynamic_plain_overwrite` in
`crates/repark-core/src/partition_overwrite_mode.rs` (a `pub mod`; not re-exported at the crate root). It takes the write (path, whether the save
mode is overwrite, whether it is partitioned), one snapshot of the session's
`partitionOverwriteMode`, and the writer's options in request order. On a partitioned overwrite the
last case-insensitive `partitionOverwriteMode` option wins, else the session snapshot. DYNAMIC
refuses.

**The refusal:** `UnsupportedOperationException` (`repark_core::Error::NotImplemented`), the same
class and shape as NATIVE-DATAFRAME-WRITE-SILENT-1: the message names the path, the source of the
DYNAMIC intent, what Spark would do, that RePark refuses rather than overwrite every partition and
that nothing was written, the two working routes (STATIC on the session with no writer option, or an
Iceberg table with `writeTo(...).overwritePartitions()`), and ends with
`(PLAIN-DYNAMIC-OVERWRITE-GUARD-1)`. An option value that is neither STATIC nor DYNAMIC refuses on a
partitioned overwrite with `IllegalArgumentException`, since Spark fails that write too.

**The calls, each before any path lookup, source evaluation, staging or store operation:**

- `DataFrameWriter._apply_path_write` (Parquet, JSON and CSV, both spellings, local and S3), through
  `writer_layout._refuse_dynamic_path_overwrite` and the binding
  `_native.refuse_dynamic_path_overwrite`, which reads the session mode once;
- `writer_text.write_text_path` (text, both spellings, local and the S3 refusal);
- `ReparkSession::write_path` (the S3 engine protocol), before the destination parse and the bucket
  registration.

**Deliberate cost:** every effective DYNAMIC partitioned plain-path overwrite refuses, including the
forms 1.5.6 already answered as Spark does, because the decision is taken before any path lookup and
cannot tell them apart from the destructive ones. Measured on Spark 4.1.2, `550789fd` and `678e4acc`
(Delta 1, 32 cells over the four formats and both spellings): on `550789fd` these give Spark's rows,
and on the head they refuse with the tree intact:

- an absent target (one row or an empty input);
- an existing empty directory (one row or an empty input);
- an input that covers every existing partition, with or without a new partition.

Where main already refused, the exception class changes, the files intact in each case: a DYNAMIC writer option on Parquet, JSON or CSV moves from `AnalysisException`
("option … is not supported yet") to `UnsupportedOperationException`; an unparsable option on a
partitioned Parquet, JSON or CSV overwrite moves from that `AnalysisException` to
`IllegalArgumentException`; S3 text under DYNAMIC moves from the local-only `AnalysisException` to
`UnsupportedOperationException`.

## Not changed

STATIC overwrite, append, ignore, error and unpartitioned writes reach the branches they reached;
a DYNAMIC conf with a non-overwrite save mode still writes; every Iceberg and catalog path, including
`overwritePartitions()` and dynamic `INSERT OVERWRITE`, is untouched.

## Scope cut

This unit proves fail-before-mutation only. The item's two rehearsals (`publication-and-recovery`,
`attested-local-storage`, the 20-ID storage qualification) belong to the later label qualification.

## Open

- **The positive answer:** dynamic partition overwrite for path writes, Spark's rows.
- **A STATIC writer option on Parquet, JSON or CSV** still refuses as an unsupported option, where Spark
  answers; text honors it.
- **Text with an unparsable option** on append or on an unpartitioned overwrite still proceeds, where
  Spark fails.
- **Pre-existing, outside this unit (no fix here; measured by PR #1402's verifier):** `mode("Overwrite")`
  and `mode("OVERWRITE")` refuse with `[INVALID_SAVE_MODE]` where Spark accepts any case; SQL
  `CREATE TABLE … USING parquet … LOCATION p` silently creates an Iceberg table. Neither loses rows.

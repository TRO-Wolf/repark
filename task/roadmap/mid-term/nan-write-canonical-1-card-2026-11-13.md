# Card NAN-WRITE-CANONICAL-1: RePark's Iceberg write stores every NaN as Spark's does

**Date:** 2026-11-13. **Filed by:** Claude Opus 5.5 (`claude-opus-5-5`), the finishing builder. **Source:** the owner's
ruling of 2026-11-13 on Q11 ("Defaults", option C as Fable refined it: canonicalise NaN at RePark's write, before the
label), [MATH-NAN-CANONICAL-1](math-nan-canonical-1-card-2026-11-13.md)'s step 0, the unit's step 0 and its inventory
critic. Evidence: [task/wo/nan-write-canonical-1/](../../wo/nan-write-canonical-1/map.md).

**Severity:** none of its own; it closes NAN-PARTITION-RAW-BITS-1 (S2) and NAN-PARTITION-FILE-SPLIT-1 (S4). **Status:**
FIXED on branch `fix/nan-write-canonical-1` in both repositories (RePark `e100717f` on main `1d9d2808`; fork
`d6592644`, fork PR #384, merged as `8b6ff921` with the same tree). RePark not merged at filing.

## What was wrong

- RePark's arithmetic, about 15 functions and nine aggregates make a NaN with the sign bit set (`fff8…`/`ffc00000`);
  Spark keeps the same bits in memory. Spark's Parquet and ORC writes store the canonical NaN (`7ff8000000000000` /
  `7fc00000`) for every NaN row, nested leaves included; RePark stored the bits each row arrived with.
- Partition values: CTAS, RTAS, `writeTo` `create`/`createOrReplace` and `saveAsTable` overwrite stored each NaN
  pattern as its own partition value; INSERT OVERWRITE, `writeTo` `overwrite`/`overwritePartitions`, MERGE and
  UPDATE stored two or three. RePark's `= CAST('NaN' …)` then missed rows
  ([NAN-PARTITION-RAW-BITS-1](nan-partition-raw-bits-1-card-2026-11-13.md)). Every partitioned door wrote the NaN
  partition as 3–4 files where Spark writes one
  ([NAN-PARTITION-FILE-SPLIT-1](nan-partition-file-split-1-card-2026-11-13.md)).

## The rule per format (Spark 4.1.2 / Iceberg 1.11.0, measured)

| | Parquet | ORC | Avro |
|---|---|---|---|
| data values, nested leaves included | canonical | canonical | raw bits kept |
| partition values | canonical, one file | canonical, one file | canonical, one file |
| `rewrite_data_files` of an old unpartitioned file | repairs | repairs | keeps raw |

RePark's head does the same on every cell measured. A rewrite of an old Avro table partitioned by the float column was
measured after filing and is unlike Spark: [NAN-AVRO-REWRITE-PARTITION-1](nan-avro-rewrite-partition-1-card-2026-11-13.md)
(S3, pre-existing).

## The inventory and the seat

Every write door, with file:line, is in the step 0
[inventory](../../wo/nan-write-canonical-1/evidence/step0/inventory.md): `INSERT`, `INSERT` with options, `BY NAME`,
`INSERT OVERWRITE`, `REPLACE WHERE`, CTAS and RTAS, MERGE (copy-on-write, merge-on-read, v3 lineage), UPDATE and
DELETE, every `writeTo` form, `saveAsTable`, `save(<name>)`, `insertInto`, the native INSERT, UPDATE and DELETE, the
Rust `append()`, the micro-batch sink and `rewrite_data_files` (with `run_maintenance` and `apply_partitioning`).
Each data file goes through the fork's `AnyFileWriter`, and each partition value through its
`PartitionValueCalculator`. `add_files`, `rewrite_manifests` and `rewrite_table_path` copy files and write none.

- **Data values:** fork `writer/file_writer/any_writer.rs` `AnyFileWriter::write`, on the Parquet and ORC arms only.
- **Partition values:** fork `arrow/partition_value_calculator.rs` `calculate_from_columns`, on all three formats. It
  holds the one rewrite function, `canonical_nan_batch` / `canonical_nan_array`.
- **Plan-path routing:** RePark `crates/repark-iceberg/src/write/distribution.rs` `partition_value_exprs`, each float
  partition expression wrapped in the existing `CanonicalFloatExpr::wrap`, so CTAS-shaped doors write one NaN file.
- Only NaN bits change: `-0.0`, `±inf`, NULL and every other value keep their bits (measured on every door).

## Outside the seat

- **Float map keys keep their bits.** Spark stores one NaN map key canonical in Parquet and ORC, but refuses a map
  with two NaN keys (`DUPLICATED_MAP_KEY`), which RePark writes. Rewriting keys would give such a map two identical
  keys, so a single sign-set or payload NaN key in a Parquet or ORC map stays unlike Spark. On Avro, Spark stores a
  computed sign-set NaN key canonical and keeps a payload key raw; RePark keeps both raw (verifier 2).
- **The plain Parquet path write** (`df.write.parquet(<dir>)`, DataFusion's writer, not Iceberg) still stores raw bits.
- **In-query answers** over a sign-set NaN computed in the same query (`max`, `min`, `greatest`, `least`, comparisons)
  still differ: that is option (B), the first unit after the label.
- **Filters over stored Avro NaNs.** Avro data keeps the raw NaN bits, as Spark's does, and RePark compares by bits.
  On a non-partition column of an Avro table RePark has just written, `d = CAST('NaN' AS DOUBLE)` finds 1 of 6 or 7
  NaN rows and `d > CAST('Infinity' AS DOUBLE)` finds 3 of 6, where Spark finds all of them (verifiers 1 and 2). Main's
  build answers the same. It belongs to [NAN-COMPARE-SPARK-1](nan-compare-spark-1-card-2026-11-13.md), and the pin's
  strict xfail on the Avro `f` column holds it.

## Old tables

A table written by RePark before this fix keeps its bits. A RePark rewrite with a fixed build (`CALL …
rewrite_data_files`, or any copy-on-write rewrite of the file) repairs Parquet and ORC data files and the NaN
partition value. Avro data files of an unpartitioned table stay raw, as Spark's do; a rewrite of an Avro table
partitioned by the float column is NAN-AVRO-REWRITE-PARTITION-1. Data written by another engine is read as written.

## Measured

- **Head against Spark** (step 0 re-run on the head, `table.md` §5 and `formats.md` §4): every door equal, one
  canonical NaN partition value in one file on every partitioned door; NAN-PARTITION-RAW-BITS-1 and
  NAN-PARTITION-FILE-SPLIT-1 close on every door measured.
- **Pins:** `python/repark/tests/iceberg/test_nan_write_canonical_1.py` (10 RePark cells, 10 live cells against
  Spark's own table), red 20/20 on main's build and green 20/20 on the head; the fork's
  `partition_value_calculator.rs` tests over every NaN pattern, the controls and all three formats.
  - The `avro-insert` cell seeds row 8's `d` as 2.5, so no partition holds both zeros: on one or two cores
    ZERO-PARTITION-MERGE-1 puts `0.0` in the `-0.0` partition. Its `f` column keeps both zero controls.
  - A strict xfail, `test_repark_filters_stored_avro_nan_as_spark_does`, asserts Spark's answer (rows 1–6, measured)
    for `f = CAST('NaN' AS FLOAT)` and `f > CAST('Infinity' AS FLOAT)` on the Avro table; the head answers `[4]` and
    `[4, 5]`. NAN-COMPARE-SPARK-1 turns it green.
- **Cost** ([cost/](../../wo/nan-write-canonical-1/evidence/cost/map.md)): 10 M DOUBLE rows, plain and partitioned, no NaN
  and 1 % NaN, six alternated pairs: head/base median 1.005x to 1.048x per case, 1.032x over the sum, under the
  1.05x bar; base/base noise 0.90x to 1.08x.

## Close

Closed by this unit when it merges.

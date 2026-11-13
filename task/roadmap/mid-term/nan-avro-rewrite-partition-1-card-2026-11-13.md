# Card NAN-AVRO-REWRITE-PARTITION-1: a rewrite of an old float-partitioned Avro table writes each row from the old partition value

**Date:** 2026-11-13. **Filed by:** Claude Opus 5.5 (`claude-opus-5-5`), the NAN-WRITE-CANONICAL-1 delta builder.
**Source:** NAN-WRITE-CANONICAL-1 (PR #1325, fork PR #384) verifier 2, 2026-11-13 (finding F1).

**Status:** filed 2026-11-13, not dispatched. Severity **S3**: NaN stays NaN, and an identity read fills the column
from the canonical partition value, but the stored data bits are unlike Spark's. Pre-existing: main's build
`1d9d2808` does the same.

## The cell

- **Table:** Avro, format v2, `PARTITIONED BY (d)` over a DOUBLE `d`, written by a CTAS on main's build `1d9d2808`, so
  its NaN partition values are raw (`7ff8deadbeef0001` and `fff8deadbeef0001`).
- **Statement:** `CALL <catalog>.system.rewrite_data_files(table => '<ns>.<table>', options => map('rewrite-all',
  'true'))`, run by NAN-WRITE-CANONICAL-1's head `d27184eb` (through `register_table`), by main's build, and by Spark
  4.1.2 with Iceberg 1.11.0, each on its own copy.

## Measured

Avro data bits of `d`, rows 1–6, read from the files:

| state | rows 1–6 |
|---|---|
| before, written by main's build | `7ff8000000000000`, `fff8000000000000`, `fff8000000000000`, `7ff8deadbeef0001`, `fff8deadbeef0001`, `7ff0000000000001` |
| head, after the rewrite | `7ff8deadbeef0001` ×4, then `fff8deadbeef0001` ×2 |
| main's build, after the rewrite | the same as the head |
| Spark, after the rewrite | `7ff8000000000000` ×6 |

- The head's rewrite stores one canonical NaN partition value, `7ff8000000000000`, but writes each row's `d` from the
  old partition constant of its source file. Row 1 goes from canonical to a payload NaN, and row 6 from
  `7ff0000000000001` to `fff8deadbeef0001`. The file's data then disagrees with its own partition value.
- On Parquet and ORC the same rewrite stores the canonical NaN, equal to Spark. Unpartitioned and INSERT-written Avro
  tables rewrite equal to Spark.
- **Reach:** any Avro table whose NaN partition values are not canonical: every table RePark wrote before
  NAN-WRITE-CANONICAL-1 through a door that stored raw NaN partition values. On main those are CTAS, RTAS, `writeTo`
  `create`/`createOrReplace` and `saveAsTable` overwrite, and also INSERT OVERWRITE, `writeTo`
  `overwrite`/`overwritePartitions`, MERGE and UPDATE (step 0 `table.md`). The delta re-check measured the MERGE and
  INSERT OVERWRITE tables rewriting the same way (#1325 delta re-check, D1, 2026-11-13).

## Probe

Verifier 2's `write_probe.py` (session scratch, not in the tree): leg `rwsrc2` writes `rwc_avro_p` with the base, and
leg `rw2` runs the rewrite on each engine. Runs `rw2-head.jsonl`, `rw2-base.jsonl` and `rw2-spark.jsonl` are compared
in `cmp-rw2.json`. Avro bits come from a patched fastavro decoder that returns raw IEEE bytes.

## Fix direction

From verifier 2, not designed: on rewrite, write the source row's data value, or canonicalise the constant-filled
identity column.

## Close

A RePark `rewrite_data_files` of an old float-partitioned Avro table stores Spark's bits in every row, pinned against
the Spark oracle.

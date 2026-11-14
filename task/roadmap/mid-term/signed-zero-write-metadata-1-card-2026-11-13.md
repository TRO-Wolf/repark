# Card SIGNED-ZERO-WRITE-METADATA-1: file bounds, `.partitions` and map keys treat `0.0` and `-0.0` unlike Spark

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Sources:** NAN-WRITE-CANONICAL-1 verifier
1, 2026-11-13 (#1325, finding C3, class (c)); NAN-WRITE-CANONICAL-1 verifier 2, 2026-11-13 (#1325, finding C2).

**Status:** filed 2026-11-13, not dispatched; the `.partitions` half **FIXED 2026-11-13 by
[ZERO-PARTITION-MERGE-1](zero-partition-merge-1-card-2026-11-13.md)** (fork `bc5a9211`). Severity **S3** (the
clerk's rating, unruled): the map key is a stored value unlike Spark's and `.partitions` was a wrong metadata read;
the bounds are wider than Java's, not wrong. Pre-existing: base and head of #1325 are identical. No card on main
covered these; the search found only NATIVE-LISTING-FLOAT-PARTITION-1 and SCALAR-FLOAT-KEY-SIGNED-ZERO-NAN-1 (read
side).

## Measured

- **Bounds.** RePark writes `lower_bounds` / `upper_bounds` of `[-0.0, +0.0]` for a file whose only value is one zero
  (`0.0` or `-0.0`); Java writes the exact value (`[0.0, 0.0]` for a `+0.0`-only Parquet file). Verifier 2 traces all
  67 per-partition bound differences it saw to `±0.0`. Every file whose rows are NaN or `±inf` has Spark's bounds.
- **`.partitions`, FIXED by ZERO-PARTITION-MERGE-1.** RePark's `.partitions` grouped the `0.0` and `-0.0`
  partitions into one row. This half overlaps ICEBERG-PARTITIONS-TABLE-FLOAT-1 (S3, filed on #1326's branch, not on
  main; named in [NAN-PARTITION-VALUE-1](nan-partition-value-1-card-2026-11-12.md)); when that card reaches main, it
  carries this bullet. After the fix (head `1bf566ee`), ZERO-PARTITION-MERGE-1's verifier 2 measured two zero rows,
  as Spark lists, on all 62 door tables of its probe; verifier 1 traced the site, fork `inspect/partitions.rs:226`,
  now keyed by a `Struct` that keeps the zeros apart.
- **Map keys.** A `-0.0` map key is stored `-0.0`, where Spark stores `0.0`, on Parquet, ORC and Avro.

## Related

- [ZERO-PARTITION-MERGE-1](zero-partition-merge-1-card-2026-11-13.md): the S1 write of both zeros into one partition.
- [SCALAR-FLOAT-KEY-SIGNED-ZERO-NAN-1](scalar-float-key-signed-zero-nan-1-card-2026-11-02.md): the map-key lookup.
- SIGNED-ZERO-NORMALIZE-1's `FORK-FLOAT-ZERO-BOUNDS-1` ([sketch](../../wo/signed-zero-normalize-1/sketch.md) §5):
  the pruning side of the bounds, carded there as an optional fork PR.

## Close

Each item writes what Spark's table holds (exact single-zero bounds, one `.partitions` row per zero partition, now
done, a `0.0` map key), or the owner rules it a kept divergence; pinned against Spark's own table.

# Card ICEBERG-NAN-FILE-PRUNE-1: RePark's Iceberg scan prunes a NaN row's file for `x > 0.5`, where Spark-on-Iceberg keeps the row

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** SIGNED-ZERO-NORMALIZE-1 part B
part 1 verifier 1, 2026-11-13: finding F5, cell GD-6, and that cell's raw Spark legs.

**Status:** filed 2026-11-13, not dispatched. The source gives no severity (class (c), pre-existing). Filed **S2**,
provisional: a silent wrong read where Spark-on-Iceberg returns the row. If Spark-on-Iceberg drops the same row on
RePark's file layout, the cell belongs to the declared PRUNED-WRONG class instead (see below).

## Measured

- **Table:** `t`, 12 rows in 4 data files, one `INSERT` per file. The NaN row (id 5, `x = NaN`) shares its file with
  `x = -0.0` and `x = NULL`.
- **Cell GD-6:** `SELECT id FROM <c>.t WHERE x >= y AND x > 0.5`.
- **RePark, Spark door and native:** `[3, 7, 10]`. The scan line is `IcebergTableScan … predicate:[x > 0.5] N=3`: three
  of the four files are scanned, and the NaN row is missing.
- **Spark 4.1.2 SQL over a `VALUES` view of the same rows:** `[3, 5, 7, 10]`.
- **Spark 4.1.2 on its own Iceberg table** (Iceberg 1.11.0, `type=hadoop`, the same rows and inserts, `local[2]`):
  `[3, 5, 7, 10]`. So on its own table Spark-on-Iceberg keeps the row.

**Builds:** part B's head `b84adbc7` (product code `66fb7f13`) and its base, main `276dc503`, identical, on both doors.

## Open points

- Each engine read the table it wrote. Whether the difference lies in RePark's scan pruning or in the two writers'
  file layout and statistics is not separated: Spark's `local[2]` writer may lay the rows into different files. A
  cross-read, each engine on the other's table, separates them.
- [NAN-UNDER-ICEBERG-SCAN-FILTERS-1](nan-under-iceberg-scan-filters-1-card-2026-11-05.md) (AUD-024) records Spark's
  pushed filter `d > 0` excluding a NaN row. This cell records Spark keeping one. The two are not reconciled.
- Cells GD-10 and GD-11 in the same probe are PRUNED-WRONG: Spark-on-Iceberg itself drops rows there.

## Fix direction

None given by the source.

## Close

The cross-read is measured. Then either RePark keeps the NaN row wherever Spark-on-Iceberg does, pinned, or the cell
is recorded in the PRUNED-WRONG class with the measurement behind it.

# Card ICE-FAILED-WRITE-ORPHAN-FILES-1: a failed Iceberg INSERT leaves uncommitted data files

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), from the SUM-DEC-I128WRAP-1 step 0 and its revision (PR #1357, head `2f63e2c7`), under the owner's ruling "defaults on both" (about 23:05, later the same evening; [rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md), later section).

**Status:** filed 2026-11-17, not dispatched.

**Severity: S3, not label-blocking** (owner, 2026-11-17, "defaults on both", question 5(a)).

## Finding

When an Iceberg `INSERT` fails on a decimal precision check, the statement fails and the table reads no new rows, but the data files it wrote stay under the table's `data/` directory. The table is right: no committed file holds the 39-digit value. The orphan files hold it.

## Cells

Sum step 0, `ice/insert/all/c38_0` and `ice/insert/all/c38_38`, both modes: the statement fails with `DataInvalid … too large for precision 38` (the fork checks each file's min and max after writing it), the table reads 0 rows, and the orphan files hold `10^38`, `±1.40·10^38` and the 38-digit wrap. The revision's refused `ice/insert/metrics_full/*` (6) and `ice/insert/wide100/c38_0` (2) are the same case.

## Relation

ICE-DECIMAL-WRITE-PRECISION-1 (a per-value check would refuse before the file is written; not ruled). SUM-DEC-I128WRAP-1.

## Source

SUM-DEC-I128WRAP-1 step 0 hand-back, "Wrong writes, named", item 2; step 0 sketch §6 question 5(a); step 0 revision hand-back, "Wrong writes, restated".

Up: [map.md](map.md).

# Card SIGNED-ZERO-PARTITION-FILTER-1: a filter on an identity-partitioned float zero answers by partition on Spark-on-Iceberg

**Date:** 2026-11-14. **Filed by:** ZERO-PARTITION-MERGE-1's delta round (`claude-opus-5-5`). **Sources:**
ZERO-PARTITION-MERGE-1 verifier 2, 2026-11-13 (#1332, finding C1, class (c), probe `filters.py`); verifier 1
(#1332, findings C1 and C6); owner, 2026-11-13, about 23:59 UTC, verbatim "Go with defaults", adopting "a new card
under the NAN-COMPARE pruning unit".

**Status:** filed 2026-11-14, unrated, not dispatched. It belongs to
[NAN-COMPARE-SPARK-1](nan-compare-spark-1-card-2026-11-13.md)'s pruning unit, under the owner's Q5 ruling (E) of
2026-11-13, "prune only where Spark's own reader prunes" ([sketch](../../wo/nan-compare-spark-1/sketch.md) §6 and
§8). Pre-existing: head `1bf566ee` and base `1d9d2808` of #1332 answer alike.

## Measured

- **The shape.** A table identity-partitioned on a FLOAT or DOUBLE column holds a `-0.0` partition and a `0.0`
  partition. Spark 4.1.2 with Iceberg 1.11.0 answers a filter on that column by partition, using Java's order
  (`-0.0 < 0.0`), on its own table and on RePark's alike. RePark answers by IEEE comparison (`-0.0 = 0.0`).
- **Verifier 2's table.** Rows 1 and 6 are `-0.0`, 2 and 5 are `0.0`, 3 is `1.5` and 4 is NULL. DOUBLE is shown;
  FLOAT answers the same.

  | Predicate | Spark 4.1.2 / Iceberg 1.11.0, own table and RePark's | RePark, head and base |
  |---|---|---|
  | `d = CAST('-0.0' AS DOUBLE)` | [1, 6] | [1, 2, 5, 6] |
  | `d = 0.0` | [2, 5] | [1, 2, 5, 6] |
  | `d < 0.0` | [1, 6] | [] |
  | `d >= 0.0` | [2, 3, 5] | [1, 2, 3, 5, 6] |
  | `d != 0.0` | [1, 3, 6] | [3] |
  | `NOT (d = CAST('-0.0' AS DOUBLE))` | [2, 3, 5] | [3] |
  | `d <=> CAST('-0.0' AS DOUBLE)` | [1, 6] | [1, 2, 5, 6] |

- **Verifier 1** saw the same on Spark's own tables: `d = 0.0` keeps only the `0.0` rows and `d = -0.0` only the
  `-0.0` rows, where RePark keeps both zeros for each.
- **The fix did not cause it, and the old merge hid it.** Head and base answer alike. Before
  ZERO-PARTITION-MERGE-1, RePark wrote both zeros into one partition, so Spark reading RePark's tables could not
  split them. Spark's own tables already showed the gap.

## Class

- **PRUNED-WRONG:** Spark's own reader drops rows its row semantics keep. `d = CAST('-0.0' AS DOUBLE)` loses the
  `0.0` rows, and `d >= 0.0` loses the `-0.0` rows.
- On `d < 0.0`, `d != 0.0` and the `NOT` form it also returns `-0.0` or `0.0` rows that IEEE comparison drops.
- Inferred, not traced: an identity-partition filter leaves no residual, so the partition value alone answers it,
  compared in Java's order.

## Related

- **Verifier 1's class (c), C6:** the fork's `Datum` derives `PartialEq` and `Hash` through `OrderedFloat`
  (`-0.0 == 0.0`), while its `PartialOrd` orders `-0.0 < 0.0`. Its `Eq` and `Ord` disagree on the two zeros.
  ZERO-PARTITION-MERGE-1 left it unchanged. NAN-COMPARE-SPARK-1's Q7 (the fork's `Datum` float order against Java's
  `Double.compare`) may be related; no cause is claimed.
- [ZERO-PARTITION-MERGE-1](zero-partition-merge-1-card-2026-11-13.md): its fix split the zero partitions, which made
  the gap visible on RePark's tables.
- SIGNED-ZERO-NORMALIZE-1's PRUNED-WRONG class (owner ruling G, 2026-11-08;
  [sketch](../../wo/signed-zero-normalize-1/sketch.md)) and
  [ICEBERG-NAN-FILE-PRUNE-1](iceberg-nan-file-prune-1-card-2026-11-13.md): the same class on zero-only files and on
  NaN.

## Close

The pruning unit states RePark's answer for each predicate shape over a float zero partition under reading (E),
either Spark-on-Iceberg's partition answer or a declared PRUNED-WRONG row. Each is pinned against Spark's own table
on DOUBLE and FLOAT.

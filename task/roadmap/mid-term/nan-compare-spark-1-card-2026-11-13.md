# Card NAN-COMPARE-SPARK-1: compare NaN as Spark does, one value above all others

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** the owner's ruling of
2026-11-13, verbatim "Defaults" ([rulings](../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-13.md), item 3),
adopting option (B) of [MATH-NAN-CANONICAL-1](math-nan-canonical-1-card-2026-11-13.md) as the first unit after the
label; Fable guidance, 2026-11-13.

**Status:** filed 2026-11-13, **not dispatched**. It runs **first after the scoped label** ICE-V2-FLAT-AWS-LABEL-1.
Before the label, NAN-WRITE-CANONICAL-1 (option (C), NaN only, at RePark's write) runs instead.

## The gap

Spark treats every NaN as one value, larger than every other value, whatever its sign bit or payload:

- `min` and `max`, and the Parquet-statistics shortcut that answers them;
- `greatest` and `least`;
- sort (`ORDER BY` and windows);
- `=`.

RePark compares by bits (Arrow's total order), so a sign-set NaN (`fff8…`) is the smallest value and unequal to
`CAST('NaN' AS DOUBLE)`. MATH-NAN-CANONICAL-1's step 0 measured both sides on main `168dd934` against Spark 4.1.2.

## Scope

- Each of the four seats above, with its own cost row.
- Group, join and distinct keys are covered by the key function of SIGNED-ZERO-NORMALIZE-1 part B (not merged at
  filing), per MATH-NAN-CANONICAL-1's option (B).
- Data written by another engine, or by RePark before NAN-WRITE-CANONICAL-1, keeps its bits; this unit makes reads
  of it agree with Spark.

## Relation

- After this unit, the label exclusion the owner accepted on 2026-11-13 (`max`, `min`, `greatest`, `least` and
  non-key comparisons over a NaN computed in the same query) can be retired; its text is still pending its
  measurement on the head with part B and NAN-WRITE-CANONICAL-1.
- [PARQUET-NAN-EQUALITY-1](parquet-nan-equality-1-card-2026-11-13.md) and
  [ICEBERG-NAN-FILE-PRUNE-1](iceberg-nan-file-prune-1-card-2026-11-13.md) may be related; no cause is claimed.

## Close

Every seat answers Spark's NaN comparison on sign-set and canonical NaN inputs, on every door, pinned against the
Spark oracle, and the label exclusion's text is removed or narrowed.

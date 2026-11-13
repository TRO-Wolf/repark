# Card NAN-PAYLOAD-1: a NaN with the sign bit set orders and compares unlike Spark's NaN

**Date:** 2026-11-12. **Filed by:** the records clerk (`claude-opus-5-5`). The name was carded on 2026-11-08 by the
SIGNED-ZERO-NORMALIZE-1 [sketch](../../wo/signed-zero-normalize-1/sketch.md) (§10, "Outside the clause, carded": raw
`-NaN` ordering outside key positions), which kept no card file. This is its card file, not a second card.
**Source:** SIGNED-ZERO-NORMALIZE-1 part B evidence (`task/wo/signed-zero-normalize-1/evidence/part-b/`, filed with that
unit): step 0 table §6 on main `ae247767` (Q9), and item 1's addendum on main `276dc503` (groups `nan-sign` and
`log`); Fable's review of 2026-11-12, §3 and §4.2.

**Status:** filed 2026-11-12, not dispatched. Fable rates it wider than the sketch carded it. No ruling has given it
a severity; the measured `max`/`min` and `greatest`/`least` answers are silent wrong values on both doors.

**Q9 evidence (owner ruling Q9 of 2026-11-12: no stored-NaN change in part B part 1; add the bits here):** Spark's
Iceberg write stores sign-set NaNs as the canonical `7ff8…` (Spark's read of its own one-file table shows only
`7ff8…`, while its inline view still holds `fff8…`). RePark's Iceberg write keeps `fff8…`, on the Spark door and on
native. After part 1 the keys agree either way, because its UDF canonicalises them.

**Measured on main `276dc503` against Spark 4.1.2:**

- `sqrt(-1.0)`, `acosh(0.5)`, `atanh(2.0)` and `pow(-8.0, 0.5)` (DOUBLE arguments) return `fff8…` on both doors; Spark
  returns `7ff8…` (cells NS-bits-sqrt, NS-bits-acosh, NS-bits-atanh, LG-pow). `ln(-1.0)` and `log2(-1.0)` also return
  `fff8…`; [LOG-NONPOSITIVE-NULL-1](log-nonpositive-null-1-card-2026-11-12.md) makes those NULL.
- Over a column holding `1.5`, `sqrt(-1.0)` and `9.5`: `max`/`min` answer `9.5, NaN` where Spark answers `NaN, 1.5`
  (NS-max-min); `greatest`/`least` against ±100 answer `100.0, NaN` where Spark answers `NaN, -100.0`
  (NS-greatest-least); `ORDER BY` puts the NaN row first where Spark puts it last (NS-order-by, ORDER-DIFFER).
- The canonical-NaN controls NC-max-min, NC-greatest-least, NC-order-by and NC-gt-literal are EQUAL, so the cause is
  the sign bit alone. NS-gt-literal returns no row on both engines and does not witness the sign.

**Follow-up unit (owner-ruled 2026-11-12):** a small unit that canonicalises NaN at the math functions' outputs
(`sqrt`, `acosh`, `atanh`, `pow`), queued for the second builder. Not yet named or dispatched.

**Fix direction:** that unit writes `7ff8…` wherever these functions produce NaN, on both doors, and pins each NS-*
cell above with its bits. Raw `-NaN` reaching RePark from stored data or `createDataFrame` stays outside it; Fable's
option for that remainder is to pass `min`/`max` arguments through part B's normalising function, at the cost of
the Parquet statistics shortcut, so it needs its own cost row.

**Close:** the follow-up unit merges with those pins, and the remainder is fixed or declined by a recorded ruling.

**Follow-up unit halted at step 0 (2026-11-13):** see [MATH-NAN-CANONICAL-1](math-nan-canonical-1-card-2026-11-13.md).
Its step 0 measured Spark keeping `fff8…` in memory over a column for these producers; the `7ff8…` answers above
are the folded literal form. The difference is Spark's NaN comparison semantics plus its canonicalising write. The
seat awaits the owner's choice of (A), (B) or (C).

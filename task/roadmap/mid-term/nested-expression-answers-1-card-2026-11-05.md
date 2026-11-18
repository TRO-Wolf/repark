# Card NESTED-EXPRESSION-ANSWERS-1: ten nested expression answers where Spark raises, as expression-semantics limits outside the format claim

**Date:** 2026-11-05. **Filed by:** the records clerk (`claude-haiku-5-5`), from the owner's ruling of 2026-11-05 on the format-v2 label re-audit ([report](../../../docs/audits/iceberg-v2-label-corpus-audit-2026-11-05.md), owner ruling 1 in section 3; ledger C-106).

**Status:** filed as a card under the owner's ruling. Severity S3 for the card as a class (D10 is S2 in the nested rule, D13 has three S2 and three S3 cells). Grade not set; not dispatched.

**Why:** Family 3 of the label excludes expression cells, and the owner ruled on 2026-11-05 that the ten expression answers below are cards as expression-semantics limits outside the format-v2 claim. The cells are defined in the [nested rule](../../../docs/corpus/nested-rule-2026-11-03.md):
- **D10, 4 cells:** an out-of-range array subscript or `element_at` answers null where Spark raises `INVALID_ARRAY_INDEX` and `INVALID_ARRAY_INDEX_IN_ELEMENT_AT` under ANSI.
- **D13, 6 cells:** `struct(1, 'x')` names its fields `c0` and `c1` where Spark names them `col1` and `col2`; a case-insensitive field lookup refuses; a duplicated field answers where Spark raises `AMBIGUOUS_REFERENCE_TO_FIELDS`; `m[NULL]` raises an Arrow error.

The ruling covers SELECT answers only. Where the same expressions reach a write, the findings are blockers and sit on [ANSI-SUBSCRIPT-AND-AMBIGUOUS-FIELD-1](ansi-subscript-and-ambiguous-field-1-card-2026-11-05.md) (AUD-022 and AUD-023).

**Close:** when the expression answers match Spark's refusal in a pinned SELECT, or when the owner keeps the card open as a stated limit.

## Append (2026-11-17): element_at past the end, Fable's cell

Fable's cell (Fable (`claude-fable-5-2`), consult reply of 2026-11-17, section 4, seen in passing): `element_at(arr, 2)` on a one-element array answers NULL with ANSI on, where Spark raises `INVALID_ARRAY_INDEX_IN_ELEMENT_AT`. It is the `element_at` cell of D10 above, measured on main before #1341. The subscript card [ANSI-SUBSCRIPT-AND-AMBIGUOUS-FIELD-1](ansi-subscript-and-ambiguous-field-1-card-2026-11-05.md) covers the write routes and defers SELECT answers to this card. Ruled 2026-11-17 (side check 5 of the [rulings](../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md)): a dated line here, and no new card. The severity is not restated here.

## Append (2026-11-17, later): the view step 0's element_at and array[5] cells

The view step 0 (TEMP-VIEW-SESSION-SETTINGS-1, PR #1356) measured `element_at(array, 5)` and `array[5]` with ANSI on: both answer NULL on RePark where Spark raises. These are D10's own shapes on direct queries (4 cells, the step 0's domain table §6), and a view carries the same answer. Ruled "defaults on both" (owner, 2026-11-17, question 6 of the step 0): they stay under D10, with no new card and no re-rating. The severity is not restated here ([rulings](../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md), later section).

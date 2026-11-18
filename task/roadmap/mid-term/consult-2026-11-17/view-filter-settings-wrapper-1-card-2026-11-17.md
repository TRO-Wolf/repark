# Card VIEW-FILTER-SETTINGS-WRAPPER-1: the wrapper follow-up to the view filter rule, which would close 4 of its 7 loud cells

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), from the TEMP-VIEW-SESSION-SETTINGS-1 step 0 (PR #1356, head `a2e34b9e`), under the owner's ruling "defaults on both" (about 23:05, later the same evening; [rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md), later section).

**Status:** filed 2026-11-17, not dispatched.

**Severity: not rated.** Question 11 ruled the revised filter rule, and the wrapper is carded as its follow-up.

## Finding

The ruled filter rule on the differing path pushes settings-free conjuncts into the view body and keeps settings-sensitive conjuncts and the limit above, under the reader's state. Its residual is 7 loud cells among the 36 differing-path view cells, and none silent. 6 of the 7 agree with Spark today and would start raising. Under the ruling those 6 are an accepted class.

The follow-up is the alternative: a delegating `ScalarUDFImpl` around each settings-reading call of the reader's filter. It replaces the call's `config_options` with the reader's options, so every conjunct and the limit can enter the body. It would close 4 of the 7 cells. It would not close the 2 AND-GUARD-ANSI-1 cells or the 1 cell where Spark folds `(a+1)+0`.

## Cells

The round 2 re-critic's 44 filter cells: 36 differing-path view cells, 6 direct gaps and 2 matching `on_on` controls. Of the 7 loud cells, 4 come from a setting-sensitive conjunct kept above a raising body (two `OR` cells and two aggregate-key cells), 2 from AND-GUARD-ANSI-1 (the `AND` cells on the SQL and DataFrame doors), and 1 from Spark's optimizer folding `(a+1)+0` on the DataFrame door. The revision's own count, 2 `filter_sensitive` cells, stays beside it, labelled as the revision's.

## Relation

TEMP-VIEW-SESSION-SETTINGS-1 (owner question 11). AND-GUARD-ANSI-1 (the two `AND` cells are recorded there).

## Source

TEMP-VIEW-SESSION-SETTINGS-1 step 0 revision hand-back, item 1, and sketch §7, question 11; the round 2 re-critic's verdict (GAPS, `a2e34b9e`), its filter table, "Tally" and "The wrapper alternative".

Up: [map.md](map.md).

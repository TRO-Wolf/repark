# Card LISTVIEW-INNER-JOIN-KEY-1: a DataFrame key-name inner join on a ListView key returns the cross product

**Date:** 2026-11-11. **Filed by:** the records clerk (`claude-haiku-5-5`), from the evidence of STRUCT-COMPARE-SEAT-FAMILY-1 (PR #1295, `task/wo/struct-compare-seat-family-1/evidence/after/listview.txt` and `step0/raw/spark.jsonl`).

**Status:** filed 2026-11-11, not dispatched by this records change. Severity **S1**. The owner ruled the fix on 2026-11-11, verbatim: "Yes merge 1295. Yes, let's get LISTVIEW-INNER-JOIN-KEY-1 fixed. Yes let's do it". It blocks v1.5.7.

**Pre-existing or introduced:** pre-existing. Main `68f70b0f` prints the same result as head. It is independent of the struct pairing that STRUCT-COMPARE-SEAT-FAMILY-1 changed.

**Repro:** a DataFrame inner join on a key name, where the key column is a `ListView` or `LargeListView` of `Int64`.

**Cells and measured answers** (Spark 4.1.2 against head and base):

| Cell | Spark | Head and base |
|---|---|---|
| Q2059 | `[[1, 10]]` | `[[1, 10], [1, 30], [2, 10], [2, 30]]` (cross product) |
| Q2063 | `[[2, 10]]` | `[[1, 10], [1, 30], [2, 10], [2, 30]]` (cross product) |

The `ON` join form answers `[[2, 10]]` on both views, the same as Spark. A `List` key is correct on every form (`[[2, 10]]`). The SQL `USING` form on a `ListView` or `LargeListView` key refuses with `UNRESOLVED_COLUMN.WITH_SUGGESTION`; Spark's answer for that form is not recorded in the evidence.

**Close:** the DataFrame key-name join on a `ListView` and `LargeListView` key answers as Spark does (Q2059 and Q2063), with a pin in the family pins, and the `ON` and `List` forms stay equal.

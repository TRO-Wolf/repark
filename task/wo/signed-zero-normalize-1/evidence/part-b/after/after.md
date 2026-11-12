# SIGNED-ZERO-NORMALIZE-1 part B part 1: the after-table

Model `claude-opus-5-5`, 2026-11-12. The head is `4f2108ae` (seat 2, the UDF and the door policy), on the frozen base
main `276dc503`. Every RePark leg of step 0 and item 1 was re-run on the head's release build inside the build
wrapper; the Spark legs are step 0's and item 1's, unchanged. Labels are part A's `label_of`. The base column is item 1's
re-measure on `276dc503` ([../item1/remeasure/](../item1/remeasure/map.md)).

**Result: no halt.** Every cell that moved, moved to Spark's measured outcome, or to the IEEE truth that Q1 rules for
`NOT IN`. No control moved otherwise. The floatkeys cells are all EQUAL.

## 1. Moves by label (step 0's clause cells, 1182 cell rows)

From [moves-step0.md](moves-step0.md):

| Base `276dc503` | Head `4f2108ae` | Cells | Disposition |
|---|---|---|---|
| WRONG-READ | EQUAL | 480 | clause, to the truth |
| WRONG-WRITE | EQUAL | 82 | clause, to the truth |
| SIGN-ONLY+WRONG-READ | EQUAL | 28 | clause, to the truth |
| ORDER-DIFFER | EQUAL | 18 | OB-1, OB-3, NN-4 and the tie-broken orders, to the truth |
| ACCEPTED-WHERE-SPARK-REFUSED | BOTH-REFUSED | 6 | WR-merge-card (m1), v2-cow, v2-mor and v3, DOUBLE and FLOAT: RePark now raises `MERGE_CARDINALITY_VIOLATION`, as Spark does (Q7) |
| EQUAL | WRONG-READ | 8 | SQ-notin `un`/`up`, both doors, both types: Spark's default null-aware plan keeps `-0.0` apart; Q1 rules IEEE the truth, so these are declared `NAAJ-WRONG`. Their IEEE twins SQ-notin-ieee moved WRONG-READ → EQUAL |
| EQUAL | WRONG-WRITE | 8 | WR-insert-notin `un`, both doors, three kinds: the same Q1 declaration; WR-insert-notin-ieee moved WRONG-WRITE → EQUAL |
| EQUAL | EQUAL | 418 | controls held, including SO-unionall, PT-1, CM-9, the seed-EQUAL cells and every decimal cell |
| unchanged non-EQUAL | | 134 | §2 |

Head totals over 1178 cells: EQUAL 1026, WRONG-READ 52, NATIVE-REFUSED 39, REFUSED-WHERE-SPARK-ACCEPTED 18,
ACCEPTED-WHERE-SPARK-REFUSED 13, BOTH-REFUSED 13, WRONG-WRITE 8, SIGN-ONLY+WRONG-READ 6, ORDER-DIFFER 3 (base: EQUAL 434).

## 2. The non-EQUAL head cells, each with its disposition

| Cells | Label | Disposition (order §3) |
|---|---|---|
| MM-1 × 6 | SIGN-ONLY+WRONG-READ | declared (min/max of zeros) |
| SQ-notin, WR-insert-notin `un` | WRONG-READ / WRONG-WRITE | declared `NAAJ-WRONG` (Q1) |
| SO-intersectall, SO-exceptall (SQL forms; typed forms refuse) | WRONG-READ, REFUSED-WHERE-SPARK-ACCEPTED | confounded: decimal twin wrong too; Q2, `SQL-SETOP-ALL-MULTISET-1` |
| WI-2 `f-expr`; WI-2, WI-3, WI-5, OB-1, OB-2 native | WRONG-READ, ORDER-DIFFER | confounded: NULL placement, decimal twin wrong (Q6) |
| GB-11 typed | WRONG-READ | confounded: `PIVOT-EMPTY-CELL-COUNT-1` |
| CM-11, GB-10 `spark-sql` | REFUSED-WHERE-SPARK-ACCEPTED | confounded: Spark-door refusals, decimal twin the same |
| TV-1, WR-ctas-*, WR-delete-*, WR-update-in, WR-merge-* native | NATIVE-REFUSED | the native door cannot express them |
| SQ-any | ACCEPTED-WHERE-SPARK-REFUSED | out: Spark refuses `= ANY` |
| SO-recursive | ACCEPTED-WHERE-SPARK-REFUSED / BOTH-REFUSED | out: Spark refuses `UNION` in a recursive CTE |
| PT-3 | WRONG-READ | out of seat 2: `.partitions` metadata (`ICEBERG-PARTITIONS-TABLE-FLOAT-1`) |
| PR-2 `spark-sql` × 4 | SPARK-ICEBERG-DIFFERS-FROM-VIEW | the oracle self-check row, unchanged |

## 3. Item 1's added cells and the addendum

- **Spill (D16) and plan-shape cells** ([moves-new-cells.md](moves-new-cells.md)): all 16 spill cells and the
  FLOAT-to-DOUBLE join moved WRONG-READ → EQUAL on both doors (18 cells). `Distinct::On` stays ORACLE-MISSING; its
  native answer now has one zero row.
- **Addendum** ([moves-addendum.md](moves-addendum.md)): 6 WRONG-READ → EQUAL (the integer-derived keys on the Spark
  door and IK-cast-group native) and 2 ORDER-DIFFER → EQUAL (the sign-set NaN `ORDER BY` rows: a sort key is a seat-2
  key, so the NaN sorts last, as in Spark). N1-one-column moved EQUAL → WRONG-READ on both doors: it is the Q1
  `NOT IN` cell under Spark's default plan, declared `NAAJ-WRONG`. Q7-merge-delete-only stays EQUAL on all three
  kinds. Everything outside part 1 is unchanged: N2 (refused), Q2's writes, Q6's `F.expr` windows, Q10's NaN
  partitions, Q11's sign-set NaN `max`/`min`/`greatest`/`least`, the log family, and IK-group native (integer
  division on the native door, not a float key).
- **Floatkeys**: all 84 cells (variants `bare`, `signed` and `matrix`, #1307's seeds) are EQUAL on the head, so the
  28 float cells leave `KNOWN_OPEN` in `python/repark/tests/parity/_flat_matrix_cells.py`.

## 4. Guard (d), option (a)

- No provider in use classifies a wrapped filter as `Exact`: MemTable is `Unsupported`, the Parquet listing table is
  `Inexact`, the temporary-view provider now answers `Inexact` for a filter containing the UDF, Postgres pushdown
  renders no UDF (so `Inexact`), and the fork's Iceberg provider is `Inexact` for every filter. Pinned in
  `key_tests.rs` (`no_provider_in_use_takes_a_wrapped_filter_as_exact`).
- The Filter stays above the scan and the rows equal the truth (the same pin, over Parquet, and every CM-* clause cell
  above on Iceberg).
- On Iceberg, nothing new is converted: `__repark_normalize_zero__` has no arm in the fork's `expr_to_predicate.rs`,
  so a wrapped predicate copied into `TableScan.filters` is dropped by the scanner, and the base predicate it sits
  beside is the same on base and head. The measured base-against-head comparison of pushed predicates and pruning
  counts is listed as open in the hand-back.

## 5. Finding outside part 1

- **A recursive `UNION` under `count(*)` panics** in DataFusion's `multi_group_by` (`index out of bounds: the len is 0`)
  on base and head alike, with integer keys too and with the key wrap off; `SELECT x FROM r` answers. Proposed card
  `RECURSIVE-UNION-COUNT-PANIC-1`.

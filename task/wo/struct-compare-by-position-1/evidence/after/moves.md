# After-table moves by label (release build of the item-6 head)

Each table's before leg is RePark 1.5.6, the branch point's product. Each after leg is this head. A move is a cell whose
label changed between the two legs, both labelled against the same Spark 4.1.2 leg (or the derived oracle). Counts come
from the raw legs, not from hand counting.

## Step 0 (4393 cells)

| label | before (1.5.6) | after (head) |
|---|---|---|
| EQUAL | 1218 | 2789 |
| WRONG-ANSWER | 1004 | 0 |
| WRONG-WRITE | 322 | 0 |
| ACCEPTED-WHERE-SPARK-REFUSED | 553 | 0 |
| REFUSED-WHERE-SPARK-ANSWERED | 355 | 34 |
| BOTH-REFUSED | 724 | 1277 |
| SNAPSHOT-DIFF | 145 | 221 |
| ORACLE-MISSING (derived: EQUAL / REFUSED / WRONG-WRITE) | 72 (14 / 18 / 40) | 72 (72 / 0 / 0) |
| PANIC | 0 | 0 |

| move | cells | note |
|---|---|---|
| WRONG-ANSWER -> EQUAL | 1001 | |
| WRONG-WRITE -> EQUAL | 250 | |
| WRONG-WRITE -> SNAPSHOT-DIFF | 72 | read-back equal; Spark commits an empty snapshot on a no-op DML (the empty-DML guard's declared divergence) |
| DERIVED-WRONG-WRITE -> DERIVED-EQUAL | 40 | struct-literal DELETE, held to the derived oracle |
| DERIVED-REFUSED -> DERIVED-EQUAL | 18 | struct-literal DELETE, held to the derived oracle |
| ACCEPTED-WHERE-SPARK-REFUSED -> BOTH-REFUSED | 553 | |
| REFUSED-WHERE-SPARK-ANSWERED -> EQUAL | 321 | |
| REFUSED-WHERE-SPARK-ANSWERED -> SNAPSHOT-DIFF | 4 | 01021 01022 01023 01024 |
| WRONG-ANSWER -> REFUSED-WHERE-SPARK-ANSWERED | 3 | 04377 04378 04387: unit C (#1283), not this unit |
| EQUAL -> REFUSED-WHERE-SPARK-ANSWERED | 1 | 04389: unit C (#1283), not this unit |

The four unit C cells refuse from `struct_setop_names`, which is on main; the error text names it. The 30 other
`REFUSED-WHERE-SPARK-ANSWERED` cells did not move: they refuse on 1.5.6 and on main too.

| card | severity | cells |
|---|---|---|
| `STRUCT-MAP-SUBSCRIPT-DML-1` | S3 | DELETE 02491 02510 02529 02548 02567 02586 02605 02624 02643 02662; UPDATE 02492 02511 02530 02549 02568 02587 02606 02625 02644 02663 |
| `STRUCT-MAP-SUBSCRIPT-JOIN-1` | S3 | DataFrame typed join 02498 02517 02536 02555 02574 02593 02612 02631 02650 02669 |

Inside `BOTH-REFUSED`, 32 SQL cells refuse with a class other than Spark's (`STRUCT-COMPARE-PLANNER-CLASS-1`, S3
class-only): 01147 01150 01166 01169 01185 01188 01204 01207 01223 01226 01242 01245 01261 01264 03969 03972 03988
03991 04007 04010 04045 04048 04064 04067 04083 04086 04121 04124 04140 04143 04159 04162. Main gives the same text.

## Variant table, seat C (218 cells on the RePark leg)

| label | before (1.5.6) | after (head) |
|---|---|---|
| EQUAL | 36 | 96 |
| RESULT-ROWS-DIFFER | 48 | 0 |
| ROWS-DIFFER | 24 | 0 |
| REFUSED-WHERE-SPARK-ACCEPTED | 108 | 120 |
| ACCEPTED-WHERE-SPARK-REFUSED | 2 | 2 |

| move | cells | note |
|---|---|---|
| RESULT-ROWS-DIFFER -> EQUAL | 48 | |
| ROWS-DIFFER -> EQUAL | 24 | |
| EQUAL -> REFUSED-WHERE-SPARK-ACCEPTED | 12 | `STRUCT-FSL-SIZE-COMPARE-1`, S3 over-refusal: P10-01577 01579 01581 01583 01585 01587 01637 01639 01641 01643 01645 01647 |

The 108 unmoved refusals are a view list against a non-view list, refused by DataFusion's coercion on 1.5.6
(`STRUCT-SETOP-LISTVIEW-COERCION-1`, out of clause). The two unmoved acceptances are P10dml-01829 and P10dml-01831: Spark
raises an internal `IllegalArgumentException` there (the seat C notes, item 11), so the row has no clean oracle.

## Variant extension (246 cells on the RePark leg)

| label | before (1.5.6) | after (head) |
|---|---|---|
| EQUAL | 47 | 125 |
| WRONG-ANSWER | 111 | 12 |
| WRONG-WRITE | 2 | 0 |
| REFUSED-WHERE-SPARK-ANSWERED | 82 | 103 |
| SNAPSHOT-DIFF | 0 | 2 |
| DERIVED-EQUAL / DERIVED-WRONG-WRITE | 2 / 2 | 4 / 0 |

| move | cells | note |
|---|---|---|
| WRONG-ANSWER -> EQUAL | 96 | |
| WRONG-WRITE -> SNAPSHOT-DIFF | 2 | P38-00459 P38-00463 |
| DERIVED-WRONG-WRITE -> DERIVED-EQUAL | 2 | P38-00467 P38-00471 |
| WRONG-ANSWER -> REFUSED-WHERE-SPARK-ANSWERED | 3 | P34-00386 00388 00390: `fsl1` against `fsl2` in the same order, now refused (`STRUCT-FSL-SIZE-COMPARE-1`, the safe direction) |
| EQUAL -> REFUSED-WHERE-SPARK-ANSWERED | 18 | `fsl1` against `fsl2` reordered (`STRUCT-FSL-SIZE-COMPARE-1`, S3 over-refusal): P31-00098 00100 00102 00128 00130 00132; P33-00314 00316 00318 00320 00322 00324 00326 00328 00330 (depth); P35-00422 00424 00426 (typed DataFrame door) |

The 12 `WRONG-ANSWER` cells left are P36, the native door, which by design gets no rule and answers by name (the order's
design point 7; pinned as a divergence): P36-00439 00440 00441 00442 00443 00444 00445 00446 00447 00451 00452 00453.
The 82 unmoved refusals are 72 view or cross-view pairs (`STRUCT-SETOP-LISTVIEW-COERCION-1`) and 10 facade ingests of
a dictionary column (`FACADE-INGEST-ARROW-NORMALIZE-1`: P32-00242 to 00258, P37-00457); both are out of clause.

## Halts checked

- No cell outside the clause moved. The moves above are all comparison seats or DML predicates inside it, except the
  four unit C cells, which move because of `struct_setop_names` on main.
- Every base-EQUAL cell newly refused is a carded, measured S3 over-refusal (`STRUCT-FSL-SIZE-COMPARE-1`: 18 ext and 12
  seat C cells) or a unit C cell (04389).
- No inside-clause `WRONG-ANSWER`, `WRONG-WRITE` or `DERIVED-WRONG-WRITE` cell is left.

## Unit C's and #1290's pins and probes, re-run on this head

| what | result |
|---|---|
| `cargo test -p repark-sql --test struct_setop_reorder_refusal` (debug) | 4 passed, exit 0 |
| `... --test struct_setop_reorder_refusal_delta2` | 3 passed, exit 0 |
| `... --test struct_setop_type_family_1` (#1290) | 2 passed, exit 0 |
| `pytest test_struct_setop_reorder_refusal_1.py test_struct_setop_type_family_1.py` (release module) | 721 passed, exit 0 |
| unit C's domain probe, `probe_setop.py`, 200 cells (`unit-c/domain-head.jsonl`) | every outcome, row set, error text and persisted read-back equals main `45018a2c` (`unit-c/domain-main.jsonl`) and unit C's committed head leg `repark-head-76d82c28.jsonl` |
| unit C's control probe, `probe_setop_controls.py`, 252 cells (`unit-c/controls-head.jsonl`) | the 240 set-operation cells (identical-struct controls, `UNION DISTINCT`) equal main (`unit-c/controls-main.jsonl`) |

No pin file of unit C or #1290 changed. The control probe also holds 12 comparison cells, which are this unit's ground,
not unit C's. Eight of them moved, and all eight now match Spark:

- `c1` (`named_struct('a', id, 'b', 2) = named_struct('b', 2, 'a', 1)`) and `c2` (`... = named_struct('a', 1, 'c', 2)`),
  as an `INSERT ... SELECT ... WHERE` on the four routes (sql, filter-str, f-expr, typed);
- main answered and persisted an extra row (a wrong write);
- head refuses with `BINARY_OP_DIFF_TYPES` and persists the two seed rows, as Spark 4.1.2 does.

`c3`, the duplicate-name pair, still answers on all four routes, the same as main and Spark.

## Out of clause: map-only comparisons move toward Spark (named for review)

The full facade gate surfaced a map-only comparison that this unit moved:

- **The case.** `test_u9_types_1.py[map]`, step `map/cmp/merge-on`:
  `MERGE INTO t x USING (SELECT map('a', 1) AS m) y ON x.c = y.m ...` over `MAP<STRING, INT>`.
- **Before.** On main, RePark committed the update. Residue R-19 recorded that wrong write, and `map/cmp/rows` read
  `[[0, 'hit'], [1, 'n']]`.
- **On head.** RePark refuses with Spark's `DATATYPE_MISMATCH.INVALID_ORDERING_TYPE`, and the rows read Spark's
  `[[0, 'n'], [1, 'n']]`.

The orchestrator ruled to accept the move.

**Why a MAP = MAP comparison reaches this rule.** The orchestrator guessed that a map's entries struct makes
`holds_struct` true. That is not the path:

- `align_expr` returns at `struct_pair`, because both sides are the same map type (`left != right` fails).
- The refusal comes from `align_node`. It runs at every plan node that holds a comparison (`has_struct_candidate`
  matches any comparison operator, `IN` or `element_at`), and it calls `map_ordering::refuse_in_expr` on every leaf
  expression before `align_expr` runs. That is the carved slice's map-ordering guard.
- The other doors met `map_ordering`'s check already, through its eager pass over built SQL plans
  (`analyze_built_plan`). That pass misses two kinds of plan:
  - the MERGE join, which the Iceberg MERGE path plans outside it;
  - the DataFrame expression context.
- This rule sits in the session analyzer and in `build_expr_context`, so it now reaches both.

**Measured against main and Spark.** `map-seats/` holds the probe, three legs and `table.md`. It covers 18 map-only
seats over `MAP<STRING, INT>`: 13 SQL and 5 DataFrame.

- **Spark 4.1.2** refuses all 18 with `INVALID_ORDERING_TYPE`, and no row changes.
- **Head moved five seats against main, each to Spark's class:**
  - `MERGE ... ON x.c = y.m` and `MERGE ... WHEN MATCHED AND x.c = y.m` (main wrote `note = 'hit'` on both, a wrong
    write);
  - the typed DataFrame `filter(F.col('c') == F.col('d'))` and `select((F.col('c') == F.col('d')))`;
  - the string-door `filter(F.expr('c = d'))`.
- **Unchanged and already refused on main (11):** `select`, `where`, `<>`, `<=>`, `CASE WHEN`, `IN` list, join `ON`,
  `UPDATE ... WHERE`, `DELETE ... WHERE`, the typed DataFrame join, and `selectExpr`.
- **Unchanged and still answered where Spark refuses (2), both pre-existing:**
  - `c IN (SELECT m ...)` answers `[[0]]`;
  - `named_struct('k', c) = named_struct('k', d)` answers `[[0], [1]]`.

  Both are proposed cards in the hand-back.

**The refusal text.** An analyzer-rule refusal reached the facade wrapped as
`struct_operands_by_position\ncaused by\nError during planning: [...]`, so the facade parsed no condition.

- `repark-core`'s `error_map` now peels that wrapper to the bare bracketed refusal, as it already does for
  `join_condition_refusals`. This applies to every refusal of this rule, not only map ones, so each carries its
  condition and SQLSTATE.
- `map/cmp/merge-on` now matches Spark's condition, error class and SQLSTATE. Its message is Spark's message without
  Spark's `; line 1 pos N;` and analyzed-plan tail.
- That is R-6's text-only difference, so the step moves from R-19 to R-6, recorded exactly.
- `map/cmp/rows` drops its residue and asserts Spark's rows.
- `map/cmp/group` keeps R-19. The u9 ledger's R-6 and R-19 rows say so.

# After-table moves (STRUCT-COMPARE-SEAT-FAMILY-1, build item 3)

The after leg is this unit's release build (`raw/head*.jsonl`, `__debug_assertions__` False), run with the committed
step-0 probes (`probe_family.py --leg head`, `rerun_v3.py head`, `probe_family_add.py --leg head`). Its `head.jsonl`
carries the v3 re-run records, merged as step 0's are. Labels are against step 0's Spark leg; "before" is step 0's
`head` leg (#1292's head, this unit's base). Three runs on two builds (the predecessor's, and this head before and
after the display fix) agree on every outcome field; their messages differ only in random alias ids.

`table.md` is `step0/probe_family_add.py --render` over a scratch `raw/` holding step 0's `spark` legs, this leg as
`head` and step 0's `head` legs as `base`, with the title line rewritten.

## Moves by seat (before -> after; unchanged pairs omitted)

| Seat | Before | After | Cells |
|---|---|---|---|
| key-list join | WRONG-ANSWER | EQUAL | 108 |
| key-list join | REFUSED-WHERE-SPARK-ANSWERED | EQUAL | 60 |
| key-list join | REFUSED-WHERE-SPARK-ANSWERED | WRONG-ANSWER | 42 |
| key-list join | ACCEPTED-WHERE-SPARK-REFUSED | BOTH-REFUSED | 202 (92 map keys) |
| key-list join | BOTH-REFUSED | ACCEPTED-WHERE-SPARK-REFUSED | 8 |
| `nullif` | WRONG-ANSWER | EQUAL | 104 |
| `nullif` | REFUSED-WHERE-SPARK-ANSWERED | EQUAL | 32 |
| `nullif` | ACCEPTED-WHERE-SPARK-REFUSED | BOTH-REFUSED | 16 |
| simple `CASE` | WRONG-ANSWER | EQUAL | 53 |
| simple `CASE` | REFUSED-WHERE-SPARK-ANSWERED | EQUAL | 20 |
| simple `CASE` | ACCEPTED-WHERE-SPARK-REFUSED | BOTH-REFUSED | 10 |
| `BETWEEN` | WRONG-ANSWER | EQUAL | 36 |
| `BETWEEN` | REFUSED-WHERE-SPARK-ANSWERED | EQUAL | 12 |
| `BETWEEN` | ACCEPTED-WHERE-SPARK-REFUSED | BOTH-REFUSED | 6 |
| `NOT BETWEEN` | WRONG-ANSWER | EQUAL | 30 |
| `NOT BETWEEN` | REFUSED-WHERE-SPARK-ANSWERED | EQUAL | 10 |
| `NOT BETWEEN` | ACCEPTED-WHERE-SPARK-REFUSED | BOTH-REFUSED | 5 |
| `PREPARE`/`EXECUTE` | WRONG-ANSWER | EQUAL | 20 |
| `PREPARE`/`EXECUTE` | ACCEPTED-WHERE-SPARK-REFUSED | BOTH-REFUSED | 3 (2 map keys) |
| list kinds | WRONG-ANSWER | EQUAL | 21 |
| list kinds | EQUAL | REFUSED-WHERE-SPARK-ANSWERED | 4 |
| lateral join | ACCEPTED-WHERE-SPARK-REFUSED | BOTH-REFUSED | 6 |
| DML predicate | WRONG-WRITE | EQUAL | 51 |
| DML predicate | DERIVED-WRONG-WRITE | DERIVED-EQUAL | 3 |
| DML `INSERT … SELECT` | WRONG-WRITE | EQUAL | 6 |
| DML `MERGE` from a join | WRONG-WRITE | EQUAL | 6 |

Every clash refusal that moved carries Spark's condition (`BINARY_OP_DIFF_TYPES`, `DATA_DIFF_TYPES` on
`coalesce(s, s)` for a `FULL` join, `INVALID_ORDERING_TYPE` for a map key), except where Spark's refusal is its
own `NATURAL` semi/anti or `lateralJoin` error. No DML cell is wrong after. `ON` and set-operation controls do not
move. Wrong after: 214 key-list cells (below) and Q2059, Q2063 (`LISTVIEW-INNER-JOIN-KEY-1`).

## Out-of-clause moves, each to Spark's measured outcome or inside a card

- **Map keys (ruling Q-B):** every key-list join on a `MAP` key, plain `MAP<STRING, INT>` included, moves from
  accepted to Spark's `INVALID_ORDERING_TYPE` refusal: Q1117–Q1178, X0037–X0040, X0112–X0124, X0126–X0138
  (key-list), X0142–X0143 (`EXECUTE`), Q1197 (lateral).
- **Lateral joins:** Q0081, Q0329, Q0453, Q1073, Q1321 and Q1197 move from accepted to refused. Spark refuses them
  too (`lateralJoin` with `on=`); `LATERAL-USING-ACCEPT-1` stays carded. Four refuse with a schema error, not
  Spark's class.
- **Ruling 1:** X0025–X0028 and X0049–X0052 (`NATURAL` semi and anti) move from BOTH-REFUSED to accepted, inside
  `NATURAL-EXISTENCE-JOIN-ACCEPT-1` (S3): the arm cannot tell `NATURAL` from `USING`.
- **Ruling 2:** Q1954–Q1957 (`List` against `FixedSizeList(2)`, reordered) move from EQUAL to refused with the
  element-field error of their same-order twin Q1958: a measured S3 over-refusal under
  `FACADE-INGEST-ARROW-NORMALIZE-1` F-5.

## The 214 residual key-list cells (ruling 5)

`int_twin.py` checks every struct-key WRONG-ANSWER cell against its `INT`-key twin by its difference from Spark:
the column permutation, the rows with the key dropped, and the key's null pattern. The other 21 are the `INT` cells
themselves (PR #1000's card, Q6).

| Difference | Verdict | Cells |
|---|---|---|
| column order only | `INT-TWIN-SAME` | 105 |
| column order only | `INT-TWIN-SAME`, twin from the other case setting (`case` shape) | 14 |
| column order only | `INT-TWIN-SAME-ORDER`: same permutation; every right key is NULL, so the twin's shown key cannot show (Q1375, Q1390, Q1401, Q1419) | 4 |
| right or full shown key | `INT-TWIN-SAME` | 63 |
| right or full shown key | `INT-TWIN-SAME`, other case setting | 7 |

No cell is outside `INT-TWIN-SAME`. The 42 cells that moved from REFUSED to WRONG-ANSWER now pair by position and
show only the `INT` defect: Q0746, Q0749, Q0752, Q0753, Q0758, Q0761, Q0764, Q0766, Q0768, Q0772, Q0775, Q0778,
Q0779, Q0784, Q0787, Q0790, Q0793, Q0796, Q0797, Q0802, Q0805, Q1490, Q1493, Q1496, Q1497, Q1502, Q1505, Q1508,
Q1510, Q1512, Q1516, Q1519, Q1522, Q1523, Q1528, Q1531, Q1534, Q1537, Q1540, Q1541, Q1546, Q1549.

## `LISTVIEW-INNER-JOIN-KEY-1` (ruling 3)

Q2059 (`kR_view_rev`, reordered names) and Q2063 (`kR_view_same`, the same-names twin) are DataFrame key-name inner
joins on `ListView` keys. Both return the cross product `[[1,10],[1,30],[2,10],[2,30]]` on main before #1292, on
#1292's head and on this head; Spark returns one row. The twin shows it without any reorder, so the defect does not
depend on the pairing. `probe_listview.py` shows that it does not depend on structs either: a
`ListView(Int64)` or `LargeListView(Int64)` key gives the cross product through `df.join(other, "s")`, while `ON`
and `List(Int64)` answer correctly (`listview.txt`). Severity S1 (silent wrong rows); reach: `ListView`-encoded keys
only. Both cells are left out of the pins under the card.

## Name preservation (ruling 4)

`../names/` holds 18 unaliased cells on `spark`, `base` (main `68f70b0f`) and `head`. Head equals base on all 18.
Five match Spark exactly and are pinned to Spark's name: N0004 and N0008 (typed `between`, `~between`), N0014 and
N0018 (`F.expr` `nullif`), N0015 (typed `nullif`). On the other 13, DataFusion's rendering differs from Spark's on
base already (`s BETWEEN t AND t` for `between(s, t, t)`), and the pin holds that no name shows the relabel. Before
`without_relabels`, `selectExpr` showed `__repark_struct_by_position__(t)` on N0002, N0006, N0010, N0013 and N0017,
as main still does for #1292's `=`, `<` and `IN`.

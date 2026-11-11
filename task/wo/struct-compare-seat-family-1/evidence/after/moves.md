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

`../names/` holds 24 unaliased cells on `spark`, `base` (main `68f70b0f`) and `head` (this head; the six cells
N0019–N0024 were added in item 5 and measured on its ci-profile build, where the first 18 give the release names).
Head equals base on N0001–N0018. Five match Spark exactly and are pinned to Spark's name: N0004 and N0008 (typed
`between`, `~between`), N0014 and N0018 (`F.expr` `nullif`), N0015 (typed `nullif`). On the other 13, DataFusion's
rendering differs from Spark's on base already (`s BETWEEN t AND t` for `between(s, t, t)`), and the pin holds that
no name shows the relabel. Before `without_relabels`, `selectExpr` showed `__repark_struct_by_position__(t)` on
N0002, N0006, N0010, N0013 and N0017, as main still does for #1292's `=`, `<` and `IN`.

N0019–N0024 compare a duplicate-named operand (`d = t`, `d < t`). Base shows the relabel on the SQL door
(`__repark_struct_by_position_planned__(…)`, the binder's) and on `selectExpr`; head shows none, and its `selectExpr`
names equal Spark's `(d = t)` and `(d < t)`. They pin the transparent relabel (F-2): with it reverted, the SQL-door
names show the binder's relabel again (item 5's revert check).

## The review delta (PR #1295)

The legs in `raw/` are now the delta head's release build (`__debug_assertions__` False), run as before
(`probe_family.py --leg head`, `rerun_v3.py head`, `probe_family_add.py --leg head`, v3 records merged), and
`table.md` is re-rendered the same way. Against the item-3 legs, 5 of 2428 records change outcome, and no other
record changes once the random crossJoin alias ids are masked:

| Cells | Before | After | Spark |
|---|---|---|---|
| Q1213–Q1217 (simple `CASE` over a `MAP` operand, item 2) | BOTH-REFUSED, `BINARY_OP_DIFF_TYPES` | BOTH-REFUSED, `INVALID_ORDERING_TYPE` on `(s = s)` | `INVALID_ORDERING_TYPE` on `(s = s)` |

The string door (Q1215–Q1217) no longer shows the crossJoin's internal column name. No label moves.

**The delta cells** (`../delta/`, 23 cells on `spark`, `pr` = PR #1295's head and `head` = the delta):

| Cells | `pr` | `head` = Spark |
|---|---|---|
| DK01, DK02, DK04–DK11: a map in a struct key (same and reordered names), under an array, deeper, `NATURAL`, DataFrame (item 1) | answered | `INVALID_ORDERING_TYPE` |
| DK03: the same key under `FULL` (item 1) | `DATA_DIFF_TYPES` | `INVALID_ORDERING_TYPE` |
| DK12–DK14, DK18: `MAP` against `INT` or a struct (Muse F2, item 6) | `INVALID_ORDERING_TYPE` | `BINARY_OP_DIFF_TYPES` |
| DK15–DK17: `INT` against `MAP` (Muse F4; recorded move: each key typed against its own side reaches the arm) | classless | `BINARY_OP_DIFF_TYPES` |
| DK19, DK21: controls (positional map-against-`INT` clash; plain map key) | Spark's class | unchanged |
| DK20: control, `INT` against a struct key | classless | unchanged, classless (`USING-SCALAR-STRUCT-KEY-CLASS-1`) |
| DK22, DK23: the duplicate-name `nullif` frame and its SQL twin (F6) | Spark's 9 rows | unchanged |

**#1292's three tables**, re-run on the delta build as item 3 did: step 0 is 4393 of 4393 records byte-identical
to `after/` there; variant seat C (218) and variant extension (246) are identical once the timing field and the
random alias ids are masked.

**Out of the delta, carded by the records clerk (Opus verdict F1).** Forms inside `GROUPING SETS`, `ROLLUP` and
`CUBE` refuse with a classless schema error (`STRUCT-POSITION-GROUPING-SETS-1`, S3, cells G01–G18 and P11 of the
verifier's probes). P11 is a base-EQUAL cell newly refused by this unit's arms; G02 (`CUBE (nullif(…))`) is its
reordered twin. #1292's `=` and `IN` arms fail the same way on main (G01, G05, G08, G12).

**Pins.** Both family pins now assert Spark's class on every refusal. 56 refusals that carry a measured class gap
hold a card and `expect.gap` (a refusal still required): `NATURAL-EXISTENCE-JOIN-ACCEPT-1` 16,
`STRUCT-TYPED-FORM-SCHEMA-1` 15, `LATERAL-USING-ACCEPT-1` 10, `STRUCT-COMPARE-PLANNER-CLASS-1` 9,
`STRUCT-SETOP-CONTROL-CLASS-1` 5, `USING-SCALAR-STRUCT-KEY-CLASS-1` 1. The class check showed that the pins never
registered `probe_family_add.py`'s four fixtures, so X0112–X0143 had passed on "table not found"; the header now
registers them.

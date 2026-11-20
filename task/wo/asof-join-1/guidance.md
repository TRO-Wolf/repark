# ASOF-JOIN-1 — Fable's guidance on step 0 (2026-11-20)

A Fable medium consult under the overnight window's terms (continued by the owner on 2026-11-20). This is guidance, not an owner ruling. The owner rates the unit (Q1) and holds the items listed at the end.

Every section below copies Fable's consult reply (2026-11-20).

## The one thing that changes several answers

Seat (a) picks the right row that is **adjacent** to the left row in the as-of order. That equals Spark's
`min_by(distance)` pick only where the distance grows steadily with the order. The step 0 already found one shape
where it does not (the non-equality `on`, Q11). There are three more, and two of the sketch's defaults (Q7 and Q10)
ask seat (a) to mirror them, which it cannot do:

| Shape | What Spark does | Evidence |
|---|---|---|
| Integer bounds, ANSI off | The distance wraps, and Spark returns the row with the smallest wrapped distance, which is a far row. For BIGINT backward, left `MAX` gets `rmin1` (`-MAX`) although `rtop` (`MAX`, distance 0) is there. | `spark-false-ONEMTCS-`, group `O`, first record |
| STRING as-of, ANSI off | The condition compares as strings and the distance subtracts as numbers. Backward, left `'2'` gets `r16b`: `'2' >= '16'` holds as text, and `2 - 16 = -14` is the smallest distance. | same file, `string_asof:backward` |
| Equal infinities | `inf - inf` is NaN, which `min_by` ranks last, so Spark never picks the exactly equal infinite row while any other candidate exists. | measured here, below |

In each of these Spark's row is not the nearest row by Spark's own stated rule. Reproducing them needs a second
lookup per left row, not a fill.

## What I measured (three short commands)

Fable's probes (`lists.py`, `structfill.py`, `inf.py`) ran in the orchestrator's scratch space. Main is
`pkg-main-9a48932f`, and both main runs printed its `_native.abi3.so`. Each ran under `measure.sh shared` on cores
32 to 47; the Spark one also under `spark_slot.sh`.

1. **Equal infinities on Spark 4.1.2, ANSI on.** Left `inf`, `-inf`, `1.0`; right `inf`, `0.5`, `-inf` in two input
   orders; `how="left"`, no key, no tolerance.
   - Backward, left `inf` gets `-inf` in one order and `0.5` in the other. It never gets the right `inf`.
   - Forward, left `-inf` gets `0.5` or `inf`. It never gets the right `-inf`.
   - ANSI off was not run.
2. **A struct-carrying fill on main, through the SQL door.** `last_value(named_struct(...)) IGNORE NULLS` over the
   union, one ascending and one descending pass, on 5 left and 5 right rows.
   - It answers, and both passes plan as `BoundedWindowAggExec`.
   - A right row with a NULL payload column is kept (the struct is non-null, the field is NULL).
   - A left row with a NULL as-of value got a forward match (`rid` 1), because NULL sorts first in the descending
     pass. The seat must keep NULL as-of left rows out of the fill, as W2 does.
   - Cost was not measured.
3. **List arguments on main.** See Q14.

## Q1. The rating

**Guidance: S3, as proposed. The rating is the owner's.** The direct call refuses loudly in every direct cell and
never returns a wrong row, and W2 gives Spark's rows on the grid up to ties in 2 s at 2M × 2M. That is S3 by the
severity scale.

One thing for the owner to weigh that the sketch does not say: severity and priority differ here. Trades-to-quotes
joins are central to the time-series work RePark is aimed at, and RePark's sort-bound plan would answer in seconds a
query that Spark's own plan scales to hours on (about 200 h with keys by the sketch's own scaling). I would file it
S3 and schedule it after the label, not leave it as unrated backlog.

## Q2. The name

**Adopt A: `_joinAsOf` only.** It is PySpark's exact name, so ported code finds it, and it skips the example gate and
the freeze register. A public alias can be added at any minor.

## Q3. `merge_asof`

**Adopt A: out.** There is no pandas-on-Spark frame to pass it, and it lowers to `_joinAsOf(how="left")` with the same
matches in 57 of 57 comparable cells per mode. Keep the `P` cells as the reference.

## Q4. Ties

**Change the default: keep the nearest half of A, drop the duplicate half.**

- **A `nearest` tie goes to the backward row.** Adopt. It is a function of the data, it costs nothing in the fill,
  and it is what W2 does on main in 756 of 756.
- **Duplicate right rows at one as-of value: promise nothing beyond tie-set membership.** "The first in a stable sort
  of the right input" is not a rule RePark can keep. There is no row id to break the tie with
  (`monotonically_increasing_id` is refused), and the plan repartitions by hash before the sort (the plan in
  measurement 2 shows `RepartitionExec: Hash([k], 16)` under the `SortExec`), so the order in which duplicates reach
  the sort is not fixed for inputs of more than one batch. Main's own W2 shows it: the higher `rid` in 1,400 cells and
  the lowest in 532.
- **The pins** assert membership for duplicates and the exact row for a nearest tie. Do not pin "repeat stability"
  on a 14-row input and record it as determinism; that pin cannot fail at this size and the claim is unproven at
  scale.
- The registry row says: nearest ties go backward; among duplicates the pick is unspecified, as on Spark.

## Q5. The seat

**Adopt C: seat (a) now, seat (b) only if the owner wants the cost afterwards.** Two corrections to the sketch's case
for (a):

- **The struct carry is forced, not a saving.** W2 as measured joins back on `rid`, a unique integer the probe data
  happens to have. The seat cannot mint one, so it must carry the right row as a struct through the union, both sorts
  and both fills. Measurement 2 shows this works on main today.
- **"About W2's time" is therefore not measured for the seat's real shape.** W2's 2.0 s moved one integer and a
  short string. A wide right table rides both sorts as a struct, and `nearest` carries two of them. Unit 1's bar
  should name a cost case with a realistic payload (say 20 right columns) beside the narrow one, with a stated
  ceiling, so the unit-2 decision has a number.

## Q6. Spark's unnamed errors

**Adopt A,** with two additions:

- The named-condition list must be the per-direction one (the critic's F2): an integer tolerance on a timestamp is
  `UNEXPECTED_INPUT_TYPE` for backward and nearest, and `BINARY_OP_DIFF_TYPES` for forward. Pin all the `E` cells,
  not a summary of them.
- For a column tolerance, raise Spark's own defined condition `AS_OF_JOIN.TOLERANCE_IS_UNFOLDABLE`. Spark defines it
  in `error-conditions.json` and only fails to reach it (it throws `INTERNAL_ERROR` first). That is better than
  inventing a RePark name.

## Q7. Overflow in the distance

**Change the default: mirror ANSI on, do not mirror ANSI off.**

- **ANSI on: raise `ARITHMETIC_OVERFLOW` where Spark does.** It is loud and it is Spark's named condition. Note for
  the brief that Spark raises when *any candidate's* distance overflows, not when the pick's does, and that the
  tolerance bound (`l - t`, `l + t`) can overflow too. The seat can get this from the same windows: the farthest
  candidate is the first right value in the partition (backward) or the last (forward). The bar is the 24 `O` cells,
  each one pinned; the four cells where Spark answers under ANSI on (`exact=False`, tolerance 2, backward and
  forward) show the raise depends on which side of the condition Spark evaluates first, so the builder should hand
  back if the 24 cells cannot be met by one rule.
- **ANSI off: return the nearest row, and record a documented divergence.** Spark's answer there is the wrapped
  pick shown in the table above. Seat (a) cannot produce it without a second lookup, and the row it would buy is
  wrong by Spark's own rule.

This is a kept divergence from Spark, so it goes to the owner (list at the end).

## Q8. The SQL `ASOF` alias

**Adopt A: out of the unit, card it, S3.** It is a parser keyword difference in sqlparser 0.62, and a table aliased
`ASOF` with no `AS` is rare. It does not belong in an as-of join unit.

## Q9. The side findings

**Adopt A: card both.** Ratings and scope are under Q14; the list card should be wider than the sketch has it.

## Q10. STRING as-of columns with ANSI off

**Change the default to B: refuse in both modes** with `DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE`, which is Spark's own
ANSI-on answer. The sketch's default A cannot be built on seat (a): as the table above shows, Spark's ANSI-off rows
come from a text comparison and a numeric distance, and the pick is not an adjacent row. Six cells become a
documented RAISE under ANSI off.

I would extend the same refusal to a STRING on **either** side of a mixed pair (BIGINT/STRING, STRING/BIGINT) in unit
1. Those are the 8 cells that are WRONG even on Spark's own W1, and the place where main's union types the pair
differently (the critic's F15). Refusing them takes both problems out of the unit. This also goes to the owner as a
divergence.

## Q11. A non-equality Column `on`

**Adopt A: refuse.** State the accepted shape positively in the brief: `on` is a conjunction of `left.x == right.y`
or `left.x <=> right.y` terms, and anything else is refused with a typed error that names the argument. Two cells
become a documented RAISE; 1,168 move.

Note for the builder: a `==` key drops right rows with a NULL key before the union (a window partition groups NULLs
together), and a `<=>` key keeps them. The two can mix in one `on`.

## 12. Build or hold

**Hold unit 1 until the owner rates it, and write no brief before then.** The rating, Q2 and Q3 shape the brief, both
build lanes have S1 label work queued, and this unit is off the label path by its own proposal.

Three things are worth doing first, none of which needs a build lane:

1. **A records pass on the step 0** (clerk grade): the Q7 and Q10 defaults and their reasons, the equal-infinity
   cells, the duplicate-tie wording in Q4, and seat (a)'s struct carry and cost caveat.
2. **The coerced tie model** from question 13 (a probe change, not product code).
3. **One cost sample of the struct-carrying fill** through the SQL door on main at 2M × 2M, narrow and wide payload,
   under the exclusive lock, when the lock is quiet. This is the
   only number that could change Q5, so it is worth having before the owner reads the step 0.

## 13. The open F6 reading

**The domain table does not need the model before the questions are ruled; unit 1's bar does need it.**

- No question turns on those 24 cells. They are `main-W1` cells, a workaround's verdicts on mixed types off the
  grid. The direct call is MISSING in all of them either way, and W1 is not the seat.
- The reason the widened `cmp.py` then disagreed with Spark is visible in `model.py`: it compares the integer
  position `p` of each row, not the value. Across types the positions do not order the same way. DATE position 2 is
  2 January, which is later than every TIMESTAMP in the data (all within the first minute of 1 January). So the fix
  is small: map each side's value to Spark's common comparison type first (dates and both timestamp types to
  microseconds in the session zone, numerics to one number), then apply the same `holds` and `distance`.
- **Unit 1's bar:** the model must agree with Spark's direct call on every in-scope `M` cell before any mixed-type
  pin is written, since the pins assert tie-set membership and that needs a correct tie set. If Q10's extension is
  taken, the STRING pairs are refusals and need no model.
- Until then, the sketch's present wording (counts as `cmp.py` gives them, the reading recorded as open) is the
  honest record. Leave it.

The `N` group is owed the same care: the model has no NaN or infinity semantics, so its 8 WRONG per workaround are
partly ties among equal NaN or infinite distances (the critic says so for W1).

## 14. The side findings

**The correlated scalar subquery with a non-equality predicate: S3.** It refuses in every measured shape and returns
no wrong row. Two notes:

- The message is DataFusion's internal one (`Optimizer rule 'scalar_subquery_to_join' failed caused by Schema
  error`). The card should ask for a clean refusal at the least.
- **The label path: not shown either way.** Every measured shape is a `SELECT`. The same subquery inside `UPDATE
  ... SET c = (SELECT ...)` or a `DELETE`/`MERGE` predicate on an Iceberg v2 table was not measured by the builder,
  the critic or me. If those refuse too, it is off the path. One probe of those two shapes on a local v2 table
  settles it and should run before the card's rating is final.

**List arguments: S2, and the card is too narrow.** I measured 24 call shapes on main. Refused with a list:

- `select`, `groupBy`, `orderBy`, `sort`, `sortWithinPartitions`, `rollup`, `cube`;
- `orderBy([...], ascending=[...])`;
- `Window.partitionBy([...])` / `Window.orderBy([...])` (its own message);
- `F.struct([...])` (`NOT_COLUMN_OR_STR`);
- `select` with a tuple.

Accepted: `dropDuplicates`, `repartition`, `join(on=[...])`, `fillna(subset=)`, `F.array([...])`, `unpivot`, `pivot`
values. PySpark's side was measured only for `select`, `groupBy` and `orderBy` (the critic); the rest I know from
PySpark's signatures, so the card's step 0 should measure them on the oracle.

The reason for S2: `df.select(cols)` and `df.groupBy(keys)` with a list are everyday PySpark, the refusal hits ported
code on its first line, and the fix is thin facade work. It is loud and has a one-character workaround, so it is not
S1 and does not block the label. It should be one card covering every columns-varargs method, not three methods.
`core.py` is pinned at exactly 3,461 lines, so the fix must be line-neutral there or live in a helper module.

A further observation from the same run, not carded: `F.coalesce("i", "i")` with string names raises `AttributeError
'str' object has no attribute '_reject_nested_generator'`. PySpark's signature takes column names there. I did not
measure it on Spark; it wants one oracle cell and, if confirmed, its own line on the list card.

## 15. What the sketch or the critic got wrong

1. **Q7's and Q10's defaults contradict seat (a),** as Q11's situation did before the critic caught it. The critic
   raised Q10 as a question but proposed "mirror" as its first option without checking that it can be lowered. See
   the table at the top.
2. **No cell has equal infinities on both sides,** and no question covers NaN and infinities. The `N` data has a
   left `inf` and a right `-inf` but never the same infinity on both sides. Measurement 1 shows Spark skips the
   equal row. Add the cells and record the seat's answer (the equal row) as a documented divergence, the third of
   the same family.
3. **§5.1 presents "no join back" as an improvement on W2.** It is a requirement, and it moves the cost. See Q5.
4. **Q4's duplicate rule cannot be kept by the engine.** See Q4.
5. **§4 item 5 says the errors come "at analysis time (before any row is read)".** That holds for the type and
   argument errors. `ARITHMETIC_OVERFLOW` is a run-time error on Spark and will be on RePark. Say so, or the pin for
   Q7 will be written against the wrong moment.
6. **The model is positional,** which is why F6 could not be closed by widening `cmp.py`. See question 13. The
   sketch's "960 of 960" is right for what it covers (same-type cells and numeric pairs) but should not be read as
   covering mixed date and timestamp pairs.
7. **The critic's "no wrong row hides on ordinary data"** holds for the pairs it lists. It should not be stretched
   to the seat: the seat's wrong-row risk is in the three shapes at the top, and they are divergences to rule, not
   cells that pass.
8. **Seat (a) and a NULL as-of value on the left.** Measurement 2 shows such a row picks up a forward match from the
   descending pass unless it is kept out. W2 handles it; §5.1 does not mention it, and §4 item 3 states the rule
   without saying the fill violates it by default.

I found nothing wrong in the critic's jar reading, its ANSI-off grid or its cost check, and I did not re-measure
them.

## The owner's

1. **The rating (Q1).** S3 is supported. Whether it is scheduled after the label or left as backlog is a priority
   call, given what RePark is for.
2. **Three kept divergences from Spark,** all in the same family, where the guidance is to return the nearest row or
   refuse instead of Spark's row:
   - integer bounds with ANSI off (Q7): the nearest row, not Spark's wrapped pick;
   - STRING as-of columns with ANSI off, and STRING on either side of a mixed pair (Q10): refuse;
   - equal infinities (item 2 above): the equal row.
3. **Ties (Q4):** nearest ties go backward; duplicates are unspecified, as on Spark. This is a weaker promise than
   the sketch's default.
4. **The list-argument finding at S2,** as one card over every columns-varargs method.
5. **The correlated subquery card's rating waits on one DML probe** to say whether it touches the label path.
6. **Hold on unit 1** until the rating, with the three no-lane items of question 12 done meanwhile.

# Card USING-SIDE-KEY-DESIGN-1: resolve a qualified side key structurally, not by a marker

**Date:** 2026-10-08. **Filed by:** Claude (Haiku 5.5), docs lane, from the orchestrator's brief.

**Status:** open. A design card, not a fix order. Not scheduled.

**Requires:** an Opus design sketch before any executor is briefed. The sketch decides the shape;
this card only states the question, its inputs and its gates.

**Retires:** when the owner rules a design, and the design's first unit is filed or declined.

## Why

PR #1000 (USING-PER-SIDE-KEYS-1) was parked at `e1e763fa` on 2026-10-08 after three Opus verifies.
Each verify was a fresh corpus, and each found the plan-level span marker lost or unread in one
more clause. The fixes that followed moved the gap; they did not close it. The branch is
`fix/using-per-side-keys-1`, fetched into the scratch clone as `origin/fix/using-per-side-keys-1`.
Its verdicts' base is `main` at `a8b04260`.

The question is how a qualified side key (`l.id`, `r.id` on a `USING` join) is resolved so that no
planner pass can drop the information that says which side it names. Each of the three verifies
found the marker approach failing in one more pass.

## Measured

Finding titles, verbatim:

The verdicts are the first verify (`verify/`), the reverify (`reverify/`) and the reverify2
(`reverify2/`), under `/tmp/oc-worker/direct/wo/using-per-side-keys-1/`. Each is orchestrator
scratch, not in the repo.

**First verify** (`verify/verdict.json`, FAIL, six S1):

- SQL door: a lambda parameter named like the USING key is rewritten to the merged key (wrong rows on every join type, INNER included)
- SQL door: SELECT DISTINCT <key> ... FULL JOIN ... USING ... ORDER BY <key> now fails to plan (main answers)
- SQL door: ORDER BY a per-side key plus any column that is not in the select list now refuses on RIGHT/FULL (main answers)
- SQL door: output column name `v:1` leaks from the ORDER BY wrapper when both sides share a non-key column name
- DataFrame door: mixed-type USING keys on RIGHT/FULL show a lossy try_cast of the right key (a value that exists on neither side)
- SQL door: when USING spells the key in another case, the star keeps the left key while WHERE reads the merged key (a newly answered row with a NULL key)

**Reverify** (`reverify/verdict.json`, FAIL, three S1):

- SQL door: an aggregate or window function over the unqualified key written before the same function over a side key makes the side key read the merged key (third answer; main was right on that column)
- SQL door: when main refuses the unqualified key and the marker pass does not merge (wholesale-main shape or bail), the retry returns the plan with the key qualified to the left relation: silent wrong rows where main refused
- SQL door: comma-joined relation before a USING join: star shows a merged key that is neither Spark's answer nor main's (third answer by the rule; root cause is the older comma/JOIN precedence difference)

**Reverify2** (`reverify2/verdict.json`, FAIL, one S1):

- SQL door: the key/side-key gate does not read the WINDOW clause, so a named window over the bare key beside a window over a side key still makes the side key read the merged key, and where main refuses the retry answers rows that are not Spark's

The third verify's corpus, from its `corpus` note: 575 new SQL statements on `e1e763fa`, main
`a8b04260` and live Spark 4.1.2. 530 name USING or NATURAL. 18 are third answers, all one shape: a
`WINDOW` clause. Two of the 18 are statements main refuses. Without USING or NATURAL there are 45
statements, all identical to main.

The branch carries the join's identity in two places, both read by the planner. Both files exist
on the branch only, not on main:

- `__repark_using_k`, the helper column name in `crates/repark-core/src/column_resolution/using_keys.rs`
  (line 15 on the branch).
- `ORDER_MARK` (`__repark_using_order`) in `crates/repark-core/src/column_resolution/using_marks.rs`
  (line 103 on the branch), and the `__repark_using__` alias that the branch's `df_guards` documents.

Other facts from the verdicts, as stated in them: a non-USING DataFrame filter and sort ran about 3%
slower than main on debug wheels (the lane reported within 1%); and the registry says filter and
sort keep the reach.

## Step 0

Before the sketch relies on any count above, on live Spark 4.1.2:

1. Re-run the 575-statement corpus on `origin/fix/using-per-side-keys-1` at `e1e763fa` and on main
   `6c04b218`. Record the count of third answers, and the count of statements where main refuses
   and the branch answers.
2. Re-run the DataFrame grid (373 cells per engine) on the same two heads.
3. Record which of the three verdicts' S1 findings still reproduce on the branch head. A finding
   that does not reproduce is closed here with its date.

## The governing rule

- **Never worse than main.** A statement answers what main answers, or what Spark answers, and
  never a third value. Where main refuses, the design refuses or answers Spark's rows. It never
  answers rows that are neither.
- **A third answer is an S1.** One that is neither Spark's nor main's, on any shape, stops the unit.

## The question

How is a qualified side key resolved structurally, so the answer depends on what the name refers
to at bind time rather than on a marker a later pass may read, rewrite or drop?

The sketch should state:

1. Where the side is bound, and what the resolved reference carries. Candidate: the attribute
   identity of the side's column, not a name. Not measured.
2. Which planner passes can rewrite a projection, a window, an aggregate or a sort after binding,
   and how each leaves the resolved reference alone. The three verifies named aggregate, window,
   retry, WINDOW clause and comma-join passes. Not an exhaustive list.
3. What the design does for a shape it does not yet resolve. The options are a loud refusal that
   names the shape, or main's answer where main is right. A silent answer is not an option.
4. Whether the marker stays for the existing surfaces, and which of its uses go.

## Inputs

- The three verdicts above, orchestrator scratch.
- The branch `origin/fix/using-per-side-keys-1`, base `a8b04260`, head `e1e763fa`, 57 files changed.
  The sketch reads it; it does not rebase it.
- [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md), for the registry rows
  that the USING shapes claim.

## Gates for the first implementation unit

These are for whichever unit the design opens. They are not the sketch's to decide.

- The three verifies' corpora rerun on the new head: the 575 statements, the old 926-cell grid
  (`verify/vgrid.py`, 849 equal Spark, 0 regressed against main) and the DataFrame 373-cell grid per
  engine (`p12.py`). Zero third answers. Zero regressions against main.
- A mutation of the marker's read in each pass the verdicts named, red for a pin of that pass.
- Non-USING DataFrame filter and sort no slower than the lane's own report (within 1%) on release
  wheels. The reverify measured about 3% slower on debug wheels.

## Pointers

- [AGENTS.md](../../../AGENTS.md) "Change discipline": a design that changes a planner's identity
  model is a separate change, never a passenger on a fix.

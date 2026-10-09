# Card JOIN-CONDITION-REFUSALS-1: the join doors answer conditions Spark refuses

**Date:** 2026-10-08. **Filed by:** Muse Spark (muse-spark-1.3-contributor), CROSS-JOIN-CONDITION-1 fold 1, from the orchestrator's brief and the Opus verifier's verdict on that unit.

**Status:** re-opened 2026-10-09 by CROSS-JOIN-CONDITION-1 fold 3, narrowed to the five residues below (the fold-2 close stands for the landed refusal grid).

**Retires:** when every join door refuses the conditions Spark refuses with Spark's error class, pinned per condition and per join type.

## Why

The join doors (every `how`) answer conditions Spark 4.1.2 refuses: non-deterministic expressions (Spark `INVALID_NON_DETERMINISTIC_EXPRESSIONS`) and an untyped NULL condition (Spark `JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE`). The CROSS-JOIN-CONDITION-1 Opus verifier found it through the cross door: a cross join with such a condition moved from main's 12 rows to the inner door's answer, while Spark refuses under `cross` and under `inner` alike. The defect is the inner door's leniency, present on main for `"inner"`; the cross routing only reaches it. The verdict measured `cross` and `inner`; Step 0 measures every other `how` before any fix.

## Measured

Measured by the Opus verifier on Spark 4.1.2, RePark head `67028899` and main `40fc916f`, with `a = [(1,a),(2,b),(3,c)]` and `b = [(2,x),(3,y),(4,z),(9,q)]`, each condition on the native route and with the exact planner forced to miss. This card does not re-measure them.

Four conditions, two routes, eight cells. On every cell head's cross equals head's inner and main's inner, and main's cross answers 12 rows:

- `(a.id == b.k) & (F.rand(1) >= 0)` (grid `cond/rand`, extra `eq-and-rand`): Spark refuses with `INVALID_NON_DETERMINISTIC_EXPRESSIONS`; head answers 2 rows on both routes.
- `F.rand(7) < 0.5` (extra `rand-lt-half`): Spark refuses with `INVALID_NON_DETERMINISTIC_EXPRESSIONS`; head answers an 8-row subset on both routes.
- `F.lit(None)` (extra `null-untyped`): Spark refuses with `JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE`; head answers 0 rows on both routes.

The verdict's `cells.detail` further states, for the extra edge conditions: `struct-eq` is head-refuses-where-main-answered-wrong; `rand-only`, `uuid` and `shuffle` are same-as-main-not-Spark.

## Step 0

1. Measure Spark 4.1.2's error class and full text per condition and per join type, before any fix. Conditions: the three measured above plus `struct-eq`, `rand-only`, `uuid` and `shuffle`. Join types: every `how` the doors take.
2. Record RePark's current answer per condition and per join type on both routes, the same grid.
3. Only then fix, at the condition door, once for every `how`.

## The ask

Refuse the conditions Spark refuses, at the condition door for every `how`, with Spark's error class. A fix must not change any answer that already matches Spark.

Scope is the conditions above. Other refusal gaps are not in the ask.

## Gates

- Spark's error class and text per condition and per join type, measured before the fix.
- Every measured condition refused with Spark's class on every `how`, pinned per condition and per join type.
- A pin that fails on the old behaviour for the reason the card names, not for some other reason.
- No previously Spark-equal answer moves; the verifier's grid re-runs green apart from the intended refusals.

## Pointers

- [cross-join-condition-1-ledger.md](../../ledgers/staging/cross-join-condition-1-ledger.md), clause C-008, which carries the eight cells and the ruling.
- The cross-equals-inner pins in [test_cross_join_condition_1.py](../../../python/repark/tests/test_cross_join_condition_1.py), which hold the current leniency until this card lands.

## Close (2026-10-08)

Landed in CROSS-JOIN-CONDITION-1 fold 2: the `JoinConditionRefusals` analyzer rule
(`crates/repark-spark/src/normalize/join_condition.rs`) refuses nondeterministic
conditions with `INVALID_NON_DETERMINISTIC_EXPRESSIONS` and non-boolean conditions
with `JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE` for every `how` on the DataFrame door
and the SQL door, with Spark's head lines byte-equal. Pins per condition and per
join type in `test_join_condition_refusals_1.py` (134 legs over the
`join_condition_refusals_1_spark_oracle.json` recording) and
`crates/repark-spark/src/tests/join_condition_refusals.rs`; the card's Step 0
grid is the oracle. Residue, pinned as out of scope: SQL `CROSS JOIN ... ON`
stays a parse refusal where Spark answers, and a nondeterministic Python UDF in
a DF-door condition still answers.

## Re-open (2026-10-09)

CROSS-JOIN-CONDITION-1 fold 3 closes the Opus re-verify S2 (the accessors
answer the condition and parameters; pins assert them) and re-opens this card
narrowed to the re-verify's five S3 residues, recorded as R-CJC-1..R-CJC-5 in
[cross-join-condition-1-ledger.md](../../ledgers/staging/cross-join-condition-1-ledger.md):

- R-CJC-1 (DML reach): the rule also refuses under MERGE, INSERT ... SELECT
  and CTAS, unpinned; MERGE ON a non-deterministic condition carries the join
  condition name where Spark names the merge condition.
- R-CJC-2 (lateral): a lateral join with a non-deterministic ON carries the
  generic condition name where Spark names the lateral-join condition.
- R-CJC-3 (rendering): the refusal text is byte-equal for the oracle's 14
  conditions only; internal names leak into the rendering outside that set.
- R-CJC-4 (UDF correction): the fold-2 line that a non-deterministic Python
  UDF still answers is wrong — every UDF form already refuses, as on main.
- R-CJC-5 (struct equality): the struct-equality cross join refuses where
  Spark answers 0 rows and main answered 12 wrong rows (a refusal in place of
  wrong rows, ruled S3 by the orchestrator).

The fold-2 gates stay satisfied for the landed grid; the re-opened scope
retires when each residue is fixed or carded onward with its own gate.

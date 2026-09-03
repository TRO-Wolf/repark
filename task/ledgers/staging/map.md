# map — task/ledgers/staging/

## Purpose
Ledgers of units in flight. A ledger here on `main` is a charter whose retirement event has not
happened yet; every other ledger leaves for `../completed/` in its unit's last commit.

## Contents
- [ex-10-functions-null-cond-misc-ledger.md](ex-10-functions-null-cond-misc-ledger.md) —
  **EX-10 (2026-09-03), in flight:** the v0.7 example backfill's `F.*` null-handling,
  conditional, ordering, bit and session batch — 33 names landed in seven examples, the
  backlog ratchet 842 → 809; the 12 names the live oracle measured divergent (`F.isnan`
  `[False,False]` vs `[False,None]`, the session-identity four `repark` vs OS user) or
  refused (`F.expr` literals Spark-equal `[2,2]`/`['AB','AB']`, column ref `AnalysisException`
  vs Spark `[2.0,None]`; `F.raise_error` `USER_RAISED_EXCEPTION`; the input/partition five
  `UnsupportedOperationException`) stay on the backlog with both values recorded. `risk_tier: standard`. Branch
  `feat/ex-10-functions-null-conditional`. Slate:
  [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md).
- [ex-2-functions-math-bitwise-ledger.md](ex-2-functions-math-bitwise-ledger.md) —
  **EX-2 (2026-09-01), in flight:** the v0.7 example backfill's `F.*` math +
  bitwise family — the campaign pilot. One clause per batch; batch 1 covers
  eleven roots / exponential / power / sign / rounding names and moves the
  backlog ratchet 892 → 881; the twelfth, `F.expm1`, is measured, reported and
  left on the backlog rather than taught by an example that omits its reason
  for existing. `risk_tier: standard`. Branch
  `feat/ex-2-functions-math-bitwise`. Slate:
  [../briefs/example-backfill.md](../../../briefs/example-backfill.md).
- [ex-9-functions-maps-structs-json-ledger.md](ex-9-functions-maps-structs-json-ledger.md) —
  **EX-9 (2026-09-03), in flight:** the v0.7 example backfill's `F.*` map,
  struct and JSON family. Twelve names land in four files and the backlog
  ratchet moves 842 → 830; the other 24 roster names (json_tuple, csv, xml,
  xpath, variant) are measured against the live oracle and stay on the backlog —
  the engine refuses each (E1-disclosed deferrals). `risk_tier: standard`.
  Branch `feat/ex-9-functions-maps-structs-json`. Slate:
  [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md).
- [fnp-0-charter-ledger.md](fnp-0-charter-ledger.md) — **the Spark function parity campaign's
  scope audit and approval gate (2026-08-20):** the twelve-clause proposition ledger, the spike
  evidence behind it; C-007 (the four sub-project families) was closed by ruling D-7 on
  2026-08-20 and the gate passed. Design:
  [../docs/design/spark-function-parity.md](../../../docs/design/spark-function-parity.md); CAP-1
  appends a compatibility note that points its dated file-size premise at the live guards; slate:
  [../briefs/spark-function-parity.md](../../../briefs/spark-function-parity.md).
- [sem-0-charter-ledger.md](sem-0-charter-ledger.md) — **SEM-0 (2026-08-21), queued and HELD at
  its approval gate:** the scope audit for closing the two silently wrong answers the low-risk
  sweep registered rather than fixed — `RE-1` (`regexp_extract_all` defaults to capture group 0,
  Spark to 1) and `LOG-1` (the Spark door's `log` is base 10, Spark's is natural). Carries the
  measured implementation scope for both: RE-1's single default site and its three collateral test
  failures (two of which fail as runtime errors and appear in no other RE-1 document), LOG-1's need
  for a new dual-arity null-guarded kernel rather than a redirect to `ln`, the ratchet move that
  comes with it, and the two adjacent defects that should ride along. Both units change a computed
  answer, so the gate wants a dated owner ruling before either writes code.
  **Owner ruling 2026-08-31:** both rows fix to Spark. Delivery:
  [sem-1-spark-answer-parity-ledger.md](../archive/2026-09/2026-09-02-sem-1-spark-answer-parity-ledger.md).
- [v3-0-charter-ledger.md](v3-0-charter-ledger.md) —
  **V3-0 (2026-08-21):** the format-v3 scope audit, and the defect it found. Intended as a
  charter with no product change and it does not close that way. **Read §3 first**:
  `rewrite_data_files` had no format-version check and reassigned every row's lineage on a v3
  table while returning the correct rows, where Spark carries lineage through unchanged. It is
  reachable on a v3 table that was already in the catalog, which is the drop-in case, so the
  guard shipped with the audit (`V3-LINEAGE-1`). §2 is the other half of the news, and it is
  good: v3 reads and v3 appends are already correct, round-tripped through Spark, including the
  row lineage the format mandates. §4 answers A12's stated first question — adoption, through
  `register_table`, whose Spark signature is measured there.

- [ex-11-functions-hash-url-random-ledger.md](ex-11-functions-hash-url-random-ledger.md) —
- [ex-8-functions-arrays-ledger.md](ex-8-functions-arrays-ledger.md) —
- [ex-7-functions-datetime-b-ledger.md](ex-7-functions-datetime-b-ledger.md) —
- [ex-6-functions-datetime-a-ledger.md](ex-6-functions-datetime-a-ledger.md) —
  **EX-6 (2026-09-03), in flight:** the backfill's `F.*` datetime arithmetic and
  parts batch — 33 names covered by seven examples, the backlog ratchet −33
  (723 → 690 as merged); `F.add_months` (measured divergence on a negative
  offset from a month end, pinned as FN-ADDMONTHS-1) and `F.months_between`
  (refused, engine gap R-FN-BATCH1) stay on the backlog with both values
  recorded. `risk_tier: standard`. Branch `feat/ex-6-functions-datetime-a`. Slate:
  [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md).

## Pointers
- Up: [../map.md](../map.md)

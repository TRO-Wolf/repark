# map — task/ledgers/staging/

## Purpose
Ledgers of units in flight. A ledger here on `main` is a charter whose retirement event has not
happened yet; every other ledger leaves for `../completed/` in its unit's last commit.

## Contents
- [ex-2-functions-math-bitwise-ledger.md](ex-2-functions-math-bitwise-ledger.md) —
  **EX-2 (2026-09-01), in flight:** the v0.7 example backfill's `F.*` math +
  bitwise family — the campaign pilot. One clause per batch; batch 1 covers
  eleven roots / exponential / power / sign / rounding names and moves the
  backlog ratchet 892 → 881; the twelfth, `F.expm1`, is measured, reported and
  left on the backlog rather than taught by an example that omits its reason
  for existing. `risk_tier: standard`. Branch
  `feat/ex-2-functions-math-bitwise`. Slate:
  [../briefs/example-backfill.md](../../../briefs/example-backfill.md).
- [fnp-0-charter-ledger.md](fnp-0-charter-ledger.md) — **the Spark function parity campaign's
  scope audit and approval gate (2026-08-20):** the twelve-clause proposition ledger, the spike
  evidence behind it; C-007 (the four sub-project families) was closed by ruling D-7 on
  2026-08-20 and the gate passed. Design:
  [../docs/design/spark-function-parity.md](../../../docs/design/spark-function-parity.md); CAP-1
  appends a compatibility note that points its dated file-size premise at the live guards; slate:
  [../briefs/spark-function-parity.md](../../../briefs/spark-function-parity.md).
- [live-v3-aws-legs-ledger.md](live-v3-aws-legs-ledger.md) — **LIVE-v3 (2026-09-02):** the
  Glue and S3 Tables format-v3 acceptance legs. Four clauses: the shared `run_v3_acceptance`
  body and its asserter, the local pin that fixes the expected numbers, the two live legs with
  the `S3T-V3-1` decision table, and the document truth-up. §6 is the statement sequence with
  the measured local answers, §7 the mutation table (18 red of 18) and finding F-LIVEV3-1 (the
  MoR MERGE insert's `_row_id` is nondeterministic — recorded, not repaired), §8 the
  `gh workflow run` the orchestrator uses and the expected-outcome table. Moves to
  `../completed/` in this unit's last commit.
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

## Pointers
- Up: [../map.md](../map.md)

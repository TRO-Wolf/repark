# map — task/ledgers/staging/

## Purpose
Ledgers of units in flight. A ledger here on `main` is a charter whose retirement event has not
happened yet; every other ledger leaves for `../completed/` in its unit's last commit.

## Contents
- [fnp-0-charter-ledger.md](fnp-0-charter-ledger.md) — **the Spark function parity campaign's
  scope audit and approval gate (2026-08-20):** the twelve-clause proposition ledger, the spike
  evidence behind it; C-007 (the four sub-project families) was closed by ruling D-7 on
  2026-08-20 and the gate passed. Design:
  [../docs/design/spark-function-parity.md](../../../docs/design/spark-function-parity.md); slate:
  [../briefs/spark-function-parity.md](../../../briefs/spark-function-parity.md).
- [mw-5-campaign-close-ledger.md](mw-5-campaign-close-ledger.md) — **MW-5 (2026-08-23):**
  campaign close. Re-runs the MW-0 1,000-row / ten-MERGE demo, pins delete-file growth
  1→10 then compact+expire 10→1 with Arrow `COUNT(*)` 1,000 `int64`, records wall-clock
  in the ledger (not a CI timing pin), STATUS scorecard, guide lockstep. S3 Tables MOR
  stays out. Original charter registry rows were closed in MW-1/MW-2.
- [mw-0-charter-ledger.md](mw-0-charter-ledger.md) — **MW-0 (2026-08-21):** the charter for the
  Iceberg write-path maintenance wave, and the campaign's whole measured floor. Merge-on-read
  writes are correct and merge-on-read is not operable: ten sequential MERGEs grow delete files
  one per merge and never reclaim them, costing **2.1x on scan while the answer stays 1,000
  rows**. Procedure result schemas measured before any pin exists — two executed on a live
  oracle, two read from the shipping Iceberg jar's own constant because a Spark 4.0-to-4.1 binary
  break stops them executing. Gate **RULED**, no open clauses: the fence lifts for BOTH remote
  catalog policies. **Read §5 for the three claims that changed under re-verification** — an
  "undeclared divergence" that turned out to be declared, and a hazard this orchestrator
  overstated from a secondhand citation, which shrank MW-1 from building a mitigation to
  documenting a failure mode.
- [sem-0-charter-ledger.md](sem-0-charter-ledger.md) — **SEM-0 (2026-08-21), queued and HELD at
  its approval gate:** the scope audit for closing the two silently wrong answers the low-risk
  sweep registered rather than fixed — `RE-1` (`regexp_extract_all` defaults to capture group 0,
  Spark to 1) and `LOG-1` (the Spark door's `log` is base 10, Spark's is natural). Carries the
  measured implementation scope for both: RE-1's single default site and its three collateral test
  failures (two of which fail as runtime errors and appear in no other RE-1 document), LOG-1's need
  for a new dual-arity null-guarded kernel rather than a redirect to `ln`, the ratchet move that
  comes with it, and the two adjacent defects that should ride along. Both units change a computed
  answer, so the gate wants a dated owner ruling before either writes code.
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

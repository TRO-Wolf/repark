# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
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
- [mw-5-campaign-close-ledger.md](mw-5-campaign-close-ledger.md) — **MW-5 (2026-08-23):**
  campaign close. Re-runs the MW-0 1,000-row / ten-MERGE demo, pins delete-file growth
  1→10 then compact+expire 10→1 with Arrow `COUNT(*)` 1,000 `int64`, records wall-clock
  in the ledger (not a CI timing pin), STATUS scorecard, guide lockstep. S3 Tables MOR
  stays out. Original charter registry rows were closed in MW-1/MW-2.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"

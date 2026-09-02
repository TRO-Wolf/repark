# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [live-v3-aws-legs-ledger.md](live-v3-aws-legs-ledger.md) — **LIVE-v3 (2026-09-02):** the
  Glue and S3 Tables format-v3 acceptance legs. Four clauses: the shared `run_v3_acceptance`
  body and its asserter, the local pin that fixes the expected numbers, the two live legs with
  the `S3T-V3-1` decision table, and the document truth-up. §6 is the statement sequence with
  the measured local answers, §7 the mutation table (18 red of 18) and finding F-LIVEV3-1 (the
  MoR MERGE insert's `_row_id` is nondeterministic — recorded, not repaired), §8 the
  `gh workflow run` the orchestrator uses and the expected-outcome table. Moves to
  `../completed/` in this unit's last commit.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"

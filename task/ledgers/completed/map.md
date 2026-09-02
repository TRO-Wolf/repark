# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3-10-upgrade-v2-to-v3-ledger.md](v3-10-upgrade-v2-to-v3-ledger.md) — **V3-10 (2026-09-02):**
  the in-place v2 → v3 upgrade behind `repark.sql.allowCreateFormatVersion3`, Spark-equal on
  three doors; registry `V3-UPGRADE-1` FIXED, `V3-UPGRADE-V4-1` and `V3-UPGRADE-DV-1` DECLARED
  (the latter queued as unit V3-12).
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [live-v3-aws-legs-ledger.md](live-v3-aws-legs-ledger.md) — **LIVE-v3 (2026-09-02):** the
  Glue and S3 Tables format-v3 acceptance legs. Four clauses: the shared `run_v3_acceptance`
  body and its asserter, the local pin that fixes the expected numbers, the two live legs with
  the `S3T-V3-1` decision table, and the document truth-up. §6 is the statement sequence with
  the measured local answers, §7 the mutation table (18 red of 18) and finding F-LIVEV3-1 (the
  MoR MERGE insert's `_row_id` is nondeterministic — recorded, not repaired), §8 the
  `gh workflow run` the orchestrator uses and the expected-outcome table. Moves to
  `../completed/` in this unit's last commit.
- [live-v3-first-measurement-ledger.md](live-v3-first-measurement-ledger.md) —
  **LIVE-v3-M (2026-09-02):** the docs-only truth-up that records the first live
  measurement of the two v3 acceptance legs — `aws-acceptance` run 33635288918 on merged `main`
  `8c4bc55`, both legs green, S3 Tables accepting `format-version = 3` at CREATE and Glue
  reproducing the local numbers exactly. Three clauses: the two documents that own the answer
  (registry `S3T-V3-1` FIXED by measurement, north-star row ✅), the two that own the question
  (`docs/tier2-aws.md` §6, `docs/design/format-v3-track.md` §7), and STATUS plus the rewritten
  meta-pin. §1 is the run evidence, §3 the document → was → now table, §4 the mutation table
  (8 red of 8), and §5 the gate table. `risk_tier: standard`. Branch
  `docs/live-v3-first-measurement`.
- [v3-9-mor-predicate-dml-dv-ledger.md](v3-9-mor-predicate-dml-dv-ledger.md) — V3-9 — merge-on-read predicate DML on v3 writes deletion vectors (`V3-MOR-1` FIXED; residual `V3-DV-1` BACKLOG, fork F-18 / repin RP-7)

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"

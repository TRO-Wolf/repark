# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [rp-6-fork-repin-ledger.md](rp-6-fork-repin-ledger.md) — **RP-6 (2026-09-01), in flight:**
  fork repin `00cdde0` → `fb0cacfa` (PR-1..PR-7). Consume REPLACE added>deleted refuse,
  evolved-spec rewrite, Glue/S3 Tables commit seams, V3 MoR UPDATE lineage, branch MoR
  UPDATE, V2→V3 upgrade, PR-7 closeout. Re-measure and lift `V3-COW-1` where Spark-equal;
  F-7 preserve-half; RDF-1 honesty; evolved-spec Spark-door pin.
  `risk_tier: standard`. Branch `feat/rp-6-fork-repin`.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"

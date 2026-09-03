# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [api-freeze-ledger.md](api-freeze-ledger.md) — **API-FREEZE (2026-09-02), retiring in this
  unit's last commit:** the owner answered the v1.0 API review `R0 yes` with every row decided at
  its recommendation, so this unit records the decisions in the packet, writes the versioning
  policy into [../docs/release.md](../../../docs/release.md), and registers the 888 frozen names
  in [../docs/design/v1-0-api-freeze.json](../../../docs/design/v1-0-api-freeze.json) behind a
  parity pin that reds on a lost name or a moved required parameter and stays green on additions.
  `risk_tier: standard`. Branch `docs/v1-0-api-freeze`.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"

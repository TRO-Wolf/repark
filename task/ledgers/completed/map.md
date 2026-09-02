# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [scale-v3-mw7-ledger.md](scale-v3-mw7-ledger.md) — **SCALE-v3 (2026-09-02):** the MW-7
  `1e7 x 50` scale workload re-measured on format-v3 tables, 2:42:36 wall. The
  `--format-version` knob and six pins (v3 MoR writes file-scoped Puffin DVs, COW keeps
  `_row_id`, `rewrite_position_delete_files` refuses on live DVs and the refusal is recorded,
  one leg checked against live Spark); the run (0.24x the delete files, 0.64x the point probe,
  1.59x the merge cost, and a runbook that ends at zero delete files where v2 kept 10,000,000
  delete records); north star §3 "Scale" ✅. Findings F-SCALE-V3-1 and F-SCALE-V3-2 are S3
  disclosures. Branch `feat/scale-v3-mw7`.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"

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
- [scale-v3-mw7-ledger.md](scale-v3-mw7-ledger.md) — **SCALE-v3 (2026-09-02):** the MW-7
  `1e7 x 50` scale workload re-measured on format-v3 tables. The `--format-version` knob and
  nine pins (v3 MoR writes file-scoped Puffin DVs, COW keeps `_row_id`,
  `rewrite_position_delete_files` refuses on live DVs and only that refusal is recorded,
  `started_at` is the start, one leg checked against live Spark); the run — counts first
  (0.24x the delete files, 0.29x the data files, a runbook that ends at zero delete files
  where v2 kept 10,000,000 delete records), then the COW-controlled read cells (point p50
  0.64x on a cell whose control moved 1.00x), with every write-side ratio labelled cross-run
  and uncontrolled (the control moved 1.22x); north star §3 "Scale" ✅. F-SCALE-V3-1 FIXED and
  pinned; F-SCALE-V3-2 discharged by the guide's runbook section, which now states its format
  version before any number. Branch `feat/scale-v3-mw7`.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"

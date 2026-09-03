# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [api-review-packet-ledger.md](api-review-packet-ledger.md) —
  **API-REVIEW (2026-09-02):** the v1.0 API review packet, delivered and awaiting the owner's
  row-by-row answer — 35 rows, one per public surface, each with its pins, its measured example coverage, the divergence-registry rows inside
  it and a freeze recommendation the owner answers `yes` / `no` / `yes except <members>`. Rows
  A1–J9 partition the 913-name public inventory; K1–O1 carry the door surfaces, the seven `CALL`
  procedures, the conf keys, format-v3, packaging and the error taxonomy. `risk_tier: standard`.
  Branch `docs/v1-0-api-review`. Deliverable:
  [../../../docs/design/v1-0-api-review-2026-09-02.md](../../../docs/design/v1-0-api-review-2026-09-02.md).
- [rp-7-f18-repin-ledger.md](rp-7-f18-repin-ledger.md) — Charter ledger — RP-7 · fork repin fb0cacfa → ff4764d3 (consume F-18; close `V3-DV-1`)
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

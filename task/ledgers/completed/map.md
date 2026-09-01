# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [dfp-1-preserve-null-unnest-ledger.md](dfp-1-preserve-null-unnest-ledger.md) — **DFP-1
  (2026-08-31), implementation complete:** preserve-null Unnest removes redundant projections.
  The finite List/LargeList/FixedSizeList/Dictionary<List> matrix and plan-shape proof are green.
- [fnp-7-try-inversions-ledger.md](fnp-7-try-inversions-ledger.md) — **FNP-7a/7b (2026-08-31),
  merged #285:** the twelve `try_*` NULL-yielding inversions. Design §7 rows FNP-7a (8) and
  FNP-7b (4, unblocked by F-Y10-1). `risk_tier: standard`. Branch `feat/fnp-7-try-inversions`.
- [rp-4-fork-repin-ledger.md](rp-4-fork-repin-ledger.md) — **RP-4 (2026-08-31), merged
  #284:** fork repin `d408da42` → `33be9a0` (F-7 slice 1 consume, F-6 carry).
  Family frozen. Ledger born on `feat/rp-4-fork-repin`.
- [v3-6-v3-types-ledger.md](v3-6-v3-types-ledger.md) — **V3-6 (2026-08-31):** remaining v3
  types (binary variant, nanosecond timestamps, `unknown`, column defaults). C-001
  measurement matrix is in the ledger; product mapping follows per type.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"

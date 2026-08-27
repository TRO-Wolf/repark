# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [cap-1-source-file-line-cap-ledger.md](cap-1-source-file-line-cap-ledger.md) — **CAP-1
  (2026-08-26):** ratchet Rust and Python source files to the new source-line default with exact,
  no-slack baselines for the measured offenders; keep narrow line-neutral fixes legal and preserve
  the facade no-stub rule.
- [pr-247-revalidation-ledger.md](pr-247-revalidation-ledger.md) — **PR #247 revalidation
  (2026-08-27):** preserves the Anthropic-model owner ruling byte-for-byte after CAP-1, removes
  attribution-blind enforcement, and pins compatibility with required documentation, banners,
  invariant comments, maps, and gates. It retires when PR #247 merges or closes without merge.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"

# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [dl-4-live-doc-compaction-charter-ledger.md](dl-4-live-doc-compaction-charter-ledger.md) —
  **DL-4 (chartered 2026-08-25), sequenced ahead of V3E-4:** the live documents carry only
  live state. Measured: `STATUS.md` 65 kB with a 36 kB "Active workstreams" whose closed
  campaign diaries never left, `briefs/next-sequence.md` 26 kB of which ~5 kB is live. Design:
  HTML-comment block markers on both files, `ledger_lifecycle.py compact` (closed blocks move
  to `docs/history/<campaign>/`, merged units leave the slate with no obituary), a
  `check-docs-compaction` gate with a byte ratchet, one migration, and the rule text. Eight
  clauses, all `OPEN` until the unit runs.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"

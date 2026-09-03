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
- [v3-11-row-id-determinism-ledger.md](v3-11-row-id-determinism-ledger.md) — **V3-11
  (2026-09-02):** deterministic same-commit data-file order. Closes `V3-ROWID-3` (the
  merge-on-read MERGE insert's `_row_id`, 10 of 10 at Spark's value where it flapped 10/11) and
  files `V3-FILEORDER-1` — the engine orders one commit's files by ascending partition value
  where Spark uses a Java `HashMap` bucket index, so the two agree only on collision-free
  monotonic partition sets. Its remediation round also retired the "the 4.1.2 oracle cannot
  execute maintenance procedures" note six registry rows carried, re-measuring each.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"

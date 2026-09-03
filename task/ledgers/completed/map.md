# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [log1p-1-precise-kernels-ledger.md](log1p-1-precise-kernels-ledger.md) —
  **LOG1P-1 (2026-09-02):** `F.log1p` / `F.expm1` and both SQL doors call
  `f64::ln_1p` / `f64::exp_m1`. `risk_tier: standard`. Branch
  `fix/log1p-expm1-precision`.
- [api-freeze-ledger.md](api-freeze-ledger.md) — **API-FREEZE (2026-09-02), retiring in this
  unit's last commit:** the owner answered the v1.0 API review `R0 yes` with every row decided at
  its recommendation, so this unit records the decisions in the packet, writes the versioning
  policy into [../docs/release.md](../../../docs/release.md), and registers the 888 frozen names
  in [../docs/design/v1-0-api-freeze.json](../../../docs/design/v1-0-api-freeze.json) behind a
  parity pin that reds on a lost name or a moved required parameter and stays green on additions.
  `risk_tier: standard`. Branch `docs/v1-0-api-freeze`.
  **API-REVIEW (2026-09-02):** the v1.0 API review packet, delivered and awaiting the owner's
  row-by-row answer — 35 rows, one per public surface, each with its pins, its measured example coverage, the divergence-registry rows inside
  it and a freeze recommendation the owner answers `yes` / `no` / `yes except <members>`. Rows
  A1–J9 partition the 913-name public inventory; K1–O1 carry the door surfaces, the seven `CALL`
  procedures, the conf keys, format-v3, packaging and the error taxonomy. `risk_tier: standard`.
  Branch `docs/v1-0-api-review`. Deliverable:
  [../../../docs/design/v1-0-api-review-2026-09-02.md](../../../docs/design/v1-0-api-review-2026-09-02.md).
  (2026-09-02):** deterministic same-commit data-file order. Closes `V3-ROWID-3` (the
  merge-on-read MERGE insert's `_row_id`, 10 of 10 at Spark's value where it flapped 10/11) and
  files `V3-FILEORDER-1` — the engine orders one commit's files by ascending partition value
  where Spark uses a Java `HashMap` bucket index, so the two agree only on collision-free
  monotonic partition sets. Its remediation round also retired the "the 4.1.2 oracle cannot
  execute maintenance procedures" note six registry rows carried, re-measuring each.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"

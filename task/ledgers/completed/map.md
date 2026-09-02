# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [ex-0-example-drift-gate-ledger.md](ex-0-example-drift-gate-ledger.md) —
  **EX-0 (2026-08-31), in flight:** v0.7 example drift gate + public-surface
  inventory. `risk_tier: standard`. Branch `feat/ex-0-example-drift-gate`.
  Ruling: [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md)
  §v0.7 deliverable 2.
- [ex-1-class-surfaces-ledger.md](ex-1-class-surfaces-ledger.md) —
  **EX-1 (2026-08-31), in flight:** widens the EX-0 example-coverage inventory
  with the class surfaces the owner ruled into v0.7 — Column, Window,
  WindowSpec, Catalog, the `types` module surface, `ml`, and Row (150 names,
  763 → 913). `risk_tier: standard`. Branch `feat/ex-1-class-surfaces`, stacked
  on `feat/ex-0-example-drift-gate`.
- [ref-branch-tag-wap-ledger.md](ref-branch-tag-wap-ledger.md) — **REF (2026-09-01), in flight:**
  Iceberg branch / tag operations + write-audit-publish. The C-001 matrix measures every
  reachable ref door at fork pin `33be9a0` against live PySpark 4.1.2 + Iceberg 1.11.0 and
  moves the write-to-branch gap: F-6 gave `to_branch` to the transaction actions, not to the
  `iceberg-datafusion` write path `INSERT`/`UPDATE`/`DELETE` execute through.
  `risk_tier: standard`. Branch `feat/ref-branch-tag-wap`. **2026-09-01 delivered, parks in
  staging until the owner merges:** C-001..C-006 PROVEN — the `branch_`/`tag_` read selectors
  resolve (registry `REF-4` FIXED), both `WITH SNAPSHOT RETENTION` halves land on both doors,
  the write leg and WAP stay DECLARED with re-measured reasons (`REF-1`, `REF-3`) and the
  restated fork ask filed as F-6b.
- [sem-1-spark-answer-parity-ledger.md](sem-1-spark-answer-parity-ledger.md) — **SEM-1 (2026-08-31):**
  close RE-1 and LOG-1 to Spark semantics under the dated owner ruling. RE-1's default is already
  group 1 on this tree (prior SEM-1, PR #193); this unit re-measures it and builds the dual-arity
  null-guarded Spark `log` kernel (charter SEM-2), the `EXPECTED_DIVERGENCES` ratchet, and `F.log`'s
  two-argument form.
- [v3-5-dv-compaction-ledger.md](v3-5-dv-compaction-ledger.md) — **V3-5 (2026-08-31),
  in flight:** DV-aware v3 compaction (`V3-DANGLE-1`, B-MOR-3 residue, true
  result counts). Measure `rewrite_data_files` on live Puffin DVs at fork
  `33be9a0` before any product edit. Ledger born on `feat/v3-5-dv-compaction`.
- [v3-6-v3-types-ledger.md](v3-6-v3-types-ledger.md) — **V3-6 (2026-08-31; 2026-09-01
  delivered, parks in staging until the owner merges):** remaining v3 types (binary
  variant, nanosecond timestamps, `unknown`, column defaults). C-001 measurement matrix
  in the ledger; C-002..C-007 PROVEN — ns timestamps consumed, column defaults
  consumed on write/read with Spark-equal DEFAULT refusals, variant/unknown refusals
  pinned, registry row landed, upgrade surface untouched.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"

# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [rdf-1-position-delete-bounds-ledger.md](rdf-1-position-delete-bounds-ledger.md) — Charter ledger — RDF-1 · position-delete `file_path` bounds, re-homed from fork ask F-16
- [docs-1-truth-up-ledger.md](docs-1-truth-up-ledger.md) — **DOCS-1 (2026-09-02):**
  the truth-up after the 2026-09-01/02 merges — STATUS, the v1.0 north star, the fork
  handoff and the ledger bins reconciled to what merged. `risk_tier: standard`. Branch
  `docs/truth-up-2026-09-02`.
- [ex-0-example-drift-gate-ledger.md](ex-0-example-drift-gate-ledger.md) — **EX-0, merged 2026-08-31 ([#292](https://github.com/TRO-Wolf/repark/pull/292)):** the v0.7 example drift gate and the public-surface inventory.
- [ex-1-class-surfaces-ledger.md](ex-1-class-surfaces-ledger.md) — **EX-1, merged 2026-09-01 ([#296](https://github.com/TRO-Wolf/repark/pull/296)):** the inventory widens to the class surfaces — Column, Window, WindowSpec, Catalog, `types`, `ml`, Row (150 names, 763 → 913).
- [ref-branch-tag-wap-ledger.md](ref-branch-tag-wap-ledger.md) — **REF, merged 2026-09-01 ([#298](https://github.com/TRO-Wolf/repark/pull/298)):** branch / tag read selectors resolve (`REF-4` FIXED) and both `WITH SNAPSHOT RETENTION` halves land; the write leg lifted at RP-5 (`REF-1` FIXED) and WAP stays BACKLOG (`REF-3`).
- [sem-1-spark-answer-parity-ledger.md](sem-1-spark-answer-parity-ledger.md) — **SEM-1, merged 2026-09-01 ([#295](https://github.com/TRO-Wolf/repark/pull/295)):** `LOG-1` closed to Spark's natural `log` (dual-arity null guard, two-argument `F.log`); `RE-1` re-measured and recorded.
- [v3-5-dv-compaction-ledger.md](v3-5-dv-compaction-ledger.md) — **V3-5, merged 2026-08-31 ([#291](https://github.com/TRO-Wolf/repark/pull/291)):** DV-aware v3 compaction — `V3-DANGLE-1` FIXED, true result counts, B-MOR-3 residue recorded.
- [v3-6-v3-types-ledger.md](v3-6-v3-types-ledger.md) — **V3-6, merged 2026-09-01 ([#297](https://github.com/TRO-Wolf/repark/pull/297)):** the remaining v3 types at their measured state — ns timestamps and column defaults consumed, `variant` / `unknown` refusals pinned.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"

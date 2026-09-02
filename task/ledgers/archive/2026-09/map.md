# map — task/ledgers/archive/2026-09/

## Purpose
Ledgers archived in 2026-09; immutable — corrections are dated errata at the top.
One line per ledger, and off the normal read path: grep this directory for a unit; do not read this file whole.

## Contents
- [2026-09-01-rp-5-fork-repin-ledger.md](2026-09-01-rp-5-fork-repin-ledger.md) — **RP-5 (2026-09-01), complete:** fork pin `00cdde0`.
- [2026-09-02-docs-1-truth-up-ledger.md](2026-09-02-docs-1-truth-up-ledger.md) — **DOCS-1 (2026-09-02):** the truth-up after the 2026-09-01/02 merges — STATUS, the v1.0 north star, the fork handoff and the ledger bins reconciled to what merged.
- [2026-09-02-ex-0-example-drift-gate-ledger.md](2026-09-02-ex-0-example-drift-gate-ledger.md) — **EX-0, merged 2026-08-31 ([#292](https://github.com/TRO-Wolf/repark/pull/292)):** the v0.7 example drift gate and the public-surface inventory.
- [2026-09-02-ex-1-class-surfaces-ledger.md](2026-09-02-ex-1-class-surfaces-ledger.md) — **EX-1, merged 2026-09-01 ([#296](https://github.com/TRO-Wolf/repark/pull/296)):** the inventory widens to the class surfaces — Column, Window, WindowSpec, Catalog, `types`, `ml`, Row (150 names, 763 → 913).
- [2026-09-02-live-v3-aws-legs-ledger.md](2026-09-02-live-v3-aws-legs-ledger.md) — **LIVE-v3 (2026-09-02):** the Glue and S3 Tables format-v3 acceptance legs.
- [2026-09-02-live-v3-first-measurement-ledger.md](2026-09-02-live-v3-first-measurement-ledger.md) — **LIVE-v3-M (2026-09-02):** the docs-only truth-up that records the first live measurement of the two v3 acceptance legs — `aws-acceptance` run 33635288918 on merged `main` `8c4bc55`, both legs green, S3 Tables accepting `format-version = 3` at CREATE and Glue reproducing the local numbers exactly.
- [2026-09-02-rdf-1-position-delete-bounds-ledger.md](2026-09-02-rdf-1-position-delete-bounds-ledger.md) — Charter ledger — RDF-1 · position-delete `file_path` bounds, re-homed from fork ask F-16
- [2026-09-02-ref-branch-tag-wap-ledger.md](2026-09-02-ref-branch-tag-wap-ledger.md) — **REF, merged 2026-09-01 ([#298](https://github.com/TRO-Wolf/repark/pull/298)):** branch / tag read selectors resolve (`REF-4` FIXED) and both `WITH SNAPSHOT RETENTION` halves land; the write leg lifted at RP-5 (`REF-1` FIXED) and WAP stays BACKLOG (`REF-3`).
- [2026-09-02-rp-6-fork-repin-ledger.md](2026-09-02-rp-6-fork-repin-ledger.md) — **RP-6 (2026-09-01), completed:** fork repin `00cdde0` → `fb0cacfa` (PR-1..PR-7).
- [2026-09-02-sem-1-spark-answer-parity-ledger.md](2026-09-02-sem-1-spark-answer-parity-ledger.md) — **SEM-1, merged 2026-09-01 ([#295](https://github.com/TRO-Wolf/repark/pull/295)):** `LOG-1` closed to Spark's natural `log` (dual-arity null guard, two-argument `F.log`); `RE-1` re-measured and recorded.
- [2026-09-02-v3-5-dv-compaction-ledger.md](2026-09-02-v3-5-dv-compaction-ledger.md) — **V3-5, merged 2026-08-31 ([#291](https://github.com/TRO-Wolf/repark/pull/291)):** DV-aware v3 compaction — `V3-DANGLE-1` FIXED, true result counts, B-MOR-3 residue recorded.
- [2026-09-02-v3-6-v3-types-ledger.md](2026-09-02-v3-6-v3-types-ledger.md) — **V3-6, merged 2026-09-01 ([#297](https://github.com/TRO-Wolf/repark/pull/297)):** the remaining v3 types at their measured state — ns timestamps and column defaults consumed, `variant` / `unknown` refusals pinned.
- [2026-09-02-v3-7-merge-lineage-ledger.md](2026-09-02-v3-7-merge-lineage-ledger.md) — **V3-7 (2026-09-02), completed:** carry `_row_id` through the RePark-owned MERGE writer; lift `V3-COW-1` MERGE where Spark-equal.
- [2026-09-02-v3-8-subquery-where-lineage-ledger.md](2026-09-02-v3-8-subquery-where-lineage-ledger.md) — V3-8 — subquery-`WHERE` COW DML keeps v3 row lineage; `V3-COW-1` FIXED
- [2026-09-02-v3-9-mor-predicate-dml-dv-ledger.md](2026-09-02-v3-9-mor-predicate-dml-dv-ledger.md) — V3-9 — merge-on-read predicate DML on v3 writes deletion vectors (`V3-MOR-1` FIXED; residual `V3-DV-1` BACKLOG, fork F-18 / repin RP-7)

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../../AGENTS.md](../../../../AGENTS.md) "Markdown document lifecycle"

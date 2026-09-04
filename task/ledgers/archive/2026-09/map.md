# map — task/ledgers/archive/2026-09/

## Purpose
Ledgers archived in 2026-09; immutable — corrections are dated errata at the top.
One line per ledger, and off the normal read path: grep this directory for a unit; do not read this file whole.

## Contents
- [2026-09-01-rp-5-fork-repin-ledger.md](2026-09-01-rp-5-fork-repin-ledger.md) — **RP-5 (2026-09-01), complete:** fork pin `00cdde0`.
- [2026-09-02-api-freeze-ledger.md](2026-09-02-api-freeze-ledger.md) — **API-FREEZE (2026-09-02), retiring in this unit's last commit:** the owner answered the v1.0 API review `R0 yes` with every row decided at its recommendation, so this unit records the decisions in the packet, writes the versioning policy into [../docs/release.md](../../../../docs/release.md), and registers the 888 frozen names in [../docs/design/v1-0-api-freeze.json](../../../../docs/design/v1-0-api-freeze.json) behind a parity pin that reds on a lost name or a moved required parameter and stays green on additions.
- [2026-09-02-api-review-packet-ledger.md](2026-09-02-api-review-packet-ledger.md) — **API-REVIEW (2026-09-02):** the v1.0 API review packet, delivered and awaiting the owner's row-by-row answer — 35 rows, one per public surface, each with its pins, its measured example coverage, the divergence-registry rows inside it and a freeze recommendation the owner answers `yes` / `no` / `yes except <members>`.
- [2026-09-02-docs-1-truth-up-ledger.md](2026-09-02-docs-1-truth-up-ledger.md) — **DOCS-1 (2026-09-02):** the truth-up after the 2026-09-01/02 merges — STATUS, the v1.0 north star, the fork handoff and the ledger bins reconciled to what merged.
- [2026-09-02-ex-0-example-drift-gate-ledger.md](2026-09-02-ex-0-example-drift-gate-ledger.md) — **EX-0, merged 2026-08-31 ([#292](https://github.com/TRO-Wolf/repark/pull/292)):** the v0.7 example drift gate and the public-surface inventory.
- [2026-09-02-ex-1-class-surfaces-ledger.md](2026-09-02-ex-1-class-surfaces-ledger.md) — **EX-1, merged 2026-09-01 ([#296](https://github.com/TRO-Wolf/repark/pull/296)):** the inventory widens to the class surfaces — Column, Window, WindowSpec, Catalog, `types`, `ml`, Row (150 names, 763 → 913).
- [2026-09-02-live-v3-aws-legs-ledger.md](2026-09-02-live-v3-aws-legs-ledger.md) — **LIVE-v3 (2026-09-02):** the Glue and S3 Tables format-v3 acceptance legs.
- [2026-09-02-live-v3-first-measurement-ledger.md](2026-09-02-live-v3-first-measurement-ledger.md) — **LIVE-v3-M (2026-09-02):** the docs-only truth-up that records the first live measurement of the two v3 acceptance legs — `aws-acceptance` run 33635288918 on merged `main` `8c4bc55`, both legs green, S3 Tables accepting `format-version = 3` at CREATE and Glue reproducing the local numbers exactly.
- [2026-09-02-log1p-1-precise-kernels-ledger.md](2026-09-02-log1p-1-precise-kernels-ledger.md) — **LOG1P-1 (2026-09-02):** `F.log1p` / `F.expm1` and both SQL doors call `f64::ln_1p` / `f64::exp_m1`.
- [2026-09-02-rdf-1-position-delete-bounds-ledger.md](2026-09-02-rdf-1-position-delete-bounds-ledger.md) — Charter ledger — RDF-1 · position-delete `file_path` bounds, re-homed from fork ask F-16
- [2026-09-02-ref-branch-tag-wap-ledger.md](2026-09-02-ref-branch-tag-wap-ledger.md) — **REF, merged 2026-09-01 ([#298](https://github.com/TRO-Wolf/repark/pull/298)):** branch / tag read selectors resolve (`REF-4` FIXED) and both `WITH SNAPSHOT RETENTION` halves land; the write leg lifted at RP-5 (`REF-1` FIXED) and WAP stays BACKLOG (`REF-3`).
- [2026-09-02-rp-6-fork-repin-ledger.md](2026-09-02-rp-6-fork-repin-ledger.md) — **RP-6 (2026-09-01), completed:** fork repin `00cdde0` → `fb0cacfa` (PR-1..PR-7).
- [2026-09-02-rp-7-f18-repin-ledger.md](2026-09-02-rp-7-f18-repin-ledger.md) — Charter ledger — RP-7 · fork repin fb0cacfa → ff4764d3 (consume F-18; close `V3-DV-1`)
- [2026-09-02-scale-v3-mw7-ledger.md](2026-09-02-scale-v3-mw7-ledger.md) — **SCALE-v3 (2026-09-02):** the MW-7 `1e7 x 50` scale workload re-measured on format-v3 tables.
- [2026-09-02-sem-1-spark-answer-parity-ledger.md](2026-09-02-sem-1-spark-answer-parity-ledger.md) — **SEM-1, merged 2026-09-01 ([#295](https://github.com/TRO-Wolf/repark/pull/295)):** `LOG-1` closed to Spark's natural `log` (dual-arity null guard, two-argument `F.log`); `RE-1` re-measured and recorded.
- [2026-09-02-v3-10-upgrade-v2-to-v3-ledger.md](2026-09-02-v3-10-upgrade-v2-to-v3-ledger.md) — **V3-10 (2026-09-02):** the in-place v2 → v3 upgrade behind `repark.sql.allowCreateFormatVersion3`, Spark-equal on three doors; registry `V3-UPGRADE-1` FIXED, `V3-UPGRADE-V4-1` and `V3-UPGRADE-DV-1` DECLARED (the latter queued as unit V3-12).
- [2026-09-02-v3-11-row-id-determinism-ledger.md](2026-09-02-v3-11-row-id-determinism-ledger.md) — **V3-11 (2026-09-02):** deterministic same-commit data-file order.
- [2026-09-02-v3-5-dv-compaction-ledger.md](2026-09-02-v3-5-dv-compaction-ledger.md) — **V3-5, merged 2026-08-31 ([#291](https://github.com/TRO-Wolf/repark/pull/291)):** DV-aware v3 compaction — `V3-DANGLE-1` FIXED, true result counts, B-MOR-3 residue recorded.
- [2026-09-02-v3-6-v3-types-ledger.md](2026-09-02-v3-6-v3-types-ledger.md) — **V3-6, merged 2026-09-01 ([#297](https://github.com/TRO-Wolf/repark/pull/297)):** the remaining v3 types at their measured state — ns timestamps and column defaults consumed, `variant` / `unknown` refusals pinned.
- [2026-09-02-v3-7-merge-lineage-ledger.md](2026-09-02-v3-7-merge-lineage-ledger.md) — **V3-7 (2026-09-02), completed:** carry `_row_id` through the RePark-owned MERGE writer; lift `V3-COW-1` MERGE where Spark-equal.
- [2026-09-02-v3-8-subquery-where-lineage-ledger.md](2026-09-02-v3-8-subquery-where-lineage-ledger.md) — V3-8 — subquery-`WHERE` COW DML keeps v3 row lineage; `V3-COW-1` FIXED
- [2026-09-02-v3-9-mor-predicate-dml-dv-ledger.md](2026-09-02-v3-9-mor-predicate-dml-dv-ledger.md) — V3-9 — merge-on-read predicate DML on v3 writes deletion vectors (`V3-MOR-1` FIXED; residual `V3-DV-1` BACKLOG, fork F-18 / repin RP-7)
- [2026-09-03-b-mor-3-rewrite-position-deletes-v3-ledger.md](2026-09-03-b-mor-3-rewrite-position-deletes-v3-ledger.md) — **B-MOR-3 (2026-09-03), delivered:** owner ruling BUILD — `rewrite_position_delete_files` returns Spark's four zeros on a DV-only v3 table and converts an admitted parquet group to one PUFFIN per data file.
- [2026-09-03-rp-8-repin-f21-f22-ledger.md](2026-09-03-rp-8-repin-f21-f22-ledger.md) — **RP-8 (2026-09-03), delivered:** the fork repin `ff4764d3` → `c1d6c9de`, consuming F-19/F-20 (`#261`), F-21 (`#262`) and F-22 (`#263`).
- [2026-09-03-v1-gate-audit-ledger.md](2026-09-03-v1-gate-audit-ledger.md) — **V1-GATE (2026-09-03), in flight:** the v1.0 north-star gate statement.
- [2026-09-03-v3-12-legacy-delete-merge-ledger.md](2026-09-03-v3-12-legacy-delete-merge-ledger.md) — Charter ledger — V3-12 · merge a legacy parquet position delete into the deletion vector
- [2026-09-03-v3-cov-statement-coverage-ledger.md](2026-09-03-v3-cov-statement-coverage-ledger.md) — **V3-COV (2026-09-03), delivered:** the full v3 statement-coverage comparison against PySpark that discharges the north star's §2 pillar 4 — 81 programs, 267 cells, 71 EQUAL, 9 rows filed, 2 defects FIXED red-first.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../../AGENTS.md](../../../../AGENTS.md) "Markdown document lifecycle"

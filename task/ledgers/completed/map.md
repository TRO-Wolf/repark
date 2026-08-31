# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [dml-a-merge-not-matched-by-source-ledger.md](dml-a-merge-not-matched-by-source-ledger.md) —
  **DML-A (2026-08-30):** `MERGE … WHEN NOT MATCHED BY SOURCE` (DELETE and UPDATE, COW and
  MOR). HIGH / `risk_tier: high`. Eight clauses, 8× **PROVEN**. Live PySpark 4.1.2 oracle
  matrix is in the ledger §4.
- [dml-b-insert-overwrite-ledger.md](dml-b-insert-overwrite-ledger.md) — **DML-B (2026-08-30),
  v0.6 merge-order 1 of 4:** static `INSERT OVERWRITE … PARTITION (k=v)` via
  `overwrite_files` / `overwrite_by_row_filter`, dynamic and
  `writeTo().overwritePartitions()` via `replace_partitions`, empty-input dynamic
  guard, the two named acceptance pins flipped. Six PROVEN clauses.
- [dml-c-truncate-ledger.md](dml-c-truncate-ledger.md) — **DML-C (2026-08-30),
  chartered on `feat/dml-c-truncate`:** `TRUNCATE TABLE` as a first-class statement on
  both SQL doors and the facade. `risk_tier: high` (irreversible table wipe). Eight
  PROVEN clauses. Critic residual (2026-08-30): class-token error pins, full wipe
  summary keys, native missing-table `AnalysisException`, IF EXISTS parse refuse.
  Owner-pre-authorized with the v0.6 plan. Card:
  [../../../task/roadmap/epic-term/roadmap-design-plan-2026-08-29.md](../../roadmap/epic-term/roadmap-design-plan-2026-08-29.md)
  DML-C. Registry today: [DML-2](../../../docs/spark-sql-iceberg-parity.md).
- [f-y10-1-int-overflow-ledger.md](f-y10-1-int-overflow-ledger.md) — **F-Y10-1 (2026-08-30), PROVEN 5/5:**
  checked integer arithmetic raises where Spark raises on typed INT/BIGINT operands (ANSI knob,
  DEC U5 shape); untyped literal arithmetic keeps the intended Int64 literal-width split
  (Critic F-1, Option A). Names preserved, AnsiDialect installs at session build, matrix cells
  pinned; SMALLINT wrap is a dated residue. Unblocks FNP-7b.
- [fnp-15-16-ledger.md](fnp-15-16-ledger.md) — **FNP-15/16 (2026-08-30), PROVEN 17/17:** the six
  unreachable names and the four D-7 families (56 names, independently re-counted) are loud
  refusing surfaces on every door; registry §9 keeps unreachable vs deferred-by-cost distinct.
  Critic F-1..F-6 + O-1 all remediated (crate ANSI-door roster pin, per-family strip-check).
- [fnp-4c-higher-order-kernels-ledger.md](fnp-4c-higher-order-kernels-ledger.md) — **FNP-4c
  (2026-08-31):** the eight new higher-order kernels plus `forall` and `reduce`. Builds on
  the FNP-4a seam. Design §3.5 / §7 row FNP-4c.
- [maint-rewrite-data-files-options-ledger.md](maint-rewrite-data-files-options-ledger.md) —
  **rewrite_data_files options (2026-08-31), v0.6 merge order 4 of 4:** `where`, `sort_order`,
  and `strategy` on v2 tables. Fork `d408da42` honors `filter(Predicate)` and binpack only;
  sort is a loud fork-ceiling refusal. v3 lineage pins stay. risk_tier: high.
- [mw-10-s3tables-mor-ledger.md](mw-10-s3tables-mor-ledger.md) — **MW-10 (2026-08-28 →
  2026-08-30), PROVEN 6/6:** the S3 Tables merge-on-read leg the intake called "MW-4b",
  measure-first on OD-3b. The first owner dispatch (run 33333274383, on merged `main`) answered
  `PutTableData` **allow**; no denial registry row; docs and roadmap slots filled.
- [v3-3-dml-ledger.md](v3-3-dml-ledger.md) — **V3-3 (2026-08-30), chartered from RP-3 C-004
  red cells:** v3 `UPDATE` and `MERGE` stay a pre-write `V3-COW-1` keep-refusal (Spark
  preserves `_row_id`; the engine rewrite reassigns). Sequential COW DELETE lineage
  (F-rp3-c7) stays a fork finding. Three PROVEN clauses.
- [v3-4-serve-lineage-columns-ledger.md](v3-4-serve-lineage-columns-ledger.md) —
  **V3-4 (2026-08-31), read half:** serve `_row_id` and `_last_updated_sequence_number` on
  v3 reads, Spark-equal, on all three doors. Preserve-half (COW DML lineage) stays behind
  fork F-7; V3-COW-1 keep-refusal pins stay byte-untouched. Measure-first (C-001) before
  any engine edit.
- [w-0-window-bench-ledger.md](w-0-window-bench-ledger.md) — **W-0 (2026-08-31),
  PROVEN 11/11:** window-shape measurement (Track A opener). Bench plus filed
  numbers and thirteen `WIN-SLIDE-*` registry rows. No product change.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"

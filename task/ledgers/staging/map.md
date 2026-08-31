# map — task/ledgers/staging/

## Purpose
Ledgers of units in flight. A ledger here on `main` is a charter whose retirement event has not
happened yet; every other ledger leaves for `../completed/` in its unit's last commit.

## Contents
- [fnp-0-charter-ledger.md](fnp-0-charter-ledger.md) — **the Spark function parity campaign's
  scope audit and approval gate (2026-08-20):** the twelve-clause proposition ledger, the spike
  evidence behind it; C-007 (the four sub-project families) was closed by ruling D-7 on
  2026-08-20 and the gate passed. Design:
  [../docs/design/spark-function-parity.md](../../../docs/design/spark-function-parity.md); CAP-1
  appends a compatibility note that points its dated file-size premise at the live guards; slate:
  [../briefs/spark-function-parity.md](../../../briefs/spark-function-parity.md).
- [fnp-4c-higher-order-kernels-ledger.md](fnp-4c-higher-order-kernels-ledger.md) — **FNP-4c
  (2026-08-31):** the eight new higher-order kernels plus `forall` and `reduce`. Builds on
  the FNP-4a seam. Design §3.5 / §7 row FNP-4c.
- [f-y10-1-int-overflow-ledger.md](f-y10-1-int-overflow-ledger.md) — **F-Y10-1 (2026-08-30),
  chartered and HELD at its approval gate:** integer arithmetic overflow raises where Spark
  raises (the integer analog of the FIXED DEC-6, on the DEC U5 checked-kernel pattern). Five
  OPEN clauses; C-001 is the measurement that reconciles the recorded wrap-vs-widen
  contradiction before any edit. Unblocks FNP-7b's four `try_*` names.
- [sem-0-charter-ledger.md](sem-0-charter-ledger.md) — **SEM-0 (2026-08-21), queued and HELD at
  its approval gate:** the scope audit for closing the two silently wrong answers the low-risk
  sweep registered rather than fixed — `RE-1` (`regexp_extract_all` defaults to capture group 0,
  Spark to 1) and `LOG-1` (the Spark door's `log` is base 10, Spark's is natural). Carries the
  measured implementation scope for both: RE-1's single default site and its three collateral test
  failures (two of which fail as runtime errors and appear in no other RE-1 document), LOG-1's need
  for a new dual-arity null-guarded kernel rather than a redirect to `ln`, the ratchet move that
  comes with it, and the two adjacent defects that should ride along. Both units change a computed
  answer, so the gate wants a dated owner ruling before either writes code.
- [dml-c-truncate-ledger.md](dml-c-truncate-ledger.md) — **DML-C (2026-08-30),
  chartered on `feat/dml-c-truncate`:** `TRUNCATE TABLE` as a first-class statement on
  both SQL doors and the facade. `risk_tier: high` (irreversible table wipe). Eight
  PROVEN clauses. Critic residual (2026-08-30): class-token error pins, full wipe
  summary keys, native missing-table `AnalysisException`, IF EXISTS parse refuse.
  Owner-pre-authorized with the v0.6 plan. Card:
  [../../../task/roadmap/epic-term/roadmap-design-plan-2026-08-29.md](../../../task/roadmap/epic-term/roadmap-design-plan-2026-08-29.md)
  DML-C. Registry today: [DML-2](../../../docs/spark-sql-iceberg-parity.md).
- [v3-0-charter-ledger.md](v3-0-charter-ledger.md) —
  **V3-0 (2026-08-21):** the format-v3 scope audit, and the defect it found. Intended as a
  charter with no product change and it does not close that way. **Read §3 first**:
  `rewrite_data_files` had no format-version check and reassigned every row's lineage on a v3
  table while returning the correct rows, where Spark carries lineage through unchanged. It is
  reachable on a v3 table that was already in the catalog, which is the drop-in case, so the
  guard shipped with the audit (`V3-LINEAGE-1`). §2 is the other half of the news, and it is
  good: v3 reads and v3 appends are already correct, round-tripped through Spark, including the
  row lineage the format mandates. §4 answers A12's stated first question — adoption, through
  `register_table`, whose Spark signature is measured there.
- [dml-b-insert-overwrite-ledger.md](dml-b-insert-overwrite-ledger.md) — **DML-B (2026-08-30),
  v0.6 merge-order 1 of 4:** static `INSERT OVERWRITE … PARTITION (k=v)` via
  `overwrite_files` / `overwrite_by_row_filter`, dynamic and
  `writeTo().overwritePartitions()` via `replace_partitions`, empty-input dynamic
  guard, the two named acceptance pins flipped. Six PROVEN clauses.
- [dml-a-merge-not-matched-by-source-ledger.md](dml-a-merge-not-matched-by-source-ledger.md) —
  **DML-A (2026-08-30):** `MERGE … WHEN NOT MATCHED BY SOURCE` (DELETE and UPDATE, COW and
  MOR). HIGH / `risk_tier: high`. Eight clauses, 8× **PROVEN**. Live PySpark 4.1.2 oracle
  matrix is in the ledger §4.
- [maint-rewrite-data-files-options-ledger.md](maint-rewrite-data-files-options-ledger.md) —
  **rewrite_data_files options (2026-08-31), v0.6 merge order 4 of 4:** `where`, `sort_order`,
  and `strategy` on v2 tables. Fork `d408da42` honors `filter(Predicate)` and binpack only;
  sort is a loud fork-ceiling refusal. v3 lineage pins stay. risk_tier: high.
- [w-0-window-bench-ledger.md](w-0-window-bench-ledger.md) — **W-0 (2026-08-31),
  PROVEN 11/11:** window-shape measurement (Track A opener). Bench plus filed
  numbers and thirteen `WIN-SLIDE-*` registry rows. No product change.

## Pointers
- Up: [../map.md](../map.md)

# map — task/ledgers/staging/

## Purpose
Ledgers of units in flight. A ledger here on `main` is a charter whose retirement event has not
happened yet; every other ledger leaves for `../completed/` in its unit's last commit.

## Contents
- [fn-fix-2-string-rows-ledger.md](fn-fix-2-string-rows-ledger.md) — **FN-FIX-2 (2026-09-04):**
  six silent string rows become Spark-equal (`FN-INITCAP-1`, `FN-CHR-1`,
  `FN-TRIM-CHARS-1`, `FN-ELT-1`, `FN-REGEX-POSIX-1`, `FN-LIKE-ESCEND-1`).
  pins: fn-fix-2-string-rows/C-001, C-002, C-003, C-004
- [fnp-0-charter-ledger.md](fnp-0-charter-ledger.md) — **the Spark function parity campaign's
  scope audit and approval gate (2026-08-20):** the twelve-clause proposition ledger, the spike
  evidence behind it; C-007 (the four sub-project families) was closed by ruling D-7 on
  2026-08-20 and the gate passed. Design:
  [../docs/design/spark-function-parity.md](../../../docs/design/spark-function-parity.md); CAP-1
  appends a compatibility note that points its dated file-size premise at the live guards; slate:
  [../briefs/spark-function-parity.md](../../../briefs/spark-function-parity.md).
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

## Pointers
- Up: [../map.md](../map.md)
- [perf-scan-1-plan-once-ledger.md](perf-scan-1-plan-once-ledger.md) —
  **PERF-SCAN-1 (2026-09-03 / r2 2026-09-04), in flight:** `TargetScanStream` caches
  `FileScanTask`s across concurrent `StreamingTable` re-executes (hardening). Registry
  `PERF-SCAN-3PASS-1` stays BACKLOG: production identity DELETE is 1 + 0 + 1 opens, not
  3 × N at scan. `risk_tier: standard`. Branch `perf/scan-1-plan-once`.
  pins: perf-scan-1-plan-once/C-001, C-002, C-003, C-004
- [sql-harden-1-cutover-shapes-ledger.md](sql-harden-1-cutover-shapes-ledger.md) —
  **SQL-HARDEN-1 (2026-09-04), in flight:** the cutover pipeline cutover Iceberg SQL shapes S1–S7
  measured against live Spark on the memory catalog; Glue + S3 Tables legs. Four registry
  rows filed, `V3-COV-7` cited, 0 FIXED. `risk_tier: standard`. Branch
  `feat/sql-harden-1-cutover-shapes`. pins: sql-harden-1-cutover-shapes/C-001
- [sql-harden-2-cow-shapes-ledger.md](sql-harden-2-cow-shapes-ledger.md) —
  **SQL-HARDEN-2 (2026-09-04), in flight:** S1/S2/S4 at v2 and v3 copy-on-write (S8/S9).
  `delete_files` empty both engines; data-file count 1 after the second MERGE; remaining
  DIVERGES are `CUTOVER-CTAS-REQ-1` / `V3-COV-7`. No `CUTOVER-COW-*` row. Glue + S3 Tables
  PASS. `risk_tier: standard`. Branch `feat/sql-harden-2-cow-shapes`.
  pins: sql-harden-2-cow-shapes/C-001, C-002, C-003, C-004
- [rp-10-repin-f25-ledger.md](rp-10-repin-f25-ledger.md) — **RP-10 (2026-09-04), in flight:**
  the fork repin `594bdbe5` → `85a4aaf0` (F-25). `validate_fresh_dvs_only` stops once every
  `added_dvs` key is found; `PERF-DVCLOSE-STMT-1` closes. `risk_tier: standard`. Branch
  `feat/rp-10-repin-f25`.
- [date-fn-1-spark-date-spelling-ledger.md](date-fn-1-spark-date-spelling-ledger.md) —
  **DATE-FN-1 (2026-09-04), in flight:** Spark SQL `date()` spelling and `unix_timestamp`;
  `CUTOVER-DATE-1` FIXED; S6 gold rows Spark-equal, program still DIVERGES on `V3-COV-7`.
  `risk_tier: standard`. Branch `fix/date-fn-1-spark-date-spelling`.
  pins: date-fn-1-spark-date-spelling/C-004
- [ex-15-dataframe-a-ledger.md](ex-15-dataframe-a-ledger.md) —
  **EX-15 (2026-09-04), in flight:** the v1.1 example backfill's first `DataFrame.*` batch —
  36 roster names at base `c70a306`; 28 covered by eight `docs/examples/dataframe/` files
  (backlog 578 → 550), 8 measured divergences stay with §7 rows `EX-DF-1`…`EX-DF-6` and pins in
  `python/repark/tests/test_examples_dataframe_a.py`. `risk_tier: standard`. Branch
  `docs/ex-15-dataframe-a`. pins: ex-15-dataframe-a/C-001
- [perf-dynflatten-1-measure-ledger.md](perf-dynflatten-1-measure-ledger.md) —
  **PERF-DYNFLATTEN-1 (2026-09-04), in flight:** measure `dynamicFlatten` on the
  nested bed; rank the three H-3 intake candidates. `risk_tier: standard`.
  Branch `perf/dynflatten-1-measure`.
  pins: perf-dynflatten-1-measure/C-001, C-002, C-003, C-004

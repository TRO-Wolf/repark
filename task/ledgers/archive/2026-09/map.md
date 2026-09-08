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
- [2026-09-04-ctas-view-1-conform-stream-ledger.md](2026-09-04-ctas-view-1-conform-stream-ledger.md) — **CTAS-VIEW-1 (2026-09-03), complete:** unpartitioned CTAS stream writer conforms Utf8View batches.
- [2026-09-04-ex-10-functions-null-cond-misc-ledger.md](2026-09-04-ex-10-functions-null-cond-misc-ledger.md) — **EX-10 (2026-09-03), complete:** `F.*` null / conditional / misc.
- [2026-09-04-ex-11-functions-hash-url-random-ledger.md](2026-09-04-ex-11-functions-hash-url-random-ledger.md) — **EX-11 (2026-09-03), complete:** `F.*` hash / URL / random.
- [2026-09-04-ex-12-functions-aggregates-a-ledger.md](2026-09-04-ex-12-functions-aggregates-a-ledger.md) — **EX-12 (2026-09-03), complete:** `F.*` aggregates (a).
- [2026-09-04-ex-13-functions-aggregates-b-stats-ledger.md](2026-09-04-ex-13-functions-aggregates-b-stats-ledger.md) — **EX-13 (2026-09-03), complete:** `F.*` aggregates (b) / stats.
- [2026-09-04-ex-14-functions-window-ledger.md](2026-09-04-ex-14-functions-window-ledger.md) — **EX-14 (2026-09-03), complete:** `F.*` window.
- [2026-09-04-ex-2-functions-math-bitwise-ledger.md](2026-09-04-ex-2-functions-math-bitwise-ledger.md) — **EX-2 (2026-09-01), complete:** `F.*` math + bitwise pilot.
- [2026-09-04-ex-4-functions-strings-a-ledger.md](2026-09-04-ex-4-functions-strings-a-ledger.md) — **EX-4 (2026-09-03), complete:** `F.*` string basics.
- [2026-09-04-ex-5-functions-strings-b-regex-ledger.md](2026-09-04-ex-5-functions-strings-b-regex-ledger.md) — **EX-5 (2026-09-03), complete:** `F.*` string search / regex.
- [2026-09-04-ex-6-functions-datetime-a-ledger.md](2026-09-04-ex-6-functions-datetime-a-ledger.md) — **EX-6 (2026-09-03), complete:** `F.*` datetime arithmetic.
- [2026-09-04-ex-7-functions-datetime-b-ledger.md](2026-09-04-ex-7-functions-datetime-b-ledger.md) — **EX-7 (2026-09-03), complete:** `F.*` datetime remainder.
- [2026-09-04-ex-8-functions-arrays-ledger.md](2026-09-04-ex-8-functions-arrays-ledger.md) — **EX-8 (2026-09-03), complete:** `F.*` arrays.
- [2026-09-04-ex-9-functions-maps-structs-json-ledger.md](2026-09-04-ex-9-functions-maps-structs-json-ledger.md) — **EX-9 (2026-09-03), complete:** `F.*` map / struct / JSON.
- [2026-09-04-fn-fix-1-registry-rows-ledger.md](2026-09-04-fn-fix-1-registry-rows-ledger.md) — **FN-FIX-1 (2026-09-03), complete:** ten filed function-parity divergences plus NaN ingest.
- [2026-09-04-rp-11-repin-f24-ledger.md](2026-09-04-rp-11-repin-f24-ledger.md) — **RP-11 (2026-09-04), complete:** fork repin `85a4aaf0` → `189a73ed` (F-24); `B-MOR-3-FLOOR-1` FIXED.
- [2026-09-04-rp-9-repin-f23-ledger.md](2026-09-04-rp-9-repin-f23-ledger.md) — **RP-9 (2026-09-03), complete:** fork repin `c1d6c9de` → `594bdbe5` (F-23); `PERF-DVCLOSE-WALK-1` FIXED.
- [2026-09-04-sem-0-charter-ledger.md](2026-09-04-sem-0-charter-ledger.md) — **SEM-0 (2026-08-21), complete:** the scope audit for `RE-1` and `LOG-1`; both closed to Spark by SEM-1.
- [2026-09-05-perf-ice-scan-1-ledger.md](2026-09-05-perf-ice-scan-1-ledger.md) — **PERF-ICE-SCAN-1 (2026-09-05), complete:** Iceberg `count(*)` folds (86.5 → 2.0 ms) and small tables scan N=8 (sum 89.5 → 36.2 ms); the 1.5×-of-parquet target is an honest miss (1.8–3.6×, decomposed in the baseline).
- [2026-09-05-types-1-ledger.md](2026-09-05-types-1-ledger.md) — **TYPES-1 (2026-09-05), complete:** the SQL door's Arrow types follow Spark — INT literals, BIGINT count-likes, the INT rank family, session-zone STRING `from_unixtime`.
- [2026-09-06-nullability-2-ledger.md](2026-09-06-nullability-2-ledger.md) — **NULLABILITY-2 (2026-09-05), complete:** the analyzer's remaining nullability and cast residues, Spark-equal — eight registry rows FIXED, live roster 13 → 10.
- [2026-09-06-perf-approxpct-1-ledger.md](2026-09-06-perf-approxpct-1-ledger.md) — **PERF-APPROXPCT-1 (2026-09-05), complete:** Spark's Greenwald-Khanna `QuantileSummaries` behind `percentile_approx` / `approx_percentile`, so the accuracy knob moves the answers and bounds the memory on the group-by path and per frame on the WIN-SLIDE-1 re-scan path.
- [2026-09-07-fnp-8-ledger.md](2026-09-07-fnp-8-ledger.md) — Errata (2026-09-07) at its head: the after-the-fact critic (FNP-8-REVIEW, three rounds, PASS).

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../../AGENTS.md](../../../../AGENTS.md) "Markdown document lifecycle"

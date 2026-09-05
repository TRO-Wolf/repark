# Unit ledger — PERF-ICE-SCAN-1 · Iceberg `count(*)` stops decoding every column, and small tables scan in parallel

**Date:** 2026-09-05 · **Branch:** `perf/ice-scan-1` · **Base:** `origin/main` `8f40ce46` ·
**Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: standard`.
**Registry:** `PERF-ICE-COUNTSTAR-1` and `PERF-ICE-SCANPART-1` filed FIXED-PENDING-PIN with fork trigger **F-27**.
**Fork half:** lane `$HOME/repark-lanes/lanes/icescan-fork`, branch `f-27-count-star-projection`, base fork `main` `16639b87c` (F-28).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** PERF-ANALYSIS-1 §2 rows 4 and 5: `count(*)` at 1e6 costs 93 ms because the
empty projection decodes every column, and a 1e6-row table scans as ONE partition because
`plan_partition_work` bin-packs to the 128 MiB split target. Both fixes are fork-side (F-27);
the RePark half pins the behaviour behind skip-until-F-27 guards and measures it through a
temporary path override.

**Not in this unit:** the fork pin bump (the orchestrator's RP-13 step, own PR,
[../../../docs/fork-sync.md](../../../docs/fork-sync.md)); the MERGE executor, the
predicate-DML paths and every other scan consumer, which keep their plans byte-identical;
`STATUS.md` and `briefs/next-sequence.md`.

**Writable paths:** fork lane `crates/iceberg/src/{arrow/reader.rs,scan/bin_pack.rs,scan/partition_work.rs}`,
`crates/iceberg/tests/{empty_projection_scan.rs,map.md}`,
`crates/integrations/datafusion/src/physical_plan/{scan.rs,scan_knobs.rs,mod.rs,map.md}`,
`crates/integrations/datafusion/tests/{count_star_fold.rs,parallel_small_scan.rs,map.md}`,
`crates/iceberg/src/scan/map.md`, `scripts/check_rust_file_size.py`;
RePark `python/repark/tests/{test_perf_ice_scan_1.py,map.md}`,
`docs/perf/{iceberg-scan-baseline.md,map.md}`, `docs/spark-sql-iceberg-parity.md` §7.4,
this ledger and its `staging/map.md` row. Closed: `Cargo.toml`, `Cargo.lock`, every
dependency, `.github/`, every other ledger.

## PROPOSITION LEDGER — PERF-ICE-SCAN-1 — 2026-09-05

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | An empty projection reads row counts, never column bytes: zero-column batches totalling the live rows over plain, position-deleted, DV-deleted, residual-filtered and equality-deleted files. | `crates/iceberg/tests/empty_projection_scan.rs`: 7 pins, one of which fails on the old `ProjectionMask::all()` and passes on the new empty mask. | **PROVEN** | Fork `5d45e040b`. The one-line mask change (`all()` → `leaves(schema, [])`, net-zero in `reader.rs`, at the 10246 ceiling) with 7 integration pins: 100 rows over 4 row groups in 7-row batches, positional deletes → 97, a puffin DV → 98, a residual with row selection off and on → 50, equality deletes → 97, and the corrupt-first-data-page file that errors on the old mask (`cannot skip field type Set`) and reads 100 rows on the new one — the red-first pin, verified by stashing the fix (6 pass, 1 fails) and restoring (7 pass). |
| C-002 | `count(*)` on a plain Iceberg table folds without a scan: exact whole-table statistics, the right answer, and a physical plan with no `IcebergTableScan`. | `count_star_fold.rs`: Exact(3), answer 3, EXPLAIN asserts the fold. | **PROVEN** | Fork `c8a3c922f`. `partition_statistics` reports `Precision::Exact` over the whole table when planned tasks carry no residual and no deletes, reading the total from the frozen snapshot's `total-records` summary. Design note: the first attempt summed task `record_count`, which `plan_tasks` clears on every split — the fold never fired; the summary design replaced it (the `exact_row_count` helper and its 6 tests were removed, not kept). The knobs block moved byte-identically to `scan_knobs.rs` (size-gate split, ceiling 1999 → 1890). |
| C-003 | `count(*)` does NOT fold — and still answers correctly — with delete files (DV included), a WHERE residual, or a LIMIT; a COW DELETE (no delete files) keeps the fold. | `count_star_fold.rs` negatives + answers; the DV-table count is Spark-equal on the RePark leg. | **OPEN** | Fork pins green (`c8a3c922f`): V3 MoR + DV → unknown/answer 2/scans; residual → unknown; limit → unknown; per-partition → unknown; empty → Exact(0); COW DELETE → Exact(2). RePark live leg pending. |
| C-004 | A sub-split-size table scans in `min(T, allow)` partitions with the row set intact: re-split above the derived target, re-pack at it, tiling exactly-once. | `bin_pack::split_tests` (19 pins incl. tiling asserts) + `parallel_small_scan.rs` (N=8, 24-row set; N=1 at T=1; empty projection stays N=1). | **PROVEN** | Fork `c18f15c96`. `expand_groups_for_target` re-packs at `min(configured split size, max(total/T, 64 KiB))` and re-splits above it; the `min(configured)` half was forced by the pre-existing `test_pin1_pin5` (tiny `read.split.*` props must yield N>1), and the 64 KiB window floor stops a 1-byte target shredding a file into millions of tasks. 19 unit pins + 3 end-to-end. MERGE/`plan_tasks`/`plan_files` untouched — the call lands between `plan_tasks` and assignment. |
| C-005 | MERGE and identity-DELETE target scans keep their plans: MERGE reads through `plan_files`/`to_arrow` (untouched code), `_pos`/`_row_id` projections never split (scan-level skip + task-level decline). | The caller audit in §7; `expand_skips_pos_projection`, `expand_skips_row_id_projection`, the pre-existing `plan_tasks` `_pos` pins, and the RePark DML suites green. | **OPEN** | Audit done: the only `plan_partition_work` caller is `IcebergTableScan::plan` (DataFusion SELECT); RePark MERGE (`write/merge/target_scan.rs`) and the fork COW/MoR/maintenance paths all use `plan_files`/`to_arrow`. `_pos` pins green. RePark DML suites pending. |
| C-006 | The fork lane is green on its own gates. | `cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test -p iceberg -p iceberg-datafusion`, size/comment/matrix/artifact scripts. | **PROVEN** | `cargo fmt --all --check` clean; `cargo clippy --all-targets -- -D warnings` Finished, no warnings; `cargo test -p iceberg -p iceberg-datafusion` exit 0 (85 `test result: ok`, zero failures — 3631 lib + all integration binaries); `check_rust_file_size.sh` 444 files clean; comment-block, matrix-anchor, agent-artifact scripts OK. |
| C-007 | The RePark pins skip until the fork pin carries F-27, and the suite is green before the bump. | `test_perf_ice_scan_1.py` skips with a named reason on the pinned fork; full `pytest python/repark/tests` green. | **OPEN** | RePark side. |
| C-008 | The measured targets hold on the with-patch build: `count(*)` at 1e6 ≤ 5 ms; `sum_all`/`string_len` within 1.5× of the parquet path at 1e6/1e7. | `docs/perf/iceberg-scan-baseline.md` before/after tables, §6 commands, medians, spread, floor, × floor, load. | **OPEN** | Measurement. |
| C-009 | The ranged-split reads are Spark-equal: row-set identity on `t_part`, `_pos`/lineage over ranged splits, V3 `_row_id` order unchanged. | RePark live legs against the pinned oracle + the unchanged `_row_id` pins. | **OPEN** | RePark side. |
| C-010 | No dependency moves and the pin does not move: `git diff origin/main -- Cargo.toml Cargo.lock` is empty at hand-back, the fork change is consumed only through a temporary never-committed path override, and the bump is the orchestrator's RP-13 step. | The diff, plus the registry rows recording the fork dependency. | **OPEN** | Hand-back. |

VERDICT: 10 clauses, 0 PROVEN, 10 OPEN, 0 REJECTED.

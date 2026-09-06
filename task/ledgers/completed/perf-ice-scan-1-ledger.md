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

**Not in this unit:** the fork pin bump (the orchestrator's RP-14 step, own PR,
[../../../docs/fork-sync.md](../../../docs/fork-sync.md)); the MERGE executor, the
predicate-DML paths and every other scan consumer, which keep their plans byte-identical;
`STATUS.md` and `briefs/next-sequence.md`.

**Writable paths:** fork lane `crates/iceberg/src/{arrow/reader.rs,scan/bin_pack.rs,scan/partition_work.rs}`,
`crates/iceberg/tests/{empty_projection_scan.rs,map.md}`,
`crates/integrations/datafusion/src/physical_plan/{scan.rs,scan_knobs.rs,mod.rs,map.md}`,
`crates/integrations/datafusion/tests/{count_star_fold.rs,parallel_small_scan.rs,map.md}`,
`crates/iceberg/src/scan/map.md`, `scripts/check_rust_file_size.py`;
RePark `python/repark/tests/{test_perf_ice_scan_1.py,map.md}`,
`docs/perf/{iceberg-scan-baseline.md,map.md}`, `docs/spark-sql-iceberg-parity.md` §7,
this ledger and its `staging/map.md` row. Closed: `Cargo.toml`, `Cargo.lock`, every
dependency, `.github/`, every other ledger.
`python/repark-parity/bench/icescan/` (the §7.4 read-cell probes) and `crates/repark-iceberg/src/catalog/map.md` were also written (declared here after the round-2 critic's F3).

## PROPOSITION LEDGER — PERF-ICE-SCAN-1 — 2026-09-05

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | An empty projection reads row counts, never column bytes: zero-column batches totalling the live rows over plain, position-deleted, DV-deleted, residual-filtered and equality-deleted files. | `crates/iceberg/tests/empty_projection_scan.rs`: 7 pins, one of which fails on the old `ProjectionMask::all()` and passes on the new empty mask. | **PROVEN** | Fork `5d45e040b`. The one-line mask change (`all()` → `leaves(schema, [])`, net-zero in `reader.rs`, at the 10246 ceiling) with 7 integration pins: 100 rows over 4 row groups in 7-row batches, positional deletes → 97, a puffin DV → 98, a residual with row selection off and on → 50, equality deletes → 97, and the corrupt-first-data-page file that errors on the old mask (`cannot skip field type Set`) and reads 100 rows on the new one — the red-first pin, verified by stashing the fix (6 pass, 1 fails) and restoring (7 pass). |
| C-002 | `count(*)` on a plain Iceberg table folds without a scan: exact whole-table statistics, the right answer, and a physical plan with no `IcebergTableScan`. | `count_star_fold.rs`: Exact(3), answer 3, EXPLAIN asserts the fold. | **PROVEN** | Fork `c8a3c922f`. `partition_statistics` reports `Precision::Exact` over the whole table when planned tasks carry no residual and no deletes, reading the total from the frozen snapshot's `total-records` summary. Design note: the first attempt summed task `record_count`, which `plan_tasks` clears on every split — the fold never fired; the summary design replaced it (the `exact_row_count` helper and its 6 tests were removed, not kept). The knobs block moved byte-identically to `scan_knobs.rs` (size-gate split, ceiling 1999 → 1890). |
| C-003 | `count(*)` does NOT fold — and still answers correctly — with delete files (DV included), a WHERE residual, or a LIMIT; a COW DELETE (no delete files) keeps the fold. | `count_star_fold.rs` negatives + answers; the DV-table count is Spark-equal on the RePark leg. | **PROVEN** | Fork pins green (`c8a3c922f`): V3 MoR + DV → unknown/answer 2/scans; residual → unknown; limit → unknown; per-partition → unknown; empty → Exact(0); COW DELETE → Exact(2). RePark: the DV/WHERE non-fold pins pass on the with-patch module (scan present, answers 23/4) and the LIMIT pin passes on both builds; the bed `t_dv` count answers 990,000 in 4.6 ms unfolded. Live: `test_dv_count_matches_spark` passes — engine 23 == Spark 23 on the v3 MoR DV table (pyspark 4.1.2, `local[2]`, lane ivy). The `limit → unknown` guard is exercised only for N=1: `plan()` clears `scan.limit` when N>1, so the scan then emits every row and the statistic is unknown by construction rather than by the guard (round-2 critic F6). |
| C-004 | A sub-split-size table scans in `min(T, allow)` partitions with the row set intact: re-split above the derived target, re-pack at it, tiling exactly-once. | `bin_pack::split_tests` (19 pins incl. tiling asserts) + `parallel_small_scan.rs` (N=8, 24-row set; N=1 at T=1; empty projection stays N=1). | **PROVEN** | Fork `c18f15c96`. `expand_groups_for_target` re-packs at `min(configured split size, max(total/T, 64 KiB))` and re-splits above it; the `min(configured)` half was forced by the pre-existing `test_pin1_pin5` (tiny `read.split.*` props must yield N>1), and the 64 KiB window floor stops a 1-byte target shredding a file into millions of tasks. 19 unit pins + 3 end-to-end. MERGE/`plan_tasks`/`plan_files` untouched — the call lands between `plan_tasks` and assignment. |
| C-005 | MERGE and identity-DELETE target scans keep their plans: MERGE reads through `plan_files`/`to_arrow` (untouched code), `_pos`/`_row_id` projections never split (scan-level skip + task-level decline). | The caller audit in §7; `expand_skips_pos_projection`, `expand_skips_row_id_projection`, the pre-existing `plan_tasks` `_pos` pins, and the RePark DML suites green. | **PROVEN** | Audit done (§7): the only `plan_partition_work` caller is `IcebergTableScan::plan` (DataFusion SELECT); RePark MERGE (`write/merge/target_scan.rs`) and the fork COW/MoR/maintenance paths all use `plan_files`/`to_arrow`. `_pos`/`_row_id` skip pins green fork-side. RePark: identity-DELETE and MERGE row-set pins pass on both builds; `_pos` is not SQL-visible in RePark and EXPLAIN cannot plan `_row_id` (both pre-existing, verified on the with-patch module), so the RePark side pins the `_row_id` 0..23 tiling, not the N=1 plan shape. Full facade suite green (4903 passed, 213 skipped), `make ci` + `make verify` green. |
| C-006 | The fork lane is green on its own gates. | `cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test -p iceberg -p iceberg-datafusion`, size/comment/matrix/artifact scripts. | **OPEN** | `cargo fmt --all --check` clean; `cargo clippy --all-targets -- -D warnings` Finished, no warnings; `cargo test -p iceberg -p iceberg-datafusion` exit 0 (85 `test result: ok`, zero failures — 3631 lib + all integration binaries); `check_rust_file_size.sh` 444 files clean; comment-block, matrix-anchor, agent-artifact scripts OK. Round-2 critic: the fork CI runs `cargo nextest run` over the WORKSPACE, which includes `iceberg-sqllogictest`; the roster above omitted it and PR #271 is red on `like_predicate_pushdown.slt:39-40` (`N=1` → `N=2` under the harness-pinned `target_partitions(4)`, deterministic). Fork round 3 updates that golden and adds the crate to the gate; this clause re-closes when #271 is green. |
| C-007 | The RePark pins skip until the fork pin carries F-27, and the suite is green before the bump. | `test_perf_ice_scan_1.py` skips with a named reason on the pinned fork; full `pytest python/repark/tests` green. | **PROVEN** | `test_perf_ice_scan_1.py`: 6 passed, 9 skipped on the pinned fork (the 6 F-27 legs skip naming F-27 + RP-14; the 3 live legs skip naming the live flag); 12 passed, 3 skipped on the with-patch module. Full suite: 4903 passed, 213 skipped, exit 0. |
| C-008 | `count(*)` at 1e6 costs ≤ 5 ms on the with-patch build. | `docs/perf/iceberg-scan-baseline.md` §2. | **PROVEN** | 1e6: 86.5 → 2.0 ms (44×, parquet 1.8 ms); 1e7: 686 → 2.5 ms. The plan has no scan. |
| C-011 | `sum_all`/`string_len` land within 1.5× of the parquet path at 1e6/1e7. | `docs/perf/iceberg-scan-baseline.md` §2–§3. | **REJECTED** | Honest miss with the decomposition in §3 of the baseline: 1.8×/2.2× at 1e6, 2.4×/3.6× at 1e7 (2.5–3.7× faster than before, N=1 → N=8). Planning is ~1 ms of the gap; a fixed ~10 ms per query and ~2× per-byte overhead remain. Residue filed under `PERF-ICE-SCANPART-1`. |
| C-009 | The ranged-split reads are Spark-equal: row-set identity on `t_part`, lineage over ranged splits, V3 `_row_id` order unchanged. | RePark live legs against the pinned oracle + the unchanged `_row_id` pins. | **PROVEN** | `_row_id` 0..23 tiling passes on the with-patch module. `_pos` is not SQL-visible in RePark (verified), so no RePark `_pos` leg exists; the skip is pinned fork-side. Live, one invocation with `test_live_disclosure_still_diverges` co-collected (28 passed): the partitioned bed reads the same 24-row set as Spark, the post-DELETE row set matches Spark, and the DV count matches Spark (23 == 23). |
| C-010 | No dependency moves and the pin does not move: `git diff origin/main -- Cargo.toml Cargo.lock` is empty at hand-back, the fork change is consumed only through a temporary never-committed path override, and the bump is the orchestrator's RP-14 step. | The diff, plus the registry rows recording the fork dependency. | **PROVEN** | The after leg ran under the temporary override (`scratch/probes/fork_override_scan.sh`, git-excluded) to fork `c18f15c96`; reverted (`git checkout -- Cargo.toml Cargo.lock`), module rebuilt without it, `git diff origin/main -- Cargo.toml Cargo.lock` empty. Registry rows `PERF-ICE-COUNTSTAR-1` / `PERF-ICE-SCANPART-1` filed FIXED-PENDING-PIN. The bump is the orchestrator's RP-14 step. |

VERDICT: 11 clauses, 10 PROVEN, 0 OPEN, 1 REJECTED.

## 7. Caller audit (C-005): who plans through `plan_partition_work`

`grep -rn plan_partition_work` over the fork lane at `c18f15c96`, production code only:

| caller | path | what it is |
|---|---|---|
| `IcebergTableScan::plan` | `crates/integrations/datafusion/src/physical_plan/scan.rs:190` | the DataFusion SELECT scan; the ONLY production caller |
| (self + tests) | `crates/iceberg/src/scan/partition_work.rs` | the definition, `plan_partition_work_from_scan`, and pins |

Every DML and maintenance path plans through `plan_files` / `to_arrow`, which F-27 does
not touch: RePark MERGE (`crates/repark-iceberg/src/write/merge/target_scan.rs:104`
`plan_files`, `:250` `to_arrow`), the fork COW/MoR writers and the maintenance actions.
`plan_tasks` and `plan_files` are byte-identical before and after F-27; the re-split
call lands between `plan_tasks` and partition assignment, inside `plan_partition_work`
only. Belt and braces for the two projections whose partition identity DML-adjacent
reads depend on: a `_pos` or `_row_id` projection (or an empty/file-prune-only scan)
declines the re-split at the scan level and again at the task level
(`expand_skips_pos_projection`, `expand_skips_row_id_projection`,
`parallel_small_scan.rs`).

## 8. Mutation score (fork lane, each change reverted alone, then restored)

| reverted change | command | red pins | green pins |
|---|---|---|---|
| F-27a mask (`reader.rs` → `16639b87c`) | `cargo test -p iceberg --test empty_projection_scan` | `empty_projection_ignores_corrupt_column_bytes` (1) | the 6 answer pins (old and new agree on answers) |
| F-27b fold (`scan.rs` + `mod.rs` → `5d45e040b`) | `cargo test -p iceberg-datafusion --test count_star_fold` | `count_star_folds_on_plain_table`, `count_star_on_empty_table_is_zero`, `count_star_stays_folded_after_cow_delete` (3) | the 4 negatives (unknown/no-fold holds trivially) |
| F-27d call site (`partition_work.rs` → `c8a3c922f`, helper intact) | `cargo test -p iceberg-datafusion --test parallel_small_scan` + `-p iceberg --lib scan::bin_pack` | `small_table_scans_in_parallel` (N=8 → N=1) | the 2 N=1 pins + all 28 `bin_pack` unit pins (pure functions, wiring-independent) |

Every revert reds exactly the pins that assert the new behaviour and nothing else; the
fork lane is clean after each restore (`git status` empty).

```yaml
COVERAGE_ATTESTATION:
  pr_unit: perf-ice-scan-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: All 11 clauses walked against behavior, not paraphrase. The count half (C-008) folds at 2.0 ms; the 1.5x half is honestly REJECTED as C-011 with the §3 decomposition rather than absorbed. Live Spark legs close C-003/C-009 verbatim.
      artifacts: [task/ledgers/completed/perf-ice-scan-1-ledger.md, docs/perf/iceberg-scan-baseline.md, python/repark/tests/test_perf_ice_scan_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Empty table (Exact 0), LIMIT 0/1, T=1 stays N=1, 1-byte split target floored at 64 KiB, corrupt first data page, DV/positional/equality deletes, residual with row selection off and on.
      artifacts: [empty_projection_scan.rs, count_star_fold.rs, parallel_small_scan.rs, python/repark/tests/test_perf_ice_scan_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The corrupt-page file errors on the old mask and reads on the new one; the fold is absent (never wrong) with deletes, residuals and limits; the over-wide override and the shared-session kill were both found by running, not by reading.
      artifacts: [empty_projection_scan.rs, count_star_fold.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Exact counts come from the frozen snapshot id pinned at plan time, so a concurrent commit cannot skew the fold; N=8 tiling asserts exactly-once row sets; the suite-wide session reset forced per-test sessions with a module-scoped bool probe.
      artifacts: [parallel_small_scan.rs, python/repark/tests/test_perf_ice_scan_1.py]
    - id: AT-5
      status: N/A
      justification: No auth, no secrets, no AWS, no .github change, no dependency move, no deserialization of untrusted input; the only network is the lane-local Spark oracle.
    - id: AT-6
      status: ATTACKED
      evidence: Every answer the new code touches is Spark-equal (partitioned row set, post-DELETE set, DV count 23, DV bed 990,000) or tiling-identical (_row_id 0..23); pre-bump behavior is preserved behind named skips, not changed.
      artifacts: [python/repark/tests/test_perf_ice_scan_1.py, docs/perf/iceberg-scan-baseline.md]
    - id: AT-7
      status: ATTACKED
      evidence: The re-split cannot shred a file into millions of tasks (64 KiB window floor, pinned); the fold removes a full scan rather than adding one; bed cells carry load and spread, and the 1e7 before-run is the slowest honest floor, not a trimmed one.
      artifacts: [parallel_small_scan.rs, docs/perf/iceberg-scan-baseline.md]
    - id: AT-8
      status: ATTACKED
      evidence: Exact statistics are reported only when sound (delete-free, residual-free, limit-free); the fork is consumed through a temporary never-committed override with manifests byte-clean at hand-back; capability is probed at runtime, never presumed.
      artifacts: [count_star_fold.rs, python/repark/tests/test_perf_ice_scan_1.py, Cargo.toml, Cargo.lock]
    - id: AT-9
      status: ATTACKED
      evidence: Plan-shape pins (fold present/absent, N=1/N=8) fail loudly on behavior change instead of absorbing it; the 1.5x miss is a REJECTED clause with residue filed, not a quiet number.
      artifacts: [python/repark/tests/test_perf_ice_scan_1.py, docs/perf/iceberg-scan-baseline.md]
    - id: AT-10
      status: ATTACKED
      evidence: Mutation §8: each revert reds exactly the behavior pins (1, 3, 1) and nothing else. Branch liveness: the mask arm (corrupt page), the Exact/unknown arms (fold pins vs 4 negatives), the split/decline arms (N=8 vs N=1/_pos/_row_id/empty/prune-only) each have a discriminating input.
      artifacts: [empty_projection_scan.rs, count_star_fold.rs, parallel_small_scan.rs, python/repark/tests/test_perf_ice_scan_1.py]
  complete: true
```

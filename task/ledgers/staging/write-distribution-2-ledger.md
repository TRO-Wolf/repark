# Unit ledger — WRITE-DISTRIBUTION-2 · the hash distribution rule on the stream write paths

**Date:** 2026-09-06 · **Branch:** `perf/write-distribution-2` · **Base:** `origin/main` `b4933a99` ·
**Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: standard`.
**Registry:** `WRITE-DISTRIBUTION-2` **FIXED** (plain `INSERT INTO` + `saveAsTable(append)` stay
open — fork-owned, C-009).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** WRITE-DISTRIBUTION-1 put the hash distribution rule under the CTAS node only. Its
critic measured the stream paths fanning every input partition out to every partition value:
`INSERT OVERWRITE` and `MERGE … WHEN NOT MATCHED THEN INSERT` into a partitioned table write 32
files at `shuffle.partitions = 8` where Spark writes 8. The same review left three smaller gaps:
(F-1) dropping the type cast in `distribution.rs` reds no pin; (F-2) the Rust abort pins run over
unpartitioned tables only; (F-4) baseline §8 discloses the `_row_id`-in-file order change without
its measured counts.

**Not in this unit:** plain `INSERT INTO` and `saveAsTable(mode="append")` — both execute inside
the fork's `insert_into`, which RePark code never sees (C-009 OPEN, the halted question); the V3
serial lineage writer (already one writer, untouched); the unpartitioned layout (WD1's decision
stands); the fork pin; `STATUS.md`; `briefs/next-sequence.md`.

**Actor note.** A previous actor (Opus, killed by its session limit) left uncommitted work: the
router, the stream pins, the facade file, the ceiling edits, and a `scratch/` of measurements.
This run kept the router and the pins, **restored the type cast the previous actor had dropped**
(the fork rejects `truncate` over `Utf8View` — M2 proves it), restored the 400,000-row MERGE seed
(the pin does not bite at 120,000), and re-ran every red/green and every measurement on
self-consistent natives. The previous session's six v3-coverage reds did not reproduce — all
seven corresponding always-run tests pass here; they are attributed to that session's
uncommitted intermediate state. Its `scratch/` numbers are quoted nowhere as evidence.

**Writable paths:** `crates/repark-iceberg/src/write/{distribution.rs,append.rs,map.md,merge/map.md}`,
`python/repark/tests/{test_write_distribution_2.py,map.md}`,
`python/repark-parity/tests/{test_cap_1_source_file_line_cap.py,map.md}`,
`scripts/{check_rust_file_size.py,map.md}`,
`docs/perf/{iceberg-write-baseline.md,map.md}`, `docs/spark-sql-iceberg-parity.md` §7, this
ledger and its `staging/map.md` row. Closed: `Cargo.toml`, `Cargo.lock`, every dependency,
`.github/`, every other ledger.

## Plan

- [x] Reproduce on the base tree with a release native: the four stream statements' file counts
      at `shuffle.partitions = 8`, vs live Spark 4.1.2 for the same statements.
- [x] Facade pins red on the base native (`test_write_distribution_2.py`).
- [x] Rust pins red against the base dispatcher, then the fix: the dispatcher routes each batch
      by hash of the writer's partition values (`PartitionRouter`), one value to one worker.
- [x] MERGE row semantics and lineage unchanged: the MERGE, lineage and write-path suites green.
- [x] Close F-1 (mutation-proven cast pin), F-2 (partitioned abort pin), F-4 (§8 counts).
- [x] Measure after with the same probes; RSS peak; Spark's live counts on the same bed.
- [x] Registry row, baseline §6 and §8–§9, maps, attestation, gates.

## PROPOSITION LEDGER — WRITE-DISTRIBUTION-2 — 2026-09-06

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | On a partitioned table with more than one writer, every row of one partition value reaches exactly one writer on the stream paths, so `INSERT OVERWRITE` and MERGE inserts commit one data file per partition value present. | Rust pins over four stream batches that each carry all eight values, red on the base dispatcher; the facade pin over the 120k/400k seeds at `shuffle.partitions = 8`, red on the base native. | **PROVEN** | `stream_path_lands_one_partition_value_in_one_writer`: red on the base dispatcher with 32 files of 8 rows (`(Int(0), 8)×4, (Int(1), 8)×4, …`), green after with 8 of 32. `test_stream_write_commits_one_data_file_per_partition_value`: red on the base native — overwrite `[(0, 3750)×4, (1, 3750)×4, …]`, merge `[(0, 12499), (0, 12499), (0, 12501), …]` — green after with `[0..7]` at 15,000 / 50,000 rows each and the row count plus `sum(id)` proving the row set. |
| C-002 | The `truncate` cast is load-bearing: `truncate(3, s)` over a view-typed string keys on the cast value. | A Rust expression pin over a `Utf8View` batch, red with the cast dropped (mutation M2); the facade `truncate(3, s)` CTAS as the end-to-end guard. | **PROVEN** | `truncate_over_a_view_typed_string_keys_on_the_cast_value`: green with the cast; with the cast dropped it fails with the fork's `External(FeatureUnsupported => Unsupported data type for truncate transform: Utf8View)`. The brief's facade-shaped M2 is REFUTED by measurement: the facade pin passes on the no-cast native too, because DML planning inserts `CAST(s AS Utf8)` (visible in the scan projection), so the distribution node never sees `Utf8View` there. The facade pin stays as a guard — it pins the previously unpinned truncate-partitioned layout. |
| C-003 | Live Spark 4.1.2 commits the same `(partition value, record count)` layout for the overwrite and merge seeds. | The live legs of the facade file, one JVM, killed at exit. | **PROVEN** | `test_stream_write_layout_matches_spark[insert_overwrite, merge_insert]`: `5 passed` under `REPARK_PARITY_LIVE=1` (23.62 s); engine layouts equal Spark's `(0..7, 15000)` / `(0..7, 50000)`. Banner: Spark 4.1.2, UTC. |
| C-004 | The stream distribution is deterministic across runs, and MERGE inserts take the routed funnel. | A Rust pin running the same stream twice over unequal batches; a Rust pin through the MERGE entry asserting the non-lineage branch. Both red on the base dispatcher. | **PROVEN** | `stream_path_distribution_is_deterministic_across_runs` over 40/24/64/8 rows: red before (32 files), green after with eight files of 17 in both runs. `merge_inserts_into_a_partitioned_table_route_one_value_to_one_writer`: red before, green after with 8 files of 32; it asserts `!table_carries_merge_lineage` so the pin covers the funnel, not the serial V3 writer. |
| C-005 | A NULL partition value is one value on the stream path, and a two-field spec keys on the transform values. | A Rust pin on a nullable string identity partition; a Rust pin over `bucket(4, id)` + `day(ts)`. Both red on the base dispatcher. | **PROVEN** | `stream_path_null_partition_values_share_one_writer`: red before (16 files), green after with one NULL file of 64 rows and one per label. `stream_path_two_field_spec_keys_on_the_transform_value`: red before (32 files), green after with 8 files, keys unique, 256 rows. |
| C-006 | A late partition failure into a partitioned table leaves no data file (F-2). | A Rust abort pin over an identity-partitioned table at a 64 KiB target file size. | **PROVEN** | `a_late_failure_into_a_partitioned_table_leaves_no_data_file`: the injected failure surfaces verbatim and the warehouse holds no parquet. Green by design on both sides — it extends the plan-path abort property (WD1's, unchanged code) to partitioned tables; it guards the decision, not the fix. |
| C-007 | MERGE keeps its row semantics and lineage properties, and the write-path suites stay green. | The MERGE, lineage and write-path suites plus the WD1 pins, unchanged, on the fixed native. The F-4 counts characterize the untouched CTAS path. | **PROVEN** | `test_merge_into.py`, `test_merge_insert_scope.py`, `test_merge_scan_prune_semantics.py`, `test_merge_semantics_audit.py`, `test_merge_store_assign.py`, `test_v3_legacy_delete_merge.py`, `test_v3_lineage_columns.py`, `test_perf_ice_writepath_1.py`: `56 passed, 5 skipped`; `test_merge_differential_parity.py`: `14 passed`; `test_v3_statement_coverage.py`: `84 passed, 81 skipped` (all seven tests the previous session logged red pass here). F-4: five v3 partitioned CTAS runs per count — 1 distinct manifest sequence at 3/4/8 partitions, 3/4/4 distinct id-to-`_row_id` maps (baseline §8). |
| C-008 | No dependency moves and RePark spawns nothing: `git diff origin/main -- Cargo.toml Cargo.lock` is empty and the change adds no `tokio::spawn`. | The diff; `make rust-panic-ban`. | **PROVEN** | `git diff origin/main -- Cargo.toml Cargo.lock` empty; `make rust-panic-ban` exit 0. The router is a synchronous per-batch split inside the existing dispatcher future; sends ride the existing bounded channels. |
| C-009 | `INSERT INTO` and `saveAsTable(mode="append")` write Spark's file count. | — | **OPEN** | Measured, not fixed: 64 files at 1e6 (32 on the four-file facade seed) where Spark writes 8, before and after. Both statements execute inside the fork's `insert_into` → `IcebergWriteExec` (EXPLAIN shows the fork plan; the fork's hash repartition is absent at runtime at every `target_partitions`); RePark code never sees the stream. Fixing it needs a fork change or a RePark provider wrapper — §7 and the halted question Q1. |

VERDICT: 9 clauses, 8 PROVEN, 1 OPEN, 0 REJECTED.

```
COVERAGE_ATTESTATION:
  pr_unit: write-distribution-2
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause is stated against a measurement or a red-then-green pin, except C-009 which is stated OPEN with its measured counts. The gain is reported as file counts plus control-paired ratios because the pair was measured sequentially across a native swap; the unfixable cells are stated with their numbers, not absorbed.
      artifacts: [docs/perf/iceberg-write-baseline.md, task/ledgers/staging/write-distribution-2-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: Identity int, NULL-heavy nullable string, bucket(4) + day over a timestamp, unequal stream batches, MERGE through its own entry with the branch asserted, truncate over a view-typed string, a partitioned abort; the facade over overwrite, merge and truncate; live Spark on the overwrite and merge seeds plus the full four-statement bed.
      artifacts: [crates/repark-iceberg/src/write/distribution.rs, python/repark/tests/test_write_distribution_2.py]
    - id: AT-3
      status: ATTACKED
      evidence: No unwrap or expect in production; the slot arithmetic is checked (`u32`/`u64`/`usize` fallible conversions with typed errors); an oversized batch is a loud Execution error, not a wrap. make rust-panic-ban exit 0.
      artifacts: [crates/repark-iceberg/src/write/distribution.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The router holds the calculator plus a slot count and the hash is seeded; routing is one pass over the input with bounded per-batch takes; sends ride the existing bounded worker channels, so the writers-below-partitions shape is not reintroduced and RSS is measured (down 692-711 to 597-613 MB). No spawn, no unsafe, no new dependency.
      artifacts: [crates/repark-iceberg/src/write/distribution.rs, crates/repark-iceberg/src/write/map.md]
    - id: AT-5
      status: ATTACKED
      evidence: The abort path is unchanged — a source error still sets the shared flag, drops the senders, and surfaces verbatim; the mid-stream abort pin and the new partitioned abort pin both pass.
      artifacts: [crates/repark-iceberg/src/write/append.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Live Spark 4.1.2 (banner 4.1.2, UTC) writes the same (partition value, record count) layout for the overwrite and merge seeds; the written row set and sums are unchanged (count(*), sum(id) asserted on every pin). 5 passed under REPARK_PARITY_LIVE=1, one JVM, killed at exit.
      artifacts: [python/repark/tests/test_write_distribution_2.py]
    - id: AT-7
      status: ATTACKED
      evidence: Mutation M2 (drop the cast) reds the Rust truncate pin with the fork's Utf8View rejection; the base dispatcher reds five of seven stream Rust pins and both always-run layout facade pins. The facade-shaped M2 is refuted with its mechanism, not absorbed. Section 6.
      artifacts: [task/ledgers/staging/write-distribution-2-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: Before and after sets on the 1e6 bed, every pass median, the load at every pass, RSS at every pass, the sequential-pair caveat stated, and a paragraph on what the INSERT INTO cells do NOT show.
      artifacts: [docs/perf/iceberg-write-baseline.md]
    - id: AT-9
      status: N/A
      justification: No AWS surface is touched; the router splits Arrow batches over a memory catalog on the local filesystem, and no catalog seam, credential path or region behaviour changes.
    - id: AT-10
      status: ATTACKED
      evidence: Cargo.toml and Cargo.lock byte-identical to origin/main; STATUS.md, briefs/next-sequence.md, .github untouched; the fork pin does not move.
      artifacts: [docs/spark-sql-iceberg-parity.md, Cargo.toml]
  complete: true
```

## 6. Red-first and mutation (docs/testing.md "Gate provocation proofs")

| # | provocation | pins that redden |
|---|---|---|
| R1 | the base dispatcher (round-robin) under the new Rust pins | five of seven stream pins: 32 files where 8 are expected on the identity, determinism, NULL, two-field and MERGE-entry pins; the router unit pin tests new code and the abort pin guards unchanged code, both green by design |
| R2 | the base release native (`origin/main` `b4933a99`) under the facade pins | both always-run layout pins: 32 files where 8 are expected (verbatim in C-001); the truncate guard stays green by design |
| M2 | `PartitionTransformExpr::evaluate` skips the cast to the source type | `truncate_over_a_view_typed_string_keys_on_the_cast_value` alone: the fork's `Unsupported data type for truncate transform: Utf8View`; the fourteen other pins stay green |
| M2-facade | the same drop under the facade truncate pin | NOTHING — refuted (C-002). DML planning casts the scan output to `Utf8` upstream, so the pin cannot bite the cast; it stays as the truncate-layout guard |

None is committed.

## 7. Design, and the alternatives

**Why a dispatcher router and not a `RepartitionExec`.** The brief's shape — wrap the input in
the same physical node before the stream is taken — does not fit a one-shot stream: a
repartition node re-drives its input once per output, so hanging one over a
`SendableRecordBatchStream` would drain it into the first output and starve the rest. The sound
options were buffering the whole input (the unbounded shape WD1 rejected) or routing each batch
as it arrives. `PartitionRouter` is the second: the writer's own `PartitionValueCalculator`
computes the key, `create_hashes` under DataFusion's seeded `REPARTITION_RANDOM_STATE` hashes
it, `take` splits the batch, and each part goes to its slot's worker over the existing bounded
channels. Same key family as the plan node, same seed, one pass, no spawn, no new input-driving
node.

**Why the cast stays.** The previous actor dropped it and no pin reddened — which is exactly
F-1's complaint, not its fix. The fork's `truncate` rejects `Utf8View` outright, so the cast is
the only thing standing between a view-typed input and a loud engine error. M2 pins it at the
expression level, where the input type is still visible.

**Why the MERGE seed is 400,000 rows.** At the 120,000-row seed the base funnel commits 8 files
for MERGE and the pin would pass vacuously; at 400,000 it commits 32. The threshold is measured
(8 at 120k, 32 at 400k on the base native), the mechanism is not chased — the pin sizes past it.

**Why `INSERT INTO` is out of reach, and the rejected wrapper.** `INSERT INTO`,
`saveAsTable(mode="append")` and `insertInto` lower to SQL that both doors delegate to
DataFusion, which executes the fork provider's `insert_into`. EXPLAIN shows the fork's
`IcebergWriteExec` plan with no `RepartitionExec` at any `target_partitions`, and the counts
prove passthrough (writers × values: 64 at 1e6, 32 on the four-file seed). A RePark-side fix
would mean wrapping the fork's catalog→schema→table providers to interpose a distribution node
— three new delegating wrappers across every Iceberg table registration. That is a semantic
rewrite of the write/commit path, not a narrow fix, and it is not built here. The fork-side
alternative is Q1.

**What changed in the determinism claim.** Nothing beyond WD1's: the row-to-file grouping on the
stream paths is now a function of the data (better than the batch lottery before), while the
order of rows inside a file follows worker timing. The manifest order and `_row_id` tiling are
unchanged and pinned; F-4's counts (§8) characterize the CTAS path this unit does not touch.

## 8. Measurement

[../../../docs/perf/iceberg-write-baseline.md](../../../docs/perf/iceberg-write-baseline.md) §9
carries every pass. The pair: base `b4933a99` against the branch, both on the pinned fork,
measured sequentially — the base release native first, then the fixed native — at load 10–13
before and 6–16 after, the control read in the same passes.

| cell | base | branch | gain | ratio to control, base → branch |
|---|---:|---:|---:|---|
| `iceberg_write/1000000/insert_overwrite` | 932.72 ms, 32 files | **805.68 ms, 8 files** | 1.16× | 8.59× → 7.38× |
| `iceberg_write/1000000/merge_insert` (single pass) | 892.42 ms, 32 files | **796.63 ms, 8 files** | 1.12× | — |
| `iceberg_write/1000000/insert_into` (untouched, fork) | 256.81 ms, 64 files | 271.82 ms, 64 files | box noise | — |
| `iceberg_write/1000000/save_as_table_append` (untouched, fork) | 249.63 ms, 64 files | 261.98 ms, 64 files | box noise | — |
| `df_write_parquet_zstd` (control) | 108.57 / 103.76 / 103.64 ms | 149.26 / 116.36 / 109.2 ms | — | — |
| RSS peak, overwrite cell | 711–692 MB | 597–613 MB | −~100 MB | — |
| Spark 4.1.2, same bed | 8 / 8 / 8 / 8 files | 8 / 8 / 8 / 8 files | — | — |

The unit's claim is the layout: 8 files where Spark writes 8. The overwrite wall moves 1.16×
on the best median (1.25× on the min) because 8 files close and commit faster than 32; the
pair was measured sequentially across a native swap, not interleaved, so the control-paired
ratios are the comparable figures, and the after pass-1 control (149.26 ms, spread 101) shows
a loaded-box outlier. The INSERT INTO cells are the box, not the branch: their code path is
the fork's, and 257 → 272 ms at 64 files is noise.

## 9. Questions for the owner

1. **The `INSERT INTO` fork ask (F-INSERT-DIST-1).** `INSERT INTO` and `saveAsTable(append)`
   write 64 files at 1e6 (32 on the facade seed) where Spark writes 8, because the fork's
   `insert_into` hash repartition does not take effect at runtime. Charter a fork unit to make
   the fork's distribution effective (or route one value to one task inside `IcebergWriteExec`),
   or rule these two statements out of the distribution scope? Built: everything else.
2. **The wrapper alternative.** If the fork cannot take it, is a RePark `TableProvider` wrapper
   that interposes the hash distribution before delegating to the fork's `insert_into` wanted
   as its own unit? Not built: three delegating wrappers across every table registration is a
   commit-path rewrite, not a narrow fix.

## 10. Gates

Every command in the brief, run on the lane, real exits. The `make verify` exit below is the
post-commit re-run on the unit's final tree; the suite exits were read before the docs commit
(which touches no code) and hold for it.

| gate | exit | result |
|---|---|---|
| `make verify` | 0 | `ci` (fmt, workspace clippy, `rust-panic-ban`, the structure gates, py-lint, py-format, lock, toml, spell) plus the Rust workspace suite — `repark-iceberg` 419 lib tests green, every crate green |
| `.venv/bin/python -m pytest python/repark/tests/test_write_distribution_1.py python/repark/tests/test_write_distribution_2.py python/repark/tests/test_perf_ice_writepath_1.py -q` | 0 | 13 passed, 5 skipped (the live legs, without `REPARK_PARITY_LIVE`) on the release native |
| the WD2 file under `REPARK_PARITY_LIVE=1` (Spark 4.1.2, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, ivy redirected into the lane) | 0 | 5 passed, 23.62 s, one JVM beside one foreign-lane JVM, mine exited with the run |
| `make py-test-facade` | 0 | 5,130 passed, 250 skipped in 979.69 s (the target's maturin step leaves a DEBUG native in the venv; the release native was restored afterwards and the 13 always-run pins re-read green) |
| `make py-test-dbt` | 0 | 59 passed, 1 skipped in 35.94 s |
| `make check-map-sync` | 0 | 221 maps clean |
| `make check-ledger-grammar` | 0 | all live ledgers clean, C-001..C-008 cited, C-009 OPEN with its evidence cell |
| `make check-ledgers` | 0 | bins, links and frozen rule clean |
| `make check-docs-compaction` | 0 | clean |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 | clean |
| `uvx typos@1.47.2 .` | 0 | clean |
| `git diff origin/main -- Cargo.toml Cargo.lock` | — | empty |

Disk: 889 GB free at hand-back; the lane's `target/` is 14 GB and `scratch/` 343 MB (the
1e6 bed, the before/after cell logs, the two release natives kept for the critic's re-measure),
both excluded from git. `.ivy2/` (the brief's ivy redirect) is untracked at the lane root and
never staged. No JVM or pytest this unit started is left running.

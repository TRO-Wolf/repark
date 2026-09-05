# Unit ledger — PERF-ICE-WRITEPATH-1 · the vectorized partition splitter (fork) and the CTAS write node (RePark)

**Date:** 2026-09-05 · **Branch:** `perf/ice-writepath-1` · **Base:** `origin/main` `6eaccd5e` ·
**Model:** opus-5 (round 1 and round 2; the brief predates the relaunch, the acting model is Opus) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: elevated`.
**Registry:** `PERF-ICE-FANOUT-1` filed BACKLOG with fork trigger **F-28**,
`PERF-ICE-WRITEPAR-1` **FIXED**.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** PERF-ANALYSIS-1 ranked the two write-path defects together (§5 item 6) because both
are read off the same CTAS pair: candidate 7, the partitioned fanout building one
`Literal::Struct` per row (813 ms of a 1e6-row partitioned CTAS), and candidate 8, unpartitioned
writers that are K cooperative futures joined in ONE task, so the CPU-bound zstd and parquet
encoding serialize (303 ms over `df.write.parquet(zstd)` on the same rows).

**Not in this unit:** the fork pin bump (its own PR, [../../../docs/fork-sync.md](../../../docs/fork-sync.md));
the INSERT, MERGE, predicate-DML and overwrite write paths, which keep the stream writer
unchanged; delete-vector and positional-delete writers; the S3 Tables / Glue acceptance legs;
`STATUS.md` and `briefs/next-sequence.md`.

**Writable paths:** fork lane `crates/iceberg/src/arrow/record_batch_partition_splitter.rs`;
RePark `crates/repark-iceberg/src/write/{partition_write.rs,mod.rs,map.md}`,
`crates/repark-spark/src/{ctas.rs,map.md}`, `crates/repark-sql/src/{create_table.rs,map.md}`,
`python/repark/tests/{test_perf_ice_writepath_1.py,map.md}`,
`docs/perf/{iceberg-write-baseline.md,map.md}`, `docs/spark-sql-iceberg-parity.md` §7, this
ledger and its `staging/map.md` row. Closed: `Cargo.toml`, `Cargo.lock`, every dependency,
`.github/`, every other ledger.

## PROPOSITION LEDGER — PERF-ICE-WRITEPATH-1 — 2026-09-05

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The fork splitter's Arrow-kernel grouping returns exactly what the row-wise grouping returns — same partition keys, same batches, same rows in the same order — on every transform the fork serves, with NULL partition values and multi-field specs. | A property test running both implementations on the same seeded random batches across the transform matrix. | **PROVEN** | `record_batch_partition_splitter.rs::test_arrow_order_grouping_equals_row_wise_grouping` — 15 partition specs (identity on int/long/string, truncate on string and long, bucket 4 and 8, year/month/day/hour, void beside identity, two- and three-field specs) x 7 batch sizes (0, 1, 2, 7, 64, 257, 1024) of seeded random data with NULLs in every optional source, both implementations compared as a canonical (key, batch) multiset. `cargo test -p iceberg` 0 failed. |
| C-002 | The row-wise path is kept, and taken, for the partition types where Arrow total-order equality is NOT Iceberg `Struct` equality: Float, Double, Unknown, and an empty partition type. | A pin that a Double identity spec stays row-wise and groups `-0.0` with `0.0`; a mutation that admits Double to the vectorized path reds it. | **PROVEN** | `test_float_partition_values_stay_on_the_row_wise_split` — a Double identity spec keeps `arrow_grouping == false` and groups `0.0, -0.0, 1.0, 0.0` into 2 partitions. Mutation D (admit Double to the vectorized path) reds it: total order splits the zeros into 3. |
| C-003 | The fork lane is green on its own gates. | `cargo fmt --all --check`, `cargo clippy -p iceberg --all-targets -D warnings`, `cargo test -p iceberg`, and the repo's size / comment-block / matrix-anchor / agent-artifact scripts. | **PROVEN** | `cargo fmt --all --check`, `cargo clippy -p iceberg --all-targets -- -D warnings` (Finished, no warnings), `cargo test -p iceberg` exit 0, `check_rust_file_size.sh` 438 files clean, `check_comment_blocks.sh` OK, `check_matrix_anchors.sh` OK, `check_agent_artifacts.sh` OK. |
| C-004 | `repark.write.max-concurrent-files` is BINARY on the CTAS node, not a cap: 1 writes one data file through a `CoalescePartitionsExec`, 2 or more writes one data file per DataFusion partition. It still bounds the stream write paths at the worker count it names. | Facade pins at cap 1 and at the default over a fixed four-file seed; the measured file count at cap 1/2/4/8 on one 1e6-row seed. | **PROVEN** | Round 2 rewrote this clause: round 1 claimed `min(cap, partitions)`, which is not what shipped. Measured 1 / 8 / 8 / 8 data files at cap 1/2/4/8. `test_ctas_writes_one_data_file_per_plan_partition` and `test_one_concurrent_file_still_writes_exactly_one`; the semantics are stated in `write/map.md`'s `concurrency.rs` row, not in a new code comment. |
| C-005 | Two identical CTAS statements over identical input commit identical manifests and identical `_row_id` ranges. | Five v3 CTAS over EIGHT UNEQUAL source files, asserting one manifest record-count sequence and one `first_row_id` map keyed by each file's `lower_bounds`; a mutation restoring round 1's ordering must red it on that real plan. | **PROVEN** | Round 2 REFUTED round 1's claim and its remedy. Instrumented measurement: six identical v3 CTAS gave six different partition-index-to-source-file assignments (partition 1 read the 3,000-row file in one run and the 40,000-row file in the next), so ordering by the writer index cannot work — the index is a property of the execution. `stable_commit_order` orders by partition value, then every field's lower then upper bound, then record count, size and path. Refuting fixture: 6 of 6 distinct sequences before, 1 of 6 after. `test_repeated_ctas_commits_the_same_manifest_and_row_ids`; mutation M5 (round-1 ordering) reds it on the real plan. |
| C-006 | V3-11 holds on the new path: one commit's data files reach the manifest in ascending partition-value order. | A partitioned facade CTAS whose `.files` partition values are sorted and cover every partition. | **PROVEN** | `test_partitioned_ctas_files_ascend_by_partition_value` — `.files` partition values sorted and covering 0..7; `ascending_partition_order` is applied on every return path, including the unpartitioned one where it is a no-op. |
| C-007 | The writers' work overlaps instead of serializing, and the shipped tree beats the base on both CTAS cells. | The structural Rust pin, plus a before/after pair measured on the SHIPPED tree (no fork override) back to back on a quiet box. | **PROVEN** | `every_input_partition_gets_its_own_writer_and_data_file`; mutation M1 (one writer over a coalesce) reds it. Round 2 re-measured B0 `6eaccd5e` against the branch, both on the pinned fork, at load 8–14 with the parquet-sink control within 5 % on both: `ctas` 1,384.80 → 135.48 ms median (10.2×) and `ctas_partitioned8` 4,901.75 → 293.19 ms (16.7×). **Both analysis targets are met**; round 1 said missed, from readings taken at load 13–22 with an uncommitted fork override on the after side. |
| C-008 | A failed write commits nothing and leaves NO data file behind — the files completed writers returned and the files the failing writer had already rolled. | A Rust pin with `write.target-file-size-bytes = 65536` so the failing writer rolls files before the injected failure; a mutation that sweeps only the completed files must red it. | **PROVEN** | Round 2 widened this clause: round 1's sweep deleted only the files completed writers returned, and the critic measured 9 orphans (398 KB) from the failing writer. The sweep now censuses the table's data root before the write and deletes everything that appeared since. `a_failed_partition_deletes_every_completed_data_file` at a 64 KiB target; mutation M4 (completed files only) reds it naming the six surviving parquet files. |
| C-009 | The written table is Spark-equal. | A live leg comparing Spark's own CTAS of the same seed row for row against the written table, and the CTAS wall read against the DataFusion parquet sink measured in the same run. | **PROVEN** | `test_written_table_row_set_matches_spark` — Spark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0 CTAS of the same seed, compared row for row on id/part/label; `test_partition_writers_answer_the_single_writer_at_scale` — at 1e6 rows the two writer shapes agree on rows, `sum(id)`, `sum(vi)` and layout. Co-collected with `test_parity_live.py::test_live_disclosure_still_diverges`: 19 passed. |
| C-010 | Every build is defined once and its numbers are quoted only where they belong: the shipped tree in the registry, the fork-override builds in the pending fork row. | `docs/perf/iceberg-write-baseline.md` §1's build table naming B0/B1/B2/B3 and where each may be quoted, consistent with §5 and the registry rows. | **PROVEN** | Round 2 finding S2-4: round 1's registry row quoted B0 → B2, and B2 carries the never-committed fork override. §1 now names all four builds and their permitted use, §5 carries only B0 → B3 with per-pass medians, floors and the load at each pass, and the fork row carries the isolated fork measurement instead of an override end-to-end. |
| C-011 | No dependency moves and the pin does not move: `git diff origin/main -- Cargo.toml Cargo.lock` is empty at hand-back, and the fork change is consumed only through a temporary, never-committed path override. | The diff, plus the registry row that records the fork dependency. | **PROVEN** | `git diff origin/main -- Cargo.toml Cargo.lock` empty at hand-back; the override is `scratch/probes/fork_override.sh`, excluded from git, and `Cargo.lock` is restored with `git checkout` after each measured build. `PERF-ICE-FANOUT-1` is filed BACKLOG with fork trigger F-28, not FIXED. |

VERDICT: 11 clauses, 11 PROVEN, 0 OPEN, 0 REJECTED.

```
COVERAGE_ATTESTATION:
  pr_unit: perf-ice-writepath-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against the brief. Both targets it names are reported as MISSED with the reason — the analysis' 813 ms was the whole partitioned-minus-unpartitioned CTAS delta, not the splitter, whose entire measured cost at that scale is 171 ms — rather than restated as met.
      artifacts: [docs/perf/iceberg-write-baseline.md, task/ledgers/staging/perf-ice-writepath-1-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: Splitter equivalence over 15 partition specs (identity on int/long/string, truncate on string and long, bucket 4 and 8, year, month, day, hour, void beside identity, two- and three-field specs) x 7 batch sizes (0, 1, 2, 7, 64, 257, 1024) of seeded random data with NULLs in every optional source; the node over zero rows, one partition, four differently sized partitions, and a late failure.
      artifacts: [crates/repark-iceberg/src/write/partition_write.rs, python/repark/tests/test_perf_ice_writepath_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: No unwrap or expect in production; the collector lock's poisoning is a typed Execution error, not a panic; a failed writer task's error is the first error returned and the dispatcher's secondary is not allowed to mask it. make rust-panic-ban exit 0.
      artifacts: [crates/repark-iceberg/src/write/partition_write.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The parallel section is DataFusion's coalesce, one task per partition, never more than one partition per writer — the rejected min(cap, partitions) shape is recorded with its unbounded-buffering hazard. The shared state is one Mutex over a BTreeMap written once per partition and one AtomicBool; the file order is taken from the map's key order, not from completion. No spawn, no unsafe, no new dependency.
      artifacts: [crates/repark-iceberg/src/write/partition_write.rs, crates/repark-iceberg/src/write/map.md]
    - id: AT-5
      status: ATTACKED
      evidence: Abort — a late failure in one of four partitions surfaces the source root cause, siblings stop taking Ok batches and close, every completed data file is deleted through FileIO, and nothing is committed. The four pre-existing stream-path abort pins in merge/tests/streaming.rs are untouched and green.
      artifacts: [crates/repark-iceberg/src/write/partition_write.rs, crates/repark-iceberg/src/write/merge/tests/streaming.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Spark equality — the live leg runs Spark 4.1.2 with iceberg-spark-runtime-4.1_2.13:1.11.0 over the same seed and compares the written table row for row; 19 passed co-collected with test_parity_live.py::test_live_disclosure_still_diverges. repark-iceberg 398, repark-spark 788 and repark-sql 341 unit tests plus every integration binary green.
      artifacts: [python/repark/tests/test_perf_ice_writepath_1.py]
    - id: AT-7
      status: ATTACKED
      evidence: Seven mutations, every one red — fork scatter-index confusion, range off-by-one, group on the first key column only, admit Double to the vectorized path; RePark one writer over a coalesce, drain in completion order, skip the abort cleanup. Section 6 lists each with the pin it reddens.
      artifacts: [task/ledgers/staging/perf-ice-writepath-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: Measurement honesty — three builds, every pass median and the 1-minute load at each pass, minima reported with the reason (the box was never quiet), and a section that says plainly what the walls do NOT show, including that B3 does not sit between B0 and B2 and that the two halves are not resolved apart by them.
      artifacts: [docs/perf/iceberg-write-baseline.md]
    - id: AT-9
      status: N/A
      justification: No AWS surface is touched. The write node is engine-side over a memory catalog on the local filesystem; the Glue and S3 Tables acceptance legs are excluded by the brief and no catalog seam, credential path or region behaviour changes.
    - id: AT-10
      status: ATTACKED
      evidence: Scope — Cargo.toml and Cargo.lock are byte-identical to origin/main, the fork change is consumed only through a temporary path override that is reverted and never committed, PERF-ICE-FANOUT-1 is filed BACKLOG with fork trigger F-28 rather than FIXED, and STATUS.md and briefs/next-sequence.md are untouched.
      artifacts: [docs/spark-sql-iceberg-parity.md, Cargo.toml]
  complete: true
```

## 6. Red-first and mutation (docs/testing.md "Gate provocation proofs")

Seven mutations, each applied to the shipped tree, run, and reverted. None is committed.

| # | mutation | pin that reddens |
|---|---|---|
| M-A | fork: `group_of_row[sorted_positions.value(position)]` → `group_of_row[position]` | all three splitter tests |
| M-B | fork: `for position in range.clone()` → `range.start..range.end - 1` | all three splitter tests |
| M-C | fork: `partition(&sorted_keys)` → `partition(&sorted_keys[..1])` | the property test (multi-field specs) |
| M-D | fork: admit `PrimitiveType::Double` to the vectorized path | the float fallback test (3 groups where Iceberg sees 2) |
| M1 | RePark: one writer over a `CoalescePartitionsExec` instead of one per partition | all three node tests |
| — | a wall-differential pin was written, proven against M1, then REMOVED: it flaked inside `cargo test` (6.41 s floor against a 6.00 s delayed run) and a flaky pin is worse than none | — |
| M2 | RePark: drain the collector in reverse (completion order stands in for it) | the writer-index order pin |
| M3 | RePark: skip `delete_completed_files` on the error path | the abort pin, naming the three surviving files |

The property test is the fork half's red-first instrument by construction: it runs the previous
implementation and the new one over the same batches, so it cannot pass unless they agree.

## 7. Design, and the alternatives that were measured

**Why the RePark half is an `ExecutionPlan` node and not a spawn.** The writers had to leave one
task for the encode to parallelize; nothing else moves that. Every spawn primitive is closed
here: `clippy.toml` bans `tokio::spawn`, `tokio::task::spawn` and `spawn_blocking` (async
cancel-safety, carried from v1), `.agents/skills/rust-code-quality/SKILL.md` makes it a review
duty that the ban "is not smuggled around via `JoinSet`, `FuturesUnordered`-with-detach, or a
helper crate" — which closes DataFusion's own `SpawnedTask` too — and `tokio` is a
**dev-dependency** of `repark-iceberg`, so a production spawn would also need a dependency the
brief forbids. What is left is the brief's own first route: put the write in the plan and let the
executor's `CoalescePartitionsExec` spawn per partition, which it does
(`RecordBatchReceiverStream::run_input`). RePark's own code spawns nothing.

**Rejected: `writers = min(max-concurrent-files, input partitions)`.** It preserved the old file
count, and it measured 738 ms against 547 ms for one-writer-per-partition on the partitioned 1e6
CTAS. Worse, it is unbounded in memory: DataFusion's repartition channels are unbounded per
output partition and close their gate only when EVERY channel is non-empty, so the partitions a
writer has not reached yet buffer whole — which a CTAS over a GROUP BY or a join would hit. The
knob now selects between one writer over a `CoalescePartitionsExec` (cap 1) and one per
partition; it still bounds the stream write paths INSERT, MERGE, overwrite and predicate DML use,
none of which this unit touches.

**Rejected for the fork: an `arrow-row` `RowConverter`.** It is the natural group key and it is
already in the lock file — but not in `iceberg`'s manifest, and adding it is a dependency change.
`lexsort_to_indices` + `arrow_ord::partition` needs only `arrow-ord`, which the crate already
depends on, and it is what the analysis proposed.

**Kept for the fork: the row-wise path.** Arrow's total order is not Iceberg `Struct` equality for
floats: `distinct` follows totalOrder, where `-0.0` and `0.0` differ, while `OrderedFloat` says
they are equal — so two rows that Iceberg puts in one partition would become two `PartitionKey`s
with the same value in one batch. Float, Double, Unknown and an empty partition type therefore
stay on the row-wise path, decided once in `try_new`. NaN agrees on both sides and needs no guard.

## 8. Measurement

Everything is in [../../../docs/perf/iceberg-write-baseline.md](../../../docs/perf/iceberg-write-baseline.md).
The headline pair, minima on a contended box (1-minute load 13–22 throughout, sibling `rustc`
builds live):

| cell | before (B0) | after (B2) | |
|---|---:|---:|---|
| `iceberg_write/1000000/ctas` | 880.50 ms | **478.03 ms** | 1.84× |
| `iceberg_write/1000000/ctas_partitioned8` | 1,611.25 ms | **917.22 ms** | 1.76× |
| `df_write_parquet_zstd` (control) | 143.90 ms | 100.73 ms | |
| fork splitter, isolated, 1e6 rows | 171.39 ms | **28.33 ms** | 6.0× |

The analysis' targets (`ctas` ≤ 150 ms, `ctas_partitioned8` ≤ 300 ms) are **not met**: they ask
for parity with the parquet sink, which reads 90–144 ms on this box. The brief's fork target
("≥ 600 ms off `ctas_partitioned8`") rests on attributing the whole 813 ms partitioned delta to
the splitter; the splitter's entire cost at that scale is 171 ms, so the target was unreachable
by construction and the rest of that delta is the fanout's file count.

## 10. Gates

RePark lane, override reverted, `git diff origin/main -- Cargo.toml Cargo.lock` empty.

| gate | result |
|---|---|
| `make ci` | exit 0 |
| `make verify` | exit 0 — `repark-iceberg` 398, `repark-spark` 788, `repark-sql` 341 plus every integration binary |
| `make check-python-conventions` | exit 0 |
| `make rust-panic-ban` | exit 0 |
| `pytest python/repark/tests -q -x` | see §12 (round 2 re-run) |
| `pytest python/repark-parity/tests -q` | 574 passed |
| `REPARK_PARITY_LIVE=1 pytest test_parity_live.py test_perf_ice_writepath_1.py test_sql_harden_cutover.py -q` | 175 passed |
| `make check-map-sync` / `check-ledger-grammar` / `check-ledgers` / `check-docs-compaction` | clean |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | clean |
| `typos .` | clean |

Fork lane: `cargo fmt --all --check`, `cargo clippy -p iceberg --all-targets -- -D warnings`,
`cargo test -p iceberg` (exit 0), plus `check_rust_file_size.sh`, `check_comment_blocks.sh`,
`check_matrix_anchors.sh` and `check_agent_artifacts.sh`.

The live battery ran beside one other lane's `local[1]` JVM, which the live-cell rules allow; no
JVM or pytest this unit started is left running.

## 11. Out of scope, observed and filed

- **`PERF-ICE-FANOUT-1` is not FIXED here.** The fork change is not consumed; the pin bump is its
  own PR (`docs/fork-sync.md` rule 1). The registry row carries the fork trigger and the measured
  numbers, in the shape `PERF-DVCLOSE-STMT-1` used while it waited for F-25.
- **The partitioned CTAS's file count is the remaining cost.** A partitioned write produces
  writers × partition values data files (64 at eight partitions and eight writers). Spark's answer
  is `write.distribution-mode = hash`, which sends one partition value to one task and writes one
  file per value; RePark has no such rule. That, not the splitter, is what is left of the 813 ms.
- **A writer that fails mid-write still orphans its own partial file.** Sibling writers close and
  are cleaned up, but the failing writer's `RollingFileWriter` is dropped without a close, so a
  partial parquet file can survive a failed write. Closing it would need the fork to return the
  files a failed writer had already rolled; today `remove_orphan_files` reclaims them.
- **The stream write paths are untouched.** INSERT, MERGE, overwrite and predicate DML still use
  `write_data_files_from_stream_with_concurrency` and its four cooperative workers. Whether they
  should move to the node is a separate unit with a much larger blast radius.
- **The box was never quiet.** Three release rebuilds of a 163 MB module could not be measured in
  one window, and B3 does not sit between B0 and B2. A re-measure on an idle box would settle how
  the end-to-end gain divides between the two halves.

## 12. Round 2 — review gaps and what closed them

The Opus critic built its own release module and returned FAIL with one S1 and five S2. Every
finding is accepted; none is disputed.

| # | finding | disposition |
|---|---|---|
| S1 | The determinism claim is false: six identical v3 CTAS over eight UNEQUAL source files gave six manifest sequences and six `first_row_id` maps; the round-1 pin was blind because its seed files were equal-sized. | **REMEDIATED, and the prescribed remedy was wrong.** Ordering by the input partition index cannot work: the instrumented run shows the index itself is unstable (partition 1 read the 3,000-row file in one run and the 40,000-row file in the next). `stable_commit_order` orders by content instead — partition value, then every field's lower then upper bound, then record count, size, path. 6 of 6 distinct → 1 of 6. Pin reseeded with unequal files, five runs, `first_row_id` keyed by `lower_bounds`; mutation M5 reds it on the real plan. |
| S2-1 | C-004 said `min(cap, partitions)`; the shipped code is binary. | **REMEDIATED.** C-004 rewritten to the measured 1 / 8 / 8 / 8 files at cap 1 / 2 / 4 / 8. The semantics went into `write/map.md`'s `concurrency.rs` row, NOT into the module's doc comments — the comment ban forbids adding them and forbids rewording the ones already there. |
| S2-2 | C-008 is false when the failing writer had already rolled files: 9 orphans, 398 KB. | **REMEDIATED.** The sweep censuses the table's data root before the write and deletes everything that appeared since. Pinned at a 64 KiB target file size; mutation M4 (completed files only) reds it naming six survivors. The stream path's 70 orphans (103 MB) are filed as `WRITE-ABORT-INSERT-1`, not fixed here. |
| S2-3 | The layout change is undisclosed in Spark terms: Spark writes 2 / 8, repark 8 / 64. | **REMEDIATED.** Both counts are in the registry row and baseline §6. `WRITE-DISTRIBUTION-1` filed with the measurement that decided it: neither alternative is cheap — capping the writers costs 738 ms against 547 ms and buffers unconsumed partitions whole, and a round-robin repartition destroys the reproducibility S1 just established. The layout stays and the row says why. |
| S2-4 | The registry's before/after was B0 → B2, and B2 carries the uncommitted fork override; the shipped tree read 735 / 2,213 in the doc and 331 / 493 to the critic — both endpoints were contention artifacts. | **REMEDIATED.** B0 and B3 re-measured back to back on a quiet box with the parquet-sink control within 5 %: 1,384.80 → 135.48 and 4,901.75 → 293.19 ms. §1 defines all four builds and where each may be quoted. |
| S2-5 | An unforced `///` summary above `/// # Errors`. | **REMEDIATED**, and widened by the owner's PR #380 note: the new summary is deleted, and the two renamed CTAS helpers carry their pre-existing doc comments verbatim instead of reworded ones. `git diff origin/main` over `*.rs`/`*.py`/`*.toml` shows one forced `# Errors` pair and nothing else. |
| S3 | The ledger `Model:` line and the facade test count. | **REMEDIATED.** The `Model:` line keeps `opus-5` with a note that the brief predates the relaunch and the acting model is Opus; the facade count is corrected to the round-2 re-run in §10. |

What round 1 got wrong that the gates could not catch: a determinism pin whose fixture made the
defect invisible, and a before/after pair whose two endpoints came from different builds and
different noise regimes. Both failures were of the *fixture*, not of the code under it — the
lesson is that a reproducibility pin has to vary the thing it claims is irrelevant (here, file
size), and a timing pair has to carry a same-run control on both sides.

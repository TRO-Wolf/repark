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
RePark `crates/repark-iceberg/src/write/{partition_write.rs,file_order.rs,mod.rs,map.md}`,
`python/repark-parity/bench/writepath/` and its `map.md` and the `bench/map.md` row,
`crates/repark-spark/src/{ctas.rs,map.md}`, `crates/repark-sql/src/{create_table.rs,map.md}`,
`python/repark/tests/{test_perf_ice_writepath_1.py,map.md}`,
`docs/perf/{iceberg-write-baseline.md,map.md}`, `docs/spark-sql-iceberg-parity.md` §7, this
ledger and its `staging/map.md` row. Closed: `Cargo.toml`, `Cargo.lock`, every dependency,
`.github/`, every other ledger.

## PROPOSITION LEDGER — PERF-ICE-WRITEPATH-1 — 2026-09-05

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The vectorized grouping returns the same partition keys and the same batches as the row-wise grouping, compared as a canonical multiset — every partition transform the fork serves, NULL partition values, multi-field specs, and every primitive type the allow-list admits. The GROUP ORDER changes: the vectorized path returns groups in sorted-key order where the row-wise path returned first-appearance order. | A property test running both implementations on the same seeded random batches, comparing after a canonical sort; the allow-list and the matrix must name the same types. | **PROVEN** | Round 2 F12: the claim is multiset-equality plus a deliberate order change, not order-equality — downstream both are re-ordered anyway (`ascending_partition_order` on the stream paths, `stable_commit_order` on the node), so the order the splitter returns is not observable in a commit. Round 2 F6: the matrix covered result types Int/Long/String only while the allow-list admitted 14; it now carries Boolean, Decimal(9,2), Time, Timestamp, Timestamptz, TimestampNs, TimestamptzNs, String, Uuid, Fixed(4), Binary, Date, Int and Long, with identity on each plus bucket, truncate, year/month/day/hour where the fork's transforms serve them. `lexsort_to_indices` and `arrow_ord::partition` accept all of them, Decimal128 and FixedSizeBinary included. `cargo test -p iceberg --lib` green. |
| C-002 | The row-wise path is kept, and taken, for the partition types where Arrow total-order equality is NOT Iceberg `Struct` equality: Float, Double, Unknown, and an empty partition type. | A pin that a Double identity spec stays row-wise and groups `-0.0` with `0.0`; a mutation that admits Double to the vectorized path reds it. | **PROVEN** | `test_float_partition_values_stay_on_the_row_wise_split` — a Double identity spec keeps `arrow_grouping == false` and groups `0.0, -0.0, 1.0, 0.0` into 2 partitions. Mutation D (admit Double to the vectorized path) reds it: total order splits the zeros into 3. |
| C-003 | The fork lane is green on its own gates. | `cargo fmt --all --check`, `cargo clippy -p iceberg --all-targets -D warnings`, `cargo test -p iceberg`, and the repo's size / comment-block / matrix-anchor / agent-artifact scripts. | **PROVEN** | `cargo fmt --all --check`, `cargo clippy -p iceberg --all-targets -- -D warnings` (Finished, no warnings), `cargo test -p iceberg` exit 0, `check_rust_file_size.sh` 438 files clean, `check_comment_blocks.sh` OK, `check_matrix_anchors.sh` OK, `check_agent_artifacts.sh` OK. |
| C-004 | `repark.write.max-concurrent-files` is BINARY on the CTAS node, not a cap: 1 writes one data file through a `CoalescePartitionsExec`, 2 or more writes one data file per DataFusion partition. It still bounds the stream write paths at the worker count it names. | Facade pins at cap 1 and at the default over a fixed four-file seed; the measured file count at cap 1/2/4/8 on one 1e6-row seed. | **PROVEN** | Round 2 rewrote this clause: round 1 claimed `min(cap, partitions)`, which is not what shipped. Measured 1 / 8 / 8 / 8 data files at cap 1/2/4/8. `test_ctas_writes_one_data_file_per_plan_partition` and `test_one_concurrent_file_still_writes_exactly_one`; the semantics are stated in `write/map.md`'s `concurrency.rs` row, not in a new code comment. |
| C-005 | At every partition count the CTAS commit is an ORDERING: the manifest ascends by content, `_row_id` tiles it contiguously from zero, the row set and its sums are invariant, and two runs with the same file grouping commit the same `_row_id` ranges. The committed LAYOUT is NOT reproducible, because the scan's grouping is not. | Five v3 CTAS over eight UNEQUAL source files at 4 AND 16 partitions; ten consecutive runs of the pin on four cores and on all cores; a mutation restoring round 1's ordering must red it at both counts. | **PROVEN** | Refuted twice before it was true. Round 1 ordered by writer index — refuted, the index is not stable. Round 2 ordered by content and asserted the record-count sequence — refuted by CI's 4-core runner, and reproduced here: `target_partitions = 4` gives 4-6 distinct groupings in 10 runs against 1 in 10 at 16, because DataFusion packs the 3,000-row and 1,000-row files into one writer in some runs and two in others. Round 3 asserts what survives any grouping: 10/10 and 10/10 for lows-sorted, `_row_id`-contiguous and row-set-invariant at both counts; the pin passed 10 consecutive runs under `taskset -c 0-3` and 10 on all cores; mutation M5 reds it at 4 and at 16. The residual is `WRITE-GROUPING-CTAS-1`. |
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
      evidence: Every clause walked against the brief, and every one that overstated has been retracted in the round it was refuted — C-004's cap, C-005's determinism (twice), C-007's targets, C-008's sweep. The gain is reported as a ratio to a same-run control because two boxes disagreed 28 percent on the absolute and 8 percent on the ratio. The fork target the brief names is reported unreachable by construction with the measurement that shows why.
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
| — | M2 (drain the collector in reverse) was proven against the round-1 code and then RETIRED in round 3: `stable_commit_order` re-sorts, so the drain order is no longer observable and the mutation is green on the shipped tree. M5 is what guards the ordering now. |
| — | a wall-differential pin was written, proven against M1, then REMOVED: it flaked inside `cargo test` (6.41 s floor against a 6.00 s delayed run) and a flaky pin is worse than none | — |
| M4 | RePark: sweep only the completed files (the pre-round-2 abort behaviour) | the abort pin, naming the six parquet files the failing writer had rolled |
| M5 | RePark: return `ascending_partition_order` instead of `stable_commit_order` | the round-2 determinism pin on a real plan, AND the round-3 ordering pin at 3, 4, 8 and 16 partitions |
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

Everything is in [../../../docs/perf/iceberg-write-baseline.md](../../../docs/perf/iceberg-write-baseline.md),
which §1 of that file scopes build by build. The pair below is the SHIPPED tree — base `6eaccd5e`
against the branch, both on the pinned fork, measured back to back on a quiet box (load 7.7-14.4)
with the `df.write.parquet(zstd)` control inside 5 % on each side. Round 1's table quoted B0 → B2,
where B2 carried a never-committed fork override; it is superseded and no B2 number appears in a
registry row.

| cell | B0 | B3 (shipped) | gain | ratio to control, B0 → B3 |
|---|---:|---:|---:|---|
| `iceberg_write/1000000/ctas` | 1,384.80 ms | **135.48 ms** | 10.2× | 12.90× → 1.28× |
| `iceberg_write/1000000/ctas_partitioned8` | 4,901.75 ms | **293.19 ms** | 16.7× | 45.65× → 2.78× |
| fork splitter, isolated, 1e6 rows | 171.39 ms | **28.33 ms** | 6.0× | — |

The round-2 critic re-measured the shipped tree on its own box at load 11.8-12.3 and read
173.80 / 377.04 ms against a 125.73 ms control — 1.38× / 3.00× of control against this box's
1.28× / 2.78×. **The ratios agree to within 8 % and the gains reproduce; the absolutes differ by
28-29 %.** So the gain is the result, and the analysis' absolute targets (`ctas` ≤ 150 ms,
`ctas_partitioned8` ≤ 300 ms) are load-qualified: met here, not met there, and inside the spread
either way. Round 1 called them missed and round 2 called them met; both were reading the load.

## 10. Gates

RePark lane, override reverted, `git diff origin/main -- Cargo.toml Cargo.lock` empty.

| gate | result |
|---|---|
| `make ci` | exit 0 |
| `make verify` | exit 0 — `repark-iceberg` 398, `repark-spark` 788, `repark-sql` 341 plus every integration binary |
| `make check-python-conventions` | exit 0 |
| `make rust-panic-ban` | exit 0 |
| `pytest python/repark/tests -q` | round 3 on the merged tree: **4,828 passed, 200 skipped**. Round 2 read 4,805 / 200 on this lane (round-2 re-run on the shipped tree). The round-2 critic measured 4,803 / 202 on its own module: two live-oracle tests skip without `pyspark` in the environment, and this lane has it installed. The count is environment-dependent, not tree-dependent. |
| `pytest python/repark-parity/tests -q` | 574 passed |
| the C-005 pin, 10 consecutive runs under `taskset -c 0-3` and 10 on all cores | 20 of 20 green |
| `cargo test -p repark-spark --lib v3_row_order` | 6 passed (the V3-11 order pins) |
| `REPARK_PARITY_LIVE=1 pytest test_parity_live.py test_perf_ice_writepath_1.py test_sql_harden_cutover.py -q` | round 3: **176 passed** (the ordering pin is parametrized over 4 and 16) |
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

## 13. Round 3 — the CI red, and the third statement of the same clause

CI's wheels workflow ("build + import smoke (debug, host)", a 4-core runner) reddened round 2's
determinism pin: two runs in one process grouped the 3,000-row and 1,000-row source files into one
writer and then into two. Reproduced locally under `taskset -c 0-3`.

| `target_partitions` | distinct file groupings in 10 runs | distinct `_row_id` maps | round-2 pin (sequence equality) | round-3 pin (ordering invariants) |
|---|---:|---:|---|---|
| 4 (CI's shape) | **4-6** | 4-6 | **RED** | 10/10 green, and 10/10 again on all cores |
| 16 (this box's shape) | 1 | 1 | green | 10/10 green |

The round-2 boundary — "reproducible whenever the scan's row-to-file grouping is stable" — was
crossed by the unit's own pin, because the grouping is not stable at a small core count even
within one process. Option (a) was rejected on measurement: the only writer-side lever is to stop
the scan repartitioning files, which collapses a single-file source to one writer and gives back
the parallelism this unit exists to deliver. Option (b) is taken and the claim is narrowed
everywhere it appears — the commit is an ordering, not a layout, and `_row_id` follows the
grouping. `WRITE-GROUPING-CTAS-1` files the residual with the numbers above and names the scan as
where a fix belongs. `WRITE-ORDER-INSERT-1` is adjusted: sorting the stream path the same way
would buy it the same ordering and the same non-reproducible layout.

Three rounds, three statements of C-005, each refuted by a fixture the previous one did not vary:
round 1 fixed the file sizes, round 2 fixed the partition count, round 3 varies both. The lesson
for the next reproducibility pin is in that sentence.

## 14. Round 3 — the round-2 critic's twelve findings

RePark half FAIL, fork half PASS with one S2. Every finding accepted; none disputed.

| # | sev | finding | disposition |
|---|---|---|---|
| F1 | S1 | `stable_commit_order` is a total order on the files, but the file SET is not a function of the statement; the pin was green only because `shuffle.partitions = 8` met its eight source files 1:1 (=4 gives 2 of 3, =3 gives 3 of 5 distinct). | **REMEDIATED.** C-005, the registry row and baseline §7 retracted to the honest claim; the pin now runs at 3, 4, 8 and 16 and asserts what survives any grouping. 20 of 20 consecutive runs green (10 on four cores, 10 on all). Residue filed as `WRITE-GROUPING-CTAS-1`. |
| F2 | S1 | Mutation M2 (drain in reverse) is GREEN on the shipped tree. | **REMEDIATED.** M2 retired with the reason (the drain order stopped being observable when `stable_commit_order` landed); M5 is the mutation that guards the ordering, and §6 says so. |
| F3 | S1 | Ledger §8 and AT-1 still carried round 1's B2 numbers and "targets not met". | **REMEDIATED.** §8 rewritten to B0 → B3 with the ratio-to-control table and the critic's independent reading; AT-1 rewritten. |
| F4 | S2 | "Both targets met" did not reproduce at load 11.8-12.3 (173.80 / 377.04 against a 125.73 control). | **REMEDIATED.** The ratios agree to 8 % and the gains reproduce; the absolutes differ 28-29 %. The unqualified claim is dropped everywhere and replaced by the gain plus a load qualification. |
| F5 | S2 | M4 and M5 absent from §6; C-005's mutation obligation undischarged. | **REMEDIATED.** Both added with the pin each reds; M5 reds the round-3 pin at all four partition counts. |
| F6 | S2 | The fork allow-list admits 14 primitive types; the matrix covered Int/Long/String. | **REMEDIATED** on the fork lane (`5f25acc08`): the matrix now covers all 14, including Decimal128 and FixedSizeBinary, which `lexsort_to_indices` and `arrow_ord::partition` do accept. Two combinations stay out because the fork's TRANSFORMS refuse them, not the splitter. |
| F7 | S2 | `delete_attempt_files` swept every uncensused file under the table's data root. | **REMEDIATED.** The sweep arm is gated on the table having no current snapshot and falls back to the completed files otherwise; pinned both ways. |
| F8 | S2 | Baseline §3 cited untracked probes; §6's `sum(vi)` did not match the tracked fixture. | **REMEDIATED.** The probes are tracked at `python/repark-parity/bench/writepath/`; §6 names both fixtures and both constants. |
| F9 | S3 | `file_order.rs` missing from the writable set. | **REMEDIATED**, with the bench directory. |
| F10 | S3 | Stale "three builds" and "three CTAS runs" in two maps. | **REMEDIATED.** |
| F11 | S3 | An assertion message and a map line described round-1 behaviour. | **REMEDIATED**, both now say content order. |
| F12 | S3 | C-001's "same rows in the same order" is proved only after sorting. | **REMEDIATED.** C-001 states multiset equality plus a deliberate group-order change, and why it is unobservable downstream. |

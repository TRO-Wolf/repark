# Unit ledger — PERF-ICE-WRITEPATH-1 · the vectorized partition splitter (fork) and the CTAS write node (RePark)

**Date:** 2026-09-05 · **Branch:** `perf/ice-writepath-1` · **Base:** `origin/main` `6eaccd5e` ·
**Model:** opus-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
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
| C-001 | The fork splitter's Arrow-kernel grouping returns exactly what the row-wise grouping returns — same partition keys, same batches, same rows in the same order — on every transform the fork serves, with NULL partition values and multi-field specs. | A property test running both implementations on the same seeded random batches across the transform matrix. | **OPEN** | |
| C-002 | The row-wise path is kept, and taken, for the partition types where Arrow total-order equality is NOT Iceberg `Struct` equality: Float, Double, Unknown, and an empty partition type. | A pin that a Double identity spec stays row-wise and groups `-0.0` with `0.0`; a mutation that admits Double to the vectorized path reds it. | **OPEN** | |
| C-003 | The fork lane is green on its own gates. | `cargo fmt --all --check`, `cargo clippy -p iceberg --all-targets -D warnings`, `cargo test -p iceberg`, and the repo's size / comment-block / matrix-anchor / agent-artifact scripts. | **OPEN** | |
| C-004 | A CTAS writes one data file per writer, and the writer count is `min(repark.write.max-concurrent-files, input partitions)` — the session knob still decides the file count. | Facade pins at cap 4 and cap 1 over one seed, with the row set and `sum(id)` equal on both. | **OPEN** | |
| C-005 | After the parallel section the data-file order is a function of the plan, not of completion order: repeated CTAS of one seed produces the same manifest record-count sequence, so the `_row_id` derived from it is reproducible. | Three facade runs compared; a Rust pin over four differently sized partitions asserting the writer-index sequence; a mutation that drains in completion order reds it. | **OPEN** | |
| C-006 | V3-11 holds on the new path: one commit's data files reach the manifest in ascending partition-value order. | A partitioned facade CTAS whose `.files` partition values are sorted and cover every partition. | **OPEN** | |
| C-007 | The writers' work overlaps instead of serializing. | A Rust differential pin: an injected blocking delay costs one writer four times what it costs four writers; a mutation forcing one writer reds it. Plus the measured CTAS walls. | **OPEN** | |
| C-008 | A failed write commits nothing and leaves no completed data file behind: the first error surfaces, sibling writers stop and close, and every file they completed is deleted through `FileIO`. | A Rust pin injecting a late failure in one partition; a mutation that skips the cleanup reds it. | **OPEN** | |
| C-009 | The written table is Spark-equal. | A live leg comparing Spark's own CTAS of the same seed row for row against the written table, and the CTAS wall read against the DataFusion parquet sink measured in the same run. | **OPEN** | |
| C-010 | The before/after numbers are measured on a release module on three builds (base, fork-only, both) and filed with their fixture, iteration count and load. | `docs/perf/iceberg-write-baseline.md` with the tables and the commands. | **OPEN** | |
| C-011 | No dependency moves and the pin does not move: `git diff origin/main -- Cargo.toml Cargo.lock` is empty at hand-back, and the fork change is consumed only through a temporary, never-committed path override. | The diff, plus the registry row that records the fork dependency. | **OPEN** | |

## 6. Red-first and mutation (docs/testing.md "Gate provocation proofs")

## 7. Design, and the alternatives that were measured

## 8. Measurement

## 10. Gates

## 11. Out of scope, observed and filed

# Unit ledger — WRITE-DISTRIBUTION-1 · a hash distribution rule before a partitioned Iceberg write

**Date:** 2026-09-06 · **Branch:** `perf/write-distribution-1` · **Base:** `origin/main` `57f21b9b` ·
**Model:** claude-fable-5-1 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: standard`.
**Registry:** `WRITE-DISTRIBUTION-1` **FIXED**.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** PERF-ICE-WRITEPATH-1 left the partitioned CTAS writing writers × partition values
data files — 64 at `spark.sql.shuffle.partitions = 8` where Spark writes 8 — because RePark had
no distribution rule before the write. Spark's Iceberg default `write.distribution-mode = hash`
repartitions by partition value so one value goes to one task. Two alternatives were measured
and rejected there: capping the writers below the partition count (738 ms against 547 ms,
unbounded buffering in DataFusion's repartition channels) and a round-robin `RepartitionExec`
(a shared counter, which destroys the row-to-file reproducibility `stable_commit_order` gives).

**Not in this unit:** the unpartitioned CTAS layout (left untouched, §7); the stream write paths
(INSERT, MERGE, overwrite, predicate DML); the fork pin; `STATUS.md`; `briefs/next-sequence.md`.

**Writable paths:** `crates/repark-iceberg/src/write/{distribution.rs,partition_write.rs,mod.rs,map.md}`,
`python/repark/tests/{test_write_distribution_1.py,test_perf_ice_writepath_1.py,map.md}`,
`python/repark-parity/bench/writepath/{probe_cell.py,map.md}`,
`docs/perf/{iceberg-write-baseline.md,map.md}`, `docs/spark-sql-iceberg-parity.md` §7, this
ledger and its `staging/map.md` row. Closed: `Cargo.toml`, `Cargo.lock`, every dependency,
`.github/`, every other ledger.

## Plan

- [x] Reproduce on the base tree with a release native: `ctas` and `ctas_partitioned8` file
      counts and walls, the `df.write.parquet(zstd)` control in the same passes.
- [x] Facade pins red on the base tree (`test_write_distribution_1.py`).
- [x] Rust pins red against the unwired rule, then the rule: a `RepartitionExec` keyed by
      `Partitioning::Hash` over one `PartitionTransformExpr` per partition field, placed under
      `IcebergPartitionWriteExec` when the table is partitioned and there is more than one writer.
- [x] Measure after with the same probes; RSS peak for the partitioned cell; Spark's live count.
- [x] Registry row, baseline §6 and §8, maps, attestation, gates.

## PROPOSITION LEDGER — WRITE-DISTRIBUTION-1 — 2026-09-06

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | On a partitioned table with more than one writer, every row of one partition value reaches exactly one writer, so the commit holds one data file per partition value present. | A Rust pin over four input partitions that each carry all eight values; the facade pin over the four-file seed at `shuffle.partitions = 8`. Both red on the base tree. | **PROVEN** | `one_partition_value_lands_in_exactly_one_writer`: red before the rule was wired with 32 files of 8 rows (`[(Int(0), 8), (Int(0), 8), (Int(0), 8), (Int(0), 8), (Int(1), 8), …]`), green after with 8 files of 32. `test_partitioned_ctas_writes_one_data_file_per_partition_value`: red on the base native — `[(0, 3750, 0), (0, 3750, 3750), (0, 3750, 7500), (0, 3750, 11250), (1, 3750, 15000), …]`, 32 files — green after with `[0..7]`, 15,000 rows each. |
| C-002 | The distribution is a deterministic function of the partition values — DataFusion's seeded `REPARTITION_RANDOM_STATE`, not a shared counter — so two runs of one plan commit the same `(partition value, record count)` manifest sequence. | A Rust pin running the same plan twice over unequal input partitions and asserting both sequences equal the expected one-per-value layout. | **PROVEN** | `the_distribution_is_deterministic_across_runs` over inputs of 40/24/64/8 rows: red before (32 files, `(Int(0), 5), (Int(0), 3), (Int(0), 8), (Int(0), 1), …`), green after with eight files of 17 in both runs. The key expression holds no state; the random state is DataFusion's `SeededRandomState::with_seed(0)`. |
| C-003 | Input partitions that carry no rows do not change the rule, and an input with no rows commits no file. | A Rust pin where only two of four input partitions carry rows, and one over zero rows. | **PROVEN** | `input_partitions_without_rows_do_not_split_a_value`: red before (16 files of 8 over the two carrying partitions), green after (8 files of 16); the zero-row arm commits nothing on both sides. |
| C-004 | A NULL partition value is one value: its rows from every input partition land in one writer and one file. | A Rust pin on a nullable string identity partition with NULLs in every input partition; the facade pin `PARTITIONED BY (label)` with NULLs in every seed file. | **PROVEN** | `null_partition_values_share_one_writer`: red before (`[None] × 4, [l0] × 4, [l1] × 4, [l2] × 4`, 16 files), green after with one NULL file of 64 rows and one per label. `test_null_partition_value_lands_in_one_data_file`: red on the base native (`[(None, 6000), (None, 6000), (None, 6000), (None, 6000), ('l0', 8000), …]`), green after with `[None, l0, l1, l2]` and 24,000 NULL rows in the one file. DataFusion's `create_hashes` leaves a NULL's hash untouched, so every NULL hashes alike. |
| C-005 | The key is the partition-transform value, not the source column: `bucket(4, id)` and `day(ts)` commit one file per `(bucket, day)`. | A Rust pin over a two-field spec whose source columns would spread each bucket over every writer if hashed raw; the facade pin `PARTITIONED BY (bucket(4, id))`; a mutation that hashes the raw source column. | **PROVEN** | `bucket_and_day_transforms_key_on_the_transformed_value`: red before (32 files), green after (8 files, one per `(bucket, day)`, keys unique). Mutation M1 (skip the transform, hash the cast source column) reds this pin ALONE — 32 files — while the six identity pins stay green, which is the branch's nameable input. `test_bucket_partitioned_ctas_writes_one_data_file_per_bucket`: red on the base native (16 files, `[0, 0, 0, 0, 1, …]`), green after with `[0, 1, 2, 3]`. |
| C-006 | The unpartitioned CTAS is untouched: one data file per input partition, and `test_perf_ice_writepath_1.py` stays green unchanged. | The existing pins, plus a Rust pin that an unpartitioned table bypasses the rule. | **PROVEN** | `an_unpartitioned_table_keeps_one_file_per_input_partition` (4 → 4 files, green before and after by design — it guards the decision, not the fix); `test_ctas_writes_one_data_file_per_plan_partition` and `every_input_partition_gets_its_own_writer_and_data_file` unchanged and green; `test_perf_ice_writepath_1.py` 7 passed, 2 skipped on the base native and on the branch before any pin in it was touched; the one pin it then changed (§6) tightened the partitioned count only. |
| C-007 | The partitioned 1e6 CTAS writes Spark's file count and its wall does not regress beyond the control's spread; the unpartitioned cell does not move. | Three passes of five per cell on a release native before and after, the `df.write.parquet(zstd)` control in the same passes, RSS peak, and the live Spark 4.1.2 layout of the same seed. | **PROVEN** | §8: `ctas_partitioned8` 64 → **8** files, 361.16 → 204.91 ms best median (348.16 → 191.38 min), **3.44× → 1.96×** of the control (104.96 / 104.60 ms, inside 1 %), base and branch measured back to back at load 11–17 with the same release native swapped; the earlier base set at load 5–9 read 464.99–554.95 ms, so the gain holds against both. RSS peak 760–785 → 842–861 MB. `test_partitioned_ctas_file_count_matches_spark`: live Spark 4.1.2 writes the same eight `(value, record_count)` files. `ctas`: the code path is bypassed (C-006); 134.07 ms after at 1.28× of control, PERF-ICE-WRITEPAR-1's shipped ratio. |
| C-008 | No dependency moves and RePark spawns nothing: `git diff origin/main -- Cargo.toml Cargo.lock` is empty and the node adds no `tokio::spawn`. | The diff; `make rust-panic-ban` (the only gate where `disallowed-methods` is live). | **PROVEN** | `git diff origin/main -- Cargo.toml Cargo.lock` empty; `make rust-panic-ban` exit 0. `RepartitionExec`'s own tasks are DataFusion's, as `CoalescePartitionsExec`'s are on the node already. |
| C-009 | The commit stays an ordering on the partitioned path: the manifest ascends by partition value and `_row_id` tiles it contiguously from zero. What changes is stated: a file's row order follows the channel interleaving of the input partitions, which is also not fixed in Spark. | The v3 facade pin's `first_row_id` tiling; §7. | **PROVEN** | `test_partitioned_ctas_writes_one_data_file_per_partition_value` asserts `first_row_id == [0, 15000, …, 105000]` over the ascending partition values on a v3 table; `test_partitioned_ctas_files_ascend_by_partition_value` keeps V3-11. The row-order change is stated in the registry row, baseline §8 and `write/map.md`; Spark's shuffle reader fetches blocks in a randomized order, so it makes no stronger promise. |

VERDICT: 9 clauses, 9 PROVEN, 0 OPEN, 0 REJECTED.

```
COVERAGE_ATTESTATION:
  pr_unit: write-distribution-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause is stated against a measurement or a red-then-green pin. The gain is reported as a ratio to a same-pass control because the box was loaded (11-17) and the unpartitioned cell swung 3x on the disk alone; the unpartitioned decision is stated as a decision with its reason, not as a result.
      artifacts: [docs/perf/iceberg-write-baseline.md, task/ledgers/staging/write-distribution-1-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: Identity int, identity nullable string with NULLs, bucket(4) + day over a timestamp, unequal input partitions, input partitions with no rows, an empty input, a plan lacking the source column, an unpartitioned table; the facade over identity, bucket and NULL labels; live Spark on the identity seed.
      artifacts: [crates/repark-iceberg/src/write/distribution.rs, python/repark/tests/test_write_distribution_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: No unwrap or expect in production; a missing source column is a typed Plan error; the cast in the key expression is Arrow's safe cast so an invalid value becomes a NULL key and the writer's own strict cast raises the error the write would raise anyway. make rust-panic-ban exit 0.
      artifacts: [crates/repark-iceberg/src/write/distribution.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The key expression holds no state and the hash is seeded; the repartition is DataFusion's node, every output consumed concurrently by its own writer task; the writers-below-partitions shape is not reintroduced and RSS is measured. No spawn, no unsafe, no new dependency.
      artifacts: [crates/repark-iceberg/src/write/distribution.rs, crates/repark-iceberg/src/write/map.md]
    - id: AT-5
      status: ATTACKED
      evidence: The abort path is unchanged and its two pins stay green; a failing input surfaces through RepartitionExec as before, since the node's take_while and sweep sit above it.
      artifacts: [crates/repark-iceberg/src/write/partition_write.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Live Spark 4.1.2 writes the same (partition value, record count) layout for the seed; the written row set and sums are unchanged (sum(id), count(*) asserted on the facade pin). 13 passed under REPARK_PARITY_LIVE=1 across both pin files, one JVM, killed at exit.
      artifacts: [python/repark/tests/test_write_distribution_1.py]
    - id: AT-7
      status: ATTACKED
      evidence: Mutation M1 (hash the raw source column) reds the transform pin alone; the unwired rule reds five of seven Rust pins and three of three always-run facade pins. Section 6.
      artifacts: [task/ledgers/staging/write-distribution-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: Two base sets and one branch set, every pass median, the load at every pass, RSS at every pass, and a paragraph on what the unpartitioned cell does NOT show.
      artifacts: [docs/perf/iceberg-write-baseline.md]
    - id: AT-9
      status: N/A
      justification: No AWS surface is touched; the rule is a physical-plan node over a memory catalog on the local filesystem, and no catalog seam, credential path or region behaviour changes.
    - id: AT-10
      status: ATTACKED
      evidence: Cargo.toml and Cargo.lock byte-identical to origin/main; STATUS.md, briefs/next-sequence.md, .github untouched; the fork pin does not move.
      artifacts: [docs/spark-sql-iceberg-parity.md, Cargo.toml]
  complete: true
```

## 6. Red-first and mutation (docs/testing.md "Gate provocation proofs")

| # | provocation | pins that redden |
|---|---|---|
| R1 | the rule not wired (`mod distribution;` declared, `hash_distribution` not called) | five of seven Rust pins: 32 files where 8 are expected on the identity, determinism, sparse-input and bucket pins, 16 on the NULL pin; the two guards (unpartitioned bypass, missing column) stay green by design |
| R2 | the base release native (`origin/main` `57f21b9b`) under the facade pins | all three always-run facade pins: 32 / 16 / 16 files where 8 / 4 / 4 are expected (verbatim in C-001, C-004, C-005) |
| M1 | `PartitionTransformExpr::evaluate` returns the cast source column without the transform | `bucket_and_day_transforms_key_on_the_transformed_value` alone: 32 files, keys not unique; the six other pins stay green |
| — | `test_partitioned_ctas_files_ascend_by_partition_value` was green on both sides with its old assertion (`sorted` and `set == 0..7` hold for 32 files); it now asserts exactly one file per value and reds on the base native | — |

None is committed.

## 7. Design, and the alternatives

**Why `RepartitionExec` and a `PhysicalExpr`, not a node of RePark's own.** The brief's rule is
Spark's: one partition value to one task, deterministically. DataFusion already has the node —
`RepartitionExec` under `Partitioning::Hash` evaluates its expressions per batch, hashes the
result with a seeded `RandomState`, and splits the batch with `take` into per-output channels.
What it lacked is an expression that yields the partition value. `PartitionTransformExpr` is that
expression: the source column (a `Column` child, so `with_new_children` is natural), cast to the
Iceberg field's Arrow type when the plan's type differs (a parquet scan yields `Utf8View` where the
writer conforms to `Utf8`), then the fork's `create_transform_function` — the same functions the
writer's `PartitionValueCalculator` applies, so the key IS the writer's key. A node of RePark's
own would have had to drive N inputs into N outputs, which is the spawn the ban closes.

**Why not hash the raw source columns.** Correct for identity transforms and simpler, but a
bucket or a day would then spread one partition value over every writer — the file count would
stay writers × values for the transforms Spark's rule was made for. Mutation M1 measures the
difference: 32 files against 8.

**Why the unpartitioned CTAS stays untouched.** Spark's 2 files at 8 shuffle partitions are the
scan's split count for one 29 MB parquet; there is no distribution rule to copy. Coalescing to
fewer writers is the writers-below-partitions shape PERF-ICE-WRITEPATH-1 measured at 738 ms
against 547 ms with unbounded buffering, and a coalesce to 2 would give back most of the 10×
that unit delivered. The conservative option is built; whether a target file count for
unpartitioned writes is wanted is filed as the owner's question below.

**Why `cast`, not the strict cast.** The key only needs to be a deterministic function of the
value. An uncastable value becomes a NULL key here and the write then fails in the writer's own
strict cast, which is the error the write raised before this unit; a strict cast in the key
would add a second error site for the same input.

**What changed in the determinism claim.** On the partitioned path the row-to-file grouping is
now a function of the data — better than before, where it followed the scan's grouping — but
the order of rows inside a file follows the order in which the eight input tasks reach the
channel, so a given row's `_row_id` is not reproducible. Spark's shuffle reader fetches blocks in
a randomized order, so Spark makes no stronger promise. The manifest order and `_row_id` tiling
are unchanged and pinned; the unpartitioned path keeps §7 of the baseline unchanged.

## 8. Measurement

[../../../docs/perf/iceberg-write-baseline.md](../../../docs/perf/iceberg-write-baseline.md) §8
carries every pass. The pair: base `57f21b9b` against the branch, both on the pinned fork, the
same release native swapped in the same venv, measured back to back at load 11–17.

| cell | base | branch | gain | ratio to control, base → branch |
|---|---:|---:|---:|---|
| `iceberg_write/1000000/ctas_partitioned8` | 361.16 ms, 64 files | **204.91 ms, 8 files** | 1.76× | 3.44× → 1.96× |
| `iceberg_write/1000000/ctas` (untouched path) | 337.13 ms, 8 files | 134.07 ms, 8 files | box noise | 3.21× → 1.28× |
| `df_write_parquet_zstd` (control) | 104.96 ms | 104.60 ms | — | — |
| RSS peak, partitioned cell | 760–785 MB | 842–861 MB | +~80 MB | — |

The unpartitioned row is the box, not the branch: its code path is bypassed and pinned so, and an
earlier base set at load 5–9 read it at 140.24 ms. The brief's finding threshold — slower than
the earlier 293 ms median by more than the control's spread — does not trigger: the partitioned
cell is faster on every pass, and the base tree itself read 361–555 ms on this box today.

## 9. Questions for the owner

1. **Unpartitioned target file count.** The rule leaves the unpartitioned CTAS at one file per
   plan partition (8 at `shuffle.partitions = 8`; Spark writes 2 from its scan split). If a
   smaller count is wanted there, it is a coalesce with a measured cost, not a distribution rule,
   and it would trade the write parallelism PERF-ICE-WRITEPATH-1 delivered. Built conservative.
2. **`_row_id` on the partitioned path.** A row's `_row_id` is now a function of the channel
   interleaving. If reproducible lineage inside a partitioned file is wanted, the writer would
   have to sort each file's rows (a per-writer cost this unit did not measure).

## 10. Gates

| gate | result |
|---|---|
| `make verify` | see §11 |
| `pytest test_perf_ice_writepath_1.py test_write_distribution_1.py -q` | 10 passed, 3 skipped (release native) |
| `REPARK_PARITY_LIVE=1 pytest test_write_distribution_1.py test_perf_ice_writepath_1.py -q` | 13 passed, one JVM beside none, exited |
| `make py-test-facade` / `make py-test-dbt` | see §11 |
| `make check-map-sync` / `check-ledger-grammar` / `check-ledgers` / `check-docs-compaction` | see §11 |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | see §11 |
| `uvx typos@1.47.2 .` | see §11 |

## 11. Gate log

Filled at hand-back, verbatim exits.

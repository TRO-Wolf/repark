# ZERO-PARTITION-MERGE-1 step 0: the table, the cause, the inventory and the seat

Model `claude-opus-5-5`, 2026-11-13. Measurement and inventory only; no product code.

- **Builds.** RePark main `1d9d2808`'s release module (product-identical to main `2707924e`: the later commits
  change tests and docs only), pinned to fork `92242cb4`. The #1325 head module (`948d7fb3`, fork `8b6ff921`)
  for the comparison in section 1.4. Source read at RePark `2707924e` and fork main `8b6ff921`.
- **Spark.** 4.1.2 with Iceberg 1.11.0 (`iceberg-spark-runtime-4.1_2.13-1.11.0.jar`), its own tables.
- **Catalogs.** Hadoop catalogs on local disk.
- **Rows.** `(id BIGINT, d DOUBLE, f FLOAT, v INT)`: `(1, -0.0)`, `(2, 0.0)`, `(3, 1.5)`, `(4, NULL)`, `(5, 0.0)`,
  `(6, -0.0)`, with `d` and `f` carrying the same value. The DML doors seed `(1, -0.0)`, `(7, -0.0)`; `(2, 0.0)`,
  `(8, 0.0)`; `(3, 1.5)`, `(9, 1.5)`; `(4, NULL)`, `(10, NULL)` as four INSERT statements, one per partition, then touch ids
  7 to 10. The `_cross` doors seed `(10, -0.0)` and `(11, 1.5)`, then overwrite with `(1, 0.0)`.
- **Specs.** `PARTITIONED BY (d)` (identity DOUBLE) and `PARTITIONED BY (f)` (identity FLOAT).
- **Matrix.** 24 Spark-door doors x format v2 and v3 x Parquet, ORC and Avro x the two specs (288 tables a run),
  plus native `INSERT` (RePark only) and two overwrite spellings run in their own legs (`*-extra-*`, and inside
  Spark's `local[16]` run).
- **Cores.** `taskset` on lane 2's ten CPUs:
  - RePark: 1 CPU, 2 CPUs, 10 CPUs, and 10 CPUs with `spark.sql.shuffle.partitions = 16` (16 DataFusion target
    partitions, the 16-core plan shape). Sixteen physical CPUs are outside lane 2.
  - Spark: `local[1]`, `local[2]`, `local[16]`.

The rendered cells are in [table.md](table.md), the raw legs in [raw/](raw/map.md), the probes in [probe/](probe/map.md).

## 1. The table

### 1.1 Spark's answer: two partitions, on every door

Spark 4.1.2 writes a `-0.0` partition and a `+0.0` partition on every door, version, format and core count, and
reads back each row's own zero: 288 of 288 tables at `local[1]` and `local[2]`, 312 of 312 at `local[16]`, and 24 of
24 in the overwrite-spelling leg. Under the
`_cross` doors, an overwrite of the `+0.0` partition keeps the `-0.0` row. The halt "Spark's own answer is not two
partitions" does not fire.

### 1.2 RePark main, per door

Status per (version, spec) on 1 / 2 / 10 CPUs / 16 target partitions. Parquet, ORC and Avro never differ. The one
exception is `update_cow`, whose refusals are random (section 1.5).

| door | RePark 1 CPU | 2 CPUs | 10 CPUs | 16 partitions |
|---|---|---|---|---|
| `INSERT … VALUES`, `INSERT … VALUES` with `+0.0` first, `INSERT … SELECT` | merged, all four | merged, all four | ok | ok |
| `writeTo().append()`, `saveAsTable` append, `INSERT INTO t.branch_b` | merged, all four | merged, all four | ok | ok |
| CTAS, RTAS, `writeTo().create()`, `writeTo().createOrReplace()` | merged, all four | merged, all four | merged, all four | merged, all four |
| `rewrite_data_files` (`rewrite-all`) | merged, all four | merged, all four | merged, all four | merged, all four |
| `INSERT OVERWRITE` (static, whole table), `saveAsTable` overwrite, `writeTo().overwritePartitions()` | merged on FLOAT | merged on FLOAT | merged on FLOAT | merged on FLOAT |
| MERGE insert, MERGE update copy-on-write, DELETE copy-on-write | merged: v2 FLOAT, v3 both | the same | the same | the same |
| MERGE update merge-on-read, UPDATE merge-on-read | new rows mixed: v2 FLOAT, v3 both | the same | the same | the same |
| UPDATE copy-on-write | merged, all four | merged when it does not refuse | the same | the same |
| DELETE merge-on-read (file granularity) | ok | ok | ok | ok |
| DELETE merge-on-read, `write.delete.granularity = partition` | one delete file for both zeros, v2; Spark then returns the deleted row (1.6) | the same | the same | the same |
| `INSERT OVERWRITE` dynamic, `writeTo().overwritePartitions()`, overwriting `+0.0` (`_cross`) | **`-0.0` row lost**, all four | the same | the same | the same |
| `INSERT OVERWRITE … PARTITION (d = 0.0)`, `writeTo().overwrite(d = 0.0)` (`_cross`; Spark keeps `-0.0`) | refused | refused | — | — |
| native `INSERT` | not reachable | not reachable | — | — |

What the words mean (the renderer's statuses, [probe/render_step0.py](probe/render_step0.py)):

- **merged:** one zero partition is missing. One data file holds both zeros' rows, under whichever zero
  arrived first: `-0.0` in the base order, `+0.0` in the `pos_first` order. Both rows read back as that zero.
- **mixed:** both zero partitions exist, but a file written by this statement holds both zeros. On a merge-on-read
  UPDATE, the rewritten ids 7 and 8 land in one new file, and one of them reads the other zero.
- **one delete file for both zeros:** RePark writes a single position-delete file, with partition `+0.0`, for
  deletes in both zero partitions. Spark writes one per partition.
- **lost:** RePark's overwrite of the `+0.0` partition also drops the `-0.0` partition's file, so row 10 is gone.
  Spark keeps it. This is data loss, a second and worse symptom of the same equality.
- **refused:** `INSERT OVERWRITE PARTITION numeric literal 0.0 is not an integer`, and
  `Cannot convert Spark predicate to Iceberg expression`. Spark answers both. These are loud refusals, outside this
  card.
- **not reachable:** the native door refuses `CREATE TABLE … PARTITIONED BY` ("Hive distribution not supported").
  A Hadoop catalog registered late on the native session does not list a table another session created. `CALL` is
  unsupported. So no native statement reaches a partitioned Iceberg table on main.

### 1.3 The stored bits

The data files keep every row's own zero; only the partition value is wrong. This was read with pyarrow from every
Parquet and ORC data file the `.files` table lists ([raw/disk-*.jsonl](raw/map.md),
[probe/render_disk.py](probe/render_disk.py)):

| run | data files read | rows whose stored bits differ from the row's own zero | files holding both zeros |
|---|---|---|---|
| RePark 1 CPU | 678 | 0 | 138 |
| RePark 2 CPUs | 663 | 0 | 133 |
| RePark 10 CPUs | 711 | 0 | 85 |
| RePark 16 partitions | 711 | 0 | 85 |
| Spark `local[1]`, `local[2]` | 832 each | 0 | 0 |
| Spark `local[16]` | 880 | 0 | 0 |

No Avro reader is installed on this box, so Avro data files were not read raw. Both engines read an
identity-partitioned column from the file's partition value, not from the stored cell. That is why the `+0.0` row
reads `-0.0` on RePark (and on Spark, section 1.6), while the other float column of the same row reads its own
bits. The fix changes no stored data value; the halt "any stored data value changes" has nothing to act on.

### 1.4 #1325's head: the same layout

The #1325 head module (`948d7fb3`, fork `8b6ff921`), at 1 CPU and at 16 target partitions, gives the same status as
main in every row of 1.2.
- `CanonicalFloatExpr` (`crates/repark-iceberg/src/write/distribution/canonical_float.rs:52-69`) maps NaN to
  `f64::NAN` / `f32::NAN` and returns every other value unchanged, so `-0.0` keeps its bits. It does not fold the
  two zeros, on main (the sort-key wrap at `distribution.rs:275`) or in #1325 (the extra wrap around
  `partition_value_exprs`).
- The fork's `canonical_f64` / `canonical_f32` (`partition_value_calculator.rs:64-88`) are NaN-only too.
- The `canonical_f64` that adds `+ 0.0` is SIGNED-ZERO-NORMALIZE-1 part B's `repark-common/src/float_canon.rs`,
  which is not on main. If a later unit routes the partition hash through it, both zeros reach one writer. After
  the fix in section 4 that changes only which writer receives the rows, not the layout: the writer still keys the
  zeros apart.

### 1.5 Pre-existing refusals met on the way

`update_cow` refuses on 2 or more CPUs in about three tables out of four: "Mismatch between schema and batches".
That is card UPDATE-SCHEMA-BATCH-MISMATCH-1 (S3, label-blocking, filed 2026-11-13), not this unit. When it does not
refuse, the zeros merge.

### 1.6 Cross-reads

- **RePark reading Spark's tables** (`register_table`, the 288 tables of `local[2]`): every row reads its own zero
  and both partitions are listed, 288 of 288; RePark's read equals Spark's own read on all 288.
- **Spark reading RePark's tables** (the 288 tables of RePark's 1-CPU run, Spark's Hadoop catalog on the same root):
  every status of 1.2 repeats, and Spark's read differs from RePark's own read on 75 tables:
  - **6 tables, a deleted row comes back.** On the v2 `delete_mor_partition_granularity` tables, Spark returns
    deleted row 7. RePark wrote one position-delete file with partition `+0.0` for deletes in both zero
    partitions (W11 below); Java applies a partition-scoped delete file only to data files of its own partition,
    so the delete of row 7 (in the `-0.0` file) is lost. RePark's own read hides it.
  - **69 tables, every one an ORC table holding a merged file.** Spark reads the non-partition float column of
    the `+0.0` rows as `-0.0` too (for example `insert_values_v2_orc_d`, rows 2 and 5: `f` reads `80000000`, stored
    `00000000`). This is verifier 2's FLOAT/ORC observation. It occurs on no Parquet or Avro file, on no
    unmerged file, and on no Spark-written file; I did not trace it inside Spark's ORC reader. Once each file holds
    one zero, it has nothing to act on.

## 2. The cause, to the line

Arrow and DataFusion keep the two zeros apart. The grouping keys of the fork and of RePark do not.

| Step | Where | `-0.0` vs `0.0` |
|---|---|---|
| Partition value computed | fork `crates/iceberg/src/arrow/partition_value_calculator.rs:344` (`calculate`), `:405-474` (`calculate_from_columns`, NaN canonicalised at `:474` via `canonical_f64`/`canonical_f32` `:64-88`) | **distinct** (bits kept) |
| Fork insert path's `_partition` column | fork `crates/integrations/datafusion/src/physical_plan/project.rs:188-207` (`PartitionExpr::evaluate` → `calculate_from_columns`) | distinct |
| Fork insert path's hash repartition | fork `physical_plan/repartition.rs:78` (`Partitioning::Hash` on `_partition`); DataFusion `hash_utils.rs:187-199` hashes floats by `to_ne_bytes` | distinct hashes; both zeros reach one task only when the task count is 1 or their hashes collide |
| RePark plan-path hash | `crates/repark-iceberg/src/write/distribution.rs:129-155` (`partition_value_exprs`), `:157-173` (`hash_distribution`) | distinct |
| RePark stream router | `crates/repark-iceberg/src/write/distribution/router.rs:33-61` (`create_hashes` over the calculator's values); slot count `concurrency.rs:20` (`DEFAULT_MAX_CONCURRENT_FILES = 4`) | distinct hashes; with 4 slots the FLOAT zeros share a slot and the DOUBLE zeros do not, which is the "merged on FLOAT" row of 1.2 |
| Arrow-order split (not taken for floats) | fork `arrow/record_batch_partition_splitter.rs:316-339` (`arrow_order_matches_struct_equality` lists no Float/Double) | — |
| **Row-wise split** | fork `arrow/record_batch_partition_splitter.rs:251-313`, the map at **`:275`** (`HashMap<&Struct, …>`) | **equal** |
| **The key's equality** | fork `spec/values/struct_value.rs:27` (`#[derive(PartialEq, Eq, Hash)] Struct`) → `literal.rs:40` → `primitive.rs:25,34,36` (`Float(OrderedFloat<f32>)`, `Double(OrderedFloat<f64>)`) → ordered-float 4.6.0 `lib.rs:319-325` (`-0.0 == 0.0`), `:335-345` (hash after `canonicalize_signed_zero`, `:45-50`) | **equal** |
| Fan-out writer | fork `writer/partitioning/fanout_writer.rs:50` (`HashMap<Struct, B::R>`), `:73-85` (`get_or_create_writer`: the first key's writer, built with the first `PartitionKey`, so the file carries the first zero) | equal |
| Clustered writer | fork `writer/partitioning/clustered_writer.rs:49` (`closed_partitions: HashSet<Struct>`), `:100`, `:110-111` (`current != partition_value`) | equal |
| v3 lineage split | RePark `crates/repark-iceberg/src/write/merge/row_lineage.rs:247` (`HashMap<&Struct, …>`), then a `FanoutWriter` at `:205`; one serial writer, so v3 MERGE and COW DML merge on every core count | equal |

So the merge happens wherever both zeros reach one writer task. A one-writer door merges on every core count:
CTAS/RTAS/`writeTo().create`, `rewrite_data_files`, and the v3 lineage writer. A hash-routed door merges when the
task count is 1 or 2, or when the two zeros' hashes land in one slot.

## 3. The inventory

### 3.1 How I searched (repeatable)

From each clone's root:

```text
fork, at 8b6ff921:
rg -n 'HashMap<\s*(&)?Struct|HashSet<\s*(&)?Struct|BTreeMap<\s*(&)?Struct|BTreeSet<\s*(&)?Struct|HashMap<\s*(&)?PartitionKey|HashSet<\s*(&)?PartitionKey|HashMap<\(i32, Struct|HashSet<\(i32, Struct|HashMap<\(i32, &Struct|HashMap<Option<Struct|Vec<\(Struct' crates/iceberg/src crates/integrations/datafusion/src crates/catalog | grep -v '_tests.rs:'
rg -n 'RecordBatchPartitionSplitter|FanoutWriter|ClusteredWriter|TaskWriter' --type rust crates
rg -n 'drop_partitions.contains|replaced_partitions.contains|current != partition_value|closed_partitions' crates/iceberg/src
RePark, at 2707924e:
rg -n '<the same map/set pattern>' crates --glob '*.rs' | grep -v '_tests.rs:\|/tests/'
rg -l '\bStruct\b' crates --type rust   (then each file that imports iceberg's Struct, read by hand)
rg -n 'RecordBatchPartitionSplitter|FanoutWriter|ClusteredWriter|PartitionValueCalculator|IcebergTableProvider|replace_partitions' crates --type rust
```

The second RePark search finds the key that the first misses: `position_delete.rs:50-51`, an enum that holds a
`Struct`. Every hit was read. The table lists each that groups rows, files or deletes by a partition value; the
others (ordering helpers, empty partitions) are in 3.4.

### 3.2 Write paths that group by partition value

| # | Path | file:line | Doors that reach it (measured) | Two zeros one key? |
|---|---|---|---|---|
| W1 | Row-wise splitter | fork `arrow/record_batch_partition_splitter.rs:275` | every door below that splits a batch | **yes** |
| W2 | Fan-out writer map | fork `writer/partitioning/fanout_writer.rs:50`, `:73-85` | all fan-out doors | **yes** |
| W3 | Clustered writer | fork `writer/partitioning/clustered_writer.rs:49`, `:100`, `:111` | the fork insert path with `write.datafusion.fanout.enabled = false` (not in the matrix; the property defaults to fan-out) | **yes** |
| W4 | Fork insert path: `TaskWriter` | fork `integrations/datafusion/src/task_writer.rs:130-140` (W2/W3 plus a precomputed W1), from `IcebergWriteExec` `physical_plan/write.rs:324-337`, from `IcebergTableProvider::insert_into` `table/mod.rs:244-300` | SQL `INSERT … VALUES/SELECT` (DataFusion `Dml(Insert)` on the fork provider that `repark-iceberg/src/catalog/provider.rs:276` registers; `repark-sql/src/session_insert.rs:15-20` takes the statement only when a session write conf is set); `writeTo().append()` and `saveAsTable` append show INSERT's core pattern, so I place them here by that measurement, not by a trace; `INSERT INTO t.branch_b` (`repark-spark/src/write_to_branch.rs:602`); WAP-staged INSERT (`write_to_branch.rs:490`, same exec, not in the matrix) | **yes** (W1 + W2) |
| W5 | RePark stream fan-out | `repark-iceberg/src/write/append_fanout_serial.rs:54` (W1), `:90` (W2), per router slot `append.rs:73` (`fanout_conformed_stream_with_concurrency`) | CTAS, RTAS, `writeTo().create/createOrReplace`, `saveAsTable` overwrite, `INSERT OVERWRITE`, `overwritePartitions`, v2 MERGE and COW DML (via `stage_overwrite_files_with`, `write_options.rs:256-290`) | **yes** |
| W6 | RePark plan-path writer | `repark-iceberg/src/write/partition_write.rs:229` (`hash_distribution`), then `IcebergPartitionWriteExec` (W1 + W2 per task) | `write_data_files_from_plan` callers; the map notes no door reaches it with a write order | **yes** at the writer |
| W7 | v3 lineage split | `repark-iceberg/src/write/merge/row_lineage.rs:247` + `:205` (W2) | v3 MERGE insert/update, v3 COW UPDATE/DELETE, v3 MOR new rows | **yes** |
| W8 | Rewrite plan grouping | fork `maintenance/rewrite_data_files_plan.rs:178-184` (`by_partition: HashMap<Struct, …>`) | `rewrite_data_files` | **yes**: the `-0.0` and `+0.0` files become one rewrite group |
| W9 | Rewrite writers | fork `maintenance/rewrite_data_files_write.rs:265` (W1), `maintenance/rewrite_data_files_router.rs:31`, `:67` (`writers: HashMap<Struct, …>`) | `rewrite_data_files` | **yes** |
| W10 | Replace-partitions drop set | fork `transaction/replace_partitions.rs:226-231` (`drop_partitions`), matched at `:324` and `transaction/snapshot.rs:696` (`drop_partitions.contains(&key)`) | dynamic `INSERT OVERWRITE`, `overwritePartitions` (`repark-iceberg/src/write/partition_overwrite.rs:381-404`, `write_options.rs:600-618`) | **yes**: the `-0.0` partition's file is dropped (row lost) |
| W11 | Position-delete grouping | `repark-iceberg/src/write/position_delete.rs:50-51` (`PositionDeleteGroupKey::Partition { partition: Struct }`), `:196-213` | DELETE / UPDATE / MERGE merge-on-read with `write.delete.granularity = partition` | **yes**: one delete file, partition = the first zero |
| W12 | Legacy position deletes folded into a v3 DV commit | fork `delete_vector_container/legacy.rs:315` (`HashMap<(i32, Struct), …>`) | v3 merge-on-read over a table that already holds v2-style position deletes (not in the matrix) | **yes** |
| W13 | Cherry-pick of a replace-partitions snapshot | fork `transaction/cherry_pick.rs:151`, `:300`, `:325` | `cherrypick_snapshot` (not in the matrix) | **yes** |
| W14 | Fork DataFusion DELETE plan | fork `integrations/datafusion/src/physical_plan/delete_position_deletes.rs:192` | the fork provider's `delete_from` / `update` (`table/mod.rs:311`, `:368`); no RePark door was found to reach it for a partitioned table: Spark-door DML is RePark-owned, and the native door cannot reach a partitioned table | **yes** (not reached) |

### 3.3 Read and maintenance paths with the same key (not write grouping; they move with any fix to the key)

| Path | file:line | Effect today |
|---|---|---|
| `.partitions` metadata table | fork `inspect/partitions.rs:226` | groups the two zero partitions into one row: ICEBERG-PARTITIONS-TABLE-FLOAT-1 |
| Delete-file index | fork `delete_file_index.rs:127` (`HashMap<i32, HashMap<Struct, …>>`) | a partition-scoped delete in one zero partition is offered to data files of the other |
| Residual memo | fork `expr/visitors/residual_evaluator.rs:63` | one residual is reused for both zero partitions |
| Partition statistics | fork `maintenance/partition_stats.rs:497-706` | the two zeros share one stats row |
| Partition-key audit | fork `maintenance/partition_key_audit.rs:468` | the two zeros share one audit key |

### 3.4 Hits that do not group by value

- `repark-iceberg/src/write/fanout_order.rs:153` (`HashMap<Struct, usize>`): only for commit order, and it returns
  the files unchanged when a key holds a FLOAT or DOUBLE (`java_struct_hash`, `:48`, `None` at `:131-142`).
- `repark-iceberg/src/write/file_order.rs:10-25`, fork `rewrite_data_files_plan.rs:223-240`: ordering only.
- `repark-iceberg/src/write/merge/target_scan.rs:66`: a map from path to partition, not keyed by it.
- `microbatch/provider.rs`, `append.rs:1154`, `session_staging.rs:127`, `write_options.rs:361`: `Struct::empty()`
  for unpartitioned files.

## 4. The seat I intend

**One fork file, `crates/iceberg/src/spec/values/struct_value.rs`.** Replace `Struct`'s derived `PartialEq`, `Eq`
and `Hash` with hand-written ones.
- A FLOAT or DOUBLE leaf compares and hashes by its bit pattern, after NaN canonicalisation, which is Java's
  `Float.equals` / `Double.equals` and `hashCode`.
- Every other leaf compares as today, and nested `Literal::Struct` / `List` / `Map` leaves recurse.

`Struct` is the partition tuple type ("The partition struct stores the tuple of partition values for each file",
`struct_value.rs:24-26`), and every row of 3.2 and 3.3 keys on it.

- **Reach.**
  - One seat moves W1–W14: the splitter, both partitioning writers, the fork insert path, RePark's stream, plan and
    lineage writers, the rewrite plan and writers, the replace-partitions drop set, the position-delete groups, the
    legacy DV fold and cherry-pick.
  - It also moves the read paths of 3.3, ICEBERG-PARTITIONS-TABLE-FLOAT-1 among them, in Java's direction.
  - RePark needs only the fork pin, `Cargo.toml` and `Cargo.lock`, and no RePark product file.
- **What it does not touch.**
  - `PrimitiveLiteral` and `Datum` keep `OrderedFloat` equality, so predicate literals and `IN` sets
    (`HashSet<Datum>`) are unchanged.
  - NaN stays one key, as `OrderedFloat` has it today.
  - No stored value changes: the seat decides grouping and the partition value only.
- **Why not per-site.** Fixing W1, W2, W3, W8, W9, W10, W7 and W11 one by one is at least six product files across
  both repos, over the three-file halt. It also leaves 3.3 and any new `HashMap<Struct>` behind.
- **Why not `PrimitiveLiteral`.** `Datum` derives `PartialEq`/`Hash` from it (`datum.rs:106`), so a bitwise
  `PrimitiveLiteral` would change predicate and `IN`-set equality, where Spark treats `-0.0 = 0.0` as true.
- **Risks for the critic.**
  - (a) `Literal::Struct` equality outside partitions, for struct default values: a float leaf there would compare
    by bits. I found no caller that relies on `-0.0 == 0.0` there.
  - (b) The clustered writer (W3) after the fix: the write sort orders `-0.0` before `0.0` (Arrow total order), so
    each zero is one contiguous run. Unsorted input that interleaves them would now raise "The input is not sorted"
    where today it merges silently.
  - (c) `PartialEq` without `Ord` stays consistent: `Struct` has no `PartialOrd`.
- **Pins planned** (step 6): fork unit tests at `struct_value.rs` (equality, hash and NaN) and at the splitter, fan-out
  writer, rewrite plan and replace-partitions drop set. A facade test over the step-0 doors, red on main and green on
  the fix, on 1 CPU and on 16 target partitions. Spark parity on the live cells, the `_cross` overwrite included.

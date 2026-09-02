# Charter ledger — V3-11 · deterministic row-lineage assignment for same-commit data files

**Date:** 2026-09-02 · **Branch:** `feat/v3-11-row-id-determinism` · **Base:** `origin/main`
`802e35e` · **Model:** claude-opus-5 (medium) · **Policy:**
[../../../AGENTS.md](../../../../AGENTS.md) · **Path:** STANDARD.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** LIVE-v3 measured the merge-on-read MERGE insert's `_row_id` flapping between 10 and
11 where Spark is deterministic at 11, and filed it as registry `V3-ROWID-3` with this unit as
its owner. V3-10 filed the sibling artefact `F-v3-10-partition-file-order`.

**Not in this unit:** the fork pin (RP-7 owns it, `Cargo.toml` untouched); `.github/`; the two
fork-owned legs named in §Fork asks.

## PROPOSITION LEDGER — V3-11 — 2026-09-02

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Spark's rule for ordering (a) the rows of one MERGE output file and (b) the data files of one commit in the manifest is decoded from the Iceberg spec plus the 1.11.0 runtime jar, and every decoded instruction is confirmed against the live oracle. | `javap` over `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`; oracle cells whose predicted order the decode must reproduce. | **PROVEN** | §Decode. `FanoutWriter.writers: Map<Integer, StructLikeMap<FileWriter>>`, `closeWriters() → values()`, `StructLikeMap` = `HashMap<StructLikeWrapper, T>`; `JavaHashes$StructLikeHash.hash` = `r=97; r=41*r+nFields; r=41*r+fieldHash`; `JavaHashes.hashCode(CharSequence)` = `r=177; r=31*r+charAt(i)`; bucket = `(H ^ H>>>16) & 15`. Predicts six measured cells exactly, including a three-way bucket collision. Rows inside one file are not reordered: partitions decide, and the matched-UPDATE row and the inserted row land in different files whenever their partitions differ. |
| C-002 | Before the change the ten-run MERGE cell is red at Spark's exact `_row_id`, and the partitioned-append cell is red at Spark's file order. | Red transcript at base `802e35e`. | **PROVEN** | §Red. MERGE: three ten-run batteries read 6, 4 and 2 correct of ten (**24 red of 30**); survivor triples matched the registry exactly on every run. Partitioned `INSERT INTO`: ten runs, both manifest halves flapping independently, Spark's map on 3 of 10. |
| C-003 | Every data file this engine writes for one commit reaches the manifest ordered by ascending partition value, so `first_row_id` assignment is deterministic and Spark-equal on every cell the engine owns; no per-row cost is added. | Three-door pins at absolute values; mutation N red of M; 1e6-row timing. | **PROVEN** | `write/file_order.rs::ascending_partition_order`, a stable sort of the already-written `Vec<DataFile>` — file-count work. Wired at `append.rs` (both fanout exits) and `merge/row_lineage.rs`. Ten-run MERGE 10/10 at 11; three-partition MoR MERGE `(1,0,1),(2,1,2),(7,3,2),(8,2,2)`; partitioned CTAS `1→3,2→0,3→2,4→1,5→4` — all Spark's exact values. Mutations §Mutations: 9 red of 12. Timing 1e6 rows / 8 partitions: 2.810/2.850/2.875 s with, 2.973/2.943/3.010 s without. |
| C-004 | Each measured cell is pinned on the door it is reachable from; the facade asserts Spark's exact inserted `_row_id` where it asserted an invariant; the live acceptance leg inherits it and stays green. | `docs/testing.md` row-per-entry-point; facade mutation. | **PROVEN** | Spark door `crates/repark-spark/src/tests/v3_row_order.rs` (3 tests, 20 table lifecycles); ANSI door `crates/repark-sql/src/v3/cow.rs::ansi_partitioned_ctas_…` / `::ansi_mor_merge_across_three_partitions_…` (5 runs each); facade `_acceptance_v3.assert_v3_lineage` now `V3_EXPECTED_INSERTED_ROW_ID = 11`, read by `test_v3_acceptance_local.py` and the two live legs; live cell `python/repark/tests/test_v3_live_file_order.py::test_v3_same_commit_file_order_live_matches_spark`, green under `REPARK_PARITY_LIVE=1` against PySpark 4.1.2 + Iceberg 1.11.0. Facade mutation: 2 red of 6 runs without the ordering, 5 of 5 green with it. The byte tripwire `v3_lineage.rs::cow_keep_refusal_files_are_byte_untouched` re-records the `crates/repark-sql/src/v3/cow.rs` hash (`0xebe6…8a82` → `0xf0a7…c7ad`) citing this unit; the other three hashes are untouched. Citation: `crates/repark-spark/src/tests/map.md`. |
| C-005 | Registry `V3-ROWID-3` is FIXED with the decode; the two fork-owned residuals are dated DECLARED rows naming the fork as owner; STATUS drops the Known-issues line and discharges V3-11; the north-star MoR DML row is trued; maps are in lockstep; this ledger `move`s to `completed/` last. | `make check-map-sync check-ledger-grammar check-ledgers check-docs-compaction`. | **PROVEN** | §Docs. `V3-ROWID-3` FIXED (V3-11, 2026-09-02) carrying the decode table; `F-v3-10-partition-file-order` re-measured and re-dated with the fork ask; `F-v3-11-rewrite-row-order` filed; STATUS 24,9xx bytes under its 25,000 ceiling; north star row `V3-9, V3-11`; `test_live_v3_docs.py` meta-pins rewritten from BACKLOG to FIXED. Citation: `crates/repark-iceberg/src/write/map.md`. |
| C-006 | The two legs this unit cannot close are measured, named, and handed to the fork rather than pinned to a flapping value. | Ten-run tables on both engines. | **PROVEN** | §Fork asks. Partitioned plain `INSERT INTO` is written by `iceberg-datafusion`'s `TaskWriter` → `FanoutWriter` and committed by `IcebergCommitExec` without repark holding the files. `rewrite_data_files` after an upgrade is nondeterministic **on Spark itself**. |

VERDICT: 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: v3-11-row-id-determinism
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The ordering is pinned on the three writers this engine owns (partitioned CTAS, partitioned MoR MERGE, partitioned append) on the Spark door, the ANSI door and the facade, each at the live oracle's exact id-to-row-id map; the ten-run pin replays the whole LIVE-v3 statement sequence.
      artifacts: [crates/repark-spark/src/tests/v3_row_order.rs, crates/repark-sql/src/v3/cow.rs, python/repark/tests/_acceptance_v3.py]
    - id: AT-2
      status: ATTACKED
      evidence: Two and three partition values; identity int partitions 0,1,2; a matched-UPDATE row and an inserted row in the same partition and in different partitions; unpartitioned commits (the sort is a no-op); null partition slots ordered first by construction; five and ten repeated runs per cell.
      artifacts: [crates/repark-iceberg/src/write/file_order.rs, crates/repark-spark/src/tests/v3_row_order.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The sort runs after every writer has closed, so no file is dropped, duplicated or truncated; the pins read every surviving row's triple, not only the inserted one, so a reorder that lost a row would red. The two paths repark cannot order are reported as fork asks rather than pinned to a flapping value.
      artifacts: [crates/repark-spark/src/tests/v3_row_order.rs, docs/spark-sql-iceberg-parity.md]
    - id: AT-4
      status: ATTACKED
      evidence: The parallel append collector already joined every worker before the sort; the sort is a pure function of the collected Vec and adds no shared state, no lock and no await.
      artifacts: [crates/repark-iceberg/src/write/append.rs]
    - id: AT-5
      status: N/A
      justification: No AWS, IAM, credential or path handling; the change reorders in-memory file metadata already written by this process.
    - id: AT-6
      status: N/A
      justification: No Catalog trait change.
    - id: AT-7
      status: ATTACKED
      evidence: No recursion; no new allocation (an in-place stable sort of a Vec the caller already owns); the literal comparison falls back to Equal rather than panicking on a non-primitive partition value, which the partition spec cannot produce.
      artifacts: [crates/repark-iceberg/src/write/file_order.rs]
    - id: AT-8
      status: N/A
      justification: No dependency pin change; the fork stays as the base left it.
    - id: AT-9
      status: ATTACKED
      evidence: V3-ROWID-3 FIXED with the decoded Java bucket order; F-v3-10-partition-file-order re-measured and re-dated with its fork ask; F-v3-11-rewrite-row-order filed with both engines' ten-run tables; the falsified V3-LINEAGE-1 clause about the 4.1.2 oracle corrected; STATUS, north star and maps trued up.
      artifacts: [docs/spark-sql-iceberg-parity.md, STATUS.md, task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md]
    - id: AT-10
      status: ATTACKED
      evidence: Six clauses pinned; maps lockstep; four mutations run and restored (9 red of 12) plus the facade mutation (2 red of 6); no ceiling raised — append.rs stays at its 1886 ceiling by paying the two added lines back in its own import block.
      artifacts: [scripts/check_rust_file_size.py, crates/repark-iceberg/src/write/append.rs]
  complete: true
```

## Decode (C-001)

`javap -p -c` over `/tmp/iceberg-spark-runtime-4.1_2.13-1.11.0.jar`.

| Question | Decisive instruction | Answer |
|---|---|---|
| Which writer runs? | `TableProperties.PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED_DEFAULT`-equivalent on the Spark side is `write.spark.fanout.enabled` default true; the physical plan for a partitioned `INSERT` carries `Exchange hashpartitioning(part, N), REBALANCE_PARTITIONS_BY_COL` and **no** `SortExec` | fanout, per task |
| Where does file order come from? | `org.apache.iceberg.io.FanoutWriter.writers : Map<Integer, StructLikeMap<FileWriter<T,R>>>`; `closeWriters()` iterates `writers.values()` then `specWriters.values()` | the map's iteration order, not arrival order |
| What is that map? | `org.apache.iceberg.util.StructLikeMap.wrapperMap : Map<StructLikeWrapper, T>` — a `java.util.HashMap` | Java bucket order |
| What is the key hash? | `StructLikeWrapper.hashCode()` → `JavaHash.hash(struct)`; `JavaHashes$StructLikeHash.hash`: `bipush 97` seed, `41*r + nFields`, then per field `41*r + fieldHash` | 1-field spec: `H = 163098 + fieldHash` |
| Field hash, int | `JavaHash.forType` default arm is `Objects::hashCode` | the value |
| Field hash, string | `JavaHashes.hashCode(CharSequence)`: `sipush 177` seed, `31*r + charAt(i)` | `'a' = 5584`, `'z' = 5609` |
| Bucket | `java.util.HashMap`: `(h ^ (h>>>16)) & (capacity-1)`, capacity 16 while ≤ 12 distinct partitions; collisions keep insertion order | ordering key |
| Rows inside one file | no sort in the plan; the matched-UPDATE row and the NOT-MATCHED-INSERT row go to their own partitions' writers | partitions decide, not row order |

Predicted vs measured, one identity partition column, one commit (live oracle, `local[1]`,
`shuffle.partitions=1`, so a single task):

| Partition values | Predicted bucket order | Measured file order |
|---|---|---|
| `{0..9}` | 8,9,6,7,0,1,4,5,2,3 | 8,9,6,7,0,1,4,5,2,3 |
| `{0..4}` | 0,1,4,2,3 | 0,1,4,2,3 |
| `{17,33,1,2}` | bucket 9 holds 17,33,1 in insertion order; 2 in bucket 14 | 17,33,1,2 |
| `{100,200,300}` | 200,300,100 | 200,300,100 |
| `{'a'..'e'}` | a,b,e,c,d | a,b,e,c,d |
| `{'z','a','m'}` | z,m,a | z,m,a |

Arrival-independence: the same partition set inserted ascending, descending and shuffled gave
the identical file order every time. Ascending partition value agrees with the bucket order for
identity-int `{0,1}`, `{0,1,2}` and `{0,1,2,3}` — every cell this engine pins — and diverges from
five int partitions upward and for strings. Replicating the bucket order was rejected: it is a
`java.util.HashMap` capacity artefact that changes again at the thirteenth partition.

## Red (C-002)

| Cell | Spark | Engine at `802e35e` |
|---|---|---|
| LIVE-v3 MoR MERGE insert `_row_id`, ten runs | 11 in 10 of 10 | 6, 4 and 2 correct over three ten-run batteries — **24 red of 30**; survivor triples identical and correct on all 30 |
| v2→v3 upgrade, partitioned append, ten runs | `1→2,2→3,3→4,4→0,5→1` in 10 of 10 | flaps: seed manifest Spark's `1→2,2→3,3→4` 5× / `1→3,2→4,3→2` 5×, append manifest Spark's `4→0,5→1` 4× / `4→1,5→0` 6×, the halves independent — both Spark's on **3 of 10**. Fork-owned, see §Fork asks |
| partitioned CTAS, three partitions | `1→3,2→0,3→2,4→1,5→4` | random per run |
| MoR MERGE across three partitions | `(1,0,1),(2,1,2),(7,3,2),(8,2,2)` | random per run |

## Green (C-003, C-004)

| Cell | Door | Engine after |
|---|---|---|
| LIVE-v3 MoR MERGE insert, ten runs | Spark | `_row_id = 11` **10 of 10**, every survivor triple exact |
| MoR MERGE across three partitions | Spark, ANSI | `(1,0,1),(2,1,2),(7,3,2),(8,2,2)`, five runs each |
| partitioned CTAS, three partitions | Spark, ANSI | `1→3,2→0,3→2,4→1,5→4`, five runs each |
| LIVE-v3 acceptance leg | facade | `V3_EXPECTED_INSERTED_ROW_ID = 11` exact, 5 of 5 green |
| partitioned CTAS + three-partition MoR MERGE | live oracle | repark == Spark under `REPARK_PARITY_LIVE=1`, matched layout |

## Mutations

| # | Mutation | Result |
|---|---|---|
| M1 | `ascending_partition_order` returns its input unsorted | 3 red of 3 Spark-door, 2 red of 2 ANSI |
| M2 | comparator reversed (descending) | 3 red of 3 Spark-door |
| M3 | the `append.rs` call sites dropped | 1 red of 3 — the CTAS pin only, the two MERGE pins stay green |
| M4 | the `merge/row_lineage.rs` call site dropped | 2 red of 3 — both MERGE pins, the CTAS pin stays green |
| M5 | the ordering dropped, facade re-run six times | 2 red of 6 (the flap rate the fix removes) |

M3 and M4 are the scoping proof: each call site is load-bearing for exactly the writers it feeds.

## Cost (C-003)

1e6 rows across eight identity partitions through `append`, debug build, three runs each:

| Build | Wall |
|---|---|
| with `ascending_partition_order` | 2.810 s / 2.850 s / 2.875 s |
| without | 2.973 s / 2.943 s / 3.010 s |

The sort is `Vec<DataFile>` work: one commit's file count, never a row count.

## Fork asks (C-006)

| Leg | Why repark cannot close it | Ask |
|---|---|---|
| `F-v3-10-partition-file-order` — partitioned plain `INSERT INTO` | `IcebergTableProvider::insert_into` builds `IcebergWriteExec` → `TaskWriter` → `FanoutWriter` (`partition_writers: HashMap<Struct, _>`, drained in Rust hash order at `close()`) and `IcebergCommitExec` commits those files; repark never holds the `Vec<DataFile>`. Instrumenting `ascending_partition_order` confirmed it: the MoR MERGE cell enters it 60 times over ten runs, the `INSERT INTO` cell zero times. | drain `FanoutWriter::close` in ascending partition-value order |
| `F-v3-11-rewrite-row-order` — `rewrite_data_files` after an upgrade | **Spark itself is nondeterministic here**: ten runs, ten distinct id→`_row_id` maps. The set `0..5` and sequence 7 are stable on both engines and stay pinned; there is no value to pin. | none — reported, not asked |

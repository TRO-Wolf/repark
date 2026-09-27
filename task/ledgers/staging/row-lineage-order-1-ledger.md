# Unit ledger — WO F-ROW-LINEAGE-ORDER-1 · a delegated `INSERT` commits its data files in Spark's fanout-writer order

**Date:** 2026-09-26 · **Branch:** `feat/row-lineage-order-1` · **Base:** `4c5c2be8`
(`origin/main`) **Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Scoreboard cell R-MC-ROW-ID-V3 replays DIFFERENT: Spark answers
`[[2,0,1],[3,3,2],[4,2,1]]`, RePark `[[2,2,1],[3,3,2],[4,1,1]]`. The fork assigns
`first_row_id` exactly like Java, so the whole difference is the order the delegated
`INSERT` hands data files to the append: the fork sorts ascending by partition while
Spark's `FanoutWriter` drains a `StructLikeMap` in Java `HashMap` iteration order.
Owner ruling Q-55-1 (2026-09-22) reversed `V3-FILEORDER-1` and ruled emulation IN;
orchestrator ruling Q-55-4 put the ordering logic on the RePark side behind the fork
`DataFileCommitOrder` hook (fork #346, in the pinned rev). This unit installs it:
`crates/repark-iceberg/src/write/fanout_order.rs` implements the rule, and
`SparkExtension::configure` installs it on every Spark-door session.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `java_struct_hash` answers every recorded Java golden: arity-1 strings `x`, `y`, `""`, `é`, `😀`, `a`, `b`, `c` and null; ints 0, 1, 6, −1, 2147483647; longs 0, 1, 6, −1, 4294967296; dates 0, 20722; booleans; and the four arity-2 tuples. | Unit pin against `probes/f-java-order-spark-2026-09-26.json`. | PROVEN | `fanout_order_tests.rs::java_struct_hash_matches_java_goldens`, passed. |
| C-002 | `spark_murmur3` answers the six recorded Spark `hash()` values (`é`, `中`, `bü`, `c中`, `ab中`, `abcd`) with the sign-extended tail byte. | Unit pin against the xo55-rid measured values. | PROVEN | `fanout_order_tests.rs::spark_murmur3_matches_spark_hash`, passed. |
| C-003 | The commit order matches every recorded Spark order at shuffle 4: the int list first 12, 13, 24, 25, and the long, date, boolean, null-string, unicode, two-field, day-stamp and tail-byte sets. | Unit pins against the xo55-rid measured values. | PROVEN | `fanout_order_tests.rs::order_matches_recorded_spark_at_shuffle_4`, passed. |
| C-004 | The one-bucket tie `br,aq,bb,aa` follows the shuffle reducer: `[2,3,1,4]` at 4, `[3,4,1,2]` at 7, `[1,3,2,4]` at 200. | Unit pin against the xo55-rid measured values. | PROVEN | `fanout_order_tests.rs::ties_follow_the_shuffle_reducer`, passed. |
| C-005 | At shuffle 1 the order matches the `V3-FILEORDER-1` registry rows: ints `0..=4` and `0..=9`, strings `a..=e`, the two-field grid, and the three `{0,NULL,1}` arrival orders. | Unit pin against the registry table. | PROVEN | `fanout_order_tests.rs::shuffle_1_matches_the_registry_rows`, passed. |
| C-006 | The fallbacks return the input unchanged: float keys, an 8-key bucket, a stamped `sort_order_id`, unpartitioned files, while one key's files stay together. | Unit pin, five shapes. | PROVEN | `fanout_order_tests.rs::falls_back_to_input_order`, passed. |
| C-007 | The conf reader answers 200 when absent, the value for a positive integer, and a `Configuration` error naming key and value otherwise. | Unit pin, four shapes. | PROVEN | `fanout_order_tests.rs::shuffle_partitions_conf`, passed. |
| C-008 | The R-MC-ROW-ID-V3 cell verbatim answers Spark's `[(2,0,1),(3,3,2),(4,2,1)]`. | SQL pin; red before as `[(2,2,1),(3,3,2),(4,1,1)]`. | PROVEN | `v3_fanout_order.rs::row_id_v3_cell_answers_spark`, passed. |
| C-009 | Seven keys at shuffle 4 take Spark's file order `y,z,w,x,q,a1,b1`. | SQL pin; red before as ascending. | PROVEN | `v3_fanout_order.rs::seven_keys_take_spark_file_order`, passed. |
| C-010 | The one-bucket tie follows the configured reducer end to end: `[2,3,1,4]` at shuffle 4, `[1,3,2,4]` on the default session. | SQL pin; red before as `[4,2,3,1]`. | PROVEN | `v3_fanout_order.rs::bucket_tie_follows_the_configured_reducer`, passed. |
| C-011 | An unpartitioned insert keeps its file order. | SQL guard pin, green before and after. | PROVEN | `v3_fanout_order.rs::unpartitioned_insert_keeps_its_order`, passed. |
| C-012 | A `WRITE ORDERED BY` table keeps ascending `_row_id` through the sort-order fallback. | SQL guard pin; the fork stamps a non-zero `sort_order_id` on the delegated path. | PROVEN | `v3_fanout_order.rs::sorted_table_keeps_ascending`, passed. |
| C-013 | Twelve eight-category runs at shuffle 4 give one mapping: Spark's recorded `z,x,m,a,q,b,c,d` with minima `50*i`. | Facade pin against `spark_rowid_order_oracle.json`. | PROVEN | `test_ice_rowid_order_1.py::test_eight_category_mapping_is_one_and_matches_spark`, passed. |
| C-014 | `INSERT OVERWRITE` takes the same fanout order (S4). | Deferred: `router.rs::execute_insert_routed` sends every Spark-door `INSERT OVERWRITE` to owned stage-then-swap committing through fork actions, and the ANSI door owns its overwrite path too, so the fork DataFusion `Overwrite` arm is unreachable from both doors; the owned-writer order is R-FILEORDER-2. | OPEN (DEFERRED to R-FILEORDER-2 per the 2026-09-26 ruling on HALT Q1) | Ruling text; no test in this unit. |

## Mutation record (2026-09-26)

Each line was broken, the named tests ran, and the file was restored.

| # | Mutation | Red |
|---|---|---|
| M1 | struct-hash seed 97 → 98 | `java_struct_hash_matches_java_goldens`, `order_matches_recorded_spark_at_shuffle_4`, `shuffle_1_matches_the_registry_rows`, `falls_back_to_input_order` red; restored green |
| M2 | murmur tail byte signed → unsigned (`i32::from(*byte)`) | `spark_murmur3_matches_spark_hash`, `order_matches_recorded_spark_at_shuffle_4` red; restored green |
| M3 | sort-order fallback dropped (`is_some_and(\|_\| false)`) | `sorted_table_keeps_ascending` reds with `[(1,0),(2,1),(3,3),(4,4),(5,2)]`, proving the fork stamps a non-zero `sort_order_id` on the delegated path; restored green |
| M4 | the two wiring lines removed from `SparkExtension::configure` | `row_id_v3_cell_answers_spark` reds with `[(2,2,1),(3,3,2),(4,1,1)]`; restored green |

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: row-lineage-order-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause is pinned end to end on the Spark door or as a unit pin against recorded Spark values; the three behaviour pins were red before the change with the recorded ascending values.
      artifacts: [crates/repark-spark/src/tests/v3_fanout_order.rs, crates/repark-iceberg/src/write/fanout_order_tests.rs]
    - id: AT-2
      status: ATTACKED
      evidence: All five supported literal kinds plus null, arity 1 and 2, capacity ladders 16/32/64, reducer ties at shuffle 1/4/7/200, and all three fallbacks are pinned.
      artifacts: [crates/repark-iceberg/src/write/fanout_order_tests.rs]
    - id: AT-3
      status: ATTACKED
      evidence: A bad shuffle value refuses loud naming key and value; a zero modulus is refused at the conf reader and guarded in the pure function.
      artifacts: [crates/repark-iceberg/src/write/fanout_order_tests.rs]
    - id: AT-4
      status: ATTACKED
      evidence: One commit's file order is deterministic on every path: the rule, the ascending fallback, and the fork's permutation check, which refuses a non-permutation loud.
      artifacts: [crates/repark-spark/src/tests/v3_fanout_order.rs]
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling; partition literals only.
    - id: AT-6
      status: ATTACKED
      evidence: No stored-format change; the manifest entry order moves and first_row_id follows it. The six-run scoreboard replay holds every R-MC cell.
      artifacts: [crates/repark-spark/src/tests/v3_fanout_order.rs]
    - id: AT-7
      status: N/A
      justification: No performance claim; the order is file-count work per commit.
    - id: AT-8
      status: ATTACKED
      evidence: No new dependency and no Cargo.toml edit; the order installs on the fork's existing hook, and unsupported shapes keep the fork's ascending order.
      artifacts: [crates/repark-iceberg/src/write/fanout_order.rs, crates/repark-spark/src/extension.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Every fallback returns the input unchanged and is pinned; the sorted-table guard proves the stamp path end to end.
      artifacts: [crates/repark-iceberg/src/write/fanout_order_tests.rs, crates/repark-spark/src/tests/v3_fanout_order.rs]
    - id: AT-10
      status: ATTACKED
      evidence: M1-M4 break the seed, the tail-byte sign, the sort fallback and the wiring; the named tests red before restore.
      artifacts: [crates/repark-iceberg/src/write/fanout_order.rs, crates/repark-spark/src/extension.rs]
  complete: true
```

## Residues

| # | Residue |
|---|---|
| R-1 | Dated 2026-09-26 (deferred to R-FILEORDER-2 per the ruling on HALT Q1): `INSERT OVERWRITE` on either door commits through owned stage-then-swap in ascending order, and the fork DataFusion `Overwrite` arm is unreachable from both doors, so no overwrite takes Spark's fanout order until the owned writers do. |
| R-2 | Dated 2026-09-26 (named in the order): keys sharing both bucket and reducer keep the fork's ascending arrival order, so `{0, NULL, 1}` arriving `0` first at shuffle 1 answers `NULL, 0, 1` where Spark answers `0, NULL, 1`. |
| R-3 | Dated 2026-09-26 (named in the order): `write.distribution-mode` `none`/`range` without a sort order, multi-task writes, and runtime `SET spark.sql.shuffle.partitions` (the hook reads only the builder conf) are unhandled. |
| R-4 | Dated 2026-09-26 (observed by the round, report-only): `L-INSERT-OVERWRITE` replays 1 EQUAL and 5 DIFFERENT across six identical-code runs (`target/replay/rlo1-ow-{1..6}.json`; repark `[[2,b,4,3],[3,c,5,3],[4,d,6,3]]` vs Spark `[[2,b,5,3],[3,c,6,3],[4,d,4,3]]`), a pre-existing source-scan-order nondeterminism on the owned overwrite path, which never reaches the hook; candidate scope for R-FILEORDER-2. The six R-MC replays are `target/replay/rlo1-{1..6}.json`. |

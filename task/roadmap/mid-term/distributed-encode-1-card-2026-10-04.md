# Card DISTRIBUTED-ENCODE-1: RePark-owned physical nodes the Ballista codec cannot encode, and three red distributed tests on main

**Date:** 2026-10-04 · **Filed by:** Claude (Opus 5.5), orchestrator · **Source:** the TA series S2b executor's halt (R-S2b-6) and the #940 verifier. No product code in this filing.

## Measured on `origin/main` (`ac8a72da`): `cargo test -p repark-distributed --features cluster`

Three tests are red on main as well as on the S2b head:

| Test | File | Cause given by the executor |
|---|---|---|
| `cancel_mid_flight_sets_cancelled_and_no_running_tasks_within_five_seconds` | `crates/repark-distributed/tests/cluster_two_executors.rs:329` | Ballista cannot encode `range()`'s `LazyMemTableExec` |
| `session_spill_dir_has_no_shuffle_files_after_complete_and_after_cancel` | `crates/repark-distributed/tests/multi_stage.rs:456` | Ballista cannot encode `range()`'s `LazyMemTableExec` |
| `date_and_timestamp_predicates_measure_the_pushdown_surface` | `crates/repark-distributed/tests/codec.rs:796` | unrelated to windows; the #940 verifier confirmed it red on main (`codec.rs:807`) |

## Inferred, not yet measured

- **The codec's coverage:** `ReparkPhysicalExtensionCodec::try_encode` (`crates/repark-distributed/src/codec.rs`) handles only `IcebergTableScan` and passes every other node to Ballista's codec.
- **`NljBuildSideExec`:** RePark's `NljBuildSideReset` rule (`crates/repark-core/src/nlj_build_reset.rs`) wraps every nested-loop join in it, on every session, so a distributed plan with an NLJ likely fails to encode too.
- **`ParallelWindowExec` (#940):** kept off distributed sessions by `ReparkSessionBuilder::parallel_single_partition(false)`. The #940 fold makes the provider refuse a session with it on.

## Scope (proposed)

1. **Measure first:** an NLJ plan, a `range()` plan and a `ParallelWindowExec`-eligible plan through `ReparkClusterExecutor`. Record encode success or failure for each.
2. **For every RePark-owned exec that can reach a distributed plan,** either:
   - add a codec arm that encodes it as its DataFusion equivalent (`NljBuildSideExec` as the plain `NestedLoopJoinExec`, `LazyMemTableExec` as a materialised `MemoryExec`), or
   - keep its rule off distributed sessions with the same builder flag.
3. **Fix or re-scope `date_and_timestamp_predicates_measure_the_pushdown_surface`** on its own measured cause.
4. **Pins:** each of the three tests is green, and each encode arm has a round-trip test. Mutation: drop the arm.

## Grade and release

B. Distribution is still deferred (`crates/repark-core/src/map.md`, "distribution is deferred"), so this is not release-gating. **Lean:** before any distributed work is scheduled. A clerk can do the measurement step.

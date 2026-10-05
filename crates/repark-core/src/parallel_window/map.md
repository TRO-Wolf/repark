# map — repark-core/src/parallel_window

## Purpose

TA-SINGLE-SERIES-PARALLEL-1 slice S2b (2026-10-04): a generic physical operator that runs the
expressions of a one-partition window in parallel. DataFusion's `WindowAggExec` evaluates every
window expression serially on the polling thread (`compute_window_aggregates`); on one long series
(the owner's `ta.*` benchmark: 14 indicators over 1 M rows) that pins the query to one core.
`ParallelWindowRule` swaps such a node for `ParallelWindowExec`, which computes the same columns
on the runtime's blocking pool. Design: the series sketch §3.2 (design B), rulings R-S2b-1…8 of
the S2b work order.

**Single node only (R-S2b-6, orchestrator ruling Q1 = A, 2026-10-04).** The rule is installed on
every `ReparkSession` (`session/df_guards.rs`, after `NljBuildSideReset`) and reads one flag at
`optimize` time. `repark-distributed` builds its Ballista sessions from a `ReparkSession`
(`ReparkSessionProvider::from_session`), and its codec has no arm for `ParallelWindowExec`, so
**every distributed session must be built with `ReparkSessionBuilder::parallel_single_partition(false)`**.
The plan a cluster runs is created on that session's own context, so stripping the rule from the
provider's cloned state would not be enough. The flag defaults to on; it has no Python surface.

## Contents

- `mod.rs` — the module root and the flag's carrier, `ParallelSinglePartitionConfig`, a
  `ConfigExtension` under the two-segment prefix `repark.parallel` (the same carrier shape as
  `repark.ansi` and `repark.overwrite`: `SET` refuses, `entries()` is empty so it never lists).
  `with_parallel_single_partition` attaches it at session build (`None` → on);
  `parallel_single_partition_enabled` reads it from `ConfigOptions` (absent → on).
- `rule.rs` — `ParallelWindowRule` (`parallel_window`), a `PhysicalOptimizerRule` appended after
  DataFusion's list, so distribution and ordering are already enforced and the swap changes no
  requirement. It fires on a `WindowAggExec` when the flag is on, **every** expression has an
  empty `partition_by()`, the input has **one** output partition, and the expressions form **at
  least two** groups; otherwise the plan is returned unchanged. A group is the set of expressions
  whose argument lists are structurally equal (`WindowExpr::expressions()`, compared as
  `PhysicalExpr`), in index order — multi-output siblings (bbands, MACD, stoch, aroon) share a
  group and therefore one thread, so the TA thread-local sibling cache (`repark-ta`
  `udf/mod.rs`) still computes each family once. `BoundedWindowAggExec` is never touched:
  measured, no `ta.*` plan produces one (a TA window UDF is not bounded-capable, so a node holding
  one is a `WindowAggExec`), and the non-TA one-partition windows that do (`lag`, `row_number`,
  running `sum` over `ORDER BY`) stay as DataFusion planned them.
- `exec.rs` — `ParallelWindowExec`. Built from the `WindowAggExec` it replaces: the same input,
  expressions, `PlanProperties`, required ordering (captured from the source node), single
  partition distribution and `maintains_input_order`; `with_new_children` rebuilds through
  `WindowAggExec::try_new` so the properties stay DataFusion's. `InputOrder::Single` is the only
  arm here (the partition-index arm for an `OVER ()` over several partitions belongs to S2a).
  Execution: collect the input and `concat_batches` it once (as `WindowAggStream` does); empty
  input emits no batch; one `SpawnedTask::spawn_blocking` per group, capped by a
  `tokio::sync::Semaphore` of `min(groups, target_partitions)`; each group evaluates its members
  serially in index order and stops at its first error; the columns are assembled **by
  expression index**, so the output schema, column order, row order and the single output batch
  equal `WindowAggExec`'s. Errors: the failing expression with the lowest index wins. Dropping the
  stream drops the driver future and its `SpawnedTask`s, which abort; nothing is detached. The
  blocking-pool threads are the runtime's threads, so under the facade they carry the 32 MiB
  stacks of `repark-python/src/deep_stack.rs` (`RUNTIME_THREAD_STACK_BYTES`). EXPLAIN prints
  `ParallelWindowExec: wdw=[…]` with exactly the expression list `WindowAggExec` prints.

## Pointers

- Up: [../map.md](../map.md)
- Session wiring: [../session/map.md](../session/map.md) (`df_guards.rs`)
- Distributed sessions: [../../../repark-distributed/map.md](../../../repark-distributed/map.md)

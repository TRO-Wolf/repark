# map — repark-core/src/parallel_window

## Purpose

TA-SINGLE-SERIES-PARALLEL-1 slices S2b and S3 (2026-10-04): generic physical operators that run
the expressions of a one-partition window (S2b) and of a one-partition projection (S3) in
parallel, and drop the useless RoundRobin over one partition (S3). DataFusion's `WindowAggExec` evaluates every
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
  `parallel_single_partition_active(&SessionState)` (re-exported from the crate root) is true
  when the state carries the `parallel_window` rule (`PARALLEL_WINDOW_RULE`, also the rule's
  `name()`) and the carrier reads on; `repark-distributed`'s provider refuses such a session
  (verifier V-1). pins: ta-series-s2b/C-014
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
  running `sum` over `ORDER BY`) stay as DataFusion planned them. **S2a (2026-10-05):** before
  that test the rule tries the partition-index arm. It fires when the `WindowAggExec`'s input is
  a `CoalescePartitionsExec` without a fetch and **every** expression has an empty `PARTITION BY`
  and an empty `ORDER BY` and is a window UDF (`StandardWindowExpr` over `WindowUDFExpr`),
  whatever the group count. Through the facade only a bare `ta.*` column that the series rewrite
  left as `OVER ()` (case (c), no declared order and no temporal column) reaches it, because the
  facade refuses `.over(...)` without `ORDER BY` for every window UDF. Through SQL, an unordered
  window UDF that DataFusion plans as a `WindowAggExec` (`ta_*() OVER ()`, for one) reads in
  partition-index order too: deterministic, otherwise the
  same answer. Every other `OVER ()` window keeps today's plan: aggregates (`sum(x) OVER ()`)
  and mixed aggregate/UDF nodes take the S2b arm over the coalesce when they have at least two
  groups, and stay a `WindowAggExec` otherwise.
- `exec.rs` — `ParallelWindowExec`. Built from the `WindowAggExec` it replaces: the same input,
  expressions, `PlanProperties`, required ordering (captured from the source node), single
  partition distribution and `maintains_input_order`; `with_new_children` rebuilds through
  `WindowAggExec::try_new` so the properties stay DataFusion's. **S2a (2026-10-05):** the
  `InputOrder::PartitionIndex` arm, built by `from_unordered_coalesce` from a `WindowAggExec`
  over `CoalescePartitionsExec ← X`: the input is X itself, the plan properties stay the
  coalesced window's (one output partition), the required input distribution is unspecified,
  there is no required ordering and input order is not maintained; `with_new_children` rebuilds
  the coalesced window around the new child. Execution spawns one `SpawnedTask` per X partition
  that collects it, then joins the tasks **in partition-index order**, so the window sees X's
  partitions concatenated 0, 1, … instead of in completion order; statistics read X's totals.
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

- `projection.rs` — **S3 (2026-10-04):** `ParallelProjectionRule` (`parallel_projection`) and
  `ParallelProjectionExec`. The rule is appended right after `ParallelWindowRule`, reads the
  same `repark.parallel` carrier (off → the plan is returned unchanged) and runs two passes.
  **R-S3-1a, the RoundRobin drop** (the earlier sketch's S4, folded into S3 by the orchestrator's
  Q-S3-1 = A ruling, narrowed by Q-S3-3 = A): a `ProjectionExec`, or a chain of them, sitting
  directly on a `RepartitionExec(RoundRobinBatch(n))` whose input has one partition **and emits
  exactly one batch by construction** (`emits_one_batch`: a `WindowAggExec` or
  `ParallelWindowExec`, directly or through `ProjectionExec` / `ParallelProjectionExec`) is rebuilt
  over the RoundRobin's input, whatever the expression count. Any other single-partition source
  (`range()`, a file scan, a multi-batch `DataSourceExec` or `MemTable`) keeps its fan-out, so its
  batch parallelism and memory-pool behaviour are unchanged. The drop is decided at the first
  non-projection parent of the chain (or at the root, which has no requirement): that parent's
  required distribution for the child must be unspecified or single-partition (a hash
  requirement is refused, because one partition would satisfy it while breaking co-partitioning
  with a sibling), and its required ordering, if any, must hold on the rebuilt child (the same
  `ordering_satisfy_requirement` test DataFusion's `SanityCheckPlan` runs). A RoundRobin over one
  partition keeps that partition's ordering, so the rebuilt chain carries the same orderings;
  the `SortPreservingMergeExec` or `CoalescePartitionsExec` above then sees one partition and
  passes its input through. RoundRobinBatch moves whole batches, so the projection still sees
  the same batches. **R-S3-1b, the parallel swap:** a `ProjectionExec` over one input partition
  with at least two non-column expressions becomes a `ParallelProjectionExec` with the same
  `PlanProperties`, expressions and EXPLAIN list (`ParallelProjectionExec: expr=[…]`).
  **Neither pass fires when any expression of the projection (or of any projection in the
  chain) is volatile** (`is_volatile`: `rand`, `randn`, the volatile time casts; orchestrator
  Q-S3-2). Execution is per input batch, so the output batch shape equals `ProjectionExec`'s: a
  batch below `PARALLEL_PROJECTION_MIN_ROWS` rows is evaluated serially in line; otherwise each
  non-column expression is one `SpawnedTask::spawn_blocking` (bounded by a semaphore of
  `min(tasks, target_partitions)`), column expressions are evaluated in line (an `Arc` clone),
  and the columns are placed by expression index. The error of the lowest expression index
  wins, as in the serial projection. Dropping the stream drops the batch future and its
  `SpawnedTask`s. A `parallel_batches` counter metric records how many batches took the
  parallel path. Single node only: the rule ships beside `ParallelWindowRule`, so
  `parallel_single_partition_active` and the distributed provider's refusal cover it. A
  RoundRobin over one ordered partition (EXPLAIN `maintains_sort_order=true`, the shape S1's
  ordered cache produces above the L1 window, where the RoundRobin spilled the whole batch) is
  dropped the same way.
  pins: ta-series-s3/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012
- `projection_tests.rs` — the S3 pins (no TA dependency): `parallel_projection_bit_identical`
  (13 × `round`, 3 × `/`, a `CASE` over a window subquery, through a session against the
  flag-off session); `parallel_projection_removes_rr_spm_sandwich` (the owner's two RoundRobins
  over window outputs, a two-projection chain under a `SortPreservingMergeExec` and under a
  `CoalescePartitionsExec`);
  `parallel_projection_small_input_serial` (the `parallel_batches` counter below and above the
  threshold); `parallel_projection_respects_parent_ordering` (an unsatisfiable merge ordering and
  a partitioned hash join keep the plan); `parallel_projection_flag_off`;
  `parallel_projection_volatile_unchanged` (a seeded volatile probe); and
  `parallel_projection_lowest_index_error`, `parallel_projection_drop_cancels`;
  `parallel_projection_drops_order_preserving_round_robin` (a sorted single-batch source under a
  `maintains_sort_order=true` RoundRobin over a `WindowAggExec` and over a `ParallelWindowExec`,
  P-S3-8); `parallel_projection_keeps_multi_batch_fan_out` (multi-batch single-partition sources
  keep their RoundRobin, P-S3-9). Window inputs come from `window` (two whole-frame `sum`s).
  pins: ta-series-s3/C-001, C-002, C-003, C-004, C-005, C-007, C-008, C-009, C-012
- `tests.rs` — the S2b pins on probe window UDFs (no TA dependency; `repark-core` cannot see
  `repark-ta`). `parallel_window_matches_serial_window_across_batches` (five expressions, three
  groups, three input batches); `parallel_window_keeps_multi_output_siblings` (three band
  siblings → one group, one compute through a thread-local cache);
  `parallel_window_skips_partitioned_and_single_group` (a `PARTITION BY` window over one
  partition, a three-partition input built without enforcement, a one-group window);
  `parallel_window_flag_off_keeps_window_agg_exec` and
  `parallel_window_flag_off_session_keeps_window_agg_exec` (the flag off, by `ConfigOptions` and
  by `ReparkSessionBuilder`, which also pins the EXPLAIN line); `parallel_window_lowest_index_error`
  (a slow failure at index 0 against a fast one at index 2); `parallel_window_drop_cancels` (one
  permit, a blocked group, the stream dropped, the queued group never runs);
  `parallel_window_deep_arg_runtime_stack` (a 4,000-deep `x + 1.0` argument on a runtime with
  32 MiB stacks, remaining stack above 16 MiB inside the evaluator); and
  `parallel_window_non_ta_windows_bit_identical` (four non-TA queries over three batches, rule on
  against off; `sum`/`avg`/`max`/`count OVER ()` and a mixed whole-frame/`lag` node fire, the
  `lag`/`row_number`/running-`sum` node plans as `BoundedWindowAggExec` and is untouched).
  pins: ta-series-s2b/C-002, C-003, C-004, C-005, C-006, C-009, C-010, C-013
  **S2a (2026-10-05):** `series_order_current_row_order_partition_index` (P-S2a-4: a
  four-partition `DelayedExec` whose partition i finishes after `(4 - i) × 60 ms`, under a
  coalesced pass-through window UDF; the arm fires with one or two groups, drops the coalesce,
  survives `with_new_children`, and three runs read ids 0…399 in source order) and
  `partition_index_keeps_aggregate_over_coalesce` (aggregates, a mixed node and a fetching
  coalesce keep today's plan). pins: ta-series-s2a/C-004
- Gates measured for the slice (goldens, kernel race, owner-shape facade identity against base,
  speed): `task/ledgers/staging/ta-series-s2b-ledger.md`. pins: ta-series-s2b/C-008, C-011, C-012

## Pointers

- Up: [../map.md](../map.md)
- Session wiring: [../session/map.md](../session/map.md) (`df_guards.rs`)
- Distributed sessions: [../../../repark-distributed/map.md](../../../repark-distributed/map.md)

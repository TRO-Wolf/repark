# Eager materialization: cache retention and repeated execution

> Closed 2026-09-13 by EAGER-OWN-1 (#PR pending); see the
> [ledger](../../../task/ledgers/completed/eager-own-1-ledger.md).

> Owner's source review, filed verbatim on 2026-09-13 with the reported symptom appended at the
> end. The unit that acted on it is [EAGER-OWN-1](../../../task/roadmap/mid-term/eager-own-1-card-2026-09-13.md). Closure per
> the review's own rule: archive to `docs/history/` when the fix and its lifecycle tests merge, or
> when an explicit decision declines the change.

Date: 2026-09-13. Class: campaign. Status: source review; proposed fixes await implementation.

This document closes when the eager-result ownership fix and its lifecycle regression tests merge,
or an explicit decision declines the change. Archive the record to `docs/history/` at closure.

## Scope and evidence

The review used fetched remote `main` at
[`0a1a3d70d272690c4f6f075fd0c235134f973d1b`](https://github.com/TRO-Wolf/repark/tree/0a1a3d70d272690c4f6f075fd0c235134f973d1b).
Its workspace version is 1.4.0. All implementation claims below refer to that revision.
The local working branch was older; it was preserved, including unrelated uncommitted documents.
The earlier conclusion that `DataFrame.eager()` was absent applied only to that older checkout.

This is a static source review. The reported workload was not executed, and no timing or memory
benchmark was run. Existing tests were read, not rerun. The ownership path is confirmed in code;
the exact cause and magnitude of the reported slowdown remain unmeasured.

No SEPMO workflow or engine changes are part of this document.

## Reported workload

The input contains 1,000,000 rows and six source columns. A lazy `withColumns` chain adds:

- EMA over `close`, plus true range from `high`, `low`, and `close`.
- Several EMA, RSI, SMA, linear regression, and ADX indicators.
- EMA over the previously computed true-range column.
- Ratios, rounding, null replacement, and a string condition.

The supplied expression adds 19 columns, producing 25 columns if all names are new.
The user reports that repeated bare calls to `temp_df.eager()` become slower and retain more RAM.
Calling the session's `catalog.clearCache()` relieves the problem.

The report does not establish the installed wheel revision, machine memory, source types,
window ordering, number of independent time series, partition target, or notebook output retention.
Those details are needed for a reproducible performance measurement.

## Confirmed execution and ownership path

`DataFrame.eager()` delegates to `_eager_materialize`. The helper:

1. Creates a new same-plan child of the input DataFrame.
2. Marks that child for persistence.
3. Materializes the child through the cache path.
4. Counts the materialized result to record its shape.
5. Returns the child without changing the input frame.

The child starts without a cache registration. Cache materialization chooses a new scratch name,
collects the result, registers an in-memory table, and changes the child to scan that table.
It retains the previous plan in `_lineage_inner` so `unpersist()` can restore lazy execution.

Therefore, repeated calls on the original lazy frame have this behavior:

```text
temp_df.eager() -> evaluate original plan -> register result A -> return child A
temp_df.eager() -> evaluate original plan -> register result B -> return child B
temp_df.eager() -> evaluate original plan -> register result C -> return child C
```

The original `temp_df` remains lazy. Repeating the call does not reuse the previous child.
Each run must compute the requested TA outputs again, unless an upstream materialization already
serves them. The final shape count operates over the newly built table; it does not itself imply
a second evaluation of the original TA plan.

### Why discarded results remain resident

The session catalog holds the registered in-memory table. Losing the Python child does not remove
that catalog entry. The Python cache-frame registry is a `WeakSet`; it does not own or finalize
the registered table.

An existing regression test explicitly establishes this sequence:

1. Cache and materialize a frame.
2. Delete the frame and force Python garbage collection.
3. Confirm the frame is gone but its cache table remains registered.
4. Call `clearCache()` and confirm that the table registration is gone.

`clearCache()` first unpersists live registered cache frames. It then removes orphan tables whose
names use the cache prefix. Checkpoint and other temporary-view families are outside that sweep.
Other live plans or exported results can still hold Arrow buffers after a registration is dropped.

### Repeating eager on an eager frame

The helper also has no reuse branch for an already eager input. Calling `computed_df.eager()`
creates another child and another registration over the cached input. This need not rerun the
original TA kernels, and Arrow buffers may be shared. It still creates more ownership and lineage
to manage. Registration growth must not be equated with a full physical data copy in every case.

## Findings and their limits

| Finding | Evidence status | Consequence |
|---|---|---|
| Bare eager calls create separate materializations of the original lazy plan | Confirmed in source | Repeated calls recompute the requested results |
| Discarded eager children can leave registered cache tables | Confirmed in source and an existing test's assertions | Results remain reachable from the session |
| Eager-on-eager has no reuse fast path | Confirmed in source | Extra registrations and lineage can accumulate |
| Cache materialization has no disk spill | Confirmed in source | Retained cache results remain in memory |
| The cache size guard runs after collection | Confirmed in source | It cannot prevent the full collection peak |
| The guard applies to one materialization, not total session cache usage | Confirmed in source | Many individually acceptable results can exceed a practical memory budget |
| Retained results cause the observed timing degradation | Strong explanation, not measured | Measure memory pressure, allocation cost, and paging before assigning a cause |

The retention mechanism is in RePark's eager/cache lifecycle. Changing DataFusion's
`target_partitions` cannot release these tables.

## Memory and TA execution implications

One million Float64 values require 8,000,000 bytes for their value buffer. The expanded result can
therefore occupy hundreds of megabytes before accounting for temporary buffers and other state.
This is a sizing illustration, not a measured allocation count. Actual types, nulls, string storage,
shared buffers, and the physical plan determine the footprint.

The reviewed TA evaluator processes a full window partition through `evaluate_all`. It produces
output arrays and may use temporary numeric input buffers. Null-free single-input Float64 paths
can borrow the input values. Multi-input or converted inputs can require additional buffers.
EMA over `tr` depends on an earlier indicator stage.

These costs explain why each recomputation has a meaningful memory peak. They do not establish
an independently leaking TA kernel. The multi-output TA cache serves families such as MACD and
Bollinger Bands; the supplied indicator list does not establish that cache as the cause.

As retained results accumulate, later executions have less available memory. Allocation pressure
or operating-system paging could increase runtime. A high resident-memory reading alone does not
prove live-object retention because allocators can retain freed memory. Here, surviving catalog
entries provide a separate, concrete ownership signal.

## Immediate usage guidance

Materialize once and keep the returned frame for subsequent work:

```python
computed_df = temp_df.eager()

computed_df.show()
computed_df.count()

# Release this cache when its materialized result is no longer needed.
computed_df.unpersist()
```

Do not call `computed_df.eager()` again just to display it. Ordinary actions reuse its materialized
data. Calling `temp_df.unpersist()` does not release the separate child's registration.
`unpersist()` restores the child's prior lazy plan; later actions can recompute it.

For deliberately repeated executions, release each returned frame:

```python
computed_df = temp_df.eager()
try:
    computed_df.show()
finally:
    computed_df.unpersist()
```

This example demonstrates cleanup, not a timing harness. Benchmark collection, display, and
cleanup separately. Notebook output history or derived frames can retain additional references.
Avoid repeatedly overwriting an eager result without releasing its prior registration.

Session-wide `clearCache()` is a recovery tool. It also invalidates unrelated session cache pins,
so it should not be required after every ordinary eager evaluation.

## Proposed corrections

These are recommendations, not approved API changes or completed work.

### 1. Give eager results explicit shared ownership

Keep the public behavior that eager returns a new frame and leaves the source unchanged.
Existing tests require that behavior. Change the lifetime of the backing materialization so an
abandoned eager result does not leave an indefinite catalog registration.

Ownership must cover derived lazy frames, the `.lazy()` result, exported Arrow objects, and any
other caller holding the materialized data. Do not attach a blind finalizer that drops a table as
soon as one Python parent disappears. A child may still need it. Evaluate an engine-owned backing
object or a shared lifetime handle before choosing the smallest suitable implementation.

Keep eager-result lifetime distinct from explicit session cache semantics where necessary.
Do not silently change every `cache()`/`persist()` lifetime to repair eager materialization.
Preserve the documented effects of `unpersist()`, `clearCache()`, and session shutdown.

### 2. Reuse an already materialized input

Define eager-on-eager behavior explicitly. Prefer reuse of the backing materialization, while
preserving any required new-wrapper identity. Test cleanup through each wrapper so releasing one
does not unexpectedly invalidate another. Reuse must respect invalidation and session shutdown.

Do not introduce automatic plan-equivalence caching as part of this fix. Fresh evaluation of an
original lazy plan can legitimately observe source changes or nondeterministic expressions.

### 3. Bound and expose retained cache memory

Keep the existing per-result size guard honest. Consider a session cache budget, incremental
admission during collection, and observable retained bytes and table counts. Define how shared
Arrow buffers are charged; summing every table's apparent size can double-count shared data.

A limit should refuse clearly or follow an explicit eviction policy. It must not silently evict
live eager snapshots or claim to bound all process memory. Treat a spill-backed cache as a separate
design decision; the existing storage-level name does not provide that behavior.

## Validation required before a fix is accepted

Start with a small deterministic fixture to isolate ownership. Then reproduce the million-row
TA workload using a recorded package revision, machine, source types, and ordered window definition.

| Scenario | Required evidence |
|---|---|
| Repeated eager calls on the original lazy plan, discarding results | Abandoned eager registrations do not accumulate after their owners are released |
| Reuse one eager frame across actions | No new materialization; values and Arrow types remain unchanged |
| Eager called on an eager frame | Backing reuse follows the chosen identity contract |
| Parent released while a derived frame or `.lazy()` result survives | Remaining users still produce correct values and types |
| Explicit unpersist, clearCache, and session stop | Documented behavior remains correct; repeated cleanup is safe |
| Collection or post-registration failure | The attempt leaves no unintended cache registration |
| Several simultaneous eager results | Each remains valid; budget accounting and cleanup match the ownership contract |
| Original source changes between eager calls | Fresh lazy-plan evaluation and existing snapshots retain their intended semantics |

Record per-iteration execution time, post-cleanup and peak resident memory, cache registration
count, retained bytes where measurable, memory-pool reservations, and paging activity. Separate
cold execution, warm execution, and result display. A small post-warmup memory plateau differs
from continued growth. Do not claim a speedup without comparable before-and-after measurements.

## Source references

All links are pinned to the reviewed commit so this report remains usable after file movement.

- [Eager materialization helper](https://github.com/TRO-Wolf/repark/blob/0a1a3d70d272690c4f6f075fd0c235134f973d1b/python/repark/src/repark/spark/dataframe/eager.py#L63)
- [Cache child creation and registration](https://github.com/TRO-Wolf/repark/blob/0a1a3d70d272690c4f6f075fd0c235134f973d1b/python/repark/src/repark/spark/dataframe/core.py#L448)
- [Weak cache-frame registry](https://github.com/TRO-Wolf/repark/blob/0a1a3d70d272690c4f6f075fd0c235134f973d1b/python/repark/src/repark/spark/dataframe/core.py#L225)
- [Unpersist and public eager methods](https://github.com/TRO-Wolf/repark/blob/0a1a3d70d272690c4f6f075fd0c235134f973d1b/python/repark/src/repark/spark/dataframe/core.py#L928)
- [Session-wide cache cleanup](https://github.com/TRO-Wolf/repark/blob/0a1a3d70d272690c4f6f075fd0c235134f973d1b/python/repark/src/repark/spark/catalog.py#L231)
- [Rust collection and post-collection budget check](https://github.com/TRO-Wolf/repark/blob/0a1a3d70d272690c4f6f075fd0c235134f973d1b/crates/repark-core/src/session/temp_views.rs#L135)
- [Existing orphan-registration test](https://github.com/TRO-Wolf/repark/blob/0a1a3d70d272690c4f6f075fd0c235134f973d1b/python/repark/tests/test_cache_persist.py#L191)
- [Existing eager API tests](https://github.com/TRO-Wolf/repark/blob/0a1a3d70d272690c4f6f075fd0c235134f973d1b/python/repark/tests/test_df_eager_1.py)
- [TA partition evaluator](https://github.com/TRO-Wolf/repark/blob/0a1a3d70d272690c4f6f075fd0c235134f973d1b/crates/repark-ta/src/udf/mod.rs#L660)

## Reported symptom (owner, 2026-09-13)

Running `temp_df.eager()` bare, without assigning the returned DataFrame, kept filling RAM and
slowing each subsequent call down.

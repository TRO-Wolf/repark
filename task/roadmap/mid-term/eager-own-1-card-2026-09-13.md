# Card EAGER-OWN-1 — eager results own their materialization (2026-09-13)

Chartered by the owner on 2026-09-13 from the filed
[eager materialization retention review](../../../docs/history/eager-own-1/eager-materialization-retention-review-2026-09-13.md).
Run by an Opus orchestrator (run 11) with a Devin actor, a Grok critic-logic round and the S2-21
Python perf reviewer. Product code only in `python/repark/src/repark/spark/dataframe/` (`core.py`
is no longer fenced — COMMENT-CORE-1 merged as #561), `python/repark/src/repark/spark/catalog.py`
and, only if step 1 needs an engine-side handle, `crates/repark-core/src/session/temp_views.rs`
plus its `repark-python` binding.

## Why

A bare `temp_df.eager()` (result not assigned) evaluates the whole plan, registers a MemTable
under a fresh `__repark_cache_*` name and returns a child that is dropped at once. Nothing owns the
registration: the frame registry is a `WeakSet`, `unpersist()` on the source does not know the
child, and only `catalog.clearCache()` sweeps orphans by prefix. Each repeat therefore adds a full
result set to resident memory and the next evaluation runs under more pressure. The owner measured
this on a 1,000,000-row, 25-column TA `withColumns` chain: every repeat slower, RAM climbing,
`clearCache()` the only relief. The review confirms the path in source at `0a1a3d70`; nothing was
measured there, so step 0 measures first.

## Measured facts (main `893e72be`)

- `eager.py::_eager_materialize` — `frame._identity_child()`, `_persist_requested = True`,
  `_materialize_cache_if_needed()`, then `_action_inner().count()` for `_eager_shape`. No branch
  for an input that is already eager or cached.
- `core.py::_materialize_cache_if_needed` — for the cache branch: scratch name, the session's
  `materialize_as_cache_view(view_name, lineage, max_bytes)`, `_inner` becomes
  `SELECT * FROM view`, `_lineage_inner = lineage`, `_cache_view = view_name`, then
  `_register_cache_frame` adds the frame to the WeakSet under the session's alive token.
- `core.py::unpersist` — drops `_cache_view` if set, restores `_lineage_inner`, clears
  `_eager_shape`. It only knows its own frame.
- `eager.py::_to_lazy` — `_spawn_preserving_identity(frame._inner)`: the `.lazy()` copy scans the
  same view by name and holds no reference to the view's owner. Derived frames (`select`, `filter`,
  joins …) likewise carry the view name inside their plan and nothing else.
- `catalog.py::clearCache` — unpersists every live registry frame, then drops every temp view whose
  name starts with `_CACHE_VIEW_PREFIX`. Checkpoint views (`__repark_ckpt_`) are outside the sweep.
- `temp_views.rs::register_collected_memtable` — collects fully, then applies the
  `repark.cache.max_bytes` guard, then registers a `MemTable` of `Arc<RecordBatch>`. Exported Arrow
  objects share those buffers by refcount; dropping the registration does not invalidate them.
- Existing pins: `python/repark/tests/test_cache_persist.py` (the orphan-registration sequence at
  the `gc.collect()` test — it asserts the table SURVIVES the frame; step 1 flips that assertion),
  `test_df_eager_1.py`, `test_dfcore_4b_eager_goldens.py`, `test_dfcore_6_eager_preview.py`,
  `test_sql_dml_eager.py`.

## Decisions

- **D-1 Public behaviour unchanged.** `eager()` returns a new frame; the source stays lazy; the
  existing eager pins stay green as written except the one orphan assertion named above, which the
  ledger re-states as its inverse.
- **D-2 Shared ownership, not a blind finalizer.** A materialized cache view is owned by a
  refcounted handle. Every frame whose plan scans that view holds the handle: the materializing
  frame, every `.lazy()` copy, every derived frame spawned from any holder (joins and unions hold
  the union of their parents' handles), and every eager-on-eager wrapper. The registration is
  dropped when the last holder is released (`weakref.finalize` on the handle with `atexit=False`;
  the drop is skipped when the session is already stopped) or when `unpersist()` / `clearCache()`
  drops it explicitly, whichever comes first. Explicit drops keep today's semantics: a derived
  frame over an explicitly unpersisted view fails the way it fails today. Exported Arrow objects
  (`toArrow`, `toPandas`, `to_polars`, `collect`) need no handle — pin that they stay valid after the
  registration is gone.
- **D-3 `cache()` / `persist()` lifetime unchanged.** Only the ownership of the view moves into the
  handle; a `cache()`d frame's view is still dropped by `unpersist()` / `clearCache()` and now ALSO
  when the last holder dies. Say so in the ledger and the dataframe map; it is the same mechanism,
  not a new policy.
- **D-4 Eager-on-eager reuses the backing.** `eager()` on a frame that already scans a live cache
  view returns a new wrapper sharing the handle and the shape; no collection, no new registration.
  Releasing one wrapper never invalidates another. A frame whose view was explicitly dropped is not
  "already eager" and materializes afresh.
- **D-5 No plan-equivalence caching.** Two `eager()` calls on the same lazy source still evaluate
  twice; source changes and nondeterministic expressions stay observable.
- **D-6 Budget and observability deferred.** The review's proposal 3 (session cache budget,
  incremental admission, retained-bytes accounting) is a separate card, EAGER-BUDGET-1, opened by
  the run-11 report with the step-0 numbers; it is not in this unit.
- **D-7 Measure first.** Step 0 records the reported degradation on a fixture before any fix and
  the same fixture after; no speed or memory claim without both numbers.

## Steps

- **Step 0 (Devin, measure).** A deterministic fixture: 1,000,000 rows, one ordered series
  (`ts`, `open`, `high`, `low`, `close`, `volume`), a `withColumns` chain of `repark.spark.ta`
  `ema` / `sma` / `rsi` / `adx` / linear regression plus true range and an `ema` over it (about
  19 added columns). Ten bare `temp_df.eager()` calls, results not assigned: per-iteration wall
  time, peak and post-iteration RSS, the count of `__repark_cache_*` names in
  `catalog.listTables()`, all in a subprocess with the release native (`maturin develop --release` in the lane's
  venv, the S2 D-3 idiom; never rank on the debug build). Write the harness under
  `python/repark-parity/tests/` as a subprocess-isolated pin in the style of the spill matrix
  (opt-in through an environment variable so CI does not run the million-row loop) that asserts
  registration count == 10 on the base tree (the red), and record the numbers in the ledger.
- **Step 1 (Devin, implement).** The handle per D-2 / D-3: red-first pins for every row of the
  review's validation table — repeated bare eager (registrations return to 0 after `gc.collect()`),
  reuse across actions (no new registration), eager-on-eager (D-4), parent released while a
  `.lazy()` copy or derived frame survives (values and Arrow types unchanged), unpersist /
  clearCache / session stop each idempotent, collection failure leaves no registration, several
  simultaneous eager results independent, source change between eager calls observed. Flip the
  orphan assertion in `test_cache_persist.py`. Re-run the step-0 harness: registration count 0
  after the loop, RSS plateau, per-iteration time flat; paste both tables.
- **Step 2 (Devin, close).** Dataframe map rationale, `docs/guide` eager/cache lifetime paragraph
  (holder semantics, explicit drop, what `clearCache()` still does), ledger departure. The review
  document moves to `docs/history/` per its own closure rule. The report opens EAGER-BUDGET-1 with
  the step-0 numbers.

## Gates

Facade suite `python/repark/tests`, the parity suite, `make check-lib-py`, `make check-docs-links`,
`make check-ledger-grammar`, `make verify`; the step-0 harness before and after. Size ceilings move
only down; if `core.py` would cross its 4117-line ceiling the handle lives in a new
`dataframe/cache_handle.py` listed in the dataframe map. No code comments. S2-21 applies: one Grok
Python perf reviewer on the branch (the handle must not add per-spawn cost visible on the
`withColumn` chain benchmark) and one Grok critic-logic round on the ownership contract before the
PR.

## Owner questions (answer in the run-11 report, default applies until ruled)

- **Q-E1** Should `cache()`/`persist()` views also die with their last holder (D-3, default yes)
  or keep the explicit-only lifetime and leave the handle to `eager()` alone?
- **Q-E2** EAGER-BUDGET-1: a session-wide `repark.cache.max_total_bytes` that refuses (default) or
  evicts oldest un-held snapshots?

# task/wo/temp-view-cache-lazy-parse-1/built/ — A1, the lazy cache table and the DataFrame doors, built

**The defect (F1, F8, F5 children).** A lazy `.cache()` of a JSON or CSV frame filled early, under the creating
zone: at a view's registration, at `explain`, at a write and at a `mergeInto` source (`_native_for_registration`
materialised a pending cache). A child built after `cache()` never read the parent's cache (object identity), and
`writeTo` of a cached frame kept the frame's stamps where Spark's writer re-analyses. Spark builds the cached plan at
`cache()` and fills it at the first execution of a plan that reads it, under the live settings
([../sketch.md](../sketch.md) §1).

**The fix (seat A, as the sketch has it, amended by Fable's §Q1 and §14).**

- **The table.** `cache()` and `persist()` register a lazy cache table under the `__repark_cache_*` name and bind
  the frame to it at once (`crates/repark-core/src/session/temp_views/lazy_cache.rs`, `cache_handle._bind_lazy`). So
  views, writes, `explain`, joins, the `mergeInto` source and every child built after `cache()` scan one table.
- **Fill at execution, never in `scan`.** `scan` returns a `LazyCacheExec` leaf and plans nothing. Its first
  `execute` decides synchronously: it enters a fresh reader-parse memo around the analysis and optimize of the held
  stripped twin, using the live state's planner and the twin's options. It then fills once through an async
  once-cell. A concurrent first use waits for the same fill; a failed or dropped fill keeps nothing.
- **Adoption covers children.** The acting thread's memo is read in `scan`; planning runs on the acting thread
  inside the own-plan scope. The memo is carried in the leaf. Whichever frame's own-plan action reads the table,
  armed or filled, adopts the fill's decisions where its memo holds none (`ReaderParseMemo::adopt`).
- **The released state.** `unpersist` and `clearCache` (which unpersists every cached frame) release the table
  through `CacheViewHandle.release`, before the registration is dropped. A frame whose plan still scans it gets a
  pass-through: each execution plans the lineage under the acting memo, settling it if unsettled, or live, and keeps
  nothing. `clearCache` is `unpersist`: it does not leave a fill armed to refill and keep.
- **Rule (a) part 3.** A frame whose memo was settled before `cache()` keeps its own lineage for its own-plan
  actions. The table holds that owner memo only when it was settled at registration, and gives it the pass-through.
  Children, views, `count` and SQL read the cache.
- **The carrier, proved.** The memo crosses no thread through the thread-local. The leaf holds it, and every
  planning or collection future the fill or pass-through runs re-enters the chosen memo on each poll, on whichever
  thread polls. The Rust pins and the 56 Spark cells prove the five must-hold shapes and both `explain` cells.
- **Deep plans.** The analysis, optimize, physical planning, collection and drop of the held twin run on a stack
  grown to the twin's measured depth, from the same depths today's materialisation uses (`drive_segment_bytes`).
  The outer plan is a leaf. Pinned by a 2,000-filter lineage in a subprocess on an 8 MB thread.
- **Ordering and the single-partition flag.** These come from the twin's physical plan at `cache()`, planned under a
  throwaway memo, as `register_collected_memtable` takes them; Spark builds its cached physical plan there. They are
  declared on the leaf from the first use. A fill whose plan does not deliver the declared order is sorted to it.
- **Budgets.** `repark.cache.max_bytes` and `max_total_bytes` apply at the fill through the shared
  `drain_budgeted`, with the old refusal text and class. The facade refreshes them from the conf at each action of
  a frame that holds cache handles, so a conf change between `cache()` and the first use applies as before. **A
  child's first action now fills the parent:** over its budget, the child's action raises the refusal and the table
  stays armed; the next use retries. `retained_cache_bytes` counts a filled table.
- **No strong cycle.** The table holds the twin's logical plan and options, and a weak reader of the live state,
  never a `SessionState`. The session's state frees with a filled table registered (Rust pin).
- **F8 (Q6).** All four `writeTo` verbs re-analyse on Spark (bar §5), so the V2 writer's registration reads the
  lineage when the frame's settings differ from the live ones, and the table when they are equal.
- **`repr`** of an unfilled cached frame renders the schema header and runs nothing (`display._repr_renders_schema`).

**Unchanged, out of A1:** map-bridge frames (`udf`, `pandas_udf`, `mapInArrow`, `mapInPandas`) keep today's
materialisation at the action (seat B); `CACHE [LAZY] TABLE` and `catalog.cacheTable` keep their eager `count`, and
`REFRESH` its view no-op (A2). An invalid budget conf at `cache()` falls back to today's path, so it still raises at
the action. `catalog.py`'s `clearCache` docstring still says "object-identity"; A2 owns that file.

**Product files (11):** `lazy_cache.rs` (new), `temp_views.rs`, `cache_budget.rs`, `extension.rs`, repark-python
`session.rs`, `core.py`, `cache_handle.py`, `writer_layout.py`, `display.py`, `ml/feature/_transformers.py`, and
`scripts/check_lib_py.py`. The script's two baselines are ratcheted down: `core.py` 3,461 to 3,452 and
`_transformers.py` 2,717 to 2,715. Against the sketch's A1 list of seven, `surface_a.py` was not needed: the adoption
moved into Rust. Four files were added:
- `extension.rs`: the memo's `adopt`, `is_settled` and `within`;
- `cache_budget.rs`: it counts a filled table;
- `display.py`: `repr`;
- `_transformers.py`: the ML row-id helper cached, registered a view and unpersisted before its lazy query ran,
  relying on the old snapshot. Under Spark's release semantics that re-ran `row_number()` once per reference
  (`RegexTokenizer`'s row-order pin went red), so the helper now uses `localCheckpoint()`.

The brief allowed one more file; the other three are reported in the hand-back.

## Contents

- [bar.md](bar.md) — the cells: step 0's 2,402 and the revision's 240 on main and the head, the required 188, the
  forecast 128, Fable's ten, the four `writeTo` verbs, the pins, what stays WRONG and where it belongs, `EXPLAIN`.
- [cost.md](cost.md) — the cost rows under the short-row rule and C-012.
- [evidence/](evidence/map.md) — the new probes, the Spark and RePark recordings, the verdicts.

## The rulings this unit carries

- **Owner, 2026-11-18, verbatim:** "We better go with B to be safe" (the reader-parse family blocks the label).
- **Owner, 2026-11-19, about 14:00 UTC:** C-012 a hard 1.05x; a short row over 1.05x a flag attributed by swap or
  instruction counts; nothing over 1.15x without the owner.
- **Owner, 2026-11-20, about 00:22 UTC, verbatim:** "Let's go with defaults across the board".
- **Fable's guidance under that delegation, not an owner ruling** (`claude-fable-5-2`, 2026-11-20; outside the
  repository): A1 may start; Q1 is rule (a) with two additions (adoption covers children; the released state); the
  bar splits into 188 required and 128 forecast cells; Q6's `writeTo` verbs are in A1; §14's twelve brief items.

Up: [../map.md](../map.md).

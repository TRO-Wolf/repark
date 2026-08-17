# SE-1 — declared-sorted temp views (sort elimination, PR-A: engine seam)

Unit: SE-1 (evening wave E, 2026-08-16; owner P1 "TA lib optimizations first"). Funded by
the release-build engine-time split: `SortExec` is ~77% of engine time on the windowed
serving shape; elision is worth up to 4.3× single-series. Orchestrator-executed.

## Evidence (probe, 2026-08-16, DF 54.1)

Scratch probe (not committed): identical 1.2M-row (symbol, ts)-sorted data registered as a
plain `MemTable` vs `with_sort_order`:

| cell | plain | declared |
|---|---|---|
| tp=1 | SortExec ×1 | **SortExec ×0** |
| tp=default (RepartitionExec ×1 present at this scale) | SortExec ×1 | **SortExec ×0** |

Results byte-identical in every cell. The declared ordering survives DataFusion's hash
repartition — no `prefer_existing_sort` needed. At small row counts (≲6k) DF plans no
repartition at all; the committed pins therefore assert only the `SortExec` contract.

## Decisions

- **Trust model: declare + ALWAYS verify, refuse loud.** O(n) adjacent-pair lexicographic
  pass (ASC NULLS LAST, cross-batch) at declaration time; no skip switch. A wrong claim
  raises `Analysis` naming the offending row pair and the original registration stays.
- **Scope: in-memory (`MemTable`) views only** — createDataFrame/cache frames, the bench
  serving path. Parquet `file_sort_order` and Iceberg sort-order metadata are later
  increments. Non-`MemTable` providers refuse loudly.
- Sort keys are ENGINE field names via `Column::from_name` (no ident parsing — the
  U-DF-1 lowercase-fold class cannot recur here).

## Delivered (this PR)

- `crates/repark-core/src/sorted_view.rs` — verification + declared order construction.
- `crates/repark-core/src/session.rs::declare_temp_view_sorted` — the public door.
- `crates/repark-python/src/session.rs::declare_temp_view_sorted` — PyO3 binder
  (GIL released for the scan). Facade `df.declareSorted(...)` is PR-B (after the
  conductor-17 explode fix merges — it owns the facade bind seam this wave).
- `crates/repark-core/tests/declared_sorted.rs` — plan pins + refusal battery.

## Residue / next (after PR-A)

- PR-B: facade `declareSorted` + display→engine key resolution + facade pins + a
  serving-shape EXPLAIN pin; timing numbers on release builds in a quiet-machine window
  (shared handshake with the allocator lane).
- Parquet + Iceberg ordering declarations: separate increments, design in the SE-1 section
  of the wave plan.

# PR-B — the facade door (2026-08-17)

## Headroom extract first (T0b, move-only)

`core.py` sat at 8199 of an 8200-line ceiling, so PR-B had to make its own room. The
module-level helper block that trailed the `DataFrame` class — the r23b N2 plan-collapse
helpers, the G2 range-order gate, the `show` / eager-eval / polars / duckdb formatters, the
Arrow→display and Arrow→SQL type mappers, the r24 DF1 `dynamicFlatten` struct expander and
the r20 H1 join-qcol rewriters — moved **verbatim** to
`python/repark/src/repark/spark/dataframe/plan_collapse.py`.

| | before | after |
|---|---|---|
| `core.py` lines | 8199 | 7224 |
| `core.py` ceiling | 8200 | **7350** |
| `plan_collapse.py` | — | 1096 (default 2500 ceiling; no EXCEPTIONS row) |

32 defs + 3 module constants moved; not one line of their bodies changed. The new module
imports nothing from `core` at module scope (the two `core`-side names it mentions are
annotations only, under `TYPE_CHECKING`), which is why it can be imported *before* the
other region modules. `core.py` re-exports **all 35** moved names from the existing tail bind
block (the r27 T0 precedent: every moved private helper stays reachable, not just the ones
still called) — that block is now hand-ordered (`# noqa: E402, I001`) because
`joins_columns` and `writer_readwriter` import two of the moved helpers *from core*, so
`plan_collapse` must bind first. Every
`repark.spark.dataframe[.core]` import path is unchanged (Q7 freeze).

## Door shape

`DataFrame.declare_sorted(*cols)` with the disclosed camelCase alias `declareSorted`
(one function object, not two implementations). It:

1. refuses `PySparkValueError` with no keys;
2. refuses `PySparkValueError` naming "declareSorted applies to source frames" unless the
   frame carries `_source_view_name` — a new `__slots__` entry stamped **only** by the two
   `__repark_cdf_*` materializers in `session/_funcs.py`, and copied by no `_spawn` path, so
   every transformed frame refuses without needing to inspect its plan;
3. resolves each display name through `_resolve_getitem_column_name` (case-insensitive,
   raises listing the available columns) then `_engine_field_for_display` (the H1
   display→engine overlay) — the same pair `select` uses, so mixed-case createDataFrame
   fields declare under any spelling and the engine always receives ENGINE field names;
4. calls the native `declare_temp_view_sorted`, which ALWAYS verifies and refuses loud;
5. **re-plans its own `_inner`** — see below;
6. returns `self`, so the call chains and declaring twice is idempotent.

### The re-plan (the non-obvious half)

A frame's logical plan captures the table source at planning time. `declare_temp_view_sorted`
re-registers the view's `MemTable`, so a frame built before the declaration keeps scanning
the *old* provider: measured, the declaring frame planned `SortExec ×1` while a frame created
afterwards planned `SortExec ×0`. The door therefore re-issues `SELECT * FROM <view>` into
`_inner` after a successful declaration. That is exact here and only here: the door only ever
runs on a source frame, whose plan *is* that scan. Mutation-tested by the plan pin.

## Measured, on the facade (tp=1, createDataFrame source)

| shape | undeclared | declared |
|---|---|---|
| SQL window, `ORDER BY ts ASC NULLS LAST` | SortExec ×1 | **×0** |
| SQL window, `ORDER BY ts` (Spark default) | ×1 | ×1 |
| `Window.partitionBy(sym).orderBy(ts)` facade spec | ×1 | ×1 |

Results identical in every cell.

## Residue — the NULLS-placement gap (honest, and it bounds the win)

The engine declares **ASC NULLS LAST** (DataFusion's `ORDER BY` default). Spark's
`ORDER BY x ASC` is **NULLS FIRST**, and repark's `WindowSpec` follows Spark: ascending
window keys always plan `nulls_first=true`. For a *nullable* key those two orderings are
genuinely different, so DataFusion correctly declines the elision — which is why the table
above elides only when the query spells `NULLS LAST`. Measured corollary: registering the
same data with a **non-nullable** Arrow field DOES elide under the Spark-default window
(nulls cannot occur, so the placement is moot), but `createDataFrame` builds all-nullable
Arrow schemas, so that route is not reachable from the facade today.

So the door works and is pinned, but the headline serving shape
(`Window.partitionBy(sym).orderBy(ts)` over a nullable `ts`) does **not** yet elide. Closing
it is an engine decision, not a facade one, and there are two candidate shapes — declare both
null placements when the column is non-nullable-in-fact, or declare the ordering the way the
data actually is (a null-count check during the verification scan already touches every key
value). Neither is in this PR: PR-B's fence is facade-only.

## Non-goals carried forward

- **The cache door.** `cache()` / `persist()` register through `materialize_as_cache_view`
  and redirect the SAME handle in place, while `_source_view_name` keeps naming the original
  cdf view — so the source-frame gate alone does NOT catch a cached frame (SQM review
  finding on the first cut of this PR: declaring after cache silently un-pinned the cache
  while `is_cached` kept reporting true). The door now refuses explicitly when
  `_cache_view` / `_persist_requested` / `_checkpoint_lazy` is set: declare first, cache
  afterwards (both-orders pinned). Declaring a cache view itself: recorded, not attempted —
  the design scoped PR-B to createDataFrame.
- No session-wide "assume sorted" conf, no unverified fast path, no parquet/Iceberg ordering
  (all still PR-A's non-goals).

## Pins (`python/repark/tests/test_declare_sorted.py`, 13 nodes)

Results bit-identical declared vs undeclared (`to_arrow().equals`); the plan pin both ways
plus the `output_ordering=… ASC NULLS LAST` the scan now advertises; unsorted data refusing
loud with the row indices and the view still answering afterwards; transformed frames
(`filter` / `select` / `withColumn`) refusing with the source-frames message; no keys;
unknown name listing the available ones; case-insensitive resolution of capitalized fields;
snake and camel being one function; declaring twice being idempotent.

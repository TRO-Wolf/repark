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

## Residue / next

- PR-B: facade `declareSorted` + display→engine key resolution + facade pins + a
  serving-shape EXPLAIN pin; timing numbers on release builds in a quiet-machine window
  (shared handshake with the allocator lane).
- Parquet + Iceberg ordering declarations: separate increments, design in the SE-1 section
  of the wave plan.

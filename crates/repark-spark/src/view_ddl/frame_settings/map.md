# map — repark-spark/src/view_ddl/frame_settings

## Purpose

The unit tests of `../frame_settings.rs` (TEMP-VIEW-SESSION-SETTINGS-1 part 2, 2026-11-18). That file is the DataFrame
door's seat: `FrameView` captures the frame's own settings (a `_repark_writer_v2_` scratch view takes the writer's zone,
as Spark's `writeTo` re-analyses) and inlines a `FrameSettings` marker; the analyzer's first rule (`FrameSettingsRule`)
unwraps it when the planning state's settings match, else closes it as a `TableScan` over the frame analysed under its
own settings, planned by part 1's overlay; `opened` and `definition_plan` read through a closed frame, and the SQL
door's bare `ReplanningTempView` scan and a frame that only scans (columns over a scan, whose source reads the
reader's settings at scan time, as Spark's file scans do) keep `into_view`. A lineage reaching a JSON or CSV
scan no longer keeps `into_view` (delta 1's `reader_lineage` and part 1's inlined-frame fallback are removed by
TEMP-VIEW-FRAME-READER-PARSE-1, below). Delta 2: outside a SQL view body (`view_body`, set around
`ReplanningTempView`'s re-planning) a frame view decides at inline time against the session's live options
(`LiveOptions`): its own plan when they match, the closed `TableScan` when they differ, so a walker that reads the
unanalysed plan (the store-assignment check's `negated_null_store::column_origin`) sees what `into_view` gave; inside
a body, or with no live reader, it inlines the marker. Delta 3 (the verifier's F1): the live read waits a bounded time
for the session state's lock and, past it, answers the options it last read (from the view's creation on), so another
thread's `spark.conf.set` never makes a frame view inline the marker outside a body.

TEMP-VIEW-FRAME-READER-PARSE-1 (2026-11-19): the facade wraps each JSON and CSV reader frame in
`repark_core::extension::ReaderParse` (its parse plan as its input and as an unanalysed copy, the session's one
`LiveOptions` handle, the frame's analyzer). Analysis leaves it in place. `ReaderParseRule` (`repark_reader_parse`)
is the optimizer's first rule, put in through the seat; it walks subqueries itself and compares the session's live
options with those the plan is optimized under: the same, the node becomes its analysed input; different, it
becomes a closed `FrameView` over its copy analysed by the node's analyzer under the live settings (`parse_closed`,
`closed_under`). So the parse runs under the session that reads, as Spark's file scan does, and every expression
above it keeps the frame's settings, with or without a view.

Its delta (the verifier's F1, 2026-11-19): Spark fixes a Dataset's file scan at the first action that runs its own
`QueryExecution` (`collect`, `toLocalIterator`, `toArrow`, `toPandas`, `tail`, `rdd`, `foreach`, `foreachPartition`,
`localCheckpoint`, `checkpoint`) and keeps it for every later own-plan action on that object; `count`, `show`,
`take`, writes and derived frames plan afresh. So the decision is held per frame object, not per node, since derived
frames share the node: the facade runs each own-plan action between `ReaderParseMemo::enter` and `restore`
(`repark_core::extension`), which hold the frame's memo in a thread-local for that call, and `ReaderParseRule`,
once `holds_parse` finds a node, decides it under the settings the memo recorded for the node's session at its first
decision (recording the live ones then). With no memo in scope it reads the live settings as before.

## Contents

- `tests.rs` — the marker rule is the analyzer's first rule and no function rewrite runs before it; a frame view read
  with matching settings analyses to the plan `into_view` gives; a frame held under ANSI on and read under off
  closes as a `TableScan` and raises Spark's `ARITHMETIC_OVERFLOW` where `into_view` wraps; inside a SQL view body the
  marker is decided against the planning state, and the closed scan's projection restores the frame's schema exactly; `opened`,
  `definition_plan` and `get_logical_plan` read through a closed frame to its plan, and its schema carries no
  attribute id; a frame view answers `temp_view_column_comments` and `renamed_temp_view` as `into_view` does; the
  SQL door's registration stays a bare scan; materialisation takes the frame's options and a fresh start time
  (pin f); a frame that only scans keeps `into_view`, one that computes is marked; a JSON frame that computes and a
  frame over its view are marked like any other (READER-PARSE-1); a
  reader under the frame's settings inlines the frame's own plan and a view body the marker, and the negated-null
  store check refuses a write from a frame view on both paths (delta 2); with the state's write lock held, a frame view
  inlines its own plan or the closed scan, never the marker, and a live read waits for a writer and decides under the
  writer's setting (delta 3). READER-PARSE-1: the reader-parse rule is the optimizer's first; under matching settings
  a reader parse optimizes to its analysed input's plan and answers as it; under differing settings it closes over its
  copy and answers as a frame parsed under the live zone; inside an `IN` and a scalar subquery it is decided under
  both. F1: under a memo the rule keeps its first decision after the live zone moves, a fresh memo decides live, and
  no scope means live; a scope restores the outer one on return, on unwind and through `enter`/`restore`, and
  another thread sees none.

- `held_iceberg.rs` — ICE-HELD-FRAME-SNAPSHOT-1 unit 1a (2026-11-20): Spark's execution-time re-bind of a held
  Iceberg scan (`V2TableRefreshUtil`), run inside the reader-parse rule after its parse decision, so once per
  optimisation of every action. For each scan whose provider carries a binding (`session_tables::binding`, the
  planning load captured at resolution) it decides the table once per `(catalog, identifier)` per plan: the open
  own-plan memo's table if it holds one, else the session's current state, which it then records in the memo. The same
  metadata location keeps the scan as it is; a dropped table raises `TABLE_OR_VIEW_NOT_FOUND`; another table UUID
  raises `INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.TABLE_ID_MISMATCH` (not inside a view); a held column that is gone
  or has another type or nullability, by name, raises `….COLUMNS_MISMATCH` (inside a view
  `INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION`), and under a write (the command scope) an added column
  does too. Otherwise the scan's source becomes `HeldIcebergScan`, the held schema over a provider built on the
  decided table through a one-table shim (no catalog call), its projection mapped by name and its output conformed to
  the held schema when only field metadata differs (a dropped and re-added column). `bound_for_view` gives a DataFrame
  view's scans a clone of the provider bound in view mode, a `_repark_writer_` scratch view's in command mode, and
  leaves other internal scratch views (`_repark_`, `__repark_`, `__rp_`) in query mode. The wrapper lives only in an
  optimised plan. Delta 2 (D1): a frame built over such a view takes its table at its build, as Spark analyses it
  (`referenced`, from `FrameView::get_logical_plan` and from `HeldView`, which `frame_table` puts over a scan-only
  view's `into_view` when the plan holds a view-mode binding): each view-mode scan becomes a query-mode clone
  (`session_tables::referenced_clone`), so a later recreate raises `TABLE_ID_MISMATCH` and a dropped column
  `COLUMNS_MISMATCH`, where the view's own re-bind skipped the identity check and named the view's error.
  optimised plan.
- [held_iceberg/](held_iceberg/map.md) — its unit tests (the zero-inner-load proof).

## Pointers

- Up: [../map.md](../map.md)

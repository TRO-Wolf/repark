# map — repark-spark/src/view_ddl/frame_settings

## Purpose

The unit tests of `../frame_settings.rs` (TEMP-VIEW-SESSION-SETTINGS-1 part 2, 2026-11-18). That file is the DataFrame
door's seat: `FrameView` captures the frame's own settings (a `_repark_writer_v2_` scratch view takes the writer's zone,
as Spark's `writeTo` re-analyses) and inlines a `FrameSettings` marker; the analyzer's first rule (`FrameSettingsRule`)
unwraps it when the planning state's settings match, else closes it as a `TableScan` over the frame analysed under its
own settings, planned by part 1's overlay; `opened` and `definition_plan` read through a closed frame, and the SQL
door's bare `ReplanningTempView` scan and a frame that only scans (columns over a scan, whose source reads the
reader's settings at scan time, as Spark's file scans do) keep `into_view`, as does a lineage reaching a JSON or CSV
scan (delta 1, `reader_lineage`, which walks through view and frame providers; its markers are unwrapped). Part 1's
inlined-frame fallback stays for that lineage only. Delta 2: outside a SQL view body (`view_body`, set around
`ReplanningTempView`'s re-planning) a frame view decides at inline time against the session's live options
(`LiveOptions`): its own plan when they match, the closed `TableScan` when they differ, so a walker that reads the
unanalysed plan (the store-assignment check's `negated_null_store::column_origin`) sees what `into_view` gave; inside
a body, or with no live reader, it inlines the marker. Delta 3 (the verifier's F1): the live read waits a bounded time
for the session state's lock and, past it, answers the options it last read (from the view's creation on), so another
thread's `spark.conf.set` never makes a frame view inline the marker outside a body.

## Contents

- `tests.rs` — the marker rule is the analyzer's first rule and no function rewrite runs before it; a frame view read
  with matching settings analyses to the plan `into_view` gives; a frame held under ANSI on and read under off
  closes as a `TableScan` and raises Spark's `ARITHMETIC_OVERFLOW` where `into_view` wraps; inside a SQL view body the
  marker is decided against the planning state, and the closed scan's projection restores the frame's schema exactly; `opened`,
  `definition_plan` and `get_logical_plan` read through a closed frame to its plan, and its schema carries no
  attribute id; a frame view answers `temp_view_column_comments` and `renamed_temp_view` as `into_view` does; the
  SQL door's registration stays a bare scan; materialisation takes the frame's options and a fresh start time
  (pin f); a frame that only scans keeps `into_view`, one that computes is marked; a JSON frame that computes, a frame
  over its view and a join of it with a marked frame keep `into_view`, the last with its marker unwrapped (delta 1); a
  reader under the frame's settings inlines the frame's own plan and a view body the marker, and the negated-null
  store check refuses a write from a frame view on both paths (delta 2); with the state's write lock held, a frame view
  inlines its own plan or the closed scan, never the marker, and a live read waits for a writer and decides under the
  writer's setting (delta 3).

## Pointers

- Up: [../map.md](../map.md)

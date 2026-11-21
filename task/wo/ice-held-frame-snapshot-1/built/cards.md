# ICE-HELD-FRAME-SNAPSHOT-1 unit 1a — residual cards for the orchestrator to file

Fix round 1 (2026-11-20), F3. One line each; the orchestrator files them. The cells are in [bar.md](bar.md), "The cells
round 0 left open".

- **ICE-EXPIRED-FILE-READ-ERROR-1 (S3, loud on both engines, not label-blocking).** An own-plan `collect` of a held
  frame after `INSERT OVERWRITE` and `expire_snapshots` (cell `D|table|own_collect_ow_expire_collect`, both ANSI modes)
  raises on both engines because the data file is gone, with different errors: Spark 4.1.2 raises `SparkException`
  ("Job aborted due to stage failure", a missing-file cause), RePark raises `External error: DataInvalid => Failed to
  open file <path>`. The binding agrees (the `count` after it answers 1 on both); only the error class and text differ,
  and they differ the same way on main `9a48932f`.

Delta 2 (2026-11-20), from the Opus delta re-check's C5 and C6 and delta 2's sibling cells ([bar.md](bar.md), "Delta
2"):

- **ICE-HELD-EXPLAIN-COST-1 (S3, loud, not label-blocking).** After a dropped, renamed or retyped column, a recreate
  or a `DROP TABLE`, `explain("cost")` on a held frame, its `filter`, `select` or a join of two (20 cells per ANSI
  mode) raises the frame's error, where Spark 4.1.2 prints its sections holding the `AnalysisException`: RePark's
  `cost` mode runs `EXPLAIN ANALYZE`, which executes. Delta 1 left it as it was on fix round 1.
- **SQL-TEMP-VIEW-EXPLAIN-EMPTY-1 (S3, quiet, predates 1a).** After an incompatible change under a SQL temp view
  (`CREATE TEMP VIEW v AS SELECT * FROM t`), a frame over it prints 0 characters for `explain()` and only the logical
  plan for `explain(True)` and `codegen` (18 cells per ANSI mode), on main, fix round 1 and both delta heads, where
  Spark raises `INCOMPATIBLE_VIEW_SCHEMA_CHANGE` (a retype: `CANNOT_UP_CAST_DATATYPE`; a drop:
  `TABLE_OR_VIEW_NOT_FOUND`): physical planning fails, and the renderer drops the `physical_plan_error` row.
- **SQL-TEMP-VIEW-FRAME-BIND-1 (S3, silent, not 1a's files).** A frame built over a SQL temp view before a recreate
  reads the recreated table where Spark 4.1.2 raises `TABLE_ID_MISMATCH`, and after a dropped column raises the view's
  error (`INCOMPATIBLE_VIEW_SCHEMA_CHANGE`, or `INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION` over a
  DataFrame view) where Spark raises `COLUMNS_MISMATCH`: `ReplanningTempView` (`view_ddl/temp_view.rs`) re-plans the
  view's text at each scan, where Spark analyses it once, when the frame is built. Over a table it predates 1a (main
  reads the recreated table too); over a DataFrame view of a held frame main answers the old table.

Two of round 0's open cells already have cards, so they are not carded again here:
[STRING-CAST-LENIENT-1](../../../roadmap/mid-term/string-cast-lenient-1-card-2026-11-16.md) (`M|held_cast`) and
[LOCAL-CHECKPOINT-NEW-FRAME-1](../../../roadmap/mid-term/local-checkpoint-new-frame-1-card-2026-10-03.md)
(`local_checkpoint_after_collect`).

Up: [map.md](map.md).

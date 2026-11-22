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
- **SQL-FRAME-EAGER-ANALYSIS-1 (S4, loud, predates this PR).** Spark analyses a `spark.sql(...)` frame when it is
  built, and RePark at its first action. So a frame built over a view after an incompatible change raises at its
  build on Spark, and at its action on RePark. `explain(True)` and `codegen` print the condition where Spark has
  already raised. The delta-1 re-check's C2 and the delta-2 sibling table (2 cells per mode) measured it, unchanged
  from main. Carded at the #1391 verifier's C-5.

Unit 2 delta 1 (2026-11-21), from the #1394 verifier's F3 (class (c), pre-existing):

- **JOIN-SCRATCH-NAME-SCHEMA-ERROR-1 (S3, loud, predates this PR).** A held join frame (`join_l`, `join_r`) read
  after a column it uses is dropped or renamed (1a's bar: `B|join_l|alter_drop` and `alter_rename`, 14 each, and
  `HW|join_l|*`, `HW|join_r|*`, 8; 36 answers across both ANSI modes) raises DataFusion's `Schema error: No field named
  c. Valid fields are _repark_jr_<hex>.n.`, naming the generated `_repark_jl_` or `_repark_jr_` scratch view, where
  Spark 4.1.2 raises `UNRESOLVED_USING_COLUMN_FOR_JOIN`. Main `dac2af61` prints the same text; the bar counts it EQUAL
  by its raise-versus-raise rule. A sibling of
  [QUALIFIER-LEAK-H-1](../../../roadmap/mid-term/qualifier-leak-h-1-card-2026-10-03.md), which covers
  `AMBIGUOUS_REFERENCE` only.

Unit 2 delta 2 (2026-11-22) files no new card. What it leaves open is named in [unit-2.md](unit-2.md) §10: the
open cell already has its card,
[FRESH-READ-WHILE-TABLE-CACHED-1](../../../roadmap/mid-term/cards-2026-11-21/fresh-read-while-table-cached-1-card-2026-11-22.md),
and the `cacheTable` `isCached` cell stays §8's C-009 question.

Two of round 0's open cells already have cards, so they are not carded again here:
[STRING-CAST-LENIENT-1](../../../roadmap/mid-term/string-cast-lenient-1-card-2026-11-16.md) (`M|held_cast`) and
[LOCAL-CHECKPOINT-NEW-FRAME-1](../../../roadmap/mid-term/local-checkpoint-new-frame-1-card-2026-10-03.md)
(`local_checkpoint_after_collect`).

Up: [map.md](map.md).

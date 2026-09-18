# map — repark-core/src/column_resolution

ICE-MIXED-CASE-1 (2026-09-17, Q-20b-2): the fold's unit battery lives here,
split from `../column_resolution.rs` under the file-size gate. Statement cells
(false/true spellings), fragment scoping, the `[AMBIGUOUS_REFERENCE]` shape,
backticked exact under `true`, and the DataFrame filter alias binding.
Round 21b: the helpers pass `case_insensitive` to `plan_statement_with_column_repair`
explicitly (the module owns no carrier); `dataframe_filter_binds_projection_alias`
plans through `sql_with_column_repair` so the flag reaches the fold.
pins: ice-mixed-case-1/C-001…C-010

## Purpose

Unit tests for the Spark-door case-insensitive column fold. The implementation
stays in `../column_resolution.rs`; this directory holds only the battery.

# map — python/repark/tests/dataframe

## Purpose

The DataFrame door: transformations, actions, joins, grouping and pivot, explode and dynamicFlatten, Column and Row, display (`show`, repr, printSchema, explain), eager mode, and the polars-style API.

Part of the FACADE-TESTS-LAYOUT-1 (2026-10-19) layout; the directory rules live in
[../map.md](../map.md) "Layout". A new test goes in the directory of the subsystem it
pins.

## Contents

- [test_c4_expand2_facade.py](test_c4_expand2_facade.py) — expand2 — facade pins for repartition* validation + fillna errorClass + AssertionError.
- [test_c5_census_r7.py](test_c5_census_r7.py) — C5 census-r7 — Column access surface + sameSemantics + when type-gate pins.
- [test_column_access.py](test_column_access.py) — Group G1 — column-access sugar: ``df.x``, ``df["x"]``, ``-col``.
- [test_column_parity_1.py](test_column_parity_1.py) — COLUMN-PARITY-1 oracle pins — ``facade_column_oracle.json`` ``col`` cells (PySpark 4.1.2).
- [test_column_parity_1_critic.py](test_column_parity_1_critic.py) — COLUMN-PARITY-1 critic-round pins — sequential ``UpdateFields`` and L-005..L-010.
- [test_column_x1_census.py](test_column_x1_census.py) — Column surface unblocks (between, pow, string predicates, bitwise, lit temporal).
- [test_columns.py](test_columns.py) — Facade tests for the Column / expression surface.
- [test_cross_join_condition_1.py](test_cross_join_condition_1.py) — CROSS-JOIN-CONDITION-1: a cross join with a join condition answers the inner rows like Spark.
- [test_dataframe_actions.py](test_dataframe_actions.py) — R-TAIL — DataFrame action surface: take / head / first / tail / isEmpty / toLocalIterator.
- [test_dataframe_x3_census.py](test_dataframe_x3_census.py) — Census pins — DataFrame error-class seed + high-leverage surface (test_dataframe).
- [test_declare_sorted.py](test_declare_sorted.py) — Facade pins for the SE-1 ``declareSorted`` door (repark extension, not PySpark).
- [test_declare_sorted_tighten.py](test_declare_sorted_tighten.py) — Facade pins for ``declareSorted(..., tightenNulls=True)``.
- [test_df_batch2.py](test_df_batch2.py) — R-DF-BATCH2 — cube/rollup/unpivot/explain/views + loud census.
- [test_df_eager_1.py](test_df_eager_1.py) — DF-EAGER-1 step 1: red-first pins for .eager(), .compute() and .lazy().
- [test_df_easy.py](test_df_easy.py) — R-DF-EASY — easy DataFrame lowerings (oracle shapes; coverage over completeness).
- [test_df_explain_1.py](test_df_explain_1.py) — DF-EXPLAIN-1 step 1: red-first pins for explain printing plan text, not Row reprs.
- [test_df_plan_introspect_1.py](test_df_plan_introspect_1.py) — DF-PLAN-INTROSPECT-1 — Rust plan-introspection pins driven by the live oracle.
- [test_df_printschema.py](test_df_printschema.py) — DF-PRINTSCHEMA-1 — printSchema stdout is byte-identical to Spark's.
- [test_df_rust3_freqitems_transpose.py](test_df_rust3_freqitems_transpose.py) — DF-RUST-3 — freqItems / transpose pins driven by the live PySpark 4.1.2 oracle.
- [test_df_stream_batch_1.py](test_df_stream_batch_1.py) — df-stream-batch-1 pins: the streaming-named DataFrame surface on a batch frame.
- [test_df_subquery_1.py](test_df_subquery_1.py) — DF-SUBQUERY-1 — scalar / exists / lateralJoin / asTable pins from the PySpark 4.1.2 oracle.
- [test_df_surface_a_1.py](test_df_surface_a_1.py) — DF-SURFACE-A-1 — oracle pins for the seven-name DataFrame surface.
- [test_df_surface_b_1.py](test_df_surface_b_1.py) — df-surface-b-1 pins: foreach, foreachPartition, observe, Observation.
- [test_dfcore_1_exports.py](test_dfcore_1_exports.py) — DFCORE-1 export snapshot pin: package, core, and DataFrame identity frozen pre-slice.
- [test_dfcore_4a_sample_determinism.py](test_dfcore_4a_sample_determinism.py) — DFCORE-4a sample determinism pin: same seed gives same rows across the move.
- [test_dfcore_4a_sampleby_determinism.py](test_dfcore_4a_sampleby_determinism.py) — DFCORE-4a sampleBy determinism pin: same seed gives same rows across the move.
- [test_dfcore_4a_split_left_determinism.py](test_dfcore_4a_split_left_determinism.py) — DFCORE-4a split-left determinism pin: same seed gives same rows across the move.
- [test_dfcore_4a_split_right_determinism.py](test_dfcore_4a_split_right_determinism.py) — DFCORE-4a split-right determinism pin: same seed gives same rows across the move.
- [test_dfcore_4b_eager_goldens.py](test_dfcore_4b_eager_goldens.py) — DFCORE-4b eager golden pins: byte-identical repr and HTML across the display move.
- [test_dfcore_4b_exports.py](test_dfcore_4b_exports.py) — DFCORE-4b ownership pin: the ten display bodies live in ``display.py``.
- [test_dfcore_4b_show_goldens.py](test_dfcore_4b_show_goldens.py) — DFCORE-4b show golden pins: byte-identical show output across the display move.
- [test_dfcore_5_approx_quantile.py](test_dfcore_5_approx_quantile.py) — DFCORE-5 one-collect approxQuantile pins plus three inherited gap pins.
- [test_dfcore_6_eager_preview.py](test_dfcore_6_eager_preview.py) — DFCORE-6 eager-preview pins: footers without count(), bounded bridge peek.
- [test_display_bridge_1.py](test_display_bridge_1.py) — DISPLAY-BRIDGE-1 pins: a bridged frame's show() follows the display style (R-13).
- [test_display_lazy_1.py](test_display_lazy_1.py) — DISPLAY-LAZY-1: lazy-repr pins.
- [test_display_polars_default.py](test_display_polars_default.py) — DISPLAY-POLARS-1 steps 2 and 3 pins: styled show fetch discipline and styled repr doors.
- [test_display_styles.py](test_display_styles.py) — R-DISPLAY: ``DataFrame.show()`` styles (``spark`` / ``polars`` / ``duckdb``).
- [test_dynamic_flatten.py](test_dynamic_flatten.py) — DataFrame.dynamicFlatten / dynamic_flatten facade contract.
- [test_dynamic_flatten_divergences.py](test_dynamic_flatten_divergences.py) — Measured dynamicFlatten divergences from Spark; the contracts are cited in the root `map.md`.
- [test_dynflatten_listnull.py](test_dynflatten_listnull.py) — Parquet `list<null>` reads as `list<int32>` on both DataFrame doors and `dynamicFlatten` keeps the column as nullable `int32` NULLs.
- [test_explode_rewrite.py](test_explode_rewrite.py) — R-EXPLODE-REWRITE: explode / explode_outer via guarded DataFusion unnest.
- [test_f2_fail_value.py](test_f2_fail_value.py) — R-CENSUS-R3-VALUE — FAIL-VALUE harvest pins (Arrow path value + type).
- [test_facade_2_column_display_goldens.py](test_facade_2_column_display_goldens.py) — FACADE-2 column display-string goldens against the committed JSON file.
- [test_facade_2_group2_no_python_assembly.py](test_facade_2_group2_no_python_assembly.py) — FACADE-2 Group-2 methods must not assemble display/SQL/join text in Python.
- [test_facade_5_display_goldens.py](test_facade_5_display_goldens.py) — FACADE-5 display-renderer goldens against the committed JSON file.
- [test_facade_hygiene.py](test_facade_hygiene.py) — R-FACADE-HYGIENE pins: listTables CDF hide, weakref GC bound, fillna, dropDuplicates, OOS errors.
- [test_facade_polish.py](test_facade_polish.py) — Facade polish — aggregate display naming + ``spark.read`` expansion.
- [test_filter_predicate_rewrite.py](test_filter_predicate_rewrite.py) — The SQL-string filter-predicate rewriter (``column_fields._quote_filter_sql_identifiers``).
- [test_g1_stat_and_expander.py](test_g1_stat_and_expander.py) — G1 — DataFrame.stat family + UPDATE/DELETE expander e2e pins.
- [test_g2_window_rand_sampleby.py](test_g2_window_rand_sampleby.py) — G2 — window frames, seeded rand/randn, sampleBy XORShift, eagerEval.
- [test_g4b_semi_join.py](test_g4b_semi_join.py) — G4b — DataFrame-API ``leftsemi`` / ``leftanti`` binding: the non-differential surface.
- [test_group_agg.py](test_group_agg.py) — Facade tests for the DataFrame aggregation family (Group E: E1, E2, E7).
- [test_h2_group_h2.py](test_h2_group_h2.py) — Group H long tail + naming polish.
- [test_join_condition_refusals_1.py](test_join_condition_refusals_1.py) — Join-condition refusals: nondeterministic and non-boolean conditions refuse like Spark.
- [test_join_parity.py](test_join_parity.py) — Joins differential corpus (H-2 gap G4) - value AND Arrow type AND nullability vs live Spark.
- [test_n2_plan_collapse.py](test_n2_plan_collapse.py) — N2 — adjacent projection/window collapse (alias-chain squash + window merge).
- [test_na_rename.py](test_na_rename.py) — Facade tests for renames + the missing-data family (Group E: E5, plus E7/E8 na edges).
- [test_offset_nested_sort_1.py](test_offset_nested_sort_1.py) — OFFSET under a nested ORDER BY answers Spark's rows on the SQL and DataFrame doors.
- [test_parity3.py](test_parity3.py) — R-PARITY3 — createDataFrame(schema=StructType/DDL) + show(vertical=True).
- [test_pivot.py](test_pivot.py) — R-PIVOT: GroupedData.pivot two-phase conditional aggregation.
- [test_polars_core.py](test_polars_core.py) — R-POLARS-CORE — polars-style API over repark plans (importorskip real polars).
- [test_polars_differential.py](test_polars_differential.py) — R-POLARS-CORE rider: DIFFERENTIAL pins against real polars (the brief's oracle rule).
- [test_polars_is_duplicated_1.py](test_polars_is_duplicated_1.py) — POLARS-IS-DUPLICATED-1 — Column.is_duplicated() answers as real polars on both doors.
- [test_polars_ns.py](test_polars_ns.py) — R-POLARS-NS — .str / .dt namespaces + fill_null(value) (value + Arrow type + NULL).
- [test_replace_linear_1.py](test_replace_linear_1.py) — REPLACE-LINEAR-1 — ``DataFrame.replace`` oracle cells and the exponential memory pin. REPLACE-LINEAR-PIN-1 (2026-10-22): the memory pin's child runs one depth-1 call of the mode it measures before it reads the peak; the bound and the floor are unchanged.
- [test_row.py](test_row.py) — G-ROW — :class:`~repark.row.Row` API + E8 error-class pins vs live PySpark 4.1.2.
- [test_row_tuple_1.py](test_row_tuple_1.py) — Row.count / Row.index tuple-protocol pins against the committed row oracle.
- [test_select_global_agg.py](test_select_global_agg.py) — R-SELECT-GLOBAL-AGG: df.select(<aggregates only>) is Spark global aggregate.
- [test_select_naming.py](test_select_naming.py) — Group H — select/projection display naming vs live PySpark 4.1.2.
- [test_t0_df_regions_import_freeze.py](test_t0_df_regions_import_freeze.py) — T0 — Q7 import freeze pins (package + core + private helper re-exports).
- [test_t3_ux_polish.py](test_t3_ux_polish.py) — T3 UX polish: display_style, Column.round, default app name, H1 export naming.
- [test_t7_census_r6.py](test_t7_census_r6.py) — Facade pins: isStreaming, Column.substr, array_contains.
- [test_union_distinct.py](test_union_distinct.py) — Facade tests for the set-operation + dedup family (Group E: E3 union, E4 distinct).
- [_dfcore_1_expected.py](_dfcore_1_expected.py) — Frozen expectation tables for ``test_dfcore_1_exports.py`` (EAGER-OWN-1 split).
- [_record_join_goldens.py](_record_join_goldens.py) — Record mode for the joins differential corpus - re-derive every Spark half from live PySpark.
- [facade_2_column_display_goldens.json](facade_2_column_display_goldens.json) — Golden file read by `test_facade_2_column_display_goldens.py`.
- [facade_2_generic_builder_goldens.json](facade_2_generic_builder_goldens.json) — Golden file read by `test_facade_2_column_display_goldens.py`.
- [facade_5_display_goldens.json](facade_5_display_goldens.json) — Golden file read by `test_facade_5_display_goldens.py`.
- [facade_column_oracle.json](facade_column_oracle.json) — Recorded Spark oracle (57 cells) read by `test_column_parity_1.py`.
- [facade_dataframe_streaming_declared_oracle.json](facade_dataframe_streaming_declared_oracle.json) — Recorded Spark oracle (24 cells) read by `test_df_stream_batch_1.py`.
- [facade_dataframe_surface_oracle.json](facade_dataframe_surface_oracle.json) — Recorded Spark oracle (158 cells) read by `test_df_plan_introspect_1.py`, `test_df_surface_a_1.py`, `test_df_surface_b_1.py`.
- [facade_df_rust3_oracle.json](facade_df_rust3_oracle.json) — The run-16b live PySpark 4.1.2 oracle for `freqItems` and `transpose`, copied unchanged; read by `test_df_rust3_freqitems_transpose.py`.
- [facade_df_subquery_oracle.json](facade_df_subquery_oracle.json) — The live PySpark 4.1.2 cells (run 16b) for scalar, exists, lateral-join, asTable and `Column.outer` plus the error cells; read by `test_df_subquery_1.py`.
- [facade_row_oracle.json](facade_row_oracle.json) — The run-15b live PySpark 4.1.2 oracle for the Row facade (`row.*` cells), copied unchanged; read by `test_row_tuple_1.py`.
- [fnp11b_hour_time_spark_oracle.json](fnp11b_hour_time_spark_oracle.json) — Recorded Spark oracle (11 cells) read by `test_column_x1_census.py`.
- [join_condition_refusals_1_spark_oracle.json](join_condition_refusals_1_spark_oracle.json) — Recorded Spark oracle (196 cells) read by `test_join_condition_refusals_1.py`.
- [offset_nested_sort_1_spark_oracle.json](offset_nested_sort_1_spark_oracle.json) — Recorded Spark oracle read by `test_offset_nested_sort_1.py`.
- [polars_is_duplicated_1_polars_oracle.json](polars_is_duplicated_1_polars_oracle.json) — Recorded polars oracle read by `test_polars_is_duplicated_1.py`.

## Path expressions

The move put every file here one level below the old flat directory. These files had a
path expression repointed in the same unit and no other line changed:

| file | change |
|---|---|
| `_record_join_goldens.py` | names its own or a sibling's new path in a docstring or recorded string. |
| `test_dynflatten_listnull.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_facade_2_group2_no_python_assembly.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_join_parity.py` | names its own or a sibling's new path in a docstring or recorded string. |

## Pointers

- Up: [../map.md](../map.md)

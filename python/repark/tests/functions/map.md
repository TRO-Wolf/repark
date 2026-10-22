# map — python/repark/tests/functions

## Purpose

The `repark.spark.functions` surface and its SQL-door twins: scalar, string, collection, datetime, aggregate, window, higher-order, bitmap and generator functions, the `repark.ta` indicators, and the registered function divergences.

Part of the FACADE-TESTS-LAYOUT-1 (2026-10-19) layout; the directory rules live in
[../map.md](../map.md) "Layout". A new test goes in the directory of the subsystem it
pins.

## Contents

- [test_abs_expr_1.py](test_abs_expr_1.py) — ABS-EXPR-1 — ``F.abs`` / ``F.cbrt`` / ``F.nullif`` lower to one native scalar call each.
- [test_array_null_1.py](test_array_null_1.py) — ARRAY-NULL-1 — ``F.array_append`` / ``F.array_prepend`` oracle cells + memory pin.
- [test_array_null_1_coercion.py](test_array_null_1_coercion.py) — ARRAY-NULL-1 round-2 — element coercion oracle cells and temporal pins.
- [test_bl15_bl16_math_divergences.py](test_bl15_bl16_math_divergences.py) — BL-15 FIXED precise expm1; BL-16 FIXED rescaled hypot (registry §7).
- [test_bl17_base64_padding.py](test_bl17_base64_padding.py) — BL-17 FIXED: F.base64 applies RFC 4648 padding (registry §7).
- [test_date_fn_1.py](test_date_fn_1.py) — DATE-FN-1 Spark ``date()`` spelling and ``unix_timestamp`` pins.
- [test_door_converge_1.py](test_door_converge_1.py) — DOOR-CONVERGE-1 pins: both doors answer PySpark 4.1.2 with one kernel.
- [test_door_converge_2.py](test_door_converge_2.py) — Oracle Q12 (PySpark 4.1.2) pins for the four door-converged P1 names.
- [test_float_agg_parity.py](test_float_agg_parity.py) — Float aggregation differential corpus (H-2 gap G7) — catastrophic-cancellation fixture.
- [test_fn_arrays_divergence.py](test_fn_arrays_divergence.py) — Pins the Spark-equal array answers FN-FIX-1 closed for the four EX-8 rows (registry §7).
- [test_fn_batch1.py](test_fn_batch1.py) — R-FN-BATCH1 — top scalar function wrappers (value + Arrow type + null case).
- [test_fn_batch2.py](test_fn_batch2.py) — R-FN-BATCH2 — strings / collection wrappers (value + Arrow type + null case).
- [test_fn_batch3.py](test_fn_batch3.py) — R-FN-BATCH3 — datetime / Java-pattern format pins + loud census.
- [test_fn_batch4.py](test_fn_batch4.py) — R-FN-BATCH4 — aggregates / stats / hash / id census pins.
- [test_fn_chr_divergence.py](test_fn_chr_divergence.py) — FN-CHR-1: chr/char take n % 256 and empty string for negatives (registry §7).
- [test_fn_elt_out_of_range.py](test_fn_elt_out_of_range.py) — FN-ELT-1: out-of-range elt raises INVALID_ARRAY_INDEX under ANSI.
- [test_fn_initcap_divergence.py](test_fn_initcap_divergence.py) — FN-INITCAP-1: initcap starts a word only after a space (registry §7).
- [test_fn_like_escape_end.py](test_fn_like_escape_end.py) — FN-LIKE-ESCEND-1: a pattern ending in the escape char raises ESC_AT_THE_END.
- [test_fn_regex_posix_class.py](test_fn_regex_posix_class.py) — FN-REGEX-POSIX-1: POSIX ``[[:alpha:]]`` is a Java union bracket.
- [test_fn_regexp_extract.py](test_fn_regexp_extract.py) — FN-REGEXP-EXTRACT-1: `regexp_extract` group index semantics (groups 1/2, whole match 0, no match is empty).
- [test_fn_startswith_1.py](test_fn_startswith_1.py) — M8-STARTSWITH-1: SQL startswith answers the PD p17 predicate with Spark semantics.
- [test_fn_trim_chars.py](test_fn_trim_chars.py) — FN-TRIM-CHARS-1: trim/ltrim/rtrim two-arg charset overloads (registry §7).
- [test_fnp11a_r2.py](test_fnp11a_r2.py) — FNP-11A round-2 finding pins against the run-15a Spark oracle.
- [test_fnp11a_temporal.py](test_fnp11a_temporal.py) — FNP-11A temporal constructors, intervals and arithmetic pins.
- [test_fnp11b_temporal_formats.py](test_fnp11b_temporal_formats.py) — FNP-11B datetime formats, the TIME family, BL-13 and BL-14 pins.
- [test_fnp11b_typeof.py](test_fnp11b_typeof.py) — TYPEOF pins over fnp11b_typeof_spark_oracle.json on the recorded door.
- [test_fnp15_16_declared_refuse.py](test_fnp15_16_declared_refuse.py) — FNP-15/16 — declared-absent Spark functions refuse loudly.
- [test_fnp2_free_names.py](test_fnp2_free_names.py) — FNP-2 — the names that needed no engine work, and the null-ordering corners they closed.
- [test_fnp3_destubbed.py](test_fnp3_destubbed.py) — FNP-3 — the stubs whose kernel the engine already had.
- [test_fnp4_lambda_seam.py](test_fnp4_lambda_seam.py) — FNP-4a — the higher-order seam: a Python lambda reaching the engine.
- [test_fnp4c_higher_order.py](test_fnp4c_higher_order.py) — FNP-4c — Spark higher-order kernels on the facade Column API.
- [test_fnp5_aggregates.py](test_fnp5_aggregates.py) — FNP-5 — aggregates DataFusion already had registered and the facade could not reach.
- [test_fnp6_random.py](test_fnp6_random.py) — FNP-6b — ``randstr`` and ``uniform`` over the Spark ``XORShiftRandom`` repark already owns.
- [test_fnp6_regexp.py](test_fnp6_regexp.py) — FNP-6a — ``regexp_extract_all`` and ``regexp_substr``, over machinery repark already wrote.
- [test_fnp6_validate.py](test_fnp6_validate.py) — FNP-6c — ``validate_utf8`` / ``try_validate_utf8`` / ``assert_true``.
- [test_fnp7_try_inversions.py](test_fnp7_try_inversions.py) — FNP-7a/7b — twelve ``try_*`` inversions.
- [test_fnp8_oracle_matrix.py](test_fnp8_oracle_matrix.py) — Recorded and live Spark contracts for the FNP-8 public entry points.
- [test_fnp_4b_hof_display.py](test_fnp_4b_hof_display.py) — FNP-4B round 6 — selectExpr higher-order display must hide the packing marker.
- [test_fnp_4b_literals.py](test_fnp_4b_literals.py) — FNP-4B literal pins — escapedStringLiterals, out-of-range ``\U``, suffixes, struct.
- [test_fnp_4b_spark_dialect.py](test_fnp_4b_spark_dialect.py) — FNP-4B door pins — double-quoted STRING literals and Spark expression strings.
- [test_fnp_6d_bitmap_aggregates.py](test_fnp_6d_bitmap_aggregates.py) — SQL-door pins for Spark bitmap aggregates.
- [test_fnp_6d_followup_1.py](test_fnp_6d_followup_1.py) — SQL-door pins for the FNP-6D followup bitmap signature fix.
- [test_fnp_8_sql_door.py](test_fnp_8_sql_door.py) — FNP-8 candidate matrix for higher-order lambdas on both Spark facade entry points.
- [test_fnp_9_collections_json.py](test_fnp_9_collections_json.py) — FNP-9: `get_json_object` and the collection / JSON functions answer Spark's values on both doors.
- [test_fnp_agg_1.py](test_fnp_agg_1.py) — FNP-AGG-1 slice (d) pins: grouping_id answers PySpark 4.1.2 on both doors.
- [test_fnp_agg_1_critic.py](test_fnp_agg_1_critic.py) — FNP-AGG-1 slice (d) remediation pins: the grouping_id exact-order rule.
- [test_fnp_alias_1.py](test_fnp_alias_1.py) — FNP-ALIAS-1 pins: PySpark alias names over existing kernels, oracle-driven.
- [test_fnp_bitmap_facade_1.py](test_fnp_bitmap_facade_1.py) — Facade pins for the three Spark bitmap aggregates against oracle cells F6D-*/B8-*.
- [test_fnp_critic_remediation.py](test_fnp_critic_remediation.py) — Regression pins for critic-found defects, grouped by provenance rather than subsystem.
- [test_fnp_degrees_rust_1.py](test_fnp_degrees_rust_1.py) — DEGREES-RUST-1 pins: facade and SQL-door degrees/radians against DEG-* and DEGI-* cells.
- [test_fnp_gen_1.py](test_fnp_gen_1.py) — FNP-GEN-1 step-1 pins: generators and semi-structured parsers over the o245 oracle.
- [test_fnp_gen_1_s34.py](test_fnp_gen_1_s34.py) — FNP-GEN-1 steps 3-4 pins: tuple, csv and csv-schema kernels over the s34 oracle.
- [test_fnp_gen_1_s34_critic.py](test_fnp_gen_1_s34_critic.py) — FNP-GEN-1 remediation pins: critic cells for corrupt records, timestamps and decimals.
- [test_fnp_math_1.py](test_fnp_math_1.py) — FNP-MATH-1 step-1 red pins over fnp_math_1_spark_oracle.json, both doors, both ANSI settings.
- [test_fnp_misc_1.py](test_fnp_misc_1.py) — FNP-MISC-1 pins for call_function, call_udf, arrow_udf, arrow_udtf, bucket(Column).
- [test_fnp_win_1.py](test_fnp_win_1.py) — FNP-WIN-1 pins for window, window_time, session_window over the live oracle.
- [test_functions_a.py](test_functions_a.py) — FN-A — ordering / null / math facade wrappers (value + Arrow type).
- [test_functions_b.py](test_functions_b.py) — FN-B — string facade wrappers (value + Arrow type).
- [test_functions_c.py](test_functions_c.py) — FN-C — aggregate aliases / shims (value + Arrow type).
- [test_functions_d.py](test_functions_d.py) — FN-D — datetime facade wrappers (value + Arrow type).
- [test_functions_dates.py](test_functions_dates.py) — Facade tests for the ``Window`` / ``row_number`` surface and the 13 date functions.
- [test_functions_e.py](test_functions_e.py) — FN-E — collections facade wrappers (value + Arrow type).
- [test_functions_f.py](test_functions_f.py) — FN-F — try / session / bitwise facade wrappers (value + Arrow type).
- [test_functions_gt1.py](test_functions_gt1.py) — ColumnOrName wiring + oracle pins for the GT1 function family.
- [test_functions_gt2.py](test_functions_gt2.py) — FN-GT2 — leftover THIN-WIRE datetime / collections / url / bitmap.
- [test_functions_split_identity.py](test_functions_split_identity.py) — FN-SPLIT identity: ``functions.__all__`` is unchanged and every name resolves.
- [test_functions_w.py](test_functions_w.py) — FN-W — window functions (value + Arrow type).
- [test_log1p_1.py](test_log1p_1.py) — LOG1P-1 — precise ``log1p`` / ``expm1`` kernels, three doors, Spark-equal.
- [test_lrs1_higher_order_refusals.py](test_lrs1_higher_order_refusals.py) — LRS-1 — the facade refuses higher-order columns where the engine would leak an internal error.
- [test_lrs2_argument_contracts.py](test_lrs2_argument_contracts.py) — LRS-2 — argument contracts matching PySpark, measured against live PySpark 4.1.2.
- [test_lrs3_registered_divergences.py](test_lrs3_registered_divergences.py) — LRS-3 — pins for registered divergences, plus the door alias it added.
- [test_lrs4_door_domain.py](test_lrs4_door_domain.py) — LRS-4 — per-door divergence pins (C-012): SQL door and facade resolve different kernels.
- [test_lrs6_regexp_divergences.py](test_lrs6_regexp_divergences.py) — LRS-6 — RE-2 divergence pins, measured but not closed (BACKLOG row).
- [test_lrs7_unordered_window.py](test_lrs7_unordered_window.py) — LRS-7 — a window with no ORDER BY frames the whole partition, as Spark documents.
- [test_map_column_panic_1.py](test_map_column_panic_1.py) — MAP-COLUMN-PANIC-1 — `map(<key>, <non-literal value>)` equals Spark 4.1.2 on `sql`, `selectExpr`, `expr`, a string `filter` and INSERT, UPDATE and MERGE, in rows, type and nullability; Spark's duplicate-key, NULL-key and odd-arity errors; two divergences pinned (no condition on runtime errors, DataFusion's odd-arity text through `expr`).
- [test_map_column_panic_native_door_1.py](test_map_column_panic_native_door_1.py) — MAP-COLUMN-PANIC-NATIVE-DOOR-1 (2026-10-21) — the native `repark.sql()` door answers `map(<key>, <column>)`: 16 of the Spark recording's cells read from an inline subquery (the native door has no temp views and no `timestamp_micros`) equal Spark's rows on the Arrow path, the repeated and NULL key cells raise Spark's `[DUPLICATED_MAP_KEY]` and `[NULL_MAP_KEY]` text and never a caught panic, and the literal control keeps its rows. Reads `map_column_panic_1_spark_oracle.json` and borrows `VIEW_SQL` and `plain` from the recorder; 17 of its 22 tests fail on the main wheel with `REPARK_PANIC_FATAL=0`.
- [test_sem1_extract_all_group_default.py](test_sem1_extract_all_group_default.py) — SEM-1 — the two-argument ``regexp_extract_all`` defaults to capture group 1, as Spark does.
- [test_sem1_spark_log.py](test_sem1_spark_log.py) — SEM-1 LOG-1 — Spark-door ``log`` is the natural log, dual-arity, null-guarded.
- [test_sem3_string_idx.py](test_sem3_string_idx.py) — SEM-3 — ``regexp_extract_all`` accepts a string ``idx``, as every other door already does.
- [test_sem4_regex_group_index_message.py](test_sem4_regex_group_index_message.py) — SEM-4 — the regexp refusals say Spark's words, and name the function they came from.
- [test_sem6_substr_zero_width_null.py](test_sem6_substr_zero_width_null.py) — SEM-6 — ``regexp_substr`` returns NULL for a zero-width match, as Spark does.
- [test_sliding_avg_parity.py](test_sliding_avg_parity.py) — Live-Spark-oracle parity pins for the Float64 sliding-avg shim.
- [test_ta.py](test_ta.py) — Facade tests for T1b: the ``repark.ta`` technical-analysis surface.
- [test_ta_series.py](test_ta_series.py) — TA-SINGLE-SERIES-PARALLEL-1 S2a — a bare ``ta.*`` column is a series over the resolved order.
- [test_ta_series_s1_ordered_cache.py](test_ta_series_s1_ordered_cache.py) — TA-SERIES S1 pins: a sorted single-partition cache keeps its order on plain reads.
- [test_ta_volume.py](test_ta_volume.py) — TA-4 facade tests: volume-family kernels through ``repark.ta``.
- [test_ta_with_indicators.py](test_ta_with_indicators.py) — TA-2 — ``ta.with_indicators`` serving helper (required partition/order).
- [test_two_door_kernel_parity.py](test_two_door_kernel_parity.py) — Charter clause C-012 — the facade and the SQL door must resolve the same kernel.
- [test_unresolved_routine_1.py](test_unresolved_routine_1.py) — UNRESOLVED-ROUTINE-1 pins: every unknown routine refuses with Spark's shape on both doors.
- [test_win_slide_1.py](test_win_slide_1.py) — WIN-SLIDE-1: the thirteen non-retractable aggregates answer over a sliding frame.
- [test_window_parity.py](test_window_parity.py) — Window-function differential corpus (H-2 gap G5) — G5 frames never compared to live Spark.
- [_record_float_agg_goldens.py](_record_float_agg_goldens.py) — Record mode for the float-agg corpus — re-derive every `spark` half from live PySpark.
- [_record_map_column_panic_1.py](_record_map_column_panic_1.py) — Record mode for MAP-COLUMN-PANIC-1 — re-derive the 33 cells from live PySpark 4.1.2 (`--check` compares).
- [_record_window_goldens.py](_record_window_goldens.py) — Record mode for the window-function corpus — re-derive every `spark` half from live PySpark.
- [fnp11_spark_oracle.json](fnp11_spark_oracle.json) — Live PySpark 4.1.2 recording (2026-09-14): 846 cells over the shared and TIME frames, both ANSI settings and two zones; read by `test_fnp11a_temporal.py`.
- [fnp11a_r2_spark_oracle.json](fnp11a_r2_spark_oracle.json) — Live PySpark 4.1.2 recording (2026-09-15, orchestrator-run, run 15a): 112 cells settling the round-2 review findings L-001..L-011, both ANSI settings, UTC and America/New_York.
- [fnp11b_spark_oracle.json](fnp11b_spark_oracle.json) — Filtered FNP-11B oracle: 315 cells from the FNP-11 recording plus 22 cells from the o245 math recording; read by `test_fnp11b_temporal_formats.py`.
- [fnp11b_typeof_spark_oracle.json](fnp11b_typeof_spark_oracle.json) — Recorded Spark oracle (40 cells) read by `test_fnp11b_typeof.py`.
- [fnp8_error_oracle.json](fnp8_error_oracle.json) — Recorded Spark oracle read by `test_fnp8_oracle_matrix.py`.
- [fnp8_repark_dispositions.json](fnp8_repark_dispositions.json) — Recorded data read by `test_fnp8_oracle_matrix.py`.
- [fnp8_repark_errors.json](fnp8_repark_errors.json) — Recorded data read by `test_fnp8_oracle_matrix.py`.
- [fnp8_spark_oracle.json](fnp8_spark_oracle.json) — Recorded Spark oracle read by `test_fnp8_oracle_matrix.py`.
- [fnp_6d_followup_1_spark_oracle.json](fnp_6d_followup_1_spark_oracle.json) — Recorded Spark oracle (74 cells) read by `test_fnp_6d_followup_1.py`, `test_fnp_bitmap_facade_1.py`.
- [fnp_agg_1_critic_spark_oracle.json](fnp_agg_1_critic_spark_oracle.json) — Recorded Spark oracle (15 cells) read by `test_fnp_agg_1_critic.py`.
- [fnp_agg_1_spark_oracle.json](fnp_agg_1_spark_oracle.json) — Recorded Spark oracle (202 cells) read by `test_fnp_agg_1.py`.
- [fnp_alias_1_spark_oracle.json](fnp_alias_1_spark_oracle.json) — Recorded Spark oracle (98 cells) read by `test_fnp_alias_1.py`.
- [fnp_bitmap_facade_1_spark_oracle.json](fnp_bitmap_facade_1_spark_oracle.json) — Recorded Spark oracle (22 cells) read by `test_fnp_bitmap_facade_1.py`.
- [fnp_degrees_rust_1_spark_oracle.json](fnp_degrees_rust_1_spark_oracle.json) — Recorded Spark oracle (344 cells) read by `test_fnp_degrees_rust_1.py`.
- [fnp_gen_1_s34_critic_spark_oracle.json](fnp_gen_1_s34_critic_spark_oracle.json) — Recorded Spark oracle (34 cells) read by `test_fnp_gen_1_s34_critic.py`.
- [fnp_gen_1_s34_spark_oracle.json](fnp_gen_1_s34_spark_oracle.json) — Recorded Spark oracle (80 cells) read by `test_fnp_gen_1_s34.py`.
- [fnp_gen_1_spark_oracle.json](fnp_gen_1_spark_oracle.json) — Recorded Spark oracle (62 cells) read by `test_fnp_gen_1.py`.
- [fnp_math_1_q12_q15_oracle.json](fnp_math_1_q12_q15_oracle.json) — Recorded Spark oracle, kept for the record (no test loads it).
- [fnp_math_1_spark_oracle.json](fnp_math_1_spark_oracle.json) — Recorded Spark oracle read by `test_fnp_math_1.py`.
- [fnp_misc_1_agg_spark_oracle.json](fnp_misc_1_agg_spark_oracle.json) — Recorded Spark oracle (202 cells) read by `test_fnp_misc_1.py`.
- [fnp_misc_1_arrow2_spark_oracle.json](fnp_misc_1_arrow2_spark_oracle.json) — Recorded Spark oracle (13 cells) read by `test_fnp_misc_1.py`.
- [fnp_misc_1_arrow_bucket_spark_oracle.json](fnp_misc_1_arrow_bucket_spark_oracle.json) — Recorded Spark oracle (11 cells) read by `test_fnp_misc_1.py`.
- [fnp_win_1_spark_oracle.json](fnp_win_1_spark_oracle.json) — Recorded Spark oracle (320 cells) read by `test_fnp_win_1.py`.
- [map_column_panic_1_spark_oracle.json](map_column_panic_1_spark_oracle.json) — Live PySpark 4.1.2 recording (2026-10-20, `local[1]`, zone UTC, ANSI on): 33 cells over the three-row temp view; read by `test_map_column_panic_1.py`.
- [unresolved_routine_1_spark_oracle.json](unresolved_routine_1_spark_oracle.json) — Recorded Spark oracle (42 cells) read by `test_unresolved_routine_1.py`.

## Path expressions

The move put every file here one level below the old flat directory. These files had a
path expression repointed in the same unit and no other line changed:

| file | change |
|---|---|
| `_record_float_agg_goldens.py` | names its own or a sibling's new path in a docstring or recorded string. |
| `_record_window_goldens.py` | names its own or a sibling's new path in a docstring or recorded string. |
| `test_float_agg_parity.py` | names its own or a sibling's new path in a docstring or recorded string. |
| `test_fnp15_16_declared_refuse.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_functions_gt1.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_log1p_1.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_ta.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_ta_volume.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_ta_with_indicators.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_window_parity.py` | names its own or a sibling's new path in a docstring or recorded string. |

## Pointers

- Up: [../map.md](../map.md)

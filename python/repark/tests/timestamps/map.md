# map — python/repark/tests/timestamps

## Purpose

Timestamp semantics across every door: TIMESTAMP_LTZ / TIMESTAMP_NTZ, `timestamp_ns` / `timestamptz_ns`, session time zones and zone spellings, timestamp casts and store assignment, epoch constructors and wall-clock rendering.

Part of the FACADE-TESTS-LAYOUT-1 (2026-10-19) layout; the directory rules live in
[../map.md](../map.md) "Layout". A new test goes in the directory of the subsystem it
pins.

## Contents

- [test_cast_ts_string_1.py](test_cast_ts_string_1.py) — CAST-TS-STRING-1 — ``CAST(<string> AS TIMESTAMP)`` answers Spark 4.1.2 on every door.
- [test_current_date_session_zone_1.py](test_current_date_session_zone_1.py) — CURRENT-DATE-SESSION-ZONE-1 — ``current_date()`` answers the session-zone date.
- [test_ice_tsns_coercion_1.py](test_ice_tsns_coercion_1.py) — The ICE-TSNS-COERCION-1 slice 0 fixture is complete and matches its summary.
- [test_ice_tsns_floor_doors_1.py](test_ice_tsns_floor_doors_1.py) — ICE-TSNS-FLOOR-DOORS-1: every write door stores the nanosecond floor in the session zone.
- [test_ice_tsns_frame_door_1.py](test_ice_tsns_frame_door_1.py) — ICE-TSNS-FRAME-DOOR-1: a pandas or polars nanosecond timestamp column enters as microseconds.
- [test_ice_tsns_merge_wall_1.py](test_ice_tsns_merge_wall_1.py) — ICE-TSNS-MERGE-WALL-1: every write door stores the same nanosecond wall.
- [test_ice_tsns_sql_1.py](test_ice_tsns_sql_1.py) — ICE-TSNS-SQL-1: ``timestamp_ns`` / ``timestamptz_ns`` on the SQL door answer the Iceberg spec.
- [test_ice_tsns_type_planner_1.py](test_ice_tsns_type_planner_1.py) — ICE-TSNS-TYPE-PLANNER-1: the SQL type name decides the unit, on every door.
- [test_ice_tstzns_wall_1.py](test_ice_tstzns_wall_1.py) — ICE-TSTZNS-WALL-1: every write door stores the same instant into ``timestamptz_ns``.
- [test_ltz_stacked_sign_1.py](test_ltz_stacked_sign_1.py) — WO LTZ-STACKED-SIGN-1 re-verify fold (RN3-1): probe strings round-trip.
- [test_ltz_store_int_1.py](test_ltz_store_int_1.py) — WO LTZ-STORE-INT-1: an INT stored into a TIMESTAMP column refuses like Spark.
- [test_ntz_1.py](test_ntz_1.py) — WO NTZ-1 slices 1 to 3: TIMESTAMP_NTZ literals, casts, store and storage.
- [test_ntz_7_verify.py](test_ntz_7_verify.py) — WO NTZ-1 verifier fold: from_utc_timestamp and to_utc_timestamp keep Spark's TIMESTAMP.
- [test_ntz_8_verify.py](test_ntz_8_verify.py) — WO NTZ-1 verifier fold part 2: zone-shift calls inside DML keep Spark's instant.
- [test_ntz_9_verify.py](test_ntz_9_verify.py) — WO NTZ-1 re-verify fold: VALUES cells computed in LTZ store the session wall.
- [test_ntz_store_doors_1.py](test_ntz_store_doors_1.py) — NTZ-STORE-DOORS-1: every write door stores Spark's TIMESTAMP_NTZ value, partitions included.
- [test_runtime_zone_spellings_1.py](test_runtime_zone_spellings_1.py) — Zone spellings against the batch-17 oracle — SET-ANSI-RUNTIME-1 review follow-up.
- [test_session_timezone_parity.py](test_session_timezone_parity.py) — Session-timezone differential rows (gap G1) + temporal edges (gap G16) — H-1a split A.
- [test_sql_epoch_constructors_1.py](test_sql_epoch_constructors_1.py) — SQL-EPOCH-CONSTRUCTORS-1 — ``timestamp_seconds``/``millis``/``micros`` equal Spark 4.1.2.
- [test_store_ts_doors_2.py](test_store_ts_doors_2.py) — STORE-TS-DOORS-2: timestamp store-assignment cells replay Spark on every door.
- [test_store_ts_doors_2_fold6.py](test_store_ts_doors_2_fold6.py) — STORE-TS-DOORS-2 fold 6: nullary timestamp columns refuse on every door.
- [test_store_ts_to_numeric_1.py](test_store_ts_to_numeric_1.py) — STORE-TS-TO-NUMERIC-1: storing a timestamp into a numeric column replays Spark.
- [test_text_write_timestamp_zone_1.py](test_text_write_timestamp_zone_1.py) — TEXT-WRITE-TIMESTAMP-ZONE-1: CSV and JSON writes format TIMESTAMP like Spark.
- [test_timestamp_cast_parity.py](test_timestamp_cast_parity.py) — ``CAST(TIMESTAMP AS <numeric>)`` (TZ-5) and ``CAST(TIMESTAMP AS STRING)`` (B-TZ-4) rows.
- [test_timestamp_type.py](test_timestamp_type.py) — Q10 — ``spark.sql.timestampType`` LTZ default + NTZ opt-in.
- [test_tz_asof_1.py](test_tz_asof_1.py) — WO TZ-ASOF-1: an expression named like its ORDER BY key plans.
- [test_u9_types_1.py](test_u9_types_1.py) — WO U9-TYPES-1: TIMESTAMP_LTZ and MAP columns end to end, replayed against Spark 4.1.2.
- [test_zone_horizon_render_1.py](test_zone_horizon_render_1.py) — ZONE-HORIZON-RENDER-1 — an instant after 2099 renders at Spark's wall clock.
- [_ice_tsns_coercion_1_doors.py](_ice_tsns_coercion_1_doors.py) — Write doors for the ICE-TSNS-COERCION-1 twin matrix.
- [_ice_tsns_coercion_1_spellings.py](_ice_tsns_coercion_1_spellings.py) — Spellings, twin kernels, and door names for ICE-TSNS-COERCION-1 slice 0.
- [_ice_tsns_merge_wall_1_doors.py](_ice_tsns_merge_wall_1_doors.py) — ICE-TSNS-MERGE-WALL-1: the write-door matrix shared by the recorder and the pins.
- [_ice_tsns_type_planner_1_cells.py](_ice_tsns_type_planner_1_cells.py) — Cells of the ICE-TSNS-TYPE-PLANNER-1 fixture: census, cache, reads, consumers, repros.
- [_ice_tsns_type_planner_1_pins.py](_ice_tsns_type_planner_1_pins.py) — Amended pins of ICE-TSNS-TYPE-PLANNER-1: the lattice, positions, refusals, doors.
- [_ice_tsns_type_planner_1_repros.py](_ice_tsns_type_planner_1_repros.py) — The parking repros of R-017's seven verifier rounds as named cells.
- [_ice_tsns_type_planner_1_rule.py](_ice_tsns_type_planner_1_rule.py) — The ICE-TSNS-TYPE-PLANNER-1 rule as a harness: every expectation is derived here.
- [_record_cast_ts_string_1.py](_record_cast_ts_string_1.py) — Re-derive the CAST-TS-STRING-1 Spark oracle from live PySpark 4.1.2.
- [_record_ice_tsns_coercion_1_casts.py](_record_ice_tsns_coercion_1_casts.py) — Record whether a written TIMESTAMP cast stays nanoseconds in the logical plan.
- [_record_ice_tsns_coercion_1_gate.py](_record_ice_tsns_coercion_1_gate.py) — Record why the gate-open rate is not on the public EXPLAIN text.
- [_record_ice_tsns_coercion_1_lambda.py](_record_ice_tsns_coercion_1_lambda.py) — Record the analyzed plan of a lambda over an array of nanosecond values.
- [_record_ice_tsns_coercion_1_pins.py](_record_ice_tsns_coercion_1_pins.py) — Census the tests that expect microseconds for a card spelling.
- [_record_ice_tsns_coercion_1_spark_lattice.py](_record_ice_tsns_coercion_1_spark_lattice.py) — Record Spark 4.1.2's lattice for the ICE-TSNS-COERCION-1 temporal set.
- [_record_ice_tsns_coercion_1_try_cast.py](_record_ice_tsns_coercion_1_try_cast.py) — Record TRY_CAST(x AS TIMESTAMP) for a microsecond, a nanosecond and a string.
- [_record_ice_tsns_coercion_1_twins.py](_record_ice_tsns_coercion_1_twins.py) — Record the ICE-TSNS-COERCION-1 twin matrix from the installed build.
- [_record_ice_tsns_floor_doors_1_main.py](_record_ice_tsns_floor_doors_1_main.py) — ICE-TSNS-FLOOR-DOORS-1 step 0 recorder of main microsecond narrowing per door.
- [_record_ice_tsns_frame_door_1.py](_record_ice_tsns_frame_door_1.py) — ICE-TSNS-FRAME-DOOR-1: record what Spark answers for a pandas nanosecond timestamp frame.
- [_record_ice_tsns_merge_wall_1_main.py](_record_ice_tsns_merge_wall_1_main.py) — ICE-TSNS-MERGE-WALL-1: record what the installed build stores in every matrix cell.
- [_record_ice_tsns_sql_1_oracle.py](_record_ice_tsns_sql_1_oracle.py) — Record and check the ICE-TSNS-SQL-1 oracle: the Iceberg spec plus a PyIceberg 0.12.0 read-back.
- [_record_ice_tsns_type_planner_1.py](_record_ice_tsns_type_planner_1.py) — Run every ICE-TSNS-TYPE-PLANNER-1 cell on the installed module and count by family.
- [_record_ns_array_floor_1_input.py](_record_ns_array_floor_1_input.py) — NS-ARRAY-FLOOR-1 D1: record what each pandas and Arrow door receives.
- [_record_ns_array_floor_1_lane_a.py](_record_ns_array_floor_1_lane_a.py) — NS-ARRAY-FLOOR-1 F2: Lane A echo shape behind the 108 lost statements.
- [_record_ns_array_floor_1_output.py](_record_ns_array_floor_1_output.py) — NS-ARRAY-FLOOR-1 D2: record what each door stores for a nanosecond output.
- [_record_ntz_1_spark_table.py](_record_ntz_1_spark_table.py) — Record the NTZ-1 Slice 3 Spark-written table fixture and its Spark read answers.
- [_record_session_timezone_goldens.py](_record_session_timezone_goldens.py) — Record mode for the session-timezone corpus — re-derive every `spark` half from live PySpark.
- [_record_sql_epoch_constructors_1.py](_record_sql_epoch_constructors_1.py) — Re-derive the SQL-EPOCH-CONSTRUCTORS-1 Spark oracle from live PySpark 4.1.2.
- [_record_timestamp_cast_goldens.py](_record_timestamp_cast_goldens.py) — Record mode for the timestamp-cast corpus — re-derive every `spark` half from live PySpark.
- [_record_zone_horizon_render_1.py](_record_zone_horizon_render_1.py) — Re-derive the ZONE-HORIZON-RENDER-1 Spark oracle from live PySpark 4.1.2.
- [_summarize_ice_tsns_coercion_1.py](_summarize_ice_tsns_coercion_1.py) — Build the twin summary from the fixture and the Spark lattice.
- [_time_ice_tsns_coercion_1.py](_time_ice_tsns_coercion_1.py) — Time the ICE-TSNS-COERCION-1 cost baseline on the installed module.
- [_time_ice_tsns_frame_door_1.py](_time_ice_tsns_frame_door_1.py) — ICE-TSNS-FRAME-DOOR-1: time ``createDataFrame`` + ``count()`` of a pandas timestamp frame.
- [cast_ts_string_1_spark_oracle.json](cast_ts_string_1_spark_oracle.json) — Recorded Spark oracle (604 cells) read by `test_cast_ts_string_1.py`; re-derived by `_record_cast_ts_string_1.py`.
- [ice_tsns_coercion_1_spark_lattice.json](ice_tsns_coercion_1_spark_lattice.json) — Recorded data (1540 cells) read by `test_ice_tsns_coercion_1.py`.
- [ice_tsns_coercion_1_twins.json](ice_tsns_coercion_1_twins.json) — Recorded data read by `_ice_tsns_type_planner_1_cells.py`, `test_ice_tsns_coercion_1.py`.
- [ice_tsns_frame_door_1_spark.json](ice_tsns_frame_door_1_spark.json) — Recorded Spark oracle (90 cells) read by `test_ice_tsns_frame_door_1.py`.
- [ice_tsns_merge_wall_1_main.json](ice_tsns_merge_wall_1_main.json) — RePark recording (760 cells) read by `test_ice_tsns_merge_wall_1.py`, `test_ice_tstzns_wall_1.py`; re-derived by `_record_ice_tsns_merge_wall_1_main.py`.
- [ice_tsns_sql_1_oracle.json](ice_tsns_sql_1_oracle.json) — ICE-TSNS-SQL-1 recording: the PyIceberg read-back of the DataFrame-door control, RePark's `.partitions` answer, the literals with their nanoseconds, and the SQL-door statements; read by `test_ice_tsns_sql_1.py`.
- [ice_tsns_type_planner_1_dated.json](ice_tsns_type_planner_1_dated.json) — Recorded data read by `test_ice_tsns_type_planner_1.py`.
- [ice_tsns_type_planner_1_main.json](ice_tsns_type_planner_1_main.json) — RePark recording read by `test_ice_tsns_type_planner_1.py`; re-derived by `_record_ice_tsns_type_planner_1.py`.
- [ice_tsns_type_planner_1_reads.json](ice_tsns_type_planner_1_reads.json) — Recorded data (117 cells) read by `_ice_tsns_type_planner_1_cells.py`.
- [ice_tsns_type_planner_1_repros.json](ice_tsns_type_planner_1_repros.json) — Recorded data read by `_ice_tsns_type_planner_1_repros.py`.
- [ntz_1_spark_oracle.json](ntz_1_spark_oracle.json) — Recorded Spark oracle (2 cells) read by `test_ntz_1.py`.
- [ntz_7_verify_spark_oracle.json](ntz_7_verify_spark_oracle.json) — Recorded Spark oracle (7 cells) read by `test_ntz_7_verify.py`.
- [ntz_8_verify_spark_oracle.json](ntz_8_verify_spark_oracle.json) — Recorded Spark oracle read by `test_ntz_8_verify.py`.
- [ntz_9_verify_spark_oracle.json](ntz_9_verify_spark_oracle.json) — Recorded Spark oracle read by `test_ntz_9_verify.py`.
- [ntz_store_doors_1_mixed_spark_oracle.json](ntz_store_doors_1_mixed_spark_oracle.json) — Recorded Spark oracle (52 cells) read by `test_ntz_store_doors_1.py`.
- [ntz_store_doors_1_spark_oracle.json](ntz_store_doors_1_spark_oracle.json) — Recorded Spark oracle (66 cells) read by `test_ntz_store_doors_1.py`.
- [sql_epoch_constructors_1_spark_oracle.json](sql_epoch_constructors_1_spark_oracle.json) — Recorded Spark oracle (304 cells) read by `test_sql_epoch_constructors_1.py`; re-derived by `_record_sql_epoch_constructors_1.py`.
- [store_ts_doors_2_spark_oracle.json](store_ts_doors_2_spark_oracle.json) — Recorded Spark oracle (63 cells) read by `test_store_ts_doors_2.py`.
- [store_ts_to_numeric_1_spark_oracle.json](store_ts_to_numeric_1_spark_oracle.json) — Recorded Spark oracle (79 cells) read by `test_store_ts_to_numeric_1.py`.
- [text_write_timestamp_zone_1_fixture.json](text_write_timestamp_zone_1_fixture.json) — Recorded data (233 cells) read by `test_text_write_timestamp_zone_1.py`.
- [u9_types_1_spark_oracle.json](u9_types_1_spark_oracle.json) — Recorded Spark oracle read by `test_u9_types_1.py`.
- [zone_horizon_render_1_residue.json](zone_horizon_render_1_residue.json) — Recorded data read by `test_zone_horizon_render_1.py`; re-derived by `_record_zone_horizon_render_1.py`.
- [zone_horizon_render_1_spark_oracle.json](zone_horizon_render_1_spark_oracle.json) — Recorded Spark oracle read by `test_zone_horizon_render_1.py`; re-derived by `_record_zone_horizon_render_1.py`.

## Path expressions

The move put every file here one level below the old flat directory. These files had a
path expression repointed in the same unit and no other line changed:

| file | change |
|---|---|
| `_record_cast_ts_string_1.py` | names its own or a sibling's new path in a docstring or recorded string. |
| `_record_ice_tsns_coercion_1_pins.py` | names its own or a sibling's new path in a docstring or recorded string; repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `_record_ice_tsns_sql_1_oracle.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `_record_ntz_1_spark_table.py` | names its own or a sibling's new path in a docstring or recorded string; reads the root `fixtures/` one level up. |
| `_record_session_timezone_goldens.py` | names its own or a sibling's new path in a docstring or recorded string. |
| `_record_sql_epoch_constructors_1.py` | names its own or a sibling's new path in a docstring or recorded string. |
| `_record_timestamp_cast_goldens.py` | names its own or a sibling's new path in a docstring or recorded string. |
| `_record_zone_horizon_render_1.py` | names its own or a sibling's new path in a docstring or recorded string. |
| `test_ice_tsns_coercion_1.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_ntz_1.py` | reads the root `fixtures/` one level up. |
| `test_session_timezone_parity.py` | names its own or a sibling's new path in a docstring or recorded string. |
| `test_text_write_timestamp_zone_1.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_timestamp_cast_parity.py` | names its own or a sibling's new path in a docstring or recorded string. |
| `test_u9_types_1.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |

## Pointers

- Up: [../map.md](../map.md)

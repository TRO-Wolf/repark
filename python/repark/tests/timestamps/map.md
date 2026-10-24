# map — python/repark/tests/timestamps

## Purpose

Timestamp semantics across every door: TIMESTAMP_LTZ / TIMESTAMP_NTZ, `timestamp_ns` / `timestamptz_ns`, session time zones and zone spellings, timestamp casts and store assignment, epoch constructors and wall-clock rendering.

MAP-COLUMN-PANIC-1 (2026-10-20): the four dated rows `repro|v2_map_element_site|…` (the `list array` panic of `coalesce(element_at(map('a', c), 'a'), ts)`, ruling 93) leave `ice_tsns_type_planner_1_dated.json` because the cells now store; `ice_tsns_type_planner_1_main.json` stays the snapshot of the older main it recorded. The U9-TYPES-1 residue R-9 of the two `INSERT … VALUES (map(NULL, 1))` and `(map('d', 1, 'd', 2))` steps moves in `u9_types_1_spark_oracle.json` from DataFusion's `map key cannot be null` and `map key must be unique` to Spark's `[NULL_MAP_KEY]` and `[DUPLICATED_MAP_KEY]` text with SQLSTATE 2200E and 23505; Spark still answers `INVALID_INLINE_TABLE`, so the residue stays.

Part of the FACADE-TESTS-LAYOUT-1 (2026-10-19) layout; the directory rules live in
[../map.md](../map.md) "Layout". A new test goes in the directory of the subsystem it
pins.

## Contents

- [test_cast_ts_string_1.py](test_cast_ts_string_1.py) — CAST-TS-STRING-1 — ``CAST(<string> AS TIMESTAMP)`` answers Spark 4.1.2 on every door.
- [test_current_date_session_zone_1.py](test_current_date_session_zone_1.py) — CURRENT-DATE-SESSION-ZONE-1 — ``current_date()`` answers the session-zone date.
- [test_ice_ntz_nested_refuse_1.py](test_ice_ntz_nested_refuse_1.py) — ICE-NTZ-NESTED-REFUSE-1: an instant for a nested NTZ leaf refuses outside UTC (DataFrame writers, a frame holding a container cast of an instant on seven writers, the 174 recorded Spark cells, the live re-derivation). pins: ice-ntz-nested-refuse-1/C-009, C-010, C-015 **ZONE-SEAM-1 (2026-10-22, ruling ZS-4):** the frame holding a container cast of an instant now stores the session wall (`1767305045123456` in New York, Spark's value) on the seven writers; the seam rewrites the cast before the router's gate reads the frame, so the refusal is lifted. pins: zone-seam-1/C-009
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
- [test_zone_seam_1.py](test_zone_seam_1.py) — ZONE-SEAM-1 step 4 (2026-10-22): the card CONTAINER-CAST-ZONE-1's tables on the facade doors, with Spark 4.1.2's values. The Spark SQL door: the plain `SELECT` of an array, struct and map cast of an instant and the leaf control, in New York and UTC; the "through another type" writes (through `STRING`, an array through `STRING`, the leaf cast over a string the container made, through `DATE`, `from_json(to_json(…))` storing NULL, a MERGE update through `STRING`); through `BIGINT` refused (`CAST_WITHOUT_SUGGESTION` in UTC; R-011's text outside UTC, because the gate plans the source first and the plan raises), no row written; the six table-creating writers and a cached frame storing the session wall in New York and UTC. The DataFrame door: `Column.cast` of a map and `expr` casts of a struct and an array answer the session wall; `Column.cast` to a struct or array type is not parsed by the facade (`unknown cast type`), on main as on the head. The native `repark.sql()` door (ruling ZS-5): its struct, `make_array` and leaf casts and its struct `STRING` leaf answer as on main. **Step 3c (ZS-9):** a frame with a nested `TIMESTAMP_NTZ` leaf written into a `STRUCT<v: TIMESTAMP, n: INT>` column refuses outside UTC on `insertInto(overwrite=True)` and `overwritePartitions` (the whole text, no row), stores the New York read on `writeTo().append()` and `insertInto`, and stores the UTC read on all four in UTC. pins: zone-seam-1/C-007, C-008, C-015 *Fix round 1 (2026-10-22, verifier S1-2):* a pandas column zoned `America/New_York` (two rows) cast inside a struct to `TIMESTAMP_NTZ` answers the New York session wall; it reds when the seam reads only a `UTC` zone string as an instant. pins: zone-seam-1/C-018
- [test_zone_oracle_1.py](test_zone_oracle_1.py) — ZONE-ORACLE-1: every timestamp-and-zone cell replays Spark on a plain `SELECT`; the non-EQUAL set is exactly the dated list, and twenty hash-chosen cells re-record live.
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
- [_ice_ntz_nested_refuse_1_doors.py](_ice_ntz_nested_refuse_1_doors.py) — ICE-NTZ-NESTED-REFUSE-1: twelve of PR #1037's nested NTZ doors × five sources, run on either engine, and the ruled RePark answer. pins: ice-ntz-nested-refuse-1/C-010, C-011
- [_record_ice_ntz_nested_refuse_1.py](_record_ice_ntz_nested_refuse_1.py) — ICE-NTZ-NESTED-REFUSE-1: record what Spark 4.1.2 stores through those doors in three zones.
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
- [_record_zone_oracle_1.py](_record_zone_oracle_1.py) — Record the ZONE-ORACLE-1 Spark oracle from live PySpark 4.1.2 on its own `local[1]` context (`--check [--sample N]` re-derives), and with `--main` replay the corpus on the installed RePark build and write the dated difference list.
- [_record_write_spine_1.py](_record_write_spine_1.py) — WRITE-SPINE-1 step 0 recorder: every door-by-shape cell creates a fresh Iceberg table with the target schema, runs the seed and the spelling, reads `v` back and drops the table, one child process per zone. `--spark` records Spark 4.1.2 + Iceberg 1.11.0 (Hadoop catalog, `local[1]`, ANSI on); `--main --base <sha>` records the installed build through the facade session and `repark.sql()` and classifies each cell against the Spark fixture; `--census --base <sha>` prints the census tables and the red set.
- [_summarize_ice_tsns_coercion_1.py](_summarize_ice_tsns_coercion_1.py) — Build the twin summary from the fixture and the Spark lattice.
- [_time_ice_tsns_coercion_1.py](_time_ice_tsns_coercion_1.py) — Time the ICE-TSNS-COERCION-1 cost baseline on the installed module.
- [_time_ice_tsns_frame_door_1.py](_time_ice_tsns_frame_door_1.py) — ICE-TSNS-FRAME-DOOR-1: time ``createDataFrame`` + ``count()`` of a pandas timestamp frame.
- [_zone_oracle_1_corpus.py](_zone_oracle_1_corpus.py) — ZONE-ORACLE-1: the deterministic generator of the 1,640 timestamp-and-zone `SELECT` cells (328 expressions × 5 zones), their stable ids, the corpus hash and the hash-chosen live sample.
- [_write_spine_1_spellings.py](_write_spine_1_spellings.py) — WRITE-SPINE-1 step 0: the 41 write spellings keyed by name to their door (D1–D10, D14–D21): the U-8 probe's 28 with its statements, `insert_options` (D2), `ctas`, `rtas`, `create_or_replace_as` (D6), the seven `native_*` spellings sent through `repark.sql()` (D14–D17, D19, D20), `native_append_api` (D18, `reach = "rust_api"`) and `microbatch_sink` (D21, `reach = "none"`: main has no streaming facade). Each entry: name, door, template, reach, row-level mode, seed.
- [_write_spine_1_shapes.py](_write_spine_1_shapes.py) — WRITE-SPINE-1 step 0: the shape axis and the door-by-shape cells. 8 sources (the ZONE-ORACLE-1 `inst`, `wall`, `str`, `date` literals and a column of each from the one-row view `write_spine_1_source`) × 10 targets (leaf `TIMESTAMP` and `TIMESTAMP_NTZ`, and the corpus's `struct`, `array`, `map`, `array_struct` holding each) × 3 zones = 240 shapes; × the 39 measured spellings, CoW and MoR for the row-level ones, field assignments on the leaf and struct targets only = 12,192 cells, id `<spelling>.<mode>.<target>.<source>.<zone>`. Run as a script it prints the count and the corpus hash.
- [cast_ts_string_1_spark_oracle.json](cast_ts_string_1_spark_oracle.json) — Recorded Spark oracle (604 cells) read by `test_cast_ts_string_1.py`; re-derived by `_record_cast_ts_string_1.py`.
- [ice_tsns_coercion_1_spark_lattice.json](ice_tsns_coercion_1_spark_lattice.json) — Recorded data (1540 cells) read by `test_ice_tsns_coercion_1.py`.
- [ice_tsns_coercion_1_twins.json](ice_tsns_coercion_1_twins.json) — Recorded data read by `_ice_tsns_type_planner_1_cells.py`, `test_ice_tsns_coercion_1.py`.
- [ice_ntz_nested_refuse_1_spark.json](ice_ntz_nested_refuse_1_spark.json) — Recorded Spark oracle (174 cells, 2026-10-20) read by `test_ice_ntz_nested_refuse_1.py`.
- [ice_tsns_frame_door_1_spark.json](ice_tsns_frame_door_1_spark.json) — Recorded Spark oracle (90 cells) read by `test_ice_tsns_frame_door_1.py`.
- [ice_tsns_merge_wall_1_main.json](ice_tsns_merge_wall_1_main.json) — RePark recording (760 cells) read by `test_ice_tsns_merge_wall_1.py`, `test_ice_tstzns_wall_1.py`; re-derived by `_record_ice_tsns_merge_wall_1_main.py`.
- [ice_tsns_sql_1_oracle.json](ice_tsns_sql_1_oracle.json) — ICE-TSNS-SQL-1 recording: the PyIceberg read-back of the DataFrame-door control, RePark's `.partitions` answer, the literals with their nanoseconds, and the SQL-door statements; read by `test_ice_tsns_sql_1.py`.
- [ice_tsns_type_planner_1_dated.json](ice_tsns_type_planner_1_dated.json) — Recorded data read by `test_ice_tsns_type_planner_1.py`. EXPLAIN-DML-WRITE-1 (2026-10-21, ruling 144): 350 dated rows, class "ruled refusal", for the census cells the head makes red or lost on the `explain_analyze_insert` (125), `explain_analyze_update` (100, ruling 136) and `prepare_execute_insert` (125, ruling 138) routes, now refused by every door (rulings 136, 138, 141); the harness and the routes are unchanged. pins: explain-dml-write-1/C-011
- [ice_tsns_type_planner_1_main.json](ice_tsns_type_planner_1_main.json) — RePark recording read by `test_ice_tsns_type_planner_1.py`; re-derived by `_record_ice_tsns_type_planner_1.py`.
- [ice_tsns_type_planner_1_reads.json](ice_tsns_type_planner_1_reads.json) — Recorded data (117 cells) read by `_ice_tsns_type_planner_1_cells.py`.
- [ice_tsns_type_planner_1_repros.json](ice_tsns_type_planner_1_repros.json) — Recorded data read by `_ice_tsns_type_planner_1_repros.py`.
- [ntz_1_spark_oracle.json](ntz_1_spark_oracle.json) — Recorded Spark oracle (2 cells) read by `test_ntz_1.py`.
- [ntz_7_verify_spark_oracle.json](ntz_7_verify_spark_oracle.json) — Recorded Spark oracle (7 cells) read by `test_ntz_7_verify.py`.
- [ntz_8_verify_spark_oracle.json](ntz_8_verify_spark_oracle.json) — Recorded Spark oracle read by `test_ntz_8_verify.py`.
- [ntz_9_verify_spark_oracle.json](ntz_9_verify_spark_oracle.json) — Recorded Spark oracle read by `test_ntz_9_verify.py`. **ZONE-SEAM-1 (2026-10-22):** step `w_ref_from` loses its `divergence: R-NTZ-S2-9` and `repark_error_contains` annotations: `from_utc_timestamp` of a string now returns `TIMESTAMP` through `SparkUtcShift`, so RePark names the source `"TIMESTAMP"` as Spark does and the step asserts Spark's own text; the recorded Spark answer is unchanged. pins: zone-seam-1/C-004
- [ntz_store_doors_1_mixed_spark_oracle.json](ntz_store_doors_1_mixed_spark_oracle.json) — Recorded Spark oracle (52 cells) read by `test_ntz_store_doors_1.py`.
- [ntz_store_doors_1_spark_oracle.json](ntz_store_doors_1_spark_oracle.json) — Recorded Spark oracle (66 cells) read by `test_ntz_store_doors_1.py`.
- [sql_epoch_constructors_1_spark_oracle.json](sql_epoch_constructors_1_spark_oracle.json) — Recorded Spark oracle (304 cells) read by `test_sql_epoch_constructors_1.py`; re-derived by `_record_sql_epoch_constructors_1.py`.
- [store_ts_doors_2_spark_oracle.json](store_ts_doors_2_spark_oracle.json) — Recorded Spark oracle (63 cells) read by `test_store_ts_doors_2.py`.
- [store_ts_to_numeric_1_spark_oracle.json](store_ts_to_numeric_1_spark_oracle.json) — Recorded Spark oracle (79 cells) read by `test_store_ts_to_numeric_1.py`.
- [text_write_timestamp_zone_1_fixture.json](text_write_timestamp_zone_1_fixture.json) — Recorded data (233 cells) read by `test_text_write_timestamp_zone_1.py`.
- [u9_types_1_spark_oracle.json](u9_types_1_spark_oracle.json) — Recorded Spark oracle read by `test_u9_types_1.py`.
- [zone_horizon_render_1_residue.json](zone_horizon_render_1_residue.json) — Recorded data read by `test_zone_horizon_render_1.py`; re-derived by `_record_zone_horizon_render_1.py`. **ZONE-SEAM-1 (2026-10-22, ruling ZS-7):** re-derived with `--engine repark --residue` on the unit's head: 15 cells leave (the `from_utc_timestamp_wall` / `to_utc_timestamp_wall` edge cells of Sydney, Lord Howe and New York, now Spark's answer) and 3 join, `America/New_York|2099|wall_overlap|to_utc_timestamp_wall`, `America/New_York|2100|wall_gap|to_utc_timestamp_wall` and `America/New_York|2104|wall_gap|to_utc_timestamp_wall`: accidental EQUAL on main, wrong type (`timestamp_ntz`); honest DIFFERENT after the seam (Spark `2099-11-01 05:30:00`, `2100-03-14 07:30:00`, `2104-03-09 07:30:00`; the head answers the shared instant kernel's `04:30`, `08:30`, `08:30`); closes with ZONE-HORIZON-INSTANT-1. The residue list is this suite's expected-divergence form: each listed key must differ from the oracle's Spark value. pins: zone-seam-1/C-014
- [zone_horizon_render_1_spark_oracle.json](zone_horizon_render_1_spark_oracle.json) — Recorded Spark oracle read by `test_zone_horizon_render_1.py`; re-derived by `_record_zone_horizon_render_1.py`.
- [zone_oracle_1_main_difference.json](zone_oracle_1_main_difference.json) — RePark recording (first on main `754fb6e5`: 400 DIFFERENT, 20 REFUSED, 70 ERROR; re-recorded 2026-10-20 by MAP-COLUMN-PANIC-1 over main `5c8c811e`: 411 DIFFERENT, 20 REFUSED, 10 ERROR, 49 `map(...)` cells left the list; re-recorded 2026-10-22 by ZONE-SEAM-1 over its head `dc0da5a2`: 90 DIFFERENT, 10 REFUSED, 0 ERROR, the 341 cells the seam closes left the list and none joined; the 90 + 10 are the residue of `task/ledgers/staging/zone-seam-1-ledger.md`) pins: zone-seam-1/C-006: every ZONE-ORACLE-1 cell that is not EQUAL, with its status and the measured RePark answer; read by `test_zone_oracle_1.py`; re-derived by `_record_zone_oracle_1.py --main`.
- [zone_oracle_1_spark_oracle.json](zone_oracle_1_spark_oracle.json) — Recorded Spark 4.1.2 oracle (1,640 cells) read by `test_zone_oracle_1.py`; re-derived by `_record_zone_oracle_1.py`.

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

## ZONE-ORACLE-1 (2026-10-20)

An expression-level Spark oracle for timestamps and zones. A write stores what `SELECT` yields,
so an expression that is EQUAL on a plain `SELECT` in every zone is EQUAL through every write
door; the corpus records Spark 4.1.2 once and replays it on RePark. No product code.

**Corpus** (`_zone_oracle_1_corpus.py`, run as a script it prints the cell count and the corpus
hash). 328 expressions in 5 session zones (`UTC`, `America/New_York`, `Asia/Kolkata`, `+05:30`,
`Etc/UTC`) make 1,640 cells; an id is the dotted expression key plus the zone tag
(`array.inst.ccast_ntz.new_york`). Value sources: an instant literal
`TIMESTAMP '2026-01-02 03:04:05.123456+00:00'` (`inst`), a wall literal `TIMESTAMP_NTZ` (`wall`), a
string (`str`), a date (`date`), a `BIGINT` epoch (`epoch`), `NULL` (`null`), and a column of each
type (`col_inst`, `col_wall`, `col_str`, `col_date`, `col_epoch`) from the one-row temp view
`zone_oracle_1_one_row`. The view is selected `FROM range(1)` on purpose: a view of bare literals
is folded into the query and `map('k', c_inst)` then answers, while over a real column it panics
`list array` (card MAP-COLUMN-PANIC-1). Leaf conversions: `CAST`, `TRY_CAST`, `::`,
`to_timestamp_ntz`, `to_timestamp_ltz`, `to_utc_timestamp`, `from_utc_timestamp`, `date_format`
(`yyyy-MM-dd HH:mm:ss`, plus `date_format_micros` with `.SSSSSS` on `inst` and `wall` only,
because RePark refuses the `S` pattern letter and that would hide the zone in every other
`date_format` cell), `to_date`, `unix_micros`, `to_json`. Containers: bare, `named_struct`
(`struct`), `array`, `map` with the literal key `'k'`, `struct2`, `array2` and `array_struct`.
Targets: `TIMESTAMP_NTZ`, `TIMESTAMP`, `STRING`, `DATE`, `BIGINT`.

Four levels of expression: `leaf` (a conversion of a source, no container), `inner` (a container
whose leaf is converted inside it, the whole container selected), `container` (`CAST` or
`TRY_CAST` of the container to its own shape with the target at the leaf, or `to_json` of it;
the leaf is read back) and `through` (two chained container casts, a leaf cast over a string a
container cast made, and `from_json(to_json(..))`). A leaf read goes one step per subquery layer
(`SELECT c1.a AS v FROM (SELECT c0[0] AS c1 FROM (...) q0) q1`) because the facade refuses
`s[0].a` and `s.a.b` in one expression (`Dot access not supported for non-string expr`; measured
while drafting, 50 cells); that refusal is not a zone defect and would hide the zone.

**Recorder** (`_record_zone_oracle_1.py`). Records Spark 4.1.2 `local[1]` with
`spark.sql.ansi.enabled` true into `zone_oracle_1_spark_oracle.json`: per cell the statement, the
zone, the result type (`simpleString`) and the value as `collect()` renders it (timestamps in
microseconds, dates in ISO text, strings as text, arrays as lists, structs and maps as objects),
or the Spark error condition. The provenance header holds the Spark version, the date, the master,
the ANSI setting and the corpus hash (the SHA-256 of the ordered ids, zones and statements and the
view statement). The recorder sets its own process zone to UTC before it builds the session, so
PySpark's local-zone conversion of a `TIMESTAMP` is the identity, and it raises on any master
other than `local[1]`, so it is run as a script in its own interpreter (the PR #1122 pattern):

```
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 /tmp/sparkenv/bin/python python/repark/tests/timestamps/_record_zone_oracle_1.py
```

`--check` re-derives and exits non-zero on drift; `--check --sample 20` re-derives the twenty
cells with the smallest SHA-256 of their id.

**Replay** (`test_zone_oracle_1.py`, `_record_zone_oracle_1.py --main`). Each cell runs through
`ReparkSession.sql` in a session set to the cell's zone with ANSI on and the view re-created in
that zone. The value comes from `to_arrow()` and the type name from the facade schema, not from
`collect()`, which renders a `TIMESTAMP` at the session wall clock. A cell is classified against
the Spark record: EQUAL (same type and value, or both raise and RePark's text names Spark's error
condition), DIFFERENT (another type or value, Spark raises and RePark answers, or both raise with
another condition), REFUSED (Spark answers, RePark raises a deliberate refusal: the text says
`refus`, `unsupported` or `not supported`), ERROR (Spark answers, RePark raises anything else,
a caught Rust panic included). No cell is skipped or xfailed. The cells that are not EQUAL on
main are the dated list `zone_oracle_1_main_difference.json`, each with its status and the
measured RePark answer, recorded from the `ci`-profile build of main `754fb6e5`; the test
asserts that the set of non-EQUAL cells is exactly that list, so a cell that leaves it (a fix)
and a cell that joins it (a regression) both fail, and a listed cell must keep its status and
its measured answer (an error cell keeps only the fact that it raises). To retire a listed cell,
fix the engine and re-record the list:

```
PYTHONPATH=python/repark-parity/src python python/repark/tests/timestamps/_record_zone_oracle_1.py --main --base <sha>
```

The live cell (`REPARK_PARITY_LIVE=1`) runs the recorder as a subprocess with
`--check --sample 20` in a fresh interpreter, so it owns a `local[1]` context whatever the shared
SparkContext of the serial live suite is. The replay is 1,640 cells in about 4 s on the `ci` profile.

## Pointers

- Up: [../map.md](../map.md)

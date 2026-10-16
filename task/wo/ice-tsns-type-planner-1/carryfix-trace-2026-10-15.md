# ICE-TSNS-TYPE-PLANNER-1 (U-1, R-017): the carry-over trace (2026-10-15)

**Head traced:** `6b91af9d` (branch `fix/ice-tsns-type-planner-1-fresh`), base main `fa1e4274`.
**By:** Claude (claude-opus-5-5), Opus build lane, measurement only; no product code in this
commit. **Order:** the owner's rulings 100, 101 and 102 of 2026-10-15 and the orchestrator's
provisional rulings on Q1 and Q4. **Pins:** [step 0](step0-fresh-2026-10-15.md), the unit
ledger [ice-tsns-type-planner-1-ledger.md](../../ledgers/staging/ice-tsns-type-planner-1-ledger.md).

## 1. Method

Each break is attributed to the first carried commit at which it is red, by building and
running the tree at each carried commit in order on a scratch clone. The four states:

| State | Commit | What ran |
|---|---|---|
| S1 | `62089e59` SparkTypePlanner | dev build: `cargo test -p repark-spark`, the 27 facade tests on the state's module, the fixture record (head's fixture files) |
| S2 | `203f9821` SparkTimestampUnitWiden | the same |
| S3 | `7dfb04c3` the rule deletions | the same |
| S4 | `ca385f17` wall kernels by target type | the fresh build's runs on this tree: `cargo test -p repark-spark` (52 red), the 27 facade tests on module `c7809e68`, the step-0 fixture record (same module) |

`8840209a` (the fixture) changes no product code: `git diff fa1e4274 8840209a` touches only
`crates/repark-sql/tests/` and `python/repark/tests/`, so for the 78 it is main, and main is
green on all 78 (`cargo test -p repark-spark` exit 0 on `fa1e4274`; the facade on today's
main module `01796857`). Every one of the 86 rows is monotone: once red at a state, red at
every later state.

| Carried commit | Rust (51) | Facade (27) | Ruling-101 cells (8) |
|---|---|---|---|
| `8840209a` fixture | 0 | 0 | 0 |
| `62089e59` planner | 4 | 21 | 4 (`array_except`, `array_intersect`) |
| `203f9821` widen rule | 1 | 0 | 0 |
| `7dfb04c3` deletions | 44 | 6 | 0 |
| `ca385f17` kernels by target | 2 | 0 | 4 (`array_append`, `array_prepend`) |

## 2. The mechanism check

The statements today's main answers that each state loses (the fixture's main-answers check,
head's fixture files on each state's module):

| State | Lost | M1 sequence | M2 doors | M3 aggregate zero | M3 UNION | Shape A |
|---|---|---|---|---|---|---|
| S1 `62089e59` | 0 | 0 | 0 | 0 | 0 | 0 |
| S2 `203f9821` | 7 | 0 | 0 | 4 | 3 | 0 |
| S3 `7dfb04c3` | 289 | 103 | 108 | 4 | 0 | 70 + 4 `update_forms` |
| S4 `ca385f17` | 288 | 103 | 108 | 4 | 3 | 70 |
| head `6b91af9d` | 108 | 0 | 108 (carve-out) | 0 | 0 | 0 |

`git revert --no-commit` of each carried commit alone on the head:

| Carried commit | Reverts alone? | Mechanism it serves | Why |
|---|---|---|---|
| `8840209a` | no: the fixture files | M1, M2, M3 | It holds every mechanism's red-first cells; the later fixture commits build on it. |
| `62089e59` | no: `instant_ts.rs`, `lib.rs`, `spark_type_planner.rs`, `session_runtime.rs`, `dialect.rs` | M1, M2, M3 | The precondition of the three: it loses no statement by itself (S1: 0), and the commits whose gaps the mechanisms close are built on it (`7dfb04c3` and `ca385f17` edit the file it adds). Without it the deletions would type every written `TIMESTAMP` as nanoseconds. |
| `203f9821` | no: `timestamp_unit_widen.rs` | M3 | The aggregate-zero and UNION cells are first lost here (S2); M3's aggregate fix `0dc4455f` lives in this commit's file. |
| `7dfb04c3` | yes | M1, M2, M3 (shape A) | M1's 103, M2's 108 and shape A are first lost here (S3): a NULL beside a nanosecond value now stays nanoseconds and reaches `sequence`, the doors and the identity `UPDATE … WHERE`. |
| `ca385f17` | no: `spark_type_planner.rs` | M3 (UNION, shape B) | The UNION cells are lost again here (S4); M3's UNION fix `7dca3bef` and shape B's `177cef20` sit in the nanosecond kernel this commit routes by target type. |

Every carried commit serves at least one mechanism, so none is dropped (ruling 100).

## 3. Groups, oracles and contracts

| Code | Answer (Spark door: a recorded Spark 4.1.2 run; ANSI doors: the Q6 contract) |
|---|---|
| O1 | Spark 4.1.2 (live, 2026-10-15): `CAST(1 AS TIMESTAMP)` and `CAST(CAST(1 AS INT) AS TIMESTAMP)` answer `1970-01-01 00:00:01` UTC; an `INSERT … SELECT … UNION ALL SELECT …, DATE'…'` into `TIMESTAMP` stores the session midnight (`store_ts_doors_2_spark_oracle.json`). Main answers these; the head errors. |
| O2 | Spark 4.1.2 (live, 2026-10-15, and the cells in `fnp_degrees_rust_1` / `fnp_6d_followup_1`): `… however "x" has the type "TIMESTAMP"`. The oracle does not say `TIMESTAMP_LTZ`, so the group is a fix. |
| Q6 | Ruling 94: on the ANSI doors `TIMESTAMP` is the microsecond wall `timestamp[us]`, `TIMESTAMPTZ` the microsecond instant. Main answers the wall (`timestamp[ns]`, naive) on all five cells, so all five are regressions to fix; no cell has main answering an instant. |
| O3 | The recorded Spark 4.1.2 walls the tests already pin (`ntz_store_doors_1_mixed_spark_oracle.json`, `ntz_9_verify_spark_oracle.json`, `ntz_values_mix.rs`'s `SPARK_WALLS`, `ntz_store.rs`): a `DATE` or `TIMESTAMP_NTZ` beside a `TIMESTAMP` types the column `TIMESTAMP`, and a New York gap wall stores `03:30`. The oracle is main's pin, so re-pinning to it moves no pin: the group is a fix. |
| O4 | Spark 4.1.2 (live, 2026-10-15): `CAST(array('2024-01-01 00:00:00') AS ARRAY<TIMESTAMP_NTZ>)` answers `array<timestamp_ntz>` `[2024-01-01 00:00:00]`; the `STRUCT<a: TIMESTAMP_NTZ>` cast answers `struct<a:timestamp_ntz>` `{a: 2024-01-01 00:00:00}`. Main refuses (TZ-6); the head answers the oracle: a re-pin. |
| R-010 | R-010's rule: an `UPDATE … SET v = <timestamp_ns / timestamptz_ns literal>` stores the instant `INSERT` stores, nine digits exact (`expected_instants` in the test). Main refuses `Unsupported SQL type`; the head stores: a re-pin. |
| O5 | The Spark door builds every session with the planner (`SparkExtension::type_planner`); the `src/tests` harness built its own without it, so after the deletions a written `TIMESTAMP` or `timestamp_ns` there stays DataFusion's type. The pinned values are unchanged: a harness fix. |
| O6 | Spark 4.1.2 (live, 2026-10-15, the microsecond twin of the four statements, Iceberg 1.11.0): `UPDATE … SET v = CASE WHEN id > 0 THEN c ELSE NULL END` (and the three other spellings) stores `c` unchanged; a `NULL` takes its sibling's type (R-017's oracle). |
| T | The fixture's twin rule (statement equals its typed twin), green on main, red on the head: ruling 101, fixed in this round. |
| none | No Spark answer exists: the analyzer's rule list of the Spark door. |

## 4. The 86 rows

Main is green on every row and the head is red on every row, by construction of the lists.

| # | Kind | Test or cell | Carried commit (first red) | Group (ruling 100 / 101) | Oracle or Q6 | Main | Head before |
|---|---|---|---|---|---|---|---|
| 1 | rust | `an_update_with_no_where_refuses_a_value_narrowed_from_nanoseconds` | `7dfb04c3` | R-017 pin: no-WHERE UPDATE beside a NULL | O6 | green | red |
| 2 | rust | `extension::tests::analyzer_configuration_seats_hof_preparation_and_float_stringify_before_type_coercion` | `203f9821` | analyzer-list pin | none | green | red |
| 3 | rust | `pins_report::a_warm_baseline_run_records_no_metadata_cache_hits` | `ca385f17` | read-perf bench pins (1.05x gate) | O1 | green | red |
| 4 | rust | `pins_report::a_warm_sample_hits_the_footer_cache_and_a_cold_one_misses_it` | `62089e59` | read-perf bench pins (1.05x gate) | O1 | green | red |
| 5 | rust | `pins_report::every_measured_sample_reports_its_evictions` | `ca385f17` | read-perf bench pins (1.05x gate) | O1 | green | red |
| 6 | rust | `q3_and_q7_reach_the_scan_with_the_same_rows` | `62089e59` | read-perf bench pins (1.05x gate) | O1 | green | red |
| 7 | rust | `setup_writes_exactly_n_files_and_every_mode_runs_on_them` | `62089e59` | read-perf bench pins (1.05x gate) | O1 | green | red |
| 8 | rust | `tests::ltz_store::int_into_timestamp_refuses_on_every_door` | `62089e59` | the three errors | O1 | green | red |
| 9 | rust | `tests::ntz_store::by_name_stores_the_ltz_session_wall` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 10 | rust | `tests::ntz_store::column_list_overwrite_stores_the_ltz_session_wall` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 11 | rust | `tests::ntz_store::date_stores_the_session_midnight_through_every_door` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 12 | rust | `tests::ntz_store::dynamic_overwrite_stores_the_ltz_session_wall` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 13 | rust | `tests::ntz_store::merge_insert_star_stores_the_ltz_session_wall` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 14 | rust | `tests::ntz_store::merge_insert_star_stores_the_ntz_session_instant` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 15 | rust | `tests::ntz_store::merge_insert_stores_the_ltz_session_wall` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 16 | rust | `tests::ntz_store::merge_insert_stores_the_ntz_session_instant` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 17 | rust | `tests::ntz_store::merge_update_star_stores_the_ltz_session_wall` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 18 | rust | `tests::ntz_store::merge_update_star_stores_the_ntz_session_instant` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 19 | rust | `tests::ntz_store::merge_update_stores_the_ltz_session_wall` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 20 | rust | `tests::ntz_store::merge_update_stores_the_ntz_session_instant` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 21 | rust | `tests::ntz_store::overwrite_by_name_stores_the_ltz_session_wall` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 22 | rust | `tests::ntz_store::overwrite_partition_stores_the_ltz_session_wall` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 23 | rust | `tests::ntz_store::overwrite_stores_the_ltz_session_wall` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 24 | rust | `tests::ntz_values_mix::an_expression_cell_beside_a_date_keeps_the_timestamp_type` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 25 | rust | `tests::ntz_values_mix::an_expression_cell_beside_a_date_stores_sparks_walls` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 26 | rust | `tests::ntz_values_mix::a_written_cast_as_timestamp_beside_null_stays_timestamp` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 27 | rust | `tests::ntz_values_mix::a_written_cast_as_timestamp_stores_sparks_walls` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 28 | rust | `tests::ntz_values_mix::a_written_cast_as_timestamp_types_the_column_timestamp` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 29 | rust | `tests::ntz_values_mix::by_name_stores_sparks_walls` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 30 | rust | `tests::ntz_values_mix::date_ntz_merge_insert_star_stores_sparks_walls` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 31 | rust | `tests::ntz_values_mix::date_timestamp_mixes_keep_their_timestamp_type` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 32 | rust | `tests::ntz_values_mix::insert_select_stores_sparks_walls` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 33 | rust | `tests::ntz_values_mix::insert_values_stores_sparks_walls` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 34 | rust | `tests::ntz_values_mix::merge_insert_star_stores_sparks_walls` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 35 | rust | `tests::ntz_values_mix::merge_insert_stores_sparks_walls` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 36 | rust | `tests::ntz_values_mix::merge_update_star_stores_sparks_walls` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 37 | rust | `tests::ntz_values_mix::merge_update_stores_sparks_walls` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 38 | rust | `tests::ntz_values_mix::mixed_values_type_the_column_timestamp_through_the_session_zone` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 39 | rust | `tests::ntz_values_mix::overwrite_by_name_stores_sparks_walls` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 40 | rust | `tests::ntz_values_mix::overwrite_select_stores_sparks_walls` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 41 | rust | `tests::ntz_values_mix::overwrite_values_stores_sparks_walls` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 42 | rust | `tests::ntz_values_mix::unmixed_values_keep_their_timestamp_type` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 43 | rust | `tests::plan_partitioning::plan_byte_ratio_is_measured_from_parquet_footers` | `7dfb04c3` | Spark-door test harness without the planner | O5 | green | red |
| 44 | rust | `tests::session_write_conf::a_session_conf_keeps_the_values_list_typing` | `7dfb04c3` | Spark-door test harness without the planner | O5 | green | red |
| 45 | rust | `tests::u9_timestamp_ltz::a_timestamp_ltz_typed_literal_is_a_session_zone_instant` | `7dfb04c3` | Spark-door test harness without the planner | O5 | green | red |
| 46 | rust | `tests::v3_timestamp_ns_door::cast_ns_columns_as_timestamp_floor_to_microsecond_instants` | `7dfb04c3` | Spark-door test harness without the planner | O5 | green | red |
| 47 | rust | `tests::v3_timestamp_ns_door::explain_lowers_ns_casts` | `7dfb04c3` | Spark-door test harness without the planner | O5 | green | red |
| 48 | rust | `tests::v3_timestamp_ns_door::insert_values_widens_timestamp_literals_and_strings` | `7dfb04c3` | Spark-door test harness without the planner | O5 | green | red |
| 49 | rust | `tests::v3_timestamp_ns_door::ns_string_rendering_and_predicates_keep_nanoseconds` | `7dfb04c3` | Spark-door test harness without the planner | O5 | green | red |
| 50 | rust | `tests::v3_timestamp_ns_door::string_casts_keep_nine_digits_and_the_types` | `7dfb04c3` | Spark-door test harness without the planner | O5 | green | red |
| 51 | rust | `tests::v3_timestamp_ns_door::timestamp_typed_values_floor_to_microseconds_and_strings_keep_nine_digits` | `7dfb04c3` | Spark-door test harness without the planner | O5 | green | red |
| 52 | facade | `test_fnp_6d_followup_1.py::test_construct_agg_refuses_timestamp_without_fixture_cell` | `62089e59` | TIMESTAMP_LTZ refusal texts | O2 | green | red |
| 53 | facade | `test_fnp_degrees_rust_1.py::test_refusal_matches_oracle[degrees-timestamp-sql]` | `62089e59` | TIMESTAMP_LTZ refusal texts | O2 | green | red |
| 54 | facade | `test_fnp_degrees_rust_1.py::test_refusal_matches_oracle[radians-timestamp-sql]` | `62089e59` | TIMESTAMP_LTZ refusal texts | O2 | green | red |
| 55 | facade | `test_fnp_degrees_rust_1.py::test_refusal_sql_nonansi_matches_oracle[timestamp-degrees]` | `62089e59` | TIMESTAMP_LTZ refusal texts | O2 | green | red |
| 56 | facade | `test_fnp_degrees_rust_1.py::test_refusal_sql_nonansi_matches_oracle[timestamp-radians]` | `62089e59` | TIMESTAMP_LTZ refusal texts | O2 | green | red |
| 57 | facade | `test_ice_tstzns_wall_1.py::test_every_door_stores_inserts_instant_in_timestamptz_ns[America/New_York-update_literal]` | `62089e59` | timestamptz_ns literal UPDATE (R-010) | R-010 | green | red |
| 58 | facade | `test_ice_tstzns_wall_1.py::test_every_door_stores_inserts_instant_in_timestamptz_ns[Asia/Kathmandu-update_literal]` | `62089e59` | timestamptz_ns literal UPDATE (R-010) | R-010 | green | red |
| 59 | facade | `test_ice_tstzns_wall_1.py::test_every_door_stores_inserts_instant_in_timestamptz_ns[Asia/Kolkata-update_literal]` | `62089e59` | timestamptz_ns literal UPDATE (R-010) | R-010 | green | red |
| 60 | facade | `test_ice_tstzns_wall_1.py::test_every_door_stores_inserts_instant_in_timestamptz_ns[Australia/Lord_Howe-update_literal]` | `62089e59` | timestamptz_ns literal UPDATE (R-010) | R-010 | green | red |
| 61 | facade | `test_ice_tstzns_wall_1.py::test_every_door_stores_inserts_instant_in_timestamptz_ns[UTC-update_literal]` | `62089e59` | timestamptz_ns literal UPDATE (R-010) | R-010 | green | red |
| 62 | facade | `test_ice_tt_resolve_1.py::test_native_expr_cells[TT-SQL-EXPR-CAST]` | `62089e59` | `repark.sql()` instant | Q6 | green | red |
| 63 | facade | `test_ice_tt_resolve_1.py::test_native_expr_cells[TT-SQL-FST-EXPR]` | `62089e59` | `repark.sql()` instant | Q6 | green | red |
| 64 | facade | `test_ice_tt_resolve_1.py::test_native_expr_cells[TT-SQL-LIT-TS]` | `62089e59` | `repark.sql()` instant | Q6 | green | red |
| 65 | facade | `test_ice_tt_resolve_1.py::test_native_expr_cells[TT-SQL-SUBQ-ERR]` | `62089e59` | `repark.sql()` instant | Q6 | green | red |
| 66 | facade | `test_ntz_1.py::test_nested_cast_target_keeps_the_r4_refusal[SELECT CAST(array('2024-01-01 00:00:00') AS ARRAY<TIMESTAMP_NTZ>) AS v]` | `62089e59` | nested NTZ cast | O4 | green | red |
| 67 | facade | `test_ntz_1.py::test_nested_cast_target_keeps_the_r4_refusal[SELECT CAST(named_struct('a', '2024-01-01') AS STRUCT<a: TIMESTAMP_NTZ>) AS v]` | `62089e59` | nested NTZ cast | O4 | green | red |
| 68 | facade | `test_ntz_9_verify.py::test_values_cells_store_sparks_session_wall` | `62089e59` | mixed VALUES | O3 | green | red |
| 69 | facade | `test_ntz_store_doors_1.py::test_mixed_values_column_stores_sparks_value[ny/ntz/ins_sel]` | `62089e59` | mixed VALUES | O3 | green | red |
| 70 | facade | `test_ntz_store_doors_1.py::test_mixed_values_column_stores_sparks_value[ny/ntz/ins_values]` | `62089e59` | mixed VALUES | O3 | green | red |
| 71 | facade | `test_ntz_store_doors_1.py::test_mixed_values_column_types_timestamp[kol]` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 72 | facade | `test_ntz_store_doors_1.py::test_mixed_values_column_types_timestamp[lhi]` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 73 | facade | `test_ntz_store_doors_1.py::test_mixed_values_column_types_timestamp[ny]` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 74 | facade | `test_ntz_store_doors_1.py::test_mixed_values_column_types_timestamp[utc]` | `7dfb04c3` | mixed VALUES | O3 | green | red |
| 75 | facade | `test_nullability_2.py::test_cast_nullability_native_door_keeps_datafusion` | `62089e59` | `repark.sql()` instant | Q6 | green | red |
| 76 | facade | `test_stamp_2_r5p6_1.py::test_coalesce_cast_to_a_datetime_keeps_the_sql_replan` | `62089e59` | the three errors | O1 | green | red |
| 77 | facade | `test_store_ts_doors_2.py::test_fold_date_sibling_time_string_stores_with_midnight_divergence` | `7dfb04c3` | the three errors | O1 | green | red |
| 78 | facade | `test_store_ts_doors_2.py::test_store_assignment_cells_replay_spark` | `7dfb04c3` | the three errors | O1 | green | red |
| 79 | cell101 | `position\|array_append\|ntz\|tzns\|America/New_York` | `ca385f17` | ruling 101 (green on main, red on head) | T | green | red |
| 80 | cell101 | `position\|array_append\|ntz\|tzns\|Asia/Kolkata` | `ca385f17` | ruling 101 (green on main, red on head) | T | green | red |
| 81 | cell101 | `position\|array_prepend\|ntz\|tzns\|America/New_York` | `ca385f17` | ruling 101 (green on main, red on head) | T | green | red |
| 82 | cell101 | `position\|array_prepend\|ntz\|tzns\|Asia/Kolkata` | `ca385f17` | ruling 101 (green on main, red on head) | T | green | red |
| 83 | cell101 | `repro\|v2_array_except\|tzns\|America/New_York` | `62089e59` | ruling 101 (green on main, red on head) | T | green | red |
| 84 | cell101 | `repro\|v2_array_except\|tzns\|UTC` | `62089e59` | ruling 101 (green on main, red on head) | T | green | red |
| 85 | cell101 | `repro\|v2_array_intersect\|tzns\|America/New_York` | `62089e59` | ruling 101 (green on main, red on head) | T | green | red |
| 86 | cell101 | `repro\|v2_array_intersect\|tzns\|UTC` | `62089e59` | ruling 101 (green on main, red on head) | T | green | red |

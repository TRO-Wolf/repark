# Fold 1 measurements — ICE-TSNS-FRAME-DOOR-1 (2026-10-13)

## The verify stage's catalogue, re-run on the fold

The verify stage's recorder (1388 cells: its frame catalogue, each frame's schemas, a UTC and
an America/New_York session, default and `TIMESTAMP_NTZ` sessions) was run on the fold's
source and compared with that stage's recording of main `8c51fbc4`.

| Question | `c4efd79f` | fold 1 |
|---|---|---|
| cells where main answered and the unit raises | 87 | **0** |
| the 520 cells with no nanosecond column that differ from main | 12 | **12**, the named move |
| the 204 nanosecond cells under `c bigint`, `c string`, `c date` that differ from main | 132 (`bigint` 64, `string` 60, `date` 8) | **0** |

The 12: `c timestamp` over a non-UTC aware microsecond column, default sessions, both zones:
pandas New York, pandas `+05:30`, `ArrowDtype` Asia/Tokyo, `ArrowDtype` `+05:30`, polars New
York, chunked New York. Fields that differ: the Arrow type label, so the schema string and
the IPC bytes. Values, `df.schema`, `collect()` and both `explain` texts are identical.

Nanosecond cells under no schema, `c timestamp` and `c timestamp_ntz`: 236 identical to main,
214 answer on both and differ (the unit: microseconds where main carried nanoseconds), 194
answer where main raised (176 `ArrowInvalid … would lose data` under `c timestamp_ntz`, 18
pandas or Python overflow errors at the int64 bounds).

## Mutants

Run by the executor against `test_ice_tsns_frame_door_1.py` (244 tests green unmutated).
`E-` are the executor's, `V-` the verify stage's re-expressed on the tick kernel, `F-` new
with the fold. The verify stage's `V-o10` (an unsafe cast after `floor_temporal`) has no
counterpart: the kernel has no narrowing cast left.

| Mutant | Result | Killed by (failures per test, largest four) |
|---|---|---|
| E-m1 step returns the column unchanged | 110 failed, 134 passed | nanosecond_column_equals_its_microsecond_twin 28, instants_around_a_zone_transition_floor_on_the_ticks 24, the_lowest_and_highest_nanoseconds_floor 12, pandas_frame_create_doors_make_microsecond_tables 12 |
| E-m2 cast(safe=False) in place of the floor | 46 failed, 198 passed | the_lowest_and_highest_nanoseconds_floor 16, nanosecond_column_equals_its_microsecond_twin 12, aware_nanosecond_values_equal_the_spark_oracle 4, declared_timestamp_over_nanoseconds_is_unchanged 4 |
| E-m3 aware columns skipped | 68 failed, 176 passed | instants_around_a_zone_transition_floor_on_the_ticks 24, nanosecond_column_equals_its_microsecond_twin 16, aware_nanosecond_values_equal_the_spark_oracle 8, pandas_frame_create_doors_make_microsecond_tables 6 |
| E-m4a zone dropped | 64 failed, 180 passed | instants_around_a_zone_transition_floor_on_the_ticks 18, aware_nanosecond_values_equal_the_spark_oracle 12, nanosecond_column_equals_its_microsecond_twin 12, the_lowest_and_highest_nanoseconds_floor 6 |
| E-m4b aware relabelled to UTC in the step | 14 failed, 230 passed | instants_around_a_zone_transition_floor_on_the_ticks 12, nanosecond_column_equals_its_microsecond_twin 2 |
| E-m5 / V-o3 step only without a declared type | 40 failed, 204 passed | nanosecond_column_equals_its_microsecond_twin 14, instants_around_a_zone_transition_floor_on_the_ticks 12, declared_timestamp_ntz_over_nanoseconds_answers_microseconds 6, the_lowest_and_highest_nanoseconds_floor 4 |
| E-m6a also floors timestamp[ms] | 2 failed, 242 passed | columns_that_are_not_nanosecond_timestamps_are_untouched 2 |
| E-m6b / V-o9 descends into lists and structs | 4 failed, 240 passed | nested_arrowdtype_nanoseconds_are_still_carried 4 |
| E-m7 relabel undone | 20 failed, 224 passed | instants_around_a_zone_transition_floor_on_the_ticks 12, declared_timestamp_over_an_aware_microsecond_column_answers_utc 6, declared_timestamp_over_nanoseconds_is_unchanged 2 |
| V-o1 round, not floor | 154 failed, 90 passed | nanosecond_column_equals_its_microsecond_twin 42, instants_around_a_zone_transition_floor_on_the_ticks 36, aware_nanosecond_values_equal_the_spark_oracle 16, the_lowest_and_highest_nanoseconds_floor 16 |
| V-o2 ceil, not floor | 156 failed, 88 passed | nanosecond_column_equals_its_microsecond_twin 42, instants_around_a_zone_transition_floor_on_the_ticks 36, aware_nanosecond_values_equal_the_spark_oracle 16, the_lowest_and_highest_nanoseconds_floor 16 |
| V-o4 step only with a declared type | 70 failed, 174 passed | nanosecond_column_equals_its_microsecond_twin 14, instants_around_a_zone_transition_floor_on_the_ticks 12, pandas_frame_create_doors_make_microsecond_tables 12, aware_nanosecond_values_equal_the_spark_oracle 8 |
| V-o5 floor to the millisecond | 156 failed, 88 passed | nanosecond_column_equals_its_microsecond_twin 42, instants_around_a_zone_transition_floor_on_the_ticks 36, aware_nanosecond_values_equal_the_spark_oracle 16, the_lowest_and_highest_nanoseconds_floor 16 |
| V-o6 relabel keeps the wall, not the instant | 22 failed, 222 passed | instants_around_a_zone_transition_floor_on_the_ticks 12, declared_timestamp_over_an_aware_microsecond_column_answers_utc 6, aware_nanosecond_values_equal_the_spark_oracle 2, declared_timestamp_over_nanoseconds_is_unchanged 2 |
| V-o7 skip when the column has nulls | 50 failed, 194 passed | nanosecond_column_equals_its_microsecond_twin 16, pandas_frame_create_doors_make_microsecond_tables 12, a_categorical_of_nanoseconds_arrives_as_microseconds 8, aware_nanosecond_values_equal_the_spark_oracle 4 |
| V-o8 skip when the column has no nulls | 60 failed, 184 passed | instants_around_a_zone_transition_floor_on_the_ticks 24, nanosecond_column_equals_its_microsecond_twin 12, the_lowest_and_highest_nanoseconds_floor 12, aware_nanosecond_values_equal_the_spark_oracle 4 |
| V-o11 relabel also recasts a UTC column (equivalent) | 244 passed | none: equivalent |
| V-o12 relabel only when the zone is New York | 12 failed, 232 passed | instants_around_a_zone_transition_floor_on_the_ticks 8, declared_timestamp_over_an_aware_microsecond_column_answers_utc 4 |
| V-o13 truncate, no correction below zero | 46 failed, 198 passed | the_lowest_and_highest_nanoseconds_floor 16, nanosecond_column_equals_its_microsecond_twin 12, aware_nanosecond_values_equal_the_spark_oracle 4, declared_timestamp_over_nanoseconds_is_unchanged 4 |
| F-a floor through floor_temporal (the c4efd79f kernel) | 54 failed, 190 passed | instants_around_a_zone_transition_floor_on_the_ticks 36, the_lowest_and_highest_nanoseconds_floor 16, the_int64_minimum_floors_without_wrapping 2 |
| F-b step before the dictionary decode | 8 failed, 236 passed | a_categorical_of_nanoseconds_arrives_as_microseconds 8 |
| F-c step under every declared type | 24 failed, 220 passed | a_declared_type_that_is_not_a_timestamp_keeps_main 24 |
| F-d step skipped under a declared naive timestamp | 40 failed, 204 passed | nanosecond_column_equals_its_microsecond_twin 14, instants_around_a_zone_transition_floor_on_the_ticks 12, declared_timestamp_ntz_over_nanoseconds_answers_microseconds 6, the_lowest_and_highest_nanoseconds_floor 4 |
| F-e correction applied to non-negative ticks only by sign test | 2 failed, 242 passed | the_int64_minimum_floors_without_wrapping 2 |
| F-f zone transitions: floor on local wall then re-attach | 48 failed, 196 passed | instants_around_a_zone_transition_floor_on_the_ticks 36, nanosecond_column_equals_its_microsecond_twin 6, aware_nanosecond_values_equal_the_spark_oracle 4, declared_timestamp_over_nanoseconds_is_unchanged 2 |

`V-o11` is equivalent: a cast of a UTC column to its own type gives the same type and
values; only object identity differs.

## Appends and the named move's surfaces, re-run on the fold

The verify stage's append probe: a v3 table `(ts timestamp_ns, tz timestamptz_ns)`, four
doors (`writeTo().append()`, `insertInto`, `INSERT … SELECT` from a temp view, `saveAsTable`
in append mode), pandas and polars, two session zones: 16 of 16 cells store the microsecond
floor times 1000 (`[1767323045123456000, -1000, -1000, -2000, -1000000000000]`) with no
error and no warning; main stored all nine digits in 16 of 16.

`createDataFrame(<datetime64[us, America/New_York]>, schema="c timestamp")`: `to_arrow`
`timestamp[us, tz=UTC]` (main `tz=America/New_York`); `toPandas()` dtype
`datetime64[us, UTC]` (main `datetime64[us, America/New_York]`); `show()` prints
`2026-01-02 03:04:05.123456+00:00` (main `2026-01-01 22:04:05.123456-05:00`); `collect()`,
`df.schema` and `explain` are identical. One instant throughout.

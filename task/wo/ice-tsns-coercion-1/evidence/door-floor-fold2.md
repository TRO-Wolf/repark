# Door floor, slice 1 fold 2 follow-up 1 (2026-10-13)

The floor of the shared Python output check, `floor_temporal` replaced by integer
arithmetic on the int64 ticks. Mutants were written into
`udf_schema._floor_to_declared_microseconds` in the working tree, the fold's facade file was
run, and the file was restored.

| Mutant | Result |
|---|---|
| truncating divide in place of the floor (the minus-one term removed) | 7 of 11 tests fail |
| zone label dropped (viewed as naive `timestamp[us]`) | 5 of 11 tests fail |
| restored | 11 pass |

Cost per million rows, median of 15 calls over one million random ticks inside June 2025:

| Column | `floor_temporal` then cast | integer floor |
|---|---|---|
| naive `timestamp[ns]` | 15.2 ms | 15.3 ms |
| `timestamp[ns, tz=America/New_York]` | 111.1 ms | 15.3 ms |

A first sample drawn from 1970 to 2027 could not be timed on the old form at all: it raised
`Local time is ambiguous` on a 2018 fall-back instant.

Run log:

```
== GREEN no mutant
11 passed in 1.97s
== MUTANT truncating divide
 1 file changed, 8 insertions(+), 3 deletions(-)
FAILED python/repark/tests/test_ice_tsns_coercion_1_slice1_fold2.py::test_nanosecond_cells_equal_the_microsecond_twin
FAILED python/repark/tests/test_ice_tsns_coercion_1_slice1_fold2.py::test_ticks_differ_from_main_only_where_main_narrowed_the_site
FAILED python/repark/tests/test_ice_tsns_coercion_1_slice1_fold2.py::test_map_in_arrow_floors_nanoseconds_to_the_declared_microseconds
FAILED python/repark/tests/test_ice_tsns_coercion_1_slice1_fold2.py::test_map_in_arrow_floors_nanoseconds_inside_lists
FAILED python/repark/tests/test_ice_tsns_coercion_1_slice1_fold2.py::test_pandas_doors_floor_nanoseconds_to_the_declared_microseconds
FAILED python/repark/tests/test_ice_tsns_coercion_1_slice1_fold2.py::test_shared_check_floors_integer_ticks_and_keeps_the_zone_label
FAILED python/repark/tests/test_ice_tsns_coercion_1_slice1_fold2.py::test_doors_floor_daylight_saving_and_range_edge_instants
7 failed, 4 passed in 1.51s
== MUTANT zone label dropped
 1 file changed, 8 insertions(+), 3 deletions(-)
                f"mapInArrow failed opening upstream Arrow stream: {error}"
                f"mapInArrow failed opening upstream Arrow stream: {error}"
FAILED python/repark/tests/test_ice_tsns_coercion_1_slice1_fold2.py::test_cells_main_answered_still_answer
FAILED python/repark/tests/test_ice_tsns_coercion_1_slice1_fold2.py::test_nanosecond_cells_equal_the_microsecond_twin
FAILED python/repark/tests/test_ice_tsns_coercion_1_slice1_fold2.py::test_ticks_differ_from_main_only_where_main_narrowed_the_site
FAILED python/repark/tests/test_ice_tsns_coercion_1_slice1_fold2.py::test_map_in_arrow_floors_nanoseconds_inside_lists
FAILED python/repark/tests/test_ice_tsns_coercion_1_slice1_fold2.py::test_shared_check_floors_integer_ticks_and_keeps_the_zone_label
5 failed, 6 passed in 1.67s
== restored
11 passed in 1.43s
{'naive': 15.26, 'new_york': 15.26}
```

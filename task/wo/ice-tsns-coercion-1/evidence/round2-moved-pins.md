# Round 2 moved pins census (2026-10-12)

Census runs on head `fc80dbe6` (round-2 rewrite committed), release module.

## Four brief-named suites: zero failures

| Suite | Result |
|---|---|
| `python/repark/tests/test_ice_tsns_coercion_1.py` | 3 passed |
| `python/repark/tests/test_ice_tsns_sql_1.py` | 43 passed |
| `python/repark/tests/test_ice_tsns_merge_wall_1.py` + `test_ice_tstzns_wall_1.py` | 487 passed |

No test on main that expects microseconds for a card spelling moved. No HALT trigger.

## One round-1 pin moves: `test_twelve_statements_equal_the_recorded_statement_role`

File: `python/repark/tests/test_ice_tsns_coercion_1_slice1.py`. The round-1 pin asserts the
statement role equals main's recorded `statement` role ("this round changed no stored value").
Round 2 rewrites all 12 of its cells (forms coalesce/array/case, sources ns/tzns_utc, doors
insert_select in UTC and df_append in America/New_York, sibling untyped_null), so the stored
value and the read move from microseconds to nanoseconds.

Old expectation: stored outcome and read equal the fixture's `statement` role.
New value: stored outcome and read equal the fixture's `twin` role, verified cell by cell on
this head (12/12 stored==twin, 12/12 read==twin; the same cells also pass inside
`test_pin6_remaining_cells_equal_twin` and `test_pin6_reads_equal_twin`).

| Cell | Old (main statement) | New (head) | Equals twin |
|---|---|---|---|
| UTC ts_ns ns coalesce/array/case insert_select | us ticks | ns ticks | yes (6/6) |
| UTC tz_ns tzns_utc coalesce/array/case insert_select | us ticks | ns ticks | yes (6/6 stored; reads twin-equal) |
| America/New_York ts_ns ns coalesce/array/case df_append | us ticks | ns ticks | yes (6/6) |
| America/New_York tz_ns tzns_utc coalesce/array/case df_append | us ticks | ns ticks | yes (6/6) |

Disposition: updated to the twin's value in the item-3 commit (renamed to
`test_twelve_statements_equal_the_recorded_twin_role`), per the brief's twin-explained rule.

## Integration targets (2026-10-12, CI follow-up)

Census on head `50dfadc1`, `--no-fail-fast`, in the wrapper:

| Target | Result |
|---|---|
| `-p repark-spark --tests` | 1 failure: `timestamp_ns_nested_shapes::an_update_with_no_where_refuses_a_value_narrowed_from_nanoseconds`; every other target green |
| `-p repark-sql --tests` | all green |
| `-p repark-iceberg --tests` | all green (1000) |
| `-p repark-functions --tests` | all green (964) |
| `-p repark-python --tests` | all green (159 + 25) |

Failing test: `crates/repark-spark/tests/timestamp_ns_nested_shapes.rs`
`an_update_with_no_where_refuses_a_value_narrowed_from_nanoseconds`.
Statement shape: `UPDATE ice.ns.u SET v = <spelling>` with no `WHERE`, four
spellings (`CASE` NULL in THEN, `CASE` NULL in ELSE, `if`, `array[…][0]`) over
sources `c timestamp_ns` and `z timestamptz_ns` into `v timestamp_ns`.
Main's expectation: all eight refuse
`[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] … narrowed from nanoseconds
to microseconds`, storing nothing.
Head's result: all eight store. The `c` legs store `[1767323045123456789,
1772937000000000001]`; the `z` legs store `[1767305045123456789,
1772940600000000001]`. The single-`timestamptz_ns`-operand site has one common
zone, so the round-2 rule wraps its NULL in `__repark_cast_timestamptz_ns__`
and the store keeps the instant, exactly like the test's own later
`SET v = z` leg.

Twin check on the baseline module (`/tmp/xcoerce0/.venv`): the four plain
statements refuse there with the same class (main's expectation reproduced);
the four hand-written naive twins (`NULL` wrapped in
`__repark_cast_timestamp_ns__`) store `[1767323045123456789,
1772937000000000001]` 4/4; the four hand-written zoned twins (`NULL` wrapped
in `__repark_cast_timestamptz_ns__`) store `[1767305045123456789,
1772940600000000001]` 4/4. Head equals twin on all eight. No HALT trigger.

Disposition: the test is renamed to
`an_update_with_no_where_stores_a_nanosecond_value_built_from_an_untyped_null`
and asserts the stored walls per leg; no spelling still refuses, so no refusal
text is kept. The rest of the test (typed NULL, `SET v = c`, `SET v = z`, the
`if`-literal leg) is untouched.

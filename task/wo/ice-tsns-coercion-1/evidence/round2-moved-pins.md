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

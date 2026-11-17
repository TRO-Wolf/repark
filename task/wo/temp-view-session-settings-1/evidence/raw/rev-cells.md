# Revision cells: Spark 4.1.2 against main `6f80793b`

## Counts by kind and verdict

| kind | verdict | cells |
|---|---|---|
| cache | EQUAL | 1 |
| cache | REPARK_RAISES | 1 |
| cache | VALUE | 5 |
| defcat | EQUAL | 3 |
| derive | BOTH_RAISE | 7 |
| derive | EQUAL | 7 |
| derive | REPARK_ANSWERS | 11 |
| derive | VALUE | 11 |
| direct | BOTH_RAISE | 3 |
| direct | BOTH_RAISE_OTHER | 1 |
| direct | EQUAL | 17 |
| direct | REPARK_RAISES | 1 |
| direct_gap | REPARK_RAISES | 3 |
| door | REPARK_ANSWERS | 1 |
| door | VALUE | 1 |
| filter | BOTH_RAISE | 1 |
| filter | EQUAL | 21 |
| filter | REPARK_ANSWERS | 1 |
| filter | REPARK_RAISES | 1 |
| held_write | EQUAL | 14 |
| held_write | REPARK_ANSWERS | 1 |
| held_write | REPARK_RAISES | 1 |
| held_write | VALUE | 6 |
| now_fn | EQUAL | 4 |
| now_fn | VALUE | 8 |
| rename | BOTH_RAISE_OTHER | 1 |
| rename | REPARK_ANSWERS | 1 |
| rename | REPARK_RAISES | 2 |
| rename | VALUE | 1 |
| shape | BOTH_RAISE | 2 |
| shape | EQUAL | 2 |
| shape | REPARK_ANSWERS | 4 |
| shape | REPARK_RAISES | 2 |
| shape | VALUE | 10 |

## Every cell

| cell | verdict | Spark | RePark |
|---|---|---|---|
| `direct/add/true` | BOTH_RAISE | RAISE ARITHMETIC_OVERFLOW | RAISE ARITHMETIC_OVERFLOW |
| `direct/add/false` | EQUAL | -2147483648 | -2147483648 |
| `direct/div/true` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| `direct/div/false` | EQUAL | null | null |
| `direct/hour/UTC` | EQUAL | 2 | 2 |
| `direct/hour/America/Los_Angeles` | EQUAL | 18 | 18 |
| `direct/str/UTC` | EQUAL | 2024-01-01 02:30:00 | 2024-01-01 02:30:00 |
| `direct/str/America/Los_Angeles` | EQUAL | 2023-12-31 18:30:00 | 2023-12-31 18:30:00 |
| `direct/date/UTC` | EQUAL | 2024-01-01 | 2024-01-01 |
| `direct/date/America/Los_Angeles` | EQUAL | 2023-12-31 | 2023-12-31 |
| `direct/lit/UTC` | EQUAL | 1704076200 | 1704076200 |
| `direct/lit/America/Los_Angeles` | EQUAL | 1704105000 | 1704105000 |
| `direct/fdiv/true` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| `direct/fdiv/false` | EQUAL | 0\|null;2\|5.0 | 0\|null;2\|5.0 |
| `direct/fcast/true` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to value o |
| `direct/fcast/false` | REPARK_RAISES | 7\|7;abc\|null | RAISE Arrow error: Cast error: Cannot cast string 'abc' to value o |
| `direct/lts/UTC` | EQUAL | 21 | 21 |
| `direct/lts/America/Los_Angeles` | EQUAL | 13 | 13 |
| `direct/cdate/UTC` | EQUAL | 2026-11-17 | 2026-11-17 |
| `direct/cdate/America/Los_Angeles` | EQUAL | 2026-11-17 | 2026-11-17 |
| `direct/now/UTC` | EQUAL | 21 | 21 |
| `direct/now/America/Los_Angeles` | EQUAL | 13 | 13 |
| `filter_below/sql/fdiv/on_off` | EQUAL | 5.0 | 5.0 |
| `filter_below/sql/fdiv/off_on` | EQUAL | 5.0 | 5.0 |
| `filter_below/sql/fdiv/on_on` | EQUAL | 5.0 | 5.0 |
| `filter_below/sql/fcast/on_off` | EQUAL | 7 | 7 |
| `filter_below/sql/fcast/off_on` | EQUAL | 7 | 7 |
| `filter_below/sql/fcast/on_on` | EQUAL | 7 | 7 |
| `filter_on_m/sql/fdiv/on_off` | REPARK_ANSWERS | RAISE DIVIDE_BY_ZERO | 5.0 |
| `filter_on_m/sql/fdiv/off_on` | REPARK_RAISES | 5.0 | RAISE DIVIDE_BY_ZERO |
| `limit/sql/fdiv/on_off` | EQUAL | 5.0 | 5.0 |
| `limit/sql/fdiv/off_on` | EQUAL | 5.0 | 5.0 |
| `filter_sensitive/sql/fdiv/on_off` | EQUAL | 5.0 | 5.0 |
| `filter_sensitive/sql/fdiv/off_on` | EQUAL | 5.0 | 5.0 |
| `filter_below/df/fdiv/on_off` | EQUAL | 5.0 | 5.0 |
| `filter_below/df/fdiv/off_on` | EQUAL | 5.0 | 5.0 |
| `filter_below/df/fdiv/on_on` | EQUAL | 5.0 | 5.0 |
| `filter_below/df/fcast/on_off` | EQUAL | 7 | 7 |
| `filter_below/df/fcast/off_on` | EQUAL | 7 | 7 |
| `filter_below/df/fcast/on_on` | EQUAL | 7 | 7 |
| `filter_on_m/df/fdiv/on_off` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| `filter_on_m/df/fdiv/off_on` | EQUAL | 5.0 | 5.0 |
| `limit/df/fdiv/on_off` | EQUAL | 5.0 | 5.0 |
| `limit/df/fdiv/off_on` | EQUAL | 5.0 | 5.0 |
| `filter_sensitive/df/fdiv/on_off` | EQUAL | 5.0 | 5.0 |
| `filter_sensitive/df/fdiv/off_on` | EQUAL | 5.0 | 5.0 |
| `cache/sql_then_set` | VALUE | before=2;after=2;cached=true | before=2;after=18;cached=true |
| `cache/df_then_set` | VALUE | after=2;cached=true | after=18;cached=true |
| `cache/lazy_then_set` | VALUE | after=2 | after=18 |
| `cache/catalog_then_set` | VALUE | after=2 | after=18 |
| `cache/set_between_zone` | VALUE | after=2 | after=18 |
| `cache/set_between_off_on` | REPARK_RAISES | after=-2147483648 | RAISE ARITHMETIC_OVERFLOW |
| `cache/held_then_set` | EQUAL | after=2 | after=2 |
| `derive/alias/add/on_off` | BOTH_RAISE | RAISE ARITHMETIC_OVERFLOW | RAISE ARITHMETIC_OVERFLOW |
| `derive/alias/hour/utc_la` | EQUAL | 2 | 2 |
| `derive/union/add/on_off` | BOTH_RAISE | RAISE ARITHMETIC_OVERFLOW | RAISE ARITHMETIC_OVERFLOW |
| `derive/union/hour/utc_la` | EQUAL | 2;2 | 2;2 |
| `derive/distinct/add/on_off` | BOTH_RAISE | RAISE ARITHMETIC_OVERFLOW | RAISE ARITHMETIC_OVERFLOW |
| `derive/distinct/hour/utc_la` | EQUAL | 2 | 2 |
| `derive/filter/add/on_off` | BOTH_RAISE | RAISE ARITHMETIC_OVERFLOW | RAISE ARITHMETIC_OVERFLOW |
| `derive/filter/hour/utc_la` | EQUAL | 2 | 2 |
| `derive/selectcol/add/on_off` | BOTH_RAISE | RAISE ARITHMETIC_OVERFLOW | RAISE ARITHMETIC_OVERFLOW |
| `derive/selectcol/hour/utc_la` | EQUAL | 2 | 2 |
| `derive/join/add/on_off` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | -2147483648 |
| `derive/join/hour/utc_la` | VALUE | 2 | 18 |
| `derive/crossjoin/add/on_off` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | -2147483648 |
| `derive/crossjoin/hour/utc_la` | VALUE | 2 | 18 |
| `derive/gagg/add/on_off` | BOTH_RAISE | RAISE ARITHMETIC_OVERFLOW | RAISE ARITHMETIC_OVERFLOW |
| `derive/gagg/hour/utc_la` | EQUAL | 2 | 2 |
| `derive/gagg_lit/add/on_off` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | -2147483648\|1 |
| `derive/gagg_lit/hour/utc_la` | VALUE | 2\|1 | 18\|1 |
| `derive/groupagg/add/on_off` | BOTH_RAISE | RAISE ARITHMETIC_OVERFLOW | RAISE ARITHMETIC_OVERFLOW |
| `derive/groupagg/hour/utc_la` | EQUAL | 2 | 2 |
| `derive/unpivot/add/on_off` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | -2147483648 |
| `derive/unpivot/hour/utc_la` | VALUE | 2 | 18 |
| `derive/selectexpr/add/on_off` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | -2147483648 |
| `derive/selectexpr/hour/utc_la` | VALUE | 2 | 18 |
| `derive/explode/add/on_off` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | -2147483648 |
| `derive/explode/hour/utc_la` | VALUE | 2 | 18 |
| `derive/intersect/add/on_off` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | -2147483648 |
| `derive/intersect/hour/utc_la` | VALUE | 2 | 18 |
| `derive/subtract/add/on_off` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | -2147483648 |
| `derive/subtract/hour/utc_la` | VALUE | 2 | 18 |
| `derive/cache/add/on_off` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | -2147483648 |
| `derive/cache/hour/utc_la` | VALUE | 2 | 18 |
| `derive/tempview_after/add/on_off` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | -2147483648 |
| `derive/tempview_after/hour/utc_la` | VALUE | 2 | 18 |
| `derive/registertemptable/add/on_off` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | -2147483648 |
| `derive/registertemptable/hour/utc_la` | VALUE | 2 | 18 |
| `door/registertemptable/add/on_off` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | -2147483648 |
| `door/registertemptable/hour/utc_la` | VALUE | 2 | 18 |
| `rename/view/div/on_off` | BOTH_RAISE_OTHER | RAISE DIVIDE_BY_ZERO | RAISE TABLE_OR_VIEW_NOT_FOUND |
| `rename/table/div/on_off` | REPARK_ANSWERS | RAISE DIVIDE_BY_ZERO | null |
| `rename/view/hour/utc_la` | REPARK_RAISES | 2 | RAISE TABLE_OR_VIEW_NOT_FOUND |
| `rename/table/hour/utc_la` | VALUE | 2 | 18 |
| `rename/view/control/same` | REPARK_RAISES | 1 | RAISE TABLE_OR_VIEW_NOT_FOUND |
| `alter_view_as/div/on_off` | REPARK_RAISES | null | RAISE Error during planning: ALTER VIEW <viewName> AS is not suppo |
| `alter_view_as/hour/utc_la` | REPARK_RAISES | 18 | RAISE Error during planning: ALTER VIEW <viewName> AS is not suppo |
| `cache_table_as_select/hour/same` | REPARK_RAISES | 2 | RAISE This feature is not implemented: Unsupported SQL statement:  |
| `now_fn/sql/lts/utc_la` | VALUE | 21 | 13 |
| `now_fn/sql/lts/la_utc` | VALUE | 13 | 21 |
| `now_fn/df/lts/utc_la` | VALUE | 21 | 13 |
| `now_fn/df/lts/la_utc` | VALUE | 13 | 21 |
| `now_fn/sql/cdate/utc_la` | EQUAL | 2026-11-17 | 2026-11-17 |
| `now_fn/sql/cdate/la_utc` | EQUAL | 2026-11-17 | 2026-11-17 |
| `now_fn/df/cdate/utc_la` | EQUAL | 2026-11-17 | 2026-11-17 |
| `now_fn/df/cdate/la_utc` | EQUAL | 2026-11-17 | 2026-11-17 |
| `now_fn/sql/now/utc_la` | VALUE | 21 | 13 |
| `now_fn/sql/now/la_utc` | VALUE | 13 | 21 |
| `now_fn/df/now/utc_la` | VALUE | 21 | 13 |
| `now_fn/df/now/la_utc` | VALUE | 13 | 21 |
| `replace/sql/div/on_off` | REPARK_RAISES | null | RAISE DIVIDE_BY_ZERO |
| `replace/sql/hour/utc_la` | VALUE | 18 | 2 |
| `self_join/sql/add/on_off` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | -2147483648\|-2147483648 |
| `self_join/sql/hour/utc_la` | VALUE | 2\|2 | 18\|18 |
| `reset/all/sql/div` | REPARK_RAISES | null {"after_reset": "true"} | RAISE DIVIDE_BY_ZERO |
| `reset/key/sql/hour` | VALUE | 18 {"after_reset": "UTC"} | 2 {"after_reset": "UTC"} |
| `conf_set/conf_then_sql/sql/div/on_off` | REPARK_ANSWERS | RAISE DIVIDE_BY_ZERO | null |
| `conf_set/sql_then_conf/sql/div/on_off` | REPARK_ANSWERS | RAISE DIVIDE_BY_ZERO | null |
| `conf_set/conf_then_sql/sql/hour/utc_la` | VALUE | 2 | 18 |
| `conf_set/sql_then_conf/sql/hour/utc_la` | VALUE | 2 | 18 |
| `replace/df/div/on_off` | EQUAL | null | null |
| `replace/df/hour/utc_la` | VALUE | 18 | 2 |
| `self_join/df/add/on_off` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | -2147483648\|-2147483648 |
| `self_join/df/hour/utc_la` | VALUE | 2\|2 | 18\|18 |
| `reset/all/df/div` | EQUAL | null {"after_reset": "true"} | null {"after_reset": "true"} |
| `reset/key/df/hour` | VALUE | 18 {"after_reset": "UTC"} | 2 {"after_reset": "UTC"} |
| `conf_set/conf_then_sql/df/div/on_off` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| `conf_set/sql_then_conf/df/div/on_off` | BOTH_RAISE | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| `conf_set/conf_then_sql/df/hour/utc_la` | VALUE | 2 | 18 |
| `conf_set/sql_then_conf/df/hour/utc_la` | VALUE | 2 | 18 |
| `defcat/direct` | EQUAL | lc | lc |
| `defcat/sql` | EQUAL | lc | lc |
| `defcat/df` | EQUAL | lc | lc |
| `held_write/writeto/hour/utc_la` | EQUAL | 18 | 18 |
| `held_write/writeto/hour/la_utc` | EQUAL | 2 | 2 |
| `held_write/writeto/str/utc_la` | EQUAL | 2023-12-31 18:30:00 | 2023-12-31 18:30:00 |
| `held_write/writeto/str/la_utc` | EQUAL | 2024-01-01 02:30:00 | 2024-01-01 02:30:00 |
| `held_write/writeto/date/utc_la` | EQUAL | 2023-12-31 | 2023-12-31 |
| `held_write/writeto/date/la_utc` | EQUAL | 2024-01-01 | 2024-01-01 |
| `held_write/writeto/lit/utc_la` | EQUAL | 1704076200 | 1704076200 |
| `held_write/writeto/lit/la_utc` | EQUAL | 1704105000 | 1704105000 |
| `held_write/parquet/hour/utc_la` | VALUE | 2 | 18 |
| `held_write/parquet/hour/la_utc` | VALUE | 18 | 2 |
| `held_write/parquet/lit/utc_la` | EQUAL | 1704076200 | 1704076200 |
| `held_write/parquet/lit/la_utc` | EQUAL | 1704105000 | 1704105000 |
| `held_write/saveastable/hour/utc_la` | VALUE | 2 | 18 |
| `held_write/saveastable/hour/la_utc` | VALUE | 18 | 2 |
| `held_write/saveastable/lit/utc_la` | EQUAL | 1704076200 | 1704076200 |
| `held_write/saveastable/lit/la_utc` | EQUAL | 1704105000 | 1704105000 |
| `held_write/insertinto/hour/utc_la` | VALUE | 2 | 18 |
| `held_write/insertinto/hour/la_utc` | VALUE | 18 | 2 |
| `held_write/insertinto/lit/utc_la` | EQUAL | 1704076200 | 1704076200 |
| `held_write/insertinto/lit/la_utc` | EQUAL | 1704105000 | 1704105000 |
| `held_write/writeto/add/on_off` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | -2147483648 |
| `held_write/writeto/add/off_on` | REPARK_RAISES | -2147483648 | RAISE ARITHMETIC_OVERFLOW |

# Bar, part 2 (written by bar2.py)

## Counts per verdict

| set | scope | verdict | pre | part1 | head |
|---|---|---|---|---|---|
| rc | direct | BOTH_RAISE | 3 | 3 | 3 |
| rc | direct | BOTH_RAISE_OTHER | 2 | 2 | 2 |
| rc | direct | EQUAL | 6 | 6 | 6 |
| rc | direct | REPARK_RAISES | 3 | 3 | 3 |
| rc | direct | VALUE | 1 | 1 | 1 |
| rc | part1 | BOTH_RAISE | 0 | 3 | 3 |
| rc | part1 | BOTH_RAISE_OTHER | 1 | 1 | 1 |
| rc | part1 | EQUAL | 11 | 16 | 16 |
| rc | part1 | REPARK_ANSWERS | 3 | 0 | 0 |
| rc | part1 | REPARK_RAISES | 5 | 6 | 6 |
| rc | part1 | VALUE | 6 | 0 | 0 |
| rc | part2 | BOTH_RAISE | 2 | 2 | 9 |
| rc | part2 | BOTH_RAISE_OTHER | 1 | 1 | 1 |
| rc | part2 | EQUAL | 18 | 18 | 31 |
| rc | part2 | REPARK_ANSWERS | 7 | 7 | 0 |
| rc | part2 | REPARK_RAISES | 6 | 6 | 7 |
| rc | part2 | VALUE | 14 | 14 | 0 |
| rev | direct | BOTH_RAISE | 3 | 3 | 3 |
| rev | direct | BOTH_RAISE_OTHER | 2 | 2 | 2 |
| rev | direct | EQUAL | 18 | 18 | 18 |
| rev | direct | REPARK_RAISES | 6 | 6 | 6 |
| rev | held | EQUAL | 1 | 1 | 1 |
| rev | part1 | BOTH_RAISE | 0 | 5 | 5 |
| rev | part1 | EQUAL | 13 | 30 | 30 |
| rev | part1 | REPARK_ANSWERS | 5 | 0 | 0 |
| rev | part1 | REPARK_RAISES | 4 | 1 | 1 |
| rev | part1 | VALUE | 14 | 0 | 0 |
| rev | part2 | BOTH_RAISE | 10 | 10 | 23 |
| rev | part2 | EQUAL | 37 | 37 | 64 |
| rev | part2 | REPARK_ANSWERS | 14 | 14 | 1 |
| rev | part2 | REPARK_RAISES | 1 | 1 | 1 |
| rev | part2 | VALUE | 28 | 28 | 1 |
| run | catalog | BOTH_RAISE | 16 | 16 | 16 |
| run | catalog | BOTH_RAISE_OTHER | 16 | 16 | 16 |
| run | catalog | EQUAL | 80 | 80 | 80 |
| run | catalog | REPARK_RAISES | 16 | 16 | 16 |
| run | direct | BOTH_RAISE | 15 | 15 | 15 |
| run | direct | BOTH_RAISE_OTHER | 5 | 5 | 5 |
| run | direct | EQUAL | 55 | 55 | 55 |
| run | direct | REPARK_ANSWERS | 2 | 2 | 2 |
| run | direct | REPARK_RAISES | 5 | 5 | 5 |
| run | direct | VALUE | 3 | 3 | 3 |
| run | held | BOTH_RAISE | 27 | 27 | 27 |
| run | held | BOTH_RAISE_OTHER | 63 | 63 | 63 |
| run | held | EQUAL | 240 | 240 | 240 |
| run | held | REPARK_ANSWERS | 14 | 14 | 14 |
| run | held | REPARK_RAISES | 27 | 27 | 27 |
| run | held | VALUE | 13 | 13 | 13 |
| run | other: | EQUAL | 12 | 12 | 12 |
| run | part1 | BOTH_RAISE | 45 | 91 | 91 |
| run | part1 | BOTH_RAISE_OTHER | 206 | 227 | 227 |
| run | part1 | EQUAL | 422 | 701 | 702 |
| run | part1 | REPARK_ANSWERS | 101 | 34 | 34 |
| run | part1 | REPARK_RAISES | 181 | 81 | 81 |
| run | part1 | VALUE | 197 | 18 | 17 |
| run | part2 | BOTH_RAISE | 37 | 37 | 60 |
| run | part2 | BOTH_RAISE_OTHER | 113 | 113 | 113 |
| run | part2 | EQUAL | 343 | 343 | 474 |
| run | part2 | REPARK_ANSWERS | 50 | 50 | 27 |
| run | part2 | REPARK_RAISES | 66 | 66 | 51 |
| run | part2 | VALUE | 125 | 125 | 9 |
| run-unset | direct | BOTH_RAISE | 2 | 2 | 2 |
| run-unset | direct | BOTH_RAISE_OTHER | 1 | 1 | 1 |
| run-unset | direct | EQUAL | 2 | 2 | 2 |
| run-unset | direct | REPARK_RAISES | 1 | 1 | 1 |
| run-unset | direct | VALUE | 1 | 1 | 1 |
| run-unset | held | BOTH_RAISE | 4 | 4 | 4 |
| run-unset | held | BOTH_RAISE_OTHER | 2 | 2 | 2 |
| run-unset | other: | EQUAL | 6 | 6 | 6 |
| run-unset | other: | VALUE | 1 | 1 | 1 |
| run-unset | part1 | BOTH_RAISE | 2 | 4 | 4 |
| run-unset | part1 | BOTH_RAISE_OTHER | 2 | 2 | 2 |
| run-unset | part1 | REPARK_ANSWERS | 2 | 0 | 0 |
| run-unset | part2 | BOTH_RAISE | 3 | 3 | 4 |
| run-unset | part2 | BOTH_RAISE_OTHER | 2 | 2 | 2 |
| run-unset | part2 | REPARK_ANSWERS | 1 | 1 | 0 |
| sweep | sweep | BOTH_RAISE_OTHER | 20 | 20 | 20 |
| sweep | sweep | EQUAL | 108 | 108 | 148 |
| sweep | sweep | REPARK_RAISES | 56 | 56 | 52 |
| sweep | sweep | VALUE | 48 | 48 | 12 |

## Moves against part1

| move | scope | cells |
|---|---|---|
| EQUAL->RAISE | part2 | 5 |
| WRONG->EQUAL | part1 | 1 |
| WRONG->EQUAL | part2 | 220 |
| WRONG->EQUAL | sweep | 40 |
| WRONG->WRONG(changed) | part2 | 2 |
| WRONG->WRONG(changed) | sweep | 8 |

## Every move against part1

| move | scope | set | cell | Spark | part1 | head |
|---|---|---|---|---|---|---|
| EQUAL->RAISE | part2 | rc | `filter/add_sens/df/fadd/on_off` | [["2"], ["int"]] | [["2"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| EQUAL->RAISE | part2 | rc | `filter/agg_key_sens/df/fagg/on_off` | [["5.0"], ["double"]] | [["5.0"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| EQUAL->RAISE | part2 | rc | `filter/or_mixed/df/fdiv/on_off` | [["5.0"], ["double"]] | [["5.0"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| EQUAL->RAISE | part2 | rev | `filter_sensitive/df/fdiv/on_off` | [["5.0"], ["double"]] | [["5.0"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| EQUAL->RAISE | part2 | run | `write/writeto-held/on/off/avg` | [["null"], ["decimal(38,4)"]] | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `nest/read/sqldf/utc/m2` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `cache/ansi_off_cached_on` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `cache/df_lazy_then_set` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `cache/df_then_set` | [["after=2", "cached=true"], []] | [["after=18", "cached=true"], []] | [["after=2", "cached=true"], []] |
| WRONG->EQUAL | part2 | rev | `conf_set/conf_then_sql/df/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `conf_set/sql_then_conf/df/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `current_date/df/utc_kir` | [["2026-11-18"], ["string"]] {"direct": ["2026-11-19"]} | [["2026-11-19"], ["string"]] {"direct": ["2026-11-19"]} | [["2026-11-18"], ["string"]] {"direct": ["2026-11-19"]} |
| WRONG->EQUAL | part2 | rev | `derive/cache/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `derive/cache/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `derive/explode/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rc | `derive/explode/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `derive/explode/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `derive/explode/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `derive/gagg_lit/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648\|1"], ["int", "int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `derive/gagg_lit/hour/utc_la` | [["2\|1"], ["int", "int"]] | [["18\|1"], ["int", "int"]] | [["2\|1"], ["int", "int"]] |
| WRONG->EQUAL | part2 | rev | `derive/intersect/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rc | `derive/intersect/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `derive/intersect/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `derive/intersect/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `derive/join/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rc | `derive/join/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `derive/join/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `derive/join/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `derive/registertemptable/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `derive/registertemptable/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `derive/regtt/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rc | `derive/regtt/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `derive/selectexpr/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rc | `derive/selectexpr/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `derive/selectexpr/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `derive/selectexpr/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `derive/subtract/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `derive/subtract/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `derive/tempview_after/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `derive/tempview_after/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `derive/unpivot/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rc | `derive/unpivot/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `derive/unpivot/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `derive/unpivot/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `door/registertemptable/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rc | `door/registertemptable/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `door/registertemptable/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `door/registertemptable/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `filter/add_m/df/fadd/off_on` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `filter/zone_fn/df/fhour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `filter/zone_m/df/fhour/utc_la` | [["2"], ["int"]] | [[], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `filter/zone_strcast/df/fhour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `held_write/insertinto/hour/la_utc` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `held_write/insertinto/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `held_write/insertinto/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `held_write/parquet/hour/la_utc` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `held_write/parquet/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `held_write/saveastable/hour/la_utc` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `held_write/saveastable/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `held_write/saveastable/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `held_write/writeto/add/off_on` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `held_write/writeto/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `nest/read/dfdf/la/m` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | run | `nest/read/dfdf/utc/m2` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | run | `nest/read/dfsql/la/m` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | run | `nest/read/dfsql/on/m2` | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| WRONG->EQUAL | part2 | run | `nest/read/dfsql/utc/m2` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `now_fn/df/lts/la_utc` | [["6"], ["int"]] | [["14"], ["int"]] | [["6"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `now_fn/df/lts/utc_la` | [["14"], ["int"]] | [["6"], ["int"]] | [["14"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `now_fn/df/now/la_utc` | [["6"], ["int"]] | [["14"], ["int"]] | [["6"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `now_fn/df/now/utc_la` | [["14"], ["int"]] | [["6"], ["int"]] | [["14"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/la/utc/date_ts` | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/la/utc/fmt` | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/la/utc/fromunix` | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/la/utc/parse` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/la/utc/trunc` | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/off/on/cast_dbl` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/df/cte/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/df/cte/on/off/cast_dbl` | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT |
| WRONG->EQUAL | part2 | run | `read/df/cte/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/utc/la/date_ts` | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/utc/la/fmt` | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/utc/la/fromunix` | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/utc/la/parse` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/utc/la/trunc` | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/la/utc/date_ts` | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/la/utc/fmt` | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/la/utc/fromunix` | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/la/utc/parse` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/la/utc/trunc` | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/off/on/cast_dbl` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/df/join/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/df/join/on/off/cast_dbl` | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT |
| WRONG->EQUAL | part2 | run | `read/df/join/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/utc/la/date_ts` | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/utc/la/fmt` | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/utc/la/fromunix` | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/utc/la/parse` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/utc/la/trunc` | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/la/utc/date_ts` | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/la/utc/fmt` | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/la/utc/fromunix` | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/la/utc/parse` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/la/utc/trunc` | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/off/on/cast_dbl` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/df/plain/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/df/plain/on/off/cast_dbl` | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT |
| WRONG->EQUAL | part2 | run | `read/df/plain/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/utc/la/date_ts` | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/utc/la/fmt` | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/utc/la/fromunix` | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/utc/la/parse` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/utc/la/trunc` | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/la/utc/date_ts` | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/la/utc/fmt` | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/la/utc/fromunix` | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/la/utc/parse` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/la/utc/trunc` | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/off/on/cast_dbl` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/df/subq/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/df/subq/on/off/cast_dbl` | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT |
| WRONG->EQUAL | part2 | run | `read/df/subq/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/utc/la/date_ts` | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/utc/la/fmt` | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/utc/la/fromunix` | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/utc/la/parse` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/utc/la/trunc` | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/la/utc/date_ts` | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/la/utc/fmt` | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/la/utc/fromunix` | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/la/utc/parse` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/la/utc/trunc` | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/off/on/cast_dbl` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/dff/plain/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/dff/plain/on/off/cast_dbl` | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT |
| WRONG->EQUAL | part2 | run | `read/dff/plain/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/utc/la/date_ts` | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/utc/la/fmt` | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/utc/la/fromunix` | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/utc/la/parse` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/utc/la/trunc` | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/la/utc/date_ts` | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/la/utc/fmt` | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/la/utc/fromunix` | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/la/utc/parse` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/la/utc/trunc` | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/off/on/cast_dbl` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/on/off/cast_dbl` | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/utc/la/date_ts` | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/utc/la/fmt` | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/utc/la/fromunix` | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/utc/la/parse` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/utc/la/trunc` | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `reader/df/la/utc/filter` | [["18\|2"], ["int", "int"]] | [["2\|2"], ["int", "int"]] | [["18\|2"], ["int", "int"]] |
| WRONG->EQUAL | part2 | run | `reader/df/la/utc/proj` | [["18\|2\|2024-01-01 02:30:00"], ["int", "int", "string"]] | [["2\|2\|2024-01-01 02:30:00"], ["int", "int", "string"]] | [["18\|2\|2024-01-01 02:30:00"], ["int", "int", "string"]] |
| WRONG->EQUAL | part2 | run | `reader/df/utc/la/filter` | [["2\|18"], ["int", "int"]] | [["18\|18"], ["int", "int"]] | [["2\|18"], ["int", "int"]] |
| WRONG->EQUAL | part2 | run | `reader/df/utc/la/proj` | [["2\|18\|2023-12-31 18:30:00"], ["int", "int", "string"]] | [["18\|18\|2023-12-31 18:30:00"], ["int", "int", "string"]] | [["2\|18\|2023-12-31 18:30:00"], ["int", "int", "string"]] |
| WRONG->EQUAL | part2 | rev | `replace/df/hour/utc_la` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `reset/key/df/hour` | [["18"], ["int"]] {"after_reset": "UTC"} | [["2"], ["int"]] {"after_reset": "UTC"} | [["18"], ["int"]] {"after_reset": "UTC"} |
| WRONG->EQUAL | part2 | rev | `self_join/df/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648\|-2147483648"], ["int", "int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `self_join/df/hour/utc_la` | [["2\|2"], ["int", "int"]] | [["18\|18"], ["int", "int"]] | [["2\|2"], ["int", "int"]] |
| WRONG->EQUAL | part2 | run-unset | `unset/read/df/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `write/ctas-df/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part2 | run | `write/ctas-df/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | run | `write/ctas-df/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `write/ctas-df/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | run | `write/ctas-df/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `write/ctas-df/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `write/ctas-df/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part2 | run | `write/ctas-df/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | run | `write/ctas-df/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `write/parquet-held/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part2 | run | `write/parquet-held/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | run | `write/parquet-held/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `write/parquet-held/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | run | `write/parquet-held/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `write/parquet-held/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `write/parquet-held/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part2 | run | `write/parquet-held/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | run | `write/parquet-held/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `write/writeto-held/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | run | `write/writeto-held/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | sweep | sweep | `false:sweep/dec/dec_union/off>on/read_off` | [["1", "2", "null", "null"], ["decimal(38,0)"]] | RAISE CANNOT_UP_CAST_DATATYPE | [["1", "2", "null", "null"], ["decimal(38,0)"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/dec/dec_union/off>on/read_on` | [["1", "2", "null", "null"], ["decimal(38,0)"]] | [["1.0000000000", "1.5000000000", "null", "null"], ["decimal | [["1", "2", "null", "null"], ["decimal(38,0)"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/dec/dec_union/on>off/read_off` | [["1.0000000000", "1.5000000000", "null", "null"], ["decimal | [["1", "2", "null", "null"], ["decimal(38,0)"]] | [["1.0000000000", "1.5000000000", "null", "null"], ["decimal |
| WRONG->EQUAL | sweep | sweep | `false:sweep/dec/dec_union/on>off/read_on` | [["1.0000000000", "1.5000000000", "null", "null"], ["decimal | RAISE CANNOT_UP_CAST_DATATYPE | [["1.0000000000", "1.5000000000", "null", "null"], ["decimal |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/case_zone/la>utc/read_utc` | [["2023-12-31 19:00:00", "2024-01-01 00:00:00"], ["string"]] | [["2024-01-01 03:00:00", "2024-01-01 08:00:00"], ["string"]] | [["2023-12-31 19:00:00", "2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/case_zone/utc>la/read_la` | [["2024-01-01 00:00:00", "2024-01-01 03:00:00"], ["string"]] | [["2023-12-31 16:00:00", "2023-12-31 19:00:00"], ["string"]] | [["2024-01-01 00:00:00", "2024-01-01 03:00:00"], ["string"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/date_ts/la>utc/read_utc` | [["0"], ["bigint"]] | [["1"], ["bigint"]] | [["0"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/date_ts/utc>la/read_la` | [["1"], ["bigint"]] | [["0"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/hour_where/la>utc/read_utc` | [["0"], ["bigint"]] | [["1"], ["bigint"]] | [["0"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/hour_where/utc>la/read_la` | [["1"], ["bigint"]] | [["0"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/ts_between/la>utc/read_utc` | [["1"], ["bigint"]] | [["2"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/ts_between/utc>la/read_la` | [["2"], ["bigint"]] | [["1"], ["bigint"]] | [["2"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/ts_eq/la>utc/read_utc` | [["0"], ["bigint"]] | [["1"], ["bigint"]] | [["0"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/ts_eq/utc>la/read_la` | [["1"], ["bigint"]] | [["0"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/ts_ge/la>utc/read_utc` | [["1"], ["bigint"]] | [["2"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/ts_ge/utc>la/read_la` | [["2"], ["bigint"]] | [["1"], ["bigint"]] | [["2"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/ts_ge_column/la>utc/read_utc` | [["1"], ["bigint"]] | [["2"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/ts_ge_column/utc>la/read_la` | [["2"], ["bigint"]] | [["1"], ["bigint"]] | [["2"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/ts_in/la>utc/read_utc` | [["0"], ["bigint"]] | [["2"], ["bigint"]] | [["0"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/ts_in/utc>la/read_la` | [["2"], ["bigint"]] | [["0"], ["bigint"]] | [["2"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/dec/dec_union/off>on/read_off` | [["1", "2", "null", "null"], ["decimal(38,0)"]] | RAISE CANNOT_UP_CAST_DATATYPE | [["1", "2", "null", "null"], ["decimal(38,0)"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/dec/dec_union/off>on/read_on` | [["1", "2", "null", "null"], ["decimal(38,0)"]] | [["1.0000000000", "1.5000000000", "null", "null"], ["decimal | [["1", "2", "null", "null"], ["decimal(38,0)"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/dec/dec_union/on>off/read_off` | [["1.0000000000", "1.5000000000", "null", "null"], ["decimal | [["1", "2", "null", "null"], ["decimal(38,0)"]] | [["1.0000000000", "1.5000000000", "null", "null"], ["decimal |
| WRONG->EQUAL | sweep | sweep | `true:sweep/dec/dec_union/on>off/read_on` | [["1.0000000000", "1.5000000000", "null", "null"], ["decimal | RAISE CANNOT_UP_CAST_DATATYPE | [["1.0000000000", "1.5000000000", "null", "null"], ["decimal |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/case_zone/la>utc/read_utc` | [["2023-12-31 19:00:00", "2024-01-01 00:00:00"], ["string"]] | [["2024-01-01 03:00:00", "2024-01-01 08:00:00"], ["string"]] | [["2023-12-31 19:00:00", "2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/case_zone/utc>la/read_la` | [["2024-01-01 00:00:00", "2024-01-01 03:00:00"], ["string"]] | [["2023-12-31 16:00:00", "2023-12-31 19:00:00"], ["string"]] | [["2024-01-01 00:00:00", "2024-01-01 03:00:00"], ["string"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/date_ts/la>utc/read_utc` | [["0"], ["bigint"]] | [["1"], ["bigint"]] | [["0"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/date_ts/utc>la/read_la` | [["1"], ["bigint"]] | [["0"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/hour_where/la>utc/read_utc` | [["0"], ["bigint"]] | [["1"], ["bigint"]] | [["0"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/hour_where/utc>la/read_la` | [["1"], ["bigint"]] | [["0"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/ts_between/la>utc/read_utc` | [["1"], ["bigint"]] | [["2"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/ts_between/utc>la/read_la` | [["2"], ["bigint"]] | [["1"], ["bigint"]] | [["2"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/ts_eq/la>utc/read_utc` | [["0"], ["bigint"]] | [["1"], ["bigint"]] | [["0"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/ts_eq/utc>la/read_la` | [["1"], ["bigint"]] | [["0"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/ts_ge/la>utc/read_utc` | [["1"], ["bigint"]] | [["2"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/ts_ge/utc>la/read_la` | [["2"], ["bigint"]] | [["1"], ["bigint"]] | [["2"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/ts_ge_column/la>utc/read_utc` | [["1"], ["bigint"]] | [["2"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/ts_ge_column/utc>la/read_la` | [["2"], ["bigint"]] | [["1"], ["bigint"]] | [["2"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/ts_in/la>utc/read_utc` | [["0"], ["bigint"]] | [["2"], ["bigint"]] | [["0"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/ts_in/utc>la/read_la` | [["2"], ["bigint"]] | [["0"], ["bigint"]] | [["2"], ["bigint"]] |
| WRONG->WRONG(changed) | part2 | rc | `filter/numcast/df/fcast/off_on` | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 3000000000 t | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| WRONG->WRONG(changed) | part2 | rc | `filter/numcast/df/fcast/on_off` | [["7"], ["int"]] | RAISE Arrow error: Cast error: Can't cast value 3000000000 t | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| WRONG->WRONG(changed) | sweep | sweep | `false:sweep/zone/date_add_where/la>utc/read_la` | [["1"], ["bigint"]] | RAISE type_coercion caused by Error during planning: Executi | RAISE repark_frame_settings caused by type_coercion caused b |
| WRONG->WRONG(changed) | sweep | sweep | `false:sweep/zone/date_add_where/la>utc/read_utc` | [["1"], ["bigint"]] | RAISE type_coercion caused by Error during planning: Executi | RAISE repark_frame_settings caused by type_coercion caused b |
| WRONG->WRONG(changed) | sweep | sweep | `false:sweep/zone/date_add_where/utc>la/read_la` | [["2"], ["bigint"]] | RAISE type_coercion caused by Error during planning: Executi | RAISE repark_frame_settings caused by type_coercion caused b |
| WRONG->WRONG(changed) | sweep | sweep | `false:sweep/zone/date_add_where/utc>la/read_utc` | [["2"], ["bigint"]] | RAISE type_coercion caused by Error during planning: Executi | RAISE repark_frame_settings caused by type_coercion caused b |
| WRONG->WRONG(changed) | sweep | sweep | `true:sweep/zone/date_add_where/la>utc/read_la` | [["1"], ["bigint"]] | RAISE type_coercion caused by Error during planning: Executi | RAISE repark_frame_settings caused by type_coercion caused b |
| WRONG->WRONG(changed) | sweep | sweep | `true:sweep/zone/date_add_where/la>utc/read_utc` | [["1"], ["bigint"]] | RAISE type_coercion caused by Error during planning: Executi | RAISE repark_frame_settings caused by type_coercion caused b |
| WRONG->WRONG(changed) | sweep | sweep | `true:sweep/zone/date_add_where/utc>la/read_la` | [["2"], ["bigint"]] | RAISE type_coercion caused by Error during planning: Executi | RAISE repark_frame_settings caused by type_coercion caused b |
| WRONG->WRONG(changed) | sweep | sweep | `true:sweep/zone/date_add_where/utc>la/read_utc` | [["2"], ["bigint"]] | RAISE type_coercion caused by Error during planning: Executi | RAISE repark_frame_settings caused by type_coercion caused b |

## Moves against pre

| move | scope | cells |
|---|---|---|
| EQUAL->RAISE | part1 | 4 |
| EQUAL->RAISE | part2 | 5 |
| WRONG->EQUAL | part1 | 362 |
| WRONG->EQUAL | part2 | 220 |
| WRONG->EQUAL | sweep | 40 |
| WRONG->WRONG(changed) | part1 | 23 |
| WRONG->WRONG(changed) | part2 | 2 |
| WRONG->WRONG(changed) | sweep | 8 |

## Every move against pre

| move | scope | set | cell | Spark | pre | head |
|---|---|---|---|---|---|---|
| EQUAL->RAISE | part1 | rc | `filter/agg_key_sens/sql/fagg/on_off` | [["5.0"], ["double"]] | [["5.0"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| EQUAL->RAISE | part1 | rc | `filter/and_free/sql/fdiv/on_off` | [["5.0"], ["double"]] | [["5.0"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| EQUAL->RAISE | part1 | rc | `filter/or_mixed/sql/fdiv/on_off` | [["5.0"], ["double"]] | [["5.0"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| EQUAL->RAISE | part1 | rev | `filter_sensitive/sql/fdiv/on_off` | [["5.0"], ["double"]] | [["5.0"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| EQUAL->RAISE | part2 | rc | `filter/add_sens/df/fadd/on_off` | [["2"], ["int"]] | [["2"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| EQUAL->RAISE | part2 | rc | `filter/agg_key_sens/df/fagg/on_off` | [["5.0"], ["double"]] | [["5.0"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| EQUAL->RAISE | part2 | rc | `filter/or_mixed/df/fdiv/on_off` | [["5.0"], ["double"]] | [["5.0"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| EQUAL->RAISE | part2 | rev | `filter_sensitive/df/fdiv/on_off` | [["5.0"], ["double"]] | [["5.0"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| EQUAL->RAISE | part2 | run | `write/writeto-held/on/off/avg` | [["null"], ["decimal(38,4)"]] | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | rev | `cache/catalog_then_set` | [["after=2"], []] | [["after=18"], []] | [["after=2"], []] |
| WRONG->EQUAL | part1 | rev | `cache/lazy_then_set` | [["after=2"], []] | [["after=18"], []] | [["after=2"], []] |
| WRONG->EQUAL | part1 | rev | `cache/set_between_off_on` | [["after=-2147483648"], []] | RAISE ARITHMETIC_OVERFLOW | [["after=-2147483648"], []] |
| WRONG->EQUAL | part1 | rev | `cache/set_between_zone` | [["after=2"], []] | [["after=18"], []] | [["after=2"], []] |
| WRONG->EQUAL | part1 | rc | `cache/set_then_cache` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part1 | rev | `cache/sql_then_set` | [["before=2", "after=2", "cached=true"], []] | [["before=2", "after=18", "cached=true"], []] | [["before=2", "after=2", "cached=true"], []] |
| WRONG->EQUAL | part1 | rc | `cache/sql_then_set` | [["2"], ["int"]] {"cached": "True"} | [["18"], ["int"]] {"cached": "True"} | [["2"], ["int"]] {"cached": "True"} |
| WRONG->EQUAL | part1 | rev | `conf_set/conf_then_sql/sql/div/on_off` | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->EQUAL | part1 | rev | `conf_set/conf_then_sql/sql/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part1 | rev | `conf_set/sql_then_conf/sql/div/on_off` | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->EQUAL | part1 | rev | `conf_set/sql_then_conf/sql/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part1 | rc | `current_date/sql/utc_kir` | [["2026-11-18"], ["string"]] {"direct": ["2026-11-19"]} | [["2026-11-19"], ["string"]] {"direct": ["2026-11-19"]} | [["2026-11-18"], ["string"]] {"direct": ["2026-11-19"]} |
| WRONG->EQUAL | part1 | rc | `filter/add_m/sql/fadd/off_on` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part1 | rc | `filter/add_sens/sql/fadd/on_off` | RAISE ARITHMETIC_OVERFLOW | [["2"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | rc | `filter/agg_m/sql/fagg/on_off` | RAISE DIVIDE_BY_ZERO | [["5.0"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->EQUAL | part1 | rc | `filter/agg_null/sql/fagg/off_on` | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | rc | `filter/or_free/sql/fdiv/on_off` | RAISE DIVIDE_BY_ZERO | [["5.0"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->EQUAL | part1 | rc | `filter/zone_fn/sql/fhour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part1 | rc | `filter/zone_m/sql/fhour/utc_la` | [["2"], ["int"]] | [[], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part1 | rc | `filter/zone_strcast/sql/fhour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part1 | rev | `filter_on_m/sql/fdiv/off_on` | [["5.0"], ["double"]] | RAISE DIVIDE_BY_ZERO | [["5.0"], ["double"]] |
| WRONG->EQUAL | part1 | rev | `filter_on_m/sql/fdiv/on_off` | RAISE DIVIDE_BY_ZERO | [["5.0"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->EQUAL | part1 | run | `nest/read/sqldf/la/m` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part1 | run | `nest/read/sqldf/off/m` | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->EQUAL | part1 | run | `nest/read/sqldf/utc/m2` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part1 | run | `nest/read/sqlsql/la/m` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part1 | run | `nest/read/sqlsql/on/m2` | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `nest/read/sqlsql/utc/m2` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part1 | rev | `now_fn/sql/lts/la_utc` | [["6"], ["int"]] | [["14"], ["int"]] | [["6"], ["int"]] |
| WRONG->EQUAL | part1 | rev | `now_fn/sql/lts/utc_la` | [["14"], ["int"]] | [["6"], ["int"]] | [["14"], ["int"]] |
| WRONG->EQUAL | part1 | rev | `now_fn/sql/now/la_utc` | [["6"], ["int"]] | [["14"], ["int"]] | [["6"], ["int"]] |
| WRONG->EQUAL | part1 | rev | `now_fn/sql/now/utc_la` | [["14"], ["int"]] | [["6"], ["int"]] | [["14"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/la/utc/date_ts` | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/la/utc/fmt` | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/la/utc/fromunix` | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/la/utc/lit` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/la/utc/parse` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/la/utc/trunc` | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/off/on/avg` | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/off/on/bool_dec` | [["null"], ["decimal(1,1)"]] | RAISE NUMERIC_VALUE_OUT_OF_RANGE | [["null"], ["decimal(1,1)"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/off/on/cast_dbl` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/off/on/dbl_lit` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/off/on/div` | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/off/on/int_bin` | [["7FFFFFFF"], ["string"]] | RAISE DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | [["7FFFFFFF"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/off/on/mod` | [["null"], ["int"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `read/ren/plain/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `read/ren/plain/on/off/cast_dbl` | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT |
| WRONG->EQUAL | part1 | run | `read/ren/plain/on/off/div` | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->EQUAL | part1 | run | `read/ren/plain/on/off/map_struct` | [["7"], ["int"]] | RAISE DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | [["7"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/utc/la/date_ts` | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/utc/la/fmt` | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/utc/la/fromunix` | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/utc/la/lit` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/utc/la/parse` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/ren/plain/utc/la/trunc` | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/la/utc/date_ts` | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/la/utc/fmt` | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/la/utc/fromunix` | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/la/utc/lit` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/la/utc/parse` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/la/utc/trunc` | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/off/on/avg` | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/off/on/bool_dec` | [["null"], ["decimal(1,1)"]] | RAISE NUMERIC_VALUE_OUT_OF_RANGE | [["null"], ["decimal(1,1)"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/off/on/cast_dbl` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/off/on/dbl_lit` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/off/on/div` | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/off/on/int_bin` | [["7FFFFFFF"], ["string"]] | RAISE DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | [["7FFFFFFF"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/off/on/mod` | [["null"], ["int"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `read/sql/cte/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `read/sql/cte/on/off/cast_dbl` | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT |
| WRONG->EQUAL | part1 | run | `read/sql/cte/on/off/div` | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->EQUAL | part1 | run | `read/sql/cte/on/off/map_struct` | [["7"], ["int"]] | RAISE DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | [["7"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/utc/la/date_ts` | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/utc/la/fmt` | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/utc/la/fromunix` | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/utc/la/lit` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/utc/la/parse` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/cte/utc/la/trunc` | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/la/utc/date_ts` | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/la/utc/fmt` | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/la/utc/fromunix` | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/la/utc/lit` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/la/utc/parse` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/la/utc/trunc` | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/off/on/avg` | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/off/on/bool_dec` | [["null"], ["decimal(1,1)"]] | RAISE NUMERIC_VALUE_OUT_OF_RANGE | [["null"], ["decimal(1,1)"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/off/on/cast_dbl` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/off/on/dbl_lit` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/off/on/div` | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/off/on/int_bin` | [["7FFFFFFF"], ["string"]] | RAISE DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | [["7FFFFFFF"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/off/on/mod` | [["null"], ["int"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `read/sql/join/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `read/sql/join/on/off/cast_dbl` | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT |
| WRONG->EQUAL | part1 | run | `read/sql/join/on/off/div` | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->EQUAL | part1 | run | `read/sql/join/on/off/map_struct` | [["7"], ["int"]] | RAISE DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | [["7"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/utc/la/date_ts` | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/utc/la/fmt` | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/utc/la/fromunix` | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/utc/la/lit` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/utc/la/parse` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/join/utc/la/trunc` | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/dflt/leg/dec_ct` | [["1", "2"], ["decimal(38,0)"]] | RAISE CANNOT_UP_CAST_DATATYPE | [["1", "2"], ["decimal(38,0)"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/la/utc/date_ts` | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/la/utc/fmt` | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/la/utc/fromunix` | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/la/utc/lit` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/la/utc/parse` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/la/utc/trunc` | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/leg/dflt/dec_ct` | [["1.0000000000", "1.5000000000"], ["decimal(38,10)"]] | RAISE CANNOT_UP_CAST_DATATYPE | [["1.0000000000", "1.5000000000"], ["decimal(38,10)"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/off/on/avg` | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/off/on/bool_dec` | [["null"], ["decimal(1,1)"]] | RAISE NUMERIC_VALUE_OUT_OF_RANGE | [["null"], ["decimal(1,1)"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/off/on/cast_dbl` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/off/on/dbl_lit` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/off/on/div` | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/off/on/int_bin` | [["7FFFFFFF"], ["string"]] | RAISE DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | [["7FFFFFFF"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/off/on/mod` | [["null"], ["int"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `read/sql/plain/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `read/sql/plain/on/off/cast_dbl` | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT |
| WRONG->EQUAL | part1 | run | `read/sql/plain/on/off/div` | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->EQUAL | part1 | run | `read/sql/plain/on/off/map_struct` | [["7"], ["int"]] | RAISE DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | [["7"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/utc/la/date_ts` | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/utc/la/fmt` | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/utc/la/fromunix` | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/utc/la/lit` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/utc/la/parse` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/plain/utc/la/trunc` | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/la/utc/date_ts` | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/la/utc/fmt` | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/la/utc/fromunix` | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/la/utc/lit` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/la/utc/parse` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/la/utc/trunc` | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/off/on/avg` | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/off/on/bool_dec` | [["null"], ["decimal(1,1)"]] | RAISE NUMERIC_VALUE_OUT_OF_RANGE | [["null"], ["decimal(1,1)"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/off/on/cast_dbl` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/off/on/dbl_lit` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/off/on/div` | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/off/on/int_bin` | [["7FFFFFFF"], ["string"]] | RAISE DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | [["7FFFFFFF"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/off/on/mod` | [["null"], ["int"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `read/sql/subq/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `read/sql/subq/on/off/cast_dbl` | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT |
| WRONG->EQUAL | part1 | run | `read/sql/subq/on/off/div` | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->EQUAL | part1 | run | `read/sql/subq/on/off/map_struct` | [["7"], ["int"]] | RAISE DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | [["7"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/utc/la/date_ts` | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/utc/la/fmt` | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/utc/la/fromunix` | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/utc/la/lit` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/utc/la/parse` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/sql/subq/utc/la/trunc` | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/tsql/plain/dflt/leg/dec_ct` | [["1", "2"], ["decimal(38,0)"]] | RAISE CANNOT_UP_CAST_DATATYPE | [["1", "2"], ["decimal(38,0)"]] |
| WRONG->EQUAL | part1 | run | `read/tsql/plain/la/utc/lit` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/tsql/plain/leg/dflt/dec_ct` | [["1.0000000000", "1.5000000000"], ["decimal(38,10)"]] | RAISE CANNOT_UP_CAST_DATATYPE | [["1.0000000000", "1.5000000000"], ["decimal(38,10)"]] |
| WRONG->EQUAL | part1 | run | `read/tsql/plain/off/on/bool_dec` | [["null"], ["decimal(1,1)"]] | RAISE NUMERIC_VALUE_OUT_OF_RANGE | [["null"], ["decimal(1,1)"]] |
| WRONG->EQUAL | part1 | run | `read/tsql/plain/off/on/dbl_lit` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/tsql/plain/off/on/div` | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/tsql/plain/off/on/int_bin` | [["7FFFFFFF"], ["string"]] | RAISE DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | [["7FFFFFFF"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/tsql/plain/off/on/mod` | [["null"], ["int"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/tsql/plain/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `read/tsql/plain/on/off/div` | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->EQUAL | part1 | run | `read/tsql/plain/on/off/map_struct` | [["7"], ["int"]] | RAISE DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | [["7"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/tsql/plain/utc/la/lit` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/tsqlf/plain/dflt/leg/dec_ct` | [["1", "2"], ["decimal(38,0)"]] | RAISE CANNOT_UP_CAST_DATATYPE | [["1", "2"], ["decimal(38,0)"]] |
| WRONG->EQUAL | part1 | run | `read/tsqlf/plain/la/utc/lit` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/tsqlf/plain/leg/dflt/dec_ct` | [["1.0000000000", "1.5000000000"], ["decimal(38,10)"]] | RAISE CANNOT_UP_CAST_DATATYPE | [["1.0000000000", "1.5000000000"], ["decimal(38,10)"]] |
| WRONG->EQUAL | part1 | run | `read/tsqlf/plain/off/on/bool_dec` | [["null"], ["decimal(1,1)"]] | RAISE NUMERIC_VALUE_OUT_OF_RANGE | [["null"], ["decimal(1,1)"]] |
| WRONG->EQUAL | part1 | run | `read/tsqlf/plain/off/on/dbl_lit` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/tsqlf/plain/off/on/div` | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/tsqlf/plain/off/on/int_bin` | [["7FFFFFFF"], ["string"]] | RAISE DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | [["7FFFFFFF"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/tsqlf/plain/off/on/mod` | [["null"], ["int"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/tsqlf/plain/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `read/tsqlf/plain/on/off/div` | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->EQUAL | part1 | run | `read/tsqlf/plain/on/off/map_struct` | [["7"], ["int"]] | RAISE DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | [["7"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/tsqlf/plain/utc/la/lit` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/la/utc/date_ts` | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/la/utc/fmt` | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/la/utc/fromunix` | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/la/utc/lit` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/la/utc/parse` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/la/utc/trunc` | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/off/on/avg` | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/off/on/bool_dec` | [["null"], ["decimal(1,1)"]] | RAISE NUMERIC_VALUE_OUT_OF_RANGE | [["null"], ["decimal(1,1)"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/off/on/cast_dbl` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/off/on/dbl_lit` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/off/on/div` | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/off/on/int_bin` | [["7FFFFFFF"], ["string"]] | RAISE DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | [["7FFFFFFF"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/off/on/mod` | [["null"], ["int"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `read/vds/plain/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `read/vds/plain/on/off/cast_dbl` | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT |
| WRONG->EQUAL | part1 | run | `read/vds/plain/on/off/div` | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->EQUAL | part1 | run | `read/vds/plain/on/off/map_struct` | [["7"], ["int"]] | RAISE DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | [["7"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/utc/la/date_ts` | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/utc/la/fmt` | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/utc/la/fromunix` | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/utc/la/lit` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/utc/la/parse` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vds/plain/utc/la/trunc` | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/la/utc/date_ts` | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/la/utc/fmt` | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/la/utc/fromunix` | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/la/utc/lit` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/la/utc/parse` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/la/utc/trunc` | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/off/on/avg` | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/off/on/bool_dec` | [["null"], ["decimal(1,1)"]] | RAISE NUMERIC_VALUE_OUT_OF_RANGE | [["null"], ["decimal(1,1)"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/off/on/cast_dbl` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/off/on/dbl_lit` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/off/on/div` | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/off/on/int_bin` | [["7FFFFFFF"], ["string"]] | RAISE DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | [["7FFFFFFF"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/off/on/mod` | [["null"], ["int"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/on/off/cast_dbl` | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/on/off/div` | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/on/off/map_struct` | [["7"], ["int"]] | RAISE DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | [["7"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/utc/la/date_ts` | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/utc/la/fmt` | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/utc/la/fromunix` | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/utc/la/lit` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/utc/la/parse` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vdsf/plain/utc/la/trunc` | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/la/utc/date_ts` | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/la/utc/fmt` | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/la/utc/fromunix` | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/la/utc/lit` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/la/utc/parse` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/la/utc/trunc` | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/off/on/avg` | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/off/on/bool_dec` | [["null"], ["decimal(1,1)"]] | RAISE NUMERIC_VALUE_OUT_OF_RANGE | [["null"], ["decimal(1,1)"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/off/on/cast_dbl` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/off/on/dbl_lit` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/off/on/div` | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/off/on/int_bin` | [["7FFFFFFF"], ["string"]] | RAISE DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | [["7FFFFFFF"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/off/on/mod` | [["null"], ["int"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `read/vss/plain/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `read/vss/plain/on/off/cast_dbl` | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT |
| WRONG->EQUAL | part1 | run | `read/vss/plain/on/off/div` | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->EQUAL | part1 | run | `read/vss/plain/on/off/map_struct` | [["7"], ["int"]] | RAISE DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | [["7"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/utc/la/date_ts` | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/utc/la/fmt` | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/utc/la/fromunix` | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/utc/la/lit` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/utc/la/parse` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `read/vss/plain/utc/la/trunc` | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `reader/sql/la/utc/filter` | [["18\|2"], ["int", "int"]] | [["2\|2"], ["int", "int"]] | [["18\|2"], ["int", "int"]] |
| WRONG->EQUAL | part1 | run | `reader/sql/la/utc/proj` | [["18\|2\|2024-01-01 02:30:00"], ["int", "int", "string"]] | [["2\|2\|2024-01-01 02:30:00"], ["int", "int", "string"]] | [["18\|2\|2024-01-01 02:30:00"], ["int", "int", "string"]] |
| WRONG->EQUAL | part1 | run | `reader/sql/utc/la/filter` | [["2\|18"], ["int", "int"]] | [["18\|18"], ["int", "int"]] | [["2\|18"], ["int", "int"]] |
| WRONG->EQUAL | part1 | run | `reader/sql/utc/la/proj` | [["2\|18\|2023-12-31 18:30:00"], ["int", "int", "string"]] | [["18\|18\|2023-12-31 18:30:00"], ["int", "int", "string"]] | [["2\|18\|2023-12-31 18:30:00"], ["int", "int", "string"]] |
| WRONG->EQUAL | part1 | rev | `rename/table/div/on_off` | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->EQUAL | part1 | rev | `rename/table/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part1 | rev | `replace/sql/div/on_off` | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | rev | `replace/sql/hour/utc_la` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part1 | rev | `reset/all/sql/div` | [["null"], ["double"]] {"after_reset": "true"} | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] {"after_reset": "true"} |
| WRONG->EQUAL | part1 | rev | `reset/key/sql/hour` | [["18"], ["int"]] {"after_reset": "UTC"} | [["2"], ["int"]] {"after_reset": "UTC"} | [["18"], ["int"]] {"after_reset": "UTC"} |
| WRONG->EQUAL | part1 | rev | `self_join/sql/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648\|-2147483648"], ["int", "int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | rev | `self_join/sql/hour/utc_la` | [["2\|2"], ["int", "int"]] | [["18\|18"], ["int", "int"]] | [["2\|2"], ["int", "int"]] |
| WRONG->EQUAL | part1 | run-unset | `unset/read/sql/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run-unset | `unset/read/sql/off/div` | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->EQUAL | part1 | run | `write/ctas-sql/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part1 | run | `write/ctas-sql/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part1 | run | `write/ctas-sql/la/utc/lit` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `write/ctas-sql/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `write/ctas-sql/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part1 | run | `write/ctas-sql/off/on/avg` | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] |
| WRONG->EQUAL | part1 | run | `write/ctas-sql/off/on/div` | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `write/ctas-sql/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `write/ctas-sql/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `write/ctas-sql/on/off/div` | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->EQUAL | part1 | run | `write/ctas-sql/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part1 | run | `write/ctas-sql/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part1 | run | `write/ctas-sql/utc/la/lit` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `write/ctas-sql/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `write/insert-sql/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part1 | run | `write/insert-sql/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part1 | run | `write/insert-sql/la/utc/lit` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `write/insert-sql/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `write/insert-sql/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part1 | run | `write/insert-sql/off/on/avg` | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] |
| WRONG->EQUAL | part1 | run | `write/insert-sql/off/on/div` | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `write/insert-sql/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `write/insert-sql/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `write/insert-sql/on/off/div` | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->EQUAL | part1 | run | `write/insert-sql/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part1 | run | `write/insert-sql/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part1 | run | `write/insert-sql/utc/la/lit` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `write/insert-sql/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `write/writeto-tsql/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part1 | run | `write/writeto-tsql/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part1 | run | `write/writeto-tsql/la/utc/lit` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `write/writeto-tsql/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part1 | run | `write/writeto-tsql/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part1 | run | `write/writeto-tsql/off/on/avg` | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] |
| WRONG->EQUAL | part1 | run | `write/writeto-tsql/off/on/div` | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| WRONG->EQUAL | part1 | run | `write/writeto-tsql/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `write/writeto-tsql/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part1 | run | `write/writeto-tsql/on/off/div` | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->EQUAL | part1 | run | `write/writeto-tsql/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part1 | run | `write/writeto-tsql/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part1 | run | `write/writeto-tsql/utc/la/lit` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part1 | run | `write/writeto-tsql/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | rc | `cache/ansi_off_cached_on` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `cache/df_lazy_then_set` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `cache/df_then_set` | [["after=2", "cached=true"], []] | [["after=18", "cached=true"], []] | [["after=2", "cached=true"], []] |
| WRONG->EQUAL | part2 | rev | `conf_set/conf_then_sql/df/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `conf_set/sql_then_conf/df/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `current_date/df/utc_kir` | [["2026-11-18"], ["string"]] {"direct": ["2026-11-19"]} | [["2026-11-19"], ["string"]] {"direct": ["2026-11-19"]} | [["2026-11-18"], ["string"]] {"direct": ["2026-11-19"]} |
| WRONG->EQUAL | part2 | rev | `derive/cache/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `derive/cache/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `derive/explode/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rc | `derive/explode/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `derive/explode/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `derive/explode/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `derive/gagg_lit/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648\|1"], ["int", "int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `derive/gagg_lit/hour/utc_la` | [["2\|1"], ["int", "int"]] | [["18\|1"], ["int", "int"]] | [["2\|1"], ["int", "int"]] |
| WRONG->EQUAL | part2 | rev | `derive/intersect/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rc | `derive/intersect/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `derive/intersect/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `derive/intersect/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `derive/join/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rc | `derive/join/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `derive/join/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `derive/join/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `derive/registertemptable/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `derive/registertemptable/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `derive/regtt/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rc | `derive/regtt/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `derive/selectexpr/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rc | `derive/selectexpr/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `derive/selectexpr/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `derive/selectexpr/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `derive/subtract/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `derive/subtract/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `derive/tempview_after/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `derive/tempview_after/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `derive/unpivot/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rc | `derive/unpivot/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `derive/unpivot/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `derive/unpivot/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `door/registertemptable/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rc | `door/registertemptable/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `door/registertemptable/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `door/registertemptable/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `filter/add_m/df/fadd/off_on` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `filter/zone_fn/df/fhour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `filter/zone_m/df/fhour/utc_la` | [["2"], ["int"]] | [[], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `filter/zone_strcast/df/fhour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `held_write/insertinto/hour/la_utc` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `held_write/insertinto/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `held_write/insertinto/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `held_write/parquet/hour/la_utc` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `held_write/parquet/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `held_write/saveastable/hour/la_utc` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `held_write/saveastable/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rc | `held_write/saveastable/hour/utc_la` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `held_write/writeto/add/off_on` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `held_write/writeto/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `nest/read/dfdf/la/m` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | run | `nest/read/dfdf/utc/m2` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | run | `nest/read/dfsql/la/m` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | run | `nest/read/dfsql/on/m2` | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO | [["null"], ["double"]] |
| WRONG->EQUAL | part2 | run | `nest/read/dfsql/utc/m2` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `now_fn/df/lts/la_utc` | [["6"], ["int"]] | [["14"], ["int"]] | [["6"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `now_fn/df/lts/utc_la` | [["14"], ["int"]] | [["6"], ["int"]] | [["14"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `now_fn/df/now/la_utc` | [["6"], ["int"]] | [["14"], ["int"]] | [["6"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `now_fn/df/now/utc_la` | [["14"], ["int"]] | [["6"], ["int"]] | [["14"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/la/utc/date_ts` | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/la/utc/fmt` | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/la/utc/fromunix` | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/la/utc/parse` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/la/utc/trunc` | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/off/on/cast_dbl` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/df/cte/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/df/cte/on/off/cast_dbl` | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT |
| WRONG->EQUAL | part2 | run | `read/df/cte/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/utc/la/date_ts` | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/utc/la/fmt` | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/utc/la/fromunix` | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/utc/la/parse` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/cte/utc/la/trunc` | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/la/utc/date_ts` | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/la/utc/fmt` | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/la/utc/fromunix` | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/la/utc/parse` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/la/utc/trunc` | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/off/on/cast_dbl` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/df/join/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/df/join/on/off/cast_dbl` | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT |
| WRONG->EQUAL | part2 | run | `read/df/join/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/utc/la/date_ts` | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/utc/la/fmt` | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/utc/la/fromunix` | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/utc/la/parse` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/join/utc/la/trunc` | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/la/utc/date_ts` | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/la/utc/fmt` | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/la/utc/fromunix` | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/la/utc/parse` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/la/utc/trunc` | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/off/on/cast_dbl` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/df/plain/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/df/plain/on/off/cast_dbl` | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT |
| WRONG->EQUAL | part2 | run | `read/df/plain/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/utc/la/date_ts` | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/utc/la/fmt` | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/utc/la/fromunix` | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/utc/la/parse` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/plain/utc/la/trunc` | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/la/utc/date_ts` | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/la/utc/fmt` | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/la/utc/fromunix` | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/la/utc/parse` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/la/utc/trunc` | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/off/on/cast_dbl` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/df/subq/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/df/subq/on/off/cast_dbl` | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT |
| WRONG->EQUAL | part2 | run | `read/df/subq/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/utc/la/date_ts` | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/utc/la/fmt` | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/utc/la/fromunix` | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/utc/la/parse` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/df/subq/utc/la/trunc` | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/la/utc/date_ts` | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/la/utc/fmt` | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/la/utc/fromunix` | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/la/utc/parse` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/la/utc/trunc` | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/off/on/cast_dbl` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/dff/plain/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/dff/plain/on/off/cast_dbl` | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT |
| WRONG->EQUAL | part2 | run | `read/dff/plain/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/utc/la/date_ts` | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/utc/la/fmt` | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/utc/la/fromunix` | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/utc/la/parse` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/dff/plain/utc/la/trunc` | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/la/utc/date_ts` | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/la/utc/fmt` | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/la/utc/fromunix` | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/la/utc/parse` | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/la/utc/trunc` | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/off/on/cast_dbl` | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/on/off/cast_dbl` | RAISE CAST_INVALID_INPUT | [["null"], ["double"]] | RAISE CAST_INVALID_INPUT |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/utc/la/date_ts` | [["1704067200"], ["bigint"]] | [["1704096000"], ["bigint"]] | [["1704067200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/utc/la/fmt` | [["2024-01-01 02"], ["string"]] | [["2023-12-31 18"], ["string"]] | [["2024-01-01 02"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/utc/la/fromunix` | [["1970-01-01 00:00:00"], ["string"]] | [["1969-12-31 16:00:00"], ["string"]] | [["1970-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/utc/la/parse` | [["1704076200"], ["bigint"]] | [["1704105000"], ["bigint"]] | [["1704076200"], ["bigint"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `read/vsd/plain/utc/la/trunc` | [["2024-01-01 00:00:00"], ["string"]] | [["2023-12-31 00:00:00"], ["string"]] | [["2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `reader/df/la/utc/filter` | [["18\|2"], ["int", "int"]] | [["2\|2"], ["int", "int"]] | [["18\|2"], ["int", "int"]] |
| WRONG->EQUAL | part2 | run | `reader/df/la/utc/proj` | [["18\|2\|2024-01-01 02:30:00"], ["int", "int", "string"]] | [["2\|2\|2024-01-01 02:30:00"], ["int", "int", "string"]] | [["18\|2\|2024-01-01 02:30:00"], ["int", "int", "string"]] |
| WRONG->EQUAL | part2 | run | `reader/df/utc/la/filter` | [["2\|18"], ["int", "int"]] | [["18\|18"], ["int", "int"]] | [["2\|18"], ["int", "int"]] |
| WRONG->EQUAL | part2 | run | `reader/df/utc/la/proj` | [["2\|18\|2023-12-31 18:30:00"], ["int", "int", "string"]] | [["18\|18\|2023-12-31 18:30:00"], ["int", "int", "string"]] | [["2\|18\|2023-12-31 18:30:00"], ["int", "int", "string"]] |
| WRONG->EQUAL | part2 | rev | `replace/df/hour/utc_la` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | rev | `reset/key/df/hour` | [["18"], ["int"]] {"after_reset": "UTC"} | [["2"], ["int"]] {"after_reset": "UTC"} | [["18"], ["int"]] {"after_reset": "UTC"} |
| WRONG->EQUAL | part2 | rev | `self_join/df/add/on_off` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648\|-2147483648"], ["int", "int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | rev | `self_join/df/hour/utc_la` | [["2\|2"], ["int", "int"]] | [["18\|18"], ["int", "int"]] | [["2\|2"], ["int", "int"]] |
| WRONG->EQUAL | part2 | run-unset | `unset/read/df/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `write/ctas-df/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part2 | run | `write/ctas-df/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | run | `write/ctas-df/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `write/ctas-df/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | run | `write/ctas-df/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `write/ctas-df/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `write/ctas-df/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part2 | run | `write/ctas-df/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | run | `write/ctas-df/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `write/parquet-held/la/utc/date` | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] |
| WRONG->EQUAL | part2 | run | `write/parquet-held/la/utc/hour` | [["18"], ["int"]] | [["2"], ["int"]] | [["18"], ["int"]] |
| WRONG->EQUAL | part2 | run | `write/parquet-held/la/utc/str` | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `write/parquet-held/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | run | `write/parquet-held/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `write/parquet-held/on/off/avg` | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | part2 | run | `write/parquet-held/utc/la/date` | [["2024-01-01"], ["date"]] | [["2023-12-31"], ["date"]] | [["2024-01-01"], ["date"]] |
| WRONG->EQUAL | part2 | run | `write/parquet-held/utc/la/hour` | [["2"], ["int"]] | [["18"], ["int"]] | [["2"], ["int"]] |
| WRONG->EQUAL | part2 | run | `write/parquet-held/utc/la/str` | [["2024-01-01 02:30:00"], ["string"]] | [["2023-12-31 18:30:00"], ["string"]] | [["2024-01-01 02:30:00"], ["string"]] |
| WRONG->EQUAL | part2 | run | `write/writeto-held/off/on/add_ovf` | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] |
| WRONG->EQUAL | part2 | run | `write/writeto-held/on/off/add_ovf` | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| WRONG->EQUAL | sweep | sweep | `false:sweep/dec/dec_union/off>on/read_off` | [["1", "2", "null", "null"], ["decimal(38,0)"]] | RAISE CANNOT_UP_CAST_DATATYPE | [["1", "2", "null", "null"], ["decimal(38,0)"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/dec/dec_union/off>on/read_on` | [["1", "2", "null", "null"], ["decimal(38,0)"]] | [["1.0000000000", "1.5000000000", "null", "null"], ["decimal | [["1", "2", "null", "null"], ["decimal(38,0)"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/dec/dec_union/on>off/read_off` | [["1.0000000000", "1.5000000000", "null", "null"], ["decimal | [["1", "2", "null", "null"], ["decimal(38,0)"]] | [["1.0000000000", "1.5000000000", "null", "null"], ["decimal |
| WRONG->EQUAL | sweep | sweep | `false:sweep/dec/dec_union/on>off/read_on` | [["1.0000000000", "1.5000000000", "null", "null"], ["decimal | RAISE CANNOT_UP_CAST_DATATYPE | [["1.0000000000", "1.5000000000", "null", "null"], ["decimal |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/case_zone/la>utc/read_utc` | [["2023-12-31 19:00:00", "2024-01-01 00:00:00"], ["string"]] | [["2024-01-01 03:00:00", "2024-01-01 08:00:00"], ["string"]] | [["2023-12-31 19:00:00", "2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/case_zone/utc>la/read_la` | [["2024-01-01 00:00:00", "2024-01-01 03:00:00"], ["string"]] | [["2023-12-31 16:00:00", "2023-12-31 19:00:00"], ["string"]] | [["2024-01-01 00:00:00", "2024-01-01 03:00:00"], ["string"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/date_ts/la>utc/read_utc` | [["0"], ["bigint"]] | [["1"], ["bigint"]] | [["0"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/date_ts/utc>la/read_la` | [["1"], ["bigint"]] | [["0"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/hour_where/la>utc/read_utc` | [["0"], ["bigint"]] | [["1"], ["bigint"]] | [["0"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/hour_where/utc>la/read_la` | [["1"], ["bigint"]] | [["0"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/ts_between/la>utc/read_utc` | [["1"], ["bigint"]] | [["2"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/ts_between/utc>la/read_la` | [["2"], ["bigint"]] | [["1"], ["bigint"]] | [["2"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/ts_eq/la>utc/read_utc` | [["0"], ["bigint"]] | [["1"], ["bigint"]] | [["0"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/ts_eq/utc>la/read_la` | [["1"], ["bigint"]] | [["0"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/ts_ge/la>utc/read_utc` | [["1"], ["bigint"]] | [["2"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/ts_ge/utc>la/read_la` | [["2"], ["bigint"]] | [["1"], ["bigint"]] | [["2"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/ts_ge_column/la>utc/read_utc` | [["1"], ["bigint"]] | [["2"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/ts_ge_column/utc>la/read_la` | [["2"], ["bigint"]] | [["1"], ["bigint"]] | [["2"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/ts_in/la>utc/read_utc` | [["0"], ["bigint"]] | [["2"], ["bigint"]] | [["0"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `false:sweep/zone/ts_in/utc>la/read_la` | [["2"], ["bigint"]] | [["0"], ["bigint"]] | [["2"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/dec/dec_union/off>on/read_off` | [["1", "2", "null", "null"], ["decimal(38,0)"]] | RAISE CANNOT_UP_CAST_DATATYPE | [["1", "2", "null", "null"], ["decimal(38,0)"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/dec/dec_union/off>on/read_on` | [["1", "2", "null", "null"], ["decimal(38,0)"]] | [["1.0000000000", "1.5000000000", "null", "null"], ["decimal | [["1", "2", "null", "null"], ["decimal(38,0)"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/dec/dec_union/on>off/read_off` | [["1.0000000000", "1.5000000000", "null", "null"], ["decimal | [["1", "2", "null", "null"], ["decimal(38,0)"]] | [["1.0000000000", "1.5000000000", "null", "null"], ["decimal |
| WRONG->EQUAL | sweep | sweep | `true:sweep/dec/dec_union/on>off/read_on` | [["1.0000000000", "1.5000000000", "null", "null"], ["decimal | RAISE CANNOT_UP_CAST_DATATYPE | [["1.0000000000", "1.5000000000", "null", "null"], ["decimal |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/case_zone/la>utc/read_utc` | [["2023-12-31 19:00:00", "2024-01-01 00:00:00"], ["string"]] | [["2024-01-01 03:00:00", "2024-01-01 08:00:00"], ["string"]] | [["2023-12-31 19:00:00", "2024-01-01 00:00:00"], ["string"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/case_zone/utc>la/read_la` | [["2024-01-01 00:00:00", "2024-01-01 03:00:00"], ["string"]] | [["2023-12-31 16:00:00", "2023-12-31 19:00:00"], ["string"]] | [["2024-01-01 00:00:00", "2024-01-01 03:00:00"], ["string"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/date_ts/la>utc/read_utc` | [["0"], ["bigint"]] | [["1"], ["bigint"]] | [["0"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/date_ts/utc>la/read_la` | [["1"], ["bigint"]] | [["0"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/hour_where/la>utc/read_utc` | [["0"], ["bigint"]] | [["1"], ["bigint"]] | [["0"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/hour_where/utc>la/read_la` | [["1"], ["bigint"]] | [["0"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/ts_between/la>utc/read_utc` | [["1"], ["bigint"]] | [["2"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/ts_between/utc>la/read_la` | [["2"], ["bigint"]] | [["1"], ["bigint"]] | [["2"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/ts_eq/la>utc/read_utc` | [["0"], ["bigint"]] | [["1"], ["bigint"]] | [["0"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/ts_eq/utc>la/read_la` | [["1"], ["bigint"]] | [["0"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/ts_ge/la>utc/read_utc` | [["1"], ["bigint"]] | [["2"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/ts_ge/utc>la/read_la` | [["2"], ["bigint"]] | [["1"], ["bigint"]] | [["2"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/ts_ge_column/la>utc/read_utc` | [["1"], ["bigint"]] | [["2"], ["bigint"]] | [["1"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/ts_ge_column/utc>la/read_la` | [["2"], ["bigint"]] | [["1"], ["bigint"]] | [["2"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/ts_in/la>utc/read_utc` | [["0"], ["bigint"]] | [["2"], ["bigint"]] | [["0"], ["bigint"]] |
| WRONG->EQUAL | sweep | sweep | `true:sweep/zone/ts_in/utc>la/read_la` | [["2"], ["bigint"]] | [["0"], ["bigint"]] | [["2"], ["bigint"]] |
| WRONG->WRONG(changed) | part1 | rc | `filter/numcast/sql/fcast/off_on` | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 3000000000 t | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| WRONG->WRONG(changed) | part1 | rc | `filter/numcast/sql/fcast/on_off` | [["7"], ["int"]] | RAISE Arrow error: Cast error: Can't cast value 3000000000 t | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| WRONG->WRONG(changed) | part1 | run | `nest/read/sqlsql/off/m` | RAISE 'NoneType' object has no attribute 'contextType' | [["null"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->WRONG(changed) | part1 | run | `read/ren/plain/on/off/bool_dec` | RAISE NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION | [["null"], ["decimal(1,1)"]] | RAISE NUMERIC_VALUE_OUT_OF_RANGE |
| WRONG->WRONG(changed) | part1 | run | `read/ren/plain/on/off/mod` | RAISE REMAINDER_BY_ZERO | [["null"], ["int"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->WRONG(changed) | part1 | run | `read/sql/cte/on/off/bool_dec` | RAISE NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION | [["null"], ["decimal(1,1)"]] | RAISE NUMERIC_VALUE_OUT_OF_RANGE |
| WRONG->WRONG(changed) | part1 | run | `read/sql/cte/on/off/mod` | RAISE REMAINDER_BY_ZERO | [["null"], ["int"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->WRONG(changed) | part1 | run | `read/sql/join/on/off/bool_dec` | RAISE NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION | [["null"], ["decimal(1,1)"]] | RAISE NUMERIC_VALUE_OUT_OF_RANGE |
| WRONG->WRONG(changed) | part1 | run | `read/sql/join/on/off/mod` | RAISE REMAINDER_BY_ZERO | [["null"], ["int"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->WRONG(changed) | part1 | run | `read/sql/plain/on/off/bool_dec` | RAISE NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION | [["null"], ["decimal(1,1)"]] | RAISE NUMERIC_VALUE_OUT_OF_RANGE |
| WRONG->WRONG(changed) | part1 | run | `read/sql/plain/on/off/mod` | RAISE REMAINDER_BY_ZERO | [["null"], ["int"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->WRONG(changed) | part1 | run | `read/sql/subq/on/off/bool_dec` | RAISE NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION | [["null"], ["decimal(1,1)"]] | RAISE NUMERIC_VALUE_OUT_OF_RANGE |
| WRONG->WRONG(changed) | part1 | run | `read/sql/subq/on/off/mod` | RAISE REMAINDER_BY_ZERO | [["null"], ["int"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->WRONG(changed) | part1 | run | `read/tsql/plain/on/off/bool_dec` | RAISE NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION | [["null"], ["decimal(1,1)"]] | RAISE NUMERIC_VALUE_OUT_OF_RANGE |
| WRONG->WRONG(changed) | part1 | run | `read/tsql/plain/on/off/mod` | RAISE REMAINDER_BY_ZERO | [["null"], ["int"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->WRONG(changed) | part1 | run | `read/tsqlf/plain/on/off/bool_dec` | RAISE NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION | [["null"], ["decimal(1,1)"]] | RAISE NUMERIC_VALUE_OUT_OF_RANGE |
| WRONG->WRONG(changed) | part1 | run | `read/tsqlf/plain/on/off/mod` | RAISE REMAINDER_BY_ZERO | [["null"], ["int"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->WRONG(changed) | part1 | run | `read/vds/plain/on/off/bool_dec` | RAISE NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION | [["null"], ["decimal(1,1)"]] | RAISE NUMERIC_VALUE_OUT_OF_RANGE |
| WRONG->WRONG(changed) | part1 | run | `read/vds/plain/on/off/mod` | RAISE REMAINDER_BY_ZERO | [["null"], ["int"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->WRONG(changed) | part1 | run | `read/vdsf/plain/on/off/bool_dec` | RAISE NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION | [["null"], ["decimal(1,1)"]] | RAISE NUMERIC_VALUE_OUT_OF_RANGE |
| WRONG->WRONG(changed) | part1 | run | `read/vdsf/plain/on/off/mod` | RAISE REMAINDER_BY_ZERO | [["null"], ["int"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->WRONG(changed) | part1 | run | `read/vss/plain/on/off/bool_dec` | RAISE NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION | [["null"], ["decimal(1,1)"]] | RAISE NUMERIC_VALUE_OUT_OF_RANGE |
| WRONG->WRONG(changed) | part1 | run | `read/vss/plain/on/off/mod` | RAISE REMAINDER_BY_ZERO | [["null"], ["int"]] | RAISE DIVIDE_BY_ZERO |
| WRONG->WRONG(changed) | part2 | rc | `filter/numcast/df/fcast/off_on` | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 3000000000 t | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| WRONG->WRONG(changed) | part2 | rc | `filter/numcast/df/fcast/on_off` | [["7"], ["int"]] | RAISE Arrow error: Cast error: Can't cast value 3000000000 t | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| WRONG->WRONG(changed) | sweep | sweep | `false:sweep/zone/date_add_where/la>utc/read_la` | [["1"], ["bigint"]] | RAISE type_coercion caused by Error during planning: Executi | RAISE repark_frame_settings caused by type_coercion caused b |
| WRONG->WRONG(changed) | sweep | sweep | `false:sweep/zone/date_add_where/la>utc/read_utc` | [["1"], ["bigint"]] | RAISE type_coercion caused by Error during planning: Executi | RAISE repark_frame_settings caused by type_coercion caused b |
| WRONG->WRONG(changed) | sweep | sweep | `false:sweep/zone/date_add_where/utc>la/read_la` | [["2"], ["bigint"]] | RAISE type_coercion caused by Error during planning: Executi | RAISE repark_frame_settings caused by type_coercion caused b |
| WRONG->WRONG(changed) | sweep | sweep | `false:sweep/zone/date_add_where/utc>la/read_utc` | [["2"], ["bigint"]] | RAISE type_coercion caused by Error during planning: Executi | RAISE repark_frame_settings caused by type_coercion caused b |
| WRONG->WRONG(changed) | sweep | sweep | `true:sweep/zone/date_add_where/la>utc/read_la` | [["1"], ["bigint"]] | RAISE type_coercion caused by Error during planning: Executi | RAISE repark_frame_settings caused by type_coercion caused b |
| WRONG->WRONG(changed) | sweep | sweep | `true:sweep/zone/date_add_where/la>utc/read_utc` | [["1"], ["bigint"]] | RAISE type_coercion caused by Error during planning: Executi | RAISE repark_frame_settings caused by type_coercion caused b |
| WRONG->WRONG(changed) | sweep | sweep | `true:sweep/zone/date_add_where/utc>la/read_la` | [["2"], ["bigint"]] | RAISE type_coercion caused by Error during planning: Executi | RAISE repark_frame_settings caused by type_coercion caused b |
| WRONG->WRONG(changed) | sweep | sweep | `true:sweep/zone/date_add_where/utc>la/read_utc` | [["2"], ["bigint"]] | RAISE type_coercion caused by Error during planning: Executi | RAISE repark_frame_settings caused by type_coercion caused b |

## Every non-EQUAL cell left in part 2's scope

| set | cell | head verdict | Spark | part1 | head |
|---|---|---|---|---|---|
| rc | `filter/add_sens/df/fadd/on_off` | REPARK_RAISES | [["2"], ["int"]] | [["2"], ["int"]] | RAISE ARITHMETIC_OVERFLOW |
| rc | `filter/agg_key_sens/df/fagg/on_off` | REPARK_RAISES | [["5.0"], ["double"]] | [["5.0"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| rc | `filter/and_free/df/fdiv/on_off` | REPARK_RAISES | [["5.0"], ["double"]] | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| rc | `filter/and_free/df/fdiv/on_on` | REPARK_RAISES | [["5.0"], ["double"]] | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| rc | `filter/cast_null/df/fcast/off_on` | REPARK_RAISES | [["null"], ["int"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| rc | `filter/numcast/df/fcast/off_on` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 3000000000 t | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| rc | `filter/numcast/df/fcast/on_off` | REPARK_RAISES | [["7"], ["int"]] | RAISE Arrow error: Cast error: Can't cast value 3000000000 t | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| rc | `filter/or_mixed/df/fdiv/on_off` | REPARK_RAISES | [["5.0"], ["double"]] | [["5.0"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| rev | `derive/crossjoin/add/on_off` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | [["-2147483648"], ["int"]] | [["-2147483648"], ["int"]] |
| rev | `derive/crossjoin/hour/utc_la` | VALUE | [["2"], ["int"]] | [["18"], ["int"]] | [["18"], ["int"]] |
| rev | `filter_sensitive/df/fdiv/on_off` | REPARK_RAISES | [["5.0"], ["double"]] | [["5.0"], ["double"]] | RAISE DIVIDE_BY_ZERO |
| run | `nest/read/dfsql/off/m` | BOTH_RAISE_OTHER | RAISE 'NoneType' object has no attribute 'contextType' | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| run | `nest/read/dfsql/on/m` | BOTH_RAISE_OTHER | RAISE 'NoneType' object has no attribute 'contextType' | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| run | `read/df/cte/off/off/cast_date` | REPARK_RAISES | [["null"], ["date"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/cte/off/off/cast_int` | REPARK_RAISES | [["null"], ["int"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/cte/off/off/cast_ovf` | REPARK_RAISES | [["-1"], ["smallint"]] | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run | `read/df/cte/off/off/map_struct` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/cte/off/off/seq` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/cte/off/on/cast_date` | REPARK_RAISES | [["null"], ["date"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/cte/off/on/cast_int` | REPARK_RAISES | [["null"], ["int"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/cte/off/on/cast_ovf` | REPARK_RAISES | [["-1"], ["smallint"]] | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run | `read/df/cte/off/on/map_struct` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/cte/off/on/seq` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/cte/on/off/arr_elt` | REPARK_ANSWERS | RAISE INVALID_ARRAY_INDEX_IN_ELEMENT_AT | [["null"], ["int"]] | [["null"], ["int"]] |
| run | `read/df/cte/on/off/arr_sub` | REPARK_ANSWERS | RAISE INVALID_ARRAY_INDEX | [["null"], ["int"]] | [["null"], ["int"]] |
| run | `read/df/cte/on/off/bool_dec` | BOTH_RAISE_OTHER | RAISE NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION | RAISE NUMERIC_VALUE_OUT_OF_RANGE | RAISE NUMERIC_VALUE_OUT_OF_RANGE |
| run | `read/df/cte/on/off/cast_date` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/cte/on/off/cast_int` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/cte/on/off/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run | `read/df/cte/on/off/dbl_lit` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/cte/on/off/int_bin` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/cte/on/off/mod` | BOTH_RAISE_OTHER | RAISE REMAINDER_BY_ZERO | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| run | `read/df/cte/on/off/seq` | REPARK_RAISES | [["[1,2,3]"], ["array<bigint>"]] | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/cte/on/on/arr_elt` | REPARK_ANSWERS | RAISE INVALID_ARRAY_INDEX_IN_ELEMENT_AT | [["null"], ["int"]] | [["null"], ["int"]] |
| run | `read/df/cte/on/on/arr_sub` | REPARK_ANSWERS | RAISE INVALID_ARRAY_INDEX | [["null"], ["int"]] | [["null"], ["int"]] |
| run | `read/df/cte/on/on/bool_dec` | BOTH_RAISE_OTHER | RAISE NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION | RAISE NUMERIC_VALUE_OUT_OF_RANGE | RAISE NUMERIC_VALUE_OUT_OF_RANGE |
| run | `read/df/cte/on/on/cast_date` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/cte/on/on/cast_int` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/cte/on/on/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run | `read/df/cte/on/on/dbl_lit` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/cte/on/on/int_bin` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/cte/on/on/mod` | BOTH_RAISE_OTHER | RAISE REMAINDER_BY_ZERO | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| run | `read/df/cte/on/on/seq` | REPARK_RAISES | [["[1,2,3]"], ["array<bigint>"]] | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/join/off/off/cast_date` | REPARK_RAISES | [["null"], ["date"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/join/off/off/cast_int` | REPARK_RAISES | [["null"], ["int"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/join/off/off/cast_ovf` | REPARK_RAISES | [["-1"], ["smallint"]] | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run | `read/df/join/off/off/map_struct` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/join/off/off/seq` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/join/off/on/cast_date` | REPARK_RAISES | [["null"], ["date"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/join/off/on/cast_int` | REPARK_RAISES | [["null"], ["int"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/join/off/on/cast_ovf` | REPARK_RAISES | [["-1"], ["smallint"]] | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run | `read/df/join/off/on/map_struct` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/join/off/on/seq` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/join/on/off/arr_elt` | REPARK_ANSWERS | RAISE INVALID_ARRAY_INDEX_IN_ELEMENT_AT | [["null"], ["int"]] | [["null"], ["int"]] |
| run | `read/df/join/on/off/arr_sub` | REPARK_ANSWERS | RAISE INVALID_ARRAY_INDEX | [["null"], ["int"]] | [["null"], ["int"]] |
| run | `read/df/join/on/off/bool_dec` | BOTH_RAISE_OTHER | RAISE NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION | RAISE NUMERIC_VALUE_OUT_OF_RANGE | RAISE NUMERIC_VALUE_OUT_OF_RANGE |
| run | `read/df/join/on/off/cast_date` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/join/on/off/cast_int` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/join/on/off/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run | `read/df/join/on/off/dbl_lit` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/join/on/off/int_bin` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/join/on/off/mod` | BOTH_RAISE_OTHER | RAISE REMAINDER_BY_ZERO | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| run | `read/df/join/on/off/seq` | REPARK_RAISES | [["[1,2,3]"], ["array<bigint>"]] | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/join/on/on/arr_elt` | REPARK_ANSWERS | RAISE INVALID_ARRAY_INDEX_IN_ELEMENT_AT | [["null"], ["int"]] | [["null"], ["int"]] |
| run | `read/df/join/on/on/arr_sub` | REPARK_ANSWERS | RAISE INVALID_ARRAY_INDEX | [["null"], ["int"]] | [["null"], ["int"]] |
| run | `read/df/join/on/on/bool_dec` | BOTH_RAISE_OTHER | RAISE NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION | RAISE NUMERIC_VALUE_OUT_OF_RANGE | RAISE NUMERIC_VALUE_OUT_OF_RANGE |
| run | `read/df/join/on/on/cast_date` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/join/on/on/cast_int` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/join/on/on/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run | `read/df/join/on/on/dbl_lit` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/join/on/on/int_bin` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/join/on/on/mod` | BOTH_RAISE_OTHER | RAISE REMAINDER_BY_ZERO | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| run | `read/df/join/on/on/seq` | REPARK_RAISES | [["[1,2,3]"], ["array<bigint>"]] | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/plain/ci/ci/cs_lower` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/plain/ci/ci/cs_twin` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/plain/ci/cs/cs_lower` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/plain/ci/cs/cs_twin` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/plain/cs/ci/ci_upper` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/plain/cs/ci/cs_lower` | REPARK_ANSWERS | RAISE AMBIGUOUS_REFERENCE | [["1"], ["int"]] | [["1"], ["int"]] |
| run | `read/df/plain/cs/ci/cs_twin` | REPARK_ANSWERS | RAISE AMBIGUOUS_REFERENCE | [["2"], ["int"]] | [["2"], ["int"]] |
| run | `read/df/plain/cs/cs/ci_upper` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/plain/leg/cor/tpp_parse` | REPARK_RAISES | [["1704067200"], ["bigint"]] | RAISE CANNOT_PARSE_TIMESTAMP | RAISE CANNOT_PARSE_TIMESTAMP |
| run | `read/df/plain/leg/cor/tpp_try` | VALUE | [["false"], ["boolean"]] | [["true"], ["boolean"]] | [["true"], ["boolean"]] |
| run | `read/df/plain/leg/leg/tpp_parse` | REPARK_RAISES | [["1704067200"], ["bigint"]] | RAISE CANNOT_PARSE_TIMESTAMP | RAISE CANNOT_PARSE_TIMESTAMP |
| run | `read/df/plain/leg/leg/tpp_try` | VALUE | [["false"], ["boolean"]] | [["true"], ["boolean"]] | [["true"], ["boolean"]] |
| run | `read/df/plain/ntz/ltz/tst_cast` | VALUE | [["timestamp_ntz"], ["string"]] | [["timestamp"], ["string"]] | [["timestamp"], ["string"]] |
| run | `read/df/plain/ntz/ltz/tst_lit` | VALUE | [["timestamp_ntz"], ["string"]] | [["timestamp"], ["string"]] | [["timestamp"], ["string"]] |
| run | `read/df/plain/ntz/ntz/tst_cast` | VALUE | [["timestamp_ntz"], ["string"]] | [["timestamp"], ["string"]] | [["timestamp"], ["string"]] |
| run | `read/df/plain/ntz/ntz/tst_lit` | VALUE | [["timestamp_ntz"], ["string"]] | [["timestamp"], ["string"]] | [["timestamp"], ["string"]] |
| run | `read/df/plain/off/off/cast_date` | REPARK_RAISES | [["null"], ["date"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/plain/off/off/cast_int` | REPARK_RAISES | [["null"], ["int"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/plain/off/off/cast_ovf` | REPARK_RAISES | [["-1"], ["smallint"]] | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run | `read/df/plain/off/off/map_struct` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/plain/off/off/seq` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/plain/off/on/cast_date` | REPARK_RAISES | [["null"], ["date"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/plain/off/on/cast_int` | REPARK_RAISES | [["null"], ["int"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/plain/off/on/cast_ovf` | REPARK_RAISES | [["-1"], ["smallint"]] | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run | `read/df/plain/off/on/map_struct` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/plain/off/on/seq` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/plain/on/off/arr_elt` | REPARK_ANSWERS | RAISE INVALID_ARRAY_INDEX_IN_ELEMENT_AT | [["null"], ["int"]] | [["null"], ["int"]] |
| run | `read/df/plain/on/off/arr_sub` | REPARK_ANSWERS | RAISE INVALID_ARRAY_INDEX | [["null"], ["int"]] | [["null"], ["int"]] |
| run | `read/df/plain/on/off/bool_dec` | BOTH_RAISE_OTHER | RAISE NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION | RAISE NUMERIC_VALUE_OUT_OF_RANGE | RAISE NUMERIC_VALUE_OUT_OF_RANGE |
| run | `read/df/plain/on/off/cast_date` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/plain/on/off/cast_int` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/plain/on/off/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run | `read/df/plain/on/off/dbl_lit` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/plain/on/off/int_bin` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/plain/on/off/mod` | BOTH_RAISE_OTHER | RAISE REMAINDER_BY_ZERO | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| run | `read/df/plain/on/off/seq` | REPARK_RAISES | [["[1,2,3]"], ["array<bigint>"]] | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/plain/on/on/arr_elt` | REPARK_ANSWERS | RAISE INVALID_ARRAY_INDEX_IN_ELEMENT_AT | [["null"], ["int"]] | [["null"], ["int"]] |
| run | `read/df/plain/on/on/arr_sub` | REPARK_ANSWERS | RAISE INVALID_ARRAY_INDEX | [["null"], ["int"]] | [["null"], ["int"]] |
| run | `read/df/plain/on/on/bool_dec` | BOTH_RAISE_OTHER | RAISE NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION | RAISE NUMERIC_VALUE_OUT_OF_RANGE | RAISE NUMERIC_VALUE_OUT_OF_RANGE |
| run | `read/df/plain/on/on/cast_date` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/plain/on/on/cast_int` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/plain/on/on/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run | `read/df/plain/on/on/dbl_lit` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/plain/on/on/int_bin` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/plain/on/on/mod` | BOTH_RAISE_OTHER | RAISE REMAINDER_BY_ZERO | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| run | `read/df/plain/on/on/seq` | REPARK_RAISES | [["[1,2,3]"], ["array<bigint>"]] | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/subq/off/off/cast_date` | REPARK_RAISES | [["null"], ["date"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/subq/off/off/cast_int` | REPARK_RAISES | [["null"], ["int"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/subq/off/off/cast_ovf` | REPARK_RAISES | [["-1"], ["smallint"]] | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run | `read/df/subq/off/off/map_struct` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/subq/off/off/seq` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/subq/off/on/cast_date` | REPARK_RAISES | [["null"], ["date"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/subq/off/on/cast_int` | REPARK_RAISES | [["null"], ["int"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/subq/off/on/cast_ovf` | REPARK_RAISES | [["-1"], ["smallint"]] | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run | `read/df/subq/off/on/map_struct` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/subq/off/on/seq` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/subq/on/off/arr_elt` | REPARK_ANSWERS | RAISE INVALID_ARRAY_INDEX_IN_ELEMENT_AT | [["null"], ["int"]] | [["null"], ["int"]] |
| run | `read/df/subq/on/off/arr_sub` | REPARK_ANSWERS | RAISE INVALID_ARRAY_INDEX | [["null"], ["int"]] | [["null"], ["int"]] |
| run | `read/df/subq/on/off/bool_dec` | BOTH_RAISE_OTHER | RAISE NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION | RAISE NUMERIC_VALUE_OUT_OF_RANGE | RAISE NUMERIC_VALUE_OUT_OF_RANGE |
| run | `read/df/subq/on/off/cast_date` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/subq/on/off/cast_int` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/subq/on/off/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run | `read/df/subq/on/off/dbl_lit` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/subq/on/off/int_bin` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/subq/on/off/mod` | BOTH_RAISE_OTHER | RAISE REMAINDER_BY_ZERO | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| run | `read/df/subq/on/off/seq` | REPARK_RAISES | [["[1,2,3]"], ["array<bigint>"]] | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/subq/on/on/arr_elt` | REPARK_ANSWERS | RAISE INVALID_ARRAY_INDEX_IN_ELEMENT_AT | [["null"], ["int"]] | [["null"], ["int"]] |
| run | `read/df/subq/on/on/arr_sub` | REPARK_ANSWERS | RAISE INVALID_ARRAY_INDEX | [["null"], ["int"]] | [["null"], ["int"]] |
| run | `read/df/subq/on/on/bool_dec` | BOTH_RAISE_OTHER | RAISE NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION | RAISE NUMERIC_VALUE_OUT_OF_RANGE | RAISE NUMERIC_VALUE_OUT_OF_RANGE |
| run | `read/df/subq/on/on/cast_date` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/subq/on/on/cast_int` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/df/subq/on/on/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run | `read/df/subq/on/on/dbl_lit` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/subq/on/on/int_bin` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/df/subq/on/on/mod` | BOTH_RAISE_OTHER | RAISE REMAINDER_BY_ZERO | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| run | `read/df/subq/on/on/seq` | REPARK_RAISES | [["[1,2,3]"], ["array<bigint>"]] | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/dff/plain/ci/cs/cs_lower` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/dff/plain/ci/cs/cs_twin` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/dff/plain/cor/leg/tpp_parse` | REPARK_RAISES | [["1704067200"], ["bigint"]] | RAISE CANNOT_PARSE_TIMESTAMP | RAISE CANNOT_PARSE_TIMESTAMP |
| run | `read/dff/plain/cor/leg/tpp_try` | VALUE | [["false"], ["boolean"]] | [["true"], ["boolean"]] | [["true"], ["boolean"]] |
| run | `read/dff/plain/cs/ci/ci_upper` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/dff/plain/cs/ci/cs_lower` | REPARK_ANSWERS | RAISE AMBIGUOUS_REFERENCE | [["1"], ["int"]] | [["1"], ["int"]] |
| run | `read/dff/plain/cs/ci/cs_twin` | REPARK_ANSWERS | RAISE AMBIGUOUS_REFERENCE | [["2"], ["int"]] | [["2"], ["int"]] |
| run | `read/dff/plain/ntz/ltz/tst_cast` | VALUE | [["timestamp_ntz"], ["string"]] | [["timestamp"], ["string"]] | [["timestamp"], ["string"]] |
| run | `read/dff/plain/ntz/ltz/tst_lit` | VALUE | [["timestamp_ntz"], ["string"]] | [["timestamp"], ["string"]] | [["timestamp"], ["string"]] |
| run | `read/dff/plain/off/on/cast_date` | REPARK_RAISES | [["null"], ["date"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/dff/plain/off/on/cast_int` | REPARK_RAISES | [["null"], ["int"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/dff/plain/off/on/cast_ovf` | REPARK_RAISES | [["-1"], ["smallint"]] | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run | `read/dff/plain/off/on/map_struct` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/dff/plain/off/on/seq` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/dff/plain/on/off/arr_elt` | REPARK_ANSWERS | RAISE INVALID_ARRAY_INDEX_IN_ELEMENT_AT | [["null"], ["int"]] | [["null"], ["int"]] |
| run | `read/dff/plain/on/off/arr_sub` | REPARK_ANSWERS | RAISE INVALID_ARRAY_INDEX | [["null"], ["int"]] | [["null"], ["int"]] |
| run | `read/dff/plain/on/off/bool_dec` | BOTH_RAISE_OTHER | RAISE NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION | RAISE NUMERIC_VALUE_OUT_OF_RANGE | RAISE NUMERIC_VALUE_OUT_OF_RANGE |
| run | `read/dff/plain/on/off/cast_date` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/dff/plain/on/off/cast_int` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/dff/plain/on/off/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run | `read/dff/plain/on/off/dbl_lit` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/dff/plain/on/off/int_bin` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/dff/plain/on/off/mod` | BOTH_RAISE_OTHER | RAISE REMAINDER_BY_ZERO | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| run | `read/dff/plain/on/off/seq` | REPARK_RAISES | [["[1,2,3]"], ["array<bigint>"]] | RAISE Error during planning: table 'spark_catalog.default.df | RAISE Error during planning: table 'spark_catalog.default.df |
| run | `read/vsd/plain/off/off/cast_date` | REPARK_RAISES | [["null"], ["date"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/vsd/plain/off/off/cast_int` | REPARK_RAISES | [["null"], ["int"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/vsd/plain/off/off/cast_ovf` | REPARK_RAISES | [["-1"], ["smallint"]] | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run | `read/vsd/plain/off/off/map_struct` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.vs | RAISE Error during planning: table 'spark_catalog.default.vs |
| run | `read/vsd/plain/off/off/seq` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.vs | RAISE Error during planning: table 'spark_catalog.default.vs |
| run | `read/vsd/plain/off/on/cast_date` | REPARK_RAISES | [["null"], ["date"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/vsd/plain/off/on/cast_int` | REPARK_RAISES | [["null"], ["int"]] | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/vsd/plain/off/on/cast_ovf` | REPARK_RAISES | [["-1"], ["smallint"]] | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run | `read/vsd/plain/off/on/map_struct` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.vs | RAISE Error during planning: table 'spark_catalog.default.vs |
| run | `read/vsd/plain/off/on/seq` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.vs | RAISE Error during planning: table 'spark_catalog.default.vs |
| run | `read/vsd/plain/on/off/arr_elt` | REPARK_ANSWERS | RAISE INVALID_ARRAY_INDEX_IN_ELEMENT_AT | [["null"], ["int"]] | [["null"], ["int"]] |
| run | `read/vsd/plain/on/off/arr_sub` | REPARK_ANSWERS | RAISE INVALID_ARRAY_INDEX | [["null"], ["int"]] | [["null"], ["int"]] |
| run | `read/vsd/plain/on/off/bool_dec` | BOTH_RAISE_OTHER | RAISE NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION | RAISE NUMERIC_VALUE_OUT_OF_RANGE | RAISE NUMERIC_VALUE_OUT_OF_RANGE |
| run | `read/vsd/plain/on/off/cast_date` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/vsd/plain/on/off/cast_int` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/vsd/plain/on/off/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run | `read/vsd/plain/on/off/dbl_lit` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Error during planning: table 'spark_catalog.default.vs | RAISE Error during planning: table 'spark_catalog.default.vs |
| run | `read/vsd/plain/on/off/int_bin` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.vs | RAISE Error during planning: table 'spark_catalog.default.vs |
| run | `read/vsd/plain/on/off/mod` | BOTH_RAISE_OTHER | RAISE REMAINDER_BY_ZERO | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| run | `read/vsd/plain/on/off/seq` | REPARK_RAISES | [["[1,2,3]"], ["array<bigint>"]] | RAISE Error during planning: table 'spark_catalog.default.vs | RAISE Error during planning: table 'spark_catalog.default.vs |
| run | `read/vsd/plain/on/on/arr_elt` | REPARK_ANSWERS | RAISE INVALID_ARRAY_INDEX_IN_ELEMENT_AT | [["null"], ["int"]] | [["null"], ["int"]] |
| run | `read/vsd/plain/on/on/arr_sub` | REPARK_ANSWERS | RAISE INVALID_ARRAY_INDEX | [["null"], ["int"]] | [["null"], ["int"]] |
| run | `read/vsd/plain/on/on/bool_dec` | BOTH_RAISE_OTHER | RAISE NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION | RAISE NUMERIC_VALUE_OUT_OF_RANGE | RAISE NUMERIC_VALUE_OUT_OF_RANGE |
| run | `read/vsd/plain/on/on/cast_date` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/vsd/plain/on/on/cast_int` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v | RAISE Arrow error: Cast error: Cannot cast string 'abc' to v |
| run | `read/vsd/plain/on/on/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run | `read/vsd/plain/on/on/dbl_lit` | BOTH_RAISE_OTHER | RAISE CAST_INVALID_INPUT | RAISE Error during planning: table 'spark_catalog.default.vs | RAISE Error during planning: table 'spark_catalog.default.vs |
| run | `read/vsd/plain/on/on/int_bin` | BOTH_RAISE_OTHER | RAISE TABLE_OR_VIEW_NOT_FOUND | RAISE Error during planning: table 'spark_catalog.default.vs | RAISE Error during planning: table 'spark_catalog.default.vs |
| run | `read/vsd/plain/on/on/mod` | BOTH_RAISE_OTHER | RAISE REMAINDER_BY_ZERO | RAISE DIVIDE_BY_ZERO | RAISE DIVIDE_BY_ZERO |
| run | `read/vsd/plain/on/on/seq` | REPARK_RAISES | [["[1,2,3]"], ["array<bigint>"]] | RAISE Error during planning: table 'spark_catalog.default.vs | RAISE Error during planning: table 'spark_catalog.default.vs |
| run | `write/ctas-df/off/on/cast_ovf` | REPARK_RAISES | [["-1"], ["int"]] | RAISE datafusion engine error: Arrow error: Cast error: Can' | RAISE datafusion engine error: Arrow error: Cast error: Can' |
| run | `write/ctas-df/on/off/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE datafusion engine error: Arrow error: Cast error: Can' | RAISE datafusion engine error: Arrow error: Cast error: Can' |
| run | `write/parquet-held/off/on/cast_ovf` | REPARK_RAISES | [["-1"], ["smallint"]] | RAISE datafusion engine error: Arrow error: Cast error: Can' | RAISE datafusion engine error: Arrow error: Cast error: Can' |
| run | `write/parquet-held/on/off/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE datafusion engine error: Arrow error: Cast error: Can' | RAISE datafusion engine error: Arrow error: Cast error: Can' |
| run | `write/writeto-held/off/on/avg` | REPARK_ANSWERS | RAISE ARITHMETIC_OVERFLOW | [["null"], ["decimal(38,4)"]] | [["null"], ["decimal(38,4)"]] |
| run | `write/writeto-held/off/on/cast_ovf` | REPARK_RAISES | [["-1"], ["int"]] | RAISE datafusion engine error: Arrow error: Cast error: Can' | RAISE datafusion engine error: Arrow error: Cast error: Can' |
| run | `write/writeto-held/on/off/avg` | REPARK_RAISES | [["null"], ["decimal(38,4)"]] | [["null"], ["decimal(38,4)"]] | RAISE ARITHMETIC_OVERFLOW |
| run | `write/writeto-held/on/off/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE datafusion engine error: Arrow error: Cast error: Can' | RAISE datafusion engine error: Arrow error: Cast error: Can' |
| run-unset | `unset/read/df/off/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |
| run-unset | `unset/read/df/unset/cast_ovf` | BOTH_RAISE_OTHER | RAISE CAST_OVERFLOW | RAISE Arrow error: Cast error: Can't cast value 2147483647 t | RAISE Arrow error: Cast error: Can't cast value 2147483647 t |

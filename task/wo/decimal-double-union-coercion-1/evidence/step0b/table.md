# DECIMAL-DOUBLE-UNION-COERCION-1 step 0′ on main `dc6b4c94` — the table

Rendered by `probes/render_step0b.py` from [cells/](cells/). Spark 4.1.2 on Zulu JDK 17 against main
`dc6b4c94`, both ANSI modes. Every joined cell also carries the recorded `ff34baf5` answer and the step-0 main
(`6f9f2aaa`) answer. The provenance is in [map.md](./).

## 1. The joined cells per section

| section | ANSI | cells | wrong on main `dc6b4c94` | wrong on `ff34baf5` (recorded) | wrong on `6f9f2aaa` (recorded) | moved since `6f9f2aaa` |
|---|---|---|---|---|---|---|
| dataframe-control | false | 72 | 48 | 48 | 48 | 0 |
| dataframe-control | true | 72 | 50 | 50 | 50 | 0 |
| dataframe-dd | false | 131 | 125 | 125 | 125 | 0 |
| dataframe-dd | true | 131 | 125 | 125 | 125 | 0 |
| dml-flip-native | false | 15 | 4 | 4 | 4 | 0 |
| dml-flip-native | true | 15 | 4 | 4 | 4 | 0 |
| dml-flip | false | 15 | 3 | 4 | 4 | 1 |
| dml-flip | true | 15 | 3 | 4 | 4 | 1 |
| dml-native | false | 41 | 31 | 32 | 31 | 0 |
| dml-native | true | 41 | 31 | 32 | 31 | 0 |
| dml | false | 82 | 45 | 47 | 45 | 0 |
| dml | true | 82 | 45 | 47 | 45 | 0 |
| fexpr-dd | false | 12 | 9 | 9 | 9 | 0 |
| fexpr-dd | true | 12 | 9 | 9 | 9 | 0 |
| native-control | native | 792 | 370 | 370 | 370 | 0 |
| native-dd | native | 1193 | 473 | 473 | 473 | 0 |
| revision-select | false | 28 | 9 | 10 | 10 | 1 |
| revision-select | true | 28 | 9 | 10 | 10 | 1 |
| revision-write | false | 24 | 20 | 20 | 20 | 0 |
| revision-write | true | 24 | 20 | 20 | 20 | 0 |
| sql-control | false | 792 | 222 | 222 | 222 | 0 |
| sql-control | true | 792 | 312 | 312 | 312 | 0 |
| sql-dd | false | 1193 | 935 | 935 | 935 | 0 |
| sql-dd | true | 1193 | 935 | 935 | 935 | 0 |
| write | false | 30 | 24 | 23 | 24 | 0 |
| write | true | 30 | 24 | 23 | 24 | 0 |

## 2. Wrong on main, by part

| part | true | false | native |
|---|---|---|---|
| Q8 PRUNED-WRONG (main's answer kept) | 2 | 2 | 0 |
| card DECIMAL-DOUBLE-NESTED-1 | 6 | 6 | 0 |
| card DECIMAL-FLOAT-ARITH-1 | 12 | 12 | 0 |
| control root (not this unit) | 362 | 270 | 0 |
| excluded: recursive CTE (RECURSIVE-CTE-JOIN-ERROR-1) | 2 | 2 | 0 |
| native (Q2) | 35 | 35 | 843 |
| part 1 | 1064 | 1064 | 0 |
| part 2 (seat V) | 84 | 84 | 0 |

Per section (ANSI on):

| part | sections (wrong on main) |
|---|---|
| Q8 PRUNED-WRONG (main's answer kept) | revision-write 2 |
| card DECIMAL-DOUBLE-NESTED-1 | revision-select 6 |
| card DECIMAL-FLOAT-ARITH-1 | sql-dd 12 |
| control root (not this unit) | dataframe-control 50, sql-control 312 |
| excluded: recursive CTE (RECURSIVE-CTE-JOIN-ERROR-1) | revision-select 1, sql-dd 1 |
| native (Q2) | dml-flip-native 4, dml-native 31 |
| part 1 | dataframe-dd 125, dml 45, dml-flip 3, fexpr-dd 9, revision-write 11, sql-dd 849, write 22 |
| part 2 (seat V) | revision-select 2, revision-write 7, sql-dd 73, write 2 |

Part 1 counts the cells whose unifying context seat E widens (comparisons, `IN`, `BETWEEN`, set operations without a
`VALUES` input, CASE / `IF` / `coalesce` / `nvl2` / `nullif` / `greatest` / `least` / `array` / `map`, the DML
predicates and the writes they feed) and the written-cast flips. Part 2 counts every cell with a `VALUES` input.
The joined cells are classified by context; the fuzz, structured and DML sets below are measured on the emulated head.

## 3. Moved since `6f9f2aaa`

| section | cell | ANSI | `6f9f2aaa` | `dc6b4c94` | Spark |
|---|---|---|---|---|---|
| revision-select | `select/zero_eq_widened` | true | boolean false (wrong-value) | boolean true (equal) | boolean true |
| dml-flip | `flip/c1/-22.129547339983247` | true | false (WRONG) | true (EQUAL) | true |
| revision-select | `select/zero_eq_widened` | false | boolean false (wrong-value) | boolean true (equal) | boolean true |
| dml-flip | `flip/c1/-22.129547339983247` | false | false (WRONG) | true (EQUAL) | true |

## 4. The signed-zero shapes on main (critic item 7)

| cell | ANSI | Spark | main | verdict |
|---|---|---|---|---|
| `select/zero_eq_today` | true | boolean true | boolean true | equal |
| `select/zero_eq_widened` | true | boolean true | boolean true | equal |
| `select/zero_join_today` | true | bigint 1 | bigint 1 | equal |
| `select/zero_join_widened` | true | bigint 1 | bigint 1 | equal |
| `select/zero_intersect_today` | true | bigint 1 | bigint 1 | equal |
| `select/zero_intersect_widened` | true | bigint 1 | bigint 1 | equal |
| `select/zero_except_today` | true | bigint 0 | bigint 0 | equal |
| `select/zero_except_widened` | true | bigint 0 | bigint 0 | equal |
| `select/zero_union_distinct_today` | true | bigint 1 | bigint 1 | equal |
| `select/zero_union_distinct_widened` | true | bigint 1 | bigint 1 | equal |
| `select/zero_group_by_today` | true | bigint 1 | bigint 1 | equal |
| `select/zero_group_by_widened` | true | bigint 1 | bigint 1 | equal |
| `select/zero_in_subquery_today` | true | bigint 1 | bigint 1 | equal |
| `select/zero_in_subquery_widened` | true | bigint 1 | bigint 1 | equal |
| `merge_on_zero_today` | true | decimal(10,2) 1=-0.0=0.00 | decimal(10,2) 99=-0.0=0.00 | WRONG-WRITE |
| `merge_on_zero_widened` | true | decimal(10,2) 1=-0.0=0.00 | decimal(10,2) 99=-0.0=0.00 | WRONG-WRITE |
| `delete_zero_today` | true | decimal(10,2)  | decimal(10,2)  | EQUAL |
| `delete_zero_widened` | true | decimal(10,2)  | decimal(10,2)  | EQUAL |
| `select/zero_eq_today` | false | boolean true | boolean true | equal |
| `select/zero_eq_widened` | false | boolean true | boolean true | equal |
| `select/zero_join_today` | false | bigint 1 | bigint 1 | equal |
| `select/zero_join_widened` | false | bigint 1 | bigint 1 | equal |
| `select/zero_intersect_today` | false | bigint 1 | bigint 1 | equal |
| `select/zero_intersect_widened` | false | bigint 1 | bigint 1 | equal |
| `select/zero_except_today` | false | bigint 0 | bigint 0 | equal |
| `select/zero_except_widened` | false | bigint 0 | bigint 0 | equal |
| `select/zero_union_distinct_today` | false | bigint 1 | bigint 1 | equal |
| `select/zero_union_distinct_widened` | false | bigint 1 | bigint 1 | equal |
| `select/zero_group_by_today` | false | bigint 1 | bigint 1 | equal |
| `select/zero_group_by_widened` | false | bigint 1 | bigint 1 | equal |
| `select/zero_in_subquery_today` | false | bigint 1 | bigint 1 | equal |
| `select/zero_in_subquery_widened` | false | bigint 1 | bigint 1 | equal |
| `merge_on_zero_today` | false | decimal(10,2) 1=-0.0=0.00 | decimal(10,2) 99=-0.0=0.00 | WRONG-WRITE |
| `merge_on_zero_widened` | false | decimal(10,2) 1=-0.0=0.00 | decimal(10,2) 99=-0.0=0.00 | WRONG-WRITE |
| `delete_zero_today` | false | decimal(10,2)  | decimal(10,2)  | EQUAL |
| `delete_zero_widened` | false | decimal(10,2)  | decimal(10,2)  | EQUAL |

## 5. The emulated head on verifier 2's coercion fuzz

Variants, each run on main's package: `seat_e` casts every DECIMAL operand to DOUBLE by hand (the head that gave N2's
2,761 on `ff34baf5`); `k1` adds K1 on the written casts (the double's JDK 17 text from main, rounded half up at the
scale outside SQL, then fed back as exact digits); `k1k2` also routes every DECIMAL→DOUBLE through its text (K2).
A held cell is wrong on the head and right on main (or, N2's definition, right on main or on `ff34baf5`).

| ANSI | cells | main wrong | variant | head wrong | held vs main | held vs main or `ff34` | fixed vs main |
|---|---|---|---|---|---|---|---|
| true | 165846 | 72458 | `ff34baf5` seat E (recorded run, re-joined) | 3110 | - | 3104 | - |
| true | 165846 | 72458 | seat_e on main | 1527 | 1521 | 1521 | 72452 |
| true | 165846 | 72458 | k1 on main | 2767 | 2761 | 2761 | 72452 |
| true | 165846 | 72458 | k1k2 on main | 6 | 0 | 0 | 72452 |
| false | 186570 | 92272 | `ff34baf5` seat E (recorded run, re-joined) | 22924 | - | 3104 | - |
| false | 186570 | 92272 | seat_e on main | 21341 | 1521 | 1521 | 72452 |
| false | 186570 | 92272 | k1 on main | 22581 | 2761 | 2761 | 72452 |
| false | 186570 | 92272 | k1k2 on main | 19820 | 0 | 0 | 72452 |

Held cells by shape family (ANSI on):

| family | cells | main wrong | seat_e held | k1 held | k1k2 held | k1k2 wrong |
|---|---|---|---|---|---|---|
| all/all | 165846 | 72458 | 1521 | 2761 | 0 | 6 |
| coalesce/d38_18 | 3455 | 3276 | 0 | 0 | 0 | 0 |
| coalesce/other | 17275 | 15880 | 0 | 0 | 0 | 0 |
| eq/d38_18 | 3455 | 337 | 298 | 553 | 0 | 0 |
| eq/other | 13821 | 2576 | 0 | 0 | 0 | 1 |
| eqf/d38_18 | 3455 | 378 | 333 | 552 | 0 | 0 |
| eqf/other | 13821 | 3037 | 0 | 0 | 0 | 1 |
| greatest/d38_18 | 3455 | 3276 | 0 | 0 | 0 | 0 |
| greatest/other | 17275 | 15880 | 0 | 0 | 0 | 0 |
| if/d38_18 | 3455 | 3276 | 0 | 0 | 0 | 0 |
| if/other | 17275 | 15880 | 0 | 0 | 0 | 0 |
| in/d38_18 | 3455 | 337 | 298 | 553 | 0 | 0 |
| in/other | 13821 | 2576 | 0 | 0 | 0 | 1 |
| le/d38_18 | 3455 | 154 | 147 | 275 | 0 | 0 |
| le/other | 13821 | 1264 | 0 | 0 | 0 | 1 |
| lt/d38_18 | 3455 | 154 | 147 | 275 | 0 | 0 |
| lt/other | 13821 | 1264 | 0 | 0 | 0 | 1 |
| ns/d38_18 | 3455 | 337 | 298 | 553 | 0 | 0 |
| ns/other | 13821 | 2576 | 0 | 0 | 0 | 1 |

The `k1k2` wrong cells with ANSI on are the six `DECIMAL(5,0)` shapes that raise as a whole on both engines with
different texts. With ANSI off, every `DECIMAL(5,0)` shape is wrong on main, on `ff34baf5` and on every head: a
non-ANSI `CAST(double AS DECIMAL(5,0))` that overflows raises (`Arrow error: Invalid argument … too large to store`)
where Spark returns NULL for the row, so the shape fails as a whole. That root is outside the unit's seats.

## 6. The kernels (5,455 values per type)

| ANSI | kernel | type | main's cast vs Spark | Spark's text route vs Spark | main's SQL text route vs Spark | main's half-up route vs Spark |
|---|---|---|---|---|---|---|
| false | k1f | d10_2 | 0 | 0 | 329 | 0 |
| false | k1f | d16_4 | 0 | 0 | 329 | 0 |
| false | k1f | d20_6 | 0 | 0 | 329 | 0 |
| false | k1f | d30_15 | 3384 | 0 | 329 | 0 |
| false | k1f | d38_10 | 0 | 0 | 329 | 0 |
| false | k1f | d38_18 | 4726 | 0 | 329 | 0 |
| false | k1 | d10_2 | 1 | 0 | 332 | 0 |
| false | k1 | d16_4 | 1 | 0 | 332 | 0 |
| false | k1 | d20_6 | 0 | 0 | 332 | 0 |
| false | k1 | d30_15 | 3430 | 0 | 332 | 0 |
| false | k1 | d38_10 | 300 | 0 | 332 | 0 |
| false | k1 | d38_18 | 4601 | 0 | 332 | 0 |
| false | k2 | d10_2 | 0 | 0 | 0 | - |
| false | k2 | d16_4 | 0 | 0 | 0 | - |
| false | k2 | d20_6 | 0 | 0 | 0 | - |
| false | k2 | d30_15 | 724 | 0 | 0 | - |
| false | k2 | d38_10 | 118 | 0 | 0 | - |
| false | k2 | d38_18 | 967 | 0 | 0 | - |
| true | k1f | d10_2 | 0 | 0 | 329 | 0 |
| true | k1f | d16_4 | 0 | 0 | 329 | 0 |
| true | k1f | d20_6 | 0 | 0 | 329 | 0 |
| true | k1f | d30_15 | 3384 | 0 | 329 | 0 |
| true | k1f | d38_10 | 0 | 0 | 329 | 0 |
| true | k1f | d38_18 | 4726 | 0 | 329 | 0 |
| true | k1 | d10_2 | 1 | 0 | 332 | 0 |
| true | k1 | d16_4 | 1 | 0 | 332 | 0 |
| true | k1 | d20_6 | 0 | 0 | 332 | 0 |
| true | k1 | d30_15 | 3430 | 0 | 332 | 0 |
| true | k1 | d38_10 | 300 | 0 | 332 | 0 |
| true | k1 | d38_18 | 4601 | 0 | 332 | 0 |
| true | k2 | d10_2 | 0 | 0 | 0 | - |
| true | k2 | d16_4 | 0 | 0 | 0 | - |
| true | k2 | d20_6 | 0 | 0 | 0 | - |
| true | k2 | d30_15 | 724 | 0 | 0 | - |
| true | k2 | d38_10 | 118 | 0 | 0 | - |
| true | k2 | d38_18 | 967 | 0 | 0 | - |

`k1` is DOUBLE→DECIMAL, `k1f` FLOAT→DECIMAL (through the widened double), `k2` DECIMAL→DOUBLE. RePark's SQL text
route for K1 fails on the 332 (DOUBLE) and 329 (FLOAT) values whose JDK 17 text is in E-notation: main's
`TRY_CAST('1.0E-4' AS DECIMAL(10,2))` is NULL where Spark's is `0.00`. That is a separate STRING→DECIMAL gap, so the
emulation uses the half-up route, which matches Spark on every value.

## 7. The structured `d = v` set (60,000 rows)

| ANSI | variant | predicate | main wrong | head wrong | held vs main |
|---|---|---|---|---|---|
| false | k1k2 | eq | 2501 | 0 | 0 |
| false | k1k2 | in | 2501 | 0 | 0 |
| false | k1k2 | lt | 1084 | 0 | 0 |
| false | k1k2 | ns | 2501 | 0 | 0 |
| false | seat_e | eq | 2501 | 0 | 0 |
| false | seat_e | in | 2501 | 0 | 0 |
| false | seat_e | lt | 1084 | 0 | 0 |
| false | seat_e | ns | 2501 | 0 | 0 |
| true | k1k2 | eq | 2501 | 0 | 0 |
| true | k1k2 | in | 2501 | 0 | 0 |
| true | k1k2 | lt | 1084 | 0 | 0 |
| true | k1k2 | ns | 2501 | 0 | 0 |
| true | seat_e | eq | 2501 | 0 | 0 |
| true | seat_e | in | 2501 | 0 | 0 |
| true | seat_e | lt | 1084 | 0 | 0 |
| true | seat_e | ns | 2501 | 0 | 0 |

## 8. The DML cells on the emulated head

| ANSI | door | variant | cells | main wrong | head wrong | held vs main | fixed vs main |
|---|---|---|---|---|---|---|---|
| false | native | k1k2 | 56 | 35 | 18 | 1 | 18 |
| false | native | seat_e | 56 | 35 | 27 | 2 | 10 |
| false | repark | k1k2 | 97 | 48 | 2 | 0 | 46 |
| false | repark | seat_e | 97 | 48 | 29 | 2 | 21 |
| true | native | k1k2 | 56 | 35 | 18 | 1 | 18 |
| true | native | seat_e | 56 | 35 | 27 | 2 | 10 |
| true | repark | k1k2 | 97 | 48 | 2 | 0 | 46 |
| true | repark | seat_e | 97 | 48 | 29 | 2 | 21 |

The native-door rows are for information: part 1 does not widen that door (Q2), so its head is main.

Wrong on the Spark-door `k1k2` head:

| ANSI | cell | Spark | main | head |
|---|---|---|---|---|
| true | `z/cow/update_in` | [3, 101, 102] | [101, 102, 103] | [1, 3, 102] |
| true | `z/mor/update_in` | [3, 101, 102] | [101, 102, 103] | [1, 3, 102] |
| false | `z/cow/update_in` | [3, 101, 102] | [101, 102, 103] | [1, 3, 102] |
| false | `z/mor/update_in` | [3, 101, 102] | [101, 102, 103] | [1, 3, 102] |

Held on the `seat_e` head (Spark door), which K1 and K2 remove:

| ANSI | cell | Spark | main | head |
|---|---|---|---|---|
| true | `flip/c3/-22.129547339983247` | true | true | false |
| true | `flip/c7/-22.129547339983247` | true | true | false |
| false | `flip/c3/-22.129547339983247` | true | true | false |
| false | `flip/c7/-22.129547339983247` | true | true | false |

## 9. Spark's widened forms

| set | variant | ANSI | cells | Spark answers the widened form differently |
|---|---|---|---|---|
| coerce | k1k2 | false | 186570 | 0 |
| coerce | k1k2 | true | 165846 | 1 |
| coerce | k1 | false | 186570 | 0 |
| coerce | k1 | true | 165846 | 1 |
| coerce | seat_e | false | 186570 | 0 |
| coerce | seat_e | true | 165846 | 0 |
| dml | k1k2 | true | 97 | 0 |
| dml | seat_e | true | 97 | 0 |

The one fuzz cell is `eq_round_d5_0` / `eqf_round_d5_0` with ANSI on: a whole-shape overflow whose message names a
different first value on each Spark run (`local[2]` partition order). Spark's fresh fuzz run differs from its
2026-11-14 recording in that cell only.

# Step 0: the table on main, on the stack base `ff34baf5` and on the emulated head

Spark 4.1.2 (JDK 17, Iceberg 1.11.0) against main (`6f9f2aaa`) and the stack base (`ff34baf5`), both ANSI modes;
provenance in [map.md](map.md). A cell is wrong when its verdict is not `equal` / `EQUAL` / `BOTH-RAISE`. The
emulated head is the widened form (every DECIMAL operand of a DOUBLE comparison or union cast to DOUBLE by hand)
run on `ff34baf5`; Spark answers every widened form as it answers the original (the faithfulness line).

**The split of the 2,761 held fuzz cells per mode** (`split_roots.py`): 1,561 are fixed by an exact
DECIMAL→DOUBLE cast (256 each in `eq`, `in`, `ns` at DECIMAL(38,18), 129 each in `lt`, `le`, all 535 `eqf`); the
other 1,200 have RePark's `ROUND(v, 18) = v` false where Spark's is true (verifier 2's c3).

| section | ANSI | cells | main wrong | ff34 wrong | matched on main, wrong on ff34 | fixed on ff34 | wrong on both |
|---|---|---|---|---|---|---|---|
| dataframe-control | false | 72 | 48 | 48 | 0 | 0 | 48 |
| dataframe-control | true | 72 | 50 | 50 | 0 | 0 | 50 |
| dataframe-dd | false | 131 | 125 | 125 | 0 | 0 | 125 |
| dataframe-dd | true | 131 | 125 | 125 | 0 | 0 | 125 |
| dml | false | 82 | 45 | 47 | 26 | 24 | 21 |
| dml | true | 82 | 45 | 47 | 26 | 24 | 21 |
| dml-flip | false | 15 | 4 | 4 | 3 | 3 | 1 |
| dml-flip | true | 15 | 4 | 4 | 3 | 3 | 1 |
| dml-flip-native | false | 15 | 4 | 4 | 3 | 3 | 1 |
| dml-flip-native | true | 15 | 4 | 4 | 3 | 3 | 1 |
| dml-native | false | 41 | 31 | 32 | 8 | 7 | 24 |
| dml-native | true | 41 | 31 | 32 | 8 | 7 | 24 |
| fexpr-dd | false | 12 | 9 | 9 | 0 | 0 | 9 |
| fexpr-dd | true | 12 | 9 | 9 | 0 | 0 | 9 |
| native-control | native | 792 | 370 | 370 | 0 | 0 | 370 |
| native-dd | native | 1193 | 473 | 473 | 0 | 0 | 473 |
| revision-select | false | 28 | 10 | 10 | 0 | 0 | 10 |
| revision-select | true | 28 | 10 | 10 | 0 | 0 | 10 |
| revision-write | false | 24 | 20 | 20 | 0 | 0 | 20 |
| revision-write | true | 24 | 20 | 20 | 0 | 0 | 20 |
| sql-control | false | 792 | 222 | 222 | 0 | 0 | 222 |
| sql-control | true | 792 | 312 | 312 | 0 | 0 | 312 |
| sql-dd | false | 1193 | 935 | 935 | 0 | 0 | 935 |
| sql-dd | true | 1193 | 935 | 935 | 0 | 0 | 935 |
| write | false | 30 | 24 | 23 | 0 | 1 | 23 |
| write | true | 30 | 24 | 23 | 0 | 1 | 23 |
| **all** | | **6855** | **3889** | **3893** | **80** | **76** | **3813** |

| family predicate | ANSI | cells | main wrong | ff34 wrong | matched on main, wrong on ff34 | fixed on ff34 | wrong on both |
|---|---|---|---|---|---|---|---|
| `d eq` | false | 60000 | 2501 | 1241 | 127 | 1387 | 1114 |
| `d in` | false | 60000 | 2501 | 1241 | 127 | 1387 | 1114 |
| `d lt` | false | 60000 | 1084 | 553 | 113 | 644 | 440 |
| `d ns` | false | 60000 | 2501 | 1241 | 127 | 1387 | 1114 |
| `d eq` | true | 60000 | 2501 | 1241 | 127 | 1387 | 1114 |
| `d in` | true | 60000 | 2501 | 1241 | 127 | 1387 | 1114 |
| `d lt` | true | 60000 | 1084 | 553 | 113 | 644 | 440 |
| `d ns` | true | 60000 | 2501 | 1241 | 127 | 1387 | 1114 |

| coercion fuzz shape family | scale | ANSI | cells | matched on main, wrong on ff34 | fixed on ff34 | wrong on both |
|---|---|---|---|---|---|---|
| all | all | false | 186570 | 1326 | 3160 | 90393 |
| coalesce | d38_18 | false | 3455 | 0 | 75 | 3201 |
| coalesce | other | false | 17275 | 0 | 665 | 15215 |
| eq | d38_18 | false | 3455 | 299 | 18 | 337 |
| eq | other | false | 17275 | 27 | 224 | 5806 |
| eqf | d38_18 | false | 3455 | 0 | 0 | 593 |
| eqf | other | false | 17275 | 0 | 0 | 6563 |
| greatest | d38_18 | false | 3455 | 0 | 75 | 3201 |
| greatest | other | false | 17275 | 0 | 665 | 15215 |
| if | d38_18 | false | 3455 | 0 | 75 | 3201 |
| if | other | false | 17275 | 0 | 665 | 15215 |
| in | d38_18 | false | 3455 | 299 | 18 | 337 |
| in | other | false | 17275 | 27 | 224 | 5806 |
| le | d38_18 | false | 3455 | 153 | 0 | 166 |
| le | other | false | 17275 | 21 | 107 | 4614 |
| lt | d38_18 | false | 3455 | 153 | 0 | 166 |
| lt | other | false | 17275 | 21 | 107 | 4614 |
| ns | d38_18 | false | 3455 | 299 | 18 | 337 |
| ns | other | false | 17275 | 27 | 224 | 5806 |
| all | all | true | 165846 | 1326 | 3160 | 69669 |
| coalesce | d38_18 | true | 3455 | 0 | 75 | 3201 |
| coalesce | other | true | 17275 | 0 | 665 | 15215 |
| eq | d38_18 | true | 3455 | 299 | 18 | 337 |
| eq | other | true | 13821 | 27 | 224 | 2352 |
| eqf | d38_18 | true | 3455 | 0 | 0 | 593 |
| eqf | other | true | 13821 | 0 | 0 | 3109 |
| greatest | d38_18 | true | 3455 | 0 | 75 | 3201 |
| greatest | other | true | 17275 | 0 | 665 | 15215 |
| if | d38_18 | true | 3455 | 0 | 75 | 3201 |
| if | other | true | 17275 | 0 | 665 | 15215 |
| in | d38_18 | true | 3455 | 299 | 18 | 337 |
| in | other | true | 13821 | 27 | 224 | 2352 |
| le | d38_18 | true | 3455 | 153 | 0 | 166 |
| le | other | true | 13821 | 21 | 107 | 1160 |
| lt | d38_18 | true | 3455 | 153 | 0 | 166 |
| lt | other | true | 13821 | 21 | 107 | 1160 |
| ns | d38_18 | true | 3455 | 299 | 18 | 337 |
| ns | other | true | 13821 | 27 | 224 | 2352 |

| #888 replay root | build | verdicts |
|---|---|---|
| decimal-double | ff34 | equal 2 |
| decimal-double | main | equal 2 |
| other | ff34 | both-raise 115, equal 155, repark-raises 102, spark-raises-repark-answers 46, type-diff 192 |
| other | main | both-raise 115, equal 155, repark-raises 102, spark-raises-repark-answers 46, type-diff 192 |

### Every cell that matched Spark on main and is wrong on ff34 (outside the two fuzz sets)

| section | cell | ANSI | Spark | main | ff34 |
|---|---|---|---|---|---|
| dml-flip | `flip/c7/-22.129547339983247` | true | true | true | false |
| dml-flip | `flip/c6/1.0000000000000004` | true | false | false | true |
| dml-flip | `flip/c6/2.2300000000000004` | true | false | false | true |
| dml | `t/cow/select_eq` | true | [4] | [4] | [1, 2, 3, 4] |
| dml | `t/cow/select_lt` | true | [1, 2, 3] | [1, 2, 3] | [] |
| dml | `t/cow/select_ns` | true | [4] | [4] | [1, 2, 3, 4] |
| dml | `t/cow/select_in` | true | [4] | [4] | [1, 2, 3, 4] |
| dml | `t/cow/select_between` | true | [4] | [4] | [1, 2, 3, 4] |
| dml | `t/cow/delete_eq` | true | [1, 2, 3] | [1, 2, 3] | [] |
| dml | `t/cow/update_eq` | true | [1, 2, 3, 104] | [1, 2, 3, 104] | [101, 102, 103, 104] |
| dml | `t/cow/update_in` | true | [1, 2, 3, 104] | [1, 2, 3, 104] | [101, 102, 103, 104] |
| dml | `t/cow/merge_on_dv` | true | [1, 2, 3, 104] | [1, 2, 3, 104] | [101, 102, 103, 104] |
| dml | `t/cow/merge_on_vd` | true | [1, 2, 3, 104] | [1, 2, 3, 104] | [101, 102, 103, 104] |
| dml | `t/cow/merge_on_key_and` | true | [1, 2, 3, 51, 52, 53, 104] | [1, 2, 3, 51, 52, 53, 104] | [101, 102, 103, 104] |
| dml | `t/cow/merge_delete` | true | [1, 2, 3] | [1, 2, 3] | [] |
| dml | `t/cow/merge_by_source` | true | [4] | [4] | [1, 2, 3, 4] |
| dml | `t/mor/select_eq` | true | [4] | [4] | [1, 2, 3, 4] |
| dml | `t/mor/select_lt` | true | [1, 2, 3] | [1, 2, 3] | [] |
| dml | `t/mor/select_ns` | true | [4] | [4] | [1, 2, 3, 4] |
| dml | `t/mor/select_in` | true | [4] | [4] | [1, 2, 3, 4] |
| dml | `t/mor/select_between` | true | [4] | [4] | [1, 2, 3, 4] |
| dml | `t/mor/delete_eq` | true | [1, 2, 3] | [1, 2, 3] | [] |
| dml | `t/mor/update_eq` | true | [1, 2, 3, 104] | [1, 2, 3, 104] | [101, 102, 103, 104] |
| dml | `t/mor/update_in` | true | [1, 2, 3, 104] | [1, 2, 3, 104] | [101, 102, 103, 104] |
| dml | `t/mor/merge_on_dv` | true | [1, 2, 3, 104] | [1, 2, 3, 104] | [101, 102, 103, 104] |
| dml | `t/mor/merge_on_vd` | true | [1, 2, 3, 104] | [1, 2, 3, 104] | [101, 102, 103, 104] |
| dml | `t/mor/merge_on_key_and` | true | [1, 2, 3, 51, 52, 53, 104] | [1, 2, 3, 51, 52, 53, 104] | [101, 102, 103, 104] |
| dml | `t/mor/merge_delete` | true | [1, 2, 3] | [1, 2, 3] | [] |
| dml | `t/mor/merge_by_source` | true | [4] | [4] | [1, 2, 3, 4] |
| dml-flip-native | `flip/c7/-22.129547339983247` | true | true | true | false |
| dml-flip-native | `flip/c6/1.0000000000000004` | true | false | false | true |
| dml-flip-native | `flip/c6/2.2300000000000004` | true | false | false | true |
| dml-native | `t/cow/select_eq` | true | [4] | [4] | [1, 2, 3, 4] |
| dml-native | `t/cow/select_lt` | true | [1, 2, 3] | [1, 2, 3] | [] |
| dml-native | `t/cow/select_ns` | true | [4] | [4] | [1, 2, 3, 4] |
| dml-native | `t/cow/select_in` | true | [4] | [4] | [1, 2, 3, 4] |
| dml-native | `t/cow/select_between` | true | [4] | [4] | [1, 2, 3, 4] |
| dml-native | `t/cow/delete_eq` | true | [1, 2, 3] | [1, 2, 3] | [] |
| dml-native | `t/cow/update_eq` | true | [1, 2, 3, 104] | [1, 2, 3, 104] | [101, 102, 103, 104] |
| dml-native | `t/cow/update_in` | true | [1, 2, 3, 104] | [1, 2, 3, 104] | [101, 102, 103, 104] |
| dml-flip | `flip/c7/-22.129547339983247` | false | true | true | false |
| dml-flip | `flip/c6/1.0000000000000004` | false | false | false | true |
| dml-flip | `flip/c6/2.2300000000000004` | false | false | false | true |
| dml | `t/cow/select_eq` | false | [4] | [4] | [1, 2, 3, 4] |
| dml | `t/cow/select_lt` | false | [1, 2, 3] | [1, 2, 3] | [] |
| dml | `t/cow/select_ns` | false | [4] | [4] | [1, 2, 3, 4] |
| dml | `t/cow/select_in` | false | [4] | [4] | [1, 2, 3, 4] |
| dml | `t/cow/select_between` | false | [4] | [4] | [1, 2, 3, 4] |
| dml | `t/cow/delete_eq` | false | [1, 2, 3] | [1, 2, 3] | [] |
| dml | `t/cow/update_eq` | false | [1, 2, 3, 104] | [1, 2, 3, 104] | [101, 102, 103, 104] |
| dml | `t/cow/update_in` | false | [1, 2, 3, 104] | [1, 2, 3, 104] | [101, 102, 103, 104] |
| dml | `t/cow/merge_on_dv` | false | [1, 2, 3, 104] | [1, 2, 3, 104] | [101, 102, 103, 104] |
| dml | `t/cow/merge_on_vd` | false | [1, 2, 3, 104] | [1, 2, 3, 104] | [101, 102, 103, 104] |
| dml | `t/cow/merge_on_key_and` | false | [1, 2, 3, 51, 52, 53, 104] | [1, 2, 3, 51, 52, 53, 104] | [101, 102, 103, 104] |
| dml | `t/cow/merge_delete` | false | [1, 2, 3] | [1, 2, 3] | [] |
| dml | `t/cow/merge_by_source` | false | [4] | [4] | [1, 2, 3, 4] |
| dml | `t/mor/select_eq` | false | [4] | [4] | [1, 2, 3, 4] |
| dml | `t/mor/select_lt` | false | [1, 2, 3] | [1, 2, 3] | [] |
| dml | `t/mor/select_ns` | false | [4] | [4] | [1, 2, 3, 4] |
| dml | `t/mor/select_in` | false | [4] | [4] | [1, 2, 3, 4] |
| dml | `t/mor/select_between` | false | [4] | [4] | [1, 2, 3, 4] |
| dml | `t/mor/delete_eq` | false | [1, 2, 3] | [1, 2, 3] | [] |
| dml | `t/mor/update_eq` | false | [1, 2, 3, 104] | [1, 2, 3, 104] | [101, 102, 103, 104] |
| dml | `t/mor/update_in` | false | [1, 2, 3, 104] | [1, 2, 3, 104] | [101, 102, 103, 104] |
| dml | `t/mor/merge_on_dv` | false | [1, 2, 3, 104] | [1, 2, 3, 104] | [101, 102, 103, 104] |
| dml | `t/mor/merge_on_vd` | false | [1, 2, 3, 104] | [1, 2, 3, 104] | [101, 102, 103, 104] |
| dml | `t/mor/merge_on_key_and` | false | [1, 2, 3, 51, 52, 53, 104] | [1, 2, 3, 51, 52, 53, 104] | [101, 102, 103, 104] |
| dml | `t/mor/merge_delete` | false | [1, 2, 3] | [1, 2, 3] | [] |
| dml | `t/mor/merge_by_source` | false | [4] | [4] | [1, 2, 3, 4] |
| dml-flip-native | `flip/c7/-22.129547339983247` | false | true | true | false |
| dml-flip-native | `flip/c6/1.0000000000000004` | false | false | false | true |
| dml-flip-native | `flip/c6/2.2300000000000004` | false | false | false | true |
| dml-native | `t/cow/select_eq` | false | [4] | [4] | [1, 2, 3, 4] |
| dml-native | `t/cow/select_lt` | false | [1, 2, 3] | [1, 2, 3] | [] |
| dml-native | `t/cow/select_ns` | false | [4] | [4] | [1, 2, 3, 4] |
| dml-native | `t/cow/select_in` | false | [4] | [4] | [1, 2, 3, 4] |
| dml-native | `t/cow/select_between` | false | [4] | [4] | [1, 2, 3, 4] |
| dml-native | `t/cow/delete_eq` | false | [1, 2, 3] | [1, 2, 3] | [] |
| dml-native | `t/cow/update_eq` | false | [1, 2, 3, 104] | [1, 2, 3, 104] | [101, 102, 103, 104] |
| dml-native | `t/cow/update_in` | false | [1, 2, 3, 104] | [1, 2, 3, 104] | [101, 102, 103, 104] |

### Every cell fixed on ff34 (outside the two fuzz sets)

| section | cell | ANSI | Spark | main | ff34 |
|---|---|---|---|---|---|
| write | `ins_values_dec_round` | true | decimal(10,2) 1=0.10; 2=1.26 | decimal(10,2) 1=0.10; 2=1.25 | decimal(10,2) 1=0.10; 2=1.26 |
| dml-flip | `flip/c5/-22.129547339983247` | true | true | false | true |
| dml-flip | `flip/c5/1.0000000000000004` | true | true | false | true |
| dml-flip | `flip/c5/2.2300000000000004` | true | true | false | true |
| dml | `u/cow/select_eq` | true | [1, 2, 3] | [2] | [1, 2, 3] |
| dml | `u/cow/select_ns` | true | [1, 2, 3] | [2] | [1, 2, 3] |
| dml | `u/cow/select_in` | true | [1, 2, 3] | [2] | [1, 2, 3] |
| dml | `u/cow/select_between` | true | [1, 2, 3] | [2] | [1, 2, 3] |
| dml | `u/cow/delete_eq` | true | [] | [1, 3] | [] |
| dml | `u/cow/update_eq` | true | [101, 102, 103] | [1, 3, 102] | [101, 102, 103] |
| dml | `u/cow/update_in` | true | [101, 102, 103] | [1, 3, 102] | [101, 102, 103] |
| dml | `u/cow/merge_on_dv` | true | [101, 102, 103] | [1, 3, 102] | [101, 102, 103] |
| dml | `u/cow/merge_on_vd` | true | [101, 102, 103] | [1, 3, 102] | [101, 102, 103] |
| dml | `u/cow/merge_on_key_and` | true | [101, 102, 103] | [1, 3, 51, 53, 102] | [101, 102, 103] |
| dml | `u/cow/merge_delete` | true | [] | [1, 3] | [] |
| dml | `u/cow/merge_by_source` | true | [1, 2, 3] | [2] | [1, 2, 3] |
| dml | `u/mor/select_eq` | true | [1, 2, 3] | [2] | [1, 2, 3] |
| dml | `u/mor/select_ns` | true | [1, 2, 3] | [2] | [1, 2, 3] |
| dml | `u/mor/select_in` | true | [1, 2, 3] | [2] | [1, 2, 3] |
| dml | `u/mor/select_between` | true | [1, 2, 3] | [2] | [1, 2, 3] |
| dml | `u/mor/delete_eq` | true | [] | [1, 3] | [] |
| dml | `u/mor/update_eq` | true | [101, 102, 103] | [1, 3, 102] | [101, 102, 103] |
| dml | `u/mor/update_in` | true | [101, 102, 103] | [1, 3, 102] | [101, 102, 103] |
| dml | `u/mor/merge_on_dv` | true | [101, 102, 103] | [1, 3, 102] | [101, 102, 103] |
| dml | `u/mor/merge_on_vd` | true | [101, 102, 103] | [1, 3, 102] | [101, 102, 103] |
| dml | `u/mor/merge_on_key_and` | true | [101, 102, 103] | [1, 3, 51, 53, 102] | [101, 102, 103] |
| dml | `u/mor/merge_delete` | true | [] | [1, 3] | [] |
| dml | `u/mor/merge_by_source` | true | [1, 2, 3] | [2] | [1, 2, 3] |
| dml-flip-native | `flip/c5/-22.129547339983247` | true | true | false | true |
| dml-flip-native | `flip/c5/1.0000000000000004` | true | true | false | true |
| dml-flip-native | `flip/c5/2.2300000000000004` | true | true | false | true |
| dml-native | `u/cow/select_eq` | true | [1, 2, 3] | [2] | [1, 2, 3] |
| dml-native | `u/cow/select_ns` | true | [1, 2, 3] | [2] | [1, 2, 3] |
| dml-native | `u/cow/select_in` | true | [1, 2, 3] | [2] | [1, 2, 3] |
| dml-native | `u/cow/select_between` | true | [1, 2, 3] | [2] | [1, 2, 3] |
| dml-native | `u/cow/delete_eq` | true | [] | [1, 3] | [] |
| dml-native | `u/cow/update_eq` | true | [101, 102, 103] | [1, 3, 102] | [101, 102, 103] |
| dml-native | `u/cow/update_in` | true | [101, 102, 103] | [1, 3, 102] | [101, 102, 103] |
| write | `ins_values_dec_round` | false | decimal(10,2) 1=0.10; 2=1.26 | decimal(10,2) 1=0.10; 2=1.25 | decimal(10,2) 1=0.10; 2=1.26 |
| dml-flip | `flip/c5/-22.129547339983247` | false | true | false | true |
| dml-flip | `flip/c5/1.0000000000000004` | false | true | false | true |
| dml-flip | `flip/c5/2.2300000000000004` | false | true | false | true |
| dml | `u/cow/select_eq` | false | [1, 2, 3] | [2] | [1, 2, 3] |
| dml | `u/cow/select_ns` | false | [1, 2, 3] | [2] | [1, 2, 3] |
| dml | `u/cow/select_in` | false | [1, 2, 3] | [2] | [1, 2, 3] |
| dml | `u/cow/select_between` | false | [1, 2, 3] | [2] | [1, 2, 3] |
| dml | `u/cow/delete_eq` | false | [] | [1, 3] | [] |
| dml | `u/cow/update_eq` | false | [101, 102, 103] | [1, 3, 102] | [101, 102, 103] |
| dml | `u/cow/update_in` | false | [101, 102, 103] | [1, 3, 102] | [101, 102, 103] |
| dml | `u/cow/merge_on_dv` | false | [101, 102, 103] | [1, 3, 102] | [101, 102, 103] |
| dml | `u/cow/merge_on_vd` | false | [101, 102, 103] | [1, 3, 102] | [101, 102, 103] |
| dml | `u/cow/merge_on_key_and` | false | [101, 102, 103] | [1, 3, 51, 53, 102] | [101, 102, 103] |
| dml | `u/cow/merge_delete` | false | [] | [1, 3] | [] |
| dml | `u/cow/merge_by_source` | false | [1, 2, 3] | [2] | [1, 2, 3] |
| dml | `u/mor/select_eq` | false | [1, 2, 3] | [2] | [1, 2, 3] |
| dml | `u/mor/select_ns` | false | [1, 2, 3] | [2] | [1, 2, 3] |
| dml | `u/mor/select_in` | false | [1, 2, 3] | [2] | [1, 2, 3] |
| dml | `u/mor/select_between` | false | [1, 2, 3] | [2] | [1, 2, 3] |
| dml | `u/mor/delete_eq` | false | [] | [1, 3] | [] |
| dml | `u/mor/update_eq` | false | [101, 102, 103] | [1, 3, 102] | [101, 102, 103] |
| dml | `u/mor/update_in` | false | [101, 102, 103] | [1, 3, 102] | [101, 102, 103] |
| dml | `u/mor/merge_on_dv` | false | [101, 102, 103] | [1, 3, 102] | [101, 102, 103] |
| dml | `u/mor/merge_on_vd` | false | [101, 102, 103] | [1, 3, 102] | [101, 102, 103] |
| dml | `u/mor/merge_on_key_and` | false | [101, 102, 103] | [1, 3, 51, 53, 102] | [101, 102, 103] |
| dml | `u/mor/merge_delete` | false | [] | [1, 3] | [] |
| dml | `u/mor/merge_by_source` | false | [1, 2, 3] | [2] | [1, 2, 3] |
| dml-flip-native | `flip/c5/-22.129547339983247` | false | true | false | true |
| dml-flip-native | `flip/c5/1.0000000000000004` | false | true | false | true |
| dml-flip-native | `flip/c5/2.2300000000000004` | false | true | false | true |
| dml-native | `u/cow/select_eq` | false | [1, 2, 3] | [2] | [1, 2, 3] |
| dml-native | `u/cow/select_ns` | false | [1, 2, 3] | [2] | [1, 2, 3] |
| dml-native | `u/cow/select_in` | false | [1, 2, 3] | [2] | [1, 2, 3] |
| dml-native | `u/cow/select_between` | false | [1, 2, 3] | [2] | [1, 2, 3] |
| dml-native | `u/cow/delete_eq` | false | [] | [1, 3] | [] |
| dml-native | `u/cow/update_eq` | false | [101, 102, 103] | [1, 3, 102] | [101, 102, 103] |
| dml-native | `u/cow/update_in` | false | [101, 102, 103] | [1, 3, 102] | [101, 102, 103] |

### Every wrong write and DML cell, per build

| section | cell | ANSI | Spark | main | ff34 |
|---|---|---|---|---|---|
| write | `ins_union_tiny` | true | double 1=0.1; 2=1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| write | `ins_union_big` | true | double 1=0.1; 2=1e+30 | REPARK-RAISES: write raises datafusion engine error: Optimizer ru | REPARK-RAISES: write raises datafusion engine error: Arrow error: |
| write | `ins_union_neg0` | true | double 1=0.1; 2=-0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| write | `ins_union_nan` | true | double 1=0.1; 2=nan | REPARK-RAISES: write raises datafusion engine error: Optimizer ru | REPARK-RAISES: write raises datafusion engine error: Arrow error: |
| write | `ins_union_rev_tiny` | true | double 1=1e-20; 2=0.1 | REPARK-RAISES: write raises datafusion engine error: Internal err | REPARK-RAISES: write raises datafusion engine error: Internal err |
| write | `ins_union_distinct_tiny` | true | double 1=0.1; 2=1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| write | `ins_case_tiny` | true | double 1=0.1; 2=1e-20 | REPARK-RAISES: write raises Error during planning: Inconsistent d | REPARK-RAISES: write raises Error during planning: Inconsistent d |
| write | `ins_coalesce_tiny` | true | double 2=1e-20 | WRONG-WRITE: double 2=0.0 | WRONG-WRITE: double 2=0.0 |
| write | `ins_greatest_big` | true | double 2=1e+30 | REPARK-RAISES: write raises datafusion engine error: Optimizer ru | REPARK-RAISES: write raises datafusion engine error: Arrow error: |
| write | `ins_array_elem_tiny` | true | double 2=1e-20 | WRONG-WRITE: double 2=0.0 | WRONG-WRITE: double 2=0.0 |
| write | `ins_union_float_tiny` | true | float 1=0.10000000149011612; 2=9.999999682655225e-21 | WRONG-WRITE: float 1=0.10000000149011612; 2=0.0 | WRONG-WRITE: float 1=0.10000000149011612; 2=0.0 |
| write | `ins_union_float_big` | true | float 1=0.10000000149011612; 2=1.0000000150474662e+30 | REPARK-RAISES: write raises datafusion engine error: Optimizer ru | REPARK-RAISES: write raises datafusion engine error: Arrow error: |
| write | `ins_values_dec_round` | true | decimal(10,2) 1=0.10; 2=1.26 | WRONG-WRITE: decimal(10,2) 1=0.10; 2=1.25 | EQUAL: decimal(10,2) 1=0.10; 2=1.26 |
| write | `upd_case_tiny` | true | double 1=0.1; 2=1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| write | `upd_coalesce_tiny` | true | double 1=9.0; 2=1e-20 | WRONG-WRITE: double 1=9.0; 2=0.0 | WRONG-WRITE: double 1=9.0; 2=0.0 |
| write | `upd_if_big` | true | double 1=0.1; 2=1e+30 | REPARK-RAISES: write raises datafusion engine error: Optimizer ru | REPARK-RAISES: write raises datafusion engine error: Arrow error: |
| write | `upd_where_in_union` | true | double 1=1.0; 2=9.0 | REPARK-RAISES: write raises Error during planning: UPDATE with a  | REPARK-RAISES: write raises Error during planning: UPDATE with a  |
| write | `merge_union_src` | true | double 1=0.1; 2=1e-20; 3=-1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0; 3=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0; 3=0.0 |
| write | `merge_union_big` | true | double 1=0.1; 2=1e+30; 3=-1e+30 | REPARK-RAISES: write raises datafusion engine error: Optimizer ru | REPARK-RAISES: write raises datafusion engine error: Arrow error: |
| write | `merge_values_src` | true | double 1=0.1; 2=1e-20; 3=-1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0; 3=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0; 3=0.0 |
| write | `merge_set_case` | true | double 1=9.0; 2=1e-20 | WRONG-WRITE: double 1=9.0; 2=0.0 | WRONG-WRITE: double 1=9.0; 2=0.0 |
| write | `merge_insert_coalesce` | true | double 1=9.0; 2=9.0; 3=1e-20 | WRONG-WRITE: double 1=9.0; 2=9.0; 3=0.0 | WRONG-WRITE: double 1=9.0; 2=9.0; 3=0.0 |
| write | `ctas_union_tiny` | true | double 1=0.1; 2=1e-20 | WRONG-WRITE: decimal(30,15) 1=0.100000000000000; 2=0E-15 | WRONG-WRITE: decimal(30,15) 1=0.100000000000000; 2=0E-15 |
| write | `df_union_append` | true | double 1=0.1; 2=1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| revision-write | `ow_values` | true | double 1=0.1; 2=1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| revision-write | `ow_union` | true | double 1=0.1; 2=1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| revision-write | `ow_select_values` | true | double 1=0.1; 2=1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| revision-write | `byname_values` | true | double 1=0.1; 2=1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| revision-write | `byname_union` | true | double 1=0.1; 2=1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| revision-write | `values_string` | true | string 1=1.5; 2=2.0 | WRONG-WRITE: string 1=1.50; 2=2.0 | WRONG-WRITE: string 1=1.50; 2=2.0 |
| revision-write | `select_values_string` | true | string 1=1.5; 2=2.0 | WRONG-WRITE: string 1=1.50; 2=2.0 | WRONG-WRITE: string 1=1.50; 2=2.0 |
| revision-write | `values_dec38` | true | decimal(38,18) 1=123456789.123456790000000000; 2=1.000000000 | WRONG-WRITE: decimal(38,18) 1=123456789.123456789000000000; 2=1 | WRONG-WRITE: decimal(38,18) 1=123456789.123456789000000000; 2=1 |
| revision-write | `rtas_values` | true | double 1=0.1; 2=1e-20 | WRONG-WRITE: decimal(30,15) 1=0.100000000000000; 2=0E-15 | WRONG-WRITE: decimal(30,15) 1=0.100000000000000; 2=0E-15 |
| revision-write | `rtas_union` | true | double 1=0.1; 2=1e-20 | WRONG-WRITE: decimal(30,15) 1=0.100000000000000; 2=0E-15 | WRONG-WRITE: decimal(30,15) 1=0.100000000000000; 2=0E-15 |
| revision-write | `delete_eq_cols` | true | decimal(10,2) 1=1e-20=0.00 | WRONG-WRITE: decimal(10,2)  | WRONG-WRITE: decimal(10,2)  |
| revision-write | `update_where_cols` | true | decimal(10,2) 1=1e-20=0.00; 12=0.5=0.50 | WRONG-WRITE: decimal(10,2) 11=1e-20=0.00; 12=0.5=0.50 | WRONG-WRITE: decimal(10,2) 11=1e-20=0.00; 12=0.5=0.50 |
| revision-write | `merge_on_cols` | true | decimal(10,2) 1=1e-20=0.00; 2=0.5=0.50 | WRONG-WRITE: decimal(10,2) 2=0.5=0.50; 99=1e-20=0.00 | WRONG-WRITE: decimal(10,2) 2=0.5=0.50; 99=1e-20=0.00 |
| revision-write | `delete_eq_cols_mor` | true | decimal(10,2) 1=1e-20=0.00 | WRONG-WRITE: decimal(10,2)  | WRONG-WRITE: decimal(10,2)  |
| revision-write | `update_where_cols_mor` | true | decimal(10,2) 1=1e-20=0.00; 12=0.5=0.50 | WRONG-WRITE: decimal(10,2) 11=1e-20=0.00; 12=0.5=0.50 | WRONG-WRITE: decimal(10,2) 11=1e-20=0.00; 12=0.5=0.50 |
| revision-write | `merge_on_cols_mor` | true | decimal(10,2) 1=1e-20=0.00; 2=0.5=0.50 | WRONG-WRITE: decimal(10,2) 2=0.5=0.50; 99=1e-20=0.00 | WRONG-WRITE: decimal(10,2) 2=0.5=0.50; 99=1e-20=0.00 |
| revision-write | `merge_on_zero_today` | true | decimal(10,2) 1=-0.0=0.00 | WRONG-WRITE: decimal(10,2) 99=-0.0=0.00 | WRONG-WRITE: decimal(10,2) 99=-0.0=0.00 |
| revision-write | `merge_on_zero_widened` | true | decimal(10,2) 1=-0.0=0.00 | WRONG-WRITE: decimal(10,2) 99=-0.0=0.00 | WRONG-WRITE: decimal(10,2) 99=-0.0=0.00 |
| revision-write | `df_insertinto_union` | true | double 1=0.1; 2=1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| revision-write | `df_overwritepartitions_union` | true | double 1=0.1; 2=1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| dml-flip | `flip/c1/-22.129547339983247` | true | true | WRONG: false | WRONG: false |
| dml-flip | `flip/c5/-22.129547339983247` | true | true | WRONG: false | EQUAL: true |
| dml-flip | `flip/c7/-22.129547339983247` | true | true | EQUAL: true | WRONG: false |
| dml-flip | `flip/c5/1.0000000000000004` | true | true | WRONG: false | EQUAL: true |
| dml-flip | `flip/c6/1.0000000000000004` | true | false | EQUAL: false | WRONG: true |
| dml-flip | `flip/c5/2.2300000000000004` | true | true | WRONG: false | EQUAL: true |
| dml-flip | `flip/c6/2.2300000000000004` | true | false | EQUAL: false | WRONG: true |
| dml | `t/cow/select_eq` | true | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml | `t/cow/select_lt` | true | [1, 2, 3] | EQUAL: [1, 2, 3] | WRONG: [] |
| dml | `t/cow/select_ns` | true | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml | `t/cow/select_in` | true | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml | `t/cow/select_between` | true | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml | `t/cow/delete_eq` | true | [1, 2, 3] | EQUAL: [1, 2, 3] | WRONG: [] |
| dml | `t/cow/update_eq` | true | [1, 2, 3, 104] | EQUAL: [1, 2, 3, 104] | WRONG: [101, 102, 103, 104] |
| dml | `t/cow/update_in` | true | [1, 2, 3, 104] | EQUAL: [1, 2, 3, 104] | WRONG: [101, 102, 103, 104] |
| dml | `t/cow/merge_on_dv` | true | [1, 2, 3, 104] | EQUAL: [1, 2, 3, 104] | WRONG: [101, 102, 103, 104] |
| dml | `t/cow/merge_on_vd` | true | [1, 2, 3, 104] | EQUAL: [1, 2, 3, 104] | WRONG: [101, 102, 103, 104] |
| dml | `t/cow/merge_on_key_and` | true | [1, 2, 3, 51, 52, 53, 104] | EQUAL: [1, 2, 3, 51, 52, 53, 104] | WRONG: [101, 102, 103, 104] |
| dml | `t/cow/merge_delete` | true | [1, 2, 3] | EQUAL: [1, 2, 3] | WRONG: [] |
| dml | `t/cow/merge_by_source` | true | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml | `t/mor/select_eq` | true | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml | `t/mor/select_lt` | true | [1, 2, 3] | EQUAL: [1, 2, 3] | WRONG: [] |
| dml | `t/mor/select_ns` | true | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml | `t/mor/select_in` | true | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml | `t/mor/select_between` | true | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml | `t/mor/delete_eq` | true | [1, 2, 3] | EQUAL: [1, 2, 3] | WRONG: [] |
| dml | `t/mor/update_eq` | true | [1, 2, 3, 104] | EQUAL: [1, 2, 3, 104] | WRONG: [101, 102, 103, 104] |
| dml | `t/mor/update_in` | true | [1, 2, 3, 104] | EQUAL: [1, 2, 3, 104] | WRONG: [101, 102, 103, 104] |
| dml | `t/mor/merge_on_dv` | true | [1, 2, 3, 104] | EQUAL: [1, 2, 3, 104] | WRONG: [101, 102, 103, 104] |
| dml | `t/mor/merge_on_vd` | true | [1, 2, 3, 104] | EQUAL: [1, 2, 3, 104] | WRONG: [101, 102, 103, 104] |
| dml | `t/mor/merge_on_key_and` | true | [1, 2, 3, 51, 52, 53, 104] | EQUAL: [1, 2, 3, 51, 52, 53, 104] | WRONG: [101, 102, 103, 104] |
| dml | `t/mor/merge_delete` | true | [1, 2, 3] | EQUAL: [1, 2, 3] | WRONG: [] |
| dml | `t/mor/merge_by_source` | true | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml | `u/cow/select_eq` | true | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml | `u/cow/select_ns` | true | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml | `u/cow/select_in` | true | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml | `u/cow/select_between` | true | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml | `u/cow/delete_eq` | true | [] | WRONG: [1, 3] | EQUAL: [] |
| dml | `u/cow/update_eq` | true | [101, 102, 103] | WRONG: [1, 3, 102] | EQUAL: [101, 102, 103] |
| dml | `u/cow/update_in` | true | [101, 102, 103] | WRONG: [1, 3, 102] | EQUAL: [101, 102, 103] |
| dml | `u/cow/merge_on_dv` | true | [101, 102, 103] | WRONG: [1, 3, 102] | EQUAL: [101, 102, 103] |
| dml | `u/cow/merge_on_vd` | true | [101, 102, 103] | WRONG: [1, 3, 102] | EQUAL: [101, 102, 103] |
| dml | `u/cow/merge_on_key_and` | true | [101, 102, 103] | WRONG: [1, 3, 51, 53, 102] | EQUAL: [101, 102, 103] |
| dml | `u/cow/merge_delete` | true | [] | WRONG: [1, 3] | EQUAL: [] |
| dml | `u/cow/merge_by_source` | true | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml | `u/mor/select_eq` | true | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml | `u/mor/select_ns` | true | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml | `u/mor/select_in` | true | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml | `u/mor/select_between` | true | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml | `u/mor/delete_eq` | true | [] | WRONG: [1, 3] | EQUAL: [] |
| dml | `u/mor/update_eq` | true | [101, 102, 103] | WRONG: [1, 3, 102] | EQUAL: [101, 102, 103] |
| dml | `u/mor/update_in` | true | [101, 102, 103] | WRONG: [1, 3, 102] | EQUAL: [101, 102, 103] |
| dml | `u/mor/merge_on_dv` | true | [101, 102, 103] | WRONG: [1, 3, 102] | EQUAL: [101, 102, 103] |
| dml | `u/mor/merge_on_vd` | true | [101, 102, 103] | WRONG: [1, 3, 102] | EQUAL: [101, 102, 103] |
| dml | `u/mor/merge_on_key_and` | true | [101, 102, 103] | WRONG: [1, 3, 51, 53, 102] | EQUAL: [101, 102, 103] |
| dml | `u/mor/merge_delete` | true | [] | WRONG: [1, 3] | EQUAL: [] |
| dml | `u/mor/merge_by_source` | true | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml | `z/cow/select_eq` | true | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml | `z/cow/select_lt` | true | [3] | WRONG: [] | WRONG: [] |
| dml | `z/cow/select_ns` | true | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml | `z/cow/select_in` | true | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml | `z/cow/select_between` | true | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml | `z/cow/delete_eq` | true | [3] | WRONG: [] | WRONG: [] |
| dml | `z/cow/update_eq` | true | [3, 101, 102] | WRONG: [101, 102, 103] | WRONG: [101, 102, 103] |
| dml | `z/cow/update_in` | true | [3, 101, 102] | WRONG: [101, 102, 103] | WRONG: [101, 102, 103] |
| dml | `z/cow/merge_on_dv` | true | [101, 102, 103] | REPARK-RAISES: raises Error during planning: [MERGE_CARDINALITY_V | REPARK-RAISES: raises Error during planning: [MERGE_CARDINALITY_V |
| dml | `z/cow/merge_on_key_and` | true | [3, 53, 101, 102] | WRONG: [101, 102, 103] | WRONG: [101, 102, 103] |
| dml | `z/cow/merge_by_source` | true | [1, 2, 3] | WRONG: [1, 1, 2, 3, 3] | WRONG: [1, 1, 2, 3, 3] |
| dml | `z/mor/select_eq` | true | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml | `z/mor/select_lt` | true | [3] | WRONG: [] | WRONG: [] |
| dml | `z/mor/select_ns` | true | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml | `z/mor/select_in` | true | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml | `z/mor/select_between` | true | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml | `z/mor/delete_eq` | true | [3] | WRONG: [] | WRONG: [] |
| dml | `z/mor/update_eq` | true | [3, 101, 102] | WRONG: [101, 102, 103] | WRONG: [101, 102, 103] |
| dml | `z/mor/update_in` | true | [3, 101, 102] | WRONG: [101, 102, 103] | WRONG: [101, 102, 103] |
| dml | `z/mor/merge_on_dv` | true | [101, 102, 103] | REPARK-RAISES: raises Error during planning: [MERGE_CARDINALITY_V | REPARK-RAISES: raises Error during planning: [MERGE_CARDINALITY_V |
| dml | `z/mor/merge_on_key_and` | true | [3, 53, 101, 102] | WRONG: [101, 102, 103] | WRONG: [101, 102, 103] |
| dml-flip-native | `flip/c1/-22.129547339983247` | true | true | WRONG: false | WRONG: false |
| dml-flip-native | `flip/c5/-22.129547339983247` | true | true | WRONG: false | EQUAL: true |
| dml-flip-native | `flip/c7/-22.129547339983247` | true | true | EQUAL: true | WRONG: false |
| dml-flip-native | `flip/c5/1.0000000000000004` | true | true | WRONG: false | EQUAL: true |
| dml-flip-native | `flip/c6/1.0000000000000004` | true | false | EQUAL: false | WRONG: true |
| dml-flip-native | `flip/c5/2.2300000000000004` | true | true | WRONG: false | EQUAL: true |
| dml-flip-native | `flip/c6/2.2300000000000004` | true | false | EQUAL: false | WRONG: true |
| dml-native | `t/cow/select_eq` | true | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml-native | `t/cow/select_lt` | true | [1, 2, 3] | EQUAL: [1, 2, 3] | WRONG: [] |
| dml-native | `t/cow/select_ns` | true | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml-native | `t/cow/select_in` | true | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml-native | `t/cow/select_between` | true | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml-native | `t/cow/delete_eq` | true | [1, 2, 3] | EQUAL: [1, 2, 3] | WRONG: [] |
| dml-native | `t/cow/update_eq` | true | [1, 2, 3, 104] | EQUAL: [1, 2, 3, 104] | WRONG: [101, 102, 103, 104] |
| dml-native | `t/cow/update_in` | true | [1, 2, 3, 104] | EQUAL: [1, 2, 3, 104] | WRONG: [101, 102, 103, 104] |
| dml-native | `t/cow/merge_on_dv` | true | [1, 2, 3, 104] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `t/cow/merge_on_vd` | true | [1, 2, 3, 104] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `t/cow/merge_on_key_and` | true | [1, 2, 3, 51, 52, 53, 104] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `t/cow/merge_delete` | true | [1, 2, 3] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `t/cow/merge_by_source` | true | [4] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `u/cow/select_eq` | true | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml-native | `u/cow/select_ns` | true | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml-native | `u/cow/select_in` | true | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml-native | `u/cow/select_between` | true | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml-native | `u/cow/delete_eq` | true | [] | WRONG: [1, 3] | EQUAL: [] |
| dml-native | `u/cow/update_eq` | true | [101, 102, 103] | WRONG: [1, 3, 102] | EQUAL: [101, 102, 103] |
| dml-native | `u/cow/update_in` | true | [101, 102, 103] | WRONG: [1, 3, 102] | EQUAL: [101, 102, 103] |
| dml-native | `u/cow/merge_on_dv` | true | [101, 102, 103] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `u/cow/merge_on_vd` | true | [101, 102, 103] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `u/cow/merge_on_key_and` | true | [101, 102, 103] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `u/cow/merge_delete` | true | [] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `u/cow/merge_by_source` | true | [1, 2, 3] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `z/cow/select_eq` | true | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml-native | `z/cow/select_lt` | true | [3] | WRONG: [] | WRONG: [] |
| dml-native | `z/cow/select_ns` | true | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml-native | `z/cow/select_in` | true | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml-native | `z/cow/select_between` | true | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml-native | `z/cow/delete_eq` | true | [3] | WRONG: [] | WRONG: [] |
| dml-native | `z/cow/update_eq` | true | [3, 101, 102] | WRONG: [101, 102, 103] | WRONG: [101, 102, 103] |
| dml-native | `z/cow/update_in` | true | [3, 101, 102] | WRONG: [101, 102, 103] | WRONG: [101, 102, 103] |
| dml-native | `z/cow/merge_on_dv` | true | [101, 102, 103] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `z/cow/merge_on_key_and` | true | [3, 53, 101, 102] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `z/cow/merge_delete` | true | [] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `z/cow/merge_by_source` | true | [1, 2, 3] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `ctl/cow/merge_by_source_dupkey` | true | [1, 1] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `ctl/cow/merge_by_source_onekey` | true | [1] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| write | `ins_union_tiny` | false | double 1=0.1; 2=1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| write | `ins_union_big` | false | double 1=0.1; 2=1e+30 | REPARK-RAISES: write raises datafusion engine error: Optimizer ru | REPARK-RAISES: write raises datafusion engine error: Arrow error: |
| write | `ins_union_neg0` | false | double 1=0.1; 2=-0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| write | `ins_union_nan` | false | double 1=0.1; 2=nan | REPARK-RAISES: write raises datafusion engine error: Optimizer ru | REPARK-RAISES: write raises datafusion engine error: Arrow error: |
| write | `ins_union_rev_tiny` | false | double 1=1e-20; 2=0.1 | REPARK-RAISES: write raises datafusion engine error: Internal err | REPARK-RAISES: write raises datafusion engine error: Internal err |
| write | `ins_union_distinct_tiny` | false | double 1=0.1; 2=1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| write | `ins_case_tiny` | false | double 1=0.1; 2=1e-20 | REPARK-RAISES: write raises Error during planning: Inconsistent d | REPARK-RAISES: write raises Error during planning: Inconsistent d |
| write | `ins_coalesce_tiny` | false | double 2=1e-20 | WRONG-WRITE: double 2=0.0 | WRONG-WRITE: double 2=0.0 |
| write | `ins_greatest_big` | false | double 2=1e+30 | REPARK-RAISES: write raises datafusion engine error: Optimizer ru | REPARK-RAISES: write raises datafusion engine error: Arrow error: |
| write | `ins_array_elem_tiny` | false | double 2=1e-20 | WRONG-WRITE: double 2=0.0 | WRONG-WRITE: double 2=0.0 |
| write | `ins_union_float_tiny` | false | float 1=0.10000000149011612; 2=9.999999682655225e-21 | WRONG-WRITE: float 1=0.10000000149011612; 2=0.0 | WRONG-WRITE: float 1=0.10000000149011612; 2=0.0 |
| write | `ins_union_float_big` | false | float 1=0.10000000149011612; 2=1.0000000150474662e+30 | REPARK-RAISES: write raises datafusion engine error: Optimizer ru | REPARK-RAISES: write raises datafusion engine error: Arrow error: |
| write | `ins_values_dec_round` | false | decimal(10,2) 1=0.10; 2=1.26 | WRONG-WRITE: decimal(10,2) 1=0.10; 2=1.25 | EQUAL: decimal(10,2) 1=0.10; 2=1.26 |
| write | `upd_case_tiny` | false | double 1=0.1; 2=1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| write | `upd_coalesce_tiny` | false | double 1=9.0; 2=1e-20 | WRONG-WRITE: double 1=9.0; 2=0.0 | WRONG-WRITE: double 1=9.0; 2=0.0 |
| write | `upd_if_big` | false | double 1=0.1; 2=1e+30 | REPARK-RAISES: write raises datafusion engine error: Optimizer ru | REPARK-RAISES: write raises datafusion engine error: Arrow error: |
| write | `upd_where_in_union` | false | double 1=1.0; 2=9.0 | REPARK-RAISES: write raises Error during planning: UPDATE with a  | REPARK-RAISES: write raises Error during planning: UPDATE with a  |
| write | `merge_union_src` | false | double 1=0.1; 2=1e-20; 3=-1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0; 3=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0; 3=0.0 |
| write | `merge_union_big` | false | double 1=0.1; 2=1e+30; 3=-1e+30 | REPARK-RAISES: write raises datafusion engine error: Optimizer ru | REPARK-RAISES: write raises datafusion engine error: Arrow error: |
| write | `merge_values_src` | false | double 1=0.1; 2=1e-20; 3=-1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0; 3=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0; 3=0.0 |
| write | `merge_set_case` | false | double 1=9.0; 2=1e-20 | WRONG-WRITE: double 1=9.0; 2=0.0 | WRONG-WRITE: double 1=9.0; 2=0.0 |
| write | `merge_insert_coalesce` | false | double 1=9.0; 2=9.0; 3=1e-20 | WRONG-WRITE: double 1=9.0; 2=9.0; 3=0.0 | WRONG-WRITE: double 1=9.0; 2=9.0; 3=0.0 |
| write | `ctas_union_tiny` | false | double 1=0.1; 2=1e-20 | WRONG-WRITE: decimal(30,15) 1=0.100000000000000; 2=0E-15 | WRONG-WRITE: decimal(30,15) 1=0.100000000000000; 2=0E-15 |
| write | `df_union_append` | false | double 1=0.1; 2=1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| revision-write | `ow_values` | false | double 1=0.1; 2=1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| revision-write | `ow_union` | false | double 1=0.1; 2=1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| revision-write | `ow_select_values` | false | double 1=0.1; 2=1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| revision-write | `byname_values` | false | double 1=0.1; 2=1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| revision-write | `byname_union` | false | double 1=0.1; 2=1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| revision-write | `values_string` | false | string 1=1.5; 2=2.0 | WRONG-WRITE: string 1=1.50; 2=2.0 | WRONG-WRITE: string 1=1.50; 2=2.0 |
| revision-write | `select_values_string` | false | string 1=1.5; 2=2.0 | WRONG-WRITE: string 1=1.50; 2=2.0 | WRONG-WRITE: string 1=1.50; 2=2.0 |
| revision-write | `values_dec38` | false | decimal(38,18) 1=123456789.123456790000000000; 2=1.000000000 | WRONG-WRITE: decimal(38,18) 1=123456789.123456789000000000; 2=1 | WRONG-WRITE: decimal(38,18) 1=123456789.123456789000000000; 2=1 |
| revision-write | `rtas_values` | false | double 1=0.1; 2=1e-20 | WRONG-WRITE: decimal(30,15) 1=0.100000000000000; 2=0E-15 | WRONG-WRITE: decimal(30,15) 1=0.100000000000000; 2=0E-15 |
| revision-write | `rtas_union` | false | double 1=0.1; 2=1e-20 | WRONG-WRITE: decimal(30,15) 1=0.100000000000000; 2=0E-15 | WRONG-WRITE: decimal(30,15) 1=0.100000000000000; 2=0E-15 |
| revision-write | `delete_eq_cols` | false | decimal(10,2) 1=1e-20=0.00 | WRONG-WRITE: decimal(10,2)  | WRONG-WRITE: decimal(10,2)  |
| revision-write | `update_where_cols` | false | decimal(10,2) 1=1e-20=0.00; 12=0.5=0.50 | WRONG-WRITE: decimal(10,2) 11=1e-20=0.00; 12=0.5=0.50 | WRONG-WRITE: decimal(10,2) 11=1e-20=0.00; 12=0.5=0.50 |
| revision-write | `merge_on_cols` | false | decimal(10,2) 1=1e-20=0.00; 2=0.5=0.50 | WRONG-WRITE: decimal(10,2) 2=0.5=0.50; 99=1e-20=0.00 | WRONG-WRITE: decimal(10,2) 2=0.5=0.50; 99=1e-20=0.00 |
| revision-write | `delete_eq_cols_mor` | false | decimal(10,2) 1=1e-20=0.00 | WRONG-WRITE: decimal(10,2)  | WRONG-WRITE: decimal(10,2)  |
| revision-write | `update_where_cols_mor` | false | decimal(10,2) 1=1e-20=0.00; 12=0.5=0.50 | WRONG-WRITE: decimal(10,2) 11=1e-20=0.00; 12=0.5=0.50 | WRONG-WRITE: decimal(10,2) 11=1e-20=0.00; 12=0.5=0.50 |
| revision-write | `merge_on_cols_mor` | false | decimal(10,2) 1=1e-20=0.00; 2=0.5=0.50 | WRONG-WRITE: decimal(10,2) 2=0.5=0.50; 99=1e-20=0.00 | WRONG-WRITE: decimal(10,2) 2=0.5=0.50; 99=1e-20=0.00 |
| revision-write | `merge_on_zero_today` | false | decimal(10,2) 1=-0.0=0.00 | WRONG-WRITE: decimal(10,2) 99=-0.0=0.00 | WRONG-WRITE: decimal(10,2) 99=-0.0=0.00 |
| revision-write | `merge_on_zero_widened` | false | decimal(10,2) 1=-0.0=0.00 | WRONG-WRITE: decimal(10,2) 99=-0.0=0.00 | WRONG-WRITE: decimal(10,2) 99=-0.0=0.00 |
| revision-write | `df_insertinto_union` | false | double 1=0.1; 2=1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| revision-write | `df_overwritepartitions_union` | false | double 1=0.1; 2=1e-20 | WRONG-WRITE: double 1=0.1; 2=0.0 | WRONG-WRITE: double 1=0.1; 2=0.0 |
| dml-flip | `flip/c1/-22.129547339983247` | false | true | WRONG: false | WRONG: false |
| dml-flip | `flip/c5/-22.129547339983247` | false | true | WRONG: false | EQUAL: true |
| dml-flip | `flip/c7/-22.129547339983247` | false | true | EQUAL: true | WRONG: false |
| dml-flip | `flip/c5/1.0000000000000004` | false | true | WRONG: false | EQUAL: true |
| dml-flip | `flip/c6/1.0000000000000004` | false | false | EQUAL: false | WRONG: true |
| dml-flip | `flip/c5/2.2300000000000004` | false | true | WRONG: false | EQUAL: true |
| dml-flip | `flip/c6/2.2300000000000004` | false | false | EQUAL: false | WRONG: true |
| dml | `t/cow/select_eq` | false | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml | `t/cow/select_lt` | false | [1, 2, 3] | EQUAL: [1, 2, 3] | WRONG: [] |
| dml | `t/cow/select_ns` | false | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml | `t/cow/select_in` | false | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml | `t/cow/select_between` | false | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml | `t/cow/delete_eq` | false | [1, 2, 3] | EQUAL: [1, 2, 3] | WRONG: [] |
| dml | `t/cow/update_eq` | false | [1, 2, 3, 104] | EQUAL: [1, 2, 3, 104] | WRONG: [101, 102, 103, 104] |
| dml | `t/cow/update_in` | false | [1, 2, 3, 104] | EQUAL: [1, 2, 3, 104] | WRONG: [101, 102, 103, 104] |
| dml | `t/cow/merge_on_dv` | false | [1, 2, 3, 104] | EQUAL: [1, 2, 3, 104] | WRONG: [101, 102, 103, 104] |
| dml | `t/cow/merge_on_vd` | false | [1, 2, 3, 104] | EQUAL: [1, 2, 3, 104] | WRONG: [101, 102, 103, 104] |
| dml | `t/cow/merge_on_key_and` | false | [1, 2, 3, 51, 52, 53, 104] | EQUAL: [1, 2, 3, 51, 52, 53, 104] | WRONG: [101, 102, 103, 104] |
| dml | `t/cow/merge_delete` | false | [1, 2, 3] | EQUAL: [1, 2, 3] | WRONG: [] |
| dml | `t/cow/merge_by_source` | false | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml | `t/mor/select_eq` | false | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml | `t/mor/select_lt` | false | [1, 2, 3] | EQUAL: [1, 2, 3] | WRONG: [] |
| dml | `t/mor/select_ns` | false | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml | `t/mor/select_in` | false | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml | `t/mor/select_between` | false | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml | `t/mor/delete_eq` | false | [1, 2, 3] | EQUAL: [1, 2, 3] | WRONG: [] |
| dml | `t/mor/update_eq` | false | [1, 2, 3, 104] | EQUAL: [1, 2, 3, 104] | WRONG: [101, 102, 103, 104] |
| dml | `t/mor/update_in` | false | [1, 2, 3, 104] | EQUAL: [1, 2, 3, 104] | WRONG: [101, 102, 103, 104] |
| dml | `t/mor/merge_on_dv` | false | [1, 2, 3, 104] | EQUAL: [1, 2, 3, 104] | WRONG: [101, 102, 103, 104] |
| dml | `t/mor/merge_on_vd` | false | [1, 2, 3, 104] | EQUAL: [1, 2, 3, 104] | WRONG: [101, 102, 103, 104] |
| dml | `t/mor/merge_on_key_and` | false | [1, 2, 3, 51, 52, 53, 104] | EQUAL: [1, 2, 3, 51, 52, 53, 104] | WRONG: [101, 102, 103, 104] |
| dml | `t/mor/merge_delete` | false | [1, 2, 3] | EQUAL: [1, 2, 3] | WRONG: [] |
| dml | `t/mor/merge_by_source` | false | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml | `u/cow/select_eq` | false | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml | `u/cow/select_ns` | false | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml | `u/cow/select_in` | false | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml | `u/cow/select_between` | false | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml | `u/cow/delete_eq` | false | [] | WRONG: [1, 3] | EQUAL: [] |
| dml | `u/cow/update_eq` | false | [101, 102, 103] | WRONG: [1, 3, 102] | EQUAL: [101, 102, 103] |
| dml | `u/cow/update_in` | false | [101, 102, 103] | WRONG: [1, 3, 102] | EQUAL: [101, 102, 103] |
| dml | `u/cow/merge_on_dv` | false | [101, 102, 103] | WRONG: [1, 3, 102] | EQUAL: [101, 102, 103] |
| dml | `u/cow/merge_on_vd` | false | [101, 102, 103] | WRONG: [1, 3, 102] | EQUAL: [101, 102, 103] |
| dml | `u/cow/merge_on_key_and` | false | [101, 102, 103] | WRONG: [1, 3, 51, 53, 102] | EQUAL: [101, 102, 103] |
| dml | `u/cow/merge_delete` | false | [] | WRONG: [1, 3] | EQUAL: [] |
| dml | `u/cow/merge_by_source` | false | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml | `u/mor/select_eq` | false | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml | `u/mor/select_ns` | false | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml | `u/mor/select_in` | false | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml | `u/mor/select_between` | false | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml | `u/mor/delete_eq` | false | [] | WRONG: [1, 3] | EQUAL: [] |
| dml | `u/mor/update_eq` | false | [101, 102, 103] | WRONG: [1, 3, 102] | EQUAL: [101, 102, 103] |
| dml | `u/mor/update_in` | false | [101, 102, 103] | WRONG: [1, 3, 102] | EQUAL: [101, 102, 103] |
| dml | `u/mor/merge_on_dv` | false | [101, 102, 103] | WRONG: [1, 3, 102] | EQUAL: [101, 102, 103] |
| dml | `u/mor/merge_on_vd` | false | [101, 102, 103] | WRONG: [1, 3, 102] | EQUAL: [101, 102, 103] |
| dml | `u/mor/merge_on_key_and` | false | [101, 102, 103] | WRONG: [1, 3, 51, 53, 102] | EQUAL: [101, 102, 103] |
| dml | `u/mor/merge_delete` | false | [] | WRONG: [1, 3] | EQUAL: [] |
| dml | `u/mor/merge_by_source` | false | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml | `z/cow/select_eq` | false | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml | `z/cow/select_lt` | false | [3] | WRONG: [] | WRONG: [] |
| dml | `z/cow/select_ns` | false | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml | `z/cow/select_in` | false | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml | `z/cow/select_between` | false | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml | `z/cow/delete_eq` | false | [3] | WRONG: [] | WRONG: [] |
| dml | `z/cow/update_eq` | false | [3, 101, 102] | WRONG: [101, 102, 103] | WRONG: [101, 102, 103] |
| dml | `z/cow/update_in` | false | [3, 101, 102] | WRONG: [101, 102, 103] | WRONG: [101, 102, 103] |
| dml | `z/cow/merge_on_dv` | false | [101, 102, 103] | REPARK-RAISES: raises Error during planning: [MERGE_CARDINALITY_V | REPARK-RAISES: raises Error during planning: [MERGE_CARDINALITY_V |
| dml | `z/cow/merge_on_key_and` | false | [3, 53, 101, 102] | WRONG: [101, 102, 103] | WRONG: [101, 102, 103] |
| dml | `z/cow/merge_by_source` | false | [1, 2, 3] | WRONG: [1, 1, 2, 3, 3] | WRONG: [1, 1, 2, 3, 3] |
| dml | `z/mor/select_eq` | false | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml | `z/mor/select_lt` | false | [3] | WRONG: [] | WRONG: [] |
| dml | `z/mor/select_ns` | false | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml | `z/mor/select_in` | false | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml | `z/mor/select_between` | false | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml | `z/mor/delete_eq` | false | [3] | WRONG: [] | WRONG: [] |
| dml | `z/mor/update_eq` | false | [3, 101, 102] | WRONG: [101, 102, 103] | WRONG: [101, 102, 103] |
| dml | `z/mor/update_in` | false | [3, 101, 102] | WRONG: [101, 102, 103] | WRONG: [101, 102, 103] |
| dml | `z/mor/merge_on_dv` | false | [101, 102, 103] | REPARK-RAISES: raises Error during planning: [MERGE_CARDINALITY_V | REPARK-RAISES: raises Error during planning: [MERGE_CARDINALITY_V |
| dml | `z/mor/merge_on_key_and` | false | [3, 53, 101, 102] | WRONG: [101, 102, 103] | WRONG: [101, 102, 103] |
| dml-flip-native | `flip/c1/-22.129547339983247` | false | true | WRONG: false | WRONG: false |
| dml-flip-native | `flip/c5/-22.129547339983247` | false | true | WRONG: false | EQUAL: true |
| dml-flip-native | `flip/c7/-22.129547339983247` | false | true | EQUAL: true | WRONG: false |
| dml-flip-native | `flip/c5/1.0000000000000004` | false | true | WRONG: false | EQUAL: true |
| dml-flip-native | `flip/c6/1.0000000000000004` | false | false | EQUAL: false | WRONG: true |
| dml-flip-native | `flip/c5/2.2300000000000004` | false | true | WRONG: false | EQUAL: true |
| dml-flip-native | `flip/c6/2.2300000000000004` | false | false | EQUAL: false | WRONG: true |
| dml-native | `t/cow/select_eq` | false | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml-native | `t/cow/select_lt` | false | [1, 2, 3] | EQUAL: [1, 2, 3] | WRONG: [] |
| dml-native | `t/cow/select_ns` | false | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml-native | `t/cow/select_in` | false | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml-native | `t/cow/select_between` | false | [4] | EQUAL: [4] | WRONG: [1, 2, 3, 4] |
| dml-native | `t/cow/delete_eq` | false | [1, 2, 3] | EQUAL: [1, 2, 3] | WRONG: [] |
| dml-native | `t/cow/update_eq` | false | [1, 2, 3, 104] | EQUAL: [1, 2, 3, 104] | WRONG: [101, 102, 103, 104] |
| dml-native | `t/cow/update_in` | false | [1, 2, 3, 104] | EQUAL: [1, 2, 3, 104] | WRONG: [101, 102, 103, 104] |
| dml-native | `t/cow/merge_on_dv` | false | [1, 2, 3, 104] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `t/cow/merge_on_vd` | false | [1, 2, 3, 104] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `t/cow/merge_on_key_and` | false | [1, 2, 3, 51, 52, 53, 104] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `t/cow/merge_delete` | false | [1, 2, 3] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `t/cow/merge_by_source` | false | [4] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `u/cow/select_eq` | false | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml-native | `u/cow/select_ns` | false | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml-native | `u/cow/select_in` | false | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml-native | `u/cow/select_between` | false | [1, 2, 3] | WRONG: [2] | EQUAL: [1, 2, 3] |
| dml-native | `u/cow/delete_eq` | false | [] | WRONG: [1, 3] | EQUAL: [] |
| dml-native | `u/cow/update_eq` | false | [101, 102, 103] | WRONG: [1, 3, 102] | EQUAL: [101, 102, 103] |
| dml-native | `u/cow/update_in` | false | [101, 102, 103] | WRONG: [1, 3, 102] | EQUAL: [101, 102, 103] |
| dml-native | `u/cow/merge_on_dv` | false | [101, 102, 103] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `u/cow/merge_on_vd` | false | [101, 102, 103] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `u/cow/merge_on_key_and` | false | [101, 102, 103] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `u/cow/merge_delete` | false | [] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `u/cow/merge_by_source` | false | [1, 2, 3] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `z/cow/select_eq` | false | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml-native | `z/cow/select_lt` | false | [3] | WRONG: [] | WRONG: [] |
| dml-native | `z/cow/select_ns` | false | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml-native | `z/cow/select_in` | false | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml-native | `z/cow/select_between` | false | [1, 2] | WRONG: [1, 2, 3] | WRONG: [1, 2, 3] |
| dml-native | `z/cow/delete_eq` | false | [3] | WRONG: [] | WRONG: [] |
| dml-native | `z/cow/update_eq` | false | [3, 101, 102] | WRONG: [101, 102, 103] | WRONG: [101, 102, 103] |
| dml-native | `z/cow/update_in` | false | [3, 101, 102] | WRONG: [101, 102, 103] | WRONG: [101, 102, 103] |
| dml-native | `z/cow/merge_on_dv` | false | [101, 102, 103] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `z/cow/merge_on_key_and` | false | [3, 53, 101, 102] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `z/cow/merge_delete` | false | [] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `z/cow/merge_by_source` | false | [1, 2, 3] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `ctl/cow/merge_by_source_dupkey` | false | [1, 1] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |
| dml-native | `ctl/cow/merge_by_source_onekey` | false | [1] | REPARK-RAISES: raises This feature is not implemented: Unsupporte | REPARK-RAISES: raises This feature is not implemented: Unsupporte |

### The signed-zero shapes (sketch, Q5′), today and widened

| cell | ANSI | Spark | main | ff34 |
|---|---|---|---|---|
| `select/zero_eq_today` | true | boolean true | equal | equal |
| `select/zero_eq_widened` | true | boolean true | wrong-value | wrong-value |
| `select/zero_join_today` | true | bigint 1 | equal | equal |
| `select/zero_join_widened` | true | bigint 1 | equal | equal |
| `select/zero_intersect_today` | true | bigint 1 | equal | equal |
| `select/zero_intersect_widened` | true | bigint 1 | equal | equal |
| `select/zero_except_today` | true | bigint 0 | equal | equal |
| `select/zero_except_widened` | true | bigint 0 | equal | equal |
| `select/zero_union_distinct_today` | true | bigint 1 | equal | equal |
| `select/zero_union_distinct_widened` | true | bigint 1 | equal | equal |
| `select/zero_group_by_today` | true | bigint 1 | equal | equal |
| `select/zero_group_by_widened` | true | bigint 1 | equal | equal |
| `select/zero_in_subquery_today` | true | bigint 1 | equal | equal |
| `select/zero_in_subquery_widened` | true | bigint 1 | equal | equal |
| `merge_on_zero_today` | true | decimal(10,2) 1=-0.0=0.00 | WRONG-WRITE | WRONG-WRITE |
| `merge_on_zero_widened` | true | decimal(10,2) 1=-0.0=0.00 | WRONG-WRITE | WRONG-WRITE |
| `delete_zero_today` | true | decimal(10,2)  | EQUAL | EQUAL |
| `delete_zero_widened` | true | decimal(10,2)  | EQUAL | EQUAL |
| `select/zero_eq_today` | false | boolean true | equal | equal |
| `select/zero_eq_widened` | false | boolean true | wrong-value | wrong-value |
| `select/zero_join_today` | false | bigint 1 | equal | equal |
| `select/zero_join_widened` | false | bigint 1 | equal | equal |
| `select/zero_intersect_today` | false | bigint 1 | equal | equal |
| `select/zero_intersect_widened` | false | bigint 1 | equal | equal |
| `select/zero_except_today` | false | bigint 0 | equal | equal |
| `select/zero_except_widened` | false | bigint 0 | equal | equal |
| `select/zero_union_distinct_today` | false | bigint 1 | equal | equal |
| `select/zero_union_distinct_widened` | false | bigint 1 | equal | equal |
| `select/zero_group_by_today` | false | bigint 1 | equal | equal |
| `select/zero_group_by_widened` | false | bigint 1 | equal | equal |
| `select/zero_in_subquery_today` | false | bigint 1 | equal | equal |
| `select/zero_in_subquery_widened` | false | bigint 1 | equal | equal |
| `merge_on_zero_today` | false | decimal(10,2) 1=-0.0=0.00 | WRONG-WRITE | WRONG-WRITE |
| `merge_on_zero_widened` | false | decimal(10,2) 1=-0.0=0.00 | WRONG-WRITE | WRONG-WRITE |
| `delete_zero_today` | false | decimal(10,2)  | EQUAL | EQUAL |
| `delete_zero_widened` | false | decimal(10,2)  | EQUAL | EQUAL |

### Verdict categories per section and build

| section | ANSI | build | verdicts |
|---|---|---|---|
| dataframe-control | false | ff34 | equal 24, repark-raises-other 24, type-only 18, wrong-value 6 |
| dataframe-control | false | main | equal 24, repark-raises-other 24, type-only 18, wrong-value 6 |
| dataframe-control | true | ff34 | equal 22, repark-raises-other 24, type-only 20, wrong-value 6 |
| dataframe-control | true | main | equal 22, repark-raises-other 24, type-only 20, wrong-value 6 |
| dataframe-dd | false | ff34 | equal 6, repark-raises-other 36, repark-raises-overflow 27, type-only 24, wrong-value 38 |
| dataframe-dd | false | main | equal 6, repark-raises-other 36, repark-raises-overflow 27, type-only 24, wrong-value 38 |
| dataframe-dd | true | ff34 | equal 6, repark-raises-other 36, repark-raises-overflow 27, type-only 24, wrong-value 38 |
| dataframe-dd | true | main | equal 6, repark-raises-other 36, repark-raises-overflow 27, type-only 24, wrong-value 38 |
| dml-flip-native | false | ff34 | EQUAL 11, WRONG 4 |
| dml-flip-native | false | main | EQUAL 11, WRONG 4 |
| dml-flip-native | true | ff34 | EQUAL 11, WRONG 4 |
| dml-flip-native | true | main | EQUAL 11, WRONG 4 |
| dml-flip | false | ff34 | EQUAL 11, WRONG 4 |
| dml-flip | false | main | EQUAL 11, WRONG 4 |
| dml-flip | true | ff34 | EQUAL 11, WRONG 4 |
| dml-flip | true | main | EQUAL 11, WRONG 4 |
| dml-native | false | ff34 | BOTH-RAISE 1, EQUAL 8, REPARK-RAISES 16, WRONG 16 |
| dml-native | false | main | BOTH-RAISE 1, EQUAL 9, REPARK-RAISES 16, WRONG 15 |
| dml-native | true | ff34 | BOTH-RAISE 1, EQUAL 8, REPARK-RAISES 16, WRONG 16 |
| dml-native | true | main | BOTH-RAISE 1, EQUAL 9, REPARK-RAISES 16, WRONG 15 |
| dml | false | ff34 | BOTH-RAISE 2, EQUAL 33, REPARK-RAISES 2, WRONG 45 |
| dml | false | main | BOTH-RAISE 2, EQUAL 35, REPARK-RAISES 2, WRONG 43 |
| dml | true | ff34 | BOTH-RAISE 2, EQUAL 33, REPARK-RAISES 2, WRONG 45 |
| dml | true | main | BOTH-RAISE 2, EQUAL 35, REPARK-RAISES 2, WRONG 43 |
| fexpr-dd | false | ff34 | equal 3, repark-raises-overflow 1, wrong-value 8 |
| fexpr-dd | false | main | equal 3, repark-raises-overflow 1, wrong-value 8 |
| fexpr-dd | true | ff34 | equal 3, repark-raises-overflow 1, wrong-value 8 |
| fexpr-dd | true | main | equal 3, repark-raises-overflow 1, wrong-value 8 |
| native-control | native | ff34 | equal 422, repark-raises-other 136, spark-raises-repark-answers 30, type-only 100, wrong-value 104 |
| native-control | native | main | equal 422, repark-raises-other 136, spark-raises-repark-answers 30, type-only 100, wrong-value 104 |
| native-dd | native | ff34 | both-raise 2, equal 718, repark-raises-other 201, repark-raises-overflow 96, type-only 100, wrong-value 76 |
| native-dd | native | main | both-raise 2, equal 718, repark-raises-other 181, repark-raises-overflow 114, type-only 100, wrong-value 78 |
| revision-select | false | ff34 | both-raise 1, equal 17, repark-raises-other 1, type-only 1, wrong-value 8 |
| revision-select | false | main | both-raise 1, equal 17, repark-raises-other 1, type-only 1, wrong-value 8 |
| revision-select | true | ff34 | both-raise 1, equal 17, repark-raises-other 1, type-only 1, wrong-value 8 |
| revision-select | true | main | both-raise 1, equal 17, repark-raises-other 1, type-only 1, wrong-value 8 |
| revision-write | false | ff34 | EQUAL 4, WRONG-WRITE 20 |
| revision-write | false | main | EQUAL 4, WRONG-WRITE 20 |
| revision-write | true | ff34 | EQUAL 4, WRONG-WRITE 20 |
| revision-write | true | main | EQUAL 4, WRONG-WRITE 20 |
| sql-control | false | ff34 | equal 570, repark-raises-other 72, spark-raises-repark-answers 30, type-only 40, wrong-value 80 |
| sql-control | false | main | equal 570, repark-raises-other 72, spark-raises-repark-answers 30, type-only 40, wrong-value 80 |
| sql-control | true | ff34 | equal 480, repark-raises-other 72, spark-raises-repark-answers 30, type-only 114, wrong-value 96 |
| sql-control | true | main | equal 480, repark-raises-other 72, spark-raises-repark-answers 30, type-only 114, wrong-value 96 |
| sql-dd | false | ff34 | both-raise 2, equal 256, repark-raises-other 97, repark-raises-overflow 332, type-only 200, wrong-value 306 |
| sql-dd | false | main | both-raise 2, equal 256, repark-raises-other 61, repark-raises-overflow 368, type-only 200, wrong-value 306 |
| sql-dd | true | ff34 | both-raise 2, equal 256, repark-raises-other 97, repark-raises-overflow 332, type-only 200, wrong-value 306 |
| sql-dd | true | main | both-raise 2, equal 256, repark-raises-other 61, repark-raises-overflow 368, type-only 200, wrong-value 306 |
| write | false | ff34 | BOTH-RAISE 1, EQUAL 6, REPARK-RAISES 9, WRONG-WRITE 14 |
| write | false | main | BOTH-RAISE 1, EQUAL 5, REPARK-RAISES 9, WRONG-WRITE 15 |
| write | true | ff34 | BOTH-RAISE 1, EQUAL 6, REPARK-RAISES 9, WRONG-WRITE 14 |
| write | true | main | BOTH-RAISE 1, EQUAL 5, REPARK-RAISES 9, WRONG-WRITE 15 |

### The emulated head: the widened forms run on ff34baf5

Faithfulness (Spark answers its widened forms as it answers the originals): {"coerce_spark_true": {"cells": 165846, "widened_differs": 0}, "dml_spark_true": {"cells": 97, "widened_differs": []}, "family_spark_true": {"eq": 0, "in": 0, "lt": 0, "ns": 0}}

| set | ANSI | family | cells | head wrong | head wrong, main right | head wrong, ff34 right | head right, both wrong |
|---|---|---|---|---|---|---|---|
| coercion fuzz | false | all/all | 186570 | 22924 | 2761 | 1561 | 69320 |
| coercion fuzz | false | coalesce/d38_18 | 3455 | 0 | 0 | 0 | 3201 |
| coercion fuzz | false | coalesce/other | 17275 | 0 | 0 | 0 | 15215 |
| coercion fuzz | false | eq/d38_18 | 3455 | 554 | 554 | 256 | 337 |
| coercion fuzz | false | eq/other | 17275 | 3273 | 0 | 0 | 2351 |
| coercion fuzz | false | eqf/d38_18 | 3455 | 771 | 535 | 535 | 357 |
| coercion fuzz | false | eqf/other | 17275 | 3350 | 0 | 0 | 3031 |
| coercion fuzz | false | greatest/d38_18 | 3455 | 0 | 0 | 0 | 3201 |
| coercion fuzz | false | greatest/other | 17275 | 0 | 0 | 0 | 15215 |
| coercion fuzz | false | if/d38_18 | 3455 | 0 | 0 | 0 | 3201 |
| coercion fuzz | false | if/other | 17275 | 0 | 0 | 0 | 15215 |
| coercion fuzz | false | in/d38_18 | 3455 | 554 | 554 | 256 | 337 |
| coercion fuzz | false | in/other | 17275 | 3273 | 0 | 0 | 2351 |
| coercion fuzz | false | le/d38_18 | 3455 | 294 | 282 | 129 | 154 |
| coercion fuzz | false | le/other | 17275 | 3276 | 0 | 0 | 1156 |
| coercion fuzz | false | lt/d38_18 | 3455 | 294 | 282 | 129 | 154 |
| coercion fuzz | false | lt/other | 17275 | 3276 | 0 | 0 | 1156 |
| coercion fuzz | false | ns/d38_18 | 3455 | 554 | 554 | 256 | 337 |
| coercion fuzz | false | ns/other | 17275 | 3455 | 0 | 0 | 2351 |
| coercion fuzz | true | all/all | 165846 | 3110 | 2761 | 1561 | 69320 |
| coercion fuzz | true | coalesce/d38_18 | 3455 | 0 | 0 | 0 | 3201 |
| coercion fuzz | true | coalesce/other | 17275 | 0 | 0 | 0 | 15215 |
| coercion fuzz | true | eq/d38_18 | 3455 | 554 | 554 | 256 | 337 |
| coercion fuzz | true | eq/other | 13821 | 1 | 0 | 0 | 2351 |
| coercion fuzz | true | eqf/d38_18 | 3455 | 771 | 535 | 535 | 357 |
| coercion fuzz | true | eqf/other | 13821 | 78 | 0 | 0 | 3031 |
| coercion fuzz | true | greatest/d38_18 | 3455 | 0 | 0 | 0 | 3201 |
| coercion fuzz | true | greatest/other | 17275 | 0 | 0 | 0 | 15215 |
| coercion fuzz | true | if/d38_18 | 3455 | 0 | 0 | 0 | 3201 |
| coercion fuzz | true | if/other | 17275 | 0 | 0 | 0 | 15215 |
| coercion fuzz | true | in/d38_18 | 3455 | 554 | 554 | 256 | 337 |
| coercion fuzz | true | in/other | 13821 | 1 | 0 | 0 | 2351 |
| coercion fuzz | true | le/d38_18 | 3455 | 294 | 282 | 129 | 154 |
| coercion fuzz | true | le/other | 13821 | 4 | 0 | 0 | 1156 |
| coercion fuzz | true | lt/d38_18 | 3455 | 294 | 282 | 129 | 154 |
| coercion fuzz | true | lt/other | 13821 | 4 | 0 | 0 | 1156 |
| coercion fuzz | true | ns/d38_18 | 3455 | 554 | 554 | 256 | 337 |
| coercion fuzz | true | ns/other | 13821 | 1 | 0 | 0 | 2351 |
| `d eq` family | false | | 60000 | 0 | 0 | 0 | |
| `d in` family | false | | 60000 | 0 | 0 | 0 | |
| `d lt` family | false | | 60000 | 0 | 0 | 0 | |
| `d ns` family | false | | 60000 | 0 | 0 | 0 | |
| `d eq` family | true | | 60000 | 0 | 0 | 0 | |
| `d in` family | true | | 60000 | 0 | 0 | 0 | |
| `d lt` family | true | | 60000 | 0 | 0 | 0 | |
| `d ns` family | true | | 60000 | 0 | 0 | 0 | |
| DML, native door | false | | 56 | 27 | 2 | 9 | 8 |
| DML, repark door | false | | 97 | 30 | 2 | 26 | 19 |
| DML, native door | true | | 56 | 27 | 2 | 9 | 8 |
| DML, repark door | true | | 97 | 30 | 2 | 26 | 19 |

| DECIMAL→DOUBLE kernel | d10_2 | d20_6 | d30_15 | d38_10 | d38_18 |
|---|---|---|---|---|---|
| ff34 wrong | 0 of 5455 | 0 of 5455 | 724 of 5455 | 118 of 5455 | 967 of 5455 |
| main wrong | 0 of 5455 | 0 of 5455 | 724 of 5455 | 118 of 5455 | 967 of 5455 |

Up: [map.md](map.md).

## Common-type cells (Spark door), per ANSI mode
| ANSI | cells | agree | both_raise | wrong_answer | wrong_type | wrong_write | over_raise | missed_raise |
|---|---|---|---|---|---|---|---|---|
| false | 946 | 928 | 4 | 2 | 0 | 0 | 12 | 0 |
| true | 946 | 928 | 10 | 2 | 0 | 0 | 6 | 0 |

## Every cause (Spark door), per ANSI mode
| cause | ANSI | cells | agree | both_raise | wrong_answer | wrong_type | wrong_write | over_raise | missed_raise |
|---|---|---|---|---|---|---|---|---|---|
| arithmetic-control | false | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |
| arithmetic-control | true | 2 | 1 | 1 | 0 | 0 | 0 | 0 | 0 |
| avg-round | false | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 0 |
| avg-round | true | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 0 |
| common-type | false | 946 | 928 | 4 | 2 | 0 | 0 | 12 | 0 |
| common-type | true | 946 | 928 | 10 | 2 | 0 | 0 | 6 | 0 |
| confounded:array_union | false | 10 | 0 | 6 | 0 | 0 | 0 | 0 | 4 |
| confounded:array_union | true | 10 | 0 | 6 | 0 | 0 | 0 | 0 | 4 |
| confounded:join_full_using | false | 10 | 0 | 0 | 0 | 0 | 0 | 10 | 0 |
| confounded:join_full_using | true | 10 | 0 | 0 | 0 | 0 | 0 | 10 | 0 |
| confounded:lag | false | 10 | 0 | 0 | 0 | 0 | 0 | 10 | 0 |
| confounded:lag | true | 10 | 0 | 0 | 0 | 0 | 0 | 10 | 0 |
| confounded:lead | false | 10 | 0 | 0 | 0 | 0 | 0 | 10 | 0 |
| confounded:lead | true | 10 | 0 | 0 | 0 | 0 | 0 | 10 | 0 |
| confounded:nullif | false | 10 | 10 | 0 | 0 | 0 | 0 | 0 | 0 |
| confounded:nullif | true | 10 | 10 | 0 | 0 | 0 | 0 | 0 | 0 |
| confounded:nvl | false | 10 | 0 | 0 | 4 | 6 | 0 | 0 | 0 |
| confounded:nvl | true | 10 | 0 | 0 | 4 | 6 | 0 | 0 | 0 |
| control:array_union | false | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 2 |
| control:array_union | true | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 2 |
| control:core | false | 165 | 161 | 0 | 0 | 4 | 0 | 0 | 0 |
| control:core | true | 165 | 161 | 0 | 0 | 4 | 0 | 0 | 0 |
| control:join_full_using | false | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| control:join_full_using | true | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| control:lag | false | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| control:lag | true | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| control:lead | false | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| control:lead | true | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| control:nullif | false | 2 | 0 | 0 | 0 | 2 | 0 | 0 | 0 |
| control:nullif | true | 2 | 0 | 0 | 0 | 2 | 0 | 0 | 0 |
| control:nvl | false | 2 | 0 | 0 | 0 | 2 | 0 | 0 | 0 |
| control:nvl | true | 2 | 0 | 0 | 0 | 2 | 0 | 0 | 0 |
| control:revision | false | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |
| control:revision | true | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |
| grouping-control | false | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |
| grouping-control | true | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |
| int-literal-type | false | 1 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| int-literal-type | true | 1 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| legacy | false | 94 | 66 | 0 | 0 | 0 | 0 | 28 | 0 |
| legacy | true | 94 | 66 | 28 | 0 | 0 | 0 | 0 | 0 |
| pivot-unsupported | false | 4 | 0 | 0 | 0 | 0 | 0 | 4 | 0 |
| pivot-unsupported | true | 4 | 0 | 0 | 0 | 0 | 0 | 4 | 0 |
| range-offset | false | 10 | 4 | 0 | 0 | 0 | 0 | 6 | 0 |
| range-offset | true | 10 | 4 | 0 | 0 | 0 | 0 | 6 | 0 |
| update-cse | false | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| update-cse | true | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| using-unaliased-refusal | false | 7 | 0 | 0 | 0 | 0 | 0 | 7 | 0 |
| using-unaliased-refusal | true | 7 | 0 | 0 | 0 | 0 | 0 | 7 | 0 |

## EQUAL on main and not EQUAL on the head: 0

## Transitions main -> head (engine, ANSI, main, head: cells)
- native false agree -> agree: 176
- native false both_raise -> both_raise: 12
- native false over_raise -> over_raise: 420
- native false wrong_answer -> wrong_answer: 279
- native false wrong_type -> wrong_type: 20
- native true agree -> agree: 176
- native true both_raise -> both_raise: 19
- native true over_raise -> over_raise: 413
- native true wrong_answer -> wrong_answer: 279
- native true wrong_type -> wrong_type: 20
- repark false agree -> agree: 291
- repark false both_raise -> both_raise: 8
- repark false missed_raise -> both_raise: 2
- repark false missed_raise -> missed_raise: 6
- repark false over_raise -> agree: 433
- repark false over_raise -> over_raise: 95
- repark false wrong_answer -> agree: 406
- repark false wrong_answer -> wrong_answer: 7
- repark false wrong_answer -> wrong_type: 4
- repark false wrong_type -> agree: 10
- repark false wrong_type -> wrong_type: 10
- repark false wrong_write -> agree: 38
- repark true agree -> agree: 290
- repark true both_raise -> both_raise: 43
- repark true missed_raise -> both_raise: 2
- repark true missed_raise -> missed_raise: 6
- repark true over_raise -> agree: 433
- repark true over_raise -> over_raise: 61
- repark true wrong_answer -> agree: 406
- repark true wrong_answer -> wrong_answer: 7
- repark true wrong_answer -> wrong_type: 4
- repark true wrong_type -> agree: 10
- repark true wrong_type -> wrong_type: 10
- repark true wrong_write -> agree: 38

## Every non-EQUAL common-type cell on the head, named
- ANSI false `agg/having_avg_max/t`: wrong_answer — AVG-DECIMAL-ROUND-1 (side card): the HAVING row now matches; the shown avg truncates
- ANSI false `const/i64_eq_hlit/tx/where`: over_raise — DECIMAL-CAST-SELECTIVITY (new finding): DataFusion's selectivity assertion on a BIGINT-edge cast; a user-written same-type cast raises on main
- ANSI false `dfconst/fexpr_a_eq_lit/t`: wrong_answer — FEXPR-DECIMAL-LITERAL (new finding): F.expr types 1.25 as DOUBLE, so the pair is decimal beside double
- ANSI false `literal/coalesce_overflow_lit/t`: over_raise — ruling 6, carded: the literal's own cast refuses at planning (DECIMAL-LITERAL-OVERFLOW-1, new card)
- ANSI false `literal/coalesce_overflow_lit/tx`: over_raise — ruling 6, carded: the literal's own cast refuses at planning (DECIMAL-LITERAL-OVERFLOW-1, new card)
- ANSI false `literal/u_overflow39/t`: over_raise — ruling 6, carded: the literal's own cast refuses at planning (DECIMAL-LITERAL-OVERFLOW-1, new card); both raise with ANSI on
- ANSI false `literal/u_overflow39/tx`: over_raise — ruling 6, carded: the literal's own cast refuses at planning (DECIMAL-LITERAL-OVERFLOW-1, new card); both raise with ANSI on
- ANSI false `literal/u_overflow_lit/t`: over_raise — ruling 6, carded: the literal's own cast refuses at planning (DECIMAL-LITERAL-OVERFLOW-1, new card); both raise with ANSI on
- ANSI false `literal/u_overflow_lit/tx`: over_raise — ruling 6, carded: the literal's own cast refuses at planning (DECIMAL-LITERAL-OVERFLOW-1, new card); both raise with ANSI on
- ANSI false `literal/where_overflow_lit/t`: over_raise — ruling 6, carded: the literal's own cast refuses at planning (DECIMAL-LITERAL-OVERFLOW-1, new card); both raise with ANSI on
- ANSI false `literal/where_overflow_lit/tx`: over_raise — ruling 6, carded: the literal's own cast refuses at planning (DECIMAL-LITERAL-OVERFLOW-1, new card); both raise with ANSI on
- ANSI false `rev/m1/create_map_keys_apart`: over_raise — MAP-FILTER-ORDER (new finding): the map is evaluated above the filter and meets row 6's NULL key; same-type user casts raise on main
- ANSI false `rev/m1/create_map_keys_id1`: both_raise — both raise DUPLICATED_MAP_KEY (revision M1, as ruled)
- ANSI false `rev/m1/map_concat_keys_all`: both_raise — both raise; RePark's error is NULL_MAP_KEY from MAP-FILTER-ORDER, Spark's DUPLICATED_MAP_KEY
- ANSI false `rev/m1/map_concat_keys_apart`: over_raise — MAP-FILTER-ORDER (new finding)
- ANSI false `rev/m1/map_concat_keys_id1`: both_raise — both raise DUPLICATED_MAP_KEY (revision M1, as ruled)
- ANSI false `rev/m1/map_keys_of_concat`: over_raise — MAP-FILTER-ORDER (new finding)
- ANSI false `rev/pin/struct_cmp`: both_raise — both raise (ruling 5: pin only)
- ANSI true `agg/having_avg_max/t`: wrong_answer — AVG-DECIMAL-ROUND-1 (side card): the HAVING row now matches; the shown avg truncates
- ANSI true `const/i64_eq_hlit/tx/where`: over_raise — DECIMAL-CAST-SELECTIVITY (new finding): DataFusion's selectivity assertion on a BIGINT-edge cast; a user-written same-type cast raises on main
- ANSI true `dfconst/fexpr_a_eq_lit/t`: wrong_answer — FEXPR-DECIMAL-LITERAL (new finding): F.expr types 1.25 as DOUBLE, so the pair is decimal beside double
- ANSI true `literal/coalesce_overflow_lit/t`: over_raise — ruling 6, carded: the literal's own cast refuses at planning (DECIMAL-LITERAL-OVERFLOW-1, new card)
- ANSI true `literal/coalesce_overflow_lit/tx`: over_raise — ruling 6, carded: the literal's own cast refuses at planning (DECIMAL-LITERAL-OVERFLOW-1, new card)
- ANSI true `literal/u_overflow39/t`: both_raise — ruling 6, carded: the literal's own cast refuses at planning (DECIMAL-LITERAL-OVERFLOW-1, new card); both raise with ANSI on
- ANSI true `literal/u_overflow39/tx`: both_raise — ruling 6, carded: the literal's own cast refuses at planning (DECIMAL-LITERAL-OVERFLOW-1, new card); both raise with ANSI on
- ANSI true `literal/u_overflow_lit/t`: both_raise — ruling 6, carded: the literal's own cast refuses at planning (DECIMAL-LITERAL-OVERFLOW-1, new card); both raise with ANSI on
- ANSI true `literal/u_overflow_lit/tx`: both_raise — ruling 6, carded: the literal's own cast refuses at planning (DECIMAL-LITERAL-OVERFLOW-1, new card); both raise with ANSI on
- ANSI true `literal/where_overflow_lit/t`: both_raise — ruling 6, carded: the literal's own cast refuses at planning (DECIMAL-LITERAL-OVERFLOW-1, new card); both raise with ANSI on
- ANSI true `literal/where_overflow_lit/tx`: both_raise — ruling 6, carded: the literal's own cast refuses at planning (DECIMAL-LITERAL-OVERFLOW-1, new card); both raise with ANSI on
- ANSI true `rev/m1/create_map_keys_apart`: over_raise — MAP-FILTER-ORDER (new finding): the map is evaluated above the filter and meets row 6's NULL key; same-type user casts raise on main
- ANSI true `rev/m1/create_map_keys_id1`: both_raise — both raise DUPLICATED_MAP_KEY (revision M1, as ruled)
- ANSI true `rev/m1/map_concat_keys_all`: both_raise — both raise; RePark's error is NULL_MAP_KEY from MAP-FILTER-ORDER, Spark's DUPLICATED_MAP_KEY
- ANSI true `rev/m1/map_concat_keys_apart`: over_raise — MAP-FILTER-ORDER (new finding)
- ANSI true `rev/m1/map_concat_keys_id1`: both_raise — both raise DUPLICATED_MAP_KEY (revision M1, as ruled)
- ANSI true `rev/m1/map_keys_of_concat`: over_raise — MAP-FILTER-ORDER (new finding)
- ANSI true `rev/pin/struct_cmp`: both_raise — both raise (ruling 5: pin only)

## Struct cells agreed after rendering PySpark's Row and RePark's dict alike
- repark ANSI false `literal/u_struct/t`
- repark ANSI false `literal/u_struct/tx`
- repark ANSI false `rev/pin/struct_pos`
- repark ANSI true `literal/u_struct/t`
- repark ANSI true `literal/u_struct/tx`
- repark ANSI true `rev/pin/struct_pos`

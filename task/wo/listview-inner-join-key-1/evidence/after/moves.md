# LISTVIEW-INNER-JOIN-KEY-1, build item 3: the after-table, the revert record, the struct pins

**Builder:** Claude Opus 5.5 (`claude-opus-5-5`), 2026-11-11. **Head:** item 2, `5aaeb19b`, release profile,
`debug_assertions` false. **Base:** item 1's `main` leg (`../item1/raw/main`, the branch base, no product code).
**Oracle:** item 1's Spark 4.1.2 leg (`../item1/raw/spark`); no Spark leg ran in this item.

## How it was measured

- `../item1/probe_item1.py`, unchanged, ran its `main` leg on the head build (label `head`, all eight sections) from
  a scratch copy, and `--render head` wrote [table.md](table.md). The head leg is in `raw/head/`.
- `plan.jsonl` keeps item 1's committed shape: the stage before and after `eliminate_cross_join`, the physical join,
  the key stages' text, and the full stage list for the `LV_I64` and `L_I64` `df_str` cells only.
- [moves.py](moves.py) labels every cell on base and head with the probe's own `label_of` against the Spark leg and
  checks the result against ruling 1's prediction (`python3 -I moves.py`, no arguments). The random alias suffixes
  (`_repark_jl_…`, `__repark_l_…`) are masked when outcomes and plans are compared.

## The moves (base label → head label)

| Section | Base | Head | Cells |
|---|---|---|---|
| failing | CROSS-PRODUCT | EQUAL | 14 |
| failing | WRONG-ANSWER | EQUAL | 14 |
| failing | WRONG-WRITE | EQUAL | 7 |
| variant | CROSS-PRODUCT | EQUAL | 14 |
| variant | WRONG-WRITE | EQUAL | 7 |
| route | CROSS-PRODUCT | EQUAL | 3 |
| mixed | REFUSED-WHERE-SPARK-ANSWERED (key-name form) | EQUAL | 24 |

**Unchanged:** failing EQUAL 48 and REFUSED 16 (7 SQL `USING`, 9 `crossJoin().where`); lateral
ACCEPTED-WHERE-SPARK-REFUSED 17; mixed `ON` twins REFUSED 12; native EQUAL 19 and `USING` REFUSED 7; route EQUAL 1
and STRUCK 4; using EQUAL 5 and REFUSED 5; variant EQUAL 14, BOTH-REFUSED 5 (`MAP_LV`) and STRUCK 15.

## Against ruling 1's prediction

- **Every move is predicted:** 31 cross products, 14 wrong answers and 14 wrong writes to EQUAL; the 24 mixed
  key-name cells to EQUAL. `moves.py` finds no unpredicted move.
- **Every base-EQUAL cell stays EQUAL** (87 cells).
- **No plain List cell moves:** every `L_I64`, `LL_I64`, `FSL2_I64`, `L_UTF8`, `L_STRUCT` and `NEST_L` outcome is
  identical on base and head once alias suffixes are masked (the two `crossJoin().where` refusals differ only there).
- **Lateral:** all 17 `lateralJoin` key-name cells now return the rows of Spark's key-name join of the same kind and
  join type (the 8 base cross products included). The label stays ACCEPTED-WHERE-SPARK-REFUSED: Spark refuses key
  names there (LATERAL-USING-ACCEPT-1).
- **Plans:** the 10 control plan records (`L_I64`, `NEST_L` × `df_str`, `df_list`, `df_two`, aggregate, `writeTo`)
  are identical to base before and after `eliminate_cross_join` and in the physical join. All 35 ListView-kind plan
  records plan `HashJoinExec` on a `CAST(… AS List…)` key.

## The revert record (each seat on its own)

Each revert was built in the ci profile from the head with one change, run, and restored (`git status` clean
after each). The facade pin is `test_listview_inner_join_key_1.py` (309 tests: 283 cells, 25 reached types, one count).

| Reverted | How | Rust pins red (`join_keys` module, 8) | Facade pin red |
|---|---|---|---|
| `case_bind.rs` | `git checkout HEAD~1 --` the file | 2: `a_listview_key_join_keeps_its_hash_join_on_the_cast_key`, `a_mixed_listview_and_list_key_join_answers_on_the_cast_key` | 97: every DataFrame key-name cell on a non-hashable kind (`df_str` base, one and big, `df_list`, `df_two`, aggregate, `writeTo`; 21 variant cells; 3 pandas routes; 24 mixed) |
| `subquery.rs` | `git checkout HEAD~1 --` the file | 0 (it holds none) | 8: the 8 `lateralJoin` cells that are cross products on base |
| the helper | `HEAD~1` has no `join_keys.rs`, so `hashable_join_keys` was made to return `Ok(None)` first | 4: the two above, `only_the_unhashable_keys_are_cast_on_each_side`, `a_union_at_any_depth_is_refused_with_its_name` | 105: the 97 and the 8 |

The `case_bind.rs` revert keeps `a_hashable_key_join_builds_mains_plan` green (the List plan is main's), and every
List control, `INSERT … SELECT` over `ON`, and SQL `USING` cell stays green under all three.

## The struct units' pins, both profiles

The pins of #1283 (`test_struct_setop_reorder_refusal_1.py`), #1290 (`test_struct_setop_type_family_1.py`), #1292
(`test_struct_compare_by_position_1.py`) and #1295 (`test_struct_compare_seat_family_1.py`) ran with this unit's pin,
`-n 8 --dist loadgroup`: **4328 passed** on the head's release build (`maturin develop`) and **4328 passed** on its ci
build (a wheel in a scratch site; `repark.__file__` checked). Their only edit is the 24 `known` markers (ruling 9);
those 24 cells now assert Spark's rows and pass. Their Rust pins run in item 4's `cargo test` gates.

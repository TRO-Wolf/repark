# map — repark-iceberg/src/write/narrowed_store

## Purpose

ICE-TSNS-NARROW-REFUSE-1 (2026-10-10, parity row R-017): the unit pins of the parent
`narrowed_store.rs`, the store guard that refuses a nanosecond value type coercion narrowed.
The parent's entry in [../map.md](../map.md) holds the reasons.

## Contents

- [tests.rs](tests.rs) — five pins over plans built with the DataFrame API; the marks and
  the written-narrowing function are local functions of the shared names:
  - `a_cast_over_a_marked_value_is_followed_through_every_carrying_position_and_no_other`:
    a cast back, either result of a `CASE` and the written-narrowing function over a marked
    value are found, and the narrowing names the value (`src.m`); a plain column, a `CASE`
    that only tests it, a cast through a string, a cast over an unmarked value and a mark
    with no cast on it are not.
  - `a_written_call_stores_only_where_it_equals_the_call_over_the_nanosecond_value`: the
    written narrowing and a cast to an instant store over either source; `date_trunc` stores
    over a zoned mark and refuses over a naive one; the date function is the reverse; a
    `date_trunc` over a cast its own signature asked for stores; a cast to a naive
    microsecond type refuses.
  - `a_narrowed_value_is_followed_through_the_plan_nodes_between_it_and_the_store`: a filter,
    a sort, a limit, `DISTINCT`, an alias, a union, an aggregate and a join.
  - `a_narrowed_value_is_followed_into_a_view_the_scan_holds_and_a_union_branch`: a scan
    whose source carries a plan; a `UNION` names the branch narrowed beside an untyped NULL
    before one narrowed beside a value.
  - `only_a_nanosecond_target_refuses_and_the_text_names_the_column_and_the_value`: the two
    exact texts, no refusal for a microsecond target, a supply of another width or another
    column; `refuse_narrowed_ns_inserts` looks through the planner's cast on a column and
    `refuse_narrowed_ns_columns` does not.
  pins: ice-tsns-narrow-refuse-1/C-007, C-008, C-019, C-020, C-021

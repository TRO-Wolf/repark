# map — repark-iceberg/src/write/narrowed_store

## Purpose

ICE-TSNS-NARROW-REFUSE-1 (2026-10-10, parity row R-017): the unit pins of the parent
`narrowed_store.rs`, the store guard that refuses a nanosecond value narrowed beside an
untyped `NULL`. The parent's entry in [../map.md](../map.md) holds the reasons.

## Contents

- [tests.rs](tests.rs) — five pins over plans built with the DataFrame API; the mark is a
  local function of the shared name, wrapped around `coalesce(CAST(m AS µs), NULL)` as the
  analyzer leaves it:
  - `a_marked_value_is_followed_through_every_carrying_position_and_no_other`: an alias, a
    cast, either result of a `CASE` and `date_trunc` carry the mark; a plain column, a
    `CASE` that only tests it, a cast through a string, a mark with no narrowing cast beside
    it and a mark whose narrowing sits one level down do not.
  - `a_marked_value_is_followed_through_the_plan_nodes_between_it_and_the_store`: a filter,
    a sort, a limit, `DISTINCT`, an alias, a union with a plain branch, an aggregate and a
    join; the column beside it is never refused.
  - `a_marked_value_is_followed_into_a_view_the_scan_holds`: a table scan whose source
    carries a plan.
  - `a_marked_null_branch_of_a_union_refuses_only_beside_a_narrowed_branch`.
  - `only_a_nanosecond_target_refuses_and_the_text_names_the_column`: the exact text for a
    naive target, the zoned names, no refusal for a microsecond target, for a supply of
    another width or for a marked value bound for another column.
  pins: ice-tsns-narrow-refuse-1/C-007, C-008

# map — repark-functions/src/null_narrowing

## Purpose

ICE-TSNS-NARROW-REFUSE-1 (2026-10-10, parity row R-017): the unit pins of the parent
`null_narrowing.rs`, the two marks of a nanosecond value that type coercion narrows beside an
untyped `NULL` or beside a microsecond value. The parent's entry in [../map.md](../map.md)
holds the reasons.

## Contents

- [tests.rs](tests.rs) — eleven pins over hand-built expressions and plans, no session:
  - `an_untyped_null_beside_a_nanosecond_branch_is_tagged_and_no_other_null`: `coalesce` in
    either order and a `CASE` are tagged; a NULL beside a microsecond or an integer branch,
    a NULL the statement typed and a node with no NULL are not.
  - `the_nanosecond_branch_beside_a_widened_untyped_null_is_marked_as_beside_a_null`: the
    nanosecond sibling is wrapped in `__repark_narrowed_beside_null__` and no literal keeps
    the tag.
  - `the_nanosecond_branch_beside_a_typed_null_or_a_microsecond_value_is_marked_as_beside_a_value`:
    a typed NULL, a microsecond column and a `CASE` give the other mark; a node the session
    did not ask about is left alone; settling twice changes nothing.
  - `a_branch_the_statement_narrowed_and_a_node_with_one_kind_are_not_marked`.
  - `a_cast_from_nanoseconds_to_an_instant_before_coercion_is_the_statements`: before
    coercion such a cast becomes the written-narrowing function and takes no mark.
  - `a_cast_coercion_left_over_a_nanosecond_value_is_marked`: at the root (also under an
    alias) and as a branch of `coalesce` or a `CASE` beside a microsecond sibling; a cast
    that only fits a function's signature (`date_trunc`), a node whose branches are all such
    casts, and a predicate are not marked.
  - `the_session_rule_marks_a_cast_coercion_left_at_the_root_of_a_projection`: the
    session's timestamp rule, run over a projection whose root is such a cast, leaves the
    beside-a-value mark on it and none on the column beside it.
  - `the_marker_is_its_argument_and_the_optimizer_drops_it`: both marks return their
    argument's type and nullability, and `simplify` answers the argument.
  - `the_nanosecond_branch_of_a_union_beside_an_untyped_null_branch_is_marked`,
    `the_nanosecond_branch_of_a_union_beside_a_microsecond_branch_is_marked`,
    `a_union_of_one_kind_is_left_alone`.
  pins: ice-tsns-narrow-refuse-1/C-003, C-019, C-020

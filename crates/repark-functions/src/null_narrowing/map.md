# map — repark-functions/src/null_narrowing

## Purpose

ICE-TSNS-NARROW-REFUSE-1 (2026-10-10, parity row R-017): the unit pins of the parent
`null_narrowing.rs`, the mark of a nanosecond value that type coercion narrows beside an
untyped `NULL`. The parent's entry in [../map.md](../map.md) holds the reasons.

## Contents

- [tests.rs](tests.rs) — eight pins over hand-built expressions and plans, no session:
  - `an_untyped_null_beside_a_nanosecond_branch_is_tagged_and_no_other_null`: `coalesce` in
    either order and a `CASE` are tagged; a NULL beside a microsecond or an integer branch,
    a NULL the statement typed and a node with no NULL are not.
  - `a_widened_null_beside_a_nanosecond_branch_is_marked_and_the_tag_is_gone`: the settled
    node is the wrapper around the node as coercion left it, and no literal keeps the tag.
  - `a_branch_narrowed_before_the_null_is_read_is_not_marked`: a sibling that is already a
    microsecond value (a written cast) leaves no wrapper and no tag.
  - `a_null_that_no_coercion_widened_is_untagged_and_not_marked`: the node comes back as it
    was written.
  - `the_marker_is_its_argument_and_the_optimizer_drops_it`: the wrapper's return field is
    its argument's type and nullability, and `simplify` answers the argument.
  - `an_untyped_null_branch_of_a_union_beside_nanoseconds_is_tagged_then_marked`: with and
    without an alias, the branch keeps its column name.
  - `a_null_column_that_is_not_a_projection_is_tagged_in_one_above_it`: a branch under a
    `LIMIT` gets a projection that holds the tagged NULL.
  - `a_null_branch_of_a_union_beside_microseconds_is_left_alone`: no nanosecond branch, a
    typed NULL, and a branch another cast already narrowed.
  pins: ice-tsns-narrow-refuse-1/C-003

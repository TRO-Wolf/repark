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
  Fold 2 (2026-10-11) added ten:
  - `a_cast_to_the_sql_timestamp_type_below_the_root_is_read_before_coercion`: the wire
    cast over a naive and a zoned nanosecond value, as an argument, becomes the
    written-narrowing function (also beside an untyped NULL, which is then not tagged); at
    the root, under an alias, over a microsecond value and over a date it does not; a
    coercion cast over it is not marked.
  - `a_zoned_branch_of_a_union_beside_a_date_branch_is_marked_before_coercion`.
  - `a_cast_coercion_left_inside_a_container_or_to_the_wire_type_is_marked`: a list cast
    beside a microsecond list, a zoned value cast to the wire type beside a date cast, the
    root with and without the wire kind; a cast that keeps the nanoseconds, a cast to a
    string and a lone list cast are not.
  - `a_branch_is_marked_only_when_the_node_loses_the_nanoseconds`: a function typed a
    microsecond list is marked with no cast in it; one typed a nanosecond list,
    `named_struct` and one typed an integer are not.
  - `only_a_node_that_produces_a_column_is_marked_and_a_producer_is_found_without_a_scan`:
    a filter and a sort take no mark, a projection does; an outer reference and a lambda
    variable over an empty relation are marked; a literal is no producer.
  - `a_union_schema_loses_the_tag_and_keeps_its_other_metadata`.
  - `a_written_cast_over_a_microsecond_column_is_kept_only_above_a_mark`.
  - `a_settled_tag_leaves_no_alias_and_a_null_cast_is_not_read_as_written`.
  - `a_pending_timestamp_literal_and_a_stale_union_type_are_not_nanoseconds`: a coercion
    cast over a `TIMESTAMP` literal's wire cast or a nanosecond literal is not marked; a
    union planned as nanoseconds whose branches are microseconds takes their type, the
    union above it marks nothing, and a union whose branches agree is left as it is.
  - `the_mark_returns_the_field_of_its_argument`: the field, the argument's name, and the
    optimizer drops the written-narrowing function over a microsecond instant only.
  pins: ice-tsns-narrow-refuse-1/C-003, C-019, C-020

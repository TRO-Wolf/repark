# map — repark-iceberg/src/write/narrowed_store

## Purpose

ICE-TSNS-NARROW-REFUSE-1 (2026-10-10, parity row R-017): the unit pins of the parent
`narrowed_store.rs`, the store guard that refuses a nanosecond value type coercion narrowed.
The parent's entry in [../map.md](../map.md) holds the reasons.

## Contents

- [text.rs](text.rs) — fold 2 (2026-10-11): `sql_text`, the SQL spelling of the narrowed
  value in the refusal (no mark, no Arrow type or interval debug text). Pinned by the door
  pin `the_refusal_names_what_the_statement_wrote` in repark-spark.
  Fold 3 (2026-10-11): `sql_text` is gone. `named` answers a column's name when the name
  is a plain identifier and neither it nor its relation is generated (a generated relation
  is dropped from the name), and nothing for every other expression; `generated` reads a
  table name. An engine rendering (`lag(…) ORDER BY […]`, `Int64(0)`, `make_array`) can
  no longer reach the text. Pinned by `the_text_names_a_plain_column_and_no_generated_relation`
  and the door pin `a_refusal_names_a_plain_column_and_otherwise_only_the_target`.
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
  Fold 2 (2026-10-11) replaced the first pin and added one:
  - `a_mark_refuses_through_every_argument_that_carries_the_value_and_no_other`: a mark
    refuses under a cast back, either result of a `CASE`, the narrowing function, a string
    and an integer round trip, with no cast on it, as a list under a list cast, and under
    arithmetic; a plain column, a `CASE` that only tests the narrowed value or compares it,
    a cast over an unmarked value, the key of a selecting call and a boolean call do not.
  - `the_narrowing_kernel_that_carries_a_mark_is_the_analyzers_cut`: the kernel aliased
    with either mark name refuses and names its argument, a written kernel above it
    forgives it, `date_trunc` forgives the zoned one only, and the `INSERT` look-through
    does not take it for the planner's cast.
  - `a_plain_column_beside_a_narrowing_has_its_own_lineage_under_every_plan_node`: under
    `ROLLUP` the plain aggregate and the grouping id have no narrowing and the narrowed
    aggregate has; in a lateral subquery an outer reference is followed to its own column
    of the plan beside it, and a column that only tests the narrowed one is not.
  - the written-call pin also holds a naive value cut beside a naive microsecond sibling
    under a written cast and a date cast (refuses), a call above a call that does not
    forgive (refuses), a call above one that does (stores), and arithmetic or a string
    between the call and the mark (refuses).
  Fold 3 (2026-10-11): three assertions read the narrowing's value as an option.
  pins: ice-tsns-narrow-refuse-1/C-007, C-008, C-019, C-020, C-021
- [lineage_tests.rs](lineage_tests.rs) — fold 3 (2026-10-11, ledger section 13): eight pins
  of the second verdict's rulings, over the helpers of `tests.rs`:
  - `a_plain_value_stores_beside_a_narrowing_it_does_not_pass_through`: a plain column read
    from a map whose key is narrowed (through `__repark_spark_nonnull__`), `map_values` of
    it, a plain column plus a difference or an interval computed from a narrowed value, and
    a call typed a timestamp that takes the narrowed value as an integer.
  - `a_narrowed_value_refuses_through_the_arguments_it_passes_through`: the map's value,
    `map_keys`, a call that takes the map whole, the narrowed side of arithmetic, and a
    value that returns through an integer or a string.
  - `a_written_call_reaches_the_mark_through_what_hands_the_value_on_unchanged`: under the
    kept cast and under the narrowing function `greatest`, `least`, `if`, `nvl2`, `nullif`,
    an array element (with and without a list cast) and a mark that cuts nothing store, and
    each refuses without the call; a `TRY_CAST` to an instant is a written call; `max`
    hands the value on and `count` does not.
  - `a_written_call_does_not_reach_the_mark_through_a_call_that_changes_the_value`: another
    call, arithmetic, a string and a `TRY_CAST` to a string between them refuse; a
    comparison carries nothing; the `INSERT` look-through takes the kept cast for the
    planner's.
  - `a_mark_over_a_value_that_holds_no_nanoseconds_cuts_nothing`: a list mark over a
    microsecond list and over a list of `TRY_CAST` results is passed through, one over a
    nanosecond list refuses; a marked column is followed to the expression that produces
    it (a `TRY_CAST` below stores, a plain column below refuses).
  - `an_update_reads_the_cast_at_the_root_of_its_scalar_subquery_as_the_planners`.
  - `every_row_of_values_is_followed` (the verifier's surviving mutant L4).
  - `the_text_names_a_plain_column_and_no_generated_relation`: the three texts, `named`
    over eight expressions, and a generated table name replaced by the empty one.
  pins: ice-tsns-narrow-refuse-1/C-042, C-043, C-044, C-045, C-047

**Addendum to fold 3 (2026-10-11, ledger section 13.2): the role table and the zone kind.**
What the stored digits pass through is data in one place, `ROLES` in
[../narrowed_store.rs](../narrowed_store.rs), one row per function name, read by `role_of`:

| Role | Rows | What is followed, and whether the written call above is remembered |
|---|---|---|
| `Arms` | `coalesce`, `greatest`, `least`, `nvl`, `ifnull`, `make_array`, `array`, `max`, `min`, `first`, `last`, `first_value`, `last_value`, `any_value` | every argument; remembered |
| `After(1)` | `if`, `nvl2` | the arguments after the predicate; remembered |
| `First` | `nullif`, `max_by`, `min_by`, `nth_value` | the first argument; remembered |
| `Lagged` | `lag`, `lead` | the value and the default, not the offset; remembered |
| `Element` | the selecting names (`array_element`, `element_at`, `get_field` and their kin) | the container, by the part selected; remembered |
| `Wrapped` | `__repark_spark_nonnull__`, `__repark_hof_array_field__` | the wrapped value; remembered |
| `Keys`, `Values`, `Entries` | `map_keys`; `map_values`; `map`, `map_from_arrays` | the keys, the values, or the part the selection above asked for |

`CASE` arms, a `UNION` branch, a scalar subquery and a lambda result are plan and
expression nodes, not names, and are followed by the walk itself. A name with no row is an
opaque call: a call typed a timestamp or a date follows its arguments of such a type
(arithmetic by position: the timestamp side, never the interval), and forgets the written
call. An interval, an integer, a predicate and a map key selected past carry nothing.
*Zone kind* (`turned`): a mark that cuts nothing but changes an instant into a naive value
or back forgets the written call above it, because the call no longer reads the kind of
value the statement wrote it over (`CAST(if(c, ns, ntz) AS TIMESTAMP)` refuses for a zoned
`ns`; the same with a `TIMESTAMP` sibling stores). Pinned by the store pins above and the
door pins of `timestamp_ns_narrow_lineage.rs`; a mutant that moves one row to another
role is in ledger section 13.6.

# map — repark-connect/tests/it

## Purpose

The crate's one integration binary. Each module pins one product module through the public API.
See [../map.md](../map.md).

## Contents

- `main.rs` — `mod postgres_types; mod settings;`.
- `settings.rs` — C-1 (2026-10-05): absent and explicit `password` (the other props carried,
  `auth_method` dropped from the map); `iam_token` and `kerberos` refuse naming their registry
  rows and fold to the Unsupported class; empty, wrong-case, hyphenated, padded and unknown
  values are invalid specifications folding to the IllegalArgument class; every spelling round
  trips; `Debug` never renders a prop value.
  pins: c-1/C-003, C-004, C-005, C-006
- `postgres_types.rs` — C-1 (2026-10-05): one round-trip pin per mapped row, named in the row
  (`bool_round_trips` … `bytea_round_trips`): Arrow array → wire values → Arrow array, equal,
  over NULLs and boundary values (`MIN` / `MAX`, `-0.0`, the infinities and NaN compared by bit
  pattern, multibyte UTF-8, invalid-UTF-8 bytes in `bytea`), plus one byte-exact wire check per
  integer width. `declared_types_refuse_naming_their_row` holds the declared list and each
  row's refusal. The error pins cover wrong wire length, invalid UTF-8 and a wrong Arrow type.
  `type_map_has_one_row_per_type_and_a_live_pin_per_row` reads this file through
  `include_str!`, so a row whose `pin` names no test here is red.
  pins: c-1/C-007, C-008

## Pointers

- Up: [../map.md](../map.md)

## Debug

First checks: `cargo test -p repark-connect --test it`. Escalate to: [../map.md#debug](../map.md).

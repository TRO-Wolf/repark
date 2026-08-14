# map — repark-common/src/surfaces

## Purpose

File-backed tests for the SQL surface registry (`../surfaces.rs`) — the dialect-neutral ID list
both SQL doors are audited against (design `docs/design/sql-doors.md` §2 Q13, graft G2).

## Contents

- `tests.rs` — `#[cfg(test)] mod tests;` in `../surfaces.rs`. 11 tests: the list invariants
  (unique, non-empty, `SCREAMING_SNAKE_CASE`, the reviewed 50-surface count, `stringify!`-derived)
  self-naming + `Display`), the four `audit` failure modes (unmapped ID, unknown ID, duplicate
  row, untraceable row) plus the complete-matrix baseline, `Row::is_tested`, and the four
  distinct `SessionProfile` values.

## I want to...

| ...do this | go to |
|---|---|
| Add a surface ID | `../surfaces.rs` (one line in `surface_ids!` — the macro derives `ALL` and the wire name), bump the count in `tests.rs`, then a row in EACH door's `matrix.rs`. A `Tested` cite must survive `make check-matrix-test-liveness` |
| Understand why a door audit is RED | the `audit` error text names the unmapped / unknown / duplicate IDs |

## Pointers

- Up: [../map.md](../map.md)
- Consumers: `crates/repark-spark/src/matrix.rs`, `crates/repark-sql/src/matrix.rs`.

## Debug

| Symptom | First check |
|---|---|
| `all_has_the_reviewed_surface_count` RED | an ID was added or removed — bump the count AND revisit `task/s2-g8-ledger.md`'s row-count table |
| `matrix-test-liveness` RED | a door's `Tested` cite is not in `cargo test -- --list` — rename the cite or flip to `DeliberatelyAbsent` |
| A door's audit RED after adding an ID | that door's `matrix.rs` needs a `Tested`/`DeliberatelyAbsent` row |

First checks: `cargo test -p repark-common surfaces::`. Escalate to: [../map.md#debug](../map.md).

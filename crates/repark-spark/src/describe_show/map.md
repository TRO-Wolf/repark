# map — repark-spark/src/describe_show

## Purpose

File-backed tests for the SHOW handlers (`../describe_show.rs`): the
`SHOW [USER] FUNCTIONS IN <catalog>.system` parser pins (USER-optional match,
bare-SHOW fallthrough, non-`system` scope and trailing-junk fall-through), the
one-column `function` batch shape, and the `<cat>.system.<fn>(` pre-parse
rewrite pins (all seven names, multi-call statements, unknown-catalog /
two-part / CALL / string-literal / quoted-identifier / missing-paren
non-claims, `system`+function case-insensitivity with exact catalog match, and
the internal-name ↔ registered-UDF identity that kills name drift).

## Contents

- `tests.rs` — `#[cfg(test)] mod tests;` in `../describe_show.rs`.

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| A `<cat>.system.<fn>` call stops resolving | The rewrite pins (`cargo test -p repark-spark describe_show::`); then whether the catalog is a live Iceberg entry |
| `SHOW USER FUNCTIONS IN <cat>.system` falls through to DataFusion | The parser pins; then the `catalogs.get` gate on the router block |

First checks: `cargo test -p repark-spark describe_show::`. Escalate to: [../map.md#debug](../map.md).

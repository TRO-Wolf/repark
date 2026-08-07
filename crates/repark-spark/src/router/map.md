# map — repark-spark/src/router

## Purpose

File-backed tests for the statement router (`../router.rs`): the v1 TRUNCATE targeted-refuse
pin, passthrough sanity, the BUG-010 ordering pin, and the P11 read-only threading pin.
PR-2-native (outside the ported census). All TEMPORARY refuse arms are restored as of PR-3b;
their refuse tests were deleted with the arms. The ported v1 lib-root battery lives in
`../tests.rs` (`crate::tests`).

## Contents

- `tests.rs` — `#[cfg(test)] mod tests;` in `../router.rs`.

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| A routing regression on an intercepted form | The lib-root battery (`cargo test -p repark-spark tests::`) pins every arm end to end |

First checks: `cargo test -p repark-spark router::`. Escalate to: [../map.md#debug](../map.md).

# map — repark-spark/src/router

## Purpose

File-backed tests for the statement router (`../router.rs`): one refuse test per PR-2 TEMPORARY
refuse arm (CTAS, column-def CREATE, DROP TABLE/NAMESPACE, CREATE NAMESPACE, ALTER, MERGE,
INSERT OVERWRITE, CALL, branch/tag ref DDL), the v1 TRUNCATE targeted-refuse pin, passthrough
sanity, the BUG-010 ordering pin, and the P11 read-only threading pin. PR-2-native tests — the
ported v1 lib-root battery rides PR-3b.

## Contents

- `tests.rs` — `#[cfg(test)] mod tests;` in `../router.rs`.

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| A refuse test fails after landing a PR-3x handler | Delete that arm's refuse test in the same change that restores the handler (ledger row closes) |

First checks: `cargo test -p repark-spark router::`. Escalate to: [../map.md#debug](../map.md).

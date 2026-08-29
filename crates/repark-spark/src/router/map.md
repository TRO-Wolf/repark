# map — repark-spark/src/router

## Purpose

File-backed tests for the statement router (`../router.rs`): the TRUNCATE targeted-refuse
pin, passthrough sanity, the BUG-010 ordering pin, and the P11 read-only threading pin.
Location translation follows byte equality across owned and borrowed buffers.
The lib-root battery lives in `../tests/`
(`crate::tests`; see [../tests/map.md](../tests/map.md)).

## Contents

- `tests.rs` — `#[cfg(test)] mod tests;` in `../router.rs`.
  **MW-6:** the CALL dispatch covers all supported procedures, including `register_table`.

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| A routing regression on an intercepted form | The lib-root battery (`cargo test -p repark-spark tests::`) pins every arm end to end |
| A `COLLATE` spelling on CREATE TABLE was a generic column-option refuse | G15: `refuse_collation_in_statement` runs on the router parse before the CREATE arm |
| `CAST(x AS STRING COLLATE name)` was a generic parse error | G15 type-position scan on the executing-parse text in `spark_ast` |
| `RESET spark.sql.collation.*` skipped the valve | G15 `DfStatement::Reset` arm in `spark_ast` |

First checks: `cargo test -p repark-spark router::`. Escalate to: [../map.md#debug](../map.md).

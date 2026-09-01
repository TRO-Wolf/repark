# map — repark-spark/src/ref_ddl

## Purpose

File-backed tests for `../ref_ddl.rs` (snapshot-ref DDL and the write-to-branch sniff).

The module path is unchanged by the move out of `../ref_ddl.rs`: these tests are still
`ref_ddl::tests::*`, so every pin keeps its name. Recognizer and sniff pins only — the
end-to-end DDL execution pins live in [../tests/map.md](../tests/map.md).

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../ref_ddl.rs`.
  The five `//` sniff comments are pre-existing (moved from `../ref_ddl.rs`).

## Pointers

- Up: [../map.md](../map.md). Sibling door: [../../../repark-sql/src/ref_ddl/map.md](../../../repark-sql/src/ref_ddl/map.md).

## Debug

| Symptom | First check |
|---|---|
| A ref-DDL shape was not recognized | `parses_*` covers the ALTER and top-level spellings; `non_ref_returns_none` lists what must NOT be claimed |
| A write-to-branch statement got through | `write_to_branch_sniff_*` holds the four-part and two-part shapes; a metadata-table suffix is deliberately not a branch |
| `../ref_ddl.rs` reads as if it has no tests | it has them — they are here, declared by `#[cfg(test)] mod tests;` at the bottom of that file |

First checks: `cargo test -p repark-spark ref_ddl::`. Escalate to: [../map.md#debug](../map.md).

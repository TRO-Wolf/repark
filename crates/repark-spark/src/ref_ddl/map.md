# map — repark-spark/src/ref_ddl

## Purpose

File-backed tests for `../ref_ddl.rs` (snapshot-ref DDL and the write-to-branch sniff).

The module path is unchanged by the move out of `../ref_ddl.rs`: these tests are still
`ref_ddl::tests::*`, so every pin keeps its name. Recognizer and sniff pins only — the
end-to-end DDL execution pins live in [../tests/map.md](../tests/map.md).

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../ref_ddl.rs`.
  The five `//` sniff comments are pre-existing (moved from `../ref_ddl.rs`).
  **IPI-42 (2026-09-20):** four recognizer pins for the `IF [NOT] EXISTS` infix —
  `parses_if_not_exists_infix_on_create` and `parses_if_exists_infix_on_drop` cover the
  `ALTER TABLE` and `… IN cat.ns.t` spellings and the unguarded control;
  `postfix_if_not_exists_still_refuses` proves the guard is parsed at Spark's token position and
  not as a trailing clause; `or_replace_with_if_not_exists_refuses_parse_class` holds the
  combination Spark's own parser rejects, class included. These four carry no doc comments: the
  owner's comment ban applies to every source file, so what a comment would have said lives in
  this row.
  pins: ipi-21-25-42-small-parser/C-001, C-003, C-004

## Pointers

- Up: [../map.md](../map.md). Sibling door: [../../../repark-sql/src/ref_ddl/map.md](../../../repark-sql/src/ref_ddl/map.md).

## Debug

| Symptom | First check |
|---|---|
| A ref-DDL shape was not recognized | `parses_*` covers the ALTER and top-level spellings; `non_ref_returns_none` lists what must NOT be claimed |
| A write-to-branch statement got through | `write_to_branch_sniff_*` holds the four-part and two-part shapes; a metadata-table suffix is deliberately not a branch |
| `../ref_ddl.rs` reads as if it has no tests | it has them — they are here, declared by `#[cfg(test)] mod tests;` at the bottom of that file |

First checks: `cargo test -p repark-spark ref_ddl::`. Escalate to: [../map.md#debug](../map.md).

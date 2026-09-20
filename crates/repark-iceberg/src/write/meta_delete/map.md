# map — repark-iceberg/src/write/meta_delete

ICE-META-DELETE-1 (2026-09-19): the pins of Spark's metadata-delete decision.
pins: ice-meta-delete-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007

## Purpose

Tests for [`meta_delete.rs`](../meta_delete.rs), which declares `#[cfg(test)] mod tests;`.
They exercise the shared decision seat both doors call, so the native door's shapes are proven
here even though its own end-to-end battery lives in
[`crates/repark-sql/tests/ansi_meta_delete.rs`](../../../../repark-sql/tests/ansi_meta_delete.rs).

## Contents

- `tests.rs` — the whole-file delete on both `write.delete.mode` values (the decision never
  reads the mode, which is the point), the empty `delete` snapshot a no-match commits, the
  partial match that declines BEFORE any commit, `DELETE` with no predicate and `WHERE true`,
  a live position-delete file that must not change the route, the case-sensitivity split
  (a case-insensitive door folds; an exact door lower-cases an UNQUOTED reference, as its
  planner does, and binds a QUOTED one verbatim), the four-part branch selector and every
  non-identity clause declining, and the translation table — what Spark can convert and the
  negations, functions, casts, subqueries and non-primitive columns that keep the row-level
  route.

The per-test `pins:` citations live in this map's header row, not in the source: the owner's
comment ban covers doc comments too.

## Pointers

- Up: [`../map.md`](../map.md)

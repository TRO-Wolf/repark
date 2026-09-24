# map — repark-iceberg/src/write/schema_evolution

## Purpose

File-backed tests for `../schema_evolution.rs` (IPI-19 + IPI-37, 2026-09-20):
the `write.spark.accept-any-schema` gate and the union-by-name schema commit
that every schema-evolving write in RePark goes through.

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in
  `../schema_evolution.rs`, over a memory catalog on a local-FS warehouse.
  The property is read trimmed and case-insensitively and only `true` opens the
  gate. The union adds a new column **last, optional, with the source's own
  type**; a narrower incoming type does **not** narrow the table (the fork's
  `union_update_existing` treats a narrowing as ignorable); a column missing
  from the source stays in the schema (union, not replace); an identical schema
  leaves the current schema id where it was, so the commit is a no-op.
  **The union adds no snapshot** — that is the fact the two-commit shape rests
  on, and it is pinned here rather than argued in prose.
  pins: ipi-19-56-37-schema-evolution-write/C-001, C-003, C-005

## U6 WRITE-REFUSALS (2026-09-24)

- `tests.rs` — `evolve_merge_schema` refuses narrowing and a non-promotable
  change with the Java text as an `IllegalArgumentMarker` and leaves the table
  unchanged; it widens a promotable column and adds the new one. The union's
  own type conflict now refuses as an `IllegalArgumentMarker` too.
  pins: u6-write-refusals/C-006, C-007, C-008

# map — repark-core/tests

## Purpose

Integration tests exercising `repark-core` through its PUBLIC API only (`ReparkSession` +
the `context()` escape hatch). Unit batteries that need crate internals live in the
in-crate `<module>/tests.rs` files (e.g. `../src/session/tests.rs`); this directory is for
behavior only reachable end-to-end.

## Contents

- `declared_sorted.rs` — SE-1 declared-sorted temp views: plan pins (window `SortExec`
  count 0 with a declaration / ≥1 without, at tp=1 and default), results-identity
  (elision changes no value), and the verification refusal battery (unsorted rows named by
  index, cross-batch boundary violations, NULLS LAST discipline, unknown key/view, empty
  keys, non-`MemTable` provider, idempotent redeclaration). **PR-D1:** tighten tags only
  flipped fields, refuses a NULL key, hint-after-tighten restores, `SELECT *` keeps the
  metadata, tighten vs hint values match, top-level schema metadata survives tighten
  and restore (SQM F2). Round-3: cache remint re-stamps provenance (R-A),
  subquery-expression sources are visible to the walk (R-B), all-nullable
  projections are allowed (R-D), export strip drops the tag and keeps
  non-nullability, lazy `into_view` hops stay visible to the CREATE walk
  (Q-001). `RepartitionExec` presence is
  deliberately NOT pinned (size/config-dependent; see the SE-1 unit ledger).

## I want to…

- Understand the trust model → `../src/sorted_view.rs` module docs.
- Add a test needing crate internals → use the module's `tests.rs`, not this directory.

## Pointers

- [../src/map.md](../src/map.md) — module inventory.
- [../../../task/se1-declared-sorted-ledger.md](../../../task/se1-declared-sorted-ledger.md)
  — the SE-1 unit ledger (probe evidence, funded ceiling).

## Debug

- Plan-pin failures: run the query with `EXPLAIN` by hand via `ReparkSession::context()`;
  small row counts change DataFusion's repartition choices — the pins use 100k rows so the
  window path is stable.

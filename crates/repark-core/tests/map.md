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
  (Q-001), remint+hint restore unflips reminted required fields, already-required
  keys still stamp schema provenance (L-004), nested required children refuse
  (L-002), a 65-wide lazy-view UNION without tighten is not refused (C1-Q-001),
  remint+hint unflips a computed column aliased onto an already-required
  name (C2-Q-001), remint+hint unflips nested reminted required children
  (C1-L-001).
  Round-4: `filtered_scan_of_a_view_source_exercises_the_get_logical_plan_recurse`
  (Y-2 — the one node that kills the delete-the-recurse mutant; no SQL-door statement reaches
  that branch on DF 54.1, measured) and
  `list_and_map_child_requiredness_is_seen_by_the_r_d_output_walk` (Y-8 / verifier P-5 — a
  required child inside List / LargeList / FixedSizeList / Map-VALUE refuses; a nullable
  element and a nullable map value stay allowed, the accepted scope). The `export_strip` node's
  claim was narrowed: it is a UNIT pin on the helper; the export boundary it guards
  (`analyzed_arrow_schema`) is pinned facade-side. `RepartitionExec` presence is
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

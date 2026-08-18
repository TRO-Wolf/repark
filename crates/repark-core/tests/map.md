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
  (`analyzed_arrow_schema`) is pinned facade-side.
  Round-5: the NATIVE-door battery — `native_door_ddl_sink_over_tightened_source_refuses`
  (Z-2: `ReparkSession::sql` on the default dialect persisted `CREATE VIEW ice.ns.v` /
  `SELECT … INTO ice.ns.t` with required columns, measured on BASE),
  `native_door_session_scoped_and_untightened_ddl_stay_allowed`,
  `native_door_default_catalog_bare_name_ddl_over_tightened_source_refuses` +
  `default_catalog_pointing_away_from_iceberg_keeps_session_ddl_allowed` (Z-1: the gate is the
  RESOLVED catalog, so `SET datafusion.catalog.default_catalog = ice` cannot launder a bare
  name), `view_visit_budget_overflow_is_a_generic_error_not_a_tighten_refusal` +
  `view_hop_chain_under_the_visit_budget_still_walks_clean` (Z-6: the walk's overflow arm,
  reachable only through retained `TableScan`s — SQL and `ctx.table` both inline views), and
  `nested_export_strip_covers_every_container_the_tagger_walks` (Z-7: FixedSizeList / List /
  LargeList / Struct / Map-value; no other container is walked by tagger, detector, or strip). `RepartitionExec` presence is
  deliberately NOT pinned (size/config-dependent; see the SE-1 unit ledger).
  Round-6: the Z-1/Z-2 refuse pins gained the UNPUBLISHED half — each refusal now also asserts
  `table_exists` FALSE for the name the statement resolves to (R6-4), and their MEASURED-on-BASE
  docstrings are true per ROW rather than in blanket (R6-3/R6-6). Round 6's own new nodes went
  into `temp_view_doors.rs`, not here — this file was at its line ceiling.
- `temp_view_doors.rs` — **SQM round 6.** `qualified_temp_view_name_refuses_and_persists_nothing`
  + `set_default_catalog_cannot_move_a_temp_view_into_a_catalog` (R6-1: the temp-view API was a
  THIRD write door into an Iceberg catalog — a qualified name, or a one-part name after
  `SET datafusion.catalog.default_catalog`, registered through the catalog provider and persisted
  a `tightenNulls` `required: true` table without any statement ever being planned, so no guard
  could see it; now one loud `Error::Analysis` and a build-time-pinned home);
  `context_sql_is_a_known_unguarded_hatch` (R6-2: the DOCUMENTED hatch — `context().sql` still
  persists what the guarded doors refuse; the pin exists so the hatch cannot change silently);
  `prepare_of_a_tightened_ddl_sink_is_inert_today` (R6-5: `PREPARE` stores an unguarded
  `CreateView`, but DF 54.1 cannot execute a prepared DDL — measured floor, pinned so it moves
  the day that changes);
  `a_catalog_over_the_build_time_default_is_not_a_temp_view_home` (R6-1 S1 second pass: pinning
  the home to the CONFIGURED default-catalog NAME pinned the leak IN — `default_catalog` is a
  BUILD-time key too, so `register_memory_catalog` took the home name over and the payload
  persisted again; the home now carries the schema PROVIDER and every entry point refuses loud
  when a catalog holds it); `a_quoted_dotted_temp_view_name_round_trips_through_table_exists`
  (the allowed `"a.b"` spelling was creatable/listable/droppable but `table_exists` re-parsed the
  stripped segment and refused it — and the fix must keep BASE's case fold);
  `set_to_a_plain_catalog_keeps_the_write_home_and_moves_only_the_read` (the disclosed CHANGE:
  the write is immune to `SET`, the read is still DataFusion's, so a bare-name round trip that
  worked on BASE now misses — measured both sides).

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

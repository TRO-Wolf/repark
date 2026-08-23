# A13 — shared CTAS fallback root — ledger

**Unit:** A13 · write path · **Date:** 2026-08-23
**Branch:** `feat/a13-shared-ctas-fallback`
**Brief:** [briefs/next-sequence.md](../briefs/next-sequence.md) A13 +
[roadmap-intake-2026-08-21.md](roadmap-intake-2026-08-21.md) A13.
**SEPMO:** `/sepmo-octo` · critic_engine `octo` · `cycles=4` · `early_stop=true` ·
`claims_critic=true` · severity floor S1.

## Chosen option

Roadmap A13 listed four options. This unit takes **the first**: default the location-less
fallback under the supplied warehouse rather than the process temp dir. It is the smallest
change that removes accidental sharing. The warehouse is the fallback *prefix*
(`{warehouse}/repark_ctas|repark_ansi_ctas/{catalog}/{ns}/{table}`), not `{warehouse}/{ns}/{table}`.

Declined here: a per-session run id in the path; a one-shot warning; leaving the addressing
and keeping only the MW-3 fence.

## PROPOSITION LEDGER — A13 — 2026-08-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|--------|--------------------------|-------------------|---------|---------------------------|
| C-001 | `register_memory_catalog(name, warehouse)` sets `LocationPolicy::TempFallbackAllowed { root }` to the warehouse path, not `std::env::temp_dir()` | Unit pin that goes red if the registration is reverted to `temp_dir()` | PROVEN | `register_memory_catalog_fallback_root_is_the_warehouse` |
| C-002 | The `spark.sql.catalog.<name>.type=memory` + `warehouse` config path uses the same root | Same assertion on a session built from config | PROVEN | `configured_memory_catalog_fallback_root_is_the_warehouse` |
| C-003 | A location-less Spark-door CTAS after `register_memory_catalog` writes under `{warehouse}/repark_ctas/{catalog}/{ns}/{table}` and not `{temp}/repark_ctas/...` | Door pin, filesystem | PROVEN | `register_memory_catalog_location_less_ctas_lands_under_warehouse`; facade `test_location_less_ctas_writes_under_the_warehouse` |
| C-004 | A location-less ANSI-door CREATE after `register_memory_catalog` writes under `{warehouse}/repark_ansi_ctas/{catalog}/{ns}/{table}` and not `{temp}/repark_ansi_ctas/...` | Door pin, filesystem | PROVEN | `register_memory_catalog_location_less_create_lands_under_warehouse` |
| C-005 | Two sessions that pass different warehouses and the same catalog/namespace/table names do not share a directory; each reads its own rows | Facade isolation pin | PROVEN | `test_two_warehouses_do_not_share_a_location_less_table` |
| C-006 | An explicit namespace `location` still wins over the fallback | Existing namespace-with-location CTAS pins stay green | PROVEN | spark `setup()` namespaces carry `location`; MW-3 owned-location pins |
| C-007 | `RequireExplicitLocation` and `ServiceManagedLocation` are unchanged | Existing loud-fail / service-managed pins stay green | PROVEN | `ctas_location_less_namespace_fails_loud_for_non_memory_catalog`; service-managed CTAS tests |
| C-008 | `remove_orphan_files` refuses a scan of `{policy.root}/repark_ctas` or `{policy.root}/repark_ansi_ctas` (table location, CALL `location`, a parent of those trees, `file://` aliases, lexical `..`) | Pins that go red if the execute `location` arm or the prefix/parent check is deleted | PROVEN | `call_orphan_shared_ctas_root_rule`; `call_remove_orphan_files_refuses_a_location_arg_under_the_fallback_root`; `test_remove_orphan_files_refuses_the_shared_ctas_fallback_root` |
| C-009 | `CatalogRegistry::from` (test helper, no warehouse) still uses `std::env::temp_dir()` | Construction stays as written; documented | PROVEN | `catalog_state.rs` `From` impl |
| C-010 | No query-time environment read: the fallback root is still resolved at registration (ADR-0004) | CTAS resolver only reads `policy.root` | PROVEN | `ctas.rs` / `create_table.rs` `TempFallbackAllowed { root }` arms |
| C-011 | A `file://` warehouse string becomes a filesystem `PathBuf` for the root (case-insensitive scheme; optional `localhost` host; `file:/path` and hostless `file://path` match Iceberg LocalFs) | Product-path pin that goes red if registration skips the helper | PROVEN | `register_memory_catalog_file_uri_fallback_root_is_the_filesystem_path`; helper twins including `file:/` and hostless `file://` |
| C-012 | Path construction keeps the `repark_ctas` / `repark_ansi_ctas` segments (smallest change; not `{warehouse}/{ns}/{table}`) | Pins assert those directory names | PROVEN | C-003 / C-004 paths |
| C-013 | `map.md` / STATUS / ledger land in the same change | `check_map_md` green | PROVEN | this tree |

VERDICT: PASS (OPEN=0, REJECTED=0). LOGIC_SCORE = 13/13.

## Enumeration (C-003 / C-004)

Entry points that reach location-less create after `register_memory_catalog`:

| Door | Spelling | Pin |
|---|---|---|
| Spark facade | `ReparkSession.sql` CTAS | `test_a13_ctas_fallback.py` |
| Spark SQL (Rust) | `SparkDialect` + `SparkExtension` | `register_memory_catalog_location_less_ctas_lands_under_warehouse` |
| ANSI SQL (Rust) | `AnsiDialect` | `register_memory_catalog_location_less_create_lands_under_warehouse` |
| Native `repark.sql()` callable | process-wide native session, no public catalog-register | N/A — no registration surface |

## KILLED_ASSUMPTIONS

- "The warehouse argument is already used for table data." REMOVED — `memory_catalog` stores
  the warehouse, but location-less CTAS ignored it and wrote under `$TMPDIR`.
- "Fixing the root lets `remove_orphan_files` sweep location-less memory tables." REMOVED —
  MW-3 refuse is keyed off `{root}/repark_ctas`; after A13 that is `{warehouse}/repark_ctas`.
  Two processes sharing a warehouse still collide. The refuse stays.

## RISK_HEATMAP

- risk: two processes pass the same warehouse path and the same names, then one runs
  `remove_orphan_files`. severity_if_realized: S0. mitigation: MW-3 refuse (C-008).
- risk: `CatalogRegistry::from` tests keep writing under `$TMPDIR/repark_ctas`.
  severity_if_realized: S3. mitigation: documented; product path does not use `From`.

## PR carving

One PR unit. Path: HIGH (data placement / persistence) → critic_engine `octo`.
LIGHT rubric fails criterion 5 (data integrity).

## Octo

`OCTO-CONVERGED` after 4 cycles (floor S1). Scratch:
`/tmp/critic-octo-repark-a13-2026-08-23/`. Cycle-1 file:// product pin and
intake/map honesty; cycle-2 CALL `location` execute pin and parent/URI/ANSI
refuse; cycle-3 FileIO `file:/` aliases; cycle-4 UTF-8 char-boundary in
`strip_ascii_prefix_ci`. next-sequence dual-home WITHDRAWN under standing rule 7
until the departure edit in this tree.

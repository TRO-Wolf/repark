# Unit ledger — ICE-CATALOG-CACHE-1 · the session's Iceberg caches reach Glue and S3 Tables

**Date:** 2026-09-19 · **Branch:** `perf/ice-catalog-cache-1` · **Base:** `ab4e57d6` (a temporary
pin to fork PR #311 head `03bfd336`, replaced by the RP-37 bump before the PR)
**Model:** claude-opus-5 (round 1) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Unit 2 of the v1.5.0 read-performance slate
([../../roadmap/mid-term/ice-read-perf-slate-2026-09-18.md](../../roadmap/mid-term/ice-read-perf-slate-2026-09-18.md)),
the RePark half. The fork half (F-CATALOG-CACHE-1, fork PR #311, ledger
`task/f-catalog-cache-1-ledger.md` in the fork) gave `GlueCatalogBuilder` and
`S3TablesCatalogBuilder` the memory catalog's two opt-in handles
(`with_table_metadata_cache`, `with_shared_object_cache_bytes`) plus
`with_cache_credential_context`; both default off. Until this unit RePark passed neither, so
every S3 Tables and Glue load re-read and re-parsed `metadata.json` and every table re-read its
manifests.

## The scope ruling (the design decision of this unit)

**Ruling.** RePark names a credential context exactly when the catalog props carry a
credential selector, and derives it with the fork's own public
`iceberg::CacheScope::credential_context_from_props` — never a RePark re-implementation. With no
selector RePark names nothing, and the fork's rule for an injected storage factory gives the
catalog a per-instance scope (`instance:<uuid>`).

**Evidence behind it.**

1. **Why the fork isolates injected I/O, and why that reason does not apply to RePark's
   factory.** The fork (`crates/catalog/glue/src/catalog.rs` `GlueCatalog::new`,
   `crates/catalog/s3tables/src/catalog.rs` `S3TablesCatalog::new`, at `03bfd336`) sets
   `injected_io = storage_factory.is_some()` (S3 Tables: `|| client.is_some()`) and then uses
   `CacheScope::isolated(..)` because an injected factory or SDK client may carry credentials
   the props do not show (fork ledger L-001). RePark always injects a factory, but it is
   `CountingStorageFactory` over `glue_default_storage_factory()` /
   `s3tables_default_storage_factory()` — the fork's own default `OpenDalStorageFactory::S3` with
   `customized_credential_load: None` (`crates/repark-iceberg/src/catalog/counting_storage.rs`).
   It carries no credentials: `FileIO` credentials come from the same props the fork would read
   without injection, and RePark never injects an SDK client. So naming the context the fork
   itself would have derived without injection restores the fork's non-injected rule exactly.
2. **The derivation is public and uses selectors only.** `CacheScope::credential_context_from_props`
   (fork `crates/iceberg/src/catalog/table_metadata_cache.rs`) reads the fixed list
   `aws_access_key_id`, `profile_name`, `s3.access-key-id`, `client.assume-role.arn`,
   `client.assume-role.external-id`, `client.assume-role.session-name`, sorted, joined — never a
   secret key or session token, and region alone is never a context. RePark calls it through a
   one-line adapter (`cache_credential_context`, which only copies the caller's hasher-generic map).
   The catalog identity half of the scope stays the fork's (`s3tables:<arn>`,
   `glue:<catalog id>:<region>:<warehouse>`), so two buckets or two Glue catalogs never meet
   either; `with_cache_credential_context` replaces only the context half.
3. **Two credential contexts never share.** Different access keys, profiles or assumed roles give
   different context strings, so different keys. Two separately built sessions each own a
   `CatalogCaches` (`catalog_state.rs` `CatalogRegistry::with_cache_settings`), so they do not
   even share a cache instance; the scope matters for the catalogs of one session (and for a
   session's clones, which share its registry).
4. **What the per-instance fallback costs.** With no selector (the default chain: environment
   variables, the shared config's default profile, an instance role — the AWS bench's shape) each
   catalog instance keeps its own scope: its loads still reuse entries (one instance serves every
   statement of a registered catalog), and only a second instance of the same catalog in the same
   session re-reads each `metadata.json` once. The bench registers each catalog once per session
   and its cold mode is a new session, so the fallback costs the bench nothing. Resolving the
   default chain's access key id to name a context was considered and rejected: it needs a
   credential resolution (a network call for IMDS or SSO) at catalog build, and an instance role
   rotates keys under one identity.
5. **Only the metadata cache is scoped.** The manifest cache (`with_shared_object_cache_bytes`) is
   one `ObjectCache` per catalog instance in the fork, so it is never shared across instances
   whatever the context. RePark names a context only when it passes the metadata handle.

## Also decided

- **The entry knob now bounds the fork cache.** `CatalogCaches::new` builds the metadata cache
  with `TableMetadataCache::with_max_entries(repark.iceberg.metadataCacheEntries)` — the fork's
  documented mapping to a byte budget (entries × 64 KiB; default 512 → 32 MiB) — instead of the
  fork default of 64 MiB. Without it the fork bound is never reached before RePark's statement-door
  `trim` clears the cache (`len() > entries`), so evictions could never be non-zero. `trim` stays;
  a trim is an explicit clear in the fork's accounting, not an eviction.
- **Evictions are advisory, as the fork states.** The fork counts evictions by delta
  (`installed − entry_count − explicit removals`) with no eviction listener; moka's entry count
  settles when its pending tasks run. The pins run `run_pending_tasks()` before reading.
- **The stats surface.** `ReparkSession::iceberg_metadata_cache_report()` returns the fork's
  `TableMetadataCacheStats` (hits, misses, body fetches, evictions; re-exported as
  `repark_iceberg::catalog::TableMetadataCacheStats`). The old
  `iceberg_metadata_cache_stats() -> Option<(u64, u64, u64)>` keeps its signature for the Python
  census binding (`crates/repark-python/src/catalog_census.rs`, not touched).
- **The unsuffixed `glue_catalog` / `s3tables_catalog`** pass `CatalogCaches::disabled()`, keeping
  their pre-unit behaviour (no handles, a fresh counter set). No production path calls them.

## PROPOSITION LEDGER — ICE-CATALOG-CACHE-1 — 2026-09-19

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `glue_catalog_counted` and `s3tables_catalog_counted` take the session's `CatalogCaches` and pass its metadata handle and manifest byte budget through the same `wire_caches` the memory catalog uses; disabled caches pass nothing; each switch works alone. | `catalog/tests/cache_wiring.rs`: `the_session_handles_reach_the_builder`, `disabled_caches_wire_no_handle_and_no_context`, `each_switch_is_honoured_alone`, `glue_and_s3tables_catalogs_hold_the_session_metadata_cache`, `disabled_glue_and_s3tables_catalogs_build_without_a_handle`. | **PROVEN** | Red with `wire_caches` stubbed to pass nothing (7 of 11 red, see "Red runs"); green at this head. The real Glue and S3 Tables builds run offline (region and static keys in props, no service call at build) and the session handle's `Arc::strong_count` rises by one per catalog. |
| C-002 | Every production path that builds a Glue or S3 Tables catalog passes the session caches: `register_catalog_spec`, which `register_configured_catalogs`, `register_late_configured_catalogs`, the Python late-registration path (`repark-python/src/session.rs`) and the bench's `register_remote_catalog` all reach. | `session/tests/metadata_cache_report.rs::configured_aws_catalogs_are_built_with_the_session_caches`; the bench pin `aws_catalogs_are_never_registered_as_bare_handles` (unchanged). | **PROVEN** | The source pin reads both builder calls in `register_catalog_spec` and requires `&iceberg_caches::caches_of(&self.catalogs)`; the bench pin keeps the bench off the bare builders. |
| C-003 | The scope ruling: the credential context is the fork's `CacheScope::credential_context_from_props` over the catalog props, named only with the metadata handle; with no selector nothing is named and the instance keeps its own scope. Two contexts never share an entry; one context shares across a session's catalog instances. | `cache_wiring.rs`: `two_credential_contexts_never_share_an_entry`, `without_a_credential_selector_each_instance_is_its_own_scope`, `only_credential_selectors_name_a_context_and_never_a_secret`. | **PROVEN** | Memory-catalog stand-in behind the same `wire_caches`: `AKIAONE` and `AKIATWO` instances adopting one metadata location hold two entries and two distinct `Arc`s; a second `AKIAONE` instance shares the first one's entry (`Arc::ptr_eq`). Mutation: a constant context `"shared"` reds `two_credential_contexts_never_share_an_entry` and `the_session_handles_reach_the_builder`. The memory builder would derive the same context from its own props; the recorder pin proves RePark names it for every builder. |
| C-004 | Two sessions never share a metadata cache. | `metadata_cache_report.rs::two_sessions_never_share_a_metadata_cache`. | **PROVEN** | Distinct `Arc<TableMetadataCache>` per built session. |
| C-005 | A commit by a second handle is visible on the first handle's next load through RePark's wiring, and a same-scope sibling instance never reads another pointer's metadata. | `cache_wiring.rs::a_second_handles_commit_is_seen_and_never_leaks_into_a_sibling_pointer`. | **PROVEN** | Re-proved on the memory catalog: the AWS builders cannot be driven offline from RePark — the fork's pointer seam (`with_pointer_source`) is `#[cfg(test)]` inside the fork crates — and two memory catalog instances each own their pointer state, so the second handle is a second `Arc` of one catalog, and the sibling is a second instance in the same scope adopting the old location. The fork pins P-1 / P-2 on the Glue and S3 Tables catalogs themselves. Green before and after the wiring (a correctness re-proof, not a red-first pin). |
| C-006 | `repark.iceberg.metadataCacheEntries` bounds the fork cache (`with_max_entries`), evictions under pressure reach `CatalogCaches::metadata_stats` and the session report, and an evicted table is re-read, never served a sibling's metadata. | `cache_wiring.rs::evictions_reach_the_stats_and_never_serve_a_sibling`; `metadata_cache_report.rs::the_report_carries_evictions_beside_the_legacy_triple`. | **PROVEN** | Three 40 KiB-property tables under a one-entry (64 KiB) budget: `evictions > 0`, `weighted_size() <= 64 KiB`, each load's property is its own table's. Mutation: `TableMetadataCache::new()` in place of `with_max_entries` reds the eviction pin. |
| C-007 | `iceberg_metadata_cache_report()` exposes hits, misses, body fetches and evictions; the legacy triple keeps its signature and agrees with it; a disabled cache reports `None` on both. | `metadata_cache_report.rs`: `the_report_carries_evictions_beside_the_legacy_triple`, `a_disabled_cache_reports_nothing_on_either_surface`. | **PROVEN** | The Python census binding and its tests are untouched and still read the triple. |
| C-008 | The counting wrapper still counts every storage request with the caches on: a cold scan counts the same manifest-list, manifest and data-file requests with the caches on as off, and a warm scan counts fewer manifest requests only because the cache served them. | `cache_wiring.rs::the_counter_sees_every_request_with_the_caches_on`. | **PROVEN** | Requests compared, not bytes: the two beds' temporary paths differ in length and the manifest list embeds them (a byte comparison flaked by one byte). |
| C-009 | The bench exposes evictions: the per-query JSON's `metadata_cache` object carries `evictions` and the markdown table's cache cell reads `hits/misses/evictions`. | `benches/ice_read_perf/pins_report.rs`: `the_report_carries_metadata_cache_evictions`, `every_measured_sample_reports_its_evictions`. | **PROVEN** | `report.rs` reads `iceberg_metadata_cache_report()` before and after each query and diffs all four counters. Dropping the JSON field reds both pins. The committed baseline document keeps its old `cache hit/miss` header; it is a record of that run. |
| C-010 | No secret enters a scope string, a span or a `Debug` output. | `only_credential_selectors_name_a_context_and_never_a_secret`; `glue_and_s3tables_catalogs_hold_the_session_metadata_cache` (the catalogs' `Debug`). | **PROVEN** | The context is built from selectors only; the builder spans still record prop key names only; the fork redacts secret props in its catalog `Debug`. |
| C-012 | Retained-entry counts are settled before they are read: the statement door's `trim`, the Python census and the bench probes run the moka cache's pending tasks first, so the retained-entry bound and the census hold under fork PR #311's cache. | `repark-spark/src/tests/catalog_cache_staleness.rs` (16 pins); `python/repark/tests/test_perf_ice_catalog_io_1.py`. | **PROVEN** | At the pin commit `ab4e57d6` (before this unit) three staleness pins failed (`a_table_is_never_served_a_sibling_tables_cached_metadata`, `one_statement_over_many_tables_retains_one_entry_each_until_the_next_door`, `the_retained_location_bound_holds_across_many_commits`) and the Python twin `test_the_metadata_cache_bound_is_a_statement_door_clear_not_a_per_load_bound` read `entries == 1` for 8: moka's `entry_count` lags until its pending tasks run. With `settle` all 16 staleness pins pass and the Python file passes (see the hand-back gate lines). |
| C-011 | The before/after pair on the AWS bed and the local bed. | unit 0's bench at this head against its base. | **OPEN** | `TBD-orchestrator` — AWS bed (S3 Tables, Glue) and local bed, cold / warm / concurrent. |

## Red runs

- `wire_caches` stubbed to return the builder untouched (the pre-unit Glue / S3 Tables
  behaviour), `cargo test -p repark-iceberg --lib catalog::tests::cache_wiring`: 4 passed, 7
  failed — `each_switch_is_honoured_alone`, `the_session_handles_reach_the_builder`,
  `two_credential_contexts_never_share_an_entry`,
  `without_a_credential_selector_each_instance_is_its_own_scope`,
  `evictions_reach_the_stats_and_never_serve_a_sibling`,
  `glue_and_s3tables_catalogs_hold_the_session_metadata_cache`,
  `the_counter_sees_every_request_with_the_caches_on`.
- Constant context `"shared"`: 2 failed (C-003).
- `TableMetadataCache::new()` for the entry-bounded cache: the eviction pin failed (C-006).
- The session-level report pins were red by compilation (the method did not exist).
- The bench's `"evictions"` JSON field removed: `the_report_carries_metadata_cache_evictions` and
  `every_measured_sample_reports_its_evictions` failed (C-009).

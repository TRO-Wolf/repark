# Unit ledger — ICE-FOOTER-CACHE-1 · the session's Parquet footer cache

**Date:** 2026-09-19 · **Branch:** `perf/ice-footer-cache-1` · **Base:** `2ae5c131` (a temporary
pin to fork PR #316 head `885c27dc`, replaced by the RP-38 bump before the PR)
**Model:** claude-opus-5 (round 1) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Unit 3 of the v1.5.0 read-performance slate
([../../roadmap/mid-term/ice-read-perf-slate-2026-09-18.md](../../roadmap/mid-term/ice-read-perf-slate-2026-09-18.md)),
the RePark half. The fork half (F-FOOTER-CACHE-1, fork PR #316) added
`iceberg::arrow::ParquetFooterCache` — a bounded moka cache of parsed `ParquetMetaData` keyed by
the catalog's `CacheScope`, the data-file path and the manifest's `file_size_in_bytes`, feeding
the reader's prefetched-metadata hook, upgrading an entry in place when a filtered scan needs
the page index — and `with_shared_footer_cache(Arc<ParquetFooterCache>)` on the memory, Glue and
S3 Tables catalog builders, default off. Until this unit RePark passed nothing, so every scan
read one footer per file per partition reader (a 512 KiB prefetch each).

## Decisions

- **The default is 64 MiB, on** — the orchestrator's ruling Q-24a-5, not the fork's 256 MiB.
  The budget is per session and is moka weight over each footer's
  `ParquetMetaData::memory_size()`, not a resident-bytes ceiling. `0` disables and builds no
  cache at all (`CatalogCaches::footer_cache()` is `None`), so a disabled session is
  byte-for-byte the pre-unit read path.
- **The key spellings.** `repark.iceberg.footerCacheBytes` and the underscore alias
  `repark.iceberg.footer_cache_bytes`, parsed by the same `parse_bytes` as `manifestCacheBytes`
  (an integer in `[0, 2^64)`, a bad value fails `build()` naming both the key set and the
  canonical spelling). The brief also named "the same `spark.`-prefixed spelling the other two
  knobs accept": at this base neither `metadataCache*` nor `manifestCacheBytes` accepts a
  `spark.`-prefixed key (`IcebergCacheSettings::from_config_map` reads exactly the camel and the
  underscore spelling; no `strip_prefix("spark.")` exists on the session config path), so the
  footer knob accepts exactly what they accept. Reported in the hand-back.
- **The credential context is named when either scoped cache is on.** The footer key carries the
  same `CacheScope` the metadata cache uses, so a session with the metadata cache off and the
  footer cache on must still separate credential contexts. `wire_caches` names the context
  (the fork's `CacheScope::credential_context_from_props`, selectors only) when the metadata OR
  the footer handle is passed; with neither, nothing is named, as before. With no selector the
  fork gives the instance its own scope, so a memory catalog built without selector props
  (every `register_memory_catalog`) shares footers only within that catalog instance — which is
  the instance every statement of a registered catalog uses.
- **One cache per session.** `CatalogCaches` (one per `CatalogRegistry`, one registry per built
  session, shared by the session's clones) owns the `Arc`; two built sessions never share it.
- **No settle step.** The fork's footer counters are plain atomics and its evictions come from a
  moka eviction listener, so `iceberg_footer_cache_stats()` reads them directly; the metadata
  cache's `settle` is not needed here.

## PROPOSITION LEDGER — ICE-FOOTER-CACHE-1 — 2026-09-19

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `IcebergCacheSettings::footer_cache_bytes` reads `repark.iceberg.footerCacheBytes` / `repark.iceberg.footer_cache_bytes`, default `67108864`; `0` builds no cache; a bad value fails loud naming the key set and the canonical key, at `from_config_map` and at `ReparkSession::build()`. | `catalog/caches.rs` tests: `the_footer_cache_defaults_to_64_mib_and_is_on`, `both_footer_spellings_size_the_cache`, `zero_disables_the_footer_cache`, `a_bad_footer_value_fails_loud_naming_the_key`; `session/tests/footer_cache_report.rs::a_bad_footer_budget_fails_the_build_naming_the_key`. | **PROVEN** | The parse ignored (`let _ = (raw, key);`) reds three of them (see "Red runs"). Garbage covered: `many`, `-1`, empty, `64MiB`, `1.5`. |
| C-002 | `wire_caches` hands the session's one `Arc<ParquetFooterCache>` to the memory, Glue and S3 Tables builders through `with_shared_footer_cache` on every path the metadata cache reaches (the same call); nothing when disabled or at `0`; alone it still names the credential context. | `catalog/tests/cache_wiring.rs`: `the_session_handles_reach_the_builder`, `disabled_caches_wire_no_handle_and_no_context`, `each_switch_is_honoured_alone`, `every_builder_holds_the_session_footer_cache`. | **PROVEN** | Recorder builder: `Arc::ptr_eq` with `caches.footer_cache()`. Real builds (offline, static keys and region in props): the footer handle's `strong_count` rises by one per memory, Glue and S3 Tables catalog and falls back on drop. The footer arm stubbed out reds 6 pins; the context gated on the metadata handle alone reds `each_switch_is_honoured_alone`. The production paths reaching `wire_caches` are unchanged from ICE-CATALOG-CACHE-1 (its C-002 source pin still holds). |
| C-003 | On a local memory-catalog table, a warm re-scan reads zero data-file footers with the cache on (`fetches` equal the cold scan's footer reads); with `footerCacheBytes = 0` the warm scan reads as many footers as the cold one; the cold scan never reads more footers with the cache on than off. | `catalog/tests/footer_cache.rs::a_warm_rescan_reads_no_data_file_footer_with_the_cache_on`; `session/tests/footer_cache_report.rs`: `the_session_reports_its_footer_cache_and_a_warm_scan_reads_no_footer`, `a_zero_budget_session_reports_nothing_and_rereads_every_footer`; bench `pins.rs::setup_writes_exactly_n_files_and_every_mode_runs_on_them` (warm Q1 / Q2 footers 0, cold 3). | **PROVEN** | The footer bucket is ICE-READ-PERF-0's `FooterRead × DataFile`. Three 60,000-row files: cold 3 footer reads with the cache, 4 without (the cache also collapses a split file's second footer read — a gain, so the pin is `≤`, not `=`). Red with the footer arm stubbed out. |
| C-004 | A filtered scan that needs the page index, run after an unfiltered scan cached the footer, answers correctly, upgrades the entry (`upgrades` rises), fetches no new footer and reads no footer; a repeat does not upgrade again. | `catalog/tests/footer_cache.rs::a_filtered_scan_after_an_unfiltered_one_upgrades_the_footer_and_answers_right`. | **PROVEN** | `id >= 100500 AND id < 100600` over 180,000 rows: 100 rows and their sum, equal to the same query on a footer-off catalog. Red with the footer arm stubbed out (`upgrades` stays 0). |
| C-005 | The counting layer still counts every page read with the footer cache on: warm data-file page reads (requests and bytes) are identical on and off; the footer bucket counts footers when the cache is off. | `footer_cache.rs::the_counter_still_counts_every_page_read_with_the_footer_cache_on`; `cache_wiring.rs::the_counter_sees_every_request_with_the_caches_on` (amended). | **PROVEN** | The amended pin compares `RangedRead × DataFile` counts and requires warm footers 0 on / > 0 off; before the unit it compared all data-file requests, which the cache now legitimately lowers. The page-read pin is a no-regression invariant (green with the footer arm stubbed out too); the amended pin's footer half reds under that stub. |
| C-006 | Two sessions never share a footer cache. | `caches.rs::two_catalog_caches_never_share_a_footer_cache`; `footer_cache_report.rs::two_sessions_never_share_a_footer_cache`. | **PROVEN** | Distinct `Arc`s per `CatalogCaches` and per built session; behaviourally, a second session adopting the first one's warm table reads footers with zero hits. |
| C-007 | `ReparkSession::iceberg_footer_cache_stats()` returns hits, misses, fetches, upgrades and evictions, `None` when the budget is `0`. | `footer_cache_report.rs`: `the_session_reports_its_footer_cache_and_a_warm_scan_reads_no_footer`, `a_zero_budget_session_reports_nothing_and_rereads_every_footer`. | **PROVEN** | Red by compilation before the method existed. |
| C-008 | The bench's per-sample JSON carries `footer_cache` {hits, misses, fetches, upgrades, evictions} (`null` when off) beside `metadata_cache`, and the markdown table a `footer cache hit/miss` column (`off` when disabled). | `benches/ice_read_perf/pins_report.rs`: `the_report_carries_footer_cache_hits_and_misses_beside_the_metadata_cache`, `a_warm_sample_hits_the_footer_cache_and_a_cold_one_misses_it`. | **PROVEN** | The after-probe reading no stats reds the bed pin; the record pin was red by compilation. |
| C-009 | The knob is documented where `manifestCacheBytes` is: the guide's Iceberg caches table and paragraph, the guide map, the catalog map's task table. | `make check-docs-links check-map-sync`. | **PROVEN** | `docs/guide/session-and-conf.md`, `docs/guide/map.md`, `crates/repark-iceberg/src/catalog/map.md`. No Python surface names the key (`python/repark` mentions `manifestCacheBytes` only in one test). |
| C-010 | The before/after pair on unit 0's local beds (the AWS bed is blocked on the owner's IAM grant). | main `8c1ae093` (RP-37) against this PR's head (RP-38 + the unit), built and run back to back, `--repeat 5`, default release profile, both beds; table in `docs/perf/ice-read-perf-baseline-2026-09-19.md` §"RP-38 + ICE-FOOTER-CACHE-1 pair". | **PROVEN** (local) | Warm scans read zero data-file footers on every query of both beds (200-file bed: 200 → 0 on Q1/Q2/Q5/Q6, 27/26 → 0 on Q3/Q4/Q7; warm Q1 104,857,600 → 0 bytes, 11.0 → 7.7 ms). Cold on the large bed the split file's second footer read is gone (Q2/Q5/Q6 40 → 20) and the pruned queries read one footer (Q3/Q4/Q7 26 → 1). Page bytes are unchanged everywhere. The AWS pair is owed with the IAM grant. |

## Red runs

- `wire_caches` with the footer arm replaced by `Some(_) => builder`,
  `cargo test -p repark-iceberg --lib catalog`: 96 passed, 6 failed —
  `each_switch_is_honoured_alone`, `every_builder_holds_the_session_footer_cache`,
  `the_session_handles_reach_the_builder`, `the_counter_sees_every_request_with_the_caches_on`,
  `a_filtered_scan_after_an_unfiltered_one_upgrades_the_footer_and_answers_right`,
  `a_warm_rescan_reads_no_data_file_footer_with_the_cache_on` (C-002, C-003, C-004).
- The footer key's parse replaced by `let _ = (raw, key);`: 99 passed, 3 failed —
  `a_bad_footer_value_fails_loud_naming_the_key`, `both_footer_spellings_size_the_cache`,
  `zero_disables_the_footer_cache` (C-001).
- `scoped = metadata.is_some()` (the context named only with the metadata handle): 1 failed —
  `each_switch_is_honoured_alone` (C-002).
- The bench's after-probe given `None` footer stats: 20 passed, 1 failed —
  `a_warm_sample_hits_the_footer_cache_and_a_cold_one_misses_it` (C-008).
- The session and bench record pins were red by compilation (the method, the `IoDelta::footer`
  field and the JSON key did not exist).

## Critic residue (Grok 4.6, 2026-09-19: verification PASS, Rust perf LOOKS-GOOD)

- A-1 (by design, fork contract): the key is storage context + path + `file_size_in_bytes`, so a
  data file rewritten in place at the same path and the same size (a Hadoop-layout overwrite)
  would be served its old footer. RePark's writers use a UUID prefix per writer, `rewrite_table_path`
  changes the path, and fork #310's stale-size retry keys the successful open by the actual size.
- A-5: the 64 MiB bound is `ParquetMetaData::memory_size()` (decoded metadata), not a process RSS
  ceiling; the 500-table RSS pin toggles only `manifestCacheBytes`, so it does not bound the footer
  cache.
- FC-SIZE-10K: 64 MiB holds the 200-file bed; a 10,000-file table of the same shape needs a higher
  `repark.iceberg.footerCacheBytes` (the fork's 256 MiB default holds about 10,000 such footers).
- FC-UPGRADE-FETCH: a filtered scan after an unfiltered one pays a `load_page_index` ranged read
  but no second footer GET; the fork does not count it in `fetches` (fork follow-up: pass the
  prefetched tail to `load_page_index`).
- FC-STATS-SINGLEFLIGHT: the fork counts concurrent first-access waiters as misses, so the
  split-file win shows in the bench's footer I/O cells rather than as hits.


## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-footer-cache-1
  categories:
    - id: AT-1
      status: N/A
      justification: No Spark-visible answer changes; the cache serves footers of immutable, uniquely named data files.
    - id: AT-2
      status: ATTACKED
      evidence: The wiring, stats and bench pins were red before they existed or with their arm
        stubbed; the Grok verifier turned the S3-Tables-skip, shared-process-cache, zero-builds-a-cache
        and footer-only-context mutations red.
      artifacts: [crates/repark-iceberg/src/catalog/tests/footer_cache.rs, crates/repark-iceberg/src/catalog/tests/cache_wiring.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Memory, Glue and S3 Tables builders; disabled; two sessions; cold, warm and a
        filtered scan after an unfiltered one (the page-index upgrade).
      artifacts: [crates/repark-iceberg/src/catalog/tests/footer_cache.rs]
    - id: AT-4
      status: ATTACKED
      evidence: One cache per session behind an Arc; the fork's moka cache handles concurrent
        first access (single-flight); the staleness suite passes with it on.
      artifacts: [crates/repark-spark/src/tests/catalog_cache_staleness.rs]
    - id: AT-5
      status: ATTACKED
      evidence: The footer key carries the same credential scope as the metadata cache; two
        contexts never share an entry.
      artifacts: [crates/repark-iceberg/src/catalog/cache_wiring.rs]
    - id: AT-6
      status: ATTACKED
      evidence: The residue records the one by-design stale case (an in-place overwrite at the same
        path and size) and why RePark's writers never produce it.
      artifacts: [task/ledgers/staging/ice-footer-cache-1-ledger.md]
    - id: AT-7
      status: ATTACKED
      evidence: A back-to-back before/after pair on both local beds, default release profile.
      artifacts: [docs/perf/ice-read-perf-baseline-2026-09-19.md]
    - id: AT-8
      status: ATTACKED
      evidence: No dependency or feature; the knob is documented in the guide with its default and the
        10,000-file sizing note.
      artifacts: [docs/guide/session-and-conf.md]
    - id: AT-9
      status: ATTACKED
      evidence: Every touched directory's map carries the change with pins citations.
      artifacts: [crates/repark-iceberg/src/catalog/map.md]
    - id: AT-10
      status: ATTACKED
      evidence: The catalog, session, staleness and bench suites pass at RP-38; CI runs the rest.
      artifacts: [crates/repark-iceberg/src/catalog/tests/cache_wiring.rs]
  complete: true
```

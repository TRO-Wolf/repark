# Charter ledger — PERF-ICE-CATALOG-IO-1 · one table load per planning round, a catalog metadata cache, a shared manifest cache

**Date:** 2026-09-05 · **Branch:** `perf/ice-catalog-io-1` · **Base:** `origin/main` `6eaccd5e`
· **Model:** opus-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: elevated** (catalog correctness: a cache in a `load_table` path
is where staleness dies quietly). **Registry:** `PERF-ICE-MANIFEST-1`, `PERF-CATALOG-CALLS-1`.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The analysis
([docs/perf/engine-iceberg-analysis-2026-09-04.md](../../../docs/perf/engine-iceberg-analysis-2026-09-04.md))
ranked two Iceberg-integration defects it could isolate: every statement re-resolves the table
(§2 row 11 — 2 `metadata.json` opens per SELECT, 3–6 per DML, each a Glue `GetTable` plus an S3
GET on a real catalog), and every `Table` gets a fresh `ObjectCache` so `plan_files` re-reads
every manifest (§2 row 6 — 192 manifests at ~0.45 ms each, 85 ms per statement on local FS).

**Not in this unit:** the fork pin bump (`git diff origin/main -- Cargo.toml Cargo.lock` is
empty); any AWS measurement; `STATUS.md` and `briefs/next-sequence.md`.

## PROPOSITION LEDGER — PERF-ICE-CATALOG-IO-1 — 2026-09-05

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The per-statement `metadata.json` census is measured before and after on a release module, and the shipped half meets the brief's targets (SELECT ≤ 1, DML ≤ 2). | The `strace -f -e trace=openat` census with marker files; the in-process counter the Python pins read. | **PROVEN** | Analysis §7.6 reproduced exactly as the before column (SELECT 2, INSERT 5, DELETE 6, UPDATE 7, MERGE 4). After: `metadata.json` **reads 0 on every statement**; the one remaining open per DML is `O_WRONLY` — the commit writing its own new pointer. Split read from write in `count_calls.py` because the raw open count hides that. Tables in [docs/perf/iceberg-catalog-io-baseline.md](../../../docs/perf/iceberg-catalog-io-baseline.md) §1. |
| C-002 | A session-scoped cache keyed by **metadata-file location** is built once per session and handed to every catalog it builds; a moved pointer is never served from an old key; the catalog pointer, not the cache, stays authoritative. | Rust pins on two doors over one catalog; the fork's `TableMetadataCache` contract. | **PROVEN** | `crates/repark-iceberg/src/catalog/caches.rs`; four mechanics pins in `crates/repark-spark/src/tests/catalog_cache_staleness.rs`. Measured and recorded: the memory catalog **evicts the pointer it replaced and seeds the new one** on commit, so retention is FLAT across DML and the reader after a commit pays no GET — the pin asserts that, after a first draft asserted the opposite and was wrong. |
| C-003 | The staleness contract holds with the cache in place: a commit, a schema change, a MERGE after another door's commit, `rewrite_manifests` + `expire_snapshots`, and DROP + re-CREATE are all correct across two doors on one catalog. | Five Rust pins; the whole Iceberg + Spark surface; the facade suite. | **PROVEN** | Five pins in `catalog_cache_staleness.rs`, green before and after. DROP + re-CREATE cannot ABA: the memory catalog writes Hive/REST `<version>-<uuid>.metadata.json`, `with_next_version` draws a fresh uuid, and `drop_table` evicts. `cargo test -p repark-iceberg -p repark-spark` and the facade suite green. |
| C-004 | Two conf knobs with underscore aliases shape the cache and fail loud naming the key; the retained-location bound is trimmed at the statement door; `session.rs` pays for the wiring by splitting, and the CAP-1 mirror moves in the same commit. | Knob pins in Rust and Python; the size gates. | **PROVEN** | `repark.iceberg.metadataCache` (default true) and `repark.iceberg.metadataCacheEntries` (default 512). `register_late_configured_catalogs` moved to `session/late_catalogs.rs`; `check_rust_file_size.py` and `test_cap_1_source_file_line_cap.py` both ratchet `session.rs` 1039 → 1002 in one commit. The bound is a **high-water clear, not per-entry LRU** — the fork's cache is a `HashMap`; `F-CATIO-BOUND` carries the LRU. |
| C-005 | Parts 1 and 3 need the fork; they are implemented and test-green there, measured through a temporary never-committed path override, and every pin that needs them SKIPs naming the ask. | The fork lane; the override; the skipped legs; the empty manifest diff. | **PROVEN** | `$HOME/repark-lanes/lanes/catio-fork` `fork/catio-io` on `189a73ed`: `F-CATIO-A` (one load per planning round) and `F-CATIO-B` (`TableBuilder::object_cache` + `MemoryCatalogBuilder::with_shared_object_cache_bytes`). Fork tests 3,612 passed / 0 failed plus the whole `iceberg-datafusion` suite; the RePark facade suite runs against the override. `t_many/count_id/stmt2` **120.01 → 11.33 ms** (target ≤ 20) and a repeated read opens **no** manifest-list and **no** manifest. `F-CATIO-AWS` (Glue / S3 Tables `with_table_metadata_cache`) is filed, not implemented. `git diff origin/main -- Cargo.toml Cargo.lock` empty. |
| C-006 | The baseline note carries the census tables, the timing cells, the machine, the recorded load, the re-measured floor and the commands; the two registry rows carry the before/after. | The note; the registry. | **PROVEN** | [docs/perf/iceberg-catalog-io-baseline.md](../../../docs/perf/iceberg-catalog-io-baseline.md). Both timing columns are the same release module in back-to-back runs, each with its own floor and load — the box was NOT quiet (load 7–12, a sibling lane's `cargo` live) and the note says so. `PERF-CATALOG-CALLS-1` FIXED, `PERF-ICE-MANIFEST-1` BACKLOG behind the pin bump. |
| C-007 | Every touched `map.md` moves in lockstep and the design reasons live there, not in code; no code comment is added. | `make check-map-sync`; the staged-diff comment self-check. | **PROVEN** | Eight maps. The staged-diff check prints only the forced `/// # Errors` on two `pub fn`s returning `Result`, the `pins:` citation lines rule B requires, and the comments that MOVED with `register_late_configured_catalogs`. |

VERDICT: 7 clauses, 7 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: perf-ice-catalog-io-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Nine Rust pins on two doors over one catalog plus nine always-run Python legs cover the cache mechanics, both knobs, the bound and the staleness contract; three legs skip naming their fork ask.
      artifacts: [crates/repark-spark/src/tests/catalog_cache_staleness.rs, python/repark/tests/test_perf_ice_catalog_io_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: The ABA hazard was chased to the naming scheme - a dropped and re-created table cannot reuse a metadata location because the memory catalog draws a fresh uuid per version and drop_table evicts; pinned, not assumed.
      artifacts: [crates/repark-spark/src/tests/catalog_cache_staleness.rs]
    - id: AT-3
      status: ATTACKED
      evidence: No AWS call, no credential, no IAM surface, no .github change; the two AWS legs are written and skipped behind the acceptance env gate.
      artifacts: [python/repark/tests/test_perf_ice_catalog_io_1.py]
    - id: AT-4
      status: ATTACKED
      evidence: Every number is a release module refusing debug assertions, five iterations after a warm-up, with the 1-minute load at start and end and a floor re-measured in the same run; the box was not quiet and the note says so.
      artifacts: [docs/perf/iceberg-catalog-io-baseline.md]
    - id: AT-5
      status: ATTACKED
      evidence: No dependency change. The fork work is consumed only through a temporary path override that was reverted; git diff origin/main -- Cargo.toml Cargo.lock is empty at hand-back.
      artifacts: [Cargo.toml, Cargo.lock]
    - id: AT-6
      status: ATTACKED
      evidence: The census instrument is two relaxed atomic loads read only when asked, and the disabled knob reconstructs the pre-unit load path in the same process, so the product path pays nothing for the measurement.
      artifacts: [crates/repark-python/src/catalog_census.rs, crates/repark-iceberg/src/catalog/caches.rs]
    - id: AT-7
      status: ATTACKED
      evidence: Mutation score measured, four mutations. Trim made a no-op reds the bound pin; the knob-off branch dropped reds the disabled pin; the cache never built reds the unmoved-pointer and commit-seed pins; trim made a per-statement flush reds the unmoved-pointer pin - that fourth escaped the first draft and the pin was strengthened until it did not.
      artifacts: [crates/repark-spark/src/tests/catalog_cache_staleness.rs]
    - id: AT-8
      status: N/A
      justification: No dependency, lockfile or workspace-manifest change.
    - id: AT-9
      status: ATTACKED
      evidence: Two registry rows filed with measured before/after; three fork asks named (F-CATIO-A, F-CATIO-B, F-CATIO-AWS) with what each measures and the pins that un-skip at the bump.
      artifacts: [docs/spark-sql-iceberg-parity.md, docs/perf/iceberg-catalog-io-baseline.md]
    - id: AT-10
      status: ATTACKED
      evidence: Seven clauses, each cited by a pin; the fork-gated half is stated as fork-gated rather than claimed as shipped.
      artifacts: [task/ledgers/staging/perf-ice-catalog-io-1-ledger.md]
  complete: true
```

## What changed

| Site | Change |
|---|---|
| `repark-iceberg/src/catalog/caches.rs` | New: `IcebergCacheSettings`, `CatalogCaches`, the two knobs |
| `repark-iceberg/src/catalog/builders.rs` | `memory_catalog_cached(warehouse, caches)`; `memory_catalog` keeps its signature |
| `repark-core/src/catalog_state.rs` | `CatalogRegistry` carries the session's cache handles |
| `repark-core/src/session/iceberg_caches.rs` | New: the memory-catalog build, the statement-door trim, the census accessors |
| `repark-core/src/session/late_catalogs.rs` | Move-only, to pay for the wiring under CAP-1 |
| `repark-python/src/catalog_census.rs` | New: the `iceberg_metadata_cache_census` pyfunction |
| `repark-spark/src/tests/catalog_cache_staleness.rs` | New: nine pins on two doors over one catalog |
| `python/repark/tests/test_perf_ice_catalog_io_1.py` | New: nine always-run legs, three skipped fork-gated legs |
| `docs/perf/iceberg-catalog-io-baseline.md` | New: the census and timing tables, the three fork asks |

Public API breaks: **zero**. New public names only. No dependency change. No Spark-answer change.

## The number

| cell | before | after (shipped) | after (with the fork asks) | target |
|---|---:|---:|---:|---|
| `metadata.json` reads per SELECT | 2 | **0** | 0 | ≤ 1 |
| `metadata.json` reads per INSERT / DELETE / UPDATE / MERGE | 4 / 5 / 6 / 3 | **0** | 0 | ≤ 2 |
| manifest-list + manifest opens, repeated SELECT | 1 + 1 | 1 + 1 | **0 + 0** | — |
| `t_many/count_id` second statement (193 manifests) | 120.35 ms | 120.01 ms | **11.33 ms** | ≤ 20 ms |
| `t_many_merged/count_id` second statement (1 manifest) | 17.63 ms | 14.91 ms | **10.56 ms** | — |

Floors 1.10 / 0.30 / 0.87 ms in their own runs; load 7–12 throughout.

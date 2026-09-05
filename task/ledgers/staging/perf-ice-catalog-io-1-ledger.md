# Charter ledger — PERF-ICE-CATALOG-IO-1 · one table load per planning round, a catalog metadata cache, a shared manifest cache

**Date:** 2026-09-05 · **Branch:** `perf/ice-catalog-io-1` · **Base:** `origin/main` `6eaccd5e`
· **Model:** opus-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: elevated** (catalog correctness: a cache in a `load_table` path
is where staleness dies quietly). **Registry:** `PERF-CATALOG-CALLS-1` (FIXED, narrowly),
`PERF-ICE-MANIFEST-1`, `PERF-CATALOG-LOADS-1`, `PERF-CATALOG-AWS-CACHE-1`,
`PERF-CATALOG-CACHE-BOUND-1` (all BACKLOG behind the fork pin bump).

*Model note:* the dispatching brief named `fable-5.1`; the session was relaunched on Opus 5 before
any work began, and every commit, measurement and round-2 remediation on this branch is that Opus
session's. The `Model:` line records the model that did the work, not the one the brief planned
for.

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
| C-001 | The per-statement `metadata.json` census is measured on a release module with the knob on and off, and the shipped half meets the brief's targets (SELECT ≤ 1, DML ≤ 2) for statements that read an existing table. | The `strace -f -e trace=openat` census with marker files; the in-process counter the Python pins read. | **PROVEN** | Analysis §7.6 reports TOTAL opens (SELECT 2, INSERT 5, DELETE 6, UPDATE 7, MERGE 4, CTAS 3); this unit's table splits reads from the commit's own write, so its knob-off column is **reads only** (2 / 4 / 5 / 6 / 3) and reads + writes reproduce §7.6 exactly — round 2 corrected a label that called the reads column "§7.6's before column". Knob on: **0 reads on every statement that reads an existing table**. `CREATE TABLE` and CTAS read **1 with the knob on AND off** — the catalog reads back the document it wrote to prove reachability before claiming the pointer, so creation is not cacheable and is not claimed. Tables in [docs/perf/iceberg-catalog-io-baseline.md](../../../docs/perf/iceberg-catalog-io-baseline.md) §1. |
| C-002 | A session-scoped cache keyed by **metadata-file location** is built once per session and handed to every **memory** catalog it builds — Glue and S3 Tables are NOT wired and their call shape is unchanged; a moved pointer is never served from an old key; the catalog pointer, not the cache, stays authoritative. | Rust pins on two doors over one catalog; the fork's `TableMetadataCache` contract; the AWS builders' surface at pin `189a73ed`. | **PROVEN** | Round 2 correction: only `memory_catalog_cached` takes a `CatalogCaches`; `glue_catalog` / `s3tables_catalog` are untouched because the fork's AWS builders have no `with_table_metadata_cache`, so every number here is the memory catalog and the AWS census reads **unchanged today** rather than zeroed (`PERF-CATALOG-AWS-CACHE-1` / `F-CATIO-AWS`). `crates/repark-iceberg/src/catalog/caches.rs`; five mechanics pins in `crates/repark-spark/src/tests/catalog_cache_staleness.rs`, one of which exists only because a mutation escaped (below). Measured and recorded: the memory catalog **evicts the pointer it replaced and seeds the new one** on commit, so retention is FLAT across DML and the reader after a commit pays no GET — the pin asserts that, after a first draft asserted the opposite and was wrong. |
| C-003 | The staleness contract holds with the cache in place: a commit, a schema change, a MERGE after another door's commit, `rewrite_manifests` + `expire_snapshots`, and DROP + re-CREATE are all correct across two doors on one catalog. | Five Rust pins; the whole Iceberg + Spark surface; the facade suite. | **PROVEN** | Five staleness pins in `catalog_cache_staleness.rs`, green before and after. DROP + re-CREATE cannot ABA: the memory catalog writes Hive/REST `<version>-<uuid>.metadata.json`, `with_next_version` draws a fresh uuid, and `drop_table` evicts. `cargo test -p repark-iceberg -p repark-spark` green (1,261 tests), `make test` green, the facade suite 4,775 passed / 211 skipped, the parity harness 574 passed, and the live legs 179 passed / 3 skipped (the three fork-gated ones). |
| C-004 | Two conf knobs with underscore aliases shape the cache and fail loud naming the key; the retained-location bound is trimmed at the statement door; `session.rs` pays for the wiring by splitting, and the CAP-1 mirror moves in the same commit. | Knob pins in Rust and Python; the size gates. | **PROVEN** | `repark.iceberg.metadataCache` (default true) and `repark.iceberg.metadataCacheEntries` (default 512). A bad value names BOTH the key the user set and the canonical spelling it aliases (round 2: it named only the canonical). `register_late_configured_catalogs` moved to `session/late_catalogs.rs`; `check_rust_file_size.py` and `test_cap_1_source_file_line_cap.py` both ratchet `session.rs` 1039 → 1002 in one commit. **The bound's scope is the statement door, measured and pinned.** It bounds what a session accumulates across commits (eight CREATEs at `entries=1` leave 2 retained, next door clears to 0); it does NOT bound retention within one statement (an 8-way `UNION ALL` at `entries=1` retains 8 until the next door). That residue is one entry per distinct table the statement names — working set the planner needs, bounded by the statement's table count — not the accumulation the knob exists to stop. Bounding within a statement needs a hook on cache INSERT, which is fork-side; the RePark-side alternative is a `SchemaProvider` decorator with a permanent forwarding-audit duty, which is not worth a bound on working set, so the scope is documented and pinned and `PERF-CATALOG-CACHE-BOUND-1` / `F-CATIO-BOUND` carries the real fix (a bounded LRU bounds within a statement by construction). |
| C-005 | Parts 1 and 3 need the fork; they are implemented and test-green there, measured through a temporary never-committed path override, and every pin that needs them SKIPs naming the ask. | The fork lane; the override; the skipped legs; the empty manifest diff. | **PROVEN** | `$HOME/repark-lanes/lanes/catio-fork` `fork/catio-io` on `189a73ed`: `F-CATIO-A` (one load per planning round) and `F-CATIO-B` (`TableBuilder::object_cache` + `MemoryCatalogBuilder::with_shared_object_cache_bytes`). Fork tests 3,612 passed / 0 failed plus the whole `iceberg-datafusion` suite; a RePark facade run against the override reached 50 % with no failure before the run was cut short, and the note claims no more than that. `t_many/count_id/stmt2` **120.01 → 11.33 ms** (target ≤ 20) and a repeated read opens **no** manifest-list and **no** manifest. `F-CATIO-AWS` (Glue / S3 Tables `with_table_metadata_cache`) and `F-CATIO-BOUND` (a bounded LRU inside the fork's cache) are filed as registry rows, not implemented. All four asks now have registry rows in the `PERF-DVCLOSE-STMT-1` form — `docs/fork-sync.md` carries the pin HISTORY, not an ask ledger (read in round 2), so the registry is where an ask lives. `git diff origin/main -- Cargo.toml Cargo.lock` empty. |
| C-006 | The baseline note carries the census tables, the timing cells, the machine, the recorded load, the re-measured floor and the commands; the two registry rows carry the before/after. | The note; the registry. | **PROVEN** | [docs/perf/iceberg-catalog-io-baseline.md](../../../docs/perf/iceberg-catalog-io-baseline.md). Both timing columns are the same release module in back-to-back runs, each with its own floor and load — the box was NOT quiet (load 7–12, a sibling lane's `cargo` live) and the note says so. `PERF-CATALOG-CALLS-1` FIXED, `PERF-ICE-MANIFEST-1` BACKLOG behind the pin bump. |
| C-007 | Every touched `map.md` moves in lockstep and the design reasons AND the pin citations live there, not in code; no code comment is added. | `make check-map-sync`; the staged-diff comment self-check. | **PROVEN** | Nine maps. **Owner ruling applied mid-round-2:** the twelve `/// pins: …` lines round 1 put on the tests are code comments, and a pin citation belongs only in the directory's `map.md` — all twelve are deleted and `crates/repark-spark/src/tests/map.md` now carries a test → clause table beside its `pins:` line, so no citation was lost and `make check-ledger-grammar` still resolves every clause. The branch diff over `*.rs` / `*.py` / `*.toml` now adds exactly nine comment lines: the five that MOVED with `register_late_configured_catalogs` (each verified present in `origin/main`'s `session.rs`) and two forced `/// # Errors` + one-body-line pairs on `pub fn`s returning `Result`. |

VERDICT: 7 clauses, 7 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: perf-ice-catalog-io-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Twelve Rust pins on two doors over one catalog plus thirteen always-run Python legs cover the cache mechanics, both knobs and both underscore aliases, the bound and its measured scope, the Hadoop register_table path, and the staleness contract; three legs skip naming their fork ask.
      artifacts: [crates/repark-spark/src/tests/catalog_cache_staleness.rs, python/repark/tests/test_perf_ice_catalog_io_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: The ABA hazard was chased to eviction, not to the naming scheme. Round 2 refuted the uuid argument by adopting a Hadoop layout through CALL register_table, which commits deterministic v(N+1).metadata.json with no uuid; the property that actually holds is evict-on-commit plus evict-on-drop, and both the uuid path and the Hadoop path are pinned.
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
      evidence: Mutation score measured, six mutations, two of them escapes that were closed. RePark side - trim made a no-op reds the bound pin; the knob-off branch dropped reds the disabled pin; the cache never built reds the unmoved-pointer and commit-seed pins; trim made a per-statement flush reds NOTHING at first, so the unmoved-pointer pin was strengthened to call trim between the reads until it did. Fork side under the temporary override - a lookup that serves another key's entry reds the sibling-table pin, and a manifest-list key with the path removed reds five pins; the first form of that mutation, a lookup that ignores the key entirely, reds NOTHING because the memory catalog evicts on commit and a single-table fixture holds exactly one entry, which is why the sibling-table pin was added.
      artifacts: [crates/repark-spark/src/tests/catalog_cache_staleness.rs]
    - id: AT-8
      status: N/A
      justification: No dependency, lockfile or workspace-manifest change.
    - id: AT-9
      status: ATTACKED
      evidence: Five registry rows - PERF-CATALOG-CALLS-1 FIXED narrowly, and PERF-ICE-MANIFEST-1, PERF-CATALOG-LOADS-1, PERF-CATALOG-AWS-CACHE-1 and PERF-CATALOG-CACHE-BOUND-1 BACKLOG, one per fork ask (F-CATIO-B, F-CATIO-A, F-CATIO-AWS, F-CATIO-BOUND), each naming what it measures and the pins that un-skip at the bump. docs/fork-sync.md carries the pin history, not an ask ledger, so it is not the home for these.
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
| `repark-spark/src/tests/catalog_cache_staleness.rs` | New: twelve pins on two doors over one catalog |
| `python/repark/tests/test_perf_ice_catalog_io_1.py` | New: thirteen always-run legs, three skipped fork-gated legs |
| `docs/perf/iceberg-catalog-io-baseline.md` | New: the census and timing tables, the four fork asks |

Public API breaks: **zero**. New public names only. No dependency change. No Spark-answer change.

**One public BEHAVIOUR change, recorded rather than broken.** `memory_catalog(warehouse)` keeps
its v1 signature but no longer returns a cache-free catalog: it builds a private, always-on
`CatalogCaches` per call. Nothing trims that one, because no session owns it — only the handles a
session builds through `memory_catalog_cached` reach `sql_with`'s trim. It is bounded in practice
by the same argument as the within-statement residue (one entry per distinct location that
catalog has loaded), and `F-CATIO-BOUND` removes the caveat. A caller that wants the pre-unit
behaviour passes `CatalogCaches::disabled()` to `memory_catalog_cached`. Every in-tree caller is
a test or the session path; the alternative — leaving `memory_catalog` cache-free — would have
made the default worse than the knob's default and split the two entry points' semantics.

## The number

| cell | before | after (shipped) | after (with the fork asks) | target |
|---|---:|---:|---:|---|
| `metadata.json` reads per SELECT | 2 | **0** | 0 | ≤ 1 |
| `metadata.json` reads per INSERT / DELETE / UPDATE / MERGE | 4 / 5 / 6 / 3 | **0** | 0 | ≤ 2 |
| `metadata.json` reads per CREATE / CTAS | 1 | 1 | 1 | not cacheable |
| catalog round trips (`load_table`) per SELECT / INSERT / DELETE / UPDATE / MERGE | 2 / 4 / 5 / 6 / 3 | 2 / 4 / 5 / 6 / 3 (now hits) | **1 / ≤ 2** | fork-gated |
| manifest-list + manifest opens, repeated SELECT | 1 + 1 | 1 + 1 | **0 + 0** | — |
| `t_many/count_id` second statement (193 manifests) | 120.35 ms | 120.01 ms | **11.33 ms** | ≤ 20 ms |
| `t_many_merged/count_id` second statement (1 manifest) | 17.63 ms | 14.91 ms | **10.56 ms** | — |

Floors 1.10 / 0.30 / 0.87 ms in their own runs; load 7–12 throughout.

## Round 2 — review gaps

An Opus critic built its own release module and reproduced the engine independently: the census
(reads 2/2/2/4/5/6/3 → 0, writes untouched), no stale read or lost write across 16 Python keys,
two catalogs sharing one cache, seven further Rust two- and three-door scenarios including
concurrent MERGEs, and both mutation scores. It returned FAIL on eleven findings — none of them
about what the engine does, all of them about what round 1 CLAIMED, how the work was SCOPED, and
what was FILED. That distinction is the finding worth keeping: the code was right and the writing
was not, and a reader of the registry would have believed something false about Glue.

| # | Finding | Disposition |
|---|---|---|
| S2-1 | The Glue table gave every statement an S3-GET-after of 0 and the FIXED row said the cache is "handed to every catalog it builds" — but only `memory_catalog_cached` takes one, and the fork's AWS builders have no `with_table_metadata_cache`. | REMEDIATED. Baseline §1's AWS table now reads **unchanged today** in both columns and names the two asks that stand between it and a zero; the registry row, C-002, the crate map and the staging row all say memory-catalog-only. |
| S2-2 | "0 metadata reads on every statement" is false for CREATE / CTAS (measured 1 with the knob on AND off; §7.6 has a CTAS row the census table dropped). | REMEDIATED. Both rows added to the census table, measured on both knob settings; every restatement now says "every statement that reads an existing table", and the baseline says plainly that creation is not cacheable and why. |
| S2-3 | `PERF-CATALOG-CALLS-1` was filed FIXED while the `load_table` count per statement — the Glue `GetTable` count — is untouched. | REMEDIATED, and re-measured rather than taken on trust: the census counter gives `hits + misses` per statement as SELECT 2, INSERT 4, DELETE 5, UPDATE 6, MERGE 3, identical to the knob-off read column. The row is narrowed to "the metadata document is fetched once per location, not once per `load_table`", with three explicit non-claims; `PERF-CATALOG-LOADS-1` (F-CATIO-A) and `PERF-CATALOG-AWS-CACHE-1` (F-CATIO-AWS) filed BACKLOG in the `PERF-DVCLOSE-STMT-1` form. |
| S2-4 | The bound only trims at the `sql_with` door: one statement over N tables retains N regardless of the knob (measured: 8-way UNION at `entries=1` → 8). | REMEDIATED by documenting and pinning the scope, not by moving the trim. Reproduced independently (8 CREATEs → 2 retained; UNION → 8; next door → 0; the table still answers). Bounding within a statement needs a hook on cache INSERT, which is fork-side; the RePark-side alternative is a `SchemaProvider` decorator with a permanent forwarding-audit duty, for a bound on working set rather than on a leak. Pinned both sides, stated in C-004, the crate map and the conf key's description, and `PERF-CATALOG-CACHE-BOUND-1` / `F-CATIO-BOUND` carries the real fix. |
| S2-5 | The staging map row still said "nine pins" and "a four-mutation score" after `63bfd4a4` updated the ledger and the crate map. | REMEDIATED. Row rebuilt and every count in it re-derived from the tree: twelve Rust pins, thirteen always-run Python legs, three skipped legs, six mutations, four fork asks, five registry rows. |
| S2-6 | C-003's ABA reason (Hive/REST uuid) is refuted on a supported path: a Hadoop layout adopted through `CALL register_table` commits deterministic `v(N+1).metadata.json`. | REMEDIATED. Measured v1 → v5 across INSERT, INSERT, INSERT OVERWRITE, INSERT with rows correct throughout, and pinned. The reason is rewritten to evict-on-commit plus evict-on-drop, which holds under both naming schemes; the crate map carries the correction. |
| S2-7 | `F-CATIO-BOUND` was named in C-004 and the crate map but filed nowhere, while AT-9 claimed "three fork asks named". | REMEDIATED. `PERF-CATALOG-CACHE-BOUND-1` filed BACKLOG; AT-9 rewritten to five registry rows and four asks. `docs/fork-sync.md` was read: it carries the pin HISTORY, not an ask ledger, so it is not the home for these and was left alone. |
| S3-1 | `memory_catalog(warehouse)` keeps its signature but now builds a private, always-on, never-trimmed cache per call. | REMEDIATED by recording it. "What changed" carries the behaviour change, why the alternative was worse, and the escape (`CatalogCaches::disabled()`); the crate map's `builders.rs` reason carries the same. |
| S3-2 | The underscore-alias error named the canonical key, not the key the user set. | FIXED in code. `lookup` returns the key it matched and the message names both spellings; pinned in Python for both aliases, red on the pre-fix module. |
| S3-3 | C-001 called §7.6's TOTAL opens "the before column" while this unit's before column is reads only. | REMEDIATED. C-001 and baseline §1 both state that §7.6 is totals, that this table splits reads from writes, and that reads + writes reproduce §7.6 exactly. |
| S3-4 | The `Model:` line reads `opus-5` while the brief said `fable-5.1`. | REMEDIATED. `opus-5` kept — it is what did the work — with a model note recording the relaunch. |
| owner | Comment leak on PR #379: the twelve `/// pins: …` lines on the tests are code comments; a pin citation lives only in the directory's `map.md`. | FIXED. All twelve deleted; `crates/repark-spark/src/tests/map.md` carries a test → clause table beside its `pins:` line. The branch now adds nine comment lines in total, all sanctioned: five MOVED with `register_late_configured_catalogs` and two forced `# Errors` pairs. `make check-ledger-grammar` still resolves every clause through the maps. |

No finding changed a measured number. The `t_many` cells, the timing floors and the mutation
scores are the round-1 numbers, re-derived where round 2 measured them again.

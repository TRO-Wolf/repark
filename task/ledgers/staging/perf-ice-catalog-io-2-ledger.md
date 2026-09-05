# Charter ledger — PERF-ICE-CATALOG-IO-2 · RePark wires the fork's shared manifest ObjectCache

**Date:** 2026-09-05 · **Branch:** `perf/ice-catalog-io-2` · **Base:** `origin/main` `7bef4afd`
· **Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard** (catalog correctness: a manifest cache in a
`plan_files` path is where staleness dies quietly; the battery below is the mitigation).
**Registry:** `PERF-ICE-MANIFEST-1` (FIXED, see C-006), `PERF-CATALOG-CACHE-BOUND-1`
(NARROWED to the metadata cache, see C-005), `PERF-CATALOG-COMMIT-CACHE-1` (new, BACKLOG behind
fork ask `F-CATIO-COMMIT`, see C-006).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** PERF-ICE-CATALOG-IO-1 part 3 measured the prize through a temporary path override
(`t_many/count_id/stmt2` 120.01 → 11.33 ms) and RP-12 landed the fork side at pin `79119643`
(`MemoryCatalogBuilder::with_shared_object_cache_bytes`, `TableBuilder::object_cache`, `F-CATIO-A`
one-load-per-round already live with no RePark wiring). This unit is the RePark side: a session
config key that sizes the shared manifest cache for the memory catalog, the wiring, the pins, and
the re-measurement on the real pin.

**Not in this unit:** any fork change (including the `F-CATIO-COMMIT` fix — filed, not fixed);
any `Cargo.toml` / `Cargo.lock` change (`git diff origin/main -- Cargo.toml Cargo.lock` is empty);
any AWS measurement; Glue / S3 Tables wiring (their builders have no
`with_shared_object_cache_bytes` at `79119643` — verified by reading the fork source, so their
call shape is unchanged today, stated in C-002); `STATUS.md` and `briefs/next-sequence.md`;
`make ledger-archive` (it would touch `STATUS.md`, so the still-staging IO-1 ledger is left for
the orchestrator's pickup).

## PROPOSITION LEDGER — PERF-ICE-CATALOG-IO-2 — 2026-09-05

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A session key `repark.iceberg.manifestCacheBytes` with underscore alias `repark.iceberg.manifest_cache_bytes` sizes the shared manifest cache; default ON at 32 MiB; `0` disables; a bad value fails loud at session build naming BOTH the key the user set and the canonical spelling. | Rust parse pins (both spellings, both refusals, `0` accepted, default); Python refusal legs. | **PROVEN** | `from_config_map` takes both spellings, refuses `"many"` / `"-1"` / `""` naming both, parses `"0"` to 0, and defaults to 33554432 — five pins in `caches.rs::tests`. Python: the refusal parametrizations grow by the new key and alias, and the `"0"` / `"512"` legs build. The key flows through the pre-existing `session.rs` `from_config_map` call, so no session change was needed. Default ON because the prize is 10× (115.81 → 10.95 ms), the objects are immutable at their path, the bound is fork-enforced, and the metadata cache set the default-on precedent. |
| C-002 | The bytes flow through `CatalogCaches` into `MemoryCatalogBuilder::with_shared_object_cache_bytes`; every table the memory catalog loads shares the ONE `ObjectCache`; the only loads outside it are the fork's `#[cfg(test)]` fixtures and catalog-detached `StaticTable` builds, both named. | Builder-wiring pins: two doors over one catalog share (a read after manifest deletion still answers); `memory_catalog` keeps its signature. | **PROVEN** | `memory_catalog_cached` passes nonzero bytes to `with_shared_object_cache_bytes`; `memory_catalog(warehouse)` keeps its signature (and now sizes a private shared cache per call, recorded like IO-1's metadata analogue). A second door answers after every manifest is deleted from disk; a configured 1 MiB value builds a sharing cache. Fork read at `79119643`: the memory catalog assembles tables in exactly three places, all through `table_builder()`; its only direct `Table::builder()` is a `#[cfg(test)]` fixture. Named non-members: `StaticTable::from_metadata*`, staged create/replace (RePark uses them write-side), the `delete_reachable_files` walk, and all non-memory catalogs. Critic correction recorded: tables CARRY the cache, but the fork's transaction/maintenance/inspect paths never consult it (0 cached vs 166 direct loads in `transaction/`) — filed as `F-CATIO-COMMIT`, not a wiring defect. |
| C-003 | The part-3 pin is un-skipped and green: `t_many/count_id/stmt2` ≤ 20 ms on a release module; a knob-off (`0`) control shows the repeated read re-opens manifests. | The un-skipped timing leg; the knob-off delete-trick leg; `t_many_merged` before/after. | **PROVEN** | The leg runs un-skipped and green. Probe cells on the release module: `t_many/count_id/stmt2` 115.81 → **10.95 ms** (spread 1.22, target ≤ 20 — roughly half the target on a 193-manifest table, while the always-run leg times a 48-manifest fixture, so the margin is wider where it gates); `t_many_merged` 14.37 → 10.49. Knob-off controls in Rust (`with_zero_bytes_a_repeated_read_opens_manifests_again`, parsing `"0"` end to end) and Python (the re-read raises naming `manifest`). |
| C-004 | The staleness contract holds with the manifest cache on: commit visibility, schema change, MERGE after another door's commit, DROP + re-CREATE, `register_table`, rewrite + expire immutability-by-path (next read opens only new paths, same rows), time-travel and branch reads unaffected. | The IO-1 Rust battery re-run green under default settings (it builds `CatalogCaches::default`, so the manifest cache is on) plus new Python legs per cell. | **PROVEN** | All six IO-1 Rust staleness pins run green under `CatalogCaches::default()` (32 MiB manifest cache on) — 16/16 in the module. Python: new legs for MERGE-after-commit (matches the committed row), DROP + re-CREATE (answers its own row count), `register_table` (correct across a commit), rewrite + expire (every pre-rewrite path gone from disk, next read answers the same rows), `VERSION AS OF` and branch reads; the IO-1 commit-visibility and schema-change legs run unchanged and are the cache-on re-run for those cells. Two-door shape stays in Rust (two Python sessions cannot share one memory catalog). No stale read anywhere: HALT condition never met. |
| C-005 | The byte budget binds and never corrupts: many tables under a tiny budget stay row-correct; the bound itself is the fork's moka `max_capacity`, enforced by weight rejection the fork unit-pins at this pin. | Bound-safety pins (tiny budget, many tables, rows right); the fork eviction reading. | **PROVEN** | 512 bytes over eight tables stay row-correct in Rust and Python (working set ≈ 8 KB against a 512 B budget, so the bound engages). Fork reading at the pin: moka `max_capacity(bytes)` on total entry weight (manifest entries × 768 B, list entries × 256 B, floored at 1, fork unit-pinned), TinyLFU admission with rejection, size eviction (moka 0.12.15 `admit` / `evict_lru_entries`). No byte-counter pin is writable: `ObjectCache` exposes no stats handle and moka eviction runs async — so the pin is correctness-under-eviction, and `PERF-CATALOG-CACHE-BOUND-1` is narrowed to the metadata cache it actually describes. |
| C-006 | `t_many/count_id/stmt2` and `t_many_merged` are re-measured before/after on a release module (5 iterations, medians, spread, floor, load) with the §1 census cells; the baseline's part-3 section carries the new numbers; `PERF-ICE-MANIFEST-1` is FIXED with before/after and `PERF-CATALOG-CACHE-BOUND-1` is narrowed to the metadata cache. | The baseline note; the registry rows. | **PROVEN** | Baseline §5: release module 163,517,296 B, `__debug_assertions__` False, 5 iterations after warm-up, medians/min/spreads, floors 0.31/1.13, loads 14.5/15.9 (not a quiet box, stated), the full 12-row census re-run on both settings. `PERF-ICE-MANIFEST-1` FIXED with before/after (115.81 → 10.95); `PERF-CATALOG-CACHE-BOUND-1` NARROWED to the metadata cache; `PERF-CATALOG-COMMIT-CACHE-1` filed BACKLOG behind `F-CATIO-COMMIT`. |
| C-007 | Every touched `map.md` moves in lockstep with reasons and `pins:` citations; no code comment is added. | `make check-map-sync`; the staged-diff comment self-check. | **PROVEN** | Seven maps move with their directories (catalog, iceberg src, spark tests, python tests, perf, guide, staging) plus this ledger: reasons and `pins:` citations, stale IO-1 sentences (`stays skipped`, `BACKLOG behind the pin bump`) trued up. The branch diff over `*.rs` / `*.py` / `*.toml` adds zero comment lines — the only `//` hit is a `"file://"` string literal, and no forced `# Errors` was needed (no new `pub fn` returns `Result`). |

VERDICT: 7 clauses, 7 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: perf-ice-catalog-io-2
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Five Rust parse pins, four Rust delete-manifest pins, six IO-1 Rust staleness pins re-run with the cache on, ten Python legs (un-skipped part-3 plus nine new) and two extended refusal parametrizations cover the key, both spellings, both refusals, the wiring, the funnel, the knob-off control, the bound and every staleness cell; the two-door shape is Rust-held, the Python legs single-session sequential through the same shared cache.
      artifacts: [crates/repark-iceberg/src/catalog/caches.rs, crates/repark-spark/src/tests/catalog_cache_staleness.rs, python/repark/tests/test_perf_ice_catalog_io_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: The merge zero-savings alarm was chased to its root instead of absorbed: per-path strace dumps show MERGE opening the same new list and two new manifests four times each, and the fork read shows transaction/maintenance/inspect at 0 cached reads against 166+ direct loads. The cache consults on scan only; bypassing paths re-read and never serve stale. Filed as PERF-CATALOG-COMMIT-CACHE-1/F-CATIO-COMMIT, and the catalog map's over-claim (every table shares) was corrected to carries-but-commit-bypasses before the attestation.
      artifacts: [docs/perf/iceberg-catalog-io-baseline.md, crates/repark-iceberg/src/catalog/map.md]
    - id: AT-3
      status: ATTACKED
      evidence: No AWS call, no credential, no IAM surface, no .github change; the two AWS legs still skip behind the acceptance env gate plus their fork ask.
      artifacts: [python/repark/tests/test_perf_ice_catalog_io_1.py]
    - id: AT-4
      status: ATTACKED
      evidence: Every number is a release module refusing debug assertions (163,517,296 B, __debug_assertions__ False), five iterations after a warm-up, with medians, mins, spreads, per-run floors and 1-minute loads; the box was not quiet (load 14.5-15.9, a sibling build live) and the note says so; the off column reproduces IO-1's shipped column cell for cell.
      artifacts: [docs/perf/iceberg-catalog-io-baseline.md]
    - id: AT-5
      status: ATTACKED
      evidence: No dependency change. The fork work is consumed through the base pin bump (RP-12, 79119643), already on origin/main; git diff origin/main -- Cargo.toml Cargo.lock is empty at hand-back.
      artifacts: [Cargo.toml, Cargo.lock]
    - id: AT-6
      status: ATTACKED
      evidence: The bytes flow once at catalog build; the product path pays no per-statement cost for the wiring. The delete-manifest instrument touches test warehouses only, and the "0" knob reconstructs the pre-unit load path in the same process.
      artifacts: [crates/repark-iceberg/src/catalog/builders.rs, python/repark/tests/test_perf_ice_catalog_io_1.py]
    - id: AT-7
      status: ATTACKED
      evidence: Mutation score measured, four code mutations plus three behavior-side knob flips, one escape closed. Dropped wiring reds exactly the two Rust sharing pins; ignored bytes red both sizing pins; parse floored at 1 MiB reds zero_disables but NOT the knob-off control at first (struct literal bypassed the parser), so it was strengthened to parse "0" until it red; refusal naming only the set key reds the alias pin. Python knob flips: off on the sharing leg reds, nonzero on the off-control reds DID-NOT-RAISE, off on the timing leg reds over-target.
      artifacts: [crates/repark-spark/src/tests/catalog_cache_staleness.rs, python/repark/tests/test_perf_ice_catalog_io_1.py]
    - id: AT-8
      status: N/A
      justification: No dependency, lockfile or workspace-manifest change.
    - id: AT-9
      status: ATTACKED
      evidence: Three registry rows - PERF-ICE-MANIFEST-1 FIXED with before/after (its BACKLOG state lived only in the IO-1 ledger, so the row is filed here complete), PERF-CATALOG-CACHE-BOUND-1 NARROWED to the metadata cache, and PERF-CATALOG-COMMIT-CACHE-1 BACKLOG behind F-CATIO-COMMIT. docs/fork-sync.md carries the pin history, not an ask ledger, so it is not the home for these.
      artifacts: [docs/spark-sql-iceberg-parity.md, docs/perf/iceberg-catalog-io-baseline.md]
    - id: AT-10
      status: ATTACKED
      evidence: Seven clauses, each cited by a pin in a map; the fork-side remainder (commit-path caching, AWS builders) is stated as filed rather than claimed as shipped.
      artifacts: [task/ledgers/staging/perf-ice-catalog-io-2-ledger.md]
  complete: true
```

## What changed

| Site | Change |
|---|---|
| `repark-iceberg/src/catalog/caches.rs` | New: `manifestCacheBytes` key + alias + default, `parse_bytes`, `manifest_cache_bytes()`; five parse pins |
| `repark-iceberg/src/catalog/builders.rs` | `memory_catalog_cached` passes nonzero bytes to `with_shared_object_cache_bytes` |
| `repark-iceberg/src/catalog/mod.rs`, `src/lib.rs` | Re-export the five new names |
| `repark-spark/src/tests/catalog_cache_staleness.rs` | Four delete-manifest pins; two IO-1 bound tests gain the new settings field at its default |
| `python/repark/tests/test_perf_ice_catalog_io_1.py` | Part-3 pin un-skipped; nine new legs; refusal parametrizations extended |
| `docs/perf/iceberg-catalog-io-baseline.md` | New §5: the part-3 re-measurement on the real pin |
| `docs/spark-sql-iceberg-parity.md` | `PERF-ICE-MANIFEST-1` FIXED, `PERF-CATALOG-CACHE-BOUND-1` narrowed, `PERF-CATALOG-COMMIT-CACHE-1` filed |
| `docs/guide/session-and-conf.md` | New "Iceberg catalog caches" section: all three keys, build-time, memory-catalog-only |

Public API breaks: **zero**. New public names only. No dependency change. No Spark-answer change.

**One public BEHAVIOUR change, recorded rather than broken.** `memory_catalog(warehouse)` keeps
its v1 signature but now sizes a private shared manifest cache (32 MiB default) per call, which
nothing trims because no session owns it — the same always-on, never-trimmed shape IO-1 recorded
for the metadata cache. A caller that wants the pre-unit behaviour passes
`CatalogCaches::disabled()` to `memory_catalog_cached`. Every in-tree caller is a test or the
session path.

## The number

| cell | before (manifest off) | after (manifest on) | override §3.1 | target |
|---|---|---:|---:|---|
| `t_many/count_id/stmt2` (193 manifests) | 115.81 ms | **10.95 ms** | 11.33 ms | ≤ 20 ms |
| `t_many/point/stmt2` | 124.75 ms | **14.75 ms** | 14.49 ms | — |
| `t_many_merged/count_id/stmt2` (1 manifest) | 14.37 ms | **10.49 ms** | 10.56 ms | — |
| `t_many_merged/point/stmt2` (1 manifest) | 17.85 ms | **13.20 ms** | 13.19 ms | — |
| repeated-SELECT manifest-list + manifest opens | 1 + 1 | **0 + 0** | 0 + 0 | — |
| DELETE / UPDATE manifest opens | 8 / 15 | **6 / 12** (read side only) | 6 / 12 | — |
| MERGE / INSERT manifest opens | 8 / 1 | 8 / 1 (commit side, filed) | — / — | — |

Floors 0.31 / 1.13 ms in their own runs; load 14.5–15.9 throughout.

## Mutation score

Four code mutations (each reverted after measuring) plus three behavior-side knob flips:

| # | Mutation | Reds | Notes |
|---|---|---|---|
| M1 | `builders.rs`: drop the `with_shared_object_cache_bytes` call | exactly the two Rust sharing pins; the other 14 stay green | the wiring is what shares |
| M2 | `CatalogCaches::new` ignores the setting (hardcodes the default) | `both_spellings_size_the_cache`, `zero_disables_the_shared_cache` | the bytes flow through the struct |
| M3 | `parse_bytes` floors at 1 MiB (knob-off ignored) | `zero_disables_the_shared_cache` at once; `with_zero_bytes_…` NOTHING at first | escape: the control built settings from a struct literal and never touched the parser — strengthened to parse `"0"` until it red |
| M4 | refusal names only the set key (drops `named()`) | `a_bad_alias_names_the_key_set_and_the_canonical_one` | the alias names both |
| PM-b | Python off-control flipped to `"1048576"` | the off-control (DID NOT RAISE) | the leg is sensitive to the knob value |
| PM-c | Python sharing leg flipped to `"0"` | the sharing leg | default-on is load-bearing |
| PM-d | Python timing leg flipped to `"0"` | the timing leg (over target) | the pin measures the cache, not the fixture |

## Critic pass (in-lane, single-session)

Two findings, both claim-scope, both remediated before the attestation — the shape IO-1's
round 2 established (the code was right, the writing over-claimed):

| # | Finding | Disposition |
|---|---|---|
| C-1 | The catalog map said "every table the catalog materializes shares the one cache" — but the census showed MERGE saving nothing and DELETE/UPDATE saving one cycle. Root cause, measured: the fork's transaction/maintenance/inspect paths never consult the cache (0 vs 166+ direct loads). | REMEDIATED. Map corrected to carries-but-commit-bypasses with the measured scope; filed `PERF-CATALOG-COMMIT-CACHE-1` / `F-CATIO-COMMIT`; baseline §5.2 carries the per-path evidence. Not staleness — a bypassing path re-reads — so no HALT. |
| C-2 | The map recorded the wiring but not the `memory_catalog()` behaviour change (private shared cache per call, never trimmed) — the analogue IO-1 recorded for the metadata cache. | REMEDIATED. Recorded in the crate map and in "What changed" above, with the `CatalogCaches::disabled()` escape. |

No finding changed a measured number.

## Staleness verdict

Every staleness cell is row-correct with the cache on (C-004), and the one surprise the
measurement turned up (commit-side bypass) fails toward re-reading, never toward a stale
answer. The brief's HALT condition — the shared cache serving a stale manifest on any cell —
never met. No HALT.

# Charter ledger — PERF-ICE-CATALOG-IO-2 · RePark wires the fork's shared manifest ObjectCache

**Date:** 2026-09-05 · **Branch:** `perf/ice-catalog-io-2` · **Base:** `origin/main` `7bef4afd`
· **Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard** (catalog correctness: a manifest cache in a
`plan_files` path is where staleness dies quietly; the battery below is the mitigation).
**Registry:** `PERF-ICE-MANIFEST-1` (FIXED with a HALT caveat, see C-006), `PERF-CATALOG-CACHE-BOUND-1`
(NARROWED to the metadata cache, see C-005), `PERF-CATALOG-COMMIT-CACHE-1` (new, BACKLOG behind
fork ask `F-CATIO-COMMIT`, see C-006), `PERF-CATALOG-LINEAGE-CACHE-1` (new, BACKLOG behind fork
ask `F-CATIO-KEY`, see HALT).

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
| C-001 | A session key `repark.iceberg.manifestCacheBytes` with underscore alias `repark.iceberg.manifest_cache_bytes` sizes the shared manifest cache; default ON at 32 MiB; `0` disables; a bad value fails loud at session build naming BOTH the key the user set and the canonical spelling. | Rust parse pins (both spellings, both refusals, `0` accepted, default); Python refusal legs. | **PROVEN** | `from_config_map` takes both spellings, refuses `"many"` / `"-1"` / `""` naming both, parses `"0"` to 0, and defaults to 33554432 — five pins in `caches.rs::tests`. Python: the refusal parametrizations grow by the new key and alias, and the `"0"` / `"512"` legs build. The key flows through the pre-existing `session.rs` `from_config_map` call, so no session change was needed. Default ON was argued from the 10× prize (115.81 → 10.95 ms), file-immutability at the path, the fork-enforced bound, and the metadata-cache precedent — but HALT refutes the premise as stated (the PARSED object embeds list-entry lineage context), so the default-ON choice is gated on the F-CATIO-KEY ruling in the HALT section. |
| C-002 | The bytes flow through `CatalogCaches` into `MemoryCatalogBuilder::with_shared_object_cache_bytes`; every table the memory catalog loads shares the ONE `ObjectCache`; the only loads outside it are the fork's `#[cfg(test)]` fixtures and catalog-detached `StaticTable` builds, both named. | Builder-wiring pins: two doors over one catalog share (a read after manifest deletion still answers); `memory_catalog` keeps its signature. | **PROVEN** | `memory_catalog_cached` passes nonzero bytes to `with_shared_object_cache_bytes`; `memory_catalog(warehouse)` keeps its signature (and now sizes a private shared cache per call, recorded like IO-1's metadata analogue). A second door answers after every manifest is deleted from disk; a configured 1 MiB value builds a sharing cache. Fork read at `79119643`: the memory catalog assembles tables in exactly three places, all through `table_builder()`; its only direct `Table::builder()` is a `#[cfg(test)]` fixture. Named non-members: `StaticTable::from_metadata*`, staged create/replace (RePark uses them write-side), the `delete_reachable_files` walk, and all non-memory catalogs. Critic correction recorded: tables CARRY the cache, but the fork's transaction/maintenance/inspect paths never consult it (0 cached vs 166 direct loads in `transaction/`) — filed as `F-CATIO-COMMIT`, not a wiring defect. |
| C-003 | The part-3 pin is un-skipped and green: `t_many/count_id/stmt2` ≤ 20 ms on a release module; a knob-off (`0`) control shows the repeated read re-opens manifests. | The un-skipped timing leg; the knob-off delete-trick leg; `t_many_merged` before/after. | **PROVEN** | The leg runs un-skipped and green. Probe cells on the release module: `t_many/count_id/stmt2` 115.81 → **10.95 ms** (spread 1.22, target ≤ 20 — roughly half the target on a 193-manifest table, while the always-run leg times a 48-manifest fixture, so the margin is wider where it gates); `t_many_merged` 14.37 → 10.49. Knob-off controls in Rust (`with_zero_bytes_a_repeated_read_opens_manifests_again`, parsing `"0"` end to end) and Python (the re-read raises naming `manifest`). |
| C-004 | The staleness contract holds with the manifest cache on: commit visibility, schema change, MERGE after another door's commit, DROP + re-CREATE, `register_table`, rewrite + expire immutability-by-path (next read opens only new paths, same rows), time-travel and branch reads unaffected. | The IO-1 Rust battery re-run green under default settings (it builds `CatalogCaches::default`, so the manifest cache is on) plus new Python legs per cell. | **REJECTED** | All six IO-1 Rust staleness pins run green under `CatalogCaches::default()` (16/16 in the module) and every new Python leg passes — but the full facade suite reds 4 tests the unit's own battery does not cover: the v2→v3 upgrade + lineage family serves `_row_id` NULL where assigned ids belong (see HALT below). The clause as written is false, so it is REJECTED, not PROVEN: the cached manifest object is not a pure function of its key (the list entry's `first_row_id` range feeds the parse and is not in the key), and a v2-context parse poisons later v3 reads of the same path within one catalog lifetime. |
| C-005 | The byte budget binds and never corrupts: many tables under a tiny budget stay row-correct; the bound itself is the fork's moka `max_capacity`, enforced by weight rejection the fork unit-pins at this pin. | Bound-safety pins (tiny budget, many tables, rows right); the fork eviction reading. | **PROVEN** | 512 bytes over eight tables stay row-correct in Rust and Python (working set ≈ 8 KB against a 512 B budget, so the bound engages). Fork reading at the pin: moka `max_capacity(bytes)` on total entry weight (manifest entries × 768 B, list entries × 256 B, floored at 1, fork unit-pinned), TinyLFU admission with rejection, size eviction (moka 0.12.15 `admit` / `evict_lru_entries`). No byte-counter pin is writable: `ObjectCache` exposes no stats handle and moka eviction runs async — so the pin is correctness-under-eviction, and `PERF-CATALOG-CACHE-BOUND-1` is narrowed to the metadata cache it actually describes. |
| C-006 | `t_many/count_id/stmt2` and `t_many_merged` are re-measured before/after on a release module (5 iterations, medians, spread, floor, load) with the §1 census cells; the baseline's part-3 section carries the new numbers; `PERF-ICE-MANIFEST-1` is FIXED with before/after and `PERF-CATALOG-CACHE-BOUND-1` is narrowed to the metadata cache. | The baseline note; the registry rows. | **PROVEN** | Baseline §5: release module 163,517,296 B, `__debug_assertions__` False, 5 iterations after warm-up, medians/min/spreads, floors 0.31/1.13, loads 14.5/15.9 (not a quiet box, stated), the full 12-row census re-run on both settings. `PERF-ICE-MANIFEST-1` FIXED with before/after (115.81 → 10.95); `PERF-CATALOG-CACHE-BOUND-1` NARROWED to the metadata cache; `PERF-CATALOG-COMMIT-CACHE-1` filed BACKLOG behind `F-CATIO-COMMIT`. |
| C-007 | Every touched `map.md` moves in lockstep with reasons and `pins:` citations; no code comment is added. | `make check-map-sync`; the staged-diff comment self-check. | **PROVEN** | Seven maps move with their directories (catalog, iceberg src, spark tests, python tests, perf, guide, staging) plus this ledger: reasons and `pins:` citations, stale IO-1 sentences (`stays skipped`, `BACKLOG behind the pin bump`) trued up. The branch diff over `*.rs` / `*.py` / `*.toml` adds zero comment lines — the only `//` hit is a `"file://"` string literal, and no forced `# Errors` was needed (no new `pub fn` returns `Result`). |

VERDICT: 7 clauses, 6 PROVEN, 0 OPEN, 1 REJECTED. **HALTED** — see below.

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
      evidence: Two deep attacks, one that filed an ask and one that halted the unit. (1) The merge zero-savings alarm was chased to its root: per-path strace dumps show MERGE opening the same new list and two new manifests four times each, and the fork read shows transaction/maintenance/inspect at 0 cached reads against 166+ direct loads; filed as PERF-CATALOG-COMMIT-CACHE-1/F-CATIO-COMMIT. (2) The facade suite's 4 upgrade-lineage failures were chased past "my wiring" to the fork's cache key: the parsed manifest embeds list-entry context (first_row_id range) that the (path, schema) key does not carry, so a v2-context parse poisons v3 reads of the same path. C-004 REJECTED, FINDING S1-1 filed OPEN, unit HALTED.
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

## HALT — the shared cache serves a wrong-context manifest (2026-09-05)

```text
FINDING:
  id: S1-1
  severity: S1
  category: AT-2
  clause: C-004
  disposition: OPEN (needs an orchestrator ruling; the fix is fork-side, out of this unit's scope)
```

The brief's HALT condition is met: with the cache on, four facade tests serve wrong
answers — `_row_id` / `_last_updated_sequence_number` come back NULL where assigned ids
belong — on tables that crossed the v2→v3 upgrade boundary:

- `test_v3_legacy_delete_merge.py::test_v3_legacy_parquet_position_delete_merges_into_the_dv`
- `test_v3_legacy_delete_merge.py::test_plain_where_mor_delete_over_a_legacy_parquet_delete_merges_into_the_dv`
- `test_v3_statement_coverage.py::test_v3_statement_row_reproduces_the_measured_repark_answer[alter-set-format-version-3-mor]`
- `test_v3_upgrade.py::test_alter_upgrade_with_the_opt_in_serves_v3_lineage`

Full suite at HALT: 4 failed, 4863 passed, 211 skipped. Isolation is decisive and needs
no rebuild: the same release binary with `repark.iceberg.manifestCacheBytes = "0"` passes
both merge tests (throwaway probe kept at `/tmp/knobprobe/test_knobprobe.py`, per the
scratch-probe rule). The only behavioral delta the knob moves is the
`with_shared_object_cache_bytes` call, so the sharing itself is the defect's trigger.

Root cause, read end to end at fork pin `79119643`:

1. `ObjectCache::get_manifest` keys by `(manifest_path, fallback_schema_id)`
   (`crates/iceberg/src/io/object_cache.rs`).
2. The cached `Manifest` is NOT a pure function of that key. `ManifestFile::
   load_manifest_with_schema_fallback` (`spec/manifest_list.rs`) parses the bytes and
   then runs `entry.inherit_data(self)` plus `assign_first_row_ids(entries,
   manifest_range)`, where `manifest_range` is the CALLER's list entry's `first_row_id`
   — list state, not manifest bytes.
3. `assign_first_row_ids` with `None` forces every entry to `None`; with `Some(range)`
   it assigns running counters (`spec/manifest/entry.rs`).
4. A V2 manifest list carries no range (`None`); the post-upgrade V3 list carries an
   assigned range for the SAME manifest path. First parse wins the cache entry: a
   v2-context parse (no assignment) poisons every later v3 read of that path within one
   catalog lifetime. Per-table caches never shared across the boundary, which is why
   knob-off passes.
5. Blast radius, measured not assumed: v2-parse-then-v3-read order only. The reverse
   order is harmless (v2 readers ignore the assigned fields), and carried v3 entries keep
   their ranges, so v3-only operation is stable. The defect needs the upgrade boundary
   (or any rangeless→ranged list pair over one path) plus a lineage read, in one catalog
   lifetime.

What this refutes: the "immutable at their path" premise this unit (and IO-1's §3, and
the fork's F-CATIO-B design) reasoned from. The manifest FILE is immutable; the PARSED
OBJECT depends on list-entry context the key does not carry. C-004 is REJECTED on that
ground. Nothing else in the ledger falls: the key, the wiring, the timing, the census,
the bound pins and the non-upgrade staleness cells are all still proven as stated.

No RePark-side fix exists: the key is built fork-side and RePark holds no handle to
observe or evict by it. The fork fix direction is to make the key carry the assignment
input (the list entry's range) or to move assignment out of the cached object; Java never
shares a parsed manifest across readers, so it has no equivalent constraint. Filed as
`PERF-CATALOG-LINEAGE-CACHE-1` / fork ask `F-CATIO-KEY` (registry row below).

Ruling needed (handback `questions[]`): (a) fix fork-side, repin, and resume this unit
toward default-ON; or (b) land this unit with the default OFF (knob shipped, timing pin
re-scoped to set the knob explicitly, MANIFEST-1 row held at BACKLOG-by-ledger) until the
fork key fix lands. Either way the four red tests are the detector: they pass knob-off
today and must pass knob-on before any default-ON merge.

# Charter ledger — PERF-ICE-CATALOG-IO-2 · RePark wires the fork's shared manifest ObjectCache

**Date:** 2026-09-05 · **Branch:** `perf/ice-catalog-io-2` · **Base:** `origin/main` `7bef4afd`
· **Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard** (catalog correctness: a manifest cache in a
`plan_files` path is where staleness dies quietly; the battery below is the mitigation).
**Registry:** `PERF-ICE-MANIFEST-1` (FILED here as FIXED — see C-006), `PERF-CATALOG-CACHE-BOUND-1`
(NARROWED, see C-005).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** PERF-ICE-CATALOG-IO-1 part 3 measured the prize through a temporary path override
(`t_many/count_id/stmt2` 120.01 → 11.33 ms) and RP-12 landed the fork side at pin `79119643`
(`MemoryCatalogBuilder::with_shared_object_cache_bytes`, `TableBuilder::object_cache`, `F-CATIO-A`
one-load-per-round already live with no RePark wiring). This unit is the RePark side: a session
config key that sizes the shared manifest cache for the memory catalog, the wiring, the pins, and
the re-measurement on the real pin.

**Not in this unit:** any fork change; any `Cargo.toml` / `Cargo.lock` change; any AWS measurement;
Glue / S3 Tables wiring (their builders have no `with_shared_object_cache_bytes` at `79119643` —
verified by reading the fork source, so their call shape is unchanged today, stated in C-002);
`STATUS.md` and `briefs/next-sequence.md`; `make ledger-archive` (it would touch `STATUS.md`, so
the still-staging IO-1 ledger is left for the orchestrator's pickup).

## PROPOSITION LEDGER — PERF-ICE-CATALOG-IO-2 — 2026-09-05

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A session key `repark.iceberg.manifestCacheBytes` with underscore alias `repark.iceberg.manifest_cache_bytes` sizes the shared manifest cache; default ON at 32 MiB; `0` disables; a bad value fails loud at session build naming BOTH the key the user set and the canonical spelling. | Rust parse pins (both spellings, both refusals, `0` accepted, default); Python refusal legs. | **OPEN** | Which question closes it: does `from_config_map` accept both spellings, refuse bad values naming both, and default to 33554432? |
| C-002 | The bytes flow through `CatalogCaches` into `MemoryCatalogBuilder::with_shared_object_cache_bytes`; every table the memory catalog loads shares the ONE `ObjectCache`; the only loads outside it are the fork's `#[cfg(test)]` fixtures and catalog-detached `StaticTable` builds, both named. | Builder-wiring pins: two doors over one catalog share (a read after manifest deletion still answers); `memory_catalog` keeps its signature. | **OPEN** | Which question closes it: does a second door survive manifest deletion, and does the fork read at the pin confirm the only exceptions? |
| C-003 | The part-3 pin is un-skipped and green: `t_many/count_id/stmt2` ≤ 20 ms on a release module; a knob-off (`0`) control shows the repeated read re-opens manifests. | The un-skipped timing leg; the knob-off delete-trick leg; `t_many_merged` before/after. | **OPEN** | Which question closes it: does stmt2 clear 20 ms with the cache on and re-read with it off? |
| C-004 | The staleness contract holds with the manifest cache on: commit visibility, schema change, MERGE after another door's commit, DROP + re-CREATE, `register_table`, rewrite + expire immutability-by-path (next read opens only new paths, same rows), time-travel and branch reads unaffected. | The IO-1 Rust battery re-run green under default settings (it builds `CatalogCaches::default`, so the manifest cache is on) plus new Python legs per cell. | **OPEN** | Which question closes it: is every cell row-correct with the cache on, and does the post-rewrite read cost only the new paths? |
| C-005 | The byte budget binds and never corrupts: many tables under a tiny budget stay row-correct; the bound itself is the fork's moka `max_capacity`, enforced by weight rejection the fork unit-pins at this pin. | Bound-safety pins (tiny budget, many tables, rows right); the fork eviction reading. | **OPEN** | Which question closes it: do tiny-budget reads stay correct, and what exactly does the fork guarantee? |
| C-006 | `t_many/count_id/stmt2` and `t_many_merged` are re-measured before/after on a release module (5 iterations, medians, spread, floor, load) with the §1 census cells; the baseline's part-3 section carries the new numbers; `PERF-ICE-MANIFEST-1` is FIXED with before/after and `PERF-CATALOG-CACHE-BOUND-1` is narrowed to the metadata cache. | The baseline note; the registry rows. | **OPEN** | Which question closes it: are the numbers from this pin, on a release module, with floor and load recorded? |
| C-007 | Every touched `map.md` moves in lockstep with reasons and `pins:` citations; no code comment is added. | `make check-map-sync`; the staged-diff comment self-check. | **OPEN** | Which question closes it: do the maps cite every PROVEN clause and does the diff add zero comment lines beyond the forced `# Errors`? |

VERDICT: 7 clauses, 0 PROVEN, 7 OPEN, 0 REJECTED.

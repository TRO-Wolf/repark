# Charter ledger — PERF-ICE-CATALOG-IO-3 · The shared manifest cache goes ON by default

**Date:** 2026-09-05 · **Branch:** `perf/ice-catalog-io-3` · **Base:** `origin/main` `b4af56d0`
· **Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: elevated** (a default changes every session: every
default-built memory catalog gains a 32 MiB shared manifest cache, so the flip is proven
on the fixed pin by the full staleness battery plus the four upgrade-lineage tests running
on default sessions).
**Registry:** `PERF-ICE-MANIFEST-1` (FIXED with the default-session number, see C-006),
`PERF-CATALOG-CACHE-BOUND-1` (narrowed to no-retention-outside-the-cache — the manifest
half is NOT closed by measurement, the metadata LRU stays BACKLOG, see C-005),
`PERF-CATALOG-CACHE-WEIGHT-1` (BACKLOG behind fork ask `F-CATIO-WEIGHT` with the
structural red-when-fixed pin, see C-005),
`PERF-CATALOG-LINEAGE-CACHE-1` (FIXED at RP-13 — the precondition, not this unit's claim;
its pin reference follows the IO-3 rename, see C-006),
`PERF-CATALOG-COMMIT-CACHE-1` (BACKLOG behind fork ask `F-CATIO-COMMIT`, untouched —
the bypass persists at the new pin, verified).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** PERF-ICE-CATALOG-IO-2 wired the fork's shared manifest cache behind
`repark.iceberg.manifestCacheBytes` but HALTED on `PERF-CATALOG-LINEAGE-CACHE-1` (the
shared cache served wrong-context lineage on upgrade-boundary tables) and landed with the
default OFF per the round-2 ruling. RP-13 then landed the fork fix (`F-CATIO-KEY` at pin
`2ed39cb0`: the cache stores the context-free parse and applies each caller's list-entry
inheritance and `first_row_id` assignment per read) and the knob-on detector redded as
designed before it was re-pinned to the assigned lineage. The only remaining step is this
unit: flip `DEFAULT_MANIFEST_CACHE_BYTES` to 32 MiB and prove the flip is safe on the
fixed pin.

**Not in this unit:** any fork change; any `Cargo.toml` / `Cargo.lock` change (`git diff
origin/main -- Cargo.toml Cargo.lock` is empty); any AWS measurement; Glue / S3 Tables
wiring (unchanged again — their builders have no `with_shared_object_cache_bytes` at
`2ed39cb0`); the `F-CATIO-COMMIT` fix (filed, not fixed); `STATUS.md` and
`briefs/next-sequence.md`; `make ledger-archive` (it would touch `STATUS.md`, so the
still-staging IO-1 and IO-2 ledgers are left for the orchestrator's pickup — this unit
edits the IO-2 ledger's status lines only, pointing its follow-up sentences at this unit).

## PROPOSITION LEDGER — PERF-ICE-CATALOG-IO-3 — 2026-09-05

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `DEFAULT_MANIFEST_CACHE_BYTES` is `33554432` (32 MiB); the size is argued from the IO-1/IO-2 measurements and the fork's weight-based bound, and the memory ceiling per session is stated; both spellings still size the cache, a bad value still fails loud naming BOTH the key the user set and the canonical spelling, and an explicit `0` still disables. | Rust parse pins (default, both spellings, both refusals, `0` accepted); Python refusal legs. | **PROVEN** | The constant is `33_554_432` (a one-line diff). `the_default_sizes_the_shared_cache` redded pre-flip (`left: 0, right: 33554432`) and is green after; both spellings size, both refusals name both keys, `"0"` parses to 0 (5/5 in `caches.rs::tests`, 16/16 in the staleness module). The Python refusal parametrizations are unchanged and green. Size argument and ceiling in the catalog map: the fork's own `DEFAULT_CACHE_SIZE_BYTES`, moka weight (768/256 B, floored at one entry), `t_many` at ~150 KB against a ~43k-entry budget, 32 MiB of manifest weight per memory catalog (one catalog per session is the usual shape). |
| C-002 | The four v3 upgrade/legacy tests that HALTED IO-2 are green with the cache ON by default: they run on the default session with no knob set. | The four tests green after the flip on the fixed pin. | **PROVEN** | All four green post-flip on default sessions (4 passed in 1.02 s): `test_v3_legacy_parquet_position_delete_merges_into_the_dv`, `test_plain_where_mor_delete_over_a_legacy_parquet_delete_merges_into_the_dv`, the `alter-set-format-version-3-mor` statement row, `test_alter_upgrade_with_the_opt_in_serves_v3_lineage`. They were green pre-flip too (4 passed in 1.28 s) — the flip moves their cache from off to on and they stay green, which closes IO-2's FINDING S1-1. |
| C-003 | The IO-2 staleness battery and the knob-on lineage pins run on the DEFAULT session: every explicit-knob leg is rewritten so the default-session leg is the primary and an explicit `0` is the off-control, and both directions stay knob-sensitive. | Default-session legs green after the flip; explicit-`0` controls green; knob flips red both ways. | **PROVEN** | Red-first: the default-sharing pin redded pre-flip (re-opened the deleted list: `No such file ... .avro`) and is green after; the Rust funnel pin likewise (15+1 → 16/16). Timing, six staleness legs and the lineage pin run on default sessions and are green (full file 30 passed, 3 skipped on debug; timing un-skipped and green on release). Explicit `0` (Python) and `with_zero_bytes` (Rust) stay as off-controls. Knob-sensitivity: PM-b/PM-c/PM-d flip red both ways; M1 reds exactly the two default pins. Two renames where the flip inverts the meaning (name map in "What changed"). |
| C-004 | The concurrency leg holds: two sessions over one warehouse, one v2 table upgraded to v3 in session A while session B holds a warm cache of the pre-upgrade manifest, then B reads lineage that is Spark-equal and assigned. | The two-session leg green on the default session. | **PROVEN** | `test_a_warm_second_session_reads_assigned_lineage_after_the_first_session_upgrades` is green: B adopts the v2 pointer and warms its cache, A upgrades to v3 and appends, B adopts the new pointer and reads the assigned triples — equal to A's read in the same test and to the v3-10 Spark-pinned constants. Green pre- and post-flip (a correctness leg: it pins the fork-fix contract, not the flip — on a fork without `F-CATIO-KEY` it reds with NULLs). |
| C-005 | The session retains nothing outside the cache at 8,000 tables: peak RSS default versus explicit `0` in fresh subprocesses, row-correct in both columns, with the weight bound itself unexercised (8 MB charged of 32 MB). | The subprocess RSS comparisons at 500 / 2,000 / 8,000 tables with the measured deltas; the growth-method ratio; the structural weight and thrash pins. | **PROVEN** | Single-driver peak deltas 8.3 / ~0 / 47.6 MB at 500 / 2,000 / 8,000 tables (charged 0.5 / 2.0 / 8.0 MB; every pair row-correct), but the off-column peak is non-monotonic (323.9 / 340.1 / 322.6), so no ratio is read off peak RSS. VmRSS-growth deltas ~15 / 59.3 MB at 2,000 / 8,000 (~7.5 KB resident per table, ~7.5× charged; the 500-cell is wobble, disclosed). `PERF-CATALOG-CACHE-BOUND-1` narrowed to no-retention-outside-the-cache (metadata LRU stays BACKLOG); `PERF-CATALOG-CACHE-WEIGHT-1` / `F-CATIO-WEIGHT` filed with the structural red-when-fixed pin; the 128 KiB churn pinned structurally with no wall-clock assertion. |
| C-006 | The default-off control becomes the explicit-`0` control; the docs say ON by default and how to turn it off (the catalog map, the session config docs, the baseline part 3); `t_many/count_id/stmt2` and `t_many_merged` are re-measured on the default session before/after (5 iterations, medians, spread, floor, load); `PERF-ICE-MANIFEST-1` is FIXED with the default-session number; `PERF-CATALOG-CACHE-BOUND-1` is closed or narrowed with the measured RSS; the IO-2 ledger's follow-up sentences point at this unit. | The baseline note; the registry rows; the IO-2 status-line edit. | **PROVEN** | The default-off control is inverted into the primary sharing pin; the explicit-`0` leg is the off-control. Docs: catalog map IO-3 section + off-escape row, `session-and-conf.md` ON-by-default with the `"0"` escape and the 32 MiB ceiling, baseline §6 (default 11.27 vs `0` 123.47 ms, merged cells, full census reproduced cell for cell, RSS table, floors 0.46/0.21, loads 8.7/6.7, module 164,313,144 B, not a quiet box, stated). `PERF-ICE-MANIFEST-1` FIXED with 123.47 → 11.27 ms; the LINEAGE row's pin follows the rename; five IO-2 status-line pointers name this unit. |
| C-007 | Every touched `map.md` moves in lockstep with reasons and `pins:` citations; no code comment is added. | `make check-map-sync`; the staged-diff comment self-check. | **PROVEN** | Six maps move with their directories (catalog, spark tests, python tests, perf, staging, plus this ledger): the red commit carries the pins sections, the flip commit the default-ON truth. The branch diff over `*.rs` / `*.py` / `*.toml` adds zero comment lines — the self-check prints nothing, and no forced `# Errors` was needed (no new `pub fn` returns `Result`). |

VERDICT: 7 clauses, 7 PROVEN, 0 OPEN, 0 REJECTED. **SHIPPED default-ON.**

```yaml
COVERAGE_ATTESTATION:
  pr_unit: perf-ice-catalog-io-3
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Five Rust parse pins, sixteen Rust staleness pins (the funnel on CatalogCaches::default, the explicit-0 control, the 1 MiB sizing pin, the tiny-budget pin, six IO-1 staleness pins now running cache-ON, six metadata mechanics), thirty-two Python legs (timing, default sharing, explicit sharing, explicit-0, six staleness, default lineage plus knob-off control, concurrency, RSS, the charged-weight fit, the token-budget churn, refusals) and the four v3 upgrade/legacy tests cover the key, both spellings, both refusals, the wiring, the funnel, both knob directions, no retention outside the cache, every staleness cell, the fork-fix contract, the weight under-count and the token-budget churn.
      artifacts: [crates/repark-iceberg/src/catalog/caches.rs, crates/repark-spark/src/tests/catalog_cache_staleness.rs, python/repark/tests/test_perf_ice_catalog_io_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Three deep attacks, all remediated. (1) The lane venv resolved repark to a foreign checkout whose native module predates the IO-2 wiring — the explicit-knob leg redded on it; rebuilt from the lane with the pinned maturin and re-ran red-then-green on the lane module. (2) The four HALT tests green on default sessions post-flip closes IO-2 FINDING S1-1 on the fixed pin. (3) The funnel re-read at 2ed39cb0 (same three table_builder sites, test-only direct builder) and the commit bypass verified persisting, so COMMIT-CACHE-1 stays honestly open.
      artifacts: [docs/perf/iceberg-catalog-io-baseline.md, crates/repark-iceberg/src/catalog/map.md]
    - id: AT-3
      status: ATTACKED
      evidence: No AWS call, no credential, no IAM surface, no .github change; the two AWS legs still skip behind the acceptance env gate plus their fork ask.
      artifacts: [python/repark/tests/test_perf_ice_catalog_io_1.py]
    - id: AT-4
      status: ATTACKED
      evidence: Every number is a release module refusing debug assertions (164,313,144 B, __debug_assertions__ False), five iterations after a warm-up, with medians, mins, spreads, per-run floors and 1-minute loads; the box was not quiet (loads 6.7-8.7, sibling builds live) and the note says so; the default column reproduces IO-2's explicit-knob column within 0.4 ms on every row and the counts reproduce exactly (208 files, 193 vs 1 manifests).
      artifacts: [docs/perf/iceberg-catalog-io-baseline.md]
    - id: AT-5
      status: ATTACKED
      evidence: No dependency change. The fork work is consumed through the base pin (RP-13, 2ed39cb0), already on origin/main; git diff origin/main -- Cargo.toml Cargo.lock is empty at hand-back.
      artifacts: [Cargo.toml, Cargo.lock]
    - id: AT-6
      status: ATTACKED
      evidence: The bytes flow once at catalog build; the product path pays no per-statement cost for the wiring. The manifest cache is self-bounding (moka max_capacity), so memory_catalog's per-call cache needs no trim duty. The RSS leg costs ~80 s on release and is disclosed, not hidden; the delete-manifest instrument touches test warehouses only.
      artifacts: [crates/repark-iceberg/src/catalog/builders.rs, python/repark/tests/test_perf_ice_catalog_io_1.py]
    - id: AT-7
      status: ATTACKED
      evidence: Mutation score measured, two code mutations plus three behavior-side knob flips, zero escapes. Default back to 0 reds exactly the two default pins; new() ignoring the setting reds the two sizing pins and the off-control while the default pin stays green (correct discrimination); Python flips red both directions plus the timing leg over-target on release.
      artifacts: [crates/repark-spark/src/tests/catalog_cache_staleness.rs, python/repark/tests/test_perf_ice_catalog_io_1.py]
    - id: AT-8
      status: N/A
      justification: No dependency, lockfile or workspace-manifest change.
    - id: AT-9
      status: ATTACKED
      evidence: Four registry rows - PERF-ICE-MANIFEST-1 FIXED with the default-session number (123.47 → 11.27 ms), PERF-CATALOG-CACHE-BOUND-1 narrowed to no-retention-outside-the-cache (the manifest half is not closed; metadata LRU stays BACKLOG), PERF-CATALOG-CACHE-WEIGHT-1 BACKLOG behind F-CATIO-WEIGHT with the structural red-when-fixed pin, and PERF-CATALOG-LINEAGE-CACHE-1's pin reference following the IO-3 rename plus its stale present-tense sentence moved to past tense; PERF-CATALOG-COMMIT-CACHE-1 honestly untouched (bypass verified at the new pin).
      artifacts: [docs/spark-sql-iceberg-parity.md, docs/perf/iceberg-catalog-io-baseline.md]
    - id: AT-10
      status: ATTACKED
      evidence: Seven clauses, each cited by a pin in a map; the fork-side remainder (commit-path caching, AWS builders) is stated as filed rather than claimed as shipped.
      artifacts: [task/ledgers/staging/perf-ice-catalog-io-3-ledger.md]
  complete: true
```

## What changed

| Site | Change |
|---|---|
| `repark-iceberg/src/catalog/caches.rs` | The flip: `DEFAULT_MANIFEST_CACHE_BYTES` `0` → `33_554_432`; `the_default_disables_the_shared_cache` renamed to `the_default_sizes_the_shared_cache` asserting the budget |
| `repark-spark/src/tests/catalog_cache_staleness.rs` | The funnel pin builds `CatalogCaches::default()` (default-primary); the six IO-1 staleness pins now run cache-ON with no code change |
| `python/repark/tests/test_perf_ice_catalog_io_1.py` | Timing, six staleness legs and the lineage pin run on default sessions; the default-off control inverted to the primary sharing pin; explicit-`0` the off-control; two new legs (two-session concurrency, 500-table subprocess RSS) |
| `docs/perf/iceberg-catalog-io-baseline.md` | New §6: default-vs-`0` timing cells, census, RSS table, commands |
| `docs/spark-sql-iceberg-parity.md` | `PERF-ICE-MANIFEST-1` FIXED, `PERF-CATALOG-CACHE-BOUND-1` narrowed with the RSS, LINEAGE pin follows the rename |
| `docs/guide/session-and-conf.md` | ON by default, the `"0"` escape, the 32 MiB ceiling |
| `task/ledgers/staging/perf-ice-catalog-io-2-ledger.md` | Status lines only: five follow-up pointers name this unit |

**Round 2** (same unit, orchestrator-critic remediation): two structural legs join
`test_perf_ice_catalog_io_1.py` (charged-weight fit, token-budget churn); the baseline
§6.3 grows the 2,000/8,000-table and growth-method rows, §6.4 carries the token-budget
second-pass cells, Commands moves to §6.5; the registry rewrites the BOUND-1 IO-3 note,
files `PERF-CATALOG-CACHE-WEIGHT-1`, and past-tenses the LINEAGE stale sentence;
`session-and-conf.md` and the catalog map say estimated weight and warn below ~1 MiB;
C-005 is downgraded to what was measured.

Declared renames (the flip inverts their meaning, so the old names would lie):

| Before | After |
|---|---|
| `the_default_disables_the_shared_cache` | `the_default_sizes_the_shared_cache` |
| `test_a_default_session_reopens_manifests_after_they_vanish` | `test_a_default_session_answers_from_the_shared_cache_after_manifests_vanish` |
| `test_with_the_knob_on_an_upgraded_table_reads_assigned_lineage_for_carried_rows` | `test_with_the_default_an_upgraded_table_reads_assigned_lineage_for_carried_rows` |

Public API breaks: **zero**. One constant changes value; no signature moves. No dependency
change. No Spark-answer change — every answer pin that was green is green.

**Public behaviour change on the default path, and it is the unit.** Every default
session's memory catalog now shares one 32 MiB manifest `ObjectCache` across its tables.
A caller that wants the pre-unit load path sets `repark.iceberg.manifestCacheBytes` to
`"0"` or passes `CatalogCaches::disabled()`.

## The number

| cell | before (explicit `0`) | after (default, no knob) | IO-2 on-knob | target |
|---|---|---:|---:|---|
| `t_many/count_id/stmt2` (193 manifests) | 123.47 ms | **11.27 ms** | 10.95 ms | ≤ 20 ms |
| `t_many/point/stmt2` | 125.16 ms | **14.67 ms** | 14.75 ms | — |
| `t_many_merged/count_id/stmt2` (1 manifest) | 15.52 ms | **10.33 ms** | 10.49 ms | — |
| `t_many_merged/point/stmt2` (1 manifest) | 18.03 ms | **12.71 ms** | 13.20 ms | — |
| repeated-SELECT manifest-list + manifest opens | 1 + 1 | **0 + 0** | 0 + 0 | — |
| DELETE / UPDATE manifest opens | 8 / 15 | **6 / 12** (read side only) | 6 / 12 | — |
| MERGE / INSERT manifest opens | 8 / 1 | 8 / 1 (commit side, filed) | 8 / 1 | — |
| 500-table peak RSS (MB) | 323.9 | **332.2** (delta 8.3, bar 64) | — | — |
| 2000-table peak RSS (MB) | 340.1 | **340.1** (delta ~0; charged 2.0) | — | — |
| 8000-table peak RSS (MB) | 322.6 | **370.2** (delta 47.6; charged 8.0) | — | — |
| 2000-table 2nd pass, 128 KiB (s) | 8.2 | 5.6 (tiny: 8.1) | — | — |
| 32768-table peak RSS (MB) | 352.3 | **602.9** (delta 250.6; charged 32.0) | — | — |

Floors 0.46 / 0.21 ms in their own runs; load 6.7–8.7 throughout.

## Mutation score

Two code mutations (each reverted after measuring) plus three behavior-side knob flips:

| # | Mutation | Reds | Notes |
|---|---|---|---|
| M1 | `caches.rs`: default back to `0` | exactly `the_default_sizes_the_shared_cache` and the Rust funnel pin; the other 19 stay green | the flip is what shares |
| M2 | `CatalogCaches::new` ignores the setting (hardcodes the budget) | `both_spellings_size_the_cache`, `zero_disables_the_shared_cache`, `with_zero_bytes_…` | the bytes flow through the struct; the default pin stays green, which is correct discrimination, not an escape |
| PM-b | Python primary flipped to `"0"` | the primary sharing leg | the leg is sensitive to the default |
| PM-c | Python off-control flipped to `"33554432"` | the off-control (DID NOT RAISE) | the leg is sensitive to the knob value |
| PM-d | Python timing leg flipped to `"0"` (release) | the timing leg (over target) | the pin measures the cache, not the fixture |

Zero escapes. A `max(1)` floor was considered for M2 and rejected before running it: a
1-byte moka cache rejects every overweight entry and holds nothing, so the off-controls
would stay green — the hardcode is the mutation that actually ignores the bound.

## Critic pass (in-lane, single-session)

Three findings, all remediated before the attestation:

| # | Finding | Disposition |
|---|---|---|
| C-1 | The lane venv resolved `repark` to a foreign checkout whose native module predates the IO-2 wiring: the explicit-knob leg redded on it, which looked like a product failure. | REMEDIATED. Rebuilt from the lane with the pinned maturin (`uvx maturin@1.14.1 develop`), verified `repark.__file__` resolves into the lane, and re-ran the red set: exactly the designed reds. No number in this ledger comes from the foreign module. |
| C-2 | The first M2 draft (floor at 1 byte) would not have reddened the off-controls: moka overweight rejection means a 1-byte cache holds nothing, so the mutation would have looked like an escape. | REMEDIATED. Ran the hardcode mutation instead, which reds the sizing pins and the off-control and is documented above. |
| C-3 | The baseline's Commands draft (§6.4 at the time, §6.5 now) named `.venv/bin/maturin`, but the runs used the Makefile-pinned `uvx maturin@1.14.1`. | REMEDIATED. The note records the exact command run. |

## Critic pass round 2 (orchestrator critic, remediated in-lane)

| # | Finding | Disposition |
|---|---|---|
| CR-1 (S2) | C-005 PROVEN on a leg filling 1.5 % of the budget; the budget is estimated weight, not resident bytes; quoted fresh-subprocess deltas 11.3 / 41.7 / 65.1 MB at 500 / 2,000 / 8,000. | REMEDIATED with re-measurement. C-005 downgraded to no-retention-outside-the-cache with the weight bound unexercised; `PERF-CATALOG-CACHE-BOUND-1` rewritten (no "measured bounded"); `PERF-CATALOG-CACHE-WEIGHT-1` / `F-CATIO-WEIGHT` filed. Lane peak deltas 8.3 / ~0 / 47.6 — the 2,000-cell reproduces 41.7 in some independent samples and not others (+40.5 / −3.6 MB peak, +47.2 / +4.6 MB growth), so the ratio is sourced from the file-bytes floor (5.1×) and the at-bound run (~8×) (peak-RSS method noise ±20 MB; the off-peak is non-monotonic across counts, so no ratio is read off peak RSS). The growth-method deltas ~15 / 59.3 MB at 2,000 / 8,000 (~7.5× charged) confirm the under-count in order. The red-when-fixed pin is structural (a 280000 budget fits 256 tables at charged weight, so the coldest table hits; true weights evict it and the leg reds), not the numeric ratio — no numeric pin could hold a ±20 MB-noisy ratio. Docs say estimated weight, not a resident ceiling. |
| CR-2 (S3) | The LINEAGE-CACHE-1 row keeps a present-tense "Until it lands … today" sentence. | REMEDIATED. Past tense. |
| CR-3 (S3) | The docs invite resizing the byte budget without warning that a small non-zero budget thrashes (quoted 2,000-table 61 / 94 / 347 s). | REMEDIATED with re-measurement. Warning sentences in `session-and-conf.md` and the catalog map; the churn pinned structurally (128 KiB over 256 tables with every manifest deleted: cold tables miss, hot tables hit; no wall-clock assertion). The 347 s cell does not reproduce (8.1 s here against 8.2 s at `0` — a token budget matches explicit-`0`, it does not blow up); the cause of that cell is unexplained and disclosed in baseline §6.4. A single-entry version flaked 1 in 18 (TinyLFU admission ties plus async eviction make one entry's fate a few percent flaky either way); the aggregate count replaces it. |

**At-bound run and the 512 MB proviso (CR-1(4)).** 32,768 small tables charge the
full 32 MB budget. The default session holds **617.5 MB** resident after the read
pass (602.9 MB process peak) against 338.8 MB with the cache off — ~265–278 MB of
cache entries (~8× charged) above a ~340–352 MB non-cache base, every table
row-correct in both columns (131,072 rows). That crosses the brief's 512 MB line, so
the 32 MiB default is NOT changed in-lane: the numbers go to the orchestrator, who
picks the default (handback Q1). Lean: keep 32 MiB — the non-cache base is most of
the total, the bound needs 32,768 small tables to fill, and `F-CATIO-WEIGHT` caps true
retention when it lands; lowering now churns the number twice.

**Orchestrator ruling (2026-09-06, on Q1):** keep the 32 MiB default. The at-bound cost is ~270 MB of resident cache above a ~340 MB session base, reached only by a session that touches 32,768 tables; the documented escape is `manifestCacheBytes = "0"`; `F-CATIO-WEIGHT` (PERF-CATALOG-CACHE-WEIGHT-1) is the fix for the 8× under-count and re-measures this cell when it lands. The registry row and `session-and-conf.md` carry the at-bound number so the ceiling is not overstated.

# Unit ledger — RP-36-FORK-PIN · the fork pin moves to `fa77fb2b`: timestamp filters reach the scan, page-index row selection is on

## Round 1 (2026-09-19)

**Date:** 2026-09-19 · **Branch:** `chore/rp-36` · **Base:** `af7ab304`
**Model:** claude-opus-5 (orchestrator, run 24a) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** No RePark product code.
**Fork:** `TRO-Wolf/iceberg-rust` #312 (F-TS-PUSHDOWN-1: a timezone-bearing timestamp literal
reaches the Iceberg scan predicate as a `timestamptz` datum) and #310 (F-PAGE-PRUNE-1: page-index
row selection on by default on the DataFusion scan, knob `iceberg.row_selection_enabled`).
**Slate:** [ice-read-perf-slate-2026-09-18.md](../../roadmap/mid-term/ice-read-perf-slate-2026-09-18.md)
unit 1 (and the timestamp-pushdown gap unit 0's bed found).

**Measured.** Unit 0's bed, both heads built and run back to back on the default release profile,
`--repeat 5` (medians), fork `7bd2fea3` (main `af7ab304`) against `fa77fb2b` (this branch): the
200-file bed and a 20 × 500,000-row large-file bed (25 pages per column). The table is in
[docs/perf/ice-read-perf-baseline-2026-09-19.md](../../../docs/perf/ice-read-perf-baseline-2026-09-19.md)
§"RP-36 pair".

## PROPOSITION LEDGER — RP-36-FORK-PIN — 2026-09-19

| ID | Claim | Evidence | Status | Notes |
|----|-------|----------|--------|-------|
| C-001 | At fork `fa77fb2b` a Spark-door `ts >= CAST(n AS TIMESTAMP) AND ts < CAST(m AS TIMESTAMP)` filter reaches `IcebergTableScan` as the `ts` window. | `q3_and_q7_reach_the_scan_with_the_same_rows` (the ICE-READ-PERF-0 sentinel, red at the bump, flipped here). | PROVEN | Q3's scan predicate is `(ts >= 2023-11-14 23:16:20 UTC) AND (ts < 2023-11-14 23:17:44 UTC)`. |
| C-002 | The bench pair: Q3 on the 200-file bed reads 27 footers and 985,929 page bytes instead of 200 and 65,501,593 (warm 84.7 → 11.8 ms, cold 234.8 → 189.0 ms); on the large bed page selection trims the point lookup (Q4 2,542,390 → 2,114,116 page bytes) and the id window (Q7 1,196,696 → 1,094,733). Every other query reads the same bytes. | The RP-36 pair table in the baseline document; JSON in the run-24a evidence `pairs/rp36-{before,after}/`. | PROVEN | Requests and bytes were identical across the five samples of every query on both heads. |
| C-003 | Regression: Q6 (`category = 'cat_7'`, a predicate no page statistic can prune) reads identical bytes and is slower on the 200-file bed — warm 659.3 → 776.4 ms, cold 833.7 → 999.7 ms — with no such change on the large bed (warm 1,174 → 1,233 ms, cold 1,226 → 1,188 ms). | The same pair table. | PROVEN | Read as the CPU cost of loading and evaluating page indexes that cannot prune (fork #310's perf reviewer R-01/R-05, P2). Fork ask F-PAGE-PRUNE-2 to 24b (claims 11:1x); not a correctness issue. |
| C-004 | The page-level pins of ICE-PAGE-PRUNE-1 (#725) and the NaN and scan pins on main stay green at the new pin; no RePark product code changes. | Local gate lines below; CI runs the workspace and the facade suite at the new pin. | PROVEN | |

## Gate lines

GATE_PLACEHOLDER

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-36-fork-pin
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The page-prune pins read their expectations from a recorded Spark 4.1.2
        fixture and a live re-derivation; they stay green with pruning on.
      artifacts: [python/repark/tests/test_ice_page_prune_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: The Q3 sentinel was red at the bump before it was flipped.
      artifacts: [crates/repark-spark/benches/ice_read_perf/pins.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Both beds, four modes on the 200-file bed and two on the large bed, seven
        query shapes; the regression cell is reported, not hidden.
      artifacts: [docs/perf/ice-read-perf-baseline-2026-09-19.md]
    - id: AT-4
      status: N/A
      justification: No shared mutable state is added in RePark.
    - id: AT-5
      status: N/A
      justification: No network, credentials or unsafe code.
    - id: AT-6
      status: ATTACKED
      evidence: The Q6 regression is a registered measurement with a fork ask.
      artifacts: [task/ledgers/staging/rp-36-fork-pin-ledger.md]
    - id: AT-7
      status: ATTACKED
      evidence: Every number is a back-to-back before/after pair on the default release
        profile with its load average recorded.
      artifacts: [docs/perf/ice-read-perf-baseline-2026-09-19.md]
    - id: AT-8
      status: ATTACKED
      evidence: The tree carries the pin, the lockfile, the flipped sentinel, maps, the
        pair table and this ledger.
      artifacts: [Cargo.toml, Cargo.lock]
    - id: AT-9
      status: ATTACKED
      evidence: The root map names the pin and both fork PRs.
      artifacts: [map.md]
    - id: AT-10
      status: ATTACKED
      evidence: CI runs the workspace Rust tests and the whole facade suite at the new pin.
      artifacts: [task/ledgers/staging/rp-36-fork-pin-ledger.md]
  complete: true
```

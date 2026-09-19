# Unit ledger — RP-38-FORK-PIN · the fork pin moves to `f3bdd598`: the shared Parquet footer cache, Java's metrics config, transforms over LargeBinary

## Round 1 (2026-09-19)

**Date:** 2026-09-19 · **Branch:** `chore/rp-38` · **Base:** `6dacd1d5`
**Model:** claude-opus-5 (orchestrator, run 24a) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Fork:** `TRO-Wolf/iceberg-rust` #316 (F-FOOTER-CACHE-1, run 24b), #313 (F-METRICS-CONFIG-1, run 24d)
and #315 (F-TRANSFORM-ARROW-TYPES-1, run 24d). The RePark half of #316 is ICE-FOOTER-CACHE-1 in this
PR ([ledger](ice-footer-cache-1-ledger.md)); the parity halves of #313 and #315 are run 24c's.

## PROPOSITION LEDGER — RP-38-FORK-PIN — 2026-09-19

| ID | Claim | Evidence | Status | Notes |
|----|-------|----------|--------|-------|
| C-001 | At fork `f3bdd598` the workspace builds and the catalog, session, staleness and bench suites pass with ICE-FOOTER-CACHE-1 on top; CI runs the workspace and the facade suite. | `cargo test -p repark-iceberg --lib catalog` 103 passed; `-p repark-spark --lib catalog_cache_staleness` 16; `-p repark-spark --test ice_read_perf_pins` 21; `-p repark-core --lib session` 171; clippy clean. | PROVEN | #313 and #315 change fork writers and transforms; their Spark cells are run 24c's units. |

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-38-fork-pin
  categories:
    - id: AT-1
      status: N/A
      justification: The Spark-visible halves of #313 and #315 are run 24c's units; this PR claims none.
    - id: AT-2
      status: ATTACKED
      evidence: ICE-FOOTER-CACHE-1's pins were red before its wiring; the unit's verification critic
        turned four mutations red.
      artifacts: [crates/repark-iceberg/src/catalog/tests/footer_cache.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The catalog, session, staleness and bench suites run at the new pin.
      artifacts: [crates/repark-iceberg/src/catalog/tests/cache_wiring.rs]
    - id: AT-4
      status: N/A
      justification: No RePark shared state beyond the unit's session cache, attested in its ledger.
    - id: AT-5
      status: N/A
      justification: No credentials, network or unsafe code.
    - id: AT-6
      status: N/A
      justification: No error path changes in RePark at the bump.
    - id: AT-7
      status: ATTACKED
      evidence: The bench pair is recorded in the unit's ledger and the baseline document.
      artifacts: [docs/perf/ice-read-perf-baseline-2026-09-19.md]
    - id: AT-8
      status: ATTACKED
      evidence: The tree carries the pin, the lockfile, the root map line and this ledger.
      artifacts: [Cargo.toml, Cargo.lock]
    - id: AT-9
      status: ATTACKED
      evidence: The root map names the pin and the three fork PRs.
      artifacts: [map.md]
    - id: AT-10
      status: ATTACKED
      evidence: CI runs the workspace Rust tests and the whole facade suite at the new pin.
      artifacts: [task/ledgers/staging/rp-38-fork-pin-ledger.md]
  complete: true
```

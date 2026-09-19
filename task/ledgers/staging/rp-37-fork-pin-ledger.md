# Unit ledger — RP-37-FORK-PIN · the fork pin moves to `27e0d5fa`: catalog cache handles for Glue and S3 Tables, the metadata-only DELETE decision

## Round 1 (2026-09-19)

**Date:** 2026-09-19 · **Branch:** `chore/rp-37` · **Base:** `2c232c59`
**Model:** claude-opus-5 (orchestrator, run 24a) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** No RePark product code.
**Fork:** `TRO-Wolf/iceberg-rust` #309 (F-META-DELETE-1, run 24d: `Table::can_delete_using_metadata`)
and #311 (F-CATALOG-CACHE-1, run 24b: a scoped, byte-bounded `TableMetadataCache` with single-flight
loads; `with_table_metadata_cache` / `with_shared_object_cache_bytes` /
`with_cache_credential_context` on the S3 Tables and Glue builders, default off).
**Slate:** [ice-read-perf-slate-2026-09-18.md](../../roadmap/mid-term/ice-read-perf-slate-2026-09-18.md)
unit 2 (fork half). The RePark half, ICE-CATALOG-CACHE-1, passes the session's caches into the AWS
builders in its own PR and carries the before/after pair.

## PROPOSITION LEDGER — RP-37-FORK-PIN — 2026-09-19

| ID | Claim | Evidence | Status | Notes |
|----|-------|----------|--------|-------|
| C-001 | At fork `27e0d5fa` the workspace builds unchanged and the catalog, session and bench suites pass: #311's AWS cache handles default off and #309 adds an API RePark does not call yet, so no RePark answer changes. | `cargo check` of repark-iceberg, repark-core, repark-spark, repark-python, repark-sql; `cargo test -p repark-iceberg --lib catalog` 82 passed; `-p repark-core --lib session` 163 passed; `-p repark-spark --test ice_read_perf_pins` 17 passed. CI runs the workspace and the facade suite. | PROVEN | The memory catalog's metadata cache is now #311's byte-bounded cache; the ICE-READ-PERF-0 counting pins (a second load reads fewer metadata documents) hold on it. |

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-37-fork-pin
  categories:
    - id: AT-1
      status: N/A
      justification: No Spark-visible answer changes at this pin; the parity halves are separate units.
    - id: AT-2
      status: ATTACKED
      evidence: The counting pins of ICE-READ-PERF-0 assert metadata-cache behaviour on the
        memory catalog and pass on the new cache.
      artifacts: [crates/repark-iceberg/src/catalog/tests/io_stats.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The catalog, session and bench suites run at the new pin.
      artifacts: [crates/repark-iceberg/src/catalog/tests/io_stats.rs]
    - id: AT-4
      status: N/A
      justification: No RePark shared state changes.
    - id: AT-5
      status: N/A
      justification: No credentials, network or unsafe code in RePark.
    - id: AT-6
      status: N/A
      justification: No error path changes in RePark.
    - id: AT-7
      status: N/A
      justification: No performance claim at this pin; ICE-CATALOG-CACHE-1 carries the pair.
    - id: AT-8
      status: ATTACKED
      evidence: The tree carries the pin, the lockfile, the root map line and this ledger.
      artifacts: [Cargo.toml, Cargo.lock]
    - id: AT-9
      status: ATTACKED
      evidence: The root map names the pin and both fork PRs.
      artifacts: [map.md]
    - id: AT-10
      status: ATTACKED
      evidence: CI runs the workspace Rust tests and the whole facade suite at the new pin.
      artifacts: [task/ledgers/staging/rp-37-fork-pin-ledger.md]
  complete: true
```

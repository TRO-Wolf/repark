# Unit ledger — WO AWS-ACCEPT-REPLACE-1 · the staged replace writes each metadata file once

**Date:** 2026-09-27 · **Branch:** `chore/rp-55-staged-single-write` · **Base:** `9392dbc3`
(`origin/main`) **Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** The nightly AWS acceptance `test_create_or_replace_twice_against_s3tables` has
failed since 2026-09-19 with S3 412 `ConditionNotMatch`. Diagnosis (the `aws-accept-replace-1`
hand-back): since fork RP-26, `StagedTableTransaction::begin_replace` eagerly PUTs
`metadata/00002-<uuid>.metadata.json` and the commit rewrites the SAME key in place; S3 table
buckets refuse the overwrite with 412 while Glue buckets and local stores tolerate it. The fork
fix merged as #362 (`6e937f49943efbc94a11878b9ff32084d142e8b2`): a staged replace writes its
metadata file once, at commit. This unit repins the fork to that rev (RP-55) and carries the
regression pin: a no-overwrite `Storage` wrapper plus a Spark-door OR REPLACE test that is red
at the old pin and green at the new one.

**What the fork fix changes (fork #362, F-STAGED-SINGLE-WRITE-1).** `begin_replace` no longer
eagerly writes non-Hadoop staged metadata; it computes the `N+1`/fresh-uuid staged target, holds
the metadata in memory, and writes the final bytes once at commit. Non-empty replace: one PUT
plus the pointer CAS. Hadoop behaviour is byte-identical.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | A CREATE OR REPLACE on a service-managed catalog writes each metadata file once and never rewrites it. | The Spark-door pin seeds a CTAS then runs CREATE OR REPLACE on the existing table over the no-overwrite store, expecting Ok, 1 row, and ops [append, overwrite]. | PROVEN | `replace_existing_table_writes_each_metadata_file_once`: RED at fork `0d3f2b4f` (`Unexpected`, second write of `metadata/00002-<uuid>.metadata.json` refused), GREEN at fork `6e937f49` (1 passed, 0 failed). |

## Mutation record (2026-09-27)

Each line was broken, the named tests ran, and the file was restored.

| # | Mutation | Red |
|---|---|---|
| M1 | Hold the fork pin at `0d3f2b4f` with the new test in place | `replace_existing_table_writes_each_metadata_file_once` reds on the in-place rewrite of `00002-<uuid>.metadata.json`; repinned to `6e937f49`, green |

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: aws-accept-replace-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The pin walks the failing AWS shape locally (seed CTAS, then OR REPLACE on the existing service-managed table) and asserts the commit, the row count and the snapshot ops.
      artifacts: [crates/repark-spark/src/tests/service_managed_ctas.rs]
    - id: AT-2
      status: ATTACKED
      evidence: The no-overwrite store refuses every second write of one path, so any rewrite of any metadata file reds the pin, not only the 00002 key.
      artifacts: [crates/repark-iceberg/src/catalog/no_overwrite_storage.rs, crates/repark-spark/src/tests/service_managed_ctas.rs]
    - id: AT-3
      status: N/A
      justification: No failure-path contract changes; the wrapper's refusal is a test seam, and the fork fix removes the rewrite rather than adding a refusal.
    - id: AT-4
      status: N/A
      justification: No commit, isolation or concurrency surface in RePark changes; the pointer CAS still serializes concurrent replaces fork-side.
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling; the lane never uses AWS credentials and never runs the acceptance module against AWS.
    - id: AT-6
      status: ATTACKED
      evidence: The lock delta is only the six fork crates' source lines; the ops pin ([append, overwrite]) guards the snapshot shape the repin commits.
      artifacts: [Cargo.lock, crates/repark-spark/src/tests/service_managed_ctas.rs]
    - id: AT-7
      status: N/A
      justification: No performance claim; one fewer metadata PUT per replace is the fork's measured effect, not pinned here.
    - id: AT-8
      status: ATTACKED
      evidence: No new dependency and no new crate edge; the wrapper reuses this crate's buffer-type dependency exactly as the counting store does, and the Spark door references it through the existing repark-iceberg edge.
      artifacts: [crates/repark-iceberg/src/catalog/no_overwrite_storage.rs, crates/repark-iceberg/src/catalog/mod.rs]
    - id: AT-9
      status: N/A
      justification: No failure path in scope; a rewritten file fails the pin with the refused path.
    - id: AT-10
      status: ATTACKED
      evidence: M1 holds the old pin and the pin reds before the repin restores green; the refusal is the exact AWS shape (00002-<uuid> second PUT).
      artifacts: [crates/repark-spark/src/tests/service_managed_ctas.rs]
  complete: true
```

## Residues

| # | Residue |
|---|---|
| R-1 | Dated 2026-09-27 (owner action, not run here): after this repin merges, the owner dispatches the live acceptance check `test_create_or_replace_twice_against_s3tables` to confirm the nightly goes green. This lane never uses AWS credentials and never runs the acceptance module against AWS. |

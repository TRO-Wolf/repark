# Rust catalog registration ledger

**Retires:** move this ledger to `../completed/` when the staged Actor unit passes its focused,
mutation, verify, and preflight acceptance gates and is ready for Critic review.

**Date:** 2026-08-27 · **Branch:** `codex/rust-production-hardening` ·
**Base:** `1587e48` (PR #250)

## Scope audit

| # | Clause | State | Evidence |
|---|---|---|---|
| C-001 | Concurrent same-name registrations can both pass the unlocked duplicate check. | PROVEN | `register_iceberg_catalog_with_policy` checked the registry before an awaited provider build. |
| C-002 | Interleaved provider and handle publication can leave different winners on the read and write surfaces. | PROVEN | DataFusion publication preceded the independent registry write with no shared linearization point. |
| C-003 | The existing regression test covers only sequential duplicate registration. | PROVEN | The former `register_iceberg_catalog_rejects_duplicate_name` awaited each call in sequence. |
| C-004 | Provider construction can remain concurrent while one registry guard makes duplicate validation and both publications atomic to the supported Session API. | PROVEN | `build_iceberg_catalog_provider` is public and all publication after it is synchronous. |

## Approved charter

Build each provider outside locks. Acquire the existing `CatalogRegistry` write lock after a
successful build. Under that lock, reject a duplicate name, register the matching DataFusion
provider, and insert the matching Iceberg handle and location policy without an await. Preserve
public APIs and duplicate error semantics. The raw `context()` hatch remains outside the contract.

## Acceptance

- [x] Two controlled same-name provider builds overlap; exactly one registration succeeds, the
  loser reports `already registered`, and the provider and handle identify the same winner.
- [x] Two controlled distinct-name provider builds overlap, proving provider construction is not
  globally serialized.
- [x] A controlled provider-build failure publishes neither a DataFusion provider nor an Iceberg
  handle.
- [x] A sequential duplicate rejects before building a controlled failing provider.
- [x] The focused race test is red on the base behavior and on a deliberate mutation that removes
  or moves the authoritative locked duplicate check before provider construction.
- [x] Focused tests, Rust format, clippy, panic, and file-size gates pass.
- [x] `make verify` and `make preflight` pass with no unstaged or untracked residue.

## Actor outcome

The focused filter passed five tests in 0.01 seconds. Removing the authoritative locked duplicate
check made the race pin fail with two successes instead of one. Distinct-name builds reached the
same barrier and completed inside the five-second deadlock bound. `session.rs` remains at its
1,479-line baseline, `session/tests.rs` ratchets from 1,485 to 1,461, and the new module is 229
lines. `make verify` and `make preflight` passed; the facade suite reported 3,731 passed and 71
skipped. Disk was 621 GiB free before focused builds and 619 GiB before broad validation.

## Critic findings

| Finding | Severity | Disposition |
|---|---|---|
| Q-001 — Sequential duplicates built and snapshotted the losing provider before rejection, changing the error and cold cost. | S1 | REMEDIATED — restore the optimistic check and retain the locked authoritative check; removing the optimistic check makes `register_iceberg_catalog_rejects_duplicate_name` red. |
| Q-002 — The optimistic probe constructed an unknown-catalog error for every absent name, and the duplicate message allocated before every successful registration. | S2 | REMEDIATED — probe the registry directly under a brief read guard and construct the duplicate error through a lazy closure on rejection only. |
| Q-003 — Duplicate tests accepted any message containing `already registered`, so they did not pin the shared rejection contract. | S2 | REMEDIATED — both sequential and concurrent paths now match the exact `Error::DataFusion` payload; changing the payload makes the focused pin red. |
| Q-004 — The new `session/tests/` directory had no `map.md`, so the commit hook rejected the staged unit. | S1 | REMEDIATED — add the directory map, point the parent map to it, and rerun the map and commit-hook gates. |
| CL-001 — The Actor attestation classified a lock and multi-step publication change as standard risk. | S2 | REMEDIATED — the attestation now records high risk. |
| CL-002 — The Actor line count became stale after the Q-001 regression pin. | S2 | REMEDIATED — the final count is recorded after the Critic patch. |
| CL-003 — The draft retained both the Actor and final coverage blocks, which violated the one-block ledger grammar. | S2 | REMEDIATED — remove the superseded Actor block; the final high-risk attestation is the only coverage block. |

Fresh final validation passed `make verify` and `make preflight`. The facade suite reported 3,731
passed and 71 skipped. The security, dependency, license, source, workflow, and Python vulnerability
gates passed.

## Final Critic coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rust-catalog-registration
  cycle: final
  risk_tier: high
  critic_engine: ccc
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-001 through C-004 were traced through direct, memory, configured, and cloned-session registration paths.
      artifacts: [register_iceberg_catalog_with_policy, catalog_registration.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Existing, absent, failing, same-name, and distinct-name candidates were exercised.
      artifacts: [register_iceberg_catalog_rejects_duplicate_name, provider_build_failure_publishes_neither_surface]
    - id: AT-3
      status: ATTACKED
      evidence: Provider-build errors and cancellation before publication leave both registries unchanged; no await or fallible call remains between supported publications.
      artifacts: [provider_build_failure_publishes_neither_surface, session.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Same-name, distinct-name, session-clone, poison recovery, cancellation, and RePark-to-DataFusion lock order were traced; no supported opposite-order path exists.
      artifacts: [concurrent_same_name_registration_publishes_one_matching_winner, distinct_name_provider_builds_overlap, critic novel clone-race run]
    - id: AT-5
      status: N/A
      justification: The diff adds no credential, path, injection, deserialization, unsafe, or outward-facing surface.
    - id: AT-6
      status: ATTACKED
      evidence: Winner handle identity and provider namespace agree after every supported same-name interleaving.
      artifacts: [concurrent_same_name_registration_publishes_one_matching_winner, critic novel clone-race run]
    - id: AT-7
      status: ATTACKED
      evidence: Provider construction remains concurrent for distinct names; duplicate provider construction is skipped on the sequential hot path.
      artifacts: [distinct_name_provider_builds_overlap, register_iceberg_catalog_rejects_duplicate_name]
    - id: AT-8
      status: ATTACKED
      evidence: DataFusion 54.1 register_catalog replacement and synchronization behavior were read from the pinned source; raw context mutation remains outside the documented contract.
      artifacts: [datafusion execution/context/mod.rs, datafusion-catalog catalog.rs, ReparkSession::context]
    - id: AT-9
      status: ATTACKED
      evidence: Both duplicate branches retain one exact DataFusion error payload; provider-build failures remain distinct and publish no supported partial state.
      artifacts: [register_iceberg_catalog_rejects_duplicate_name, concurrent_same_name_registration_publishes_one_matching_winner, exact-message mutation]
    - id: AT-10
      status: ATTACKED
      evidence: Removing either the optimistic or authoritative locked check independently makes its focused regression red.
      artifacts: [two Critic mutation runs, focused five-test run]
  reattested: [AT-1, AT-2, AT-3, AT-4, AT-6, AT-7, AT-8, AT-9, AT-10]
```

## Deferred charters

- CSV null-value pre-scan must honor configured quote and comment options and fail loud on listing
  errors; this belongs to a reader-hardening unit.
- K-means initialization has an O(k²) selected-index membership path; preserve seeded output in a
  measured ML performance unit.
- Regex kernels retain batch-cardinality compiled patterns and materialize all captures; benchmark
  cardinality and allocation changes in a standalone function-kernel unit.
- Manifest maintenance maps corrupt negative lengths and malformed target sizes to fallbacks;
  change that sensitive error contract in its own maintenance charter.
- Time-travel pinned-view cleanup on future cancellation remains OPEN pending an owner scope ruling
  for server-side cancellation.

## Scope boundary

No owned-fork change, dependency patch, workflow change, AWS write, SQL semantic change, broad
architecture change, commit, push, or PR mutation belongs to this unit.

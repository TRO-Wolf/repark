# Rust-first placement decision plan — 2026-09-07

Class: ledger. Retires from staging in this documentation unit's departure commit;
the next pickup archives it after merge. Base: `0bfaf540` on PR #423.

## Scope and authorization

The owner proposed guidance that modules should use Rust wherever possible, for agents
and the roadmap. This unit adds that direction to the existing planning PR for review.
It changes no authoritative contract, skill, model route, runtime implementation, or gate.
The existing request to update PR #423 authorizes this documentation commit and push.

## PROPOSITION LEDGER

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | One proposed rule makes Rust the default for reusable production behavior. | Place the candidate wording and its adoption decision in the existing decision plan. | PROVEN | Owner direction; FW-8 extends the existing engine API and Rust boundary for later review. |
| C-002 | Python API and ecosystem glue remain legitimate while built-in compute fallbacks remain prohibited. | Compare the boundary examples with AGENTS and the server-prep ADR. | PROVEN | Thin bindings, plan construction, and the explicit Arrow-batch UDF exception are current contracts; tests and tools are outside the production placement rule. |
| C-003 | Agent briefs and capability intakes consume the same placement proposal without authorizing a rewrite. | Link the proposal from the brief fields and roadmap; require scoped migration evidence. | PROVEN | Language placement becomes a proposed brief/intake decision; no release reservation, migration backlog, or implementation is introduced. |
| C-004 | Changes stay within planning Markdown, maps, and this unit's evidence. | Inspect incremental paths and preserve runtime, authority, and previous frozen ledgers. | PROVEN | Allowed homes are docs/sepmo, capability roadmap, their maps, this ledger/navigation, and scripts/map.md evidence. |

Scope verdict: PASS, four defined propositions, zero OPEN or REJECTED. The FW-8 adoption
decision remains open for later review; documentation scope and authorization are settled.

## Pre-execution review

`PER-rust-first-placement-plan`: PROCEED. LIGHT rubric: one planning concern, prose-only
diff, reversible in one commit, no dependency or runtime pattern, no sensitive code edit,
and settled scope. Actor and Critic run sequentially at frontier tier in this session.
No sub-agent dispatch. `SLR-1`: preserve prior PR commits and frozen ledgers; update only
the proposed placement rule and its consumers. Pickup confirms a clean PR worktree at
0bfaf540; unrelated archival and the owner's other worktree stay outside this narrow unit.

## Execution evidence

FW-8 is drafted in the existing decision plan. The roadmap links to that single home.
Review and required verification are pending.


## Documentation review

Context break executed; attacking artifacts, not memory. This is an in-session review,
not an independent agent. Inputs: C-001–C-004, the incremental diff, AGENTS' Rust rule,
the server-prep ADR, and the existing decision plan and roadmap.

The review challenged a literal reading that would require all Python files to disappear.
The proposal distinguishes a public Python module from the implementation it calls, and
preserves framework glue, user UDFs, and test/tool languages. It also challenges a Rust
rewrite that adds per-row boundary calls or duplicate behavior. Migration needs a concrete
driver and compatibility evidence; performance claims require measurements. Existing unsafe,
fork ownership, crate placement, API, and test rules stay binding.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rust-first-placement-plan
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Checked the owner direction maps to one explicit Rust-first proposal and a later adoption decision.
      artifacts: [docs/sepmo/frontier-worker-decision-plan-2026-09-07.md]
    - id: AT-2
      status: ATTACKED
      evidence: Checked Python object conversion, framework glue, user UDFs, tests, tools, and existing modules.
      artifacts: [docs/sepmo/frontier-worker-decision-plan-2026-09-07.md]
    - id: AT-3
      status: ATTACKED
      evidence: Checked that file extension, Rust feasibility, and a proposed exception cannot authorize broad migration or override existing rules.
      artifacts: [docs/sepmo/frontier-worker-decision-plan-2026-09-07.md]
    - id: AT-4
      status: N/A
      justification: No runtime state, scheduling, or concurrency implementation changes.
    - id: AT-5
      status: ATTACKED
      evidence: Preserved current unsafe and authority boundaries and kept the rule nonbinding pending deliberate adoption.
      artifacts: [docs/sepmo/frontier-worker-decision-plan-2026-09-07.md]
    - id: AT-6
      status: ATTACKED
      evidence: Checked current engine API, Python boundary, fork ownership, and component placement against their authoritative homes.
      artifacts: [AGENTS.md, docs/adr/0004-server-prep-disciplines.md]
    - id: AT-7
      status: ATTACKED
      evidence: Required evidence for speed and memory claims and accounted for copies, materialization, allocations, and boundary calls.
      artifacts: [docs/sepmo/frontier-worker-decision-plan-2026-09-07.md]
    - id: AT-8
      status: ATTACKED
      evidence: Kept one proposed-rule home; roadmap and worker fields reference it and retain existing capability identities.
      artifacts: [docs/sepmo/frontier-worker-decision-plan-2026-09-07.md, task/roadmap/epic-term/release-roadmap-2026-08-29.md]
    - id: AT-9
      status: ATTACKED
      evidence: Required exception rationale and a revisit event; preserved public error contracts during any future migration.
      artifacts: [docs/sepmo/frontier-worker-decision-plan-2026-09-07.md]
    - id: AT-10
      status: ATTACKED
      evidence: Required existing public-entry-point tests for migrations and preserved Python tests and tool languages.
      artifacts: [docs/sepmo/frontier-worker-decision-plan-2026-09-07.md]
  reattested: []
  complete: true
```

No S0–S3 documentation findings remain at this review checkpoint. Verification and
final departure checks are pending. No new runtime test is needed for this prose change.


## Final validation and disposition — 2026-09-07

`make verify` exited **0**: Rust **2,830 passed, 0 failed, 5 ignored**, across 48 result
groups; fast CI prerequisites passed. `make check-map-sync check-ledgers
check-ledger-grammar check-docs-compaction` exited **0**. Direct document assertions
verified the seven allowed incremental Markdown paths, eight FW decisions, retained
capability and prior decision rows/headings, local links, and unchanged runtime, authority,
release policy, and previous completed ledgers.

Full facade/dbt preflight was not repeated for this prose-only addendum. Runtime sources,
tests, dependencies, and workflows are identical to the previously verified PR head
0bfaf540. Earlier full preflight and main-synchronization limits remain recorded in the
prior unit's dated integration note. No live AWS or live JVM oracle was run.

`SLR-2` / final Critic: four clauses traced, all ten categories accounted for, and zero
open findings at or above S1. CONVERGED for the documentation unit. FW-8 remains an open
adoption decision. Move the ledger to completed and recheck navigation before committing
and pushing this update to PR #423. Existing authoritative rules remain unchanged.

Disk: 682 GiB free before verification. Installed commit/push hooks are executable and
remain enabled. The shared Cargo cache is reused; no worker clone or duplicate build tree
was created. Keep the isolated PR worktree for review. Remove task-created temporary logs
and caches after publication; no other task's files or shared caches are removed.

# Frontier worker decision plan — 2026-09-07

Class: ledger. Retires from staging in this documentation unit's departure commit;
the next pickup archives it after merge. Base: `2019b737` on PR #423.

## Scope and authorization

The owner asked to add the frontier/worker proposal to the existing planning PR as a
decision plan for later review. The separate Polars quality comparison remains research
in the conversation. No skill, code, routing, permission, or engineering-rule change is
authorized by this unit. The owner authorized updating the PR, including commit and push.

## PROPOSITION LEDGER

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The artifact is a proposal with explicit later decisions and a retirement event. | Inspect scope, lifecycle, decision register, and follow-up sequence. | PROVEN | Owner request; campaign home is docs/sepmo, closing with the efficiency pilot disposition. |
| C-002 | Proposed skills reuse the current packet foundation and identify its limits honestly. | Check assembler, extractor, format, adoption proposal, and baseline. | PROVEN | At 2019b737, callers/interfaces are empty; relevant paths cap at 40; adapters omit Terra; baseline establishes no cost saving. |
| C-003 | Worker qualification is measured without silently relaxing current authority. | Compare candidate routes and pilot design with AGENTS, SEPMO, and binding homes. | PROVEN | Model assignments are hypotheses; current critical-path and permission rules remain binding. |
| C-004 | Only the decision document and its navigation/evidence change. | Inspect the incremental diff against 2019b737 and preserve the prior completed ledger. | PROVEN | Allowed paths: docs/sepmo document/map, this ledger and ledger maps, scripts/map.md evidence pointer. |

Scope verdict: PASS, four defined propositions, zero OPEN or REJECTED. The FW decision
register describes later adoption choices, not missing authority to document this proposal.

## Pre-execution review

`PER-frontier-worker-decision-plan`: PROCEED. LIGHT rubric: one planning component,
Markdown-only edits, reversible in one commit, no dependency or runtime pattern, no
sensitive code path, and settled documentation scope. Actor and Critic run sequentially
at frontier tier in this session. No sub-agent dispatch.

`SLR-1`: preserve published commit 2019b737 and its frozen ledger. Add one documentation
commit. Pickup checked the clean PR worktree and existing packet implementation; unrelated
ledger archival and the owner's other worktree remain outside this narrow unit. Disk:
680 GiB free before work. Shared hooks and Cargo cache are retained.

## Execution evidence

The decision plan is drafted. Review, verification, and departure are pending.


## Documentation review

Context break executed; attacking artifacts, not memory. This is an in-session procedural
break, not a separate agent. Review inputs: C-001–C-004, the new document, the incremental
diff, and current source/authority homes. No Actor explanation is acceptance evidence.

The review challenged three failure cases: a detailed brief can preserve a false premise;
a shorter packet can increase total cost; and a proposed model route can conflict with
current critical-path rules. The document addresses these with separate patch review,
matched pilot comparisons, and an explicit later binding decision. It distinguishes brief
readiness from permission and a runner's real enforcement from instructions in Markdown.

Source inspection confirmed the assembler adapter list and the extractor's empty caller
and interface lists and 40-path cap. The baseline records larger packets for two of its
three fixtures and makes no measured savings claim. Existing files own those measurements;
the decision document links to them. No new benchmark or runtime result is asserted.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: frontier-worker-decision-plan
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Compared the owner request, four scope clauses, proposal state, and decision register.
      artifacts: [docs/sepmo/frontier-worker-decision-plan-2026-09-07.md]
    - id: AT-2
      status: ATTACKED
      evidence: Checked incomplete context, source drift, unsettled semantics, and budget exhaustion.
      artifacts: [docs/sepmo/frontier-worker-decision-plan-2026-09-07.md]
    - id: AT-3
      status: ATTACKED
      evidence: Checked that READY, model labels, cached prompts, and pilot success cannot grant authority or imply general qualification.
      artifacts: [docs/sepmo/frontier-worker-decision-plan-2026-09-07.md]
    - id: AT-4
      status: N/A
      justification: No concurrent execution or runtime state changes; the PR worktree is isolated.
    - id: AT-5
      status: ATTACKED
      evidence: Checked real runner enforcement versus Markdown rules and retained current sensitive-path prohibitions.
      artifacts: [docs/sepmo/frontier-worker-decision-plan-2026-09-07.md]
    - id: AT-6
      status: ATTACKED
      evidence: Compared extractor and adapter claims with current sources and linked authoritative homes for future rechecks.
      artifacts: [scripts/sepmo_packet_extract.py, scripts/sepmo_packet.py, docs/sepmo/packets/baseline.md]
    - id: AT-7
      status: ATTACKED
      evidence: Checked total accepted-change accounting includes preparation, cache categories, retries, critique, and failed attempts.
      artifacts: [docs/sepmo/frontier-worker-decision-plan-2026-09-07.md]
    - id: AT-8
      status: ATTACKED
      evidence: Confirmed proposed skills reuse packet groups and require explicit adoption rather than changing current bindings.
      artifacts: [docs/sepmo/frontier-worker-decision-plan-2026-09-07.md]
    - id: AT-9
      status: ATTACKED
      evidence: Required source contradictions, actual exit codes, missing telemetry, and unresolved evidence in handbacks.
      artifacts: [docs/sepmo/frontier-worker-decision-plan-2026-09-07.md]
    - id: AT-10
      status: ATTACKED
      evidence: Checked independent acceptance, seeded incorrect behavior, matched comparisons, and predeclared pilot thresholds.
      artifacts: [docs/sepmo/frontier-worker-decision-plan-2026-09-07.md]
  reattested: []
  complete: true
```

No S0–S3 documentation findings remain at this checkpoint. Required verification and
final navigation checks are pending. No new runtime tests are warranted for this prose
change. Current structural gates validate its links, ledger evidence, and scoped paths.


## Final validation and disposition — 2026-09-07

`make verify` exited **0**, using the existing shared Cargo cache. Rust: **2,830 passed,
0 failed, 5 ignored**, across 48 result groups. The fast CI prerequisites passed.
`make check-map-sync check-ledgers check-ledger-grammar check-docs-compaction` also exited
**0**. Direct assertions checked the five allowed incremental Markdown paths, seven FW
adoption decisions, all local links in the proposal, and unchanged authority, source,
release policy, and prior completed ledger.

The prior PR commit `2019b737` already passed full `make preflight`: facade 5,805 passed /
368 skipped, dbt 59 passed / 1 skipped, plus security and workflow checks. Those suites
were not repeated for this addendum: runtime code, dependencies, configuration, workflows,
and test sources are identical. This is a current documentation/verify result, not a
claim of another full preflight run. No live AWS or live JVM oracle was run.

`SLR-2` / final Critic re-attestation: the final proposal and navigation retain the scope
boundary, source evidence, and current authority. Four clauses traced, all ten categories
accounted for, zero open findings at or above S1. CONVERGED for this documentation unit.
The seven FW adoption decisions remain open by design. Move this ledger to completed,
recheck navigation after the move, then commit and push the addendum to PR #423.

Disk: 680 GiB free before work and before verification. The shared Cargo cache is retained;
no duplicate build tree or worker clone was created. The isolated PR worktree remains for
review. Task-owned temporary validation/commit/push logs can be removed after publication;
the durable evidence is recorded here. No other task's files are removed.

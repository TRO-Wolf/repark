# map — .agents/skills/sepmo/references/

## Purpose

The eight **canonical instrument homes** the SEPMO v2 spine ([../SKILL.md](../SKILL.md)) routes to —
one file per phase/role. Each file owns its formats and procedures exclusively (one home per fact);
the spine cites them, never restates them. Portable canon: no project facts (those live in
[../binding-manifest.md](../binding-manifest.md)); defects are filed to the user, never patched
silently.

## Contents

- [01-scope-auditor.md](01-scope-auditor.md) — the proposition-ledger format (`C-###`,
  `PROVEN`/`OPEN`/`REJECTED`), the six-step audit protocol (incl. the disjunctive-acceptance ban
  and, per spine v2.2, the §2.2b **enumeration obligation**: a quantified clause is `OPEN` until
  its domain is a finite, attackable partition), worked examples; reused at unit scope by
  the PR_READINESS_AUDIT (R7).
- [02-orchestrator.md](02-orchestrator.md) — PRE_EXECUTION_REVIEW procedure (spine v2.3: incl.
  the contingency-executability confirmation, R11), charter→PR carving, proportionality-rubric
  operation, context-break mechanics (R3), dispute handling (R6), the cycle-cap disposition set +
  multi-unit-assembly remand binding (spine v2.3: R12/R13), readiness checklist (R7), PR
  description template (R8).
- [03-self-logic-review.md](03-self-logic-review.md) — the `SELF_LOGIC_REVIEW` format (D3,
  per-action; spine v2.3 adds the `contingencies` executability line, R11) — also the format for
  the one-time whole-plan PRE_EXECUTION_REVIEW (state 3).
- [04-actor.md](04-actor.md) — green exit conditions (R2), clause-pinning tests (spine v2.2:
  quantified clauses pin per element of the ledger's audit-time enumeration; domain growth
  inherits the pin in the same unit; ad-hoc execution-time enumeration = HALT), the
  regression-proof protocol (R5), remediation dispositions, the Actor role prompt.
- [05-critic.md](05-critic.md) — the 10-category attack taxonomy (AT-10 carries branch
  liveness), coverage attestation format (R4), finding-record schema (`F-<unit>-<n>`, S0–S3),
  dispute conduct (R6), context-break conduct (R3 — the claim-span check against the ledger's
  enumeration and, per spine v2.2, the fresh-execution step: a **novel**, fully cited input
  through the public surface; masking paths never sole evidence), and per spine v2.3 the
  closing-authority item-by-item remand duty (R13) + the external-critic-engine constraints.
- [06-vigilance.md](06-vigilance.md) — Invariant V: watch list (incl. the unledgered-claim check
  and, per spine v2.3, VG-09 unsettled-disposition consumption / unexecutable contingency), alarm
  protocol, the single owned transition (T8).
- [07-delivery.md](07-delivery.md) — per-PR acceptance against four artifacts (ledger, attestation,
  findings, shipped-flag register); T9/T10 rejection classification.
- [08-retrospective.md](08-retrospective.md) — learning capture (PROMOTE requires a landed
  *detector* for trap-class lessons), the mandatory metrics ledger — **eight** metrics incl.
  `environment_drift_events` (spine v2.1) — guarding T12, and the feed-forward rule (spine v2.2:
  asymmetric — bar-raising lands immediately stamped, incl. from incident retrospectives;
  bar-lowering/neutral waits for the project boundary; spine v2.3: machinery incidents file the
  same `kind: incident` section, keys legitimately empty).

## I want to...

| ...do this | go to |
|---|---|
| Audit scope / write a ledger | [01-scope-auditor.md](01-scope-auditor.md) |
| Carve PRs, run the loop, assemble a PR | [02-orchestrator.md](02-orchestrator.md) |
| Log a pre-action review | [03-self-logic-review.md](03-self-logic-review.md) |
| Build a slice | [04-actor.md](04-actor.md) |
| Attack a build | [05-critic.md](05-critic.md) |
| Check for drift / unledgered claims | [06-vigilance.md](06-vigilance.md) |
| Accept or return a PR | [07-delivery.md](07-delivery.md) |
| Close a project (learnings + metrics) | [08-retrospective.md](08-retrospective.md) |

## Pointers

- Up: [../map.md](../map.md); the spine: [../SKILL.md](../SKILL.md); bindings:
  [../binding-manifest.md](../binding-manifest.md).

## Debug

A reference appears to conflict with the spine → the spine's transition table and R-rules are
normative; the reference is the defect — file it (D2). A reference restates a spine rule instead of
citing it → two-homes defect; collapse to a citation. Escalate to: [../map.md#debug](../map.md).

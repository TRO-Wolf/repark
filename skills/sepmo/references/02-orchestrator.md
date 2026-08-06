# Orchestrator

The Orchestrator is the only agent that holds the whole picture. It owns the one-time
PRE_EXECUTION_REVIEW before any build starts; carves the frozen charter into PR units and scores each
against the proportionality rubric; drives every unit's Actor–Critic cycle, enforcing the context break
between build and review; resolves disputes procedurally when they are not escalated to the user; runs
or delegates the readiness audit; and assembles every PR with the evidence a reviewer needs to trust it
without re-deriving it. This file is the canonical home of all six of those instruments; the spine
(`SKILL.md`) states only that they exist and where to find them.

## 1. Context model

The Orchestrator is the sole cross-unit context holder — Actor and Critic each see only their handed
slice (Actor blindness: home `04-actor.md`; Critic input restriction: R3, operationalized in §4 below).
The charter is derived from the plan-of-record (binding manifest) and frozen the moment APPROVAL_GATE
passes (T3); the Orchestrator never edits a frozen charter in place — new or changed scope is T8/T11,
full stop, routed back to AGGRESSIVE_LOGIC_SCOPE_AUDIT. The Orchestrator writes the active working plan
to the home the binding manifest names for "Active plan tracking" — no parallel tracker, one home per
fact. Mode (interactive vs. delegated) is bound the same way; default interactive unless told otherwise,
and every doctrine trip is an escalation in that mode, never an agent-side guess (spine, "Escalate,
never guess").

## 2. PRE_EXECUTION_REVIEW — state 3, Orchestrator sole owner

Before any unit enters ACTOR_BUILD, the Orchestrator logs exactly **one** Self Logic Review (base format:
`03-self-logic-review.md`) over the *complete plan* — not a per-action review, and not repeated per unit.
This satisfies D3 at the plan level; per-action SLRs by every agent continue throughout execution
regardless. The review's `action` is "review the whole plan for execution-readiness"; its body carries
the plan checklist below, which is this file's addition to the base SLR schema:

```yaml
PRE_EXECUTION_REVIEW:
  id: PER-<charter-id>
  slr: <SLR id — filed against the ref-03 base schema>
  plan_checklist:
    charter_frozen: SATISFIED (<APPROVAL_GATE sign-off reference>) | OPEN
    carving_clause_complete:
      forward:  SATISFIED (every clause maps to exactly one PR unit) | OPEN (<orphan clauses>)
      backward: SATISFIED (every PR unit traces to >=1 clause)      | OPEN (<orphan units, D5>)
    rubric_recorded: SATISFIED (<n>/<n> units carry a filed PROPORTIONALITY_RUBRIC, §3) | OPEN (<units missing one>)
    bindings_resolved: SATISFIED (models, tiers, and green commands all resolved in the binding manifest) | OPEN (<unresolved rows>)
  verdict: PROCEED | GAP_FOUND
  gap_route: APPROVAL_GATE | AGGRESSIVE_LOGIC_SCOPE_AUDIT | "—"   # required when GAP_FOUND, per T6
  gap_detail: <the specific unresolved item, or "—">
```

`PROCEED` is legal only when all four checklist rows read `SATISFIED` — this is T5's guard. Any `OPEN`
row is a `GAP_FOUND` and routes backward via T6; it is **never** patched inline mid-review. Classify the
gap before routing: a gap in the ledger's content itself — an ambiguous, missing, or newly surfaced
clause, or a carving that cannot be made clause-complete without touching scope — goes to
AGGRESSIVE_LOGIC_SCOPE_AUDIT for a full rewrite pass (ref 01). A gap that leaves the ledger standing —
an unfiled rubric result, an unresolved manifest binding, a carving omission that closes without
changing scope — goes to APPROVAL_GATE, where the standing ledger and the fix are re-confirmed before
PRE_EXECUTION_REVIEW is re-run. Either way the Orchestrator re-runs this whole review from a clean slate
once the gap closes; a partially-checked plan does not carry forward.

## 3. PR_SCOPING — charter carving and the proportionality rubric

Carve the frozen charter into PR units sized by logical coherence and reviewability, never clock time
(spine, "Unit of delivery"). Every clause maps to exactly one PR unit; every unit traces back to at least
one clause — this bijection is exactly what §2's `carving_clause_complete` checks. No unit bundles
unrelated scope; no single logical change splits across two units.

For **every** unit, at PR_SCOPING, evaluate the spine's six proportionality criteria and file the result
as an addressable artifact before ACTOR_BUILD opens:

```yaml
PROPORTIONALITY_RUBRIC:
  id: RUBRIC-<pr-unit-id>
  pr_unit: <id>
  criteria:                 # the spine's six — cited by name, evaluated here
    blast_radius:  PASS | FAIL | UNCERTAIN (<evidence>)
    reversibility: PASS | FAIL | UNCERTAIN (<evidence>)
    size:          PASS | FAIL | UNCERTAIN (<changed lines / files, evidence>)
    novelty:       PASS | FAIL | UNCERTAIN (<evidence>)
    sensitivity:   PASS | FAIL | UNCERTAIN (<evidence>)
    clarity:       PASS | FAIL | UNCERTAIN (<open clarifications; clause verdicts, evidence>)
  path: LIGHT | STANDARD
  recorded_by: Orchestrator
```

`path: LIGHT` is legal only when all six read `PASS`. A single `FAIL` or `UNCERTAIN` — including one the
Orchestrator cannot resolve on the spot — routes the unit to `STANDARD`; under-scoping ceremony is the
riskier error, so uncertainty never rounds down. This artifact is what §2's `rubric_recorded` check
counts, and it is what §7's LIGHT self-run allowance reads before an Orchestrator may waive the
independent readiness auditor.

## 4. AC-loop coordination

Per unit, the Orchestrator dispatches the stage sequence — ACTOR_BUILD, SELF_LOGIC_REVIEW, CONTEXT_BREAK,
CRITIC_REVIEW — and, on open findings, ACTOR_REMEDIATE back into the top of that sequence (R1, R4).
**Cycle cap: 2–3 rounds.** If findings at/above the floor persist past the cap, the Orchestrator stops
cycling and escalates rather than grinding — interactive: to the user, with the open findings; delegated:
flagged in the final report, unit held open. **Remediation mediation:** findings routed back to the Actor
are reframed as a plain defect-fix slice — the record never says "the Critic found this," it says what is
wrong and where, preserving the Actor's build-phase blindness (home: `04-actor.md`). **Convergence is the
Critic's call, never the Orchestrator's and never the Actor's** (R4); the Orchestrator's job is to audit
that the attestation is complete, not to count findings or push a unit forward on schedule pressure.

## 5. Context-break mechanics — implementing R3

Between SELF_LOGIC_REVIEW and CRITIC_REVIEW, the Orchestrator executes and logs the break:

```yaml
CONTEXT_BREAK:
  id: CB-<pr-unit-id>-<cycle-n>
  mechanism: FRESH_SUBAGENT | COMPACTED_FRESH_CONTEXT | PROCEDURAL_IN_SESSION
  manifest_binding: <the binding manifest's Sub-agent / tier policy row, which resolves this choice>
  handed_to_critic: [unit_charter_clauses, diff_and_artifacts, test_results, "attack_taxonomy (ref 05)"]
  withheld_until_initial_findings_filed: [actor_build_summary, actor_self_logic_review]
  declaration_logged: "Context break executed; attacking artifacts, not memory."
  honesty_note: <if PROCEDURAL_IN_SESSION: state plainly that the break is procedural, not amnesia>
```

Preference order is fixed by R3(d): a fresh sub-agent beats a compacted fresh context beats a procedural
break in the same session — the manifest's sub-agent/tier-policy binding determines which of these the
project actually permits, and the Orchestrator picks the strongest one that binding allows. Whichever
mechanism runs, the Critic's inputs are exactly the `handed_to_critic` list — the Actor's narrative and
Self Logic Review are withheld until the Critic has filed its initial findings, after which they may be
read only to check for undischarged flags (R3-b). Every finding the Critic files must cite artifact
evidence — file:line, failing input, trace — never a recollection of the build (R3-c); the Orchestrator
rejects a finding record that cites memory instead of artifact. The CRITIC_REVIEW pass the Orchestrator
receives must open with the declaration verbatim: *"Context break executed; attacking artifacts, not
memory."* A pass missing that opening line is not a valid CRITIC_REVIEW and is bounced before it is read.

## 6. Dispute handling — implementing R6

When the Actor disputes a finding with counter-evidence, the Critic disposes it `WITHDRAWN` (folded into
the noise ratio, ref 08) or `SUSTAINED`. The Orchestrator's job is procedural resolution of a sustained
dispute, never adjudication of the underlying technical claim — the Critic's `SUSTAINED` call stands:

```yaml
DISPUTE_RECORD:
  id: DR-<pr-unit-id>-<finding-id>
  finding: <CF-id, home: 05-critic.md>
  actor_position: <the counter-evidence filed>
  critic_position: <the sustaining rationale and evidence, if SUSTAINED>
  critic_disposition: WITHDRAWN | SUSTAINED
  severity: S0 | S1 | S2 | S3
  mode: interactive | delegated
  resolution: NA_WITHDRAWN | ESCALATED_TO_USER | UNIT_HALTED_DELEGATED | ACCEPTED_FLAGGED
  report_reference: <final-report / retrospective-ledger entry carrying this record>
```

`WITHDRAWN` needs no further action beyond the ledger entry. For `SUSTAINED` at or above the severity
floor: *interactive mode* — escalate to the user immediately with both positions and the evidence behind
each, and hold the unit open pending their call. *Delegated mode* — **the unit halts, the PR is not
assembled**, and the dispute is carried into the final report with both positions and their evidence
intact; this is a hard stop the Orchestrator cannot route around by re-cycling the unit. For `SUSTAINED`
below the floor, the unit may ship with the finding as `ACCEPTED_FLAGGED`; the flag is mandatory in the
PR description (§8) and in the retrospective ledger (ref 08) — no disposition is ever dropped silently.

## 7. Readiness checklist — implementing R7

PR_READINESS_AUDIT does not re-prove the charter; it confirms, with evidence, that the unit is actually
shippable:

```yaml
PR_READINESS_CHECKLIST:
  id: RA-<pr-unit-id>
  self_run_by_orchestrator: true | false   # true permitted only when path == LIGHT, §3
  checks:
    ci_green: PASS (<build/test/static-check commands, per the binding manifest's Done-gate row>) | FAIL
    unit_clauses_proven: PASS (<clause: evidence>, ...) | FAIL (<unproven or orphan clause, D5>)
    coverage_attestation_attached: PASS (<attestation id, ref 05>) | FAIL
    findings_ledger_closed: PASS (<every >=floor finding REMEDIATED w/ regression link, ACCEPTED_FLAGGED, or DR-resolved>) | FAIL
    clause_trace_complete: PASS (<every changed artifact traces to a clause>) | FAIL
  verdict: READY | SEND_BACK
  send_back_target: ACTOR_REMEDIATE | "N/A"
```

All five checks `PASS` is required for `READY`; any `FAIL` sends the unit back into ACTOR_REMEDIATE, not
a fresh cycle from scratch. `self_run_by_orchestrator: true` is legal only on a unit whose
`PROPORTIONALITY_RUBRIC.path == LIGHT` (§3) — every STANDARD unit routes to an independent frontier
auditor for this stage. Nothing about the checklist's content changes between LIGHT and STANDARD; only
who runs it does.

## 8. PR description template — implementing R8

Every assembled PR embeds this, filled from the artifacts above, so a reviewer can verify the unit from
the description alone without re-deriving any of it:

```markdown
## <PR title — the carved unit's scope in one line>

**Charter trace**
| Clause | Requirement (one line) | Status | Evidence |
|---|---|---|---|
| C-### | ... | PROVEN | <test / command> |

**Coverage attestation summary** (ref 05)
- Categories attested: <n>/<n> ATTACKED, <n> N/A (justified) — attestation id <CA-id>

**Findings ledger**
| Finding | Severity | Disposition | Evidence |
|---|---|---|---|
| CF-<id> | S# | REMEDIATED / ACCEPTED_FLAGGED / WITHDRAWN | <regression link or justification> |

**Shipped flags**
- <every ACCEPTED_FLAGGED finding, with rationale — R6>
- <every delegated-mode deviation flagged per Mode Handling>

**Readiness audit**: RA-<id> — READY (self-run: <yes/no>)
**Context break**: CB-<id> — mechanism <FRESH_SUBAGENT / COMPACTED_FRESH_CONTEXT / PROCEDURAL_IN_SESSION>
```

A PR missing any section above is not assembled — R8 makes the description itself part of the gate, not
decoration on top of it.

## Routing

Consumed by: state 3 **PRE_EXECUTION_REVIEW** (T5 proceeds only when §2's checklist is fully `SATISFIED`;
T6 routes a gap per §2's classification) — co-loaded with `03-self-logic-review.md` for the base SLR
schema. Consumed inside **ORCHESTRATED_EXECUTION**'s sub-machine: **PR_SCOPING** runs §3; **CONTEXT_BREAK**
runs §5 (R3); **ACTOR_REMEDIATE** and a sustained dispute run §6 (R6); **PR_READINESS_AUDIT** runs §7 (R7);
**ASSEMBLE_PR** emits §8 (R8). §4's cycle cap and mediation apply across every AC cycle inside a unit.
Downstream: `07-delivery.md` verifies the PR against exactly the artifacts §8 embeds; `08-retrospective.md`
reads every `DISPUTE_RECORD` and `ACCEPTED_FLAGGED` flag into the metrics ledger and noise ratio.

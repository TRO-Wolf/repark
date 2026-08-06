---
name: sepmo
version: "2.2"
description: >-
  Software Engineering Project Manager & Orchestration framework. Runs
  substantial software and data-engineering work through an audit-first state
  machine: scope is proven as a ledger of checkable propositions before any
  code is written, execution runs as an adversarial Actor–Critic loop with
  coverage-attested review, and every sequence of work converges on a
  CI-green, traceable pull request. Use this skill whenever the user invokes
  SEPMO or any of its artifacts (scope audit, charter, Actor–Critic, PR plan,
  readiness audit, retrospective), asks to plan, scope, orchestrate, or govern
  a multi-step software/data-engineering project or multi-agent workflow, or
  asks for rigorous assumption-killing review before building — even if they
  never say "SEPMO". Do NOT load this skill for single-file bug fixes, trivial
  edits, general Q&A, or exploratory prototyping the user has not asked to
  govern: SEPMO is a deliberate operating mode, not a default posture.
---

# SEPMO — Software Engineering Project Manager & Orchestration

SEPMO is a control system for shipping software correctly. Its premise is
simple and unforgiving: **most project failures are scope failures that were
invisible at planning time.** A vague requirement, an unstated assumption, or
an unhandled edge case costs almost nothing to fix in the spec and a fortune
to fix in production. SEPMO front-loads that cost. It refuses to let work
proceed until the plan is a provable logical contract, runs execution as an
adversarial loop — every build is attacked before it is trusted — and
converges each unit of work on a clean pull request.

One rule governs everything below: **every gate is a checkable artifact, not
a self-report.** A score with no ledger behind it, a "converged" with no
attestation behind it, a "mergeable" with no green CI behind it — each is a
defect in its own right, regardless of whether the underlying work was good.
SEPMO does not ask agents whether they did the work; it asks for the artifact
that proves it.

This file is the **spine**: the state machine, the doctrines, the agent
roster, and the routing rules that tell you which reference file to load for
the phase you are in. Load the relevant `references/` file *before* acting in
that phase — the spine tells you *when* and *why*; the reference tells you
*how*.

---

## Model assumption — frontier on the critical path

SEPMO is *designed* to run a **frontier model on every critical-path step** —
the Orchestrator, Scope Auditor, Actor, Critic, and every audit — because its
guarantees come from genuine reasoning at each gate, not pattern-matching its
way to "looks done." **"FF" / "frontier–frontier"** denotes an Actor–Critic
pair with *both* roles at frontier tier — which concrete model that is, is
the project's choice, resolved by the binding manifest, not fixed in this
portable shell.

How that aspiration is *governed* is **not restated here** (one home per
fact): when frontier is required, when tier may be turned down, single-agent
default versus literal multi-agent fan-out, and the cost discipline that
bounds it all live in the project's sub-agent & tier policy and its frontier
operating notes — see [binding-manifest.md](binding-manifest.md) (*Sub-agent /
tier policy*). For **this** repo that resolves to a **single-agent default**:
one session runs the Actor phase, then deliberately shifts into the Critic
phase, sequentially, under the Context Break rule (R3 below); a literal
separate-agent FF pair is opt-in, used only when the user lifts that policy.

---

## Unit of delivery — the pull request

**Every sequence of work converges on a pull request.** A PR is a coherent,
reviewable, mergeable slice of the approved charter. The Orchestrator's
primary act of judgment is **carving the charter into PR-sized scope
groups** — sized by logical coherence and reviewability, *not* by clock time.
A PR may take more or less time than expected; what it may never do is bundle
unrelated scope or split a single logical change across two reviews. The
top-level agent thinks in PRs, always: "what is the smallest set of changes
that is coherent, traceable, and worth a human's review as one unit?"

---

## Severity scale — global vocabulary

Every Critic finding, dispute, flag, and gate in SEPMO uses this scale. It is
defined once, here, and consumed everywhere.

| Sev | Name | Meaning |
|-----|------|---------|
| **S0** | Critical | Violates a charter clause in a way that corrupts data, loses money, breaks security, or produces silently wrong results. Shipping it is an incident. |
| **S1** | Major | Functional defect or charter-clause violation with visible failure: wrong behavior on realistic inputs, missing required error handling on a reachable path, a clause implemented without a pinning test. |
| **S2** | Minor | Robustness gap on edge inputs, degraded failure mode, unclear contract, missing test for unspecified-but-reasonable behavior. Correct on the main path. |
| **S3** | Advisory | Style, naming, future-risk observation, optional refactor. Never blocks anything. |

**The severity floor** is the level at or above which open findings block
convergence and PR assembly. **Default floor: S1.** The binding manifest may
*raise* the floor (e.g., include S2 for money- or data-critical projects); it
may never lower it below S1.

---

## The Iron State Machine

Every project moves through these states in order. **Backward transitions are
allowed and expected; skipping forward is forbidden.** You may always fall
back to a stricter, earlier state; you may never jump ahead of a gate that
has not passed.

Orientation (non-normative):
`PROPOSAL → AGGRESSIVE_LOGIC_SCOPE_AUDIT → APPROVAL_GATE →
PRE_EXECUTION_REVIEW → ORCHESTRATED_EXECUTION → DELIVERY (per PR) →
RETROSPECTIVE`, with backward transitions per the table below.

**The transition table is normative.** If prose anywhere in this repo appears
to disagree with this table, the table wins and the disagreement is a defect
to be filed.

| # | From | Event | Guard | To |
|---|------|-------|-------|-----|
| T1 | PROPOSAL | Brief written | A written brief exists as a document | AGGRESSIVE_LOGIC_SCOPE_AUDIT |
| T2 | AGGRESSIVE_LOGIC_SCOPE_AUDIT | Verdict filed | Proposition ledger produced (see gate rule below) | APPROVAL_GATE |
| T3 | APPROVAL_GATE | Gate passes | Every clause `PROVEN` ∧ zero `OPEN` ∧ zero `REJECTED` remaining ∧ user explicitly confirms | PRE_EXECUTION_REVIEW |
| T4 | APPROVAL_GATE | Gate fails | Any clause `OPEN` or `REJECTED` | AGGRESSIVE_LOGIC_SCOPE_AUDIT (rewrite) |
| T5 | PRE_EXECUTION_REVIEW | Review logs `PROCEED` | Whole-plan Self Logic Review clean (ref 03) | ORCHESTRATED_EXECUTION |
| T6 | PRE_EXECUTION_REVIEW | Review finds a gap | — | APPROVAL_GATE or AGGRESSIVE_LOGIC_SCOPE_AUDIT, per the gap |
| T7 | ORCHESTRATED_EXECUTION | All PR units accepted | Every PR passed DELIVERY | RETROSPECTIVE |
| T8 | ORCHESTRATED_EXECUTION | Drift alarm (Invariant V) or requirement change | — | AGGRESSIVE_LOGIC_SCOPE_AUDIT |
| T9 | DELIVERY (a PR) | User rejects the PR | Defect within existing scope | ORCHESTRATED_EXECUTION (reopen that unit's AC cycle) |
| T10 | DELIVERY (a PR) | User rejects the PR | Rejection implies new/changed scope | AGGRESSIVE_LOGIC_SCOPE_AUDIT |
| T11 | Any state ≥ 1 | New or changed requirement surfaces | — | AGGRESSIVE_LOGIC_SCOPE_AUDIT |
| T12 | RETROSPECTIVE | Learnings + metrics filed | Metrics ledger complete (ref 08) | *terminal* |

| # | State | Owner | Exit condition | Load |
|---|-------|-------|----------------|------|
| 0 | **PROPOSAL** | User + Orchestrator | A written brief exists | — |
| 1 | **AGGRESSIVE_LOGIC_SCOPE_AUDIT** | Scope Auditor | Proposition ledger filed | `references/01-scope-auditor.md` |
| 2 | **APPROVAL_GATE** | Scope Auditor | Ledger fully `PROVEN` **and** user confirms | `references/01-scope-auditor.md` |
| 3 | **PRE_EXECUTION_REVIEW** | Orchestrator (sole owner) | One-time whole-plan Self Logic Review logged `PROCEED` | `references/02-orchestrator.md`, `03-self-logic-review.md` |
| 4 | **ORCHESTRATED_EXECUTION** | Orchestrator + Actor + Critic | Every PR unit converged, audited, assembled | `references/02`, `04`, `05` |
| 5 | **DELIVERY** (per PR) | Delivery agent | User accepts each PR | `references/07-delivery.md` |
| 6 | **RETROSPECTIVE** | Retrospective agent | Learnings **and metrics** captured & filed | `references/08-retrospective.md` |

### The gate is a ledger, not a score

The Scope Audit's output is a **proposition ledger**: the `REFINED_CHARTER`
enumerated as clauses (`C-001`, `C-002`, …), each stated as a checkable
proposition, each carrying exactly one verdict:

- **`PROVEN`** — the proposition is stated checkably and its proof obligation
  is discharged (evidence attached).
- **`OPEN`** — cannot yet be proven: ambiguous, missing information, or an
  undischarged assumption. Every `OPEN` clause carries the question that
  would close it.
- **`REJECTED`** — not statable as a checkable proposition (a wish, not a
  requirement). Must be rewritten or removed before the gate.

**Quantified clauses carry an enumeration obligation.** A proposition that
quantifies — "parity," "every," "all," "handled," anything ranging over
classes of inputs or entry points — is checkable only once its domain is
enumerated into a **finite partition** (divergence classes × entry points, or
whatever the claim actually ranges over). The enumeration is part of the
clause's proof obligation: until it exists, the clause is `OPEN`. The
partition is itself attack surface — a lazy one-class enumeration collapses
the clause back to a single representative case, which is exactly the defect
this obligation exists to prevent — so the Scope Auditor attacks it at the
gate and the Critic's span check (ref 05) attacks it again in execution. The
enumeration is a standing, addressable artifact; execution pins against it
(R2).

**The gate passes iff the ledger shows zero `OPEN` and zero `REJECTED` — every
surviving clause `PROVEN` — and the user explicitly confirms.** One `OPEN`
clause is a rejected audit. There is no "good enough" entry into execution.

`LOGIC_SCORE` survives for continuity, defined as **nothing but the ratio**
`PROVEN clauses / total clauses`. "100/100" *means* "the attached ledger is
fully proven" — it is a summary of the artifact, never a substitute for it.
**A score asserted anywhere without its ledger attached is itself an audit
failure.** Ledger format and worked examples: `references/01-scope-auditor.md`.

### PRE_EXECUTION_REVIEW — one review, one owner

This state is a **single, one-time, whole-plan review owned by the
Orchestrator**, distinct from the per-action Self Logic Reviews that D3
requires of every agent throughout the project. Before any build begins, the
Orchestrator logs one Self Logic Review (format: ref 03) over the complete
plan, confirming at minimum: the charter is frozen; the PR carving is
clause-complete (every clause maps to exactly one PR unit, every unit traces
to clauses); each unit's path assignment (LIGHT/STANDARD, below) has a
recorded rubric result; and the binding manifest resolves every open binding
(models, tiers, green commands). A gap here routes backward via T6 — it never
gets patched inline.

### Invariant V — Vigilance (not a state)

Vigilance is a **standing invariant, not a stop on the state machine**. It is
active from the moment APPROVAL_GATE passes until RETROSPECTIVE files, it
observes every state, and it owns exactly one transition: raising the drift
alarm (T8). It watches for unstated assumptions, orphan work, scope creep,
unledgered claims ("100/100" with no ledger, "converged" with no
attestation), and proportionality-rubric violations. Protocol:
`references/06-vigilance.md`.

### Incident retrospectives — when a defect escapes

An **escaped defect** — one discovered after its PR was accepted — triggers
an **incident retrospective** immediately: a mini state-6 scoped to the
defect, filing the same ref-08 metrics now (`coverage_misses`, naming the
attestation category that sat clean over the failure, and
`escaped_defects_by_origin`) and proposing changes while the evidence is
fresh. Feed-forward from it is **asymmetric**: bar-raising proposals — a new
pinning obligation, a tightened binding, a new attack category — may land
immediately as stamped manifest updates or canon-amendment proposals;
bar-lowering or neutral changes wait for the project boundary. The asymmetry
is stricter-interpretation-wins applied to feed-forward: the
never-mid-project rule exists to stop the bar dropping under pressure, not to
delay its rise. The defect's remediation is its own unit through the normal
machine, with R5's regression proof — and, where the defect falsified a
quantified clause, a check that the clause's enumeration actually contained
the failing element (if it did not, the enumeration was the defect; grow it).

---

## Inside ORCHESTRATED_EXECUTION — the Actor–Critic / PR sub-machine

Execution is not "agents write code." It is a disciplined adversarial loop
that ends in a PR. The Orchestrator carves the charter into PR units, then
drives each unit through one or more Actor–Critic cycles and a readiness
audit before assembling the PR.

Orientation (non-normative — the stage table below is normative):

```
PR_SCOPING ─▶ per PR unit, repeat the cycle until converged:

  ACTOR_BUILD ─▶ SELF_LOGIC_REVIEW ─▶ CONTEXT_BREAK ─▶ CRITIC_REVIEW ─▶ converged (R4)?
       ▲                                                                │           │
       │                            no: open findings ≥ disposition due │           │ yes
       └──────────────── ACTOR_REMEDIATE ◀──────────────────────────────┘           │
        (remediation re-enters the cycle: green, review,                            ▼
         break, and Critic re-attestation all run again)   PR_READINESS_AUDIT ─▶ ASSEMBLE_PR ─▶ DELIVERY (this PR)
```

| Stage | Owner | Exit guard |
|-------|-------|-----------|
| **PR_SCOPING** | Orchestrator | Charter carved into PR units; every clause → exactly one unit; every unit path-assigned with a recorded rubric result |
| **ACTOR_BUILD** | Actor (frontier) | Slice implemented within doctrine; **workspace green (R2)**; every clause in the unit pinned by at least one test |
| **SELF_LOGIC_REVIEW** | Actor | Review logged `PROCEED` (ref 03) |
| **CONTEXT_BREAK** | Orchestrator | Critic inputs restricted per R3; break declared on the record |
| **CRITIC_REVIEW** | Critic (frontier) | **Full coverage attestation filed (R4)** + findings ledger filed |
| **ACTOR_REMEDIATE** | Actor | Every finding dispositioned: `REMEDIATED` (with regression proof, R5), `ACCEPTED_FLAGGED` (below floor only), or `DISPUTED` (with counter-evidence, R6) |
| **convergence** | Critic — never the Actor | Attestation complete over every applicable category **and** no open or sustained-disputed findings at/above the severity floor |
| **PR_READINESS_AUDIT** | Frontier auditor (Orchestrator may self-run on LIGHT units) | Readiness checklist green (R7) |
| **ASSEMBLE_PR** | Orchestrator | PR assembled with embedded artifacts (R8) |

### Governing rules of the sub-machine

**R1 — One sequential FF cycle is the minimum.** The smallest legal execution
of a PR unit is a single Actor–Critic cycle with *both* roles at frontier
tier, run sequentially: Actor builds, then Critic attacks. Larger or riskier
units run multiple cycles.

**R2 — Green is an exit condition, not a hope.** ACTOR_BUILD does not exit
until the workspace is green: the project builds, the relevant test suite
passes, and configured static checks pass. The concrete commands are bound
per-project in the binding manifest, never assumed. Every charter clause
implemented in the unit must be pinned by at least one test before the Critic
pass; an unpinned clause is an automatic finding (default S1). A clause that
**quantifies** is pinned **per element of the finite domain enumerated for it
at audit** (the ledger's enumeration obligation): one representative case is
not the claim, and every untested element of the domain is an unpinned clause
under this rule (procedure: ref 04; the Critic's span check: ref 05). A unit
that **grows** such a domain — a new entry point, a new divergence class —
inherits the pinning obligation for the new element *in the same unit*: the
matrix grows with the surface, or the growth is an unpinned clause. *Why:* an
untested claim of correctness is an assumption wearing a suit, and D1 already
tells you what to do with assumptions — and a one-cell-tested quantified
claim is the same assumption wearing a test.

**R3 — The Context Break.** Adversarial value comes from independence, and a
Critic that has just read the Actor's rationalizations is primed by the exact
framing that produced the defects. Before every CRITIC_REVIEW:
(a) the Critic's inputs are restricted to: the unit's charter clauses, the
diff and artifacts, test results, and the attack taxonomy (ref 05) — the
Actor's narrative and Self Logic Review are **excluded**;
(b) the Critic files its initial findings **before** reading the Actor's
self-review, and may then read it only to check for undischarged flags;
(c) every finding must cite artifact evidence (file:line, failing input,
trace) — findings justified by memory of the build are invalid;
(d) where the runtime supports a fresh context or separate sub-agent, use
one — the hard break is always preferred; the manifest binds the mechanics.
In sequential single-session mode this break is *procedural, not amnesia* —
name that honestly in the record, and **compensate for it**: for any claim
whose failure class is **silently wrong results** (the S0 wording), the
Critic's attestation must include at least one input it **freshly executed
through the public entry point during its own pass**. The input is the
Critic's own choice and must be **novel** — absent from the unit's committed
tests, or targeting an untested element of an enumerated domain — and is
cited with input, entry point, and observed-versus-expected output.
Citations to the Actor-phase test run do not qualify, because in-session the
Critic "remembers" that run in exactly the way the break exists to distrust;
re-running the committed suite is fine as independent green but does not
satisfy the novelty requirement, because it proves the tests run, not that
the claim holds beyond them (mechanics: ref 05). The manifest binds the
public surface and any standing detector for this rule as
`s0_fresh_execution`, including naming any preview or formatting path (a
`show`-style surface) that can mask the failure class and is therefore never
sole evidence. The Critic pass opens with the declaration:
"Context break executed; attacking artifacts, not memory."

**R4 — Convergence is coverage, never a findings count.** The Critic's duty
is to exhaust an **attack taxonomy** (canonical categories and attestation
format: `references/05-critic.md`), attesting every applicable category as
`ATTACKED` with evidence of what was tried, or `N/A` with justification. A
clean category is legitimate if — and only if — its evidence shows the
attack. "Exhausted" means the attestation is complete, not that findings were
found. The Orchestrator audits attestations, not finding counts; a padded
review is as much a failure as a lazy one, and the retrospective's **noise
ratio** (findings later withdrawn ÷ findings filed) holds the Critic to
precision as well as recall. Categories touched by remediation are
re-attested before convergence. Convergence is the Critic's call, never the
Actor's, and it is now a checkable call: attestation complete ∧ no open or
sustained-disputed findings at/above the severity floor.

**R5 — Remediation requires regression proof.** An accepted finding whose
defect can be expressed as a test is `REMEDIATED` only when a regression test
exists that failed before the fix and passes after it, committed with the
fix and linked in the finding record. Findings not expressible as tests
(e.g., documentation, naming) carry a one-line justification instead. "Fixed"
without proof is `OPEN`.

**R6 — Disputes terminate; they never dangle.** If the Actor believes a
finding is wrong, it files `DISPUTED` with counter-evidence. The Critic then
either `WITHDRAWN`s it (counted in the noise ratio) or **sustains** it.
Sustained disputes at/above the severity floor are a hard stop for the unit:
*interactive mode* — escalate to the user immediately; *delegated mode* — the
unit halts, the PR is **not** assembled, and the dispute is flagged in the
final report. Sustained disputes below the floor may ship as
`ACCEPTED_FLAGGED`, and the flag **must** appear in the PR description and
the retrospective ledger. No disposition is ever silently dropped.

**R7 — The readiness audit is light but real, and "mergeable" means CI
green.** PR_READINESS_AUDIT does not re-prove the whole charter. It confirms,
with evidence: CI green on the assembled branch (build, tests, static checks
per manifest); this unit's clauses all `PROVEN` at unit scope; the coverage
attestation attached and complete; the findings ledger closed at/above the
floor with regression links present; and traceability from every change to a
clause. It reuses the Scope Auditor's discipline (ref 01) at PR scope, and it
can still send the unit back.

The manifest binds two named green tiers: the **unit gate** (the R2 exit)
and the **pre-merge gate** — a faithful local mirror of the CI the PR will
face. Every CI-enforced check either runs in the pre-merge command or is
recorded in the manifest's **CI-only exception record**, each entry naming
the check, the justification, and **the residual gap the exception leaves**
(an exception that claims full equivalence is not an exception; it is an
unproven claim). A bound green command that silently skips a CI-enforced
check is a **binding defect** — Invariant V raises it — because it lets
"mergeable" be certified against a surface CI does not run. Local green must
imply CI green.

**R8 — The PR carries its own evidence.** An assembled PR embeds: the
clause-by-clause trace, the coverage attestation summary, the findings ledger
with dispositions, and any shipped flags. A reviewer must be able to verify
the unit from the PR description alone.

**R9 — DELIVERY is per-PR.** Each assembled PR runs to delivery on its own;
the project reaches T7 when the charter's full PR set is accepted.

**R10 — Environment drift.** A red gate can be caused by the environment
rather than the unit — a newly published advisory, an upstream relicense,
tool-version skew. The classification is **proven, never asserted**: run the
same gate on the base ref, without the unit's diff applied. **Base red →
environmental**: not a unit defect, so it neither reopens the AC cycle (T9)
nor implies a scope failure (T10); it is remediated as its own unit, whose
LIGHT/STANDARD path the proportionality rubric decides as always (note that
security-advisory remediation touches a security-relevant surface and so
routes STANDARD by criterion 5). **Base green and the diff red → a unit
defect: route T9.** Environmental events are recorded in the retrospective
metrics ledger as `environment_drift_events` — a distinct counter, not an
escaped defect, because nothing in the AC loop could have caught it
(canonical definition: ref 08).

---

## Proportionality — two paths, one bar

Full ceremony — multiple AC cycles, an independent readiness audit — is sized
to **substantial units**, where a missed assumption is expensive. Trivial
changes take a **LIGHT path** so rigor never becomes a tax on a one-line
change. What never scales down is the *standard*: the ledger gate, a green
workspace, a complete coverage attestation, and the severity floor hold on
every unit. Proportionality adjusts the *amount of process*, never the bar.

**A unit qualifies for LIGHT only if ALL of the following hold** (the
Orchestrator records the rubric result at PR_SCOPING; defaults below, bound
per-project in the manifest):

1. **Blast radius** — single module/component; no public interface or schema
   change.
2. **Reversibility** — revertible in one commit; no migration, no data
   rewrite.
3. **Size** — ≤ 150 changed lines and ≤ 5 files.
4. **Novelty** — no new dependency, no new external call, no new
   architectural pattern.
5. **Sensitivity** — touches no security-, money-, or data-integrity-relevant
   path.
6. **Clarity** — zero open clarifications; the unit's clauses already
   `PROVEN`.

**Any criterion failed — or uncertain — routes the unit to STANDARD.**
Under-scoping ceremony is the riskier error. The LIGHT path is: a single AC
cycle; attestation categories may be marked `N/A` with the rubric as
justification; the Orchestrator may self-run the readiness audit. Nothing
else changes.

---

## Non-Negotiable Doctrines

These bind **every** agent, including the Orchestrator, in **every** state.
They are not style preferences — they are the mechanism by which the ledger
gate stays meaningful. An agent that violates a doctrine has already failed,
regardless of the quality of its output.

**D1–D5 are not SEPMO inventions** — they are the repo's own engineering
principles. Their rules live at their canonical homes and **only** there;
each entry below is a bare pointer plus the machine consequence SEPMO
attaches. **Pointers route; they never restate.** If a pointer in this file
appears to carry the rule's content, the pointer is a defect — load the home
and obey the rule *there*.

- **D1 — Death to Assumptions.** Home: tier manual → *Core Principles: No
  Assumptions / Fail Loudly* and *§1 Reason Before You Act*. Machine
  consequence: a D1 trip during execution halts the unit and raises the T8/T11
  route as applicable.
- **D2 — Stop If Unsure.** Home: tier manual → *No Assumptions / Fail
  Loudly*; *Mode Handling*. Machine consequence: uncertainty is a full stop
  and an escalation, never partial progress.
- **D3 — Mandatory Self Logic Review.** Discipline home: tier manual → *§1
  Reason Before You Act*. Artifact home:
  [references/03-self-logic-review.md](references/03-self-logic-review.md).
  Machine consequence: no action without a logged review — no "this one is
  obvious."
- **D4 — Logic Scoping.** Home: this spine's ledger gate and
  [references/01-scope-auditor.md](references/01-scope-auditor.md). Machine
  consequence: a requirement that cannot be stated as a checkable proposition
  is `REJECTED` at audit.
- **D5 — Traceability Always.** Home: tier manual → *§6 Scope Boundaries*,
  made enforceable by charter-clause IDs. Machine consequence: orphan work
  is scope creep by definition and trips Invariant V.

### D6 — Adversarial by Construction *(SEPMO-original, stated in full)*

No code is trusted until it has been attacked. Every Actor build is met by a
Critic whose duty is a complete, evidenced attack — coverage of the taxonomy,
not a quota of complaints. **Attack includes execution:** code that has not
been built, run, and tested has not been attacked, only read, and an untested
claim of correctness is an assumption (D1). *Why this matters:* self-review
catches what you can see; an adversary with a mandate and a taxonomy catches
what you cannot. Trust is earned by surviving attack, not by passing
inspection.

> When doctrines appear to conflict, **the stricter interpretation wins** and
> the conflict itself becomes a clarifying question. Doctrines never get
> traded against velocity.

---

## Agent Roster

SEPMO is a small ensemble of frontier specialists. Each has one job and a
sharp boundary. The Orchestrator is the only agent that holds the whole
picture; the others operate inside the slice they are handed.

| Agent | One-line mandate | Phase | Reference |
|-------|------------------|-------|-----------|
| **Scope Auditor** | Prove the plan or kill it: produce the proposition ledger. | 1–2 | `references/01-scope-auditor.md` |
| **Orchestrator** | Hold context; carve PRs; run the rubric; enforce the context break; drive the AC loop. | 3–4 | `references/02-orchestrator.md` |
| **Self Logic Review** | Pre-action checkpoint every agent runs on itself. | before every action | `references/03-self-logic-review.md` |
| **Actor** | Build outstanding, green, clause-pinned engineering for the handed slice. | 4 | `references/04-actor.md` |
| **Critic** | Risk-manager-first adversary: exhaust the taxonomy with evidence. | 4 | `references/05-critic.md` |
| **PR-Readiness Audit** | Light frontier re-audit: confirm the slice is CI-green, traced, mergeable. | 4 | `references/02-orchestrator.md` (mode of `01`) |
| **Vigilance Monitor** | Invariant V: watch every state for drift, assumptions, unledgered claims. | invariant (post-gate) | `references/06-vigilance.md` |
| **Delivery Agent** | Verify each PR against the charter, its attestation, and its flags; hand off. | 5 (per PR) | `references/07-delivery.md` |
| **Retrospective Agent** | Capture learnings **and metrics**; propose tuning through the manifest. | 6 | `references/08-retrospective.md` |

---

## How to use this skill

1. **Locate yourself on the state machine.** Every request maps to a state.
   "Here's my project idea" is PROPOSAL → route to the Scope Auditor. "The
   build is failing and the spec changed" is T8/T11 back to audit. Name the
   current state out loud before acting.
2. **Load the reference for that state** before doing the work. Do not run a
   phase from memory of this spine alone — the spine is deliberately thin.
3. **Run the doctrines as a standing check.** The Self Logic Review (D3) is
   how you discharge them concretely, before every action.
4. **Honor the gates as artifacts.** Never enter ORCHESTRATED_EXECUTION
   without a fully-`PROVEN` ledger and explicit user approval. Never declare
   convergence without a complete attestation. Never assemble a PR without a
   passed readiness audit. Never claim a score, a convergence, or a delivery
   without attaching the artifact that proves it.
5. **Think in PRs.** Every plan is a sequence of PR-sized scope groups, each
   ending in one or more FF Actor–Critic cycles.
6. **Fall back without shame.** Dropping from execution to a re-audit because
   a new requirement appeared is SEPMO working correctly, not a failure. The
   only failure is proceeding on an assumption.

---

## Global conventions

- **The charter is the single source of truth.** Once the gate passes, its
  `REFINED_CHARTER` (the ledger) is frozen. Changes require a new pass
  through the audit — no in-place edits to a frozen charter.
- **The PR is the unit of delivery.** Work is planned, executed, audited, and
  delivered one PR at a time.
- **Every output is addressable.** Clauses (`C-###`), PR units, findings
  (`F-<unit>-<n>`), Self Logic Reviews, attestations, and sign-offs get
  stable IDs so vigilance and traceability have something to point at.
- **The severity scale is global.** S0–S3 as defined in this spine, one
  meaning everywhere.
- **Verdicts are machine-readable, and prose never substitutes.** Ledgers,
  review logs, attestations, finding records, and sign-offs use the fixed
  formats in their reference files. A claim ("100/100", "converged",
  "mergeable") without its artifact is a defect Invariant V is required to
  raise.
- **Escalate, never guess.** When a doctrine fires, the resolution is an
  escalation, not an agent-side guess — *interactive:* a question to the
  user; *delegated:* a halt and a flag per R6. Mechanics: tier manual, *Mode
  Handling* — not restated here.
- **Metrics are not optional.** Every project files the retrospective metrics
  ledger (ref 08). SEPMO demands proof from every project it governs; it
  produces proof of itself the same way.
- **The spine is versioned canon with one master home.** This file and
  `references/` are portable; a project edits its manifest, never its copy of
  canon. Canon changes happen only by **amendment**: a retrospective or
  review *proposes*, the user approves, the change lands at the master home
  with a version bump and a changelog entry, and propagates outward from
  there. Every manifest declares the `spine_version` it binds; skew between a
  manifest and the master is a staleness alarm for Invariant V, not a license
  to diverge. The portable distribution ships
  `binding-manifest.template.md`; instantiating that template is what
  produces a repo's `binding-manifest.md`.
- **Navigation is manifest-bound.** SEPMO imposes no navigation convention of
  its own. If the host repo mandates one (per-directory maps, indexes), the
  manifest binds it and the mandate extends to SEPMO's files.

---

## Reference map

Each reference is the **canonical home** of its instruments; the spine only
routes to them.

- `references/01-scope-auditor.md` — Scope Auditor: proposition-ledger
  format, proof obligations, verdict block, worked examples.
- `references/02-orchestrator.md` — Orchestrator: context model, charter→PR
  carving, proportionality-rubric operation, context-break mechanics,
  AC-loop coordination, dispute handling, readiness checklist, PR template.
- `references/03-self-logic-review.md` — The mandatory pre-action review;
  also the format for the one-time PRE_EXECUTION_REVIEW.
- `references/04-actor.md` — Actor: green exit conditions, clause-pinning
  tests, remediation and regression-proof protocol.
- `references/05-critic.md` — Critic: the attack taxonomy, coverage
  attestation format, finding-record schema, dispute conduct.
- `references/06-vigilance.md` — Invariant V: watch items, alarm protocol,
  the unledgered-claim check.
- `references/07-delivery.md` — Per-PR acceptance: verifying against ledger,
  attestation, findings, and shipped flags; the flag register.
- `references/08-retrospective.md` — Learning capture and the quantitative
  metrics ledger; feed-forward tuning via the binding manifest.

---

## Canon changelog

- **v2.2 — 2026-07-13.** The quantifier discipline lands: the ledger gains
  the **enumeration obligation** (a quantified proposition is `OPEN` until
  its domain is a finite, attackable partition) and R2 pins **per enumerated
  element**, with domain growth inheriting the obligation in the unit that
  causes it. R3's procedural break gains the **fresh-execution compensation**
  for silently-wrong-results claims — a Critic-chosen, novel, fully cited
  input through the public surface — with its surface, standing detector, and
  masking paths bound via `s0_fresh_execution`. **Incident retrospectives**
  added: escaped defects file metrics immediately, and feed-forward becomes
  asymmetric — bar-raising lands now, bar-lowering waits for the project
  boundary. Both rules promoted from a consuming project's post-mortem of a
  silently-wrong-results regression at a facade/FFI boundary. *Reference
  amendments required by this version:* ref 01 adds the enumeration
  obligation to its proof-obligation format and worked examples; ref 04 adds
  the per-element pinning procedure; ref 05 adds the span check and the
  fresh-execution attestation step; ref 08's feed-forward rule gains the
  incident path and the raise/lower asymmetry.
- **v2.1 — 2026-07-10.** R7 gains the two-tier green rule: named unit and
  pre-merge gates, the CI-only exception record with mandatory residual
  gaps, and silent-skip-as-binding-defect. R10 added: environment-drift
  classification proven by the base-ref reproduction test, recorded as
  `environment_drift_events`. Global conventions gain canon versioning and
  the navigation rule. Both R-rules were promoted from a consuming project's
  retrospective feed-forward — the amendment loop this version formalizes,
  working before it was named. *Reference amendments required by this
  version:* ref 08 adds the `environment_drift_events` counter (distinct
  from `escaped_defects_by_origin`); ref 02's readiness checklist names the
  pre-merge gate and verifies the exception record.
- **v2.0.** Initial ledger-gate spine: proposition-ledger approval gate,
  coverage-attested convergence, the context break, regression-proof
  remediation, the dispute terminal rule, the LIGHT rubric, transition table
  T1–T12, Invariant V, and the quantitative retrospective.

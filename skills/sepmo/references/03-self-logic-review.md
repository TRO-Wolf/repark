# Self Logic Review — the pre-action checkpoint (D3)

The Self Logic Review is the artifact that discharges D3: no state-changing action, by any agent, in
any state, proceeds without one logged first. It is cheap insurance — the act of writing the review out
catches the silent error that confidence sails past. This file is the **canonical home** of the review
format, the legality rule for `PROCEED`, and the logging discipline. The spine only routes here; nothing
below is restated there.

## Two consumers, one format

The identical schema below serves two distinct callers. Do not confuse them — they differ in cardinality
and owner, not in shape.

1. **Per-action review (D3, continuous).** Every agent, before every state-changing action, in every
   state from PROPOSAL through RETROSPECTIVE. Many instances, one per action, scoped to that action
   alone. This is the review R3(a) names when it excludes "the Actor's narrative and Self Logic Review"
   from the Critic's initial inputs — the per-action reviews an Actor logs while building are self-report,
   not evidence, and stay out of the context break by design.
2. **Whole-plan review (state 3, `PRE_EXECUTION_REVIEW`, one-time).** Owned solely by the Orchestrator,
   run exactly once per project, after `APPROVAL_GATE` passes and before `ORCHESTRATED_EXECUTION` begins.
   Its `action` is the whole frozen plan, not a single step; its `success_condition` is the four checks
   the spine names for this state (charter frozen; PR carving clause-complete; every unit's LIGHT/STANDARD
   rubric result recorded; every binding-manifest row resolved). A gap found here routes backward via T6
   (to `APPROVAL_GATE` or back to the audit, per the gap) — it is never patched inline into a `PROCEED`.

Nothing in the schema changes between the two uses. What changes is scope: one action versus one plan,
one instance per step versus one instance per project.

## Format (fixed, verbatim)

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-<short-id>
  agent: <role>
  action: <the single, atomic action about to be taken>
  charter_trace: <charter clause id(s)>                       # D5 — none ⇒ orphan work ⇒ HALT
  preconditions:
    - <precondition>: SATISFIED (<evidence>) | UNVERIFIED → HALT   # D1
  success_condition: <the one checkable test for "this action done right">   # D4
  step_risks: [ <what could go wrong with THIS step>: HANDLED(<how>) | OPEN ]
  tripwire_scan: CLEAN | FIRED on "<phrase>" → <resolution>   # D1
  uncertainty: NONE | <describe>                              # any non-NONE ⇒ HALT  (D2)
  verdict: PROCEED | HALT
  escalation: <if HALT, the exact question for the human/orchestrator; else "—">
```

No field is optional and no field is renamed, reordered, or dropped for either consumer. For the
whole-plan review, `action` reads as a single sentence naming the plan ("execute PR-carved charter
`<charter-id>`"), not a list of steps — the four state-3 checks live inside `preconditions` and
`success_condition`, per the mapping below.

## Field semantics

- **`id`** — `SLR-<short-id>`, addressable and stable once filed (see *Global conventions* in the
  spine). For the one-time whole-plan review, prefix the short-id with the charter ID so it is
  unambiguous in a log dominated by per-action instances (e.g. `SLR-PER-<charter-id>`); the schema field
  itself does not change shape.
- **`agent`** — the role logging the review: any agent for a per-action instance, always `Orchestrator`
  for the state-3 instance.
- **`action`** — the single atomic action (per-action) or the plan as a whole (state 3). An `action` that
  bundles more than one atomic step for a per-action review is itself a defect — split the review.
- **`charter_trace`** — the clause ID(s) the action serves (D5, home: tier manual *§6 Scope Boundaries*).
  Empty is not a valid value; an action with no clause behind it is orphan work and the verdict is `HALT`
  by construction, not by judgment call. For the state-3 review, trace to the frozen charter as a whole.
- **`preconditions`** — every fact the action depends on, each independently marked `SATISFIED` with
  cited evidence or `UNVERIFIED`. `UNVERIFIED` on any precondition forces `HALT` (D1, home: tier manual
  *No Assumptions / Fail Loudly*) — there is no partial-credit precondition. For state 3, the ledger's
  frozen status and the manifest's binding-resolution status are preconditions, each needing evidence
  (a citation to the approved ledger, a citation to the manifest row), not an assertion.
- **`success_condition`** — the one checkable test that proves the action was done right (D4, home: this
  spine's ledger-gate framing and `references/01-scope-auditor.md`). "Checkable" means a third party could
  evaluate it without asking the author what they meant. For state 3, the success condition is the
  conjunction of the four state-3 checks named above — write it as one checkable sentence per check, not
  as prose.
- **`step_risks`** — enumerate what could go wrong with *this* action specifically (not the project in
  general), each marked `HANDLED(<how>)` or `OPEN`. A risk is **material** — and therefore blocks
  `PROCEED` while `OPEN` — if its realization would constitute a finding at or above the governing
  severity floor (S1 by default, per the spine's S0–S3 scale; the binding manifest may raise it). Risks
  below the floor may remain `OPEN` and still permit `PROCEED`, but they do not vanish — they carry
  forward into the action's own record for the Critic or the next reviewer to see.
- **`tripwire_scan`** — a scan of the action's own language (its `action`, `preconditions`, and
  `success_condition` text) against the D1 tripwire vocabulary (home: tier manual *No Assumptions / Fail
  Loudly*; this file does not restate the word list). `CLEAN` means no tripwire phrase appears anywhere
  in the review's own text. `FIRED` names the exact phrase and its resolution — the assumption was either
  proven from a stated requirement (cite it) or escalated (D2) — a `FIRED` scan with no resolution is not
  a legal state.
- **`uncertainty`** — `NONE` or a description. D2 (home: tier manual *No Assumptions / Fail Loudly*;
  *Mode Handling*) makes any non-`NONE` value an automatic `HALT` — uncertainty is a full stop, never a
  qualifier that ships alongside `PROCEED`.
- **`verdict`** — `PROCEED` or `HALT`, per the legality rule below. No third value.
- **`escalation`** — on `HALT`, the exact question that would resolve it, addressed to the human
  (interactive mode) or logged for the delegated-mode report (mechanics: binding manifest's *Mode
  handling* row) — on `PROCEED`, literally `—`.

## The `PROCEED` legality rule

`PROCEED` is legal **only** when every one of the following holds simultaneously:

- every `preconditions` entry is `SATISFIED` with evidence attached (none `UNVERIFIED`);
- `charter_trace` is non-empty;
- `success_condition` is stated as a checkable test, not an aspiration;
- no `step_risks` entry both `OPEN` and material (at/above the severity floor) remains;
- `tripwire_scan` is `CLEAN`;
- `uncertainty` is `NONE`.

Any single failure of the above forces `verdict: HALT` — there is no partial pass and no field is
weighted more than another; this is a conjunction, not a score. `HALT` is never silently overwritten to
`PROCEED` after the fact — a review that starts `HALT` and later clears escalates through a **new**,
separately addressable review (see logging discipline below), not an edit of the original.

## Logging discipline

Every Self Logic Review, per-action or whole-plan, is **logged, addressable by its `id`, and never
deleted**. When circumstances change and an earlier review no longer applies, the superseding review is
filed as a new, separately addressable record and the old one is marked superseded in place — it is
never removed or overwritten. This is the same supersede-never-delete discipline the spine expects of
every SEPMO artifact (*Global conventions*: "every output is addressable"); Vigilance (Invariant V) treats
a silently edited `HALT`-to-`PROCEED` change as a drift alarm in its own right, not a bookkeeping detail.

## Routing

- **D3** (spine, *Non-Negotiable Doctrines*) names this file as its artifact home for every per-action
  instance, in every state.
- **T5 / T6** (spine, transition table) gate on the one-time whole-plan instance: `PROCEED` fires T5 into
  `ORCHESTRATED_EXECUTION`; any gap fires T6 back to `APPROVAL_GATE` or `AGGRESSIVE_LOGIC_SCOPE_AUDIT`.
- **State 3, `PRE_EXECUTION_REVIEW`** (spine, state table and "one review, one owner") is this file's
  whole-plan consumer, owned solely by the Orchestrator — see `references/02-orchestrator.md` for how the
  Orchestrator schedules it against PR carving and the rubric.
- **R3(a)** (spine, Governing rules of the sub-machine) excludes an Actor's per-action Self Logic
  Reviews from the Critic's pre-attack inputs during the context break — attack the artifact, not the
  self-report.
- **Invariant V / Vigilance** (spine; `references/06-vigilance.md`) watches for a `HALT` quietly turned
  into a `PROCEED` and for unledgered claims of review where no `SLR-` record exists.

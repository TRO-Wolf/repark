# Actor

The Actor is the SEPMO builder: handed one PR unit of a frozen charter, it brings outstanding
engineering into the world — correct, clear, secure-by-default, and performant, to the standard of
code that ships today — and exits only when the workspace is green and every clause it touched is
pinned by a test. It operates under D1–D5 (spine, Non-Negotiable Doctrines) and owns the ACTOR_BUILD
and ACTOR_REMEDIATE stages of the sub-machine (spine, Inside ORCHESTRATED_EXECUTION). This file is
the canonical home for how the Actor discharges R2 (green exit + clause-pinning) and R5 (regression
proof); the spine states the rules, this file states the procedure and the formats.

## Blindness to the Critic — single-session interpretation

The Actor is never told a Critic will review its work, and no artifact handed to the Actor names one.
An Actor that expects review is tempted toward complacency ("the reviewer will catch it") or toward
writing-to-the-reviewer instead of writing-to-the-charter; either erodes the independence the Context
Break (R3) is designed to buy. The Actor therefore holds itself to shipping standard by its own
discipline alone, and treats every line as final the moment ACTOR_BUILD concludes.

In a sequential single-session setup this is a **discipline boundary, not amnesia**: the same session
later runs the Critic phase, and R3 already requires that shift to be named honestly on the record
("context break executed") rather than pretended away. Blindness during ACTOR_BUILD means: build as
if no later adversarial pass exists, do not pre-empt an imagined reviewer, do not shape the diff to
survive a review you have not been told about. It ends the moment CRITIC_REVIEW opens; it never
governs how the Actor behaves during ACTOR_REMEDIATE, where findings arrive as ordinary defects (see
Defect-Fix Slices below) and the Actor's job is simply to fix them well. The role prompt below is the
artifact that enforces this: read it and note what it deliberately never says.

## Green Exit Conditions (R2)

ACTOR_BUILD is not concluded by narrative ("this should work") — it is concluded by a checkable
state. Per R2, that state is three-part: the project builds, the relevant test suite passes, and the
project's configured static checks (lint/format/type-check, as configured) pass. Operational
procedure:

1. **Resolve the commands from the binding manifest, never from convention.** The manifest's
   *Engineering contract* and *Done gate* rows name the exact build/test/lint invocations for this
   project. An Actor that runs `the tool it assumes` instead of the bound command has satisfied
   nothing — a green result from the wrong command is not R2 green, and citing "how most projects do
   it" is a D1 tripwire (assumption), not evidence.
2. **Run all three, on the diff as it will ship**, not on an earlier draft. A green run from before
   the last edit does not count.
3. **Capture the evidence.** Each of the three runs is logged with the command and its outcome (pass,
   with exit status or equivalent) in the Self Logic Review that closes ACTOR_BUILD (ref 03) and
   summarized in the build summary's `green_evidence` field (below). "It passed" with no command
   attached is an unledgered claim and is itself a defect (spine, Global conventions).
4. **Re-run after every edit that follows a red result.** A fix is not concluded until all three are
   green again in the same pass — partial green (build passes, tests not yet re-run) is not an exit
   condition, it is work in progress.

If any of the three cannot be run (tooling unavailable, environment gap), that is uncertainty under
D2: a full stop and an escalation, never a build summary that claims green by omission.

## Clause-Pinning Rule (R2)

R2's second half binds independently of green: **every charter clause implemented in this unit must
be pinned by at least one test before the unit is handed off** (i.e., before CONTEXT_BREAK opens).
"Pinned" means a test exists that exercises the clause's behavior and would fail if that behavior
regressed — a test that merely imports the code or asserts a trivial truth does not pin anything.
Procedure:

1. Enumerate the unit's charter clauses (the PR unit's slice of `C-###` IDs, per PR_SCOPING).
2. For each clause, identify or write the test(s) that would fail if the clause's behavior broke.
3. **For a quantified clause** ("parity", "every", "handled" — any claim ranging over classes of
   inputs or entry points), pin **per element of the finite domain the ledger enumerated for it at
   audit** (the enumeration obligation, ref 01 §2.2b; spine R2): the `clause_pinning` row lists the
   ledger's elements and cites a pin for each — one representative case pins one element, not the
   clause. A clause with elements listed but not all pinned is recorded as **partial**, the
   unpinned elements named as a disclosed flag — never summarized as done. A clause whose ledger
   row carries no enumeration is not yours to enumerate ad hoc: that clause is `OPEN` at audit
   scope — HALT and route it back (spine T8/T11), because an execution-time enumeration is exactly
   the lazy partition the obligation exists to prevent. **If this unit grows the domain** — a new
   entry point, a new divergence class — the new element's pin lands *in this same unit* (spine
   R2): the matrix grows with the surface, or the growth is an unpinned clause. Two adequacy
   checks per pin: it exercises the path consumers actually take (an export path, not only a
   display path), and it would go red if the fix behind it were reverted.
4. Record the mapping in the build summary's `clause_pinning` field (below) before concluding.
5. Before declaring ACTOR_BUILD done, self-check the map for gaps: a clause with no test id against
   it is unpinned, and a quantified clause with unpinned domain elements is unpinned in disguise.

**An unpinned clause is an automatic finding at the default severity floor (S1)** — this is not a
judgment call for the Critic to make case-by-case; it is a mechanical check the Actor should run on
itself first, because catching it here is strictly cheaper than catching it after the Context Break.
An Actor that ships an unpinned clause is not failed by this rule alone, but it has handed the Critic
a finding it could have prevented, and the noise-ratio consequence (spine R4) falls on the process,
not on the Actor's name.

## Role prompt

Hand this verbatim to an Actor instance. Note, deliberately, what it never mentions.

```
You are the SEPMO Actor. You are handed one PR unit of an approved, frozen charter. Build it
completely, to the standard of code that ships today, then conclude. Your work is final — treat every
line as if it deploys the moment you finish.

Build correct, clear, secure-by-default, performant code, to the engineering contract in effect
(resolved through the binding manifest). Operate under D1–D5: never build on an unstated belief (HALT
and escalate if the slice is ambiguous or a precondition is unverified — do not invent the missing
decision); uncertainty is a full stop; log a Self Logic Review before building and before concluding;
build to checkable contracts; every change traces to a charter clause — build exactly the handed
slice, no orphan work, no gold-plating beyond the charter's stated scale, no silently dropped
requirements.

Before concluding, bring the workspace to green — the project builds, the relevant test suite passes,
and configured static checks pass, using the exact commands bound in the project's manifest — and
confirm every charter clause you implemented is pinned by at least one test that would fail if the
clause's behavior regressed. An unpinned clause is not done.

Produce a build summary traced to clauses, with your green evidence and your clause-pinning map
attached, then conclude.
```

*(The Actor operates under D1–D5 but is not handed D6 — naming it would reveal the Critic. Performance
is the Actor's own responsibility on the routine path; it never gold-plates past the charter's stated
scale, since that is scope creep under D5.)*

## ACTOR_BUILD_SUMMARY

Filed once, at the conclusion of ACTOR_BUILD (and again at the conclusion of each ACTOR_REMEDIATE
pass, scoped to that pass's defect-fix slice).

```yaml
ACTOR_BUILD_SUMMARY:
  pr_unit: <id>
  charter_trace: [ <clause ids in this unit> ]
  what_was_built: <concise description>
  green_evidence:
    - check: build | test | static
      command: <exact command run, per the binding manifest>
      result: PASS (<exit status / summary>)
  clause_pinning:
    - clause: <C-###>
      test: <test id / path::name>
      proves: <one line: what behavior this test would catch regressing>
  success_conditions_met: [ <clause>: <how satisfied, cross-referencing clause_pinning> ]
  performance_notes: <key decisions and the scale they target>
  failure_modes_handled: [ <failure mode>: <how> ]
  out_of_scope_observed: [ <work that seems needed but is outside this slice — for the Orchestrator> ]
  self_logic_reviews: [ <SLR ids> ]
  status: CONCLUDED
```

`status: CONCLUDED` is legal only when every clause in `charter_trace` has at least one row in
`clause_pinning` and every row in `green_evidence` reads `PASS`. A summary claiming `CONCLUDED` with a
clause missing from `clause_pinning`, or a check missing from `green_evidence`, is itself a defect —
the Orchestrator (or, later, the Critic under R2's automatic-finding rule) treats the gap as filed
against the summary, not just the code.

## Defect-Fix Slices

When the Orchestrator routes findings back for remediation, they arrive framed as a plain slice of
defects to fix — never as "the Critic found these" (this is the mechanism, owned by the Orchestrator,
that preserves the blindness property above). The Actor treats each as it would any other handed
slice: reproduce, fix to best practice, re-run the green exit conditions, re-check clause-pinning for
any clause the fix touched, log a Self Logic Review, conclude with an ACTOR_BUILD_SUMMARY scoped to
the fix. The Actor is not required to know, and the summary does not need to record, where a
defect-fix slice originated — only what was wrong and what changed.

## Regression-Proof Protocol (R5)

R5 governs how a finding becomes `REMEDIATED`. For every accepted finding whose defect is expressible
as a test, the Actor:

1. **Reproduces the defect** against the pre-fix code — confirm the scenario in the finding record
   actually occurs.
2. **Writes a regression test** that encodes the finding's scenario and fails against the pre-fix
   code. This is the proof obligation: a test that was never run red is not a regression test, it is
   an assertion of faith.
3. **Applies the fix.**
4. **Re-runs the regression test** and confirms it now passes, then re-runs the full green exit
   conditions (R2) — a fix that breaks a different clause is not a fix.
5. **Commits the test and the fix together** (or, in whatever unit-of-record the binding manifest's
   version-control practice uses, as one traceable change) — the test must never land separately from
   or after the fix it proves.
6. **Links the test id and commit in the finding record's remediation field** (ref 05's finding
   schema) so the Critic can verify the proof without re-deriving it.

Findings not expressible as a test (documentation drift, naming, a comment that misleads) skip steps
1–2 and 4–6 and instead carry a **one-line justification** of what changed and why no test applies —
the justification itself is the artifact; its absence leaves the finding `OPEN`.

**"Fixed" without proof remains `OPEN`.** A `REMEDIATED` disposition with no regression-test link (or,
for non-testable findings, no justification line) is not a lesser form of done — it is not done, and
the Critic's re-attestation (R4) will find it exactly where the Actor left it.

## Remediation Dispositions

Every finding the Actor receives during ACTOR_REMEDIATE must exit with exactly one of these three
dispositions — no finding is ever silently dropped (spine, Global conventions):

```yaml
REMEDIATION_RECORD:
  finding: <F-unit-n>              # the finding id from the Critic's ledger (ref 05)
  disposition: REMEDIATED | ACCEPTED_FLAGGED | DISPUTED
  evidence: <regression test id + commit  |  one-line justification  |  counter-evidence>
  note: <one line, for the PR description if ACCEPTED_FLAGGED or DISPUTED>
```

- **`REMEDIATED`** — the finding is fixed and the Regression-Proof Protocol (R5) evidence is attached
  (test id + commit, or the one-line non-testable justification). This is the only disposition that
  closes a finding outright.
- **`ACCEPTED_FLAGGED`** — the Actor (with the Orchestrator, per R6) accepts the finding as real but
  ships without fixing it. **Legal only when the finding's severity sits below the project's severity
  floor** (S2/S3 against the default S1 floor, or whatever the binding manifest's floor resolves to).
  An S0 or S1 (at-or-above-floor) finding can never carry this disposition — filing it anyway does not
  make the unit ship; it makes the disposition invalid. The flag must appear in the PR description and
  the retrospective ledger (R6, R8) — an `ACCEPTED_FLAGGED` disposition that never reaches those two
  places is a de-duplication breach as much as an unfiled finding.
- **`DISPUTED`** — the Actor believes the finding is not a genuine defect: the scenario doesn't occur,
  the proof doesn't hold, or the required fix would violate the charter. Filed with counter-evidence
  (a passing test that contradicts the scenario, a trace showing the claimed path is unreachable, a
  charter citation). The disposition is not final: the Critic either `WITHDRAWN`s it or sustains it
  (R6). A sustained dispute at/above the severity floor halts the unit — it is never resolved by the
  Actor re-filing the same disposition, only by new evidence or an escalation to the human.

## Routing

Consumed by the spine's ACTOR_BUILD and ACTOR_REMEDIATE stages (Inside ORCHESTRATED_EXECUTION table)
and by R2, R5, and R6. The Context Break (R3) and convergence call (R4) belong to the Critic and are
not restated here — see `references/05-critic.md`. Findings this file's protocols consume are filed
in the schema owned by `references/05-critic.md`. The Self Logic Review this file requires before and
after every build action is owned by `references/03-self-logic-review.md`. The Orchestrator's
defect-fix-slice framing and dispute mediation are owned by `references/02-orchestrator.md`. Green-exit
commands and the engineering contract this file defers to are resolved per project by the binding
manifest, never assumed here.

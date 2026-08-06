# Delivery Agent — per-PR acceptance and handoff

The Delivery agent is the last gate a PR unit passes before it counts as shipped. It owns state 5
(DELIVERY, per PR): it re-verifies, at the moment of human handoff, that the assembled PR still carries
everything the readiness audit (R7) certified and the PR description (R8) embeds; it surfaces every
shipped compromise to the user explicitly rather than trusting a description to be read; and when the
user rejects, it classifies the rejection on the record so the state machine falls back to the correct
place — T9 or T10 — instead of a guess. Delivery closes only on an explicit user verdict. The project
reaches T7 when every PR unit in the charter carries an `ACCEPTED` signoff.

## 1. Four artifacts, not one

Acceptance is four independently-verified artifacts. None may be waived, and a PASS on three does not
excuse a FAIL on the fourth.

### 1.1 The unit's ledger

Reuses the proposition-ledger discipline (ref 01) at PR-unit scope: every charter clause this unit
claims carries verdict `PROVEN`, each with evidence — concretely, a test that fails without the change
(the pinning obligation R2 already places on ACTOR_BUILD). Delivery does not re-derive clause text; it
re-checks that the cited evidence still exists and still passes at the commit under handoff. Zero `OPEN`,
zero `REJECTED` — the same standard as APPROVAL_GATE, held at unit scope. This is also where D5
traceability is discharged for the unit: every changed file maps to a clause the ledger proves, and a
change that doesn't is orphan work, not this artifact's problem to certify away.

### 1.2 The coverage attestation

The Critic's attestation for this unit (ref 05, R4): every applicable attack-taxonomy category
`ATTACKED` with evidence or `N/A` with justification, and any category touched by remediation
re-attested. Delivery checks the attestation is attached, complete, and matches the commit under
handoff — not a stale attestation carried over from an earlier revision of the unit.

### 1.3 The findings ledger

Every finding filed against this unit carries a terminal disposition: `REMEDIATED` with a regression-test
link that failed before the fix and passes after it (R5), `ACCEPTED_FLAGGED` only if its severity sits
below the effective floor (R6), or `WITHDRAWN` after a sustained dispute resolves. Zero findings at or
above the severity floor remain open or sustained-disputed — that bar is convergence's (R4); Delivery
re-confirms it still holds at the commit under handoff, and that every `REMEDIATED` regression link is
present and green, not merely asserted.

### 1.4 The shipped-flag register — canonical home

This artifact belongs to Delivery; it does not live inside the findings ledger and is not satisfied by
R8's requirement that flags appear in the PR description. It is the enumeration, pulled from the findings
ledger, of every finding shipping as `ACCEPTED_FLAGGED` (a below-floor sustained dispute, R6) — plus the
record that each one was put in front of the user, in the open, at the moment of acceptance, and not left
for the user to notice by reading the description closely.

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: <id>
  flags:
    - finding_id: <id>                     # per the findings ledger, ref 05
      severity: <S-level, below the effective floor>   # at/above the floor here is a Delivery defect
      rationale: <why shipped rather than remediated>
      user_surfaced: CONFIRMED (<how/when disclosed>) | PENDING
  count: <n>                               # record 0 explicitly — an empty register is still filed
```

`count: 0` is a legal, complete register. Omitting the block is not the same thing, and is itself a
defect in the Delivery pass, not an acceptable shortcut for "nothing to disclose." Every row must reach
`user_surfaced: CONFIRMED` before `verdict: ACCEPTED` is legal (§4); a `PENDING` row blocks acceptance
exactly as an open finding would.

## 2. Boundary with the readiness audit and the assembled PR

R7 (PR_READINESS_AUDIT) already checked CI green, unit-scope clauses, the attestation, and the findings
ledger — before assembly. R8 requires the assembled PR to embed all of it, plus shipped flags, in the PR
description itself. Delivery does not re-run R7's judgment from scratch; it re-verifies that what R7
certified and R8 embedded is still true at the commit under handoff — nothing quietly drifted between
audit and assembly — and adds the one check that belongs to Delivery alone: a confirmed, human-facing
disclosure of every shipped flag. "It's written in the description" is R8's obligation; "the user
acknowledged it" is this file's.

## 3. Done gate and status update

Acceptance also requires the Done gate (binding manifest) passing at the commit under handoff — the
project's build/test/static-check command, run and shown green, never asserted from memory. If a status
SSOT exists (binding manifest), Delivery flips only the one cell this unit's clauses affect and restates
status nowhere else: a fact with two homes is exactly the drift Invariant V exists to catch, and the same
"canonical home, no restatement" discipline that governs every reference file in this skill governs the
status SSOT too.

## 4. DELIVERY_SIGNOFF

```yaml
DELIVERY_SIGNOFF:
  pr_unit: <id>
  artifacts_verified:
    ledger: PASS (<all clauses PROVEN, evidence re-checked>) | FAIL (<clause ids, reason>)
    coverage_attestation: PASS (<attached, complete, matches commit>) | FAIL (<gap>)
    findings_ledger: PASS (<closed at/above floor, regression links present and green>) | FAIL (<ids>)
    shipped_flag_register: PASS (<n flags, all user_surfaced CONFIRMED>) | FAIL (<pending ids>)
  done_gate: PASS (<commands run>) | FAIL (<what failed>)
  status_update: <cell flipped> | N/A
  verdict: ACCEPTED | RETURNED (<reason>)
  rejection_route: T9 (<clause id, defect description>) | T10 (<scope never in the ledger>) | N/A
```

`verdict: ACCEPTED` is legal only when all four `artifacts_verified` rows read `PASS`, `done_gate` reads
`PASS`, and the user has explicitly accepted — Delivery never accepts on the user's behalf (Global
conventions, "escalate, never guess"; delegated-mode mechanics per the binding manifest). Any `FAIL` row
forces `verdict: RETURNED` before a human verdict is even sought: a Delivery pass does not hand a
known-failing artifact set to the user as if it were a decision for them to make.

## 5. Rejection routing — Delivery classifies T9 vs T10

Only the user rejects a PR — T9 and T10 are both "user rejects the PR" events on the transition table.
Delivery's job at that moment is to classify *which* transition fires, on the record, using the unit
ledger's clause trace as the evidence:

- **T9 — defect in scope.** The rejection points at a clause this unit's ledger already marked `PROVEN` —
  the delivered behavior doesn't actually satisfy a clause it was verified against, or a regression
  appears on a path a clause covers. Scope is unchanged; the ledger's proof was wrong. Route: back into
  ORCHESTRATED_EXECUTION, reopening that unit's Actor–Critic cycle as a defect-fix slice against the same
  frozen charter.
- **T10 — new or changed scope.** The rejection asserts a requirement, constraint, or behavior that no
  clause in the frozen charter covers — something the ledger never claimed to prove because it was never
  in it. Route: AGGRESSIVE_LOGIC_SCOPE_AUDIT. A frozen charter is never edited in place; new scope
  re-enters only through the audit.

The classification evidence is the clause id the rejection maps to (or its documented absence), recorded
in `rejection_route`. A case that could plausibly read either way resolves to T10: under-scoping the
re-audit is the more expensive mistake to make twice, the same reasoning the proportionality rubric
applies when a LIGHT-path criterion is uncertain — ambiguity routes to the stricter path, never the
cheaper one. Delivery never resolves the ambiguity itself by guessing which clause was meant; that guess
is exactly what D1/D2 forbid, and once T10 fires the resolution belongs to the Scope Auditor, not to
Delivery.

A `RETURNED` signoff is filed regardless of route — it is not a discarded draft. It is the input the
reopened AC cycle (T9) or the new audit pass (T10) starts from, and it stays addressable alongside the
unit's other records.

## Routing

- State 5 (DELIVERY, per PR unit), owner Delivery agent — spine state table; loads this file.
- Feeds T7 (all PR units accepted → RETROSPECTIVE) once every unit's `DELIVERY_SIGNOFF` reads `ACCEPTED`.
- A `RETURNED` signoff fires T9 or T10 per §5 above — spine transition table is normative on both targets.
- Instruments this file consumes without restating: the proposition ledger (ref 01); the coverage
  attestation and findings-ledger disposition schema (ref 05, R4/R5/R6); the readiness checklist and the
  assembled-PR embedding requirement (R7/R8).
- Severity floor and the S0–S3 vocabulary: spine "Severity scale."
- Mode handling (interactive escalation vs. delegated flag-and-proceed) and the Done gate / status SSOT
  concrete commands: binding manifest.

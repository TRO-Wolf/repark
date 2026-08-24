# Critic — risk-manager-first adversary

**Mandate.** The Critic is a risk manager first, reviewer second. The question is never "is this
good?" — it is **"how does this fail, and what does that failure cost?"** The Critic's job is to
find what will break before production does. Its exit condition (spine R4) is coverage of the
attack taxonomy below, evidenced end to end — never a tally of complaints, never a quota. This file
is the canonical home of that taxonomy, the coverage-attestation format, the finding-record schema,
and dispute conduct; the spine (R3–R6, D6, the S0–S3 severity scale) governs when and why this file
is invoked and is never restated here.

## Context break, Critic side (R3)

Independence is where adversarial value comes from. Before acting:

1. **Open with the declaration** R3 requires: *"Context break executed; attacking artifacts, not
   memory."* Log it as the first line of the review record.
2. **Restrict inputs** to exactly what R3(a) allows: the unit's charter clauses, the diff and its
   artifacts, test results, and this taxonomy. The Actor's build summary and Self Logic Review are
   not read yet.
3. **File initial findings first.** Attack every applicable taxonomy category (below) and produce
   the first pass of the findings ledger before opening the Actor's self-review — R3(b).
4. **Then, and only then, read the self-review** — for exactly one purpose: an undischarged-flag
   check. Where the Actor's summary claims a success condition, a handled failure mode, or a test
   coverage claim, verify it against the actual diff and test run. A claim with no artifact behind
   it is a finding in its own right (usually AT-1 or AT-10, below) — this is where the old
   claim-vs-code audit lives now, run once, after the independent pass, not as a parallel track.
   The check has **span semantics, not existence semantics**: for a quantified claim ("parity",
   "every", "handled" — spine R2), an artifact exists for the claim only if the `clause_pinning`
   row cites a pin for **every element of the domain the ledger enumerated at audit** (the
   enumeration obligation, ref 01 §2.2b) — and the partition itself is attack surface: ask
   whether the enumeration actually covers the claim, not just whether its cells are green. One
   green representative with the remainder unenumerated or unpinned is an unpinned clause wearing
   a test (S1 minimum; S0 when the untested remainder's failure class is silently wrong results) —
   proven live 2026-07-13, when "F.expr parity" was pinned by one lucky case while integer
   division bit-reinterpreted at the Arrow boundary.
5. **Freshly execute S0-class claims** (spine R3, sequential mode): for any claim whose failure
   class is silently wrong results, run at least one adversarial input yourself, during this
   pass, through the public entry point consumers use — the surface, standing detector, and
   masking paths are bound in the manifest's `s0_fresh_execution` row. The input is your own
   choice and must be **novel**: absent from the unit's committed tests, or targeting an
   untested element of an enumerated domain. Cite it in the attestation as input + entry point +
   observed-versus-expected output. Citations to the Actor-phase suite do not qualify — the
   procedural break means you "remember" that run in exactly the way this step exists to
   distrust — and re-running the committed suite is independent green, not novelty: it proves
   the tests run, not that the claim holds beyond them. A preview/formatting surface named as a
   masking path (a `show`-style output) is never sole evidence — the 2026-07-13 regression was
   invisible on `show` and wrong on `collect`. A five-line smoke here is worth more than a page
   of structural review.
6. **Cite evidence on every finding** — file:line, the exact failing input, or a trace. A finding
   justified by recollection of "how the build went" is invalid; re-derive it from an artifact or
   drop it.
7. **Name the break honestly.** In sequential single-session mode there is no fresh context — R3(d)
   calls this *procedural, not amnesia*. Say so in the record rather than implying an isolation that
   did not happen.

## The attack taxonomy (canonical)

Ten categories, exhaustive by design. Every applicable category gets attacked; every inapplicable
one gets a justified N/A (coverage attestation format, below). A category is "applicable" unless the
unit's clauses and artifacts give it genuinely no surface — that absence is itself the evidence for
the N/A, not a reason to skip the row.

```markdown
| ID    | Category                         | What "attacked" means |
|-------|-----------------------------------|------------------------|
| AT-1  | Spec conformance                  | Walk the charter clauses one by one; check behavior against each clause, not a paraphrase of it. |
| AT-2  | Input domain & boundaries         | Empty, null, max, malformed, adversarial, boundary-adjacent inputs actually exercised. |
| AT-3  | Failure & partial-failure modes   | Errors, retries, timeouts, idempotency under retry, crash mid-operation, cleanup on the failure path. |
| AT-4  | State, ordering & concurrency     | Races, reentrancy, ordering assumptions, shared/mutable state, non-atomic sequences. |
| AT-5  | Security surface                  | AuthN/Z on every privileged action, injection, secrets handling, unsafe deserialization, path traversal. |
| AT-6  | Data integrity & compatibility    | Corruption paths, migrations, schema drift, backward/forward compatibility. |
| AT-7  | Resource & performance behavior   | Unbounded growth, N+1 patterns, hot loops, leaks — filed only when system-breaking (see limit below). |
| AT-8  | Interface & dependency contracts  | API/version assumptions honored, error contracts honored, upstream behavior not silently presumed. |
| AT-9  | Observability & operability       | Can the failure be diagnosed from logs/metrics; do the failure paths alarm. |
| AT-10 | Test adequacy                     | Do existing tests pin the clauses (span semantics for quantified clauses — see step 4); spot-check by mutation — would the suite catch a deliberate bug here? **Branch liveness:** every branch the diff adds must have a nameable input on which it changes the output — a branch with no such input is dead code masquerading as handling, and usually means the mechanism was not understood (the 2026-07-13 identity-cast arm). |
```

Two scope limits keep this from becoming a padding exercise:

- **AT-7 is not a performance review.** Routine performance is the Actor's responsibility under the
  engineering contract (binding manifest → *Engineering contract* / *Risk lens*). The Critic files
  AT-7 findings only where the defect is system-breaking — an outage, an SLA breach, an OOM, a
  runaway cost — never for micro-optimization or style-level performance opinions.
- **No category is a style pass.** Naming, formatting, and other non-functional opinions are not
  attack findings. A real-but-non-failure observation is S3 Advisory at most (spine severity scale),
  and S3 never blocks. Do not manufacture S3 filler to look thorough (see Noise accountability).

**"Exhausted" means every applicable category is attested — attacked-with-evidence or a justified
N/A — not that every category yielded a finding.** A clean AT-row is legitimate exactly when its
evidence shows the attack happened; there is no rule that a clean pass is suspect, and no re-run is
triggered by a category merely *looking* clean. That framing is retired; only the attestation below
is checked.

## Coverage attestation format

R4 defines convergence as attestation, not a findings count: every applicable category `ATTACKED`
with evidence, or `N/A` with justification, and any category touched by remediation is re-attested
before convergence. This is the filed shape:

```yaml
COVERAGE_ATTESTATION:
  pr_unit: <id>
  categories:
    - id: AT-1
      status: ATTACKED   # or N/A
      evidence: >
        <what was tried — the inputs/scenarios exercised, >= 1 sentence, mechanism not vibe>
      artifacts: [ <file:line | test name | trace | repro command>, ... ]   # required if ATTACKED
      justification: <why this category has no surface on this unit>       # required if N/A;
                                                                             # on a LIGHT-path unit
                                                                             # the recorded rubric
                                                                             # result may itself serve
    # ... one entry per AT-1 .. AT-10
  reattested: [ <AT-ids re-run because remediation touched them> ]
  complete: true | false   # true iff every category is ATTACKED or a justified N/A
```

`complete: true` is necessary but not sufficient for convergence: R4 also requires no open or
sustained-disputed finding at/above the severity floor. Convergence remains the Critic's call, never
the Actor's (spine sub-machine table) — this artifact is what makes that call checkable instead of
asserted. The Orchestrator audits the artifact, not a headcount of findings inside it.

## Finding-record schema

```yaml
FINDING:
  id: F-<unit>-<n>                       # spine addressability convention
  severity: S0 | S1 | S2 | S3            # spine severity scale — meaning defined there, not here
  category: AT-<n>                       # the taxonomy category that surfaced it
  clause: [ <charter clause id(s) implicated> ]
  claim: <one sentence: what breaks, and the mechanism — not "this seems risky">
  evidence: <artifact citation: file:line | failing input | trace | repro command>
  disposition: OPEN
    | REMEDIATED (<regression test/link>)     # R5 — proof required, not an assertion of "fixed"
    | ACCEPTED_FLAGGED                        # legal only below the severity floor
    | DISPUTED (<counter-evidence>) -> SUSTAINED | WITHDRAWN   # R6, see below
```

A finding with no `clause` link is orphan work under D5 — trace it or drop it. A finding with no
artifact evidence violates R3(c) and is not filed; go re-derive it or drop it. `severity` tracks
consequence — what breaks and what it costs — never the effort spent finding it.

## Dispute conduct and noise accountability (R6)

When the Actor disputes a finding, the Critic re-adjudicates against the counter-evidence — it does
not defend the original filing on reputation:

- **Sustain** when the counter-evidence does not defeat the claim: disposition becomes `SUSTAINED`.
  At/above the severity floor this is a hard stop for the unit (interactive: escalate immediately;
  delegated: halt the unit, do not assemble the PR, flag it). Below the floor it may ship
  `ACCEPTED_FLAGGED`, flagged in both the PR description and the retrospective ledger.
- **Withdraw** when the counter-evidence defeats the claim: disposition becomes `WITHDRAWN`. This is
  the dispute process working, not a defeat for the Critic — withdrawal is never suppressed to
  protect a filing count.

**Noise accountability.** Every `WITHDRAWN` finding feeds the retrospective's noise ratio (withdrawn
÷ filed, ref 08). The Critic is scored on **precision and coverage** — evidenced, correctly-severed
findings across a complete taxonomy attestation — **never on volume**. Filing marginal or padded
findings to look thorough raises the noise ratio and is itself a Critic failure; under-filing to
dodge a dispute is the same failure in the other direction. Both are visible in the same two
numbers — attestation completeness and noise ratio — and neither is a quota.

## Closing authority — multi-unit assemblies (R13, spine v2.3)

A **bundled PR** — several units assembled into one delivery — has a closing authority: the
independent **bundle-scope Critic** that attests the assembly as a whole. When the assembly
carries a **REMANDED** unit — one that reached its cycle cap without converging and was carried
forward with its open findings enumerated (`02-orchestrator.md` §4, the cycle cap) — those
findings become the closing authority's own duty:

- **Disposition every enumerated finding, item by item.** Each one is closed **with evidence**,
  disproved **with evidence**, or converted into a **recorded user decision** (waive, strip,
  accept) — and every such decision is named as an explicit merge gate in the PR description (R8).
- **A complete assembly-scope attestation does not by itself reach a remanded unit.** It covers
  that unit only once each of its findings carries one of those three closings. A closing
  authority that converges an assembly containing a remanded unit without the item-by-item
  disposition **has not converged it**.
- **Check the disjointness claims.** Downstream units may build atop a remanded unit only where
  their scope is demonstrably disjoint from the open findings' blast radius; that claim is
  recorded when the work starts, and testing it against the assembled diff is part of the closing
  pass.

## External critic engines (optional, manifest-bound; spine v2.3)

The Critic stage described above is the default and needs nothing else. A project **may** bind an
**external critic engine** — a multi-critic harness, a different runtime — for
**STANDARD-and-above** units; that binding lives in the project's manifest (`critic_engine`),
never in portable canon. An engine changes how the attack is *run*; it never becomes a way to
*skip* one. Four constraints are normative:

1. **An engine's convergence signal is never Delivery.** Its output maps into this reference's
   instruments — a coverage attestation plus a findings ledger — and `PR_READINESS_AUDIT` then
   proceeds exactly as always (R7). "The engine converged" is an unledgered claim until those
   artifacts exist.
2. **LIGHT units never select an external engine.** The proportionality rubric decides the path
   first; a LIGHT unit runs the single in-line AC cycle.
3. **The engine's attack taxonomy must satisfy R4.** Every category it works maps onto the
   canonical categories above (plus any manifest `taxonomy_extensions`), or each unmapped category
   is justified `N/A`. The taxonomy may be extended, never shrunk, and a differently-named
   category is not a missing one.
4. **Engine-specific tunables bind per project** — cycle counts, early-stop policy, scratch
   locations — in the project's manifest. Canon carries the constraints; the project carries the
   knobs.

## Routing

- Sub-machine stage consuming this file: CRITIC_REVIEW, gated by "full coverage attestation filed
  (R4) + findings ledger filed."
- Rules operationalized here: **R3** (context break — procedure above), **R4** (convergence is
  coverage — the attestation format above is its filed artifact), **R5** (remediation regression
  proof — the `REMEDIATED` disposition above), **R6** (disputes — Dispute conduct above), **D6**
  (adversarial by construction — this file is D6's operating instrument).
- Severity vocabulary: the spine's S0–S3 scale and severity floor, cited here, never restated.
- Consumed downstream by: PR_READINESS_AUDIT (`references/02-orchestrator.md`, a mode of `01`),
  which reuses this attestation and findings ledger rather than re-deriving them; RETROSPECTIVE
  (`references/08-retrospective.md`), which consumes the noise ratio and any recurring finding class.

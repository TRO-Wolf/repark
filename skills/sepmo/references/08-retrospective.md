# Retrospective Agent

The Retrospective Agent closes the loop SEPMO opened at PROPOSAL: it runs the one-time learning pass
over the project's memory system, and it produces the **metrics ledger** — the quantitative proof that
the audit, the Actor–Critic loop, and Invariant V actually worked, not just that they were run. Where
every other agent proves a claim about code, the Retrospective proves a claim about the *process*: it is
SEPMO's own D4 (Logic Scoping) turned on itself. Nothing here changes mid-project; everything here
changes the *next* one.

---

## 1. Position in the state machine

RETROSPECTIVE is state 6, owner Retrospective Agent, entered via T7 (every PR unit accepted). **T12** is
its sole exit guard: *"Learnings + metrics filed; metrics ledger complete."* Two artifacts satisfy T12,
and both are mandatory — neither substitutes for the other:

1. The **learning pass** (§2) — a write into the project's memory system.
2. The **metrics ledger** (§3) — the fixed set of eight quantitative metrics below, computed and filed.

A retrospective that files lessons but no ledger, or a ledger with a metric silently omitted, has not
satisfied T12 — per the spine's governing rule, a claim without its artifact is itself a defect
(Invariant V's unledgered-claim check applies to the Retrospective's own output as much as to any other
agent's).

The Retrospective Agent runs once the full charter's PR set reaches DELIVERY (T7). Where a project's own
cadence calls for interim retrospectives at a milestone boundary, the same two artifacts are still
required for that partial pass — a lighter cadence never means a shorter ledger.

---

## 2. The learning pass

The Retrospective runs a learning pass over the project's **memory system** (the binding manifest's
*Memory / lessons* role) and the record accumulated across the project: `HALT` verdicts logged in Self
Logic Reviews (ref 03), the closed findings ledgers filed by the Critic (ref 05), sustained disputes
(R6), and Invariant V's alarm log (ref 06). The pass mines these for **classes of failure that slipped
the scope audit** — not individual mistakes, but the pattern a future audit pass should catch on
sentence one.

**Default lifecycle**, used verbatim unless the project's own memory system defines a different one:

- **PROMOTE** — a durable, reusable lesson moves to the relevant canonical home (the engineering
  contract, the root agent file, a debug note) so the *next* audit or build starts from it. A promoted
  lesson names the failure class, the artifact it slipped through, and the rule that now catches it.
  **For a trap-class lesson — a failure mode that will recur mechanically — "the rule that now catches
  it" must be a *detector landed in an enforced gate*** (a test the trap turns red, a corpus entry, a
  lint/grep gate in the bound green commands), not a paragraph: prose demonstrably does not protect the
  next unit that touches the same seam (proven 2026-07-13 — a written-up boundary trap was re-hit 24
  hours later by code whose comment claimed the trap was handled). A trap lesson that cannot be given a
  detector says so explicitly and records the accepted residual risk.
- **KEEP** — recent context that is not yet generalizable stays where it is.
- **ARCHIVE** — the rest is retained, not discarded.

**Conserve by default.** A lesson is never deleted to save space; superseding a lesson is a dated
addition, not an erasure — the same discipline D3 already imposes on Self Logic Reviews (never deleted,
superseded ones marked). If the project's memory system has its own compaction or lifecycle policy, that
policy governs instead of the default above; the Retrospective follows it rather than overriding it.

**Output of the pass:** the list of promoted lessons and their destinations, filed as part of the
retrospective record (§5). A learning pass that finds nothing to promote is legitimate only when the
project's Self Logic Review and Critic-finding history is itself empty or already fully mined; an empty
result asserted without that justification is, per the spine's Global conventions, a claim with no
artifact behind it — itself a defect, on exactly the same footing as an unledgered "converged" or
"100/100."

---

## 3. The metrics ledger

The ledger is the Retrospective's mandatory artifact — a fixed set of exactly eight metrics, computed
over every PR unit the retrospective covers. No metric is optional, none is replaced by a narrative
substitute, and none is dropped because a project "didn't really have" the underlying event (an empty
population is a legitimate value — `0` or `N/A` with the reason — never an absent row).

### 3.1 Metric definitions

| Metric | Definition |
|---|---|
| `findings_per_cycle` | Count of Critic findings, by severity (S0–S3), per AC cycle. |
| `cycles_to_convergence` | Number of AC cycles run before the Critic declared convergence, per PR unit. |
| `noise_ratio` | Findings `WITHDRAWN` ÷ findings filed, per Critic (i.e., per reviewing agent/session). |
| `coverage_misses` | Post-delivery defects mapped to the taxonomy category (ref 05) that was attested `ATTACKED`-clean or `N/A` for that unit. |
| `escaped_defects_by_origin` | Every post-delivery defect classified into exactly one origin: missed clause (audit failure), execution defect (AC failure), or novel scope (vigilance failure). |
| `light_path_escapes` | Post-delivery defects whose originating PR unit ran the LIGHT path. |
| `flags_shipped` | Count of `ACCEPTED_FLAGGED` dispositions shipped, plus the eventual outcome of each. |
| `environment_drift_events` | Count of red gates proven environmental by the R10 base-ref test (base red without the unit's diff), each with the drifted surface named. A **distinct counter, not an escaped defect** — nothing in the AC loop could have caught it *(spine v2.1)*. |

### 3.2 Computation notes — how each metric is populated

**`findings_per_cycle`** — for every PR unit, walk its AC_CYCLE history (R1) and tally the findings the
Critic filed at each CRITIC_REVIEW pass, bucketed by severity. A declining severity distribution across
successive cycles is the signature of genuine convergence; a flat or rising one across many cycles is
itself a signal — it says PR_SCOPING under-carved the unit or the Actor's remediation (R5) is not fixing
root causes.

**`cycles_to_convergence`** — one integer per PR unit: how many AC cycles ran before the Critic's
convergence call (R4) fired. Read alongside `findings_per_cycle`; a high count with findings that stay
severe past the first cycle indicates a scoping problem to feed back into PR_SCOPING (never a floor to
quietly relax).

**`noise_ratio`** — per Critic (per reviewing agent or session, since a project may rotate Critics across
units), the fraction of that Critic's filed findings that were later `WITHDRAWN` under dispute (R6). This
is the same ratio the spine's R4 names as the check on Critic precision. It is diagnostic, not a target
to zero out: a ratio near zero with heavy disputing elsewhere may mean the Actor is disputing legitimate
findings rather than remediating them; a high ratio means the Critic is filing findings that fail R3(c)'s
evidence bar (file:line, failing input, trace) and needs tighter attack discipline.

**`coverage_misses`** — for each post-delivery defect, look up the attestation the Critic filed (ref 05)
for the taxonomy category the defect falls under, on the unit where it originated. If that category was
attested `ATTACKED` with no findings, or `N/A`, the defect is a coverage miss: the attestation was either
superficial (R4 requires the evidence show the attack, not just the label) or the attack itself missed a
real case. Every coverage miss links back to the specific attestation record it invalidates.

**`escaped_defects_by_origin`** — classify every post-delivery defect into exactly one bucket:
  - **missed clause** — the defect traces to a requirement that was never captured as a checkable
    proposition (D4) at AGGRESSIVE_LOGIC_SCOPE_AUDIT. Origin: audit failure.
  - **execution defect** — the clause existed, was `PROVEN` in the ledger, but the AC cycle (build or
    review) failed to catch the implementation gap. Origin: AC failure.
  - **novel scope** — genuinely new behavior or requirement, unknowable at audit time. Origin: vigilance
    failure only if Invariant V's drift alarm (T8/T11) should have caught it sooner and did not; a defect
    that was truly unforeseeable is filed here for completeness but is not itself a process failure.
  Environment-driven red gates are **not** classified here — they are not escaped defects
  (nothing in the AC loop could have caught them); they file under `environment_drift_events` below.

**`light_path_escapes`** — cross-reference `escaped_defects_by_origin` against the proportionality rubric
result recorded at PR_SCOPING for each unit (Proportionality, criteria 1–6). Any post-delivery defect
whose unit ran LIGHT is counted here. This is the direct empirical check on whether the LIGHT rubric is
too permissive — the metric that either vindicates or indicts the current LIGHT thresholds.

**`flags_shipped`** — every `ACCEPTED_FLAGGED` disposition (R6: a sustained dispute below the severity
floor) that shipped in a PR, cross-referenced against R8 (flags must appear in the PR description). Each
flag gets a tracked outcome: still accepted and unproblematic, later remediated in a follow-on unit, or
materialized into a post-delivery defect (in which case it also appears in `escaped_defects_by_origin`
and `coverage_misses` as applicable). This metric is the retrospective-side half of the loop R8 opens —
a flag that is never revisited here is a flag R8's "reviewer must be able to verify from the PR alone"
promise quietly broke.

### 3.3 Ledger record format

```yaml
METRICS_LEDGER:
  id: ML-<retro-id>
  covers: [ <PR unit id>, ... ]              # every unit this retrospective pass accounts for
  findings_per_cycle:
    - pr_unit: <id>
      cycles: [ { cycle: 1, S0: <n>, S1: <n>, S2: <n>, S3: <n> }, ... ]
  cycles_to_convergence:
    - pr_unit: <id>
      cycles: <n>
  noise_ratio:
    - critic: <agent/session id>
      withdrawn: <n>
      filed: <n>
      ratio: <withdrawn/filed>
  coverage_misses:
    - defect: <id or description>
      pr_unit: <origin unit id>
      category: <ref-05 taxonomy category>
      attestation_was: ATTACKED_clean | N/A
  escaped_defects_by_origin:
    - defect: <id or description>
      origin: missed_clause | execution_defect | novel_scope | environment
      evidence: <clause id / AC cycle id / T8-T11 event ref>
  light_path_escapes:
    - defect: <id or description>
      pr_unit: <origin unit id>
  flags_shipped:
    - finding: <F-<unit>-<n> id from R6>
      pr_unit: <id>
      outcome: STILL_ACCEPTED | REMEDIATED_LATER(<unit>) | ESCAPED(<defect id>)
  status: COMPLETE                            # T12's guard — no metric row omitted
```

Every row is addressable (per the spine's Global conventions) so a later audit, a future Scope Auditor
pass, or Invariant V can cite it directly rather than re-deriving it.

**`environment_drift_events`** — one entry per red gate proven environmental by R10's base-ref
test: run the same gate on the base ref without the unit's diff; base red → environmental. Each entry
names the drifted surface (the advisory, the relicense, the tool-version skew) and the remediation
unit that closed it. A drift event is a process failure only if the pre-merge gate (R7) could have
caught it locally and was not run, or if the bound green commands did not mirror the CI surface (a
binding defect per R7) — file THAT as a coverage/binding finding, not as an escaped defect.

---

## 4. The feed-forward rule

The Retrospective may **propose** tuning based on what the ledger shows: raising or re-justifying the
severity floor, tightening or loosening the LIGHT-path thresholds (Proportionality criteria 1–6), or
adding/retiring a taxonomy category in the Critic's attack taxonomy (ref 05). A proposal is only ever
grounded in this project's own ledger evidence — a `light_path_escapes` count that indicts the current
LIGHT criteria, a `noise_ratio` that indicts Critic discipline, a `coverage_misses` cluster that indicts
a taxonomy gap.

**A proposal is not a change.** It lands only as a **versioned update to the binding manifest** — never
as an inline edit to the spine, never as an implicit adjustment an Orchestrator picks up mid-project, and
never applied retroactively to the project that produced it. The severity floor's own rule already holds
here without exception: a proposal may raise the floor, never lower it below S1.

**Feed-forward is asymmetric** *(spine v2.2)*: **bar-raising proposals** — a new pinning obligation, a
tightened binding, a new attack category — **may land immediately** as stamped manifest updates (date +
provenance recorded in the row), including mid-project when they come from an **incident
retrospective** (the mini state-6 an escaped defect triggers — spine, *Incident retrospectives*).
**Bar-lowering or neutral changes always wait for the project boundary.** The asymmetry is
stricter-interpretation-wins applied to feed-forward: the never-mid-project rule exists to stop the bar
dropping under pressure, not to delay its rise. Canon changes are a different procedure entirely
(spine, *versioned canon*): proposed here, approved by the user, landed at the master home with a
version bump.

Each proposal is filed with the evidence that motivated it, the manifest row it targets, its
direction (`RAISES | LOWERS | NEUTRAL`), and a status — `PROPOSED` until accepted, or
`LANDED_IMMEDIATE` for an accepted bar-raising change, which then governs from its stamp date; every
other accepted change governs the *next* project or audit pass, not the one already in RETROSPECTIVE.

```yaml
FEED_FORWARD_PROPOSALS:
  - id: FF-<n>
    targets: severity_floor | light_thresholds | taxonomy_category | binding_row
    manifest_row: <binding-manifest.md row this would change>
    evidence: <ledger metric(s) and values that motivate it>
    proposal: <the concrete change>
    direction: RAISES | LOWERS | NEUTRAL       # RAISES may land immediately (stamped); others wait
    status: PROPOSED | LANDED_IMMEDIATE        # never SELF-APPLIED; LANDED_IMMEDIATE only for RAISES
```

---

## 5. Output — the retrospective record

```yaml
RETROSPECTIVE_RECORD:
  id: RETRO-<n>
  pr_units_covered: [ <id>, ... ]
  learning_pass:
    promoted: [ { lesson: <text>, destination: <home> }, ... ]
    kept: [ <lesson> ]
    archived: [ <lesson> ]
  metrics_ledger: ML-<retro-id>                # see §3.3
  feed_forward_proposals: [ FF-<n>, ... ]       # may be empty; never omitted as a key
  verdict: FILED
```

`FILED` is legal only when both the learning pass and the metrics ledger (all seven metrics present,
`status: COMPLETE`) are attached — this is the artifact T12 checks for.

---

## Routing

Consumed by: **T12** (this file's ledger is the literal guard on the terminal transition out of
RETROSPECTIVE, state 6). Draws on: **R4** (noise ratio definition origin), **R6** (`ACCEPTED_FLAGGED` /
`WITHDRAWN` dispositions feeding `flags_shipped` and `noise_ratio`), **R8** (the flag-visibility promise
`flags_shipped` closes the loop on), **D3/D5** (`HALT` verdicts and traceability as learning-pass inputs),
**Invariant V** (ref 06 — alarm log as `escaped_defects_by_origin` evidence and as the check on `novel
scope` classification), the **Severity scale** and **Proportionality** sections of the spine (targets of
feed-forward proposals), and the binding manifest's *Memory / lessons* role (destination of the learning
pass) and general manifest-versioning discipline (destination of feed-forward proposals).

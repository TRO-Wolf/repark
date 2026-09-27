# history — .agents/skills/sepmo/

The SEPMO spine's canon changelog, moved out of [SKILL.md](SKILL.md) by the 2026-09-27
read-path split (presentation only; semantics unchanged). Historical rationale: why each
amendment landed.

## Canon changelog

- **v2.3 — 2026-07-26.** The disposition discipline lands: **R11**
  (contingencies must be executable — additive-by-construction or
  sign-off-pre-authorized, verified at PRE_EXECUTION_REVIEW), **R12** (every
  unit and assembly group ends in a recorded disposition — CONVERGED /
  REMOVED / REMANDED; an unsettled disposition blocks the line, and logging a
  breach is not settling it), **R13** (remand to the assembly's closing
  authority: explicit record with enumerated findings, recorded disjoint-scope
  rule for downstream work, item-by-item closing disposition, user decisions
  as named PR merge gates). The spine's *Incident retrospectives* section
  widens its trigger to **lifecycle-machinery incidents** (Amendment D) —
  machinery failures file the same immediate `kind: incident` section whether
  or not a product defect escaped. Promoted from a consuming project's
  incident retrospective: a bundle group parked on an open finding, its
  destructive parking contingency proved unexecutable under the live
  permission regime, downstream groups consumed the unsettled state, and the
  bundle-scope closing Critic caught the breach and improvised what R13 now
  legalizes. *Reference amendments required by this version:* ref 02 —
  PRE_EXECUTION_REVIEW checklist gains contingency-executability; *Cycle-cap
  escalation* gains the REMOVED/REMANDED dispositions and the
  multi-unit-assembly binding; ref 03 — the review format gains a
  contingency-executability line; ref 05 — the bundle-scope closing Critic's
  item-by-item remand duty AND the external-critic-engine constraints
  (Amendment E); ref 06 — new watch item at each lineage's next unused id
  (**W9** in the master's references): unsettled-disposition consumption /
  invalid contingency; ref 08 — mirrors Amendment D (canonical rule stays in
  the spine); the template — the optional `critic_engine` binding row
  (Amendment E, runtime-neutral).
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

# binding-manifest.template.md — SEPMO ⨉ \<REPO\>

**This template instantiates the one project-specific SEPMO file.** The spine
([SKILL.md](SKILL.md)) and its [references/](references/) are portable canon and are
never edited per-project; every project-specific role and tunable resolves through the
tables below. To port SEPMO to a repo, instantiate this template there; to port it
elsewhere, instantiate it again. **A manifest binds; it does not restate** — every row is
a pointer into the repo or a declared fallback, never a copy of a rule that lives
somewhere else.

## Instantiation protocol

Lines beginning `> Fill:` are instructions to the installing agent and are deleted from
the instantiated file. Every role row resolves to exactly one mode:

- **BIND** — the repo already has a canonical home for this role; point at it.
- **CREATE** — no home exists and SEPMO cannot run without one; create the minimal file
  now and point at it.
- **DEFAULT** — the spine's built-in fallback suffices; name it in the row.

Steps: (1) survey the repo and resolve every role row — read the candidate files, don't
guess from filenames (D1); (2) bind every tunable; (3) delete all `> Fill:` lines;
(4) run the instantiation checklist at the end — every box is a checkable proposition;
(5) the filled manifest is itself a PR unit: run it through SEPMO, and let the rubric —
not habit — decide whether it is LIGHT.

## Spine version

> Fill: copy the version from the spine's frontmatter (`version:`) / canon changelog.

`spine_version:` **vX.Y** — the canon version this manifest binds. If the master spine
moves past it, this manifest is **stale, not silently wrong** (Invariant V staleness
alarm): re-bind before the next project starts.

## Precedence

> Fill: locate the repo's conflict-resolution or precedence statement (commonly in the
> root agent/contributor doc). If one exists, point at it and delete the default split
> below. If none exists, keep the default split as written — it then becomes this repo's
> statement, and this section is its single home.

On any conflict, the chain in \<PATH — e.g. the repo's root agent doc, `## Precedence`\>
wins; SEPMO cedes the engineering contract to it. That home is the single statement of
the chain — this manifest points there and never restates it.

*Default split (repos with no chain):* SEPMO governs **lifecycle and orchestration
only** — states, gates, the AC loop, delivery, retrospective. The repo's engineering
contract governs **how code is written**. On conflict inside engineering, the contract
wins, and the fix lands in this manifest or its usage — never in canon. Not cede-able
under any chain: the spine's own lifecycle law — the gates, the raise-only severity
floor, the extend-only taxonomy, and the artifact rule (every gate is a checkable
artifact, not a self-report).

## Role bindings

> Fill: replace each *italic guidance cell* with the resolved binding as relative links,
> then set Mode. A row may compose several files. A BIND row pointing at a file that does
> not exist is a defect, not a placeholder.

| SEPMO role | Canonical home in this repo | Mode | Relationship |
|---|---|---|---|
| Engineering contract | *the authoritative agent/contributor rules: AGENTS.md, CONTRIBUTING, CLAUDE.md, tier manuals, formatter/linter/type-checker configs* | BIND · or DEFAULT: tier manual + tool configs as found, with a real contract flagged as an early PR unit | Actor binds — defers entirely |
| Risk lens | *a threat model, risk register, or risk-first section, if the repo has one* | BIND · or DEFAULT: the attack taxonomy ([references/05](references/05-critic.md)) alone | Critic uses as attack basis |
| Done gate | *test/CI documentation plus the commands that define green — must satisfy the two-tier rule bound in `green_commands` below* | BIND · or CREATE a minimal testing doc | Delivery invokes; R2/R7 exits |
| Plan-of-record | *roadmap, PROJECT, or north-star docs the charter derives from* | BIND · or CREATE a minimal plan doc — SEPMO cannot run without a charter source | Orchestrator derives the charter |
| Status SSOT | *the one place current state lives* | BIND · or CREATE a "Current state" section in the plan-of-record | Delivery updates; never restated |
| PR-unit grouping | *the repo's own unit vocabulary (epics, workstreams, milestones), if any* | BIND-and-map · or DEFAULT: SEPMO PR units, sized by logical coherence + reviewability | Orchestrator maps to it |
| Active plan tracking | *an existing working-plan / todo file* | BIND · or CREATE one — exactly one; no parallel tracker | Orchestrator writes the plan here |
| Memory / lessons | *lessons file, ADRs, decision log* | BIND · or CREATE a date-stamped lessons file (supersede, don't delete) | Retrospective runs the learning pass |
| Navigation | *the repo's navigation mandate (per-directory maps, indexes), if any* | BIND · or DEFAULT: none — SEPMO imposes no navigation convention (spine, Global conventions) | If bound, the mandate extends to SEPMO's files |
| Prohibitions | *hard rules: non-negotiables, destructive/outward-facing operation policies* | BIND · or DEFAULT: none added — SEPMO adds no prohibitions of its own | All agents obey |
| Sub-agent / tier policy | *the repo's policy on fan-out and model tiers* | BIND · or DEFAULT: single-agent sequential; fan-out only on explicit user request | Orchestrator's AC mode follows it (see `context_break_mechanics`) |
| Mode handling | *the interactive/delegated definitions in the tier manual* | BIND · or DEFAULT: interactive = ask now; delegated = halt + flag (spine R6) | Orchestrator + all agents adopt |
| Debugging protocol | *a repo debugging doc or section, if any* | BIND · or DEFAULT: R5's reproduce-then-fix discipline as the minimum | Actor/Critic follow on failure |

## SEPMO binding points (tunables)

Spine defaults apply wherever a row is silent. Per the spine: `severity_floor` may only
be **raised** and the taxonomy only **extended**. Changes to this section land as
versioned manifest updates proposed by a retrospective — including an **incident
retrospective** after an escaped defect (spine, *Incident retrospectives*). Feed-forward
is asymmetric: **bar-raising changes may land immediately**, stamped with date and
provenance; bar-lowering or neutral changes wait for the project boundary
([references/08](references/08-retrospective.md)). Canon changes are a different
procedure entirely (spine, *versioned canon*): proposed here, approved by the user,
landed at the master home.

| Binding point | This repo's value | Constraints / notes |
|---|---|---|
| `severity_floor` | > Fill: **S1** unless raising | Raise-only. Raising to S2 for money-, security-, or data-integrity-critical surfaces is the expected proposal for repos that have them — route it through a retrospective. |
| `green_commands` | > Fill: **unit gate (R2 exit):** build/static + test commands. **pre-merge gate (R7):** the command that faithfully mirrors CI locally. **CI-only exception record:** one entry per unmirrored check = check + justification + **the residual gap it leaves**. | If the repo has CI, R7's mirror rule is mandatory, and a **parity guard** — a script or make target that diffs tool pins between the local gate and the CI workflows — is strongly recommended: without it, "pinned identically" is a claim that rots silently, and the spine classes a silent skip as a binding defect. If the repo has **no** CI, the pre-merge gate *is* the whole gate and the exception record is empty by construction — say so in this cell. |
| `light_thresholds` | > Fill: **≤ 150 changed lines / ≤ 5 files** unless tightening | These are the size criterion only; the other five spine criteria must also all hold, and uncertain → STANDARD. |
| `context_break_mechanics` | > Fill: **procedural in-session** (default) · fresh-context · sub-agent hard break | R3 prefers the hard break; bind what the repo's sub-agent policy actually permits, and name a procedural break honestly as procedural, not amnesia. |
| `s0_fresh_execution` | > Fill: the **public entry point(s)** the Critic drives for silently-wrong-results claims — the exact build-and-run commands for the surface users actually consume (a facade on a built wheel, a CLI binary, a served API), the **standing detector** (the corpus/matrix test file that per-element pinning maintains and domain growth must join), and the **masking surface(s)** — any preview/formatting path (a `show`-style output) that can hide the failure class and is therefore never sole evidence. | Mandatory when `context_break_mechanics` binds a procedural break; with a standing hard break, mark N/A citing that as the justification. The input's novelty standard is canon (spine R3) — this row binds the surface and detector, never the standard. *(spine v2.2+)* |
| `metrics_ledger_location` | > Fill: path (CREATE if absent) | One section per retrospective; the [references/08](references/08-retrospective.md) metric set verbatim, including `environment_drift_events` (spine v2.1+). |
| `critic_engine` | > Fill: **the spine's own Critic stage** (the default — say so) · or the external engine bound for STANDARD-and-above units, named together with its tunables: cycle counts, early-stop policy, scratch locations | **Optional.** An external critic engine — a multi-critic harness, a different runtime — changes how the attack is *run*, never whether it happens. Normative constraints ([references/05](references/05-critic.md), *External critic engines*): (1) an engine's convergence signal is **never Delivery** — its output maps to a coverage attestation plus a findings ledger, and `PR_READINESS_AUDIT` proceeds exactly as always (R7); (2) **LIGHT units never select an external engine**; (3) its attack taxonomy must satisfy **R4** — every category maps onto the canonical taxonomy in [references/05](references/05-critic.md) (plus `taxonomy_extensions`), or each unmapped category is justified `N/A`; (4) **engine-specific tunables bind in this row**, never in portable canon. *(spine v2.3+)* |
| `taxonomy_extensions` | > Fill: none, or the added categories | Extend-only. An extension is a new attack category, so it widens the Critic's attestation duty on every subsequent unit — add deliberately. |

## Pointers

> Fill: Up: the repo's entry/read-order docs. Related: [SKILL.md](SKILL.md) (the spine),
> [references/](references/) (canonical instrument homes), and the repo docs bound above.

## Debug

- A SEPMO behavior contradicts the engineering contract → the contract wins on
  engineering; fix this manifest or its usage, never canon. Canon defects are filed
  upstream to the master home (D2), not patched in this repo's copy.
- The same status appears in two places → SSOT breach; the Status SSOT row is the only
  home.
- A row points at a missing file → this manifest is stale; fix the row.
- The AC loop seems to need sub-agents → it does not by default: run
  `context_break_mechanics` as bound; hard breaks only as the sub-agent policy permits.
- A claim ("100/100", "converged", "mergeable", "delivered") appears without its
  artifact → Invariant V alarm ([references/06](references/06-vigilance.md)); demand the
  ledger / attestation / gate evidence.
- A converged unit goes red at the pre-merge gate or in CI → run the **R10 base-ref
  test**: reproduce on the base ref without the unit's diff. Base red → environmental;
  remediate as its own unit (path per the rubric). Base green → a unit defect; T9.
- A defect surfaces **after its PR was accepted** → incident retrospective **now**
  (spine, *Incident retrospectives*): file `coverage_misses` naming the category that
  sat clean, file `escaped_defects_by_origin`, land bar-raising proposals immediately
  with a stamp; the fix is its own unit with R5 regression proof.
- This manifest's `spine_version` trails the master spine → staleness alarm; re-bind
  before the next project starts.

## Instantiation checklist

The install is complete when every proposition below is provable:

- [ ] I-1 — Every role row resolves to BIND (existing path), CREATE (the file now
      exists), or DEFAULT (spine fallback named); no italic guidance and no `> Fill:`
      lines remain.
- [ ] I-2 — `spine_version` is declared and matches the shipped spine's frontmatter and
      changelog.
- [ ] I-3 — Every path this manifest links resolves (link-check the file).
- [ ] I-4 — `green_commands` names both gates; every CI-enforced check is either
      mirrored locally or in the exception record **with its residual gap stated**.
- [ ] I-5 — A parity guard exists, or its absence is justified inside the
      `green_commands` row.
- [ ] I-6 — The metrics ledger location exists and carries the references/08 metric
      set, including `environment_drift_events`.
- [ ] I-7 — `severity_floor` ≥ S1; taxonomy changes, if any, are pure additions.
- [ ] I-8 — A status sweep finds exactly one home for current state (the SSOT row).
- [ ] I-9 — The filled manifest itself passed a SEPMO cycle as a unit, with its
      LIGHT/STANDARD path decided by the rubric and recorded.
- [ ] I-10 — `s0_fresh_execution` is bound (surface + standing detector + masking
      paths named), or marked N/A with a standing hard break as the justification.

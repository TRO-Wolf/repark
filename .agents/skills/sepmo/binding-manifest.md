# binding-manifest.md — SEPMO ⨉ RePark

This is the one project-specific SEPMO file. The v2 spine ([SKILL.md](SKILL.md)) and its
[references/](references/map.md) are portable canon; every project-specific role and tunable
resolves through the tables below. To port SEPMO elsewhere, rewrite only this file (instantiate
[binding-manifest.template.md](binding-manifest.template.md)). Each row is BIND (existing file) /
CREATE (a minimal file made at install) / DEFAULT (built-in fallback). **SEPMO binds; it does not
restate.**

## Spine version
`spine_version:` **v2.3** — declared per the spine's versioned-canon convention; the master home is
the operator's SEPMO canon repository outside this repo (kept byte-identical). Skew between this
line and the master spine's frontmatter is an Invariant V staleness alarm: re-bind before the next
project starts. *(Re-bound v2.2 → v2.3 on 2026-08-24: the disposition discipline — R11 executable
contingencies, R12 recorded dispositions, R13 remand — plus machinery-incident retrospectives,
the VG-09 watch item, and the optional `critic_engine` binding row below.)*

## Precedence
SEPMO governs lifecycle/orchestration only. On any conflict the chain in [AGENTS.md](../../../AGENTS.md)
`## Precedence` wins; SEPMO cedes the engineering contract to it. That block is the single home for the
chain — this manifest points there and never restates it.

## Role bindings
| SEPMO role | Canonical home in this repo | Mode | Relationship |
|---|---|---|---|
| Engineering contract | [AGENTS.md](../../../AGENTS.md) (authoritative) + [DEVELOPMENT.md](../../../DEVELOPMENT.md) (build/test/verify + tool configs) + the portable working method ([.agents/skills/engineering-method/SKILL.md](../engineering-method/SKILL.md) — agent-agnostic since 2026-08-24; the former `docs/skills/` tier manuals generalized into it) | BIND | Actor binds — defers entirely |
| Risk lens | [.agents/skills/engineering-method/SKILL.md](../engineering-method/SKILL.md) `<risk_first>` + its project risk-surface table; the attack taxonomy ([references/05-critic.md](references/05-critic.md)) as the systematic basis | BIND | Critic uses as attack basis |
| Done gate | [docs/testing.md](../../../docs/testing.md) + `make verify` (= `make ci` + `make test`); pre-merge (R7): `make preflight` — see `green_commands` below; test with `cargo test --workspace`, **never** `--all-features`; new behavior pins per the entry-point matrix (native DataFrame / ANSI SQL / Spark facade) once those surfaces exist | BIND | Delivery invokes |
| Plan-of-record | [PROJECT.md](../../../PROJECT.md) (north-star charter + locked invariants) + [docs/port/PLAN.md](../../../docs/port/PLAN.md) (the four-phase port plan and its acceptance gates) | BIND | Orchestrator derives the charter |
| Status SSOT | [STATUS.md](../../../STATUS.md) | BIND | Delivery updates; never restated |
| PR-unit grouping | BIND-and-map: delegated slate work arrives as versioned briefs in [briefs/](../../../briefs/map.md); otherwise DEFAULT SEPMO PR units — sized by logical coherence + reviewability | BIND-and-map | Orchestrator maps to it |
| Active plan tracking | the unit's own `task/ledgers/staging/<unit>-ledger.md` (see [task/ledgers/map.md](../../../task/ledgers/map.md); DL-1, 2026-08-23 — `move`d to `completed/` in the departure commit); the live backlog is [STATUS.md](../../../STATUS.md) — [task/todo.md](../../../task/todo.md) is a pointer to both since 2026-08-09 | BIND | Orchestrator writes the working plan in the unit ledger; no parallel tracker |
| Memory / lessons | [task/lessons.md](../../../task/lessons.md) (DO/DO-NOT, date-stamped, supersede-don't-delete) + [docs/adr/](../../../docs/adr/map.md) (decision records) | BIND | Retrospective runs the learning pass |
| Navigation | `map.md` in **every** directory — **mandatory** here (guard `scripts/check_map_md.sh`, lockstep rule); **overrides SEPMO's opt-in default** | BIND | each SEPMO dir carries one |
| Prohibitions | [AGENTS.md](../../../AGENTS.md) "Hard rules" + "Safety — destructive / outward-facing operations" (no Glue/S3 Tables/S3/IAM mutation without explicit user action) + the working method's `<non_negotiables>` locator ([.agents/skills/engineering-method/SKILL.md](../engineering-method/SKILL.md)) | BIND | all agents obey; SEPMO adds none |
| Sub-agent / tier policy | [AGENTS.md](../../../AGENTS.md) "Delegated work" (the neutral rule: single-agent default) + [CLAUDE.md](../../../CLAUDE.md) "capability tiers and sub-agents" (the tier mapping: delegated fan-out is Sonnet/Haiku; **Opus sub-agents need an explicit user command naming Opus**) | BIND | Orchestrator's AC mode follows it (see `context_break_mechanics` below + Debug) |
| Mode handling | [.agents/skills/engineering-method/SKILL.md](../engineering-method/SKILL.md) "Mode Handling" (interactive / delegated) — the spine's escalation convention adopts it | BIND | Orchestrator + agents adopt both |
| Debugging protocol | [.agents/skills/engineering-method/SKILL.md](../engineering-method/SKILL.md) §8 (read→reproduce→isolate→hypothesize→fix→verify→regress) + `map.md#debug` as the first hop | BIND | Actor/Critic follow on failure |
| Unit pickup / departure | [briefs/next-sequence.md](../../../briefs/next-sequence.md) standing rule 7 (the ritual's single home) + [AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle" (the document classes the ritual serves) + [.agents/skills/compact-context-docs/SKILL.md](../compact-context-docs/SKILL.md) "Pickup ritual (scoped mode)" (the executor) + `make check-docs-compaction` (`scripts/check_docs_compaction.py`, DL-4 2026-08-25 — the live documents carry only live state; `ledger_lifecycle.py compact` runs inside `archive` at pickup and inside the departure `move`) + `make check-map-sync` (`scripts/sync_map_md.py`) as the mechanical drift gate — bound 2026-08-22, owner directive (feed-forward, bar-raising) | BIND | Orchestrator runs the pickup ritual as the unit's **first** act: confirm the prior PR merged and that the local base carries its departure edit, run the drift check, and compact **only** the just-merged delta as a docs-only first commit. Delivery lands the departure edit as the unit's **last** act. Steps live in rule 7 + the skill; never restated here |
| Conventions instruments | [.agents/skills/code-quality/SKILL.md](../code-quality/SKILL.md) (Python) + [.agents/skills/rust-code-quality/SKILL.md](../rust-code-quality/SKILL.md) (Rust) — bound 2026-08-22, owner directive (feed-forward, bar-raising) | BIND | Actor reads/invokes the matching skill **before** writing code in that language; the rule of record stays [AGENTS.md](../../../AGENTS.md) (its Python / Rust sections) — the skills carry the reasoning and the sanctioned exceptions, and add no rule of their own |
| Ledger grammar instrument | `scripts/check_ledger_grammar.py` (`make check-ledger-grammar`, in `make ci`) — bound 2026-08-23, owner directive (feed-forward, bar-raising; DL-2) | BIND | The **shape** of the Scope Auditor's proposition ledger ([references/01](references/01-scope-auditor.md) §2 — clause rows `C-NNN`, one verdict cell, evidence) and of the Critic's coverage attestation and finding records ([references/05](references/05-critic.md)) is checked mechanically over `task/ledgers/staging/`; the Actor cites each clause it discharges from the test that discharges it (`pins: <unit>/C-NNN`, [docs/testing.md](../../../docs/testing.md) "Pinning a charter clause"), and a `PROVEN` clause nobody pins is red. The attestation is required once no clause is `OPEN`. The *meanings* — verdicts, the enumeration obligation, the taxonomy, convergence — stay in the references, which this row does not restate. **Carrier: markdown tables, measured and declined XML (2026-08-23)** — every gate here is markdown-aware and the ledgers are read in PR diffs; the record is the DL-2 ledger |

## SEPMO v2 binding points (tunables)

Spine defaults apply wherever a row is silent; per the spine, `severity_floor` may only be raised
and the taxonomy may only be extended. Changes to this section land as versioned manifest updates
proposed by a Retrospective — including an **incident retrospective** after an escaped defect —
under the asymmetric feed-forward rule (spine v2.2 / [references/08](references/08-retrospective.md)):
bar-raising changes may land immediately, stamped with date + provenance; bar-lowering or neutral
changes wait for the project boundary.

| Binding point | This repo's value | Notes |
|---|---|---|
| `severity_floor` | **S1** (spine default; raise-only) | Raising to S2 for data-integrity-critical paths (the Iceberg write/commit path, once ported) is an open product decision — propose via a retrospective. |
| `green_commands` | **Unit gate (R2 exit):** build/static `make ci` · tests `make test` · combined `make verify`. **Pre-merge gate (R7 `ci_green`, the local CI mirror):** `make preflight` = verify + the security/workflow gates CI also runs (taplo, typos, cargo-deny, zizmor, workflow-parse) — tool versions pinned identically in the Makefile and the workflows; nothing silently skips (uvx provisions). **CI-only exceptions (recorded per R7, residual gap stated):** none at phase 0 — every CI-enforced check is mirrored locally; the record grows only when a later phase adds a check that cannot run locally, each entry naming the check, the justification, and the residual gap. **Parity guard:** pinned tool versions are enforced by construction (uvx `tool@version`); a pin that appears in a workflow but not the Makefile (or vice versa) is a binding defect. | **Never `cargo test --all-features`** — see [AGENTS.md](../../../AGENTS.md) "PyO3 build notes". |
| `light_thresholds` | A unit that changes **only** prose, `map.md`, ledgers, recorded evidence (e.g. `task/mw-6-critic-evidence/`) or tests is **LIGHT** whatever its line count, when the six spine criteria hold. A unit that changes code keeps the spine defaults: ≤ 150 changed lines and ≤ 5 files. | Re-bound 2026-08-25 (owner ruling): a docs-only sweep is not forced onto the STANDARD path by size alone; the six criteria still gate it, and a code change of any size still meets the line/file caps. |
| `context_break_mechanics` | **Procedural in-session break** (default): one session runs Actor, then declares the break and shifts to Critic with inputs restricted per R3 — named honestly as procedural, not amnesia. **Sub-agent hard break**: only on explicit user opt-in per the sub-agent / tier policy row above (Sonnet/Haiku default; Opus only when the user names Opus). | R3 prefers the hard break; this repo's sub-agent policy gates it, so the procedural break is the standing default. |
| `s0_fresh_execution` | For claims whose failure class is **silently wrong results** (engine-vs-reference divergence, numeric/type fidelity, Arrow-boundary schema): the Critic freshly executes ≥ 1 adversarial input through the **public entry point of the surface under claim** — once the engine lands (phase 1+), the native DataFrame / ANSI SQL door; once the facade lands (phase 3), the facade on the built wheel driving `collect`/`to_arrow` — and cites that run in the attestation (spine R3 / ref 05 step 5). Standing detector: the **entry-point matrix** mandated by [docs/testing.md](../../../docs/testing.md) — new entry points must join the matrix; new divergence classes get a row per door. **Masking surfaces (never sole evidence):** `show`-style pretty-printed/preview output. At phase 0 the repo has no code surface, so no unit yet carries this failure class; the binding takes effect with the first ported surface. | The compensating control for this repo's procedural (non-amnesiac) break. |
| `review_profile` | Choose **LIGHT**, **STANDARD**, or **HIGH** from the riskiest touched path. The tier scales only attack depth, pass count, isolation, and mutation probes. **LIGHT** uses the spine's single in-line Critic stage and never selects an external engine. **STANDARD** selects the bound CCC engine at standard intensity. **HIGH** selects the same engine at high intensity with fresh isolated passes. | Every tier keeps the complete bar: every clause pinned, a full `COVERAGE_ATTESTATION`, the **S1** severity floor, the bound `green_commands`, `s0_fresh_execution` when applicable, and the **R7** readiness audit. Every changed pin is shown red before its fix and green after it. Add one mutation probe per new guard seat. A rubric can justify `N/A` for an attack category, but it cannot omit the category. Provenance: owner ruling 2026-08-25, corrected by PR #244 revalidation on 2026-08-26. |
| `critic_engine` | Every execution unit runs one Actor, then one Critic stage, sequentially. **LIGHT** uses the spine's in-line Critic and never selects external **[critic-critic-critic](../critic-critic-critic/SKILL.md) (CCC)**. **STANDARD/HIGH** may select one bound CCC engine at the `review_profile` intensity; its specialised Critics are attack lenses or passes inside that one stage. `mode=review-only`; remediation returns to the Actor; `max_cycles` uses the unit ledger; `severity_floor` uses this manifest; `claims_critic=true`; risk comes from the riskiest touched path; external Critics attack a scratch clone; `context_break_mechanics` controls isolation. **Mapping:** Critic-1 → AT-8, AT-10 and the crates contract; Critic-2 → AT-3, AT-4, AT-5; Critic-3 → AT-1, AT-2, AT-6; Critic-4 → claims and readiness outside AT-1..AT-10; AT-7 is attacked only for system-breaking change, else justified `N/A`; AT-9 is attacked where a failure path exists, else justified `N/A`. Every attestation lists all ten. | Finder and Verifier are not roles in this loop. Delivery remains post-convergence readiness verification through `PR_READINESS_AUDIT`; `CCC-CONVERGED` is not Delivery. A user can explicitly select a separate hardening lane outside this loop. The CCC skill owns its attack lenses, risk tiers, and report schema; this row owns the SEPMO binding and tunables. Provenance: owner ruling 2026-08-25, corrected by PR #244 revalidation on 2026-08-26. |
| `metrics_ledger_location` | `task/metrics.md` (CREATE — installed at the first retrospective) | One section per retrospective, the ref-08 metric set verbatim — eight metrics incl. `environment_drift_events` (spine v2.1+). |
| `taxonomy_extensions` | **None** — the ten spine categories as canon defines them. | Extend-only; an extension widens the Critic's attestation duty on every subsequent unit — add deliberately, via feed-forward. |

## Pointers
Up: [AGENTS.md](../../../AGENTS.md) (the authoritative contract + its `## Precedence` chain).
Related: [SKILL.md](SKILL.md) (the spine);
[references/map.md](references/map.md) (the canonical instrument homes); [../map.md](../map.md)
(the `skills/` container).

## Debug
- A SEPMO behavior contradicts the engineering contract → the contract wins; fix the manifest/usage,
  never the spine or references (they are portable canon; D2 — file defects back to the user).
- The same status appears twice → de-duplication breach (spine Global conventions / Invariant V); the
  Status SSOT ([STATUS.md](../../../STATUS.md)) is the only home.
- A row points at a missing file → manifest is stale; fix the row.
- SEPMO's Actor–Critic seems to need sub-agents → it does **not** by default here: run the procedural
  context break per `context_break_mechanics` above. Fan out to real sub-agents **only** on an explicit
  user request, and Opus sub-agents only when the user names Opus (the sub-agent / tier policy row).
- A claim ("100/100", "converged", "mergeable", "delivered") appears without its artifact → Invariant V
  alarm ([references/06](references/06-vigilance.md)); demand the ledger/attestation/CI evidence.
- PR checks red on a converged unit → the **R10 base-ref test**: run the same gate on the base ref
  without the unit's diff (`make preflight` both sides). Base red → environmental: remediate as its
  own unit (rubric decides the path; security-advisory work routes STANDARD by criterion 5) and file
  `environment_drift_events`. Base green → a unit defect: T9 per the spine.
- A defect surfaces **after its PR was accepted** → incident retrospective **now** (spine, *Incident
  retrospectives*): file `coverage_misses` + `escaped_defects_by_origin` in `task/metrics.md`
  (creating it if this is the first retrospective); bar-raising proposals land immediately, stamped.
- This manifest's `spine_version` trails the master spine → staleness alarm; re-bind before the next
  project starts.

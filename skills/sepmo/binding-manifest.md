# binding-manifest.md — SEPMO ⨉ RePark

This is the one project-specific SEPMO file. The v2 spine ([SKILL.md](SKILL.md)) and its
[references/](references/map.md) are portable canon; every project-specific role and tunable
resolves through the tables below. To port SEPMO elsewhere, rewrite only this file (instantiate
[binding-manifest.template.md](binding-manifest.template.md)). Each row is BIND (existing file) /
CREATE (a minimal file made at install) / DEFAULT (built-in fallback). **SEPMO binds; it does not
restate.**

## Spine version
`spine_version:` **v2.2** — declared per the spine's versioned-canon convention; the master home is
the operator's SEPMO canon repository outside this repo (kept byte-identical). Skew between this
line and the master spine's frontmatter is an Invariant V staleness alarm: re-bind before the next
project starts.

## Precedence
SEPMO governs lifecycle/orchestration only. On any conflict the chain in [CLAUDE.md](../../CLAUDE.md)
`## Precedence` wins; SEPMO cedes the engineering contract to it. That block is the single home for the
chain — this manifest points there and never restates it.

## Role bindings
| SEPMO role | Canonical home in this repo | Mode | Relationship |
|---|---|---|---|
| Engineering contract | [AGENTS.md](../../AGENTS.md) (authoritative) + [docs/skills/Opus.md](../../docs/skills/Opus.md) / [Sonnet.md](../../docs/skills/Sonnet.md) / [Haiku.md](../../docs/skills/Haiku.md) (tier manuals) + [CLAUDE.md](../../CLAUDE.md) conventions (rustfmt/clippy/ruff configs) | BIND | Actor binds — defers entirely |
| Risk lens | [docs/skills/Opus.md](../../docs/skills/Opus.md) `<risk_first>` + its project risk-surface table; the attack taxonomy ([references/05-critic.md](references/05-critic.md)) as the systematic basis | BIND | Critic uses as attack basis |
| Done gate | [docs/testing.md](../../docs/testing.md) + `make verify` (= `make ci` + `make test`); pre-merge (R7): `make preflight` — see `green_commands` below; test with `cargo test --workspace`, **never** `--all-features`; new behavior pins per the entry-point matrix (native DataFrame / ANSI SQL / Spark facade) once those surfaces exist | BIND | Delivery invokes |
| Plan-of-record | [PROJECT.md](../../PROJECT.md) (north-star charter + locked invariants) + [docs/port/PLAN.md](../../docs/port/PLAN.md) (the four-phase port plan and its acceptance gates) | BIND | Orchestrator derives the charter |
| Status SSOT | [PROJECT.md](../../PROJECT.md) "Current state" + [task/todo.md](../../task/todo.md) checkboxes | BIND | Delivery updates; never restated |
| PR-unit grouping | BIND-and-map: delegated slate work arrives as versioned briefs in [briefs/](../../briefs/map.md); otherwise DEFAULT SEPMO PR units — sized by logical coherence + reviewability | BIND-and-map | Orchestrator maps to it |
| Active plan tracking | [task/todo.md](../../task/todo.md) | BIND | Orchestrator writes the working plan here; no parallel tracker |
| Memory / lessons | [task/lessons.md](../../task/lessons.md) (DO/DO-NOT, date-stamped, supersede-don't-delete) + [docs/adr/](../../docs/adr/map.md) (decision records) | BIND | Retrospective runs the learning pass |
| Navigation | `map.md` in **every** directory — **mandatory** here (guard `scripts/check_map_md.sh`, lockstep rule); **overrides SEPMO's opt-in default** | BIND | each SEPMO dir carries one |
| Prohibitions | [CLAUDE.md](../../CLAUDE.md) `<non_negotiable_invariants>` + "Destructive / outward-facing operations" (no Glue/S3 Tables/S3/IAM mutation without explicit user action) + [AGENTS.md](../../AGENTS.md) "Hard rules" + [docs/skills/Opus.md](../../docs/skills/Opus.md) Non-Negotiables | BIND | all agents obey; SEPMO adds none |
| Sub-agent / tier policy | [CLAUDE.md](../../CLAUDE.md) `<subagent_policy>` — single-agent default; delegated fan-out is Sonnet/Haiku; **Opus sub-agents need an explicit user command naming Opus** | BIND | Orchestrator's AC mode follows it (see `context_break_mechanics` below + Debug) |
| Mode handling | [docs/skills/Opus.md](../../docs/skills/Opus.md) "Mode Handling" (interactive / delegated) — the spine's escalation convention adopts it | BIND | Orchestrator + agents adopt both |
| Debugging protocol | [docs/skills/Opus.md](../../docs/skills/Opus.md) §8 (read→reproduce→isolate→hypothesize→fix→verify→regress) + `map.md#debug` as the first hop | BIND | Actor/Critic follow on failure |

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
| `green_commands` | **Unit gate (R2 exit):** build/static `make ci` · tests `make test` · combined `make verify`. **Pre-merge gate (R7 `ci_green`, the local CI mirror):** `make preflight` = verify + the security/workflow gates CI also runs (taplo, typos, cargo-deny, zizmor, workflow-parse) — tool versions pinned identically in the Makefile and the workflows; nothing silently skips (uvx provisions). **CI-only exceptions (recorded per R7, residual gap stated):** none at phase 0 — every CI-enforced check is mirrored locally; the record grows only when a later phase adds a check that cannot run locally, each entry naming the check, the justification, and the residual gap. **Parity guard:** pinned tool versions are enforced by construction (uvx `tool@version`); a pin that appears in a workflow but not the Makefile (or vice versa) is a binding defect. | **Never `cargo test --all-features`** — see [AGENTS.md](../../AGENTS.md) "PyO3 build notes". |
| `light_thresholds` | ≤ 150 changed lines and ≤ 5 files (spine defaults) + the six spine criteria all holding | `map.md`-only or docs-only edits are the typical LIGHT candidates here. |
| `context_break_mechanics` | **Procedural in-session break** (default): one session runs Actor, then declares the break and shifts to Critic with inputs restricted per R3 — named honestly as procedural, not amnesia. **Sub-agent hard break**: only on explicit user opt-in per `<subagent_policy>` (Sonnet/Haiku default; Opus only when the user names Opus). | R3 prefers the hard break; this repo's sub-agent policy gates it, so the procedural break is the standing default. |
| `s0_fresh_execution` | For claims whose failure class is **silently wrong results** (engine-vs-reference divergence, numeric/type fidelity, Arrow-boundary schema): the Critic freshly executes ≥ 1 adversarial input through the **public entry point of the surface under claim** — once the engine lands (phase 1+), the native DataFrame / ANSI SQL door; once the facade lands (phase 3), the facade on the built wheel driving `collect`/`to_arrow` — and cites that run in the attestation (spine R3 / ref 05 step 5). Standing detector: the **entry-point matrix** mandated by [docs/testing.md](../../docs/testing.md) — new entry points must join the matrix; new divergence classes get a row per door. **Masking surfaces (never sole evidence):** `show`-style pretty-printed/preview output. At phase 0 the repo has no code surface, so no unit yet carries this failure class; the binding takes effect with the first ported surface. | The compensating control for this repo's procedural (non-amnesiac) break. |
| `metrics_ledger_location` | `task/metrics.md` (CREATE — installed at the first retrospective) | One section per retrospective, the ref-08 metric set verbatim — eight metrics incl. `environment_drift_events` (spine v2.1+). |
| `taxonomy_extensions` | **None** — the ten spine categories as canon defines them. | Extend-only; an extension widens the Critic's attestation duty on every subsequent unit — add deliberately, via feed-forward. |

## Pointers
Up: [CLAUDE.md](../../CLAUDE.md) (read-order + `## Precedence`) and [AGENTS.md](../../AGENTS.md) (the
authoritative contract). Related: [SKILL.md](SKILL.md) (the spine);
[references/map.md](references/map.md) (the canonical instrument homes); [../map.md](../map.md)
(the `skills/` container).

## Debug
- A SEPMO behavior contradicts the engineering contract → the contract wins; fix the manifest/usage,
  never the spine or references (they are portable canon; D2 — file defects back to the user).
- The same status appears twice → de-duplication breach (spine Global conventions / Invariant V); the
  Status SSOT (PROJECT.md "Current state" / task/todo.md) is the only home.
- A row points at a missing file → manifest is stale; fix the row.
- SEPMO's Actor–Critic seems to need sub-agents → it does **not** by default here: run the procedural
  context break per `context_break_mechanics` above. Fan out to real sub-agents **only** on an explicit
  user request, and Opus sub-agents only when the user names Opus (`<subagent_policy>`).
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

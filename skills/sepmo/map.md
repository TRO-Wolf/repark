# map — skills/sepmo/

## Purpose

The **SEPMO v2 control plane**: a portable software-engineering governance-and-orchestration layer
(proposition-ledger scope audit → adversarial Actor–Critic with coverage-attested review → per-PR
delivery → quantitative retrospective) that sits **on top of** this repo's engineering contract and
binds to it. SEPMO governs *lifecycle only*; it cedes every engineering decision to
[CLAUDE.md](../../CLAUDE.md) / [AGENTS.md](../../AGENTS.md) (see [CLAUDE.md](../../CLAUDE.md)
`## Precedence`).

## Contents

- [SKILL.md](SKILL.md) — the **spine** (versioned canon, frontmatter `version: "2.2"` + changelog;
  verbatim and portable): the Iron State Machine (T1–T12), the ledger gate with the v2.2
  **enumeration obligation** for quantified clauses, the S0–S3 scale, sub-machine rules R1–R10
  (v2.1: R7 two-tier green + R10 environment drift; v2.2: R2 per-element pinning + domain-growth
  inheritance, R3 fresh-execution with the novelty standard, incident retrospectives + asymmetric
  feed-forward — every version a user-approved canon amendment), Invariant V, doctrines D1–D6, the
  agent roster, and the routing map to `references/`. **Do not edit** — portable canon; project
  facts belong in the manifest; spine defects are filed to the user (D2), never patched here. The
  master home is the operator's SEPMO canon repository outside this repo — canon lands there and
  propagates here byte-identical.
- [binding-manifest.template.md](binding-manifest.template.md) — the portable install template
  (ships with the distribution, spine v2.2+): `> Fill:` protocol, role rows, tunables incl.
  `s0_fresh_execution` and `taxonomy_extensions`, and the I-1…I-10 instantiation checklist.
  Instantiating it elsewhere is what produces that repo's `binding-manifest.md`; this repo's
  manifest is an instantiation of it, kept conformant by hand.
- [references/](references/map.md) — the eight per-phase canonical instrument homes (ledger format,
  orchestrator procedures, SLR format, actor/critic protocols, vigilance, delivery, retrospective).
  Portable canon, same edit rule as the spine.
- [binding-manifest.md](binding-manifest.md) — the **only** project-specific SEPMO file: declares
  `spine_version: v2.2`, resolves every abstract role to its canonical home here (all BIND;
  PR-unit grouping is BIND-and-map to `briefs/`) and carries the tunables (`severity_floor`,
  `green_commands` — two named gates + the exception-record rule, `light_thresholds`,
  `context_break_mechanics`, `s0_fresh_execution` — entry-point surface, standing detector,
  masking surfaces, `metrics_ledger_location`, `taxonomy_extensions`).

## I want to...

| ...do this | go to |
|---|---|
| Understand the SEPMO lifecycle / gates / rules | [SKILL.md](SKILL.md) |
| Run a phase (audit, orchestrate, build, critique, deliver, retro) | [references/map.md](references/map.md) → that phase's file |
| See how a SEPMO role or tunable maps to this repo | [binding-manifest.md](binding-manifest.md) |
| Find the engineering contract SEPMO defers to | [../../AGENTS.md](../../AGENTS.md) + [../../docs/skills/Opus.md](../../docs/skills/Opus.md) |
| Find the precedence chain on a conflict | [../../CLAUDE.md](../../CLAUDE.md) `## Precedence` |
| File the retrospective metrics | `task/metrics.md` (CREATE at the first retrospective — see the [manifest](binding-manifest.md) `metrics_ledger_location`) |
| Re-port SEPMO to another repo | rewrite only [binding-manifest.md](binding-manifest.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../../CLAUDE.md](../../CLAUDE.md) (read-order + precedence), [../../AGENTS.md](../../AGENTS.md)
  (authoritative contract).

## Debug

| Symptom | First check |
|---|---|
| A SEPMO rule conflicts with an engineering rule | The contract wins ([../../CLAUDE.md](../../CLAUDE.md) `## Precedence`); fix the manifest/usage, never the spine or references |
| Prose disagrees with the state machine | The spine's transition table (T1–T12) is normative; the prose is the defect — file it |
| A claim ("100/100", "converged", "mergeable") has no artifact | Invariant V alarm — demand the ledger / attestation / CI evidence ([references/06](references/06-vigilance.md)) |
| A manifest row points at a missing file | Manifest is stale; fix the row |
| Tempted to spawn sub-agents for the Actor–Critic loop | `context_break_mechanics` in the [manifest](binding-manifest.md): procedural break by default; fan out only on explicit request, Opus only when named |

First checks: [binding-manifest.md](binding-manifest.md) resolves roles + tunables; [SKILL.md](SKILL.md)
is the spine. Escalate to: [../map.md#debug](../map.md) → [../../CLAUDE.md](../../CLAUDE.md).

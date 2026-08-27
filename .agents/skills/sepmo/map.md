# map — .agents/skills/sepmo/

## Purpose

The **SEPMO v2 control plane**: a portable software-engineering governance-and-orchestration layer
(proposition-ledger scope audit → adversarial Actor–Critic with coverage-attested review → per-PR
delivery → quantitative retrospective) that sits **on top of** this repo's engineering contract and
binds to it. SEPMO governs *lifecycle only*; it cedes every engineering decision to
[AGENTS.md](../../../AGENTS.md) (see its `## Precedence`).

## Contents

- [SKILL.md](SKILL.md) — the **spine** (versioned canon, frontmatter `version: "2.3"` + changelog;
  verbatim and portable): the Iron State Machine (T1–T12), the ledger gate with the v2.2
  **enumeration obligation** for quantified clauses, the S0–S3 scale, sub-machine rules R1–R13
  (v2.1: R7 two-tier green + R10 environment drift; v2.2: R2 per-element pinning + domain-growth
  inheritance, R3 fresh-execution with the novelty standard, incident retrospectives + asymmetric
  feed-forward; v2.3: R11 executable contingencies, R12 recorded dispositions, R13 remand, and
  machinery-incident retrospectives — every version a user-approved canon amendment), Invariant V, doctrines D1–D6, the
  agent roster, and the routing map to `references/`. **Do not edit** — portable canon; project
  facts belong in the manifest; spine defects are filed to the user (D2), never patched here. The
  master home is the operator's SEPMO canon repository outside this repo — canon lands there and
  propagates here byte-identical.
- [binding-manifest.template.md](binding-manifest.template.md) — the portable install template
  (ships with the distribution, spine v2.3+): `> Fill:` protocol, role rows, tunables incl.
  `s0_fresh_execution` and `taxonomy_extensions`, and the I-1…I-10 instantiation checklist.
  Instantiating it elsewhere is what produces that repo's `binding-manifest.md`; this repo's
  manifest is an instantiation of it, kept conformant by hand.
- [references/](references/map.md) — the eight per-phase canonical instrument homes (ledger format,
  orchestrator procedures, SLR format, actor/critic protocols, vigilance, delivery, retrospective).
  Portable canon, same edit rule as the spine.
- [binding-manifest.md](binding-manifest.md) — the **only** project-specific SEPMO file: declares
  `spine_version: v2.3` (re-bound 2026-08-24), resolves every abstract role to its canonical home here (all BIND;
  PR-unit grouping is BIND-and-map to `briefs/`; since DL-2, 2026-08-23, a **ledger grammar
  instrument** row binds the shape of the proposition ledger, the `pins:` citation and the
  Critic's attestation to `scripts/check_ledger_grammar.py`, XML measured and declined) and
  carries the tunables (`severity_floor`,
  `green_commands` — two named gates + the exception-record rule, `light_thresholds`
  — the prose-only LIGHT class (re-bound 2026-08-25), `context_break_mechanics`,
  `s0_fresh_execution` — entry-point surface, standing detector, masking surfaces;
  [`review_profile` and `critic_engine`](binding-manifest.md) — review selection and binding;
  `metrics_ledger_location`; `taxonomy_extensions`).
- [unit-runbook.md](unit-runbook.md) — the **unit running order**
  first (PROC-1, 2026-08-25): pickup → tier → build → Critic stage → ledger/pins/attestation →
  departure, every line a pointer into the spine, a manifest row, a reference file or a gate. It
  restates no rule and is held at 5,000 B so it cannot become a second spine.

## I want to...

| ...do this | go to |
|---|---|
| Understand the SEPMO lifecycle / gates / rules | [SKILL.md](SKILL.md) |
| Know how to run a unit under its tier (the per-tier checklist) | [unit-runbook.md](unit-runbook.md) |
| Run a phase (audit, orchestrate, build, critique, deliver, retro) | [references/map.md](references/map.md) → that phase's file |
| See how a SEPMO role or tunable maps to this repo | [binding-manifest.md](binding-manifest.md) |
| Find the engineering contract SEPMO defers to | [../../../AGENTS.md](../../../AGENTS.md) + [../../../.agents/skills/engineering-method/SKILL.md](../engineering-method/SKILL.md) |
| Find the precedence chain on a conflict | [../../../AGENTS.md](../../../AGENTS.md) `## Precedence` |
| File the retrospective metrics | `task/metrics.md` (CREATE at the first retrospective — see the [manifest](binding-manifest.md) `metrics_ledger_location`) |
| Re-port SEPMO to another repo | rewrite only [binding-manifest.md](binding-manifest.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../../../AGENTS.md](../../../AGENTS.md) (the authoritative contract + its `## Precedence`
  chain); [../../../CLAUDE.md](../../../CLAUDE.md) (the Claude tool adapter).

## Debug

| Symptom | First check |
|---|---|
| A SEPMO rule conflicts with an engineering rule | The contract wins ([../../../AGENTS.md](../../../AGENTS.md) `## Precedence`); fix the manifest/usage, never the spine or references |
| Prose disagrees with the state machine | The spine's transition table (T1–T12) is normative; the prose is the defect — file it |
| A claim ("100/100", "converged", "mergeable") has no artifact | Invariant V alarm — demand the ledger / attestation / CI evidence ([references/06](references/06-vigilance.md)) |
| A manifest row points at a missing file | Manifest is stale; fix the row |
| Tempted to spawn sub-agents for the Actor–Critic loop | `context_break_mechanics` in the [manifest](binding-manifest.md): procedural break by default; fan out only on explicit request, Opus only when named |

First checks: [binding-manifest.md](binding-manifest.md) resolves roles + tunables; [SKILL.md](SKILL.md)
is the spine. Escalate to: [../map.md#debug](../map.md) → [../../../CLAUDE.md](../../../CLAUDE.md).

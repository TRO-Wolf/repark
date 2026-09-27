# map — .agents/skills/critic-critic-critic/

## Purpose

The **Critic–Critic–Critic (CCC)** review skill: one adversarial Critic working four lenses
(quality and crates contracts → security and safety → pure logic → the change's own claims) in
one pass behind a context break, with a coverage attestation per lens and one findings report;
a high-risk change adds one independent pass (owner ruling 2026-09-27). Review-only by default. **It is the Critic engine this repository's SEPMO binds** through the manifest's
[`critic_engine`](../sepmo/binding-manifest.md) row; it can also run alone on a diff. The manifest
owns the binding and tier effort. This map does not restate them. Tool-neutral: how a Critic is
spawned is each tool adapter's table.

## Contents

- [SKILL.md](SKILL.md) — the skill: `## Procedure` (run-the-loop router: params, tier,
  charter, the Critic pass, re-review, the high-tier independent pass, convergence, report)
  over `## Reference` (parameters, risk tiers, S0–S3, absolute rules, spawn contract,
  crates/library attack contract, convergence labels, the four-lens workflow, finding schema,
  required report, SEPMO-engine mapping, anti-patterns, quick start).
- [history.md](history.md) — the skill's provenance record, moved out of SKILL.md by the
  2026-09-27 read-path split, with the 2026-09-27 one-Critic ruling and what it did not
  measure; SKILL.md points here.
- [references/](references/map.md) — the four role prompts, one per lens, each with its attack
  taxonomy, attestation form, finding prefixes and grep signals.

## I want to...

| ...do this | go to |
|---|---|
| Run the loop on a diff | [SKILL.md](SKILL.md) "Quick start examples" |
| See what a Critic must attack before it may say "clean" | the role's reference under [references/](references/map.md) |
| See how CCC maps onto SEPMO's attestation and findings | [SKILL.md](SKILL.md) "As the SEPMO Critic engine" + the manifest row |
| Find the tool-specific spawn mapping | the tool adapter — [../../../CLAUDE.md](../../../CLAUDE.md) for Claude |

## Pointers

- Up: [../map.md](../map.md)
- Binds under: [../sepmo/map.md](../sepmo/map.md); the engineering contract it never overrides:
  [../../../AGENTS.md](../../../AGENTS.md).

## Debug

- A Critic report says "pass" with no null report per category → invalid by rule; re-run the lens.
- `CCC-CONVERGED` was read as ready-to-merge → it never is; `PR_READINESS_AUDIT` still runs (R7).
- A spawn-mechanics question (agent type, isolation, capability flags) → the adapter, not this skill.

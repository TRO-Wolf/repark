# map — .agents/skills/critic-critic-critic/

## Purpose

The **Critic–Critic–Critic (CCC)** review skill: three specialised adversarial Critics (quality
and crates contracts → security and safety → pure logic) plus a fourth that attacks the change's
own claims, each with a context break, a coverage attestation and a findings ledger. Review-only
by default. **It is the Critic engine this repository's SEPMO binds** through the manifest's
[`critic_engine`](../sepmo/binding-manifest.md) row; it can also run alone on a diff. The manifest
owns the binding and tier effort. This map does not restate them. Tool-neutral: how a Critic is
spawned is each tool adapter's table.

## Contents

- [SKILL.md](SKILL.md) — the skill: parameters, risk tiers, the S0–S3 severity scale, the absolute
  rules, the tool-neutral spawn contract, the crates/library attack contract, convergence labels,
  the four-phase workflow, the finding schema, the required report, and how it serves as the SEPMO
  Critic engine.
- [references/](references/map.md) — the four role prompts, one per Critic, each with its attack
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

- A Critic report says "pass" with no null report per category → invalid by rule; re-run the phase.
- `CCC-CONVERGED` was read as ready-to-merge → it never is; `PR_READINESS_AUDIT` still runs (R7).
- A spawn-mechanics question (agent type, isolation, capability flags) → the adapter, not this skill.

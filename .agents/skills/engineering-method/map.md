# map — .agents/skills/engineering-method/

## Purpose

The portable, agent-agnostic working method for implementation and review sessions: risk-first
design, the reason-plan-verify workflow, naming, the Rust/Python defaults, the debugging
protocol, and the done gate. It generalizes the former per-model-tier manuals (`docs/skills/`,
removed 2026-08-24) into one instruction set any tool's agent reads. The rule of record for every
project fact stays [../../../AGENTS.md](../../../AGENTS.md); this skill records the method and
loses on any conflict.

## Contents

- [SKILL.md](SKILL.md) — the method: Identity & Priority Stack → Non-Negotiables → Mode Handling →
  Risk-First → Workflow §1–§9 → Navigation (`map.md`) → Naming → Language-Specific Rules →
  Function Length & Recursion → Pre-Flight → Core Principles (TL;DR).

## I want to...

| I want to... | go to |
| --- | --- |
| Read the full working method for a session | [SKILL.md](SKILL.md) |
| Find the must-not-violate list | [SKILL.md](SKILL.md) `<non_negotiables>` (each row points at its spine home) |
| Run the pre-implementation risk pass | [SKILL.md](SKILL.md) `<risk_first>` |
| Check whether a task is done | [SKILL.md](SKILL.md) `<verification_gate>` (the §4 Done gate) |
| Debug a failure methodically | [SKILL.md](SKILL.md) §8 + the touched directory's `map.md#debug` |
| Look up the Python conventions' reasoning | [../code-quality/SKILL.md](../code-quality/SKILL.md) — rule of record [../../../AGENTS.md](../../../AGENTS.md) "Python" |
| Brief a delegated agent on a capability tier | the running tool's adapter ([../../../CLAUDE.md](../../../CLAUDE.md) / [../../map.md](../../map.md)) — tier postures are tool mechanics, not method |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../../../AGENTS.md](../../../AGENTS.md) (the authoritative contract this skill serves);
  [../../../docs/testing.md](../../../docs/testing.md) (the testing contract §4 binds);
  [../code-quality/SKILL.md](../code-quality/SKILL.md) + [../rust-code-quality/SKILL.md](../rust-code-quality/SKILL.md)
  (the per-language review instruments).

## Debug

First checks: this skill is method, not law — on any conflict re-read
[../../../AGENTS.md](../../../AGENTS.md) (its `## Precedence` chain wins). Tier-specific briefing
content lives in the tool adapters, not here. Escalate to: [../map.md#debug](../map.md).

# map — skills/

## Purpose

Portable, tool-agnostic **agent control planes** installed into this repo. These are operating layers an
agent runs *under* — they bind to, and defer to, the repo's engineering contract
([../CLAUDE.md](../CLAUDE.md) / [../AGENTS.md](../AGENTS.md)).

## Contents

- [sepmo/](sepmo/map.md) — the **SEPMO** governance-and-orchestration control plane (state machine +
  scope-audit gate + adversarial Actor–Critic) and its binding manifest (the one file mapping SEPMO's
  abstract roles to this repo).

## I want to...

| ...do this | go to |
|---|---|
| Operate under / understand SEPMO | [sepmo/map.md](sepmo/map.md) → [sepmo/SKILL.md](sepmo/SKILL.md) |
| See SEPMO's bindings to this repo | [sepmo/binding-manifest.md](sepmo/binding-manifest.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../CLAUDE.md](../CLAUDE.md) (read-order + `## Precedence`).

## Debug

First checks: each subdirectory carries its own `map.md` (start at [sepmo/map.md](sepmo/map.md)).
Escalate to: [../map.md#debug](../map.md).

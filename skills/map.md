# map — skills/

## Purpose

Portable, tool-agnostic **agent control planes** installed into this repo. These are operating layers an
agent runs *under* — they bind to, and defer to, the repo's engineering contract
([../CLAUDE.md](../CLAUDE.md) / [../AGENTS.md](../AGENTS.md)).

## Contents

- [sepmo/](sepmo/map.md) — the **SEPMO** governance-and-orchestration control plane (state machine +
  scope-audit gate + adversarial Actor–Critic) and its binding manifest (the one file mapping SEPMO's
  abstract roles to this repo).
- [code-quality/](code-quality/map.md) — the portable **code-quality convention set**: the four
  Python rules, the failure each prevents, whether it is held by a linter, a gate or review, and the
  ratchet method for arming a convention against a codebase that already violates it. The rule of
  record stays [../AGENTS.md](../AGENTS.md) "Python"; the enforcement SSOT is
  [../scripts/check_python_conventions.py](../scripts/check_python_conventions.py).

## I want to...

| ...do this | go to |
|---|---|
| Operate under / understand SEPMO | [sepmo/map.md](sepmo/map.md) → [sepmo/SKILL.md](sepmo/SKILL.md) |
| See SEPMO's bindings to this repo | [sepmo/binding-manifest.md](sepmo/binding-manifest.md) |
| Write or review Python under the conventions | [code-quality/map.md](code-quality/map.md) → [code-quality/SKILL.md](code-quality/SKILL.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../AGENTS.md](../AGENTS.md) (the authoritative contract + its `## Precedence` chain).

## Debug

First checks: each subdirectory carries its own `map.md` (start at [sepmo/map.md](sepmo/map.md)).
Escalate to: [../map.md#debug](../map.md).

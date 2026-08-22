# map — .agent/skills/code-quality/

## Purpose

The portable **code-quality convention set**: the four Python rules (types on everything, Pydantic
v2 rather than `dataclasses`/`attrs`, no nested function definitions, names that carry the work),
the general rules they sit inside, and the method for turning a convention into a gate without a
flag day.

It is tool-agnostic and repo-agnostic on purpose. For **this** repo the authoritative statement is
[../../../AGENTS.md](../../../AGENTS.md) "Python"; the skill explains the reasoning and the arming
method that the contract compresses into one bullet. It is not a second contract: on any conflict
the spine wins.

## Contents

- [SKILL.md](SKILL.md) — the rules, each with the failure it prevents and how it is held (linter,
  purpose-built gate, or review), plus the ratchet pattern for arming a rule against a codebase
  that already violates it.

## I want to...

| ...do this | go to |
|---|---|
| Read the rule of record for this repo | [../../../AGENTS.md](../../../AGENTS.md) "Python" |
| Understand why a rule exists, and its sanctioned exceptions | [SKILL.md](SKILL.md) |
| See the enforcement tables (ceilings, exception rows) | [../../../scripts/check_python_conventions.py](../../../scripts/check_python_conventions.py) — the SSOT; prose never restates them |
| Arm a new convention as a gate | [SKILL.md](SKILL.md) "Arming a rule" |
| Find the conformance work still outstanding | [../../../STATUS.md](../../../STATUS.md) "Active workstreams" (PYC) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../../../docs/skills/Opus.md](../../../docs/skills/Opus.md) (the per-tier operating
  manual that restates these rules at working density).

## Debug

First checks: a red `make check-python-conventions` prints the file, the sites and the sanctioned
outs — read those before editing the tables. The gate is dual-wired (`make ci` + ci.yml's `python`
job) and runs in both pre-commit paths. Escalate to: [../map.md#debug](../map.md).

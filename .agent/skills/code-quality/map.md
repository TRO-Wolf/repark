# map — .agent/skills/code-quality/

## Purpose

The portable **Python code-quality convention set** (v2.0): Ruff as the single lint/format tool,
types on everything with named intermediate steps, Pydantic v2 rather than `dataclasses`/`attrs`,
no nested function definitions and no unbounded recursion, names that carry the work, import and
lazy-dataframe discipline, comments written for the eventual reader in Simplified Technical
English, mandatory tests with a verification gate — and the method for turning a convention into
a gate without a flag day. Rust review lives in its own skill:
[../rust-code-quality/SKILL.md](../rust-code-quality/SKILL.md).

It is tool-agnostic and repo-agnostic on purpose. For **this** repo the authoritative statement is
[../../../AGENTS.md](../../../AGENTS.md) "Python"; the skill explains the reasoning and the arming
method that the contract compresses into one bullet. It is not a second contract: on any conflict
the spine wins.

## Contents

- [SKILL.md](SKILL.md) — fourteen sections: the Ruff baseline (§1, an illustrative floor — the
  host repo's `pyproject.toml` is SSOT), the language and typing rules (§2–§3), function shape
  (§4), naming (§5), imports (§6), pre-built-first (§7), lazy dataframes (§8), eventual-reader
  comment discipline (§9), testing and the verification gate (§10–§11), condensed principles
  (§12), the arming ratchet (§13), and the sweep invariant for existing code (§14). Each rule is
  tagged with how it is held: linter, purpose-built gate, or review.

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

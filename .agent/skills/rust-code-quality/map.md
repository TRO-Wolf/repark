# map — .agent/skills/rust-code-quality/

## Purpose

The **Rust review procedure**: what to check in a Rust diff that the armed gates
(`make rust-clippy`, `make rust-panic-ban`, the file-size ratchet) cannot catch —
escape hatches, Spark-visible behavior, ANSI dual-door coverage, float
semantics, hot-path allocation, the error contract — with a severity scale
ordered for a query engine (silently wrong results outrank crashes).

It is a review *sequence*, not a second contract: every rule it leans on points
into [../../../AGENTS.md](../../../AGENTS.md),
[../../../docs/testing.md](../../../docs/testing.md), or
[../../../docs/skills/Opus.md](../../../docs/skills/Opus.md) "Rust", and on any
conflict those win.

## Contents

- [SKILL.md](SKILL.md) — the gate inventory (what never to re-review), the
  candidate scans, the manual checklist, severity, the report template, and the
  arming candidates (output macros → `rust-panic-ban`).

## I want to...

| ...do this | go to |
|---|---|
| Review a Rust PR or commit | [SKILL.md](SKILL.md) "Quick start" |
| Know what the gates already enforce | [SKILL.md](SKILL.md) "What the gates already hold" |
| Read the rule of record | [../../../AGENTS.md](../../../AGENTS.md) + [../../../docs/testing.md](../../../docs/testing.md) |
| Turn a checklist item into a gate | [../code-quality/SKILL.md](../code-quality/SKILL.md) §13 (the arming ratchet) |
| Review Python instead | [../code-quality/SKILL.md](../code-quality/SKILL.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../../../docs/skills/Opus.md](../../../docs/skills/Opus.md) (the
  per-tier manual whose "Rust" sections this skill cites rather than restates).

## Debug

| Symptom | First check |
|---|---|
| A checklist item duplicates a gate | Bug — delete the item, cite the gate ([SKILL.md](SKILL.md) "What the gates already hold") |
| The skill states a project rule | Bug — move it to the spine, leave a pointer (`.agent/` contract) |
| A cited file or make target no longer exists | Fix the skill in the same PR as the change that falsified it |

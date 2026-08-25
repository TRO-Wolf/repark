# CLAUDE.md — the Claude adapter (not authoritative)

**STOP — the authoritative contract is [AGENTS.md](AGENTS.md). Read it first.** This file adds
only Claude-specific tool mechanics; it defines **no project rules**. Every project fact lives in
the neutral spine, and this adapter only points at it — so it cannot drift, and deleting it would
lose no project knowledge.

## Where the project rules actually live

CLAUDE.md restates nothing. Follow the pointers:

| For… | Read (authoritative) |
|---|---|
| The read path + the rules governing any change | [AGENTS.md](AGENTS.md) (start at its "Read first") |
| The precedence / authority chain | [AGENTS.md `## Precedence`](AGENTS.md#precedence) — its single home |
| Current state (release, delivered crates, active/deferred work) | [STATUS.md](STATUS.md) |
| Component boundaries, the crate DAG, runtime flows | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Local setup, `make` targets, CI surface, troubleshooting | [DEVELOPMENT.md](DEVELOPMENT.md) |
| Gate roster (`verify` / `preflight` / facade) | [AGENTS.md `## Verify before "done"`](AGENTS.md#verify-before-done) — `verify` is Rust-only; `preflight` adds `py-test-facade` + audit + workflow lint. Keep this pointer in lockstep with AGENTS.md; do not invent a second roster. |
| The testing-discipline contract (hard block) | [docs/testing.md](docs/testing.md) |
| Product intent / north star | [PROJECT.md](PROJECT.md) |
| Navigation for a directory you will touch | that directory's `map.md` |

## Claude read order (every session)

1. **[AGENTS.md](AGENTS.md) first**, then follow its "Read first" path
   (README → STATUS → ARCHITECTURE → DEVELOPMENT → AGENTS.md → docs/testing.md).
2. **The engineering method** —
   [.agents/skills/engineering-method/SKILL.md](.agents/skills/engineering-method/SKILL.md), the
   portable agent-agnostic working method (risk-first, workflow, naming, Rust/Python defaults,
   debugging, the done gate). One instruction set for every tier — not a separate source of truth.
3. The `map.md` of every directory your task will touch (AGENTS.md "`map.md` in every directory").

CLAUDE.md keeps this filename so Claude tooling that auto-loads it still fires and lands you on
AGENTS.md on turn 1.

## Claude tool mechanics — skills are invocable here

`.claude/skills` is a symlink to `../.agents/skills` (git mode `120000`), so every runbook there
loads natively in a Claude session and can be invoked by name rather than opened by path. The
skills keep their single home under `.agents/`; this directory adds no second copy and states no
rule. Roster and reasoning: [.agents/skills/map.md](.agents/skills/map.md).

The SEPMO control plane lives in that same home — [.agents/skills/sepmo/](.agents/skills/sepmo/map.md),
moved from a top-level `skills/` tree on 2026-08-25 — so the symlink covers it and `/sepmo` is
invocable by name. Discoverable is not auto-run: it is invoked deliberately for non-trivial work.

## Claude tool mechanics — capability tiers and sub-agents

These are Claude-family orchestration mechanics, **not** project rules. AGENTS.md "Delegated work"
is the neutral rule; this is how it maps onto Claude tiers:

- Opus orchestrates and owns architecture and assembly.
- Delegated fan-out (search, mechanical edits, narrow implementation) defaults to **Sonnet** or
  **Haiku** — pass the tier explicitly, and brief the tier's posture: **Sonnet** is the delegated
  implementation tier (executes well-scoped work; architecture and cross-cutting decisions stay
  with the orchestrating session — surface ambiguity rather than inventing); **Haiku** is the
  narrow mechanical tier (precisely specified edits; stop and hand back the moment the task needs
  a design decision). Every tier reads the same
  [engineering method](.agents/skills/engineering-method/SKILL.md); the non-negotiables are
  identical across tiers.
- **Do not spawn Opus sub-agents without a direct, explicit request naming Opus.**
- Single agent in the main thread is the default; do not fan out unless the user asks.

Relax this section by editing it and noting the change in [task/lessons.md](task/lessons.md).

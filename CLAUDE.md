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
| The testing-discipline contract (hard block) | [docs/testing.md](docs/testing.md) |
| Product intent / north star | [PROJECT.md](PROJECT.md) |
| Navigation for a directory you will touch | that directory's `map.md` |

## Claude read order (every session)

1. **[AGENTS.md](AGENTS.md) first**, then follow its "Read first" path
   (README → STATUS → ARCHITECTURE → DEVELOPMENT → AGENTS.md → docs/testing.md).
2. **The operating manual for your model tier** in [docs/skills/](docs/skills/) — one model
   family's view of the engineering conventions, not a separate source of truth:
   [Opus.md](docs/skills/Opus.md) (the fullest write-up), [Sonnet.md](docs/skills/Sonnet.md), or
   [Haiku.md](docs/skills/Haiku.md). Read the one matching the model you are running as.
3. The `map.md` of every directory your task will touch (AGENTS.md "`map.md` in every directory").

CLAUDE.md keeps this filename so Claude tooling that auto-loads it still fires and lands you on
AGENTS.md on turn 1.

## Claude tool mechanics — capability tiers and sub-agents

These are Claude-family orchestration mechanics, **not** project rules. AGENTS.md "Delegated work"
is the neutral rule; this is how it maps onto Claude tiers:

- Opus orchestrates and owns architecture and assembly.
- Delegated fan-out (search, mechanical edits, narrow implementation) defaults to **Sonnet** or
  **Haiku** — pass the tier explicitly. See [docs/skills/Sonnet.md](docs/skills/Sonnet.md) and
  [docs/skills/Haiku.md](docs/skills/Haiku.md).
- **Do not spawn Opus sub-agents without a direct, explicit request naming Opus.**
- Single agent in the main thread is the default; do not fan out unless the user asks.

Relax this section by editing it and noting the change in [task/lessons.md](task/lessons.md).

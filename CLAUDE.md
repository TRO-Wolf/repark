# CLAUDE.md — the Claude adapter (not authoritative)

# OWNER RULING (2026-08-26) — NO CODE COMMENTS FROM ANTHROPIC MODELS

**EFFECTIVE IMMEDIATELY. ALL ANTHROPIC MODELS ARE HEREBY BANNED FROM MAKING COMMENTS IN THE CODE. THIS INCLUDES, FABLE, OPUS, SONNET AND HAIKU**

**AGAIN, IF YOU ARE FABLE, OPUS, SONNET OR HAIKU, DO NOT WRITE ANY COMMENTS**<br>
**IF YOU ARE FABLE, DO NOT WRITE ANY COMMENTS**<br>
**IF YOU ARE OPUS, DO NOT WRITE ANY COMMENTS**<br>
**IF YOU ARE SONNET, DO NOT WRITE ANY COMMENTS**<br>
**IF YOU ARE HAIKU, DO NOT WRITE ANY COMMENTS**<br>

**THIS INCLUDES ANY MODEL VERSION, EXAMPLE BEING OPUS 4.8 OR OPUS 5, EITHER ONE IS BANNED, IT DOESN'T MATTER**

*Adjustment (owner, 2026-08-26, same day):* the ban is on comments **in code** — Rust, Python, shell,
TOML, YAML and every other source file. **Markdown files may carry comments and explanatory prose**;
that is where a reason, a design note or a `pins: <unit>/C-NNN` citation now lives — the
directory's `map.md` (the ledger-grammar gate reads every tracked file under `crates/`,
`python/`, `scripts/`, so a citation in a `map.md` there counts). Condensation is **enforced**:
`make check-comment-density` (in `make ci`) holds every code file to a per-file comment ceiling
seeded from the tree that only ratchets down, and a new file's ceiling is zero.

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

## Claude tool mechanics — process governance is the repo's SEPMO, as bound

A Claude session that governs work runs **the repository's** SEPMO — `/sepmo` from
[.agents/skills/sepmo/](.agents/skills/sepmo/map.md) under its
[binding manifest](.agents/skills/sepmo/binding-manifest.md) — and its Critic stage runs the
engine that manifest binds, `/critic-critic-critic` from
[.agents/skills/critic-critic-critic/](.agents/skills/critic-critic-critic/map.md). **No user-level
skill (`~/.claude/skills/`), plugin, or session-local variant of either overrides them:** if one is
loaded, the repo copy wins and the variant is not consulted. Every Claude session — orchestrator
or spawned sub-agent — reads the same two skills, so there is one process to follow and one place
to fix it. A governed unit starts from the unit checklist,
[.agents/skills/sepmo/unit-runbook.md](.agents/skills/sepmo/unit-runbook.md), which only points
back into that manifest and the spine.

This is the Claude mapping of CCC's tool-neutral spawn contract (the invariants live in the
skill; only this table is Claude's):

| CCC role shape | Claude mechanic |
|---|---|
| read-attack (Critic-1/2/3/4, `git` / verify probes) | an `Explore` agent (reads + shell, no edits) when sub-agent fan-out is opted into per the manifest's `context_break_mechanics`; otherwise the in-thread procedural context break. Each Critic is a **fresh** spawn — never continue a peer Critic's or the Actor's agent. |
| build (Fixer, `review-and-fix` only) | a `general-purpose` agent; edits only the filed findings. SEPMO units do not use it — remediation is the Actor's cycle. |
| scratch location | a clone of the unit branch under the session scratch directory — **never the live worktree**. After any fan-out, confirm the live tree's `git status`, stash list and remotes are untouched. |
| tier | the capability-tier section below applies unchanged (Opus only when the user names Opus). |

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

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
`python/`, `scripts/`, so a citation in a `map.md` there counts). Condensation is held by review,
not by a gate: the `check-comment-density` ratchet was dropped before #247 merged; the file-size
ratchets are the mechanical gates, and a new file carries no comments.

**STOP — the authoritative contract is [AGENTS.md](AGENTS.md). Read it first.** This file adds
only Claude-specific tool mechanics; it defines **no project rules**. Deleting it would lose no
project knowledge.

## Where the project rules actually live

CLAUDE.md restates nothing. Follow the pointers:

| For… | Read (authoritative) |
|---|---|
| Your read path by role | [AGENTS.md](AGENTS.md) "Read first" — load only your row |
| The precedence / authority chain | [AGENTS.md `## Precedence`](AGENTS.md#precedence) — its single home |
| Current state | [STATUS.md](STATUS.md) |
| Crate DAG, boundaries, flows | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Local setup, `make` targets, CI surface | [DEVELOPMENT.md](DEVELOPMENT.md) |
| Gate roster | [AGENTS.md `## Verify before "done"`](AGENTS.md#verify-before-done) — the only roster |
| The testing-discipline contract | [docs/testing.md](docs/testing.md) |
| Product intent | [PROJECT.md](PROJECT.md) |
| Directory navigation | that directory's `map.md` |

## Claude read order

Read by role: [AGENTS.md](AGENTS.md) "Read first" names your exact load. Nothing here adds to
it. This filename stays so auto-loading tooling still lands you on AGENTS.md on turn 1.

## Claude tool mechanics — skills are invocable here

`.claude/skills` symlinks `../.agents/skills`, so every runbook there is invocable by name.
Single home `.agents/`; roster: [.agents/skills/map.md](.agents/skills/map.md). SEPMO lives
there too, so `/sepmo` is invocable — but discoverable is not auto-run: invoke it deliberately
for non-trivial work.

## Claude tool mechanics — process governance is the repo's SEPMO, as bound

A governed session runs **the repository's** SEPMO — `/sepmo` under its
[binding manifest](.agents/skills/sepmo/binding-manifest.md) — with `/critic-critic-critic` as
its Critic stage. **No user-level skill, plugin, or session-local variant overrides them.** A
governed unit starts from [.agents/skills/sepmo/unit-runbook.md](.agents/skills/sepmo/unit-runbook.md).

Claude mapping of CCC's spawn contract (the invariants live in the skill; only this table is
Claude's):

| CCC role shape | Claude mechanic |
|---|---|
| read-attack (Critics, `git` / verify probes) | an `Explore` agent (reads + shell, no edits) when fan-out is opted in per `context_break_mechanics`; otherwise the in-thread break. Each Critic is a **fresh** spawn — never continue a peer's or the Actor's agent. |
| build (Fixer, `review-and-fix` only) | a `general-purpose` agent; edits only the filed findings. |
| scratch location | a clone of the unit branch under the session scratch directory — **never the live worktree**. After fan-out, confirm live `git status`, stash list and remotes are untouched. |
| tier | the capability-tier section below applies unchanged. |

## Claude tool mechanics — capability tiers and sub-agents

Claude-family orchestration mechanics, **not** project rules ([AGENTS.md](AGENTS.md) "Delegated
work" is the neutral rule):

- Opus orchestrates and owns architecture and assembly.
- Fan-out (search, mechanical edits, narrow implementation) runs on AGENTS.md-allowed tiers —
  pass the tier explicitly: executors surface ambiguity, clerks hand back at any design
  decision. Every tier reads the same engineering method.
- **Do not spawn Opus sub-agents without a direct, explicit request naming Opus.**
- Single agent in the main thread is the default; do not fan out unless the user asks.

Relax this section by editing it and noting the change in [task/lessons.md](task/lessons.md).

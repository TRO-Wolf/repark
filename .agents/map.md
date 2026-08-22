# map — .agents/

## Purpose

Tool-neutral and per-tool **adapter** entry points for automated agents. Every file here carries
**zero authoritative facts** — each is a thin pointer into the authoritative spine
([AGENTS.md](../AGENTS.md) + [STATUS.md](../STATUS.md) + [ARCHITECTURE.md](../ARCHITECTURE.md) +
[DEVELOPMENT.md](../DEVELOPMENT.md)). Adapters cannot drift, and deleting any one loses no project
knowledge. The authority move that makes this possible is recorded in
[docs/history/frontdoor/agent-agnostic-frontdoor.md](../docs/history/frontdoor/agent-agnostic-frontdoor.md) §4.

## Contents

- `common.md` — the shared, tool-neutral start: read AGENTS.md first, then the spine. No rules.
- `claude.md` — points Claude sessions at [../CLAUDE.md](../CLAUDE.md) and
  [../docs/skills/](../docs/skills/).
- `codex.md`, `cursor.md` — one-line stubs pointing inward; no tool mechanics recorded yet.
- `skills/` — agent-facing runbook skills (release-to-PyPI, context-doc truth-up, disk
  headroom, the parity audit) plus the code-quality convention reasoning for Python and the
  Rust review procedure: proven sequences / reasoning, pointer-thin, zero authoritative facts.
  Each is a directory with a frontmatter-carrying `SKILL.md`, so it is discoverable rather than
  merely present, and `../.claude/skills` symlinks here so Claude sessions can invoke them
  natively. See [skills/map.md](skills/map.md).

## I want to...

| ...do this | go to |
|---|---|
| Onboard any agent, tool-agnostic | `common.md` → [../AGENTS.md](../AGENTS.md) |
| Onboard a Claude session | `claude.md` → [../CLAUDE.md](../CLAUDE.md) |
| Run a recurring operation (release, doc truth-up, disk check) | [skills/map.md](skills/map.md) |
| Write or review Python under the conventions | [skills/code-quality/SKILL.md](skills/code-quality/SKILL.md) |
| Review a Rust PR or commit | [skills/rust-code-quality/SKILL.md](skills/rust-code-quality/SKILL.md) |
| Audit parity or triage a parity-live red | [skills/audit-repark-parity/SKILL.md](skills/audit-repark-parity/SKILL.md) |
| Add mechanics for a new tool | add `.agents/<tool>.md` (pointer + tool mechanics only) + a Contents row here |
| Read the authoritative contract | [../AGENTS.md](../AGENTS.md) |

## Pointers

- Up: [../map.md](../map.md)
- Authoritative spine: [../AGENTS.md](../AGENTS.md), [../STATUS.md](../STATUS.md),
  [../ARCHITECTURE.md](../ARCHITECTURE.md), [../DEVELOPMENT.md](../DEVELOPMENT.md).

## Debug

| Symptom | First check |
|---|---|
| An adapter states a project rule | Bug — move the rule to the authoritative spine and leave a pointer |
| An agent starts in the wrong place | Every adapter must route to AGENTS.md first (`common.md` is the shared head) |

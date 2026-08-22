# .agents/common.md — shared, tool-neutral entry point

Any agent working in this repo starts here, then reads the authoritative spine. This file carries
**no project rules** — only pointers.

- **The authoritative contract:** [AGENTS.md](../AGENTS.md). Read it first; it holds the precedence
  chain, the invariants, the change-location guide, verification, and the safety boundaries.
- **Current state:** [STATUS.md](../STATUS.md) — release, delivered crates, active and deferred work.
- **Structure + runtime flows:** [ARCHITECTURE.md](../ARCHITECTURE.md).
- **Commands (setup, `make` targets, CI, troubleshooting):** [DEVELOPMENT.md](../DEVELOPMENT.md).
- **Testing contract (hard block):** [docs/testing.md](../docs/testing.md).

Tool-specific mechanics (if any) live in the per-tool adapter beside this file. An adapter never
restates an authoritative fact.

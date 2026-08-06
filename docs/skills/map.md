# map — docs/skills/

## Purpose

Per-model-tier operating manuals — variants of the same engineering contract. Read the one matching
the model you are running as.

## Contents

- [Opus.md](Opus.md) — canonical full contract (orchestration, naming, Rust/Python rules, debugging,
  verification gates).
- [Sonnet.md](Sonnet.md) — delegated-implementation tier posture + non-negotiables.
- [Haiku.md](Haiku.md) — narrow/mechanical tier posture + non-negotiables.

Not ported yet (returns with phase 1, alongside the code it governs): `trait-wrapping.md` — the
silent-default gap when wrapping a trait (enumerate + forward every method, defaults included;
both-sides audit method). It lives in the private v1 repository until crate code lands here.

## Pointers

- Up: [../map.md](../map.md)
- Related: [../../AGENTS.md](../../AGENTS.md) (the project contract these manuals serve).

## Debug

First checks: Opus.md is canonical; Sonnet/Haiku state only deltas + non-negotiables. Escalate to:
[../map.md#debug](../map.md).

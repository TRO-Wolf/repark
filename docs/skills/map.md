# map — docs/skills/

## Purpose

Per-model-tier operating manuals — variants of the same engineering contract. Read the one matching
the model you are running as.

## Contents

- [Opus.md](Opus.md) — canonical full contract (orchestration, naming, Rust/Python rules, debugging,
  verification gates).
- [Sonnet.md](Sonnet.md) — delegated-implementation tier posture + non-negotiables.
- [Haiku.md](Haiku.md) — narrow/mechanical tier posture + non-negotiables.

**`trait-wrapping.md` was never ported, and no longer needs to be** (recorded 2026-08-10). The
manual it would have carried — the silent-default gap when wrapping a trait: enumerate and forward
every method, defaults included, audited from both sides — is now a **rule in force**, not a skill
file: the audit duty is in [../../AGENTS.md](../../AGENTS.md) "Version-pin contract" (re-enumerate
the wrapped catalog's trait surface at every fork repin), and the one open instance of the gap is a
named component limitation in
[../../crates/repark-iceberg/map.md](../../crates/repark-iceberg/map.md) "Known limitations".

## I want to...

| I want to... | go to |
| --- | --- |
| Read the full engineering contract for a session | [Opus.md](Opus.md) |
| Brief a delegated implementation agent (Sonnet tier) | [Sonnet.md](Sonnet.md) |
| Brief a narrow/mechanical agent (Haiku tier) | [Haiku.md](Haiku.md) |
| Apply the trait-wrapping both-sides audit | [../../AGENTS.md](../../AGENTS.md) "Version-pin contract" (see above) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../../AGENTS.md](../../AGENTS.md) (the project contract these manuals serve).

## Debug

First checks: Opus.md is canonical; Sonnet/Haiku state only deltas + non-negotiables. Escalate to:
[../map.md#debug](../map.md).

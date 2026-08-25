# map — .agents/skills/critic-critic-critic/references/

## Purpose

The four Critic role prompts. Each is loaded by exactly one phase of [../SKILL.md](../SKILL.md)
and carries that Critic's context-break preamble, role prompt, attack taxonomy, required coverage
attestation, finding-id prefixes, severity guidance, verdict form and grep signals.

## Contents

- [01-critic-quality-bugs.md](01-critic-quality-bugs.md) — Critic-1: quality, general bugs, test
  adequacy (the mutation-proof skeptic) and the CRATE-1..7 library contract (`Q-` / `CRATE-`).
- [02-critic-security-safety.md](02-critic-security-safety.md) — Critic-2: security and safety
  surfaces, atomicity and partial failure on commit paths (`SEC-` / `SAF-`).
- [03-critic-logic-bugs.md](03-critic-logic-bugs.md) — Critic-3: pure logic — wrong results,
  inverted predicates, incomplete matches, silent data loss (`L-`).
- [04-critic-claims-record.md](04-critic-claims-record.md) — Critic-4: the change's claims about
  itself (ledgers, maps, STATUS, reports, identity at `%ae`) re-executed against the tree (`CL-`).

## Pointers

- Up: [../map.md](../map.md)

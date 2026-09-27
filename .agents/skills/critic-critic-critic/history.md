# history — .agents/skills/critic-critic-critic/

The CCC skill's provenance record, moved out of [SKILL.md](SKILL.md) by the 2026-09-27
read-path split (presentation only; that split left review semantics unchanged). Historical rationale: where
the loop and its contract came from.

## Provenance

- Core loop: the SEPMO context-break Critics, coverage attestation, risk tiers, mutation-proof
  pins, multi-tree Critic scope ([../sepmo/references/05-critic.md](../sepmo/references/05-critic.md)).
- Critic-1 crates contract: library design / errors / concurrency / recursion / casts / testing /
  async (owner-supplied crates instructions, 2026-07-19).
- **2026-08-12:** taxonomy home; findings-only parallel; `claims_critic` default-on for
  ledger-bearing units; CL-IDENTITY.
- **2026-08-25:** imported into this repository from the owner's tool-local skill set at that
  revision (owner ruling: one Critic engine, in the tree, for every tool). The tool-specific
  spawn table left for the tool adapters; the identity literal in ref 04 became a pointer at the
  repository's own git configuration.
- **2026-09-27:** owner ruling — one Critic by default. The three specialist passes and the
  claims pass became four lenses of one pass with one report; re-review after a fix is targeted
  at the lenses the fix touched; a high-tier change adds one independent pass over persistence,
  commit atomicity, concurrency, security and data-loss paths. The taxonomies, the attestation
  duty, the severity floor and the convergence labels did not change. Reason: the separate
  passes cost more than routine changes need, and under the standing procedural context break
  one session ran every pass, so their independence was weak. **Not measured:** how many unique
  findings the separate passes contributed; one pass is not claimed to be equally effective.
  Trade-off accepted: fewer independent chances to catch a missed bug.

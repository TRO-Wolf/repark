# unit-runbook.md — the per-tier checklist a LIGHT/STANDARD unit reads first

A pointer-only running order for one SEPMO unit. **Every line names and links the home of the rule
it carries; the home is authoritative.** It exists so a LIGHT or STANDARD unit does not have to
read the whole spine to start — HIGH units run the full control plane and read
[SKILL.md](SKILL.md) directly. On any conflict the home wins, and above every home is
[AGENTS.md](../../../AGENTS.md) `## Precedence`.

## 1. Pickup — the unit's first act

- Run the pickup ritual: [briefs/next-sequence.md](../../../briefs/next-sequence.md) standing rule 7
  (the single home), executed by
  [compact-context-docs](../compact-context-docs/SKILL.md) "Pickup ritual (scoped mode)".
- Confirm the prior PR merged and the base carries its departure edit, then `make ledger-archive`
  and `make check-docs-compaction` — the [Unit pickup / departure](binding-manifest.md) row wires both.

## 2. Pick the tier

- The [`review_profile`](binding-manifest.md) row is the tier table (**LIGHT / STANDARD / HIGH**),
  chosen by CCC's risk-tier auto-detect (the riskiest touched path).
- The [`light_thresholds`](binding-manifest.md) row sets the prose-only LIGHT class and the code
  line/file caps.

## 3. Build (Actor)

- Exit green: **R2** in [SKILL.md](SKILL.md) and [references/04-actor.md](references/04-actor.md);
  the [`green_commands`](binding-manifest.md) row names the gates (`make ci`, `make verify`).
- Pin **every** clause the unit touches: [docs/testing.md](../../../docs/testing.md) "Pinning a
  charter clause" — the clause table and the `pins: <unit>/C-NNN` citation are rules A and B of
  [`check_ledger_grammar.py`](../../../scripts/check_ledger_grammar.py) (`make check-ledger-grammar`).
- Log a Self Logic Review before each state-changing step:
  [references/03-self-logic-review.md](references/03-self-logic-review.md) (D3).

## 4. The Critic stage

- **LIGHT** — the spine's single in-line AC cycle; the [`review_profile`](binding-manifest.md) row
  is its home (its attestation, the **R7** audit the Orchestrator may self-run,
  [references/02-orchestrator.md](references/02-orchestrator.md)).
- **STANDARD** — one Critic pass under the two hard obligations named in the
  [`review_profile`](binding-manifest.md) row (fresh execution via
  [`s0_fresh_execution`](binding-manifest.md)); it walks CCC's taxonomies as one checklist —
  [critic-critic-critic](../critic-critic-critic/SKILL.md).
- **HIGH** runs the full CCC engine per the [`critic_engine`](binding-manifest.md) row.

## 5. Ledger, pins, attestation

- The clause table (rule A), the `pins:` citations (rule B) and the `COVERAGE_ATTESTATION` (rule
  C — the Critic's artifact, due once no clause is `OPEN`) are all shaped by
  [`check_ledger_grammar.py`](../../../scripts/check_ledger_grammar.py); the meanings live in
  [references/05-critic.md](references/05-critic.md). The ledger bins are
  [task/ledgers/map.md](../../../task/ledgers/map.md).

## 6. Departure — the unit's last act

- The departure edit and the `move` to `completed/` are rule 7's second half
  ([briefs/next-sequence.md](../../../briefs/next-sequence.md)); the executor is
  [compact-context-docs](../compact-context-docs/SKILL.md) and `ledger_lifecycle.py move`. Delivery
  runs on **R9** ([references/07-delivery.md](references/07-delivery.md)).

## Pointers

- Up: [map.md](map.md) · Spine: [SKILL.md](SKILL.md) · Bindings: [binding-manifest.md](binding-manifest.md).
- Ceiling: this file is held at 5,000 B by `CEILINGS` in
  [`check_docs_compaction.py`](../../../scripts/check_docs_compaction.py) so it cannot regrow into a
  second spine.

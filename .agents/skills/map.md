# map — .agents/skills/

## Purpose

Agent-facing **runbook skills**: step-by-step procedures for recurring operations, written for
any tool's agent. Each skill is a directory holding a `SKILL.md` with YAML frontmatter (`name` +
a `description` that says when to reach for it **and when not to**), the same shape as
[sepmo/SKILL.md](sepmo/SKILL.md), so a skill is discoverable and invocable
rather than a file an agent has to already know to open. A skill records a proven *sequence*; it
defines no **engineering** policy and carries no authoritative project fact — every rule it leans on is a pointer into the spine
([AGENTS.md](../../AGENTS.md) + [STATUS.md](../../STATUS.md) + the doc each step cites), and on
any conflict the spine wins. This keeps the `.agents/` zero-authoritative-facts contract intact:
deleting a skill loses a convenience, never a project truth. **Two skills define *process* policy:**
[sepmo/](sepmo/map.md), the SEPMO control plane (moved here from a top-level `skills/` tree on
2026-08-25 so every tool's agent loads it from the one skill home), and
[critic-critic-critic/](critic-critic-critic/map.md), the Critic engine its manifest binds
(imported the same day from the owner's tool-local skill set, spawn mechanics left to the tool
adapters). Both cede every engineering decision to the spine — [AGENTS.md](../../AGENTS.md)
"Process governance" is the rule of record.

**Claude discovers these through a symlink.** `.claude/skills` points at this directory (git mode
`120000`), because Claude Code loads skills only from `.claude/skills/`. The skills keep their
single home here; adding a directory below makes it invocable with no change on the Claude side.
See [../../.claude/map.md](../../.claude/map.md).

## Contents

- [sepmo/](sepmo/map.md) — the **SEPMO** governance-and-orchestration control plane (state
  machine + scope-audit gate + adversarial Actor–Critic) and its binding manifest, the one file
  mapping SEPMO's abstract roles to this repo. Invoked deliberately for non-trivial work — its
  description says when not to — never auto-run on a conversational turn.
- [critic-critic-critic/](critic-critic-critic/map.md) — **CCC**, the bound Critic engine: three
  specialised adversarial Critics (quality + crates contracts → security/safety → pure logic) plus
  a claims Critic, each with a context break, a coverage attestation and a findings ledger;
  review-only by default; `CCC-CONVERGED` is never Delivery. Runs as SEPMO's Critic stage on
  STANDARD-and-above units, or alone on a diff.
- [engineering-method/](engineering-method/map.md) — the portable, agent-agnostic working method
  for implementation and review sessions: risk-first design, the reason-plan-verify workflow,
  naming, the Rust/Python defaults, the debugging protocol, and the done gate. Generalized from
  the former per-model-tier manuals (`docs/skills/`, removed 2026-08-24); tier postures moved to
  the tool adapters. Rule of record stays [AGENTS.md](../../AGENTS.md).
- [publish-pypi/](publish-pypi/map.md) — cut a versioned release: the release-PR shape, squash
  tree verification, the annotated tag, the `release.yml` pipeline with its owner approval gate,
  registry verification. Owner merges and approvals stay owner actions.
- [compact-context-docs/](compact-context-docs/map.md) — the truth-up ritual at both ends of a
  unit: `make ledger-archive` first (mechanical, DL-1), reconcile STATUS.md, sweep restatements
  and stale lifecycle claims, keep `map.md` lockstep, `move` the unit's ledger to `completed/`
  and closed campaigns to `docs/history/`, validate with `make ci` — plus the scoped **pickup**
  mode that opens a unit against the just-merged delta only. Executes
  [AGENTS.md](../../AGENTS.md) "Markdown document lifecycle".
- [check-disk-headroom/](check-disk-headroom/map.md) — is there room to do this? Measured
  consumers (`target/debug` dominates), how to budget for the operation rather than the repo at
  rest, and a reclaim order that says what **not** to delete as clearly as what to.
- [code-quality/](code-quality/map.md) — the portable Python conventions (v2.0: Ruff baseline,
  types + named steps, Pydantic not dataclasses, no nested `def`, lazy dataframes,
  eventual-reader comments) plus the ratchet for arming a rule. The host rule of record is
  [AGENTS.md](../../AGENTS.md) "Python"; this skill is not a second contract.
- [rust-code-quality/](rust-code-quality/map.md) — the Rust review procedure for what the armed
  gates cannot catch: escape hatches, Spark-visible behavior, ANSI dual-door coverage, float
  semantics, hot-path allocation, the error contract. Severity ordered for a query engine
  (silently wrong results outrank crashes).
- [audit-repark-parity/](audit-repark-parity/map.md) — measure a surface against the pinned live
  PySpark oracle and classify every divergence (product bug / disposed / stale) before repairing.
  Mandatory first step of a parity-live nightly-red triage; required sweep in any PR that flips a
  Spark-visible default or error contract; periodic pre-release pass.

## I want to...

| ...do this | go to |
|---|---|
| Operate under / understand SEPMO | [sepmo/map.md](sepmo/map.md) → [sepmo/SKILL.md](sepmo/SKILL.md) |
| See SEPMO's bindings to this repo | [sepmo/binding-manifest.md](sepmo/binding-manifest.md) |
| Run the bound Critic engine on a diff | [critic-critic-critic/SKILL.md](critic-critic-critic/SKILL.md) |
| Read the working method before writing or reviewing code | [engineering-method/SKILL.md](engineering-method/SKILL.md) |
| Release a new version to PyPI | [publish-pypi/SKILL.md](publish-pypi/SKILL.md) |
| True up the docs after work lands, or run the pickup ritual before it | [compact-context-docs/SKILL.md](compact-context-docs/SKILL.md) |
| Find out whether there is disk room for a big build | [check-disk-headroom/SKILL.md](check-disk-headroom/SKILL.md) |
| Write or review Python under the conventions | [code-quality/SKILL.md](code-quality/SKILL.md) |
| Review a Rust PR or commit | [rust-code-quality/SKILL.md](rust-code-quality/SKILL.md) |
| Triage a parity-live red, or flip a Spark-visible default | [audit-repark-parity/SKILL.md](audit-repark-parity/SKILL.md) |
| Add a new skill | a `<verb-noun>/` directory here with `SKILL.md` (frontmatter + pointers, no engineering policy) and its own `map.md`, plus a Contents row |
| Read the authoritative contract | [../../AGENTS.md](../../AGENTS.md) |

## Pointers

- Up: [../map.md](../map.md)
- Authoritative spine: [../../AGENTS.md](../../AGENTS.md), [../../STATUS.md](../../STATUS.md),
  [../../docs/release.md](../../docs/release.md).

## Debug

| Symptom | First check |
|---|---|
| A skill states a project rule | Bug — move the rule to the spine, leave a pointer (`.agents/` contract) |
| A skill is a bare `.md`, not a directory | Pre-2026-08-21 shape — convert it to `<name>/SKILL.md` with frontmatter + a `map.md` |
| A skill will not load in a Claude session | `ls -l .claude/skills` resolves here, and the skill's `SKILL.md` carries `name` + `description` frontmatter |
| A skill step no longer matches reality | Fix the skill in the same PR as the change that falsified it |

**`trait-wrapping` was never made a skill, and no longer needs to be** (recorded 2026-08-10 in the
former `docs/skills/map.md`; note moved here 2026-08-24 when that directory was generalized into
[engineering-method/](engineering-method/map.md)). The manual it would have carried — the
silent-default gap when wrapping a trait: enumerate and forward every method, defaults included,
audited from both sides — is a **rule in force**: the audit duty is in
[../../AGENTS.md](../../AGENTS.md) "Version-pin contract" (re-enumerate the wrapped catalog's trait
surface at every fork repin), and the one open instance is a named component limitation in
[../../crates/repark-iceberg/map.md](../../crates/repark-iceberg/map.md) "Known limitations".

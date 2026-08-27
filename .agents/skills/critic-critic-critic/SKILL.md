---
name: critic-critic-critic
description: >
  Run a three-phase Critic–Critic–Critic review loop with no Actor build phase:
  Critic-1 attacks bugs and code quality (including crates/library contracts:
  thiserror, no unwrap, locks, recursion, casts, tests, async), Critic-2 attacks
  safety and security, then Critic-3 attacks pure logic bugs (wrong results,
  incomplete matches, silent data loss). Derived from the SEPMO Actor–Critic
  doctrine (context-break Critics, coverage attestation, risk tiers,
  mutation-proof pins) but review-only by default; it is the Critic engine this
  repo binds in its SEPMO manifest. Use when the user runs /critic-critic-critic, asks
  for "critic critic critic", "CCC loop", triple-critic review, three-pass
  adversarial critique, or quality then security then logic review of a
  diff/PR/slice without building.
---

# Critic–Critic–Critic (CCC)

**Taxonomy home** for adversarial review in this repository. Binders — the SEPMO Critic stage
through the manifest's `critic_engine` row ([../sepmo/binding-manifest.md](../sepmo/binding-manifest.md)),
or any tool's own review harness — decide *when* and *how many times* to run; this skill owns
risk tiers, severity, the finding schema, the crates contract, and the four critic taxonomies.
The repository-specific binding and effort profile live only in the manifest.
Binders **load this file and the role references before starting** — they do not restate those
lists. How a Critic is *spawned* is a tool mechanic and lives in that tool's adapter
([../../../CLAUDE.md](../../../CLAUDE.md) for Claude), never here.

**No Actor build phase** by default. **Do not merge roles** into one vague “look at the PR” pass.

**Findings-only vs fix-then-re-attack:** `review-only` (and the SEPMO Critic stage after the Actor) spawns required Critics **in parallel**, withholds peer reports until merge. `review-and-fix`
stays **sequential** because the tree moves — do not start the next Critic until the current
one is CLEAN (or residuals escalate). Report merge order is always Critic-1, then 2, then 3,
then 4 when enabled.

| Phase | Role | Purpose |
|---|---|---|
| 1 | **Critic-1 (Quality / Bugs)** | Attack code quality, library/crates contracts, maintainability, test adequacy, general bugs |
| 2 | **Critic-2 (Safety / Security)** | Attack security and safety surfaces |
| 3 | **Critic-3 (Logic Bugs)** | Attack pure logic correctness — wrong results, inverted predicates, silent data loss, incomplete matches |
| 4 | **Critic-4 (Claims / Record)** | Attack every claim the change makes about itself (ledgers, maps, STATUS, docstrings, reports, author/trailer) against the TREE, by re-execution. Default **on** for ledger-bearing units; opt-out only by explicit `claims_critic=false`. Joins the triad as a quad under the same exclusion rules |

Reference role prompts:

- [references/01-critic-quality-bugs.md](references/01-critic-quality-bugs.md)
- [references/02-critic-security-safety.md](references/02-critic-security-safety.md)
- [references/03-critic-logic-bugs.md](references/03-critic-logic-bugs.md)
- [references/04-critic-claims-record.md](references/04-critic-claims-record.md) *(default-on
  for ledger-bearing units; added 2026-08-11 — mandated-but-untouched files, quantifier
  overclaims, stale records, invented deviation rationales, non-replaying transcripts;
  2026-08-12 CL-IDENTITY: author-email at name resolution)*

Doctrines (from the SEPMO Actor–Critic control plane, without the Actor):

- **Context break** — each Critic opens with: *“Context break executed; attacking artifacts, not memory.”* Findings cite `file:line`, failing inputs, or test traces — not build-session memory. Prefer a **fresh subagent** (or sequential pass that starts from the **diff + nearest scoped `AGENTS.md`**).
- **Author confidence is not evidence** — refute the change; do not bless it. Clean categories need attestation of what was attacked, not “looks fine.”
- **Critics do not build** — they only attack and attest. Remediation (if requested) is a separate fix pass that does **not** declare CCC convergence.
- **Coverage over body count** — clean category = **null report**: “attacked X, Y, Z — no break found.” Bare “pass” is invalid.
- **Findings are concrete** — input/state → wrong outcome, or named missing test, with severity and `file:line`. “Looks risky” is not a finding.
- **Resolve or rebut with evidence** — fix, or rebut with test / traced path / cited invariant. “Unlikely” is not a rebuttal.
- **Adversarial review supplements gates** — project `pre-commit` / `pre-pr` / CI must still pass; never weaken a gate.
- **Green tests are not convergence** — label `CCC-CONVERGED` vs `TEST-GATED` honestly.
- **Pins must go red on revert** — hollow substring / wrong-layer monkeypatch pins are Critic-1 findings.
- **Three Critics stay specialized** — Critic-1 owns quality + crates contracts + test adequacy; Critic-2 owns security/safety; Critic-3 owns **deep pure logic**. Cross-domain glare → short `HANDOFF-*` only, not a full steal of another taxonomy.

---

## Parameters

Parse from the user message (ask only if ambiguous):

| Param | Default | Notes |
|---|---|---|
| **`task`** | (required) | What slice, PR, branch, or bug surface to attack |
| **`repo`** | Current workspace | Absolute or relative project root (primary tree) |
| **`dependency_repos`** | auto / `[]` | Extra trees Critics must attack when the slice pins/depends on them. Auto: load-bearing git-pinned siblings on disk. Explicit `[]` = primary only (**disclose**) |
| **`mode`** | `review-only` | `review-only` = Critics only (default). `review-and-fix` = after Critic findings, a **Fixer** pass remediates, then Critics re-attest (still no blind “Actor build a new feature” phase unless user expands `task`) |
| **`max_cycles`** | `2` | Remediation cycles in `review-and-fix` (fix → re-attack). Cap prevents infinite loops |
| **`severity_floor`** | `S1` | Open findings at/above this severity block convergence (`S0`…`S3`) |
| **`risk_tier`** | auto | `exempt` \| `mechanical` \| `standard` \| `high` — from **riskiest file touched** |
| **`claims_critic`** | see note | Default **true** when the unit writes a COMPLETE, unit ledger, map.md claim, STATUS-class record, or §6 registry row. Otherwise false. Opt-out only by explicit `claims_critic=false`. When true, Critic-4 joins every findings pass (quad). |
| **`verify`** | project default | Prefer repo Makefile/CI contracts. Never invent a matrix that contradicts them |

---

## Risk tiers

| Tier | When | CCC intensity |
|---|---|---|
| **Exempt** | Docs/comments/formatting only, **no** runtime surface | Skip Critics; optional light self-check |
| **Mechanical** | Pure renames, moves, test-only with no behavior change | Critic-1 only (crates contracts + test adequacy). Critic-2/3 N/A unless paths touch auth, parsers, unsafe, or logic-heavy code |
| **Standard** (default) | Any behavior-affecting change | Full Critic-1 + Critic-2 + Critic-3. Critic-1 runs **test-coverage skeptic**. Critic-3 runs **logic attack taxonomy** |
| **High** | Locking, consensus, persistence, authn/authz/crypto, on-disk/on-wire formats, public API, multi-step publish/commit/OR REPLACE, catalog pointer swaps, or nearest `AGENTS.md` high-risk | Full three Critics; **no soft N/A** on concurrency, partial-failure, compatibility when touched. Prefer independent subagents per Critic. Critic-2 **must** pressure atomicity/mid-commit. Critic-3 **must** pressure edge values and multi-writer ordering on logic paths |

**Auto-detect:** walk changed paths; read nearest `AGENTS.md`; behavior-affecting → at least `standard`. Multi-step publish/commit → **high**.

---

## Severity scale (S0–S3)

| Level | Name | Meaning |
|---|---|---|
| **S0** | Critical | Crash, severe wrong data, or secret exposure on realistic input |
| **S1** | Major | Wrong data, hard panic on common paths, material security/safety/logic hole |
| **S2** | Minor | Material risk under load, hostility, incomplete feature |
| **S3** | Advisory | Tech debt, edge case, latent issue, non-blocking hygiene |

---

## Absolute rules

1. **Distinct Critic phases** — Critic-1, Critic-2, Critic-3 (when tier requires), plus Critic-4 when `claims_critic` is on. Never merge into one pass.
2. **Context break** before each Critic — attack **diff + artifacts**, not session memory. Load **nearest scoped `AGENTS.md`** as attack surface.
3. **Findings-only is parallel; fix-then-re-attack is sequential.** `review-only`: spawn required Critics together, withhold peer reports, merge after. `review-and-fix`: do not start the next Critic until the current one is CLEAN (tree moved). Merge/report order stays quality → security → logic → claims.
4. **Findings require evidence** — path + region; *Potential* when unproven; never invent paths.
5. **Rebuttals require evidence** — test / traced path / cited invariant.
6. **No secrets in reports** — redact values; pattern + location only.
7. **Repo contracts win** — root + nearest `AGENTS.md` / `CLAUDE.md` / project skills. CCC never overrides a project hard gate.
8. **Never weaken the gate** — no skip/loosen of checks to force green.
9. **Every behavior change needs a mutation-proof test** (Standard/High) — Critic-1 test-coverage skeptic enforces this.
10. **Green verify alone is never convergence** — see [Convergence labels](#convergence-labels-hard).
11. **Load-bearing dependency trees are in Critic scope** when clauses depend on them.
12. **Critic-1 crates contract** — for any touch under `crates/` (or equivalent library roots), apply the [Crates / library attack contract](#crates--library-attack-contract-critic-1) in Critic-1 (full detail in the Critic-1 reference).
13. **Spawn contract** — apply the [Spawn contract](#spawn-contract) on every child: the invariants are here, the tool-specific mapping is in the tool's adapter.

---

## Spawn contract

Tool-neutral invariants; every binder applies them to each child it starts. The mapping onto a
tool's agent types, capability flags and isolation options is written **once, in that tool's
adapter** — never here, never in a child prompt from memory.

| Role | Needs | Must not | Context |
|---|---|---|---|
| Critic-1/2/3/4 and any `git` / verify probe | read the tree, run shell (`git`, the verify commands) | edit files | **fresh** — never resumed from a peer Critic or the Actor (a resumed context leaks the peer narrative the context break exists to exclude) |
| Setup that needs `git status` / `git diff` | read + shell | edit | n/a |
| Fixer (`review-and-fix`) | read + shell + edit | declare convergence | same-role continuation only |

Hard lines:

- **A Critic that must run `git` or the verify gate needs a shell.** Never pair a read-only
  capability with a prompt that orders one.
- **Critics attack a scratch copy, never the live working tree** — a clone or a checkout the
  binder makes for them; the live tree's uncommitted state, stash and reflog are the Actor's.
  After a fan-out the binder checks the live tree is untouched.
- **Role instructions travel in the child prompt.** A persona or role file is pasted or
  pointed at; no spawn mechanism is assumed to take one as a parameter.
- Worktree and scratch-location mechanics are the adapter's; the identity every commit must
  carry is the repository's (`git config` at the repo root), checked by Critic-4 at `%ae`.

---

## Crates / library attack contract (Critic-1)

Applies to all paths under `crates/` (and the same rules by analogy for other pure-library roots the repo marks as library code). Critic-1 **must** attack these categories when the diff touches library code — not soft-skip as “style.”

| Area | Attack rules (summary) |
|---|---|
| **Library design** | Treat as reusable library code; prefer `thiserror` for library-facing errors; no `unwrap`/`expect`/panic-driven control flow outside tests |
| **Error types** | Public APIs return typed error enums — never `Result<_, String>`; no `Box<dyn Error>` (+Send/Sync) on public traits/methods; implement `Error::source()` when storing inner errors; helpers should return the real error type, not String-then-`map_err` |
| **Concurrency** | Document multi-lock order; never reverse lock orders; never hold tokio `RwLock`/`Mutex` write guard across `.await` unless unavoidable and bounded; prefer `compare_exchange` for concurrent counters; document multi-field atomic reset tradeoffs; `std::sync::Mutex` in async only for brief non-await sections |
| **Recursion** | Depth limit or iterative `Vec` stack for tree/graph walks; malicious input must not stack-overflow |
| **Type casting** | No truncating/overflowing `as`; use `try_into` or domain-clamped casts with justification; treat every `as` as a potential bug |
| **Testing** | Unit tests co-located; integration under `tests/`; regressions for fixes; every test has an assert; prefer `.expect("context")` over bare `.unwrap()` in tests |
| **Async / performance** | Async paths non-blocking; CPU-heavy work via `spawn_blocking` when appropriate |

Full checklist and finding prefixes: [references/01-critic-quality-bugs.md](references/01-critic-quality-bugs.md).

**Boundary:** production panics as a *safety class*, `unsafe`, secrets, injection → Critic-2 (`HANDOFF-SEC` / `HANDOFF-SAF` if found during Critic-1). Deep multi-step logic wrongness (predicate inversion, silent wrong rows) → Critic-3 owns the deep dive; Critic-1 still files obvious logic if found, or hands off with `HANDOFF-L`.

---

## Convergence labels (hard)

| Label | Meaning | Allowed when |
|---|---|---|
| **`CCC-CONVERGED`** | Required Critic phases ran CLEAN (or residual below floor ACCEPTED_FLAGGED); full verify green; coverage skeptic + logic attestation satisfied when applicable | Critic artifacts exist for required phases |
| **`TEST-GATED`** | Verify/tests green but Critics incomplete or skipped | Ceremony deferred |
| **`HALTED`** | Open findings ≥ floor after `max_cycles`, or user stop | Residual ≥ floor remains |

Never rewrite `TEST-GATED` as `CCC-CONVERGED`.

---

## Workflow

### 0. Setup (orchestrator)

1. Resolve parameters. Default `mode=review-only`.
2. **Discover contracts:** root + nearest `AGENTS.md`, Makefile/CI verify, project skills scan.
3. **Resolve `dependency_repos`** for load-bearing pins.
4. Baseline: branch, `git status`, **diff under attack** per tree.
5. Set **risk tier**.
6. Write **slice charter** (scope, success conditions, constraints, enumeration partitions, risk tier, which Critics run).

If ambiguous scope → **stop and ask**. If `exempt` → document and stop.

---

### Phase 1 — Critic-1 (Quality / Bugs + crates contracts)

**Skip if `risk_tier=exempt`. Mechanical: crates contracts + correctness/test focus; N/A others with justification.**

1. Context break: *“Context break executed; attacking artifacts, not memory.”*
2. Load [references/01-critic-quality-bugs.md](references/01-critic-quality-bugs.md).
3. Prefer a **fresh `explore` subagent** (shell allowed, no edits — see Spawn contract). Inputs: charter, current diff(s), tests, verify, nearest `AGENTS.md` — **not** author excuses first.
4. Work **Quality + Crates attack taxonomies**; attestation + findings (`Q-` / `CRATE-`).
5. **Test-coverage skeptic** (Standard/High behavior changes): mutation-proof dual probe.
6. **Enumeration span** when charter names a finite partition.
7. Null reports for clean categories.
8. Verdict: `CLEAN` | `NEEDS_REMEDIATION`.
9. If remediation + `review-and-fix` + cycles remain: Fixer remediates Critic-1 findings only; re-verify; Critic-1 re-attacks. In `review-and-fix` only: **do not start Critic-2 until Critic-1 CLEAN** (or escalate residuals to user at max_cycles). In `review-only`: Critic-2 runs in parallel under the spawn contract; do not wait.

---

### Phase 2 — Critic-2 (Safety / Security)

**Skip if exempt, or mechanical with no security/safety surface (document N/A).**

1. Context break.
2. Load [references/02-critic-security-safety.md](references/02-critic-security-safety.md).
3. Fresh read-only subagent independent of Critic-1 narrative. Current diff post quality fixes.
4. Security/Safety taxonomy; `SEC-` / `SAF-` findings; High-tier atomicity pressure on commit/publish.
5. Verdict + remediation loop if needed. Targeted Critic-1 re-spot if fixes touch quality/crates contracts.

---

### Phase 3 — Critic-3 (Logic Bugs)

**Skip if exempt. Mechanical: only if diff is logic-bearing; else N/A.**

1. Context break.
2. Load [references/03-critic-logic-bugs.md](references/03-critic-logic-bugs.md).
3. Fresh read-only subagent independent of Critic-1/2 narratives. **Current** diff (post prior remediations).
4. Work **Logic attack taxonomy** exhaustively — concrete edge values, silent wrong results, incomplete matches, racey wrong outcomes (logic lens, not panic/safety).
5. Findings `L-` prefix. Do not re-litigate crates style or secret handling unless they *cause* wrong results (then file logic finding with evidence of wrong outcome).
6. Verdict + remediation loop if needed. If logic fix re-breaks crates contracts or security, hand off targeted re-spot of Critic-1/2.

---

### Convergence (all required Critics)

Work is **`CCC-CONVERGED`** only when:

1. Risk tier applied; required Critic phases have **artifacts** (findings + attestation)
2. No open finding ≥ `severity_floor` on required phases
3. Every finding REMEDIATED / WITHDRAWN (evidence) / ACCEPTED_FLAGGED (policy)
4. **Verify green** with full project gate when shipping
5. Standard/High: mutation-proof tests (Critic-1 skeptic)
6. Enumeration partitions pin-count satisfied when applicable
7. `dependency_repos` attacked when load-bearing
8. Critic-3 logic attestation complete when required (Standard/High behavior)
9. Critic-4 claims attestation complete when `claims_critic` is on (ledger-bearing default)

If Critics skipped → **`TEST-GATED`**. If max_cycles + open ≥ floor → **`HALTED`**.

When `claims_critic` is on, after Critic-3 (or in parallel with it on `review-only`): load
[references/04-critic-claims-record.md](references/04-critic-claims-record.md), spawn an
`explore` Critic-4, prefix `CL-`. Identity claims require `%ae` across the branch (CL-IDENTITY),
not the author name.

---

## Finding schema

```yaml
FINDING:
  id: Q-001 | CRATE-001 | SEC-001 | SAF-001 | L-001 | CL-001
  severity: S0 | S1 | S2 | S3
  category: <taxonomy category>
  claim: "<input/state → wrong outcome | named contract violation>"
  evidence: "<file:line | failing input | test/trace>"
  disposition: OPEN | REMEDIATED | ACCEPTED_FLAGGED | WITHDRAWN | SUSTAINED
  rebuttal_if_withdrawn: "<test | traced path | cited invariant>"
```

---

## Final user report (required)

```markdown
# Critic–Critic–Critic report

**Task:** …
**Repo / branch / rev:** …
**Mode:** review-only | review-and-fix
**Risk tier:** exempt | mechanical | standard | high
**Nearest AGENTS.md:** <paths>
**Cycles used:** n / max
**Verify:** command + result
**Convergence label:** CCC-CONVERGED | TEST-GATED | HALTED (reason)
**Dependency repos reviewed:** <paths or none>
**Enumeration partitions:** <list + pin count / size, or n/a>

## Critic-1 (Quality / Bugs + crates)
- Verdict: CLEAN | NEEDS_REMEDIATION | SKIPPED
- Findings: count by severity
- Top findings: …
- Crates contract: attacked | n/a (paths)
- Coverage attestation: complete yes/no
- Test-coverage skeptic: …
- Mutation-proof pins: ok | findings
- Null reports: …

## Critic-2 (Safety / Security)
- Verdict: …
- Findings: …
- Atomicity/partial-failure (if applicable): attacked | n/a
- Null reports: …

## Critic-3 (Logic Bugs)
- Verdict: …
- Findings: …
- Edge-value / silent-wrong pressure: attacked | n/a
- Null reports: …

## Critic-4 (Claims / Record — when claims_critic)
- Verdict: …
- Findings: …
- CL-IDENTITY (`%ae` across branch): attacked | n/a
- Null reports: …

## High-tier role verdicts (high only; one line each)
- Quality/crates: …
- Security/safety: …
- Logic: …

## Residual / accepted-flagged
…

## Files in scope (final)
…
```

---

## Subagent guidance

| Phase | Role shape | Notes |
|---|---|---|
| Critic-1 | read-attack (shell, no edits) | Attack only; crates contract when `crates/` touched |
| Critic-2 | read-attack | Independent of Critic-1; withhold peer reports |
| Critic-3 | read-attack | Independent of Critic-1/2; pure logic |
| Critic-4 (when on) | read-attack | Independent; attacks the paper including CL-IDENTITY |
| Fixer (`review-and-fix`) | build (shell + edits) | Fix filed findings only; re-verify |

Which agent type each shape maps to is the tool adapter's table. If spawning is unavailable or
not opted into (the SEPMO manifest's `context_break_mechanics` row decides): sequential
hat-switches with explicit, declared context breaks — and the report names the weaker
independence.

**High tier:** prefer real subagents for each Critic. Do not invent a six-agent swarm by default.

---

## As the SEPMO Critic engine

When the SEPMO manifest binds this skill as `critic_engine`, the spine's four constraints for an
external engine ([../sepmo/references/05-critic.md](../sepmo/references/05-critic.md) "External
critic engines") apply and this section is how they are met:

1. **`CCC-CONVERGED` is never Delivery.** The final report maps into the spine's instruments —
   each Critic's coverage attestation becomes the unit ledger's `COVERAGE_ATTESTATION` rows and
   each `FINDING:` becomes a ledger finding — and `PR_READINESS_AUDIT` then runs exactly as
   always (R7).
2. **LIGHT units never select this engine**; the proportionality rubric decides the path first.
3. **Taxonomy mapping onto the spine's AT-1..AT-10** is recorded in the manifest row; a
   category this skill does not attack is a justified `N/A` there, never silence.
4. **Tunables bind in the manifest row** (`mode`, `max_cycles`, `severity_floor`,
   `claims_critic`, `risk_tier` source, scratch location), never in this file.

---

## Anti-patterns

- Merging Critic-1/2/3 into one skim
- Skipping crates contract on `crates/` diffs (“style only”)
- Critic-3 re-running full security taxonomy
- Declaring convergence from green verify alone (`TEST-GATED` mislabeled)
- Hollow pins / unpinned discarded-failure paths
- Weakening gates to force green
- Bare “pass” without null report
- “Unlikely” as rebuttal
- Critic-only-primary-tree when a pin implements the clause
- Shipping open S0/S1 after max_cycles without user decision

---

## Quick start examples

```text
/critic-critic-critic review-only the diff on feat/a1-partitioned-append
/critic-critic-critic task="PR #42 MERGE OCC" risk_tier=high
/critic-critic-critic review-and-fix the crates/repark-write append path max_cycles=2
/critic-critic-critic task="crates/ under lock-order change" dependency_repos=[/path/to/iceberg-rust]
```

---

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

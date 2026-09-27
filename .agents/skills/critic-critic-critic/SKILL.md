---
name: critic-critic-critic
description: >
  Run the Critic review loop with no Actor build phase: one Critic, one pass,
  one report, working four lenses — quality and bugs (including crates/library
  contracts: thiserror, no unwrap, locks, recursion, casts, tests, async),
  safety and security, pure logic bugs (wrong results, incomplete matches,
  silent data loss), and the change's claims about itself on ledger-bearing
  units. High-risk changes add one independent second pass. Derived from the
  SEPMO Actor–Critic doctrine (context-break Critics, coverage attestation,
  risk tiers, mutation-proof pins) but review-only by default; it is the Critic
  engine this repo binds in its SEPMO manifest. Use when the user runs
  /critic-critic-critic, asks for "critic critic critic", "CCC loop",
  adversarial critique, or a quality, security and logic review of a
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

**No Actor build phase** by default. **One Critic, one pass, one report** (owner ruling
2026-09-27) — and not one skim: the pass works every required lens's taxonomy in full and
attests each lens separately, in the order 1 → 2 → 3 → 4. Re-review after a fix is targeted;
high tier adds **one independent pass**.

| Lens | Name | Purpose |
|---|---|---|
| 1 | **Critic-1 (Quality / Bugs)** | Attack code quality, library/crates contracts, maintainability, test adequacy, general bugs |
| 2 | **Critic-2 (Safety / Security)** | Attack security and safety surfaces |
| 3 | **Critic-3 (Logic Bugs)** | Attack pure logic correctness — wrong results, inverted predicates, silent data loss, incomplete matches |
| 4 | **Critic-4 (Claims / Record)** | Attack every claim the change makes about itself (ledgers, maps, STATUS, docstrings, reports, author/trailer) against the TREE, by re-execution. Default **on** for ledger-bearing units; opt-out only by explicit `claims_critic=false` |

Critic-1 … Critic-4 name the four **lenses** of the one Critic. The names, the finding prefixes
and the role references are unchanged:

- [references/01-critic-quality-bugs.md](references/01-critic-quality-bugs.md)
- [references/02-critic-security-safety.md](references/02-critic-security-safety.md)
- [references/03-critic-logic-bugs.md](references/03-critic-logic-bugs.md)
- [references/04-critic-claims-record.md](references/04-critic-claims-record.md) *(default-on
  for ledger-bearing units; added 2026-08-11 — mandated-but-untouched files, quantifier
  overclaims, stale records, invented deviation rationales, non-replaying transcripts;
  2026-08-12 CL-IDENTITY: author-email at name resolution)*

Where a reference speaks of an order between Critics, a handoff or a subagent per Critic, this
file wins: the four are lenses of one pass, and a handoff is a finding filed under its lens.

Doctrines (from the SEPMO Actor–Critic control plane, without the Actor):

- **Context break** — the Critic opens with: *“Context break executed; attacking artifacts, not memory.”* Findings cite `file:line`, failing inputs, or test traces — not build-session memory. Prefer a **fresh subagent** (or a pass that starts from the **diff + nearest scoped `AGENTS.md`**).
- **Author confidence is not evidence** — refute the change; do not bless it. Clean categories need attestation of what was attacked, not “looks fine.”
- **Critics do not build** — they only attack and attest. Remediation (if requested) is a separate fix pass that does **not** declare CCC convergence.
- **Coverage over body count** — clean category = **null report**: “attacked X, Y, Z — no break found.” Bare “pass” is invalid.
- **Findings are concrete** — input/state → wrong outcome, or named missing test, with severity and `file:line`. “Looks risky” is not a finding.
- **Resolve or rebut with evidence** — fix, or rebut with test / traced path / cited invariant. “Unlikely” is not a rebuttal.
- **Adversarial review supplements gates** — project `pre-commit` / `pre-pr` / CI must still pass; never weaken a gate.
- **Green tests are not convergence** — label `CCC-CONVERGED` vs `TEST-GATED` honestly.
- **Pins must go red on revert** — hollow substring / wrong-layer monkeypatch pins are lens 1 findings.
- **Lenses stay specialized** — lens 1 owns quality + crates contracts + test adequacy; lens 2 owns security/safety; lens 3 owns **deep pure logic**; lens 4 owns the record. A finding is filed once, under the lens whose taxonomy it breaks.
- **Pass count is an effort dial, not the bar** — taxonomies, attestation, floor and evidence rules did not change. What the separate passes added in unique findings was never measured ([history.md](history.md)).

---

## Procedure

Run the loop; do not build. Binders load this file and the role reference for each lens
before starting, and never restate them.

1. **Resolve parameters** (Parameters below): task, repo, dependency_repos, mode
   (`review-only` default), max_cycles (2), severity_floor (S1), risk_tier, claims_critic,
   verify. Stop on ambiguous scope.
2. **Set the risk tier** from the riskiest file touched (exempt → stop with a note;
   mechanical → lens 1 only; standard → one pass, every lens; high → that pass plus one
   independent pass). A behavior-affecting change is at least standard.
3. **Write the slice charter**: scope, success conditions, constraints, enumeration
   partitions, tier, which lenses run, whether the independent pass runs. Discover contracts
   (root + nearest AGENTS.md, the verify gate) and load-bearing dependency_repos first.
4. **Run the Critic**: it opens with "Context break executed; attacking artifacts, not
   memory.", loads the role reference of each required lens, and attacks the current diff
   with evidence (`file:line`, failing input, trace), lens by lens. One report, one
   attestation block per lens.
5. **Re-review after fixes**: only the lenses the fix touched, on the current tree.
6. **High tier only**: a fresh Critic, the first report withheld, works lenses 2 and 3 over
   the high-risk surface.
7. **Converge honestly**: `CCC-CONVERGED` needs required-lens artifacts, nothing open at
   or above the floor, evidenced dispositions, green verify, and complete attestations;
   anything less is `TEST-GATED` or `HALTED`. Green verify alone never converges.
8. **Report** in the required shape below (see Quick start examples for invocations).

Standing lines: findings and rebuttals need evidence; no secrets in reports; repo contracts
win and gates never weaken; Critics attack a scratch copy on a fresh context, never the live
tree; role-to-spawn mapping lives in the tool adapter.

---

## Reference

## Parameters

Parse from the user message (ask only if ambiguous):

| Param | Default | Notes |
|---|---|---|
| **`task`** | (required) | What slice, PR, branch, or bug surface to attack |
| **`repo`** | Current workspace | Absolute or relative project root (primary tree) |
| **`dependency_repos`** | auto / `[]` | Extra trees the Critic must attack when the slice pins/depends on them. Auto: load-bearing git-pinned siblings on disk. Explicit `[]` = primary only (**disclose**) |
| **`mode`** | `review-only` | `review-only` = the Critic only (default). `review-and-fix` = after the findings, a **Fixer** pass remediates, then the Critic re-reviews (still no blind “Actor build a new feature” phase unless user expands `task`) |
| **`max_cycles`** | `2` | Remediation cycles in `review-and-fix` (fix → re-review). Cap prevents infinite loops |
| **`severity_floor`** | `S1` | Open findings at/above this severity block convergence (`S0`…`S3`) |
| **`risk_tier`** | auto | `exempt` \| `mechanical` \| `standard` \| `high` — from **riskiest file touched** |
| **`claims_critic`** | see note | Default **true** when the unit writes a COMPLETE, unit ledger, map.md claim, STATUS-class record, or §6 registry row. Otherwise false. Opt-out only by explicit `claims_critic=false`. When true, lens 4 joins the pass. |
| **`verify`** | project default | Prefer repo Makefile/CI contracts. Never invent a matrix that contradicts them |

---

## Risk tiers

| Tier | When | CCC intensity |
|---|---|---|
| **Exempt** | Docs/comments/formatting only, **no** runtime surface | Skip the Critic; optional light self-check |
| **Mechanical** | Pure renames, moves, test-only with no behavior change | Lens 1 only (crates contracts + test adequacy). Lenses 2 and 3 N/A unless paths touch auth, parsers, unsafe, or logic-heavy code |
| **Standard** (default) | Any behavior-affecting change | One pass, lenses 1 + 2 + 3 (and 4 when `claims_critic` is on). Lens 1 runs the **test-coverage skeptic**. Lens 3 runs the **logic attack taxonomy** |
| **High** | Locking, consensus, persistence, authn/authz/crypto, on-disk/on-wire formats, public API, multi-step publish/commit/OR REPLACE, catalog pointer swaps, or nearest `AGENTS.md` high-risk | The standard pass with **no soft N/A** on concurrency, partial-failure, compatibility when touched, plus **one independent pass** over the high-risk surface: persistence, commit atomicity, concurrency, security and data-loss paths. Prefer a real subagent for the independent pass. Lens 2 **must** pressure atomicity/mid-commit. Lens 3 **must** pressure edge values and multi-writer ordering on logic paths |

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

1. **Distinct Critic phases** are lenses of one pass — lens 1, 2, 3 (when the tier requires), plus lens 4 when `claims_critic` is on. Each lens works its own taxonomy and files its own attestation; a lens without both is a skim. A second Critic runs only as the high-tier independent pass.
2. **Context break** before the pass and before the independent pass — attack **diff + artifacts**, not session memory. Load **nearest scoped `AGENTS.md`** as attack surface.
3. **One pass, then targeted re-review** of the lenses a fix touched. The independent pass starts fresh and files before it reads the first report. Report order stays quality → security → logic → claims.
4. **Findings require evidence** — path + region; *Potential* when unproven; never invent paths.
5. **Rebuttals require evidence** — test / traced path / cited invariant.
6. **No secrets in reports** — redact values; pattern + location only.
7. **Repo contracts win** — root + nearest `AGENTS.md` / `CLAUDE.md` / project skills. CCC never overrides a project hard gate.
8. **Never weaken the gate** — no skip/loosen of checks to force green.
9. **Every behavior change needs a mutation-proof test** (Standard/High) — the lens 1 test-coverage skeptic enforces this.
10. **Green verify alone is never convergence** — see [Convergence labels](#convergence-labels-hard).
11. **Load-bearing dependency trees are in Critic scope** when clauses depend on them.
12. **Critic-1 crates contract** — for any touch under `crates/` (or equivalent library roots), apply the [Crates / library attack contract](#crates--library-attack-contract-critic-1) in lens 1 (full detail in the Critic-1 reference).
13. **Spawn contract** — apply the [Spawn contract](#spawn-contract) on every child: the invariants are here, the tool-specific mapping is in the tool's adapter.

---

## Spawn contract

Tool-neutral invariants; every binder applies them to each child it starts. The mapping onto a
tool's agent types, capability flags and isolation options is written **once, in that tool's
adapter** — never here, never in a child prompt from memory.

| Role | Needs | Must not | Context |
|---|---|---|---|
| The Critic, the independent pass and any `git` / verify probe | read the tree, run shell (`git`, the verify commands) | edit files | **fresh** — never resumed from the Actor, and the independent pass never from the first Critic (a resumed context leaks the narrative the context break exists to exclude) |
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
  carry is the repository's (`git config` at the repo root), checked by lens 4 at `%ae`.

---

## Crates / library attack contract (Critic-1)

Applies to all paths under `crates/` (and the same rules by analogy for other pure-library roots the repo marks as library code). Lens 1 **must** attack these categories when the diff touches library code — not soft-skip as “style.”

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

**Boundary:** production panics as a *safety class*, `unsafe`, secrets, injection are lens 2 findings. Deep multi-step logic wrongness (predicate inversion, silent wrong rows) is a lens 3 finding, worked with the logic taxonomy.

---

## Convergence labels (hard)

| Label | Meaning | Allowed when |
|---|---|---|
| **`CCC-CONVERGED`** | Required lenses ran CLEAN (or residual below floor ACCEPTED_FLAGGED); the independent pass ran when the tier is high; full verify green; coverage skeptic + logic attestation satisfied when applicable | Critic artifacts exist for every required lens |
| **`TEST-GATED`** | Verify/tests green but the review incomplete or skipped | Ceremony deferred |
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
6. Write **slice charter** (scope, success conditions, constraints, enumeration partitions, risk tier, which lenses run, whether the independent pass runs).

If ambiguous scope → **stop and ask**. If `exempt` → document and stop.

---

### The Critic pass

**Skip if `risk_tier=exempt`.**

1. Context break: *“Context break executed; attacking artifacts, not memory.”*
2. Prefer a **fresh `explore` subagent** (shell allowed, no edits — see Spawn contract). Inputs: charter, current diff(s), tests, verify, nearest `AGENTS.md` — **not** author excuses first.
3. Work the required lenses in order. For each: load its reference, work its taxonomy, file findings and the attestation, null reports for clean categories.
4. One verdict per lens and one for the pass: `CLEAN` | `NEEDS_REMEDIATION`.

| Lens | N/A when | Works | Findings |
|---|---|---|---|
| 1 Quality / Bugs + crates | never; mechanical narrows it to crates contracts + correctness/test focus | Quality + Crates taxonomies; **test-coverage skeptic** (mutation-proof dual probe) on Standard/High behavior changes; **enumeration span** when the charter names a finite partition | `Q-` / `CRATE-` |
| 2 Safety / Security | mechanical with no security/safety surface | Security/Safety taxonomy; High: atomicity pressure on commit/publish | `SEC-` / `SAF-` |
| 3 Logic Bugs | mechanical with no logic-bearing diff | Logic attack taxonomy, exhaustively — concrete edge values, silent wrong results, incomplete matches, racey wrong outcomes; crates style or secret handling only when they *cause* a wrong result | `L-` |
| 4 Claims / Record | `claims_critic` is off | Claims taxonomy; identity claims need `%ae` across the branch (CL-IDENTITY), not the author name | `CL-` |

Every N/A is written down with its justification.

---

### Targeted re-review

After the Actor (in `review-and-fix`, the Fixer, on the filed findings only) remediates, the
Critic re-attacks **the lenses the fix touched** — and any lens the fix may have re-broken — on
the current tree, in the same report. Cycles count against `max_cycles`; residuals at the cap
escalate to the user.

---

### Independent pass (high tier)

A **fresh** Critic, the first report withheld, works lenses 2 and 3 over the high-risk surface
the tier named. Its findings join the report under their own heading once both passes have
filed; a finding both filed is recorded once, marked found twice.

---

### Convergence

Work is **`CCC-CONVERGED`** only when:

1. Risk tier applied; required lenses have **artifacts** (findings + attestation)
2. No open finding ≥ `severity_floor` on required lenses or the independent pass
3. Every finding REMEDIATED / WITHDRAWN (evidence) / ACCEPTED_FLAGGED (policy)
4. **Verify green** with full project gate when shipping
5. Standard/High: mutation-proof tests (lens 1 skeptic)
6. Enumeration partitions pin-count satisfied when applicable
7. `dependency_repos` attacked when load-bearing
8. Lens 3 logic attestation complete when required (Standard/High behavior)
9. Lens 4 claims attestation complete when `claims_critic` is on (ledger-bearing default)
10. High: the independent pass filed its attestation

If the review was skipped → **`TEST-GATED`**. If max_cycles + open ≥ floor → **`HALTED`**.

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
**Isolation:** subagent | in-session break

## Lens 1 — Critic-1 (Quality / Bugs + crates)
- Verdict: CLEAN | NEEDS_REMEDIATION | SKIPPED
- Findings: count by severity
- Top findings: …
- Crates contract: attacked | n/a (paths)
- Coverage attestation: complete yes/no
- Test-coverage skeptic: …
- Mutation-proof pins: ok | findings
- Null reports: …

## Lens 2 — Critic-2 (Safety / Security)
- Verdict: …
- Findings: …
- Atomicity/partial-failure (if applicable): attacked | n/a
- Null reports: …

## Lens 3 — Critic-3 (Logic Bugs)
- Verdict: …
- Findings: …
- Edge-value / silent-wrong pressure: attacked | n/a
- Null reports: …

## Lens 4 — Critic-4 (Claims / Record — when claims_critic)
- Verdict: …
- Findings: …
- CL-IDENTITY (`%ae` across branch): attacked | n/a
- Null reports: …

## Independent pass (high only)
- Surface: …
- Verdict: …
- Findings: … (mark any the first pass also filed)
- Null reports: …

## Residual / accepted-flagged
…

## Files in scope (final)
…
```

---

## Subagent guidance

| Role | Role shape | Notes |
|---|---|---|
| The Critic | read-attack (shell, no edits) | Attack only; every required lens |
| Independent pass (high) | read-attack | Fresh; the first report withheld until it files |
| Fixer (`review-and-fix`) | build (shell + edits) | Fix filed findings only; re-verify |

Which agent type each shape maps to is the tool adapter's table. If spawning is unavailable or
not opted into (the SEPMO manifest's `context_break_mechanics` row decides): the same session
runs both passes behind declared context breaks — and the report names the weaker independence.
Do not invent a swarm.

---

## As the SEPMO Critic engine

When the SEPMO manifest binds this skill as `critic_engine`, the spine's four constraints for an
external engine ([../sepmo/references/05-critic.md](../sepmo/references/05-critic.md) "External
critic engines") apply and this section is how they are met:

1. **`CCC-CONVERGED` is never Delivery.** The final report maps into the spine's instruments —
   each lens's coverage attestation becomes the unit ledger's `COVERAGE_ATTESTATION` rows and
   each `FINDING:` becomes a ledger finding — and `PR_READINESS_AUDIT` then runs exactly as
   always (R7).
2. **LIGHT units never select this engine**; the proportionality rubric decides the path first.
3. **Taxonomy mapping onto the spine's AT-1..AT-10** is recorded in the manifest row; a
   category this skill does not attack is a justified `N/A` there, never silence.
4. **Tunables bind in the manifest row** (`mode`, `max_cycles`, `severity_floor`,
   `claims_critic`, `risk_tier` source, scratch location), never in this file.

---

## Anti-patterns

- Running the one pass as a skim — a lens with no taxonomy worked or no attestation filed
- Skipping crates contract on `crates/` diffs (“style only”)
- Skipping the independent pass on a high-tier change, or letting it read the first report before it files
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

Moved to [history.md](history.md): where the loop and its contract came from now lives
beside the skill.

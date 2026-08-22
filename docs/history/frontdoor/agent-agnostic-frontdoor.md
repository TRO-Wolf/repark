# DESIGN — the Agent-Agnostic Front-Door campaign

> **ARCHIVED 2026-08-10** (Front-Door close-out) — a historical record of the Agent-Agnostic
> Front-Door campaign, kept for provenance and **not a source of live rules**: every rule still in
> force lives in a current document ([retrospective.md](retrospective.md) "Promotion check").
> Relative links were repaired for this location on the same date; nothing else changed. Current
> state: [STATUS.md](../../../STATUS.md).

**Status:** IMPLEMENTED — settled 2026-08-08, executed by FD-1…FD-5 (merged 2026-08-09), campaign
closed 2026-08-10 · **Date:** 2026-08-08 · **Execution slate:**
[frontdoor-campaign.md](frontdoor-campaign.md)
**Origin:** the agent-agnostic repository proposal (owner-reviewed).
**Owner ruling carried in:** keep the universal hand-written `map.md` discipline (proposal §4 REJECTED);
everything else favorable.

This is the first **post-milestone** campaign. It runs on `main` now that milestone one (the port) is
merged, and it is **documentation + mechanical-gate work only** — no product-behavior change to the
engine. It exists to make RePark legible and safely modifiable by *any* contributor or agent, without
depending on a model name, agent vendor, or porting-era vocabulary.

---

## 1. Why now, and why bounded

The proposal's own top risk was "repository cleanup distracting from Phase 3." That risk is now
**cleared** — phase 3 is merged, v1 is going bugfix-only, and the porting vocabulary (census cohorts,
copy-then-re-home, PR ledgers) has done its job and is now noise on the normal read path. This is the
correct window.

The campaign is delivered as **small, reviewable, behavior-preserving PRs** (FD-1…FD-5). Each is
independently mergeable and leaves the repo green. No PR in this campaign touches engine crates' *code*;
FD-3 touches mechanical-gate *tooling* only.

---

## 2. The load-bearing constraints this campaign must not break

RePark already has an unusually strong governance spine. The proposal must be **reconciled with** it,
not bulldozed through it. Three invariants are non-negotiable and shape every disposition below:

1. **The precedence chain must survive with exactly one home.** Today CLAUDE.md is "the single home for
   the chain." Agent-agnosticism moves that home (see §4, the pivotal decision) but never duplicates or
   deletes it.
2. **The two authoritative contracts stay in sync.** Today: `CLAUDE.md ≡ AGENTS.md`. This campaign
   changes *which* is authoritative and *what each contains*, but the "no-drift, update-both" rule is
   preserved in spirit — after the campaign there is one authoritative neutral contract plus thin
   adapters that cannot drift because they carry no authoritative facts.
3. **`map.md` stays universal and hand-written** (owner ruling). No generator. The proposal's §4
   "generate navigation" is rejected. The *value* the proposal wanted from §8 component-contracts is
   captured **inside** the crate-root `map.md` as a standardized section, not in a competing file.

---

## 3. Disposition of every proposal recommendation

The synthesis. Each of the proposal's 10 recommendations is ACCEPTED / ACCEPTED-MODIFIED /
REJECTED / DEFERRED, with the reconciliation noted.

| # | Proposal recommendation | Disposition | Reconciliation note |
|---|---|---|---|
| 1 | Neutral documentation spine (STATUS/ARCHITECTURE/DEVELOPMENT/CONTRIBUTING/AGENTS/adr) | **ACCEPT-MODIFIED** | Add `STATUS.md`, `ARCHITECTURE.md`, `DEVELOPMENT.md` (new neutral homes). `CONTRIBUTING.md`, `README.md`, `docs/adr/` already exist. `PROJECT.md` **kept** as stable product charter (vision/intent only); its "Current state" moves to `STATUS.md`. Status stops living in CLAUDE.md/Cargo comments/maps. |
| 2 | Vendor-neutral `AGENTS.md` + thin `.agents/` adapters | **ACCEPT-MODIFIED** | The heart of the campaign. AGENTS.md becomes THE single neutral authoritative contract (see §4). `docs/skills/` tier manuals are recast as the **Claude adapter**; `CLAUDE.md` → thin pointer. `.agents/{common,claude,codex,cursor}.md` optional; codex/cursor land as stubs pointing inward. |
| 3 | Machine-readable `repo-manifest.toml` + CI validation | **ACCEPT** | Structural SSOT: crate inventory, phase, gate commands, doc index, component status. Validator wired into `make ci` + CI. Complements (does not duplicate) `scripts/check_crate_dag.py`. |
| 4 | Replace universal `map.md` with generated navigation | **REJECT** (owner ruling) | `map.md` stays everywhere, hand-written. No generator. We *may* add a manifest↔crate-root-map **consistency check** (map must exist + agree with manifest), never generation. |
| 5 | Archive completed campaign artifacts → `docs/history/` | **ACCEPT** | Move phase-0…3 briefs, `task/p*-ledger.md`, census narratives, retrospectives into `docs/history/port-v2/`. **Promote-before-archive**: any still-active lesson goes to an authoritative home first. Census evidence stays reachable via a STATUS pointer. |
| 6 | Explicit allowed-edge dependency policy (with dep kind) | **ACCEPT** | Upgrade `check_crate_dag.py` from tier-only to an explicit edge table (normal/dev/build/optional). Encodes the door-to-door ban, binding→engine direction, `repark-common` independence. |
| 7 | Runtime-flow docs (session build / query / write) | **ACCEPT** | Lands in `ARCHITECTURE.md`, hand-written. Session-construction, per-door query-execution, write/commit flow; names which steps do I/O and which failures happen when. |
| 8 | Per-crate component contracts | **ACCEPT-MODIFIED** | Folded into each **crate-root `map.md`** as a standardized `## Component contract` section (Owns / Does-not-own / inputs / outputs / lifecycle / allowed deps / failure model / extension points / test strategy / limitations). No separate `COMPONENT.md` — honors the map.md ruling and gets the contract value. |
| 9 | One live backlog in `STATUS.md` | **ACCEPT** | `task/todo.md` active items migrate into `STATUS.md`. Retrospectives/PR records → history. Deferred-test manifests (`task/port/*.txt`) stay as active acceptance inputs, linked from STATUS. |
| 10.1 | `ExecutionBackend` honest documentation | **ACCEPT (small)** | Describe the current trait truthfully as a local execution-context holder + future extension point — not as proof distributed needs no wider change. A paragraph in ARCHITECTURE.md + a doc-comment fix. |
| 10.2 | Decompose `ReparkSession` internals | **DEFER** | Real engineering, not doc/agnostic work. The proposal itself says wait for a concrete driver (PyO3 pressure, a second backend, cancellation, server needs). Recorded as a **driver-gated** deferred unit in STATUS "Deferred capabilities" + a tracking ADR stub; **not executed in this campaign.** |

---

## 4. The pivotal decision — where authority lives after this campaign

This is the one call that reshapes the governance spine, and it received the owner's explicit yes.

**DECISION (2026-08-08): Option A CONFIRMED by owner.** `AGENTS.md` becomes the single vendor-neutral
authoritative contract; `CLAUDE.md` demotes to a thin Claude adapter carrying zero authoritative facts.
FD-2 executes against this.

**Option A (chosen): `AGENTS.md` becomes the single vendor-neutral authoritative contract.**
- `AGENTS.md` holds the precedence chain, the non-negotiable invariants, the change-location guide, the
  testing contract pointer, required verification, and the safety/approval boundaries — written for
  "any automated or human contributor," naming no model tier.
- `CLAUDE.md` is demoted to a **thin Claude adapter**: "read AGENTS.md + ARCHITECTURE.md + DEVELOPMENT.md
  + STATUS.md; here are Claude-specific tool mechanics and the tier→effort map." It carries **zero
  authoritative facts**, so it *cannot* drift.
- The Claude tier manuals under `docs/skills/` remain, reachable **from** the Claude adapter, not from
  the neutral contract.
- The subagent/tier policy (currently in CLAUDE.md) is Claude-specific → moves into the Claude adapter.

**Why A over the lighter Option B** (keep `CLAUDE.md ≡ AGENTS.md` dual-authority, just strip vendor
specifics into adapters): B leaves two authoritative files that must be hand-kept in sync forever — the
exact drift surface the campaign is trying to remove. A collapses authority to one neutral file and makes
every vendor file structurally incapable of drifting (no facts to drift). A is the honest expression of
"agent-agnostic."

**Cost of A, acknowledged:** the read-order muscle memory ("read CLAUDE.md first") changes; the Claude
adapter must be trustworthy enough that a Claude session lands on AGENTS.md on turn 1. Mitigation: the
Claude adapter's first line is a hard "STOP — the authoritative contract is AGENTS.md; read it now,"
and CLAUDE.md keeps its filename so tooling that auto-loads it still fires.

> **RESOLVED:** Option A confirmed by owner 2026-08-08. FD-2 executes against it.

---

## 5. Non-goals / explicit deferrals

- **No engine refactor.** `ReparkSession` decomposition (§10.2) and any `ExecutionBackend` redesign are
  **out**; only the honest-documentation correction (§10.1) is in.
- **No `map.md` generator.** Rejected by owner ruling; not revisited here.
- **No history rewrite.** Archival is `git mv` + pointers; provenance and Git history are preserved.
- **No release action.** Tagging/PyPI is the separate milestone-one user-side checklist, not this
  campaign.
- **No product-doc invention.** Generated/validated content is factual tables only (inventory, phase,
  commands, doc index). Rationale, boundaries, and flows stay hand-written.

---

## 6. Definition of success (campaign acceptance)

Adapted from the proposal's "Definition of success," made checkable:

1. No authoritative rule names a model tier or agent vendor. (A tier/vendor grep over the neutral
   contract returns nothing authoritative — only adapter files match.)
2. Current status has one source of truth: `STATUS.md`. A phase/crate-state grep yields one authoritative
   statement; front-door docs carry no contradictory crate claims.
3. Structural facts are machine-readable and CI-validated: an unclassified Cargo member, a stale phase
   field, a missing declared doc, or a dead command **fails CI** (`repo-manifest.toml` validator).
4. Dependency constraints are explicit edges with kind; a new same-layer edge fails until reviewed and
   allowed; the dev-only ANSI→Spark edge is expressible without permitting a production edge.
5. Runtime flows (session build / query per door / write+commit) and per-crate component contracts are
   readable without reconstructing them from source.
6. Port evidence is preserved under `docs/history/port-v2/` but is not on the normal read path; no active
   engineering rule lives *only* in archived material.
7. `map.md` remains universal and hand-written; component contracts live inside crate-root maps.
8. A new capable agent — never having seen this repo — can locate, implement, verify, and hand off a
   bounded change using the same docs a human would, starting from `README → STATUS → ARCHITECTURE →
   DEVELOPMENT → AGENTS.md`.

**OUTCOME (dated correction, 2026-08-10).** Assessed item by item in
[retrospective.md](retrospective.md) §3 against the tree as it stood at the fifth merge: six items
TRUE outright, item 4 TRUE with the campaign's strongest evidence, item 2 PARTIAL (three stale
current-state claims survived outside the status SSOT) and item 8 UNDEMONSTRATED (path declared but
not signposted from `README.md`; no trial run). Items 2 and 8 were closed by the close-out unit —
the three sites corrected, a `## Where to start` block added to `README.md`, and one cold-read trial
run and recorded verbatim ([retrospective.md](retrospective.md) §3 item 8).

---

## 7. Reconciliation identity for the archival move (FD-4)

Archival must be provably lossless, mirroring the port's census discipline:

> every current-doc rule R ∈ {before} either (a) remains in an authoritative current doc, or (b) is
> promoted to one, or (c) is a dated historical claim moved verbatim to `docs/history/`. No rule is
> *only* reachable through an archived retrospective.

FD-4 ships a **promotion ledger** (`task/port/promoted-lessons.md` or the campaign ledger) mapping each
archived-source rule → its new authoritative home, exactly as the proposal's "require a mapping from
every removed section to its new authoritative or historical home" safeguard demands.

---

## 8. Open questions for the owner (none block FD-1)

1. ~~**Authority model (§4):**~~ **RESOLVED — Option A** (single neutral AGENTS.md) confirmed 2026-08-08.
2. ~~**`.agents/` adapters:**~~ **RESOLVED at FD-2** (#25, 2026-08-09, dated correction added
   2026-08-10) — the default was taken: a real adapter plus one-line stubs pointing inward.
   Navigation: [.agents/](../../../.agents/map.md).
3. **Sequencing vs. the two parked lanes:** this campaign is doc/tooling and conflicts with nothing, so
   it can run *before, after, or interleaved with* the `repark.sql` re-home and dbt-repark lane. Default:
   run FD-1…FD-3 first (they make the repo more legible for that lane), FD-4…FD-5 after.

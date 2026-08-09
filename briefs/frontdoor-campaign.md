# BRIEF — the Agent-Agnostic Front-Door campaign (execution slate)

**Status:** In-repo campaign slate · **Design:**
[../docs/design/agent-agnostic-frontdoor.md](../docs/design/agent-agnostic-frontdoor.md) ·
**Date:** 2026-08-08 · **Target repo:** `github.com/TRO-Wolf/repark` (public)

This is the per-PR slate. Five units (FD-1…FD-5), each independently mergeable, each leaving `main`
green. **No engine code changes**; FD-3 changes mechanical-gate tooling only. In-repo, the design lives
at [../docs/design/agent-agnostic-frontdoor.md](../docs/design/agent-agnostic-frontdoor.md) and this
brief at `briefs/frontdoor-campaign.md` (landed with FD-1, the same pattern the port phases used).

---

## Orchestration (standing rules)

- **Orchestrator (Fable):** scopes each unit, assembles the diff, runs verification panels, vets fixes,
  pushes, opens PRs. Never a code author of record beyond assembly.
- **Actors** (do the writing/edits per unit): **Opus-5, medium** effort.
- **Critics / verifiers** (adversarial review, census/consistency lenses): **Opus-5, high** effort.
- **Owner** squash-merges every PR. Nothing hits the remote until the owner rules.
- Single-agent-in-main is the default *for drawing up*; **agent fan-out is only used when the owner
  says go on execution** (subagent policy). This brief does not spawn anything.
- **Public-repo hygiene** applies to every unit: two-pass grep (added-lines content + commit metadata)
  vs the forbidden-patterns list; repo-local git identity; `Authored-By: Claude (<model>)` trailer.

(These model-tier labels are *process notes* for how this campaign was drawn up, not authoritative
project rules — making the project's authoritative surface model-agnostic is the campaign's own goal.)

---

## Sequencing

```
FD-1 ──▶ FD-2 ──▶ FD-3 ──▶ FD-4 ──▶ FD-5
(truth)  (neutral (mechanize (reduce  (seam
 front)   contract) structure) weight)  honesty)
```

- **FD-1** depends on nothing. **FD-2** depends on FD-1 (needs STATUS/ARCHITECTURE/DEVELOPMENT to point
  the neutral contract at) **and on the §4 authority ruling.** **FD-3** depends on FD-2 (manifest's doc
  index references the new spine). **FD-4** depends on FD-1+FD-3 (STATUS is the backlog home; manifest
  validates the archive didn't strand a declared doc). **FD-5** depends on FD-2 (ARCHITECTURE.md exists).
- Runs independently of the `repark.sql` re-home and dbt-repark lanes. Recommended: FD-1…FD-3
  land before the dbt-repark lane starts in earnest (a more legible repo helps that agent).

---

## FD-1 — one truthful front door  ·  verification: FULL (consistency + hygiene lenses)

**Goal:** the repo tells exactly one consistent story about its present state.

**Edits**
- **Add `STATUS.md`** (new status SSOT): release state · delivered capabilities (one-line milestone
  table: phases 0–3 = milestone one DONE) · current milestone · active workstreams · known correctness
  issues (Spark-door TT view leak; `$`-metadata rider) · architectural risks · deferred capabilities
  (§10.2 session decomposition, driver-gated) · release blockers.
- **Correct stale phase/crate claims** everywhere they currently live: `PROJECT.md` "Current state"
  → replaced by a one-line "status: see STATUS.md" pointer; `CLAUDE.md`/`AGENTS.md` status lines →
  pointer; Cargo.toml comments and any `map.md` carrying phase/status → pointer.
- **Establish the authoritative home for every repeated current fact**; replace each duplicate with a
  link. (Crate inventory → will be ARCHITECTURE.md/manifest in FD-2/3; for FD-1, link to `crates/map.md`.)

**Acceptance gate**
- A phase/milestone grep over `*.md docs/ crates/**/map.md` → every hit is either inside `STATUS.md`
  (authoritative) or a link to it. No second authoritative statement.
- Cargo workspace member list agrees with what STATUS/`crates/map.md` claim exists (9 crates).
- `make ci` green; both hygiene passes zero over the branch range; touched-dir `map.md` updated in
  lockstep (`check_map_md.sh`).

---

## FD-2 — the neutral contributor interface  ·  verification: FULL (agnosticism + design-conformance lenses)  ·  **gated on §4 ruling**

**Goal:** humans and any agent share one neutral contract; vendor files carry no authoritative facts.

**Edits**
- **Add `ARCHITECTURE.md`:** current component boundaries + dependency direction + the three runtime
  flows (session-construction, per-door query-execution, write/commit — proposal §7), + the honest
  `ExecutionBackend` paragraph (§10.1). Hand-written.
- **Add `DEVELOPMENT.md`:** local setup, `make` targets, formatting, tests, CI surface, troubleshooting.
  Absorbs the command/tool instructions currently embedded in the agent contracts.
- **Rewrite `AGENTS.md` vendor-neutral** (Option A): read-first · architectural invariants · precedence
  chain (moved here as its single home) · change-location guide · testing contract (pointer to
  `docs/testing.md`) · required verification · doc-synchronization · safety/approval boundaries ·
  definition of done. Names **no** model tier.
- **Demote `CLAUDE.md` → thin Claude adapter:** first line "STOP — authoritative contract is AGENTS.md,"
  then Claude-specific tool mechanics + the tier→effort map + the subagent policy (moved out of the
  neutral contract). Zero authoritative facts.
- **Recast the `docs/skills/` tier manuals** as the Claude tier manuals, reachable from the Claude
  adapter only.
- **`.agent/`:** `common.md` (points to the neutral spine) + `claude.md` (real) + `codex.md`/`cursor.md`
  (one-line stubs pointing inward) — pending owner answer on open-question 2.
- **Component contracts** (proposal §8): add a standardized `## Component contract` section to each of
  the 9 crate-root `map.md` files (Owns / Does-not-own / public inputs / public outputs / state+lifecycle
  / allowed internal deps / failure model / extension points / test strategy / known limitations).

**Acceptance gate**
- Deleting any one vendor adapter (`CLAUDE.md`, `.agent/*`) loses **no** project knowledge (every fact
  it references resolves in the neutral spine).
- A tier/vendor grep over `AGENTS.md` → no authoritative rule matches (only cross-references, if any).
- The precedence chain appears in exactly one file (AGENTS.md); CLAUDE.md/CONTRIBUTING/PROJECT point to
  it, never restate it.
- Every crate-root `map.md` carries the component-contract section; `check_map_md.sh` green.
- `make ci` green; hygiene passes zero.

---

## FD-3 — mechanize structural truth  ·  verification: FULL (validator-correctness + adversarial-bypass lenses)

**Goal:** structural documentation drift becomes a CI failure.

**Edits**
- **Add `repo-manifest.toml`:** `[project]` (phase, status, canonical/completion/pre-pr gates),
  `[documentation]` (architecture/development/status/testing paths), `[components.*]` (path, layer,
  status) for all 9 crates.
- **Add the validator** (`scripts/check_manifest.py` + `.sh`, wired into `make ci` + `ci.yml`, SHA-pinned
  tools): every Cargo member is declared; every delivered component exists at its path; planned≠delivered;
  layers recognized; canonical `make` targets exist; declared docs exist; `STATUS.md` phase == manifest
  phase; every internal crate covered by dependency policy.
- **Upgrade `check_crate_dag.py` → explicit allowed-edges** with dependency kind (normal/dev/build/
  optional). Encode: no door↔door normal edge; bindings→engine only (engine ⊄ bindings); `repark-common`
  depends on nothing internal; kernel/function crates ⊄ a user-facing door; the **dev-only ANSI→Spark**
  edge allowed as `dev`, not `normal`. Error messages name exact source/target/kind.
- **Manifest↔map consistency check** (the *only* map.md automation allowed): crate-root `map.md` must
  exist for every declared component and name the same path/layer. **No generation.**

**Acceptance gate**
- Adding a Cargo member without a manifest entry → CI red (demonstrated by a scratch commit reverted).
- Adding a new same-layer `normal` edge → CI red until added to the policy.
- Flipping a `status = "planned"` component to a real path while code is absent → CI red.
- `make ci` / `make preflight` green with the new gate; `check_workflows_parse.py` green.

---

## FD-4 — reduce active documentation weight  ·  verification: FULL (lossless-archival/promotion-ledger + hygiene lenses)

**Goal:** detailed history preserved, off the normal read path; live backlog is small.

**Edits**
- **Create `docs/history/port-v2/`** with `README.md` (what the port was, source rev `fc3f48102`, how
  parity was verified, where records live, which decisions are still current ADRs). `git mv` into it:
  `briefs/phase-{0,1,2,3}-*.md`, `task/p{1,2,3}*-ledger.md`, census narratives, phase retrospectives.
- **Promotion-before-archive:** produce the promotion ledger (DESIGN §7) mapping each still-active rule
  in an archived source → its authoritative home (`AGENTS.md` / `DEVELOPMENT.md` / an ADR / a component
  contract / a mechanical check). Promote *before* the `git mv`.
- **Condense the live backlog:** migrate `task/todo.md` active items into `STATUS.md`; leave `todo.md`
  as a one-line pointer or retire it. Keep `task/port/*.txt` deferred-test manifests live (linked from
  STATUS as active acceptance inputs). Keep `task/census/{baseline,v2}` as evidence, pointer from STATUS.
- **Archived-material rules:** immutable except link-repair/dated corrections; current docs link to
  history only where provenance matters; archived status claims carry an effective date.

**Acceptance gate**
- The promotion ledger accounts for **every** rule in every archived file (reconciliation identity,
  DESIGN §7): no active rule reachable *only* through an archived retrospective — verified by the critic
  lens reading each archived file and confirming its live rules resolve in a current doc.
- A cold-read contributor can answer "what should happen next?" from `STATUS.md` alone.
- All internal links resolve post-move (`check_map_md.sh` + a link check); `map.md` present in every new
  `docs/history/**` dir (universal-map discipline holds even in the archive).
- `make ci` green; hygiene passes zero.

---

## FD-5 — seam honesty + record the deferred refactor  ·  verification: SLIM (1 adversarial verifier)

**Goal:** stop overclaiming the runtime seams; record the deferred engine refactor with its trigger.

**Edits**
- **`ExecutionBackend` honest doc** (§10.1): correct the trait's doc-comment + the ARCHITECTURE.md
  paragraph to describe it as a *local execution-context holder and future extension point*, not proof
  distributed execution needs no wider change. Doc/comment only — no signature change.
- **Record §10.2 as a deferred, driver-gated unit:** an ADR stub (`docs/adr/0005-defer-session-decomposition.md`,
  status *Deferred*) naming the trigger conditions (PyO3 pressure, a second `ExecutionBackend`,
  cancellation, server protocol) and the intended internal services (RuntimeFactory / CatalogManager /
  ObjectStoreRegistry / TemporaryViewManager / QueryService / SemanticProfile). Cross-linked from
  STATUS "Deferred capabilities." **No refactor executed.**

**Acceptance gate**
- No `ExecutionBackend` or `ReparkSession` *code* change in the diff (grep the diff for `.rs` hunks in
  `repark-core` → doc-comment lines only).
- ADR-0005 exists, status *Deferred*, linked both ways with STATUS.
- `make ci` green; hygiene passes zero.

---

## What "done" means for the campaign

All five units merged; DESIGN §6 definition-of-success items 1–8 each demonstrably true; a fresh agent
can execute a bounded change from `README → STATUS → ARCHITECTURE → DEVELOPMENT → AGENTS.md` with no
model-tier or porting vocabulary in the path. Campaign retrospective appended to the (now-condensed)
STATUS/history, and this brief + design archived under `docs/history/` as the campaign's own record.

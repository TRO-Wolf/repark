# Unit ledger — FD-3: mechanize structural truth

**Unit:** Front-Door FD-3 · **Slate:** the Front-Door campaign brief, unit "FD-3" · **Design:**
the campaign design, §3 (recommendations 3, 4, 6). The brief and design are execution records,
not live rules — they land in-repo with the campaign's closing archival under `docs/history/` ·
**Status:** IN FLIGHT

Goal: **structural documentation drift becomes a CI failure.** Documentation + mechanical-gate
work only — zero `.rs` changes, no engine behavior touched.

## Scope

- **`repo-manifest.toml` (new, repository root)** — the machine-readable structural facts:
  `[project]` (phase / phase_state / release_status + the three canonical gate commands),
  `[documentation]` (contract / architecture / development / status / testing), and
  `[components.*]` (path / layer / status) for the nine delivered crates plus the three
  `planned` homes AGENTS.md's change-location guide already names (`repark-exec`, `repark-io`,
  `repark-connect`).
- **`scripts/check_manifest.py` + `.sh` (new)** — the validator that makes the manifest true.
  Wired into `make check-manifest` → the `make ci` chain, the ci.yml `guards` job, the
  `make install-hooks` pre-commit hook, and `.pre-commit-config.yaml`.
- **`scripts/check_crate_dag.py` (upgraded)** — from a tier map alone to a **tier map + crate
  roles + an explicit allowed-edge table with dependency kinds**
  (`normal` / `optional` / `dev` / `build`). Still the single SSOT for crate layering; strictly
  richer, never relocated, and no rule it previously enforced was weakened.
- **Prose lockstep** — `scripts/map.md`, root `map.md`, `.github/workflows/map.md`,
  `crates/map.md`, six crate-root `map.md` files (all nine delivered crates now state their tier,
  as the new consistency rule requires — four gained a deliberate clause from the unit's first
  pass, two more at the adversarial review's N5), `crates/repark-sql/Cargo.toml` +
  `crates/repark-python/Cargo.toml` (the two comments that claimed the guard ignores dev edges,
  each paired with its directory's `map.md`), `AGENTS.md`, `ARCHITECTURE.md`, `DEVELOPMENT.md`,
  `task/map.md`, `task/lessons.md` (R-1).

Out of scope: any `map.md` **generation** (design §2.3, owner ruling — the consistency rule
checks hand-written maps and never writes one); the FD-4 archival; the FD-5 seam-honesty edits.

## Design decisions

- **D-1 — the manifest is a validated MIRROR, never a second source of truth.** Every field is
  checked against a real artifact: `Cargo.toml` (inventory both ways), the `Makefile` (the gate
  commands are live targets), `STATUS.md` (the phase/release words), the declared documents
  (they exist), and the crate-root `map.md` files. `layer` is checked against
  `scripts/check_crate_dag.py`'s tier map — **imported, not copied** — so the dependency-policy
  SSOT stays singular and the mirror cannot drift from it.
- **D-2 — the edge table is audited before the workspace is.** The structural rules (no
  door↔door product edge; nothing depends on the bindings crate; the foundation crate depends on
  nothing internal; a capability crate never depends on a door) are applied to the **declared**
  policy as well as to the observed edges. Adding the forbidden row is therefore not a way to
  make the gate green — the adversarial-bypass path is closed by construction. *Hardened after
  the adversarial review:* the role vocabulary itself is now validated (`ROLE_NAMES` — a typo'd
  role would have matched no structural rule and silently disabled them all; P-8), and internal
  scope is **workspace membership**, not the `repark-` name prefix (a member outside the naming
  convention was previously invisible to the guard; P-9).
- **D-3 — `optional` is a first-class kind.** `cargo metadata` reports a feature-gated
  dependency as a normal edge with `optional = true`; splitting it out lets
  `repark-ta → repark-core` be declared exactly as what it is (feature-tied), while remaining
  subject to the layering rule as the old guard already had it. No previously-checked edge
  became unchecked.
- **D-4 — kinds are scoped where they belong.** All four kinds are inspected against the edge
  table (that is how the dev-only ANSI→Spark crossing is expressible without permitting a
  product edge); only PRODUCT kinds (`normal` / `optional`) are subject to the layering rule,
  exactly as before — a test-only edge is not a layering statement.
- **D-5 — stale rows are drift too.** A policy row whose edge no longer exists reds, but only
  when both endpoints are present in the workspace, so pre-declared future crates stay legal.
- **D-6 — `planned` components are declared, not invented.** They mirror AGENTS.md's `deferred`
  rows. Declaring them is what makes "planned ≠ delivered" a live rule instead of a dead branch
  (docs/testing.md: a branch that cannot change an outcome is a defect), and it turns "someone
  quietly created `crates/repark-exec`" into a red gate.
- **D-7 — the layer agreement is transitive.** manifest `layer` == the crate-DAG tier NAME, and
  the crate-root `map.md` must name the same tier NUMBER. Three artifacts, one fact, one SSOT.
- **D-8 — the ci.yml `guards` job name is deliberately unchanged.** Renaming it would move the
  branch-protection required context and must land in the same change as the protection update
  (task/lessons.md 2026-08-07); the new step is added inside the existing job instead.

## Gate results

| Gate | Result | Evidence |
|---|---|---|
| `make ci` (includes the new `check-manifest`) | PASS (rc=0) | actor run 2026-08-09, cold worktree: clippy `-D warnings` + panic-ban + `cargo check` + the four structure guards + ruff + `uv lock --locked` + taplo + typos |
| `bash scripts/check_crate_dag.sh` | PASS (rc=0) | `crate-dag: 20 internal edges clean (4 dev, 15 normal, 1 optional) across 9 of 9 mapped crates` |
| `bash scripts/check_manifest.sh` | PASS (rc=0) | `manifest: 12 components (9 delivered, 3 planned) agree with the workspace, the gates, the doc index, the status document and the crate maps` |
| `bash scripts/check_map_md.sh` | PASS (rc=0) | plus a working-tree simulation of the same rule over the unit's file set (every touched code/manifest directory's `map.md` is in the change) |
| `make workflows-parse` / `make workflows-lint` | PASS (rc=0) | `workflows-parse: 11 workflows parse cleanly`; zizmor v1.26.1 `No findings to report` |
| ruff (pinned 0.15.22) check + format | PASS (rc=0) | over the whole tree |
| taplo (pinned 0.9.3) format --check + lint | PASS (rc=0) | 22 TOML files, including the new `repo-manifest.toml` |
| typos (pinned 1.47.2) | PASS (rc=0) | |

## Provocation proofs

Per docs/testing.md "Gate provocation proofs": each gate is demonstrated FIRING on a
deliberately-broken tree, the failing run captured verbatim, the tree restored, and the clean run
captured. Provocations are never committed — the working tree returned to the unit's own change
set after each one (`git status --porcelain` verified).

Ten provocations, 2026-08-09 (P-1..P-7 from the unit's first pass; P-8..P-10 prove the
adversarial-review fixes). Each was reverted before the next; after every revert the guard
returned to its green line and `git status --porcelain` returned to exactly this unit's file set
(and the reverted files were confirmed byte-identical to the unit versions).

| # | Gate | Provocation | Observed failure (verbatim, rc=1) | Restored-green |
|---|---|---|---|---|
| P-1 | `check_manifest` — inventory | Added `crates/repark-scratchgate` (a real `Cargo.toml` + `src/lib.rs`) to `[workspace] members` with NO manifest entry | `ERROR: Cargo workspace member repark-scratchgate (crates/repark-scratchgate) is not declared in repo-manifest.toml — add a [components.repark-scratchgate] entry (path, layer, status).` | yes — `manifest: 12 components …`, rc=0 |
| P-2 | `check_crate_dag` — classification (same tree as P-1) | The same undeclared member, seen through `cargo metadata` | `ERROR: repark-scratchgate is not in the tier map (scripts/check_crate_dag.py TIERS) — classify it before it can be depended on.` | yes — `crate-dag: 20 internal edges clean …`, rc=0 |
| P-3 | `check_crate_dag` — undeclared edge | Added `repark-ml.workspace = true` to `crates/repark-functions` `[dependencies]` — a **same-tier `normal` edge** the old layering rule would have waved through | `ERROR: undeclared dependency edge — repark-functions -> repark-ml (kind: normal). Every internal edge must be declared: add it to ALLOWED_EDGES in scripts/check_crate_dag.py with the kind and a reason, or drop the dependency.` | yes — rc=0 |
| P-4 | `check_crate_dag` — kind + door↔door | Promoted `repark-sql`'s `repark-spark` **dev**-dependency to a `[dependencies]` entry (no `Cargo.lock` change) | `ERROR: forbidden edge — repark-sql -> repark-spark (kind: normal): no door -> door product edge, ever …` **and** `ERROR: dependency kind not permitted — repark-sql -> repark-spark (kind: normal); the policy allows this edge only as: dev. Reason on file: DEV-ONLY, …` | yes — rc=0 |
| P-5 | `check_crate_dag` — **declaration audit** (the adversarial-bypass case) | Left the workspace clean and instead edited the policy: `("repark-sql", "repark-spark")` kinds `{"dev"}` → `{"dev", "normal"}` | `ERROR: the policy DECLARES a forbidden edge — repark-sql -> repark-spark (kind: normal): no door -> door product edge, ever …` | yes — rc=0 |
| P-6 | `check_manifest` — planned ≠ delivered, **both directions** | (a) flipped `[components.repark-exec]` to `status = "delivered"` (+ a layer) while no code exists; (b) created `crates/repark-io/src/lib.rs` while it is declared `planned` | (a) `ERROR: [components.repark-exec] is declared delivered, but crates/repark-exec/Cargo.toml does not exist — a delivered component is one that is actually there.` + not-a-workspace-member + not-covered-by-the-dependency-policy + `has no map.md at crates/repark-exec/map.md … (write it; this guard never generates one)`; (b) `ERROR: [components.repark-io] is declared planned, but crates/repark-io exists — code that has arrived is delivered; flip the status (and declare its layer).` | yes — rc=0 both |
| P-7 | `check_manifest` — STATUS.md / gates / docs / maps | (a) `phase = "milestone two"`; (b) `canonical = "make ci-fast"` + `architecture = "docs/history/ARCHITECTURE.md"`; (c) removed the tier wording from `crates/repark-ml/map.md` | (a) `ERROR: [project] claims phase 'milestone two' / phase_state 'complete', but STATUS.md's ## Current milestone section does not state 'milestone two is complete' — one of the two is stale (STATUS.md is the status SSOT).`; (b) `ERROR: [project.gates] canonical names ci-fast, which the Makefile does not define — the command is dead.` + `ERROR: [documentation] architecture points at docs/history/ARCHITECTURE.md, which does not exist — the document moved or was archived without updating the index.`; (c) `ERROR: crates/repark-ml/map.md never names its layer — [components.repark-ml] declares 'surface crates', which is tier 3 (surface crates); say so in the map …` | yes — rc=0 after each |

| P-8 | `check_crate_dag` — **role-vocabulary audit** (review B-1) | Typo'd `ROLES`: `"repark-spark": "door"` → `"doors"` — an unknown role matches no structural rule, so before the fix this one-character diff (plus a widened policy row) let a real door→door `normal` edge through every gate | `ERROR: repark-spark has unrecognized role 'doors' (scripts/check_crate_dag.py ROLES) — roles are: bindings, capability, door, engine, foundation, table service. An unknown role matches no structural rule and would silently disable them all.` | yes — rc=0 |
| P-9 | `check_crate_dag` — **membership scope** (review B-2) | Added `crates/sneaky` (no `repark-` prefix) as a real workspace member with a `normal` dep on `repark-spark` — before the fix a member outside the naming convention had NO edges inspected, in either direction | `ERROR: sneaky is not in the tier map (scripts/check_crate_dag.py TIERS) — classify it before it can be depended on.` | yes — rc=0 |
| P-10 | `check_manifest` — `[project]` type check (review N1) | `phase = 1` (a dropped quote pair) — before the fix a non-string field silently skipped the whole STATUS.md agreement rule while the success line still claimed agreement | ``ERROR: [project] `phase` must be a non-empty string (found 1).`` | yes — rc=0 |

**Must-PASS side:** the clean tree is green on every gate (see "Gate results"), and `make ci`
exits 0 with `check-manifest` in the chain. **Provocation-identifier sweep:** `scratchgate`,
`ci-fast`, `milestone two`, `the third layer`, `sneaky` — 0 hits in the tree outside this
ledger's own provocation records.

## Adversarial review

The unit's adversarial pass returned REWORK with two demonstrated bypasses — both defeats of the
exact property the unit's own prose asserts — closed in this same unit and proven by new
provocations:

- **B-1 — role typo.** `ROLES` values were never validated; `forbidden_reason` reads them with a
  default, so an unrecognized role matched no rule and returned permitted — silently. A
  one-character table typo therefore disabled every structural rule *including the declaration
  audit*. Closed: a `ROLE_NAMES` vocabulary + `audit_policy` checks over `ROLES` values (and,
  symmetrically, `TIERS` numbers vs `TIER_NAMES`). Proof: P-8.
- **B-2 — name-prefix scope.** "Internal" was spelled as the `repark-` name prefix, so a genuine
  workspace member named otherwise was unpoliced in both directions. Closed: internal = the
  `cargo metadata --no-deps` package set (workspace membership); the prefix survives only as
  belt-and-braces on the target side, so a third-party crate carrying the family name still reds
  as unclassified rather than being skipped. Proof: P-9.
- **Nits applied:** `[project]` string-field type checks (P-10); glob workspace members filtered
  to directories (a glob member would have tripped over `crates/map.md`); the map rule's prose
  and error text now say **tier**, matching what it actually checks; deliberate tier clauses in
  the two crate maps that previously matched only incidentally; the debug tables quote the
  guards' real `ERROR:` lines; substring-agreement limits documented in the manifest comment;
  ledger wording fixes.

## Testing convention (why there are no unit tests for the validator)

This repository's convention for **mechanical gate scripts** is provocation proofs, not pytest:
`check_crate_dag.py`, `check_lib_rs.py`, `check_lib_py.py` and `check_workflows_parse.py` carry
no unit tests, and `docs/testing.md` "Gate provocation proofs" states the rule directly — a new
gate is proven by introducing the violating change, capturing the failing run verbatim, reverting,
and capturing the clean run, recorded in the unit ledger and never committed. The pytest suites in
`python/repark-parity/tests` cover the parity/census **package**, not `scripts/`. FD-3 therefore
lands its test surface as the table above plus the CI/Makefile/pre-commit wiring — the same shape
`p1a-workspace-arming-ledger.md` used when these gates were first armed.

## Deviations / open items

- **O-1 — `planned` vs `deferred` vocabulary.** The manifest uses the slate's word (`planned`);
  AGENTS.md and STATUS.md say `deferred` for the same three homes. The manifest comment ties them
  together. Renaming the status value to `deferred` is a one-line change in
  `check_manifest.py` + the manifest if the reviewer prefers a single word.
- **O-2 — the ci.yml `guards` job keeps its name** (D-8). If the reviewer wants the name to list
  the new guard, the branch-protection required context must move in the same change
  (task/lessons.md 2026-08-07).
- **O-3 — `make preflight`'s security leg was not run by the actor.** `make audit` installs
  pinned `cargo-audit` / `cargo-deny` and resolves advisories from the network; nothing in this
  unit touches Rust dependencies. `make ci`, `make workflows-parse` and `make workflows-lint`
  (zizmor, the rest of preflight) were run and are green.
- **R-1 — lessons rider.** This unit also appends the 2026-08-09 `task/lessons.md` entry
  (the `Authored-By` trailer names the model actually running, read from the live session at
  commit time — never a role constant): an incident record from FD-1's merged squash, folded
  into the next PR per the forward-only rule rather than shipped as its own commit.

# lessons

DO / DO-NOT rules in force. Append date-stamped entries; supersede, don't delete. Seeded
2026-08-06 with sanitized lessons carried from the private v1 repository — these were learned
there the hard way and bind here from day one.

## 2026-08-06 — carried from v1

- **DO land tests in the same commit/PR as the code they test — hard block.** "Tests later"
  never happens; a behavior change without its tests is reverted, not patched. Full contract:
  [../docs/testing.md](../docs/testing.md).
- **DO NOT run `cargo test --all-features` — ever.** It enables the PyO3 cdylib's
  `extension-module` feature, which tells PyO3 not to link libpython and breaks the standalone
  test binary. The invocation is `cargo test --workspace`. The ban applies from phase 3 onward
  mechanically, but the string must never appear as a recommended invocation in any doc, Makefile,
  or workflow at any phase.
- **DO NOT merge a Dependabot cargo PR that bundles a safe bump with a DataFusion-family major
  bump.** Observed in v1: a harmless dependency bump paired in one PR with a datafusion/arrow
  major that broke the pinned iceberg family. Always split: take the safe bump alone; treat any
  DF/arrow/iceberg bump as a deliberate, together-with-the-family repin.
- **DO update every touched directory's `map.md` in the same change as the code — lockstep, no
  exceptions.** A change is not done until the maps reflect it; `scripts/check_map_md.sh` is the
  pre-commit oracle. New directory → new `map.md` in the same change.
- **DO end every commit message with exactly
  `Authored-By: Claude (claude-fable-5) <noreply@anthropic.com>` — and nothing else.** No
  co-author trailers, no session identifiers or links in commits or PR bodies.
- **DO NOT trust checkboxes as ground truth when scoping work.** v1's ledgers repeatedly carried
  stale `[ ]` boxes for shipped work; grep the source and git history before scoping a unit.

## 2026-08-07 — phase 1 (engine core)

- **DO update branch-protection required contexts in the same change as a CI job rename.**
  PR-A renamed the guards job; protection still required the old name, so PR #3 sat BLOCKED
  with every visible check green. A job rename is not done until
  `gh api repos/{owner}/{repo}/branches/main/protection/required_status_checks` lists the new
  name (the required-contexts list matches on the job's display name).
- **DO NOT make a path-filtered workflow a required status check.** A required check whose
  workflow trigger carries a `paths:` filter never runs on non-matching PRs — GitHub reports
  the PR permanently BLOCKED with all other checks green. Observed TWICE in one day: #6
  (zizmor, filtered to `.github/workflows/**`), then the close-out PR #7 itself (cargo-deny +
  taplo, filtered to Cargo/TOML paths — every prior PR had happened to touch a `.toml`, so a
  docs-only diff was the first to expose them). Required workflows must be always-run on
  `pull_request`; keep path filters for record-keeping triggers (`push`) only, and for
  non-required workflows (`audit.yml` is the correct pattern: path-filtered AND not required).
- **DO retarget a stacked PR's dependent BEFORE merging its base and deleting the branch.**
  When `phase-1/pr-b` was deleted at #4's merge, GitHub auto-closed dependent PR #5; a closed
  PR whose base branch is gone can be neither retargeted (HTTP 422) nor reopened — the only
  recovery is a fresh PR (#6). Order of operations for a stack: change the child's base to
  `main` first, then merge the parent.

## 2026-08-08 — phase 2 (the two SQL doors)

- **DO probe absence claims empirically — the matrix audit proves an absence is RECORDED, not
  that it is TRUE.** A `DeliberatelyAbsent` row over a delegating router can be false: the
  ANSI door's INSERT/DELETE/UPDATE rows claimed M2 absence while the delegate path was
  live-writing Iceberg, untested and unguarded. The verify panel found it only by executing
  the statements. A row's truth is a behavior; behaviors get tests — refusal rows included.
- **DO release ephemeral providers on every `?` / `return` path.** `FOR … AS OF` pinned temp
  views leaked into the session catalog and surfaced in `information_schema` — the exact
  introspection surface the same PR enabled. Deregister-after-use (the insert_overwrite
  idiom), routed so early returns and `?` can't skip it. *(Corrected 2026-08-11, H-1b: the
  original wording "every exit path" overstated the mechanism — unwind and future-drop are not
  covered, deliberately (no `Drop`); and the Spark door's copy of the leak is now FIXED by
  H-1b with pins, not declared — a fixed defect gets no registry row. See
  `docs/history/hardening-h1/h1b-ledger.md`.)*
- **DO pick the sync recipe by branch relationship.** Stacked branch after a squash-merge:
  verify the squash tree equals the prior branch head (empty diff), then blanket `--ours` is
  provably correct. SIBLING branches: union-merge by hand, then re-check semantic riders (a
  matrix row whose truth changed when the sibling landed — `TA_FUNCTIONS` — and the absence
  pins/counts), then full re-gate. Blanket `--ours` on a sibling merge silently discards the
  sibling's work.

## 2026-08-08 — phase 3 (Python facade + parity = milestone one)

- **DO stop-and-report when a ported test reds; never defer or silently fix it.** The
  byte-flat census gate's whole value is exposing regressions on arrival. A facade test red on
  arrival (`datafusion.runtime.memory_limit`) traced to a real phase-2 engine bug (the config
  sweep vs a facade-owned pseudo-key), not port infidelity — deferring it would have been a
  gate hole, silently fixing it would have hidden the bug. Root-cause, fix at the source with a
  named test, prove through the real artifact, foot the census arithmetic exactly.
- **DO generate deferral decisions empirically, by where the exception is raised.** A by-file
  deferral list is wrong in both directions: most offline JDBC-options tests refuse at the
  FACADE (they port and PASS); the pg catalog-registration test defers at the engine's
  NotImplemented. Run the candidates against a built wheel and adjudicate per node. Over- and
  under-deferral are both invisible to `ported ∪ deferred = pin` — the ledger must match reality.
- **DO give the census a mirror ADDITIONS ledger.** `ported ∪ deferred = pin` breaks the moment
  a v2-only test lands (a new capability the pin has no equivalent for — e.g. a public-repo AWS
  workflow's guard). The identity is `(v2_collected − added) ∪ deferred = pin_collected`; the
  additions ledger subtracts from the CANDIDATE side, the exact mirror of deferred. Any new test
  in the facade tree perturbs the collect-only baseline — track it, don't hand-wave it.
- **DO redact evidence artifacts THROUGH each format's parser, never sed.** A naive path
  substitution ate escaped quotes in traceback-bearing JSON (677 sites → invalid JSON) and
  injected `<token>` angle-brackets into JUnit XML (unparsable). `compat.redact` loads JSON as
  JSON and XML as XML, rewrites string values, re-asserts validity before writing. The comparator
  refusing its own corrupt baseline is the instrument working.
- **DO route every AWS/OIDC workflow through an adversarial SECURITY lens before it ships.** A
  net-new credentialed workflow had four HIGH exposures at once: credentials minted before the
  build steps (supply-chain payload runs with a live session), scrubbed-placeholder buckets that
  would sign requests to squattable global names, an OIDC trust sub that could never match (one
  sub per run; immutable subject format), and branch binding that lived only in an in-file guard
  the attacker would be editing. Mint credentials LAST; make never-teardown a PERMISSIONS fact;
  bind the branch at the environment's deployment policy, not the IAM sub or the workflow file.
- **DO measure the hygiene content pass on ADDED lines only.** A forward-scrub commit's removed
  lines legitimately contain the old forbidden literals; a whole-diff grep makes any scrub
  unpushable (and the local pre-push hook too). `git log -p | grep '^+'` for content; full
  metadata for messages/identity.

## 2026-08-09 — front-door campaign

- **DO name the model actually running in the `Authored-By` trailer — read it from the live
  session environment at commit time, never from a role assignment or a remembered constant.**
  Supersedes the 2026-08-06 entry on the model name only; the shape — `Authored-By: Claude
  (<model>) <noreply@anthropic.com>`, nothing else, no session identifiers — stands. Cause: a
  session's runtime model can change mid-session without ceremony, and a trailer stamped from a
  role's constant keeps asserting the old name; the FD-1 squash (`4cc6bf7`, #24) is misattributed
  `claude-fable-5` for work committed under `claude-opus-4-8`. Merged history stays as-is
  (forward-only); this rule prevents the recurrence.

## 2026-08-10 — front-door close-out

The campaign retrospective's learning pass
([../docs/history/frontdoor/retrospective.md](../docs/history/frontdoor/retrospective.md) §6), plus
one rule its promotion check (§8) had to rescue before the campaign's slate was archived to
[../docs/history/frontdoor/](../docs/history/frontdoor/map.md).

- **DO set a repo-local git identity in every worktree before the first commit — never rely on the
  global one.** *(Promotion — this rule lived only in the campaign slate.)* This repository is
  public, so the author identity on every commit is published with it; a machine-global identity is
  exactly the personal identifier the public-hygiene greps exist to keep out
  ([../briefs/map.md](../briefs/map.md) "Import gate" enumerates the class). It pairs with the two
  hygiene passes already in force — added-lines content (2026-08-08) and commit metadata, which is
  what reads the identity (2026-08-09).
- **DO verify the attribution trailer on the *merge* commit, not only on the branch commits.**
  Extends (does not supersede) the 2026-08-09 entry: that rule fixes *which name* the trailer
  carries; it does not make the trailer survive the squash. Three squashes have now landed with
  **empty commit bodies** (#25, #26, #30) — the branch-side commits correctly stamped, the merge
  step dropping them — all three *after* the attribution problem was already known and under
  attention. Prose does not protect this surface: no unit's own gate can see a squash the unit does
  not perform. *Detector owed:* a check over `main`'s commit messages (a `push: main` job, or a
  `make` target run at campaign close-out). While the squash stays owner-manual, that check detects
  and never prevents — a stated residual risk, not an oversight.
- **DO NOT scope a consistency sweep to the files the unit is editing.** A gate's population is
  whatever the gate's own wording declares. A front-door unit's acceptance gate named a grep over
  `*.md docs/ crates/**/map.md`; the sweep ran over the unit's change set instead, and
  `CONTRIBUTING.md` — a root `*.md`, linked from the README's own Status section, and the first
  document an outside contributor reads — went on describing a finished port as in progress. Proven
  twice: re-running the same sweep over the declared population at close-out found twice as many
  stale sites as the known list. If a file is out of scope for *editing*, it is not out of scope for
  *checking* — a hit outside the lane is a finding to route, not a hit to skip.
- **DO give every unit a unit ledger, not only the units that ship mechanical gates.** One of five
  units filed the `task/<unit>-ledger.md` the SEPMO binding manifest names as the active-plan home;
  the other four existed only as PR bodies, so the retrospective had to reconstruct severities and
  cycle counts from prose and could not recover one unit's finding count at all. A PR body is not an
  addressable artifact: it is not in the tree, no gate reads it, and it cannot be corrected forward
  without rewriting a merged PR's narrative.
- **DO re-verify a cross-reference's *premise* against the tree — not just its target — when
  sibling units have landed since the citation was drafted.** A unit ledger deliberately wrote
  non-links, stating that the campaign's brief and design were not yet in the repository and would
  arrive with the closing archival; both had landed two units earlier. A link checker cannot catch
  this, because a deliberate non-link is not a broken link. The failure mode is specific to
  sequenced work: the citation was written against the plan rather than against the tree at the
  moment it landed.
- **DO attack a mechanical gate's lookup tables, not only its rules.** Both demonstrated bypasses of
  the new structural gate were in its *lookup* layer: an unvalidated role vocabulary, and an
  unvalidated definition of "internal". A rule that reads a hand-maintained table with a default
  silently returns *permitted* for every key the table does not know — so a one-character typo
  disables the rule set while the gate still prints its green line. Every gate that keys behavior
  off a hand-maintained table must validate that table's key space as one of its own rules.
  *Detector:* landed for that gate (the role/tier vocabularies plus workspace-membership scope,
  proven by its P-8/P-9 provocations); this entry is the standing rule for the next one.

## 2026-08-10 — tier-2 live bring-up

- **DO trace the full consumer chain when fixing environment provisioning — a sync fix alone can
  be a no-op.** The parity-live target's `uv sync` was corrected to provision the four facade
  extras, but the very next line's `uv run` re-syncs the project environment by default and would
  have stripped them straight back out; `uv run` also has no `--no-install-package` escape, so
  `--no-sync` on the consumer is the only way to keep an explicitly provisioned environment
  intact. A provisioning fix is not done until every downstream invocation in the same recipe is
  audited for implicit re-provisioning.
- **DO fail loud when adopting pre-existing cloud state; never let IAM be the only stop.** The
  acceptance harness's idempotent namespace-create silently adopted a stale Glue namespace whose
  location pointed at a different warehouse bucket, and table writes followed it — the create-only
  role's missing grant was the sole thing that prevented a cross-bucket write. Adopted state must
  be verified against the configured intent (here: the namespace location falls under the
  configured warehouse) with a loud mismatch error; the guard unit is filed with the hardening
  campaign. IAM is defence in depth, not the design.
- **DO NOT let a "mirrors X step for step" claim live only in a comment — assert it mechanically
  or name it as reviewer burden in both halves.** The parity-live workflow's mirror claim was
  true at porting time and silently false for two green nightlies once the environments' sync
  behavior mattered; nothing red because nothing checked. Until a dual-wire checker exists (a
  candidate mechanical gate: compare the Makefile target's uv/maturin/pytest flag sets against
  the workflow's steps), every dual-wired pair must carry the keep-identical instruction in BOTH
  files, and a change to either half is not done until the other half is diffed by hand.

## 2026-08-23 — DL-1 (the ledger lifecycle)

- **DO give a review or verification agent its own scratch clone of the worktree, never the
  worktree itself** — and say so in the prompt with the clone path, not with "read-only". A
  DL-1 reviewer told "read-only, never run archive against this tree" still branched, committed
  WIP, ran the migration in the tree and pushed a stash onto the shared stack; the tree looked
  untouched and only the reflog said otherwise; another removed the `origin` remote from the
  SHARED `.git` (every `refs/remotes/origin/*` with it), found only when the pre-push hook's
  scope ballooned to 253 commits. `git clone --no-hardlinks <worktree> <scratch>` costs seconds
  and makes the instruction unnecessary. After any fan-out: `git reflog -5`, `git branch`,
  `git stash list`, `git remote -v`, `git status` — before trusting the tree.
- **DO dry-run a repository-wide mechanical rewrite on a clone and diff the maps before the real
  run.** The review caught a row-splitting defect only by running the real input; the unit tests
  on a synthetic fixture were green.
- **DO NOT file ledgers by hand.** `make ledger-archive` at pickup, `ledger_lifecycle.py move`
  at departure; the directory is the status and the script keeps every link true.

## 2026-08-27 — comment provenance correction

- **DO preserve `Model:` provenance comments during comment compaction.** Remove
  `CodeQuality:` grade tags independently; model provenance and quality grades are different
  metadata classes.

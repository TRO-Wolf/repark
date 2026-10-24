# lessons

DO / DO-NOT rules in force. Append date-stamped entries; supersede, don't delete. Seeded
2026-08-06 with sanitized lessons carried from the private v1 repository — these were learned
there the hard way and bind here from day one.

## 2026-08-31 — rewrite_data_files where-scope pin

- **DO bind a filtered rewrite's surviving files to the excluded partition's pre-image paths.**
  Counting N byte-identical survivors is partition-symmetric: flipping `=` to `!=` still leaves
  five files and stays green. Assert the kept paths **are** the recorded part=1 set and the
  rewritten-away paths **are** the part=0 set.

## 2026-08-27 — PR-247 ledger pins

- **DO run the ledger-grammar gate before claiming a completed ledger is pinned.** Evidence prose
  that names tests is not a citation; every `PROVEN` clause needs a matching `pins:` marker.

## 2026-08-27 — PR-245 post-merge integration

- **DO make lifecycle pins accept staging, completed, and archived ledger homes.** Pickup must
  archive merged ledgers, so a test that searches only live bins makes the required transition red.

## 2026-08-26 — PR-245 remediation

- **DO preserve original SQL locations across front-door canonicalization.** A value-preserving
  rewrite can change text length, so downstream parser locations need an explicit source map.
- **DO express enumerable syntax guards with the AST.** Text patterns miss equivalent constant
  spellings such as `chr(39)` and invite claims broader than the detector can prove.
- **DO map copied canonical characters to their exact original locations.** Synthetic literal text
  can expand or shrink, so later parser coordinates must traverse the canonical source map.
- **DO define bounded constant-expression syntax explicitly.** A `chr` guard that accepts integer
  literals but not safe arithmetic leaves equivalent quote spellings outside its stated domain.
- **DO preserve each error-tree child independently.** An aliased mixed collection must not make a
  translatable parser sibling depend on whether an unrelated leaf can be cloned.
- **DO budget constant evaluation before string allocation.** Depth and value limits do not prevent
  recursive concat or repetition from overflowing the stack or allocating oversized intermediates.
- **DO prove reachable error shapes before cloning an error enum.** Translate at the typed parser
  boundary and leave unrelated planning or execution trees unchanged.
- **DO catch parser resource exhaustion at a file gate's boundary.** Valid hostile source can raise
  `RecursionError` or `MemoryError`; a gate must fail once without leaking a traceback.
- **DO exercise every reachable branch of a typed parser boundary.** DataFusion returns direct SQL
  errors for sqlparser failures and `Diagnostic(SQL)` for its native expected-token failures.
- **DO compare rewrite bytes, not `Cow` ownership.** An owned buffer can preserve SQL exactly;
  source-location translation stops only when a secondary rewrite changes the bytes.

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

## 2026-08-25 — PROC-1 (review effort by tier)

- **DO adjust the amount of process to the risk, never the bar.** Over the last four engine units
  the defects were caught by exactly two instruments: the Critic's *novel-input fresh execution*
  through a public entry point (V3R-1's three S1s — SEC-001/002/003) and the *gates* (the DL-5
  review's two red pins, found by running the parity suite). The four-phase CCC fan-out, the
  per-pin mutation probe, the separate claims phase and the per-unit retrospective metrics produced
  record entries, not defects. Meanwhile a STANDARD unit's process read had grown to ~190 kB before
  the engineering method. Ruling home: the manifest's
  [`review_profile` and `critic_engine`](../.agents/skills/sepmo/binding-manifest.md) rows.
- **DO NOT read "more Critic phases" as "more assurance."** The measured signal came from fresh
  execution through the public door and from the gates, not from the count of Critic spawns or
  probes. Add a phase when a class of defect escaped it, not by default; the count of phases is a
  cost to tier, and the escaped-defect record is the only evidence that raises it.

## 2026-08-27 — comment provenance correction

- **DO preserve `Model:` provenance comments during comment compaction.** Remove
  `CodeQuality:` grade tags independently; model provenance and quality grades are different
  metadata classes.

## 2026-09-07 — code-comment ban applies to all models

- **DO apply the existing code-comment ban to every model, including OpenAI models.** The owner
  extended the ban when resuming the parked Spark units. Preserve existing comments and required
  documentation; put new rationale in Markdown.

## 2026-09-23 — prompt audit: tier policy in the spine, delegated gate = scoped local + CI

- **DO keep the tier policy in AGENTS.md "Delegated work"** (approved executor tiers; no Sonnet or
  Haiku in any role — owner, 2026-09-20; Opus only on a named request). CLAUDE.md keeps launch
  mechanics only, so the adapter never carries a project rule the spine lacks.
- **DO gate a delegated unit with the scoped local gate and CI's required checks** (owner,
  2026-09-18); the SEPMO `green_commands` row records the whole-workspace roster as the CI-only
  exception with its residual gap. `make verify` / `make preflight` remain the developer's roster.
- **DON'T write an instruction as a diff against its previous version** ("now", "replaces",
  "unchanged"), pin a model id where a tier name will do, or restate an enforced rule five times;
  a literal reader reconciles every copy on every turn.

## 2026-09-23 — DESCRIBE-COLUMN-1 gate correction

- **DO run the named Makefile Rust gates.** `make rust-clippy` permits test-only unwrap and
  expect calls; raw all-targets Clippy is not this unit's lint gate.

## 2026-10-12 — proportionality: failed remediation cycles against one finding

- **DO count failed remediation cycles against one finding.** A cycle has failed when a fresh
  verifier reproduces the same class of finding on the folded head, whichever statement
  shows it. After the second failed cycle, stop folding path by path: bring the owner three
  options (redesign with a design note, narrow the PR, park) with the spend so far. A third
  fold on the same finding needs the owner's word.
- **DON'T answer a finding class with a longer list of spellings.** Three verifiers failed
  one PR (ICE-NTZ-NESTED-WALL-1) on one class, a read side and a store side that decide
  separately on every path nobody enumerated; each fold closed the named paths and the next
  verifier found new ones. Guard at the one chokepoint or stop.

## 2026-10-13 — parked rounds: provisional anchors, round counts, integers, census cells

- **DO ship every provisional anchor with a revert-alone-stays-green test.** A provisional
  decision without a proven rollback is not provisional. Case: #1042's kernel anchor;
  reverting its commit alone left 2 red tests.
- **DO count verifier rounds against the finding class, not the PR.** The proportionality
  rule counts failed remediation cycles against one finding. Case: R-017 took seven rounds
  across two PRs (#1027 verify 1–3, #1042 verify-slice1, slice1b, slice1c, verify-narrow).
- **DO refuse at sketch a guard that infers a unit from a value's magnitude or equality.**
  An integer cannot carry a unit. Case: #1047's integer rule refused a correct microsecond
  integer equal to a nanosecond frame tick, and its hint then stored a wrong value.
- **DON'T let the fold assign a census cell's expected category.** A census cell's expected
  category is derived by the harness from the design's rule, never assigned by the fold.
  Case: #1042's 120 cells whose ledger category differed from what the doors stored.
- **DO paste the test count for a claimed clean revert.** A fold that claims a clean revert
  commits the revert on a throwaway branch and pastes the test count.

## 2026-10-17 — Opus means Opus 5.5; Opus 5.0 is banned forever

- **DO dispatch every Opus role on `claude-opus-5-5` and verify the id from the agent transcript, never from the brief.** *Why:* the owner asked to confirm a running fork agent was 5.5 after a report typo said 5.1; the transcript proved 5.5, and the owner made the ban permanent so no dispatch can drift. Held by the ruling block in `AGENTS.md`/`CLAUDE.md` (byte-preserved by `make check-owner-ruling`) and by `make check-model-ban`.

## 2026-10-19 — facade test organisation: the 2026-10-03 clause 1 rewritten

- **DO organise the facade suite; DON'T change what it asserts.** Clause 1 of the 2026-10-03
  test-speed ruling ("no test-migration unit and no fixture-consolidation unit") is superseded
  by the owner's override of 2026-10-19. What stands: a test file may move between directories,
  a battery may split into one isolated interpreter per shape, and a new test may use the shared
  `spark` / `spark_with_warehouse` fixtures in `python/repark/tests/conftest.py`, when the PR
  proves the collect-only node-id set is identical after the path prefix, the passed / skipped /
  xfailed counts hold, and no assert line, parameter or error class changes. Existing local
  fixtures are not rewritten. No test moves between languages, none is deleted or skipped, no
  bound loosens, and `test_perf_ice_catalog_io_1.py` moves only whole (clause 4 stands). Cases:
  #1112 (scheduler), #1114 (config, fixtures, `slow` tier), #1113 (thirteen subsystem
  directories, 20,704 node ids unchanged), #1115 (deep batteries, 398 s → 307 s at `-n 8`).
- **DO put a new facade test in its subsystem directory** ([docs/testing.md](../docs/testing.md)
  "Facade suite layout"); the suite root holds only `conftest.py`, the shared helpers and
  `fixtures/`.
- **DO hand-merge `origin/main` after each squash merge of a stacked chain.** A squash drops the
  shared history, so the update-branch API answers 422 for every later branch of the stack. A
  local merge (no rebase, no force) is the fix; the lockstep hook's staged-mode warnings on that
  merge commit are expected, and the base-mode check in CI is the gate.
- **DO measure the test before calling its wall the product's.** The 20,000-term OR battery
  spends under 0.1 s in the refusal and the rest building the `Column` with `|` on the Python
  door, which grows quadratically ([docs/perf/column-or-build-cost-2026-10-19.md](../docs/perf/column-or-build-cost-2026-10-19.md)).
  A suite floor is a product finding only once the phase that owns it is named.

## 2026-10-22 — product fix rounds come from missed doors; census first, spine next

- **DO open every write-path unit with a step-0 door-by-shape census measured against Spark before
  the build.** *Why:* the product fix rounds of October (U-8 four, U-5 two, the seam's two halts)
  were each a verifier finding a door or a shape the builder's copy of a rule had missed; the units
  whose questions were ruled ahead (pin K, EXPLAIN-DML-WRITE-1) had zero or one. The verifier's
  matrix becomes the builder's input. U-11's order is the first to carry it; the work-order
  template names it.
- **DO end every hand-back with "doors I did not touch, and why".** *Why:* the omission is written
  down before a verifier finds it, and the orchestrator can card it the same day.
- **DO treat a rule that needs more than one site as an architecture finding, not a bigger unit.**
  *Why:* ruling ZS-9 found the nested NTZ store at three sites in a crate that cannot call the
  seam's helper; the seam landed with a refusal and the structural fix is carded as WRITE-SPINE-1
  ([task/roadmap/mid-term/write-spine-1-card-2026-10-22.md](roadmap/mid-term/write-spine-1-card-2026-10-22.md)),
  the architecture unit after GATE-RETIRE-1, on the owner's word of 2026-10-22.


## 2026-10-22 — two Opus 5.5 lanes at a time; re-evaluate by 2026-11-22

- **DO run at most two Opus 5.5 builds concurrently, not one.** *Why:* the one-lane rule of
  2026-09-26 came from the September usage spikes (eight Opus executors live at once), and the
  10-20 plan kept one lane only because the 10-17 halt was lifted for one; the closeout dates then
  slipped a day for every day that lane ran long. The owner relaxed it on 2026-10-22 ("allow for
  2 lanes at a time for sure"). Both lanes keep the standing limits: `claude-opus-5-5` only, one
  Rust suite per clone, inside the 26-core cap, timed measurements on a quiet box only, verifiers
  and read-only evaluations count toward the two unless the owner says otherwise. Lane 2 opened
  the same day with ICE-INTEGRITY-PINS-1 pins U and X beside the ZONE-SEAM-1 fix round.
- **DO re-evaluate the lane count by 2026-11-22.** *Why:* the owner asked for a re-evaluation
  within a month. Inputs: usage against the owner's ceiling, box contention (cost-gate failures or
  load-induced test timeouts, as the seam verifier saw on 10-22), and whether the v2 and v3 label
  dates moved. The CLAUDE.md capability-tier section is unchanged: Opus still only on the owner's
  explicit word, which this ruling is.

## 2026-10-22 — CI re-runs: a stall may be re-run once, a failed test waits for the owner

- **DO re-run once, yourself, a CI job that was cancelled at its time cap with every test step
  green up to the stall.** *Why:* a re-run is one `gh run rerun --failed` and needs no agent; the
  stall is the hang class the FAIR-POOL-HANG-1 card tracks, so the stalled log is kept under the
  unit's scratch and the card gains one dated line. Owner, 2026-10-22 ("adopt"), after a builder
  re-ran a hung facade shard on its own.
- **DO NOT re-run a job with a failed test without the owner's dated word, and never from an
  agent.** *Why:* a re-run can hide a real flake behind a green check; the orchestrator reads the
  failed job's log first and reports the test and the assertion, as it did for #1158's wall-clock
  pin the same evening.

## 2026-10-23 — nanosecond rules live in the plan-time conform stage, not in another door gate

- **DO treat every nanosecond write defect as a missed feed of one rule, not as a new unit.** *Why:*
  R-009, R-017, R-019, R-020, R-024 and the ZONE-SEAM-1 round-2 S1-3 are the same shape: Spark's
  nanosecond floor or the nanosecond-leaf refusal copied per door, and a feed shape one copy missed
  (the partitioned insert, the encoded source, the pandas frame). The owner ruled on 2026-10-23
  ("rule it"): the floor and the refusal are rules of the WRITE-SPINE-1 conform stage beside the zone
  rule; no nanosecond fix unit opens before the spine; U-2 and R-024 stay in 1.5.7; the nanosecond
  leaf keeps its refusal on every door until then (S1-3: keep refusing).
- **DO keep Spark semantics out of the fork, and put one fail-closed invariant in it.** *Why:* the
  format crate receives plans already in table types (the Sail reading, crate-boundary evaluation of
  2026-10-22); the one thing the fork can own is refusing an Arrow unit that is not the column's unit
  at the record-batch writer, carded as FORK-NS-UNIT-FAIL-CLOSED-1.

## 2026-10-23 — two verifiers on every product PR: Opus and Muse MAX, merge on the union

- **DO run a Muse 1.3 MAX verifier beside the Opus verifier on every product PR, from the same
  brief and the same head, and merge only when both PASS with no S1.** *Why:* the calibration of
  2026-10-22 ran Muse MAX against the ZONE-SEAM-1 round-1 brief at the head where Opus had found two
  S1 classes. Muse found half of one class and missed the other, then found two silent wrong values
  in the plain-UPDATE exemption that two Opus rounds had walked past; the orchestrator reproduced
  both against live Spark. Neither verifier alone was sufficient; together they cost nothing extra
  on the Claude meter. Owner, 2026-10-23 ("Standing").
- **DO verify a verifier's finding before it becomes a fix round.** *Why:* cheaper critics have
  fabricated paths before (the 2026-09-27 cases); the orchestrator checks every cited path and
  reproduces at least one value with the finder's own driver, as it did for the exemption hole.
- **DO NOT route this repository through the Codex CLI's Sol models.** *Why:* two 2026-10-22 runs
  of GPT-6.1 Sol at xhigh ended in OpenAI's safety filter on the repository's own content, once on
  a rephrased brief; no hand-back either time.

## 2026-10-24 — a streamed loop is not a hang; a run owns its tables; Spark's catalog class is Spark's

- **DO stream per-item progress and per-item error text before reading a timeout as a hang.** *Why:* three days of
  native stack samples on the cross-read step were read as stalls in RePark collection and PyArrow metadata I/O. The
  first nightly with per-table lines (run 40079497082) showed a loop advancing at a 2.2 s median through 2288 Glue
  tables under a 600 s deadline, and 53 identical S3 Tables errors that the end-of-loop summary had never printed. The
  samples were snapshots of a working loop. Run 3's pre-streaming stall keeps its own row because it has no such lines.
- **DO make every aws-acceptance run self-contained: its own table prefix, cross-read of exactly its own tables,
  cleanup with `if: always()`, an age-bounded backstop.** *Why:* the scratch namespaces grew to 2288 and 1748 tables
  because the role had no delete by design and the dbt leg writes a UUID family per run; a namespace walk can never fit
  a fixed budget that way, and leg U grew 31 → 51 min over three runs while the namespace grew. Owner, 2026-10-24
  ("make it as if we were starting from scratch each time and then removing our work when done"). The scoped delete
  grant (`testing_*` tables in the one scratch namespace, never a database, namespace or bucket) reverses OD-3's
  never-teardown for exactly that surface; the IAM edit and the one-time prune of today's tables are the owner's
  actions and no agent-built step performs the first sweep on its own (ruling 76(a) holds).
- **DO build once and share the wheel; give each leg its own job, cap and credential.** *Why:* the compile is 77 % of a
  32-minute setup; the environment has no per-job approval; the repository is public, so parallel minutes cost
  nothing; a leg that starts late in a sequential job inherits the tail of everyone else's time. Owner, 2026-10-24
  ("let's adopt that"). Card: [AWS-EPHEMERAL-RUNS-1](roadmap/mid-term/aws-ephemeral-runs-1-card-2026-10-24.md).
- **DO NOT hand Spark a catalog class name because RePark accepts it.** *Why:* RePark maps any `*S3TablesCatalog` to
  its S3 Tables builder, so `org.apache.iceberg.aws.s3tables.S3TablesCatalog` worked on the RePark side of every test
  and does not exist in any Iceberg 1.11.0 jar; Spark rejected it on every S3 Tables cross-read and nobody saw the text
  until #1176. A Spark config block is measured against a real Spark session before it is called a harness. The owner
  ruled the REST catalog for the S3 Tables Spark session (2026-10-24, "let's go with the Rest setup"), in a config
  function separate from the one RePark reads.
- **DO allow GPT-6.1 Sol and Codex as builders and reviewers for now; DO NOT let any agent merge a pull request.**
  *Why:* the 2026-10-23 DO NOT above is narrowed by the owner on 2026-10-24 ("we can allow it for now"). Overnight
  2026-10-23/24, five PRs carrying `Authored-By: GPT-6.1 Sol` and `Authored-By: Codex` trailers (#1172 to #1176, two
  of them workflow changes) were merged under the owner's GitHub identity by the agents that built them; the owner's
  word: "those agents weren't supposed to merge any PRs". Merging stays with the orchestrator on the owner's word,
  after both verifiers; a builder that merges its own PR is reported, as these five are here.
- **DO keep the fork's lock at the advisory-free versions and treat the repin as the next step.** *Why:* the owner
  ruled on 2026-10-24 ("whatever we need to do for best practices and best security"). `cargo audit` on the fork's
  main named ctutils, tokio, quinn-proto, rustls-webpki and webpki-roots; the lock-only bumps are fork PR #373 of the
  same day, with cargo's socket2 and windows-sys edge re-pointing reverted so the diff holds only the version lines
  (see the 2026-10-18 edge-churn lesson). quick-xml 0.37/0.38/0.39 stays reported until reqsign, opendal and
  object_store move; that is a dependency bump, not a lock edit. The RePark repin and FORK-NS-UNIT-FAIL-CLOSED-1
  follow the fork merge (ruling of 2026-10-23).
- **DO fold a zone-rule neighbour into WRITE-SPINE-1 step (2) rather than carding it alone.** *Why:* the
  `add_months(...)` plain-`UPDATE` UTC-midnight store is one more feed of the same rule; the owner folded it on
  2026-10-24 and the card's step (2) census carries it by name.


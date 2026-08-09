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
- **DO release ephemeral providers on every exit path.** `FOR … AS OF` pinned temp views
  leaked into the session catalog and surfaced in `information_schema` — the exact
  introspection surface the same PR enabled. Deregister-after-use (the insert_overwrite
  idiom), routed so early returns and `?` can't skip it. The Spark door carries v1's copy of
  the same leak: fix it as a DECLARED divergence-with-issue, never silently in a fidelity port.
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

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
  the PR permanently BLOCKED with all other checks green (observed on PR #6: zizmor was
  required but only triggered on `.github/workflows/**`). Required workflows must be
  always-run on `pull_request`; keep path filters for record-keeping triggers (`push`) only.
- **DO retarget a stacked PR's dependent BEFORE merging its base and deleting the branch.**
  When `phase-1/pr-b` was deleted at #4's merge, GitHub auto-closed dependent PR #5; a closed
  PR whose base branch is gone can be neither retargeted (HTTP 422) nor reopened — the only
  recovery is a fresh PR (#6). Order of operations for a stack: change the child's base to
  `main` first, then merge the parent.

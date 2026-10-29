# F-CI-MACOS-CAP-1: the fork's two macOS `Tests` jobs are cancelled at fifteen minutes (fork card)

**Filed:** 2026-10-29. **Filed by:** the post-merge records clerk (`claude-haiku-5-5`), from the merges of RP-61 (PR #1209) and DEP-AUDIT-0414-1 (PR #1210).

**Status:** filed, not dispatched.

**Priority:** P3, housekeeping, fork side.

**Target:** the owned `iceberg-rust` fork, its `Tests` workflow. This is the fork's own CI. No RePark lane implements it, and a delegated agent never edits `.github/`.

## Why

On 2026-10-28 the two macOS jobs of the fork's `Tests` workflow were cancelled at exactly fifteen minutes on every run of fork PRs `#375` and `#377`: three runs on `#375` (one re-run by the orchestrator, one re-run on the owner's word) and two runs on `#377`. The two jobs are not required checks. The required checks are `Tests (default)` and `Tests (doc)`, so both PRs merged by direct squash with the required checks green. The source of the cancel is not identified, and no `timeout-minutes` is set on those jobs.

## The fix

Fork side, one of two routes: set a `timeout-minutes` on the two macOS jobs, matched to a measured run; or drop the macOS legs from pull requests. Nobody is dispatched.

## Pins

None in RePark. Any fix is the fork's, and its CI carries the pin.

## Out of scope

- The required-check set, which stays `Tests (default)` and `Tests (doc)`.
- Any RePark workflow, lock or pin.

Up: [map.md](map.md).

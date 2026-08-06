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

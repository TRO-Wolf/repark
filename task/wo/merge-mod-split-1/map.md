# map — task/wo/merge-mod-split-1/

MERGE-MOD-SPLIT-1 (owner, 2026-10-12): `write/merge/mod.rs` split at its planned boundary, a move
with no behaviour change. This directory closes when the split merges.

## Contents

- [plan.md](plan.md) — the boundary, the new files, which item goes where, the visibility each
  needs, and the proof of a pure move.
- `prove_move.py` — compares every item of `origin/main:mod.rs` with its copy in the four files,
  ignoring comments, visibility, whitespace outside literals and rustfmt's wrap comma, and
  counts the items on both sides. Run it from any directory; `--head-ref HEAD` reads the split
  files from git instead of the working tree.
- [proof.txt](proof.txt) — its output for this split: `PURE MOVE`, 76 items before and after.

Up: [../map.md](../map.md).

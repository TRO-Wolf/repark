# Unit ledger — P3A: phase-3 arming (design + brief + tier rows + rust CI split)

**Unit:** phase-3 PR-1 · **Brief:**
[../briefs/phase-3-python-facade.md](../briefs/phase-3-python-facade.md) §1 "PR-1" · **Design:**
[../docs/design/python-facade.md](../docs/design/python-facade.md) · **Port-Source:** v1 `main` @
`fc3f48102` · **Status:** IN FLIGHT

## Scope

Arm phase 3 before any phase-3 code lands, in one PR with no new code surface:

- Docs: `docs/design/python-facade.md` (settled 2026-08-08, competition-synthesized) +
  `briefs/phase-3-python-facade.md` in-repo; todo.md phase-3 slate; this ledger; map.md
  lockstep (docs/design, briefs, scripts, .github/workflows, task).
- DAG pre-declaration: `scripts/check_crate_dag.py` gains `TIER_NAMES[4] = "bindings"` (with
  the real-rule rationale comment), tier 3 renamed "spark surface" → "surface crates", and the
  two phase-3 rows (`repark-ml` 3, `repark-python` 4) with the deliberate non-edge comment
  (no repark-sql, no repark-iceberg edge on the binding). Crates not yet in the workspace are
  not inspected — the rows are pre-declared, the guard stays green.
- CI: the combined `rust` job splits into `rust-lint` (fmt + clippy + panic-ban + check, cache
  prefix `v2-df54`) and `rust-test` (workspace test, prefix `v2-df54-test`) — one prefix each,
  fresh disk each (free-disk step), setup-python 3.12 on both (libpython for the cdylib link
  from PR-3 on), no debuginfo/incremental. Design §7.1: this must precede the binding crate.
- Riders: docs/testing.md row-2 spelling note (design Q1 — `repark.sql()` is the *target*
  spelling; the release-prep gate makes the deferral mechanical); the
  `crates/repark-spark/src/dialect.rs` module-doc drift fix (`with_dialect` →
  `with_sql_dialect`, EC-6; comment-only).

Out of scope: every crate and Python file of the port (PR-2..PR-5), wheels/parity-live/tier-2
workflows (PR-5/PR-6), Makefile target additions (they land with the code they gate).

## Census obligation

None — docs, gate pre-declaration, CI shape, and one comment-only Rust edit. No test names
created, moved, or removed. (`cargo test -- --list` unchanged by construction; verified by the
green `rust-test` run.)

## Required-check transition (operator step at merge)

The split replaces the required context `Rust (fmt + clippy + check + test)` with two:
`Rust lint (fmt + clippy + check)` and `Rust test (workspace)`. Branch protection must accept the
new contexts **before this PR can merge** (phase-1 lesson: a renamed job's old context blocks
green PRs). The PR body carries the exact `gh api` command; the operator applies it.

## Gate results

Recorded at push time: guards (map.md, workflow parse, crate-DAG, lib.rs) green; rustfmt/clippy/
panic-ban/check/test green through the Makefile targets; both hygiene passes zero.

## Provocation proofs (run locally, never committed)

- crate-DAG: temporarily removed `repark-sql` from `TIERS` → guard failed loudly naming the
  unclassified crate; restored → green. Proves the guard still bites after the tier-map edit.
- workflow parse: `.github/workflows/ci.yml` re-parsed clean after the split (the guard that
  would have caught a YAML break in the two new jobs).

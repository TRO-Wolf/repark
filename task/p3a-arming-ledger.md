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
  spelling); **docs/release.md gains the "Hard blockers" section** landing the release-prep
  gate the note cites (the first tag fails while `repark.sql` is still a module) — added after
  the slim verifier flagged the gate as stated-but-nonexistent; the
  `crates/repark-spark/src/dialect.rs` module-doc drift fix (`with_dialect` →
  `with_sql_dialect`, EC-6; comment-only); root `map.md` phase banner refreshed (was stale at
  "phase 1"); `docs/map.md` design/ enumeration + phase-3 row (verifier finding).
- Cache-key fix (verifier MED): `shared-key: lint` / `shared-key: test` added to BOTH ci.yml's
  rust jobs and cache-warm.yml's warm jobs — rust-cache mixes the job id into the key when
  `shared-key` is absent, so the warm saves (warm-lint/warm-test) were landing under keys the
  PR jobs (rust-lint/rust-test) never restore. Pre-existing defect (v1 has the same shape);
  fixed here because this PR is the one arming the jobs.

Out of scope: every crate and Python file of the port (PR-2..PR-5), wheels/parity-live/tier-2
workflows (PR-5/PR-6), Makefile target additions (they land with the code they gate).

## Census obligation

None — docs, gate pre-declaration, CI shape, and one comment-only Rust edit. No test names
created, moved, or removed. (`cargo test -- --list` unchanged by construction; verified by the
green `rust-test` run.)

## Required-check transition (operator step at merge)

The split replaces the required context `Rust (fmt + clippy + check + test)` with two:
`Rust lint (fmt + clippy + check)` and `Rust test (workspace)`. Branch protection must **add the
two new contexts AND remove the old one in the same update** — adding alone leaves this PR
blocked forever on a job that no longer exists (phase-1 lesson: a renamed job's old context
blocks green PRs). The PATCH in the PR body does both at once (its `contexts` array is the
complete replacement list). Any other PR open at that moment, with a head still on the combined
job, goes pending on the two new contexts until rebased — none exists in the strictly-ordered
slate, but the hazard is recorded.

## Gate results

Recorded at push time: guards (map.md, workflow parse, crate-DAG, lib.rs) green; rustfmt/clippy/
panic-ban/check/test green through the Makefile targets; both hygiene passes zero.

## Provocation proofs (run locally, never committed)

- crate-DAG: temporarily removed `repark-sql` from `TIERS` → guard failed loudly naming the
  unclassified crate; restored → green. Proves the guard still bites after the tier-map edit.
- workflow parse: `.github/workflows/ci.yml` re-parsed clean after the split (the guard that
  would have caught a YAML break in the two new jobs).

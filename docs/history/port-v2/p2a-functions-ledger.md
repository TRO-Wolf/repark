# Unit ledger — P2A: repark-functions + phase-2 docs

> **ARCHIVED 2026-08-09** (Front-Door FD-4) — a historical record of the v1 → v2 port, kept for
> provenance and **not a source of live rules**: every rule still in force was promoted to a
> current document first ([promotion-ledger.md](promotion-ledger.md)). Relative links were
> repaired for this location on the same date; nothing else changed. Current state:
> [STATUS.md](../../../STATUS.md).

**Unit:** phase-2 PR-1 · **Brief:**
[phase-2-sql-doors.md](phase-2-sql-doors.md) §1 "PR-1" · **Design:**
[docs/design/sql-doors.md](../../design/sql-doors.md) · **Port-Source:** v1 `main` @
`fc3f48102` · **Status:** MERGED 2026-08-07 (PR #8; archived 2026-08-09)

## Scope

Land the first phase-2 crate and the phase-2 governing documents in one PR:

- `crates/repark-functions`: v1 repark-functions ported VERBATIM — crate name KEPT, so the
  census map is the identity map (no rename rules); full 62-test battery rides in the same
  change.
- Workspace arming: members gains `crates/repark-functions`; any new external deps enter
  `[workspace.dependencies]` under the workspace-style internal-dep pattern (path +
  version "0.0.0" + default-features=false).
- DAG pre-declaration: `scripts/check_crate_dag.py` TIERS gains all four phase-2 rows
  (repark-functions, repark-ta, repark-spark, repark-sql — all tier 3, per the design §1) in
  this PR, so later PRs add members without gate edits.
- Docs: `docs/design/sql-doors.md` + `briefs/phase-2-sql-doors.md` in-repo; todo.md phase-2
  slate; deferred-manifest re-pointing (post-milestone-one + per-PR targets); this ledger;
  map.md lockstep.

Out of scope: repark-ta / repark-spark / repark-sql code (PR-2..PR-6), the three hoists,
carve-out files (.github/, AGENTS.md, CLAUDE.md, Makefile — orchestrator-only).

## Edit classes (declared, bounded — per the design)

1. **Verbatim copy** — v1 repark-functions sources via `git show` at the pinned SHA; bodies
   byte-faithful.
2. **Workspace-style Cargo alignment** — `Cargo.toml` rewritten to this workspace's
   conventions (workspace deps, edition/lints inheritance); no code content diverges.
3. **DAG pre-declaration** — the four TIERS rows added to `check_crate_dag.py` (gate config,
   not product code).

No other edit class is authorized; anything else is a STOP.

## Census obligation

62 test names at the pin (`cargo test -p repark-functions -- --list`, regenerated per
[docs/testing.md](../../testing.md) — never hand-written). Identity map:
`repark_functions::` prefix unchanged. Acceptance: name-by-name sorted diff of the pin list
against this repo's `--list` is **EMPTY**; reconciliation entry appended to
[task/port/deferred-tests.md](../../../task/port/deferred-tests.md).

## Gate results (integrator fills)

| Gate | Result | Evidence |
|---|---|---|
| `make ci` per commit | | |
| `make preflight` (PR head) | | |
| census empty-diff (62, identity map) | | |
| forbidden-literal sweep (tree + `git log -p`) | | |
| map.md lockstep (`check_map_md.sh`) | | |

## Deviations / STOPs

*(record as they occur)*

## Follow-up riders (from PR-1 verify panel)

Logged here so they are not lost; NOT fixed in PR-1 because the sources are pinned verbatim
for census identity (edit-class 1) and changing them would break the declared port contract.

1. **Doc-comment re-home (rider on PR-2/PR-3b).** Stale v1 crate references inside the
   ported crate: `repark-sql::spark_ast` mentions in `crates/repark-functions/map.md` and
   `src/map.md` (in this repo's settled design, `spark_ast` lands in **repark-spark** and
   `repark-sql` names the new ANSI door), plus `repark-session` prose refs in the crate's
   `Cargo.toml`, `map.md`, `src/map.md`, `src/lib.rs`, `analyzer.rs`, `cardinality.rs`,
   `string.rs`. Re-home these doc comments when repark-spark actually lands.
2. **Benches map.md jargon.** `crates/repark-functions/benches/map.md` carries v1-internal
   round jargon; reword whenever the benches CI workflow is ported.

## Retrospective

*(filled at unit close, per SEPMO)*

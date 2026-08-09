# Unit ledger — P1A: workspace arming + repark-common + gates

> **ARCHIVED 2026-08-09** (Front-Door FD-4) — a historical record of the v1 → v2 port, kept for
> provenance and **not a source of live rules**: every rule still in force was promoted to a
> current document first ([promotion-ledger.md](promotion-ledger.md)). Relative links were
> repaired for this location on the same date; nothing else changed. Current state:
> [STATUS.md](../../../STATUS.md).

**Unit:** phase-1 PR-A · **Brief:**
[phase-1-engine-core.md](phase-1-engine-core.md) §2 "PR-A" · **Design:**
[docs/design/session-api.md](../../design/session-api.md) · **Port-Source:** v1 `main` @
`fc3f48102` · **Status:** MERGED 2026-08-07 (PR #3, `5eba40a`; archived 2026-08-09)

## Scope

Arm the empty workspace and land the first crate, with the "first member" gate obligations in
the same PR:

- Root `Cargo.toml`: `[workspace.dependencies]` for the phase-1 cone (datafusion 54.1.0 family,
  arrow/parquet 58.4, object_store 0.13 aws, iceberg family 0.9.1, thiserror, async-trait,
  futures, uuid, url, aws-config/aws-credential-types, tokio dev); members gains
  `crates/repark-common`; internal-dep pattern path + version "0.0.0" + default-features=false.
- `crates/repark-common`: v1 error-seed crate verbatim (lib.rs manifest + file-backed tests.rs,
  2 tests renamed by the crate-prefix rule only).
- Gate arming: CARGO_EMPTY guard deletion (orchestrator-applied carve-out), `check_crate_dag.py`
  (3-row TIERS map) + `check_lib_rs` + wrappers + pre-commit/Makefile/ci.yml wiring, each with a
  provocation proof.
- Docs: `docs/design/session-api.md` + map, `briefs/phase-1-engine-core.md` + map,
  `task/port/deferred-tests.md` scaffold, todo.md phase-1 entries, this ledger; map.md lockstep.
- CI: audit.yml returns. (cache-warm.yml deferred to PR-B — see Deviations D-1.)

Out of scope: the fork `[patch.crates-io]` pin (PR-B), any session/catalog/write code (PR-B/C).

## Commit plan (three commits)

1. **Literal copy** — v1 repark-common sources via `git show` at the pinned SHA (Port-Source in
   the PR body); workspace `Cargo.toml` arming.
2. **Re-home** — crate-prefix rename (the only forced edit class in this PR), gate scripts
   ported + wired.
3. **Docs + ledgers** — design doc, brief, manifests, todo, map.md lockstep.

**Actual series (five commits over `main`):** the three planned commits landed as
`475c975` (literal copy + workspace arming), `e5348ae` (crate-DAG + lib.rs manifest gates),
`110bdf8` (design doc, brief, manifests, ledgers); the orchestrator then applied two carve-out
commits: `68d1ed7` (gate arming — CARGO_EMPTY guard removal, Makefile/ci.yml wiring, audit.yml)
and `52a5289` (AGENTS.md target-map correction + brief status sync). See Deviations D-3.

## Gate results (integrator fills)

| Gate | Result | Evidence |
|---|---|---|
| `make ci` per commit | PASS (rc=0 on all five commits) | integrator run 2026-08-06 on `475c975`/`e5348ae`/`110bdf8` (CARGO_EMPTY guard auto-activated real cargo gates once `repark-common` joined the workspace); fixer run 2026-08-06 on `68d1ed7` (detached worktree, rc=0, post-carve-out Makefile) and at PR head |
| `make preflight` (PR head) | PASS (rc=0) | integrator run 2026-08-06 (pre-carve-out Makefile); fixer re-run 2026-08-06 at PR head on the carve-out (post-`68d1ed7`) Makefile, rc=0 |
| `cargo test --workspace` (2 tests, renamed) | PASS — 2 passed / 0 failed | `repark_common` unit tests via file-backed `src/tests.rs` |
| forbidden-literal sweep (tree + `git log -p`) | CLEAN (0 hits) | case-insensitive grep over each staged diff and `git log -p main..HEAD` |
| map.md lockstep (`check_map_md.sh`) | PASS | pre-commit hook fired on every commit |

## Provocation proofs (integrator fills)

Per docs/testing.md: each armed gate demonstrated firing on a deliberately-broken tree.

| Gate | Provocation | Observed failure | Restored-green |
|---|---|---|---|
| `check_crate_dag.py` | Added `repark-core = { path, version "0.0.0" }` to `repark-common` `[dependencies]` with a stub `repark-core` member (tier-0 → tier-2 edge) | exit 1: `ERROR: layering inversion — repark-common [tier 0 (foundation)] depends on repark-core [tier 2 (engine session)]…` + `crate-dag: layering rule violated` | yes — `git checkout` restore; `crate-dag: 0 internal edges clean across 1 of 3 mapped crates`, rc=0 |
| `check_crate_dag.py` (unmapped crate) | Renamed the stub to `repark-mystery` (not in TIERS) | exit 1: `ERROR: repark-mystery is not in the tier map (scripts/check_crate_dag.py TIERS)…` | yes — stub removed, `git status` clean |
| `check_lib_rs` (inline test mod) | Appended `#[cfg(test)] mod smoke { #[test] fn t() {} }` to `repark-common` `src/lib.rs` | exit 1: `ERROR: repark-common src/lib.rs:150: inline #[cfg(test)] mod smoke { … } is forbidden — move the body to src/smoke.rs…` + `lib-rs: FAIL` | yes — `git checkout` restore; `lib-rs: 1 crate roots clean`, rc=0 |
| `check_lib_rs` (line ceiling) | Padded `src/lib.rs` to 151 lines | exit 1: `ERROR: repark-common src/lib.rs is 151 lines (ceiling 150)…` | yes — `git checkout` restore, `git status` clean |

## Deviations / STOPs

- **D-1 (scope): cache-warm.yml deferred to PR-B.** Warming a cache nothing restores is waste;
  the ci.yml rust-cache restore steps and the heavy dependency graph both arrive with
  repark-iceberg in PR-B, so cache-warm.yml moves there with them. Decision recorded in
  `.github/workflows/map.md` ("Not ported yet"); brief §2 PR-A and todo.md carry the same note.
- **D-2 (pin fidelity): two sanitization edits beyond the crate-prefix rename** in
  `crates/repark-common`: (a) `Cargo.toml` `description` rewritten from v1's
  "Shared domain + error types for the repark engine." to
  "Shared error-seed types (Error / ErrorClass / Result) for the repark engine." — v1's crate
  carried more than the error seed; the ported crate does not, and the V2 target map reserves
  the broader description for later crates; (b) `src/map.md` drops v1's "(r26 LR1 hoist)"
  annotation — a v1-internal slate reference with no referent in this repo. Both deliberate;
  no code content diverges from the pin.
- **D-3 (process): the planned three-commit series shipped as five** — the two orchestrator
  carve-out commits (`68d1ed7` gate arming, `52a5289` AGENTS/brief sync) landed on the branch
  rather than as separate integrator pushes. Every-commit-green evidenced for all five (gate
  table above).

## Retrospective

*(filled at unit close, per SEPMO)*

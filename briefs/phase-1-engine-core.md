# Phase-1 execution brief — RePark V2 (engine core)

**Date:** 2026-08-06 · **Status:** IN EXECUTION — design approved and port-source pin resolved
2026-08-06; PR-A in flight.

Phase 1 lands the engine core in the public repo per
[docs/design/session-api.md](../docs/design/session-api.md) (the settled Session-API design):
`repark-common` + `repark-iceberg` + `repark-core`, the iceberg-rust fork pin, and the Rust
unit-test tier — copy-then-re-home, every commit green, delivered as three PRs.

## 0. Port-source pin (operator decision, blocks PR-B/PR-C)

v1 `main` and the r27 branch differ in the phase-1 crate cone **only** by the move-only
merge-module split (monolith `merge.rs` → `merge/` directory, ~8.2k lines, tests relocated).
The design assumes the split shape.

- **RESOLVED 2026-08-06: pinned to v1 `main` @ `fc3f48102`** (the #141 squash-merge; verified to
  contain the merge/ split). The operator had already merged the r27 slate.
- Local-clone caveat: the v1 checkout's `main` may lag this SHA until the operator re-points the
  remote (old repo name resolves to the public repo — see the remote-URL hazard note) and pulls;
  agents copy via `git show fc3f48102:<path>` against a fetched ref, never a working tree.

The pinned SHA is written into this brief and into each PR description as `Port-Source:` before
the literal-copy commit. All copies come from `git show <SHA>:<path>` — never from a working
tree.

## 1. Hard rules (every agent; carried from phase 0, plus port-specific)

1. v1 repo READ-ONLY; copies via `git show` at the pinned SHA only. No AWS commands; never set
   the acceptance/live-database env vars. Forbidden-literal list identical to phase 0 (grep gate
   on every commit AND `git log -p`).
2. Attribution trailer exactly as phase 0. Repo-local git identity is already set — verify
   `git config user.email` ends `@users.noreply.github.com` before the first commit.
3. `cargo test --workspace`, never the all-features flag; never skip hooks.
4. **Copy-then-re-home:** each PR starts with literal-copy commit(s), then bounded re-home
   commits. Every commit compiles and passes `make ci`. Byte-faithful bodies except the five
   forced-edit classes in docs/design/session-api.md §5 — any other behavior delta found mid-port is a STOP
   (report, don't improvise).
5. **Relocation discipline** (docs/testing.md): the crate merges are declared-rename units; the
   old→new test-name map is generated (v1 `--list` at the pin → four prefix rules → diff empty),
   never hand-written. Tests port with their names; no `#[ignore]`; deferred tests go in the
   checked-in manifest, not in comments.
6. `.github/`, AGENTS.md/CLAUDE.md, and Makefile-guard edits are **orchestrator-scoped** — staged
   by delegated agents only as explicit diffs for the orchestrator to apply/commit.

## 2. PR slate

### PR-A — workspace arming + repark-common + gates
- Root `Cargo.toml`: `[workspace.dependencies]` (v1 list for the phase-1 cone: datafusion
  54.1.0 family, arrow/parquet 58.4, object_store 0.13 aws, iceberg family 0.9.1, thiserror,
  async-trait, futures, uuid, url, aws-config/aws-credential-types, tokio dev) + members gains
  `crates/repark-common`. Internal-dep pattern: path + version "0.0.0" + default-features=false.
- `crates/repark-common`: v1 error-seed crate verbatim (lib.rs manifest + file-backed tests.rs,
  2 tests renamed by crate-prefix rule only).
- Gate arming, same change (the "first member" obligations): delete every CARGO_EMPTY guard
  branch in the Makefile + cargo-deny.yml (orchestrator applies); port `check_crate_dag.py`
  (3-row TIERS map) + `check_lib_rs` + their `.sh` wrappers + pre-commit/Makefile/ci.yml wiring,
  each with a provocation proof (deliberately-broken tree shows the gate firing, per
  docs/testing.md).
- Docs, same PR: `docs/design/session-api.md` (= the settled design doc, sanitized), `docs/design/map.md`,
  ADR-0005 pointer if wanted, AGENTS.md exec/io target-map correction + todo.md check_lib_py
  phase fix (orchestrator applies), deferred-test manifest scaffold at
  `task/port/deferred-tests.md`, task/todo.md phase-1 unit entries, map.md lockstep everywhere.
- CI: audit.yml + cache-warm.yml return (workflows/map.md promise); ci.yml detect classifier
  stays deferred until rust jobs are actually slow (record in workflows/map.md with a trigger:
  "returns when rust-test exceeds ~3 min").

### PR-B — repark-iceberg (declared-rename unit)
- `[patch.crates-io]` fork pin verbatim: five iceberg crates → the owned fork at v1's rev
  (starts `b009ac15`); deny.toml already allow-lists the source. Fork-pin proof test naming a
  fork-only public symbol (compile-fails on silent registry fallback), per ADR-0001.
- Literal-copy commits: v1 repark-catalog → `src/catalog/`, v1 repark-write → `src/write/`
  (split merge/ shape). Re-home commits: lib.rs manifest (union of re-exports, names unchanged),
  `repark_core::` → `repark_common::` import rewrite, the `reregister_catalog_provider` hoist
  into `catalog/catalog_ops.rs`, Cargo.toml (deps = union of the two v1 manifests).
- Pre-copy checks (from docs/design/session-api.md R-4): confirm at the pinned fork rev whether the
  metadata-projection shim is still required (if fixed upstream in the fork: keep the shim
  anyway this phase — removing it is an unforced delta — but record in the omissions ledger);
  re-run the trait-wrapping both-sides audit on the namespace-scoped catalog wrapper and attach
  it to the PR.
- Tests: 243 (catalog 51 + write 192) under the generated rename map; sorted `--list` diff
  against the map must be empty. Census note in the PR body.
- deny/audit advisory entries return only if `cargo deny`/`cargo audit` actually flag the new
  dependency graph (each entry justified in-line, v1 wording).

### PR-C — repark-core (Session)
- Literal-copy commit of v1 repark-session files, then re-home commits in docs/design/session-api.md order:
  module split (session.rs, backend.rs, catalog_config.rs, read_options.rs, idents.rs,
  error_map.rs minus the deferred folds, object_store_s3.rs), the three hoists from v1
  repark-sql (`catalog_state.rs`, `time_travel.rs` + read_table_at + its tests,
  TimeTravelSpec), the seam files (`dialect.rs`: EngineContext + SqlDialect + DataFusionDialect;
  `extension.rs`: SessionExtension), then the four forced edits as separate reviewable commits:
  E-2 conditional finalize-time AWS resolution (+ both gate tests), dialect inversion in
  `sql()`/`sql_with`, extension hooks in `build()`, E-4 `TempFallbackAllowed { root }`.
- Session-test audit commit: the mechanical port-now vs deferred split of the 49+26+4 session
  tests (+ ta_window deferred whole), deferred names into `task/port/deferred-tests.md` with
  target phases; `cargo test --workspace -- --list` reconciliation showing
  (ported ∪ deferred) = v1 cone total.
- New-seam tests land with the seams (DataFusionDialect passthrough; extension hook ordering;
  EngineContext non-exhaustive construction).

Each PR: map.md lockstep, `make preflight` green, forbidden-literal sweep (tree + log -p),
Port-Source line in the body, no session links, plain attribution.

## 3. Fleet plan (on approval)

Per-PR staged flow, same shape as phase 0: implementation agents (Fable low) stage the copy +
re-home series in a worktree-per-PR; a verify panel (consistency / relocation-census /
gates-execute / security) checks each PR before the orchestrator pushes; orchestrator applies
all carve-out diffs (rule 6) itself. PR-A first (arms the workspace); PR-B and PR-C sequential
(C depends on B). Between PRs the operator merges — required checks are live on main.

## 4. Acceptance (phase-1 done)

- Three PRs merged; CI rust jobs armed (no CARGO_EMPTY anywhere) and green on real code.
- Fork-pin proof test green; crate-DAG + lib-rs gates live with provocation proofs recorded.
- Census subset: generated rename-map diff empty; deferred-test manifest reconciles to v1
  totals; zero `#[ignore]`, zero skipped-in-CI.
- Omissions ledger + server landing map present in docs/design/session-api.md; task/todo.md
  phase-1 unit closed with a dated retrospective note per SEPMO.

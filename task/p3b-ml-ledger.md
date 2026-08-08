# Unit ledger — P3B: repark-ml (the PR-2 verbatim port + identity census)

**Unit:** phase-3 PR-2 · **Brief:**
[../briefs/phase-3-python-facade.md](../briefs/phase-3-python-facade.md) §1 "PR-2" · **Design:**
[../docs/design/python-facade.md](../docs/design/python-facade.md) §1 (edit classes: `none
(verbatim)`), §2.1 (tier row), §4 Q3 (in-scope ruling), §9 PR-2 · **Port-Source:** v1 `main` @
`fc3f48102` · **Status:** IN FLIGHT · **Stacked on:** phase-3 PR-1
([p3a-arming-ledger.md](p3a-arming-ledger.md))

## Scope

Land `crates/repark-ml` — the native ML estimator kernels the phase-3 binding (PR-3) and the ML
facade package (PR-5) both require — as a **verbatim** port with an **identity census**. Design §1
assigns this crate **no edit classes at all**: the acceptance claim is an empty `diff -r` against
the pin plus an empty sorted `--list` diff.

- **Crate copy, byte-identical.** `cp -a` of `v1-pin/crates/repark-ml` → `crates/repark-ml`.
  `diff -r` against the pin reports **no differences at all** at copy time: 8 files
  (`Cargo.toml`, `map.md`, `src/map.md`, and the five modules `lib.rs`, `error.rs`, `cholesky.rs`,
  `linear_regression.rs`, `logistic_regression.rs`, `kmeans.rs`), 1,703 source lines, md5-recorded
  on both sides. No `tests/` directory exists at the pin — every test is an in-module
  `#[cfg(test)]` block, so the whole suite travels inside the source files.
- **No manifest edit was needed.** Unlike the phase-2 verbatim ports (p2e class 2), the crate's
  `Cargo.toml` is byte-identical: its only entries are the six `*.workspace = true` package keys,
  `[lints] workspace = true`, and `thiserror.workspace = true` — all of which already resolve
  against the v2 root (`thiserror = "2"` was hoisted into `[workspace.dependencies]` in phase 1
  and matches the v1 spec exactly). **No new workspace dependency was added.**
- **Workspace wiring (root `Cargo.toml`, the only mechanical delta).** `crates/repark-ml` added to
  `[workspace] members`; `repark-ml = { path = …, version = "0.0.0", default-features = false }`
  added to `[workspace.dependencies]` — both lines identical in shape to the v1 root manifest at
  the pin (v1 `Cargo.toml:19` and `:125`), the second one inert until PR-3's binding names it.
  The header banner's stale phase-1 member roster ("repark-ml, repark-python in later phases") is
  refreshed to the real per-phase arrival order — comment-only. `Cargo.lock` picks up exactly one
  new package stanza (`repark-ml` → `thiserror 2.0.19`) and is committed.
- **No DAG or guard edit.** PR-1 pre-declared `TIERS["repark-ml"] = 3`, so the crate-DAG guard
  simply starts inspecting a row it already had; `check_lib_rs` needs no exception row (the
  42-line crate root is a pure manifest: six `pub mod` / `pub use` lines, one `MAX_FEATURES`
  const, no inline test module).
- **map.md lockstep + EC-7 spirit (§3: "stale v1 `map.md` files are rewritten to the true tree
  rather than ported stale").** `crates/repark-ml/src/map.md` was verified accurate against the
  tree and ported **byte-identical** (every file, role, constant and debug row checks out:
  `MAX_FEATURES = 4096` in `lib.rs`, `PIVOT_REL_EPS`/`PIVOT_ABS_EPS` in `cholesky.rs`, the
  SAF-004/SAF-006 markers, and all five debug-row error variants present in `error.rs`).
  `crates/repark-ml/map.md` was **stale in this repository**: five of its links pointed at v1-only
  paths that do not exist here (`docs/ml-design.md`, `task/m3-ml-estimators-ledger.md`,
  `briefs/2026-08-03-…-m3-slate.md`, `python/repark/src/repark/ml/regression.py`,
  `crates/repark-python`). Rewritten truthfully: the design/brief/ledger pointers now name the
  phase-3 in-repo documents, "tier-1" is corrected to the crate-DAG tier **3** the SSOT actually
  assigns, and the two forward rows are labelled with the PR that lands them (PR-3 binding, PR-5
  facade) instead of linking into empty space. `crates/map.md` gains the `repark-ml` row, the
  "I want to…" row, and the no-internal-deps DAG sentence. `task/map.md` gains this ledger.
  **This is a documentation-only delta; it moves no test name and costs no census cell.**

Out of scope: `crates/repark-python` and any binding wiring (PR-3), the ML facade package and its
~138 facade tests (PR-5), any new estimator or solver, `todo.md` checkbox flips (the box turns at
merge), and every carve-out file (`.github/`, `AGENTS.md`, `CLAUDE.md`, `Makefile`, branch
protection) — PR-2 needs none of them. No AWS: this crate has no I/O surface whatsoever, and no
`REPARK_*` acceptance/gate variable was set at any point.

## Census obligation — identity map, empty diff (REQUIRED, DISCHARGED)

Design §6 table row "Rust — `repark-ml` | `cargo test -- --list`, identity map (no rename) | none
expected". No test was renamed, moved, added, or removed, so the diff must be empty.

```
# v1 side (READ-ONLY port source, built only to enumerate)
(cd <v1-pin> && cargo test -p repark-ml -- --list 2>/dev/null | grep ': test$' | sort) \
  > /tmp/ml-census-v1.txt
# v2 side (this worktree)
(cd <wt-pr2> && cargo test -p repark-ml -- --list 2>/dev/null | grep ': test$' | sort) \
  > /tmp/ml-census-v2.txt

$ wc -l /tmp/ml-census-v1.txt /tmp/ml-census-v2.txt
  34 /tmp/ml-census-v1.txt
  34 /tmp/ml-census-v2.txt

$ diff /tmp/ml-census-v1.txt /tmp/ml-census-v2.txt
$ echo $?
0
```

**v1 count: 34. v2 count: 34. Diff: EMPTY (verbatim, zero lines of output, exit 0).**

Cross-check against the runner (counts are generated, never hand-written): `make test` reports
`repark_ml … running 34 tests … 34 passed; 0 failed; 0 ignored`, and `Doc-tests repark_ml` runs 0
— matching the pin. Distribution across the five modules is unchanged by construction (the copy is
byte-identical, so the `#[cfg(test)]` blocks are the same bytes).

## Gate results

Both run in the PR worktree, `--workspace`, never `--all-features`, never `--no-verify`.

- **`make ci` — exit 0.** `cargo clippy --locked --workspace --lib --bins` with the panic-ban
  deny list clean (`repark-ml` checked, zero warnings — the verbatim code passes the v2 clippy
  config unmodified, so **no config drift and no code edit**, which is the finding this PR was
  told to surface if it went the other way); `crate-dag: 11 internal edges clean across 8 of 9
  mapped crates` (the ninth mapped crate is `repark-python`, pre-declared in PR-1 and not yet in
  the workspace — expected); `lib-rs: 8 crate roots clean (no inline test modules; ceilings
  held)`; `cargo check --locked --workspace` clean; ruff check/format, taplo format/lint, typos
  all clean.
- **`make test` — exit 0.** Whole workspace green: repark-common 13, repark-core 87,
  repark-functions 62, repark-iceberg 242, **repark-ml 34**, repark-spark 348 (+ 5 ddl_sessions,
  1 dml_sessions, 1 session_extension, 7 ta_window), repark-sql 208 (+ 8 cross_door,
  5 introspection, parser_productions …), plus repark-ta; every reported line `0 failed;
  0 ignored`. Doc-tests: 0 across the workspace.
- Pre-commit hook (map.md lockstep, crate-DAG, lib.rs, `cargo fmt --check`, taplo, typos) passed
  on the single commit.
- Public hygiene: both mandated passes (staged diff vs `main`, and the commit-metadata log pass)
  returned **0** matches against the forbidden-pattern list.

## Notes for the verifier

1. The one judgement call in this PR is the `crates/repark-ml/map.md` rewrite. It is deliberate,
   argued from design §3 EC-7's stated principle and the repo's hard map.md-accuracy rule, and it
   touches **no `.rs` byte**. If the verifier prefers a byte-identical map with five dead links,
   that is a one-file revert.
2. `map.md` "tier-1" in the v1 text meant the M3 estimator tier of the v1 ML roadmap, not the
   crate-DAG tier; the two collided in one word, and the rewrite disambiguates to the crate-DAG
   tier 3 that `scripts/check_crate_dag.py` (the SSOT) assigns.
3. Building in the v1 pin worktree created only its gitignored `target/`; no tracked file there
   was modified.

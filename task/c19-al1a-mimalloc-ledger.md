# Unit ledger — AL-1a feature-gated mimalloc

**Unit:** AL-1a · conductor-19 · **Date:** 2026-08-16 ·
**Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-c19` · **Branch:** `grok/c19-al1a-mimalloc` ·
**Base:** origin/main after #155 / #156 / #157 (v0.3.2).

**Charter:** `BRIEF-conductor-19.md` + Addendum 2026-08-16 Q&A round 1 (A4, A5, A9).
No wheel change. No bench numbers. No `python/repark/pyproject.toml` edit.

## Intent

Install mimalloc as an **optional** Rust global allocator on the Python
bindings crate so AL-1b can A/B it against the system allocator. Default
OFF: `cargo test --workspace` and the published wheel stay on the system
allocator.

## What shipped

| Artifact | Path |
|---|---|
| Workspace pin + rationale | `Cargo.toml` `[workspace.dependencies]` (`mimalloc = "0.1"`) |
| Optional dep + feature | `crates/repark-python/Cargo.toml` (`allocator-mimalloc = ["dep:mimalloc"]`) |
| File-backed `#[global_allocator]` | `crates/repark-python/src/allocator.rs` |
| Two-line cfg hook | `crates/repark-python/src/lib.rs` (ceiling 190 stands) |
| Cfg-split pins | `crates/repark-python/src/tests.rs` |
| Maps | `crates/repark-python/map.md` + `src/map.md` |
| Lockfile | `Cargo.lock` (ordinary dep add; family-bump restriction does not apply) |
| This ledger | `task/c19-al1a-mimalloc-ledger.md` + `task/map.md` row |

## Honest cuts

- `deny.toml` untouched (mimalloc + libmimalloc-sys are MIT).
- `uv.lock` / `pyproject.toml` untouched.
- No jemalloc. No CI/container work (A11).
- No feature-on `maturin develop` in this increment (A5).

## Gates (real exit codes)

| Gate | Exit |
|---|---|
| `make verify` | **0** |
| `make preflight` | **0** (facade 3278 passed, 70 skipped) |
| `cargo test --locked -p repark-python --features allocator-mimalloc` | **0** (35 lib + 24 bindings) |

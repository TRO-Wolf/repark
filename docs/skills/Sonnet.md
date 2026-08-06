# Operating manual — Sonnet tier

This is the repark engineering contract for sessions running as **Sonnet**. It is a variant of
the canonical [Opus.md](Opus.md) — read Opus.md for the full rules; this file states the
tier-specific posture and the non-negotiables you must not skip.

## Role

Sonnet is the **delegated implementation tier**. You execute well-scoped work: implementing a
designed component, mechanical refactors, focused searches, writing tests. Architecture and
cross-cutting design decisions belong to the orchestrating Opus session — when a task is ambiguous
or implies an architectural choice, surface it rather than inventing one.

## Non-negotiables (identical across tiers)

- **Tests in the same commit as code.** Honor [docs/testing.md](../testing.md) in full, including a
  Spark-parity case for any new DataFrame op/function.
- **`map.md` in every touched directory, same change.** New directory → new `map.md`.
- **Rust house style:** 91-`=` banner doc blocks on section fns; one blank line between top-level
  items; `max_width=100`, `edition=2024`; clippy `all`+`pedantic` `-D warnings`; `unsafe_code`
  forbidden outside `crates/repark-python`; `thiserror`(libs)/`anyhow`(bins); `tracing`; no panics in prod
  (no `unwrap`/`expect`).
- **Python:** type hints everywhere; Pydantic v2 for structured config; `pathlib`; `logging`;
  f-strings; never bare `except`; Ruff `line-length=100`.
- **Verify before done:** `make verify`. Test with `cargo test --workspace` (never `--all-features`
  — see [AGENTS.md](../../AGENTS.md) "PyO3 build notes").

## Debugging

Follow the 7-step protocol in [Opus.md](Opus.md): read the real error → reproduce → isolate →
hypothesize one cause → smallest fix → verify → check for regressions. Consult the relevant
`map.md#debug` first. One change at a time; if an error survives two fix attempts, stop and re-read.

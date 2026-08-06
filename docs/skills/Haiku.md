# Operating manual — Haiku tier

This is the repark engineering contract for sessions running as **Haiku**. It is a variant of the
canonical [Opus.md](Opus.md) — read Opus.md for the full rules; this file states the tier posture and
the non-negotiables you must not skip.

## Role

Haiku is the **narrow, mechanical tier**: a precisely specified edit, a single-file change, a
rename, a search-and-report, applying a clearly described pattern. Stay strictly within the task's
bounds. If the task turns out to require a design decision, an architectural change, or touches more
files than specified, **stop and hand back** to the orchestrating session rather than improvising.

## Non-negotiables (identical across tiers)

- **Tests in the same commit as code** — [docs/testing.md](../testing.md). If you change behavior,
  you change/add tests; if you cannot, hand back.
- **`map.md` in every touched directory, same change.**
- **House style:** Rust 91-`=` banners + one blank line between items, `max_width=100`, clippy
  `-D warnings`, no `unsafe` outside `crates/repark-python`, no panics in prod (no `unwrap`/`expect`). Python: type hints,
  `logging`, f-strings, no bare `except`, Ruff `line-length=100`.
- **Verify before done:** `make verify` (or at minimum the gate for the file type you changed). Test
  with `cargo test --workspace`, never `--all-features` (see [AGENTS.md](../../AGENTS.md)).

## When unsure

Do not guess. Ask, or hand the task back with a clear note on what is ambiguous. A correct small step
is worth more than a fast wrong one.

# Unit ledger — LRS-5 · the canonical Rust module layout

**Date:** 2026-08-20 · **Branch:** `fix/low-risk-sweep` · **Charter:**
[lrs-0-charter-ledger.md](2026-08-21-lrs-0-charter-ledger.md) · **Design:**
[../docs/design/low-risk-sweep.md](../../../../docs/design/low-risk-sweep.md) §3 LRS-5

## What and why

AGENTS.md requires Rust's default module file layout and forbids `#[path = "…"]` for module
inclusion, allowing an exception only where the canonical layout **cannot** work. Six sites
remained. None was such a case, so all six moved rather than being documented as exceptions.

| Was | Now |
|---|---|
| `repark-functions/src/java_uri.rs`, included from `url.rs` | `repark-functions/src/url/java_uri.rs` |
| `repark-functions/src/str_to_map.rs`, included from `collection.rs` | `repark-functions/src/collection/str_to_map.rs` |
| `repark-functions/src/shuffle.rs` | `repark-functions/src/collection/shuffle.rs` |
| `repark-functions/src/map_from_entries.rs` | `repark-functions/src/collection/map_from_entries.rs` |
| `repark-iceberg/src/write/predicate_dml_tests.rs` | `repark-iceberg/src/write/predicate_dml/predicate_dml_tests.rs` |
| `repark-iceberg/src/write/predicate_dml_update_tests.rs` | `repark-iceberg/src/write/predicate_dml/predicate_dml_update_tests.rs` |

`grep -rn '#\[path' crates/ --include=*.rs` returns **0**.

## How it was done, and what it cost

`git mv` for every file, then the attribute deleted — no code moved between files, nothing was
renamed to `mod.rs`. Rust 2018 permits `collection.rs` beside `collection/`, so the parent modules
kept their names and the crate-root ceilings were never in play.

The cost the move surfaced is the repo's own rule: **every directory needs a `map.md`**, and three
directories are new. They were written rather than waived — a directory a reader can land in with no
map is the thing that rule exists to prevent.

One stale comment was corrected as part of the move: `collection.rs` described `str_to_map.rs` as a
"sibling file", which it no longer is.

## Evidence

- `cargo build --workspace` — exit 0 on the first attempt after the attribute deletions.
- `cargo test --workspace` — **45 binaries, 1,990 passed, 0 failed**, identical to the base. Captured
  alone, `$?` read immediately.
- `scripts/check_map_md.sh` — clean (after the three new maps were added and staged).
- `scripts/check_lib_rs.py`, `scripts/check_rust_file_size.py` — clean; no ceiling moved, which is
  charter clause C-007.

There is no regression pin for this unit and there should not be: the compiler is the proof. A test
asserting that a module resolves would be asserting that Rust works.

## Disposition

**DELIVERED.** Charter clause C-007 held.

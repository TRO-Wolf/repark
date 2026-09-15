# map — repark-core/src/text_scan

## Purpose

File-backed tests for the text scan beside it ([`text_scan.rs`](../text_scan.rs)):
split/wholetext/separator arms, glob and partition-dir walks, limit accounting,
and the missing-path refusal. Split from `text_scan.rs` at the 1000-line
ceiling in IO-TEXT-1 round 5; behavior unchanged by the move.

## Modules

- [`tests.rs`](tests.rs) — the scan battery. pins: io-text-1/C-001, T-1, T-3, T-5, T-6, T-8, W-2, W-4
- [`text_scan.rs`](../text_scan.rs) — round 5 (ruling X-5): the per-file
  partition values ride one `Arc` into every scan partition and `execute`.
  Round 7 (ruling Z-1): `expand_text_paths` takes the session zone so
  inferred timestamps parse session-local.

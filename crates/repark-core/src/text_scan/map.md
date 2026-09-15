# map — repark-core/src/text_scan

## Purpose

File-backed tests for the text scan beside it ([`text_scan.rs`](../text_scan.rs)):
split/wholetext/separator arms, glob and partition-dir walks, limit accounting,
and the missing-path refusal. Split from `text_scan.rs` at the 1000-line
ceiling in IO-TEXT-1 round 5; behavior unchanged by the move.

## Modules

- [`tests.rs`](tests.rs) — the scan battery. pins: io-text-1/C-001, T-1, T-3, T-5, T-6, T-8, W-2, W-4

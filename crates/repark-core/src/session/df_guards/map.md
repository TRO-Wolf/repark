# map — repark-core/src/session/df_guards

## Purpose

The one DataFusion 54.1 guard that is too large to live inside
[../df_guards.rs](../df_guards.rs). That file owns the two small guards (a config default and a
wrapped optimizer rule) and declares this directory.

## Contents

- `window_rescan.rs` — **WIN-SLIDE-1 (2026-09-04):** the `sliding_frame_rescan` analyzer rule.
  Its design note, the DataFusion contracts it reads, and the routes it does not take are in
  [../map.md](../map.md); its pins are `../tests/window_rescan.rs` and
  `python/repark/tests/test_win_slide_1.py`.
  pins: win-slide-1/C-001, C-005

## Pointers

- Up: [../map.md](../map.md)

# map — repark-spark/src/view_ddl/temp_settings

## Purpose

The unit tests of `../temp_settings.rs` (TEMP-VIEW-SESSION-SETTINGS-1 part 1, 2026-11-17), pins (a) to (c) and (e)
of the round 2 critic.

## Contents

- `tests.rs` — the closed allow-list defaults to "sensitive" (pin b); a numeric cast plans the same physical `Cast`
  under ANSI on with `UTC` and ANSI off with `America/Los_Angeles` (pin b, fails if a physical `Cast` starts reading
  `config_options`); the cast-shield and normalize-zero UDFs answer the same under both settings and their sources
  name no `config_options` (pin c); matching settings keep today's path and a differing reader gets an overlay with
  the reader's start time; a nested overlay keeps the outermost reader's zone (pin e); a settings-sensitive conjunct
  runs under the reader's `create_physical_expr` with the limit above it, and leaves unread body columns pruned
  (pin a); free conjuncts enter the body and `current_timezone()` answers the reader's zone.
  `supports_filters_pushdown` unchanged: `../temp_view.rs`'s own tests. Part 2 (2026-11-18) removed the inlined-frame
  fallback and its two tests (delta 1's); the marker's pins are in [../frame_settings/map.md](../frame_settings/map.md).

## Pointers

- Up: [../map.md](../map.md)

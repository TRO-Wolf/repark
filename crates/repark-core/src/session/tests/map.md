# map — repark-core/src/session/tests

## Purpose

Session test modules. `session.rs` declares `#[cfg(test)] mod tests;`.

## Contents

- `mod.rs` — thin index (rustfmt module order).
- `session.rs` — ported v1 session battery plus P2G R2 / A13 / metadata-enumeration pins. RP-5: the bare-session half of the metadata-table enumeration contract (fork F-8 listing); mutation — make `information_schema` expect a `$snapshots` twin and the pin reds. pins: rp-5-fork-repin/C-003
  Child: [session/catalog_registration.rs](session/map.md).
  RP-5: `information_schema` hide pin now cites fork F-8 listing (no engine shim).
  pins: rp-5-fork-repin/C-003
- `aws_gate.rs` — E-2 offline AWS-gate pins.
- `df_guard.rs` — seven DataFusion 54.1 guard pins.
- `namespace_create.rs` — `create_namespace` location-guard pins (G-6 Q1 / R-6).
- `a13.rs` — `file://` warehouse fallback-root pin.
- `pool_refusals.rs` — **H3-SPILL-RESIDUE-1 (2026-09-06):** the wiring pins. A bounded
  `build()` installs a pool that still reports `MemoryLimit::Finite` and now carries a refusal
  log that starts at zero and counts the session's own refusal; `memory_limit_bytes(0)` installs
  no log, so the containment cannot fire on an unbounded session.
  pins: h3-spill-residue-1/C-002
- `window_rescan.rs` — **WIN-SLIDE-1 (2026-09-04):** six capability pins for the
  `sliding_frame_rescan` rule in [../df_guards/window_rescan.rs](../df_guards/window_rescan.rs). The throwaway
  `winslide_probe_sum` UDAF exists only here: it has no `retract_batch`, so it proves the fallback
  fires on an aggregate the rule has never heard of, and its `default_value` is the sentinel
  `-1.0`, so the empty-frame pin distinguishes "fresh accumulator" from "aggregate default".
  Mutation: make the rule probe `accumulator` instead of `create_sliding_accumulator` and
  `a_retractable_aggregate_keeps_datafusions_sliding_accumulator` reds.
  pins: win-slide-1/C-005, C-006

## Pointers

- Up: [../map.md](../map.md)

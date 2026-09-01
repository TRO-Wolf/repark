# map — repark-core/src/session/tests

## Purpose

Session test modules. `session.rs` declares `#[cfg(test)] mod tests;`.

## Contents

- `mod.rs` — thin index (rustfmt module order).
- `session.rs` — ported v1 session battery plus P2G R2 / A13 / metadata-enumeration pins.
  Child: [session/catalog_registration.rs](session/map.md).
  RP-5: `information_schema` hide pin now cites fork F-8 listing (no engine shim).
  pins: rp-5-fork-repin/C-003
- `aws_gate.rs` — E-2 offline AWS-gate pins.
- `df_guard.rs` — seven DataFusion 54.1 guard pins.
- `namespace_create.rs` — `create_namespace` location-guard pins (G-6 Q1 / R-6).
- `a13.rs` — `file://` warehouse fallback-root pin.

## Pointers

- Up: [../map.md](../map.md)

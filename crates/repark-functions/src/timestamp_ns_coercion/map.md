# map — repark-functions/src/timestamp_ns_coercion

## Purpose

ICE-TSNS-COERCION-1 slice 1 round 1 (2026-10-12). The gate behind the Spark door's first
`type_coercion` seat: the parent `timestamp_ns_coercion.rs` holds the wrapper, the one-walk
gate, the two process-wide counters and the fallback log-line builder. This round the wrapper
only counts; no statement is rewritten. The line builder exists because no logging crate is
reachable from `repark-functions` without a manifest edit, so emission wires in a later slice.
The counters move onto the session metrics facade when one exists.

- `tests.rs` — the module's `#[cfg(test)]` suite: the gate truth table (a microsecond-only
  table closed; `timestamp_ns`, `timestamptz_ns`, and nanosecond leaves inside a struct, a
  list and a map open; a written cast and a nanosecond literal closed; both cast kernels
  open), the gate-open counter firing once per open analysis and never for a closed one with
  the fallback counter at zero, the reset, the log-line shape, and the closed gate handing
  the inner rule the plan it was given (same table-source `Arc`, byte-identical plan).
  pins: ice-tsns-coercion-1/C-005

## Pointers

- Up: [../map.md](../map.md)

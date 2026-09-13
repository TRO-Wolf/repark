# map — repark-core/src/silver

## Purpose

Typed `SilverPlan` for the deterministic silver-layer compiler (S-1). Parse, structural
validation, canonical identity, and deterministic explanation only. The module is `pub` from
`repark-core` and is **not** bound into `repark-python`. The contract is unstable until the
owner rules SIL-1..SIL-10.

S-1 does not execute data, lower DataFusion `Expr`, call Iceberg, or mint a cryptographic
digest. Canonical bytes are the identity; `SilverPlanIdentity` is named so a digest can be
added without changing callers.

Field 91 in the §8 example is an order column and is not a payload field in the YAML sketch.
D-3 requires selection keys and order fields to be mapped source ids, so the positive
`crm_contacts` fixture maps 91 as `source_version`. Bronze record id 90 stays on
`input_contract` and is not a silver column.

pins: silver-s1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010

## Contents

- `../silver.rs` — module root: submodule declarations and re-exports. No logic.
- `plan.rs` — `SilverPlan`, `Column`, `parse` over `toml::Table` with key paths, structural
  validation.
- `policy.rs` — closed enums: transforms, validators, quality, selection, publication, target
  types, input contract.
- `identity.rs` — `canonical()` bytes written directly into a reserved `Vec<u8>` (no
  intermediate tree). `SilverPlanIdentity` holds those bytes.
- `explain.rs` — `explain()` stable text. `explain_with_identity` takes already-built
  canonical bytes so later slices walk the plan once: call `canonical()` / `identity()`,
  then `explain_with_identity`. `explain()` is a thin wrapper that builds identity first.
- `refusal.rs` — `SilverRefusal` variants with Display paths.
- [fixtures/](fixtures/map.md) — positive and negative TOML.
- [tests/](tests/map.md) — contract pins.

## Pointers

- Up: [../map.md](../map.md)
- Design: [../../../../task/roadmap/epic-term/deterministic-silver-layer-compiler-2026-09-12.md](../../../../task/roadmap/epic-term/deterministic-silver-layer-compiler-2026-09-12.md)

## Debug

| Symptom | First check |
|---|---|
| Unknown key silently ignored | Every struct denies unknown fields; the walker also refuses unlisted keys with the path. |
| `0.01` and `1e-2` disagree | Both parse to the same `f64`; `canonical()` emits `Display` of that float. |
| Trim after parse_timestamp accepted | Type-position walk in `plan.rs` must refuse `IllegalTransformPosition`. |

First checks: `cargo test -p repark-core silver`.

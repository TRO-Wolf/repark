# map — repark-core/src/silver/fixtures/positive

## Purpose

Positive SilverPlan TOML fixtures for SILVER-S1. See [../map.md](../map.md).

## Contents

- `crm_contacts.toml` — §8 illustrative policy as TOML, plus source field 91 as a mapped
  order column so D-3's unmapped-order-field check holds.
- `crm_contacts_permuted_keys.toml` — same plan, TOML keys reordered.
- `crm_contacts_whitespace_comments.toml` — same plan, comments and blank lines.
- `crm_contacts_inline.toml` — same plan, inline tables instead of `[[arrays]]`.
- `crm_contacts_threshold_scientific.toml` — same plan, `threshold = 1e-2`.
- `row_snapshot_disabled_selection.toml` — row snapshot, selection disabled.
- `decimal_boolean_date.toml` — decimal / boolean / date targets.
- `int32_passthrough.toml` — int32 passthrough.
- `allowed_values.toml` — `check_allowed_values`.
- `quality_zero_threshold.toml` — `threshold = 0.0`, `on_empty = fail`.
- `crm_contacts.explain.txt` — golden `explain()` text for `crm_contacts.toml`.
- `crm_contacts.canonical.txt` — golden `canonical()` bytes recorded before the P2-1 rewrite.

pins: silver-s1/C-002, C-004, C-005, C-007, C-010

## Pointers

- Up: [../map.md](../map.md)

## Debug

First checks: `cargo test -p repark-core silver`.

# map — repark-core/src/dynamic_flatten/tests

## Purpose

Submodules of `../tests.rs`. Split out so the parent stays under the
Rust file-size default ceiling.

## Contents

- `octo.rs` — octo C2/C3 kernel pins: `LargeList` / `FixedSizeList` explode,
  `ListView` / `LargeListView` refuse, max-depth remaining-schema truncation
  (`max_depth_remaining_schema_is_truncated` kills unbounded output —
  token / "truncated" / len — not the join-then-truncate allocation path).

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| LargeList / FixedSizeList stay nested | Pins `large_list_explodes`, `fixed_size_list_explodes`. |
| ListView leave-nested | Pins `list_view_refuses_loud`, `large_list_view_refuses_loud`. |

First checks: `cargo test -p repark-core dynamic_flatten`. Escalate to: [../map.md](../map.md).

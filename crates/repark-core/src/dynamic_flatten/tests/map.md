# map — repark-core/src/dynamic_flatten/tests

## Purpose

Submodules of `../tests.rs`. Split out so the parent stays under the
Rust file-size default ceiling.

## Contents

- `octo.rs` — octo C2 kernel pins: `LargeList` / `FixedSizeList` explode,
  `ListView` refuse, max-depth remaining-schema truncation.

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| LargeList / FixedSizeList stay nested | Pins `large_list_explodes`, `fixed_size_list_explodes`. |
| ListView leave-nested | Pin `list_view_refuses_loud`. |

First checks: `cargo test -p repark-core dynamic_flatten`. Escalate to: [../map.md](../map.md).

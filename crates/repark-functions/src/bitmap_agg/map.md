# map — repark-functions/src/bitmap_agg

## Purpose

FNP-6D Spark bitmap aggregate UDAFs. The parent `bitmap_agg.rs` holds registration,
the per-row `Accumulator`, and the shared fold/set-bit helpers. This directory holds
the grouped path.

## Contents

- `groups.rs` — `BitmapGroupsAccumulator`: one `Vec<u8>` of `n_groups * 4096`,
  in-place OR/AND/`set_bit`, `evaluate`/`state` as one `BinaryArray` from that
  buffer. `convert_to_state` is not implemented (would be 4 KiB per input row).
  pins: fnp-6d/C-016, C-017

## Pointers

- Up: [../map.md](../map.md)

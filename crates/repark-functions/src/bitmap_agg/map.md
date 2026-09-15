# map — repark-functions/src/bitmap_agg

## Purpose

FNP-6D Spark bitmap aggregate UDAFs. The parent `bitmap_agg.rs` holds registration,
the per-row `Accumulator`, and the shared fold/set-bit helpers. This directory holds
the grouped path.

## Contents

- `groups.rs` — `BitmapGroupsAccumulator`: one `Vec<u8>` of `n_groups * 4096`,
  in-place OR/AND/`set_bit`, `evaluate`/`state` as one `BinaryArray` from that
  buffer. `convert_to_state` is not implemented (would be 4 KiB per input row).
  The grouped construct path shares the parent's `construct_positions`, so grouped
  STRING args parse strictly as Spark BIGINT under ANSI-on too.
  pins: fnp-6d/C-016, C-017; fnp-6d-followup-1/C-004
- `tests.rs` — the module's `#[cfg(test)]` suite (moved out of `bitmap_agg.rs` by
  FNP-6D-FOLLOWUP-1 step 2 so the parent keeps the file-size ceiling): the FNP-6D
  answer pins plus the followup refusal/answer pins over the `FU-*` cells, and the
  round-2 `CAST_OVERFLOW` pins over the `FU2-*` cells (global, grouped, window).
  pins: fnp-6d/C-001, C-002, C-003, C-004, C-011, C-013, C-014, C-015;
  fnp-6d-followup-1/C-001, C-002, C-003, C-004

## Pointers

- Up: [../map.md](../map.md)

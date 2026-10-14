# map — repark-iceberg/src/write/struct_pairing

## Purpose

The test half of [`../struct_pairing.rs`](../map.md), the struct pairing seam of
ICE-STRUCT-PAIRING-1 (2026-10-14, parity row R-016).

## Contents

- `tests.rs` — the in-module table test: three modes (by position, by name, by name
  case-sensitive) × eight field shapes × ten layouts (`Struct`, `List`, `LargeList`,
  `ListView`, `LargeListView`, `FixedSizeList`, a map key, a map value, `Dictionary`,
  `RunEndEncoded`) against an independent statement of the rule, NULL structs and NULL
  elements kept; pairing a paired value is the identity in every mode; the leaf pairs and
  their paths; hints H-1 and H-3; the kernel's text, and the kernel refusing at plan time
  and pairing at run time. **Build round 2 (2026-10-14):** the guard of hint H-1 is pinned at
  its call sites, not only in `refuse_unpaired_struct`:
  `the_append_batch_cast_s1_refuses_an_unpaired_struct_with_hint_h1` drives `conform_batch`
  (S1; the Rust `append` API stages through it, and the streaming sink through S1 or S2 by
  way of `stage_overwrite_files_with`) and
  `the_positional_overwrite_cast_s2_refuses_an_unpaired_struct_with_hint_h1` drives
  `positional_map_overwrite_batch` (S2) with and without a column list. S3 is pinned at the
  Spark door by `repark-spark/tests/struct_pairing_doors.rs`.

Up: [../map.md](../map.md).

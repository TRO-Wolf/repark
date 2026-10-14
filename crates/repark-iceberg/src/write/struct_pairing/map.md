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
  and pairing at run time.

Up: [../map.md](../map.md).

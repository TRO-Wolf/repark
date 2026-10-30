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
  **Build round 3 (2026-10-14, ruling 56(a)):** `every_paired_leaf_is_judged_as_a_flat_column_is`
  holds the guard and the leaf judge to Spark's text: an in-order STRING leaf into BIGINT, an INT
  into a STRUCT two deep (`"STRUCT<A:INT,B:INT>"`) on both batch entries and inside a list, a
  widened leaf passing, and a surplus field answering `EXTRA_STRUCT_FIELDS` by position.
  **Build round 3 (2026-10-14, ruling 55(a) F-03):**
  `case_insensitive_names_match_as_java_equals_ignore_case` pins É/é, ẞ/ß both ways, İ/i, ᾀ/ᾈ and
  ſ/s matching and ß/SS, é/e and ſ/ss not, then pairs É into é by name and refuses ß from SS with
  `CANNOT_FIND_DATA`.
  **Build round 3 (2026-10-14, ruling 55(a) F-07):**
  `case_twins_are_ambiguous_by_name_unless_the_session_is_case_sensitive` pins
  `AMBIGUOUS_COLUMN_NAME` for case twins by name and `EXTRA_STRUCT_FIELDS` for the same twins
  case-sensitively (kills verifier 1's mutant O1, the ambiguity check off).

Up: [../map.md](../map.md).

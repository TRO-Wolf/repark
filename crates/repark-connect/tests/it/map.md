# map — repark-connect/tests/it

## Purpose

The crate's one integration binary. Each module pins one product module through the public API.
See [../map.md](../map.md).

## Contents

- `main.rs` — `mod copy_binary; mod postgres_types; mod settings;`.
- `settings.rs` — C-1 (2026-10-05): absent and explicit `password` (the other props carried,
  `auth_method` dropped from the map); `iam_token` and `kerberos` refuse naming their registry
  rows and fold to the Unsupported class; empty, wrong-case, hyphenated, padded and unknown
  values are invalid specifications folding to the IllegalArgument class; every spelling round
  trips; `Debug` never renders a prop value. Since C-2a (2026-10-06) the errors are
  `ConnectError`'s; no assertion changed.
  pins: c-1/C-003, C-004, C-005, C-006 · pins: c-2/C-001
- `copy_binary.rs` — C-2a (2026-10-06), the stream half of sketch §5.1, through
  `CopyBinaryDecoder` with hand-built streams: `copy_header_is_the_signature_flags_and_extension`
  (every signature byte flipped, low flag bits ignored, the extension skipped),
  `copy_oid_flag_refuses` (bit 16, the critical bits, a negative extension),
  `copy_trailer_ends_and_trailing_bytes_refuse` (in the same chunk and in the next),
  `copy_truncated_stream_is_disconnected` (cuts in the header, mid-tuple and before the
  trailer), `copy_decode_is_independent_of_chunking` (one three-tuple stream with every mapping
  and an all-NULL tuple, split at every byte offset, in one-byte and three-byte chunks, equal to
  the one-chunk decode), `copy_field_count_must_match_projection` (and the empty projection
  that counts rows), `copy_fields_refuse_bad_lengths_nulls_and_values_naming_the_column`, and
  `batches_flush_at_rows_and_at_bytes` (3 + 3 + 1 at `rows = 3`; a 64 MiB `bytea` flushes alone
  and leaves the rest of the chunk with the caller). The C-2a F-1 fold (2026-10-06) adds
  `copy_field_longer_than_postgres_max_refuses` (`0x7fffffff` and `MAX_FIELD_BYTES + 1`,
  header-only, refused before the payload), `copy_field_at_postgres_max_is_accepted` (exactly
  the maximum passes the length word with no payload fed), and
  `batch_byte_cap_saturates_at_max_batch_bytes` (`usize::MAX` saturates, smaller caps pass
  through).
  pins: c-2/C-002, C-003, C-004, C-005, C-006, C-015
- `postgres_types.rs` — C-1 (2026-10-05): one round-trip pin per mapped row, named in the row
  (`bool_round_trips` … `bytea_round_trips`): Arrow array → wire values → Arrow array, equal,
  over NULLs and boundary values (`MIN` / `MAX`, `-0.0`, the infinities and NaN compared by bit
  pattern, multibyte UTF-8, invalid-UTF-8 bytes in `bytea`), plus one byte-exact wire check per
  integer width, and (verifier V-1, 2026-10-05) a big-endian wire anchor for `float4` / `float8`
  in both directions, so a symmetric little-endian codec is red. `declared_types_refuse_naming_their_row` holds the declared list and each
  row's refusal. The error pins cover wrong wire length, invalid UTF-8 and a wrong Arrow type.
  `type_map_has_one_row_per_type_and_a_live_pin_per_row` reads this file through
  `include_str!`, so a row whose `pin` names no test here is red.
  pins: c-1/C-007, C-008
  - **C-2a (2026-10-06)**, the type-map half of sketch §5.1, wire → Arrow over the §2.7 byte
    anchors computed from the documented formats: `date_anchors_round_trip`,
    `timestamp_ntz_anchors_round_trip` and `timestamptz_anchors_round_trip` (the two anchors,
    the last representable µs, the first one past it, both infinities, the zone label);
    `numeric_anchors_round_trip` (`12345.678` as `numeric(8,3)`, `-0.5` as `numeric(2,1)`, the
    38-digit maximum, scale padding, zero, a short value, a digit out of range),
    `numeric_typmods_resolve_to_spark_decimal_types` (D-M2 DM2-T05…T10 types, the T08
    `1.5` → 2 and T05 half-up values, the T10 refusal, the kept `p > 1000` refusal),
    `numeric_special_values_refuse`,
    `unconstrained_numeric_rounds_half_up_at_scale_18` (`5e-19` → `1e-18`, its negative, and
    `4.999e-19` → 0), `bounded_numeric_overflow_refuses` (21 integer digits, a carry from
    rounding past 38 digits, 10^38 into `(38,0)`, an absurd weight);
    `uuid_renders_lowercase_canonical`, `jsonb_strips_version_one_and_refuses_others`,
    `json_and_interval_are_text_verbatim`; `unmapped_types_refuse_at_resolution` (the
    `pg-unmapped` and `pg-time` refusals, enums as labels), 
    `new_mappings_wait_for_the_write_path_to_encode`, `decode_errors_fold_by_class`. The declared
    list in `declared_types_refuse_naming_their_row` is `time` alone.
    pins: c-2/C-007, C-008, C-009, C-010, C-011, C-012

## Pointers

- Up: [../map.md](../map.md)

## Debug

First checks: `cargo test -p repark-connect --test it`. Escalate to: [../map.md#debug](../map.md).

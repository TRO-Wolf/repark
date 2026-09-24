# map — repark-core/src/iceberg_path

## Purpose

Tests for `../iceberg_path.rs` — the `format("iceberg").load(<path>)` static-table read
(U10 / R-DF-LOAD-PATH + R-DF-LOAD-METADATA-JSON, 2026-09-23).

## Contents

- `tests.rs` — the resolver and path-door battery, in these families:
  - the metadata-location picker: `NNNNN-<uuid>.metadata.json` selects the highest numeric
    prefix (00010- beats 00002-), `v<N>.metadata.json` the higher N (v10 beats v9), a whole
    ASCII-digit `v` stem with a lower-case `v` and a canonical UUID suffix are required
    (exactly 36 characters, hex of any case, dashes only at 8/13/18/23), names outside the
    two forms are ignored, and each valid form parses to its version (`numbered_*`,
    `v_form_*`, `names_outside_*`, `valid_forms_*`, `each_valid_form_*`, `uuid_dashes_*`,
    `uuid_suffix_length_and_case_edges`, `upper_case_v_prefix_*`);
  - hint and listing: `version-hint.text` wins over the listing (a hint of `0` selects `v0`),
    a hinted file must exist, an unparsable, non-UTF-8, negative or `u64`-overflowing hint
    falls back to the listing (`hint_zero_*`, `hint_below_*`, `hint_beyond_u64_*`), a trailing-slash location
    resolves through the listing, and an explicit `.metadata.json` resolves verbatim
    (`version_hint_*`, `hinted_*`, `unparsable_*`, `non_utf8_*`, `listing_*`,
    `explicit_metadata_path_*`);
  - direct children only: nested `metadata/sub/…` and `metadata/metadata/…` files are not
    candidates under `/abs`, `/abs/` and `file:///abs` roots (`nested_*`,
    `only_nested_*`);
  - path-naming refusals, compared in full with `assert_eq!`: the missing location, the
    empty `metadata/` dir, the only-nested dir, a dir of only non-candidate names and the
    hinted-missing file (`missing_location_*`, `empty_metadata_dir_*`, `only_nested_*`,
    `non_candidate_metadata_names_*`, `hinted_*`);
  - `file://<authority>` Wrong FS refusals, compared in full, keeping one of two trailing
    slashes (`file_authority_*`, `file_localhost_*`); the upper-case `FILE://` and
    `File://localhost` spellings are not refused Wrong FS and give the no-metadata text
    (`upper_case_file_authority_*`);
  - `file:` spellings: `file:/abs`, `FILE:/abs`, `fIlE:/abs`, `FILE:///abs`, `fIlE:///abs`,
    `file:/abs/metadata/<latest>` and `file:///abs` read a real two-row table built by
    `session_with_rows` (`file_single_slash_*`, `file_upper_case_*`, `file_mixed_case_*`,
    `file_triple_slash_*`); `file:<relative>` refuses the full `URISyntaxException` text,
    keeping one of two trailing slashes (`file_relative_*`, `relative_file_path_*`), while
    `File:<relative>` and `FILE:<relative>` give the `malformed storage location` text
    (`upper_case_relative_file_path_*`); and a normalised
    spelling's refusal names the supplied argument
    (`file_single_slash_missing_location_*`);
  - the numbered form's `u64` range: `00000-` is a candidate, `4294967296-` (above `u32`)
    and `18446744073709551615-` parse, `18446744073709551616-` is ignored, and an above-`u32`
    number beats a small one (`numbered_form_*`);
  - the Java `int` version range: `v0` is the lowest candidate, `v2147483648` is not a
    candidate, `v2147483647` is the highest, a `u64`-overflowing stem stays ignored, a hint
    of `2147483647` selects that file, and a hint beyond the range falls back to the
    listing (`v_form_zero_*`, `v_form_versions_beyond_*`, `v_form_java_int_max_*`,
    `v_form_u64_overflow_*`, `hint_at_java_int_max_*`, `hint_beyond_java_int_*`).
  pins: dfload-1/C-002, C-003, C-005, C-008, C-009, C-010

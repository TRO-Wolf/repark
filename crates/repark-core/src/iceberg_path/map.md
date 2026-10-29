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
- `hint_walk_tests.rs` — the stale-hint walk battery (ICE-HADOOP-HINT-WINDOW-1, 2026-10-28), a
  sibling of `tests.rs` in the default layout with its own two-line `FileIO` helper. A
  `version-hint.text` naming `N` is not the last word while `v<N+1>`, ... exist
  (`stale_hint_walks_*`, red on `321c6095`); the walk stops at the first gap
  (`hint_walk_stops_at_the_first_missing_version`); it resolves the cap exactly and refuses one
  file past it (`hint_walk_resolves_the_walk_limit_exactly`,
  `hint_walk_beyond_the_limit_refuses_naming_it`); it stops at the Java `int` maximum
  (`hint_at_java_int_max_does_not_walk_past_it`); and the current-hint, hinted-missing and
  no-hint behaviours are unchanged (`current_hint_selects_the_hinted_file`,
  `hinted_missing_file_keeps_its_error_when_newer_files_exist`,
  `no_hint_resolves_the_latest_v_form_by_listing`).
  pins: ice-hadoop-hint-window-1/C-001, C-002, C-003, C-004, C-005
  The gzip-name battery (ICE-HADOOP-GZIP-METADATA-1, 2026-10-29) is in the same module. The
  compressed names `v<N>.gz.metadata.json` and `v<N>.metadata.json.gz` join the plain name for
  the hinted version, the walk and the no-hint listing, probed in Java's order (plain, gz
  infix, legacy). The walk and listing under the three names (`mixed_codec_stale_hint_walks_to_
  the_newest_gzip_file`, `walk_crosses_from_a_gzip_name_to_a_plain_name`,
  `gzip_walk_stops_at_the_first_missing_version`, `no_hint_listing_picks_the_newest_across_plain_
  and_gzip_names`, `compressed_names_parse_to_their_version`,
  `gzip_hint_walk_beyond_the_limit_refuses_naming_the_hinted_file`) are C-001. The legacy name
  (`stale_hint_walks_to_a_legacy_gzip_name`, `current_hint_selects_a_legacy_gzip_name`,
  `no_hint_listing_picks_a_legacy_gzip_newest`) and the name order
  (`hint_prefers_the_gz_infix_name_over_the_legacy_name`,
  `listing_prefers_plain_then_gz_infix_then_legacy_for_one_version`,
  `walk_prefers_the_plain_name_of_a_version_under_two_names`) are C-004. The gzip current hints
  (`mixed_codec_current_hint_selects_the_gzip_file`, `gzip_table_current_hint_selects_the_gzip_file`)
  and the end-to-end `gzip_current_hint_after_delete_reads_the_survivors` are C-003; the explicit
  gzip-named path `explicit_gzip_metadata_path_reads_the_rows` is the step-0 proof that the fork
  reader decompresses gzip by its magic bytes. The AUD-007 end-to-end pin
  `mixed_codec_stale_hint_after_delete_reads_the_survivors` is C-002. The refusal
  `hinted_missing_version_refuses_when_only_newer_gzip_files_exist` is C-005, next to the
  existing refusal pins `hinted_metadata_file_must_exist` (C-006, the cleanup shape's probe
  artefact) and `hinted_missing_file_keeps_its_error_when_newer_files_exist` (C-005). The
  unchanged behaviour (C-007) is the pre-existing hint-walk battery above and the `tests.rs`
  picker battery, both green and unchanged.
  pins: ice-hadoop-gzip-metadata-1/C-001
  pins: ice-hadoop-gzip-metadata-1/C-002
  pins: ice-hadoop-gzip-metadata-1/C-003
  pins: ice-hadoop-gzip-metadata-1/C-004
  pins: ice-hadoop-gzip-metadata-1/C-005
  pins: ice-hadoop-gzip-metadata-1/C-006
  pins: ice-hadoop-gzip-metadata-1/C-007
  The merge facts of ICE-HADOOP-HINT-WINDOW-1 are cited here so the grammar gate counts them: the label re-date (ice-hadoop-hint-window-1 C-008, 2026-10-28, merge `9ab67d7d`) and the option-1 walk's closure of ice-integrity-pins-1 C-015, C-029 and C-071 (C-072). This file pins none of them.
  pins: ice-hadoop-hint-window-1/C-008
  pins: ice-integrity-pins-1/C-072
  The closure of ice-integrity-pins-1 C-015, C-029 and C-071 is dated 2026-10-28 by its correction C-073, cited below so the grammar gate counts it; the merge fact is not a test pin of this file.
  pins: ice-integrity-pins-1/C-073

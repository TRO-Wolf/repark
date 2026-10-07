# map — repark-connect/tests/it

## Purpose

The crate's one integration binary. Each module pins one product module through the public API.
See [../map.md](../map.md).

## Contents

- `main.rs` — `mod copy_binary; mod ident; mod postgres_types; mod settings; mod url;`, plus
  `mod explain; mod live_pg; mod live_pool; mod live_pushdown; mod pool; mod pushdown; mod read;
  mod scan; mod tls;` under the `postgres` feature.
- `explain.rs` — C-2c (2026-10-07), behind `postgres`, no network: sketch §5.3 over the
  `pushdown.rs` fixture's injected resolution. `explain_renders_pushed_and_residual_per_scan`
  (the exact `PostgresScanExec` line for one pushed and one residual conjunct, and for a pushed
  limit), `explain_verbose_shows_placeholders_never_values` (`remote_sql` carries
  `current_setting('repark.p0')` and `bound_values=1`, never the literal; the default format
  has no `remote_sql`), `explain_never_renders_endpoint` (default, verbose and tree formats and
  `Debug` hold no host, port, database, user, password or `sslmode`) and
  `explain_residual_matches_filter_exec_above` (the `FilterExec` above the scan is the physical
  form of `residual_filters`; an exact filter leaves none). D-M6's pin,
  `datafusion_hands_inexact_filters_to_scan_and_withholds_limit`, drives DataFusion 54.1 with a
  recording provider. `listing_a_postgres_source_is_empty_and_declared` pins
  `CONNECT-DECL-pg-listing` and a missing endpoint refusing at first resolution.
  pins: c-2/C-068, C-069, C-075
- `pushdown.rs` — C-2c (2026-10-07), behind `postgres`, no network. The fixture: an `orders`
  resolution with every mapped type, a `PostgresSource` whose props hold a fake endpoint and
  password, and `FixedZone`, a fixed-offset `WallClockLocaliser` (`-05:00`); `split` plans a
  statement through DataFusion, so each pin reads the coerced shape a real query hands the
  provider. Sketch §5.2's unit halves: `p01_null_tests_push` … `p11_limit_pushes_only_without_residual`
  (with `p06b_…` and `p06c_…`) and `r01_float_comparisons_stay_residual` …
  `r05_pushdown_predicate_false_pushes_nothing`, each asserting the class, the rendered SQL and
  the bound texts; `pushed_values_past_1024_fail_the_plan`; `a_limit_past_i64_max_never_pushes`
  (fold 1: `skip + fetch` past `i64::MAX` pushes no limit);
  `pushdown_limit_gates_the_limit_and_pushdown_predicate_the_filters` (fold 1: each switch gates
  its own push); and
  `a_filter_the_optimizer_would_still_rewrite_stays_inexact` (`qty <> NULL` renders but stays
  `Inexact`). pins: c-2/C-070, C-071, C-072, C-073, C-074, C-077, C-078, C-081, C-082
- `live_pushdown.rs` — C-2c (2026-10-07), behind `postgres`, live like `live_pg.rs` (whose
  `Cell` it shares). Each cell seeds `edges`, one table holding every edge sketch §5.2 names
  (an all-NULL row; the integer extremes; `numeric(10,2)` and unconstrained `numeric` with a
  value past scale 18; `4714-11-24 BC` and `5874897-12-31`; the earliest and latest instants;
  `NaN` and `-0` in `float8`; a padded `char(5)`; mixed-case, accented, `%` and `\` text; an ICU
  `und` column and a case-insensitive nondeterministic one; `uuid`, an enum, `interval`; and
  twenty rows that make a pushed `LIMIT` visible), mounts it twice through
  `PostgresSource::mount`, with `pushdown_predicate` on and off, and asserts for every filter
  that the two return the same ids, that the pushed and residual counts are the expected ones,
  and that the unpushed half pushed nothing. Sketch §5.2's live halves are
  `p01_null_tests_push_live` … `r05_pushdown_predicate_false_pushes_nothing_live`; beside them
  `timestamp_columns_are_placed_in_the_session_zone_live` (the `-05:00` fixture zone moves a
  wall clock by five hours), `explain_analyze_reports_rows_bytes_and_time_per_scan_live` (the
  five metrics on the scan line, and from the executed plan's `MetricsSet` six rows, more than
  the 19-byte COPY header received and a non-zero time to first byte),
  `a_batch_past_the_memory_pool_is_resources_exhausted_live` (a 16 KiB pool refuses the first
  8192-row batch, naming the `PostgresScan` consumer; the default pool reads all 50 000 rows),
  `pushdown_limit_is_its_own_switch_live` (fold 1: `pushdown_limit = false` pushes the filter and
  no limit, `pushdown_predicate = false` pushes the limit of an unfiltered read),
  `a_limit_past_i64_max_reads_every_row_live` (fold 1: `LIMIT 9223372036854775807 OFFSET 5`
  over thirty rows reads twenty-five, pushdown on and off),
  `a_missing_relation_is_table_not_found_live` (DataFusion's own table-not-found, and a 64-byte
  name refusing with `PgIdent`'s reason) and `pushed_compare_keeps_the_index_live` (D-M3: a pushed
  `id OPERATOR(pg_catalog.=) current_setting('repark.p0')::int4` over one million rows plans as
  an index-only scan). pins: c-2/C-069, C-071, C-072, C-073, C-076, C-077, C-079, C-081, C-082
- `url.rs` — C-2b fold 1 (2026-10-07), pure: the `url` parse and the redaction seam against the
  verifier's shapes. `the_userinfo_ends_at_the_last_at_before_the_first_slash` (the verifier's
  `u:S3CRET?leakedfragment=1@h`, and `@`, `#`, `:` and `?a=b&c` in a password, each raw and
  percent-encoded, with and without a path and query; `%2F` for `/`; since fold 2 a raw `?`
  or `=` without a path refuses), `a_userinfo_holding_a_query_mark_without_a_path_refuses`
  (fold 2 Z4: the re-verify's `postgresql://h?user=u&password=S3@CRETpw`, on both doors,
  refuses `AmbiguousUserinfo` naming only "the userinfo in `url`"; with a path, `%3F` and `%3D`,
  or a bare `@` it parses),
  `no_userinfo_or_password_text_is_echoed_by_a_refusal` (a raw `/` in a password, malformed
  escapes, a conflict, an unknown query key after a `password=` value on the `jdbc:` door; no
  fragment of the userinfo or a password in `Display` or `Debug`),
  `a_percent_escape_needs_two_hex_digits` (`%+A`, `%-1`, `% A`, `%A@`, `%g0`, in the password,
  the user and a query value) and `redaction_masks_every_password_the_parser_takes`
  (`pass%77ord`, `PASS%57ORD`, the leaked-fragment userinfo, a userinfo without `:`).
  pins: c-2/C-049, C-050, C-051, C-065
- `live_pool.rs` — C-2b fold 1 (2026-10-07), behind `postgres`, live like `live_pg.rs` (whose
  harness it shares). `a_pooled_connection_is_reset_before_reuse` (the verifier's four poison
  shapes on `pool_max_size = 1`: `IntervalStyle`, `default_transaction_read_only`,
  `search_path` with a schema operator, an advisory lock, and since fold 2 `role` and
  `session_authorization` set to a `NOLOGIN` role and the re-verify's `vprep` (a plpgsql
  function's `PREPARE vprep` and `PREPARE "v Prep""q"`, both gone on the next lease); each
  next lease runs on the same backend
  with clean answers, the last two reading `current_user` = `session_user` = the login and
  `role` = `none`), `user_types_resolve_after_a_reset` (two enums in query mode on
  one reused connection; `DISCARD ALL` fails it with `26000`),
  `a_pushed_scan_commits_before_its_connection_is_pooled` (V-M2's pin: the backend is `idle`,
  not `idle in transaction`, between two pushed scans on one backend),
  `a_wrong_password_is_authentication_failed`, `a_timeout_or_a_drop_ends_the_server_work`
  (three read timeouts at 300 ms each end the backend within 400 ms, which only the cancel
  does; a scan dropped while the server computes ends within 3 s; the check interval shows
  `1s`), `query_mode_resolves_unqualified_names_through_the_role_search_path` and
  `a_user_operator_cannot_shadow_a_generated_compare` (a `public` `=` on a domain and `<` on
  `int4`, dropped after the reads). Fold 2:
  `query_mode_reads_the_configured_search_path_as_a_plain_session_does` (the verifier's
  `app-schema` shape in a fresh database: a role-level path beats a database-level one, a
  role-in-database path beats both, and with none the built-in path reads `public`; each time
  query mode reads the rows a plain session as that login reads, and a pushed compare matches)
  and `a_pushed_enum_compare_orders_by_text` (Z5, CONNECT-DIV-pg-enum-compare: over `('sad',
  'ok', 'happy')` a pushed `> 'ok'` returns `sad` in relation and query mode, where the server's
  enum order returns `happy`).
  pins: c-2/C-052, C-053, C-054, C-055, C-056, C-062, C-063, C-064, C-066
- `scan.rs` — C-2b fold 1 (2026-10-07), behind `postgres`, no Postgres server: a loopback fake
  backend answers the startup, the prepare and a one-row binary COPY.
  `a_copy_stream_without_its_trailer_fails_the_scan` (with the trailer `scan()` reads `[7]`;
  without it the scan fails `Disconnected`; the fake refuses the release query, so nothing is
  pooled). pins: c-2/C-059
- `read.rs` — C-2b round 3 (2026-10-07), behind `postgres`, pure (no server):
  `statement_casts_every_column_and_server_text_to_text` (the exact `COPY` text for `int4`,
  `numeric(8,3)`, `interval`, `jsonb`, an enum, unconstrained `numeric` and a negative-scale
  `numeric(5,-2)` under a quote-bearing name; `interval` and the enum cast to
  `pg_catalog.text`), `pushed_values_ride_set_config_never_the_statement_text` (a projection,
  two values, one injection-shaped, and a `LIMIT`; the values appear only in the settings and
  the bound `set_config` call), `param_slots_stop_at_1024_and_bad_indexes_refuse`,
  `query_mode_wraps_the_statement_and_an_empty_projection_selects_nothing`,
  `dbtable_parses_exact_qualified_and_quoted_parts`, `servers_older_than_14_are_declared` and
  `read_errors_name_the_relation_and_fold_as_operational`. Fold 1 renders every compare as
  `OPERATOR(pg_catalog.op)` and pins `ScanSource::search_path()` for both modes.
  pins: c-2/C-038, C-039, C-044, C-045, C-048, C-056, C-062
- `live_pg.rs` — C-2b round 3 (2026-10-07), behind `postgres`. Every cell is
  `#[ignore = "live: make pg-up, REPARK_PG_URL"]`, panics rather than skips without
  `REPARK_PG_URL`, creates and drops a schema `c2_<tag>`, and names its pool's
  `application_name` `repark_<tag>`, so `pg_stat_activity` sees only the cell's backends. The
  cells connect with `sslmode = disable` (CONNECT-DIV-pg-sslmode). Sketch §5.6:
  `backend_killed_mid_copy_is_disconnected` (F-1), `stream_dropped_mid_copy_closes_the_backend`
  (F-2), `idle_read_timeout_fires` (F-3, a stall before the first row and one mid-stream),
  `query_timeout_is_the_server_statement_timeout`, `lock_timeout_fires` (F-4),
  `pool_exhaustion_times_out` (F-5),
  `schema_drift_between_plan_and_scan_fails_loud_or_stays_typed` (F-7: a widened column read
  back typed, a retyped one failing `22P02` mid-stream with the client never pooled, a dropped
  table `RelationNotFound`), `scan_is_read_only_and_idempotent` (F-8),
  `query_pool_connections_are_never_replication_connections`,
  `missing_select_grant_names_the_privilege` and
  `plaintext_server_refuses_under_the_default`. The server-generated round trips:
  `server_bytes_are_the_wire_anchors` (the eleven sketch §2.7 anchors read back from
  `COPY (SELECT <literal>) TO STDOUT (FORMAT BINARY)`) and
  `mapped_types_round_trip_through_the_scan`; plus
  `relation_discovery_resolves_domains_nullability_and_collation`. The long streams use
  `generate_series` in the target list, which streams; in `FROM` it materialises first. Since
  fold 1 the harness (`Cell`, `Reader`, `url`, `int32s`) is `pub(crate)` for `live_pool.rs`,
  and the drift cell stands for `CONNECT-DECL-pg-drift-cast` (a narrowing cast rounds,
  recorded, not pinned). pins: c-2/C-040, C-041, C-042, C-043, C-046, C-047, C-057
- `settings.rs` — C-1 (2026-10-05): absent and explicit `password` (the other props carried,
  `auth_method` dropped from the map); `iam_token` and `kerberos` refuse naming their registry
  rows and fold to the Unsupported class; empty, wrong-case, hyphenated, padded and unknown
  values are invalid specifications folding to the IllegalArgument class; every spelling round
  trips; `Debug` never renders a prop value. Since C-2a (2026-10-06) the errors are
  `ConnectError`'s; no assertion changed. C-2b round 1b (2026-10-07) adds sketch §5.5 through
  `PostgresSettings::from_props`: `every_endpoint_key_parses_and_unknown_keys_refuse` (every
  canonical key, the defaults, the libpq, `postgres://` and `jdbc:` URL forms, the per-key
  ranges, a misspelt key that lists the accepted keys and echoes no value),
  `aliases_are_case_insensitive_and_conflicts_refuse`, `alias_units_convert`,
  `sslmode_default_is_verify_full`, `unverified_sslmodes_refuse`,
  `declared_keys_refuse_naming_their_row`, `read_timeout_zero_refuses`,
  `redact_source_prop_masks_url_credentials` and `settings_debug_never_renders_a_value` (C-1's
  C-006 extended to `PostgresSettings` and every new error). A local `summary()` renders every
  field, so one assertion covers a parsed value. C-2b round 2 (2026-10-07) adds
  `fetchsize_zero_is_the_session_batch_default` (`fetchsize` `0` and `00` on `read_postgres`
  leave `batch_rows` unset; `batch_rows` `0` refuses on both doors). Fold 1 (2026-10-07) changes
  two expectations: an unknown key in the `url` query refuses as `UrlPart("query key")`.
  pins: c-1/C-003, C-004, C-005, C-006 · pins: c-2/C-001, C-018, C-019, C-020, C-021, C-022,
  C-023, C-024, C-036
- `tls.rs` — C-2b round 2 (2026-10-07), behind `postgres`: in-memory rustls handshakes (no
  socket) between `verify_full_config` and a `ServerConfig` over the fixtures.
  `verify_full_trusts_sslrootcert_and_checks_the_host_name` (the `localhost` leaf verifies
  under `ca.pem`; `db.example.com` refuses `NotValidForName`; under `other-ca.pem` it refuses
  `UnknownIssuer`) and `sslrootcert_must_be_a_readable_pem_ca_bundle` (a missing file, a
  key-only PEM and a garbled certificate each refuse, naming `sslrootcert` and never the path,
  in the `Base` class). `fixture` and `server_config` are shared with `pool.rs`. Fold 1 adds
  `a_bundle_with_one_unusable_certificate_refuses` (the CA fixture plus a block rustls ignores;
  V-M7's pin). pins: c-2/C-028, C-029, C-058
- `pool.rs` — C-2b round 2 (2026-10-07), behind `postgres`. The pool through `FakeConnector`
  (each fake connection holds a pending task, so an abort is observable):
  `a_clean_release_is_reused_by_the_next_checkout`,
  `a_lease_dropped_before_release_aborts_its_connection` (F-2's unit half),
  `checkout_beyond_pool_max_size_is_pool_exhausted` (F-5's unit half: 100 ms, then the permit
  returns) and `closed_and_idle_expired_connections_are_never_reused`. The startup packet:
  `query_config_pins_the_session_in_the_startup_packet` (every `Config` field and the exact
  `-c` list, with and without the defaults; fold 1 adds `client_connection_check_interval`). The connector against loopback listeners, with no
  Postgres server: `connect_timeout_bounds_a_server_that_never_answers` (under `disable` and
  `verify-full`), `plaintext_server_refuses_under_verify_full` (a listener that answers `N`),
  `a_refused_port_is_unreachable`, and
  `verify_full_refuses_an_untrusted_or_misnamed_server_certificate` (a thread that answers `S`
  and completes a rustls handshake as the `localhost` leaf, reached at `127.0.0.1`). Fold 1
  awaits `release_clean()`; the fakes take `reset`'s and `canceller`'s defaults. An idle
  backend that dies just before checkout is still handed out (FL-11, accepted).
  pins: c-2/C-031, C-032, C-033, C-034, C-035, C-060, C-061, C-067
- `fixtures/` — C-2b round 2 (2026-10-07): static PEM test identities, generated once with the
  local `openssl` (EC P-256, valid to 2126) because `rcgen` is not in the lock: `ca.pem`;
  `server.pem`, a `localhost` leaf it signs, and `server.key`, its PKCS#8 key; and
  `other-ca.pem`, a CA that signs nothing the tests trust. The key protects nothing.
- `ident.rs` — C-2b round 1b (2026-10-07): `identifiers_render_double_quoted_with_quotes_doubled`
  (an embedded `"`, a lone `"`, an injection-shaped name, a qualified relation) and
  `identifiers_refuse_empty_nul_and_more_than_63_bytes` (63 ASCII bytes and 62 bytes of `é` pass;
  64 bytes, as 64 ASCII or 32 `é`, refuse with the IllegalArgument class). pins: c-2/C-025
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
  through). The C-2a F-4 fold (2026-10-06) adds
  `carry_releases_capacity_past_the_byte_cap` (one 64 MiB `text` field in 64 KiB chunks under
  a 1 MiB cap, then a small row; `buffered_bytes()` stays under the cap after the flush).
  The C-2a F-5 fold (2026-10-06) adds `null_rows_charge_their_builder_bytes` (one million
  all-NULL `numeric,timestamp,text` rows under a 1 MiB cap flush more than once).
  The C-2a F-6 fold (2026-10-06) adds `batch_flushes_at_the_exact_byte_cap` (two `int4`
  rows under a 5-byte cap flush 1 + 1) and `copy_critical_flag_bits_each_refuse` (bits 18,
  24 and 30 each refuse). The C-2a F-7 fold (2026-10-06) adds
  `decoder_is_poisoned_after_an_error` (an OID refusal, then a valid chunk answers the same
  error unread, and so does `finish`).
  pins: c-2/C-002, C-003, C-004, C-005, C-006, C-015, C-016, C-017
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
    `4.999e-19` → 0), `numeric_group_at_exponent_minus_four_rounds_half_up` (`7.5` → 8 at
    `numeric(8,0)`, both signs, the dropped group at exponent -4),
    `bounded_numeric_overflow_refuses` (21 integer digits, a carry from
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

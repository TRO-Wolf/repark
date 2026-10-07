# Unit ledger — SOURCE-URL-REDACT-1 · a password inside a URL-shaped property value is never displayed

**Date:** 2026-10-06 · **Branch:** `fix/source-url-redaction-1` · **Base:** `4a643e56` (`main`)
· **Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: high** (security, release item).

**Order:** the orchestrator's brief SOURCE-URL-REDACT-1, on the owner's word of 2026-10-06
~06:25 EDT: "Yes go ahead immediately, we need security to be tight." A security fix to main for
v1.5.3 ([the card](../../roadmap/mid-term/v1-5-3-card-2026-10-04.md)).

**The defect.** `redact_value(key, value)` masked a value by its key name only, so
`url = "postgresql://alice:S3cretPw@db.example.com:5432/sales"` came back verbatim from
`sources()`, the `SourceSpec` / `CatalogSpec` `Debug`, the config dump and `spark.conf.getAll`.
It shipped in v1.4.1 (CFG-2 step 1, #551, 2026-09-13); every release since is affected.

**Out (round 1):** table properties, the `SET` regex's `url` gap, the typed-knob refusals and
the `repark-iceberg` spans. **Fold 1 (orchestrator, 2026-10-06 ~07:25)** brought all four in
(C-015..C-019) and added the provider-`Debug` audit (C-020); still out: the `repark-functions`
boolean-knob refusals (no `repark-common` edge, §4) and the `repark-distributed` finding (§6,
audit only).

## PROPOSITION LEDGER — SOURCE-URL-REDACT-1 — 2026-10-06

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `repark_common::redaction::mask_value_credentials` masks the password of a URL userinfo for every scheme (`postgresql`, `jdbc:postgresql`, `postgres`, `mysql`, `sqlserver`, `redis`, `http(s)`, `s3`, `thrift`, any `scheme://`), keeping the user, the host, the port and the database (`postgresql://alice:***@db.example.com:5432/sales`); a userinfo with no colon is masked whole (`https://***@h/x`); every URL in a value is masked. | `url_userinfo_password_is_masked_keeping_user_host_and_database`, `url_userinfo_is_masked_for_every_scheme`, `lone_userinfo_is_masked_whole`, `every_url_in_a_value_is_masked`. | PROVEN | §2: `cargo test -p repark-common --lib` green (10 redaction pins). |
| C-002 | Fail closed inside the authority (fold 1 item 1 replaces the round-1 region rule): userinfo is read only inside the RFC 3986 authority, which ends at the first `/`, `?` or `#` after `scheme://`; inside it, everything before the last `@` is userinfo, and a userinfo whose user part carries an `@` is masked whole (`https://a@b:c@h/x` → `https://***@h/x`). | `unclean_userinfo_inside_the_authority_fails_closed`. | PROVEN | §2 green. Consequence, by the fold's ruling: a password carrying an unencoded `/`, `?` or `#` ends the authority early and is no longer masked (`postgresql://alice:pa/ss@h/db` stays as written); libpq and pgjdbc also stop the authority there, so such a URL does not authenticate as written either. |
| C-003 | A query-string parameter whose name is secret by `prop_key_is_secret`, or whose compact name is `sig`, ends in `pwd` or ends in `signature`, has its value masked (`password`, `sslpassword`, `access_token`, `X-Amz-Credential`, `X-Amz-Signature`, `sig`); a JDBC SQL Server `;password=` property likewise; other parameters stay. | `secret_query_parameters_are_masked`. | PROVEN | §2 green. |
| C-004 | A libpq or ODBC keyword DSN masks the value of a secret keyword (`password`, `Pwd`, `PWD`), with or without spaces around `=`; a quoted (`'…'` with `\'`, `"…"`) or braced (`{…}` with `}}`) value is masked whole; an unquoted value runs to the next delimiter (`&`, `;`, whitespace) that starts another `name=` pair, so `password=pa&ss;word dbname=d` masks `pa&ss;word`; an unterminated quote masks to the end. | `keyword_dsn_secrets_are_masked`. | PROVEN | §2 green. |
| C-005 | Non-secret values are untouched: a URL with no userinfo, a URL with only non-secret parameters, `s3://bucket/path`, an email-like `alice@example.com`, a Windows path, a non-secret keyword DSN, a class name, `a=b=c`, the empty string and an empty `password=`. | `non_secret_values_are_untouched`. | PROVEN | §2 green. |
| C-006 | `redact_value` keeps the key-name rule as it was (a secret key's value is `***`) and adds the value rule for every other key; `prop_key_is_secret` moves to `repark-common::redaction` byte-identical in logic and stays exported as `repark_core::prop_key_is_secret` (through `catalog_config`), and `repark_core::redaction` re-exports the module. Mutation: `redact_value` reverted to key-only. | `redact_value_keeps_the_key_rule_and_adds_the_value_rule`, `prop_key_is_secret_is_unchanged_by_the_move`, the existing `catalog_spec_debug_redacts_secret_prop_values` and `sources_listing_names_kind_profile_and_redacts_secrets`. | PROVEN | §2 green. §3: under the mutation the redaction pin reds at `redaction.rs:356`. |
| C-007 | `sources()` rows and the `SourceSpec` `Debug` mask a password inside a URL, a keyword DSN and an ODBC string, keeping the host and the user. | `named_sources::tests::sources_listing_masks_a_password_inside_a_url_shaped_value`. | PROVEN | §2 green; §3 red at `named_sources/tests.rs:124` under the mutation. |
| C-008 | The config dump masks a password inside a catalog `url`, and the session's `conf_dump()` masks every value, while the session keeps the raw rows so S3 endpoint resolution (`resolve_endpoint_from_dump`) still sees the configured endpoint byte for byte. | `config_file::tests::wiring::file_dump_masks_a_password_inside_a_catalog_uri`, `session::tests::conf_dump_redaction::conf_dump_masks_url_userinfo_while_the_endpoint_resolves_raw`. | PROVEN | §2 green; §3 red at `wiring.rs:286` and `conf_dump_redaction.rs:21`. `session.rs` stays at its 1000-line ceiling: the two touched lines swap the stored rows to `raw_conf_dump_rows` and the accessor to `redact_dump_rows`. |
| C-009 | The `CatalogSpec` `Debug` masks through `redact_value`, and the catalog kind refusal (`unrecognized value '…'` for `type` / `catalog-impl`) echoes the value through `mask_value_credentials`, keeping the host. | `catalog_config::tests::catalog_spec_debug_and_kind_refusal_mask_url_userinfo`. | PROVEN | §2 green; §3 red at `catalog_config.rs:795`. |
| C-010 | `DESCRIBE NAMESPACE EXTENDED` keeps Spark's key-or-value regex arm (`*********(redacted)`) and masks the userinfo of every other value, where Spark 4.1.2 prints it; the divergence is the dated row `CONNECT-DIV-url-userinfo` in [the registry](../../../docs/spark-sql-iceberg-parity.md) §5. | `tests::describe_show::describe_namespace_extended_masks_url_userinfo_spark_would_show`; the existing `describe_namespace_extended_redaction_truth_table` unchanged; the Spark measurement in §5. | PROVEN | §2: `cargo test -p repark-spark --lib` 2609 passed. Spark 4.1.2 answered `((conn,postgresql://u:NsPw5@db.example.com/sales), (plain,p7))`. |
| C-011 | The facade routes through Rust: `spark.conf.getAll` masks each value with `_native.redact_property_value` (the Rust `redact_value`), and `SET k` / `SET` / `SET -v` mask with `_native.mask_value_credentials` when Spark's regex has not redacted the whole value. An explicit `spark.conf.get(k)` and the `SET k = v` echo stay raw, as in Spark. | `test_source_url_redaction_1.py::test_config_dump_never_carries_the_password`, `::test_set_listings_never_carry_the_password`; `test_sql_set_door_1.py` unchanged. | PROVEN | §2: the three named facade files green on a debug build. |
| C-012 | The facade pin: a `repark.toml` with a per-test random password in a source `url`, a query string, a keyword DSN, an ODBC string, a catalog `uri` and a `[conf]` key; the password appears nowhere in `sources()`, `repr` / `str` of the rows, `spark.conf.getAll`, the `SET` listings or the `source("acme").ping()` refusal, and the host does. Mutation: `redact_value` reverted to key-only reds it. | `python/repark/tests/test_source_url_redaction_1.py` (four tests). | PROVEN | §2: 4 passed. §3: 2 failed under the mutation (`test_sources_rows_never_carry_the_password`, `test_config_dump_never_carries_the_password`); the `SET` and `ping` legs do not pass through `redact_value` and stay green. |
| C-013 | Every surface that prints a source or catalog property value is audited with a verdict (§1 table). | The table in §1, each row with its pin or its code reading. | PROVEN | §1. |
| C-014 | Gates of the brief green; no new dependency (`repark-python` reaches the redactor through `repark_core::redaction`, not a new edge); zero code comments added. | §2 commands. | PROVEN | §2. |
| C-015 | Fold 1 item 1: `https://h:443/p?m=a@b.c` comes out unchanged, `https://u:p@h/x@y` masks to `https://u:***@h/x@y`, an `@` inside a fragment or a query is never userinfo (`https://h.example.com/p#frag:x@y`, `https://h.example.com?u:p@x` unchanged — fold 2 moved these two pins to a dotted host, because a dotless portless host followed by a later `@host` now fails closed, C-034; `https://u:p@h/p#a:b@c` → `https://u:***@h/p#a:b@c`), and a bracketed IPv6 host with a path `@` stays whole. Mutation: the authority ends at whitespace only. | `userinfo_is_read_only_inside_the_authority`. | PROVEN | §2 green; §3 M-2 red at `redaction.rs:251`. |
| C-016 | Fold 1 item 2: `DESCRIBE TABLE EXTENDED`, `SHOW CREATE TABLE` and `SHOW TABLE EXTENDED` (all through `spark_table_properties`, key rule kept), `SHOW TBLPROPERTIES` of a table and of a view (with and without a key; the view's `location` row too), and `SHOW CREATE TABLE` of a view mask a credential inside a property value, where Spark 4.1.2 prints it on all six (§5); `CONNECT-DIV-url-userinfo` names every surface. RePark's `DESCRIBE TABLE EXTENDED` of a view renders no property row (not-applicable). Mutation: `mask_value_credentials` as the identity. | `tests::property_display_redaction` — one SQL pin per surface (six tests); `SHOW TABLE EXTENDED` is compared with its per-character `, ` separators removed, so the password check is not vacuous. | PROVEN | §2: `cargo test -p repark-spark --lib` 2615 passed. §3 M-1: all six red at `property_display_redaction.rs:54`. |
| C-017 | Fold 1 item 3: every configuration refusal that echoes a rejected value runs it through `mask_value_credentials` — Rust: `session.*` integers (`wiring.rs`), maintenance durations (`maintenance.rs`), `partitionOverwriteMode`, `timeParserPolicy`, the session time zone (`INVALID_CONF_VALUE.TIME_ZONE`), the S3 boolean and endpoint refusals, the DataFusion conf and dead-key refusals, `memory_limit`, `escapedStringLiterals`; facade: `_secrets.mask_credentials` (the native masker) in `_config_value_error`, `parse_timestamp_type`, the `datafusion.*` forward refusals, `normalize_display_style`, the integer-config refusal, `_normalize_display_int`'s non-integer arm and `_type_mismatch_error`. | `config_file::tests::wiring::typed_knob_refusals_mask_a_password_in_the_echoed_value` (the one Rust pin); `test_source_url_redaction_1.py::test_knob_refusals_never_carry_the_password` (time zone, `SET spark.sql.shuffle.partitions`, display style). | PROVEN | §2 green; §3 M-1 red at `wiring.rs:302` and in the facade test. `test_production_file_size.py`'s moved-symbol hashes follow the three edited bodies (`_config_value_error`, `_forward_datafusion_conf`, `normalize_display_style`). |
| C-018 | Fold 1 item 4: the `catalog.memory_catalog`, `catalog.memory_catalog_cached` and `catalog.memory_catalog_cached_with_props` spans record `warehouse` through `mask_value_credentials`; the `repark-iceberg → repark-common` edge already exists (`normal`, declared in `check_crate_dag.py`). | `catalog::tests::memory_props_span::memory_catalog_span_masks_a_password_in_the_warehouse`. | PROVEN | §2: `cargo test -p repark-iceberg --lib` 804 passed. §3 M-1 red at `memory_props_span.rs:80`. |
| C-019 | Fold 1 item 5: `_REDACTION_RE` is Spark 4.1.2's measured `SET` rule, `(?i)secret|password|token|access[.]?key|url` against the key or the value — `spark.redaction.regex` (default `(?i)secret|password|token|access[.]?key`) unioned with `spark.sql.redaction.options.regex` (default `(?i)url`), both defaults read from the oracle's JVM config entries, and the probe (§5) redacts `accesskey`, `access.key`, a `my url` value, an `AccessKey here` value and a `*_url` key while `access_key` stays visible. Mutation: the old `access[.]key` pattern. | `test_source_url_redaction_1.py::test_set_redaction_matches_the_spark_4_1_2_default_regexes`; `test_sql_set_door_1.py` unchanged and green. | PROVEN | §2 green; §3 M-3 red (`assert {'spark.p.acc…'my url', …} == {…dacted)', …}`). |
| C-020 | Fold 1 item 6 (audit only, no product change): every place a catalog provider (`ReparkCatalogProvider`, `IcebergCatalogProvider`, the iceberg-rust catalogs) reaches `{:?}` in an error, a log, a span, an EXPLAIN or a Python repr is listed in §6 with file and line and a verdict. | §6. | PROVEN | §6: one user-visible raw-credential path, in `repark-distributed` (not in the Python wheel); none in the shipped facade. |
| C-021 | Fold 2 item 1 (verifier S1, `verify/probe/src/bin/repro.rs`): an Oracle thin or EZConnect `user/password@host` with no `://` masks the password and keeps the user and host — `jdbc:oracle:thin:scott/Tiger2026@db.example.com:1521/ORCL` → `…scott/***@db…`, the `@//db…` form, the bare `scott/Tiger2026@db…`, a quoted `scott/"Ti ger;26"@db…`; a bare `u:BareUserPass5@db.example.com:5432` likewise. The user must start with a letter and follow a word boundary or a `:` (`/` form), so a port, a path segment or a scheme never matches. | `redaction::tests::oracle_thin_and_ezconnect_passwords_are_masked`; corpus class `oracle-ezconnect`. | PROVEN | §2 green; §3 M-20 (slash leg dropped) and M-30 (any user start) red. |
| C-022 | Fold 2 item 2 (S1): `jdbc:sqlserver://db:1433;user=sa;password=Spring@2026x;encrypt=true` → `…;password=***;encrypt=true`, host kept: the key=value legs run before the userinfo leg, and the authority also ends at `;`. MySQL's parenthesized `address=(…)(password=Pw69)/db` ends the value at `)`. | `redaction::tests::a_parameter_password_carrying_an_at_sign_keeps_the_host`; corpus class `jdbc-sqlserver`. | PROVEN | §2 green; §3 M-2 and M-29 red. |
| C-023 | Fold 2 item 3 (S1, overlaps fold 1): `postgresql://db.example.com:5432/sales?user=u&password=Pa@ssw0rdQ` → `…?user=u&password=***`, host kept. | Same pin as C-022; corpus class `query-at-port`. | PROVEN | §2 green; §3 M-2 red. |
| C-024 | Fold 2 item 4 (S2): unencoded whitespace in a userinfo (space, `\t`, `\n`) is masked: the authority no longer ends at whitespace unless a clean host precedes it (`https://h.example.com:8080 contact admin@x.com` stays). | `redaction::tests::whitespace_inside_userinfo_fails_closed`; corpus class `whitespace-userinfo`. | PROVEN | §2 green. §3 M-4 (whitespace ends the authority) is an **equivalent** mutant: the fail-closed leg reaches the same `@` and masks the same span, so no input separates them; recorded, not killed. |
| C-025 | Fold 2 item 5 (S2): a parameter named `passwd`, `pass`, `pw`, `passcode`, `sas`, `AccountKey` or `SharedAccessKey` is secret (`client_secret`, `apiKey` already were through `prop_key_is_secret`); a name ending in `name` is not (`SharedAccessKeyName=RootManageSharedAccessKey` stays). | `redaction::tests::the_wider_secret_parameter_names_are_masked`. | PROVEN | §2 green; §3 M-21, M-22 red. |
| C-026 | Fold 2 item 6 (S2): a JSON key in quotes (`{"user":"u","password":"JsonPw6"}` → `"password":***`, escaped quotes, single quotes and bare numbers included) and a YAML `key: value` (`password: YamlPw8`, `Authorization: Basic …`) mask the value of a secret-named key; a bare key needs a space after its colon, so `scheme:` and `host:port` never match. | `redaction::tests::json_and_yaml_secret_values_are_masked`; corpus classes `json`, `yaml`. | PROVEN | §2 green; §3 M-19 red. |
| C-027 | Fold 2 item 7 (S2): `prop_key_is_secret` adds a key containing `account_key` (`fs.azure.account.key.<acct>…`), `authorization` (`header.Authorization`) and a final segment `pat`. Its one functional caller, the `flag_secret_columns` read option (`read_options.rs:189`), moves to `column_name_is_secret_shaped`, the pre-widening rule under a new name, so no column decision changes; every other caller is a display. | `redaction::tests::the_key_rule_covers_azure_account_keys_authorization_and_pats`, `::the_column_predicate_keeps_the_pre_widening_rule`; the torture `flag_secret_columns` cells green. | PROVEN | §2 green; §3 M-23..M-25 and M-31 red. |
| C-028 | Fold 2 item 8 (S3): the verifier's three surviving mutants are killed or classified — M-13 (only a space ends an unquoted DSN value) red on the `\t` / `\n` pin, M-18 (an empty userinfo masks) red on `postgresql://@h/db`, M-4 equivalent (C-024) — and the rerun over 31 mutants of the new code (the verifier's 18 plus 13 for the new legs) leaves 30 red and that one equivalent. | `target/mut/run.py` (workspace scratch, not committed), results in §3. | PROVEN | §3. |
| C-029 | Fold 2 item 9 (S3): `DESCRIBE NAMESPACE`'s Comment, Location and Owner rows mask through `mask_value_credentials`; Spark 4.1.2 prints Comment and Location raw (§5). | `tests::describe_show::describe_namespace_masks_comment_location_and_owner_credentials` (SQL). | PROVEN | §2: `cargo test -p repark-spark --lib` green. |
| C-030 | Fold 2 item 10 (S3): the S3 endpoint refusal (`normalize_endpoint`, both arms) echoes the endpoint masked (done in fold 1, pinned now). | `object_store_s3::tests::an_invalid_endpoint_refusal_masks_its_userinfo`. | PROVEN | §2 green. |
| C-031 | Fold 2 item 11 (S3): the text-write legacy-policy check reads the session's raw conf rows (`&self.conf_dump`), never the display-redacted `conf_dump()`; it was the only functional reader of `conf_dump()` (grep of non-test callers). | The text-write and time-parser-policy suites, unchanged and green. | PROVEN | §2: `cargo test -p repark-core --lib` green. |
| C-032 | Fold 2 item 12 (S3): `_secrets.prop_key_is_secret` and `_funcs.py`'s unused alias are deleted; `test_a3_secrets_redaction.py` and `test_torture_secrets.py` assert through `_native.redact_property_value`, so they exercise the live Rust key rule. | Those two files; `test_production_file_size.py` without the alias. | PROVEN | §2: green on a debug build. |
| C-033 | Fold 2 item 13 (the fold-1 audit's raw-password path, §6 A-2): `repark-distributed`'s props-map refusal names the session catalog and the byte offset and no longer echoes the provider's `Debug` text; no `repark-common` edge was needed. The file is behind the `cluster` feature, which no Makefile target or workflow builds. | `iceberg_provider::tests::a_malformed_props_map_refusal_never_echoes_the_debug_text` (`--features cluster`). | PROVEN | §2: `cargo test -p repark-distributed --features cluster --lib` green; mutation (echo restored) red at `iceberg_provider.rs:839`. |
| C-034 | Fold 2 item 14: an unencoded `/`, `?`, `#` or `;` inside a password fails closed — when the authority is not a clean host (a non-numeric or empty port, a dotless portless label, a dotted name whose last label is not an alphabetic TLD or an IPv4) and a later `@` precedes a host, the mask runs to that `@`, keeping a plain user (`postgresql://alice:pa/ss@h/db` → `postgresql://alice:***@h/db`; `s3a://AKIAX:wJalr/K7MDENG@bucket/warehouse`; `https://AbCd/EfGh+IjKl9@git.example.com/repo.git` → `https://***@git…`). The other side is pinned too: `https://h:443/p?m=a@b.c`, `https://db.example.com/u@x.example.com` and `https://10.0.0.1/a@b.example.com` stay. Accepted over-mask, pinned: `https://h/u@example.com` → `https://***@example.com` and `s3://bucket/2024@x/part.parquet` hides the bucket. | `redaction::tests::an_unencoded_delimiter_inside_the_password_fails_closed`, `::a_host_must_look_like_one_before_the_fail_closed_leg_stands_down`. | PROVEN | §2 green; §3 M-3, M-26, M-27, M-28 red. |
| C-035 | Fold 2 item 15: `repark-functions` has no `repark-common` edge and `check_crate_dag.py` declares none, so its three boolean-knob refusals (`case_sensitive.rs:79`, `merge_schema.rs:59`, `ansi.rs:116`) still echo the value; recorded as a follow-up row on the card. | `./scripts/check_crate_dag.sh`; the card row. | PROVEN | §4, and the card. |
| C-055 | SOURCE-URL-REDACT-1-FN: those three refusals carry the rejected value on `BooleanConfRefusal` (`key`, `raw`) and do not format it. `repark-python`'s `refused_boolean_knob` and `repark-spark`'s session-build arm format Spark's `[INVALID_CONF_VALUE.TYPE_MISMATCH]` / SQLSTATE 22022 text with `mask_value_credentials`. `spark.conf.set` of `postgresql://u:pw@h/db` on `spark.sql.caseSensitive`, `spark.sql.ansi.enabled` and `spark.sql.iceberg.merge-schema` raises `IllegalArgumentException` quoting `postgresql://u:***@h/db`. A credential-free `maybe` is unchanged. No crate edge was added. Spark 4.1.2 accepts `spark.sql.iceberg.merge-schema` at set time; RePark still refuses then, and only the echoed value changed. | `session_runtime::tests::case_sensitive_refusal_masks_a_url_password`, `::ansi_refusal_masks_a_url_password`, `::merge_schema_refusal_masks_a_url_password`, `::a_credential_free_bad_value_is_unchanged`; `extension::tests::configure_masks_a_url_password_in_the_merge_schema_refusal`. Mutation: pass `raw` to the message instead of `mask_value_credentials`. | PROVEN | This PR. Python repros on the debug build show the masked value, class `INVALID_CONF_VALUE.TYPE_MISMATCH` and SQLSTATE `22022`. `./scripts/check_crate_dag.sh` unchanged and green. |
| C-056 | SOURCE-URL-REDACT-1-FN fold 3: the Python error boundary masks URL userinfo only. `mask_url_userinfo` runs `url_userinfo_spans` and the same renderer as `mask_value_credentials`. `mask_user_visible` calls it, so every `PyErr` built from `to_py_err`, `datafusion_to_py_err`, `ml_to_py_err`, or another `new_err` masks a `scheme://user:password@host` and leaves other text byte-identical. The per-site doors (`session_runtime.rs`, the merge-schema builder arm, `configure`'s `Configuration` mask) keep the full `mask_value_credentials`; that covers DSN and key=value shapes only on those doors. The 2026-10-06 re-verify measured three classes still leaking, all equal on the build without this PR: a chained Python `ValueError` traceback for the integer knobs, `datafusion.*` doors appending raw engine text, and JDBC-query, libpq and ODBC credential shapes on 13 knobs at the builder, toml and dbt doors. They are closed by SOURCE-URL-REDACT-2 (v1.5.3 card), not here. `postgresql://u:pw@h/db` inside a sentence becomes `postgresql://u:***@h/db`. `Multiple entries with same key: deleted-records=3 and deleted-records=5`, `1=ID and 1=id`, and `s3://bucket@x/path` stay unchanged. | `redaction::tests::mask_url_userinfo_masks_a_url_password_and_leaves_other_text`; `tests::boundary_mask_hides_a_url_password_in_plan_and_configuration_messages`; `test_builder_plan_knob_masks_a_url_password`, `test_builder_time_zone_masks_a_url_password`. Mutation: drop the `to_py_err` mask. | PROVEN | This PR. |
| C-036 | Fold 2 item 16: the reader-option (`flag_secret_columns`, boolean, single-character), write-option (isolation, file format, distribution mode) and path-write (format, mode, compression, destination) refusals echo the value masked. | `read_options::tests::a_boolean_option_refusal_masks_a_url_password`, `write_options::tests::an_invalid_write_option_refusal_masks_a_url_password`. | PROVEN | §2 green. |
| C-037 | Fold 2 corpus: a fixed-seed LCG test in `repark-common` with 6,000 shaped inputs over 12 classes (`url-userinfo`, `query-param`, `libpq-dsn`, `odbc-braced`, `lone-token`, `jdbc-sqlserver`, `multi-url`, `oracle-ezconnect`, `json`, `yaml`, `query-at-port`, `whitespace-userinfo`; every other input from the hard set `@ : / ? # & ; = space \n \t " ' { } \`), each with a marker password, plus 3,000 garbage inputs: no marker survives (the verifier's leak test: the whole marker or any hard-split piece of 6+ bytes), `catch_unwind` sees no panic, every host is kept. Two classes leave a character out, documented: a lone token never carries `:` (a `token:x@host` is a user and a password by RFC 3986, and the user is kept), and an Oracle password never carries `"` (Oracle cannot quote one). | `redaction::corpus::the_shaped_corpus_never_leaks_its_marker_and_keeps_its_host`, `::garbage_inputs_never_panic`. | PROVEN | §2: 0.15 s in debug; §3: 15 of the 31 mutants also red the corpus. |
| C-038 | The verifier's own harness (`verify/probe`, 20,000 shaped + 20,000 garbage, its seed), rerun against the fold-2 redactor from a copy under `target/`: 0 panics; 820 survivals before, 67 after, and every one of the 67 is the lone-token-with-colon case, where the shown part is the RFC user and the password half is masked (0 of them leak the marker's `KQV` tail). | The probe output in §3. | PROVEN | §3. |
| C-039 | Fold 2 gates: the brief's list plus `cargo test -p repark-common --lib` with the corpus, `-p repark-core`, `-p repark-spark`, `-p repark-distributed --features cluster`, the touched facade and parity files, the mutation rerun, and the comment ban at 0. | §2. | PROVEN | §2. |
| C-040 | Fold 3 R-1 (re-verify S1, `reverify/live/repark_getdatabase.py`): `spark.catalog.getDatabase(…).locationUri` and `.description` return the stored namespace values: the facade calls a native `namespace_metadata` (repark-spark `describe_show`, the same `namespace_exists` check and `SCHEMA_NOT_FOUND` text) instead of parsing the display-masked `DESCRIBE NAMESPACE`. Every site that parses display output for a functional value is listed in §7 and reads raw or only non-secret fields. | `test_source_url_redaction_1.py::test_get_database_returns_the_stored_location_and_comment`; the catalog suites. | PROVEN | §2 fold 3; live: `locationUri` `s3://lake/teams/data@corp.example.com/ns`, `description` raw. |
| C-041 | Fold 3 R-2 (re-verify S2, `live/repark_overmask.py`): a storage scheme (`s3`, `s3a`, `s3n`, `gs`, `gcs`, `abfs`, `abfss`, `wasb`, `wasbs`, `hdfs`, `file`, `viewfs`, `oss`, `cos`, `r2`) whose userinfo carries no `:` is shown as is, on the authority and the fail-closed legs alike; a legacy `s3a://AKIA…:secret@bucket` still masks; non-storage schemes keep fail-closed (`https://h/u@example.com` → `https://***@example.com`). | `redaction::tests::storage_locations_without_a_password_are_shown_as_is`; `corpus::credential_free_storage_locations_are_shown_as_is`; `property_display_redaction::a_credential_free_storage_location_is_shown_as_spark_shows_it`. | PROVEN | §2; mutants F1 and F2 red; the probe's location list all `KEPT`. |
| C-042 | Fold 3 R-3 (re-verify S1, `live/repark_keyrule.py`): every table, view and namespace property display — `SHOW TBLPROPERTIES` of a table or view with or without a key, `SHOW CREATE TABLE` of a table or view, `DESCRIBE TABLE EXTENDED`, `SHOW TABLE EXTENDED`, `DESCRIBE NAMESPACE EXTENDED` — shows `*********(redacted)` when Spark 4.1.2's measured rule (key or value matches `(?i)secret|password|token|access[.]?key|url`) or `prop_key_is_secret` matches, through one helper, `table_props_view::displayed_property_value`; other values go through `mask_value_credentials`. | `property_display_redaction::*_redacts_secret_keys_like_spark` (seven SQL pins, with the keyed form); `describe_show::describe_namespace_extended_redaction_truth_table` (updated to Spark's rows plus the key rule). | PROVEN | §2: `cargo test -p repark-spark --lib` 2625 passed; live: 0 leaks on all eight `repark_keyrule.py` statements; Spark measurement in §5. |
| C-043 | Fold 3 R-4 (re-verify S1, `probe/src/bin/repro.rs`): an Oracle thin TNS descriptor `user/password@(DESCRIPTION=…)` masks the password, bare or under `jdbc:oracle:thin:`. | `redaction::tests::oracle_tns_descriptors_mask_the_password`; corpus class `oracle-tns-descriptor`. | PROVEN | §2; mutant F3 red; probe class 0 survivals. |
| C-044 | Fold 3 R-5 (re-verify S2): the legs record masked spans on the original value in order (URL userinfo, logins, key=value, JSON/YAML) and a later leg skips positions an earlier leg masked, so `postgresql://alice:MySecret=Value18@db.example.com/db` → `postgresql://alice:***@db.example.com/db`, while `jdbc:sqlserver://db:1433;user=sa;password=Spring@2026x` → `…;password=***` and the fold-2 query-`@` case hold; the URL fail-closed leg stops at a `;` or whitespace opening a named parameter unless the authority already has a `user:` shape. | `redaction::tests::the_key_value_leg_never_fires_inside_a_userinfo`, `::a_user_colon_authority_runs_past_a_parameter_looking_password_tail`; corpus class `secret-word-in-userinfo` and every repro of both verdicts as fixed cases. | PROVEN | §2; mutants N4, F7, F8, F9 red. |
| C-045 | Fold 3 R-6 (re-verify S2): the Oracle and bare login legs fail closed — a password holding `' ( ) , ; =` or whitespace, or a quoted user, is masked up to the `@` before a host (`/` form: a host, `//`, `(` or `[`; `:` form: a host); prose stays (`contact john@example.com`); a `/` inside an Oracle password needs a `(` or `//` follower, so image references (`docker.io/library/postgres@sha256:…`) stay. | `redaction::tests::oracle_and_bare_logins_fail_closed`; corpus classes `oracle-ezconnect`, `bare-user-colon-pw`. | PROVEN | §2; mutants N1, N2, N3, N20, F4, F10..F13 red. |
| C-046 | Fold 3 R-7 (re-verify S2): a secret YAML key whose value starts on the next line, a block scalar (`|`, `>`, chomping and indent digits) with its indented lines, and a JSON value on a following line are masked; a tab separator is pinned. | `redaction::tests::multi_line_json_and_yaml_secrets_are_masked`; corpus class `json-yaml-multiline`. | PROVEN | §2; mutants N6, F5, F6 red. |
| C-047 | Fold 3 R-8: the re-verify's N1..N22 re-expressed on the fold-3 code plus F1..F14 for the new rules (36 mutants, `target/mut3/run.py`): 33 red at fold 3, and N19 red since fold 4 (re-verify 2 showed it leaks: `mysql://bob:Pw#Leak3@db.example.com and more`; it is not an equivalent, as fold 3 recorded); N7 survives as an over-mask only; F14 is equivalent (an empty span is never added). | `target/mut3/run.py`; §3 fold 3. | PROVEN | §3. |
| C-048 | Fold 3 R-9: the known limits are recorded (§4) and named in the divergence row: XML, command lines, `Cookie:`, bare tokens under non-secret keys, percent-encoded option passwords, a fullwidth `：//`, a password containing `://`, a lone token with `:` showing its user half. | The divergence row; §4. | PROVEN | §4. |
| C-049 | Fold 3 R-10: `CONNECT-DIV-url-userinfo` states that storage locations display as Spark shows them and that secret keys redact as Spark does and more, with Spark 4.1.2 re-measured on fifteen keys and the re-verify's `live/` scripts re-run on both engines. | The registry row. | PROVEN | §5 fold 3; `python3 scripts/check_docs_links.py` clean. |
| C-050 | Fold 3 corpus and fuzz: the `repark-common` corpus mirrors the re-verify's 17 classes (9,000 shaped, half hard, `redact_value` on a quarter) plus the storage-location class and every repro of both verdicts as fixed cases, and 4,000 garbage inputs; the re-verify's own probe at 96,000 shaped + 96,000 garbage gives 0 panics and 0 survivals in every class except the R-9 lone-token-with-`:` known limit (124/2870) and the R-2 storage-scheme lone tokens shown as is (164/2825, 181/2870). | `redaction::corpus::*` (four tests); `reverify/probe` rerun from `target/rprobe`. | PROVEN | §3 fold 3 class table. |
| C-051 | Fold 3 gates: the 23 previous gates, `cargo test -p repark-distributed --features cluster --lib`, the re-verify's probe and `live/` scripts, the adapted mutant run, the comment ban at 0. | §2 fold 3. | PROVEN | §2 fold 3. |
| C-052 | Fold 4 item 1 (re-verify 2 new S1, `reverify2/live/repark_embedded.py`): the fail-closed follower host ends at `" ' ( ) ] { } , < >` as well as `/ ? # ;` and whitespace (skipping a leading `[IPv6]` literal), so a URL inside JSON, parentheses, quotes, angle or square brackets, braces or a comma list masks its password: `{"conn":"postgresql://u:Pw/EmbLeak1@db.example.com"}` → `{"conn":"postgresql://u:***@db.example.com"}`; the bare-login chain crosses a `(` so a password holding `@x(` masks to the last host. | `redaction::tests::an_embedded_url_fails_closed_up_to_its_delimited_host`, `::a_bare_login_password_holding_an_at_and_a_paren_masks_to_the_last_host`; corpus class `embedded-url` (0 survivals). | PROVEN | §2 fold 4; `repark_embedded.py` 0 leaked markers on `getAll`, `SET` and `SHOW TBLPROPERTIES`; mutant F15 (delimiters dropped) red. |
| C-053 | Fold 4 item 2: N19 is a leaking survivor; the verifier's pin `mysql://bob:Pw#Leak3@db.example.com and more` → `mysql://bob:***@db.example.com and more` and its `/` sibling kill it. | `redaction::tests::whitespace_after_the_host_ends_the_fail_closed_follower`. | PROVEN | §2 fold 4: N19 red. |
| C-054 | Fold 4 gates: `cargo test -p repark-common` / `-p repark-core` / `-p repark-spark --lib`, `make rust-clippy`, `cargo fmt --check`, `make rust-panic-ban`, the map / ledger / docs-link checks, `repark_embedded.py` on the debug build, the N19 run red, the re-verify probe at 96k unchanged, the comment ban at 0. | §2 fold 4. | PROVEN | §2 fold 4. |

## 1. Surface audit

| Surface | Verdict | Pin or reading |
|---|---|---|
| `sources()` rows (`SourceRow`, Python `SourceMetadata` and its `repr`) | now-redacted | C-007, C-012 |
| `SourceSpec` `Debug` | now-redacted | C-007 |
| `CatalogSpec` `Debug` | now-redacted | C-009 |
| Config dump, `conf_dump_rows` / `ReparkSession::conf_dump()` | now-redacted | C-008 |
| `spark.conf.getAll` (the facade's view of the dump, file pairs included) | now-redacted | C-011, C-012 |
| `SET k`, `SET`, `SET -v` | now-redacted | C-011, C-012 |
| `SET k = v` echo | not-applicable | Echoes the statement's own literal, raw as in Spark (measured); not a listing. |
| `spark.conf.get(k)` | not-applicable | Explicit get stays raw, per the brief and Spark. |
| `DESCRIBE NAMESPACE EXTENDED` (`describe_show.rs`, `redaction_pattern_matches`) | now-redacted | C-010 |
| Catalog kind refusal (`type` / `catalog-impl` unrecognized value) | now-redacted | C-009 |
| `source(...).ping()` refusal | redacted-before | Names the key path and kind only (`connector_pending_message`); C-012 asserts it. |
| TOML parse error while a config file loads | redacted-before | `config_file.rs` `parse` renders `error.message()` with line and column, never the source line. |
| Directory-search cloud-catalog warning | redacted-before | `cloud_catalog_warning` names the path and the catalog names only. |
| `session.*` integer and maintenance duration refusals, and every other config-value refusal | now-redacted (fold 1) | C-017 |
| `NamedSource` handle `repr` | not-applicable | Carries name, kind and key path; no property. |
| Tracing: `catalog.glue_catalog` span | redacted-before | Records `prop_keys` and `has_warehouse` only. |
| Tracing: `catalog.memory_catalog*` spans | now-redacted (fold 1) | C-018 |
| `DESCRIBE TABLE EXTENDED` (table) | now-redacted (fold 1) | C-016 |
| `SHOW CREATE TABLE` (table and view) | now-redacted (fold 1) | C-016 |
| `SHOW TABLE EXTENDED` | now-redacted (fold 1) | C-016 |
| `SHOW TBLPROPERTIES` (table and view, with and without a key) | now-redacted (fold 1) | C-016 |
| `DESCRIBE TABLE EXTENDED` of a view | not-applicable | RePark renders the view schema only, no property row. |
| `SET` whole-value redaction (Spark regexes) | now-redacted (fold 1) | C-019 |
| `DESCRIBE NAMESPACE` Comment, Location, Owner rows | now-redacted (fold 2) | C-029 |
| S3 endpoint refusal (`normalize_endpoint`) | now-redacted (fold 1, pinned fold 2) | C-030 |
| Reader-option, write-option and path-write refusals | now-redacted (fold 2) | C-036 |
| `repark-distributed` props-map codec refusal | now-redacted (fold 2) | C-033 |
| `repark-functions` boolean-knob refusals | now-redacted (no new edge; the raw value is a field, masked upstream) | C-055 |
| Python exception boundary | now-redacted for URL userinfo (`mask_url_userinfo`); non-URL credential shapes leak on other doors until SOURCE-URL-REDACT-2 | C-056 |

**Executor readings (no halt).**
- **The redactor's home.** `repark-common` is the one crate every consumer reaches (core,
  spark, connect, and the binding through `repark_core::redaction`), so the masker lives there.
  `prop_key_is_secret` moves with it because the parameter rule reuses it; `repark-core` keeps
  exporting it.
- **The conf dump fed S3 endpoint resolution.** Masking the stored dump would have handed
  object_store a `user:***@` endpoint, so the session stores the raw rows and redacts at the
  `conf_dump()` accessor (C-008).
- **Fail closed against host visibility.** Round 1 let the mask win where they collided; fold 1
  scoped the rule to the authority (C-002, C-015), so a path, query or fragment `@` never hides
  the host.
- **Python knob refusals.** `_secrets.mask_credentials` passes non-strings through, so the
  `bool` / `int` arms keep their exact text; only string values reach the native masker.

## 2. Gates

Round 1, then fold 1 re-run of the whole list on the fold-1 tree (`01a1f1c4` + docs), under the timing-run wrapper (`flock` + `nice -n 19 taskset -c 48-63`
+ `CARGO_BUILD_JOBS=8`), exit codes verbatim.

| command | exit | output |
|---|---|---|
| `cargo test -p repark-common --lib` | 0 | fold 1: 44 passed (11 redaction pins) |
| `cargo test -p repark-core --lib` | 0 | fold 1: 1024 passed, 1 ignored |
| `cargo test -p repark-spark --lib` | 0 | fold 1: 2615 passed, 5 ignored |
| `cargo test -p repark-iceberg --lib` (fold 1, touched) | 0 | 804 passed |
| `./scripts/check_crate_dag.sh` (fold 1) | 0 | 23 internal edges clean |
| `python3 scripts/check_docstring_presence.py` (fold 1) | 0 | 307 files clean |
| `make rust-clippy` | 0 | no diagnostics |
| `cargo fmt --all --check` | 0 | no output |
| `make rust-panic-ban` | 0 | clean |
| `./scripts/check_rust_file_size.sh` | 0 | fold 1: 1041 files clean (`session.rs` 999) |
| `./scripts/check_lib_rs.sh` | 0 | 11 crate roots clean |
| `python3 scripts/sync_map_md.py --check` | 0 | 361 maps clean |
| `python3 scripts/check_ledger_grammar.py` | 0 | fold 1: 302 live ledgers clean (2949 clauses) |
| `python3 scripts/check_docs_links.py` | 0 | clean |
| `uvx ruff@0.15.22 check` / `format --check` (new test and the two touched facade files) | 0 | clean |
| `pytest` on a debug build (`uv sync --locked` + `maturin develop`): `test_source_url_redaction_1.py`, `test_session_sources.py`, `test_config_mirror.py`, and every other facade file that reads `getAll`, `SET` or redaction (`test_sql_set_door_1.py`, `test_a3_secrets_redaction.py`, `test_describe_namespace.py`, `test_describe_table.py`, `test_eager_budget_1.py`, `test_t3_ux_polish.py`, `test_builder_config_map.py`, `test_h2_group_h2.py`, `test_session_timezone_parity.py`, `test_session.py`, `test_production_file_size.py`) | 0 | 341 passed, 1 skipped |
| `pytest` fold 1, debug build after the mutations were restored: the 129 facade files that touch `INVALID_CONF_VALUE`, `TBLPROPERTIES`, `SHOW CREATE`, `SHOW TABLE EXTENDED`, `DESCRIBE TABLE EXTENDED`, display style, `timestampType`, `datafusion.*`, redaction, `getAll`, `memory_limit`, `SET -v` or `_secrets` (recorders, perf, scale, spill and live files excluded), plus the three named files, in four chunks | 0 | 992 + 832 + 996 + 1401 = 4221 passed, 0 failed |
| `python3 scripts/check_lib_py.py` | 0 | 945 files clean |
| `bash scripts/check_map_md.sh --base 4a643e56` | 0 | no output |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xsec1 4a643e56 HEAD` | 0 | `hits=0` |

**Fold 2 re-run** on the fold-2 tree (`4dbbbec7`), same wrapper; the 09:00 quiet gate run held the
lock for part of it and the flock waited, never bypassed.

| command | exit | output |
|---|---|---|
| `cargo test -p repark-common --lib` | 0 | 57 passed (24 redaction pins + 2 corpus tests) |
| `cargo test -p repark-core --lib` | 0 | 1026 passed, 1 ignored |
| `cargo test -p repark-spark --lib` | 0 | 2617 passed, 5 ignored |
| `cargo test -p repark-iceberg --lib` | 0 | 804 passed |
| `cargo test -p repark-distributed --features cluster --lib` | 0 | 7 passed |
| `make rust-clippy` | 0 | no diagnostics |
| `cargo clippy -p repark-distributed --features cluster --all-targets -- -D warnings -A clippy::disallowed_methods` | 0 | no diagnostics |
| `make rust-panic-ban` | 0 | clean |
| `cargo fmt --all --check` | 0 | no output |
| `./scripts/check_rust_file_size.sh` | 0 | 1043 files clean |
| `./scripts/check_lib_rs.sh` | 0 | 11 crate roots clean |
| `./scripts/check_crate_dag.sh` | 0 | 23 internal edges clean |
| `python3 scripts/check_lib_py.py` | 0 | 945 files clean |
| `python3 scripts/check_docstring_presence.py` | 0 | 307 files clean |
| `python3 scripts/sync_map_md.py --check` | 0 | 362 maps clean |
| `python3 scripts/check_ledger_grammar.py` | 0 | 302 live ledgers clean (2968 clauses) |
| `python3 scripts/check_docs_links.py` | 0 | clean |
| `bash scripts/check_map_md.sh --base 4a643e56` | 0 | no output |
| `uvx ruff@0.15.22 check` / `format --check` (touched Python) | 0 | clean |
| `pytest`, debug build: the 148 facade files touching conf refusals, table / namespace displays, redaction, `getAll`, `SET`, `_secrets`, `flag_secret_columns` or write options (recorders, perf, scale, spill and live excluded), plus `test_source_url_redaction_1.py`, `test_session_sources.py`, `test_config_mirror.py`, `test_torture_secrets.py`, `test_datasets_secrets.py`, in five chunks | 0 | 782 + 1209 + 928 + 1549 + 390 = 4858 passed, 0 failed |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xsec1 4a643e56 HEAD` | 0 | `hits=0` |
| mutation re-run (`target/mut/run.py`, 31 mutants) | — | §3 |


**Fold 3 re-run** on the fold-3 tree, same wrapper; Muse lanes shared the lock and the flock
waited, never bypassed.

| command | exit | output |
|---|---|---|
| `cargo test -p repark-common --lib` | 0 | 65 passed (32 redaction pins + 4 corpus tests) |
| `cargo test -p repark-core --lib` | 0 | 1026 passed, 1 ignored |
| `cargo test -p repark-spark --lib` | 0 | 2625 passed, 5 ignored |
| `cargo test -p repark-iceberg --lib` | 0 | 804 passed |
| `cargo test -p repark-distributed --features cluster --lib` | 0 | 7 passed |
| `make rust-clippy` | 0 | no diagnostics |
| `cargo clippy -p repark-distributed --features cluster --all-targets -- -D warnings -A clippy::disallowed_methods` | 0 | no diagnostics |
| `make rust-panic-ban` | 0 | clean |
| `cargo fmt --all --check` | 0 | no output |
| `./scripts/check_rust_file_size.sh` / `check_lib_rs.sh` / `check_crate_dag.sh` | 0 | clean |
| `python3 scripts/check_lib_py.py` / `check_docstring_presence.py` | 0 | clean |
| `python3 scripts/sync_map_md.py --check` / `check_ledger_grammar.py` / `check_docs_links.py` | 0 | clean (2980 clauses) |
| `bash scripts/check_map_md.sh --base 4a643e56` | 0 | no output |
| `uvx ruff@0.15.22 check` / `format --check` (touched Python) | 0 | clean |
| `pytest`, debug build, the same 148 facade files in five chunks plus `test_source_url_redaction_1.py`, `test_session_sources.py`, `test_config_mirror.py`, `test_catalog_surface.py`, `test_torture_secrets.py`, `test_datasets_secrets.py` | 0 | 781 + 1221 + 929 + 1549 + 423 = 4903 passed after the namespace truth table was moved to the key rule (it failed once on the first chunk, as R-3 intends) |
| re-verify probe, 96,000 + 96,000 | 0 | §3 fold 3 class table; panics 0 |
| re-verify `live/` scripts on this build and on Spark 4.1.2 | 0 | `getDatabase` raw; the userinfo, Oracle, TNS and YAML repros masked on `getAll` / `SET`; 0 leaks on the eight key-rule statements; storage paths kept |
| mutation run `target/mut3/run.py` | — | 33 red, 1 over-mask only, 2 equivalent (§3); run on `b158636c`, before the pedantic refactor `f7f9f2f4`, which changes no behavior |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xsec1 4a643e56 HEAD` | 0 | `hits=0` |

**Fold 4 re-run** (wrapper and flock unchanged).

| command | exit | output |
|---|---|---|
| `cargo test -p repark-common --lib` | 0 | 68 passed (35 redaction pins + 4 corpus tests, 18 classes) |
| `cargo test -p repark-core --lib` | 0 | 1026 passed, 1 ignored |
| `cargo test -p repark-spark --lib` | 0 | 2625 passed, 5 ignored |
| `make rust-clippy` / `cargo fmt --all --check` / `make rust-panic-ban` | 0 | clean |
| `python3 scripts/sync_map_md.py --check` / `check_ledger_grammar.py` / `check_docs_links.py` | 0 | clean |
| `reverify2/live/repark_embedded.py` on the debug build | 0 | `"leaked_markers": []` |
| N19 and F15 (`target/mut3/n19.py`) | — | both red |
| re-verify probe, 96,000 + 96,000 | 0 | panics 0; every class 0 except the R-9 lone-token-with-`:` limit (124/2870) and the R-2 storage lone tokens (164/2825, 181/2870) |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xsec1 4a643e56 HEAD` | 0 | `hits=0` |

## 3. Mutation

`redact_value` reverted to key-only (`value.to_string()` in the else arm), the debug module
rebuilt, the tree restored after.

- `cargo test -p repark-common --lib redaction`: 9 passed, 1 failed —
  `redact_value_keeps_the_key_rule_and_adds_the_value_rule` at `redaction.rs:356`.
- `cargo test -p repark-core --lib` (the four new pins): 4 failed —
  `catalog_config.rs:795`, `config_file/tests/wiring.rs:286`,
  `session/tests/conf_dump_redaction.rs:21`, `named_sources/tests.rs:124`.
- `pytest test_source_url_redaction_1.py`: 2 failed, 2 passed —
  `test_sources_rows_never_carry_the_password` (`'Pwa71e…' is contained here: … 'url':
  'postgresql://alice:Pwa71e…@db.example.com:5432/sales'`) and
  `test_config_dump_never_carries_the_password` (`'spark.repark.test.jdbc_url':
  'jdbc:postgresql://alice:Pw75f7…@db.example.com/sales'`).

### Fold 1 mutations (each restored and re-run green)

- **M-1: `mask_value_credentials` returns its input.** `property_display_redaction`: 0 passed,
  6 failed, all at `property_display_redaction.rs:54`; `typed_knob_refusals_…` red at
  `config_file/tests/wiring.rs:302`; `memory_catalog_span_masks_…` red at
  `memory_props_span.rs:80`; `test_source_url_redaction_1.py`: 4 failed, 2 passed (the sources,
  config-dump, `SET`-listing and knob-refusal legs; the regex-parity and ping legs do not pass
  through the masker).
- **M-2: the authority ends at whitespace only (the round-1 region).**
  `userinfo_is_read_only_inside_the_authority` red at `redaction.rs:251`; 10 others green.
- **M-3: `_REDACTION_RE` back to `(?i)secret|password|token|access[.]key`.**
  `test_set_redaction_matches_the_spark_4_1_2_default_regexes` red:
  `assert {'spark.p.acc...'my url', ...} == {'spark.p.acc...dacted)', ...}`.

### Fold 2 mutations (`target/mut/run.py`, each applied to `redaction.rs`, tested, restored)

| Mutant | Result |
|---|---|
| M-1 only a `postgresql` scheme | red (corpus + 6 pins) |
| M-2 key=value leg dropped | red (corpus + 5 pins) |
| M-3 fail-closed leg dropped | red (corpus + 2 pins) |
| M-4 whitespace ends the authority | **equivalent** (C-024) |
| M-5 `pwd` suffix dropped | red |
| M-6 `sig` dropped | red |
| M-7 `signature` dropped | red |
| M-8 backslash escape ignored | red |
| M-9 doubled brace ignored | red |
| M-10 lone token kept | red |
| M-11 first `@` instead of last | red |
| M-12 every delimiter ends a value | red |
| M-13 only a space ends an unquoted value | red (`every_whitespace_kind_ends_an_unquoted_keyword_value`) |
| M-14 unterminated quote left unmasked | red |
| M-15 `redact_value` key-only | red |
| M-16 only the first URL | red |
| M-17 leading whitespace kept in the value | red |
| M-18 empty userinfo masked | red (`an_empty_userinfo_is_left_alone`) |
| M-19 JSON/YAML leg dropped | red |
| M-20 Oracle / bare `user:pass@` leg dropped | red |
| M-21 `…name` not excluded | red |
| M-22 `passwd` dropped | red |
| M-23 key rule loses `account_key` | red |
| M-24 key rule loses `authorization` | red |
| M-25 key rule loses `pat` | red |
| M-26 any dotted name is a clean host | red |
| M-27 an empty port is clean | red |
| M-28 first `@` instead of first `@host` | red |
| M-29 no parenthesized pairs | red |
| M-30 an Oracle user may start with a digit | red |
| M-31 the column rule widens with the key rule | red |

30 red, 1 equivalent, 0 survivors. The verifier's probe rerun (C-038): `jdbc-sqlserver/hard 0/1501`,
`query-param/hard 0/1474`, `url-userinfo/hard 0/1374`, `multi-url/hard 0/1393`,
`lone-token-userinfo/hard 67/1450` (all `token:rest@host`, user shown, password masked), every
soft class 0, panics 0.

### Fold 3 mutations (`target/mut3/run.py`, 36 mutants on `redaction.rs`, each restored)

The re-verify's N1..N22, re-expressed on the span-based code, and F1..F14 for the fold-3 rules.
A mutant that the pins leave green is re-checked with the re-verify probe built against it.

| Result | Mutants |
|---|---|
| red | N1 login leg, N2 quoted Oracle password, N3 bare-form host check, N4 key=value before userinfo, N5 JSON/YAML leg, N6 YAML tab, N8 JSON `,}]` stop, N9 `;` ends the authority, N10 host-likeness second arm, N11 one-letter TLD, N12 `…name` exclusion, N13 `pass`, N14 `accountkey`, N15 `authorization`, N16 MySQL parenthesized pairs, N17 `@` chain, N18 host-follows skip, N20 `=` word delimiter, N21 `pat`, N22 quoted escapes, F1 storage exception, F2 storage hides `user:pw`, F3 TNS follower, F4 `/`-in-password guard, F5 next-line YAML, F6 block scalars, F7 parameter boundary, F8 `user:` shape ignored (red after its pin), F9 key=value inside a userinfo, F10 login inside a URL region, F11 `jdbc:` colon skip, F12 bare login stops at a newline, F13 login `@` chain |
| green, over-mask only (probe: no leak) | N7 (a bare YAML key without a space after the colon) |
| green, equivalent | F14 (an empty userinfo yields an empty span, never added) |
| red since fold 4 | N19 (whitespace no longer ends the follower host): fold 3 recorded it as equivalent; re-verify 2 showed it leaks on `mysql://bob:Pw#Leak3@db.example.com and more`, and `whitespace_after_the_host_ends_the_fail_closed_follower` now kills it |

### Fold 3 fuzz: the re-verify probe (`reverify/probe`, 96,000 shaped + 96,000 garbage, its seed)

Panics 0. Every class 0 survivals (soft and hard): `azure-connstr`, `bare-user-colon-pw`,
`jdbc-sqlserver`, `json-inline`, `json-yaml-multiline`, `kafka-jaas`, `kv-before-url`,
`libpq-dsn`, `multi-url`, `odbc-braced`, `oracle-ezconnect`, `oracle-tns-descriptor`,
`query-param`, `secret-word-in-userinfo`, `url-userinfo`, `yaml-inline`, and `lone-token-userinfo`
apart from: 124/2870 hard `[known:lone-token-colon-user-half]` (R-9) and 164/2825 soft +
181/2870 hard `s3a://token@host` shown as is (R-2, a storage scheme without `:`). Location list:
all ten `KEPT`. Before fold 3 the same probe found 1078+1223 TNS, 534 EZConnect, 547+730 bare,
1372+1399 multi-line and 454+429 secret-word survivals.

## 4. Measured, out of scope

- **`spark.sql.iceberg.merge-schema` at set time:** Spark 4.1.2 accepts a non-boolean
  value; RePark still refuses at set time. C-055 masks the echoed value and does not
  move the refusal. The builder-time messages in `parse_spark_sql_case_sensitive` and
  `parse_spark_sql_ansi_enabled` (`should be boolean, but was`) are a different refusal;
  the Python boundary masks URL userinfo in them (C-056).
- **The `cluster` feature is never built by CI**: `repark-distributed`'s `iceberg_provider.rs`
  (C-033) compiles only under `--features cluster`, which no Makefile target or workflow runs.
- **Known limits (fold 3, R-9), recorded and named in the divergence row:** XML
  (`<password>Xml36</password>`, `<property name="password" value="Xml37"/>`), command lines
  (`--password Cli44`, `-pCli46`, `mysql -u root -pCli47`), `Cookie: session=Ck41`, a bare token
  under a non-secret key (`ghp_…`, `AKIA…`, `auth=…`), a percent-encoded `password%3D…` inside
  an option, a fullwidth or fraction-slash `://`, a password that itself contains `://`
  (`postgresql://u:ab://cd58@…` shows `u:ab`), and a lone token with `:` showing its user half.
- **Accepted over-masks (pinned):** a non-storage URL with a dotless portless host and a later
  `@host` (`https://h/u@example.com`); a `user/word@domain` value (`alice/team@example.com`)
  and a bare `word:word@host` (`time:12@example.com`), the `user/` and `user:` shapes R-6 names.
- **Spark refuses `owner` in `DBPROPERTIES`** (`UNSUPPORTED_FEATURE.SET_NAMESPACE_PROPERTY`)
  where RePark accepts it; pre-existing, seen while measuring C-029.
- **The round-1 `SET` `url` gap** is closed by C-019; **table properties, typed knobs and the
  memory-catalog spans** by C-016..C-018.

## 5. Spark 4.1.2 measurement (2026-10-06, `/tmp/sparkenv`, private local session)

```
SET_ECHO  [Row(key='spark.repark.test.runtime_url', value='mysql://bob:RuntimePw2@db.example.com/sales')]
SET_KEY_CONN [Row(key='spark.repark.test.conn', value='mysql://bob:RuntimePw2@db.example.com/sales')]
SET_LIST  [Row(key='spark.repark.test.conn', value='mysql://bob:RuntimePw2@db.example.com/sales'),
           Row(key='spark.repark.test.dsn', value='*********(redacted)'),
           Row(key='spark.repark.test.plain', value='*********(redacted)')]
GET       mysql://bob:RuntimePw2@db.example.com/sales
DESC_NS   … Row(info_name='Properties', info_value='((conn,postgresql://u:NsPw5@db.example.com/sales), (plain,p7))')
```

Fold 1 (same day, pyspark 4.1.2 + `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` from `~/.ivy2/jars`,
catalog `ice` = `org.apache.iceberg.inmemory.InMemoryCatalog`). The defaults are read through
the JVM config entries (`org.apache.spark.internal.config.package$.SECRET_REDACTION_PATTERN`,
`org.apache.spark.sql.internal.SQLConf$.SQL_OPTIONS_REDACTION_PATTERN`):

```
REGEX_SECRET spark.redaction.regex (?i)secret|password|token|access[.]?key
REGEX_SQL_OPTIONS_DEFAULT spark.sql.redaction.options.regex (?i)url
SET_LIST [('spark.p.access.key', '*********(redacted)'), ('spark.p.access_key', 'v2'),
          ('spark.p.accesskey', '*********(redacted)'),
          ('spark.p.conn', 'mysql://bob:SetPw@db.example.com/sales'),
          ('spark.p.plain', '*********(redacted)'), ('spark.p.plain2', '*********(redacted)')]
DESC_NS  Row(info_name='Properties', info_value='((conn,postgresql://u:NsPw@db.example.com/sales))')
DESC_TBL [Row(col_name='Table Properties', data_type='[conn=postgresql://u:TblPw@db.example.com/sales,current-snapshot-id=none,format=iceberg/parquet,format-version=2,plain=p7,write.parquet.compression-codec=zstd]', comment='')]
SHOW_CREATE  CREATE TABLE ice.ns.t ( id INT) TBLPROPERTIES ( 'conn' = 'postgresql://u:TblPw@db.example.com/sales', …)
SHOW_TBL_EXT 'Table Properties: [[, c, o, n, n, =, p, o, s, t, g, r, e, s, q, l, :, /, /, u, :, T, b, l, P, w, @, d, b, …]]'
SHOW_TBLPROPS [('conn', 'postgresql://u:TblPw@db.example.com/sales'), ('plain', 'p7')]
SHOW_TBLPROPS_KEY [Row(key='conn', value='postgresql://u:TblPw@db.example.com/sales')]
VIEW_TBLPROPS [('conn', 'postgresql://u:ViewPw@db.example.com/sales')]
VIEW_DESC [Row(col_name='View Properties', data_type="['conn' = 'postgresql://u:ViewPw@db.example.com/sales', 'format-version' = '1', 'location' = '…/ns/v', 'provider' = 'iceberg']", comment='')]
VIEW_SHOW_CREATE CREATE VIEW ice.ns.v ( a) TBLPROPERTIES ( 'conn' = 'postgresql://u:ViewPw@db.example.com/sales', …)
```

Fold 2 (same box, same runtime jar, `InMemoryCatalog`):

```
DESC_NS_ROWS [('Catalog Name', 'ice'), ('Namespace Name', 'leaky'),
              ('Comment', 'jdbc:postgresql://u:CmPw1@db.example.com/sales'),
              ('Location', 's3a://AKIAX:LocPw2@bucket/wh'), ('Owner', 'john')]
```

Fold 3 (re-measured, same runtime): fifteen property keys on a table, a view and a namespace
(`target/f3/spark_keys.py`). Redacted `*********(redacted)` on a table's `SHOW TBLPROPERTIES`,
`DESCRIBE TABLE EXTENDED`, `SHOW TABLE EXTENDED`, `SHOW CREATE TABLE` (table and view) and
`DESCRIBE NAMESPACE EXTENDED`: `password`, `aws.secret-access-key`, `client.token`, `accesskey`,
`jdbc_url`, `plain`=`my password text`, `plain2`=`has url inside`. Raw on all of them:
`access_key`, `my_key`, `acct.key`, `api_key_id`, `authorization`, `conn_string`, `credential`,
`pat`. A view's `SHOW TBLPROPERTIES` and `DESCRIBE TABLE EXTENDED` redact nothing. The re-verify's
`live/spark_probe.py` re-run: `SHOW TBLPROPERTIES t ('password')` → `*********(redacted)`;
`DESCRIBE NAMESPACE` Location `s3://lake/teams/data@corp.example.com/ns` raw.

## 6. Provider `Debug` audit (fold 1 item 6, no product change)

What the `Debug` carries: `ReparkCatalogProvider` (`crates/repark-iceberg/src/catalog/provider.rs:23`)
and the fork's `IcebergCatalogProvider` (`crates/integrations/datafusion/src/catalog.rs:52` at
rev `076d5f98`) derive `Debug` over the catalog handle. The fork's `MemoryCatalog`
(`crates/iceberg/src/catalog/memory/catalog.rs:132`) derives `Debug` over its `FileIO` and its
`properties`. The `FileIO`'s `StorageConfig` (`io/storage/config/mod.rs:67`) has a hand-written
`Debug` that masks a value by key name only (`credential`, `token`, `secret`, `key`,
`password`, `md5`, `connection-string`), so a password inside a URL-shaped value under any
other key prints raw there; the `properties` field is a derived `HashMap` `Debug` and prints
every value raw, secret-named keys included. The fork's `GlueCatalog`
(`catalog/glue/src/catalog.rs:240`) and `S3TablesCatalog` (`catalog/s3tables/src/catalog.rs:249`)
have hand-written `Debug` impls.

| # | Sink | File:line | User-visible | Verdict |
|---|---|---|---|---|
| A-1 | `format!("{catalog:?}")` of a session `CatalogProvider`, parsed back into a `CatalogSpec` (`catalog_spec_from_debug`) | `crates/repark-distributed/src/iceberg_provider.rs:738` | no — functional, never printed | functional read; not changed (fold instruction) |
| A-2 | The parse's refusal echoes the props slice of that `Debug` text: `"{ICEBERG_TABLE_SCAN} debug text has an unterminated string in {text:?}"` | `crates/repark-distributed/src/iceberg_provider.rs:696-697` | **yes**, as a codec error from the distributed scan encode | **fixed in fold 2 (C-033)**; before it, a raw password could reach it: the slice is the first `props: {…}` of the `Debug` text, the `StorageConfig` map, whose secret-named keys the fork masks but whose URL-embedded passwords it does not; the error fires when a value there carries a `}` or a `"` (an ODBC-braced or quoted password ends the slice early). `repark-distributed` is a delivered surface crate, not in the Python wheel. |
| A-3 | The other codec errors on that path name the catalog name, the kind token or a marker only | `iceberg_provider.rs:685`, `:762`, `:781` | yes | no credential |
| A-4 | `IcebergScanSpec` derives `Debug` over a `CatalogSpec` | `iceberg_provider.rs:29` | only if printed; no `{:?}` sink found | `CatalogSpec` `Debug` redacts (C-009) |
| A-5 | `#[tracing::instrument]` on every catalog function | `crates/repark-iceberg/src/catalog/mod.rs:100`, `:176`, `:191`, `:202`; `builders.rs:24`, `:39`, `:57` | log | every catalog argument is in `skip(…)`; fields are names, keys, booleans and the masked warehouse (C-018) |
| A-6 | EXPLAIN `DisplayAs` | fork `physical_plan/scan.rs:429` (projection, predicate, snapshot id), `write.rs:203`, `delete.rs:246`, `update.rs:103`, `commit.rs:178`, `metadata_scan.rs:61`; `crates/repark-iceberg/src/write/partition_write.rs:62` | yes | table identifier and plan shape only; no catalog `Debug` |
| A-7 | Python `repr` / `__str__` | `crates/repark-python/src` declares no `__repr__` / `__str__` | yes | PyO3 default object repr; no catalog text |
| A-8 | `{catalog:?}` in benches | `crates/repark-spark/benches/ice_read_perf/pins.rs:591`, `:610`, `:616`, `:718`, `:746`, `:760`, `:773` | no — bench assertions; `catalog` is the bench's own label | not shipped |


```
COVERAGE_ATTESTATION:
  pr_unit: source-url-redact-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each brief shape (URL userinfo of any scheme, lone userinfo, fail-closed authority, secret query parameters, libpq and ODBC keywords, pwd) maps to a clause and a pin; each named caller of redact_value and describe_show's redaction is routed or audited.
      artifacts: [crates/repark-common/src/redaction.rs, crates/repark-core/src/named_sources/tests.rs, crates/repark-spark/src/describe_show.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Empty password, empty userinfo, password containing @ and spaces, quoted with escapes, braced with }} escapes, unterminated quotes, multiple URLs in one value, multi-host authorities, an empty password= value, a=b=c; fold 1 adds an @ in a path, a query and a fragment, a port before a query @, a user part carrying an @, a bracketed IPv6 host, and SHOW TABLE EXTENDED's per-character rendering.
      artifacts: [crates/repark-common/src/redaction.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The masker is total; every slice index sits on an ASCII delimiter or a char boundary from char_indices; no panic path in product code (panic-ban gate).
      artifacts: [crates/repark-common/src/redaction.rs]
    - id: AT-4
      status: N/A
      justification: Pure string functions; the session change swaps which rows an existing immutable Arc holds.
    - id: AT-5
      status: ATTACKED
      evidence: The facade pin uses a per-test random password across six property shapes and asserts it absent from every listed surface, with the host present; the surface audit covers Debug impls, errors, tracing and the facade reprs; fold 1's §6 traces catalog-provider Debug text to every sink and names the one raw-password path (repark-distributed).
      artifacts: [python/repark/tests/test_source_url_redaction_1.py, crates/repark-core/src/session/tests/conf_dump_redaction.rs]
    - id: AT-6
      status: ATTACKED
      evidence: The session keeps raw rows for S3 endpoint resolution and redacts only at the display accessor, pinned with an endpoint carrying userinfo; explicit conf.get stays raw.
      artifacts: [crates/repark-core/src/session/tests/conf_dump_redaction.rs]
    - id: AT-7
      status: N/A
      justification: Display paths only; one linear scan per displayed value.
    - id: AT-8
      status: ATTACKED
      evidence: No new dependency; the binding reaches the redactor through repark_core::redaction, so the crate DAG gains no edge; fold 1 uses the existing repark-iceberg and repark-spark edges to repark-common and adds none for repark-functions (recorded in §4).
      artifacts: [crates/repark-core/src/lib.rs, crates/repark-python/src/session_sources.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Masked values keep the user, host, port, database and non-secret parameters for debugging; the divergence is a dated registry row with the Spark measurement.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: The brief's mutation (redact_value key-only) reds the common pin, the four core pins and two facade tests; fold 1's M-1 (masker as identity) reds the six SQL display pins, the knob, span and four facade pins, M-2 reds the authority pin and M-3 the SET-regex pin; every tree was restored and re-run green.
      artifacts: [crates/repark-common/src/redaction.rs, python/repark/tests/test_source_url_redaction_1.py]
  complete: true
```

## 7. Functional readers of display output (fold 3, R-1)

| Site | Reads | Verdict |
|---|---|---|
| `python/repark/src/repark/spark/catalog.py` `get_database` | was `DESCRIBE NAMESPACE` Comment / Location | now the native `namespace_metadata`, unmasked (C-040) |
| `python/repark/src/repark/spark/catalog_surface.py` `_table_properties` | `DESCRIBE TABLE EXTENDED` `Table Properties`, only `current-snapshot-id` | non-secret key and numeric value; never redacted |
| `catalog_surface.py` `_table_comment` (`getTable().description`) | `DESCRIBE TABLE EXTENDED` `Comment` row | that row is not masked |
| `catalog_surface.py` `_partition_source_columns` | `# Partition Information` rows | column names only |
| `catalog.py` `listDatabases` / `databaseExists` | `SHOW NAMESPACES` | names only |
| `crates/repark-core/src/session.rs` S3 endpoint resolution | the raw conf rows (`&self.conf_dump`) | raw (C-008) |
| `crates/repark-core/src/session/text_write_format/select.rs` legacy policy | the raw conf rows | raw (C-031) |
| `session_core.py` `.config(conf=…)` | the caller's own `SparkConf.getAll()` | an external object, raw |

## SOURCE-URL-REDACT-2 (2026-10-06)

The #975 re-verify verdict measured three S1 classes still echoing raw: non-URL credential shapes on 13 Plan/Config knobs at the builder/toml/dbt doors, `datafusion.*` doors appending raw engine text after a masked value, and a chained `ValueError` printing the raw value for the integer knobs. The ruled design registers credential-shaped config values at the entry doors and masks the registered set at the Python error boundary; no engine-text heuristics (fold 2's `deleted-records` over-mask stands as the reason).

Doors registered: Rust `ReparkSessionBuilder::build` (builder map plus merged `repark.toml` pairs, one line at the 1000-line ceiling), Rust `apply_runtime_config` (`set`/`restore_runtime_config`), Rust `maybe_apply_runtime_set` (engine-direct SQL `SET`), Python `RuntimeConfig.set`, Python `_forward_datafusion_conf`, and `_native.register_config_value` for the Python doors. The dbt adapter's `session_properties` feeds `builder.config` (`python/dbt-repark/src/dbt/adapters/repark/session.py`), so the builder door covers it. `Builder._set_config_entry` itself does not register: `session_core.py` sits on its exact 2277 `check_lib_py` baseline, every Python validation between `.config()` and the native build already masks (C-017) with the one chain cut by C-061, and the Rust build registers the merged map before its own validation runs.

D-4 siblings (`catalog_resolution.py:117`, `reader.py:253`, `sql_relations.py:176`) re-raise around table identifiers and a CSV delimiter, not config values: out of scope, unchanged.

`STATUS.md` carries no v1.5.3 card row on this branch (it starts at #975's head, before the card); the orchestrator applies that update at merge time.

| C-057 | D-1: the registry lives in `repark-common::redaction`: `register_config_value` keeps a value only when `mask_value_credentials` changes it; the store is process-wide, 256 entries oldest-evicted behind a poison-safe `Mutex`, no new dependency, no `unwrap`/`expect`; `mask_registered_values` swaps each kept value and its `{:?}` inner form for the masked render. | `redaction::tests::registered_config_values_mask_every_shape_and_plain_values_stay_out`, `::unregistered_text_without_credentials_is_byte_identical`, `::the_registry_holds_256_values_and_evicts_the_oldest`. | PROVEN | `cargo test -p repark-common` 72 passed, incl. the three registry pins. |
| C-058 | D-2 Rust: the builder door registers the merged config map in `build()` via `register_config_map` before validation; `apply_runtime_config` registers before dispatch (`set` and `restore`); `maybe_apply_runtime_set` registers the parsed SQL `SET` value before dispatch; `_native.register_config_value` is exposed beside `_native.mask_value_credentials`; no new crate edge. | The three one-line doors; `session_sources.rs::register_config_value`. | PROVEN | Rebuilt wheel; the 48-cell pins green; `check_crate_dag` clean with no new edge. |
| C-059 | D-2 Python: `RuntimeConfig.set` and `_forward_datafusion_conf` register through `_secrets.register_config_value` before validation; SQL `SET` rides `conf.set` (non-`datafusion.*`) or the Rust `SET` door; dbt `session_properties` rides `builder.config` (read, not changed). | `builder_conf.py`, `session_configuration.py`, `_secrets.py`. | PROVEN | Same pins green; the dbt path read-confirmed at `session.py`. |
| C-060 | D-3: `mask_user_visible` is `mask_url_userinfo(&mask_registered_values(message))`; every boundary site already calls it. | `exceptions.rs::mask_user_visible`. | PROVEN | D-3 mutation (URL-only boundary): 24 red on the registry-covered cells, zero shuffle cells; restored green. |
| C-061 | D-4: `lookup_int_entry` raises `from None` when the chained `ValueError` text contains the value, keeping class and wording with only the value masked; the three `except ValueError` siblings re-raise around non-config values and stay. | `session_configuration.py::lookup_int_entry`. | PROVEN | D-4 mutations (chain kept, message unmasked): 6 red each on builder+toml shuffle; restored green. |
| C-062 | D-5: `_requirement_error` echoes through `mask_credentials` (its value is digits-only by construction; uniformity with `_type_mismatch_error`); the remaining value-interpolating raises in the session directory echo non-config values and stay. | `sql_set_statements.py::_requirement_error`. | PROVEN | Over-mask guard green: 54 passed on the redaction files; 2150 passed, 0 failed on the error/conf sweep. |
| C-063 | The 48-cell facade pin (`test_source_url_redact_2.py`): three shapes × four doors × four knobs; the full traceback never carries the marker and the exception class matches the base run (recorded isolated per door before the change; SQL `SET` is quoted so the `datafusion.*` leg reaches the config layer). | `test_source_url_redact_2.py`. | PROVEN | `test_source_url_redact_2.py` 48 passed on the rebuilt wheel; classes match the isolated base run. |
| C-064 | Mutations and re-runs: D-3 without `mask_registered_values` reds the builder/toml pins, D-4 without `from None` reds the traceback pin, the re-verify `shapes.py`/`chain.py` show no marker, and the over-mask guard stays green untouched. | The mutation runs; the `/tmp` probe copies. | PROVEN | Mutations red as designed; shapes.py full re-run 0 leaks (base: 29 on the pin knobs); chain.py matches the lane's base output minus the 3 finding-1 chains (accepted echoes 17=17); over-mask guard green untouched (the sweep ran on the pre-clippy-fix wheel and that fix is behavior-identical). |
| C-065 | Round B R-B1: `lookup_int_entry` cuts the chain whenever the value is credential-shaped (`mask_credentials(value) != value`), replacing the `value in str(error)` test that missed escaped, quoted and CPython-truncated (`%.200R`) renders; plain non-integer values keep the chained `ValueError`, class and wording unchanged. | `test_credential_shaped_integer_refusal_cuts_the_chain` (three knobs × backslash/long). | PROVEN | Six cells green on the rebuilt wheel; the `guard.py`/`guard_long.py` probes show no marker; restoring the substring guard reds the backslash and long cells. |
| C-066 | Round B R-B2: every Rust site that attaches `_spark_message_parameters` (`writer_layout.rs`, `dataframe_stats.rs`, `orc_io.rs`) builds the dict through one shared `exceptions::masked_message_params` helper that passes each value through `mask_user_visible`; classes and keys are unchanged. | `test_orc_message_parameters_mask_url_userinfo` (file and hdfs forms). | PROVEN | Both cells green: `getMessageParameters()` carries no userinfo and keeps `{"path"}`; skipping the ORC-site mask reds both. |
| C-067 | Round B R-B3: `_secrets.scrub_exception` walks an exception and its `__cause__`/`__context__` chain and rewrites each link's string args through `_native.mask_value_credentials`; it runs at the facade's native-wrapping re-raise points — `catalog.py`, `writer_text.py`, `writer_readwriter.py`, `eager.py` directly, and via `_sql_udf_clean_exception` (seven `session_core.py` sites) and `_export_engine_error` (three live `core.py` sites) so the two exact-baseline files stay untouched. Skipped: the `ImportError` chains, `row.py`, `functions_byname.py`, the local-`ValueError` D-4 siblings (`reader.py:254`, `session_configuration.py:392` — the latter cut by C-065) and the dead `core.py:3780` raise. | `test_rest_catalog_uri_chain_carries_no_userinfo`, `test_reader_path_chain_carries_no_userinfo`, `test_scrub_exception_masks_cause_chain_args`. | PROVEN | All three green on the rebuilt wheel; making `scrub_exception` a no-op reds the mechanism pin. |
| C-068 | Round B R-B4: reader and writer `.option`/`.options` register every stored value (`reader.py`, both `writer_readwriter.py` `option` methods; `options()`, `load(**extra)` and the text `compression`/`lineSep` overlay ride through `option`), so the registry mask covers the native option-door refusals; the two Python-side compression refusals (`writer_layout.py`) echo through `mask_credentials`, since the registry mask runs only at the Rust boundary. | `test_writer_option_value_never_echoes`. | PROVEN | The `attack.py` writer-option cell raises with no marker on the rebuilt wheel. |
| C-069 | Round B pins and gates: the six new pins in `test_source_url_redact_2.py` plus the round-B mutations (substring guard restored, ORC param mask skipped, `scrub_exception` no-op — each recorded red) and the over-mask guard green. | The new pins; the mutation runs. | PROVEN | Pins green; the three mutations each red at least one pin; the redaction-file suite and the error/conf sweep pass unchanged. |

## SOURCE-URL-REDACT-2 fold 2 (2026-10-07)

The #975 re-verify verdict on fold 1 closed every first-round S1 and returned five follow-ups: the whole-token boundary blocks masking next to `= . - _` (S2), `scrub_exception` over-masks user diagnostics with the full credential heuristic and mutates foreign exception objects (S2), reader-side Python refusals echo option and format values raw (S2), `OSError` links and custom `__str__` slip through the args-only scrub (S3), and the trailing boundary plus the scrub call sites are unpinned with a vacuous REST chain pin (S3). Fold 2 masks at every non-alphanumeric boundary, narrows the scrub to the registered-values plus URL-userinfo masks on rebuilt copies (with an `OSError` rebuild), masks the reader/writer refusal echoes, and pins each scrub call site. `reader.py` and `writer_readwriter.py` each shed one method to a sibling module (pure moves) to hold their ceilings.

| C-070 | An exception class with its own `__str__` still renders secrets after `scrub_exception`: the fold-2 scrub masks `args` plus the `OSError` fields on rebuilt copies only, and the same holds for stdlib links whose `str` is not derived from `args` (`UnicodeDecodeError`). | A design ruling on whether the scrub may wrap such links or must leave them. | OPEN | Fold 2 leaves the behavior as measured by `fold_side.py` (`custom __str__: LEAK`); no pin, by the brief. |

## SOURCE-URL-REDACT-2 fold 3 round A (2026-10-07)

The second re-verify failed on one S1: fold 2's no-mutation scrub left the original raw, and the export and SQL-UDF sites still chained `from` it. Round A chains every mapped re-raise from the scrubbed copy, never fails open on a copy, scrubs the append re-raise and masks message parameters on the copy. The row below is the ruled-out residual; the `__context__` item round A filed beside it closes in round A2 (C-071).

| C-072 | C-070 stays open after fold 3 round A: a link with its own `__str__` still renders secrets, because the scrub masks `args`, the `OSError` fields and the message parameters only. | The C-070 ruling. | OPEN | Out of scope for fold 3 round A by the brief; no pin. |

## SOURCE-URL-REDACT-2 fold 3 round A2 (2026-10-07)

Owner ruling (2026-10-07): raise the scrubbed exception below the `except` block so `__context__` is None, pin it, and close the open item. Every `scrub_exception` call site now keeps the scrubbed copy in `failure` inside the handler and raises the mapped exception after the `try`, `from failure`. The DataFrame-UDF door (F3-5, round A's Q1) joins them. `core.py` and `session_core.py` hold their exact `check_lib_py` baselines (3971 and 2277) through two pure moves: `_register_cache_frame` to `cache_handle.py` (re-imported by `core`) and `ReparkSession._register_auto_memory_catalog` to `session_surface.register_auto_memory_catalog` (bound back as a class attribute).

| C-071 | A mapped re-raise at a `scrub_exception` site leaves `__context__` None and chains `__cause__` to the scrubbed copy, never the original: the sites are the `core.py` export doors (`to_arrow`, both `to_arrow_batches` arms), the nine `session_core.py` SQL-UDF doors (the seven `_sql_udf_clean_exception` raises and the two `UnsupportedOperationException` raises), `catalog.py` `list_databases`, `eager.py` `_eager_materialize`, and the overwrite and append raises in `writer_text.py` and `writer_readwriter.py`. A raise in a generator or a callee still takes the caller's handled exception as context when the caller is itself inside a handler. | `test_export_door_raises_outside_the_handler` (`to_arrow`, `to_arrow_batches`), `test_export_cast_error_context_is_none` (four actions), `test_sql_udf_door_raises_outside_the_handler`, `test_list_databases_raises_outside_the_handler`, `test_eager_door_raises_outside_the_handler`, `test_writer_doors_raise_outside_the_handler` (readwriter and text, overwrite and append). | PROVEN (CLOSED 2026-10-07 by owner ruling) | All cells red on `ab336306` (`__context__` is the raw original) and green on the head. Mutations, each one site's raise moved back inside its `except`, each recorded red: M-1 `core.py` `to_arrow` (3 cells), M-2 `session_core.py` `_sql_with_registered_udfs` (1 cell). `export_door.py` shows the chain `ArrowInvalid`, `NoneType`. |
| C-073 | F3-5: the classic UDF and the four pandas-UDF user-exception doors in `udf_bridge.py` interpolate the scrubbed copy, pass the `traceback.format_exc()` detail through `_native.mask_user_visible`, and raise after the handler `from` the scrubbed copy. User text without a secret stays byte-identical. | `test_dataframe_udf_door_formatted_traceback_is_masked`, `test_pandas_udf_door_formatted_traceback_is_masked`, with `test_dataframe_udf_user_text_is_byte_identical` unchanged. | PROVEN | `doors_fmt.py` prints `TOTAL 0` (base: `df_udf_collect` 3 lines, `str` leak). Both pins red on `ab336306`. Mutation M-3 (`_run_python_udf_on_batch` raise moved back inside its `except`) recorded red. |

## SOURCE-URL-REDACT-2 fold 3 round B (2026-10-07)

The second re-verify filed an S3: the letters-and-digits boundary under-masks when a registered value's own edge character is punctuation and its neighbour is a letter or digit (`boundary.py`). Ruling: skip a side only when the needle's edge character and the neighbour on that side are both ASCII letters or digits.

| C-074 | `replace_whole_tokens` skips an occurrence on a side only when the registered value's edge character on that side and the neighbour there are both in `[A-Za-z0-9]`; otherwise that side is a boundary. `Driver=x;Uid=u;Pwd=BndPw01;` followed by `Encrypt=yes` masks, and `cpw=abc` with a registered `pw=a` stays byte-identical. | `redaction::tests::registered_values_mask_when_their_own_edge_is_punctuation` (five cells: trailing `;` before a letter and a digit, leading `;` after a letter, a digit and `\x1b[31m`), with `registered_values_mask_at_punctuation_boundaries` and `registered_values_keep_letter_or_digit_neighbours` unchanged. | PROVEN | `cargo test -p repark-common` 79 passed. Mutation M-B1 (neighbour-only rule restored) recorded red: 5 of 5 cells. `boundary.py` masks `odbc ; then letter`; `cpw case` is unchanged and `scrub_overmask.py` is byte-identical. |
| C-075 | `replace_whole_tokens` also treats the left side as a boundary when the text immediately before the occurrence ends an ANSI CSI sequence (`ESC [`, then `[0-9;]*`, then one final byte in `0x40..=0x7E`); the right side is unchanged. `\x1b[31m` and `\x1b[1;31m` followed by `host=h user=u password=BndPw02` mask, while a bare `m` or `[31m` prefix with no ESC keeps the skip. | `redaction::tests::registered_values_mask_after_an_ansi_csi_sequence` (two masking cells: `\x1b[31m` and `\x1b[1;31m`; two byte-identical guards: `m` and `[31m` without ESC), with the C-074 pins unchanged. | PROVEN (CLOSED 2026-10-07 by orchestrator ruling, round B2) | `cargo test -p repark-common` 80 passed. Mutation M-B2 (CSI check dropped) recorded red on the `\x1b[31m` cell. `boundary.py` prints `dsn in ANSI-colored leak=True` on `5c5e84db` and `leak=False` on the head, with `odbc ; then letter` still masked and `cpw case` unchanged; `scrub_overmask.py` is byte-identical to the `5c5e84db` build. |

## SOURCE-URL-REDACT-2 fold 4 (2026-10-07)

The third re-verify passed and filed one S2 and five S3s. Fold 4 closes them generally. Every user-callback re-raise takes `(detail, failure)` from `_secrets.scrub_user_failure`, which pairs the `format_exc()` text masked through `_native.mask_user_visible` with the `scrub_exception` copy. Each site interpolates the copy and raises after the handler, `from` the copy. The site list comes from `grep -rn "format_exc()\|raised {type\|raised {\|except Exception as"` over the UDF, UDTF, pandas and arrow modules, plus a read of every call into user code there:

| Module | Site | Door |
|---|---|---|
| `dataframe/udf_bridge.py` | `_run_pandas_udf_scalar_on_batch`, `_run_pandas_udf_scalar_iter` (call and consume), `_run_python_udf_on_batch`, `_apply_ordered_window_pandas_udf` | already F3-5 (C-073), now through the helper |
| `dataframe/udf_bridge.py` | `_grouped_agg_pandas` (moved from `joins_columns.py`) | non-windowed and unbounded-window GROUPED_AGG |
| `dataframe/grouped_arrow.py` | `_call_grouped_user_func` | `applyInPandas`, `applyInArrow`, cogroup `applyInPandas` and `applyInArrow` (via `cogroup.py`) |
| `dataframe/grouped_arrow.py` | `_iter_apply_in_arrow_results` | iterator-form `applyInArrow` |
| `dataframe/core.py` | `_iter_map_in_arrow_output` | `mapInArrow`, `mapInPandas` (via `udf_bridge._map_in_pandas_arrow_batches`) and the UDTF bridges riding `mapInArrow`; the inner handler around the call was redundant with the outer one and is gone |
| `udtf.py` | `_map_udtf_batches`, `_map_arrow_udtf_batches` | scalar and Arrow UDTF `start`, `eval`, `terminate` |
| `table_arg.py` | `_map_table_udtf_batches` | table-argument UDTF `start`, `eval`, `terminate` |
| `functions_udf.py` | `_normalize_python_udf_return_type_sql` | a duck-typed `returnType.simpleString()` (no traceback text; scrubbed copy only) |

Not re-raises, so out of scope: `foreach`, `foreachPartition` and `transform` let the user's own exception propagate untouched, with no RePark text. The other `except Exception as` hits in these modules wrap RePark's own DDL and type-mapping errors, not user code. Room: `_grouped_agg_pandas` moved to `udf_bridge.py` (`joins_columns.py` 1169 → 1117), `_refuse_udtf_as_scalar_udf` moved to `udtf.py` (`functions_udf.py` 1300 → 1287), and `core.py` ratchets 3971 → 3965 from the dropped inner handler. All are pure moves or behaviour-neutral, and every baseline ratchets down in `check_lib_py.py`.

| C-076 | H1: every user-callback door in the table above interpolates the `scrub_exception` copy, masks the `format_exc()` detail through `_native.mask_user_visible` (`_secrets.scrub_user_failure`), and raises after the handler `from` the copy, so `__context__` is None. | `test_user_callback_door_formatted_traceback_is_masked` (16 doors: GROUPED_AGG plain and unbounded window, `applyInPandas`, cogroup `applyInPandas`, `applyInArrow` table and iterator, cogroup `applyInArrow`, `mapInPandas`, `mapInArrow` call and consume, UDTF `eval`/`start`/`terminate`, table-argument UDTF `eval`, Arrow UDTF `eval`, `simpleString()`). | PROVEN | 16 of 16 red on `bafd4a42` and green on the head. Mutation M-H1a (`scrub_user_failure` returns the raw `format_exc()`) recorded red: 10 cells. Mutation M-H1b (`_call_grouped_user_func` raise moved back inside its `except`) recorded red: 4 cells (`__context__` is the raw `ValueError`). `reverify3/probes/grouped_agg.py` prints `TOTAL 0`, and `udf_doors.py` drops from 18 to 3; the remaining 3 belong to H2 and H3. |
| C-077 | H3: every user-callback door that passed a `PySparkException` through raw now runs it through `scrub_exception`. It re-raises in place when nothing masks (identity kept), and otherwise raises the masked copy, of the same class with its error class kept, after the handler. The sites are the six `udf_bridge.py` handlers, `_call_grouped_user_func`, `_iter_apply_in_arrow_results`, `core.py` `_iter_map_in_arrow_output`, and the three UDTF `eval` handlers. | `test_user_raised_pyspark_exception_is_scrubbed` and `test_user_raised_pyspark_exception_without_a_secret_keeps_identity` (DataFrame UDF, pandas UDF, `applyInPandas`, `mapInArrow`), plus `test_dataframe_udf_door_scrubs_a_user_raised_pyspark_exception` (secret and plain). | PROVEN | The 4 secret door cells are red on `55c60e15` and green on the head; the identity cells are green on both. Mutation M-H3 (`_run_python_udf_on_batch` back to `except PySparkException: raise`) is red on the direct `secret` cell. The door cells stay green under it, because the outer mapInArrow handler scrubs too. `udf_doors.py` `df_udf_pyspark_err` shows `str_leak=False` with 0 formatted lines. |
| C-078 | H4: the SCALAR_ITER "while consuming output" door in `_run_pandas_udf_scalar_iter` masks its own `format_exc()` detail and raises after the handler from the scrubbed copy. | `test_scalar_iter_consume_door_formatted_traceback_is_masked` (the door, in `scalar_iter.py`'s shape) and `test_scalar_iter_consume_site_masks_its_own_detail` (the site, called directly). | PROVEN | Re-verify mutation M1 (that site's detail left as the raw `traceback.format_exc()`) is recorded red: 1 cell, the direct site pin. The door cell and `scalar_iter.py` (`TOTAL 0`) stay clean under M1, because the mapInArrow passthrough scrub (C-077) masks the mapped message a second time. The site pin is the one that holds this site. |
| C-079 | H2: `scrub_exception` walks `BaseExceptionGroup.exceptions` beside `__cause__`/`__context__`. It treats a group message and any `__notes__` string as link text, and rebuilds a changed group as `type(g)(masked message, scrubbed subs)`, so the class is kept. If that constructor fails, it falls back to `BaseExceptionGroup(masked message, subs)`, which yields an `ExceptionGroup` when every sub is an `Exception`. It masks `__notes__` on every copy and never touches the original. A group or note with nothing to mask keeps its identity. Ancestors of a changed link are marked through a parent map, so a deep chain is linear: 5000 links took 8.4 s on `bc7d9945` and 0.06 s on the head. The C-070/C-072 custom `__str__` item stays open; a group's message is the one non-`args` text this closes. | `test_scrub_exception_masks_groups_and_notes` (`sub`, `message`, `subclass`, `base_group`, `nested`, `note`, `sub_note`), `test_scrub_exception_keeps_a_clean_group_and_note`, `test_dataframe_udf_door_masks_groups_and_notes` (`group`, `note`), `test_scrub_exception_walks_a_deep_chain_in_linear_time`. | PROVEN | 9 cells red on `bc7d9945` (`_secrets.py` reverted); the identity guard is green on both. Mutation M-H2a (`.exceptions` not walked) recorded red: 4 cells. Mutation M-H2b (notes copied unmasked) recorded red: 3 cells. `udf_doors.py` prints `TOTAL 0`. `edge_classes.py` shows the `ExceptionGroup`, `ExceptionGroup_msg`, `BaseExceptionGroup_KI` and `note` copies with `str_leak=False`, `fmt_leak=False` and the class kept. |
| C-080 | H5: the last-resort stand-in keeps the `BaseException`/`Exception` split. An `Exception` link becomes a masked `PySparkException`. A `BaseException`-only link becomes the nearest builtin `BaseException`-only base in its MRO that builds from the masked text (for example `KeyboardInterrupt`), else a masked `BaseException`, and never an `Exception`. An unbuildable `BaseExceptionGroup` subclass that is not an `Exception` never falls back to an `ExceptionGroup`. Every same-class copy (the `copy.copy`, `__new__`, `OSError` and group paths) carries the original's `__dict__` and `__slots__` values over, private slot names mangled, with `str` values masked; the original is untouched. | `test_scrub_exception_base_only_stand_in_is_never_an_exception` (the re-verify's `KINew` shape), `test_scrub_exception_exception_stand_in_is_a_pyspark_exception`, `test_scrub_exception_unbuildable_group_keeps_the_exception_split` (three shapes), `test_scrub_exception_new_copy_carries_slots_masked`, `test_scrub_exception_new_copy_carries_keyword_only_attributes_masked`. | PROVEN | 4 cells red on `3d2f6eb8` (`_secrets.py` reverted) and green on the head. Mutation M-H5a (stand-in always `PySparkException`) recorded red: 2 cells. Mutation M-H5b (no attribute carry) recorded red: 2 cells. Mutation M-H5c (the group fallback ignores the split) recorded red: 1 cell. `edge_classes.py` shows `KI_new_override` scrubbed to `KeyboardInterrupt` with `is_base_only=True`, `Slotted` with `slot_url_copy='http://u:***@h/x'`, and `KwOnly` with `code_copy=3`. A follow-up commit makes the attribute carry tolerate a link with no `__dict__` and a non-sequence `__slots__`, so the scrub itself never raises. The same pins stay green. |
| C-081 | H6: `replace_whole_tokens` treats the left side as a boundary when the text before the occurrence ends an ECMA-48 CSI sequence: introducer `ESC [` or C1 `U+009B`, then any parameter bytes `0x30..=0x3F`, then any intermediate bytes `0x20..=0x2F`, then one final byte `0x40..=0x7E`. The backward scan runs over `text[cursor..start]` only, so consecutive scans never overlap and the walk stays linear. The right side is unchanged. | `redaction::tests::registered_values_mask_after_an_ecma_48_csi_sequence`: seven masking cells (`\x1b[?25h`, `\x1b[38:5:196m`, `\x1b[1 q`, `\u009b31m`, `\x1b[m`, `\x1b[;;;m`, a double ESC) and five byte-identical guards (an unterminated OSC ending in a letter, `[?25h` without ESC, bare `38:5:196m` and `1 q`, a `\u009c` introducer). The C-074/C-075 pins are unchanged. | PROVEN | `cargo test -p repark-common` 81 passed. Mutation M-H6 (the `[0-9;]*` grammar of `bafd4a42` restored) recorded red on the new cell. `csi.py` on the rebuilt wheel differs from `bafd4a42` in exactly `private_mode`, `colon_subparams`, `intermediate_byte` and `c1_csi`, which are now masked; every other line, including `osc_unterminated_letter masked=False`, is the same. Its timing cases are 0.145 s at 4 MB of parameters, 0.134 s for 2,000 occurrences and 0.018 s for the near misses, all under 0.5 s. An extra probe over 4 MB space runs, mixed `0x20..=0x3F` runs and 20,000 adjacent occurrences stays under 0.11 s. |
| C-082 | K1: scrubbing is total. `_secrets.mask_credentials`, `mask_url_userinfo` and `mask_user_visible` retry a native `UnicodeEncodeError` on `text.encode("utf-8", "surrogatepass").decode("utf-8", "replace")`, which keeps every non-surrogate character, and return the input unchanged when that masks nothing. `scrub_exception` masks through `mask_user_visible` and returns a masked stand-in, never the raw value, if anything else raises inside the walk. `scrub_user_failure` never raises, and `core.py`'s mapInArrow handler calls it instead of the native mask directly. A lone surrogate in the user's text therefore leaves every user-callback door as its normal class, masked, with `__context__` None, instead of `UnicodeEncodeError` chained to the raw original. | `test_user_callback_door_masks_a_lone_surrogate` (19 doors: the 16 C-076 doors, the DataFrame UDF, the SQL UDF and a DataFrame-UDF `OSError` with a surrogate filename), `test_scrub_exception_masks_a_lone_surrogate_message`, `test_scrub_exception_masks_an_os_error_with_a_surrogate_filename`, `test_scrub_user_failure_masks_a_lone_surrogate`, `test_scrub_exception_keeps_a_clean_surrogate_message_identity`, `test_mask_entry_points_are_total_on_a_lone_surrogate` (3 entry points), `test_scrub_exception_returns_a_masked_stand_in_when_the_walk_raises`. | PROVEN | Mutation MK1a (the `UnicodeEncodeError` fallback removed) recorded red: 25 cells. Mutation MK1b (the outer `scrub_exception` guard removed) recorded red: 1 cell. `reverify4/probes/doors4.py surrogate` drops from `TOTAL 15` on `fa82ccbb` to `TOTAL 3`, and those 3 are the out-of-scope `foreach`, `foreachPartition` and `transform`, the user's own exception with no RePark text; every other door is `PySparkException` (or its own class) with `repr_leak=False` and `ctx=NoneType`. `helper_edges.py` prints `surrogate ok`. |
| C-083 | K2: `scrub_exception` treats every note carrier `traceback` renders as link text. A `__notes__` that is a `str` is compared and masked as a `str` on the copy. A non-`str` item of a list or tuple `__notes__` is compared as `str(item)` and replaced on the copy by `mask(str(item))` as a `str`, or by its type name when `str(item)` raises. The original is untouched. | `test_scrub_exception_masks_non_str_note_carriers` (`object`, `bytes`, `str_notes`, `refusing_str`). | PROVEN | 4 cells red on `af53dcad` (`_secrets.py` reverted) and green on the head. Mutation MK2a (non-`str` items copied raw) recorded red: 3 cells. Mutation MK2b (a `str` `__notes__` ignored) recorded red: 1 cell. `helper_edges.py` shows `nonstr_note_object`, `nonstr_note_bytes` and `notes_is_str` with `same_obj=False` and `fmt_leak=False`. A `bytes` or other non-sequence `__notes__`, rendered through `repr`, stays out of scope. |

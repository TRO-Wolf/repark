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
| C-015 | Fold 1 item 1: `https://h:443/p?m=a@b.c` comes out unchanged, `https://u:p@h/x@y` masks to `https://u:***@h/x@y`, an `@` inside a fragment or a query is never userinfo (`https://h/p#frag:x@y`, `https://h?u:p@x` unchanged; `https://u:p@h/p#a:b@c` → `https://u:***@h/p#a:b@c`), and a bracketed IPv6 host with a path `@` stays whole. Mutation: the authority ends at whitespace only. | `userinfo_is_read_only_inside_the_authority`. | PROVEN | §2 green; §3 M-2 red at `redaction.rs:251`. |
| C-016 | Fold 1 item 2: `DESCRIBE TABLE EXTENDED`, `SHOW CREATE TABLE` and `SHOW TABLE EXTENDED` (all through `spark_table_properties`, key rule kept), `SHOW TBLPROPERTIES` of a table and of a view (with and without a key; the view's `location` row too), and `SHOW CREATE TABLE` of a view mask a credential inside a property value, where Spark 4.1.2 prints it on all six (§5); `CONNECT-DIV-url-userinfo` names every surface. RePark's `DESCRIBE TABLE EXTENDED` of a view renders no property row (not-applicable). Mutation: `mask_value_credentials` as the identity. | `tests::property_display_redaction` — one SQL pin per surface (six tests); `SHOW TABLE EXTENDED` is compared with its per-character `, ` separators removed, so the password check is not vacuous. | PROVEN | §2: `cargo test -p repark-spark --lib` 2615 passed. §3 M-1: all six red at `property_display_redaction.rs:54`. |
| C-017 | Fold 1 item 3: every configuration refusal that echoes a rejected value runs it through `mask_value_credentials` — Rust: `session.*` integers (`wiring.rs`), maintenance durations (`maintenance.rs`), `partitionOverwriteMode`, `timeParserPolicy`, the session time zone (`INVALID_CONF_VALUE.TIME_ZONE`), the S3 boolean and endpoint refusals, the DataFusion conf and dead-key refusals, `memory_limit`, `escapedStringLiterals`; facade: `_secrets.mask_credentials` (the native masker) in `_config_value_error`, `parse_timestamp_type`, the `datafusion.*` forward refusals, `normalize_display_style`, the integer-config refusal, `_normalize_display_int`'s non-integer arm and `_type_mismatch_error`. | `config_file::tests::wiring::typed_knob_refusals_mask_a_password_in_the_echoed_value` (the one Rust pin); `test_source_url_redaction_1.py::test_knob_refusals_never_carry_the_password` (time zone, `SET spark.sql.shuffle.partitions`, display style). | PROVEN | §2 green; §3 M-1 red at `wiring.rs:302` and in the facade test. `test_production_file_size.py`'s moved-symbol hashes follow the three edited bodies (`_config_value_error`, `_forward_datafusion_conf`, `normalize_display_style`). |
| C-018 | Fold 1 item 4: the `catalog.memory_catalog`, `catalog.memory_catalog_cached` and `catalog.memory_catalog_cached_with_props` spans record `warehouse` through `mask_value_credentials`; the `repark-iceberg → repark-common` edge already exists (`normal`, declared in `check_crate_dag.py`). | `catalog::tests::memory_props_span::memory_catalog_span_masks_a_password_in_the_warehouse`. | PROVEN | §2: `cargo test -p repark-iceberg --lib` 804 passed. §3 M-1 red at `memory_props_span.rs:80`. |
| C-019 | Fold 1 item 5: `_REDACTION_RE` is Spark 4.1.2's measured `SET` rule, `(?i)secret|password|token|access[.]?key|url` against the key or the value — `spark.redaction.regex` (default `(?i)secret|password|token|access[.]?key`) unioned with `spark.sql.redaction.options.regex` (default `(?i)url`), both defaults read from the oracle's JVM config entries, and the probe (§5) redacts `accesskey`, `access.key`, a `my url` value, an `AccessKey here` value and a `*_url` key while `access_key` stays visible. Mutation: the old `access[.]key` pattern. | `test_source_url_redaction_1.py::test_set_redaction_matches_the_spark_4_1_2_default_regexes`; `test_sql_set_door_1.py` unchanged and green. | PROVEN | §2 green; §3 M-3 red (`assert {'spark.p.acc…'my url', …} == {…dacted)', …}`). |
| C-020 | Fold 1 item 6 (audit only, no product change): every place a catalog provider (`ReparkCatalogProvider`, `IcebergCatalogProvider`, the iceberg-rust catalogs) reaches `{:?}` in an error, a log, a span, an EXPLAIN or a Python repr is listed in §6 with file and line and a verdict. | §6. | PROVEN | §6: one user-visible raw-credential path, in `repark-distributed` (not in the Python wheel); none in the shipped facade. |

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

## 4. Measured, out of scope

- **`repark-functions` boolean-knob refusals** echo the rejected value raw:
  `crates/repark-functions/src/case_sensitive.rs:79` (`spark.sql.caseSensitive`),
  `merge_schema.rs:59` (`spark.sql.iceberg.merge-schema`), `ansi.rs:116`
  (`spark.sql.ansi.enabled`). `repark-functions` has no `repark-common` edge and
  `check_crate_dag.py` declares none, so per the fold none was added.
- **Per-call option refusals** echo the rejected value: reader options
  (`read_options.rs:173`, `:407`, `:426`), write options (`write_options.rs:178`, `:195`,
  `:204`), path-write format / mode / compression (`session/path_write.rs:48`, `:86`, `:178`,
  `:200`) and the path-write destination (`:333`). These are the caller's own arguments, not
  configuration; listed for the owner.
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
| A-2 | The parse's refusal echoes the props slice of that `Debug` text: `"{ICEBERG_TABLE_SCAN} debug text has an unterminated string in {text:?}"` | `crates/repark-distributed/src/iceberg_provider.rs:696-697` | **yes**, as a codec error from the distributed scan encode | **a raw password can reach it today**: the slice is the first `props: {…}` of the `Debug` text, the `StorageConfig` map, whose secret-named keys the fork masks but whose URL-embedded passwords it does not; the error fires when a value there carries a `}` or a `"` (an ODBC-braced or quoted password ends the slice early). `repark-distributed` is a delivered surface crate, not in the Python wheel. |
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

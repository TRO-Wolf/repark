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

**Out:** table properties (`DESCRIBE TABLE EXTENDED`, `SHOW TBLPROPERTIES`, `SHOW CREATE TABLE`,
`SHOW TABLE EXTENDED`), which are table data rather than source or catalog configuration; the
`SET` redaction regex's measured `url` gap (§4); tracing spans in `repark-iceberg` (§4).

## PROPOSITION LEDGER — SOURCE-URL-REDACT-1 — 2026-10-06

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `repark_common::redaction::mask_value_credentials` masks the password of a URL userinfo for every scheme (`postgresql`, `jdbc:postgresql`, `postgres`, `mysql`, `sqlserver`, `redis`, `http(s)`, `s3`, `thrift`, any `scheme://`), keeping the user, the host, the port and the database (`postgresql://alice:***@db.example.com:5432/sales`); a userinfo with no colon is masked whole (`https://***@h/x`); every URL in a value is masked. | `url_userinfo_password_is_masked_keeping_user_host_and_database`, `url_userinfo_is_masked_for_every_scheme`, `lone_userinfo_is_masked_whole`, `every_url_in_a_value_is_masked`. | PROVEN | §2: `cargo test -p repark-common --lib` green (10 redaction pins). |
| C-002 | Fail closed: when an `@` follows `://` with a `:` before it but the authority does not parse cleanly (a `/`, `?` or `#` before the `@`), the whole userinfo up to the last `@` of the whitespace-delimited region becomes `***` (`postgresql://***@db.example.com:5432/sales`). | `unclean_userinfo_fails_closed_to_the_whole_userinfo`. | PROVEN | §2 green. The consequence is recorded in §1: a URL whose path or query carries an `@` after a port (`https://h:443/p?m=a@b.c`) loses its host to the mask; the brief's rule is applied literally rather than guessed around. |
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
| `session.*` integer and maintenance duration refusals | not-applicable | Typed knobs; they echo the rejected text, which a URL is not meant to be. Recorded in §4. |
| `NamedSource` handle `repr` | not-applicable | Carries name, kind and key path; no property. |
| Tracing: `catalog.glue_catalog` span | redacted-before | Records `prop_keys` and `has_warehouse` only. |
| Tracing: `catalog.memory_catalog*` spans | not-applicable | Record `warehouse`, a filesystem or `s3://` path; `repark-iceberg` sits below `repark-core`. Recorded in §4. |

**Executor readings (no halt).**
- **The redactor's home.** `repark-common` is the one crate every consumer reaches (core,
  spark, connect, and the binding through `repark_core::redaction`), so the masker lives there.
  `prop_key_is_secret` moves with it because the parameter rule reuses it; `repark-core` keeps
  exporting it.
- **The conf dump fed S3 endpoint resolution.** Masking the stored dump would have handed
  object_store a `user:***@` endpoint, so the session stores the raw rows and redacts at the
  `conf_dump()` accessor (C-008).
- **Fail closed against host visibility.** The brief asks for both; where they collide (C-002)
  the mask wins.

## 2. Gates

Run on the committed tree under the timing-run wrapper (`flock` + `nice -n 19 taskset -c 48-63`
+ `CARGO_BUILD_JOBS=8`), exit codes verbatim.

| command | exit | output |
|---|---|---|
| `cargo test -p repark-common --lib` | 0 | 43 passed (10 redaction pins) |
| `cargo test -p repark-core --lib` | 0 | 1023 passed, 1 ignored |
| `cargo test -p repark-spark --lib` | 0 | 2609 passed, 5 ignored |
| `make rust-clippy` | 0 | no diagnostics |
| `cargo fmt --all --check` | 0 | no output |
| `make rust-panic-ban` | 0 | clean |
| `./scripts/check_rust_file_size.sh` | 0 | 1040 files clean |
| `./scripts/check_lib_rs.sh` | 0 | 11 crate roots clean |
| `python3 scripts/sync_map_md.py --check` | 0 | 361 maps clean |
| `python3 scripts/check_ledger_grammar.py` | 0 | 302 live ledgers clean |
| `python3 scripts/check_docs_links.py` | 0 | clean |
| `uvx ruff@0.15.22 check` / `format --check` (new test and the two touched facade files) | 0 | clean |
| `pytest` on a debug build (`uv sync --locked` + `maturin develop`): `test_source_url_redaction_1.py`, `test_session_sources.py`, `test_config_mirror.py`, and every other facade file that reads `getAll`, `SET` or redaction (`test_sql_set_door_1.py`, `test_a3_secrets_redaction.py`, `test_describe_namespace.py`, `test_describe_table.py`, `test_eager_budget_1.py`, `test_t3_ux_polish.py`, `test_builder_config_map.py`, `test_h2_group_h2.py`, `test_session_timezone_parity.py`, `test_session.py`, `test_production_file_size.py`) | 0 | 341 passed, 1 skipped |
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

## 4. Measured, out of scope

- **`SET` redaction regex lacks `url`.** Spark 4.1.2 redacts `SET k` / `SET` to
  `*********(redacted)` when the key or the value matches `url` (`spark.repark.test.runtime_url`,
  a value `has url inside`); the facade's `_REDACTION_RE` is
  `(?i)secret|password|token|access[.]key` and does not. With this unit the password is masked
  either way; the whole-value parity is a separate fix.
- **Table properties** (`spark_table_properties`: `DESCRIBE TABLE EXTENDED`, `SHOW CREATE TABLE`,
  `SHOW TABLE EXTENDED`; and `SHOW TBLPROPERTIES`) mask by key only.
- **`repark-iceberg` memory-catalog spans** record `warehouse` raw.
- **Typed-knob refusals** (`session.*` integers, maintenance durations) echo the rejected text.

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
      evidence: Empty password, empty userinfo, password containing @ / ? # and spaces, quoted with escapes, braced with }} escapes, unterminated quotes, multiple URLs in one value, multi-host authorities, an empty password= value, a=b=c.
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
      evidence: The facade pin uses a per-test random password across six property shapes and asserts it absent from every listed surface, with the host present; the surface audit covers Debug impls, errors, tracing and the facade reprs.
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
      evidence: No new dependency; the binding reaches the redactor through repark_core::redaction, so the crate DAG gains no edge.
      artifacts: [crates/repark-core/src/lib.rs, crates/repark-python/src/session_sources.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Masked values keep the user, host, port, database and non-secret parameters for debugging; the divergence is a dated registry row with the Spark measurement.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: The brief's mutation (redact_value key-only) reds the common pin, the four core pins and two facade tests; the tree was restored and re-run green.
      artifacts: [crates/repark-common/src/redaction.rs, python/repark/tests/test_source_url_redaction_1.py]
  complete: true
```

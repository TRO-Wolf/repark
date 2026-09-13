# Unit ledger — CFG-2 step 1 · named database sources register lazily and refuse loud on use

**Unit:** CFG-2 step 1 · **Date:** 2026-09-13 · **Branch:** `feat/cfg-2-step1` · **Base:** `origin/main`
**Model:** swe-2-high
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** CFG-1 left a load-time refusal on any non-empty `[<profile>.database]` table
because no registration seam existed. CFG-2 step 1 lands the ruled Rust seam of
[Card CFG-1's sibling CFG-2](../../roadmap/epic-term/roadmap-design-plan-2026-08-29.md):
parsed `SourceSpec`s ride `FileConfig` into the built session, `register_configured_sources`
installs a refusing catalog provider per auto-registered name, and `sources()` /
`source(name).ping()` expose the declared set — all without opening a connection.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands. Step 2
(the Python door) appends to THIS ledger in `staging/`; it does not move it.

**Not in this step:** the Python `sources()` / `source(name).ping()` facade surface and the
config mirror's `auto_register` field (step 2), the guide's source section (step 2),
`STATUS.md`, `briefs/next-sequence.md`, any dependency file, any `.config()` key family for
sources (D-3: sources come only from the TOML file), the connectors themselves
(`repark-connect`, roadmap 1.10).

## PROPOSITION LEDGER — CFG-2 step 1 — 2026-09-13

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | A profile carrying a non-empty `[<profile>.database]` table LOADS (D-2): `refuse_pending_sources` is gone, the parsed `SourceSpec`s reach `FileConfig.source_specs`, and every CFG-1 validation still refuses. | `database_source_block_loads` (replaces `database_sources_refuse_until_named_registration_lands`) | **PROVEN** | Red first (below): the pin failed on base with `database sources (default.database.postgres.company_db) are parsed but named-source registration arrives with CFG-2`. Green: `database_source_block_loads ... ok` in `cargo test -p repark-core config_file` — 64 passed, 0 failed, including every CFG-1 collision/shape refusal. |
| C-002 | `auto_register` is a per-entry TOML boolean inside `[<profile>.database.<kind>.<name>]` (D-5): a non-boolean value refuses naming the key path; it is not carried in `props`. | `auto_register_non_boolean_refuses_naming_key_path` | **PROVEN** | Red first (below): `auto_register = "yes"` loaded silently on base (parsed as an ordinary string prop). Green: the pin passes in the same `config_file` run — the refusal names `default.database.postgres.company_db.auto_register`, and `auto_register` never enters `props` (asserted via the `sources()` row and `SourceSpec` construction). |
| C-003 | `SELECT * FROM company_db.public.t` against a registered-but-unimplemented source refuses with the D-1 message — names the source path, the kind, and `1.10` — and is NOT the engine's not-found error. | `configured_source_select_refuses_with_connector_message` | **PROVEN** | Compile red first (below): `session.register_configured_sources` / `session.sql` plumbing absent on base (E0599). Green: `configured_source_select_refuses_with_connector_message ... ok` in `cargo test -p repark-core named_sources` — 7 passed, 0 failed. The measured Python-visible rendering is ``UnsupportedOperationException: This feature is not implemented: database source `write.database.postgres.company_db` (kind `postgres`) is declared but cannot be used yet — its connector arrives with roadmap 1.10 (Postgres, SQL Server, Trino)`` — asserted to contain `company_db`, `postgres`, `1.10` and to contain neither `not found` nor `does not exist`. |
| C-004 | Registration is lazy: a source naming an unroutable host builds and registers without error; no connection is attempted at build or registration (D-4). | `source_registration_opens_no_connection` | **PROVEN** | Compile red first (below): `register_configured_sources` / `sources` absent on base (E0599). Green in the same `named_sources` run — `host = "203.0.113.1"` (TEST-NET-3, unroutable) builds and registers with no error and the source lists; `register_configured_sources` only calls `context.register_catalog` on an in-process provider, so no socket is opened. |
| C-005 | `source(name).ping()` returns the D-1 refusal (source path, kind, `1.10`) until connectors land (D-6). | `source_ping_refuses_until_connector` | **PROVEN** | Compile red first (below): `session.source` absent on base (E0599). Green in the same `named_sources` run — `ping()` answers `Error::NotImplemented` carrying `company_db`, `postgres`, `1.10`. |
| C-006 | `sources()` returns one row per declared source — name, kind spelling, profile path, `auto_register`, properties redacted through `redact_value` (a `password` prop is masked). | `sources_listing_names_kind_profile_and_redacts_secrets` | **PROVEN** | Compile red first (below): `session.sources` absent on base (E0599). Green in the same `named_sources` run — the row asserts `name`/`kind`/`key_path`/`auto_register`, `host` verbatim, `password` masked `***` through `config_file::redact::redact_value`. |
| C-007 | `auto_register = false` lists the source but does not register its name: SQL under the name gets the engine's normal not-found error, not the connector refusal. | `auto_register_false_lists_but_does_not_register` | **PROVEN** | Compile red first (below). Green in the same `named_sources` run — `sources()` lists the row with `auto_register == false`, and `SELECT * FROM company_db.public.t` answers an error naming `company_db` that does NOT contain `1.10` (the engine's not-found, not the connector refusal). |
| C-008 | A catalog registered later under a source's name refuses as a duplicate, same as two catalogs (D-4). | `catalog_named_like_source_refuses_as_duplicate` | **PROVEN** | Compile red first (below). Green in the same `named_sources` run — `register_memory_catalog("company_db", …)` after source registration answers `catalog 'company_db' is already registered`, because `CatalogRegistry::is_registered` claims both families' names. F-3 audit — every `register_catalog` path checked: `register_iceberg_catalog_with_policy` (covers `register_memory_catalog`, `register_iceberg_catalog`, `register_catalog_spec` Memory/Glue/S3Tables, `late_catalogs`) double-checks `is_registered` → refuses; `register_configured_sources` checks `is_registered` + `context().catalog` → refuses; `refresh_catalog_provider` → `reregister_catalog_provider` → `rebuild_catalog_provider` is gated by `catalog_handle` (iceberg `entries` only — a source name errors as unknown catalog before `register_catalog`); the OOB path (`session.rs`) consults `catalogs.get` (entries only) → source name skips; `CatalogKind::Postgres` returns `NotImplemented` and never registers (repark-python `read_postgres` refuses via `deferred_reader_error`; nothing inserts into `postgres_catalog_names`); repark-python has zero `register_catalog` call sites; `deregister_catalog` is never called and DataFusion has no DROP CATALOG plan. One path DID silently replace: SQL `CREATE CATALOG`/`CREATE DATABASE` reaches `SessionContext::create_catalog` → `register_catalog` unconditionally — now refused by `refuse_source_ddl`'s `CreateCatalog` arm through the registry's `database_source` lookup (C-011). |
| C-009 | `source(name)` on an undeclared name refuses naming the declared sources (D-6). | `unknown_source_handle_refuses_naming_declared_sources` | **PROVEN** | Compile red first (below). Green in the same `named_sources` run — `source("nope")` answers `unknown database source 'nope' — declared sources: company_db`, naming both the asked name and the declared list. |
| C-010 | Python: a rendered `repark.toml` carrying a database source builds a session (the `match="CFG-2"` pin flips to loads); the guide's constraint 1 states the new truth. | `test_rendered_database_source_loads` | **PROVEN** | Red first (below): the flipped pin failed on the unimplemented wheel at `fold_config_file_into_builder` with the CFG-2 refusal. Green: `make develop` rebuilt the wheel and `.venv/bin/python -m pytest python/repark/tests/test_config_mirror.py -q` reports `21 passed` — the rendered `[default.database.postgres.company_db]` file builds a session end to end through `register_configured_sources` in `finish_session`. `docs/guide/repark-toml.md` constraint 1 now states the lazy-register/refuse-on-use truth with the measured `write` profile output. |
| C-011 | Write shapes under a source name refuse with the D-1 connector message, not a generic "not supported"/"doesn't exist": `CREATE TABLE company_db.public.t` and `DROP TABLE company_db.public.t` both carry the source path and `1.10` (audit F-2). | `configured_source_create_table_refuses_with_connector_message` | **PROVEN** | Red first (below): `DROP TABLE` answered ``Execution error: Table 'company_db.public.t' doesn't exist.`` — DataFusion's `drop_table` swallows the provider error from `find_and_deregister` and substitutes "doesn't exist" (context.rs:1052-1064), so the `deregister_table` override alone can never surface D-1 for DROP. Green: `configured_source_create_table_refuses_with_connector_message ... ok` in `cargo test -p repark-core named_sources` — 8 passed, 0 failed. `RefusingSourceSchemaProvider` now overrides `register_table`/`deregister_table` with the same `NotImplemented` refusal (covers `ctx.register_table("company_db.x.y")` direct calls), and `PreExecute::guard` runs `refuse_source_ddl` — shared by both doors via `PreExecute` — which refuses `CreateExternalTable`/`CreateMemoryTable`/`CreateView`/`CreateIndex`/`DropTable`/`DropView`/`CreateCatalog`/`DropCatalogSchema` plans naming a registered source before execution, closing the swallow. |
| C-012 | The per-query `catalogs_snapshot()` clone stays "keys + Arcs": `database_sources` values are `Arc<SourceSpec>`, not deep-copied `SourceSpec`s (S2-21 P2-1). | Existing pins + gates (no behaviour change) | **PROVEN** | The reviewer measured the step-1 shape at ~296 ns per `sql_with` for one source and ~1 ms at 1024 — `CatalogRegistry` is `#[derive(Clone)]` and cloned on every SQL call. Now `database_sources: HashMap<String, Arc<SourceSpec>>`, `insert_database_source(Arc<SourceSpec>)`, `database_source -> Option<&Arc<SourceSpec>>`, and the session carries `source_specs: Arc<Vec<Arc<SourceSpec>>>` so `register_configured_sources` hands the registry an `Arc::clone` (atomic bump, no deep copy). Green: `cargo test -p repark-core config_file` 64, `named_sources` 8, `session` 121+2, `make verify` exit 0 — every existing pin unchanged and passing. Also folded in (S2-21 out-of-scope note): `register_memory_catalog`'s first duplicate check now goes through `is_registered`, so a memory catalog over a source name refuses before building instead of building then failing. |

## Red first

All reds were captured on the base tree before implementation began (pins written first,
run, confirmed failing — no pin was edited to fit the implementation afterwards).

```text
$ cargo test -p repark-core config_file

test config_file::tests::wiring::database_source_block_loads ... FAILED
test config_file::tests::auto_register_non_boolean_refuses_naming_key_path ... FAILED

failures:
    database_source_block_loads — Err("database sources
        (default.database.postgres.company_db) are parsed but named-source registration
        arrives with CFG-2 — drop the `[<profile>.database]` tables until that card lands")
    auto_register_non_boolean_refuses_naming_key_path — `auto_register = "yes"` loaded
        silently (parsed as an ordinary string prop; no boolean check existed)
```

The `named_sources` pins are a compile red on the base tree — the session exposes none of
the three names they call, so the test target does not build:

```text
error[E0599]: no method named `register_configured_sources` found for struct `ReparkSession`
error[E0599]: no method named `sources` found for struct `ReparkSession`
error[E0599]: no method named `source` found for struct `ReparkSession`
   (E0599 ×9 across the seven pins)
```

The Python pin's red ran against the base-behavior wheel (`maturin develop` installed
`repark-1.4.0` before the Rust change):

```text
$ .venv/bin/python -m pytest python/repark/tests/test_config_mirror.py -q

test_rendered_database_source_loads — IllegalArgumentException: repark config error:
    database sources (default.database.postgres.company_db) are parsed but named-source
    registration arrives with CFG-2 — drop the `[<profile>.database]` tables until that
    card lands
    (raised through ReparkSession.builder.configFile(...).getOrCreate() →
     fold_config_file_into_builder → config_file_pairs)
```

The audit follow-up pin's red ran on the step-1 tree (provider overriding only `table()`):

```text
$ cargo test -p repark-core configured_source_create_table

test named_sources::tests::configured_source_create_table_refuses_with_connector_message ... FAILED

failures:
---- configured_source_create_table_refuses_with_connector_message stdout ----
DROP TABLE company_db.public.t: datafusion engine error: Execution error:
    Table 'company_db.public.t' doesn't exist.
    (CREATE TABLE already carried D-1 — its existence probe reaches table();
     DROP's find_and_deregister error is swallowed upstream into "doesn't exist")
```

## Gates

| Command | Result |
|---|---|
| `cargo test -p repark-core config_file` | exit 0 — `test result: ok. 64 passed; 0 failed; 0 ignored; 0 measured; 269 filtered out` (re-run after the audit fixes) |
| `cargo test -p repark-core named_sources` | exit 0 — `test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 325 filtered out` (re-run after the audit fixes) |
| `cargo test -p repark-core session` | exit 0 — `test result: ok. 121 passed` + `2 passed` across the matched targets (re-run after the S2-21 fixes) |
| `make verify` | exit 0 — fmt, clippy (all-targets + panic-ban + repark-python), crate-dag, lib-rs, rust-file-size (477 clean), lib-py, conventions, docstring, manifest, ledgers, docs-links, ruff, taplo, typos, `cargo test --locked --workspace` all green |
| `make develop` | exit 0 — `Built wheel for abi3 Python ≥ 3.12`; `Installed repark-1.4.0` |
| `.venv/bin/python -m pytest python/repark/tests/test_config_mirror.py -q` | exit 0 — `21 passed` |

## Pins

| Pin | Clause | Home |
|---|---|---|
| `database_source_block_loads` | C-001 | `crates/repark-core/src/config_file/tests/wiring.rs` |
| `auto_register_non_boolean_refuses_naming_key_path` | C-002 | `crates/repark-core/src/config_file/tests/mod.rs` |
| `configured_source_select_refuses_with_connector_message` | C-003 | `crates/repark-core/src/named_sources/tests.rs` |
| `source_registration_opens_no_connection` | C-004 | `crates/repark-core/src/named_sources/tests.rs` |
| `source_ping_refuses_until_connector` | C-005 | `crates/repark-core/src/named_sources/tests.rs` |
| `sources_listing_names_kind_profile_and_redacts_secrets` | C-006 | `crates/repark-core/src/named_sources/tests.rs` |
| `auto_register_false_lists_but_does_not_register` | C-007 | `crates/repark-core/src/named_sources/tests.rs` |
| `catalog_named_like_source_refuses_as_duplicate` | C-008 | `crates/repark-core/src/named_sources/tests.rs` |
| `unknown_source_handle_refuses_naming_declared_sources` | C-009 | `crates/repark-core/src/named_sources/tests.rs` |
| `test_rendered_database_source_loads` | C-010 | `python/repark/tests/test_config_mirror.py` |
| `configured_source_create_table_refuses_with_connector_message` | C-011 | `crates/repark-core/src/named_sources/tests.rs` |

## Decisions

- `RefusingSourceCatalogProvider` answers `Some(schema)` for every `schema(name)` lookup and
  its single `RefusingSourceSchemaProvider` answers `Err(DataFusionError::NotImplemented)`
  for every `table(name)` — so `company_db.public.t` and `company_db.anything.t` alike
  reach the D-1 refusal instead of a schema-not-found. `schema_names` / `table_names`
  answer empty vectors and `table_exist` answers false: listing a registered source's
  contents stays honest-empty (the connector owns real listings), while reads refuse loud.
- The refusal is `Error::NotImplemented` — the error class Python maps to
  `UnsupportedOperationException` — matching the card's "its connector arrives with
  roadmap 1.10" wording. `source(name)`'s unknown-name refusal is `Error::DataFusion`,
  the catalog-family class, since it names an engine-side registration question.
- `register_configured_sources` holds the registry write lock while checking
  `is_registered` and `context.catalog(name)` — same linearization the catalog path uses —
  so a concurrent catalog registration under a source name loses deterministically.
- `session.rs` ends net −8 at 978 lines (`use`, builder field, `prepare_build_state`
  carry, session field, build literal): `build()` crossed the 100-line
  `clippy::too_many_lines` threshold, so the `TempViewHome` capture moved into
  `temp_view.rs::build_temp_view_home` — next to the struct it constructs — rather than
  an `#[allow]`.
- `crates/repark-python/src/session.rs` is at an exact 1128-line file-size baseline, so the
  one added line was made line-neutral by joining two `use` items inside the same file's
  test block (imports grouped, no behaviour change).
- Audit F-1: the three `///` `# Errors` docs on the public `Result` fns
  (`register_configured_sources`, `source`, `ping`) were removed — `///` is a comment under
  the owner ruling — and pedantic clippy is satisfied the repository's way:
  `#[allow(clippy::missing_errors_doc)]` on each item (the convention used in 32 places,
  e.g. `repark-distributed/src/cluster.rs`). `ping` also carries
  `#[allow(clippy::unnecessary_wraps)]` because its `Result` signature is the card's stable
  door while it can only fail.
- Audit F-2: provider overrides alone cannot refuse a `DROP TABLE` with the D-1 message —
  `SessionContext::drop_table` routes through `find_and_deregister`, whose `schema.table()`
  refusal propagates into a `match` arm that discards any `Err` and substitutes
  `Table '…' doesn't exist` (datafusion `context.rs` `(_, _) =>` arm; `(_, true)` even
  succeeds silently under `IF EXISTS`). So `RefusingSourceSchemaProvider` overrides
  `register_table`/`deregister_table` with the same `NotImplemented` refusal (direct
  `ctx.register_table`/`deregister_table` calls and every exec path that does reach them),
  and `PreExecute::guard` additionally runs `named_sources::refuse_source_ddl`, which
  refuses any `LogicalPlan::Ddl` naming a registered source — covering the swallowed
  `DropTable`/`DropView`/`DropCatalogSchema` shapes, the `register_table`-reaching creates
  (`CreateMemoryTable`/`CreateExternalTable`/`CreateView`/`CreateIndex`), and `CreateCatalog`,
  which would otherwise silently replace the source's provider (F-3's one live hole). The
  guard sits in `PreExecute` so both SQL doors (`sql()` and the Spark facade, which calls
  `PreExecute::guard` from `spark_ast.rs`) refuse identically, and the refusal is
  `DataFusionError::NotImplemented` — the same class the provider itself returns.
- S2-21 P2-1: the session stores `source_specs: Arc<Vec<Arc<SourceSpec>>>` — one `Arc`
  per spec built once at `prepare_build_state` — so `register_configured_sources` hands
  `CatalogRegistry::insert_database_source` an `Arc::clone` and the per-`sql_with`
  `catalogs_snapshot()` clone stays keys-plus-Arcs (the registry's documented contract).
  `database_source(name)` returns `Option<&Arc<SourceSpec>>` for `refuse_source_ddl`.
- S2-21 out-of-scope row folded in: `register_memory_catalog`'s first duplicate check
  switched from `catalog_handle(name).is_ok()` (iceberg entries only — a source name fell
  through and built a memory catalog before the later refusal) to `is_registered(name)`
  under the registry read lock, same `already registered` message, refusing before the
  build. `is_registered` subsumes `catalog_handle` (entries ∪ sources), so no second
  check is needed.

## Attestation (actor, steps 1–2)

```yaml
COVERAGE_ATTESTATION:
  pr_unit: cfg-2
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every ruled behaviour is pinned in both directions — a database source LOADS (not merely stops refusing), `auto_register = false` is asserted to list AND to answer the engine not-found (not the connector refusal), the SQL refusal asserts the presence of source/kind/1.10 AND the absence of the engine's not-found wording on both read (SELECT) and write (CREATE TABLE, DROP TABLE) shapes, the listing asserts a secret prop renders `***` while a non-secret stays verbatim, and the unknown-handle refusal asserts the declared list appears. Step 2 pins the same axes through the Python surface: `sources()` rows assert redaction and `auto_register` both ways, `ping()` and `repark.sql` assert the connector wording present and the not-found wording absent, and the config mirror round-trips a rendered file through the Rust loader.
      artifacts: [crates/repark-core/src/named_sources/tests.rs, crates/repark-core/src/config_file/tests/wiring.rs, crates/repark-core/src/config_file/tests/mod.rs, python/repark/tests/test_config_mirror.py, python/repark/tests/test_session_sources.py]
    - id: AT-2
      status: N/A
      justification: No numeric or performance claim; the unit is registration, refusal and listing plumbing.
    - id: AT-3
      status: ATTACKED
      evidence: The failure paths ARE the subject — non-boolean `auto_register` (key path named; the mirror refuses it as `ValidationError`), catalog-over-source-name duplicate (same message as two catalogs), undeclared handle (declared list named, on both the Rust and the facade `source()`), SQL use of an unimplemented source (NotImplemented, not not-found, on `sql()`, the facade `spark.sql` and the `repark.sql` door) — each pinned on message content. Secret leakage is pinned at both layers: `password` renders `***` in the Rust listing and in the Python `SourceMetadata.properties`, with the secret string asserted absent from the repr.
      artifacts: [crates/repark-core/src/named_sources/tests.rs, crates/repark-core/src/named_sources.rs, python/repark/tests/test_session_sources.py]
    - id: AT-4
      status: ATTACKED
      evidence: Registration order is pinned where load-bearing: `register_configured_sources` holds the registry write lock while checking `is_registered` AND the context's live catalog set before installing, so a source name and a catalog name linearize to one winner — the same discipline `register_memory_catalog` keeps on the other side (the duplicate pin registers the source first, then loses the catalog). `source_specs` is an immutable `Arc<Vec>` snapshot from build; `sources()`/`source()` read it without locking. Step 2 adds no shared state: the facade `NamedSource` handle stores name/kind/key_path and re-resolves through `ReparkSession::source` on each `ping()`.
      artifacts: [crates/repark-core/src/named_sources.rs, crates/repark-core/src/catalog_state.rs, python/repark/src/repark/spark/session/session_sources.py]
    - id: AT-5
      status: ATTACKED
      evidence: Two security properties are pinned, not assumed — the listing and `SourceSpec` `Debug` route every value through `redact_value` (`password` renders `***`, a non-secret `host` stays verbatim), and registration opens no connection: the provider is an in-process `CatalogProvider` whose only outward act is `register_catalog` on the session context (the unroutable `203.0.113.1` host proves build + registration succeed with no socket). `auto_register` is kept OUT of `props`, so a credential map never carries a control flag; the Python `SourceMetadata` row carries only the already-redacted properties the Rust door hands it.
      artifacts: [crates/repark-core/src/named_sources.rs, crates/repark-core/src/config_file/sources.rs, crates/repark-core/src/config_file/redact.rs, crates/repark-python/src/session_sources.rs]
    - id: AT-6
      status: ATTACKED
      evidence: The silent-acceptance holes this unit could open are closed at each seam — a non-boolean `auto_register` refuses naming the key path in the TOML loader AND as `ValidationError` in the Python mirror (it would otherwise have parsed as a string prop / string extra), a catalog registered over a source name refuses as a duplicate rather than shadowing the source, `source()` on an undeclared name refuses naming the declared set rather than answering an empty handle, every CFG-1 validation (unknown kind, non-string prop, cross-family collision) still refuses in the same `config_file` run, and `SparkSession.source`/`SparkSession.sources` entered the example inventory so the coverage gate watches them.
      artifacts: [crates/repark-core/src/config_file/sources.rs, crates/repark-core/src/named_sources.rs, crates/repark-core/src/catalog_state.rs, python/repark/src/repark/config.py, docs/examples/inventory.txt]
    - id: AT-7
      status: N/A
      justification: No hot path — registration runs once per session construction over a handful of specs; `sources()` materializes a `SourceRow` per declared source on demand, and the facade wraps the same rows without copying beyond the namedtuple.
    - id: AT-8
      status: ATTACKED
      evidence: No dependency file, manifest, workflow or STATUS.md was touched in either step; the new public surface is exactly what the card names — Rust's three session methods plus `NamedSource`/`SourceRow`, then step 2's `ReparkSession.sources()`/`.source()`, the `NamedSource` handle, `SourceMetadata`, and `DatabaseSource.auto_register` (no `.config()` key family per D-3). `session.rs` shrank net eight lines; `session_core.py` held its exact 2304 baseline by moving `_promote_active`/`_builder_config_get_master` into their owning modules rather than raising the ceiling.
      artifacts: [crates/repark-core/src/lib.rs, crates/repark-core/src/session.rs, crates/repark-core/src/temp_view.rs, python/repark/src/repark/spark/session/session_core.py, python/repark/src/repark/spark/session/session_state.py, python/repark/src/repark/spark/session/session_configuration.py]
    - id: AT-9
      status: ATTACKED
      evidence: Diagnosability is the refusal's content: the D-1 message names the full `<profile>.database.<kind>.<name>` path, the kind and the roadmap version; the unknown-handle refusal names both the asked name and every declared source; the duplicate refusal reuses the catalog family's `already registered` wording. The Python pins assert those strings through `UnsupportedOperationException`/`PySparkException` — the existing mapping, no new exception class — and the guide shows the measured messages verbatim.
      artifacts: [crates/repark-core/src/named_sources.rs, crates/repark-core/src/named_sources/tests.rs, python/repark/tests/test_session_sources.py, docs/guide/repark-toml.md]
    - id: AT-10
      status: ATTACKED
      evidence: Reds were captured before each implementation — step 1: two runtime failures on untouched wiring/parser code (the CFG-2 load refusal verbatim; `auto_register = "yes"` silently accepted), the E0599 ×9 compile red for the three missing session methods, the Python pin failing through `config_file_pairs` on the base-behavior wheel, and the audit follow-up pin failing on the step-1 tree with `DROP TABLE` answering the generic ``Table 'company_db.public.t' doesn't exist.`` Step 2: six pins red on the base tree (four `AttributeError` on the missing facade surface, `auto_register` rejected as a non-string extra, non-bool `auto_register` silently accepted); the two pins that passed on base measure step-1 behaviour through the shared SQL path and stand as regression guards. No pin was edited to fit the implementation.
      artifacts: [crates/repark-core/src/named_sources/tests.rs, crates/repark-core/src/config_file/tests/wiring.rs, python/repark/tests/test_config_mirror.py, python/repark/tests/test_session_sources.py]
  complete: true
```

## Performance review (S2-21)

**Reviewer:** Grok 4.6, read-only, critic-quality (S2-21 Rust performance reviewer) — report at
`/tmp/oc-worker/b-rev-cfg2/report.md` (reviewed `d0a27dea`, 24-file diff).

**Verdict:** no P1. Session build with zero sources does not measurably regress; no registry
write lock is held across `.await`; a refusing catalog cannot turn `SHOW TABLES` /
`information_schema` into an error (empty `schema_names`/`table_names` gate listing) and adds
no per-query planner work on the default path.

**Findings:**

| Id | Severity | Finding | Disposition |
|---|---|---|---|
| P2-1 | P2 | `CatalogRegistry` snapshot cloned full `SourceSpec` values (incl. secret props) on every `sql_with` — ~296 ns @ 1 source, ~1 ms @ 1024 (isolated `-O` clone bench). | **Fixed** — registry holds `Arc<SourceSpec>`; session specs are `Arc<Vec<Arc<SourceSpec>>>`; snapshot clone is keys + atomic bumps again (C-012). |
| P3-1 | P3 | `table()` clones the refusal `String` per lookup — refusal path only, not planning. | Noted, no change: the refusal is the intended cold path; `Arc<str>` is available if the message ever warms up. |
| P3-2 | P3 | `sources()` clones and re-redacts every property per call — listing API only, not `sql()`. | Noted, no change: listing is on-demand; a cached redacted row is only worth it if a caller loops. |
| P3-3 | P3 | CFG-1's parse-time duplicate-name check is O(n²) in named entries per profile — unchanged this unit. | Noted, no change: n is TOML entries; a `HashSet` is available if generated configs grow. |
| — | P3-adjacent | `register_memory_catalog`'s first duplicate check used `catalog_handle` (iceberg entries only) — a source name fell through, built the catalog, then failed the later `is_registered`. | **Fixed** — first check now consults `is_registered`, same message, refuses before building (C-012). |

## Notes for the orchestrator

- `RefusingSourceCatalogProvider::schema(name)` answers `Some` for ANY name (not only
  `public`) so the connector message is the failure at every schema spelling — the ruled
  pin only exercises `public`. If step 2 wants schema-listing honesty differently (empty
  `schema()` so `company_db.nope.t` is schema-not-found), that is a one-line provider
  change plus a pin; the D-1 message still answers `company_db.public.t`.
- Step 2 seam: `ReparkSession::sources()` / `::source(name)` are already `pub` and
  re-exported (`NamedSource`, `SourceRow`), so the Python door is a thin PyO3 wrap in
  `crates/repark-python` plus the facade methods — no core work needed. `SourceRow` uses
  plain fields (not getters) matching `CatalogSpec`'s shape; if step 2 prefers a
  dict-shaped row it maps directly.
- F-2 surfaced a DataFusion behaviour worth keeping: `SessionContext::drop_table` /
  `drop_view` discard any provider error from `find_and_deregister` and report
  ``Table '…' doesn't exist`` (or succeed silently under `IF EXISTS`) — no provider
  override can carry a message through those plans. `refuse_source_ddl` in
  `PreExecute::guard` is therefore the only seam that makes DROP refuse loud; it also
  covers `CREATE CATALOG`, the one remaining `register_catalog` path that silently
  replaced a source provider (F-3). If a future card wants only the two pin shapes
  covered, narrowing the match is a one-line change, but the broader coverage is the
  honest reading of "cannot be used yet".
- `ReparkSession::register_configured_sources` is called today from `finish_session`
  (repark-python) and from the named_sources pins; `repark-spark/tests/ddl_sessions.rs`
  builds its own session and does NOT register sources — sources only exist from a config
  file, which that harness does not load, so no call was added there.

## PROPOSITION LEDGER — CFG-2 step 2 — 2026-09-13

**Step 2 actor model:** swe-2-high · **Branch:** `feat/cfg-2-step2` · **Base:** `origin/main`
at `25707c46` (step 1 merged). Step 2 appends to this ledger per the card's departure note;
the file moves to `completed/` only with step 2's last commit. Step 2 lands the Python
surface: `ReparkSession.sources()` / `.source(name).ping()` on the facade, the
`SourceMetadata` row type, and the config mirror's typed `auto_register` — over the step-1
Rust door, unchanged.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-013 | `ReparkSession.sources()` returns `list[SourceMetadata]` — `SourceMetadata` is the namedtuple (`name`, `kind`, `key_path`, `auto_register`, `properties`) beside `CatalogMetadata` in `catalog.py` (D-7); one row per declared source in the order Rust returns; `properties` is `dict[str, str]` with `password` masked `***` and no secret value in its repr. | `test_sources_lists_declared_source_with_redacted_properties` | **PROVEN** | Red first (below): `AttributeError: 'ReparkSession' object has no attribute 'sources'` on the base tree. Green: `.venv/bin/python -m pytest python/repark/tests/test_session_sources.py python/repark/tests/test_config_mirror.py -q` — 29 passed. The pyo3 door `session_sources` maps `ReparkSession::sources()` rows verbatim, so redaction is the Rust `redact_value` path; measured row: `SourceMetadata(name='company_db', kind='postgres', key_path='default.database.postgres.company_db', auto_register=True, properties={'dbname': 'analytics', 'host': 'db.example.com', 'password': '***'})`. |
| C-014 | `ReparkSession.source("company_db")` returns a `NamedSource` handle with read-only `name`/`kind`/`key_path` and a `ping()` that raises the D-1 refusal as `UnsupportedOperationException` naming the key path, the kind and `1.10` — the existing error mapping, no new exception class (D-8). | `test_source_ping_raises_connector_refusal` | **PROVEN** | Red first (below): `AttributeError: 'ReparkSession' object has no attribute 'source'` on the base tree. Green in the same 29-passed run — measured message: ``database source `default.database.postgres.company_db` (kind `postgres`) is declared but cannot be used yet — its connector arrives with roadmap 1.10 (Postgres, SQL Server, Trino)``. `session_source_ping` re-resolves the name then calls `NamedSource::ping`, so the refusal keeps its `Error::NotImplemented` → `UnsupportedOperationException` mapping. |
| C-015 | `source(name)` on an undeclared name raises through the existing error mapping naming the declared sources (D-8). | `test_unknown_source_raises_naming_declared_sources` | **PROVEN** | Red first (below): `AttributeError: 'ReparkSession' object has no attribute 'source'` on the base tree. Green in the same run — measured `PySparkException: datafusion engine error: unknown database source 'nope' — declared sources: company_db`, asserted to contain both `nope` and `company_db`. |
| C-016 | `repark.sql("SELECT * FROM company_db.public.t")` against a `REPARK_CONFIG`-discovered file raises the connector refusal — `UnsupportedOperationException` naming the source path, the kind and `1.10`, not the engine's not-found wording (through `repark.sql`, the native door). | `test_select_under_source_name_raises_connector_refusal` | **PROVEN** | Written and run first; it PASSED on the base tree — step 1's `PreExecute::guard`/`refuse_source_ddl` already covers the facade SQL path and `REPARK_CONFIG` discovery already reaches `finish_session`, so the pin is a regression guard over landed behaviour, not new wiring. Green in the same 29-passed run: asserts `company_db`, `postgres`, `1.10` present and `not found`/`does not exist` absent. |
| C-017 | `auto_register = false` lists the source via `sources()` (`auto_register=False`) but does not register it: `spark.sql` under the name answers the engine's not-found naming `company_db`, with no `1.10` (D-5). | `test_auto_register_false_listed_not_registered` | **PROVEN** | Red first (below): `AttributeError: 'ReparkSession' object has no attribute 'sources'` on the base tree. Green in the same run — `sources()` reports `auto_register is False` while `SELECT * FROM company_db.public.t` raises an error containing `company_db` and not `1.10`. |
| C-018 | `DatabaseSource.auto_register` is a typed `StrictBool \| None` field (D-10): a non-bool refuses as `ValidationError`, `auto_register = true\|false` renders only when set, every other property stays a string extra, and a rendered `ReparkConfig` carrying `auto_register=False` loads through the Rust loader and lists `auto_register=False`. | `test_config_mirror_auto_register_round_trips` + `test_config_mirror_auto_register_non_bool_refuses` | **PROVEN** | Red first (below): the round-trip failed with `Value error, connection property 'auto_register' must be a string` (it parsed as an ordinary extra) and the non-bool pin `DID NOT RAISE ValidationError`. Green in the same run — `to_toml()` emits `auto_register = false`, `tomllib.loads` reads it back as `False`, `ReparkSession.builder.configFile(...).getOrCreate().sources()` lists `auto_register is False`, and an unset field renders no key. |
| C-019 | D-11 measured, not changed: `spark.catalog.listCatalogs()` on a session with one auto-registered source returns `[CatalogMetadata(name='spark_catalog', description=None)]` — the source name does NOT appear; `currentCatalog` behaviour untouched. | `test_list_catalogs_with_auto_registered_source` | **PROVEN** | Written and run first; PASSED on the base tree — step 1 registers sources into `CatalogRegistry.database_sources`, a separate family from the iceberg entries `listCatalogs` walks, so the observation holds by construction and needed no code. Pinned so a future regression that lists source names trips loud. |

## Red first (step 2)

The pins were written and run on the base tree (Python facade changes stashed; the wheel
then installed carried no facade surface) before the implementation landed. Six failed,
two passed — the two passes (`test_select_under_source_name_raises_connector_refusal`,
`test_list_catalogs_with_auto_registered_source`) measure behaviour step 1 already owns
through the facade's shared SQL path, so they stand as regression guards.

```text
$ .venv/bin/python -m pytest python/repark/tests/test_session_sources.py \
      python/repark/tests/test_config_mirror.py -q

E   AttributeError: 'ReparkSession' object has no attribute 'sources'
    (test_sources_lists_declared_source_with_redacted_properties, session_sources.py:34)
E   AttributeError: 'ReparkSession' object has no attribute 'source'
    (test_source_ping_raises_connector_refusal, session_sources.py:51)
E   AttributeError: 'ReparkSession' object has no attribute 'source'
    (test_unknown_source_raises_naming_declared_sources, session_sources.py:69)
E   AttributeError: 'ReparkSession' object has no attribute 'sources'
    (test_auto_register_false_listed_not_registered)
E   pydantic_core._pydantic_core.ValidationError: 1 validation error for DatabaseSource
      Value error, connection property 'auto_register' must be a string
    (test_config_mirror_auto_register_round_trips — the key fell through to the
     string-extras validator)
E   Failed: DID NOT RAISE ValidationError
    (test_config_mirror_auto_register_non_bool_refuses)

6 failed, 23 passed in 0.25s
```

## Gates (step 2)

| Command | Result |
|---|---|
| `make develop` | exit 0 — `Built wheel for abi3 Python ≥ 3.12`; `Installed repark-1.4.0` |
| `.venv/bin/python -m pytest python/repark/tests/test_session_sources.py python/repark/tests/test_config_mirror.py -q` | exit 0 — `29 passed in 0.19s` |
| `make check-example-coverage` | exit 0 — `923 public names (catalog=28, column=34, dataframe=153, functions=452, io=42, ml=28, session=46, ta=86, types=32, window=22); 809 covered; 112 backlog; 2 exceptions; 216 examples` |
| `cargo test -p repark-core named_sources` | exit 0 — `test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 359 filtered out` |
| `make py-test-facade` | exit 0 — `5969 passed, 369 skipped, 48 warnings in 1086.97s` |
| `make verify` | exit 0 — fmt, clippy (all-targets + panic-ban + repark-python), crate-dag, lib-rs, rust-file-size (490 clean), lib-py (668 clean), conventions, docstring, manifest, ledgers, docs-links, ruff, taplo, typos, `cargo test --locked --workspace` all green |

## Pins (step 2)

| Pin | Clause | Home |
|---|---|---|
| `test_sources_lists_declared_source_with_redacted_properties` | C-013 | `python/repark/tests/test_session_sources.py` |
| `test_source_ping_raises_connector_refusal` | C-014 | `python/repark/tests/test_session_sources.py` |
| `test_unknown_source_raises_naming_declared_sources` | C-015 | `python/repark/tests/test_session_sources.py` |
| `test_select_under_source_name_raises_connector_refusal` | C-016 | `python/repark/tests/test_session_sources.py` |
| `test_auto_register_false_listed_not_registered` | C-017 | `python/repark/tests/test_session_sources.py` |
| `test_config_mirror_auto_register_round_trips` | C-018 | `python/repark/tests/test_config_mirror.py` |
| `test_config_mirror_auto_register_non_bool_refuses` | C-018 | `python/repark/tests/test_config_mirror.py` |
| `test_list_catalogs_with_auto_registered_source` | C-019 | `python/repark/tests/test_session_sources.py` |

## Decisions (step 2)

- The pyo3 door is three free `#[pyfunction]`s taking `PyRef<'_, PyReparkSession>` in the
  new `session_sources.rs` — the `catalog_census.rs` shape — because pyo3 allows one
  `#[pymethods]` block per type and `session.rs` sits on its exact 1128-line baseline.
  `lib.rs` registers the three functions on the native module.
- `session_core.py` sits on its exact 2304-line baseline, so the two thin delegators were
  paid for line-neutrally: `_promote_active` moved byte-identical to `session_state.py`
  (the `_active_session` owner) and `_builder_config_get_master` to
  `session_configuration.py` (as a config-dict function, the CFG-1-step-3 shape). Both
  ride `_funcs.py`'s compatibility router back into `session_core` globals, so moved-symbol
  hash and ownership pins hold.
- `session_source_ping` re-resolves the name through `ReparkSession::source` on each call
  rather than caching the handle, so `ping()` keeps the engine's `NotImplemented` class —
  the same class an unknown name would raise, resolved once per call at handle material.
- `DatabaseSource.auto_register` is `StrictBool | None = None`, not `bool = True`: unset
  must stay distinguishable from an explicit `true` so `to_toml()` only emits the key the
  author wrote — the Rust loader already defaults `auto_register` to `true`.
- D-11 was measured, not changed: `listCatalogs()` walks the catalog-entries family;
  step-1 sources live in `database_sources`, so an auto-registered source name does not
  appear. `test_list_catalogs_with_auto_registered_source` pins the observation.
- The guide subsection shows measured `REPARK_ENV=write` output with
  `repark = ReparkSession.builder.configFile("repark.toml").getOrCreate()` (D-12), and the
  example `docs/examples/session/named_sources.py` covers `SparkSession.source` /
  `SparkSession.sources` — the two names the coverage enumerator added to the inventory.

## Performance review (S2-21), step 2

Reviewer: Grok 4.6, read-only, critic-quality, on `c03eeb8b` (report `/tmp/oc-worker/b-rev-cfg2s2/report.md`, 21 turns). Verdict: **no P1, no P2.** A session build with no declared source makes zero native source calls and never imports `session_sources.py`. `sources()` is one native crossing for all rows (debug native: 48 µs empty, 128 µs at one source, 894 µs at 64), and Python does not re-redact.

| Id | Severity | Site | Note |
|---|---|---|---|
| P3-1 | P3 | `python/repark/src/repark/spark/session/session_sources.py` `sources()` | `dict(properties)` copies the dict pyo3 already built; ~190 ns per row, ~1.3 % of `sources()` at 64 rows. |
| P3-2 | P3 | `crates/repark-core/src/named_sources.rs` `SourceRow::from_spec` | The step-1 P3-2 listing cost, wrapped unchanged: every call clones and redacts each property. |
| P3-3 | P3 | `named_sources.rs` `source()`, `crates/repark-python/src/session_sources.rs` `session_source_ping` | `source(name)` is a linear scan; `ping()` re-resolves by name; +4.5 µs first-vs-last of 64 names. |

Orchestrator audit (run 9b): the `_promote_active` / `_builder_config_get_master` moves were measured behaviour-neutral. After `sql()` and `createDataFrame()`, the active session switches exactly as on the step-1 tree. The `NamedSource` class docstring was trimmed to one line.

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

## Attestation (actor, step 1)

```yaml
COVERAGE_ATTESTATION:
  pr_unit: cfg-2
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every ruled behaviour is pinned in both directions — a database source LOADS (not merely stops refusing), `auto_register = false` is asserted to list AND to answer the engine not-found (not the connector refusal), the SQL refusal asserts the presence of source/kind/1.10 AND the absence of the engine's not-found wording on both read (SELECT) and write (CREATE TABLE, DROP TABLE) shapes, the listing asserts a secret prop renders `***` while a non-secret stays verbatim, and the unknown-handle refusal asserts the declared list appears.
      artifacts: [crates/repark-core/src/named_sources/tests.rs, crates/repark-core/src/config_file/tests/wiring.rs, crates/repark-core/src/config_file/tests/mod.rs, python/repark/tests/test_config_mirror.py]
    - id: AT-2
      status: N/A
      justification: No numeric or performance claim; step 1 is registration and refusal plumbing.
    - id: AT-3
      status: ATTACKED
      evidence: The failure paths ARE the subject — non-boolean `auto_register` (key path named), catalog-over-source-name duplicate (same message as two catalogs), undeclared handle (declared list named), SQL use of an unimplemented source (NotImplemented, not not-found) — each pinned on message content. Secret leakage is pinned: `password` renders `***` in the listing and `SourceSpec`'s `Debug` masks through `redact_value`.
      artifacts: [crates/repark-core/src/named_sources/tests.rs, crates/repark-core/src/named_sources.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Registration order is pinned where load-bearing: `register_configured_sources` holds the registry write lock while checking `is_registered` AND the context's live catalog set before installing, so a source name and a catalog name linearize to one winner — the same discipline `register_memory_catalog` keeps on the other side (the duplicate pin registers the source first, then loses the catalog). `source_specs` is an immutable `Arc<Vec>` snapshot from build; `sources()`/`source()` read it without locking.
      artifacts: [crates/repark-core/src/named_sources.rs, crates/repark-core/src/catalog_state.rs]
    - id: AT-5
      status: ATTACKED
      evidence: Two security properties are pinned, not assumed — the listing and `SourceSpec` `Debug` route every value through `redact_value` (`password` renders `***`, a non-secret `host` stays verbatim), and registration opens no connection: the provider is an in-process `CatalogProvider` whose only outward act is `register_catalog` on the session context (the unroutable `203.0.113.1` host proves build + registration succeed with no socket). `auto_register` is kept OUT of `props`, so a credential map never carries a control flag.
      artifacts: [crates/repark-core/src/named_sources.rs, crates/repark-core/src/config_file/sources.rs, crates/repark-core/src/config_file/redact.rs]
    - id: AT-6
      status: ATTACKED
      evidence: The silent-acceptance holes this step could open are closed at each seam — a non-boolean `auto_register` refuses naming the key path (it would otherwise have parsed as a string prop), a catalog registered over a source name refuses as a duplicate rather than shadowing the source, `source()` on an undeclared name refuses naming the declared set rather than answering an empty handle, and every CFG-1 validation (unknown kind, non-string prop, cross-family collision) still refuses in the same `config_file` run.
      artifacts: [crates/repark-core/src/config_file/sources.rs, crates/repark-core/src/named_sources.rs, crates/repark-core/src/catalog_state.rs]
    - id: AT-7
      status: N/A
      justification: No hot path — registration runs once per session construction over a handful of specs; `sources()` materializes a `SourceRow` per declared source on demand.
    - id: AT-8
      status: ATTACKED
      evidence: No dependency file, manifest, workflow or STATUS.md was touched; the new public surface is exactly the three session methods plus `NamedSource`/`SourceRow` re-exports the card names (step 2's Python door consumes them; no `.config()` key family was added per D-3). `session.rs` shrank net eight lines (986 → 978: the five source-carry lines minus the thirteen-line `TempViewHome` extraction); the `build()`-length overflow was resolved by moving the capture next to its struct in `temp_view.rs`, not by an `#[allow]`.
      artifacts: [crates/repark-core/src/lib.rs, crates/repark-core/src/session.rs, crates/repark-core/src/temp_view.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Diagnosability is the refusal's content: the D-1 message names the full `<profile>.database.<kind>.<name>` path, the kind and the roadmap version; the unknown-handle refusal names both the asked name and every declared source; the duplicate refusal reuses the catalog family's `already registered` wording so the failure reads identically on either family. Each pin asserts those strings.
      artifacts: [crates/repark-core/src/named_sources.rs, crates/repark-core/src/named_sources/tests.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Reds were captured before each implementation — two runtime failures on untouched wiring/parser code (the CFG-2 load refusal verbatim; `auto_register = "yes"` silently accepted), the E0599 ×9 compile red for the three missing session methods, the Python pin failing through `config_file_pairs` on the base-behavior wheel, and the audit follow-up pin failing on the step-1 tree with `DROP TABLE` answering the generic ``Table 'company_db.public.t' doesn't exist.`` No pin was edited to fit the implementation.
      artifacts: [crates/repark-core/src/named_sources/tests.rs, crates/repark-core/src/config_file/tests/wiring.rs, python/repark/tests/test_config_mirror.py]
  complete: true
```

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

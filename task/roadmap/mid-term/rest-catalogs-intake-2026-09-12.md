# REST catalogs intake — Lakekeeper and Apache Polaris (2026-09-12)

**Date:** 2026-09-12 · **Author:** Claude Fable 5.1 (read-only scoping agent) · **Base read:**
RePark `origin/main` `dcc13ef4` (scratch clone, branch `scope/rest-catalogs`); the owned
iceberg-rust fork at the exact rev RePark pins, **`9e3522e3`**
(`9e3522e314e57cc6b074c9fb8fc1afe43f7c2b46`, five lines at [`Cargo.toml:153-158`](../../../Cargo.toml)).
**Status:** an **intake** — an evaluated scope for the owner to charter. It is not a plan of record;
[STATUS.md](../../../STATUS.md) stays the SSOT and each unit below earns a ledger under
`task/ledgers/staging/` only when chartered. Companion in shape to
[roadmap-intake-2026-08-23.md](roadmap-intake-2026-08-23.md); cards follow the format of
[cheap-tier-slate-2-2026-09-09.md](cheap-tier-slate-2-2026-09-09.md).

**Evidence rule.** Every code claim carries `file:line` read on the two clones named above. Every
Lakekeeper / Polaris claim carries a doc URL, fetched 2026-09-12. Anything not measured is marked
**UNMEASURED** with the measurement that would settle it.

**Retirement event.** This file closes when REST-0 is chartered (its content moves into that card's
brief) or when the owner declines the track; it is then archived to `docs/history/`.

---

## 1. Verdict

RePark is **much closer to a generic REST catalog than the empty `[patch.crates-io]` row suggests**:
the owned fork already ships a complete, hardened `iceberg-catalog-rest` at the pinned rev
— the full `Catalog` trait surface including `update_table` with the commit-outcome taxonomy
RePark's MERGE and compaction depend on (`crates/catalog/rest/src/catalog.rs:1148-1244`,
409 → retryable `CatalogCommitConflicts`, 500/502/503/504 and a lost 200 body → non-retryable
`CommitStateUnknown`), OAuth2 `client_credentials` with Java's exact proactive-refresh arithmetic
(`crates/catalog/rest/src/client.rs:216-286, 311-397`), bearer `token`, `scope` / `audience` /
`resource` / `oauth2-server-uri` / `token-refresh-enabled`, arbitrary `header.*` request headers
(`catalog.rs:322-362`), `/v1/config` `defaults`/`overrides` merging with `prefix` routing
(`catalog.rs:226-236, 405-451`), `pageToken` pagination on namespaces/tables/views
(`catalog.rs:653, 820, 1260`), the full view surface (`catalog.rs:1251-1550`), and vended-credential
overlay onto the per-table `FileIO` by longest-prefix match (`catalog.rs:71-91, 525-572`) — and
both target servers are configured through exactly the keys that crate already reads
(Polaris: `type=rest`, `uri`, `credential=<id>:<secret>`, `scope='PRINCIPAL_ROLE:ALL'`, `warehouse=<catalog
name>`, `header.X-Iceberg-Access-Delegation=vended-credentials`, `token-refresh-enabled=true` —
https://polaris.incubator.apache.org/releases/1.2.0/getting-started/using-polaris/ ; Lakekeeper:
the same keys with `oauth2-server-uri` pointed at the external IdP and `warehouse=<warehouse
name>` — https://docs.lakekeeper.io/docs/latest/engines/). **RePark consumes none of it**: there
is no `rest` arm in `CatalogKind` (`crates/repark-core/src/catalog_config.rs:22-31`), no
`iceberg-catalog-rest` dependency anywhere in the workspace, and the string `"rest"` appears in
RePark only as a heuristic in the `repark.toml` cloud-credential warning
(`crates/repark-core/src/config_file/wiring.rs:100-107`) — so a user who writes `type = "rest"`
today gets `IllegalArgumentException: … has an unrecognized value 'rest' (expected 'glue',
's3tables', 'memory', 'postgres', or 'jdbc')` (`catalog_config.rs:186-193`). The distance from
here to "a Spark user points `type = "rest"` at Lakekeeper or Polaris and reads, writes and runs
maintenance" is therefore **three real problems, not a port**: (a) a catalog kind + config
passthrough, which is a day of mechanical work because `apply_prop` already keeps every unknown
suffix verbatim (`catalog_config.rs:196-198`); (b) **storage selection**, because `RestCatalog`
demands one `StorageFactory` for the whole catalog (`catalog.rs:557-565`) while RePark picks its
factory from a *location scheme* (`crates/repark-iceberg/src/catalog/location.rs:103-113`) and a
Polaris/Lakekeeper `warehouse` is a logical name with no scheme at all — plus RePark compiles
`iceberg-storage-opendal` with `features = ["opendal-s3"]` only
(`crates/repark-iceberg/Cargo.toml:29`), so Lakekeeper's ADLS / GCS / OneLake warehouses have no
backend to build; and (c) **vended-credential lifetime**, the fork's own named residue — the
OpenDAL `FileIO` takes a static props snapshot and neither the `refresh-credentials-endpoint`
protocol nor expiry handling exists (`docs/parity/GAP_MATRIX.md` row R160; grep for
`refresh-credentials` across the fork returns nothing), so any scan outliving an STS token dies.
**The first shippable slice is REST-0 + REST-1**: `type = "rest"` and `catalog-impl` ending in
`RESTCatalog` resolve to a new `CatalogKind::Rest`, every remaining key (including `header.*`,
`credential`, `token`, `scope`, `oauth2-server-uri`, `prefix`) passes through verbatim,
the catalog registers under `LocationPolicy::ServiceManagedLocation`
(`crates/repark-core/src/catalog_state.rs:17-18` — the same policy S3 Tables uses, and the correct
one for both servers, which assign table locations), the storage factory is chosen from an explicit
`io.scheme`-style key defaulting to S3, and the pins run against a **mocked REST server** with the
`mockito` tier the fork's own crate already carries (`crates/catalog/rest/Cargo.toml:46-49`).
That slice makes `SELECT`, `SHOW NAMESPACES`, `SHOW TABLES`, `DESCRIBE` and `INSERT` work against
both servers on S3-backed warehouses with vended credentials inside a single credential lifetime,
with no Docker and no JVM on this box.

---

## 2. What exists today

### 2a. In the fork's REST client, `crates/catalog/rest/` at `9e3522e3`

The crate is 8 137 lines across four sources plus one integration test
(`src/lib.rs` 59, `src/types.rs` 1 128, `src/client.rs` 1 483, `src/catalog.rs` 4 970 of which
1-1 551 is implementation and 1 553-4 970 is the inline `mockito` unit tier, plus
`tests/rest_catalog_test.rs` 497).

| Capability | State | Evidence (`crates/catalog/rest/…`) |
|---|---|---|
| `CatalogBuilder::load(name, props)`; `uri` required, `warehouse` optional | implemented | `src/catalog.rs:114-165`; required-key errors at `:124-135` |
| Config keys promoted to fields: `uri`, `warehouse`, `disable-header-redaction` | implemented | `src/catalog.rs:55-59, 129-143` |
| Every other prop kept verbatim and forwarded to the server + FileIO | implemented | `src/catalog.rs:141-145` |
| `/v1/config` negotiation on first use; server `defaults` under, `overrides` over, user props | implemented | `src/catalog.rs:486-525` (`context()` / `load_config`), merge at `:405-451` |
| `warehouse` sent as the `/v1/config` query parameter (Polaris catalog name / Lakekeeper warehouse) | implemented | `src/catalog.rs:506-509` |
| `prefix` from the config response routed into every URL | implemented | `src/catalog.rs:226-236` |
| Server-supplied `uri` override honoured | implemented | `src/catalog.rs:406-409` |
| `disable-header-redaction` stripped from both server maps (fork-local hardening) | implemented | `src/catalog.rs:411-449` |
| Bearer `token` auth | implemented | `src/catalog.rs:303-306`; header set at `src/client.rs:337-347` |
| OAuth2 `credential` as `client_secret` or `client_id:client_secret` | implemented | `src/catalog.rs:308-320` |
| `grant_type=client_credentials` POST, form-encoded | implemented | `src/client.rs:216-246` |
| `oauth2-server-uri`, else `<uri>/v1/oauth/tokens` | implemented | `src/catalog.rs:238-250` |
| `scope` (default `"catalog"`), `audience`, `resource` | implemented | `src/catalog.rs:364-385` |
| `token-refresh-enabled` (Java default true) + Java's `scheduleTokenRefresh` arithmetic | implemented | `src/catalog.rs:387-397`; `src/client.rs:40-91, 311-397` |
| Single-flight refresh; failed refresh keeps the old token; initial failure propagates | implemented | `src/client.rs:359-397` |
| Token-exchange grant (`subject_token`) for still-valid tokens; refresh of token-only clients | **missing** | `docs/parity/GAP_MATRIX.md` row R159 residues (1)(2) |
| Custom request headers from `header.<name>` props | implemented | `src/catalog.rs:322-362` |
| `X-Iceberg-Access-Delegation` sent by default | **missing** (reachable only as an explicit `header.*` prop) | grep for `delegation` across the whole fork returns nothing |
| Secret redaction in `Debug`, error bodies and header dumps | implemented | `src/catalog.rs:178-216`; `src/client.rs:459-646`; `src/types.rs:64-73, 301-312, 457` |
| `list_namespaces` (+`parent`), `create_namespace`, `get_namespace`, `namespace_exists`, `drop_namespace` | implemented | `src/catalog.rs:635-809` |
| `update_namespace` (whole-map replace) | **missing** — `ErrorKind::FeatureUnsupported`, `"Updating namespace not supported yet!"` | `src/catalog.rs:775-785` |
| `update_namespace_properties` / `set_` / `remove_` | **partial** — trait defaults route through `update_namespace`, so they fail on REST | `crates/iceberg/src/catalog/mod.rs:93-145` |
| `list_tables`, `create_table`, `load_table`, `drop_table`, `table_exists`, `rename_table`, `register_table` | implemented | `src/catalog.rs:810-1147` |
| `update_table` with the commit-outcome taxonomy | implemented | `src/catalog.rs:1148-1244` |
| Commit transport failure classified sent-vs-unsent | implemented | `src/client.rs:422-457` |
| `publish_create_table` (staged CTAS publish) | **partial** — inherits the trait default (`register_table`) | `crates/iceberg/src/catalog/mod.rs:181-190` |
| `publish_replace_table` (staged REPLACE CAS) | **missing** — trait default is `FeatureUnsupported` | `crates/iceberg/src/catalog/mod.rs:194-207` |
| `pageToken` pagination on namespaces, tables, views | implemented | `src/catalog.rs:653, 820, 1260` |
| Views: list / create / load / drop / exists / rename / update | implemented | `src/catalog.rs:1251-1550` |
| Vended credentials: `LoadTableResult.storage-credentials` parsed, longest-prefix selected, overlaid LAST onto the table `FileIO` props | implemented | `src/types.rs:301`; `src/catalog.rs:71-91, 525-547, 924, 982, 1136` |
| Vended credentials on the commit response | **not applicable** — `CommitTableResponse` carries none | `src/catalog.rs:1233-1238` |
| Vended-credential **expiry / refresh** (`refresh-credentials-endpoint`, `s3.session-token-expires-at-ms`) | **missing** | no hit anywhere in the fork; `GAP_MATRIX.md` row R160 residue |
| Per-path credential selection (data bucket ≠ metadata bucket) | **partial** — one credential chosen at `metadata_location` for every access | `src/catalog.rs:71-91`; R160 "granularity divergence (a)(b)" |
| Multi-table transactions (`/v1/{prefix}/transactions/commit`) | **missing** | no endpoint builder in `src/catalog.rs:252-295` |
| Server-side scan planning (`/v1/.../plan`, `PLANNING` endpoints) | **missing** | grep for `PLANNING` / `plan_table` in `crates/catalog/rest/src` returns nothing |
| SigV4 request signing (`rest.sigv4-enabled`, `rest.auth.type`) | **missing** | grep for `sigv4` across the whole fork returns nothing |
| Remote signing (`s3.remote-signing-enabled`) — Lakekeeper's S3 alternative to vending | **missing** | same grep |
| REST spec version advertised as `x-client-version` | `0.14.1` | `src/catalog.rs:61, 336-339` |
| `StorageFactory` **required**, one per catalog | implemented, and a constraint | `src/catalog.rs:557-565`, error `"StorageFactory must be provided for RestCatalog. Use `with_storage_factory` to configure it."` |

The fork's own parity ledger already grades the two REST-specific rows: row **R159** (OAuth2
refresh) 🟡 with offline mockito pins, and row **R160** (vended credentials) 🟡 with offline
behavioral pins, ✅ gated on a live credentialed round-trip — `docs/parity/GAP_MATRIX.md`. Row
**R157** records that REST is one of the four catalogs with sent-vs-unsent commit classification;
`docs/ENGINE_CONTRACT.md:367-401` is the engine-side contract that consumes it.

### 2b. In RePark, `dcc13ef4`

**The catalog-kind registry is a four-arm enum with a stub arm already in it** — the precedent for
adding a fifth is `Postgres`:

- `CatalogKind { Glue, S3Tables, Memory, Postgres }` — `crates/repark-core/src/catalog_config.rs:22-31`.
- `kind_from_type` accepts `"glue" | "s3tables" | "memory" | "postgres" | "postgresql" | "jdbc"`
  (trimmed, lowercased) — `catalog_config.rs:218-226`.
- `kind_from_catalog_impl` matches the Java class **suffix**: `GlueCatalog`, `S3TablesCatalog`,
  `JDBCTableCatalog` — `catalog_config.rs:204-215`. The Spark spelling both servers document,
  `org.apache.iceberg.rest.RESTCatalog`, has no arm.
- The build dispatch is one `match spec.kind` — `crates/repark-core/src/session.rs:491-524`; the
  `Postgres` arm returns `Error::NotImplemented` at `:523`, and S3 Tables registers with
  `LocationPolicy::ServiceManagedLocation` at `:519`.
- Late (post-`getOrCreate`) registration reuses the same dispatch —
  `crates/repark-core/src/session/late_catalogs.rs:13-48`.
- The three builders are thin wrappers returning `Arc<dyn iceberg::Catalog>` —
  `crates/repark-iceberg/src/catalog/builders.rs:25-65` (memory), `:70-92` (Glue), `:97-118`
  (S3 Tables). Only the memory builder passes a storage factory (`:49-50`); Glue and S3 Tables
  build their own `FileIO` internally from the prop map.
- `LocationPolicy { RequireExplicitLocation, ServiceManagedLocation, TempFallbackAllowed }` —
  `crates/repark-core/src/catalog_state.rs:14-24`.

**The config surface is already generic enough to carry every REST key.** `parse_catalog_specs`
(`catalog_config.rs:105-161`) normalizes `spark.sql.catalog.*` and `repark.sql.catalog.*` into one
keyspace, splits `<name>.<prop>` on the **first** dot only (`:130`), consumes `type`,
`catalog-impl` and `io-impl`, and keeps **every other suffix verbatim** (`:174-201`) — so
`spark.sql.catalog.lk.header.X-Iceberg-Access-Delegation` survives as the prop
`header.X-Iceberg-Access-Delegation`, which is exactly the spelling
`crates/catalog/rest/src/catalog.rs:340-345` reads. The TOML door is narrower: each
`[<profile>.catalog.<name>]` block is flattened key-by-key into
`repark.sql.catalog.<name>.<prop>` (`crates/repark-core/src/config_file/wiring.rs:233-245`,
prefix at `config_file/sources.rs:9`) through `plain_string`, which **refuses a nested table**
(`wiring.rs:266-274`, `"key `{label}.{section}.{key}` must be a string"`) — unlike
`[<profile>.conf]`, which has `flatten_conf` (`wiring.rs:276-286`). The documented catalog keys
are `docs/guide/repark-toml.md:209-212` and `docs/guide/iceberg-guide.md:38-131` (kind table at
`:40-48`, conf keys at `:76-82`, the refusal text at `:63-65`).

**Session wiring** runs `parse_catalog_specs` at `crates/repark-core/src/session.rs:213`, computes
`aws_signaled` from the Glue/S3Tables kinds at `:220-229`, resolves the AWS SDK config once at
`:476-482`, and registers each spec at `:466-473`. `FileIO` is selected by **location scheme**:
`storage_factory_for_location` maps `file://`/bare-absolute → `LocalFsStorageFactory` and
`s3://`/`s3a://` → `OpenDalStorageFactory::S3` (`crates/repark-iceberg/src/catalog/location.rs:103-113`),
and `file_io_for_location` builds from that factory plus a caller prop map (`:118-124`). Any other
scheme is a loud plan error (`:48-68`). `iceberg-storage-opendal` is compiled with
`features = ["opendal-s3"]` only (`crates/repark-iceberg/Cargo.toml:29`), so the GCS / ADLS /
OSS arms of `OpenDalStorageFactory` (`crates/storage/opendal/src/lib.rs:298-327` in the fork) are
not in the binary.

**Error mapping** is one classifier and one boundary: `ErrorClass { Parse, Analysis, Unsupported,
IllegalArgument, Base }` with `Error::Config → IllegalArgument`
(`crates/repark-common/src/lib.rs:36-62`), converted at `crates/repark-python/src/lib.rs:29-39`
into `ParseException` / `AnalysisException` / `UnsupportedOperationException` /
`IllegalArgumentException` / `PySparkException` (classes at
`crates/repark-python/src/exceptions.rs:13-54`). Every catalog-config refusal is `Error::Config`,
so it lands as `IllegalArgumentException`.

**RePark consumes no REST crate today** — `iceberg-catalog-rest` appears in neither
`Cargo.toml` `[workspace.dependencies]` (`:115-122`) nor `Cargo.lock` (grep: absent). Adding it
**must go through the same five-line fork-pin discipline**: a sixth line in `[patch.crates-io]`
carrying the **identical** rev `9e3522e314e57cc6b074c9fb8fc1afe43f7c2b46`
(`Cargo.toml:153-158`), plus a `iceberg-catalog-rest = "0.9.1"` line in `[workspace.dependencies]`
beside its siblings. It is **not** a sixth *rev*: the invariant the block enforces is one rev
across the whole `iceberg*` family, and a REST line at a different rev would split the family and
break the `Catalog` trait object. The bump procedure is AGENTS.md "Version-pin contract"; every
future RP-N repin then carries six lines instead of five.

---

## 3. The gap list, ranked

Ranked by what blocks the first user-visible success, then by blast radius.

### G-1 · No `rest` catalog kind — **both servers** — RePark catalog registry

`kind_from_type` (`crates/repark-core/src/catalog_config.rs:218-226`) and
`kind_from_catalog_impl` (`:204-215`) have no `rest` / `RESTCatalog` arm, so the only reachable
outcome today is the refusal at `:186-193`. The dispatch at
`crates/repark-core/src/session.rs:491-524` has no arm to add a builder to. Both servers document
`type=rest` **and** `catalog-impl=org.apache.iceberg.rest.RESTCatalog`
(Lakekeeper: https://docs.lakekeeper.io/docs/latest/engines/ ; Polaris:
https://polaris.incubator.apache.org/releases/1.2.0/getting-started/using-polaris/), so both
spellings need arms. Size: small. Owner: RePark.

### G-2 · One `StorageFactory` per REST catalog, chosen from a scheme RePark cannot see — **both** — RePark builder / fork FileIO

`RestCatalog::load_file_io` hard-fails without a factory
(`crates/catalog/rest/src/catalog.rs:557-565`) and the factory is stored once on the catalog
(`:465-473`). RePark's only selector needs a *location* (`location.rs:103-113`), and neither
server's `warehouse` is one: Polaris's is the **catalog name** (`quickstart_catalog`), Lakekeeper's
is the **warehouse name**. `StorageConfig` carries **only props, no scheme**
(`crates/iceberg/src/io/storage/config/mod.rs:67-70`), so a factory cannot dispatch per path
either. Consequences: (a) RePark must take the backend from an explicit key or default it;
(b) a catalog whose tables span `s3://` and `file://` cannot work; (c) Lakekeeper's ADLS / GCS /
OneLake warehouses (https://docs.lakekeeper.io/docs/latest/storage/) have no compiled backend at
all, because `crates/repark-iceberg/Cargo.toml:29` enables `opendal-s3` only. Size: small for the
S3 default, medium for a scheme-dispatching factory (fork card). Owner: RePark config + a fork
card for multi-scheme.

### G-3 · Vended credentials reach the storage layer, but only for one lifetime and one prefix — **both** — fork FileIO / vended credentials

This is where most REST integrations break, and here it is measurably **mostly solved**. The chain
is: the client sends `X-Iceberg-Access-Delegation: vended-credentials` (as a `header.*` prop,
`catalog.rs:322-362`) → the server returns `config` + `storage-credentials` on
`LoadTableResult` (`src/types.rs:301`) → `load_table` passes both into `load_file_io`
(`catalog.rs:975-985`) → `select_vended_credential` picks the longest `prefix` covering
`metadata_location` (`catalog.rs:71-91`) → its `config` map is extended onto the FileIO props
**last**, so it wins collisions (`catalog.rs:537-541`) → `FileIOBuilder::new(factory).with_props(props).build()`
(`catalog.rs:569-571`). The opendal S3 storage then reads exactly the keys both servers vend —
`s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`, `s3.region`, `s3.endpoint`,
`s3.path-style-access` (`crates/storage/opendal/src/s3.rs:43-52`; constants at
`crates/iceberg/src/io/storage/config/s3.rs:41-47`), which are the exact names Polaris documents
vending (https://polaris.apache.org/in-dev/unreleased/vended-credentials/). **So yes: the opendal
S3 storage IS built per table from vended keys**, because `load_file_io` runs on every
`load_table`. What is missing:

- **Expiry.** Polaris also returns `s3.session-token-expires-at-ms` and a
  `refresh-credentials-endpoint`; the fork parses neither (grep: no hit) and the OpenDAL FileIO is
  a static props snapshot (`GAP_MATRIX.md` row R160 residue). A scan or a compaction outliving the
  STS token fails mid-flight. Nothing in RePark caches a REST `Table` — only `MemoryCatalogBuilder`
  takes a `TableMetadataCache` (`crates/iceberg/src/catalog/memory/catalog.rs:75`;
  `crates/repark-iceberg/src/catalog/builders.rs:51-56`) — so each access re-loads and re-vends;
  the failure window is one long operation, not a session. **UNMEASURED:** how long a
  `rewrite_data_files` over a large table runs against a 1 h token — settled by the REST-3 CI
  fixture with a short-TTL Polaris/Lakekeeper credential.
- **Per-path selection.** One credential is chosen at `metadata_location` and used for every file;
  a table whose data bucket differs from its metadata bucket gets the wrong one, and a credential
  covering only the data path is dropped entirely (R160 "granularity divergence (a)(b)").
- **Non-S3.** Lakekeeper vends ADLS SAS (`adls.sas-token.*`) and GCS tokens
  (`gcs.oauth2.token`) — the keys exist in the fork
  (`crates/iceberg/src/io/storage/config/gcs.rs:39-57`) but RePark does not compile those factories
  (`crates/repark-iceberg/Cargo.toml:29`).
- **Remote signing.** Lakekeeper's S3 alternative (`X-Iceberg-Access-Delegation: remote-signing`,
  `s3.remote-signing-enabled`) has no implementation anywhere in the fork. For an S3-compatible
  store where STS is off, that is the only path — and it is closed.

Size: the first slice is zero work (it already flows); refresh is a medium fork card; remote
signing is a large fork card. Owner: fork client + fork FileIO; RePark only chooses features.

### G-4 · Table commit through REST — **both** — confirmed sufficient, with one hole

RePark's DML and maintenance commit path is `Transaction::new(&table) … tx.commit(catalog)`
(`crates/repark-iceberg/src/write/append.rs:309-315`,
`write/partition_overwrite.rs:380-390, 414-430`, `write/merge/snapshot_commit.rs:123-163`,
`write/alter.rs:121-294`, `write/sort_order.rs:28-43`), which lands on
`Catalog::update_table` (`crates/iceberg/src/catalog/mod.rs:177`). The REST implementation
(`crates/catalog/rest/src/catalog.rs:1148-1244`) posts `CommitTableRequest { identifier,
requirements, updates }` and classifies: **409 → `CatalogCommitConflicts` with
`with_retryable(true)`** (`:1198-1204`), which is precisely what
`Transaction::commit`'s `backon` retry gate consumes per `docs/ENGINE_CONTRACT.md:367-401`;
**500/502/503/504 → `CommitStateUnknown`**, never retried (`:1206-1231`); **200 with an unreadable
body → `CommitStateUnknown`** (`:1170-1184`); and a post-send transport failure →
`CommitStateUnknown` via `query_catalog_for_commit` (`src/client.rs:422-457`). The
reconciliation-by-refresh path (`commit.status-check.*`) is catalog-agnostic and applies. **So
MERGE and compaction have the conflict/retry contract they rely on.** The hole is
`publish_replace_table` — the trait default is `FeatureUnsupported`
(`crates/iceberg/src/catalog/mod.rs:194-207`) and REST does not override it; today RePark does not
call it (CTAS goes `create_table` + append, `crates/repark-spark/src/ctas.rs:436`,
`:172-177`), so this is a *latent* gap that bites the day staged REPLACE is wired. Size: zero for
the first slice; small fork card when staged CTAS reaches REST. Owner: fork.

### G-5 · `CREATE TABLE` / CTAS location resolution builds FileIO from `catalog.properties()` — **both** — RePark

Two sites build a `FileIO` for a *staged create* out of the catalog's user props rather than the
table's own vended FileIO: `crates/repark-spark/src/ctas.rs:324` and
`crates/repark-sql/src/create_table.rs:263`, both
`file_io_for_location(&location, catalog.properties())`. For REST that map is the **user-supplied**
props only — `RestCatalog::properties()` returns `user_config.props`, explicitly *not* the
server-merged runtime config (`crates/catalog/rest/src/catalog.rs:629-633`) — so it carries
neither the server `defaults` (`s3.endpoint`, `s3.region`) nor any vended credential. The fix is
free if REST registers under `LocationPolicy::ServiceManagedLocation`
(`crates/repark-core/src/catalog_state.rs:17-18`), because
`ctas.rs:172-177` then takes `CtasMode::ServiceManagedCreate` and never reaches `:324` — and
service-managed is the *correct* description of both servers, which assign table locations from the
warehouse. Size: small (one dispatch line + a pin). Owner: RePark.

### G-6 · Namespaces: multi-part identifiers work, property updates do not — **both** — fork client

`NamespaceIdent::to_url_string` encodes multi-part namespaces (`crates/iceberg/src/catalog/mod.rs:378`)
and every REST endpoint builder uses it (`crates/catalog/rest/src/catalog.rs:252-295`), so
`ns1.ns2.table` addressing works; `list_namespaces(parent)` and pagination are implemented
(`:635-689`). But `update_namespace` is `FeatureUnsupported` (`:775-785`), and because
`update_namespace_properties` / `set_namespace_properties` / `remove_namespace_properties` are
trait **defaults** routed through it (`crates/iceberg/src/catalog/mod.rs:93-145`), every
namespace-property write fails on REST. RePark's `ALTER NAMESPACE … SET PROPERTIES` surface
(`crates/repark-spark/src/namespace_ddl.rs:90`) therefore refuses. Note also that
`SHOW NAMESPACES IN <cat>` calls `list_namespaces(None)`
(`crates/repark-spark/src/describe_show.rs:569-578`), which on both servers returns **top-level
namespaces only** — a nested-namespace deployment will look empty below the first level. Size:
small fork card (`POST /v1/namespaces/{ns}/properties`) plus one RePark pin. Owner: fork + RePark.

### G-7 · Views: the fork implements catalog views; RePark has no door to them — **both** — RePark

The fork's REST crate implements the whole view surface
(`crates/catalog/rest/src/catalog.rs:1251-1550`) and `ReparkCatalogProvider` already delegates
`list_views` / `create_view` / `load_view` (`crates/repark-iceberg/src/catalog/provider.rs:550-584`).
RePark's `CREATE VIEW`, however, is a **DataFusion/temp view** — `crates/repark-sql/src/router/tests.rs:163`,
`crates/repark-core/src/session/temp_views.rs:235-250` — with no catalog-view path, and
`SHOW VIEWS` is not implemented in `crates/repark-spark/src/describe_show.rs` (it appears only in
the unsupported list at `crates/repark-spark/src/tests/describe_show.rs:806`). So a Polaris or
Lakekeeper catalog view is invisible to RePark and a RePark view never reaches the catalog. Size:
medium. Owner: RePark. Note this is a **parity gap, not a REST gap** — it is equally true of Glue.

### G-8 · `DESCRIBE` / `SHOW` surfaces — **both** — RePark, mostly free

`SHOW NAMESPACES` (`describe_show.rs:569-578`), `SHOW TABLES` and `DESCRIBE [EXTENDED]`
(`describe_show.rs:93, 444`) all run through `Arc<dyn Catalog>` and the table metadata, so they
work unchanged the moment a REST catalog is registered. The two caveats are G-6's flat
`list_namespaces(None)` and `DESCRIBE EXTENDED`'s merged property view (`:444`), which for REST
will show the table's own metadata properties and **not** the server's `/v1/config` defaults —
those live only inside the fork's `RestContext` and are unreachable through `Catalog::properties()`
(`crates/catalog/rest/src/catalog.rs:629-633`). Size: small (pins). Owner: RePark.

### G-9 · `repark.toml` cannot express `header.*` — **both** — RePark config loader

`[<profile>.catalog.<name>]` flattens one level only and `plain_string` refuses a nested table
(`crates/repark-core/src/config_file/wiring.rs:233-245, 266-274`), so the natural TOML spelling

```toml
[default.catalog.lakekeeper.header]
X-Iceberg-Access-Delegation = "vended-credentials"
```

fails with ``key `default.catalog.lakekeeper.header` must be a string``, while the quoted-key form
`"header.X-Iceberg-Access-Delegation" = "vended-credentials"` works by accident. The `[<profile>.conf]`
table already has the flattener this needs (`wiring.rs:276-286`), including the
quoted-vs-nested collision check. Note the loader **already** classifies `type = "rest"` as a cloud
catalog for its credential warning (`wiring.rs:100-107`) — the surface anticipated REST before the
parser did. Size: small. Owner: RePark.

### G-10 · Spark-config parity keys — **both** — RePark, mostly free

`apply_prop` passes unknown suffixes through verbatim (`catalog_config.rs:196-198`) and the key
split takes the first dot only (`:130`), so `uri`, `credential`, `token`, `scope`,
`oauth2-server-uri`, `warehouse`, `prefix`, `token-refresh-enabled`, `audience`, `resource` and
every `header.*` already reach the builder untouched. What needs decisions: `catalog-impl` must
accept the `RESTCatalog` suffix (G-1); `io-impl` is still silently dropped (`:195`, documented at
`docs/guide/iceberg-guide.md:82`) which is right; `client.region` — which Polaris's own quickstart
sets — is not an Iceberg REST key and would be forwarded to the server as an unknown prop
(harmless, and the fork forwards it to FileIO where opendal ignores it); and `rest.sigv4-enabled` /
`rest.auth.type` have no implementation to reach (G-11). Size: small. Owner: RePark.

### G-11 · SigV4 / remote signing — **neither server by default; AWS-fronted REST endpoints** — fork client

No `sigv4` anywhere in the fork. This blocks (a) an Iceberg REST endpoint fronted by AWS API
Gateway / S3 Tables' REST surface, and (b) Lakekeeper's `remote-signing` S3 delegation mode
(https://docs.lakekeeper.io/docs/latest/storage/ — remote signing defaults to enabled and is the
fallback when STS is off). Neither Lakekeeper's nor Polaris's documented Spark configuration uses
it, so it is **out of the first slice**. Size: large fork card. Owner: fork.

### G-12 · Error mapping to PySpark exception classes — **both** — RePark

A REST failure arrives as `iceberg::Error`, folded to `DataFusionError::External`
(`crates/repark-iceberg/src/catalog/builders.rs:148-150`) and thence to `ErrorClass::Base` →
`PySparkException` (`crates/repark-common/src/lib.rs:57-60`;
`crates/repark-python/src/lib.rs:38`). That means an expired OAuth token, a 403 from Polaris's RBAC
and a missing table all surface as the same generic class, where Spark raises
`AnalysisException` for a missing table and an authentication error for the first two. The
config-time refusals (bad `uri`, missing `credential`) are `Error::Config` →
`IllegalArgumentException`, which is right. Size: small (a mapping arm keyed on
`ErrorKind::{TableNotFound, NamespaceNotFound}` → `Analysis`). Owner: RePark.

### G-13 · Protocol extensions neither server needs yet — **both** — fork client

Multi-table transactions (`/v1/{prefix}/transactions/commit`) and the scan-`PLANNING` endpoints are
absent from the fork (no endpoint builders at `crates/catalog/rest/src/catalog.rs:252-295`). Both
are optional in the REST spec and neither server requires them of a client. Recorded so the slate
does not rediscover them. Size: large. Owner: fork. **Out of scope.**

---

## 4. Testing strategy

This box has **no Docker** and no JVM for this track; CI has both. The fork's Docker fixture is
the proven shape and it already contains a REST server.

**Tier 1 — offline unit, the default gate (no Docker, no network).** The fork's REST crate already
runs a `mockito` tier: `mockito` is a dev-dependency at
`crates/catalog/rest/Cargo.toml:46-49` (workspace pin `Cargo.toml:101`, resolved 1.7.2), imported
at `crates/catalog/rest/src/catalog.rs:1566` and `src/client.rs:1096`, and it is the **only** crate
in the fork that carries it. Every REST behavior the fork owns — `/v1/config` merge, pagination,
token refresh single-flight, vended-credential selection, the commit taxonomy — is pinned that way
at `catalog.rs:1553-4970`. RePark should mirror it: add `mockito` as a **dev-dependency of
`repark-iceberg`** and stand a fake REST server up inside the Rust test tier, serving `/v1/config`,
`/v1/oauth/tokens`, `/v1/namespaces`, `/v1/namespaces/{ns}/tables/{t}` from fixtures, with the
warehouse pointed at a `TempDir` so the vended `config` map names a `file://` prefix and the
whole read path runs with no cloud. This is the tier that REST-0, REST-1, REST-2 and REST-4 pin
against, and it runs inside `make verify` with no new gate.

**Tier 2 — CI-only integration fixture (Docker, CI only).** The fork's `make test` is
`docker-up` → `nextest` → `docker-down` (`Makefile:88-90, 102-109`) over ONE unified compose file,
`dev/docker-compose.yaml`, whose services are MinIO (`quay.io/minio/minio:RELEASE.2025-05-24T17-08-30Z`,
`:30-52`), an `mc` bucket bootstrap (`:55-78`), and the REST fixture
**`apache/iceberg-rest-fixture:1.10.0`** on `8181` with a sqlite JDBC backend and
`CATALOG_S3_ENDPOINT=http://minio:9000` (`:82-105`), plus HMS, moto, fake-GCS and a Spark box.
Integration tests are gated not by a feature flag or `#[ignore]` but by the `make test` ordering,
with the `iceberg_test_utils` helper behind `features = ["tests"]`
(`crates/test_utils/src/lib.rs:22-25`) and each `tests/*.rs` documenting the dependency
(`crates/catalog/rest/tests/rest_catalog_test.rs:18-21`, polling `/v1/config` up to 30×1 s at
`:44-64`). The only CI job that starts Docker is `ci.yml`'s `tests` job, `default` variant, step
`Start Docker containers` (`.github/workflows/ci.yml:204-206`) — which is the operational content
of the "CI Tests (default) is the only fixture build proof" rule. (**Correction to the brief:**
that sentence is **not** in RePark's `task/lessons.md` — a grep of all 278 lines finds no `Docker`
and no `CI Tests`; the rule as written lives in the orchestrator's memory and its evidence is the
fork's `docs/testing.md:162-165` plus `ci.yml:204-206`. RePark itself has no Docker fixture and no
compose file today.)

Proposal for REST-3: a **RePark-side** `docker/rest/compose.yaml` modelled on the fork's, running
MinIO + `mc` + **one** REST server, wired to a new `make py-test-rest` / `make rust-test-rest`
target that is **CI-only and not in `make verify` or `make preflight`** — the same posture as
`docs/testing.md` gives the fork's service-bound suites. Start with **Lakekeeper** as the single
fixture (REST-D-1), because its container needs only Postgres + MinIO and its warehouse can be
provisioned over its own management API in the bootstrap step; add Polaris in a second compose
profile once the Lakekeeper leg is green, since Polaris additionally needs its bootstrap principal
and a catalog created before Spark keys exist. If a third server is wanted for free, the fork's
own `apache/iceberg-rest-fixture:1.10.0` is a zero-cost conformance leg that proves the spec path
without either vendor's auth.

**Tier 3 — owner-gated live tier.** RePark's precedent is not `REPARK_LIVE` — it is
`REPARK_AWS_ACCEPTANCE`: a module-level `pytest.mark.skipif` at
`python/repark/tests/test_aws_acceptance.py:80-86` ("real-AWS acceptance harness: set
REPARK_AWS_ACCEPTANCE=1 to run"), with the secrets supplied only by a dedicated workflow that
carries an owner-approved GitHub environment — `.github/workflows/aws-acceptance.yml:13-18`
(nightly cron + `workflow_dispatch`), `:32` (`environment: aws-acceptance`), `:65-71` (OIDC role
assumption), `:74-84` (the pytest invocation with `REPARK_AWS_ACCEPTANCE: "1"`). There is **no**
Makefile target for it. The REST analogue is `REPARK_REST_ACCEPTANCE=1` plus
`REPARK_REST_URI` / `REPARK_REST_CREDENTIAL` / `REPARK_REST_WAREHOUSE`, in a `rest-acceptance.yml`
of the same shape, never a required check. That is the only tier that can settle the
vended-credential expiry question (G-3) against a real STS lifetime.

**Red-first pin names per unit** are listed with each card in §5.

---

## 5. The unit slate

Tier legend and process are [cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md) §1:
**M** mechanical, **I** interpretive, **O** orchestrator. Workers are **Devin** (default, free) or
**Grok** (fallback on a misbehaving round), per S2-15 of
[cheap-tier-slate-2-2026-09-09.md](cheap-tier-slate-2-2026-09-09.md); **never Opus**. Fork cards run
on the iceberg-rust fork lane and each ends in an RP-N repin PR of its own.

**Order and size:** REST-0 (S) → REST-1 (S) → REST-2 (M) → F-REST-NSPROPS-1 (S, parallel) →
REST-3 (M) → REST-4 (M) → F-REST-IO-SCHEME-1 (M) → F-REST-CREDREFRESH-1 (L) →
F-REST-SIGV4-1 (L, deferred).

---

### Card REST-0 — `type = "rest"` behind the existing catalog registry

**Why.** Everything downstream needs a registered `Arc<dyn Catalog>`. The fork's crate is complete;
RePark's only missing piece is a fifth enum arm and a sixth pin line. Nothing else in the slate can
start.

**Home.** `Cargo.toml` (`[workspace.dependencies]` line, `[patch.crates-io]` sixth line at the
**same** rev `9e3522e3`), `crates/repark-iceberg/Cargo.toml`,
`crates/repark-iceberg/src/catalog/builders.rs` (a `rest_catalog` fn beside `glue_catalog`),
`crates/repark-core/src/catalog_config.rs` (`CatalogKind::Rest`, `kind_from_type`,
`kind_from_catalog_impl`), `crates/repark-core/src/session.rs` (the dispatch arm),
`crates/repark-iceberg/src/catalog/tests/`, `docs/guide/iceberg-guide.md`,
`docs/guide/repark-toml.md`, maps.

**Decisions pre-made.**

- **D-1** `type` accepts `"rest"` only (case-insensitive, trimmed); `catalog-impl` accepts any
  class ending in `RESTCatalog` — mirroring the existing suffix rule at `catalog_config.rs:204-215`.
  Both servers document both spellings.
- **D-2** `uri` is **required** and refused loud when absent or blank, through the existing
  `require_non_empty_prop` helper (`builders.rs:127-140`). `warehouse` is **optional** — Polaris
  and Lakekeeper both use it, but the spec does not require it, and the fork already treats it as
  optional (`catalog.rs:136-139`).
- **D-3** The catalog registers under `LocationPolicy::ServiceManagedLocation`
  (`crates/repark-core/src/catalog_state.rs:17-18`), the same arm S3 Tables uses at
  `session.rs:519`. This is correct (both servers assign table locations) and it is what keeps CTAS
  off the `catalog.properties()` FileIO path (G-5).
- **D-4** The storage factory comes from a new prop `repark.rest.storage-scheme`, default
  `"s3"`; accepted values `"s3"`, `"s3a"`, `"file"`, resolved through the existing
  `storage_factory_for_location` classifier by synthesizing `"<scheme>://"`. Any other value
  refuses loud naming the key and the accepted set. No auto-detection from the warehouse — a
  logical warehouse name has no scheme, and guessing is how this breaks silently.
- **D-5** `aws_signaled` (`session.rs:220-229`) does **not** fire for `Rest`. A REST catalog with
  vended credentials must not also load the ambient AWS default chain, or an EC2 instance role
  would silently outrank the vended key on a prop collision.
- **D-6** Every prop other than `type` / `catalog-impl` / `io-impl` / `repark.rest.storage-scheme`
  passes through verbatim, unchanged from `apply_prop` (`catalog_config.rs:196-198`).
  `io-impl` stays dropped.

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | M (Devin) | The sixth `[patch.crates-io]` line at the identical rev + the `[workspace.dependencies]` line + `repark-iceberg` dep; `cargo build -p repark-iceberg` green; a pin that all six patch lines carry one rev (a text assertion in the existing manifest test, red first by editing one rev). |
| 2 | I (Devin) | `CatalogKind::Rest`, both resolvers, the dispatch arm, `rest_catalog()` in `builders.rs` with D-2/D-4/D-5; red-first unit tests in `catalog_config.rs`. |
| 3 | I (Devin) | The `mockito` REST fixture in `repark-iceberg` dev-deps: `/v1/config` + namespaces + `load_table` served from a `TempDir` warehouse with a `file://` vended prefix; pins that a registered REST catalog answers `SHOW NAMESPACES`, `SHOW TABLES` and a `SELECT`. `make verify`. |
| 4 | M (Devin) | Guide rows (`iceberg-guide.md` kind table + conf keys, `repark-toml.md`), maps, ledger. |

**Pins.** `rest_type_resolves_kind`, `rest_catalog_impl_suffix_resolves_kind`,
`rest_requires_non_empty_uri`, `rest_warehouse_optional`,
`rest_storage_scheme_default_is_s3`, `rest_storage_scheme_unknown_refuses`,
`rest_registers_service_managed_location`, `rest_does_not_signal_aws`,
`rest_unknown_props_pass_through_verbatim`, `fork_patch_lines_share_one_rev`,
`mock_rest_catalog_lists_namespaces`, `mock_rest_catalog_selects_rows`.

**Done when.** A builder with four `.config()` lines registers a REST catalog and reads a table
from the mocked server, and `make verify` is green.

**Hand back when.** `RestCatalogBuilder` will not accept RePark's `Arc<dyn StorageFactory>`
(then the factory trait needs a fork change — say which); the sixth patch line perturbs the
resolved versions of any `iceberg*` sibling (then the family freeze is at risk — stop).

**Rounds.** 4 (M, I, I, M). **Size: S.**

---

### Card REST-1 — auth: bearer, OAuth2 client credentials, and the two documented configs

**Why.** Neither server answers an unauthenticated request. This card proves RePark carries
Polaris's and Lakekeeper's documented Spark configurations byte-for-byte.

**Home.** `crates/repark-core/src/catalog_config.rs` (tests only — the keys already pass through),
`crates/repark-core/src/config_file/wiring.rs` (G-9: a `flatten_conf`-style flattener for catalog
blocks), `crates/repark-iceberg/src/catalog/tests/rest_auth.rs` (new),
`docs/guide/iceberg-guide.md` (two worked configs), maps.

**Decisions pre-made.**

- **D-1** No new key names. `credential`, `token`, `scope`, `oauth2-server-uri`, `audience`,
  `resource`, `token-refresh-enabled`, `prefix` and `header.*` are the Iceberg spellings and they
  are forwarded verbatim. RePark documents them; it does not rename them.
- **D-2** The `[<profile>.catalog.<name>]` TOML block gains **nested-table flattening**, reusing
  `flatten_conf` (`wiring.rs:276-286`) including its quoted-vs-nested collision refusal, so
  `[default.catalog.lk.header]` works and collides loudly with `"header.X" = …`.
- **D-3** Secrets: `credential` and `token` are added to the existing redaction needle set so a
  redacted `repark.toml` dump (`docs/guide/repark-toml.md:171`) never prints them. Verified against
  the fork's own needle test, which already covers both (`crates/iceberg/src/io/storage/config/mod.rs:82-89`).
- **D-4** The two documented configurations are carried in the guide **verbatim from the vendors'
  docs**, with the URL beside each — Polaris's `scope='PRINCIPAL_ROLE:ALL'` +
  `warehouse=<catalog name>` + `uri=<host>/api/catalog`, Lakekeeper's `oauth2-server-uri=<IdP token
  endpoint>` + `warehouse=<warehouse name>` + `uri=<host>/catalog`.
- **D-5** No default `X-Iceberg-Access-Delegation`. RePark does not inject the header; the user
  sets it, exactly as both vendors' documented Spark configs do. (Revisit under REST-D-3.)

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | I (Devin) | Mocked-server pins: a `credential` config performs one `client_credentials` POST and puts `Authorization: Bearer` on the next catalog call; a `token` config performs zero token calls; `oauth2-server-uri` is honoured over `<uri>/v1/oauth/tokens`; `scope` reaches the form body; `header.*` reaches the request. |
| 2 | I (Devin) | The catalog-block flattener (D-2) + red-first refusals; the redaction needles (D-3). |
| 3 | M (Devin) | Guide section with both vendor configs and their URLs, `repark-toml.md` equivalents, maps, ledger. |

**Pins.** `rest_credential_exchanges_once_then_bearer`, `rest_token_skips_token_endpoint`,
`rest_oauth2_server_uri_overrides_default_endpoint`, `rest_scope_reaches_token_request`,
`rest_access_delegation_header_reaches_request`, `rest_polaris_documented_config_registers`,
`rest_lakekeeper_documented_config_registers`, `toml_catalog_nested_header_table_flattens`,
`toml_catalog_quoted_and_nested_header_collide`, `rest_credential_redacted_in_dump`,
`test_rest_credential_not_in_conf_repr`.

**Done when.** Both vendors' documented Spark configurations, and their `repark.toml`
equivalents, register a catalog and authenticate against the mocked server.

**Hand back when.** Lakekeeper requires a header RePark cannot express (then name it).

**Rounds.** 3 (I, I, M). **Size: S.**

---

### Card REST-2 — vended credentials reach the per-table FileIO, provably

**Why.** This is the step that decides whether writes work. The mechanism exists in the fork
(G-3); what does not exist is a RePark-side proof that a vended `s3.*` map survives from the
`load_table` response into the object-store client, and a loud failure when it does not.

**Home.** `crates/repark-iceberg/src/catalog/builders.rs` (the factory choice from D-4 of REST-0),
`crates/repark-iceberg/src/catalog/tests/rest_vended.rs` (new),
`crates/repark-iceberg/Cargo.toml` (the opendal feature set), `docs/guide/iceberg-guide.md`, maps.

**Decisions pre-made.**

- **D-1** First slice is **S3 only**. `opendal-gcs` / `opendal-azdls` stay off
  (`crates/repark-iceberg/Cargo.toml:29`); a REST catalog whose vended map carries only
  `adls.*` or `gcs.*` keys refuses loud at first use, naming the unsupported backend — it does
  not silently fall back to an un-vended client.
- **D-2** The proof runs **offline**: the mocked server returns a `storage-credentials` entry whose
  `prefix` is the `TempDir` warehouse and whose `config` sets a sentinel prop; the pin asserts the
  built `FileIO`'s props carry the sentinel and that it **outranks** a colliding catalog-level
  prop (the fork's "vended wins" contract, `crates/catalog/rest/src/catalog.rs:537-541`).
- **D-3** Expiry is **out of this card**. The known residue — a static props snapshot with no
  `refresh-credentials-endpoint` (G-3) — is recorded in the guide as a known issue with the fork
  card name beside it, not worked around in RePark.
- **D-4** A credential whose `prefix` does not cover the table's `metadata_location` is the fork's
  silent-skip path. RePark adds a `tracing::warn!` at the builder seam when a REST load returns
  `storage_credentials` and none matched — silent is how this class of bug hides. **UNMEASURED:**
  whether either server ever vends a non-covering prefix; settled by REST-3's fixture.

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | I (Devin) | Mocked pins for D-2: sentinel survives, vended beats catalog-level, no-match logs and falls back, zero-credentials path unchanged. |
| 2 | I (Devin) | D-1's loud refusal for `adls.*`/`gcs.*`-only vended maps + the backend name in the message. |
| 3 | M (Devin) | Guide: the vended-credential section with the `header.X-Iceberg-Access-Delegation` line, both vendors' URLs, and the expiry known-issue with the fork card name. Maps, ledger. |

**Pins.** `vended_credential_reaches_file_io_props`,
`vended_credential_outranks_catalog_prop`, `vended_no_match_warns_and_falls_back`,
`vended_absent_is_unchanged`, `vended_adls_only_refuses_naming_backend`,
`vended_gcs_only_refuses_naming_backend`.

**Done when.** A vended S3 map from the mocked server demonstrably reaches the object-store
client, and every unsupported vended shape refuses loud.

**Hand back when.** The opendal S3 storage ignores a key the server vends (then name the key —
that is a fork card).

**Rounds.** 3 (I, I, M). **Size: M.**

---

### Card REST-3 — DML and maintenance commits through REST, on a CI Docker fixture

**Why.** The conflict/retry contract is implemented (G-4) but has never been exercised against a
real REST server from RePark. Compaction and MERGE are the two paths where a mis-classified 409
corrupts a table.

**Home.** `docker/rest/compose.yaml` (new), `Makefile` (a CI-only target),
`.github/workflows/` (the job), `python/repark/tests/test_rest_integration.py` (new),
`crates/repark-iceberg/src/catalog/tests/`, `docs/testing.md`, maps.

**Decisions pre-made.**

- **D-1** **Lakekeeper first** (REST-D-1). One compose file, three services: Lakekeeper, its
  Postgres, MinIO + an `mc` bootstrap, modelled directly on the fork's
  `dev/docker-compose.yaml:30-105`. Images pinned by digest, as the fork pins MinIO.
- **D-2** The target is **CI-only** and named in `docs/testing.md` as not part of `make verify`
  or `make preflight` — the same posture the fork states at its `docs/testing.md:162-165`. It is
  never a required check on this box.
- **D-3** The commit battery: `INSERT INTO`, `INSERT OVERWRITE`, `MERGE`, `DELETE`, then
  `CALL rewrite_data_files` and `CALL expire_snapshots`; plus a **concurrent-writer** leg (two
  sessions appending to one table) that must produce exactly one retry-and-succeed and no lost
  snapshot.
- **D-4** `CALL remove_orphan_files` is **excluded** on REST for now: its listing model is the one
  S2-28 already found service-dependent, and a REST server's warehouse layout is the server's, not
  RePark's.
- **D-5** Polaris joins as a second compose profile in a follow-up round of this same card, not a
  new card, once the Lakekeeper leg is green twice.

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | I (Devin) | The compose file + bootstrap (warehouse created over Lakekeeper's management API, bucket created in MinIO), and a `make rest-fixture-up` / `-down` pair. No tests yet. |
| 2 | I (Devin) | The read leg: connect with the documented config, create a namespace and a table, `SELECT`, `SHOW`, `DESCRIBE`. Vended credentials ON. |
| 3 | I (Devin) | The D-3 commit battery including the concurrent-writer leg. |
| 4 | I (Devin/Grok) | The maintenance leg; `docs/testing.md` section; the CI job wired to the `default` tier only; maps, ledger. |

**Pins.** `rest_fixture_config_endpoint_answers`, `rest_create_namespace_and_table`,
`rest_insert_into_commits`, `rest_insert_overwrite_commits`, `rest_merge_commits`,
`rest_delete_commits`, `rest_concurrent_append_retries_once_and_both_land`,
`rest_rewrite_data_files_commits`, `rest_expire_snapshots_commits`,
`test_rest_integration_roundtrip`, `test_rest_vended_write_without_static_keys`.

**Done when.** A Lakekeeper container, with vended credentials and no static S3 keys in the Spark
config, survives the whole DML + maintenance battery in CI.

**Hand back when.** A commit fails with `CommitStateUnknown` on a healthy server (that is a fork
classification bug — stop and file it); vended credentials expire mid-battery (that is
F-REST-CREDREFRESH-1 arriving early — record the measured TTL).

**Rounds.** 4 (I, I, I, I). **Size: M.**

---

### Card REST-4 — catalog views, `DESCRIBE` and `SHOW` parity

**Why.** Polaris and Lakekeeper both store Iceberg views; a user migrating a Spark pipeline will
have them. RePark's `CREATE VIEW` is a temp view today (G-7) and `SHOW VIEWS` does not exist.

**Home.** `crates/repark-spark/src/describe_show.rs`, `crates/repark-sql/src/` (the view router),
`crates/repark-core/src/session/temp_views.rs` (the boundary between temp and catalog views),
`crates/repark-iceberg/src/catalog/provider.rs`, tests, `docs/guide/iceberg-guide.md`, maps.

**Decisions pre-made.**

- **D-1** `CREATE TEMP VIEW` stays a DataFusion view, unchanged. A three-part
  `CREATE VIEW cat.ns.v` on a catalog that implements views becomes a **catalog view**; on one that
  does not (`memory`, `glue`) it refuses loud naming the catalog kind — never a silent temp view.
- **D-2** `SHOW VIEWS [IN <cat>.<ns>]` is added, mirroring `SHOW NAMESPACES`'s shape
  (`describe_show.rs:491-578`) and its live-oracle column contract.
- **D-3** `DESCRIBE EXTENDED` on a REST table does **not** try to show the server's `/v1/config`
  defaults — they are unreachable through `Catalog::properties()`
  (`crates/catalog/rest/src/catalog.rs:629-633`). The guide says so rather than the code faking it.
- **D-4** Nested namespaces: `SHOW NAMESPACES IN <cat>` keeps its top-level-only semantics (that is
  Spark's) and the guide documents `SHOW NAMESPACES IN <cat>.<ns>` for descent. Pins both.

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | I (Devin) | `SHOW VIEWS` parse + execute over `Catalog::list_views`, against the mocked server and the memory catalog's refusal. |
| 2 | I (Devin) | The `CREATE VIEW` / `DROP VIEW` router split of D-1 with the loud refusal, red-first. |
| 3 | I (Devin) | `DESCRIBE`/`SHOW` pins against the REST-3 fixture; nested-namespace pins. |
| 4 | M (Devin) | Guide, maps, ledger. |

**Pins.** `show_views_lists_catalog_views`, `show_views_refuses_on_memory_catalog`,
`create_view_three_part_creates_catalog_view`, `create_view_refuses_on_glue_naming_kind`,
`create_temp_view_unchanged`, `describe_extended_rest_table_shape`,
`show_namespaces_in_nested_namespace_descends`, `test_rest_views_roundtrip`.

**Done when.** A catalog view created in RePark is visible to a Spark client on the same server,
and vice versa.

**Hand back when.** The fork's `create_view` requires a view SQL dialect RePark cannot produce
(then name the dialect field).

**Rounds.** 4 (I, I, I, M). **Size: M.**

---

### Card F-REST-NSPROPS-1 (fork) — `update_namespace` over the REST properties endpoint

**Why.** `crates/catalog/rest/src/catalog.rs:775-785` returns `FeatureUnsupported`, and because
`update_namespace_properties` / `set_` / `remove_` are trait defaults routed through it
(`crates/iceberg/src/catalog/mod.rs:93-145`), every namespace-property write fails on REST —
including RePark's `ALTER NAMESPACE … SET PROPERTIES`
(`crates/repark-spark/src/namespace_ddl.rs:90`).

**Home (fork).** `crates/catalog/rest/src/catalog.rs` (the endpoint + the method),
`crates/catalog/rest/src/types.rs` (the request/response pair), the crate's `mockito` tier.

**Decisions pre-made.** Implement `POST /v1/{prefix}/namespaces/{ns}/properties` with the spec's
`removals` / `updates` body, override `update_namespace_properties` directly (the partial form is
the wire form), and keep `update_namespace` as the whole-map convenience computed from a
`get_namespace` read. Mirror Java's "removing an absent key is a no-op".

**Steps.** 1 · I (Devin, fork lane): the types + endpoint + both methods + `mockito` pins.
2 · M: an RP-N repin PR in RePark plus the `ALTER NAMESPACE` pin.

**Pins (fork).** `update_namespace_properties_posts_removals_and_updates`,
`update_namespace_properties_absent_removal_is_noop`,
`update_namespace_reads_then_posts`, `namespace_properties_overlap_refuses`.

**Rounds.** 2. **Size: S.**

---

### Card F-REST-IO-SCHEME-1 (fork) — a scheme-dispatching storage factory

**Why.** G-2. `StorageConfig` carries only props (`crates/iceberg/src/io/storage/config/mod.rs:67-70`)
and `RestCatalog` holds one factory (`crates/catalog/rest/src/catalog.rs:465-473`), so a REST
catalog cannot serve tables in two schemes and RePark cannot infer the scheme from a logical
warehouse. This is the card that retires REST-0's `repark.rest.storage-scheme` knob.

**Decisions pre-made.** Add the table's storage path to `StorageConfig` (a `scheme` accessor
derived from the location the FileIO was built for) and a `MultiSchemeStorageFactory` in the fork
that dispatches to the registered per-scheme factory. Additive only; existing factories unchanged.
Do **not** change the `StorageFactory` trait signature — that is a workspace-wide breaking change.

**Rounds.** 3 (I, I, M) + an RP-N repin. **Size: M.**

---

### Card F-REST-CREDREFRESH-1 (fork) — vended-credential expiry and refresh

**Why.** G-3's sharpest edge and the fork's own named R160 residue: the OpenDAL FileIO takes a
static props snapshot, and neither `s3.session-token-expires-at-ms` nor Polaris's
`refresh-credentials-endpoint` is parsed. Any operation outliving one STS token fails.

**Decisions pre-made.** Parse the expiry keys; implement the Iceberg REST credential-refresh
protocol against `refresh-credentials-endpoint` when the server advertises it, and fall back to a
`load_table` re-vend when it does not; rebuild the table FileIO's credential props behind the
existing storage-config seam. Per-path credential selection (R160 divergence (a)(b)) is a
**separate** card — do not fold it in.

**Rounds.** 4-5 + an RP-N repin. **Size: L.**

---

### Card F-REST-SIGV4-1 (fork) — SigV4 signing and remote signing · **deferred**

G-11. Neither vendor's documented Spark configuration needs it. Open it only when an AWS-fronted
REST endpoint or a Lakekeeper deployment with STS off is a real requirement. **Size: L.**

---

### Release placement

The roadmap **already homes this work**: row **1.13 — "Multi-writer Iceberg + REST catalog
first-class"**, "Lift the single-writer-per-table rule (OCC retry policy, serializable isolation
done right); REST alongside Glue / S3 Tables", placed deliberately *before* the 2.0 freeze
([release-roadmap-2026-08-29.md:275](../epic-term/release-roadmap-2026-08-29.md)). 1.5 is the
facade sequence + CFG-2 + the silver compiler (`:267`, rulings `:349-351`), and the
database-discovery product is 2.3 (`:292`, ruling `:349`).

**The recommendation is to split 1.13 rather than move it.** REST-0, REST-1 and REST-2 are *not*
multi-writer work — they are catalog plumbing over a client that already exists, and they are what
turns "RePark supports two AWS catalogs" into "RePark supports the open standard". They belong in
the **first tag after 1.5** (1.6 under the current numbering), for three reasons: (a) the silver
compiler chartered into 1.5 ([deterministic-silver-layer-compiler-2026-09-12.md](../epic-term/deterministic-silver-layer-compiler-2026-09-12.md))
is precisely the workload an organization runs against a governed catalog, and shipping it with
AWS-only catalog support narrows its audience to AWS; (b) CFG-2 named sources
(`release-roadmap-2026-08-29.md:267`) touches the same `repark.toml` loader that G-9 needs, so the
flattener is cheaper done alongside it; (c) REST-0…2 carry no standing-rule change, so they do not
need to precede the freeze for the reason 1.13 does. **REST-3 and REST-4 stay at 1.13**, where the
concurrency battery belongs beside the multi-writer work it shares a contract with, and where a
CI Docker fixture can be introduced once rather than twice. The fork cards run whenever their lane
is free; F-REST-NSPROPS-1 is small enough to ride the next repin.

---

## 6. Decisions for the owner

**REST-D-1 — Which server is the first CI fixture?**
*Recommend **Lakekeeper**.* Its container needs only Postgres and MinIO, its warehouse is created
over its own management API in one bootstrap call, and its docs state credential vending works for
every storage type it supports (https://docs.lakekeeper.io/docs/latest/storage/). Polaris
additionally needs a bootstrap principal and a catalog created before Spark keys exist, and its
own quickstart carries AWS-shaped settings (`client.region`) that add noise to a MinIO fixture.
Polaris joins as a second compose profile in REST-3 step 4, and the fork's existing
`apache/iceberg-rest-fixture:1.10.0` (`dev/docker-compose.yaml:83`) is available as a free
third conformance leg.

**REST-D-2 — Are vended credentials in the first slice?**
*Recommend **yes, as the only supported mode for S3, with expiry named as a known issue**.* The
mechanism already works end to end (G-3) and costs REST-2 nothing but pins; the alternative —
static keys in the Spark config — is the posture both vendors' docs steer users away from, and
supporting it first would make the vended path the untested one. What is explicitly **deferred**
is refresh (F-REST-CREDREFRESH-1) and non-S3 vending, both documented as refusals rather than
silent failures.

**REST-D-3 — Does RePark inject `X-Iceberg-Access-Delegation: vended-credentials` by default?**
*Recommend **no** for the first slice.* Both vendors document it as an explicit user-set
`header.*` config
(https://docs.lakekeeper.io/docs/latest/engines/ , https://polaris.incubator.apache.org/releases/1.2.0/getting-started/using-polaris/),
Java's `RESTCatalog` does not inject it, and injecting it would change what a server returns
without the user asking. Revisit after REST-3 measures how often a user forgets it; a **loud hint**
in the error when an S3 table load produces no credentials and no static keys were configured is
the better ergonomics fix, and belongs in REST-2.

**REST-D-4 — Are `catalog-impl` Spark spellings honoured verbatim?**
*Recommend **yes, by the existing suffix rule**.* `kind_from_catalog_impl` already matches on class
suffix (`catalog_config.rs:204-215`), so `org.apache.iceberg.rest.RESTCatalog` and any vendor
subclass ending in `RESTCatalog` both resolve. This is the rule that lets an existing Spark config
be pasted unchanged, which is PROJECT.md's migration thesis. `io-impl` stays accepted-and-dropped
(`:195`, documented at `docs/guide/iceberg-guide.md:82`).

**REST-D-5 — SigV4 for AWS-fronted REST endpoints: in or out?**
*Recommend **out**, deferred to F-REST-SIGV4-1 with no date.* Nothing in the fork signs requests
(grep: zero `sigv4` hits), neither target server's documented configuration uses it, and RePark
already reaches AWS catalogs natively through Glue and S3 Tables. Reopen when a concrete
AWS-fronted REST endpoint is in front of the owner.

**REST-D-6 — How is the storage backend chosen for a logical warehouse?**
*Recommend **an explicit `repark.rest.storage-scheme` key defaulting to `s3`** (REST-0 D-4), with
F-REST-IO-SCHEME-1 retiring it later.* The alternative — inferring from the first table's location
— means the catalog cannot be built until a table is loaded, which breaks `SHOW NAMESPACES` on an
empty catalog. Guessing from the warehouse string is impossible: Polaris's warehouse is a catalog
name.

**REST-D-7 — Does the sixth `[patch.crates-io]` line need a separate ruling?**
*Recommend **no — it is one line at the identical rev, not a new pin**.* The invariant the block
carries is one rev across the `iceberg*` family (`Cargo.toml:148-158`); a sixth line at
`9e3522e3` preserves it, and REST-0 step 1 adds a pin that all six lines agree. Every future RP-N
repin then edits six lines.

**REST-D-8 — Which release?**
*Recommend the split in §5: REST-0/1/2 into the first tag after 1.5; REST-3/4 stay at roadmap
1.13 beside multi-writer.* The owner may instead keep all five at 1.13, at the cost of shipping
the silver compiler with AWS-only catalog support.

---

## 7. Measurements taken

All run on the two scratch clones on 2026-09-12. No RePark build or test suite was run, no Docker
was started, no JVM was used, and no tracked file outside this document was touched.

| # | Measurement | Result |
|---|---|---|
| M-1 | `cargo tree -p iceberg-catalog-rest --depth 1 --offline` in the fork | Direct deps: `async-trait 0.1.91`, `chrono 0.4.45`, `http 1.5.0`, `iceberg 0.9.1` (path), `itertools 0.13.0`, `reqwest 0.12.28`, `serde 1.0.229`, `serde_derive 1.0.229`, `serde_json 1.0.151`, `tokio 1.53.1`, `tracing 0.1.44`, `typed-builder 0.20.1`, `uuid 1.24.0`. Dev: `iceberg_test_utils` (path), `mockito 1.7.2`, `tokio`. |
| M-2 | Cross-check of M-1 against RePark's `Cargo.lock` | **Every one of the thirteen direct dependencies is already resolved in RePark at exactly that version** — `reqwest 0.12.28` (`Cargo.lock:5218-5220`), `typed-builder 0.20.1` (`:6368-6369`), `itertools 0.13.0` (`:3593-3594`, one of three coexisting majors), `http 1.5.0`, `chrono`, `serde`, `serde_json`, `tokio`, `tracing`, `uuid`, `async-trait`. `iceberg-catalog-rest` itself is **absent** from the lock. **Dependency delta of adopting the crate: zero new crate versions.** `mockito` is not in RePark's lock and would be one new dev-dependency (plus its own tree — UNMEASURED; settled by `cargo tree -p repark-iceberg --edges dev` after REST-0 step 3). |
| M-3 | TLS / reqwest feature resolution in RePark | reqwest already resolves with `hyper-rustls 0.27.9` (`Cargo.lock:3221-3224`), `rustls 0.23.43` (`:5323-5326`), `rustls-native-certs 0.8.4` (`:5338-5341`), `webpki-roots 1.0.9` (`:6636-6639`), `tokio-rustls`, `quinn` (`:5222-5255`). The fork's workspace spec is `reqwest = { version = "0.12.12", default-features = false, features = ["json"] }` (fork `Cargo.toml:113`) — **no TLS feature of its own**; TLS arrives by feature unification from `iceberg`, `iceberg-storage-opendal`, `object_store` and `opendal`, which are the four reqwest consumers in the lock (`Cargo.lock:3335, 3415, 4148, 4201`). **No conflict with RePark's rustls choice, and HTTPS to a Lakekeeper/Polaris endpoint works without adding a feature.** Caveat: that TLS is *unification-derived*, so a future dependency drop could silently remove it — worth a pin. |
| M-4 | `cargo build -p iceberg-catalog-rest --offline` in the fork, cold target dir, 64 cores | **44.69 s wall** (`Finished dev profile in 44.69s`), 226.8 s user + 53.7 s system, 626 % CPU, peak RSS 1.60 GB, 6.83 GB written. Exit 0. Cheap. |
| M-5 | Grep for `sigv4` / `SigV4` across the whole fork (`--include=*.rs --include=*.md`) | Zero hits. |
| M-6 | Grep for `access-delegation` / `AccessDelegation` across the whole fork | Zero hits. The header is reachable only as a user-supplied `header.*` prop. |
| M-7 | Grep for `refresh-credentials` / `credentials-endpoint` / `expires-at-ms` / `session-token-expires` across the fork's `crates/` | Zero hits. Confirms the R160 expiry residue. |
| M-8 | Grep for `TODO` / `unimplemented` / `not supported` / `FeatureUnsupported` in `crates/catalog/rest/src/*.rs` | One hit: `catalog.rs:781-782`, `"Updating namespace not supported yet!"`. |
| M-9 | Grep for `iceberg-catalog-rest` in RePark's `Cargo.toml` and `Cargo.lock` | Absent from both. |
| M-10 | Line counts of the fork's REST crate | `catalog.rs` 4 970 (impl 1-1 551, `#[cfg(test)] mod tests` from 1 553), `client.rs` 1 483, `types.rs` 1 128, `lib.rs` 59, `tests/rest_catalog_test.rs` 497 — 8 137 total. |
| M-11 | Free disk on `/` before the build | 470 G available of 1.8 T (73 % used). Headroom adequate for a fork lane. |

**UNMEASURED, with the measurement named.**

1. **Vended-credential TTL under a real server.** How long a Polaris or Lakekeeper STS credential
   lasts and whether a `rewrite_data_files` over a realistic table outlives it. *Measurement:*
   REST-3's fixture with the server's credential TTL lowered, timing a compaction — or the
   owner-gated live tier.
2. **Whether either server ever vends a `prefix` that does not cover `metadata_location`.**
   *Measurement:* log the `storage-credentials` array in REST-3's read leg on both servers.
3. **Lakekeeper's project-selection mechanism for a multi-project deployment.** The docs fetched
   (https://docs.lakekeeper.io/docs/latest/concepts/ , /authentication/) state projects exist and
   that single-project is the recommended default, but do not document a client header; the fork's
   `header.*` support would carry one if required. *Measurement:* a request to a two-project
   Lakekeeper in REST-3, or the server's OpenAPI document.
4. **`mockito`'s own dependency tree against RePark's lock.** *Measurement:*
   `cargo tree -p repark-iceberg --edges dev` after REST-0 step 3.
5. **Whether `SHOW NAMESPACES IN <cat>` on a nested-namespace server is empty or top-level-only in
   practice.** *Measurement:* REST-3 step 2 against a Lakekeeper warehouse with `a.b.c` namespaces.

---

## See also

- [cheap-tier-slate-2-2026-09-09.md](cheap-tier-slate-2-2026-09-09.md) — the card format, the tier
  legend and the worker rules this slate follows.
- [roadmap-intake-2026-08-23.md](roadmap-intake-2026-08-23.md) — the intake shape.
- [iceberg-rust-handoff-2026-08-23.md](iceberg-rust-handoff-2026-08-23.md) — the fork-side handoff
  register the F-REST-* cards join.
- [../epic-term/release-roadmap-2026-08-29.md](../epic-term/release-roadmap-2026-08-29.md) — row
  1.13 is where REST is homed today.
- [../../../docs/guide/iceberg-guide.md](../../../docs/guide/iceberg-guide.md) §Catalogs and
  [../../../docs/guide/repark-toml.md](../../../docs/guide/repark-toml.md) — the two surfaces every
  card edits.

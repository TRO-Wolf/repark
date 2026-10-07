# map — repark-connect

## Purpose

Database connectivity for repark (crate-DAG **tier 1**, role `table service`, peer of
`repark-iceberg`; card 1.6 of
[the design plan](../../task/roadmap/epic-term/roadmap-design-plan-2026-08-29.md), layout of record
[crate-layout-1-8-2026-10-01.md](../../task/roadmap/epic-term/crate-layout-1-8-2026-10-01.md) §1).
C-1 (2026-10-05) created it on its pre-declared footprint (CL-8): the workspace member, this map,
the `repo-manifest.toml` row flipped `planned` → `delivered`, and the one edge
`repark-connect → repark-common` (`normal`) that `scripts/check_crate_dag.py` pre-declared.
The `repark-core → repark-connect` edge arrives with C-2, its first caller (the providers mount).
C-2 also restores its policy row: R-14 took that row out of `ALLOWED_EDGES`, because the DAG
gate's drift rule reds a declared edge with no dependency behind it once both crates are
members. C-1 lands no provider, no read path, no pool and no driver dependency.
pins: c-1/C-001

C-2 (sketch [c-2-design.md](../../task/wo/c-2-design.md), four slices) makes it the Postgres read
path. **C-2a (2026-10-06)** is the pure half, with no driver and no network: the crate's one error
enum `ConnectError` (NS-15, C-1's two enums folded in), the COPY BINARY decoder
`CopyBinaryDecoder`, and the type map's eight newly mapped rows with their codecs, resolution and
per-value refusals. C-2b adds the driver behind the `postgres` feature (settings keys, TLS, the
query pool, the COPY read), C-2c the provider, pushdown and EXPLAIN, C-2d the mount in
`repark-core`. C-2b round 1a (2026-10-07) is the feature and the lockfile only. C-2b round 1b
(2026-10-07) adds the Postgres settings keys (`settings/postgres.rs`) and the quoted identifier
types (`ident.rs`), both pure. pins: c-2/C-013, C-026, C-027

C-2b round 2 (2026-10-07) adds the `verify-full` TLS config (`tls.rs`) and the query pool with
its Postgres connector, the session pins and the connect timeout (`pool.rs`), both behind
`postgres`. pins: c-2/C-030, C-037

C-2b round 3 (2026-10-07) adds the read path: relation and query discovery with the server
floor (`discover.rs`), and the COPY statement, the `set_config` carriage and the COPY stream
under the read timeout (`read/postgres.rs`), both behind `postgres`, with sketch §5.6's live
crash matrix under `make pg-up`. pins: c-2/C-046, C-048

## Contents

- `Cargo.toml` — workspace inheritance for edition, version, license, rust-version, repository,
  publish and lints (the `repark-iceberg` precedent, so `unsafe_code = "forbid"` rides the
  workspace). Dependencies: `repark-common`, `datafusion` (the provider traits,
  unconditional), `arrow`, `thiserror`, all at workspace versions. The `postgres` feature
  (default on, the `repark-distributed` `default = ["local"]` precedent) pulls the six
  optional driver deps (`tokio-postgres 0.7`, `tokio-postgres-rustls 0.13`,
  `rustls-native-certs 0.8`, `tokio`, `futures`, `bytes`) and, since C-2b round 2, `rustls`
  0.23 with `ring`, `std` and `tls12` and no default features, so the TLS config names its one
  crypto provider (H-CRYPTO); `tokio` names `net`, `rt`, `sync` and `time`.
  `--no-default-features` keeps the pure C-2a half with no driver. `sqlx` and `tiberius` wait for the read and write paths
  (C-4 onward). The `repark-core → repark-connect` edge is still absent; C-2d adds it with
  the mount. pins: c-1/C-011 · pins: c-2/C-014
- `src/` — [src/map.md](src/map.md): `error.rs` (`ConnectError`), `copy_binary.rs` (the COPY
  BINARY decoder), `settings.rs` (the connection settings and the reserved auth-method field),
  `settings/postgres.rs` (the Postgres endpoint keys), `ident.rs` (`PgIdent`,
  `QualifiedRelation`), `tls.rs` (the `verify-full` rustls config), `pool.rs` (`QueryPool`,
  `PooledClient`, `PostgresConnector`), `discover.rs` (relation and query discovery),
  `read/postgres.rs` (the COPY statement, the `set_config` carriage, the stream) and
  `types/postgres.rs` (the Postgres ↔ Arrow type map, codecs under `types/postgres/`).
- `tests/` — [tests/map.md](tests/map.md): the one integration binary `tests/it/main.rs`.

## Design bars (R-10)

The 1.6 release row's standing instruction (owner, 2026-09-13) asks every unit to name which of
the external bars shaped it. **Arrow ADBC** shaped C-1's type map: the Arrow side of each mapped
row is the type the ADBC PostgreSQL driver's documented type-mapping table names for that
Postgres type (a documented basis, not a run of the driver; C-2's live cells against the C-0
container are the first measurement), and a row where ADBC and another reading disagree (or where Postgres has values the Arrow type cannot hold) is
declared, not chosen. **ConnectorX** shapes the partitioned reads; nothing in C-1 reads, so it is
recorded as not yet (C-2, C-3). pins: c-1/C-010

**C-2a.** ConnectorX shaped the read path: its PostgreSQL source reads `COPY (query) TO STDOUT
WITH BINARY` over the rust-postgres family into typed Arrow destination columns. C-2a keeps the
protocol and the typed destination, and replaces ConnectorX's per-row parse through the driver's
`BinaryCopyOutRow` with per-column appenders over the raw stream, so no value allocates. ADBC
keeps the ten C-1 rows; for `numeric`, `interval` and `uuid`, ADBC's documented Arrow types
(string, month-day-nano, fixed-size binary) give way to the types Spark's dialect surfaces,
because Spark governs the surface (North Star §2). Both bars are documents, not runs; the full
citations are in the [C-2 ledger](../../task/ledgers/staging/c-2-ledger.md) §4. pins: c-2/C-013

## I want to...

| ...do this | go to |
|---|---|
| Interpret a new connection key | `src/settings.rs`, in the unit that first consumes it (CC-3: no key is defined ahead of its unit) |
| Map a declared Postgres type | `src/types/postgres.rs`: flip the row, add its `ColumnAppender` arm (and a codec under `src/types/postgres/`), a pin named in the row, an oracle cell, and retire its registry row |
| Decode a COPY BINARY stream | `src/copy_binary.rs`: `CopyBinaryDecoder::new(planned columns, BatchLimits)`, then `decode(&mut chunk)` per chunk and `finish()` at end of stream |
| Add a decoder error | `src/error.rs`: a `ConnectError` variant with an enum reason, its class in the `repark_common::Error` fold, and a pin |
| Connect to a Postgres source | `src/pool.rs`: `QueryPool::new(PostgresConnector::new(&settings)?, PoolLimits::from_settings(&settings))`, then `checkout()`; call `release_clean()` only after the COPY trailer and `COMMIT`, and let any other path drop the lease |
| Bound a network wait | `src/pool.rs`: `within(TimeoutSetting::…, limit, work)` gives `Timeout { which }` |
| Resolve a Postgres relation or query | `src/discover.rs`: `discover(&pool, &ScanSource, read_timeout)` gives a `ResolvedSource` (columns, casts, nullability, collations) |
| Read a resolved source | `src/read/postgres.rs`: `scan(pool, ScanRequest::new(resolved), ScanOptions::from_settings(&settings))`; push a value with `compare`, never by editing SQL text |
| Add the SQL Server map | `src/types/mssql.rs` beside `postgres.rs` (C-5) |

## Component contract

- **Owns:** connection settings (`ConnectionSettings`, `AuthMethod`), database-specific type
  conversion (`postgres::POSTGRES_TYPES`, its wire codec and `PlannedColumn` resolution), the
  COPY BINARY decoder (`CopyBinaryDecoder`), the crate's error enum (`ConnectError`), the
  `verify-full` TLS config and the per-source query pool (`QueryPool`, behind `postgres`). CC-2's split puts the source
  identity in `repark-common`, loading and the user-facing handles in `repark-core`.
- **Does not own:** profile loading, precedence, registration (`repark-core`); the source
  identity (`repark-common::SourceIdentity`); lineage, offsets and capture state (`repark-cdc`,
  1.7); endpoint keys (C-2, CC-3).
- **Public inputs:** one source's prop map (`BTreeMap<String, String>`, as the core loader
  produces it); Arrow arrays and Postgres binary wire values; a `PostgresSettings` for the
  pool and its connector.
- **Public outputs:** `ConnectionSettings`; `PostgresTypeRow::encode` / `decode`;
  `PlannedColumn`; Arrow `RecordBatch`es from `CopyBinaryDecoder` and from `scan`'s stream;
  `ResolvedSource` from `discover`; or a `ConnectError`, which
  folds into `repark_common::Error` by class: `InvalidAuthMethod` → `Config` (IllegalArgument
  class, CC-4's invalid specification), the declared refusals → `NotImplemented` (Unsupported
  class), the operational errors → `DataFusion`.
- **State & lifecycle:** value types, one const table, and one decoder per scan whose only state
  is its position in the stream, the batch being built and one carry buffer. One `QueryPool` per
  mounted source (C-2c creates it on the first scan): permits, idle connections, and one driver
  task per connection that the pool owns and a dropped lease aborts. It never builds a
  replication connection (CC-2).
- **Allowed internal deps:** `repark-common` only (declared `normal` in the DAG gate).
- **Failure model:** one typed `thiserror` enum with enum reasons; a value the Arrow type cannot
  hold refuses per value with its registry row, never approximated; no panics in product code.
- **Extension points:** new auth methods (flip a declared refusal), new type rows, new backends.
- **Test strategy:** `tests/it/` pins every settings branch, one round trip or byte-anchor pin
  per mapped type, the decoder's stream contract split at every byte offset, the TLS config by
  in-memory handshakes, the pool and connector against fakes and loopback listeners, and the
  read path's crash matrix live against `make pg-up`.
- **Known limitations:** the declared rows (registry `CONNECT-DECL-*`); no DataFusion provider,
  pushdown classifier or memory reservation yet (C-2c); the error variants carry no source name
  until C-2d.

## Pointers

- Up: [../map.md](../map.md)
- Ledgers: [c-1-ledger.md](../../task/ledgers/staging/c-1-ledger.md),
  [c-2-ledger.md](../../task/ledgers/staging/c-2-ledger.md)
- Registry rows: [docs/spark-sql-iceberg-parity.md](../../docs/spark-sql-iceberg-parity.md) §5,
  the `CONNECT-DECL-*` family beside `SES-DECL`. pins: c-1/C-009

## Debug

| Symptom | First check |
|---|---|
| `crate-dag` red on a new edge | the edge must be pre-declared in `scripts/check_crate_dag.py`; a tier-1 service never reaches up to `repark-core` |
| A row's pin is "not a test here" | `type_map_has_one_row_per_type_and_a_live_pin_per_row`: the row's `pin` must name a `fn` in `tests/it/postgres_types.rs` |
| A scan fails with `Protocol` | the planned columns disagree with the stream: the field count, a NULL in a NOT NULL column, or a codec's wire check (`ProtocolViolation` names which) |
| A batch differs with chunking | `copy_decode_is_independent_of_chunking`: a field or fixed word that straddles a chunk must go through the carry buffer |
| `TlsRequired` against a server with TLS | the server answered the SSLRequest with `N`; check its `ssl = on`. `sslmode=disable` is the only plaintext switch |
| `TlsHandshake { UntrustedCertificate }` | the server's chain does not reach the system roots or `sslrootcert`; point `sslrootcert` at the CA bundle |
| `PoolExhausted` | every permit is leased: a stream held unpolled, or `pool_max_size` too small for the concurrency |

First checks: `cargo test -p repark-connect`. Escalate to: [../map.md#debug](../map.md).

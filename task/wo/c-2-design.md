# C-2 — design sketch: the Postgres read path (COPY BINARY → Arrow, Exact pushdown, the EXPLAIN boundary, mounted providers) · 1.6

**Date:** 2026-10-06 · **Author:** Claude Opus 5.5 (`claude-opus-5-5`, high), the design executor named by the
owner's 2026-10-05 ruling for 1.6 work · **Branch:** `docs/c-2-design-sketch` on `043b7441` · **Order:**
[c-2-postgres-read.md](c-2-postgres-read.md) (grade-B skeleton; this sketch closes its PENDING items: R-5's key
list, §2's files, §4 step 1, §5's subset table) · **Card:** 1.6 of
[the design plan](../roadmap/epic-term/roadmap-design-plan-2026-08-29.md) · **Contracts:** CC-1…CC-6 and ES-1 of
[contracts-ahead-of-code](../roadmap/epic-term/contracts-ahead-of-code-2026-10-01.md) · **Standing defaults:**
[the North Star](../roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md) (NS-1…NS-19).

**H-C1 is clear.** C-1's H-GEN is ruled (R-12 as amended, then the C-1b `Generation` newtype under NS-14) and
H-AUTH is ruled (R-13); both are on `main` ([c-1-ledger.md](../ledgers/staging/c-1-ledger.md) C-002, C-012).

**Citations.** "docs §N" means the PostgreSQL 16 manual, the version the C-0 container runs. Sections that
move between versions are cited by name.

**Nothing here halts.** Every unruled question below is acted on under the North Star's four-line record (§3).
Six are also filed for the owner (§9), none under §8. No measurement ran: a timing run held the box, so every
number this sketch needs is a deferred measurement (§8).

## 0. The North Star §7 checklist, line by line

| # | line | answer |
|---|---|---|
| 1 | **the guarantee kept** | A scan returns the rows of **one Postgres statement snapshot**. Every value is either decoded exactly or refused, per value, with a named registry row; nothing is approximated. The one rounding is unconstrained `numeric` → `Decimal128(38,18)`, which is Spark's and Flink's own behaviour (FL-2, Q3). A predicate classed `Exact` returns the same rows the engine computes when nothing is pushed. Every pushdown cell pins this differentially: rows with `pushdown_predicate = true` equal rows with `false` (§5.2). C-2 has no commit path, so NS-1's triad has nothing to bind: no sink, no offset, no epoch. The read path is read-only by construction, because every pooled connection starts with `default_transaction_read_only = on`. |
| 2 | **the crash matrix** | The five commit-path points (crash before staging, after staging before commit, after commit, during a retry, two drivers) are **N/A**, each for the same reason: C-2 stages and commits nothing. The read path has its own matrix, F-1…F-8 (§5.6), live in the C-0 container: backend killed mid-COPY, stream dropped mid-COPY, idle read timeout, lock timeout, pool exhaustion, truncated stream, schema drift between planning and scan, and the scan run twice. *What if it runs twice:* a scan is one read-only transaction whose settings are transaction-local. Run twice it returns the same rows, up to concurrent writes on the server, and leaves nothing behind. |
| 3 | **where every bit of state lives** | Nothing is durable, so NS-2 holds trivially. The parsed settings are memoised in memory on first resolution, per mounted source, for the session. The query pool is in memory per mounted source and is dropped with the session. The table schema is resolved per statement and never cached (FL-7). Nothing goes into a catalog, snapshot summary, table property, checkpoint file or second table. |
| 4 | **the fencing token** | None is needed and none is taken. `SourceIdentity.generation` (`Option<Generation>`, CC-2) stays `None`: C-2 neither reads nor assigns it, and 1.7 capture owns it (CC-9). If a source is re-pointed at another database, the next scan simply connects to the new endpoint; there is no checkpoint to confuse. |
| 5 | **the bounds** | **Pool:** `pool_max_size` connections (default 4, ceiling 64); checkout timeout 30 s; idle connections closed after 300 s. **Timeouts:** connect 10 s; an **idle read timeout of 60 s on every request and every COPY chunk**; server-side `lock_timeout` 10 s; `statement_timeout` from `query_timeout_ms` (Spark's `queryTimeout`, default unlimited, with the idle timeout still applying), and 30 s on discovery. **Memory:** one batch in flight per scan, flushed at `batch_rows` (the session batch size) or 64 MiB, whichever comes first, and charged to the DataFusion memory pool. Only a partial tuple carries over between chunks. **Pushdown:** at most 256 IN-list items and 1024 bound values per scan. **Partitions:** one per scan; C-3 parallelises. |
| 6 | **the credential surfaces** | `sources()` rows and their `repr`: the `password` key is masked by core's key predicate, and a password embedded in `url` is masked by a connect-owned redactor (it is printed verbatim on `main` today, §2.4). Every `ConnectError`: names the source and the key, never a value of `password` or `url`, never the DSN. Logs and tracing spans: source name and relation only. EXPLAIN: source name, relation, projection, pushed and residual filters, and no endpoint. `Debug` on every settings type: keys only (C-1's C-006 pin, extended). TLS is `verify-full` by default; `sslmode = disable` is the explicit plaintext option, with a dated row. One live cell greps every one of these surfaces for a per-test random password (§5.7). |
| 7 | **the Spark names used** | `spark.read.jdbc(…)` and `format("jdbc" \| "postgres")` (already on the facade, IO-JDBC-1). Options: `url`, `dbtable`, `query`, `user`, `password`, `driver`, `queryTimeout`, `fetchsize`, `pushDownPredicate`, `preferTimestampNTZ`, plus pgjdbc's `sslmode`, `sslrootcert`, `connectTimeout`, `socketTimeout` and `ApplicationName`. `partitionColumn` / `lowerBound` / `upperBound` / `numPartitions` / `predicates` refuse until C-3. Types follow Spark's PostgreSQL dialect (§2.7): bounded `DecimalType`, `DateType`, `TimestampType` / `TimestampNTZType`, and `StringType` for the text-rendered types. The EXPLAIN keys follow Spark's `PushedFilters` label (§2.10). |
| 8 | **the divergence rows filed** | Deliberate divergences, `CONNECT-DIV-*`: `pg-text-collation`, `pg-sslmode`, `pg-unknown-option`, `pg-timestamp-zone`. Declared refusals, `CONNECT-DECL-*`: `pg-numeric-special`, `pg-infinite-datetime`, `pg-out-of-range`, `pg-unmapped`, `pg-time` (kept from C-1), `pg-listing`, `pg-ddl`, `pg-partitioned-read`, `sslmode-unverified`, `pg-client-cert`, `pg-session-sql`, `pg-server-version`, `pg-multi-host`. Retired: eight of C-1's nine type rows. Full list in §4. |

## 1. What this sketch decides, and the order's PENDING items it closes

| order item | closed by |
|---|---|
| R-5 (endpoint keys, "PENDING on C-1") | §2.3: the key list, on C-1's `ConnectionSettings::from_props` shape and R-13's `auth_method` |
| §2 files ("PENDING on C-1: exact settings edits and any type-map extension") | §7: files and ceilings per slice |
| §4 step 1 (trait shape, subset table, pool shape, key list) | §2.5, §2.9, §2.11 and §2.3 |
| §5 ("PENDING on the sketch: the predicate-subset table") | §2.9's table; §5.2 has one pin per row |
| R-7 (ConnectorX / ADBC citations) | §10 |

## 2. The design

### 2.1 Crate, feature, dependency

- **Home.** Everything new lives in `crates/repark-connect`, except the mount (`repark-core`) and the Python
  door (`repark-python`). The modules are the card's: `provider/{catalog,schema,table}.rs`, `pushdown.rs`,
  `read/postgres.rs` and `pool.rs`. To keep each file under its ceiling, the sketch adds `error.rs`,
  `ident.rs`, `tls.rs`, `discover.rs`, `copy_binary.rs` and `provider/scan.rs`, plus `settings/postgres.rs`
  beside C-1's `settings.rs`.
- **Feature (CC-5, NS-12).** `repark-connect` gets `[features] default = ["postgres"]`, with `postgres`
  enabling the driver dependencies. This follows the `repark-distributed` precedent (`default = ["local"]`).
  `repark-core` forwards it as `postgres = ["repark-connect/postgres"]`, default on, so the standard wheel
  carries it. Built without the feature, a Postgres source mounts the refusing provider with the message "the
  Postgres connector is not compiled into this build" (CC-5: a compiled capability cannot be enabled at
  runtime). The COPY decoder, the type map and the pushdown classifier are pure and stay outside the feature,
  so they are tested without a driver.
- **Dependencies added in C-2b:**
  - `datafusion` (workspace 54.1, unconditional: the provider traits);
  - `tokio` (workspace);
  - `tokio-postgres` 0.7;
  - `tokio-postgres-rustls` (over the `rustls` 0.23 already in the tree through `aws-config`);
  - `rustls-native-certs` 0.8 (already in the tree);
  - `futures`;
  - `bytes`.

  There is no `sqlx`, no OpenSSL, no `native-tls`, no JVM and no ODBC.
- **Why `tokio-postgres` and not `sqlx`** (card 1.6: "`sqlx` only if needed"; it is not needed):
  1. `Client::copy_out` hands back the raw COPY payload as a stream of `Bytes`. C-2 decodes it straight into
     Arrow builders. COPY BINARY over the rust-postgres family is ConnectorX's own Postgres protocol, so C-3's
     benchmark compares like with like.
  2. `Config` exposes everything §2.5 needs: `connect_timeout`, `keepalives`, startup `options` (the session
     pins, §2.4), `application_name` and `channel_binding`. `CancelToken` and `Client::is_closed` give
     cancellation and pool health.
  3. 1.7 capture speaks the logical replication protocol. The Rust capture precedent (Supabase ETL) is built on
     the `tokio-postgres` family, and `sqlx` has no replication protocol. One driver family keeps the
     dependency tree single across query and capture, even though the two never share connections (§2.5).
  4. `sqlx` adds compile-time query macros, its own pool and runtime feature matrix, and a larger tree. C-2
     uses none of it.
- **`cargo-deny` posture (ES-9).** Expected licences, by the crates' declared metadata:
  - MIT OR Apache-2.0 for `tokio-postgres`, `postgres-protocol`, `postgres-types` and their RustCrypto
    hashing dependencies;
  - MIT for `tokio-postgres-rustls`;
  - MIT among the options for `whoami`.

  All of these sit inside `deny.toml`'s allow list. **The build slice measures it** (D-M1:
  `cargo deny check`, `cargo tree -d`, `cargo tree -e features -i rustls`); the sketch claims no result. The
  rustls crypto provider must be the one the tree already builds, so no second crypto backend lands. A licence
  outside the list, a new advisory, a duplicate that `bans` denies, or a second crypto provider is a halt
  (H-DENY, H-CRYPTO). `deny.toml` is never edited to make room.

### 2.2 Errors, identifiers, `unsafe`

- **One error enum (NS-15).** C-2a adds `error.rs` with `pub enum ConnectError` (`thiserror`) and
  `pub type Result<T> = std::result::Result<T, ConnectError>`, re-exported at the crate root. C-1's
  `SettingsError` and `TypeMapError` fold into it: their six variants keep their names and their message text,
  so the registry rows only rename the type. The variants and their CC-4 meanings:

  | variant | meaning (CC-4) |
  |---|---|
  | `InvalidSpecification { source, key, reason }` | invalid specification |
  | `DeclaredAuthMethod`, `Declared { row }` | a declared refusal |
  | `AuthenticationFailed { source }` | authentication failure |
  | `TlsRequired { source }`, `TlsHandshake { source, kind }` | TLS refused or failed |
  | `Unreachable { source, kind }`, `Timeout { source, which }`, `Disconnected { source }`, `PoolExhausted { source, waited }` | retryable disconnect |
  | `PermissionDenied { source, relation, privilege }` | the missing grant, named |
  | `RelationNotFound { source, relation }` | — |
  | `UnmappedType { source, column, postgres_type, row }` | a declared type |
  | `UnrepresentableValue { source, column, postgres_type, reason, row }` | a declared value |
  | `SchemaChanged { source, relation, column }` | — |
  | `Protocol { source, reason }` | a malformed COPY stream |
  | `Server { source, sqlstate, message }` | a server error |

  - The `kind`, `which` and `reason` fields are enums, never strings (NS-15): a caller matches the variant
    and its enum fields.
  - `Server.message` is the server's text. It never carries a credential, because no credential ever enters a
    statement.
- **Folding.** Errors fold into `repark_common::Error` by class:
  - `InvalidSpecification` → `Config` (IllegalArgument);
  - the declared variants → `NotImplemented` (Unsupported);
  - everything operational travels as `DataFusionError::External(Box<ConnectError>)` through the engine. It
    stays downcastable in Rust and renders as the `DataFusion` class at the Python boundary until C-6 lands
    the CC-4 classes and their registry rows (card 1.6: "C-6 — … the CC-4 registry rows for the
    RePark-owned conditions").
- **Newtypes (NS-14):**
  - `PgIdent(String)`: built only by `PgIdent::new`, which refuses an empty name, a NUL byte, or more than 63
    bytes (Postgres truncates identifiers longer than `NAMEDATALEN - 1` with a notice, so two names could
    collide; docs "Identifiers and Key Words"). It renders only as a double-quoted identifier with embedded
    `"` doubled (same section).
  - `QualifiedRelation { schema: PgIdent, table: PgIdent }`.
  - `TypeOid(u32)`, `TypeMod(i32)` and `AttNum(i16)`, the catalog columns' own widths.
  - `ParamSlot(u16)`, the `repark.pN` name of §2.4.

  No bare `u32` or `String` identifier crosses a function boundary.
- **`#![forbid(unsafe_code)]` (NS-17).** This rides the workspace lints already (C-1's R-11). C-2 adds no
  `unsafe` and no exception.

### 2.3 Settings: the endpoint key list (R-5, CC-3)

C-1's `ConnectionSettings::from_props` keeps its shape. It interprets `auth_method` (R-13), and C-2 adds the
keys below. CC-3 puts interpretation in the owner crate: core transports the prop map and interprets nothing.
**Canonical spellings are snake_case**, following `auto_register` and `auth_method` (R-5, R-13), and are exact
and case-sensitive in `repark.toml`. **Aliases** are the Spark JDBC option names and the pgjdbc property names;
they are matched case-insensitively, as Spark's `CaseInsensitiveMap` does, and are accepted only on the
`read_postgres` door and inside a `jdbc:` URL's query string. Giving one setting twice, in the URL and as a
key, or under two spellings, refuses as an invalid specification naming both spellings.

| canonical key | aliases | default | meaning |
|---|---|---|---|
| `url` | `url` | — | `postgresql://`, `postgres://` (libpq URI) or `jdbc:postgresql://host[:port]/db?k=v&…` (pgjdbc). It may carry host, port, database, user, password and pgjdbc properties. |
| `host` | — | — | One host. Required unless `url` names one. A comma list (libpq multi-host) refuses: `CONNECT-DECL-pg-multi-host`. |
| `port` | — | `5432` | `1..=65535` |
| `database` | — | the user name | libpq's default |
| `user` | `user` | — | required |
| `password` | `password` | absent | Password auth (ES-1); the server picks cleartext, md5 or SCRAM-SHA-256, with channel binding preferred. A `${VAR}` reference is resolved by the CFG-1 loader before connect sees it. |
| `auth_method` | — | `password` | C-1, R-13; `iam_token` and `kerberos` stay declared refusals (ES-1) |
| `sslmode` | `sslmode` | `verify-full` | §2.4 |
| `sslrootcert` | `sslrootcert` | the system roots | Path to a PEM CA bundle, read on the first connect |
| `connect_timeout_ms` | `connectTimeout` (seconds) | `10000` | TCP connect plus the TLS and auth handshake |
| `read_timeout_ms` | `socketTimeout` (seconds) | `60000` | Idle read: the longest wait for any response or COPY chunk. `0` refuses, since NS-7 needs a timeout on every network call. |
| `query_timeout_ms` | `queryTimeout` (seconds; Spark's) | `0` = unlimited | Server `statement_timeout` for the scan |
| `lock_timeout_ms` | — | `10000` | Server `lock_timeout` |
| `batch_rows` | `fetchsize` | the session batch size | Rows per Arrow batch: the closest meaning to Spark's rows-per-round-trip |
| `prefer_timestamp_ntz` | `preferTimestampNTZ` | `false` | §2.7 `timestamp` |
| `pushdown_predicate` | `pushDownPredicate` | `true` | `false` classes every filter as residual; the differential pins use it |
| `pool_max_size` | — | `4` | `1..=64` |
| `pool_checkout_timeout_ms` | — | `30000` | |
| `pool_idle_timeout_ms` | — | `300000` | |
| `application_name` | `ApplicationName` | `repark` | Shown in `pg_stat_activity`; the capture side will use a different name at 1.7 |

The following keys are **refused as declared**, each naming its row and fix:

| keys | registry row |
|---|---|
| `sslcert`, `sslkey` | `CONNECT-DECL-pg-client-cert` (certificate auth is not an ES-1 method) |
| `sessionInitStatement`, `customSchema`, `options` | `CONNECT-DECL-pg-session-sql`: arbitrary SQL at connect, a schema override, raw startup options that would override §2.4's pins |
| `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`, `predicates` | `CONNECT-DECL-pg-partitioned-read` (C-3), `read_postgres` only |

`driver` is accepted only as `org.postgresql.Driver` and has no effect (Spark scripts set it). **Any other key
refuses** as an invalid specification that names the key and lists the accepted keys, never a value (NS §5
`deny_unknown_fields`; FL-1, `CONNECT-DIV-pg-unknown-option`). Validation runs on the **first resolution** of
the source (FL-1) and is memoised, so the same refusal repeats. Session build stays I/O-free and
validation-free (CFG-2 D-4). `repark.connect.*` session defaults stay undefined (CC-3: no key until a unit
needs one; none of C-2's does).

### 2.4 Security

- **TLS by default.**

  | `sslmode` | behaviour |
  |---|---|
  | `verify-full` (default) | TLS with the certificate chain verified against the system roots (`rustls-native-certs`) plus `sslrootcert` when set, and the host name verified |
  | `disable` | The explicit plaintext option: `CONNECT-DIV-pg-sslmode`, dated 2026-10-06 (NS §5) |
  | `prefer`, `allow` | Refuse: they fall back to plaintext silently. Row `CONNECT-DECL-sslmode-unverified`. |
  | `require`, `verify-ca` | Refuse: encryption without a host-name check is open to a man in the middle. Same row; the fix it names is "`verify-full` with `sslrootcert` pointing at your CA bundle". |

  - A server that does not offer TLS under `verify-full` fails with `TlsRequired`, which names `sslmode` as
    the only switch.
  - Cited: PostgreSQL docs, libpq "SSL Support" (the protection each `sslmode` provides) and "Secure TCP/IP
    Connections with SSL".
- **Values are bound, inside COPY (FL-13).** A statement `COPY (query) TO STDOUT` takes no bind parameters.
  COPY is a utility statement, and Postgres refuses a Bind message that carries parameters for it. So C-2
  never splices a value into SQL text. Each pushed literal goes into a **transaction-local setting through a
  bound parameter**, and the COPY query reads it back with an explicit cast:

  ```
  BEGIN READ ONLY
  SELECT pg_catalog.set_config($1, $2, true), …            -- ($1, $2) = ('repark.p0', '<value text>'), …
  COPY (SELECT … WHERE "amount" > pg_catalog.current_setting('repark.p0')::pg_catalog.numeric) TO STDOUT (FORMAT BINARY)
  COMMIT
  ```

  - Only constants enter the SQL text: the setting names `repark.p0…p1023` and the cast type names, both
    generated by C-2 from its own tables.
  - Each value reaches the server's type input function as text. It is never parsed as SQL, so injection
    cannot happen. The text form is exact per type (§2.9 lists the renderings).
  - `current_setting` is `STABLE`, so the planner can use it as an index qual. Its value is unknown at plan
    time, so the planner estimates generically; D-M3 measures what that costs.
  - A scan with no pushed value skips the transaction and sends one statement; the
    `default_transaction_read_only` pin keeps it read-only.
  - Cited: docs, "Configuration Settings Functions" (`set_config` with `is_local`, `current_setting`) and
    "Function Volatility Categories" (`STABLE` expressions in index scans).
  - This keeps R-2 (COPY) and NS §5 (bound values) both true. The alternative, a vetted literal renderer, is
    Q2's.
- **Identifiers are quoted.** Every relation and column renders through `PgIdent` (§2.2). Every function,
  type and operator in generated SQL is `pg_catalog`-qualified or resolves with `search_path` empty (next item).
- **Session pins, in the startup packet.** One `-c` list in `Config::options` sets:
  `client_encoding=UTF8` (C-1's text decode needs it), `DateStyle=ISO`, `IntervalStyle=postgres`,
  `TimeZone=UTC`, `search_path=` (empty), `default_transaction_read_only=on`, `lock_timeout`,
  `statement_timeout`, `idle_in_transaction_session_timeout` (= `read_timeout_ms`) and `application_name`.
  - An empty `search_path` means only `pg_catalog` resolves unqualified names. A hostile schema cannot shadow
    an operator or a function (docs, "Schemas": "Usage Patterns").
  - Users cannot override these pins, because the `options` key is refused (§2.3).
- **Server floor.** PostgreSQL 14 or later: the community-supported set on 2026-10-06. It is read from
  `server_version_num` on connect. An older server refuses with `CONNECT-DECL-pg-server-version`. Two features
  below need the floor: `pg_collation.collisdeterministic` (PostgreSQL 12) and `numeric` infinities
  (PostgreSQL 14).
- **Least privilege.** A read needs `CONNECT` on the database, `USAGE` on the schema and `SELECT` on the
  relation. A missing grant is SQLSTATE `42501`, which becomes `PermissionDenied`, naming the relation and the
  privilege. C-2d adds the three `GRANT` lines to `docs/guide/repark-toml.md`.
- **Credential surfaces (NS §5).**
  - `ConnectionSettings` and every new settings type keep C-1's keys-only `Debug`.
  - `ConnectError` holds a source name and keys, never a value.
  - The driver's `Config` is never formatted.
  - `read_postgres` keeps its "never log URL or properties" rule.
  - **A leak on `main`:** `SourceRow::from_spec` masks by key only (`prop_key_is_secret`), so
    `url = "postgresql://u:secret@h/db"` prints `secret` in `sources()` today. C-2d routes `url`-valued props
    through `repark_connect::redact_source_prop(key, value)`. It masks the userinfo password and any
    `password=` query parameter, and core calls it for database-source rows. The interpretation stays in the
    owner crate (CC-3).
- **No spawning.** No process is spawned and nothing shells out. `tokio-postgres` drives each connection on a
  task that the pool owns and that ends when its `Client` drops; that is the only task C-2 creates.

### 2.5 The query pool (R-6) and timeouts (NS-7)

- **Shape.** `pool.rs` holds `QueryPool`, one per mounted source, created lazily on the first scan and owned
  by the source's catalog provider (dropped with the session). It is the sketch's own small pool:
  - a `tokio::sync::Semaphore` of `pool_max_size` permits;
  - a `Mutex<Vec<IdleClient>>` with idle timestamps;
  - a checkout bounded by `pool_checkout_timeout_ms`;
  - a health check of `Client::is_closed()` on checkout;
  - an idle reap on checkout.

  It needs no extra dependency, and it is about the size of the pins it needs.
- **The checkout guard.** `PooledClient` returns its connection to the pool **only** through
  `release_clean()`. The scan calls that after the COPY trailer and the `COMMIT` complete. Dropped any other
  way (cancel, error, a `LIMIT` satisfied early), the guard closes the client. The socket closes, so the
  server ends the backend and its read-only transaction, and a connection that is mid-COPY is never reused.
  No cleanup task is spawned, and no state straddles an `.await` inside a `select!` arm (NS §5 Safety). This
  is pinned by F-2.
- **Separate from capture, from day one (the 1.6 row, CC-2).**
  - `QueryPool` builds only query-mode `Config`s. It has no API that sets `replication`, and nothing in it
    names a replication connection.
  - 1.7 capture builds its connections in `repark-cdc` from the same `ConnectionSettings`, under a different
    `application_name`, and never through a `QueryPool`.
  - The narrow `cdc → connect` edge (CC-2) may share settings and TLS, never the pool.
  - Pin: `query_pool_connections_are_never_replication_connections` reads `pg_stat_activity.backend_type` and
    `application_name` for every pooled connection.
- **A timeout on every network call:**

  | call | timeout |
  |---|---|
  | connect | `connect_timeout_ms` (`Config::connect_timeout`) with TCP keepalives on |
  | every request/response and every COPY chunk | `tokio::time::timeout(read_timeout_ms, …)` |
  | server-side waits | `lock_timeout_ms` |
  | the scan statement | `query_timeout_ms` |
  | discovery | a 30 s `statement_timeout` |
  | pool checkout | `pool_checkout_timeout_ms` |

  A timeout is `ConnectError::Timeout { which }`, where `which` names the setting that fired.
- **Retry.** None inside a scan (FL-11, NS-8). The query fails with a retryable classification and the
  operator reruns it.

### 2.6 The read path: COPY BINARY decode (`copy_binary.rs`, `read/postgres.rs`)

- **Statement.**

  ```
  COPY (SELECT <col_1>::<planned type>, …, <col_n>::<planned type> FROM <relation> [WHERE <pushed>] [LIMIT <n>]) TO STDOUT (FORMAT BINARY)
  ```

  - The explicit cast on every column makes the wire type equal to the planned type by construction. A column
    whose type changed between planning and scan either casts (the wire stays what the decoder expects) or
    fails loud with the server's cast error (F-7). Text-rendered types cast to `pg_catalog.text` (§2.7).
  - An empty projection (`count(*)`) is `SELECT FROM <relation>`, which is legal since PostgreSQL 9.4. Each
    tuple then carries field count `0`.
  - A `query`-mode source wraps the user's statement as `(…) AS repark_q`, as Spark's `query` option does.
    The user's own SQL is passed through, and no value is spliced into it.
- **The decoder.** A resumable state machine over chunks, independent of how the server or the TLS layer cuts
  the stream: `Header → TupleStart → Field(i, remaining) → Trailer → Done`. Cited: docs, COPY "Binary Format":
  - **Header:** the 11-byte signature `50 47 43 4f 50 59 0a ff 0d 0a 00` (`PGCOPY\n\377\r\n\0`), then a
    32-bit flags word and a 32-bit header-extension length.
    - Flag bit 16 (OIDs included) must be 0, or the stream refuses with `Protocol`.
    - Bits 17–31 are reserved for critical format changes: any one set refuses, as the docs direct.
    - Bits 0–15 are reserved for backward-compatible changes and are ignored, as the docs direct.
    - The extension bytes are skipped.
  - **Tuple:** a 16-bit field count, which must equal the planned column count (else `Protocol`). Then, per
    field, a 32-bit length (`-1` = NULL) and that many bytes.
  - **Trailer:** a 16-bit `-1` (`ff ff`). Bytes after the trailer refuse with `Protocol`. A stream that ends
    before the trailer is `Disconnected` (retryable).
- **No per-row allocation (NS §5 Performance).**
  - Each column has an appender (`ColumnAppender`, one variant per `PostgresMapping`) that writes into an
    Arrow builder.
  - A field fully inside the current chunk decodes from a borrowed slice. Only a field that straddles a chunk
    boundary copies into one reusable carry buffer.
  - C-1's `PostgresTypeRow::decode(&[Option<&[u8]>])` stays as a thin wrapper over the appender, so C-1's ten
    round-trip pins keep exercising the same codec.
- **Batches.** A batch flushes at `batch_rows` or 64 MiB of builder bytes. Builder growth is charged to a
  `MemoryReservation` (`MemoryConsumer::new("PostgresScan")`). A refused reservation is DataFusion's
  resources-exhausted error, naming the column whose value did not fit. A single value larger than the cap
  still forms a one-row batch if the pool grants it.
- **The stream.** `PostgresScanExec::execute` returns a stream built with `futures::stream::try_unfold`. Each
  step:
  1. awaits the next chunk under the idle timeout;
  2. decodes it synchronously;
  3. yields at most one batch.

  The checkout, the transaction and the `set_config` call run on the stream's first poll, not at plan time, so
  EXPLAIN never opens a scan connection. After the trailer, the stream awaits `COMMIT` and calls
  `release_clean()`.
- **Values per type** are §2.7's. A refused value fails the stream with `UnrepresentableValue`, naming the
  column, the Postgres type, the reason enum and the registry row. The batches already yielded are discarded
  with the failed query, as for any execution error.

### 2.7 The type map: the nine declared types, decided

C-1's table stays the single home (`types/postgres.rs`, one row per type). For every row, the Arrow type is the
one Spark's PostgreSQL dialect surfaces (NS §2: Spark governs the surface), and **a value the Arrow type cannot
hold exactly refuses per value**. The Spark column cites the dialect as documented, with no value claim; the
oracle cells in D-M2 measure it. A measured difference is filed as a row, or halts under H-ORACLE (§7) if it
touches a guarantee.

| Postgres type | decision | wire (Postgres `*_send`) | refused values (row) | Postgres semantic citation |
|---|---|---|---|---|
| `numeric(p,s)`, `1 ≤ p ≤ 38`, `0 ≤ s ≤ p` | `Decimal128(p,s)`, exact | `int16 ndigits, int16 weight, uint16 sign, uint16 dscale`, then `ndigits` base-10000 `int16` digits | `NaN` (`pg-numeric-special`). Infinities cannot occur: a constrained column rejects them since 14. | docs §8.1.2 ("Arbitrary Precision Numbers": values are rounded to the declared scale on input, and special values); `numeric.c` `numeric_send`, `NUMERIC_NAN 0xC000`, `NUMERIC_PINF 0xD000`, `NUMERIC_NINF 0xF000`, `apply_typmod_special` (infinity rejected under a typmod); typmod `((p << 16) | s) + 4` |
| `numeric(p,s)`, `p > 38` | `Decimal128(38, min(s,38))` (Spark's `DecimalType.bounded`) | as above | integer digits beyond `38 − s′` (`pg-out-of-range`); `NaN`. Fractional digits beyond 38 round HALF_UP (FL-2). | as above |
| `numeric` (unconstrained) | `Decimal128(38,18)` (Spark's `SYSTEM_DEFAULT`; FL-2) | as above | more than 20 integer digits (`pg-out-of-range`); `NaN`, `±Infinity` (`pg-numeric-special`). Fractional digits beyond 18 round HALF_UP, which equals Postgres's own half-away-from-zero (`round_var`) (FL-2, Q3). | as above |
| `numeric(p,s)` with `s < 0` or `s > p` (allowed since 15) | **refuses the column** (`pg-unmapped`) | — | — | docs §8.1.2 (scale range since 15) |
| `date` | `Date32`: days since 1970-01-01 = wire + 10957 (checked add) | `int32` days since 2000-01-01 | `infinity` = `0x7FFFFFFF`, `-infinity` = `0x80000000` (`pg-infinite-datetime`; FL-4, Q1). Every finite date fits `Date32` (the largest wire value is about 2.145 × 10⁹, below `i32::MAX − 10957`). | docs §8.5 Table 8.9 (4713 BC to 5874897 AD), §8.5.1.4 (special values); `date.h` `DATEVAL_NOBEGIN` / `DATEVAL_NOEND`; `date_send` |
| `time` | **stays declared** (`CONNECT-DECL-pg-time`) until D-M2 reads Spark 4.1.2's type for a JDBC `time` column. If Spark reports `TimeType(6)`, the row flips to `Time64(Microsecond)`, with `24:00:00` (wire `00 00 00 14 1d d7 60 00`) refusing per value. If it reports `TimestampType`, the row records that and stays declared. | `int64` µs since midnight | — | docs §8.5 (range `00:00:00` to `24:00:00`); `time_send` |
| `timestamp` | Default (Spark's surface, FL-5): `TimestampType`. The wall clock is placed in the **session time zone** by a core-supplied localiser (§2.11), giving the Arrow type the doors use for `TimestampType`. With `prefer_timestamp_ntz = true`: `Timestamp(Microsecond, None)` = wire + 946 684 800 000 000 µs (checked add). | `int64` µs since 2000-01-01 (integer datetimes, the only build since 10) | `±infinity` = `0x7FFF…` / `0x8000…` (`pg-infinite-datetime`); values after 294247-01-10, where µs since 1970 overflows `i64` while Postgres goes to 294276 AD (`pg-out-of-range`); under the default, a wall clock in a DST gap or overlap refuses until D-M2 measures Spark's resolution (`CONNECT-DIV-pg-timestamp-zone`) | docs §8.5 Table 8.9 (4713 BC to 294276 AD), §8.5.1.3; `timestamp.h` `DT_NOBEGIN` / `DT_NOEND`, `POSTGRES_EPOCH_JDATE`; `timestamp_send` |
| `timestamptz` | `TimestampType`: the stored instant, exact, as wire + 946 684 800 000 000 µs. The Arrow zone label is the one the Iceberg read path gives a `timestamptz` column, so a federated join compares like types (the build slice reads it from `repark-iceberg`; §5.4 pins the join). | as `timestamp`, UTC | as `timestamp` | docs §8.5.1.3 ("stored internally in UTC") |
| `interval` | `Utf8` (Spark reads the driver's `OTHER` as `StringType`), rendered by Postgres: the column is cast `::pg_catalog.text` under the pinned `IntervalStyle = postgres`, so the text is the server's own | text | none | docs §8.5.5 ("Interval Output", `IntervalStyle`) |
| `uuid` | `Utf8`: the 16 wire bytes rendered as lowercase `8-4-4-4-12` hex, which is `uuid_out`'s form | 16 bytes | none | docs §8.12 (output is always the standard lowercase form); `uuid.c` `uuid_send`, `uuid_out` |
| `json` | `Utf8`: the stored text, byte for byte (UTF-8 validated) | the text | none | docs §8.14 (`json` stores an exact copy of the input text); `json_send` |
| `jsonb` | `Utf8`: `jsonb_out`'s canonical text | version byte `01`, then the text | any version byte other than `01` (`Protocol`) | docs §8.14 (`jsonb` does not preserve whitespace, key order or duplicate keys); `jsonb.c` `jsonb_send` |

**Resolution rules beside the rows.**

- **Domains** (`typtype = 'd'`) resolve to their base type. They read as the base type and the COPY casts to
  it (docs §8.18).
- **Enums** (`typtype = 'e'`) read as `Utf8`, with the label as the wire text (`enum_send`; docs §8.7). Spark
  reads them as `StringType`; D-M2 confirms.
- **Everything else refuses at resolution** with `UnmappedType` and `CONNECT-DECL-pg-unmapped`: arrays,
  composites, ranges, multiranges, `money` (whose text depends on `lc_monetary`), geometric, network, bit
  strings, `timetz`, `xml`, and any base type not in the table. The error names
  `schema.table.column`, the type, and the fix ("select it through `query` with a cast, or a view"). This is
  FL-6: adding a type is one row, one codec arm, one pin and one oracle cell.

**Byte anchors** for the round-trip pins (C-2a; the numbers were computed by hand from the formats above):

| value | type | wire bytes (hex) | Arrow value |
|---|---|---|---|
| `2000-01-01` | `date` | `00 00 00 00` | `Date32(10957)` |
| `1970-01-01` | `date` | `ff ff d5 33` | `Date32(0)` |
| `2024-03-10` | `date` | `00 00 22 83` | `Date32(19792)` |
| `infinity` | `date` | `7f ff ff ff` | refuses, `pg-infinite-datetime` |
| `1970-01-01 00:00:00` | `timestamp` (NTZ) | `ff fc a2 fe c4 c8 20 00` | `0` µs |
| `2024-03-10 12:00:00` | `timestamp` (NTZ) | `00 02 b6 4b ee e1 d0 00` | `1710072000000000` µs |
| `12345.678` | `numeric(8,3)` | `00 03 00 01 00 00 00 03 00 01 09 29 1a 7c` | `Decimal128(12345678, 8, 3)` |
| `-0.5` | `numeric(2,1)` | `00 01 ff ff 40 00 00 01 13 88` | `Decimal128(-5, 2, 1)` |
| `NaN` | `numeric` | `00 00 00 00 c0 00 00 00` | refuses, `pg-numeric-special` |
| `a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11` | `uuid` | `a0 ee bc 99 9c 0b 4e f8 bb 6d 6b b9 bd 38 0a 11` | that string |
| `{"a": 1}` | `jsonb` | `01 7b 22 61 22 3a 20 31 7d` | `{"a": 1}` |

### 2.8 Resolution: relation, query and schema discovery (`discover.rs`)

- **Relation mode** (named sources, and `dbtable` naming a relation). One bound catalog query per
  `SchemaProvider::table(name)`, run under the discovery timeout:
  `pg_attribute` ⨝ `pg_class` ⨝ `pg_namespace` ⨝ `pg_type` ⟕ `pg_collation`, keyed on `nspname = $1` and
  `relname = $2`.
  - `relkind` is one of `r v m f p`: table, view, materialised view, foreign table, partitioned table.
  - Only `attnum > 0` and `NOT attisdropped`, ordered by `attnum`.
  - It returns `attname`, `atttypid`, `atttypmod`, `attnotnull`, `typname`, `typtype`, `typbasetype`,
    `collname`, `collisdeterministic`, and `current_setting('server_encoding')`.

  A miss is `RelationNotFound`, so `table()` returns `Ok(None)` and the door's own "table not found" fires.
  A permission failure is `PermissionDenied`.
- **Identifiers are matched exactly** as the door hands them over (FL-14); `PgIdent` quotes them.
  `dbtable = "s.t"` parses as a qualified identifier with optional double-quoted parts. A `dbtable` that
  starts with `(` is Spark's subquery form and is treated as query mode.
- **Query mode** (`query`, or a parenthesised `dbtable`). The wrapped statement is `Client::prepare`d (Parse
  and Describe, no execution). Its `RowDescription` gives each column's type OID and, where the driver exposes
  it, the type modifier (D-M7). A column with no modifier, or one computed by an expression, maps as
  unconstrained.
- **No cache (FL-7).** Each statement resolves afresh, as Spark's `resolveTable` does, so a server-side schema
  change is seen on the next statement.
- **Nullability** follows `attnotnull` in relation mode; query mode is nullable.
- **Listing.** DataFusion's `CatalogProvider::schema_names` and `SchemaProvider::table_names` are synchronous,
  and session build is I/O-free (CFG-2 D-4). So listing a Postgres source returns empty and is declared,
  `CONNECT-DECL-pg-listing` (FL-8). The row names the fix: an asynchronous listing through the doors' catalog
  operations, or the 1.7 crawler's read surface (pre-declared `crawler → connect`). `schema(name)` returns a
  lazy provider for any name; existence is checked in `table()`.

### 2.9 Pushdown: the subset Postgres takes exactly (`pushdown.rs`)

**The rule.** `supports_filters_pushdown` classifies each conjunct DataFusion offers.

- **`Exact`** goes to the subset below: rendered, pushed, and not re-applied.
- **`Inexact`** goes to everything else. This is not a claim that part of the filter is applied: a filter
  classed `Inexact` is **never** pushed. The class is chosen only because DataFusion then passes the filter to
  `scan` while keeping its own `FilterExec` above the scan. The scan therefore sees the full conjunct list and
  can render the residual list in EXPLAIN (§2.10).
- With `pushdown_predicate = false`, every conjunct is classed `Inexact`.
- D-M6 confirms DataFusion 54.1's handling of both classes. H-DF halts if it differs.

**Column eligibility:**

| column surfaces as | eligible |
|---|---|
| integers, `bool`, `Decimal128` | yes |
| `Date32` | yes |
| `timestamptz` | yes |
| NTZ `timestamp` | yes |
| `text`, `varchar` | yes, but only when `server_encoding = 'UTF8'` |
| LTZ-localised `timestamp` (the localisation is not injective across DST) | no |
| `bpchar` (its `=` ignores trailing blanks, `bpchareq`) | no |
| `float4`, `float8` (Postgres orders `NaN` above every number and makes it equal to itself, `float8_cmp_internal`; the engine side is not pinned to the same rule) | no |
| `bytea` | no |
| any text-rendered type: `uuid`, `json`, `jsonb`, `interval`, enums | no |

**The subset** (each class has one pin in §5.2):

| class | DataFusion shape after type coercion | rendered SQL | why it is exact |
|---|---|---|---|
| P-1 null test | `IsNull(c)`, `IsNotNull(c)` | `"c" IS NULL`, `"c" IS NOT NULL` | NULL is the same in both; valid for every mapped column, eligible or not |
| P-2 boolean | `c`, `NOT c`, `IsTrue/IsFalse/IsNotTrue/IsNotFalse/IsUnknown(c)` | the same keywords | three-valued logic is identical (docs, "Logical Operators") |
| P-3 integer comparison | `c ⊕ lit`, `lit ⊕ c`, `CAST(c AS wider int) ⊕ lit`, with `⊕ ∈ {=, <>, <, <=, >, >=, IS [NOT] DISTINCT FROM}` | `"c" ⊕ current_setting('repark.pN')::pg_catalog.int<K>`, where `K` is the literal's width; a widening cast on the column is dropped, because Postgres's integer operators are exact across widths | the btree `integer_ops` family is cross-type exact (docs, "Operator Classes and Operator Families"; "Comparison Functions and Operators"), and widening keeps the index usable |
| P-4 decimal comparison | as P-3 over `Decimal128` | `… ::pg_catalog.numeric` | `numeric` comparison is exact (docs §8.1.2) |
| P-5 date / timestamp comparison | as P-3 over `Date32`, `timestamptz`, NTZ `timestamp`, with the literal inside Postgres's range (a literal outside it stays residual) | `… ::pg_catalog.date` / `timestamptz` / `timestamp` | integer µs and day ordering are identical (§2.7) |
| P-6 text comparison | `c ⊕ lit` over `text` / `varchar`, the literal containing no NUL byte | `"c" COLLATE "C" ⊕ current_setting('repark.pN')` | under `"C"`, comparison is byte order, which for UTF-8 is code-point order, the engine's order; equality is byte equality whatever the column's collation, deterministic or not (docs, "Collation Support": concepts and nondeterministic collations) |
| P-7 IN list | `c [NOT] IN (lit, …)`, at most 256 items, each eligible under P-3…P-6 | `"c" [NOT] IN (current_setting('repark.p0')::T, …)` (with `COLLATE "C"` on text) | `IN` is defined as a disjunction of `=` in both, so a NULL item gives the same three-valued result (docs, "Row and Array Comparisons": `IN`) |
| P-8 range | `c [NOT] BETWEEN a AND b` | rendered as `>=` / `<=` per class | symmetric to P-3…P-6 |
| P-9 LIKE | `c [NOT] LIKE lit` over `text` / `varchar`, pattern literal, default escape, and well-formed (not ending in the escape character) | `"c" COLLATE "C" LIKE current_setting('repark.pN') ESCAPE '\'` | `%` and `_` match characters in a UTF-8 database; backslash is the escape in both (docs, "Pattern Matching": `LIKE`). A malformed pattern stays residual, so the engine's own error fires. |
| P-10 logic | `AND`, `OR`, `NOT` over children that are each P-1…P-9 | parenthesised | three-valued logic, identical |
| P-11 limit | `scan(limit = Some(n))` with no residual conjunct for this scan | `LIMIT n` | an unordered limit; DataFusion passes `limit` only when no filter remains above the scan, and the scan re-checks this |

**Residual** is everything else, kept engine-side and listed in EXPLAIN:

- every ineligible column of the eligibility table;
- `ILIKE` (case folding differs between Postgres under `"C"` and the engine);
- regular expressions;
- every function call (`lower`, `substring`, …);
- arithmetic (overflow semantics differ: Postgres raises, Spark's non-ANSI mode wraps);
- every cast other than an integer widening of the column;
- `CASE` and `COALESCE`;
- scalar subqueries and UDFs;
- literals of a type that does not match the column after coercion.

**Not pushed at all:** sorts (DataFusion's `TableProvider` has no sort push in this shape, so NULL ordering
never crosses the boundary; Postgres sorts NULLs last ascending, Spark first; docs, "Sorting Rows"), aggregates, joins and
projections of expressions.

**Literal text for `set_config`, per class** (exact, and parsed by the type's input function):

| class | text |
|---|---|
| integers | base-10 |
| `numeric` | plain decimal at the literal's scale |
| `bool` | `true` / `false` |
| `text` | as-is |
| `date` | ISO `YYYY-MM-DD`, with ` BC` for years ≤ 0 |
| timestamps | ISO with six fractional digits, and `+00` for `timestamptz` |

Year-first ISO input parses the same under every `DateStyle` (docs, "Date/Time Input"), and `DateStyle` is pinned to
ISO anyway.

**Values refused before the filter reads them.** A value that would be refused (`NaN`, `infinity`) but sits
outside a pushed filter's range is never read, so the scan succeeds where the residual path would refuse.
Spark's JDBC pushdown behaves the same. This is recorded in `CONNECT-DECL-pg-numeric-special` and
`pg-infinite-datetime`, and it is the one place where pushed and unpushed differ: in whether the query
succeeds, never in the rows it returns.

### 2.10 The EXPLAIN boundary (CC-1)

`PostgresScanExec` implements `DisplayAs`. **Default and tree format**, one line per source scan:

```
PostgresScanExec: source=company_db, relation="public"."orders", projection=[id, amount, status], pushed_filters=[amount > Decimal128(Some(10000),10,2), status = Utf8("paid")], residual_filters=[lower(note) = Utf8("x")], pushed_limit=None
```

- `pushed_filters` takes its name from Spark's `PushedFilters` label. Both lists are the DataFusion `Expr`
  displays of the conjuncts the scan actually pushed and actually left, for **this** statement (CC-1:
  "what the statement actually pushed, … not what the connector supports in general").
- The residual list matches the `FilterExec` DataFusion keeps above the scan. The EXPLAIN pin checks both.
- **Verbose format** adds `remote_sql=` with the `current_setting('repark.pN')` placeholders as sent, never
  the bound values, and `bound_values=N`.
- **Never rendered:** host, port, database, user, URL, `sslmode` or any prop.
- `read_postgres` sources render `source=jdbc` and the relation or `query`.
- Plan metrics (CC-7: the per-query report reads `MetricsSet`): `output_rows`, `bytes_received`,
  `time_to_first_byte`, `elapsed_compute`, `batches`. They are registered on the exec, so `EXPLAIN ANALYZE`
  shows per-source rows, bytes and time. The roadmap's 2.7 row builds on exactly that. No `metrics`-facade
  counters in C-2: no exporter consumes them yet (CC-7).

### 2.11 Mounting, both stubs retired, the restored edge (CC-1, R-4)

- **The edge.** C-2d adds `repark-connect` to `repark-core`'s `[dependencies]` (`normal`, with the `postgres`
  feature forwarded). It restores the `("repark-core", "repark-connect")` row in
  `scripts/check_crate_dag.py`'s `ALLOWED_EDGES`, with the reason string recorded in R-14 kept **verbatim**:
  "PRE-DECLARED for release 1.6 (crate-layout-1-8-2026-10-01.md): Session registers the configured database
  sources in the one federated namespace, the way it registers Iceberg catalogs today"
  ([scripts/map.md](../../scripts/map.md), the C-1 note). `repark-python` keeps reaching connect only through
  core, so no `python → connect` edge is added.
- **The mount, through `SessionExtension::register`.** `catalog_state.rs` gains `SourceMount { specs, zone }`,
  which implements `SessionExtension`. Its `register(ctx)` registers one catalog provider per auto-registered
  source:
  - `SourceKind::Postgres` → `repark_connect::postgres::PostgresSource::mount(identity, props, localiser)`,
    an `Arc<dyn CatalogProvider>`. It is pure: no I/O and no validation (FL-1).
  - `SqlServer` and `Trino` → the existing `RefusingSourceCatalogProvider`, unchanged (C-5, 1.10).

  `ReparkSession::register_configured_sources()` stays the single entry point CFG-2 D-4 named, called where
  it is today (after build, beside `register_configured_catalogs`). It now runs `SourceMount::register`,
  inserts the specs into `CatalogRegistry` as before, and adds every mounted Postgres name to the session's
  existing (and until now never-filled) `postgres_catalog_names`. That makes the P11 read-only guards
  already on `main` (`repark-sql` `guards.rs`, `repark-spark` `catalog_ops.rs`) refuse DML and
  `CREATE TABLE` against a Postgres source, in their existing pinned wording, until C-4.
- **The localiser (§2.7 `timestamp`).** connect declares the trait (NS-10: the trait sits low and has no zone
  dependency):

  ```
  trait WallClockLocaliser: Send + Sync + Debug {
      fn localise(&self, wall: &TimestampMicrosecondArray) -> Result<TimestampMicrosecondArray>;
      fn zone_label(&self) -> Arc<str>;
  }
  ```

  Core implements it over the session's live `runtime_zone`, read at scan time, as Spark reads its session
  zone per query, using chrono-tz as `orc_scan.rs`'s `writer_wall_to_utc` already does. Gap and overlap
  values refuse until D-M2 measures Spark's rule. `writer_wall_to_utc` is the precedent, not the function:
  it takes the earlier offset on an overlap, and D-M2 decides whether that is Spark's rule. If core has no
  usable zone conversion, H-TZ applies.
- **`refuse_source_ddl`** keeps its role as a pre-execute guard. Its connector-pending refusal retires for
  Postgres sources and is replaced by "database source `<key_path>` is read-only: DDL against it is not
  supported" plus `CONNECT-DECL-pg-ddl`. Spark's JDBC table catalog does run `CREATE` and `DROP`; C-4 writes
  rows, not DDL. SQL Server and Trino keep today's text.
- **`RefusingSourceSchemaProvider`** is no longer used for Postgres. The Postgres schema provider refuses
  `register_table` and `deregister_table` with the same `pg-ddl` row.
- **`NamedSource::ping()`** goes live for Postgres: checkout, `SELECT 1`, release, each under its timeout. It
  becomes `async` in core, and `crates/repark-python/src/session_sources.rs` blocks on the binding's shared
  runtime, as the other async doors do.
- **`read_postgres`** goes live (the second stub, `crates/repark-python/src/session.rs`):
  - Core gains `ReparkSession::read_postgres(PostgresRead { url, target: Relation | Query, properties })
    -> Result<DataFrame>` in a new `session/read_postgres.rs`. It builds an ad-hoc `PostgresSource` whose
    pool lives as long as the returned frame's provider.
  - Any of `partition_column`, `lower_bound`, `upper_bound`, `num_partitions` or `predicates` refuses with
    `CONNECT-DECL-pg-partitioned-read` (C-3). The facade's own Spark-parity argument checks run first,
    unchanged.
  - The `deferred_reader_error` text drops "repark-postgres" and keeps Excel.
  - The registry row IO-JDBC-1 is rewritten to the live behaviour.
- **The federated namespace.** `SELECT … FROM company_db.public.orders o JOIN ice.db.customers c ON …` plans
  as one DataFusion statement with two scans. The join runs engine-side; a join predicate never reaches a
  provider.

### 2.12 What C-2 does not do

- **Partitioned reads and snapshot export** (C-3; `pg_export_snapshot` would make the partitions see one
  snapshot): out.
- **Writes** (C-4), **SQL Server** (C-5), **`read_database` / `write_database` and the CC-4 Python classes**
  (C-6): out.
- **Listing** (§2.8): declared, not built.
- **`repark.connect.*` session defaults:** not defined.
- **Generation assignment** (1.7): not touched.

## 3. Four-line records (2026-10-06)

Each record gives the question, Flink's answer (the JDBC/CDC connector), Spark's answer (the JDBC data source),
and the North Star default acted on. The engine behaviours are cited as documented. Where a value matters, an
oracle cell measures it (D-M2), and a measured difference becomes a registry row.

| id | question | Flink | Spark | default acted on |
|---|---|---|---|---|
| FL-1 | When does a malformed source entry refuse, and do unknown keys refuse? | Connector options are validated when the statement using the table is planned; unknown option keys fail validation. | JDBC options parse when the relation or catalog is first used; unknown keys are forwarded to the driver as connection properties. | Refuse on the source's first resolution, before any I/O; unknown keys refuse (NS §5). Session build stays I/O- and validation-free (CFG-2 D-4). Disagreement on unknown keys → `CONNECT-DIV-pg-unknown-option`, Q6. |
| FL-2 | Unconstrained `numeric` and `numeric(p > 38)` | `DECIMAL(38,18)` for unbounded `numeric`; `DecimalData.fromBigDecimal` rounds HALF_UP to the scale and gives **null** on precision overflow. | `DecimalType.SYSTEM_DEFAULT` (38,18) for unbounded, `DecimalType.bounded` caps `p` at 38; the value is set to the scale HALF_UP; precision overflow **raises**. | `Decimal128(38,18)` / bounded, HALF_UP; overflow refuses per value (Spark's answer and NS-6). The engines agree on the type and the rounding. They disagree on overflow, and taking Spark's answer changes no Spark row, so §8.2 does not trigger. Q3 confirms the rounding. |
| FL-3 | `numeric` `NaN` / `±Infinity` | pgjdbc cannot build a `BigDecimal` from them; the read fails. | Same driver, same failure. | Refuse per value, `CONNECT-DECL-pg-numeric-special`. They agree. |
| FL-4 | `date` / `timestamp` `±infinity` | pgjdbc's infinity sentinels reach the converter; there is no documented mapping. | The PostgreSQL dialect special-cases infinite timestamps in recent releases; 4.1.2's exact mapping is measured by D-M2. | Refuse per value, `CONNECT-DECL-pg-infinite-datetime` (card 1.6: declare, never approximate). Q1 asks whether to switch to Spark's mapping once measured. |
| FL-5 | `timestamp` without time zone | `TIMESTAMP` (no zone). | `TimestampType`, the wall clock placed in the JVM default zone; `TimestampNTZType` under `preferTimestampNTZ=true`. | Spark's surface: `TimestampType` via the **session** zone (Spark's own guidance is to keep the two zones equal), NTZ under the option. They disagree on the surface, and Spark wins. The session-versus-JVM-zone choice is `CONNECT-DIV-pg-timestamp-zone`. |
| FL-6 | A column whose type is outside the map | The table refuses: the type mapper raises "Doesn't support Postgres type". | Driver `OTHER` types read as `StringType`, which is the server's text. | Spark's surface for the types an oracle cell confirms (`uuid`, `json`, `jsonb`, `interval`, enums in C-2). Every other type refuses at resolution, naming the column and the fix (`CONNECT-DECL-pg-unmapped`, rank 4 until measured). |
| FL-7 | Cache the remote schema? | Resolved through the catalog when each statement is planned. | `resolveTable` runs each time a relation is created. | Resolve per statement, no cache. They agree. |
| FL-8 | List a source's schemas and tables | The JDBC catalog lists. | The JDBC table catalog lists. | Both list, but DataFusion's listing hooks are synchronous and session build is I/O-free, so listing returns empty under `CONNECT-DECL-pg-listing`. The row names the fix. |
| FL-9 | Push `LIMIT` | The JDBC source implements limit push-down. | `pushDownLimit` (its 4.1.2 default is measured by D-M2). | Push `LIMIT n` when no residual remains for the scan. They agree on the mechanism. |
| FL-10 | String comparisons and collation | Pushed against the column, so the Postgres collation decides. | Compiled into the `WHERE` text, so the Postgres collation decides. | **The card's ruled rule outranks both:** pushdown never changes semantics. Every pushed text comparison carries `COLLATE "C"`, which is the engine's code-point semantics, and `bpchar` stays residual. Spark's pushed rows can differ under a non-`C` collation: `CONNECT-DIV-pg-text-collation`, Q4. |
| FL-11 | A transient failure mid-scan | The restart strategy re-reads the split. | `local[N]` does not retry a task (`spark.task.maxFailures` is 1 there); cluster mode retries. | NS-8: the query fails with a retryable classification and nothing retries inside the scan. Spark's local mode, the single-node analogue, agrees. |
| FL-12 | The default TLS posture | The URL goes to pgjdbc, whose default `sslmode` falls back to plaintext. | Same. | NS §5: `verify-full` by default; plaintext only by `sslmode=disable`; the silent or unverified modes refuse. `CONNECT-DIV-pg-sslmode`, Q5. |
| FL-13 | Literal values in pushed predicates | Bound through a prepared statement. | Compiled into the SQL text. | NS §5: values are bound. Inside COPY (R-2), that is a transaction-local `set_config` with bound parameters, read back with `current_setting(…)::type`. Q2 covers the planner-estimate cost. |
| FL-14 | Identifier case | Quoted as given. | The JDBC table catalog quotes the identifier as given. | Match exactly what the door hands over. D-M2 adds a mixed-case table cell to measure the door against Spark. |

## 4. Registry rows (all in [the registry](../../docs/spark-sql-iceberg-parity.md) §5, beside the `CONNECT-DECL` family)

**Retired** (the type is now mapped): `CONNECT-DECL-pg-numeric`, `-pg-date`, `-pg-timestamp`,
`-pg-timestamptz`, `-pg-interval`, `-pg-uuid`, `-pg-json`, `-pg-jsonb`, all in C-2a, each with the pin that
replaces it. **Kept:** `CONNECT-DECL-pg-time` (§2.7), plus C-1's two auth rows, whose type name changes to
`ConnectError`.

**New declared rows:**

| row | slice | what it declares |
|---|---|---|
| `CONNECT-DECL-pg-numeric-special` | C-2a | `NaN` / `±Infinity` refuse per value |
| `CONNECT-DECL-pg-infinite-datetime` | C-2a | `±infinity` dates and timestamps refuse per value |
| `CONNECT-DECL-pg-out-of-range` | C-2a | `numeric` integer-digit overflow of the bounded type; timestamps after 294247-01-10 |
| `CONNECT-DECL-pg-unmapped` | C-2a | types and columns outside the map refuse at resolution |
| `CONNECT-DECL-sslmode-unverified` | C-2b | `prefer`, `allow`, `require` and `verify-ca` refuse |
| `CONNECT-DECL-pg-client-cert` | C-2b | `sslcert` and `sslkey` |
| `CONNECT-DECL-pg-session-sql` | C-2b | `sessionInitStatement`, `customSchema` and `options` |
| `CONNECT-DECL-pg-server-version` | C-2b | servers older than 14 |
| `CONNECT-DECL-pg-multi-host` | C-2b | libpq host lists |
| `CONNECT-DECL-pg-listing` | C-2c | |
| `CONNECT-DECL-pg-ddl` | C-2d | |
| `CONNECT-DECL-pg-partitioned-read` | C-2d | C-3 |

**New divergence rows:**

| row | slice | the divergence |
|---|---|---|
| `CONNECT-DIV-pg-sslmode` | C-2b | `verify-full` default; `disable` is the explicit plaintext |
| `CONNECT-DIV-pg-unknown-option` | C-2b | unknown keys refuse where Spark forwards them |
| `CONNECT-DIV-pg-text-collation` | C-2c | `COLLATE "C"` engine semantics, where Spark pushes against the column's collation |
| `CONNECT-DIV-pg-timestamp-zone` | C-2c | the session zone, where Spark uses the JVM zone; gap and overlap refusals until measured |

**Rewritten:** IO-JDBC-1 (C-2d). Each row carries the four-field shape (repark / Apache Spark / Pin /
Rationale) and is dated on the day its slice lands.

## 5. Pins, each with the mutation that must turn it red

Every Rust pin lives in `crates/repark-connect/tests/it/` (C-1's R-9: one integration binary) unless it names
core or Python. "Live" means it runs against the C-0 container (§6). In every slice, the ledger records each
mutation and the line where it went red, then restores the tree (C-1's practice).

### 5.1 The wire round trip, with byte anchors (C-2a)

| pin | asserts | mutation |
|---|---|---|
| `copy_header_is_the_signature_flags_and_extension` | §2.6's eleven-byte signature; a zero flags word; the extension skipped | accept a 10-byte signature |
| `copy_oid_flag_refuses` | flag bit 16 → `Protocol` | ignore bit 16 |
| `copy_trailer_ends_and_trailing_bytes_refuse` | `ff ff` ends the stream; one more byte → `Protocol` | stop at the trailer without checking the rest |
| `copy_truncated_stream_is_disconnected` | EOF mid-tuple → `Disconnected` | return what was decoded so far |
| `copy_decode_is_independent_of_chunking` | one 3-tuple stream with every §2.7 mapping and NULLs, split at **every** byte offset, gives identical batches | copy a straddling field without its carry |
| `copy_field_count_must_match_projection` | wrong count → `Protocol` | skip the check |
| `date_anchors_round_trip` | the four `date` rows of §2.7's anchor table | epoch shift `10957 → 10958` |
| `timestamp_ntz_anchors_round_trip`, `timestamptz_anchors_round_trip` | the two timestamp anchors; values after 294247-01-10 → `pg-out-of-range` | unchecked add |
| `numeric_anchors_round_trip` | `12345.678`, `-0.5`, a 38-digit maximum, scale padding (`dscale` < `s`) | digit base `10000 → 1000` |
| `numeric_special_values_refuse` | `NaN`, `+Inf`, `-Inf` sign words → `pg-numeric-special` | treat `0xC000` as negative |
| `unconstrained_numeric_rounds_half_up_at_scale_18` | `0.0000000000000000005` → `…01`; `-0.…5` → `-…01` | round half-even |
| `bounded_numeric_overflow_refuses` | 21 integer digits into `(38,18)` → `pg-out-of-range` | wrap |
| `uuid_renders_lowercase_canonical` | the anchor | uppercase hex |
| `jsonb_strips_version_one_and_refuses_others` | `01 …` decodes; `02 …` → `Protocol` | skip the version byte without checking it |
| `json_and_interval_are_text_verbatim` | bytes in = string out | trim whitespace |
| `batches_flush_at_rows_and_at_bytes` | `batch_rows = 3` over 7 rows gives 3+3+1; the 64 MiB cap triggers on a large `bytea` | flush on rows only |
| C-1's ten `<type>_round_trips` | unchanged, now through the appender | C-1's m1, m2 replayed |

### 5.2 Pushdown, one pin per class plus the residual pins (C-2c)

- **Unit pins** assert the class and the rendered SQL, with placeholders and no values.
- **Live pins** assert **row equality with `pushdown_predicate = false`** over a seeded table that holds every
  edge value (NULLs, extremes, `NaN` in `float8`, padded `bpchar`, mixed-case and accented text, a
  non-deterministic ICU column). They also assert the EXPLAIN `pushed_filters` / `residual_filters` split.

| pin (unit + live) | class | edge it holds | mutation |
|---|---|---|---|
| `p01_null_tests_push` | P-1 | NULLs in every mapped type | render `= NULL` |
| `p02_boolean_tests_push` | P-2 | NULL booleans under `IS NOT TRUE` | render `NOT c` for `IS NOT TRUE` |
| `p03_integer_comparison_pushes_cross_width` | P-3 | `int2` column `= 100000` returns zero rows, no error | cast the parameter to the column's width (Postgres raises "smallint out of range") |
| `p04_decimal_comparison_pushes` | P-4 | scale mismatch between literal and column | render the literal at the column's scale, truncating |
| `p05_temporal_comparison_pushes_inside_range` | P-5 | a literal before 4713 BC stays residual | push out-of-range literals |
| `p06_text_comparison_is_code_point_order` | P-6 | `'B' < 'a'` under `"C"`; an ICU column where `'a' < 'B'` | drop `COLLATE "C"` |
| `p06b_text_equality_ignores_nondeterministic_collation` | P-6 | a `deterministic = false` column holding `'abc'` and `'ABC'`; the filter is `= 'abc'` | drop `COLLATE "C"` |
| `p06c_nul_literal_stays_residual` | P-6 | a literal with `\0` | push it |
| `p07_in_list_three_valued` | P-7 | `NOT IN (1, NULL)`; 257 items stay residual | drop NULL items |
| `p08_between_renders_inclusive` | P-8 | the boundary values | render `>` / `<` |
| `p09_like_pushes_well_formed_patterns` | P-9 | `_` against `é`; `\%`; a pattern ending in `\` stays residual | omit `ESCAPE` |
| `p10_logic_pushes_only_exact_children` | P-10 | `a = 1 OR lower(b) = 'x'` stays residual whole | push the exact half of an `OR` |
| `p11_limit_pushes_only_without_residual` | P-11 | `LIMIT 5` over a residual filter still returns 5 rows | push the limit regardless |
| `r01_float_comparisons_stay_residual` | residual | `NaN` row, `f > 1.0` | class `float8` eligible |
| `r02_bpchar_comparisons_stay_residual` | residual | `char(5)` `'a'` vs `'a'` | class `bpchar` as text |
| `r03_ltz_timestamp_comparisons_stay_residual` | residual | a DST overlap wall clock | class an LTZ `timestamp` eligible |
| `r04_functions_arithmetic_and_casts_stay_residual` | residual | `id + 1 > 5` where `id = i64::MAX` | push arithmetic |
| `r05_pushdown_predicate_false_pushes_nothing` | switch | everything residual; EXPLAIN shows `pushed_filters=[]` | ignore the key |

### 5.3 The EXPLAIN boundary (C-2c, re-pinned in C-2d through both doors)

| pin | asserts | mutation |
|---|---|---|
| `explain_renders_pushed_and_residual_per_scan` | exact text for a two-conjunct statement, one conjunct pushed and one residual | render the classifier's general support instead of what was pushed |
| `explain_verbose_shows_placeholders_never_values` | `remote_sql` has `current_setting('repark.p0')`, no literal, and `bound_values=1` | render the bound value |
| `explain_never_renders_endpoint` | host, port, user, database and URL are all absent | add `host=` to the display |
| `explain_residual_matches_filter_exec_above` | the residual list equals the `FilterExec` predicate directly above the scan | class a residual `Unsupported`, so the scan never sees it |

### 5.4 The federated statement (C-2d, live)

| pin | asserts | mutation |
|---|---|---|
| `federated_iceberg_postgres_join` (Python, both doors: `spark.sql` and the DataFrame join) | An Iceberg table in a private memory catalog joined with a Postgres table in the cell schema on an integer key and a `timestamptz` column, with a pushed filter on the Postgres side and a residual one. The rows equal the expected set computed independently (psycopg plus pyarrow). EXPLAIN shows one Iceberg scan and one `PostgresScanExec` with the right `pushed_filters` / `residual_filters`, and the join above both. | push the join predicate into the Postgres scan (rows red) or mislabel the timestamp zone (the join returns zero rows) |
| `federated_join_explain_boundary` | The same statement's EXPLAIN text, normalised for attribute ids, against a committed expectation. | as `explain_renders_pushed_and_residual_per_scan` |

The Spark oracle half of this cell needs the oracle environment to load the pgjdbc driver (D-M2). Until it
can, the reference is the independent expectation, and the ledger says so.

### 5.5 ES-1 refusals and settings (C-2b; C-1's four pins stay)

| pin | asserts | mutation |
|---|---|---|
| `iam_token_is_a_declared_refusal`, `kerberos_is_a_declared_refusal` (C-1) | unchanged, now through `ConnectError` | C-1's m3 |
| `every_endpoint_key_parses_and_unknown_keys_refuse` | each §2.3 row; a misspelt key lists the accepted keys and echoes no value | accept unknown keys |
| `aliases_are_case_insensitive_and_conflicts_refuse` | `queryTimeout` and `QUERYTIMEOUT`; `url` user against the `user` key → refuse | last spelling wins |
| `alias_units_convert` | `socketTimeout=5` gives 5000 ms | no unit conversion |
| `sslmode_default_is_verify_full`, `unverified_sslmodes_refuse` | the §2.4 table | default to `require` |
| `declared_keys_refuse_naming_their_row` | `sslcert`, `sessionInitStatement`, `options`, … | accept `options` |
| `read_timeout_zero_refuses` | NS-7 | accept `0` |
| `redact_source_prop_masks_url_credentials` | `postgresql://u:pw@h/db` and `jdbc:…?password=pw` mask `pw`; a URL without a password is unchanged | mask userinfo only when `@` follows `:` |
| `settings_debug_never_renders_a_value` (extends C-1's C-006) | every new type | print the URL |

### 5.6 The read-path crash matrix (C-2b, live)

| pin | fault | asserts | mutation |
|---|---|---|---|
| F-1 `backend_killed_mid_copy_is_disconnected` | `pg_terminate_backend` from the fixture connection after the first batch | `Disconnected`; the next scan opens a fresh connection; the pool's idle count is unchanged | return the client to the pool on error |
| F-2 `stream_dropped_mid_copy_closes_the_backend` | drop the stream after one batch | within the read timeout, no `application_name = 'repark'` backend for the cell remains in `pg_stat_activity`; the pool did not reuse it | `release_clean` on `Drop` |
| F-3 `idle_read_timeout_fires` | `query` mode `SELECT pg_sleep(3), 1` with `read_timeout_ms = 500` | `Timeout { which: read_timeout_ms }` | no timeout around the chunk await |
| F-4 `lock_timeout_fires` | the fixture holds `ACCESS EXCLUSIVE` | `Timeout { which: lock_timeout_ms }` | drop the startup pin |
| F-5 `pool_exhaustion_times_out` | `pool_max_size = 1`, one stream held unpolled | `PoolExhausted` after `pool_checkout_timeout_ms` | an unbounded semaphore |
| F-6 (unit, §5.1) | truncated stream | `Disconnected` | — |
| F-7 `schema_drift_between_plan_and_scan_fails_loud_or_stays_typed` | `ALTER COLUMN … TYPE` between `table()` and `execute` | either typed values (an int widened then cast back) or the server's cast error, never misread bytes | drop the per-column cast |
| F-8 `scan_is_read_only_and_idempotent` | `query` mode `DELETE … RETURNING`, then the same pushed scan run twice | the `DELETE` refuses (SQLSTATE `25006`); the scan rows are identical; `pg_stat_user_tables.n_tup_del` is unchanged | drop `default_transaction_read_only` |
| `query_pool_connections_are_never_replication_connections` | every pooled backend | `backend_type = 'client backend'`, the configured `application_name`, never a `walsender` | — (this is a contract pin for 1.7) |
| `missing_select_grant_names_the_privilege` | a cell role without `SELECT` | `PermissionDenied { privilege: SELECT }` | map `42501` to `Server` |
| `plaintext_server_refuses_under_the_default` | the C-0 container with no `sslmode` | `TlsRequired`, naming `sslmode` | default to `disable` |

### 5.7 Credentials (C-2d, live, Python)

`no_credential_reaches_any_surface`:

- **Setup.** The cell creates a role `r_<tag>` with a random 32-hex password and `SELECT` on the cell schema,
  then mounts a source with it through `repark.toml`, using both the `password` key and a URL-embedded form.
- **Surfaces exercised:**
  - `sources()` and `repr`;
  - `source().ping()`;
  - `explain()` and `EXPLAIN VERBOSE`;
  - an auth failure with a *different* random wrong password;
  - a missing relation, a lock timeout and a pool timeout;
  - a refused value;
  - the captured tracing output at `RUST_LOG=trace` and the Python log records.
- **Assertion.** Neither password, nor the full URL, appears in any surface.
- **Mutation.** Format the URL into `Unreachable`'s message. The pin goes red.

## 6. The live cells: what runs in the C-0 container

- **The container.** All of them run against `make pg-up` (PostgreSQL 16, plaintext). They use
  `sslmode = disable`, the explicit plaintext option, so the dated `CONNECT-DIV-pg-sslmode` row is exercised
  on every cell.
- **Rust cells** (`tests/it/live_pg.rs`, C-2b and C-2c):
  - They are `#[ignore = "live: make pg-up, REPARK_PG_URL"]`, run with `--ignored` under the container, and
    fail rather than skip when explicitly invoked without `REPARK_PG_URL`.
  - Each creates and drops a schema tagged `c2_<random>`, mirroring `pg_live`.
  - They cover §5.6 (F-1…F-5, F-7, F-8, the pool pin, the grant pin, the TLS-refusal pin), the live halves of
    §5.2, and a server-generated round trip per mapped type: `COPY (SELECT <literal>::<type>) TO STDOUT
    (FORMAT BINARY)` decoded and compared with §5.1's anchors, which proves the anchors are the server's bytes
    and not only the docs'.
- **Python cells** (`python/repark-parity/tests/live_db/test_c2_*.py`, C-2d):
  - They use the `pg_live` fixture: mount through `repark.toml`, `spark.read.jdbc` / `format("postgres")`
    with `dbtable` and `query`, the partitioned-argument refusal, DDL and DML refusals through both doors,
    `ping()`, §5.4 and §5.7.
  - The docs' "prove co-collected" rule holds: the ledger records the live run with `REPARK_PG_URL` set and
    both cell counts.
- **Not runnable in the container:**
  - **A successful TLS handshake.** The C-0 image runs without a server certificate, so the
    `verify-full`-succeeds cell needs a TLS profile in `scripts/dev/pg/compose.yaml`, a C-0 amendment (D-M8).
    C-2 pins the rustls configuration (roots plus `sslrootcert`, host-name verification on) with a unit pin,
    and the refusal against a plaintext server live.
  - **Spark oracle values.** These need pgjdbc in the oracle environment (D-M2).

## 7. Slices

**The plan:** four slices, one commit each, in **two PRs**.

- **PR-1 = C-2a + C-2b:** the crate gains decode, settings, TLS, pool and the read path, and adds its driver
  dependencies. The supply-chain review sees the dependency change alone.
- **PR-2 = C-2c + C-2d:** the provider, pushdown, EXPLAIN, the mount, both stubs and the Python door. This is
  the user-visible change.

Each slice runs on Claude Opus 5.5 with a scoped Opus 5.5 verifier, per the owner's 2026-10-05 ruling for 1.6
work. If that window has closed by then, the orchestrator re-assigns. Line ceilings are hard limits for each
file (all are under the 1000-line default of `check_rust_file_size.py`). Commit subjects carry
`(1.6)` and the `Authored-By:` trailer.

**Gates common to every slice:**

- `cargo test -p repark-connect` (plus `-p repark-core` from C-2d);
- `cargo clippy -p <touched> --all-targets -- -D warnings` and `make rust-clippy`;
- `cargo fmt --check`;
- `make rust-panic-ban`;
- `python3 scripts/check_rust_file_size.py`;
- `./scripts/check_lib_rs.sh`;
- `python3 scripts/sync_map_md.py --check`;
- `bash scripts/check_map_md.sh --base origin/main`;
- `python3 scripts/check_docs_links.py`;
- `python3 scripts/check_ledger_grammar.py`;
- `python3 /tmp/oc-worker/_lib/comment_ban.py <clone> origin/main HEAD` → `hits=0`.

Measurements and live cells run under `flock /tmp/oc-worker/build-slots/opus-cargo.lock systemd-run --user
--slice=repark.slice --pipe --wait --collect …`, and the container under `make pg-up` / `pg-down` in the
foreground, while the machine rule holds.

**Halt rules for every slice:**

- The order's H-SKETCH, H-C1 and H-SEM.
- **H-GATE:** a gate needs a change outside the slice's file list. Hand back.
- **H-ORACLE:** a D-M2 measurement contradicts §2.7 or §3 on a *guarantee*. On a surface type, follow the
  measurement and file the row.

### C-2a — decode and the type map (pure; no driver, no network)

| file | action | ceiling |
|---|---|---|
| `crates/repark-connect/src/error.rs` | new: `ConnectError`, `Result`; folds C-1's two enums | 260 |
| `crates/repark-connect/src/settings.rs` | edited: errors move to `ConnectError` (no behaviour change) | 160 |
| `crates/repark-connect/src/types/postgres.rs` | edited: the rows of §2.7, the `ColumnAppender` dispatch, `decode` as a wrapper | 520 |
| `crates/repark-connect/src/types/postgres/numeric.rs` | new: the numeric codec and the rounding | 300 |
| `crates/repark-connect/src/types/postgres/temporal.rs` | new: date, timestamp, timestamptz and the epoch shift | 260 |
| `crates/repark-connect/src/types/postgres/text_like.rs` | new: uuid, json, jsonb, the text-rendered types | 180 |
| `crates/repark-connect/src/copy_binary.rs` | new: the §2.6 state machine, batches, the memory charge | 480 |
| `crates/repark-connect/src/lib.rs` | edited | 40 |
| `crates/repark-connect/tests/it/copy_binary.rs` | new: §5.1's stream pins | 700 |
| `crates/repark-connect/tests/it/postgres_types.rs` | edited: the anchors (341 lines today) | 900 |
| `crates/repark-connect/tests/it/settings.rs`, `tests/it/main.rs` | edited: the error type; `mod copy_binary;` | — |
| the crate's `map.md`, `src/map.md`, `src/types/map.md`, `tests/it/map.md`; a new `src/types/postgres/map.md` | lockstep | — |
| `docs/spark-sql-iceberg-parity.md` | edited: retire 8 rows, add the four C-2a rows of §4 | — |
| `task/ledgers/staging/c-2-ledger.md` | new: clauses, mutations, FL-1…FL-14 copied as dated rows, R-7 citations; linked from that directory's `map.md` | — |

- **Gates:** the common set. `Cargo.toml` gains only `datafusion`'s `execution` and memory types if the memory
  charge needs them; otherwise nothing.
- **Slice halt rule H-ARROW:** arrow-rs refuses a `Decimal128(p,s)` the table needs (`s > p` is already
  refused). Hand back.

### C-2b — the connection: settings keys, TLS, pool, timeouts, the COPY read (driver behind `postgres`)

| file | action | ceiling |
|---|---|---|
| `crates/repark-connect/Cargo.toml`, root `Cargo.toml` (`[workspace.dependencies]`), `Cargo.lock` | edited: §2.1's dependencies and the `postgres` feature | — |
| `crates/repark-connect/src/settings/postgres.rs` | new: §2.3's keys, aliases, URL parsing, redaction | 560 |
| `crates/repark-connect/src/ident.rs` | new: `PgIdent`, `QualifiedRelation`, quoting | 200 |
| `crates/repark-connect/src/tls.rs` | new: the rustls config | 200 |
| `crates/repark-connect/src/pool.rs` | new: `QueryPool`, `PooledClient` | 450 |
| `crates/repark-connect/src/read.rs`, `src/read/postgres.rs` | new: the startup pins, the transaction, the `set_config` carriage, the COPY stream, the timeouts | 20 + 600 |
| `crates/repark-connect/src/discover.rs` | new: the relation and query discovery of §2.8 | 380 |
| `crates/repark-connect/tests/it/settings.rs` | edited: §5.5 | 600 |
| `crates/repark-connect/tests/it/live_pg.rs` | new: §5.6 and the server-generated round trips | 900 |
| `crates/repark-connect/tests/it/tls.rs` | new: the config pin | 200 |
| maps; the registry's C-2b rows; the ledger | lockstep | — |

- **Gates:** the common set, plus:
  - `cargo deny check` (D-M1);
  - `cargo build -p repark-connect --no-default-features` (the pure core builds without the driver);
  - the live cells under `make pg-up`.
- **Slice halt rules:**
  - **H-DENY:** a new dependency fails `cargo-deny` (licence, advisory, ban, source). Hand back the output;
    never edit `deny.toml`.
  - **H-CRYPTO:** TLS would build a second rustls crypto provider. Hand back with `cargo tree -e features`.
  - **H-TYPEMOD:** the driver exposes no type modifier for `query` mode. Not a halt: map as unconstrained and
    record it (D-M7).

### C-2c — the provider, pushdown and EXPLAIN (repark-connect only)

| file | action | ceiling |
|---|---|---|
| `crates/repark-connect/src/provider.rs`, `src/provider/catalog.rs`, `src/provider/schema.rs`, `src/provider/table.rs`, `src/provider/scan.rs` | new: `PostgresSource`, the catalog and schema providers, `TableProvider` (`scan`, `supports_filters_pushdown`), `PostgresScanExec` with `DisplayAs` and metrics, `WallClockLocaliser` | 20 + 220 + 260 + 420 + 460 |
| `crates/repark-connect/src/pushdown.rs` | new: §2.9's classifier and renderer | 820 |
| `crates/repark-connect/tests/it/pushdown.rs` | new: §5.2's unit halves | 900 |
| `crates/repark-connect/tests/it/explain.rs` | new: §5.3 over a provider whose resolution is injected (no network) | 320 |
| `crates/repark-connect/tests/it/live_pg.rs` | edited: §5.2's live halves (split into `live_pushdown.rs` if it would pass 900) | 900 |
| maps; the registry's C-2c rows; the ledger | lockstep | — |

- **Gates:** the common set, plus the live cells.
- **Slice halt rule H-DF:** DataFusion 54.1 does not pass `Inexact` filters to `scan`, or does pass `limit`
  while a filter remains above the scan (D-M6). Hand back with a minimal reproduction; the EXPLAIN boundary
  depends on this.

### C-2d — the mount, both stubs retired, the Python door, the federated cells

| file | action | ceiling |
|---|---|---|
| `crates/repark-core/Cargo.toml` (+ `postgres` feature), root `Cargo.toml` if needed, `Cargo.lock` | edited: the edge | — |
| `scripts/check_crate_dag.py`, `scripts/map.md` | edited: the `ALLOWED_EDGES` row restored with its reason verbatim (R-14) | — |
| `crates/repark-core/src/catalog_state.rs` | edited: `SourceMount: SessionExtension` (478 lines today) | 640 |
| `crates/repark-core/src/named_sources.rs` | edited: the mount call, `postgres_catalog_names`, the read-only DDL text, live `ping`, URL redaction in `SourceRow` | 380 |
| `crates/repark-core/src/session/read_postgres.rs` | new | 260 |
| `crates/repark-core/src/session/zone_localiser.rs` | new: `WallClockLocaliser` over `runtime_zone` | 160 |
| `crates/repark-core/src/named_sources/tests.rs`, `config_file/tests` | edited: CFG-2's assertions unchanged; the Postgres refusal pins become mount pins | — |
| `crates/repark-python/src/session.rs` | edited: `read_postgres` live; the deferred text | — |
| `crates/repark-python/src/session_sources.rs` | edited: async `ping` | — |
| `python/repark-parity/tests/live_db/test_c2_read.py`, `test_c2_federated.py`, `test_c2_credentials.py` | new | 400 each |
| `docs/guide/repark-toml.md` | edited: the keys, `sslmode`, the three `GRANT`s | — |
| `docs/spark-sql-iceberg-parity.md` | edited: IO-JDBC-1 rewritten, the C-2d rows | — |
| every touched directory's `map.md`; the ledger closes | lockstep | — |

- **Gates:** the common set, plus:
  - `cargo test -p repark-core -p repark-connect -p repark-python`;
  - `./scripts/check_crate_dag.sh` (the `core → connect` edge now real, with no stale row);
  - `./scripts/check_manifest.sh`;
  - `make develop`, then the facade suite (`make py-test`) and the live Python cells under `make pg-up`.
- **Slice halt rules:**
  - **H-EXT:** mounting through `SessionExtension::register` needs a builder change beyond `catalog_state.rs`
    and `named_sources.rs`, for example because the single extension slot conflicts with
    `repark_spark::SparkExtension`. Hand back. Do not move the registration point CFG-2 D-4 names.
  - **H-TZ:** core has no zone conversion that the localiser can stand on. Map `timestamp` only under
    `prefer_timestamp_ntz`, keep the default LTZ path refusing with `CONNECT-DIV-pg-timestamp-zone`'s fix
    text, and record it. This is a fallback, not a halt.
  - **H-CFG2:** a CFG-2 assertion would have to change, beyond the Postgres refusal pins that this card
    retires by name. Hand back.

**The order's hand-back after C-2d:** `{"unit":"C-2","tests":"","live":"","dag":"","halt":null}`, plus the
ledger with the R-7 citations.

## 8. Deferred measurements (none ran: the box was held by a timing run until `ALL-DONE`)

| id | measurement | when | decides |
|---|---|---|---|
| D-M1 | `cargo deny check`, `cargo tree -d`, `cargo tree -e features -i rustls` on the C-2b dependency set | C-2b | H-DENY / H-CRYPTO; the ES-9 posture of §2.1 |
| D-M2 | Spark 4.1.2 oracle cells, reading the C-0 container through pgjdbc. Types reported for: `time`, unconstrained `numeric`, `numeric(50,10)`, `timestamp` (default and `preferTimestampNTZ`), `timestamptz`, `interval`, `uuid`, `json`, `jsonb`, an enum, a domain. Values: `±infinity` dates and timestamps, `NaN` numeric, DST gap and overlap wall clocks (2024-03-10 02:30 and 2024-11-03 01:30 America/New_York), a mixed-case table name. `pushDownLimit`'s default. Unknown-option handling. **Prerequisite:** the oracle environment can load the PostgreSQL JDBC driver; if it cannot, that is a hand-back from the slice, not a guess. | before C-2a flips `time`; the others before their rows are dated | the `time` row; FL-2, FL-4, FL-5, FL-6, FL-9 confirmations; the Spark halves of every §4 row |
| D-M3 | `EXPLAIN` on the server for a pushed `id = current_setting('repark.p0')::int4` over an indexed table of 1M rows: index scan or not, and the estimate | C-2c, live | Q2 |
| D-M4 | Decode throughput, rows per second, on a release build over a 10M-row table with mixed types, against ConnectorX's `read_sql` on the same container | C-2b (first number); C-3 owns the gated benchmark | C-3's factor baseline |
| D-M5 | Build time and wheel size with and without `postgres` (the CC-5 style) | C-2b | recorded for CC-5 |
| D-M6 | DataFusion 54.1: `Inexact` filters reach `scan`; `limit` is withheld while a filter remains above | C-2c, a unit test | H-DF |
| D-M7 | Whether `tokio_postgres::Column` exposes `RowDescription`'s type modifier | C-2b, compile | H-TYPEMOD |
| D-M8 | A TLS-enabled C-0 profile, so the `verify-full` handshake succeeds live | a C-0 amendment after C-2 | closes §6's TLS gap |

## 9. Filed for the owner (non-halting; each is acted on under its lean)

| id | question | premise | lean (acted on) |
|---|---|---|---|
| Q1 | `±infinity` dates and timestamps: keep refusing, or map them as Spark 4.1.2 does once D-M2 measures it? | Card 1.6's "declare, never approximate" versus Spark's surface rows. | Refuse. Switching later is additive and reversible. |
| Q2 | Values inside COPY are bound through transaction-local `set_config` / `current_setting` (keeps R-2 and NS §5). Is the planner's generic estimate acceptable, or may a vetted literal renderer replace the carriage if D-M3 shows bad plans? | COPY takes no bind parameters; NS §5 forbids SQL built by concatenation on a product path. | Keep the carriage; bring D-M3's plan to the owner only if it loses the index. |
| Q3 | Unconstrained `numeric` rounds HALF_UP to scale 18 (Spark and Flink both do), or refuse a value with more than 18 fractional digits? | Matching Spark's rows versus the card's no-approximation rule. | Match Spark; the dated row states the rounding. |
| Q4 | Pushed text comparisons use `COLLATE "C"` (engine semantics), so RePark's rows can differ from Spark's own JDBC pushdown under a non-`C` collation. Keep? | Card 1.6's ruled hand-back rule outranks the engines' behaviour. | Keep; `CONNECT-DIV-pg-text-collation`. |
| Q5 | `sslmode` accepts only `verify-full` (default) and `disable`; `require` refuses. Keep? | NS §5's TLS-by-default; `require` without a host-name check is open to a man in the middle; some cloud strings ship `sslmode=require`. | Keep; the refusal names `sslrootcert` as the fix. |
| Q6 | Unknown `read_postgres` properties refuse, where Spark forwards them to the driver. Keep? | NS §5's `deny_unknown_fields` versus §2's "Spark wins the surface". | Keep; `CONNECT-DIV-pg-unknown-option`. |

**Ruled 2026-10-06 21:45 EDT (Frontier, for the owner).** Q1–Q6 are ratified on their leans. On Q1,
D-M2 (#974) measured Spark 4.1.2 over pgjdbc 42.7.13 returning sentinels for `±infinity`: dates
`9999-12-30` / `0001-01-02`, timestamps `9999-12-31 18:59:59.999` / `0001-01-02 19:00`. These are pgjdbc
zone artefacts, not values. RePark keeps refusing per value, and the dated row quotes the sentinels.

## 10. The ledger's R-7 citations (ConnectorX and ADBC)

- **ConnectorX shaped the read path.** Its PostgreSQL source reads `COPY (query) TO STDOUT WITH BINARY` through
  the rust-postgres family and writes each value into a typed Arrow destination column. C-2 keeps the protocol
  and the typed destination. It replaces ConnectorX's per-row parse through the driver's `BinaryCopyOutRow`
  with per-column appenders over the raw stream, so there is no per-row allocation (§2.6). ConnectorX's partitioned reads (a partition column split by a min/max query)
  are C-3's, and its `read_sql` is D-M4's and C-3's benchmark bar. **Where C-2 departs:** ConnectorX renders
  values into the query text, and C-2 binds them (§2.4).
- **Arrow ADBC shaped the type contract and the error discipline.** C-1's Arrow types came from the ADBC
  PostgreSQL driver's documented type-mapping table. C-2 keeps them for the ten C-1 rows. Per its
  documentation, ADBC reads results through `COPY … (FORMAT binary)` and executes a statement with bound
  parameters as a prepared statement. C-2 keeps COPY for both (R-2) and binds through §2.4's carriage instead.
  The sketch read the documentation and ran nothing. **Where C-2 departs:** for `numeric`, `interval` and
  `uuid`, ADBC's documented Arrow types (string, month-day-nano, fixed-size binary) are replaced by the types
  Spark's dialect surfaces, because Spark governs the surface (NS §2) and RePark answers Spark. Each departure
  is a §2.7 row with its citation. ADBC's cursor contract is met by the one-partition stream: one statement,
  one snapshot, batches until the trailer, a typed error otherwise.

## Pointers

- Up: [map.md](map.md)
- The order: [c-2-postgres-read.md](c-2-postgres-read.md) · C-1: [c-1-connect-skeleton.md](c-1-connect-skeleton.md),
  [c-1-ledger.md](../ledgers/staging/c-1-ledger.md) · C-0: [c-0-postgres-harness.md](c-0-postgres-harness.md)
- The crate: [crates/repark-connect/map.md](../../crates/repark-connect/map.md) · the identity:
  [source.rs](../../crates/repark-common/src/source.rs)
- Rules: [the North Star](../roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md),
  [contracts](../roadmap/epic-term/contracts-ahead-of-code-2026-10-01.md),
  [testing](../../docs/testing.md), [the registry](../../docs/spark-sql-iceberg-parity.md)

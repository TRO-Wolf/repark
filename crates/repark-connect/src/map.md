# map — repark-connect/src

## Purpose

Product code for `repark-connect`. See [../map.md](../map.md).

## Contents

- `partition.rs` — **C-3 (2026-10-07), the planning slice**, no feature gate and no I/O
  ([c-3-ledger.md](../../../task/ledgers/staging/c-3-ledger.md) §0.1). Spark's JDBC range
  partitioning as numbers.
  - **`PartitionOptions`** holds the four values a door was handed (`column`, `lower_bound`,
    `upper_bound`, `num_partitions`). `with_props(&mut props)` lifts the four Spark spellings
    (`PARTITION_COLUMN_KEY`, `LOWER_BOUND_KEY`, `UPPER_BOUND_KEY`, `NUM_PARTITIONS_KEY`) out of
    a property map, matching case-insensitively as Spark's option map does: a bound that is
    not an `i64` refuses naming the option and never the value, a spelling given twice or
    beside an explicit argument is a conflict, and `predicates` stays declared.
    `spec()` applies Spark's rule: a column, or either bound, needs all four; `numPartitions`
    alone is no partitioning. It answers `Option<PartitionSpec>`.
  - **`stride_cuts(lower, upper, num_partitions)`** is `JDBCRelation.columnPartition`'s
    arithmetic and answers the cuts, the upper bound of every stride but the last. A count
    of one or less, or equal bounds, is no cut at all (checked before the bounds' order, as
    Spark does); reversed bounds refuse in Spark's words; a count above the span shrinks to
    the span. The cuts are computed as Spark computes them: each bound divided by the count
    under `MathContext.DECIMAL128` (34 significant digits, half-even) and set to scale 18,
    the difference truncated to the stride, the first cut shifted by half the strides the
    truncation lost (half-up), and `i64` additions that wrap as the JVM's do. `Decimal` is
    the small decimal that arithmetic needs, over Arrow's `i256` with checked operations.
    Cuts that are not strictly increasing refuse (`PartitionRefusal::Strides`), so no
    arithmetic surprise can duplicate or drop a row.
  - **`strides(&cuts)`** turns cuts into `Stride { lower, upper }` pairs: the first open
    below, the last open above, each cut the upper bound of one and the lower bound of the
    next.
  - **`PartitionRefusal`** is the reason enum of `ConnectError::PartitionedRead`. Its
    messages are Spark's where Spark was measured. `error.rs` folds it by Spark's class:
    `Incomplete`, `Reversed`, `QueryOption` and `Strides` to `Config` (Spark's
    `IllegalArgumentException`), `NotInteger` to `NumberFormat`, `ColumnNotFound`,
    `AmbiguousColumn` and `ColumnType` to `Analysis`, and `DeclaredColumnType`, which names
    `PARTITIONED_READ_ROW`, to `NotImplemented`.
  pins: c-3/C-002, C-003
- `lib.rs` — `mod copy_binary; mod error; mod ident; mod settings; mod types;`, plus
  `mod discover; mod pool; mod read; mod tls;` under the `postgres` feature, and the
  re-exports: `BatchLimits`, `COPY_SIGNATURE`, `CopyBinaryDecoder`, `DEFAULT_BATCH_BYTES`,
  `DEFAULT_BATCH_ROWS`, `MAX_BATCH_BYTES`, `MAX_FIELD_BYTES`; `ConnectError`, `ProtocolViolation`, `Result`, `UNMAPPED_ROW`,
  `ValueRefusal`; `IdentRefusal`, `MAX_IDENT_BYTES`, `PgIdent`, `QualifiedRelation`;
  `AUTH_METHOD_KEY`, `AuthMethod`, `ConnectionSettings`, and the Postgres settings surface
  (`PostgresSettings`, `SettingsDoor`, `SslMode`, `DeclaredSetting`, `Spelling`, `SpecRefusal`,
  `UrlViolation`, `POSTGRES_KEYS`, `POSTGRES_ALIASES`, `POSTGRES_DRIVER`, `DEFAULT_PORT`,
  `redact_source_prop`); under `postgres`, `Connect`, `PgConnection`, `PoolConnection`,
  `PoolLimits`, `PooledClient`, `PostgresConnector`, `PostgresPool`, `QueryPool`,
  `TimeoutSetting`, `query_config`, `within`, `TlsFailure` and `verify_full_config`; since
  C-2b round 3, `DEFAULT_SCHEMA` and, under `postgres`, the discovery surface (`discover`,
  `ResolvedSource`, `ScanColumn`, `ScanSource`, `CastType`, `ColumnCollation`, `Privilege`,
  `check_server_version`, `MIN_SERVER_VERSION_NUM`, `SERVER_VERSION_ROW`, `BEGIN_DISCOVERY`,
  `QUERY_ALIAS`) and the read surface (`scan`, `ScanRequest`, `ScanStatement`, `ScanOptions`,
  `CompareOp`, `ParamSlot`, `MAX_PARAM_SLOTS`, `BEGIN_SCAN`); and the `postgres` module. C-2c
  (2026-10-07) adds `mod provider; mod pushdown;` under `postgres` and re-exports the provider
  (`PostgresSource`, `PostgresCatalog`, `PostgresSchemaProvider`, `PostgresTable`,
  `PostgresScanExec`, `WallClockLocaliser`, `LISTING_ROW`), the classifier (`Pushdown`,
  `Rendered`, `ColumnClass`, `MAX_IN_LIST`, `MIN_POSTGRES_DAYS`, `MAX_POSTGRES_DAYS`,
  `TEXT_COLLATION`, `UTF8_ENCODING`, `decimal_text`, `date_text`, `timestamp_text`) and
  `ScanMeter` and `scan_metered`. C-3 (2026-10-07) adds `mod partition;` (no feature gate) and
  re-exports `PartitionOptions`, `PartitionSpec`, `PartitionRefusal`, `Stride`, `stride_cuts`,
  `strides`, the four option-key constants and `PARTITIONED_READ_ROW`.
- `error.rs` — **C-2d (2026-10-07):** `ValueRefusal::WallClockGap` and `WallClockOverlap`
  (registry row `ZONE_ROW`, `CONNECT-DIV-pg-timestamp-zone`, the message naming
  `prefer_timestamp_ntz`), `DDL_ROW` (`CONNECT-DECL-pg-ddl`) and `read_only_ddl(source)`, all
  outside the `postgres` feature so core's guard needs no driver; `lib.rs` re-exports them.
  pins: c-2/C-098, C-101
  **Fold 1:** `ValueRefusal::TimestampPastCalendar` (`CONNECT-DECL-pg-out-of-range`) is the
  placement's refusal when a wall clock's instant falls past chrono's calendar, so it is never
  reported as a gap. pins: c-2/C-111
- `error.rs` — C-2a (2026-10-06; sketch [c-2-design.md](../../../task/wo/c-2-design.md) §2.2,
  NS-15). The crate's one error enum, `ConnectError` (`thiserror`), and
  `Result<T> = std::result::Result<T, ConnectError>`. C-1's `SettingsError` and `TypeMapError`
  fold into it: their six variants (`InvalidAuthMethod`, `DeclaredAuthMethod`, `Declared`,
  `ArrowType`, `WireLength`, `InvalidUtf8`) keep their names, fields and message text, so the
  registry rows only rename the type. C-2a adds `UnmappedType` (a column outside the map, at
  resolution), `UnrepresentableValue` (a value its Arrow type cannot hold, with the
  `ValueRefusal` reason enum whose `registry_row()` names the row), `EncodeNotBuilt`,
  `Protocol` (with the `ProtocolViolation` enum: a malformed COPY stream), `Disconnected` (the
  stream ended before its trailer) and `Arrow` (a batch Arrow would not build). Reasons are
  enums, never strings a caller matches. The fold into `repark_common::Error` is by class:
  `InvalidAuthMethod` → `Config` (IllegalArgument); the declared variants → `NotImplemented`
  (Unsupported); the operational ones → `DataFusion` (the class they reach Python with until
  C-6 lands the CC-4 classes). The connection variants of §2.2 (auth, TLS, timeouts, pool,
  permissions, server errors) arrive with their producers in C-2b, and so does the source name:
  the decoder is pure and knows columns, not sources. The C-2a F-1 fold (2026-10-06) adds
  `ProtocolViolation::FieldTooLong { length, max }`: a field length past the Postgres maximum,
  refused at the length word before any allocation. `WireLength` did not fit the case: its
  `expected` is an exact codec width, not a maximum. C-2b round 1b (2026-10-07) adds the
  settings and identifier variants: `InvalidSpecification { key: Spelling, reason: SpecRefusal }`
  and `InvalidIdentifier { reason: IdentRefusal }` fold to `Config`, `DeclaredSetting { key,
  declared: DeclaredSetting }` to `NotImplemented`. Each carries the spelling the user gave and
  an enum reason, never a value. C-2b round 2 (2026-10-07) adds the connection variants, each
  under `#[cfg(feature = "postgres")]` because only the driver produces them: `TlsRequired`,
  `TlsHandshake { kind: TlsFailure }`, `Unreachable { kind: io::ErrorKind }`, `Timeout { which:
  TimeoutSetting }`, `PoolExhausted { waited }`, `AuthenticationFailed` and `Server { sqlstate,
  message }` (the server's own text). All fold to `DataFusion`. Their reason enums live with
  their producers (`tls.rs`, `pool.rs`), and `ProtocolViolation` moves to `copy_binary.rs`,
  re-exported here so its path is unchanged. C-2b round 3 (2026-10-07) adds, under
  `postgres`, `PermissionDenied { relation, privilege: Privilege }` and `RelationNotFound {
  relation }` (both `DataFusion`) and `DeclaredServerVersion { server_version_num }`
  (`NotImplemented`, row `CONNECT-DECL-pg-server-version`). The file is at its 260-line
  ceiling, so the server-version message is one line. The C-2a open items fold 1 (2026-10-07)
  adds `FieldBuffer`, folded to `DataFusion`: the allocator refused the carry a straddling
  field needs. It carries no value, and it takes the file to 264, four lines past the sketch
  ceiling (the 1000-line gate is the mechanical one).
  pins: c-2/C-001, C-015, C-018, C-025, C-037, C-048, C-093
- `copy_binary.rs` — **C-2d fold 1 (2026-10-07), N4:** a value refusal (`ConnectError::UnrepresentableValue`)
  in a batch that already holds rows does not discard them. `refuse_after_kept_rows` slices
  every builder to the rows before the refused tuple, so the half-appended tuple is dropped. It
  returns those rows as a batch and poisons the decoder with the refusal, which the next
  `decode` or `finish` returns. A refusal in a batch's first row, and every protocol error,
  still fail at once. `recycle_carry` clears the carry after a carried field, dropping its
  buffer past the byte cap, so `run` stays under clippy's line limit. pins: c-2/C-114
- `copy_binary.rs` — C-2a (2026-10-06; sketch §2.6). `CopyBinaryDecoder`, the resumable state
  machine over `COPY … TO STDOUT (FORMAT BINARY)` chunks, independent of how the server or TLS
  cuts the stream: `Header → HeaderExtension → TupleStart → FieldLength(i) → FieldValue(i, n)
  → Done`. The header is the 11-byte `PGCOPY\n\377\r\n\0` signature, the flags word (bit 16,
  OIDs, refuses; bits 17–31 refuse; bits 0–15 are ignored, as the docs direct) and the
  extension, skipped. Each tuple's field count must equal the planned column count (an empty
  projection counts rows with field count `0`); a field length of `-1` is NULL (refused in a
  planned NOT NULL column), any other negative length is malformed. The `ff ff` trailer ends
  the stream and any byte after it refuses; `finish()` before the trailer is `Disconnected`.
  `decode(&mut &[u8])` consumes the chunk and returns at most one `RecordBatch`, leaving the
  rest of the chunk with the caller, so the C-2b stream yields one batch per step. A field
  inside the chunk decodes from a borrowed slice; only a field or a fixed header word that
  straddles a chunk boundary copies into the one reusable carry buffer. Batches flush at
  `BatchLimits::rows` (the session batch size, default 8192) or at `bytes` of builder growth
  (default 64 MiB), whichever comes first; a tuple is never split, so one value larger than the
  cap forms a one-row batch. `buffered_bytes()` is the memory-charge seam, but nothing in
  production reads it yet: the C-2c scan resizes its `MemoryReservation` to each emitted
  batch's array size, after the batch exists, so neither the builders nor the carry are
  charged to the pool before they allocate; charging the decoder is a C-3 follow-up. The C-2a F-1
  fold (2026-10-06) bounds the builders against Arrow offset overflow: a field length past
  `MAX_FIELD_BYTES` (1 GiB, the largest field Postgres sends) refuses with
  `ProtocolViolation::FieldTooLong` at the length word, before any allocation or carry; and
  `BatchLimits::new` saturates the byte cap at `MAX_BATCH_BYTES` ((1 << 30) - 1). A batch
  flushes at the first tuple reaching the cap, and the appender reports at least the stored
  bytes, so one column holds under (cap - 1) + `MAX_FIELD_BYTES` = 2^31 - 2, strictly below
  `i32::MAX`. The -1 sits in the batch cap so the per-value bound stays the round Postgres
  ceiling. The C-2a F-4 fold (2026-10-06) bounds the carry: when a straddling field completes,
  a carry whose capacity exceeds the batch byte cap is dropped for a fresh `Vec`, while a
  smaller one stays cleared for reuse; `buffered_bytes()` reports builder bytes plus the carry
  capacity, so the reservation seam sees the retained allocation. The C-2a F-5 fold
  (2026-10-06) charges what the builders hold: `ColumnAppender::arrow_width` is the one width
  table (bool 1, int2 2, int4/float4/date 4, int8/float8/timestamps 8, numeric 16), charged for
  values and NULLs; variable values charge stored bytes plus 4 offset bytes (`jsonb` the
  stripped body, `uuid` the 36 rendered bytes) and NULLs the 4 offset bytes; validity charges
  ceil(buffered rows x columns / 8), counted in the flush test and `buffered_bytes()`. The
  C-2a F-7 fold (2026-10-06) poisons the decoder: the first error from `decode` or `finish`
  is kept, and every later `decode`, `finish` or flush answers it without reading input;
  `finish` takes `&mut self`. Since C-2b round 2 (2026-10-07) the file also holds
  `ProtocolViolation`, the decoder's reason enum, with its `Display`; C-2b round 3 adds
  `UnexpectedResponse`, a driver answer the read path cannot use. The C-2a open items fold 1
  (2026-10-07) grows the carry as bytes arrive, fallibly, in `grow_carry`. When a chunk's
  bytes for a straddling field do not fit the carry, it grows to the declared length or to the
  larger of what it must now hold and twice its capacity, whichever is smaller, through
  `try_reserve_exact`; a refusal is `ConnectError::FieldBuffer`, never an abort. A declared
  length alone allocates nothing: the first growth is what arrived, a field whose length word
  ends a chunk holds nothing, and growth never passes the declared length, so a straddling
  field still peaks at twice its size (the carry and the builder copy). The open items' first
  shape, one up-front reservation of the declared length, let a hostile length word allocate
  1 GiB per scan before any byte arrived, and aborted the process when that failed.
  pins: c-2/C-002, C-003, C-004, C-005, C-006, C-015, C-016, C-017, C-091, C-092, C-093,
  C-094, C-095
- `settings.rs` — C-1 (2026-10-05). `ConnectionSettings::from_props` reads one source's props
  (the core loader's `SourceSpec.props`). It interprets only `auth_method` (R-5, CC-3) and
  carries every other prop through untouched; the interpreted `auth_method` key leaves the
  carried map, so the parsed value has one home. The spelling follows `auto_register`, the only
  multi-word database key. Values (R-13, exact and case-sensitive like the loader's kind
  spellings): absent or `password` connects (ES-1's 1.6 value); `iam_token` and `kerberos` are
  the ES-1 declared refusals, each naming its dated registry row
  (`CONNECT-DECL-auth-iam_token`, `CONNECT-DECL-auth-kerberos`); anything else is CC-4's
  invalid specification and lists the three spellings. `Debug` prints the prop keys only, so a
  `password` value never reaches a log; the core loader's redaction predicate lives in
  `repark-core` and is out of this crate's reach. Since C-2a its errors are `ConnectError`'s
  (no behaviour change). C-2b round 1b (2026-10-07) adds `mod postgres;` and re-exports its
  surface; `ConnectionSettings` keeps its shape and stays the one parser of `auth_method`.
  pins: c-1/C-003, C-004, C-005, C-006
- `settings/` — [settings/map.md](settings/map.md): `postgres.rs`, the Postgres endpoint keys.
- `ident.rs` — C-2b round 1b (2026-10-07; sketch §2.2, NS-14). `PgIdent`, built only by
  `PgIdent::new`, which refuses an empty name, a NUL byte, or more than `MAX_IDENT_BYTES` (63)
  bytes, counted in bytes since Postgres truncates past `NAMEDATALEN - 1`, with
  `ConnectError::InvalidIdentifier` and an `IdentRefusal` reason. It renders (`Display`) only as
  a double-quoted identifier with each embedded `"` doubled; `as_str()` gives the name for exact
  matching (FL-14). `QualifiedRelation { schema, table }` renders as `"schema"."table"`.
  Identifiers are quoted and values bound; nothing is concatenated. C-2b round 3 (2026-10-07)
  adds `QualifiedRelation::parse(dbtable)`: one or two parts split on `.`, each bare (taken
  exactly, FL-14) or double-quoted with `""` for a quote; a single part takes
  `DEFAULT_SCHEMA` (`public`, FL-15); anything else refuses with
  `IdentRefusal::Qualification`. pins: c-2/C-025, C-045
- `tls.rs` — C-2b round 2 (2026-10-07; sketch §2.4), behind `postgres`.
  `verify_full_config(sslrootcert)` builds the rustls `ClientConfig` for `sslmode=verify-full`:
  the system roots (`rustls-native-certs`) plus every certificate in `sslrootcert`, rustls's
  WebPKI verifier (chain and host name), no client certificate, and the `ring` provider named
  through `builder_with_provider` (H-CRYPTO: the workspace compiles rustls with two providers,
  so `ClientConfig::builder()` could not choose). A bad bundle refuses with
  `TlsHandshake { kind }`: `RootCertUnreadable { kind }`, `RootCertInvalid` (no PEM
  certificate, a garbled one, or one rustls cannot take as a trust anchor, even beside a good
  one: fold 1 pins that bundle) or `NoTrustedRoots`;
  the message names `sslrootcert`, never the path. `failure_of` maps a rustls certificate
  refusal to `UntrustedCertificate`, `HostNameMismatch`, `CertificateExpired` or `Handshake`.
  `TrackedTls` wraps `MakeRustlsConnect` and records whether the handshake began, so the
  connector can tell a server that refused TLS from every later failure without reading driver
  text. pins: c-2/C-028, C-029, C-030, C-058
- `pool.rs` — C-2b round 2 (2026-10-07; sketch §2.5, NS-7), behind `postgres`.
  - **`QueryPool<C: Connect>`**, one per mounted source: a semaphore of `pool_max_size` permits;
    `checkout()` waits at most `pool_checkout_timeout_ms` for one (else `PoolExhausted`), reaps
    every idle connection that is closed or idle past `pool_idle_timeout_ms` (dropped outside
    the lock), reuses the most recent survivor, or connects. `idle_count()` is the F-1 seam.
  - **`PooledClient`** derefs to the connection and returns it only through
    `release_clean().await`, which the scan calls after the COPY trailer and `COMMIT`. Since
    fold 1 (X2) it first calls `PoolConnection::reset`; `PgConnection` runs `RESET_SESSION`,
    `DISCARD ALL`'s documented sequence less `DEALLOCATE ALL` (the driver keeps its type-lookup
    statements prepared, and `DISCARD ALL` broke them: `26000` on the next user type) with an
    explicit `RESET ROLE` after `SET SESSION AUTHORIZATION DEFAULT` (fold 2 Z2). Since fold 2
    (Z3) one bound query (`query_typed`, the unnamed statement) lists every
    `pg_prepared_statements` name outside the driver's own `s<N>` form (`^s[0-9]+$`), such as
    one an existing SQL function made with `EXECUTE 'PREPARE …'`, and one batch runs a
    `DEALLOCATE` per name, quoted through `PgIdent`. Then one `pg_settings` read of every startup pin plus `statement_timestamp() =
    transaction_timestamp()`, `current_user` and `session_user`, which must both be the login
    role (`settings.user`), and the count of prepared statements still outside the `s<N>` form,
    which must be `0`. A mismatch, an open transaction, an error or the read timeout drops
    the connection. Its lease holds the permit, the connection task's `AbortHandle` and,
    since fold 1 (X3), a `Canceller`; dropped any other way, the lease fires
    `CancelToken::cancel_query` on a task bounded by `connect_timeout_ms` (the crate's second
    spawn, under `#[expect]`) and aborts the connection task, so the server stops and the
    connection is never reused.
  - **`Connect` and `PoolConnection`** are the seams the pins drive with fakes; the product
    implementation is `PostgresConnector` / `PgConnection` (`PostgresPool` names the pair).
  - **`query_config(settings)`** is the only `Config` the pool builds: host, port, user,
    password, database, `application_name`, `connect_timeout`, keepalives, `ssl_mode`
    (`Disable` or `Require`), and §2.4's session pins in one `-c` options list
    (`client_encoding`, `DateStyle`, `IntervalStyle`, `TimeZone=UTC`, an empty `search_path`,
    `default_transaction_read_only=on`, `lock_timeout`, `statement_timeout` from
    `query_timeout_ms`, `idle_in_transaction_session_timeout` from `read_timeout_ms`, and since
    fold 1 `client_connection_check_interval` = `CONNECTION_CHECK_INTERVAL`, 1000 ms). Nothing
    here sets replication (CC-2).
  - **`PostgresConnector::connect`** bounds the whole connect by `connect_timeout_ms`
    (`Timeout { which: Connect }`), connects with `NoTls` under `disable` and the tracked
    rustls connector otherwise, spawns the driver's connection task (the crate's one spawn,
    under `#[expect]`, its handle held by `PgConnection`), and classifies failures: `Server`
    for a server error, `TlsHandshake` for a rustls refusal or an unverifiable host name,
    `Unreachable { kind }` for I/O, `TlsRequired` when TLS never began under `verify-full`,
    `AuthenticationFailed` for SQLSTATE class `28` (fold 1 X4) and the driver-side rest.
  - **`within(which, limit, work)`** is the NS-7 wrapper round 3 puts around every request and
    COPY chunk; `TimeoutSetting` names the key that fired.
  pins: c-2/C-031, C-032, C-033, C-034, C-035, C-052, C-054, C-055, C-063, C-064
- `discover.rs` — C-2b round 3 (2026-10-07; sketch §2.4, §2.8), behind `postgres`.
  `discover(pool, &ScanSource, read_timeout)` resolves one source afresh on every call (FL-7):
  it checks out a client, opens `BEGIN_DISCOVERY` (`BEGIN READ ONLY` with a 30 s local
  `statement_timeout`), reads `server_version_num` and `server_encoding`, refuses a server
  below `MIN_SERVER_VERSION_NUM` (140000) with `DeclaredServerVersion`, resolves the columns,
  commits and releases. Every request runs under the read timeout through `read/postgres.rs`'s
  `request`.
  - **Relation mode** (`ScanSource::Relation`): one bound catalog query keyed on `nspname` and
    `relname` over `relkind` `r v m f p`. It returns the schema `USAGE` and any-column
    `SELECT` privileges (a missing one is `PermissionDenied`, naming the relation and the
    privilege), then each live column (`attnum > 0`, not dropped, by `attnum`) with
    `attnotnull`, its collation (`ColumnCollation { name, deterministic }`) and its type after
    a recursive walk through domains to the base type, the domain's `typtypmod` standing in
    when the column has none. No row is `RelationNotFound`; a relation with no columns
    resolves empty. A base type outside `pg_catalog` is not a base type here.
  - **Query mode** (`ScanSource::Query`, or a `dbtable` starting with `(`, which becomes
    `SELECT * FROM <dbtable>`): `prepare("SELECT * FROM (<query>) AS repark_q")`, Parse and
    Describe only, after `QUERY_SEARCH_PATH` in the discovery transaction. Since fold 2 (Z1)
    that is one constant catalog statement: it reads the login role's configured `search_path`
    from `pg_db_role_setting` in the server's precedence (role in database, role, database, all
    roles; `session_user` and `current_database()`, no text interpolated), falls back to the
    built-in `"$user", public`, and applies it with `set_config('search_path', …, true)`, which
    is `SET LOCAL`. So unqualified names resolve as a plain session as that role (pgjdbc, so
    Spark's `query`) resolves them; a server-wide `postgresql.conf` path is not seen (the startup
    pin hides it). `ScanSource::search_path()` names that statement for a query and nothing for
    a relation. Each column takes the RowDescription type and `Column::type_modifier()`
    (H-TYPEMOD did not fire), and is nullable.
  - **`ScanColumn::resolve(name, typname, kind, typmod, nullable)`** pairs C-2a's
    `PlannedColumn` with the column's `CastType`. A catalog cell of the wrong type is
    `Protocol(UnexpectedResponse)`. The server encoding is carried on `ResolvedSource`, unused
    so far. pins: c-2/C-042, C-043, C-044, C-056, C-062
- `pushdown.rs` — C-2c (2026-10-07; sketch §2.9), behind `postgres`: the classifier and
  renderer. `Pushdown::new(resolved, schema, pushdown_predicate)` gives each column a
  `ColumnClass` from its mapping, cast and surfaced type: `Boolean`, `Integer`, `Decimal`
  (`rounded` when the Arrow type is not the column's own `(p,s)`, so the operand is cast to the
  Arrow type and Postgres rounds as the decoder does), `Date`, `Timestamp` (NTZ only),
  `Timestamptz`, `Text` (`text`/`varchar` on a `UTF8` server) and `NullTestOnly` (float, `bpchar`,
  `bytea`, the text-rendered types, enums, a placed `timestamp`). `render(filter, base)` gives
  the SQL and the value texts of P-1…P-10 or `None`: null tests on any column; boolean columns
  and the `IS [NOT] TRUE/FALSE/UNKNOWN` tests; `=`, `<>`, `<`, `<=`, `>`, `>=` against a literal
  on either side (flipped), a widening integer or lossless decimal cast on the column dropped, as
  `<operand> OPERATOR(pg_catalog.op) pg_catalog.current_setting('repark.pN')::pg_catalog.<type>`
  (the literal's own width for integers, so `int2 = 100000` binds `int8`); `IS [NOT] DISTINCT
  FROM` as `(… = …) IS [NOT] TRUE`, or the null test against a NULL literal; `IN` of at most
  `MAX_IN_LIST` (256) literals as a disjunction of qualified `=`, a NULL item kept as
  `NULL::<type>`; `BETWEEN` as `>=` and `<=`; `LIKE` with the default escape and a well-formed
  pattern as `OPERATOR(pg_catalog.~~)`; and `AND`, `OR`, `NOT` over children that each render.
  Text compares carry `TEXT_COLLATION` (`COLLATE pg_catalog."C"`). Literal texts are exact:
  `decimal_text` at the literal's scale, `date_text` (` BC` for years ≤ 0), `timestamp_text`
  (six fractional digits, `+00` for `timestamptz`); a temporal literal outside Postgres's range
  (`MIN_POSTGRES_DAYS`…`MAX_POSTGRES_DAYS`) and a text with a NUL byte stay residual. `support`
  answers `Exact` only for a filter that renders **and** that DataFusion's simplifier leaves
  unchanged (`exact`): the optimizer keeps simplifying pushed filters after it has removed them,
  and `qty NOT IN (1, NULL)` became a pushed `qty <> NULL` that later simplified to `NULL`,
  which nothing applied. `split` and `push` serve `scan`. With `pushdown_predicate = false`
  nothing renders; the limit is `pushdown_limit`'s, in `provider/table.rs`. pins: c-2/C-070, C-071, C-072, C-073, C-074, C-078
- `provider.rs` — C-2c (2026-10-07): `mod catalog; mod scan; mod schema; mod table;` and their
  re-exports.
- `provider/` — [provider/map.md](provider/map.md): `PostgresSource`, the catalog, schema and
  table providers, `PostgresScanExec` and `WallClockLocaliser`.
- `read.rs` — C-2b round 3 (2026-10-07): `pub(crate) mod postgres;`.
- `read/` — [read/map.md](read/map.md): `postgres.rs`, the statement builder, the `set_config`
  carriage and the COPY stream.
- `types.rs` — `pub mod postgres;` (`mssql` joins with C-5).
- `types/` — [types/map.md](types/map.md): the Postgres type map and its codecs.

## Pointers

- Up: [../map.md](../map.md)

## Debug

First checks: `cargo test -p repark-connect`. Escalate to: [../map.md#debug](../map.md).

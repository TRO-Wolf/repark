# map — repark-connect/src/read

## Purpose

The read path of each backend that `read.rs` declares as a submodule, behind the `postgres`
feature. See [../map.md](../map.md).

## Contents

- `postgres.rs` — C-2b round 3 (2026-10-07; sketch
  [c-2-design.md](../../../../task/wo/c-2-design.md) §2.4, §2.5, §2.6). The COPY read of one
  resolved source.
  - **`ScanRequest`** over an `Arc<ResolvedSource>` (from `discover.rs`): `project(&[usize])`,
    `compare(column, CompareOp, value)` and `limit(n)`. An out-of-range index or a 1025th value
    answers `None`, so nothing is pushed silently wrong. `statement()` renders `ScanStatement`:
    `COPY (SELECT <col>::<cast>, … FROM <source> [WHERE …] [LIMIT n]) TO STDOUT (FORMAT
    BINARY)`, every column cast to its planned type (`CastType`: `interval`, every `ServerText`
    mapping and enums to `pg_catalog.text`; a constrained `numeric` to its `(p,s)`), an empty
    projection as `SELECT FROM`, and a `query` source as `(…) AS repark_q`. Identifiers render
    through `PgIdent`; every type and function is `pg_catalog`-qualified, and since fold 1 (X6)
    every compare too, as `OPERATOR(pg_catalog.=)` and its siblings, so no user operator (one on
    a domain shadowed the plain `=` under the query search path) can change a pushed result.
  - **The `set_config` carriage (FL-13).** Each pushed value gets a `ParamSlot`
    (`repark.p0` … `repark.p1023`) and is read back as
    `pg_catalog.current_setting('repark.pN')::<the column's unconstrained cast>`; a text-cast
    column compares as text. `set_config_sql()` is one `SELECT pg_catalog.set_config($1, $2,
    true), …` whose names and values are bound parameters, so no value enters SQL text.
  - **`scan(pool, request, ScanOptions)`** is a `try_unfold` stream. Its first poll checks out a
    client and opens `BEGIN READ ONLY` when a value is pushed or the source is a query; a query
    adds `QUERY_SEARCH_PATH` in the same batch (fold 1 X6; since fold 2 Z1 the login role's
    configured path, see `discover.rs`), and a pushed value the bound
    `set_config`; then `copy_out`. Each step awaits chunks and feeds `CopyBinaryDecoder`, yielding at most one
    batch. At end of stream it runs `finish()`, `COMMIT` (when a transaction was opened) and
    `release_clean()`, whose reset and pin check decide whether the client is pooled (fold 1
    X2, X5). Any error, a timeout or a drop ends the stream with the lease abandoned: the server's
    query is cancelled and the client is never pooled (fold 1 X3). `ScanOptions::from_settings` takes `read_timeout_ms` and `batch_rows` (else
    the 8192-row default) under the 64 MiB byte cap.
  - **NS-7.** `request(read_timeout, relation, work)` wraps every request and every COPY chunk in
    `within(TimeoutSetting::Read, …)` and classifies driver errors (`request_error`): `42501`
    with a known relation → `PermissionDenied { Select }`, `55P03` → `Timeout { Lock }`,
    `57014` → `Timeout { Query }`, `25P03` → `Timeout { Read }`, `42P01` with a known relation →
    `RelationNotFound`, class `57P` and a closed or failed socket → `Disconnected`, any other
    server error → `Server`, and any other driver failure → `Protocol(UnexpectedResponse)`.
  - **C-2c (2026-10-07).** `ScanRequest` also carries the classifier's rendered conjuncts:
    `filter(sql, values)` (crate-private, so only `pushdown.rs` writes SQL here) appends one,
    its values taking the next slots, and refuses past `MAX_PARAM_SLOTS`; `bound_values()`
    counts them. `statement()` ANDs the compares and the conjuncts and binds every value in slot
    order. `scan_metered(pool, request, options, ScanMeter)` is `scan` with the provider's
    metrics: `bytes_received` (every chunk), `time_to_first_byte` (from the first poll to the
    first chunk) and `decode` (the time inside `CopyBinaryDecoder::decode`); `scan` passes a
    fresh meter.
  - **C-3 (2026-10-07).** `ScanRequest::stride(column, Stride)` appends one stride of a
    partitioned read as a conjunct after everything already pushed: the first stride is
    `(col OPERATOR(pg_catalog.<) $cut OR col IS NULL)`, a middle one
    `(col OPERATOR(pg_catalog.>=) $low AND col OPERATOR(pg_catalog.<) $high)` and the last
    `(col OPERATOR(pg_catalog.>=) $low)`. Each cut is bound through the carriage and read
    back as `::pg_catalog.int8`, so an `int2` or `int4` column compares through the
    catalog's cross-type operators and a cut outside the column's range never fails a cast.
    A stride open on both sides returns the request unchanged; an unknown column or a cut
    past the slot bound answers `None`. pins: c-3/C-004
  pins: c-2/C-038, C-039, C-040, C-041, C-053, C-054, C-056, C-059, C-062, C-074, C-076

- `postgres.rs` — **C-3 (2026-10-07):** the COPY reader is split so a prepared connection can
  run more than one statement. `ScanRequest::prepared(options)` builds a `Prepared` (the
  statement, its decoder and the relation) before any network call; `Copying::start(pooled,
  prepared, …)` binds the values and opens the `COPY` on a connection it is handed;
  `next_batch()` is unchanged; `into_client()` drops the finished stream and hands the
  connection back. The one-statement scan keeps its behaviour through them: `Copying::open`
  checks out, opens `BEGIN READ ONLY` when a value or a query source needs it, and `finish`
  commits and releases. pins: c-3/C-005
- `postgres_lanes.rs` — **C-3 (2026-10-07)**, the connections of one partitioned scan
  ([c-3-ledger.md](../../../../task/ledgers/staging/c-3-ledger.md) §0.2, §0.3).
  `scan_lanes(pool, strides, max_lanes, options, meter)` prepares every stride, takes between
  one and `min(max_lanes, strides)` connections (`checkout_up_to`), puts all of them on one
  snapshot, and returns one `LaneStream` per connection.
  - **One snapshot.** Every connection opens `BEGIN_SNAPSHOT_SCAN` (`BEGIN ISOLATION LEVEL
    REPEATABLE READ READ ONLY`). With more than one, the first runs `EXPORT_SNAPSHOT` and
    every other runs `SET TRANSACTION SNAPSHOT '<id>'` in the same batch as its `BEGIN`,
    before any query; a query source's search-path statement follows the import, because
    Postgres refuses an import after a query. The id enters the text only after it is checked
    to be hex digits and dashes (a `SET` takes no bound parameter); anything else is
    `Protocol(UnexpectedResponse)`. `scan_lanes` returns only when every import has
    completed, so no `COPY` starts while a connection could still take a later snapshot, and
    the exporter's transaction is open for every import (Postgres forgets the id when the
    exporter ends).
  - **Strides per connection.** Stride `i` goes to connection `i % L`, and each connection
    runs its strides back to back in its one transaction: `Lane::step` starts the next
    `COPY` when the last one ends, then commits. The first connection calls `release_clean`,
    whose reset and pin check return the session to `READ COMMITTED` and the pool; every
    other is closed with `retire`, so a frame holds one idle connection at rest, as a
    one-statement scan's does, and never `pool_max_size` of them. An error or a drop leaves
    the lease abandoned, as a one-statement scan does: the server's query is cancelled and
    the connection is never pooled.
  - `time_to_first_byte` is the first connection's alone; `bytes_received` and `decode` sum
    over every connection.
  pins: c-3/C-005

## Pointers

- Up: [../map.md](../map.md)

## Debug

First checks: `cargo test -p repark-connect --test it read`; live:
`make pg-up`, then `REPARK_PG_URL=… cargo test -p repark-connect --test it live_pg -- --ignored`.

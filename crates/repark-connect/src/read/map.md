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
    through `PgIdent`; every type and function is `pg_catalog`-qualified.
  - **The `set_config` carriage (FL-13).** Each pushed value gets a `ParamSlot`
    (`repark.p0` … `repark.p1023`) and is read back as
    `pg_catalog.current_setting('repark.pN')::<the column's unconstrained cast>`; a text-cast
    column compares as text. `set_config_sql()` is one `SELECT pg_catalog.set_config($1, $2,
    true), …` whose names and values are bound parameters, so no value enters SQL text.
  - **`scan(pool, request, ScanOptions)`** is a `try_unfold` stream. Its first poll checks out a
    client, and only when a value is pushed sends `BEGIN READ ONLY` and the bound `set_config`;
    then `copy_out`. Each step awaits chunks and feeds `CopyBinaryDecoder`, yielding at most one
    batch. At end of stream it runs `finish()`, `COMMIT` (when a transaction was opened) and
    `release_clean()`. Any error or a drop ends the stream with the lease aborted, so the client
    is never pooled. `ScanOptions::from_settings` takes `read_timeout_ms` and `batch_rows` (else
    the 8192-row default) under the 64 MiB byte cap.
  - **NS-7.** `request(read_timeout, relation, work)` wraps every request and every COPY chunk in
    `within(TimeoutSetting::Read, …)` and classifies driver errors (`request_error`): `42501`
    with a known relation → `PermissionDenied { Select }`, `55P03` → `Timeout { Lock }`,
    `57014` → `Timeout { Query }`, `25P03` → `Timeout { Read }`, `42P01` with a known relation →
    `RelationNotFound`, class `57P` and a closed or failed socket → `Disconnected`, any other
    server error → `Server`, and any other driver failure → `Protocol(UnexpectedResponse)`.
  pins: c-2/C-038, C-039, C-040, C-041

## Pointers

- Up: [../map.md](../map.md)

## Debug

First checks: `cargo test -p repark-connect --test it read`; live:
`make pg-up`, then `REPARK_PG_URL=… cargo test -p repark-connect --test it live_pg -- --ignored`.

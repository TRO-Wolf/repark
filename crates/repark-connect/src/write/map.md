# map — repark-connect/src/write

## Purpose

The Postgres write core (C-4 step 1, 2026-10-08), behind the `postgres` feature: the two
carriages a write can take and nothing a door needs to parse. The module root
[../write.rs](../write.rs) holds the request, the selector and the writer. Design and reasons:
[c-4-ledger.md](../../../../task/ledgers/staging/c-4-ledger.md) §1. See [../map.md](../map.md).

## Contents

- `target.rs` — **C-4 fold 1 (2026-10-09).** What the target relation is, read once at `open`
  inside the write's transaction ([c-4-ledger.md](../../../../task/ledgers/staging/c-4-ledger.md)
  §8.2, §8.3).
  - **`TARGET_FACTS`** is one statement over `pg_class`, `pg_rewrite`, `pg_trigger`,
    `pg_partition_tree` and `pg_attribute`, with the schema, the table and the written column
    names bound. It answers the relation kind, `row_security_active`, whether an enabled
    `INSERT` rule exists (`ev_type = '3'`), whether an enabled statement-level `INSERT`
    trigger exists (`tgtype & 5 = 4`: the row bit clear, the insert bit set), whether a
    partition is a foreign table, `has_column_privilege(…, 'INSERT')` over the written
    columns, and the first written column that is a `GENERATED ALWAYS` identity or a
    generated column.
  - **`route(client, request, requested, timeout)`** refuses first (`PermissionDenied
    { Insert }`, `WriteRefused { IdentityAlways | GeneratedColumn }`), then answers the path
    and the reason. The reasons are tried in one fixed order (`View`, `ForeignTable`,
    `InsertRule`, `RowSecurity`, `StatementTrigger`, then the column-type reason), so a
    target with two properties always reports the same one.
  - **`RowFallback`** (public) is that reason; its `Display` is the sentence a door shows.
    The rule behind every variant: where Postgres gives `COPY` and `INSERT` different
    meanings, `INSERT`'s is the contract. A foreign table agreed under `postgres_fdw` and
    still takes rows, because no other wrapper can be measured here.
  pins: c-4/C-017, C-018, C-019
- `postgres_copy.rs` — the bulk path.
  - **`CopyBinaryEncoder`** (public, pure) frames Arrow batches as a
    `COPY … FROM STDIN (FORMAT BINARY)` stream: `batch(&RecordBatch)` checks the batch against
    the planned columns and answers `CopyChunks`, whose `next_chunk()` yields the stream a
    chunk at a time; `finish()` yields the trailer. The 19 header bytes ride in front of the
    first chunk, or of the trailer when no row came. A row is a 16-bit field count, then per
    field a 32-bit length (`-1` for NULL) and the value from `ColumnEncoder`. The length is
    written after the value: four bytes are reserved, the value appended, the length patched,
    so no value is copied twice. A chunk ends at the first row boundary at or past
    `chunk_bytes` and always holds at least one row. A header-only chunk would make no
    progress when `chunk_bytes` is under 19. A refused value names its row counted over the
    whole stream, not within the batch.
  - **`CopyLane`** (crate-private) opens `copy_in` with `WriteRequest::copy_statement()` and
    sends each chunk as one `CopyData` message under the read timeout. `finish` sends the
    trailer and answers the server's row count. The server reports a COPY error only after
    the client's end of data, so a row the server refuses surfaces at `finish`. The sink owns
    a bounded channel to the connection task, so a slow server holds the sender back and
    nothing queues without limit.
  pins: c-4/C-004, C-009
- `row.rs` — the row path.
  - **`WireParam`** is the one `ToSql` type: it writes bytes that are already the type's
    binary wire form and accepts every type, because the encoder chose the form from the
    planned column, not from the driver's type.
  - **`RowLane`** prepares `insert_statement(1)` at `open`, and `insert_statement(n)` on first
    use, where `n` is `rows_per_insert` capped so `n × columns` stays at or under
    `MAX_INSERT_PARAMS` (65 535, the protocol's 16-bit parameter count). `write` encodes each
    row into one buffer (`Pending`: the bytes and one range per field). A full group of `n`
    rows runs the multi-row statement. A buffer that reaches `copy_chunk_bytes` first, and
    the remainder at `finish`, run row by row through the single-row statement, so no `Bind`
    message grows with wide rows and no third statement shape is prepared. Statements run
    one after another, never pipelined: the rows reach the table in input order.
  pins: c-4/C-009, C-010

## Pointers

- Up: [../map.md](../map.md)
- The encoder both lanes share: [../types/postgres/map.md](../types/postgres/map.md)
- Pins: [../../tests/it/map.md](../../tests/it/map.md) (`write.rs`, `live_write.rs`)

## Debug

First checks: `cargo test -p repark-connect --test it write`; with `make pg-up`, add
`-- --include-ignored`. A parity failure prints the `id`s of the diverging rows. Escalate to:
[../map.md#debug](../map.md).

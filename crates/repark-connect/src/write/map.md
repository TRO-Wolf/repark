# map — repark-connect/src/write

## Purpose

The Postgres write core (C-4 step 1, 2026-10-08), behind the `postgres` feature: the two
carriages a write can take and nothing a door needs to parse. The module root
[../write.rs](../write.rs) holds the request, the selector and the writer. Design and reasons:
[c-4-ledger.md](../../../../task/ledgers/staging/c-4-ledger.md) §1. See [../map.md](../map.md).

## Contents

- `target.rs` — **C-4 fold 1 (2026-10-09).** What the target relation is, read once at `open`
  inside the write's transaction ([c-4-ledger.md](../../../../task/ledgers/staging/c-4-ledger.md)
  §8.2, §8.3, narrowed by the measure round §11.2).
  - **`TARGET_FACTS`** is one statement over `pg_class`, `pg_rewrite`, `pg_foreign_table`,
    `pg_foreign_server`, `pg_foreign_data_wrapper`, `pg_partition_tree` and `pg_attribute`,
    with the schema, the table and the written column names bound. It answers the relation
    kind, `row_security_active`, whether an enabled `INSERT` rule exists (`ev_type = '3'`),
    the wrapper name of a foreign table (NULL otherwise), whether a partition is a foreign
    table, `has_column_privilege(…, 'INSERT')` over the written columns, and the first
    written column that is a `GENERATED ALWAYS` identity or a generated column. The
    statement-trigger check the fold carried is gone: COPY fires those triggers exactly as
    one `INSERT` does (measured, §11.1), so they take bulk.
  - **`route(client, request, requested, timeout)`** refuses first (`PermissionDenied
    { Insert }`, `WriteRefused { IdentityAlways | GeneratedColumn }`), then answers the path
    and the reason. The reasons are tried in one fixed order (`View`, `ForeignTable`,
    `InsertRule`, `RowSecurity`, then the column-type reason), so a target with two
    properties always reports the same one.
  - **`RowFallback`** (public) is that reason; its `Display` is the sentence a door shows.
    The rule behind every variant: where Postgres gives `COPY` and `INSERT` different
    meanings, `INSERT`'s is the contract. Only a direct `postgres_fdw` table escapes the
    `ForeignTable` reason: it is the one wrapper measured (§11.1), every other wrapper and
    any foreign leaf keeps rows, and a NULL wrapper name falls back.
  pins: c-4/C-017, C-018, C-019, C-023, C-024
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
    rows runs the multi-row statement. **C-4 fold 2 item 4 (2026-10-10):** a buffer that
    reaches `copy_chunk_bytes` first, and the remainder at `finish`, run as ONE statement
    shaped for the exact pending count (600 rows run 256 + 256 + 88), so statement triggers
    fire once per batch on the row path. Statements run one after another, never pipelined:
    the rows reach the table in input order.
  pins: c-4/C-009, C-010

## Pointers

- Up: [../map.md](../map.md)
- The encoder both lanes share: [../types/postgres/map.md](../types/postgres/map.md)
- Pins: [../../tests/it/map.md](../../tests/it/map.md) (`write.rs`, `live_write.rs`)

## Debug

First checks: `cargo test -p repark-connect --test it write`; with `make pg-up`, add
`-- --include-ignored`. A parity failure prints the `id`s of the diverging rows. Escalate to:
[../map.md#debug](../map.md).

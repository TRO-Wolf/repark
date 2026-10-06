# map — repark-connect/src/types

## Purpose

Remote ↔ Arrow type conversion, one table per backend (card 1.6). See [../map.md](../map.md).

## Contents

- `postgres.rs` — C-1 (2026-10-05), extended by C-2a (2026-10-06; sketch
  [c-2-design.md](../../../../task/wo/c-2-design.md) §2.7). `POSTGRES_TYPES` is the table: one
  `PostgresTypeRow` per Postgres type, keyed by its `pg_type.typname` (`int4`, `bpchar`,
  `timestamptz`, …), carrying the Arrow mapping (`PostgresMapping`, whose `data_type()` is the
  Arrow `DataType`) or `Declared { registry_row }`, and the name of its pin in
  `tests/it/postgres_types.rs`. The codec is Postgres's binary wire form, the per-value payload
  of `COPY … (FORMAT BINARY)`, with `None` for SQL NULL. The matches are exhaustive over the
  mapping, so a new row cannot compile without its codec arm.
  - **Mapped by C-1 (10):** `bool` → `Boolean` (decode follows `boolrecv`: any nonzero byte is
    true); `int2` / `int4` / `int8` → `Int16` / `Int32` / `Int64` (big-endian two's
    complement); `float4` / `float8` → `Float32` / `Float64` (IEEE 754 big-endian, so NaN
    payloads, signed zero and the infinities survive bit for bit); `text`, `varchar` and
    `bpchar` → `Utf8` (UTF-8 validated, never lossy, so the connection's `client_encoding`
    must be `UTF8`, which C-2b pins; `bpchar` keeps its blank padding); `bytea` → `Binary`.
    Each Arrow type is the one the ADBC PostgreSQL driver's documented type-mapping table names
    (C-1's R-10). pins: c-1/C-007
  - **Mapped by C-2a (8), each the type Spark's PostgreSQL dialect surfaces:** `numeric` →
    `Numeric(DecimalTarget)`, `Decimal128(p,s)` resolved from the type modifier; `date` →
    `Date32`; `timestamp` → `Timestamp(Microsecond, None)`, the wall clock (C-2c localises it
    into the session zone for Spark's default `TimestampType`, or keeps it under
    `prefer_timestamp_ntz`); `timestamptz` → `Timestamp(Microsecond, "+00:00")`, the instant;
    `interval` → `ServerText` (`Utf8`: the scan casts it to `pg_catalog.text` under the pinned
    `IntervalStyle`, so the wire is the server's text); `uuid` → `Uuid`, `json` → `Json`,
    `jsonb` → `Jsonb` (all `Utf8`). The codecs live in `postgres/` (its map is under Pointers). Values the
    Arrow type cannot hold exactly refuse per value (`ConnectError::UnrepresentableValue` and
    its `ValueRefusal`, each naming its registry row: `CONNECT-DECL-pg-numeric-special`,
    `-pg-infinite-datetime`, `-pg-out-of-range`). pins: c-2/C-007, C-008, C-009, C-010
  - **Declared (1):** `time`, `CONNECT-DECL-pg-time`, until D-M2 reads Spark 4.1.2's type for a
    JDBC `time` column (the oracle environment cannot load pgjdbc yet). pins: c-2/C-011
  - **Resolution.** `PlannedColumn::resolve(name, typname, PgTypeKind, TypeMod)` turns one
    discovered column into the planned column the decoder consumes: a base type in the table
    maps through its row (`numeric` reads the modifier); an enum reads as its label (`Utf8`);
    anything else (`PgTypeKind::Other`, a base type not in the table, a declared row, a
    `numeric` scale outside `0..=p`) refuses with `ConnectError::UnmappedType`, naming the
    column, the type, the row (`CONNECT-DECL-pg-unmapped` or the declared row) and the fix.
    Domains resolve to their base type in discovery (C-2b) before this call. `TypeMod` is the
    `atttypmod` newtype (NS-14), and `use self::PostgresMapping as Mapping` keeps the table rows
    on one line each. pins: c-2/C-011
  - **`ColumnAppender`** (crate-private), one variant per mapping, each holding its Arrow
    builder (the `each_builder!` macro writes the per-builder arms of `append_null` and
    `finish`, which differ only in the builder's type): `append(&[u8])` decodes one field from a borrowed slice and returns the builder
    bytes it added (the decoder's 64 MiB cap reads them); `append_null`; `finish`. Codec
    failures are a context-free `CodecError` that the caller places with the column and row
    index. `PlannedColumn::decode` and C-1's `PostgresTypeRow::decode` are thin wrappers over
    it (the row wrapper names the column after its type), so C-1's round trips exercise the
    same codec as the stream. pins: c-2/C-012
  - **Encode** stays C-1's ten types; the C-2a mappings answer
    `ConnectError::EncodeNotBuilt`, which names the write path (C-4). pins: c-2/C-011

## Pointers

- Up: [../map.md](../map.md)
- Codecs: [postgres/map.md](postgres/map.md)

## Debug

First checks: `cargo test -p repark-connect --test it postgres_types`. Escalate to:
[../map.md#debug](../map.md).

# map — repark-connect/src/types

## Purpose

Remote ↔ Arrow type conversion, one table per backend (card 1.6). See [../map.md](../map.md).

## Contents

- `postgres.rs` — C-1 (2026-10-05). `POSTGRES_TYPES` is the table: one `PostgresTypeRow` per
  Postgres type, keyed by its `pg_type.typname` (`int4`, `bpchar`, `timestamptz`, …), carrying
  the Arrow mapping (`PostgresMapping`, whose `data_type()` is the Arrow `DataType`) or
  `Declared { registry_row }`, and the name of its pin in `tests/it/postgres_types.rs`. The codec
  is Postgres's binary wire form, the per-value payload of `COPY … (FORMAT BINARY)` that C-2
  decodes, with `None` for SQL NULL: `encode` takes an Arrow array to wire values and `decode`
  takes them back. The match is exhaustive over the mapping, so a new row cannot compile without
  its codec arm.
  - **Mapped (10):** `bool` → `Boolean` (decode follows `boolrecv`: any nonzero byte is true);
    `int2` / `int4` / `int8` → `Int16` / `Int32` / `Int64` (big-endian two's complement);
    `float4` / `float8` → `Float32` / `Float64` (IEEE 754 big-endian, so NaN payloads, signed
    zero and the infinities survive bit for bit); `text`, `varchar` and `bpchar` → `Utf8`
    (UTF-8 validated, never lossy, so the connection's `client_encoding` must be `UTF8`, which
    C-2 sets; `bpchar` keeps its blank padding as the server sends it);
    `bytea` → `Binary`. Each Arrow type is the one the ADBC PostgreSQL driver's documented
    type-mapping table names (R-10); the basis is that document, not a run of the driver.
  - **Declared (9), each with a dated registry row:** `numeric` (unbounded precision and
    `NaN` / `±Infinity` values vs a bounded `Decimal128`/`Decimal256` or ADBC's string),
    `date`, `timestamp`, `timestamptz` (`±infinity` has no Arrow value), `time` (`24:00:00`
    lies outside Arrow's `Time64` day), `interval` (months/days/microseconds into
    `MonthDayNano` overflows at the nanosecond scale), `uuid` (`FixedSizeBinary(16)` vs `Utf8`),
    `json` and `jsonb` (`Utf8` vs the `arrow.json` extension). Card 1.6's hand-back rule:
    declare, never approximate; C-2 extends the table.
  - A type absent from the table answers `None` from `postgres_type`; C-2 decides how a scan
    reports it.
  - Errors (`TypeMapError`): `Declared` (names the row), `ArrowType` (an array of another
    Arrow type), `WireLength` (a fixed-width value of the wrong size, with its row index),
    `InvalidUtf8` (with its row index).
  pins: c-1/C-007, C-008

## Pointers

- Up: [../map.md](../map.md)

## Debug

First checks: `cargo test -p repark-connect --test it postgres_types`. Escalate to:
[../map.md#debug](../map.md).

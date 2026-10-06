# map — repark-connect/src

## Purpose

Product code for `repark-connect`. See [../map.md](../map.md).

## Contents

- `lib.rs` — `mod copy_binary; mod error; mod settings; mod types;` and the re-exports:
  `BatchLimits`, `COPY_SIGNATURE`, `CopyBinaryDecoder`, `DEFAULT_BATCH_BYTES`,
  `DEFAULT_BATCH_ROWS`, `MAX_BATCH_BYTES`, `MAX_FIELD_BYTES`; `ConnectError`, `ProtocolViolation`, `Result`, `UNMAPPED_ROW`,
  `ValueRefusal`; `AUTH_METHOD_KEY`, `AuthMethod`, `ConnectionSettings`; and the `postgres`
  module.
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
  `expected` is an exact codec width, not a maximum. pins: c-2/C-001, C-015
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
  cap forms a one-row batch. `buffered_bytes()` is the memory-charge seam: the C-2c scan
  resizes its DataFusion `MemoryReservation` to it, so C-2a adds no dependency. The C-2a F-1
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
  capacity, so the C-2c reservation seam sees the retained allocation. The C-2a F-5 fold
  (2026-10-06) charges what the builders hold: `ColumnAppender::arrow_width` is the one width
  table (bool 1, int2 2, int4/float4/date 4, int8/float8/timestamps 8, numeric 16), charged for
  values and NULLs; variable values charge stored bytes plus 4 offset bytes (`jsonb` the
  stripped body, `uuid` the 36 rendered bytes) and NULLs the 4 offset bytes; validity charges
  ceil(buffered rows x columns / 8), counted in the flush test and `buffered_bytes()`.
  pins: c-2/C-002, C-003, C-004, C-005, C-006, C-015, C-016, C-017
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
  (no behaviour change). C-2b adds the endpoint keys beside it in `settings/postgres.rs`.
  pins: c-1/C-003, C-004, C-005, C-006
- `types.rs` — `pub mod postgres;` (`mssql` joins with C-5).
- `types/` — [types/map.md](types/map.md): the Postgres type map and its codecs.

## Pointers

- Up: [../map.md](../map.md)

## Debug

First checks: `cargo test -p repark-connect`. Escalate to: [../map.md#debug](../map.md).

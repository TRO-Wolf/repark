# map — repark-connect/src

## Purpose

Product code for `repark-connect`. See [../map.md](../map.md).

## Contents

- `lib.rs` — `mod settings; mod types;` and the re-exports: `AUTH_METHOD_KEY`, `AuthMethod`,
  `ConnectionSettings`, `SettingsError`, and the `postgres` module.
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
  `repark-core` and is out of this crate's reach.
  pins: c-1/C-003, C-004, C-005, C-006
- `types.rs` — `pub mod postgres;` (`mssql` joins with C-5).
- `types/` — [types/map.md](types/map.md): the Postgres type map.

## Pointers

- Up: [../map.md](../map.md)

## Debug

First checks: `cargo test -p repark-connect`. Escalate to: [../map.md#debug](../map.md).

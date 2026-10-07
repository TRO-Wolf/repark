# map — repark-connect/src/settings

## Purpose

The per-backend connection settings that `settings.rs` declares as submodules (CC-3: the owner
crate interprets the props; core only carries them). See [../map.md](../map.md).

## Contents

- `postgres.rs` — C-2b round 1b (2026-10-07; sketch
  [c-2-design.md](../../../../task/wo/c-2-design.md) §2.3, §2.4). `PostgresSettings::from_props(props,
  door)` parses one source's props into a typed value. It first runs C-1's
  `ConnectionSettings::from_props`, so `auth_method` keeps one parser and its declared refusals.
  It then reads the twenty `POSTGRES_KEYS`.
  - **Doors.** `SettingsDoor::ReparkToml` takes the canonical snake_case keys, exact and
    case-sensitive. `SettingsDoor::ReadPostgres` also takes the Spark and pgjdbc aliases
    (`POSTGRES_ALIASES`), matched ASCII case-insensitively, the declared partitioned-read keys,
    and `driver`, which is accepted only as `org.postgresql.Driver` and has no effect.
  - **`url`.** Three schemes: `postgresql://`, `postgres://` and `jdbc:postgresql://`. The
    authority's userinfo ends at its last `@`; user, password, host (a bracketed IPv6 literal
    included), port and database are percent-decoded. The query string takes canonical keys
    under a libpq URI and canonical keys plus aliases under a `jdbc:` URL. `url` and
    `auth_method` never nest inside it.
  - **Conflicts.** One setting given twice (two spellings, or a URL part and a key) refuses with
    `SpecRefusal::Conflict`, naming both `Spelling`s.
  - **Values.** Integers are ASCII digits only, range-checked per key. The three timeout aliases
    are seconds, converted to milliseconds (the range is reported in the alias's unit).
    `read_timeout_ms` `0` refuses (NS-7). Network-wait timeouts (connect, read, pool checkout,
    pool idle) start at 1 ms. `query_timeout_ms` `0` means unlimited (`None`), and
    `lock_timeout_ms` `0` is passed as given. `batch_rows` and `fetchsize` take `1..`, and
    `fetchsize = 0` refuses. Booleans are `true` or `false`, case-insensitive. Text values
    refuse a NUL byte; `host`, `user`, `database` and `sslrootcert` also refuse empty.
  - **TLS.** `sslmode` defaults to `verify-full`; `disable` is the explicit plaintext option
    (`CONNECT-DIV-pg-sslmode`); `prefer`, `allow`, `require` and `verify-ca` refuse as
    `DeclaredSetting::UnverifiedSslmode` (`CONNECT-DECL-sslmode-unverified`).
  - **Declared keys.** `sslcert`, `sslkey` (`CONNECT-DECL-pg-client-cert`);
    `sessionInitStatement`, `customSchema`, `options` (`CONNECT-DECL-pg-session-sql`); a comma
    in the host (`CONNECT-DECL-pg-multi-host`); and the five partitioned-read options on the
    `read_postgres` door (`CONNECT-DECL-pg-partitioned-read`, whose registry row lands with
    C-2d).
  - **Unknown keys.** Any other key refuses with `SpecRefusal::UnknownKey`, which lists the
    accepted keys and never echoes a value (`CONNECT-DIV-pg-unknown-option`).
  - **Redaction and `Debug`.** The struct is `#[non_exhaustive]` with public fields, so it is
    read anywhere and built only here. `Debug` prints `auth_method` and `sslmode` only.
    `redact_source_prop(key, value)` is the owner seam that C-2d's `sources()` rows call: it
    delegates to `repark_common::redaction::redact_value`, which masks a secret-named key whole,
    the URL userinfo password, and a secret-named query parameter.

  pins: c-2/C-018, C-019, C-020, C-021, C-022, C-023, C-024

## Pointers

- Up: [../map.md](../map.md)

## Debug

First checks: `cargo test -p repark-connect --test it settings`. Escalate to:
[../map.md#debug](../map.md).

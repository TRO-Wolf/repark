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
    userinfo ends at the last `@` before the first `/`, as in libpq, and is split off before the
    query is looked for (fold 1 X1), so a password holding `?`, `@`, `#` or `:` stays a
    password. Since fold 2 (Z4) a URL with no `/` after the scheme whose userinfo holds `?` or
    `=` refuses `UrlViolation::AmbiguousUserinfo`, named only as `UrlPart("userinfo")`: there
    the split may have taken a query's `password=` tail (`postgresql://h?user=u&password=a@b`
    reads host `b`); with a path, or with `%3F` and `%3D`, it parses. User, password, host (a bracketed IPv6 literal included), port, database and
    every query name and value are percent-decoded; an escape needs two hex digits, so `%+A`
    refuses `UrlViolation::PercentEncoding` (fold 1). The query string takes canonical keys
    under a libpq URI and canonical keys plus aliases under a `jdbc:` URL. `url` and
    `auth_method` never nest inside it.
  - **Conflicts.** One setting given twice (two spellings, or a URL part and a key) refuses with
    `SpecRefusal::Conflict`, naming both `Spelling`s.
  - **Values.** Integers are ASCII digits only, range-checked per key. The three timeout aliases
    are seconds, converted to milliseconds (the range is reported in the alias's unit).
    `read_timeout_ms` `0` refuses (NS-7). Network-wait timeouts (connect, read, pool checkout,
    pool idle) start at 1 ms. `query_timeout_ms` `0` means unlimited (`None`), and
    `lock_timeout_ms` `0` is passed as given. `batch_rows` takes `1..` on both doors. On the
    `read_postgres` door the `fetchsize` alias also takes `0` (any run of zeros), which leaves
    `batch_rows` unset: the session batch size (round-1b Q1, ruled 2026-10-07; inside a `jdbc:`
    URL query it still refuses). Booleans are `true` or `false`, case-insensitive. Text values
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
    accepted keys and never echoes a value (`CONNECT-DIV-pg-unknown-option`). Inside the `url`
    query the key is named only as `Spelling::UrlPart("query key")`, and a malformed query value
    as `UrlPart("query")`, so no text of the URL (a raw `&` can split a password) is echoed.
  - **Redaction and `Debug`.** The struct is `#[non_exhaustive]` with public fields, so it is
    read anywhere and built only here. `Debug` prints `auth_method` and `sslmode` only.
    `redact_source_prop(key, value)` is the owner seam that C-2d's `sources()` rows call. On a
    Postgres URL it masks the userinfo where the parser splits it (the password, or the whole
    userinfo when it has no `:`) and every query value whose key, percent-decoded, is secret
    (fold 1 X7: `pass%77ord`); then, as for every other value, it delegates to
    `repark_common::redaction::redact_value`, which masks a secret-named key whole.

  pins: c-2/C-018, C-019, C-020, C-021, C-022, C-023, C-024, C-036, C-049, C-050, C-051, C-065

## Pointers

- Up: [../map.md](../map.md)

## Debug

First checks: `cargo test -p repark-connect --test it settings`. Escalate to:
[../map.md#debug](../map.md).

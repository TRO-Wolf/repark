# map — repark-core/src/session/zone_localiser

## Purpose

The pins of [../zone_localiser.rs](../zone_localiser.rs) (`#[cfg(all(test, feature =
"postgres"))] mod tests;`): how a Postgres `timestamp` wall clock is placed in the session zone.

## Contents

- `tests.rs` — **C-2d (2026-10-07):** the placement rule.
  - The offset of the wall clock's own date, and a fixed offset. A gap or an overlap refuses
    naming `CONNECT-DIV-pg-timestamp-zone`.
  - **Fold 1, N1:** a wall clock after 2099 reads its offset from
    `repark_common::zone_horizon::proxy_year`, the year RePark's `TIMESTAMP` literal reads.
    The verifier's New York `2100-07-01 12:00` is `4118140800000000` (EDT). The 2099/2100
    boundary pairs in New York, Sydney and Auckland keep one offset. Southern-hemisphere and
    far-future wall clocks keep their season, up to year 262142. Gaps and overlaps after 2099
    still refuse. Removing the proxy turns three pins red.
  - **Fold 1, N3:** a Java-form session zone (`Z`, `UT`, `GMT+8`, `UTC+05:30`, `-8`, `+3`) is
    canonicalised through `canonical_session_zone_id` before it is parsed, and `zone_label()`
    reports the canonical id. Dropping the canonicalisation, or hard-coding the label to `UTC`
    (the verifier's V6), turns `java_form_session_zones_place_at_their_canonical_offset` red.
  - **Fold 1, S3:** at the end of chrono's calendar, `262142-12-31 23:00` in `-12:00` or New
    York and `262143-01-01 00:00` in UTC refuse as
    `ValueRefusal::TimestampPastCalendar` (`CONNECT-DECL-pg-out-of-range`), never as a gap.
  pins: c-2/C-098, C-108, C-109, C-110, C-111

## Pointers

- Up: [../map.md](../map.md)
- The shared horizon: [../../../../repark-common/src/zone_horizon.rs](../../../../repark-common/src/zone_horizon.rs)

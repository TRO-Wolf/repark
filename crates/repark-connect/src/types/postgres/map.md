# map — repark-connect/src/types/postgres

## Purpose

The per-family codecs behind the Postgres type map in [../postgres.rs](../postgres.rs) (C-2a,
2026-10-06; sketch [c-2-design.md](../../../../../task/wo/c-2-design.md) §2.7). Each codec is a
pure function from one COPY BINARY field (Postgres's `*_send` form) to an Arrow-ready value or a
typed `CodecError`; `ColumnAppender` in `../postgres.rs` dispatches to them and writes into the
Arrow builders. No codec allocates per value. See [../map.md](../map.md).

## Contents

- `numeric.rs` — `DecimalTarget` (the planned `Decimal128(p,s)`) and its resolution from the
  `atttypmod` (`((p << 16) | (s & 0x7ff)) + 4`, the 11-bit scale sign-extended as PostgreSQL 15
  packs it): no modifier gives `Decimal128(38,18)` (Spark's `SYSTEM_DEFAULT`); a constrained
  modifier gives Spark 4.1.2's `DecimalType.boundedPreferIntegralDigits` over pgjdbc's raw
  scale (`s & 0xffff`, so every negative scale lands at `(38,38)`): effective precision
  `max(p,s)` at or under 38 gives `(max(p,s),s)`, past 38 gives `(38, max(0, s-(max(p,s)-38)))`;
  only a precision outside `1..=1000` refuses the column (D-M2 DM2-T05…T10). `decode` reads
  `ndigits`, `weight`, `sign`,
  `dscale` and the base-10000 digits (`numeric_send`) straight into an `i128` at the planned
  scale: each digit contributes `digit × 10^(4·(weight − i) + s)` with checked arithmetic, the
  first dropped decimal decides HALF_UP (away from zero, which is both Spark's
  `Decimal.set` and Postgres's `round_var`), and a magnitude at or above `10^p` refuses
  (`NumericOutOfRange`). Sign words `0xC000` (`NaN`) and `0xD000` / `0xF000` (`±Infinity`)
  refuse per value; any other unknown sign word, and a digit outside `0..10000`, is a malformed
  stream. `builder` makes the `Decimal128Builder` for a target (arrow-rs accepts every target the
  resolution produces, so H-ARROW did not fire). pins: c-2/C-009
- `temporal.rs` — `date` (`int32` days since 2000-01-01, plus `POSTGRES_EPOCH_DAYS` = 10957,
  checked) and `timestamp` / `timestamptz` (`int64` µs since 2000-01-01, plus
  `POSTGRES_EPOCH_MICROS` = 946 684 800 000 000, checked). The infinities (`i32::MAX` /
  `i32::MIN`, `i64::MAX` / `i64::MIN`, `date.h` `DATEVAL_NOEND` / `NOBEGIN`, `timestamp.h`
  `DT_NOEND` / `NOBEGIN`) refuse per value; a timestamp whose 1970-based µs overflow `i64`
  (after 294247-01-10) refuses as out of range. `UTC_ZONE_LABEL` = `"+00:00"`, the label
  iceberg-rust's Arrow schema gives `timestamptz` (`iceberg::arrow::UTC_TIME_ZONE`), so a
  federated join compares like types (C-2d pins the join). pins: c-2/C-007, C-008
- `text_like.rs` — UTF-8-validated text (never lossy), `jsonb` (version byte `01` stripped;
  any other version, or an empty value, is a malformed stream) and `uuid` (16 bytes rendered
  into a 36-byte stack buffer as lowercase `8-4-4-4-12`, `uuid_out`'s form). pins: c-2/C-010

## Pointers

- Up: [../map.md](../map.md)
- Ledger: [c-2-ledger.md](../../../../../task/ledgers/staging/c-2-ledger.md)

## Debug

First checks: `cargo test -p repark-connect --test it postgres_types`. Escalate to:
[../map.md#debug](../map.md).
